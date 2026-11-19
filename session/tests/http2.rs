/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub mod h2 {
    mod owned {
        include!("owned_h2.rs");
    }

    mod owned_upstream {
        include!("owned_upstream_h2.rs");
    }
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::future::poll_fn;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;
    use std::panic::resume_unwind;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Poll;
    use std::time::Duration;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::FutureExt;
    use roxygen_codec::CodecEvent;
    use roxygen_codec::EgressFrame;
    use roxygen_codec::H2ErrorCode;
    use roxygen_codec::HttpCodec;
    use roxygen_codec::HttpCodecCallback;
    use roxygen_codec::HttpException;
    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::buf_queue::BufQueueError;
    use roxygen_core::byte_event::ByteEventType;
    use roxygen_core::byte_event::FieldSectionType;
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
    use roxygen_core::handler::LocalHandlerFuture;
    use roxygen_core::headers::HttpHeaderCode;
    use roxygen_core::headers::HttpHeaders;
    use roxygen_core::http::CodecProtocol;
    use roxygen_core::http::HttpMethod;
    use roxygen_core::http::TransportDirection;
    use roxygen_core::ids::StreamId;
    use roxygen_core::message::HttpMessage;
    use roxygen_core::priority::HttpPriority;
    use roxygen_core::source::HttpSource;
    use roxygen_core::source::LocalSourceFuture;
    use roxygen_core::source_holder::HttpSourceHolder;
    use roxygen_http2::Http2Codec;
    use roxygen_http2::constants::CONNECTION_PREFACE;
    use roxygen_http2::constants::FRAME_HEADER_SIZE;
    use roxygen_http2::flow_control::ConnectionFlowControlCallback;
    use roxygen_http2::flow_control::FlowControlledCodec;
    use roxygen_http2::frame::FrameType;
    use roxygen_http2::settings::SettingsId;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::driver::WorkerDriver;
    use roxygen_runtime::host::Host;
    use roxygen_runtime::wait::CancellableBaton;
    use roxygen_runtime::wait::WaitStatus;
    use roxygen_runtime::worker::WorkerContext;
    use roxygen_transport::byte_event::ByteEventWriteFlags;
    use roxygen_transport::byte_event::SocketByteEventType;
    use roxygen_transport::byte_stream::ByteEventCapability;
    use roxygen_transport::byte_stream::ByteEventRegistrationError;
    use roxygen_transport::byte_stream::LocalTransportFuture;
    use roxygen_transport::byte_stream::PeerIdentity;
    use roxygen_transport::byte_stream::TransportWriteError;
    use roxygen_transport::byte_stream::WriteInfo;
    use tokio::runtime::Builder;
    use tokio::task::LocalSet;

    use crate::http2::Http2DriverState;
    use crate::http2::Http2SessionConfig;
    use crate::http2::Http2StatsEvent;
    use crate::http2::rate_limit::Http2RateLimitConfig;
    use crate::http2::rate_limit::Http2RateLimiter;
    use crate::http2::rate_limit::RateLimitConfig;
    use crate::http2::rate_limit::RateLimitKind;
    use crate::http2::rate_limit::RateLimitSample;
    use crate::http2::scheduler::Http2PriorityScheduler;
    use crate::http2::stats::Http2SessionStats;
    use crate::session::LifecycleEvent;
    use crate::session::LifecycleObserver;
    use crate::session::LifecycleObserverRegistration;
    use crate::session::PreparedOutgoingRequest;
    use crate::session::SessionCore;
    use crate::shutdown::DrainTrigger;
    use crate::uniplex::BufferedEgressStatsSnapshot;
    use crate::uniplex::H2SourceFlowSnapshot;
    use crate::uniplex::H2SourceTimerTestControl;
    use crate::uniplex::UniplexConfig;
    use crate::uniplex::UniplexHandle;
    use crate::uniplex::UniplexRead;
    use crate::uniplex::UniplexResponse;
    use crate::uniplex::UniplexSession;
    use crate::uniplex::UniplexTransport;

    fn run_local<T: 'static>(test: impl Future<Output = T> + 'static) -> T {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime")
            .block_on(LocalSet::new().run_until(test))
    }

    fn run_on_worker<T: Send + 'static>(
        test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
    ) -> T {
        if crate::uniplex_tests::on_hosted_worker() {
            return crate::uniplex_tests::run_on_hosted_worker(test);
        }
        run_local(async move {
            let (worker, driver) = WorkerDriver::new(16).expect("worker driver");
            let call = worker
                .call_local(move |context| {
                    let worker = context.handle().clone();
                    async move {
                        let result = test(context).await;
                        worker.begin_shutdown();
                        result
                    }
                })
                .expect("worker call");
            let (_, result) = tokio::join!(driver, call);
            result.expect("worker call completed")
        })
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FixtureClose {
        Open,
        ShutdownWrite,
        Closed,
        Reset,
    }

    enum FixtureRead {
        Data(Vec<u8>),
        DataEof(Vec<u8>),
        SegmentedData(BufQueue),
        WaitData(Arc<tokio::sync::Notify>, Vec<u8>),
        WaitSegmentedData(Arc<tokio::sync::Notify>, BufQueue),
        WaitDataEof(Arc<tokio::sync::Notify>, Vec<u8>),
        Error(HttpErrorCode),
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

    struct FixtureTransportState {
        reads: VecDeque<FixtureRead>,
        writes: Vec<Vec<u8>>,
        write_flags: Vec<ByteEventWriteFlags>,
        write_error: Option<HttpErrorCode>,
        short_write: bool,
        stall_writes: bool,
        stalled_writes_started: usize,
        write_gate: Option<Arc<tokio::sync::Notify>>,
        write_gate_pending: Option<usize>,
        capability: ByteEventCapability,
        immediate_native_events: bool,
        native_event_limit: Option<usize>,
        native_events_delivered: usize,
        registrations: Vec<HttpByteEventRegistration>,
        transport_dropped: bool,
        drop_receipt: Option<Arc<AtomicUsize>>,
        destroy_on_shutdown: bool,
        peer_identity: Option<PeerIdentity>,
        expected_drops_before_reset: Option<(Arc<AtomicUsize>, usize)>,
        close: FixtureClose,
    }

    struct FixtureTransport {
        state: Rc<RefCell<FixtureTransportState>>,
    }

    impl FixtureTransport {
        fn new(
            reads: impl IntoIterator<Item = FixtureRead>,
        ) -> (Rc<Self>, Rc<RefCell<FixtureTransportState>>) {
            Self::with_capability(reads, ByteEventCapability::Unsupported, false)
        }

        fn with_capability(
            reads: impl IntoIterator<Item = FixtureRead>,
            capability: ByteEventCapability,
            immediate_native_events: bool,
        ) -> (Rc<Self>, Rc<RefCell<FixtureTransportState>>) {
            let state = Rc::new(RefCell::new(FixtureTransportState {
                reads: reads.into_iter().collect(),
                writes: Vec::new(),
                write_flags: Vec::new(),
                write_error: None,
                short_write: false,
                stall_writes: false,
                stalled_writes_started: 0,
                write_gate: None,
                write_gate_pending: None,
                capability,
                immediate_native_events,
                native_event_limit: None,
                native_events_delivered: 0,
                registrations: Vec::new(),
                transport_dropped: false,
                drop_receipt: None,
                destroy_on_shutdown: false,
                peer_identity: None,
                expected_drops_before_reset: None,
                close: FixtureClose::Open,
            }));
            (
                Rc::new(Self {
                    state: Rc::clone(&state),
                }),
                state,
            )
        }
    }

    impl Drop for FixtureTransport {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            state.transport_dropped = true;
            if let Some(receipt) = &state.drop_receipt {
                receipt.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl UniplexTransport for FixtureTransport {
        fn read(
            &self,
            _context: &WorkerContext,
            _maximum: usize,
            _allocation_size: usize,
            timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
            let action = self.state.borrow_mut().reads.pop_front();
            Box::pin(async move {
                match action {
                    Some(FixtureRead::Data(bytes)) => {
                        tokio::task::yield_now().await;
                        Ok(UniplexRead::new(queue(&bytes), false))
                    }
                    Some(FixtureRead::DataEof(bytes)) => {
                        tokio::task::yield_now().await;
                        Ok(UniplexRead::new(queue(&bytes), true))
                    }
                    Some(FixtureRead::SegmentedData(bytes)) => Ok(UniplexRead::new(bytes, false)),
                    Some(FixtureRead::WaitData(ready, bytes)) => {
                        ready.notified().await;
                        Ok(UniplexRead::new(queue(&bytes), false))
                    }
                    Some(FixtureRead::WaitSegmentedData(ready, bytes)) => {
                        ready.notified().await;
                        Ok(UniplexRead::new(bytes, false))
                    }
                    Some(FixtureRead::WaitDataEof(ready, bytes)) => {
                        ready.notified().await;
                        Ok(UniplexRead::new(queue(&bytes), true))
                    }
                    Some(FixtureRead::Error(code)) => {
                        Err(HttpError::with_message(code, b"scripted H2 read error"))
                    }
                    None => {
                        let baton = CancellableBaton::new();
                        tokio::select! {
                            _ = tokio::time::sleep(timeout), if !timeout.is_zero() => {
                                Err(HttpError::with_message(
                                    HttpErrorCode::READ_TIMEOUT,
                                    b"scripted H2 read timeout",
                                ))
                            }
                            _ = baton.wait(cancellation) => {
                                Err(HttpError::with_message(
                                    HttpErrorCode::CORO_CANCELLED,
                                    b"scripted H2 read cancelled",
                                ))
                            }
                        }
                    }
                }
            })
        }

        fn write(
            &self,
            _context: &WorkerContext,
            data: BufQueue,
            flags: ByteEventWriteFlags,
            timeout: Duration,
            _cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
            let length = data.chain_length();
            if let Some(gate) = self.state.borrow_mut().write_gate.take() {
                let state = Rc::clone(&self.state);
                return Box::pin(async move {
                    let mut notified = std::pin::pin!(gate.notified());
                    poll_fn(|context| {
                        let result = notified.as_mut().poll(context);
                        if result.is_pending() {
                            state.borrow_mut().write_gate_pending = Some(length);
                        }
                        result
                    })
                    .await;
                    if let Some(code) = state.borrow_mut().write_error.take() {
                        return Err(TransportWriteError::new(
                            HttpError::with_message(code, b"scripted H2 write error"),
                            length,
                        ));
                    }
                    let bytes = data
                        .coalesce(length)
                        .expect("bounded H2 fixture write")
                        .to_vec();
                    let mut state = state.borrow_mut();
                    state.writes.push(bytes);
                    state.write_flags.push(flags);
                    Ok(WriteInfo {
                        bytes_written: length,
                    })
                });
            }
            if self.state.borrow().stall_writes {
                let mut state = self.state.borrow_mut();
                state.stalled_writes_started = state.stalled_writes_started.saturating_add(1);
                return Box::pin(async move {
                    if timeout.is_zero() {
                        std::future::pending::<()>().await;
                    } else {
                        tokio::time::sleep(timeout).await;
                    }
                    Err(TransportWriteError::new(
                        HttpError::with_message(
                            HttpErrorCode::WRITE_TIMEOUT,
                            b"scripted H2 write timeout",
                        ),
                        0,
                    ))
                });
            }
            if let Some(code) = self.state.borrow_mut().write_error.take() {
                return Box::pin(std::future::ready(Err(TransportWriteError::new(
                    HttpError::with_message(code, b"scripted H2 write error"),
                    length,
                ))));
            }
            let written = if self.state.borrow().short_write {
                length.saturating_sub(1)
            } else {
                length
            };
            let mut data = data;
            let bytes = data
                .split_at_most(written)
                .coalesce(written)
                .expect("bounded H2 fixture write")
                .to_vec();
            let mut state = self.state.borrow_mut();
            state.writes.push(bytes);
            state.write_flags.push(flags);
            Box::pin(std::future::ready(Ok(WriteInfo {
                bytes_written: written,
            })))
        }

        fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            let registrations = {
                let mut state = self.state.borrow_mut();
                state.close = FixtureClose::ShutdownWrite;
                if state.destroy_on_shutdown {
                    state.transport_dropped = true;
                    std::mem::take(&mut state.registrations)
                } else {
                    Vec::new()
                }
            };
            drop(registrations);
            Ok(())
        }

        fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            self.state.borrow_mut().close = FixtureClose::Closed;
            Ok(())
        }

        fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            let mut state = self.state.borrow_mut();
            if let Some((drops, expected)) = &state.expected_drops_before_reset {
                assert_eq!(drops.load(Ordering::SeqCst), *expected);
            }
            state.close = FixtureClose::Reset;
            Ok(())
        }

        fn peer_identity(
            &self,
            _context: &WorkerContext,
        ) -> Result<Option<PeerIdentity>, HttpError> {
            Ok(self.state.borrow().peer_identity.clone())
        }

        fn byte_event_capability(
            &self,
            _context: &WorkerContext,
        ) -> Result<ByteEventCapability, HttpError> {
            Ok(self.state.borrow().capability)
        }

        fn register_byte_event(
            &self,
            _context: &WorkerContext,
            mut registration: HttpByteEventRegistration,
        ) -> Result<(), ByteEventRegistrationError> {
            let (capability, immediate, limit, mut delivered) = {
                let state = self.state.borrow();
                (
                    state.capability,
                    state.immediate_native_events,
                    state.native_event_limit,
                    state.native_events_delivered,
                )
            };
            if capability == ByteEventCapability::Unsupported {
                return Err(ByteEventRegistrationError::Unsupported);
            }
            if immediate || limit.is_some() {
                for event_type in [ByteEventType::NicTx, ByteEventType::CumulativeAck] {
                    if registration.pending_events() & event_type.bit() != 0
                        && limit.is_none_or(|limit| delivered < limit)
                    {
                        let _ = registration.fire(HttpByteEvent::new(event_type));
                        delivered = delivered.saturating_add(1);
                    }
                }
                self.state.borrow_mut().native_events_delivered = delivered;
                if registration.pending_events() != 0 {
                    registration.cancel(
                        &HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"transport byte-event capacity exhausted",
                        ),
                        None,
                    );
                }
            } else {
                self.state.borrow_mut().registrations.push(registration);
            }
            Ok(())
        }
    }

    struct FixtureHandler {
        status: u16,
        response_body: usize,
        response_error: Option<HttpErrorCode>,
        invocations: Rc<Cell<usize>>,
        request_body: Rc<Cell<usize>>,
    }

    struct CoalescedRequestHandler {
        body_events: Rc<RefCell<Vec<(usize, bool)>>>,
    }

    struct UnboundedBodySource {
        inner: HttpFixedSource,
    }

    impl HttpSource for UnboundedBodySource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.inner.read_header_event()
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.inner.read_body_event(u32::MAX)
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.inner.stop_reading(error);
        }
    }

    struct BufferedResponseHandler {
        invocations: Rc<Cell<usize>>,
        body_size: usize,
    }

    struct HoldFirstRequestHandler {
        invocations: Rc<Cell<usize>>,
        held: Rc<RefCell<Option<HttpSourceHolder>>>,
    }

    struct CloseWhenIdleHandler {
        handle: Rc<RefCell<Option<UniplexHandle>>>,
    }

    impl HttpHandler for CloseWhenIdleHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let handle = Rc::clone(&self.handle);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                assert!(
                    handle
                        .borrow()
                        .as_ref()
                        .expect("session handle installed")
                        .close_when_idle()?
                );
                let response = HttpFixedSource::make_fixed_response(200, Some(queue(&[b'x'; 100])))
                    .expect("fixed response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    impl HttpHandler for HoldFirstRequestHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let invocations = Rc::clone(&self.invocations);
            let held = Rc::clone(&self.held);
            Box::pin(async move {
                request.read_header_event().await?;
                let invocation = invocations.get();
                invocations.set(invocation.saturating_add(1));
                if invocation == 0 {
                    held.replace(Some(request));
                }
                let response =
                    HttpFixedSource::make_fixed_response(200, None).expect("rate-limit response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    #[derive(Clone, Copy)]
    enum ErrorHandlerCase {
        StopWithoutResponse,
        NoResponse,
        RuntimeError,
        PeerIdentity,
        ReadBodyThenRespond { status: u16, body: &'static [u8] },
        CodecHeaderError,
        StateMachineHeaderError,
        ResponseBodyError(HttpErrorCode),
        WebSocket,
    }

    #[derive(Default)]
    struct ErrorHandlerProbe {
        invocations: Cell<usize>,
        request_error: RefCell<Option<HttpErrorCode>>,
        partial_message: Cell<bool>,
        request_body: Cell<usize>,
        peer_identity: RefCell<Option<Vec<u8>>>,
        websocket: Cell<bool>,
        response_error_gate: RefCell<Option<Arc<tokio::sync::Notify>>>,
    }

    struct ErrorScenarioHandler {
        case: ErrorHandlerCase,
        probe: Rc<ErrorHandlerProbe>,
    }

    impl HttpHandler for ErrorScenarioHandler {
        fn handle_request(
            &self,
            context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let case = self.case;
            let probe = Rc::clone(&self.probe);
            Box::pin(async move {
                probe.invocations.set(probe.invocations.get() + 1);
                let mut headers = match request.read_header_event().await {
                    Ok(headers) => headers,
                    Err(error) => {
                        probe.request_error.replace(Some(error.code()));
                        probe.partial_message.set(error.http_message().is_some());
                        if matches!(case, ErrorHandlerCase::CodecHeaderError) {
                            let response = HttpFixedSource::make_fixed_response(408, None)
                                .expect("discarded codec-error handler response");
                            return Ok(HttpSourceHolder::new(Box::new(response)));
                        }
                        return Ok(HttpSourceHolder::empty());
                    }
                };
                if matches!(
                    case,
                    ErrorHandlerCase::CodecHeaderError | ErrorHandlerCase::StateMachineHeaderError
                ) {
                    panic!("expected a request header error");
                }
                if matches!(case, ErrorHandlerCase::StopWithoutResponse) {
                    request.stop_reading(None);
                    return Ok(HttpSourceHolder::empty());
                }
                if matches!(case, ErrorHandlerCase::RuntimeError) {
                    return Err(HttpError::with_message(
                        HttpErrorCode::INTERNAL_ERROR,
                        b"scripted handler runtime error",
                    ));
                }
                if matches!(case, ErrorHandlerCase::NoResponse) {
                    return Ok(HttpSourceHolder::empty());
                }
                if matches!(case, ErrorHandlerCase::PeerIdentity) {
                    probe
                        .peer_identity
                        .replace(context.peer_identity().map(<[u8]>::to_vec));
                    let accepted = context.peer_identity() == Some(b"good_actor".as_slice());
                    let response = HttpFixedSource::make_fixed_response(
                        if accepted { 200 } else { 407 },
                        Some(queue(if accepted { b"ok!" } else { b"not ok!" })),
                    )
                    .expect("peer policy response");
                    return Ok(HttpSourceHolder::new(Box::new(response)));
                }
                if matches!(case, ErrorHandlerCase::WebSocket) {
                    probe.websocket.set(
                        headers.headers().is_ingress_websocket_upgrade()
                            && headers.headers_mut().method_string() == Ok("CONNECT"),
                    );
                }

                if !headers.eom() {
                    loop {
                        match request.read_body_event(u32::MAX).await {
                            Ok(event) => {
                                probe
                                    .request_body
                                    .set(probe.request_body.get().saturating_add(
                                        event.as_body().map_or(0, BufQueue::chain_length),
                                    ));
                                if event.eom() {
                                    break;
                                }
                            }
                            Err(error) => {
                                probe.request_error.replace(Some(error.code()));
                                probe.partial_message.set(error.http_message().is_some());
                                break;
                            }
                        }
                    }
                }

                match case {
                    ErrorHandlerCase::ReadBodyThenRespond { status, body } => {
                        let response = HttpFixedSource::make_fixed_response(
                            status,
                            (!body.is_empty()).then(|| queue(body)),
                        )
                        .expect("scripted error response");
                        Ok(HttpSourceHolder::new(Box::new(response)))
                    }
                    ErrorHandlerCase::ResponseBodyError(code) => {
                        let mut source =
                            HeaderThenErrorSource::partial_response(200, code, b"partialbody");
                        source.error_gate = probe.response_error_gate.borrow_mut().take();
                        Ok(HttpSourceHolder::new(Box::new(source)))
                    }
                    ErrorHandlerCase::WebSocket => {
                        let mut response =
                            HttpFixedSource::make_fixed_response(200, Some(queue(&[b'w'; 100])))
                                .expect("websocket response");
                        response
                            .message_mut()
                            .expect("websocket response headers")
                            .set_egress_websocket_upgrade();
                        Ok(HttpSourceHolder::new(Box::new(response)))
                    }
                    ErrorHandlerCase::StopWithoutResponse
                    | ErrorHandlerCase::NoResponse
                    | ErrorHandlerCase::RuntimeError
                    | ErrorHandlerCase::PeerIdentity
                    | ErrorHandlerCase::CodecHeaderError
                    | ErrorHandlerCase::StateMachineHeaderError => Ok(HttpSourceHolder::empty()),
                }
            })
        }
    }

    impl HttpHandler for BufferedResponseHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let invocations = Rc::clone(&self.invocations);
            let body_size = self.body_size;
            Box::pin(async move {
                invocations.set(invocations.get() + 1);
                request.read_header_event().await?;
                let response =
                    HttpFixedSource::make_fixed_response(200, Some(queue(&vec![b'r'; body_size])))
                        .expect("buffered fixture response");
                Ok(HttpSourceHolder::new(Box::new(UnboundedBodySource {
                    inner: response,
                })))
            })
        }
    }

    #[derive(Default)]
    struct ByteEventProbe {
        delivered: RefCell<Vec<HttpByteEvent>>,
        cancelled: RefCell<Vec<(HttpByteEvent, HttpErrorCode)>>,
    }

    impl HttpByteEventCallback for ByteEventProbe {
        fn on_byte_event(&self, event: HttpByteEvent) {
            self.delivered.borrow_mut().push(event);
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancelled.borrow_mut().push((event, error.code()));
        }
    }

    #[derive(Clone, Copy)]
    enum RegisteredResponseKind {
        Simple,
        Trailers,
        NativeOnly,
        Many(usize),
    }

    struct RegisteredResponseHandler {
        probe: Rc<ByteEventProbe>,
        kind: RegisteredResponseKind,
    }

    struct WriteFailProbe {
        probe: Rc<ByteEventProbe>,
        transport: Rc<RefCell<FixtureTransportState>>,
    }

    impl HttpByteEventCallback for WriteFailProbe {
        fn on_byte_event(&self, event: HttpByteEvent) {
            let event_type = event.event_type;
            self.probe.on_byte_event(event);
            if event_type == ByteEventType::TransportWrite {
                self.transport.borrow_mut().write_error =
                    Some(HttpErrorCode::TRANSPORT_WRITE_ERROR);
            }
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.probe.on_byte_event_cancelled(event, error);
        }
    }

    struct CallbackDropProbe {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for CallbackDropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl HttpByteEventCallback for CallbackDropProbe {
        fn on_byte_event(&self, _event: HttpByteEvent) {}

        fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {}
    }

    struct ByteEventResponse {
        header: Option<HttpHeaderEvent>,
    }

    struct EventSequenceSource {
        header: Option<HttpHeaderEvent>,
        body: VecDeque<HttpBodyEvent>,
    }

    struct HeaderThenErrorSource {
        header: Option<HttpHeaderEvent>,
        body: Option<HttpBodyEvent>,
        error: Option<HttpError>,
        error_gate: Option<Arc<tokio::sync::Notify>>,
    }

    struct PendingRequestSource {
        header: Option<HttpHeaderEvent>,
        pending_headers: bool,
        body_reads: Rc<Cell<usize>>,
        stops: Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
    }

    struct BodyOnlyRequestSource {
        body: Option<HttpBodyEvent>,
        header_reads: Rc<Cell<usize>>,
        body_reads: Rc<Cell<usize>>,
    }

    struct DelayedBodyResponseSource {
        header: Option<HttpHeaderEvent>,
        body: Rc<RefCell<Option<HttpBodyEvent>>>,
        release_body: Arc<tokio::sync::Notify>,
    }

    impl HttpSource for DelayedBodyResponseSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
            })))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            if self.body.borrow().is_none() {
                return Box::pin(std::future::ready(Err(HttpError::with_message(
                    HttpErrorCode::DROPPED,
                    b"response body exhausted",
                ))));
            }
            let release = Arc::clone(&self.release_body);
            let body = Rc::clone(&self.body);
            Box::pin(async move {
                release.notified().await;
                body.borrow_mut().take().ok_or_else(|| {
                    HttpError::with_message(HttpErrorCode::DROPPED, b"response body exhausted")
                })
            })
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
            self.release_body.notify_one();
        }
    }

    struct ExhaustConnectionThenDelayHandler {
        invocation: Rc<Cell<usize>>,
        release_body: Arc<tokio::sync::Notify>,
    }

    impl HttpHandler for ExhaustConnectionThenDelayHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let invocation = self.invocation.get();
            self.invocation.set(invocation.saturating_add(1));
            let release_body = Arc::clone(&self.release_body);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                if invocation == 0 {
                    let response =
                        HttpFixedSource::make_fixed_response(200, Some(queue(&vec![b'x'; 65_535])))
                            .expect("connection-exhausting response");
                    return Ok(HttpSourceHolder::new(Box::new(response)));
                }
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(200).expect("response status");
                message.set_http_version(1, 1);
                message
                    .headers_mut()
                    .set(HttpHeaderCode::ContentLength, b"1");
                Ok(HttpSourceHolder::new(Box::new(DelayedBodyResponseSource {
                    header: Some(HttpHeaderEvent::new(message, false).expect("response headers")),
                    body: Rc::new(RefCell::new(Some(HttpBodyEvent::body(queue(b"y"), true)))),
                    release_body,
                })))
            })
        }
    }

    impl HttpSource for BodyOnlyRequestSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.header_reads
                .set(self.header_reads.get().saturating_add(1));
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"prepared request must not read source headers",
            ))))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.body_reads.set(self.body_reads.get().saturating_add(1));
            Box::pin(std::future::ready(self.body.take().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"request body exhausted")
            })))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    impl PendingRequestSource {
        fn post(
            content_length: usize,
            pending_headers: bool,
            body_reads: Rc<Cell<usize>>,
            stops: Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
        ) -> Self {
            let mut message = roxygen_core::message::HttpMessage::new();
            message
                .set_method(HttpMethod::Post)
                .expect("request method");
            message.set_url(b"/cancel", false).expect("request URL");
            message.set_http_version(1, 1);
            message
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            message.headers_mut().set(
                HttpHeaderCode::ContentLength,
                content_length.to_string().as_bytes(),
            );
            Self {
                header: Some(
                    HttpHeaderEvent::new(message, false).expect("non-terminal request headers"),
                ),
                pending_headers,
                body_reads,
                stops,
            }
        }
    }

    impl HttpSource for PendingRequestSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            if self.pending_headers {
                return Box::pin(std::future::pending());
            }
            Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"request header exhausted")
            })))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.body_reads.set(self.body_reads.get().saturating_add(1));
            Box::pin(std::future::pending())
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.stops.borrow_mut().push(error);
        }
    }

    impl HeaderThenErrorSource {
        fn new(status: u16, code: HttpErrorCode) -> Self {
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(status).expect("response status");
            message.set_http_version(1, 1);
            Self {
                header: Some(
                    HttpHeaderEvent::new(message, false).expect("non-terminal response headers"),
                ),
                body: None,
                error: Some(HttpError::with_message(
                    code,
                    b"scripted response body error",
                )),
                error_gate: None,
            }
        }

        fn partial_response(status: u16, code: HttpErrorCode, body: &'static [u8]) -> Self {
            let mut source = Self::new(status, code);
            source.body = Some(HttpBodyEvent::body(queue(body), false));
            source
        }

        fn request(code: HttpErrorCode) -> Self {
            let mut message = roxygen_core::message::HttpMessage::new();
            message
                .set_method(HttpMethod::Post)
                .expect("request method");
            message
                .set_url(b"/bodyError_11", false)
                .expect("request URL");
            message.set_http_version(1, 1);
            message
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            message
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"12");
            Self {
                header: Some(
                    HttpHeaderEvent::new(message, false).expect("non-terminal request headers"),
                ),
                body: Some(HttpBodyEvent::body(queue(&[b'x'; 11]), false)),
                error: Some(HttpError::with_message(
                    code,
                    b"scripted request body error",
                )),
                error_gate: None,
            }
        }
    }

    impl HttpSource for HeaderThenErrorSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
            })))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            if let Some(body) = self.body.take() {
                return Box::pin(std::future::ready(Ok(body)));
            }
            let error = self
                .error
                .take()
                .unwrap_or_else(|| HttpError::new(HttpErrorCode::DROPPED));
            let gate = self.error_gate.take();
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                Err(error)
            })
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    impl HttpSource for EventSequenceSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"request header exhausted")
            })))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::ready(self.body.pop_front().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"request body exhausted")
            })))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    impl HttpHandler for RegisteredResponseHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let probe = Rc::clone(&self.probe);
            let kind = self.kind;
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(200).expect("response status");
                message.set_http_version(1, 1);
                let header_eom = matches!(kind, RegisteredResponseKind::NativeOnly);
                let mut header =
                    HttpHeaderEvent::new(message, header_eom).expect("response headers");
                let header_events = match kind {
                    RegisteredResponseKind::Simple => {
                        ByteEventType::TransportWrite.bit()
                            | ByteEventType::KernelWrite.bit()
                            | ByteEventType::NicTx.bit()
                            | ByteEventType::CumulativeAck.bit()
                    }
                    RegisteredResponseKind::NativeOnly => ByteEventType::NicTx.bit(),
                    RegisteredResponseKind::Trailers | RegisteredResponseKind::Many(_) => 0,
                };
                if header_events != 0 {
                    header
                        .byte_event_registrations_mut()
                        .push(HttpByteEventRegistration::new(None, header_events, &probe));
                }

                let mut body = VecDeque::new();
                match kind {
                    RegisteredResponseKind::Simple => {
                        let mut event = HttpBodyEvent::body(queue(&[b'x'; 100]), true);
                        event
                            .byte_event_registrations_mut()
                            .push(HttpByteEventRegistration::new(
                                None,
                                ByteEventType::TransportWrite.bit()
                                    | ByteEventType::KernelWrite.bit(),
                                &probe,
                            ));
                        body.push_back(event);
                    }
                    RegisteredResponseKind::Trailers => {
                        let mut event = HttpBodyEvent::body(queue(&[b'x'; 100]), false);
                        event
                            .byte_event_registrations_mut()
                            .push(HttpByteEventRegistration::new(
                                None,
                                ByteEventType::TransportWrite.bit(),
                                &probe,
                            ));
                        body.push_back(event);
                        let mut trailers = HttpHeaders::new();
                        trailers.set(b"x-trailer".as_slice(), b"done".as_slice());
                        let mut event = HttpBodyEvent::trailers(trailers);
                        event
                            .byte_event_registrations_mut()
                            .push(HttpByteEventRegistration::new(
                                None,
                                ByteEventType::TransportWrite.bit(),
                                &probe,
                            ));
                        body.push_back(event);
                    }
                    RegisteredResponseKind::Many(count) => {
                        for index in 0..count {
                            let mut event =
                                HttpBodyEvent::body(queue(b"x"), index.saturating_add(1) == count);
                            event.byte_event_registrations_mut().push(
                                HttpByteEventRegistration::new(
                                    None,
                                    ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                                    &probe,
                                ),
                            );
                            body.push_back(event);
                        }
                    }
                    RegisteredResponseKind::NativeOnly => {}
                }
                Ok(HttpSourceHolder::new(Box::new(EventSequenceSource {
                    header: Some(header),
                    body,
                })))
            })
        }
    }

    impl HttpSource for ByteEventResponse {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
            })))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"header-only response has no body",
            ))))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    struct WriteFailureByteEventHandler {
        probe: Rc<WriteFailProbe>,
    }

    impl HttpHandler for WriteFailureByteEventHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let probe = Rc::clone(&self.probe);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(200).expect("response status");
                message.set_http_version(1, 1);
                let mut header = HttpHeaderEvent::new(message, false).expect("response headers");
                header
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::KernelWrite.bit(),
                        &probe,
                    ));
                let mut body = HttpBodyEvent::body(queue(b"body"), true);
                body.byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                        &probe,
                    ));
                Ok(HttpSourceHolder::new(Box::new(EventSequenceSource {
                    header: Some(header),
                    body: VecDeque::from([body]),
                })))
            })
        }
    }

    struct DeadByteEventCallbackHandler {
        drops: Arc<AtomicUsize>,
    }

    struct ProtocolErrorByteEventHandler {
        probe: Rc<ByteEventProbe>,
        release_error: Arc<tokio::sync::Notify>,
    }

    struct LifetimeByteEventProbe {
        transport: Rc<RefCell<FixtureTransportState>>,
        transport_dropped_first: Cell<Option<bool>>,
    }

    impl HttpByteEventCallback for LifetimeByteEventProbe {
        fn on_byte_event(&self, _event: HttpByteEvent) {}

        fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
            self.transport_dropped_first
                .set(Some(self.transport.borrow().transport_dropped));
        }
    }

    struct LifetimeByteEventHandler {
        probe: Rc<LifetimeByteEventProbe>,
    }

    impl HttpHandler for DeadByteEventCallbackHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let drops = Arc::clone(&self.drops);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                let callback = Rc::new(CallbackDropProbe { drops });
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(204).expect("response status");
                message.set_http_version(1, 1);
                let mut header = HttpHeaderEvent::new(message, true).expect("response headers");
                header
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &callback,
                    ));
                drop(callback);
                Ok(HttpSourceHolder::new(Box::new(ByteEventResponse {
                    header: Some(header),
                })))
            })
        }
    }

    impl HttpHandler for ProtocolErrorByteEventHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let probe = Rc::clone(&self.probe);
            let release_error = Arc::clone(&self.release_error);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(204).expect("response status");
                message.set_http_version(1, 1);
                let mut header = HttpHeaderEvent::new(message, true).expect("response headers");
                header
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::NicTx.bit(),
                        &probe,
                    ));
                header.set_egress_headers_callback(move |_| release_error.notify_one());
                Ok(HttpSourceHolder::new(Box::new(ByteEventResponse {
                    header: Some(header),
                })))
            })
        }
    }

    impl HttpHandler for LifetimeByteEventHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let probe = Rc::clone(&self.probe);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(204).expect("response status");
                message.set_http_version(1, 1);
                let mut header = HttpHeaderEvent::new(message, true).expect("response headers");
                header
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::NicTx.bit(),
                        &probe,
                    ));
                Ok(HttpSourceHolder::new(Box::new(ByteEventResponse {
                    header: Some(header),
                })))
            })
        }
    }

    struct PushResponseHandler {
        pushed_error: Option<HttpErrorCode>,
        invocations: Rc<Cell<usize>>,
    }

    struct PendingPushSource {
        header: Option<HttpHeaderEvent>,
        stops: Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
    }

    impl PendingPushSource {
        fn new(stops: Rc<RefCell<Vec<Option<HttpErrorCode>>>>) -> Self {
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).expect("push status");
            message.set_http_version(1, 1);
            Self {
                header: Some(HttpHeaderEvent::new(message, false).expect("push headers")),
                stops,
            }
        }
    }

    impl HttpSource for PendingPushSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"push header exhausted")
            })))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::pending())
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.stops.borrow_mut().push(error);
        }
    }

    struct PendingPushResponseHandler {
        stops: Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
    }

    impl HttpHandler for PendingPushResponseHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let stops = Rc::clone(&self.stops);
            Box::pin(async move {
                assert!(request.read_header_event().await?.eom());
                let mut response = roxygen_core::message::HttpMessage::new();
                response.set_status_code(200).expect("response status");
                response.set_http_version(1, 1);
                let header = HttpHeaderEvent::new(response, false).expect("parent headers");
                let mut promise = roxygen_core::message::HttpMessage::new();
                promise.set_method(HttpMethod::Get).expect("push method");
                promise.set_url(b"/push", false).expect("push URL");
                promise.set_http_version(1, 1);
                promise
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
                let body = [
                    HttpBodyEvent::push_promise(
                        promise,
                        Some(Box::new(PendingPushSource::new(stops))),
                        false,
                    ),
                    HttpBodyEvent::body(BufQueue::new(0), true),
                ]
                .into_iter()
                .collect();
                Ok(HttpSourceHolder::new(Box::new(EventSequenceSource {
                    header: Some(header),
                    body,
                })))
            })
        }
    }

    impl HttpHandler for PushResponseHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let pushed_error = self.pushed_error;
            let invocations = Rc::clone(&self.invocations);
            Box::pin(async move {
                invocations.set(invocations.get() + 1);
                let headers = request.read_header_event().await?;
                assert!(headers.eom());

                let mut response = roxygen_core::message::HttpMessage::new();
                response.set_status_code(200).expect("response status");
                response.set_http_version(1, 1);
                let header =
                    HttpHeaderEvent::new(response, false).expect("parent response headers");

                let mut promise = roxygen_core::message::HttpMessage::new();
                promise.set_method(HttpMethod::Get).expect("push method");
                promise.set_url(b"/push", false).expect("push URL");
                promise.set_http_version(1, 1);
                promise
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
                let pushed: Box<dyn HttpSource> = match pushed_error {
                    Some(code) => Box::new(HeaderThenErrorSource::new(200, code)),
                    None => Box::new(
                        HttpFixedSource::make_fixed_response(200, None)
                            .expect("fixed pushed response"),
                    ),
                };
                let body = [
                    HttpBodyEvent::push_promise(promise, Some(pushed), false),
                    HttpBodyEvent::body(BufQueue::new(0), true),
                ]
                .into_iter()
                .collect();
                Ok(HttpSourceHolder::new(Box::new(EventSequenceSource {
                    header: Some(header),
                    body,
                })))
            })
        }
    }

    impl HttpHandler for FixtureHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let status = self.status;
            let response_body = self.response_body;
            let response_error = self.response_error;
            let invocations = Rc::clone(&self.invocations);
            let request_body = Rc::clone(&self.request_body);
            Box::pin(async move {
                invocations.set(invocations.get() + 1);
                let headers = request.read_header_event().await?;
                if !headers.eom() {
                    loop {
                        let event = request.read_body_event(u32::MAX).await?;
                        let eom = event.eom();
                        match event.into_kind() {
                            roxygen_core::events::HttpBodyEventKind::Body(body) => {
                                request_body.set(request_body.get() + body.chain_length());
                            }
                            roxygen_core::events::HttpBodyEventKind::Suspend(resume) => {
                                let _ = resume.await;
                                continue;
                            }
                            _ => {}
                        }
                        if eom {
                            break;
                        }
                    }
                }
                if let Some(code) = response_error {
                    return Ok(HttpSourceHolder::new(Box::new(HttpErrorSource::new(
                        HttpError::with_message(code, b"scripted response source error"),
                    ))));
                }
                let body = (response_body != 0).then(|| queue(&vec![b'r'; response_body]));
                let response =
                    HttpFixedSource::make_fixed_response(status, body).expect("fixture response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    impl HttpHandler for CoalescedRequestHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let body_events = Rc::clone(&self.body_events);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(!headers.eom());
                loop {
                    let event = request.read_body_event(u32::MAX).await?;
                    let eom = event.eom();
                    let length = event
                        .as_body()
                        .map_or(0, roxygen_core::buf_queue::BufQueue::chain_length);
                    body_events.borrow_mut().push((length, eom));
                    if eom {
                        break;
                    }
                }
                let response =
                    HttpFixedSource::make_fixed_response(200, None).expect("fixed response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    struct DownstreamOutcome {
        invocations: usize,
        request_body: usize,
        wire: Vec<u8>,
        close: FixtureClose,
        error: Option<HttpErrorCode>,
    }

    fn queue(bytes: &[u8]) -> BufQueue {
        BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len())
            .expect("fixture queue capacity")
    }

    #[derive(Clone, Copy)]
    enum PeerControl {
        None,
        Reset(H2ErrorCode),
        Goaway(H2ErrorCode),
        WindowUpdate { connection: bool, amount: u32 },
    }

    fn peer_request_bytes(
        method: HttpMethod,
        body: &[u8],
        padding: Option<u8>,
        control: PeerControl,
    ) -> Vec<u8> {
        peer_request_bytes_with_push(method, body, padding, control, true)
    }

    fn peer_exact_padded_request_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request
            .set_method(HttpMethod::Post)
            .expect("request method");
        request.set_url(b"/padded", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        request
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"10");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("request headers");
        for (body, padding) in [
            (b"".as_slice(), Some(20)),
            (b"hello".as_slice(), None),
            (b"".as_slice(), Some(30)),
            (b"world".as_slice(), None),
            (b"".as_slice(), Some(40)),
        ] {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Body {
                        stream,
                        body: queue(body),
                        padding,
                        end_stream: false,
                    },
                    &mut callback,
                )
                .expect("padded request DATA");
        }
        codec
            .generate(
                &mut output,
                EgressFrame::Body {
                    stream,
                    body: BufQueue::new(0),
                    padding: None,
                    end_stream: true,
                },
                &mut callback,
            )
            .expect("zero-byte request EOM");
        output
            .coalesce(output.chain_length())
            .expect("padded request bytes")
            .to_vec()
    }

    fn peer_request_bytes_with_push(
        method: HttpMethod,
        body: &[u8],
        padding: Option<u8>,
        control: PeerControl,
        enable_push: bool,
    ) -> Vec<u8> {
        peer_request_bytes_with_push_and_limit(method, body, padding, control, enable_push, None)
    }

    fn peer_request_bytes_with_push_and_limit(
        method: HttpMethod,
        body: &[u8],
        padding: Option<u8>,
        control: PeerControl,
        enable_push: bool,
        maximum_streams: Option<u64>,
    ) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        codec
            .egress_settings_mut()
            .set(SettingsId::EnablePush, u64::from(enable_push));
        if let Some(maximum_streams) = maximum_streams {
            codec
                .egress_settings_mut()
                .set(SettingsId::MaxConcurrentStreams, maximum_streams);
        }
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(method).expect("request method");
        request.set_url(b"/fixture", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: body.is_empty(),
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("request headers");
        if !body.is_empty() {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Body {
                        stream,
                        body: queue(body),
                        padding,
                        end_stream: true,
                    },
                    &mut callback,
                )
                .expect("request body");
        }
        match control {
            PeerControl::None => {}
            PeerControl::Reset(code) => {
                codec
                    .generate(
                        &mut output,
                        EgressFrame::Reset { stream, code },
                        &mut callback,
                    )
                    .expect("peer reset");
            }
            PeerControl::Goaway(code) => {
                codec
                    .generate(
                        &mut output,
                        EgressFrame::Goaway {
                            last_stream: stream,
                            code,
                            debug_data: None,
                        },
                        &mut callback,
                    )
                    .expect("peer GOAWAY");
            }
            PeerControl::WindowUpdate { connection, amount } => {
                codec
                    .generate(
                        &mut output,
                        EgressFrame::WindowUpdate {
                            stream: if connection {
                                StreamId::UNSPECIFIED
                            } else {
                                stream
                            },
                            amount,
                        },
                        &mut callback,
                    )
                    .expect("peer WINDOW_UPDATE");
            }
        }
        output
            .coalesce(output.chain_length())
            .expect("fixture input")
            .to_vec()
    }

    fn peer_idle_goaway_bytes(code: H2ErrorCode) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        codec
            .generate(
                &mut output,
                EgressFrame::Goaway {
                    last_stream: StreamId::UNSPECIFIED,
                    code,
                    debug_data: None,
                },
                &mut callback,
            )
            .expect("client GOAWAY");
        output
            .coalesce(output.chain_length())
            .expect("idle GOAWAY bytes")
            .to_vec()
    }

    fn peer_reset_rate_limit_bytes() -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let mut post = roxygen_core::message::HttpMessage::new();
        post.set_method(HttpMethod::Post).expect("POST method");
        post.set_url(b"/held", false).expect("POST URL");
        post.set_http_version(1, 1);
        post.headers_mut().set(HttpHeaderCode::Host, b"example.com");
        post.headers_mut()
            .set(HttpHeaderCode::ContentLength, b"200");
        let held_stream = codec.create_stream();
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: held_stream,
                    message: &post,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("held request headers");
        let initial = output
            .coalesce(output.chain_length())
            .expect("held request bytes")
            .to_vec();
        let mut output = BufQueue::new(1 << 20);
        let mut get = roxygen_core::message::HttpMessage::new();
        get.set_method(HttpMethod::Get).expect("GET method");
        get.set_url(b"/reset", false).expect("GET URL");
        get.set_http_version(1, 1);
        get.headers_mut().set(HttpHeaderCode::Host, b"example.com");
        let reset_stream = codec.create_stream();
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: reset_stream,
                    message: &get,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("reset request headers");
        for _ in 0..=1_000 {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Reset {
                        stream: reset_stream,
                        code: H2ErrorCode::STREAM_CLOSED,
                    },
                    &mut callback,
                )
                .expect("peer reset");
        }
        let resets = output
            .coalesce(output.chain_length())
            .expect("reset-rate bytes")
            .to_vec();
        (initial, resets)
    }

    fn peer_control_rate_limit_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("GET method");
        request.set_url(b"/control", false).expect("GET URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("control request headers");
        codec
            .generate(&mut output, EgressFrame::SettingsAck, &mut callback)
            .expect("settings acknowledgement");
        codec
            .generate(
                &mut output,
                EgressFrame::PingReply { data: 9 },
                &mut callback,
            )
            .expect("ping reply");
        codec
            .generate(
                &mut output,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 1,
                },
                &mut callback,
            )
            .expect("connection window update");
        codec
            .generate(
                &mut output,
                EgressFrame::Priority {
                    stream,
                    priority: HttpPriority::default(),
                },
                &mut callback,
            )
            .expect("priority update");
        for data in [11, 12] {
            codec
                .generate(
                    &mut output,
                    EgressFrame::PingRequest { data: Some(data) },
                    &mut callback,
                )
                .expect("ping request");
        }
        output
            .coalesce(output.chain_length())
            .expect("control-rate bytes")
            .to_vec()
    }

    fn peer_ping_exchange_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(256);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        codec
            .generate(
                &mut output,
                EgressFrame::PingRequest { data: Some(12_345) },
                &mut callback,
            )
            .expect("peer ping request");
        codec
            .generate(
                &mut output,
                EgressFrame::PingReply { data: 54_321 },
                &mut callback,
            )
            .expect("peer ping reply");
        output
            .coalesce(output.chain_length())
            .expect("peer ping exchange")
            .to_vec()
    }

    fn peer_two_requests_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        for path in [b"/one".as_slice(), b"/two".as_slice()] {
            let stream = codec.create_stream();
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_method(HttpMethod::Get).expect("request method");
            request.set_url(path, false).expect("request URL");
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("request headers");
        }
        output
            .coalesce(output.chain_length())
            .expect("two request bytes")
            .to_vec()
    }

    fn peer_two_requests_then_reset_parts(code: H2ErrorCode) -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut initial = BufQueue::new(1 << 20);
        codec
            .generate(&mut initial, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut initial, &mut callback)
            .expect("client settings");
        let mut streams = Vec::new();
        for path in [b"/reset".as_slice(), b"/sibling".as_slice()] {
            let stream = codec.create_stream();
            streams.push(stream);
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_method(HttpMethod::Get).expect("request method");
            request.set_url(path, false).expect("request URL");
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            codec
                .generate(
                    &mut initial,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("request headers");
        }
        let mut reset = BufQueue::new(1 << 10);
        for _ in 0..31 {
            codec
                .generate(
                    &mut reset,
                    EgressFrame::WindowUpdate {
                        stream: StreamId::UNSPECIFIED,
                        amount: 1,
                    },
                    &mut callback,
                )
                .expect("harmless connection window update");
        }
        codec
            .generate(
                &mut reset,
                EgressFrame::Reset {
                    stream: streams[0],
                    code,
                },
                &mut callback,
            )
            .expect("peer reset");
        (
            initial
                .coalesce(initial.chain_length())
                .expect("two request bytes")
                .to_vec(),
            reset
                .coalesce(reset.chain_length())
                .expect("peer reset bytes")
                .to_vec(),
        )
    }

    fn peer_incomplete_get_then_reset_parts(code: H2ErrorCode) -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut initial = BufQueue::new(1 << 20);
        codec
            .generate(&mut initial, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut initial, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("GET method");
        request.set_url(b"/reset", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        codec
            .generate(
                &mut initial,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("incomplete request headers");
        let mut reset = BufQueue::new(64);
        codec
            .generate(
                &mut reset,
                EgressFrame::Reset { stream, code },
                &mut callback,
            )
            .expect("peer reset");
        (
            initial
                .coalesce(initial.chain_length())
                .expect("incomplete request bytes")
                .to_vec(),
            reset
                .coalesce(reset.chain_length())
                .expect("reset bytes")
                .to_vec(),
        )
    }

    fn peer_request_parts() -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut first = BufQueue::new(1 << 20);
        codec
            .generate(&mut first, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut first, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("request method");
        request
            .set_url(b"/before-goaway", false)
            .expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        codec
            .generate(
                &mut first,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("first request headers");

        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("request method");
        request
            .set_url(b"/after-goaway", false)
            .expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        let mut second = BufQueue::new(256);
        codec
            .generate(
                &mut second,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("second request headers");
        (
            first
                .coalesce(first.chain_length())
                .expect("first request bytes")
                .to_vec(),
            second
                .coalesce(second.chain_length())
                .expect("second request bytes")
                .to_vec(),
        )
    }

    fn peer_padded_post_parts(body: &[u8], padding: u8) -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut initial = BufQueue::new(1 << 20);
        codec
            .generate(&mut initial, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut initial, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Post).expect("POST method");
        request.set_url(b"/padded", false).expect("POST URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        request.headers_mut().set(
            HttpHeaderCode::ContentLength,
            body.len().to_string().as_bytes(),
        );
        codec
            .generate(
                &mut initial,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("POST headers");
        let mut data = BufQueue::new(1 << 20);
        let mut offset = 0;
        while offset < body.len() {
            let end = (offset + 16_000).min(body.len());
            let last = end == body.len();
            codec
                .generate(
                    &mut data,
                    EgressFrame::Body {
                        stream,
                        body: queue(&body[offset..end]),
                        padding: last.then_some(padding),
                        end_stream: last,
                    },
                    &mut callback,
                )
                .expect("padded POST body");
            offset = end;
        }
        (
            initial
                .coalesce(initial.chain_length())
                .expect("initial POST bytes")
                .to_vec(),
            data.coalesce(data.chain_length())
                .expect("padded POST bytes")
                .to_vec(),
        )
    }

    fn peer_two_requests_with_flow_update() -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut initial = BufQueue::new(1 << 20);
        codec
            .generate(&mut initial, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut initial, &mut callback)
            .expect("client settings");
        codec
            .generate(
                &mut initial,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 65_535,
                },
                &mut callback,
            )
            .expect("initial connection window update");
        let mut streams = Vec::new();
        for path in [b"/first".as_slice(), b"/second".as_slice()] {
            let stream = codec.create_stream();
            streams.push(stream);
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_method(HttpMethod::Get).expect("request method");
            request.set_url(path, false).expect("request URL");
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            codec
                .generate(
                    &mut initial,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("request headers");
        }
        let mut update = BufQueue::new(64);
        codec
            .generate(
                &mut update,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 131_070,
                },
                &mut callback,
            )
            .expect("connection window update");
        codec
            .generate(
                &mut update,
                EgressFrame::WindowUpdate {
                    stream: streams[0],
                    amount: 65_535,
                },
                &mut callback,
            )
            .expect("first stream window update");
        (
            initial
                .coalesce(initial.chain_length())
                .expect("initial request bytes")
                .to_vec(),
            update
                .coalesce(update.chain_length())
                .expect("window update bytes")
                .to_vec(),
        )
    }

    fn peer_two_requests_with_small_stream_windows() -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut initial = BufQueue::new(1 << 20);
        codec
            .generate(&mut initial, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut initial, &mut callback)
            .expect("client settings");

        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("GET method");
        request.set_url(b"/first", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        let first = codec.create_stream();
        codec
            .generate(
                &mut initial,
                EgressFrame::Headers {
                    stream: first,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("first request headers");
        codec
            .generate(
                &mut initial,
                EgressFrame::Settings {
                    settings: &[roxygen_codec::CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: 10,
                    }],
                },
                &mut callback,
            )
            .expect("small peer stream window");
        codec
            .generate(
                &mut initial,
                EgressFrame::EndOfMessage { stream: first },
                &mut callback,
            )
            .expect("first request EOM");

        request
            .set_url(b"/second", false)
            .expect("second request URL");
        let second = codec.create_stream();
        codec
            .generate(
                &mut initial,
                EgressFrame::Headers {
                    stream: second,
                    message: &request,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("second request headers");

        let mut updates = BufQueue::new(64);
        for stream in [first, second] {
            codec
                .generate(
                    &mut updates,
                    EgressFrame::WindowUpdate {
                        stream,
                        amount: 100,
                    },
                    &mut callback,
                )
                .expect("stream window update");
        }
        (
            initial
                .coalesce(initial.chain_length())
                .expect("initial request bytes")
                .to_vec(),
            updates
                .coalesce(updates.chain_length())
                .expect("window-update bytes")
                .to_vec(),
        )
    }

    fn peer_request_with_stream_window(
        window: u32,
        update_amount: u32,
        connection_update: bool,
    ) -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut initial = BufQueue::new(1 << 20);
        codec
            .generate(&mut initial, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut initial, &mut callback)
            .expect("client settings");
        codec
            .generate(
                &mut initial,
                EgressFrame::Settings {
                    settings: &[roxygen_codec::CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: u64::from(window),
                    }],
                },
                &mut callback,
            )
            .expect("peer stream window");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("GET method");
        request.set_url(b"/pending", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        codec
            .generate(
                &mut initial,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("request headers");
        let mut update = BufQueue::new(64);
        codec
            .generate(
                &mut update,
                EgressFrame::WindowUpdate {
                    stream: if connection_update {
                        StreamId::UNSPECIFIED
                    } else {
                        stream
                    },
                    amount: update_amount,
                },
                &mut callback,
            )
            .expect("stream window update");
        (
            initial
                .coalesce(initial.chain_length())
                .expect("initial request bytes")
                .to_vec(),
            update
                .coalesce(update.chain_length())
                .expect("window update bytes")
                .to_vec(),
        )
    }

    fn peer_connection_window_update(amount: u32) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(64);
        codec
            .generate(
                &mut output,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount,
                },
                &mut callback,
            )
            .expect("connection window update");
        output
            .coalesce(output.chain_length())
            .expect("connection window-update bytes")
            .to_vec()
    }

    fn peer_two_requests_with_open_connection_window() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        codec
            .generate(
                &mut output,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 65_536,
                },
                &mut callback,
            )
            .expect("connection credit for two stream windows");
        for path in [b"/blocked-one".as_slice(), b"/blocked-two".as_slice()] {
            let stream = codec.create_stream();
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_method(HttpMethod::Get).expect("request method");
            request.set_url(path, false).expect("request URL");
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("request headers");
        }
        output
            .coalesce(output.chain_length())
            .expect("two flow-blocked requests")
            .to_vec()
    }

    fn peer_priority_requests_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let mut streams = Vec::new();
        for (path, urgency) in [(b"/slow".as_slice(), 6), (b"/urgent".as_slice(), 1)] {
            let stream = codec.create_stream();
            streams.push(stream);
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_method(HttpMethod::Get).expect("request method");
            request.set_url(path, false).expect("request URL");
            request.set_http_version(1, 1);
            request.set_http_priority(HttpPriority::from_urgency_and_incremental(urgency, false));
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("prioritized request headers");
        }
        codec
            .generate(
                &mut output,
                EgressFrame::Priority {
                    stream: streams[0],
                    priority: HttpPriority::from_urgency_and_incremental(0, false),
                },
                &mut callback,
            )
            .expect("peer priority update");
        output
            .coalesce(output.chain_length())
            .expect("priority request bytes")
            .to_vec()
    }

    fn assert_real_two_downstream_stream_stats() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::DataEof(peer_two_requests_bytes())]);
                let invocations = Rc::new(Cell::new(0));
                let stats = Rc::new(StatsProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(Rc::clone(&stats) as Rc<dyn Http2SessionStats>),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                session.run().await.expect("two-request session");
                assert_eq!(invocations.get(), 2);
                assert_eq!(stats.opened.get(), 2);
                assert_eq!(stats.closed.get(), 2);
                assert_eq!(stats.reused.get(), 1);
                assert_eq!(&*stats.served.borrow(), &[2]);
                assert_eq!(
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2)
                        .header_streams,
                    [StreamId::new(1), StreamId::new(3)]
                );
            })
        });
    }

    #[test]
    fn downstream_h2_runs_without_tokio_runtime() {
        crate::external_host_tests::run_on_host(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::DataEof(peer_two_requests_bytes())]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 3,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .unwrap();
                session.run().await.unwrap();
                assert_eq!(invocations.get(), 2);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                assert_eq!(decoded.header_streams, [StreamId::new(1), StreamId::new(3)]);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_downstream_stream_sequence_numbers() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, _) =
                    FixtureTransport::new([FixtureRead::DataEof(peer_two_requests_bytes())]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                session.run().await.expect("two-request session");
                let sequence_numbers = events
                    .borrow()
                    .iter()
                    .filter_map(|event| match event {
                        LifecycleEvent::RequestBegin {
                            sequence_number, ..
                        } => Some(*sequence_number),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(sequence_numbers, [0, 1]);
            })
        });
    }

    fn assert_real_downstream_peer_stream_limit_transitions() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(
                    peer_stream_limit_transition_bytes(),
                )]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                session.run().await.expect("settings session");
                let transitions = events
                    .borrow()
                    .iter()
                    .filter(|event| {
                        matches!(
                            event,
                            LifecycleEvent::Http2StreamsFull | LifecycleEvent::Http2StreamsNotFull
                        )
                    })
                    .copied()
                    .collect::<Vec<_>>();
                assert_eq!(
                    transitions,
                    [
                        LifecycleEvent::Http2StreamsFull,
                        LifecycleEvent::Http2StreamsNotFull,
                    ]
                );
                assert_eq!(
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 0)
                        .settings_acks,
                    2
                );
            })
        });
    }

    fn assert_real_priority_contract() {
        let mut scheduler = Http2PriorityScheduler::new();
        let urgent = StreamId::new(1);
        let first_incremental = StreamId::new(3);
        let second_incremental = StreamId::new(5);
        let paused = StreamId::new(7);
        assert!(scheduler.insert(urgent, HttpPriority::from_urgency_and_incremental(0, false),));
        for stream in [first_incremental, second_incremental] {
            assert!(scheduler.insert(stream, HttpPriority::from_urgency_and_incremental(2, true),));
        }
        assert!(scheduler.insert(paused, HttpPriority::new(0, false, 0, true, 0)));
        for stream in [urgent, first_incremental, second_incremental] {
            assert!(scheduler.enqueue(stream));
        }
        assert!(!scheduler.enqueue(paused));
        assert_eq!(scheduler.next(), Some(urgent));
        assert!(scheduler.consume(urgent, 0));
        assert_eq!(scheduler.next(), Some(urgent));
        assert!(scheduler.dequeue(urgent));
        assert_eq!(scheduler.next(), Some(first_incremental));
        assert!(scheduler.consume(first_incremental, 1));
        assert_eq!(scheduler.next(), Some(second_incremental));
        assert!(scheduler.pause(paused, false));
        assert!(scheduler.enqueue(paused));
        assert_eq!(scheduler.next(), Some(paused));
        assert!(scheduler.remove(paused));
        assert!(!scheduler.contains(paused));

        let mut scheduler = Http2PriorityScheduler::new();
        let blocked = StreamId::new(9);
        let eligible_first = StreamId::new(11);
        let eligible_second = StreamId::new(13);
        for stream in [blocked, eligible_first, eligible_second] {
            assert!(scheduler.insert(stream, HttpPriority::from_urgency_and_incremental(4, true),));
            assert!(scheduler.enqueue(stream));
        }
        assert_eq!(
            scheduler.next_matching(|stream| stream != blocked),
            Some(eligible_first)
        );
        assert!(scheduler.consume(eligible_first, 1));
        assert_eq!(
            scheduler.next_matching(|stream| stream != blocked),
            Some(eligible_second)
        );

        let outcome = run_downstream_bytes(peer_priority_requests_bytes(), 200, 16);
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.invocations, 2);
        let decoded = decode_server_frames_with_requests(&outcome.wire, 2);
        assert_eq!(decoded.header_streams, [StreamId::new(3), StreamId::new(1)]);
        assert_eq!(decoded.body_streams, [StreamId::new(3), StreamId::new(1)]);
    }

    fn assert_real_head_has_no_data() {
        let outcome = run_downstream_request(HttpMethod::Head, Vec::new(), None, 200, 100);
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.invocations, 1);
        let decoded = decode_server_frames(&outcome.wire);
        assert_eq!(decoded.header_streams, [StreamId::new(1)]);
        assert_eq!(decoded.body_bytes, 0);
    }

    fn server_push_bytes(large_promise: bool, pushed_reset: Option<H2ErrorCode>) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("parent response headers");
        let pushed_stream = codec.create_stream();
        assert_eq!(pushed_stream, StreamId::new(2));
        let mut promise = roxygen_core::message::HttpMessage::new();
        promise.set_method(HttpMethod::Get).expect("push method");
        promise.set_url(b"/push", false).expect("push URL");
        promise.set_http_version(1, 1);
        promise
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        if large_promise {
            promise
                .headers_mut()
                .set(b"x-split-push", vec![b'x'; 20_000]);
        }
        codec
            .generate(
                &mut output,
                EgressFrame::PushPromise {
                    stream: pushed_stream,
                    associated_stream: StreamId::new(1),
                    message: &promise,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("push promise");
        if let Some(code) = pushed_reset {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Reset {
                        stream: pushed_stream,
                        code,
                    },
                    &mut callback,
                )
                .expect("pushed reset");
        } else {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream: pushed_stream,
                        message: &response,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("pushed response");
        }
        codec
            .generate(
                &mut output,
                EgressFrame::EndOfMessage {
                    stream: StreamId::new(1),
                },
                &mut callback,
            )
            .expect("parent response EOM");
        output
            .coalesce(output.chain_length())
            .expect("server push bytes")
            .to_vec()
    }

    fn server_incomplete_push_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("parent response headers");
        let pushed_stream = codec.create_stream();
        assert_eq!(pushed_stream, StreamId::new(2));
        let mut promise = roxygen_core::message::HttpMessage::new();
        promise.set_method(HttpMethod::Get).expect("push method");
        promise.set_url(b"/push", false).expect("push URL");
        promise.set_http_version(1, 1);
        promise
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        codec
            .generate(
                &mut output,
                EgressFrame::PushPromise {
                    stream: pushed_stream,
                    associated_stream: StreamId::new(1),
                    message: &promise,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("push promise");
        codec
            .generate(
                &mut output,
                EgressFrame::EndOfMessage {
                    stream: StreamId::new(1),
                },
                &mut callback,
            )
            .expect("parent response EOM");
        output
            .coalesce(output.chain_length())
            .expect("incomplete server push bytes")
            .to_vec()
    }

    fn server_partial_push_promise_parts() -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("parent response headers");
        let pushed_stream = codec.create_stream();
        assert_eq!(pushed_stream, StreamId::new(2));
        let mut promise = roxygen_core::message::HttpMessage::new();
        promise.set_method(HttpMethod::Get).expect("push method");
        promise.set_url(b"/push", false).expect("push URL");
        promise.set_http_version(1, 1);
        promise
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        promise
            .headers_mut()
            .set(b"x-split-push", vec![b'x'; 20_000]);
        codec
            .generate(
                &mut output,
                EgressFrame::PushPromise {
                    stream: pushed_stream,
                    associated_stream: StreamId::new(1),
                    message: &promise,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("split push promise");
        let mut bytes = output
            .coalesce(output.chain_length())
            .expect("split push wire")
            .to_vec();
        let held = bytes.split_off(bytes.len() - 1);
        (bytes, held)
    }

    fn server_invalid_push_promise_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("parent response headers");
        let pushed_stream = codec.create_stream();
        assert_eq!(pushed_stream, StreamId::new(2));
        let mut promise = roxygen_core::message::HttpMessage::new();
        promise.set_method(HttpMethod::Get).expect("push method");
        promise.set_url(b"/push", false).expect("push URL");
        promise.set_http_version(1, 1);
        promise
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        promise
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"100");
        promise
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"200");
        codec
            .generate(
                &mut output,
                EgressFrame::PushPromise {
                    stream: pushed_stream,
                    associated_stream: StreamId::new(1),
                    message: &promise,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("invalid push promise is serializable");
        output
            .coalesce(output.chain_length())
            .expect("invalid push promise wire")
            .to_vec()
    }

    fn server_reset_release_connection_credit_parts() -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut first = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut first, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        codec
            .generate(
                &mut first,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("first response headers");
        codec
            .generate(
                &mut first,
                EgressFrame::Body {
                    stream: StreamId::new(1),
                    body: queue(&vec![b'a'; 65_535]),
                    padding: None,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("first response body");
        let mut second = BufQueue::new(1 << 20);
        codec
            .generate(
                &mut second,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::CANCEL,
                },
                &mut callback,
            )
            .expect("first response reset");
        codec
            .generate(
                &mut second,
                EgressFrame::Headers {
                    stream: StreamId::new(3),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("second response headers");
        codec
            .generate(
                &mut second,
                EgressFrame::Body {
                    stream: StreamId::new(3),
                    body: queue(&[b'b'; 100]),
                    padding: None,
                    end_stream: true,
                },
                &mut callback,
            )
            .expect("second response body");
        (
            first
                .coalesce(first.chain_length())
                .expect("first response bytes")
                .to_vec(),
            second
                .coalesce(second.chain_length())
                .expect("reset/second response bytes")
                .to_vec(),
        )
    }

    fn server_response_then_late_body_parts() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut headers = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut headers, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        codec
            .generate(
                &mut headers,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("response headers");
        let mut body = BufQueue::new(1 << 20);
        codec
            .generate(
                &mut body,
                EgressFrame::Body {
                    stream: StreamId::new(1),
                    body: queue(&vec![b'x'; 65_535 / 2]),
                    padding: None,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("late response body");
        let mut final_body = BufQueue::new(64);
        codec
            .generate(
                &mut final_body,
                EgressFrame::Body {
                    stream: StreamId::new(1),
                    body: queue(b"x"),
                    padding: None,
                    end_stream: true,
                },
                &mut callback,
            )
            .expect("threshold-crossing late response body");
        (
            headers
                .coalesce(headers.chain_length())
                .expect("response header bytes")
                .to_vec(),
            body.coalesce(body.chain_length())
                .expect("response body bytes")
                .to_vec(),
            final_body
                .coalesce(final_body.chain_length())
                .expect("final response body bytes")
                .to_vec(),
        )
    }

    fn server_push_truncated_mid_continuation() -> Vec<u8> {
        let wire = server_push_bytes(true, None);
        let mut offset = 0;
        while offset + FRAME_HEADER_SIZE <= wire.len() {
            let length = (usize::from(wire[offset]) << 16)
                | (usize::from(wire[offset + 1]) << 8)
                | usize::from(wire[offset + 2]);
            if wire[offset + 3] == FrameType::CONTINUATION.as_u8() {
                return wire[..offset + FRAME_HEADER_SIZE + length.max(2) / 2].to_vec();
            }
            offset = offset.saturating_add(FRAME_HEADER_SIZE + length);
        }
        panic!("large push promise must contain a CONTINUATION frame")
    }

    fn assert_pending_push_cleanup(eof: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let partial = server_push_truncated_mid_continuation();
                let reads = if eof {
                    vec![FixtureRead::DataEof(partial)]
                } else {
                    vec![FixtureRead::Data(partial)]
                };
                let (transport, state) = FixtureTransport::new(reads);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut run = Box::pin(session.run());
                if !eof {
                    let headers = tokio::time::timeout(Duration::from_secs(1), async {
                        tokio::select! {
                            result = response.read_header_event() => result.expect("parent response headers"),
                            result = run.as_mut() => panic!("session ended before parent headers: {result:?}"),
                        }
                    })
                    .await
                    .expect("parent headers timed out");
                    assert!(!headers.eom());
                    drop(run);
                    let error = match response.read_body_event(u32::MAX).await {
                        Ok(_) => panic!("run cancellation produced parent body"),
                        Err(error) => error,
                    };
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    drop(response);
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while !handle.is_finished() {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("pending push cancellation cleanup timed out");
                    assert_eq!(state.borrow().close, FixtureClose::Reset);
                } else {
                    let error = tokio::time::timeout(Duration::from_secs(1), async {
                        tokio::select! {
                            result = response.read_header_event() => match result {
                                Ok(_) => panic!("truncated push promise produced complete parent headers"),
                                Err(error) => error,
                            },
                            result = run.as_mut() => panic!("session ended before source error: {result:?}"),
                        }
                    })
                    .await
                    .expect("truncated push source error timed out");
                    assert!(error.http_message().is_some());
                    drop(response);
                    let run_error = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                        .await
                        .expect("truncated push cleanup timed out")
                        .expect_err("truncated push promise fails the session");
                    assert_eq!(run_error.code(), error.code());
                }
            })
        });
    }

    #[test]
    fn pending_push_holder_is_cancelled_when_run_is_abandoned_mid_continuation() {
        assert_pending_push_cleanup(false);
    }

    #[test]
    fn pending_push_holder_is_cancelled_on_eof_mid_continuation() {
        assert_pending_push_cleanup(true);
    }

    fn server_settings_bytes(maximum: u32) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        codec
            .egress_settings_mut()
            .set(SettingsId::MaxConcurrentStreams, u64::from(maximum));
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        output
            .coalesce(output.chain_length())
            .expect("server settings bytes")
            .to_vec()
    }

    fn peer_stream_limit_transition_bytes() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .egress_settings_mut()
            .set(SettingsId::MaxConcurrentStreams, 0);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("full settings");
        codec
            .egress_settings_mut()
            .set(SettingsId::MaxConcurrentStreams, 10);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("not-full settings");
        output
            .coalesce(output.chain_length())
            .expect("settings transition bytes")
            .to_vec()
    }

    fn server_response_bytes(body_size: usize, request_body_size: usize) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        if request_body_size > 65_535 {
            let amount =
                u32::try_from(request_body_size - 65_535).expect("fixture flow-control increment");
            for stream in [StreamId::UNSPECIFIED, StreamId::new(1)] {
                codec
                    .generate(
                        &mut output,
                        EgressFrame::WindowUpdate { stream, amount },
                        &mut callback,
                    )
                    .expect("request flow-control update");
            }
        }
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        response.set_http_version(1, 1);
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: body_size == 0,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("response headers");
        if body_size != 0 {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Body {
                        stream: StreamId::new(1),
                        body: queue(&vec![b's'; body_size]),
                        padding: None,
                        end_stream: true,
                    },
                    &mut callback,
                )
                .expect("response body");
        }
        output
            .coalesce(output.chain_length())
            .expect("server response bytes")
            .to_vec()
    }

    fn server_response_then_reset_parts(body_size: usize, code: H2ErrorCode) -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut response_bytes = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut response_bytes, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        response.set_http_version(1, 1);
        codec
            .generate(
                &mut response_bytes,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: body_size == 0,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("response headers");
        if body_size != 0 {
            codec
                .generate(
                    &mut response_bytes,
                    EgressFrame::Body {
                        stream: StreamId::new(1),
                        body: queue(&vec![b's'; body_size]),
                        padding: None,
                        end_stream: true,
                    },
                    &mut callback,
                )
                .expect("response body");
        }
        let response_bytes = response_bytes
            .coalesce(response_bytes.chain_length())
            .expect("response wire")
            .to_vec();
        let mut reset_bytes = BufQueue::new(64);
        codec
            .generate(
                &mut reset_bytes,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code,
                },
                &mut callback,
            )
            .expect("late response reset");
        let reset_bytes = reset_bytes
            .coalesce(reset_bytes.chain_length())
            .expect("reset wire")
            .to_vec();
        (response_bytes, reset_bytes)
    }

    fn server_response_flow_parts(body_size: usize) -> (Vec<u8>, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        response.set_http_version(1, 1);
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("response headers");
        let headers = output
            .coalesce(output.chain_length())
            .expect("response header bytes")
            .to_vec();
        output = BufQueue::new(1 << 20);
        codec
            .generate(
                &mut output,
                EgressFrame::Body {
                    stream: StreamId::new(1),
                    body: queue(&vec![b'f'; body_size]),
                    padding: None,
                    end_stream: true,
                },
                &mut callback,
            )
            .expect("response body");
        let body = output
            .coalesce(output.chain_length())
            .expect("response body bytes")
            .to_vec();
        (headers, body)
    }

    fn server_goaway_bytes(
        last_stream: StreamId,
        code: H2ErrorCode,
        response_body: Option<usize>,
    ) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        codec
            .generate(
                &mut output,
                EgressFrame::Goaway {
                    last_stream,
                    code,
                    debug_data: None,
                },
                &mut callback,
            )
            .expect("server GOAWAY");
        if let Some(body_size) = response_body {
            let mut response = roxygen_core::message::HttpMessage::new();
            response.set_status_code(200).expect("response status");
            response.set_http_version(1, 1);
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream: StreamId::new(1),
                        message: &response,
                        end_stream: body_size == 0,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("response headers");
            if body_size != 0 {
                codec
                    .generate(
                        &mut output,
                        EgressFrame::Body {
                            stream: StreamId::new(1),
                            body: queue(&vec![b'g'; body_size]),
                            padding: None,
                            end_stream: true,
                        },
                        &mut callback,
                    )
                    .expect("response body");
            }
        }
        output
            .coalesce(output.chain_length())
            .expect("server GOAWAY bytes")
            .to_vec()
    }

    fn server_window_update_bytes(stream: StreamId) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        codec
            .generate(
                &mut output,
                EgressFrame::WindowUpdate {
                    stream,
                    amount: i32::MAX as u32,
                },
                &mut callback,
            )
            .expect("server WINDOW_UPDATE");
        output
            .coalesce(output.chain_length())
            .expect("server WINDOW_UPDATE bytes")
            .to_vec()
    }

    fn server_two_empty_responses_bytes() -> Vec<u8> {
        server_empty_responses_bytes([StreamId::new(1), StreamId::new(3)])
    }

    fn server_empty_responses_bytes(streams: impl IntoIterator<Item = StreamId>) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        response.set_http_version(1, 1);
        for stream in streams {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream,
                        message: &response,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("response headers");
        }
        output
            .coalesce(output.chain_length())
            .expect("response bytes")
            .to_vec()
    }

    #[test]
    fn upstream_h2_reuses_capacity_with_one_hundred_unread_completed_responses() {
        run_on_worker(|context| {
            Box::pin(async move {
                let zero = Arc::new(tokio::sync::Notify::new());
                let reopen = Arc::new(tokio::sync::Notify::new());
                let finish = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(server_empty_responses_bytes(
                        (0..100).map(|index| StreamId::new(1 + 2 * index)),
                    )),
                    FixtureRead::WaitData(Arc::clone(&zero), server_settings_bytes(0)),
                    FixtureRead::WaitData(Arc::clone(&reopen), server_settings_bytes(100)),
                    FixtureRead::WaitDataEof(
                        Arc::clone(&finish),
                        server_empty_responses_bytes([StreamId::new(201)]),
                    ),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .unwrap();
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .unwrap();
                let mut responses = Vec::new();
                for _ in 0..100 {
                    responses.push(
                        handle
                            .send_request(
                                HttpSourceHolder::new(Box::new(
                                    HttpFixedSource::make_get_request(b"/", None).unwrap(),
                                )),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap(),
                    );
                }
                assert_eq!(handle.outgoing_transaction_activity().available(), 0);
                let exercise = async {
                    for response in &responses {
                        response
                            .source
                            .ingress_completion()
                            .unwrap()
                            .wait()
                            .await
                            .unwrap();
                        assert!(response.source.readable());
                        assert!(handle.outgoing_capacity_released(response.stream).unwrap());
                    }
                    assert_eq!(handle.outgoing_transaction_activity().available(), 100);
                    zero.notify_one();
                    while !events.borrow().contains(&LifecycleEvent::Http2Setting {
                        setting: roxygen_codec::CodecSetting {
                            identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                            value: 0,
                        },
                    }) {
                        tokio::task::yield_now().await;
                    }
                    assert_eq!(handle.outgoing_transaction_activity().available(), 0);
                    assert!(
                        handle.reserve_request().is_err(),
                        "peer zero wins over previously released capacity"
                    );
                    reopen.notify_one();
                    while !handle.outgoing_transaction_activity().supports_more() {
                        tokio::task::yield_now().await;
                    }
                    let next = handle
                        .send_request(
                            HttpSourceHolder::new(Box::new(
                                HttpFixedSource::make_get_request(b"/101", None).unwrap(),
                            )),
                            CancellationToken::never(),
                        )
                        .await
                        .expect("request101 is admitted while100 responses remain unread");
                    assert_eq!(next.stream, StreamId::new(201));
                    finish.notify_one();
                    next.source
                        .ingress_completion()
                        .unwrap()
                        .wait()
                        .await
                        .unwrap();
                    assert_eq!(
                        handle.outgoing_transaction_activity().available(),
                        0,
                        "known EOF keeps admission closed"
                    );
                    drop((responses, next));
                };
                let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(session.run(), exercise)
                })
                .await
                .expect("completed responses release H2 capacity without consumer progress");
                result.unwrap();
                assert_eq!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .header_streams
                        .len(),
                    101
                );
                assert_eq!(
                    events
                        .borrow()
                        .iter()
                        .filter(|event| matches!(
                            event,
                            LifecycleEvent::OutgoingCapacityReleased { .. }
                        ))
                        .count(),
                    101
                );
            })
        });
    }

    #[test]
    fn upstream_h2_response_eom_does_not_release_a_pending_upload() {
        run_on_worker(|context| {
            Box::pin(async move {
                let finish = Arc::new(tokio::sync::Notify::new());
                let (transport, _) = FixtureTransport::new([
                    FixtureRead::Data(server_response_bytes(0, 0)),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        max_concurrent_outgoing_streams: 1,
                        ..Http2SessionConfig::default()
                    },
                )
                .unwrap();
                let cancellation = CancellationSource::new();
                let body_reads = Rc::new(Cell::new(0));
                let stops = Rc::new(RefCell::new(Vec::new()));
                let response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(PendingRequestSource::post(
                            1_000,
                            false,
                            body_reads,
                            Rc::clone(&stops),
                        ))),
                        cancellation.token(),
                    )
                    .await
                    .unwrap();
                let exercise = async {
                    response
                        .source
                        .ingress_completion()
                        .unwrap()
                        .wait()
                        .await
                        .unwrap();
                    assert_eq!(handle.outgoing_transaction_activity().available(), 0);
                    assert!(!handle.outgoing_capacity_released(response.stream).unwrap());
                    assert!(handle.reserve_request().is_err());
                    cancellation.request_cancellation();
                    drop(response);
                    while !handle.outgoing_transaction_activity().supports_more() {
                        tokio::task::yield_now().await;
                    }
                    let replacement = handle
                        .reserve_request()
                        .expect("cancelled upload retires once");
                    drop(replacement);
                    assert_eq!(handle.outgoing_transaction_activity().available(), 1);
                    finish.notify_one();
                };
                let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(session.run(), exercise)
                })
                .await
                .expect("cancelled upload retires after both source owners are released");
                result.unwrap();
                assert_eq!(stops.borrow().len(), 1);
            })
        });
    }

    #[test]
    fn upstream_empty_terminal_data_survives_a_negative_stream_window() {
        run_on_worker(|context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let mut settings = BufQueue::new(4096);
                peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                    .unwrap();
                let mut shrink = eligibility_frame(
                    &mut peer,
                    EgressFrame::Settings {
                        settings: &[roxygen_codec::CodecSetting {
                            identifier: SettingsId::InitialWindowSize.as_u16(),
                            value: 0,
                        }],
                    },
                );
                shrink.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::PingRequest { data: Some(44) },
                ));
                let mut response_headers = HttpMessage::new();
                response_headers.set_status_code(200).unwrap();
                response_headers.set_http_version(1, 1);
                let peer_finish = eligibility_frame(
                    &mut peer,
                    EgressFrame::Headers {
                        stream,
                        message: &response_headers,
                        end_stream: true,
                        extra_headers: None,
                    },
                );
                let send_shrink = Arc::new(tokio::sync::Notify::new());
                let send_terminal = Arc::new(tokio::sync::Notify::new());
                let send_response = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(settings.coalesce(settings.chain_length()).unwrap().to_vec()),
                    FixtureRead::WaitData(Arc::clone(&send_shrink), shrink),
                    FixtureRead::WaitDataEof(Arc::clone(&send_response), peer_finish),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        ..Http2SessionConfig::default()
                    },
                )
                .unwrap();
                let release_terminal = Arc::clone(&send_terminal);
                let mut request_headers = committed_test_headers(b"/negative-window", false);
                request_headers
                    .headers_mut()
                    .headers_mut()
                    .set(HttpHeaderCode::ContentLength, b"1");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(EventSequenceSource {
                            header: Some(request_headers),
                            body: VecDeque::from([
                                HttpBodyEvent::body(queue(b"r"), false),
                                HttpBodyEvent::suspend(Box::pin(async move {
                                    release_terminal.notified().await;
                                    ResumeStatus::Signalled
                                })),
                                HttpBodyEvent::body(BufQueue::new(0), true),
                            ]),
                        })),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                let completion = response.completion.clone();
                let mut run = Box::pin(session.run());
                let progress = async {
                    complete_credit_wait(|| {
                        complete_credit_wire(true, &state).frames.body_bytes == 1
                    })
                    .await;
                    assert!(complete_credit_wire(true, &state).completed.is_empty());
                    send_shrink.notify_one();
                    complete_credit_wait(|| {
                        complete_credit_wire(true, &state).frames.ping_replies == [44]
                    })
                    .await;
                    assert_eq!(handle.http2_stream_send_credit(stream).unwrap(), 0);
                    assert!(complete_credit_wire(true, &state).completed.is_empty());
                    send_terminal.notify_one();
                    complete_credit_wait(|| {
                        complete_credit_wire(true, &state).completed == [stream]
                    })
                    .await;
                };
                let mut early_run = None;
                let progress = tokio::select! {
                    result = run.as_mut() => {
                        early_run = Some(result);
                        None
                    },
                    result = AssertUnwindSafe(tokio::time::timeout(
                        Duration::from_secs(1), progress,
                    )).catch_unwind() => Some(result),
                };
                send_shrink.notify_one();
                send_terminal.notify_one();
                send_response.notify_one();
                let consume = async {
                    let headers = response.source.read_header_event().await;
                    drop(response);
                    (headers, completion.wait().await)
                };
                let (run_result, (headers, receipt)) =
                    tokio::time::timeout(Duration::from_secs(2), async {
                        match early_run {
                            Some(result) => (result, consume.await),
                            None => tokio::join!(run.as_mut(), consume),
                        }
                    })
                    .await
                    .expect("negative-window fixture joins after releasing its gates");
                run_result.expect("empty terminal request DATA survives negative stream credit");
                progress
                    .expect("session remained open until request EOM")
                    .unwrap_or_else(|payload| resume_unwind(payload))
                    .expect("empty terminal request DATA reaches the peer without WINDOW_UPDATE");
                assert!(headers.unwrap().eom());
                receipt.unwrap();
                let decoded = complete_credit_wire(true, &state);
                assert_eq!(decoded.frames.body_data, b"r");
                assert_eq!(decoded.completed, [stream]);
                assert!(decoded.frames.resets.is_empty());
                assert!(decoded.frames.errors.is_empty());
            })
        });
    }

    #[test]
    fn upstream_request_headers_remain_fifo_across_reversed_priorities() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(
                    server_two_empty_responses_bytes(),
                )]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut first =
                    HttpFixedSource::make_get_request(b"/first", None).expect("first request");
                first
                    .message_mut()
                    .expect("first headers")
                    .set_http_priority(HttpPriority::from_urgency_and_incremental(7, false));
                let mut second =
                    HttpFixedSource::make_get_request(b"/second", None).expect("second request");
                second
                    .message_mut()
                    .expect("second headers")
                    .set_http_priority(HttpPriority::from_urgency_and_incremental(0, false));
                let mut first_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(first)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("first request admitted")
                    .source;
                let mut second_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(second)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("second request admitted")
                    .source;
                let consume = async move {
                    assert!(
                        first_response
                            .read_header_event()
                            .await
                            .expect("first response")
                            .eom()
                    );
                    assert!(
                        second_response
                            .read_header_event()
                            .await
                            .expect("second response")
                            .eom()
                    );
                };
                let (run, ()) = tokio::join!(session.run(), consume);
                run.expect("two-request session completes");
                assert_eq!(
                    decode_client_frames(&state.borrow().writes.concat()).header_streams,
                    [StreamId::new(1), StreamId::new(3)]
                );
            })
        });
    }

    fn server_duplicate_content_length_response() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        let mut response = roxygen_core::message::HttpMessage::new();
        response.set_status_code(200).expect("response status");
        response
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"100");
        response
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"200");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("invalid response headers are serializable");
        output
            .coalesce(output.chain_length())
            .expect("invalid response wire")
            .to_vec()
    }

    fn peer_request_message_bytes(
        request: roxygen_core::message::HttpMessage,
        body: Option<BufQueue>,
        end_stream: bool,
    ) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: end_stream && body.is_none(),
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("request headers");
        if let Some(body) = body {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Body {
                        stream,
                        body,
                        padding: None,
                        end_stream,
                    },
                    &mut callback,
                )
                .expect("request body");
        }
        output
            .coalesce(output.chain_length())
            .expect("request wire")
            .to_vec()
    }

    fn peer_duplicate_content_length_request() -> Vec<u8> {
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("request method");
        request.set_url(b"/", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        request
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"100");
        request
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"200");
        peer_request_message_bytes(request, None, true)
    }

    fn peer_duplicate_content_length_requests(count: usize) -> Vec<FixtureRead> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let mut chunks = Vec::with_capacity(count);
        for index in 0..count {
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_method(HttpMethod::Get).expect("GET method");
            request
                .set_url(format!("/{index}").as_bytes(), false)
                .expect("request URL");
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            request
                .headers_mut()
                .add(HttpHeaderCode::ContentLength, b"100");
            request
                .headers_mut()
                .add(HttpHeaderCode::ContentLength, b"200");
            let stream = codec.create_stream();
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("duplicate content-length request");
            let bytes = output
                .coalesce(output.chain_length())
                .expect("duplicate content-length wire")
                .to_vec();
            chunks.push(FixtureRead::Data(bytes));
            output = BufQueue::new(1 << 20);
        }
        chunks
    }

    fn peer_content_length_mismatch_request() -> Vec<u8> {
        let mut request = roxygen_core::message::HttpMessage::new();
        request
            .set_method(HttpMethod::Post)
            .expect("request method");
        request.set_url(b"/", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        request
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"200");
        peer_request_message_bytes(request, Some(queue(&[b'x'; 100])), true)
    }

    fn peer_websocket_request() -> Vec<u8> {
        let mut request = roxygen_core::message::HttpMessage::new();
        request
            .set_method(HttpMethod::Connect)
            .expect("extended CONNECT method");
        request.set_url(b"/", false).expect("request URL");
        request.set_http_version(1, 1);
        request.set_secure(true);
        request.set_upgrade_protocol(b"websocket");
        request.set_egress_websocket_upgrade();
        request.headers_mut().set(HttpHeaderCode::Host, b"foo.com");
        peer_request_message_bytes(request, Some(queue(&[b'w'; 100])), true)
    }

    fn peer_state_machine_error() -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("request method");
        request.set_url(b"/", false).expect("request URL");
        request.set_http_version(1, 1);
        request.headers_mut().set(b"cache-control", b"max-age=0");
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("request headers");
        let mut trailers = HttpHeaders::new();
        trailers.set(b"x-forwarded-for", b"");
        let mut trailer_wire = BufQueue::new(1024);
        codec
            .generate(
                &mut trailer_wire,
                EgressFrame::Trailers {
                    stream,
                    trailers: &trailers,
                },
                &mut callback,
            )
            .expect("request trailers");
        let trailer_length = trailer_wire.chain_length();
        let mut trailer_bytes = trailer_wire
            .coalesce(trailer_length)
            .expect("trailer wire")
            .to_vec();
        trailer_bytes[4] &= !0x1;
        let mut trailer_queue = queue(&trailer_bytes);
        output.append(&mut trailer_queue).expect("append trailers");
        codec
            .generate(
                &mut output,
                EgressFrame::Body {
                    stream,
                    body: queue(&[b'x'; 10]),
                    padding: None,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("body after trailers");
        output
            .coalesce(output.chain_length())
            .expect("invalid state-machine wire")
            .to_vec()
    }

    fn peer_stream_zero_request() -> Vec<u8> {
        let mut wire = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
        let mut offset = 24;
        while offset + 9 <= wire.len() {
            let length = (usize::from(wire[offset]) << 16)
                | (usize::from(wire[offset + 1]) << 8)
                | usize::from(wire[offset + 2]);
            if wire[offset + 3] == FrameType::HEADERS.as_u8() {
                wire[offset + 5..offset + 9].fill(0);
                break;
            }
            offset = offset.saturating_add(9).saturating_add(length);
        }
        wire
    }

    fn server_reset_bytes(code: H2ErrorCode) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("server settings");
        codec
            .generate(
                &mut output,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code,
                },
                &mut callback,
            )
            .expect("server reset");
        output
            .coalesce(output.chain_length())
            .expect("server reset bytes")
            .to_vec()
    }

    fn peer_incomplete_post_bytes(body_size: usize) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut callback = IgnoreCodecEvents;
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        let stream = codec.create_stream();
        let mut request = roxygen_core::message::HttpMessage::new();
        request
            .set_method(HttpMethod::Post)
            .expect("request method");
        request.set_url(b"/", false).expect("request URL");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        request.headers_mut().set(
            HttpHeaderCode::ContentLength,
            body_size.to_string().as_bytes(),
        );
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("request headers");
        codec
            .generate(
                &mut output,
                EgressFrame::Body {
                    stream,
                    body: queue(&vec![b'x'; body_size]),
                    padding: None,
                    end_stream: false,
                },
                &mut callback,
            )
            .expect("incomplete request body");
        output
            .coalesce(output.chain_length())
            .expect("incomplete request bytes")
            .to_vec()
    }

    fn assert_real_upstream_exchange(request_body_size: usize, response_body_size: usize) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = server_response_bytes(response_body_size, request_body_size);
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let body = (request_body_size != 0).then(|| queue(&vec![b'c'; request_body_size]));
                let request = if let Some(body) = body {
                    HttpFixedSource::make_fixed_request(b"/", HttpMethod::Post, Some(body))
                        .expect("fixed POST")
                } else {
                    HttpFixedSource::make_get_request(b"/", None).expect("fixed GET")
                };
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let consume = async move {
                    let headers = response
                        .read_header_event()
                        .await
                        .expect("response headers");
                    let mut received = 0;
                    if !headers.eom() {
                        loop {
                            let event = response
                                .read_body_event(u32::MAX)
                                .await
                                .expect("response body");
                            received += event.as_body().map_or(0, BufQueue::chain_length);
                            if event.eom() {
                                break;
                            }
                        }
                    }
                    received
                };
                let (run, received) = tokio::join!(session.run(), consume);
                run.expect("upstream H2 session completes");
                assert_eq!(received, response_body_size);
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                assert_eq!(decoded.body_bytes, request_body_size);
                assert!(decoded.resets.is_empty());
            })
        });
    }

    fn assert_real_upstream_response_followed_by_reset(request_body_pending: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (response_bytes, reset_bytes) =
                    server_response_then_reset_parts(1_500, H2ErrorCode::NO_ERROR);
                let release_reset = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(response_bytes),
                    FixtureRead::WaitDataEof(Arc::clone(&release_reset), reset_bytes),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let body_reads = Rc::new(Cell::new(0));
                let stops = Rc::new(RefCell::new(Vec::new()));
                let request: Box<dyn HttpSource> = if request_body_pending {
                    Box::new(PendingRequestSource::post(
                        1_000,
                        false,
                        Rc::clone(&body_reads),
                        Rc::clone(&stops),
                    ))
                } else {
                    Box::new(HttpFixedSource::make_get_request(b"/", None).expect("GET request"))
                };
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(request),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let consume = async move {
                    let mut headers = response
                        .read_header_event()
                        .await
                        .expect("response headers");
                    assert_eq!(headers.headers_mut().status_code().expect("status"), 200);
                    let mut received = 0;
                    if !headers.eom() {
                        loop {
                            let event = response
                                .read_body_event(u32::MAX)
                                .await
                                .expect("response body");
                            received += event.as_body().map_or(0, BufQueue::chain_length);
                            if event.eom() {
                                break;
                            }
                        }
                    }
                    assert_eq!(received, 1_500);
                    release_reset.notify_one();
                };
                let (run, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(session.run(), consume)
                })
                .await
                .expect("late-reset session timed out");
                run.expect("late NO_ERROR reset is stream-local");
                assert!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .resets
                        .is_empty()
                );
                if request_body_pending {
                    assert!(body_reads.get() > 0);
                    assert_eq!(stops.borrow().as_slice(), [Some(HttpErrorCode::CANCEL)]);
                } else {
                    assert!(stops.borrow().is_empty());
                }
            })
        });
    }

    fn assert_real_upstream_prepared_headers() {
        run_on_worker(|context| {
            Box::pin(async move {
                let header_reads = Rc::new(Cell::new(0));
                let body_reads = Rc::new(Cell::new(0));
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::DataEof(server_response_bytes(0, 0))]);
                let (session, handle) = UniplexSession::new_http2(
                    context.clone(),
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut message = roxygen_core::message::HttpMessage::new();
                message
                    .set_method(HttpMethod::Post)
                    .expect("request method");
                message.set_url(b"/available", false).expect("request URL");
                message.set_http_version(1, 1);
                message
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
                message
                    .headers_mut()
                    .set(HttpHeaderCode::ContentLength, b"1500".as_slice());
                let prepared = UniplexHandle::prepare_request_with_headers(
                    HttpHeaderEvent::new(message, false).expect("request headers"),
                    HttpSourceHolder::new(Box::new(BodyOnlyRequestSource {
                        body: Some(HttpBodyEvent::body(queue(&[b'x'; 1_500]), true)),
                        header_reads: Rc::clone(&header_reads),
                        body_reads: Rc::clone(&body_reads),
                    })),
                    handle.reserve_request().expect("request reservation"),
                );
                let mut response = handle
                    .send_prepared_request(prepared)
                    .expect("prepared request admitted")
                    .source;
                let consume = async move {
                    let headers = response
                        .read_header_event()
                        .await
                        .expect("response headers");
                    assert!(headers.eom());
                };
                let (run, ()) = tokio::join!(session.run(), consume);
                run.expect("prepared-header session completes");
                assert_eq!(header_reads.get(), 0);
                assert_eq!(body_reads.get(), 1);
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                assert_eq!(decoded.body_bytes, 1_500);

                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(
                    server_goaway_bytes(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR, None),
                )]);
                let (draining_session, draining_handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("draining upstream H2 session");
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_method(HttpMethod::Get).expect("request method");
                message.set_url(b"/rejected", false).expect("request URL");
                message.set_http_version(1, 1);
                let prepared = UniplexHandle::prepare_request_with_headers(
                    HttpHeaderEvent::new(message, true).expect("request headers"),
                    HttpSourceHolder::new(Box::new(BodyOnlyRequestSource {
                        body: None,
                        header_reads: Rc::clone(&header_reads),
                        body_reads: Rc::clone(&body_reads),
                    })),
                    draining_handle
                        .reserve_request()
                        .expect("request reservation"),
                );
                assert!(draining_handle.initiate_drain().expect("start drain"));
                let error = match draining_handle.send_prepared_request(prepared) {
                    Ok(_) => panic!("draining session accepted prepared request"),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                draining_session
                    .run()
                    .await
                    .expect("draining session completes");
                assert_eq!(header_reads.get(), 0);
            })
        });
    }

    pub(super) fn assert_committed_request_drain_order(commit_before_drain: bool) {
        let (
            exercise,
            run,
            response_read,
            wire,
            events,
            close,
            dropped,
            finished,
            source_counts,
            encoded_observations,
        ) = run_on_worker(move |context| {
            Box::pin(async move {
                let input = if commit_before_drain {
                    server_response_bytes(0, 0)
                } else {
                    server_goaway_bytes(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR, None)
                };
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("committed-header order session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("committed-header lifecycle observer");
                let probe = Rc::new(EligibilityProbe::default());
                let gate = EligibilityGate::new(
                    EligibilityPhase::Headers,
                    Arc::new(tokio::sync::Notify::new()),
                );
                let source = EligibilitySource::new(
                    EligibilityPhase::Headers,
                    Arc::clone(&gate),
                    Rc::clone(&probe),
                    Bytes::from_static(b"unread"),
                    true,
                );
                let mut response = None;
                let encoded = Rc::new(Cell::new(None));
                let mut encoded_at_commit = None;
                let exercise = AssertUnwindSafe(async {
                    let reservation = handle.reserve_request().expect("owned reservation");
                    if commit_before_drain {
                        let mut message = HttpMessage::new();
                        message.set_method(HttpMethod::Get).expect("GET request");
                        message.set_url(b"/committed", false).expect("request URL");
                        message.set_http_version(1, 1);
                        message
                            .headers_mut()
                            .set(HttpHeaderCode::Host, b"example.com");
                        let mut headers = HttpHeaderEvent::new(message, true)
                            .expect("terminal prepared request headers");
                        let encoded_callback = Rc::clone(&encoded);
                        let callback_handle = handle.clone();
                        headers.set_egress_headers_callback(move |size| {
                            encoded_callback.set(Some((
                                size.compressed,
                                callback_handle
                                    .protocol()
                                    .expect("callback outside driver borrow"),
                            )));
                        });
                        let prepared = UniplexHandle::prepare_request_with_headers(
                            headers,
                            HttpSourceHolder::new(Box::new(source)),
                            reservation,
                        );
                        response = Some(
                            handle
                                .send_prepared_request(prepared)
                                .expect("commit succeeds before drain"),
                        );
                        assert_eq!(
                            response.as_ref().expect("retained response").stream,
                            StreamId::new(1)
                        );
                        assert_eq!(probe.header_reads.get(), 0);
                        assert_eq!(probe.body_reads.get(), 0);
                        encoded_at_commit = encoded.get();
                        assert!(
                            state.borrow().writes.is_empty(),
                            "writer remains unpolled after commit"
                        );
                        assert!(handle.initiate_drain().expect("drain follows commit"));
                    } else {
                        let mut prepared = Box::pin(UniplexHandle::prepare_request(
                            HttpSourceHolder::new(Box::new(source)),
                            reservation,
                            CancellationToken::never(),
                        ));
                        poll_fn(|task_context| {
                            assert!(prepared.as_mut().poll(task_context).is_pending());
                            Poll::Ready(())
                        })
                        .await;
                        assert_eq!(probe.header_reads.get(), 1);
                        assert_eq!(gate.polls.load(Ordering::SeqCst), 1);
                        assert!(
                            handle
                                .initiate_drain()
                                .expect("drain while headers are pending")
                        );
                        assert!(!gate.released.load(Ordering::SeqCst));
                        gate.release();
                        let prepared = prepared.await.expect("owned header preparation finishes");
                        let error = match handle.send_prepared_request(prepared) {
                            Ok(unexpected) => {
                                response = Some(unexpected);
                                panic!("reservation incorrectly permitted commit after drain");
                            }
                            Err(error) => error,
                        };
                        assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                        assert_eq!(error.message(), b"session is draining");
                    }
                    assert!(
                        state.borrow().writes.is_empty(),
                        "no writer poll before commit/drain decisions"
                    );
                    assert_eq!(probe.body_reads.get(), 0);
                })
                .catch_unwind()
                .await;
                gate.release();
                let consume =
                    AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1), async {
                        if let Some(mut response) = response.take() {
                            let completion = response.completion.clone();
                            let mut headers = response
                                .source
                                .read_header_event()
                                .await
                                .expect("committed response headers");
                            assert_eq!(
                                headers
                                    .headers_mut()
                                    .status_code()
                                    .expect("response status"),
                                200
                            );
                            assert!(headers.eom());
                            drop(response);
                            completion.wait().await.expect("committed exchange cleanup");
                            completion
                                .wait()
                                .await
                                .expect("committed exchange cleanup reobservation");
                        }
                    }))
                    .catch_unwind();
                let (run, response_read) = tokio::join!(
                    tokio::time::timeout(Duration::from_secs(1), session.run()),
                    consume,
                );
                drop(response);
                let wire = state.borrow().writes.concat();
                let events = events.borrow().clone();
                let close = state.borrow().close;
                let finished = handle.is_finished();
                drop(handle);
                drop(transport);
                let dropped = state.borrow().transport_dropped;
                let source_counts = (
                    probe.header_reads.get(),
                    probe.body_reads.get(),
                    probe.source_drops.get(),
                    probe.cancelled_reads.get(),
                );
                (
                    exercise,
                    run,
                    response_read,
                    wire,
                    events,
                    close,
                    dropped,
                    finished,
                    source_counts,
                    (encoded_at_commit, encoded.get()),
                )
            })
        });
        exercise.unwrap_or_else(|payload| resume_unwind(payload));
        let summary = run
            .expect("committed-header session joined within bound")
            .expect("committed-header session completes successfully");
        response_read
            .unwrap_or_else(|payload| resume_unwind(payload))
            .expect("committed response observation completes within bound");
        assert!(!summary.reset);
        assert_eq!(summary.bytes_written, wire.len() as u64);
        assert!(finished && dropped);
        assert_eq!(close, FixtureClose::ShutdownWrite);
        assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
        assert_eq!(source_counts, (usize::from(!commit_before_drain), 0, 1, 0));
        let decoded = decode_client_frames(&wire);
        assert!(
            decoded.errors.is_empty(),
            "codec errors: {:?}",
            decoded.errors
        );
        assert!(decoded.resets.is_empty());
        assert_eq!(decoded.body_bytes, 0);
        assert_eq!(
            decoded.goaways,
            [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
        );
        let expected_order = if commit_before_drain {
            assert_eq!(decoded.header_streams, [StreamId::new(1)]);
            vec![
                (StreamId::new(1), u64::from(FrameType::HEADERS.as_u8())),
                (StreamId::UNSPECIFIED, u64::from(FrameType::GOAWAY.as_u8())),
            ]
        } else {
            assert!(decoded.header_streams.is_empty());
            vec![(StreamId::UNSPECIFIED, u64::from(FrameType::GOAWAY.as_u8()))]
        };
        assert_eq!(decoded.header_goaway_order, expected_order);
        if commit_before_drain {
            let (compressed, protocol) = encoded_observations
                .0
                .expect("header serialization callback ran before commit returned");
            assert!(compressed > 0);
            assert_eq!(protocol, CodecProtocol::Http2);
            assert_eq!(encoded_observations.1, encoded_observations.0);
        } else {
            assert_eq!(encoded_observations, (None, None));
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum HeaderReentryPhase {
        TransactionOpened,
        RequestEnd,
    }

    struct HeaderReentryProbe {
        phase: HeaderReentryPhase,
        action: RefCell<Option<Box<dyn FnOnce()>>>,
        opened: Cell<usize>,
        closed: Cell<usize>,
        events: RefCell<Vec<LifecycleEvent>>,
    }

    impl HeaderReentryProbe {
        fn reenter(&self) {
            let action = self.action.borrow_mut().take();
            if let Some(action) = action {
                action();
            }
        }
    }

    impl Http2SessionStats for HeaderReentryProbe {
        fn record_transaction_opened(&self) {
            self.opened.set(self.opened.get() + 1);
            if self.phase == HeaderReentryPhase::TransactionOpened {
                self.reenter();
            }
        }

        fn record_transaction_closed(&self) {
            self.closed.set(self.closed.get() + 1);
        }
    }

    impl LifecycleObserver for HeaderReentryProbe {
        fn on_event(&self, event: LifecycleEvent) {
            let reenter = self.phase == HeaderReentryPhase::RequestEnd
                && matches!(event, LifecycleEvent::RequestEnd { stream } if stream == StreamId::new(1));
            self.events.borrow_mut().push(event);
            if reenter {
                self.reenter();
            }
        }
    }

    fn committed_test_headers(path: &[u8], eom: bool) -> HttpHeaderEvent {
        let mut message = HttpMessage::new();
        message.set_method(HttpMethod::Post).expect("POST request");
        message.set_url(path, false).expect("request URL");
        message.set_http_version(1, 1);
        message
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        HttpHeaderEvent::new(message, eom).expect("prepared request headers")
    }

    fn committed_test_request(
        handle: &UniplexHandle,
        headers: HttpHeaderEvent,
        probe: &Rc<EligibilityProbe>,
    ) -> PreparedOutgoingRequest {
        let source = EligibilitySource::new(
            EligibilityPhase::Body,
            EligibilityGate::new(EligibilityPhase::Body, Arc::new(tokio::sync::Notify::new())),
            Rc::clone(probe),
            Bytes::from_static(b"retained request body"),
            true,
        );
        UniplexHandle::prepare_request_with_headers(
            headers,
            HttpSourceHolder::new(Box::new(source)),
            handle.reserve_request().expect("same-session reservation"),
        )
    }

    fn reentrant_response_bytes(nested: bool) -> Vec<u8> {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut output = BufQueue::new(4096);
        let mut callback = IgnoreCodecEvents;
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("peer settings");
        let mut response = HttpMessage::new();
        response.set_status_code(200).expect("peer status");
        response.set_http_version(1, 1);
        for stream in [StreamId::new(1), StreamId::new(3)]
            .into_iter()
            .take(if nested { 2 } else { 1 })
        {
            codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream,
                        message: &response,
                        end_stream: true,
                        extra_headers: None,
                    },
                    &mut callback,
                )
                .expect("peer terminal response");
        }
        output
            .coalesce(output.chain_length())
            .expect("bounded peer responses")
            .to_vec()
    }

    fn assert_committed_header_reentry(phase: HeaderReentryPhase, nested: bool) {
        let (
            exercise,
            run,
            consume,
            wire,
            delivered,
            cancelled,
            lifecycle,
            counts,
            sources,
            close,
            finished,
            dropped,
        ) = run_on_worker(move |context| {
            Box::pin(async move {
                let probe = Rc::new(HeaderReentryProbe {
                    phase,
                    action: RefCell::new(None),
                    opened: Cell::new(0),
                    closed: Cell::new(0),
                    events: RefCell::new(Vec::new()),
                });
                let (transport, state) = FixtureTransport::with_capability(
                    [FixtureRead::DataEof(reentrant_response_bytes(nested))],
                    ByteEventCapability::TransportEvents,
                    true,
                );
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        stats: Some(probe.clone()),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("reentrant committed-header session");
                session
                    .add_lifecycle_observer(probe.clone())
                    .expect("reentry observer");
                let byte_events = Rc::new(ByteEventProbe::default());
                let source_probes = [
                    Rc::new(EligibilityProbe::default()),
                    Rc::new(EligibilityProbe::default()),
                ];
                let responses = Rc::new(RefCell::new(Vec::<UniplexResponse>::new()));
                let exercise = catch_unwind(AssertUnwindSafe(|| {
                    let mut first = committed_test_headers(b"/first", true);
                    first
                        .byte_event_registrations_mut()
                        .push(HttpByteEventRegistration::new(
                            None,
                            ByteEventType::ALL
                                .into_iter()
                                .fold(0, |bits, kind| bits | kind.bit()),
                            &byte_events,
                        ));
                    let first = committed_test_request(&handle, first, &source_probes[0]);
                    let callback_handle = handle.clone();
                    if nested {
                        let mut second = committed_test_headers(b"/second", true);
                        second
                            .byte_event_registrations_mut()
                            .push(HttpByteEventRegistration::new(
                                None,
                                ByteEventType::ALL
                                    .into_iter()
                                    .fold(0, |bits, kind| bits | kind.bit()),
                                &byte_events,
                            ));
                        let second = committed_test_request(&handle, second, &source_probes[1]);
                        let nested_responses = Rc::clone(&responses);
                        *probe.action.borrow_mut() = Some(Box::new(move || {
                            let response = callback_handle
                                .send_prepared_request(second)
                                .expect("reentrant stream commit");
                            assert_eq!(response.stream, StreamId::new(3));
                            nested_responses.borrow_mut().push(response);
                        }));
                    } else {
                        *probe.action.borrow_mut() = Some(Box::new(move || {
                            assert!(
                                callback_handle
                                    .initiate_drain()
                                    .expect("reentrant local drain")
                            );
                        }));
                    }
                    let response = handle
                        .send_prepared_request(first)
                        .expect("outer stream commit");
                    assert_eq!(response.stream, StreamId::new(1));
                    responses.borrow_mut().push(response);
                    assert!(
                        probe.action.borrow().is_none(),
                        "the selected callback really reentered"
                    );
                    assert!(
                        state.borrow().writes.is_empty(),
                        "writer unpolled through callback reentry"
                    );
                    if nested {
                        assert!(handle.initiate_drain().expect("drain after both commits"));
                    }
                }));
                let unused = probe.action.borrow_mut().take();
                drop(unused);
                let retained = std::mem::take(&mut *responses.borrow_mut());
                drop(responses);
                let consume =
                    AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1), async move {
                        let mut completions = Vec::with_capacity(retained.len());
                        for mut response in retained {
                            let completion = response.completion.clone();
                            let mut headers = response
                                .source
                                .read_header_event()
                                .await
                                .expect("actual response headers");
                            assert_eq!(
                                headers
                                    .headers_mut()
                                    .status_code()
                                    .expect("response status"),
                                200
                            );
                            assert!(headers.eom());
                            drop(response);
                            completions.push(completion);
                        }
                        for completion in completions {
                            completion.wait().await.expect("exchange cleanup");
                            completion
                                .wait()
                                .await
                                .expect("exchange cleanup reobservation");
                        }
                    }))
                    .catch_unwind();
                let (run, consume) = tokio::join!(
                    AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1), session.run()))
                        .catch_unwind(),
                    consume,
                );
                let wire = state.borrow().writes.concat();
                let close = state.borrow().close;
                let finished = handle.is_finished();
                drop(handle);
                drop(transport);
                let dropped = state.borrow().transport_dropped;
                let sources = source_probes.map(|source| {
                    (
                        source.header_reads.get(),
                        source.body_reads.get(),
                        source.source_drops.get(),
                    )
                });
                let delivered = byte_events.delivered.borrow().clone();
                let cancelled = byte_events.cancelled.borrow().clone();
                let lifecycle = probe.events.borrow().clone();
                (
                    exercise,
                    run,
                    consume,
                    wire,
                    delivered,
                    cancelled,
                    lifecycle,
                    (probe.opened.get(), probe.closed.get()),
                    sources,
                    close,
                    finished,
                    dropped,
                )
            })
        });
        exercise.unwrap_or_else(|payload| resume_unwind(payload));
        let summary = run
            .unwrap_or_else(|payload| resume_unwind(payload))
            .expect("bounded session join")
            .expect("healthy reentrant session");
        consume
            .unwrap_or_else(|payload| resume_unwind(payload))
            .expect("bounded exchange joins");
        assert!(!summary.reset);
        assert_eq!(summary.bytes_written, wire.len() as u64);
        assert!(finished && dropped);
        assert_eq!(close, FixtureClose::ShutdownWrite);
        assert_eq!(lifecycle.last(), Some(&LifecycleEvent::Destroy));
        let transactions = if nested { 2 } else { 1 };
        assert_eq!(counts, (transactions, transactions));
        assert_eq!(sources, [(0, 0, 1), (0, 0, usize::from(nested))]);
        let decoded = decode_client_frames(&wire);
        assert!(
            decoded.errors.is_empty(),
            "actual decoder errors: {:?}",
            decoded.errors
        );
        assert!(decoded.resets.is_empty());
        assert_eq!(decoded.body_bytes, 0);
        let streams = if nested {
            vec![StreamId::new(1), StreamId::new(3)]
        } else {
            vec![StreamId::new(1)]
        };
        assert_eq!(decoded.header_streams, streams);
        let mut expected_order = streams
            .iter()
            .map(|stream| (*stream, u64::from(FrameType::HEADERS.as_u8())))
            .collect::<Vec<_>>();
        expected_order.push((StreamId::UNSPECIFIED, u64::from(FrameType::GOAWAY.as_u8())));
        assert_eq!(decoded.header_goaway_order, expected_order);
        assert_eq!(
            decoded.goaways,
            [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
        );
        assert!(cancelled.is_empty());
        for event_type in ByteEventType::ALL {
            let actual = delivered
                .iter()
                .filter(|event| event.event_type == event_type)
                .map(|event| {
                    (
                        event.stream_id.expect("registered stream"),
                        event.transport_offset,
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual, decoded.header_end_offsets,
                "exact wire-ordered {event_type:?} boundaries"
            );
        }
        assert_eq!(delivered.len(), transactions * ByteEventType::ALL.len());
        for event in delivered {
            assert!(event.eom);
            assert_eq!(event.body_offset, 0);
            let stream = event.stream_id.expect("registered stream");
            let frame_length = decoded
                .header_frame_lengths
                .iter()
                .find(|(candidate, _)| *candidate == stream)
                .expect("actual encoded header frame")
                .1;
            assert_eq!(event.stream_offset, frame_length);
            let section = event.field_section_info.expect("header section metadata");
            assert_eq!(section.section_type, FieldSectionType::Headers);
            assert!(section.final_headers && section.size.compressed > 0);
        }
    }

    pub(super) fn assert_committed_header_stats_reentry() {
        assert_committed_header_reentry(HeaderReentryPhase::TransactionOpened, true);
        assert_committed_header_reentry(HeaderReentryPhase::TransactionOpened, false);
    }

    pub(super) fn assert_committed_header_lifecycle_reentry() {
        assert_committed_header_reentry(HeaderReentryPhase::RequestEnd, true);
    }

    struct CommittedHeaderPanic {
        site: &'static str,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for CommittedHeaderPanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct HeaderCancellationProbe {
        errors: RefCell<Vec<(ByteEventType, HttpError)>>,
        panic_once: Cell<bool>,
        panic_drops: Arc<AtomicUsize>,
        delivered: Cell<usize>,
    }

    impl HttpByteEventCallback for HeaderCancellationProbe {
        fn on_byte_event(&self, _event: HttpByteEvent) {
            self.delivered.set(self.delivered.get() + 1);
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.errors.borrow_mut().push((event.event_type, error));
            if self.panic_once.replace(false) {
                panic_any(CommittedHeaderPanic {
                    site: "cancel",
                    drops: Arc::clone(&self.panic_drops),
                });
            }
        }
    }

    pub(super) fn assert_committed_header_panic_retirement(encoding_error: bool) {
        let panic_drops = Arc::new(AtomicUsize::new(0));
        let observed_drops = Arc::clone(&panic_drops);
        let (
            exercise,
            payload,
            run,
            response_read,
            cancellation_errors,
            delivered,
            encoded,
            counts,
            sources,
            lifecycle,
            close,
            finished,
            dropped,
        ) = run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let stats = Rc::new(StatsProbe::default());
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig {
                        buffer_limit: 256,
                        ingress_buffer_limit: 256,
                        maximum_read_size: 256,
                        read_allocation_size: 256,
                        maximum_write_size: 256,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        stats: Some(stats.clone()),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("bounded encoding-error session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("failure lifecycle observer");
                let cancellation = Rc::new(HeaderCancellationProbe {
                    errors: RefCell::new(Vec::new()),
                    panic_once: Cell::new(encoding_error),
                    panic_drops: Arc::clone(&panic_drops),
                    delivered: Cell::new(0),
                });
                let probes = [
                    Rc::new(EligibilityProbe::default()),
                    Rc::new(EligibilityProbe::default()),
                ];
                let mut response = None;
                let mut payload = None;
                let encoded = Rc::new(Cell::new(0));
                let exercise = catch_unwind(AssertUnwindSafe(|| {
                    response = Some(
                        handle
                            .send_prepared_request(committed_test_request(
                                &handle,
                                committed_test_headers(b"/retained", false),
                                &probes[0],
                            ))
                            .expect("independently retained admitted request"),
                    );
                    let path = if encoding_error {
                        let mut path = vec![b'Z'; 4096];
                        path[0] = b'/';
                        path
                    } else {
                        b"/panic".to_vec()
                    };
                    let mut headers = committed_test_headers(&path, false);
                    for event_type in [ByteEventType::TransportWrite, ByteEventType::KernelWrite] {
                        headers.byte_event_registrations_mut().push(
                            HttpByteEventRegistration::new(None, event_type.bit(), &cancellation),
                        );
                    }
                    let encoded_callback = Rc::clone(&encoded);
                    let callback_drops = Arc::clone(&panic_drops);
                    headers.set_egress_headers_callback(move |_| {
                        encoded_callback.set(encoded_callback.get() + 1);
                        panic_any(CommittedHeaderPanic {
                            site: "headers",
                            drops: callback_drops,
                        });
                    });
                    let prepared = committed_test_request(&handle, headers, &probes[1]);
                    payload = Some(
                        match catch_unwind(AssertUnwindSafe(|| {
                            handle.send_prepared_request(prepared)
                        })) {
                            Err(payload) => payload,
                            Ok(_) => panic!(
                                "the original callback panic did not reach the commit caller"
                            ),
                        },
                    );
                    let rejected = match handle.reserve_request() {
                        Err(error) => error,
                        Ok(_) => {
                            panic!("caught callback panic left retained-session admission open")
                        }
                    };
                    assert_eq!(rejected.code(), HttpErrorCode::DROPPED);
                    assert_eq!(rejected.message(), b"uniplex session has finished");
                    assert!(
                        !handle.is_finished(),
                        "the original session still needs its actual Run join"
                    );
                    assert!(
                        state.borrow().writes.is_empty(),
                        "no writer was polled to trigger the callback"
                    );
                    assert_eq!(
                        panic_drops.load(Ordering::SeqCst),
                        0,
                        "caller alone retains the first payload through cleanup"
                    );
                }));
                let consume =
                    AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1), async {
                        let mut response = response.take().expect("retained first response");
                        let completion = response.completion.clone();
                        let error = match response.source.read_header_event().await {
                            Err(error) => error,
                            Ok(_) => panic!("admitted response lost the native failure"),
                        };
                        drop(response);
                        completion
                            .wait()
                            .await
                            .expect("failed exchange cleanup confirmed");
                        completion
                            .wait()
                            .await
                            .expect("failed exchange cleanup reobserved");
                        error
                    }))
                    .catch_unwind();
                let (run, response_read) = tokio::join!(
                    AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1), session.run()))
                        .catch_unwind(),
                    consume,
                );
                drop(response);
                let close = state.borrow().close;
                let finished = handle.is_finished();
                drop(handle);
                drop(transport);
                let dropped = state.borrow().transport_dropped;
                let cancellation_errors = cancellation.errors.borrow().clone();
                let sources = probes.map(|probe| {
                    (
                        probe.header_reads.get(),
                        probe.body_reads.get(),
                        probe.source_drops.get(),
                    )
                });
                let lifecycle = events.borrow().clone();
                (
                    exercise,
                    payload,
                    run,
                    response_read,
                    cancellation_errors,
                    cancellation.delivered.get(),
                    encoded.get(),
                    (stats.opened.get(), stats.closed.get()),
                    sources,
                    lifecycle,
                    close,
                    finished,
                    dropped,
                )
            })
        });
        exercise.unwrap_or_else(|payload| resume_unwind(payload));
        let error = run
            .unwrap_or_else(|payload| resume_unwind(payload))
            .expect("bounded failed Run join")
            .expect_err("callback failure is sticky in actual native Run");
        let response_error = response_read
            .unwrap_or_else(|payload| resume_unwind(payload))
            .expect("bounded retained response cleanup");
        let payload = payload
            .expect("caller owns original panic")
            .downcast::<CommittedHeaderPanic>()
            .unwrap_or_else(|payload| resume_unwind(payload));
        assert_eq!(
            payload.site,
            if encoding_error { "cancel" } else { "headers" }
        );
        assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
        assert_eq!(response_error.code(), error.code());
        assert_eq!(
            response_error.message(),
            b"HTTP source aborted err=INTERNAL_ERROR, details=connection error"
        );
        assert_eq!(cancellation_errors.len(), 2);
        assert_eq!(
            cancellation_errors
                .iter()
                .map(|(kind, _)| *kind)
                .collect::<Vec<_>>(),
            [ByteEventType::TransportWrite, ByteEventType::KernelWrite]
        );
        for (_, observed) in cancellation_errors {
            assert_eq!(observed.code(), error.code());
            assert_eq!(
                observed.message(),
                error.message(),
                "actual encoding error survives cancellation callback panic"
            );
        }
        if encoding_error {
            assert_ne!(error.message(), b"uniplex transport loop panicked");
            assert_eq!(encoded, 0);
        } else {
            assert_eq!(error.message(), b"uniplex transport loop panicked");
            assert_eq!(encoded, 1);
        }
        assert_eq!(delivered, 0);
        assert_eq!(counts, (2, 2));
        assert_eq!(sources, [(0, 0, 1), (0, 0, 1)]);
        assert_eq!(lifecycle.last(), Some(&LifecycleEvent::Destroy));
        assert_eq!(close, FixtureClose::Reset);
        assert!(finished && dropped);
        assert_eq!(observed_drops.load(Ordering::SeqCst), 0);
        drop(payload);
        assert_eq!(
            observed_drops.load(Ordering::SeqCst),
            1,
            "one destruction of the original caller-owned payload"
        );
    }

    fn assert_real_upstream_response_reset() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(
                    server_reset_bytes(H2ErrorCode::CANCEL),
                )]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let _registration = session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let consume = async move {
                    match response.read_header_event().await {
                        Ok(_) => panic!("reset stream produced response headers"),
                        Err(error) => error,
                    }
                };
                let (run, error) = tokio::join!(session.run(), consume);
                run.expect("peer reset remains stream-local");
                assert_eq!(error.code(), HttpErrorCode::CANCEL);
                assert!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .resets
                        .is_empty()
                );
                assert!(events.borrow().iter().any(|event| {
                    *event
                        == LifecycleEvent::PeerReset {
                            stream: StreamId::new(1),
                            code: HttpErrorCode::CANCEL,
                        }
                }));
            })
        });
    }

    fn assert_real_upstream_padding() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = server_response_bytes(0, 0);
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut message = roxygen_core::message::HttpMessage::new();
                message
                    .set_method(HttpMethod::Post)
                    .expect("request method");
                message.set_url(b"/padding", false).expect("request URL");
                message.set_http_version(1, 1);
                message
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
                let source = EventSequenceSource {
                    header: Some(HttpHeaderEvent::new(message, false).expect("request headers")),
                    body: [
                        HttpBodyEvent::padding(20, false),
                        HttpBodyEvent::body(queue(b"hello"), false),
                        HttpBodyEvent::padding(30, false),
                        HttpBodyEvent::body(queue(b"world"), false),
                        HttpBodyEvent::padding(40, false),
                        HttpBodyEvent::body(BufQueue::new(0), true),
                    ]
                    .into_iter()
                    .collect(),
                };
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(source)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let consume = async move {
                    let headers = response
                        .read_header_event()
                        .await
                        .expect("response headers");
                    assert!(headers.eom());
                };
                let (run, ()) = tokio::join!(session.run(), consume);
                run.expect("padding session completes");
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.padding, 90);
                assert_eq!(decoded.body_bytes, 10);
                assert!(decoded.resets.is_empty());
            })
        });
    }

    fn assert_real_upstream_push(large_promise: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = server_push_bytes(large_promise, None);
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .unwrap();
                let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let consume = async move {
                    let mut headers =
                        tokio::time::timeout(Duration::from_secs(1), response.read_header_event())
                            .await
                            .expect("parent headers timed out")
                            .expect("parent headers");
                    assert_eq!(headers.headers_mut().status_code().expect("status"), 200);
                    assert_eq!(handle.outgoing_transaction_activity().available(), 0);
                    assert!(
                        handle.reserve_request().is_err(),
                        "EOF closes outgoing admission while final pushed ingress stays valid"
                    );
                    assert!(!headers.eom());
                    let mut push_count = 0;
                    loop {
                        let event = tokio::time::timeout(
                            Duration::from_secs(1),
                            response.read_body_event(u32::MAX),
                        )
                        .await
                        .expect("parent body timed out")
                        .expect("parent body");
                        let eom = event.eom();
                        if let HttpBodyEventKind::PushPromise(mut push) = event.into_kind() {
                            push_count += 1;
                            assert_eq!(push.promise_mut().path().expect("push path"), b"/push");
                            let mut pushed =
                                push.move_push_source().expect("pushed response source");
                            let mut pushed_headers = tokio::time::timeout(
                                Duration::from_secs(1),
                                pushed.read_header_event(),
                            )
                            .await
                            .expect("pushed headers timed out")
                            .expect("pushed headers");
                            assert_eq!(
                                pushed_headers.headers_mut().status_code().expect("status"),
                                200
                            );
                            assert!(pushed_headers.eom());
                        }
                        if eom {
                            break;
                        }
                    }
                    push_count
                };
                let (run, push_count) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(session.run(), consume)
                })
                .await
                .expect("upstream push session timed out");
                run.expect("upstream push session completes");
                assert_eq!(push_count, 1);
                assert!(
                    events
                        .borrow()
                        .contains(&LifecycleEvent::OutgoingAdmissionClosed)
                );
            })
        });
    }

    fn assert_real_upstream_abandoned_push_resets_child() {
        run_on_worker(|context| {
            Box::pin(async move {
                let finish = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(server_incomplete_push_bytes()),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("parent request admitted")
                    .source;
                let consume_state = Rc::clone(&state);
                let consume_finish = Arc::clone(&finish);
                let consume = async move {
                    let headers = response
                        .read_header_event()
                        .await
                        .expect("parent response headers");
                    assert!(!headers.eom());
                    let event = response
                        .read_body_event(u32::MAX)
                        .await
                        .expect("parent push event");
                    let parent_eom = event.eom();
                    let HttpBodyEventKind::PushPromise(mut push) = event.into_kind() else {
                        panic!("parent response did not deliver its push promise")
                    };
                    assert_eq!(push.promise_mut().path().expect("push path"), b"/push");
                    drop(push);
                    if !parent_eom {
                        assert!(
                            response
                                .read_body_event(u32::MAX)
                                .await
                                .expect("parent EOM")
                                .eom()
                        );
                    }
                    loop {
                        let decoded = decode_client_frames(&consume_state.borrow().writes.concat());
                        if decoded
                            .resets
                            .contains(&(StreamId::new(2), H2ErrorCode::CANCEL))
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    consume_finish.notify_one();
                };
                let (run, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(session.run(), consume)
                })
                .await
                .expect("abandoned-push cleanup timed out");
                run.expect("abandoned push remains stream-local");
                assert_eq!(
                    decode_client_frames(&state.borrow().writes.concat()).resets,
                    [(StreamId::new(2), H2ErrorCode::CANCEL)]
                );
            })
        });
    }

    fn assert_real_upstream_pushed_stream_reset() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = server_push_bytes(false, Some(H2ErrorCode::CANCEL));
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let consume = async move {
                    let headers = response.read_header_event().await.expect("parent headers");
                    assert!(!headers.eom());
                    let event = response
                        .read_body_event(u32::MAX)
                        .await
                        .expect("push event");
                    let parent_eom = event.eom();
                    let HttpBodyEventKind::PushPromise(mut push) = event.into_kind() else {
                        panic!("expected push promise")
                    };
                    let mut pushed = push.move_push_source().expect("pushed response source");
                    let error = match pushed.read_header_event().await {
                        Ok(_) => panic!("peer reset unexpectedly produced pushed headers"),
                        Err(error) => error,
                    };
                    assert_eq!(error.code(), HttpErrorCode::CANCEL);
                    if !parent_eom {
                        let eom = response
                            .read_body_event(u32::MAX)
                            .await
                            .expect("parent EOM");
                        assert!(eom.eom());
                    }
                };
                let (run, ()) = tokio::join!(session.run(), consume);
                run.expect("pushed reset is stream-local");
            })
        });
    }

    fn assert_real_push_parent_reset_before_promise_completion() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (initial, continuation_tail) = server_partial_push_promise_parts();
                let release_tail = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitDataEof(Arc::clone(&release_tail), continuation_tail),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut run = Box::pin(session.run());
                let mut header_read = Box::pin(response.read_header_event());
                let mut headers = tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::select! {
                        headers = header_read.as_mut() => headers.expect("parent response headers"),
                        result = run.as_mut() => {
                            panic!("session ended before parent headers: {result:?}")
                        }
                    }
                })
                .await
                .expect("parent headers timed out");
                drop(header_read);
                assert_eq!(headers.headers_mut().status_code().expect("status"), 200);
                assert!(!headers.eom());
                response.stop_reading(None);
                release_tail.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("partial push cleanup timed out")
                    .expect("parent reset discards incomplete push promise");
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(
                    decoded.resets,
                    [
                        (StreamId::new(1), H2ErrorCode::CANCEL),
                        (StreamId::new(2), H2ErrorCode::CANCEL),
                    ]
                );
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_push_promise_parse_error() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = server_invalid_push_promise_bytes();
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_millis(5),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut run = Box::pin(session.run());
                let error = tokio::time::timeout(Duration::from_secs(1), async {
                    let mut read = Box::pin(response.read_header_event());
                    tokio::select! {
                        result = read.as_mut() => match result {
                            Ok(_) => panic!("invalid push promise preserved the parent response"),
                            Err(error) => error,
                        },
                        result = run.as_mut() => {
                            panic!("session ended before push parse error: {result:?}")
                        }
                    }
                })
                .await
                .expect("push parse error was delivered");
                assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
                let mut partial = error
                    .http_message()
                    .cloned()
                    .expect("parent response is attached to the error");
                assert_eq!(partial.status_code().expect("partial status"), 200);
                assert!(handle.initiate_drain().expect("drain accepted"));
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("push parse drain timed out")
                    .expect("push parse failure remains stream-local");
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(
                    decoded.resets,
                    [
                        (StreamId::new(1), H2ErrorCode::PROTOCOL_ERROR),
                        (StreamId::new(2), H2ErrorCode::PROTOCOL_ERROR),
                    ]
                );
            })
        });
    }

    fn run_downstream_request(
        method: HttpMethod,
        request_body: Vec<u8>,
        padding: Option<u8>,
        status: u16,
        response_body: usize,
    ) -> DownstreamOutcome {
        let input = peer_request_bytes(method, &request_body, padding, PeerControl::None);
        run_downstream_bytes(input, status, response_body)
    }

    fn run_downstream_bytes(
        input: Vec<u8>,
        status: u16,
        response_body: usize,
    ) -> DownstreamOutcome {
        run_downstream_bytes_with_config(
            input,
            status,
            response_body,
            UniplexConfig::default().write_timeout,
            65_535,
            65_535,
        )
    }

    fn run_downstream_bytes_with_config(
        input: Vec<u8>,
        status: u16,
        response_body: usize,
        write_timeout: Duration,
        stream_receive_window: u32,
        connection_receive_window: u32,
    ) -> DownstreamOutcome {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, transport_state) =
                    FixtureTransport::new([FixtureRead::DataEof(input)]);
                let invocations = Rc::new(Cell::new(0));
                let consumed = Rc::new(Cell::new(0));
                let handler = Rc::new(FixtureHandler {
                    status,
                    response_body,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::clone(&consumed),
                });
                let session_config = UniplexConfig {
                    write_timeout,
                    ..UniplexConfig::default()
                };
                let http2_config = Http2SessionConfig {
                    stream_receive_window,
                    connection_receive_window,
                    ..Http2SessionConfig::default()
                };
                let (session, _) = UniplexSession::new_http2(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    session_config,
                    http2_config,
                )
                .expect("downstream H2 session");
                let result = session.run().await;
                let state = transport_state.borrow();
                DownstreamOutcome {
                    invocations: invocations.get(),
                    request_body: consumed.get(),
                    wire: state.writes.concat(),
                    close: state.close,
                    error: result.err().map(|error| error.code()),
                }
            })
        })
    }

    fn run_downstream_handler_error(code: HttpErrorCode) -> DownstreamOutcome {
        let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, transport_state) =
                    FixtureTransport::new([FixtureRead::DataEof(input)]);
                let invocations = Rc::new(Cell::new(0));
                let consumed = Rc::new(Cell::new(0));
                let handler = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: Some(code),
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::clone(&consumed),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_millis(5),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let result = session.run().await;
                let state = transport_state.borrow();
                DownstreamOutcome {
                    invocations: invocations.get(),
                    request_body: consumed.get(),
                    wire: state.writes.concat(),
                    close: state.close,
                    error: result.err().map(|error| error.code()),
                }
            })
        })
    }

    struct ErrorSessionOutcome {
        invocations: usize,
        request_error: Option<HttpErrorCode>,
        partial_message: bool,
        request_body: usize,
        peer_identity: Option<Vec<u8>>,
        websocket: bool,
        wire: Vec<u8>,
        close: FixtureClose,
        session_error: Option<HttpErrorCode>,
    }

    fn run_downstream_error_case(
        reads: Vec<FixtureRead>,
        case: ErrorHandlerCase,
        config: UniplexConfig,
        peer_identity: Option<PeerIdentity>,
    ) -> ErrorSessionOutcome {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new(reads);
                state.borrow_mut().peer_identity = peer_identity;
                let probe = Rc::new(ErrorHandlerProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(ErrorScenarioHandler {
                    case,
                    probe: Rc::clone(&probe),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    config,
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("error scenario session timed out");
                ErrorSessionOutcome {
                    invocations: probe.invocations.get(),
                    request_error: *probe.request_error.borrow(),
                    partial_message: probe.partial_message.get(),
                    request_body: probe.request_body.get(),
                    peer_identity: probe.peer_identity.borrow().clone(),
                    websocket: probe.websocket.get(),
                    wire: state.borrow().writes.concat(),
                    close: state.borrow().close,
                    session_error: result.err().map(|error| error.code()),
                }
            })
        })
    }

    fn assert_real_simple_downstream(body_size: usize, padding: Option<u8>) {
        let body = vec![b'x'; body_size];
        let outcome = run_downstream_request(
            if body.is_empty() {
                HttpMethod::Get
            } else {
                HttpMethod::Post
            },
            body,
            padding,
            200,
            3,
        );
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.invocations, 1);
        assert_eq!(outcome.request_body, body_size);
        assert!(outcome.wire.len() >= 18);
        assert_eq!(outcome.wire[3], 4);
        assert_eq!(outcome.close, FixtureClose::ShutdownWrite);
    }

    fn assert_real_exact_padded_downstream_request() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([
                        FixtureRead::DataEof(peer_exact_padded_request_bytes()),
                    ]);
                let body_events = Rc::new(RefCell::new(Vec::new()));
                let handler: Rc<dyn HttpHandler> = Rc::new(CoalescedRequestHandler {
                    body_events: Rc::clone(&body_events),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("padded request session timed out")
                    .expect("padded request session");
                assert_eq!(&*body_events.borrow(), &[(10, true)]);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                assert!(decoded.resets.is_empty());
                assert!(decoded.window_updates.is_empty());
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_read_failure_resets_session() {
        let outcome = run_on_worker(|context| {
            Box::pin(async move {
                let (transport, transport_state) = FixtureTransport::new([FixtureRead::Error(
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                )]);
                let invocations = Rc::new(Cell::new(0));
                let consumed = Rc::new(Cell::new(0));
                let handler = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: consumed,
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let error = session.run().await.expect_err("read error reaches session");
                (
                    error.code(),
                    invocations.get(),
                    transport_state.borrow().close,
                )
            })
        });
        assert_eq!(outcome.0, HttpErrorCode::TRANSPORT_READ_ERROR);
        assert_eq!(outcome.1, 0);
        assert_eq!(outcome.2, FixtureClose::Reset);
    }

    fn assert_real_upstream_request_body_error() {
        run_on_worker(|context| {
            Box::pin(async move {
                let release_error = Arc::new(tokio::sync::Notify::new());
                let finish_input = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(100)),
                    FixtureRead::WaitDataEof(Arc::clone(&finish_input), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let _release_on_drop =
                    HeldBodyGates([Arc::clone(&release_error), Arc::clone(&finish_input)]);
                let mut source = HeaderThenErrorSource::request(HttpErrorCode::INTERNAL_ERROR);
                source.error_gate = Some(Arc::clone(&release_error));
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(source)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(2),
                        async {
                            complete_credit_wait(|| {
                                decode_client_frames(&state.borrow().writes.concat()).body_bytes
                                    == 11
                            })
                            .await;
                            release_error.notify_one();
                            let error = match response.read_header_event().await {
                                Ok(_) => panic!("request body failure produced response headers"),
                                Err(error) => error,
                            };
                            complete_credit_wait(|| {
                                !decode_client_frames(&state.borrow().writes.concat())
                                    .resets
                                    .is_empty()
                            })
                            .await;
                            error
                        },
                    ))
                    .await;
                    release_error.notify_one();
                    finish_input.notify_one();
                    drop(response);
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, outcome) = tokio::join!(session.run(), exercise);
                if let Err(payload) = outcome {
                    resume_unwind(payload);
                }
                run.expect("request source error stays stream-local");
                let error = outcome
                    .unwrap()
                    .expect("request body-error prefix and reset");
                assert_ne!(error.code(), HttpErrorCode::READ_TIMEOUT);
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.body_bytes, 11);
                assert_eq!(
                    decoded.resets,
                    [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
                );
                assert!(handle.is_finished());
                assert!(state.borrow().transport_dropped);
            })
        });
    }

    fn assert_real_upstream_write_error_after_complete_response() {
        run_on_worker(|context| {
            Box::pin(async move {
                let response_wire = server_response_bytes(100, 0);
                let settings_wire = server_settings_bytes(17);
                let send_response = Arc::new(tokio::sync::Notify::new());
                let send_settings = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::WaitData(Arc::clone(&send_response), response_wire),
                    FixtureRead::WaitDataEof(Arc::clone(&send_settings), settings_wire),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                assert!(handle.initiate_drain().expect("drain accepted"));
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if !decode_client_frames(&state.borrow().writes.concat())
                            .header_streams
                            .is_empty()
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before request write: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("request was written");
                send_response.notify_one();
                let received = tokio::time::timeout(Duration::from_secs(1), async {
                    let headers = response
                        .read_header_event()
                        .await
                        .expect("response headers");
                    assert!(!headers.eom());
                    let body = response
                        .read_body_event(u32::MAX)
                        .await
                        .expect("response body");
                    assert!(body.eom());
                    body.as_body().map_or(0, BufQueue::chain_length)
                })
                .await
                .expect("completed response remained readable");
                assert_eq!(received, 100);
                state.borrow_mut().write_error = Some(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                send_settings.notify_one();
                let error = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("settings-ack failure session hung")
                    .expect_err("settings ACK write must fail");
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
            })
        });
    }

    fn assert_real_upstream_codec_http_error() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(
                    server_duplicate_content_length_response(),
                )]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let consume = async move {
                    match response.read_header_event().await {
                        Ok(_) => panic!("duplicate content-length response was accepted"),
                        Err(error) => error,
                    }
                };
                let mut run = Box::pin(session.run());
                let error = tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::select! {
                        error = consume => error,
                        result = run.as_mut() => panic!("session ended before semantic error: {result:?}"),
                    }
                })
                    .await
                    .expect("response semantic error was delivered");
                assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
                assert!(handle.initiate_drain().expect("drain accepted"));
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("semantic-error drain timed out")
                    .expect("response semantic error stays stream-local");
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.resets.len(), 1);
            })
        });
    }

    fn assert_real_unsolicited_upstream_response() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::DataEof(server_response_bytes(100, 0))]);
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let error = session
                    .run()
                    .await
                    .expect_err("unsolicited response fails the session");
                assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
                assert!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .goaways
                        .iter()
                        .any(|(_, code)| *code == H2ErrorCode::PROTOCOL_ERROR)
                );
            })
        });
    }

    fn assert_real_clean_downstream_eof() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(Vec::new())],
            ErrorHandlerCase::NoResponse,
            UniplexConfig::default(),
            None,
        );
        assert_eq!(outcome.invocations, 0);
        assert_eq!(outcome.session_error, None);
        assert_eq!(outcome.close, FixtureClose::ShutdownWrite);
        assert!(
            decode_server_frames_with_requests(&outcome.wire, 0)
                .goaways
                .iter()
                .all(|(_, code)| *code == H2ErrorCode::NO_ERROR)
        );
    }

    fn assert_real_downstream_response_body_error() {
        run_on_worker(|context| {
            Box::pin(async move {
                let release_error = Arc::new(tokio::sync::Notify::new());
                let finish_input = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(peer_request_bytes(
                        HttpMethod::Get,
                        &[],
                        None,
                        PeerControl::None,
                    )),
                    FixtureRead::WaitDataEof(Arc::clone(&finish_input), Vec::new()),
                ]);
                let probe = Rc::new(ErrorHandlerProbe::default());
                probe
                    .response_error_gate
                    .replace(Some(Arc::clone(&release_error)));
                let handler: Rc<dyn HttpHandler> = Rc::new(ErrorScenarioHandler {
                    case: ErrorHandlerCase::ResponseBodyError(HttpErrorCode::INTERNAL_ERROR),
                    probe,
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let _release_on_drop =
                    HeldBodyGates([Arc::clone(&release_error), Arc::clone(&finish_input)]);
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(2),
                        async {
                            complete_credit_wait(|| {
                                decode_server_frames(&state.borrow().writes.concat()).body_data
                                    == b"partialbody"
                            })
                            .await;
                            release_error.notify_one();
                            complete_credit_wait(|| {
                                !decode_server_frames(&state.borrow().writes.concat())
                                    .resets
                                    .is_empty()
                            })
                            .await;
                        },
                    ))
                    .await;
                    release_error.notify_one();
                    finish_input.notify_one();
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, outcome) = tokio::join!(session.run(), exercise);
                if let Err(payload) = outcome {
                    resume_unwind(payload);
                }
                outcome
                    .unwrap()
                    .expect("response body-error prefix and reset");
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                assert_eq!(decoded.body_data, b"partialbody");
                assert_eq!(
                    decoded.resets,
                    [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
                );
                assert_eq!(run.err().map(|error| error.code()), None);
                assert!(handle.is_finished());
                assert!(state.borrow().transport_dropped);
            })
        });
    }

    fn assert_real_peer_identity_policy() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_request_bytes(
                HttpMethod::Get,
                &[],
                None,
                PeerControl::None,
            ))],
            ErrorHandlerCase::PeerIdentity,
            UniplexConfig::default(),
            Some(PeerIdentity::new(b"bad_actor")),
        );
        assert_eq!(
            outcome.peer_identity.as_deref(),
            Some(b"bad_actor".as_slice())
        );
        let decoded = decode_server_frames(&outcome.wire);
        assert_eq!(decoded.header_statuses, [(StreamId::new(1), 407)]);
        assert_eq!(decoded.body_data, b"not ok!");
    }

    fn assert_real_stop_reading_default_500() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::Data(peer_incomplete_post_bytes(0))]);
                let probe = Rc::new(ErrorHandlerProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(ErrorScenarioHandler {
                    case: ErrorHandlerCase::StopWithoutResponse,
                    probe: Rc::clone(&probe),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_millis(5),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let decoded = decode_server_frames(&state.borrow().writes.concat());
                        if decoded.header_statuses == [(StreamId::new(1), 500)]
                            && decoded.resets == [(StreamId::new(1), H2ErrorCode::NO_ERROR)]
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before default response: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("default 500 and NO_ERROR reset were written");
                assert_eq!(probe.invocations.get(), 1);
                assert!(handle.initiate_drain().expect("drain accepted"));
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("default-response cleanup timed out")
                    .expect("default-response session completes");
            })
        });
    }

    fn assert_real_content_length_mismatch() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_content_length_mismatch_request())],
            ErrorHandlerCase::ReadBodyThenRespond {
                status: 200,
                body: b"ok",
            },
            UniplexConfig::default(),
            None,
        );
        assert_eq!(
            outcome.request_error,
            Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH)
        );
        assert_eq!(outcome.request_body, 0);
        let decoded = decode_server_frames(&outcome.wire);
        assert_eq!(
            decoded.resets,
            [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
        );
    }

    fn assert_real_handler_runtime_error() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_request_bytes(
                HttpMethod::Get,
                &[],
                None,
                PeerControl::None,
            ))],
            ErrorHandlerCase::RuntimeError,
            UniplexConfig::default(),
            None,
        );
        assert_eq!(
            decode_server_frames(&outcome.wire).header_statuses,
            [(StreamId::new(1), 500)]
        );
    }

    fn assert_real_state_machine_error() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_state_machine_error())],
            ErrorHandlerCase::StateMachineHeaderError,
            UniplexConfig::default(),
            None,
        );
        assert_eq!(
            outcome.request_error,
            Some(HttpErrorCode::INVALID_STATE_TRANSITION)
        );
        assert!(outcome.partial_message);
        assert_eq!(
            decode_server_frames(&outcome.wire).resets,
            [(StreamId::new(1), H2ErrorCode::PROTOCOL_ERROR)]
        );
    }

    fn assert_real_eof_with_stream() {
        let outcome = run_downstream_error_case(
            vec![
                FixtureRead::Data(peer_incomplete_post_bytes(0)),
                FixtureRead::DataEof(Vec::new()),
            ],
            ErrorHandlerCase::ReadBodyThenRespond {
                status: 500,
                body: b"",
            },
            UniplexConfig::default(),
            None,
        );
        assert_eq!(outcome.request_error, Some(HttpErrorCode::TRANSPORT_EOF));
        assert_eq!(
            decode_server_frames(&outcome.wire).header_statuses,
            [(StreamId::new(1), 500)]
        );
    }

    fn assert_real_h2_websocket_upgrade() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_websocket_request())],
            ErrorHandlerCase::WebSocket,
            UniplexConfig::default(),
            None,
        );
        assert!(outcome.websocket);
        assert_eq!(outcome.request_body, 100);
        let decoded = decode_server_frames(&outcome.wire);
        assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
        assert_eq!(decoded.body_bytes, 100);
        assert_eq!(decoded.resets, []);
    }

    fn assert_real_codec_connection_error() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_stream_zero_request())],
            ErrorHandlerCase::NoResponse,
            UniplexConfig::default(),
            None,
        );
        assert_eq!(outcome.invocations, 0);
        assert_eq!(outcome.session_error, Some(HttpErrorCode::PROTOCOL_ERROR));
        assert!(
            decode_server_frames_with_requests(&outcome.wire, 0)
                .goaways
                .iter()
                .any(|(stream, code)| {
                    *stream == StreamId::UNSPECIFIED && *code == H2ErrorCode::PROTOCOL_ERROR
                })
        );
    }

    fn assert_real_downstream_codec_http_error() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_duplicate_content_length_request())],
            ErrorHandlerCase::CodecHeaderError,
            UniplexConfig::default(),
            None,
        );
        let decoded = decode_server_frames(&outcome.wire);
        assert_eq!(decoded.header_statuses, [(StreamId::new(1), 400)]);
        assert!(decoded.resets.is_empty());
        assert_eq!(outcome.session_error, None);
    }

    fn assert_real_handler_no_response() {
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_request_bytes(
                HttpMethod::Get,
                &[],
                None,
                PeerControl::None,
            ))],
            ErrorHandlerCase::NoResponse,
            UniplexConfig::default(),
            None,
        );
        assert_eq!(
            decode_server_frames(&outcome.wire).header_statuses,
            [(StreamId::new(1), 500)]
        );
    }

    fn assert_real_response_headers_error() {
        let outcome = run_downstream_handler_error(HttpErrorCode::ENHANCE_YOUR_CALM);
        let decoded = decode_server_frames(&outcome.wire);
        assert!(decoded.header_statuses.is_empty());
        assert_eq!(
            decoded.resets,
            [(StreamId::new(1), H2ErrorCode::ENHANCE_YOUR_CALM)]
        );
    }

    fn assert_real_custom_timeout_response(status: u16) {
        let outcome = run_on_worker(move |context| {
            Box::pin(async move {
                let finish = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(peer_incomplete_post_bytes(0)),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let probe = Rc::new(ErrorHandlerProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(ErrorScenarioHandler {
                    case: ErrorHandlerCase::ReadBodyThenRespond {
                        status,
                        body: b"custom response generated",
                    },
                    probe: Rc::clone(&probe),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig {
                        read_timeout: Duration::from_millis(100),
                        stream_read_timeout: Duration::from_millis(5),
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 error session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if decode_server_frames(&state.borrow().writes.concat())
                            .header_statuses
                            .contains(&(StreamId::new(1), 408))
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before custom timeout response: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("custom timeout response was written");
                finish.notify_one();
                let result = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("custom timeout session hung");
                ErrorSessionOutcome {
                    invocations: probe.invocations.get(),
                    request_error: *probe.request_error.borrow(),
                    partial_message: probe.partial_message.get(),
                    request_body: probe.request_body.get(),
                    peer_identity: probe.peer_identity.borrow().clone(),
                    websocket: probe.websocket.get(),
                    wire: state.borrow().writes.concat(),
                    close: state.borrow().close,
                    session_error: result.err().map(|error| error.code()),
                }
            })
        });
        assert_eq!(outcome.request_error, Some(HttpErrorCode::READ_TIMEOUT));
        let decoded = decode_server_frames(&outcome.wire);
        assert_eq!(decoded.header_statuses, [(StreamId::new(1), 408)]);
        if status == 408 {
            assert_eq!(decoded.body_data, b"custom response generated");
        } else {
            assert!(decoded.body_data.is_empty());
        }
    }

    struct IgnoreCodecEvents;

    impl HttpCodecCallback for IgnoreCodecEvents {
        fn on_event(&mut self, _event: CodecEvent<'_>) {}
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum EligibilityPhase {
        Headers,
        Body,
        Resume,
        SynchronousWake,
    }

    struct EligibilityGate {
        released: std::sync::atomic::AtomicBool,
        polls: AtomicUsize,
        synchronous: bool,
        waker: std::sync::Mutex<Option<std::task::Waker>>,
        changed: Arc<tokio::sync::Notify>,
    }

    impl EligibilityGate {
        fn new(phase: EligibilityPhase, changed: Arc<tokio::sync::Notify>) -> Arc<Self> {
            Arc::new(Self {
                released: std::sync::atomic::AtomicBool::new(false),
                polls: AtomicUsize::new(0),
                synchronous: phase == EligibilityPhase::SynchronousWake,
                waker: std::sync::Mutex::new(None),
                changed,
            })
        }

        fn poll(&self, context: &mut std::task::Context<'_>) -> std::task::Poll<()> {
            if self.released.load(Ordering::SeqCst) {
                return std::task::Poll::Ready(());
            }
            let polls = self.polls.fetch_add(1, Ordering::SeqCst) + 1;
            assert!(polls <= 256, "pending source monopolized the native writer");
            *self.waker.lock().unwrap() = Some(context.waker().clone());
            if polls == 1 {
                self.changed.notify_one();
            }
            if self.synchronous {
                context.waker().wake_by_ref();
            }
            std::task::Poll::Pending
        }

        fn release(&self) {
            self.released.store(true, Ordering::SeqCst);
            let waker = self.waker.lock().unwrap().take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    #[derive(Default)]
    struct EligibilityProbe {
        header_reads: Cell<usize>,
        body_reads: Cell<usize>,
        reads_dropped: Cell<usize>,
        cancelled_reads: Cell<usize>,
        resume_drops: Cell<usize>,
        cancelled_resumes: Cell<usize>,
        source_drops: Cell<usize>,
        stops: RefCell<Vec<Option<HttpErrorCode>>>,
    }

    struct EligibilityRead<Event> {
        event: Option<Box<Event>>,
        gate: Option<Arc<EligibilityGate>>,
        probe: Rc<EligibilityProbe>,
    }

    impl<Event> Future for EligibilityRead<Event> {
        type Output = Result<Event, HttpError>;

        fn poll(
            mut self: Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if let Some(gate) = &self.gate
                && gate.poll(context).is_pending()
            {
                return std::task::Poll::Pending;
            }
            std::task::Poll::Ready(Ok(*self.event.take().expect("one retained source read")))
        }
    }

    impl<Event> Drop for EligibilityRead<Event> {
        fn drop(&mut self) {
            self.probe
                .reads_dropped
                .set(self.probe.reads_dropped.get() + 1);
            if self.event.is_some() {
                self.probe
                    .cancelled_reads
                    .set(self.probe.cancelled_reads.get() + 1);
            }
        }
    }

    struct EligibilityResume {
        gate: Arc<EligibilityGate>,
        probe: Rc<EligibilityProbe>,
        completed: bool,
    }

    impl Future for EligibilityResume {
        type Output = roxygen_core::events::ResumeStatus;

        fn poll(
            mut self: Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if self.gate.poll(context).is_pending() {
                return std::task::Poll::Pending;
            }
            self.completed = true;
            std::task::Poll::Ready(roxygen_core::events::ResumeStatus::Signalled)
        }
    }

    impl Drop for EligibilityResume {
        fn drop(&mut self) {
            self.probe
                .resume_drops
                .set(self.probe.resume_drops.get() + 1);
            if !self.completed {
                self.probe
                    .cancelled_resumes
                    .set(self.probe.cancelled_resumes.get() + 1);
            }
        }
    }

    struct EligibilitySource {
        header: Option<HttpHeaderEvent>,
        events: VecDeque<HttpBodyEvent>,
        phase: EligibilityPhase,
        gate: Arc<EligibilityGate>,
        probe: Rc<EligibilityProbe>,
    }

    impl EligibilitySource {
        fn new(
            phase: EligibilityPhase,
            gate: Arc<EligibilityGate>,
            probe: Rc<EligibilityProbe>,
            bytes: Bytes,
            upstream: bool,
        ) -> Self {
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_http_version(1, 1);
            if upstream {
                message.set_method(HttpMethod::Post).unwrap();
                message.set_url(b"/pending", false).unwrap();
                message
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
            } else {
                message.set_status_code(200).unwrap();
            }
            let mut events = VecDeque::new();
            if phase == EligibilityPhase::Resume {
                events.push_back(HttpBodyEvent::suspend(Box::pin(EligibilityResume {
                    gate: Arc::clone(&gate),
                    probe: Rc::clone(&probe),
                    completed: false,
                })));
            }
            let middle = bytes.len() / 2;
            for part in [bytes.slice(..middle), bytes.slice(middle..)] {
                events.push_back(HttpBodyEvent::body(
                    BufQueue::from_bytes(part, 1 << 20).unwrap(),
                    false,
                ));
            }
            let mut trailers = HttpHeaders::new();
            trailers.set("x-source-finish", b"retained");
            events.push_back(HttpBodyEvent::trailers(trailers));
            Self {
                header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                events,
                phase,
                gate,
                probe,
            }
        }
    }

    impl HttpSource for EligibilitySource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.probe
                .header_reads
                .set(self.probe.header_reads.get() + 1);
            Box::pin(EligibilityRead {
                event: Some(Box::new(self.header.take().expect("headers read once"))),
                gate: (self.phase == EligibilityPhase::Headers).then(|| Arc::clone(&self.gate)),
                probe: Rc::clone(&self.probe),
            })
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            let first = self.probe.body_reads.get() == 0;
            self.probe.body_reads.set(self.probe.body_reads.get() + 1);
            Box::pin(EligibilityRead {
                event: Some(Box::new(
                    self.events.pop_front().expect("body read once per event"),
                )),
                gate: (first
                    && matches!(
                        self.phase,
                        EligibilityPhase::Body | EligibilityPhase::SynchronousWake
                    ))
                .then(|| Arc::clone(&self.gate)),
                probe: Rc::clone(&self.probe),
            })
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.probe.stops.borrow_mut().push(error);
        }
    }

    impl Drop for EligibilitySource {
        fn drop(&mut self) {
            self.probe
                .source_drops
                .set(self.probe.source_drops.get() + 1);
            self.gate.changed.notify_one();
        }
    }

    struct EligibilityHandler(RefCell<Option<EligibilitySource>>);

    impl HttpHandler for EligibilityHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let source = self.0.borrow_mut().take();
            Box::pin(async move {
                assert!(request.read_header_event().await?.eom());
                Ok(match source {
                    Some(source) => HttpSourceHolder::new(Box::new(source)),
                    None => HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_fixed_response(200, Some(queue(b"ready-sibling")))
                            .unwrap(),
                    )),
                })
            })
        }
    }

    struct EligibilityTransport {
        inner: Rc<FixtureTransport>,
        changed: Arc<tokio::sync::Notify>,
        reads_started: Cell<usize>,
        backing_start: usize,
        backing_length: usize,
        shared_written: Cell<usize>,
        /// Drops of the backing buffer. A segment counts as shared only while the buffer is
        /// alive: once freed, its address range can hold a later allocation.
        backing_drops: Arc<AtomicUsize>,
    }

    impl UniplexTransport for EligibilityTransport {
        fn read(
            &self,
            context: &WorkerContext,
            maximum: usize,
            allocation_size: usize,
            timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
            self.reads_started.set(self.reads_started.get() + 1);
            self.changed.notify_one();
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
            for segment in data.segments() {
                let start = segment.as_ptr() as usize;
                if self.backing_drops.load(Ordering::SeqCst) == 0
                    && start >= self.backing_start
                    && start < self.backing_start + self.backing_length
                {
                    assert!(start + segment.len() <= self.backing_start + self.backing_length);
                    self.shared_written
                        .set(self.shared_written.get() + segment.len());
                }
            }
            let write = self
                .inner
                .write(context, data, flags, timeout, cancellation);
            let changed = Arc::clone(&self.changed);
            Box::pin(async move {
                let result = write.await;
                changed.notify_one();
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

    #[derive(Default)]
    struct EligibilityWire {
        body: std::collections::BTreeMap<StreamId, Vec<u8>>,
        statuses: Vec<(StreamId, u16)>,
        resets: Vec<(StreamId, H2ErrorCode)>,
        completed: Vec<StreamId>,
        trailers: Vec<(StreamId, Vec<u8>)>,
        pings: Vec<u64>,
    }

    impl HttpCodecCallback for EligibilityWire {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            match event {
                CodecEvent::HeadersComplete {
                    stream,
                    mut message,
                } => {
                    self.statuses
                        .push((stream, message.status_code().expect("real response status")));
                }
                CodecEvent::Abort { stream, code } => self.resets.push((stream, code)),
                CodecEvent::Body { stream, body, .. } => {
                    for segment in body.segments() {
                        self.body
                            .entry(stream)
                            .or_default()
                            .extend_from_slice(segment);
                    }
                }
                CodecEvent::MessageComplete { stream, .. } => self.completed.push(stream),
                CodecEvent::TrailersComplete { stream, trailers } => {
                    self.trailers.push((
                        stream,
                        trailers.get_single_or_empty("x-source-finish").to_vec(),
                    ));
                }
                CodecEvent::PingReply { data } => self.pings.push(data),
                CodecEvent::Error { .. } => {
                    panic!("native source eligibility produced invalid H2 wire")
                }
                _ => {}
            }
        }
    }

    fn eligibility_wire(state: &RefCell<FixtureTransportState>) -> EligibilityWire {
        let wire = state.borrow().writes.concat();
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        for _ in 0..3 {
            codec.create_stream();
        }
        let mut decoded = EligibilityWire::default();
        let mut output = BufQueue::new(1 << 20);
        let result = codec.on_ingress(&queue(&wire), &mut decoded, &mut output);
        assert_eq!(result.consumed, wire.len());
        assert!(result.egress_results.iter().all(Result::is_ok));
        decoded
    }

    async fn eligibility_catch_unwind<Operation: Future>(
        operation: Operation,
    ) -> std::thread::Result<Operation::Output> {
        let mut operation = std::pin::pin!(operation);
        std::future::poll_fn(|context| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                operation.as_mut().poll(context)
            })) {
                Ok(result) => result.map(Ok),
                Err(payload) => std::task::Poll::Ready(Err(payload)),
            }
        })
        .await
    }

    async fn eligibility_wait(changed: &tokio::sync::Notify, condition: impl Fn() -> bool) {
        loop {
            let notified = changed.notified();
            if condition() {
                return;
            }
            notified.await;
        }
    }

    fn eligibility_frame(codec: &mut Http2Codec, frame: EgressFrame<'_>) -> Vec<u8> {
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, frame, &mut IgnoreCodecEvents)
            .unwrap();
        output.coalesce(output.chain_length()).unwrap().to_vec()
    }

    fn eligibility_request(codec: &mut Http2Codec, urgency: u8) -> Vec<u8> {
        let stream = codec.create_stream();
        let mut message = roxygen_core::message::HttpMessage::new();
        message.set_method(HttpMethod::Get).unwrap();
        message.set_url(b"/eligibility", false).unwrap();
        message.set_http_version(1, 1);
        message
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        message.set_http_priority(HttpPriority::from_urgency_and_incremental(urgency, false));
        eligibility_frame(
            codec,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
        )
    }

    #[test]
    fn egress_event_prefetch_cap_yields_to_ping_and_reopens_with_credit() {
        run_on_worker(|context| {
            Box::pin(async move {
                const K: usize = 65_535;
                let stream = StreamId::new(1);
                let changed = Arc::new(tokio::sync::Notify::new());
                let gate = EligibilityGate::new(EligibilityPhase::Headers, changed);
                gate.release();
                let probe = Rc::new(EligibilityProbe::default());
                let mut source = EligibilitySource::new(
                    EligibilityPhase::Headers,
                    gate,
                    Rc::clone(&probe),
                    Bytes::from(vec![b'p'; K]),
                    false,
                );
                let trailers = source.events.pop_back().unwrap();
                for ordinal in 0..300 {
                    source.events.push_back(if ordinal % 2 == 0 {
                        HttpBodyEvent::padding(0, false)
                    } else {
                        HttpBodyEvent::body(BufQueue::new(0), false)
                    });
                }
                source
                    .events
                    .push_back(HttpBodyEvent::body(queue(b"x"), false));
                source.events.push_back(trailers);
                let handler: Rc<dyn HttpHandler> =
                    Rc::new(EligibilityHandler(RefCell::new(Some(source))));
                let mut peer = Http2Codec::new(TransportDirection::Upstream);
                let mut initial = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
                let mut settings = BufQueue::new(4096);
                peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                    .unwrap();
                initial.extend_from_slice(&settings.coalesce(settings.chain_length()).unwrap());
                initial.extend(eligibility_request(&mut peer, 3));
                let ping = Arc::new(tokio::sync::Notify::new());
                let credit = Arc::new(tokio::sync::Notify::new());
                let finish = Arc::new(tokio::sync::Notify::new());
                let mut update = eligibility_frame(
                    &mut peer,
                    EgressFrame::WindowUpdate {
                        stream: StreamId::UNSPECIFIED,
                        amount: 2,
                    },
                );
                update.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::WindowUpdate { stream, amount: 1 },
                ));
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(
                        Arc::clone(&ping),
                        eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(91) }),
                    ),
                    FixtureRead::WaitData(Arc::clone(&credit), update),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport.clone(),
                    Some(handler),
                    UniplexConfig {
                        write_timeout: Duration::from_secs(5),
                        read_timeout: Duration::ZERO,
                        stream_read_timeout: Duration::ZERO,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        ..Http2SessionConfig::default()
                    },
                )
                .unwrap();
                let control = handle.observe_h2_source_timers_for_test(stream).unwrap();
                control.pause_next_source_yield.set(true);
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(5),
                        async {
                            complete_credit_wait(|| {
                                control.source_yield_paused.get() || probe.source_drops.get() != 0
                            })
                            .await;
                            assert!(
                                control.source_yield_paused.get(),
                                "the finite source must yield before consuming its terminal event"
                            );
                            assert!(probe.body_reads.get() > 2 && probe.body_reads.get() < 258);
                            assert_eq!(probe.source_drops.get(), 0);
                            control.resume_source_yield.notify_one();
                            complete_credit_wait(|| probe.body_reads.get() >= 258).await;
                            assert_eq!(probe.body_reads.get(), 258);
                            assert_eq!(probe.reads_dropped.get(), 259);
                            assert_eq!(probe.cancelled_reads.get(), 0);
                            assert_eq!(probe.source_drops.get(), 0);
                            let before = eligibility_wire(&wire);
                            assert_eq!(before.body[&stream], vec![b'p'; K]);
                            assert!(before.completed.is_empty());
                            ping.notify_one();
                            complete_credit_wait(|| eligibility_wire(&wire).pings == [91]).await;
                            let snapshot = control.snapshot.borrow().clone().unwrap();
                            assert_eq!(
                                (snapshot.connection_credit, snapshot.stream_credit),
                                (0, 0)
                            );
                            assert!(snapshot.flow_deadline.is_some());
                            assert_eq!(snapshot.buffer_timeout, None);
                            assert_eq!(probe.body_reads.get(), 258);
                            assert_eq!(probe.source_drops.get(), 0);
                            assert_eq!(wire.borrow().close, FixtureClose::Open);
                            let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                            assert_eq!((owners.queued, owners.complete), (1, 0));
                            assert_eq!(owners.egress_complete, Some(false));
                            credit.notify_one();
                            complete_credit_wait(|| eligibility_wire(&wire).completed == [stream])
                                .await;
                            let after = eligibility_wire(&wire);
                            let mut expected = vec![b'p'; K];
                            expected.push(b'x');
                            assert_eq!(after.body[&stream], expected);
                            assert_eq!(after.trailers, [(stream, b"retained".to_vec())]);
                            assert!(after.resets.is_empty());
                            assert_eq!(
                                (
                                    probe.body_reads.get(),
                                    probe.reads_dropped.get(),
                                    probe.cancelled_reads.get()
                                ),
                                (304, 305, 0)
                            );
                            assert_eq!(probe.source_drops.get(), 1);
                        },
                    ))
                    .await;
                    control.pause_next_source_yield.set(false);
                    control.resume_source_yield.notify_one();
                    ping.notify_one();
                    credit.notify_one();
                    finish.notify_one();
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, exercise) =
                    tokio::join!(eligibility_catch_unwind(session.run()), exercise);
                let finished = handle.is_finished();
                drop(handle);
                drop(transport);
                if let Err(payload) = exercise {
                    resume_unwind(payload);
                }
                if let Err(payload) = run {
                    resume_unwind(payload);
                }
                exercise
                    .unwrap()
                    .expect("bounded count-prefetch writer fixture");
                run.unwrap()
                    .expect("count-prefetch preserves the connection");
                assert!(finished);
                assert_eq!(probe.source_drops.get(), 1);
                assert_eq!(probe.cancelled_reads.get(), 0);
                assert!(wire.borrow().transport_dropped);
            })
        });
    }

    async fn response_source_eligibility_case(
        context: WorkerContext,
        phase: EligibilityPhase,
        urgent: bool,
        reset: bool,
    ) {
        let changed = Arc::new(tokio::sync::Notify::new());
        let gate = EligibilityGate::new(phase, Arc::clone(&changed));
        let probe = Rc::new(EligibilityProbe::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let expected: Vec<u8> = (0..70_000).map(|offset| (offset % 251) as u8).collect();
        let bytes = Bytes::from_owner(DropTrackedBytes {
            bytes: expected.clone(),
            drops: Arc::clone(&drops),
        });
        let backing_start = bytes.as_ptr() as usize;
        let mut source =
            EligibilitySource::new(phase, Arc::clone(&gate), Rc::clone(&probe), bytes, false);
        source
            .header
            .as_mut()
            .unwrap()
            .headers_mut()
            .set_http_priority(HttpPriority::from_urgency_and_incremental(
                if urgent { 0 } else { 3 },
                false,
            ));
        let mut peer = Http2Codec::new(TransportDirection::Upstream);
        let mut initial = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
        let mut settings = BufQueue::new(1 << 20);
        peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
            .unwrap();
        initial.extend_from_slice(&settings.coalesce(settings.chain_length()).unwrap());
        initial.extend(eligibility_request(&mut peer, if urgent { 0 } else { 3 }));
        let sibling = eligibility_request(&mut peer, 3);
        let releases: Vec<_> = (0..6)
            .map(|_| Arc::new(tokio::sync::Notify::new()))
            .collect();
        let ping_one = eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(41) });
        let ping_two = eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(42) });
        let mut action = Vec::new();
        if reset {
            action.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::CANCEL,
                },
            ));
            action.extend(eligibility_request(&mut peer, 3));
        } else {
            for stream in [StreamId::UNSPECIFIED, StreamId::new(1)] {
                action.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::WindowUpdate {
                        stream,
                        amount: 100_000,
                    },
                ));
            }
        }
        let ping_three = eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(43) });
        let (inner, state) = FixtureTransport::new([
            FixtureRead::SegmentedData(queue(&initial)),
            FixtureRead::WaitData(Arc::clone(&releases[0]), sibling),
            FixtureRead::WaitData(Arc::clone(&releases[1]), ping_one),
            FixtureRead::WaitData(Arc::clone(&releases[2]), ping_two),
            FixtureRead::WaitData(Arc::clone(&releases[3]), action),
            FixtureRead::WaitData(Arc::clone(&releases[4]), ping_three),
            FixtureRead::WaitDataEof(Arc::clone(&releases[5]), Vec::new()),
        ]);
        let transport = Rc::new(EligibilityTransport {
            inner,
            changed: Arc::clone(&changed),
            reads_started: Cell::new(0),
            backing_start,
            backing_length: expected.len(),
            shared_written: Cell::new(0),
            backing_drops: Arc::clone(&drops),
        });
        let (session, _) = UniplexSession::new_http2(
            context,
            TransportDirection::Downstream,
            transport.clone(),
            Some(Rc::new(EligibilityHandler(RefCell::new(Some(source))))),
            UniplexConfig::default(),
            Http2SessionConfig::default(),
        )
        .unwrap();
        let exercise = async {
            let outcome =
                eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(3), async {
                    eligibility_wait(&changed, || gate.polls.load(Ordering::SeqCst) != 0).await;
                    let header_reads = probe.header_reads.get();
                    let body_reads = probe.body_reads.get();
                    releases[0].notify_one();
                    eligibility_wait(&changed, || {
                        eligibility_wire(&state)
                            .completed
                            .contains(&StreamId::new(3))
                    })
                    .await;
                    let wire = eligibility_wire(&state);
                    assert_eq!(wire.body[&StreamId::new(3)].as_slice(), b"ready-sibling");
                    assert!(wire.statuses.contains(&(StreamId::new(3), 200)));
                    assert!(!wire.completed.contains(&StreamId::new(1)));
                    assert!(!gate.released.load(Ordering::SeqCst));
                    for (index, ping) in [(1, 41), (2, 42)] {
                        releases[index].notify_one();
                        eligibility_wait(&changed, || {
                            eligibility_wire(&state).pings.contains(&ping)
                        })
                        .await;
                    }
                    assert_eq!(probe.header_reads.get(), header_reads);
                    assert_eq!(probe.body_reads.get(), body_reads);
                    assert_eq!(probe.source_drops.get(), 0);
                    assert_eq!(probe.cancelled_reads.get(), 0);
                    assert_eq!(probe.cancelled_resumes.get(), 0);
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    if phase != EligibilityPhase::SynchronousWake {
                        assert_eq!(gate.polls.load(Ordering::SeqCst), 1);
                    }
                    if reset {
                        let stale = gate.waker.lock().unwrap().as_ref().unwrap().clone();
                        releases[3].notify_one();
                        eligibility_wait(&changed, || {
                            probe.source_drops.get() == 1
                                && eligibility_wire(&state)
                                    .completed
                                    .contains(&StreamId::new(5))
                        })
                        .await;
                        let polls = gate.polls.load(Ordering::SeqCst);
                        stale.wake();
                        releases[4].notify_one();
                        eligibility_wait(&changed, || eligibility_wire(&state).pings.contains(&43))
                            .await;
                        assert_eq!(gate.polls.load(Ordering::SeqCst), polls);
                        assert!(!gate.released.load(Ordering::SeqCst));
                        assert_eq!(
                            eligibility_wire(&state).body[&StreamId::new(5)].as_slice(),
                            b"ready-sibling"
                        );
                        assert_eq!(transport.shared_written.get(), 0);
                    } else {
                        gate.release();
                        eligibility_wait(&changed, || {
                            eligibility_wire(&state)
                                .body
                                .get(&StreamId::new(1))
                                .is_some_and(|body| body.len() == 65_535 - b"ready-sibling".len())
                        })
                        .await;
                        assert!(
                            !eligibility_wire(&state)
                                .completed
                                .contains(&StreamId::new(1))
                        );
                        releases[3].notify_one();
                        eligibility_wait(&changed, || {
                            eligibility_wire(&state)
                                .completed
                                .contains(&StreamId::new(1))
                        })
                        .await;
                        let wire = eligibility_wire(&state);
                        assert_eq!(wire.body[&StreamId::new(1)], expected);
                        assert!(wire.statuses.contains(&(StreamId::new(1), 200)));
                        assert!(wire.resets.is_empty());
                        assert_eq!(wire.trailers, [(StreamId::new(1), b"retained".to_vec())]);
                        assert_eq!(transport.shared_written.get(), expected.len());
                    }
                }))
                .await;
            if !matches!(&outcome, Ok(Ok(()))) {
                gate.release();
            }
            for release in &releases {
                release.notify_one();
            }
            outcome
        };
        let (run, exercise) = tokio::join!(session.run(), exercise);
        if let Err(payload) = exercise {
            std::panic::resume_unwind(payload);
        }
        exercise
            .unwrap()
            .expect("bounded native source eligibility phases");
        run.expect("native source eligibility session joins cleanly");
        assert_eq!(probe.source_drops.get(), 1);
        assert_eq!(
            probe.reads_dropped.get(),
            probe.header_reads.get() + probe.body_reads.get()
        );
        assert_eq!(
            probe.resume_drops.get(),
            usize::from(phase == EligibilityPhase::Resume)
        );
        assert_eq!(
            probe.cancelled_reads.get(),
            usize::from(reset && phase != EligibilityPhase::Resume)
        );
        assert_eq!(
            probe.cancelled_resumes.get(),
            usize::from(reset && phase == EligibilityPhase::Resume)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
        drop(transport);
        assert!(state.borrow().transport_dropped);
    }

    #[test]
    fn pending_response_source_preserves_sibling_progress() {
        for phase in [
            EligibilityPhase::Headers,
            EligibilityPhase::Body,
            EligibilityPhase::Resume,
            EligibilityPhase::SynchronousWake,
        ] {
            for urgent in [false, true] {
                run_on_worker(move |context| {
                    Box::pin(response_source_eligibility_case(
                        context, phase, urgent, false,
                    ))
                });
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CompleteCreditCase {
        Exact,
        Paused,
        Unfinished,
        BufferTimeout,
    }

    struct CompleteCreditHandler {
        response: RefCell<Option<EligibilitySource>>,
        held_request: Rc<RefCell<Option<HttpSourceHolder>>>,
    }

    impl HttpHandler for CompleteCreditHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let response = self.response.borrow_mut().take().unwrap();
            let held = Rc::clone(&self.held_request);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(!headers.eom());
                drop(headers);
                *held.borrow_mut() = Some(request);
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    #[derive(Default)]
    struct CompleteCreditWire {
        frames: DecodedServerFrames,
        completed: Vec<StreamId>,
        trailers: Vec<(StreamId, Vec<u8>)>,
    }

    impl HttpCodecCallback for CompleteCreditWire {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            if let CodecEvent::MessageComplete { stream, .. } = &event {
                self.completed.push(*stream);
            }
            if let CodecEvent::TrailersComplete { stream, trailers } = &event {
                self.trailers.push((
                    *stream,
                    trailers.get_single_or_empty("x-source-finish").to_vec(),
                ));
            }
            self.frames.on_event(event);
        }
    }

    fn complete_credit_wire(
        upstream: bool,
        state: &RefCell<FixtureTransportState>,
    ) -> CompleteCreditWire {
        let wire = state.borrow().writes.concat();
        let mut codec = Http2Codec::new(if upstream {
            TransportDirection::Downstream
        } else {
            TransportDirection::Upstream
        });
        if !upstream {
            assert_eq!(codec.create_stream(), StreamId::new(1));
        }
        let mut decoded = CompleteCreditWire::default();
        let mut output = BufQueue::new(1 << 20);
        let result = codec.on_ingress(&queue(&wire), &mut decoded, &mut output);
        assert_eq!(result.consumed, wire.len());
        assert!(result.egress_results.iter().all(Result::is_ok));
        decoded
    }

    async fn complete_credit_wait(condition: impl Fn() -> bool) {
        while !condition() {
            tokio::task::yield_now().await;
        }
    }

    async fn complete_credit_case(
        context: WorkerContext,
        upstream: bool,
        case: CompleteCreditCase,
    ) {
        let stream = StreamId::new(1);
        let initial_credit: u32 = 4;
        let timeout_case = case == CompleteCreditCase::BufferTimeout;
        assert!(upstream || case == CompleteCreditCase::Exact);
        let mut peer = Http2Codec::new(if upstream {
            TransportDirection::Downstream
        } else {
            TransportDirection::Upstream
        });
        peer.egress_settings_mut()
            .set(SettingsId::InitialWindowSize, u64::from(initial_credit));
        let mut initial = if upstream {
            Vec::new()
        } else {
            eligibility_frame(&mut peer, EgressFrame::ConnectionPreface)
        };
        let mut settings = BufQueue::new(4096);
        peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
            .unwrap();
        initial.extend_from_slice(&settings.coalesce(settings.chain_length()).unwrap());
        if !upstream {
            assert_eq!(peer.create_stream(), stream);
            let request = committed_test_headers(b"/held-ingress", false).into_headers();
            initial.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
            ));
        }
        let release_credit = Arc::new(tokio::sync::Notify::new());
        let release_peer = Arc::new(tokio::sync::Notify::new());
        let mut reads = vec![FixtureRead::Data(initial)];
        if case == CompleteCreditCase::Unfinished {
            reads.push(FixtureRead::WaitData(
                Arc::clone(&release_credit),
                eligibility_frame(&mut peer, EgressFrame::WindowUpdate { stream, amount: 4 }),
            ));
        }
        let peer_finish = if timeout_case {
            Vec::new()
        } else if upstream {
            let mut response = HttpMessage::new();
            response.set_status_code(200).unwrap();
            response.set_http_version(1, 1);
            eligibility_frame(
                &mut peer,
                EgressFrame::Headers {
                    stream,
                    message: &response,
                    end_stream: true,
                    extra_headers: None,
                },
            )
        } else {
            eligibility_frame(&mut peer, EgressFrame::EndOfMessage { stream })
        };
        reads.push(FixtureRead::WaitDataEof(
            Arc::clone(&release_peer),
            peer_finish,
        ));
        let (transport, wire) = FixtureTransport::new(reads);
        let probe = Rc::new(EligibilityProbe::default());
        let gate =
            EligibilityGate::new(EligibilityPhase::Body, Arc::new(tokio::sync::Notify::new()));
        let payload_length = match case {
            CompleteCreditCase::Unfinished => 8,
            CompleteCreditCase::BufferTimeout => 65_539,
            _ => 4,
        };
        let payload = vec![b'b'; payload_length];
        let mut source = EligibilitySource::new(
            EligibilityPhase::Body,
            Arc::clone(&gate),
            Rc::clone(&probe),
            Bytes::from_static(b"unused fixture events"),
            upstream,
        );
        source.events = VecDeque::from([HttpBodyEvent::body(queue(&payload), !timeout_case)]);
        let mut source = Some(source);
        let held_request = Rc::new(RefCell::new(None));
        let handler = (!upstream).then(|| {
            Rc::new(CompleteCreditHandler {
                response: RefCell::new(source.take()),
                held_request: Rc::clone(&held_request),
            }) as Rc<dyn HttpHandler>
        });
        let (session, handle) = UniplexSession::new_http2(
            context,
            if upstream {
                TransportDirection::Upstream
            } else {
                TransportDirection::Downstream
            },
            transport.clone(),
            handler,
            UniplexConfig {
                write_timeout: if timeout_case {
                    Duration::from_millis(500)
                } else {
                    UniplexConfig::default().write_timeout
                },
                ..UniplexConfig::default()
            },
            Http2SessionConfig {
                double_goaway_drain: false,
                ..Http2SessionConfig::default()
            },
        )
        .unwrap();
        let ended = Rc::new(Cell::new(false));
        let observer = Rc::new(HeaderReentryProbe {
            phase: HeaderReentryPhase::RequestEnd,
            action: RefCell::new(None),
            opened: Cell::new(0),
            closed: Cell::new(0),
            events: RefCell::new(Vec::new()),
        });
        if !timeout_case {
            let callback_handle = handle.clone();
            let callback_ended = Rc::clone(&ended);
            *observer.action.borrow_mut() = Some(Box::new(move || {
                let owners = callback_handle.h2_egress_owners_for_test(stream).unwrap();
                assert_eq!(owners.egress_complete, Some(true));
                assert_eq!((owners.queued, owners.complete), (1, 1));
                assert_eq!(callback_handle.http2_stream_send_credit(stream).unwrap(), 0);
                if case == CompleteCreditCase::Paused {
                    let mut priority = HttpPriority::from_urgency_and_incremental(3, true);
                    priority.paused = true;
                    assert!(
                        callback_handle
                            .update_http2_priority(stream, priority)
                            .unwrap()
                    );
                }
                callback_ended.set(true);
            }));
        }
        let _registration = session.add_lifecycle_observer(observer.clone()).unwrap();
        let mut response = if upstream {
            let prepared = UniplexHandle::prepare_request_with_headers(
                committed_test_headers(b"/complete-credit", false),
                HttpSourceHolder::new(Box::new(source.take().unwrap())),
                handle.reserve_request().unwrap(),
            );
            Some(handle.send_prepared_request(prepared).unwrap())
        } else {
            None
        };
        let stage = Cell::new("waiting for source and initial credit");
        let exercise = async {
            let outcome =
                eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(3), async {
                    complete_credit_wait(|| {
                        handle.http2_stream_send_credit(stream).ok() == Some(initial_credit)
                            && gate.polls.load(Ordering::SeqCst) != 0
                    })
                    .await;
                    assert_eq!(probe.body_reads.get(), 1);
                    gate.release();
                    if matches!(
                        case,
                        CompleteCreditCase::Unfinished | CompleteCreditCase::BufferTimeout
                    ) {
                        stage.set("waiting for unfinished credit-blocked prefix");
                        complete_credit_wait(|| {
                            handle.http2_stream_send_credit(stream).unwrap() == 0
                                && complete_credit_wire(upstream, &wire).frames.body_bytes == 4
                        })
                        .await;
                        let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                        assert_eq!(owners.egress_complete, Some(false));
                        assert_eq!((owners.queued, owners.complete), (1, 0));
                        if !timeout_case {
                            assert!(!ended.get());
                            let ping = handle.send_ping().unwrap();
                            complete_credit_wait(|| {
                                complete_credit_wire(upstream, &wire)
                                    .frames
                                    .ping_requests
                                    .contains(&ping)
                            })
                            .await;
                            assert_eq!(complete_credit_wire(upstream, &wire).frames.body_bytes, 4);
                            let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                            assert_eq!((owners.queued, owners.complete), (1, 0));
                            release_credit.notify_one();
                        }
                    }
                    if timeout_case {
                        stage.set("waiting for real buffer-timeout owner retirement");
                        complete_credit_wait(|| {
                            complete_credit_wire(upstream, &wire).frames.resets
                                == [(stream, H2ErrorCode::INTERNAL_ERROR)]
                        })
                        .await;
                        let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                        assert_eq!((owners.queued, owners.complete), (0, 0));
                        assert_eq!(
                            probe.stops.borrow().as_slice(),
                            [Some(HttpErrorCode::READ_TIMEOUT)]
                        );
                        assert_eq!(probe.source_drops.get(), 1);
                        assert_eq!(probe.reads_dropped.get(), 1);
                        assert!(
                            response
                                .as_ref()
                                .unwrap()
                                .completion
                                .wait()
                                .now_or_never()
                                .is_none()
                        );
                    } else {
                        stage.set("waiting for RequestEnd");
                        complete_credit_wait(|| ended.get()).await;
                        if case == CompleteCreditCase::Paused {
                            stage.set("observing paused owner across control progress");
                            let ping = handle.send_ping().unwrap();
                            complete_credit_wait(|| {
                                complete_credit_wire(upstream, &wire)
                                    .frames
                                    .ping_requests
                                    .contains(&ping)
                            })
                            .await;
                            let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                            assert_eq!((owners.queued, owners.complete), (1, 1));
                            assert_eq!(handle.http2_stream_send_credit(stream).unwrap(), 0);
                            assert!(
                                handle
                                    .update_http2_priority(
                                        stream,
                                        HttpPriority::from_urgency_and_incremental(3, true),
                                    )
                                    .unwrap()
                            );
                        }
                        stage.set("waiting for complete owner retirement without credit");
                        complete_credit_wait(|| {
                            handle.h2_egress_owners_for_test(stream).unwrap().queued == 0
                        })
                        .await;
                        let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                        assert_eq!(owners.egress_complete, Some(true));
                        assert_eq!((owners.queued, owners.complete), (0, 0));
                        assert_eq!(handle.http2_stream_send_credit(stream).unwrap(), 0);
                        stage.set("waiting for terminal frame on wire");
                        complete_credit_wait(|| {
                            complete_credit_wire(upstream, &wire).completed == [stream]
                        })
                        .await;
                        assert_eq!(
                            complete_credit_wire(upstream, &wire).frames.body_data,
                            payload
                        );
                        assert_eq!(probe.body_reads.get(), 1);
                        assert_eq!(probe.source_drops.get(), 1);
                        if let Some(response) = &response {
                            assert!(response.completion.wait().now_or_never().is_none());
                        } else {
                            assert!(held_request.borrow().is_some());
                        }
                    }
                    release_peer.notify_one();
                    stage.set("retiring held ingress and joining exchange");
                    if let Some(mut response) = response.take() {
                        let completion = response.completion.clone();
                        if timeout_case {
                            let error = match response.source.read_header_event().await {
                                Err(error) => error,
                                Ok(_) => panic!("buffer timeout produced response headers"),
                            };
                            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                        } else {
                            let mut headers = response.source.read_header_event().await.unwrap();
                            assert!(headers.eom());
                            assert_eq!(headers.headers_mut().status_code().unwrap(), 200);
                        }
                        drop(response);
                        completion.wait().await.unwrap();
                    } else {
                        let mut request = held_request.borrow_mut().take().unwrap();
                        let mut suspended = false;
                        loop {
                            let event = request.read_body_event(u32::MAX).await.unwrap();
                            let eom = event.eom();
                            match event.into_kind() {
                                HttpBodyEventKind::Suspend(resume) => {
                                    assert!(!eom);
                                    assert!(!suspended, "held request suspended more than once");
                                    suspended = true;
                                    assert_eq!(resume.await, ResumeStatus::Signalled);
                                }
                                HttpBodyEventKind::Body(body) => {
                                    assert_eq!(body.chain_length(), 0);
                                    assert!(eom);
                                    break;
                                }
                                _ => panic!("unexpected held request event before empty EOM"),
                            }
                        }
                        assert!(!request.readable());
                        drop(request);
                    }
                }))
                .await;
            let failure = matches!(&outcome, Ok(Err(_))).then(|| {
                format!(
                    "upstream={upstream}, case={case:?}, stage={}; \
                     owners={:?}, credit={:?}; body_reads={}, source_drops={}",
                    stage.get(),
                    handle.h2_egress_owners_for_test(stream),
                    handle.http2_stream_send_credit(stream),
                    probe.body_reads.get(),
                    probe.source_drops.get(),
                )
            });
            gate.release();
            release_credit.notify_one();
            release_peer.notify_one();
            drop(response.take());
            drop(held_request.borrow_mut().take());
            let _ = handle.initiate_drain();
            (outcome, failure)
        };
        let (run, (exercise, failure)) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(4), session.run()),
            exercise,
        );
        exercise
            .unwrap_or_else(|payload| resume_unwind(payload))
            .unwrap_or_else(|error| panic!("complete credit exercise: {error:?}; {failure:?}"));
        run.expect("complete credit session joins").unwrap();
        let decoded = complete_credit_wire(upstream, &wire);
        assert!(decoded.frames.errors.is_empty());
        assert_eq!(decoded.frames.header_streams, [stream]);
        if timeout_case {
            assert_eq!(
                decoded.frames.resets,
                [(stream, H2ErrorCode::INTERNAL_ERROR)]
            );
            assert_eq!(decoded.frames.body_data, vec![b'b'; 4]);
            assert!(decoded.completed.is_empty());
        } else {
            assert!(decoded.frames.resets.is_empty());
            assert_eq!(decoded.completed, [stream]);
            assert_eq!(decoded.frames.body_data, payload);
        }
        assert_eq!(probe.header_reads.get(), usize::from(!upstream));
        assert_eq!(probe.body_reads.get(), 1);
        assert_eq!(probe.reads_dropped.get(), 1 + usize::from(!upstream));
        assert_eq!(probe.cancelled_reads.get(), 0);
        assert_eq!(probe.source_drops.get(), 1);
        assert!(handle.is_finished());
        drop(observer);
        drop(handle);
        drop(transport);
        assert!(wire.borrow().transport_dropped);
    }

    #[test]
    fn complete_upstream_transfer_retires_without_more_stream_credit() {
        run_on_worker(|context| {
            Box::pin(complete_credit_case(
                context,
                true,
                CompleteCreditCase::Exact,
            ))
        });
    }

    #[test]
    fn complete_transfer_credit_exception_preserves_pause() {
        run_on_worker(|context| {
            Box::pin(complete_credit_case(
                context,
                true,
                CompleteCreditCase::Paused,
            ))
        });
    }

    #[test]
    fn unfinished_transfer_stays_blocked_then_retires_at_zero_credit() {
        run_on_worker(|context| {
            Box::pin(complete_credit_case(
                context,
                true,
                CompleteCreditCase::Unfinished,
            ))
        });
    }

    #[test]
    fn complete_downstream_transfer_retires_with_ingress_held() {
        run_on_worker(|context| {
            Box::pin(complete_credit_case(
                context,
                false,
                CompleteCreditCase::Exact,
            ))
        });
    }

    #[test]
    fn complete_transfer_credit_preserves_zero_window_buffer_timeout() {
        run_on_worker(|context| {
            Box::pin(complete_credit_case(
                context,
                true,
                CompleteCreditCase::BufferTimeout,
            ))
        });
    }
    struct UploadCompletion {
        events: Rc<RefCell<Vec<(StreamId, Option<HttpErrorCode>, Instant)>>>,
        drops: Rc<Cell<usize>>,
        processed: Rc<RefCell<Vec<usize>>>,
    }

    impl roxygen_core::stream_source::HttpStreamSourceCallback for UploadCompletion {
        fn bytes_processed(&mut self, _stream: StreamId, amount: usize, _acknowledge: usize) {
            self.processed.borrow_mut().push(amount);
        }

        fn source_complete(&mut self, stream: StreamId, error: Option<HttpError>) {
            let completed_at = Instant::now();
            self.events
                .borrow_mut()
                .push((stream, error.map(|error| error.code()), completed_at));
        }
    }

    impl Drop for UploadCompletion {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct HeldProgressSource {
        source: roxygen_core::stream_source::HttpStreamSource,
        panic_armed: Rc<Cell<bool>>,
        armed_polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl HttpSource for HeldProgressSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.source.read_header_event()
        }

        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            let mut read = self.source.read_body_event(maximum);
            let armed = Rc::clone(&self.panic_armed);
            let polls = Arc::clone(&self.armed_polls);
            Box::pin(poll_fn(move |context| {
                if armed.replace(false) {
                    polls.fetch_add(1, Ordering::SeqCst);
                    panic_any("held body source poll panic");
                }
                read.as_mut().poll(context)
            }))
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.source.stop_reading(error);
        }

        fn get_stream_id(&self) -> Option<StreamId> {
            self.source.get_stream_id()
        }

        fn ingress_completion(
            &self,
        ) -> Option<roxygen_core::ingress_completion::IngressCompletion> {
            self.source.ingress_completion()
        }

        fn set_read_timeout(&mut self, timeout: Duration) {
            self.source.set_read_timeout(timeout);
        }
    }

    impl Drop for HeldProgressSource {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct HeldBodyGates<const N: usize>([Arc<tokio::sync::Notify>; N]);

    impl<const N: usize> Drop for HeldBodyGates<N> {
        fn drop(&mut self) {
            for gate in &self.0 {
                gate.notify_one();
            }
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum HeldSourceCase {
        Progress,
        Panic,
        ReadyWriteError,
    }

    #[test]
    fn upstream_body_source_progresses_while_physical_data_write_is_pending() {
        held_body_source_progress_case(HeldSourceCase::Progress);
    }

    #[test]
    fn held_body_source_panic_retires_the_owned_write_and_joins() {
        held_body_source_progress_case(HeldSourceCase::Panic);
    }

    #[test]
    fn ready_held_write_error_precedes_new_body_source_panic() {
        held_body_source_progress_case(HeldSourceCase::ReadyWriteError);
    }

    fn held_body_source_progress_case(case: HeldSourceCase) {
        use roxygen_core::stream_source::FlowControlState;
        use roxygen_core::stream_source::HttpStreamSource;

        let panic_after_prefix = case == HeldSourceCase::Panic;
        let prefix_retired = Arc::new(AtomicUsize::new(0));
        let source_retired = Arc::new(AtomicUsize::new(0));
        let transport_retired = Arc::new(AtomicUsize::new(0));
        let armed_polls = Arc::new(AtomicUsize::new(0));
        let worker_slot = Arc::new(std::sync::Mutex::new(None));
        let observations = (
            Arc::clone(&prefix_retired),
            Arc::clone(&source_retired),
            Arc::clone(&transport_retired),
            Arc::clone(&armed_polls),
            Arc::clone(&worker_slot),
        );
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            run_on_worker(move |context| {
                *worker_slot.lock().unwrap() = Some(context.handle().clone());
                Box::pin(async move {
                    let stream = StreamId::new(1);
                    let prefix = b"held DATA";
                    let suffix = b" plus late DATA";
                    let expected = [prefix.as_slice(), suffix.as_slice()].concat();
                    let prefix_drops = prefix_retired;
                    let completed = Rc::new(RefCell::new(Vec::new()));
                    let source_drops = Rc::new(Cell::new(0));
                    let processed = Rc::new(RefCell::new(Vec::new()));
                    let panic_armed = Rc::new(Cell::new(false));
                    let mut upload = HttpStreamSource::new_session(
                        stream,
                        UploadCompletion {
                            events: Rc::clone(&completed),
                            drops: Rc::clone(&source_drops),
                            processed: Rc::clone(&processed),
                        },
                        65_535,
                        Duration::ZERO,
                    )
                    .unwrap();
                    let producer = upload.producer();
                    let mut head =
                        committed_test_headers(b"/held-body-write", false).into_headers();
                    head.headers_mut().set(
                        HttpHeaderCode::ContentLength,
                        expected.len().to_string().as_bytes(),
                    );
                    assert!(producer.headers(head, false));
                    let header = upload.read_header_event().await.unwrap();
                    assert!(!header.eom());

                    let mut peer = Http2Codec::new(TransportDirection::Downstream);
                    let mut settings = BufQueue::new(4096);
                    peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                        .unwrap();
                    let mut initial = settings.coalesce(settings.chain_length()).unwrap().to_vec();
                    initial.extend(eligibility_frame(&mut peer, EgressFrame::SettingsAck));
                    let mut message = HttpMessage::new();
                    message.set_status_code(200).unwrap();
                    message.set_http_version(1, 1);
                    let release_response = Arc::new(tokio::sync::Notify::new());
                    let finish_input = Arc::new(tokio::sync::Notify::new());
                    let release_write = Arc::new(tokio::sync::Notify::new());
                    let (transport, wire) = FixtureTransport::new([
                        FixtureRead::Data(initial),
                        FixtureRead::WaitData(
                            Arc::clone(&release_response),
                            eligibility_frame(
                                &mut peer,
                                EgressFrame::Headers {
                                    stream,
                                    message: &message,
                                    end_stream: true,
                                    extra_headers: None,
                                },
                            ),
                        ),
                        FixtureRead::WaitDataEof(Arc::clone(&finish_input), Vec::new()),
                    ]);
                    wire.borrow_mut().drop_receipt = Some(transport_retired);
                    let (session, handle) = UniplexSession::new_http2(
                        context,
                        TransportDirection::Upstream,
                        transport.clone(),
                        None,
                        UniplexConfig {
                            read_timeout: Duration::ZERO,
                            stream_read_timeout: Duration::ZERO,
                            ..UniplexConfig::default()
                        },
                        Http2SessionConfig {
                            double_goaway_drain: false,
                            ..Http2SessionConfig::default()
                        },
                    )
                    .unwrap();
                    let _release_on_drop = HeldBodyGates([
                        Arc::clone(&release_write),
                        Arc::clone(&release_response),
                        Arc::clone(&finish_input),
                    ]);
                    let mut response: Option<UniplexResponse> = None;
                    let mut progress_before_release = None;
                    let exercise = async {
                        let outcome = eligibility_catch_unwind(tokio::time::timeout(
                            Duration::from_secs(5),
                            async {
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire).frames.settings_acks == 1
                                })
                                .await;
                                response = Some(
                                    handle
                                        .send_prepared_request(
                                            UniplexHandle::prepare_request_with_headers(
                                                header,
                                                HttpSourceHolder::new(Box::new(
                                                    HeldProgressSource {
                                                        source: upload,
                                                        panic_armed: Rc::clone(&panic_armed),
                                                        armed_polls: Arc::clone(&armed_polls),
                                                        drops: source_retired,
                                                    },
                                                )),
                                                handle.reserve_request().unwrap(),
                                            ),
                                        )
                                        .unwrap(),
                                );
                                assert_eq!(response.as_ref().unwrap().stream, stream);
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire).frames.header_streams
                                        == [stream]
                                })
                                .await;
                                let before_write = wire.borrow().writes.clone();
                                assert_eq!(complete_credit_wire(true, &wire).frames.body_bytes, 0);
                                assert!(processed.borrow().is_empty());
                                wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                                let bytes = Bytes::from_owner(DropTrackedBytes {
                                    bytes: prefix.to_vec(),
                                    drops: Arc::clone(&prefix_drops),
                                });
                                let mut first = BufQueue::new(prefix.len());
                                first.push_bytes(bytes).unwrap();
                                assert_eq!(producer.body(first, 0, false), FlowControlState::Open);
                                complete_credit_wait(|| wire.borrow().write_gate_pending.is_some())
                                    .await;
                                assert_eq!(
                                    wire.borrow().write_gate_pending,
                                    Some(FRAME_HEADER_SIZE + prefix.len())
                                );
                                assert!(wire.borrow().write_gate.is_none());
                                assert_eq!(*processed.borrow(), [prefix.len()]);
                                assert!(completed.borrow().is_empty());
                                assert_eq!(source_drops.get(), 0);
                                assert_eq!(prefix_drops.load(Ordering::SeqCst), 0);
                                assert_eq!(wire.borrow().writes, before_write);

                                panic_armed.set(case != HeldSourceCase::Progress);
                                assert_eq!(
                                    producer.body(queue(suffix), 0, true),
                                    FlowControlState::Open
                                );
                                if case == HeldSourceCase::ReadyWriteError {
                                    // The input/response remain gated. With no await between
                                    // these publications, both the source and this held write
                                    // become ready before the session can run again.
                                    wire.borrow_mut().write_error =
                                        Some(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                                    release_write.notify_one();
                                    complete_credit_wait(|| {
                                        wire.borrow().write_error.is_none() || !panic_armed.get()
                                    })
                                    .await;
                                    return;
                                }
                                let progress = tokio::time::timeout(
                                    Duration::from_secs(1),
                                    complete_credit_wait(|| {
                                        if panic_after_prefix {
                                            armed_polls.load(Ordering::SeqCst) == 1
                                        } else {
                                            processed.borrow().iter().sum::<usize>()
                                                == expected.len()
                                                && source_drops.get() == 1
                                        }
                                    }),
                                )
                                .await;
                                progress_before_release = Some((
                                    progress.is_ok(),
                                    processed.borrow().clone(),
                                    completed
                                        .borrow()
                                        .iter()
                                        .map(|(id, error, _)| (*id, *error))
                                        .collect::<Vec<_>>(),
                                    source_drops.get(),
                                ));
                                if panic_after_prefix {
                                    assert!(progress.is_ok(), "the held Body poll was reached");
                                    assert_eq!(wire.borrow().writes, before_write);
                                    return;
                                }
                                assert_eq!(prefix_drops.load(Ordering::SeqCst), 0);
                                assert_eq!(wire.borrow().writes, before_write);
                                let held = complete_credit_wire(true, &wire);
                                assert_eq!(held.frames.body_bytes, 0);
                                assert!(held.completed.is_empty());
                                assert!(held.frames.resets.is_empty());
                                assert!(
                                    response
                                        .as_ref()
                                        .unwrap()
                                        .completion
                                        .wait()
                                        .now_or_never()
                                        .is_none()
                                );

                                release_write.notify_one();
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire).completed == [stream]
                                })
                                .await;
                                let decoded = complete_credit_wire(true, &wire);
                                assert_eq!(decoded.frames.body_data, expected);
                                assert_eq!(decoded.frames.header_streams, [stream]);
                                assert!(decoded.frames.resets.is_empty());
                                assert!(decoded.frames.goaways.is_empty());
                                assert!(decoded.frames.errors.is_empty());
                                release_response.notify_one();
                                let mut healthy = response.take().unwrap();
                                let completion = healthy.completion.clone();
                                let mut header = healthy.source.read_header_event().await.unwrap();
                                assert!(header.eom());
                                assert_eq!(header.headers_mut().status_code().unwrap(), 200);
                                drop(header);
                                drop(healthy);
                                completion.wait().await.unwrap();
                            },
                        ))
                        .await;
                        release_write.notify_one();
                        release_response.notify_one();
                        finish_input.notify_one();
                        drop(response.take());
                        let _ = handle.initiate_drain();
                        outcome
                    };
                    let (run, exercise) =
                        tokio::join!(eligibility_catch_unwind(session.run()), exercise);
                    let finished = handle.is_finished();
                    drop(handle);
                    drop(producer);
                    drop(transport);
                    if let Err(payload) = exercise {
                        resume_unwind(payload);
                    }
                    if case == HeldSourceCase::ReadyWriteError {
                        exercise
                            .unwrap()
                            .expect("bounded ready-write/source-panic race");
                        let error = run
                            .expect("the ready native write must prevent the source panic")
                            .expect_err("the held physical write reports its original error");
                        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                        assert_eq!(error.message(), b"scripted H2 write error");
                        assert!(wire.borrow().write_error.is_none());
                        assert!(panic_armed.get(), "the newly ready source was not polled");
                        assert_eq!(armed_polls.load(Ordering::SeqCst), 0);
                        assert_eq!(processed.borrow().iter().sum::<usize>(), expected.len());
                        assert!(finished);
                        assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
                        assert_eq!(source_drops.get(), 1);
                        assert_eq!(wire.borrow().close, FixtureClose::Reset);
                        assert!(wire.borrow().transport_dropped);
                        return;
                    }
                    if panic_after_prefix {
                        panic!("the armed source poll did not reach the worker panic boundary");
                    }
                    if let Err(payload) = run {
                        resume_unwind(payload);
                    }
                    exercise.unwrap().expect("bounded held-DATA write fixture");
                    run.unwrap().expect("held write and exchange join cleanly");
                    assert!(finished);
                    assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
                    assert_eq!(*processed.borrow(), [prefix.len(), suffix.len()]);
                    assert_eq!(completed.borrow().len(), 1);
                    assert_eq!(completed.borrow()[0].1, None);
                    assert_eq!(source_drops.get(), 1);
                    assert_eq!(wire.borrow().close, FixtureClose::ShutdownWrite);
                    assert!(wire.borrow().transport_dropped);
                    let (progress, amounts, events, drops) = progress_before_release.unwrap();
                    assert!(
                        progress,
                        "source DATA/EOM did not retire while the physical write was held: amounts={amounts:?}, events={events:?}, drops={drops}"
                    );
                    assert_eq!(amounts, [prefix.len(), suffix.len()]);
                    assert_eq!(events, [(stream, None)]);
                    assert_eq!(drops, 1);
                })
            })
        }));
        if case == HeldSourceCase::Panic {
            let payload = outcome.expect_err("the source panic reaches the joined worker boundary");
            assert_eq!(
                payload.downcast_ref::<&str>().copied(),
                Some("held body source poll panic")
            );
            assert_eq!(observations.0.load(Ordering::SeqCst), 1);
            assert_eq!(observations.1.load(Ordering::SeqCst), 1);
            assert_eq!(observations.2.load(Ordering::SeqCst), 1);
            assert_eq!(observations.3.load(Ordering::SeqCst), 1);
            assert_eq!(
                observations.4.lock().unwrap().as_ref().unwrap().state(),
                roxygen_runtime::worker::WorkerState::Closed
            );
        } else if let Err(payload) = outcome {
            resume_unwind(payload);
        }
    }

    #[test]
    fn held_write_body_sources_yield_and_cancel_without_replacing_the_write() {
        use roxygen_core::stream_source::FlowControlState;
        use roxygen_core::stream_source::HttpStreamSource;

        run_on_worker(|context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let sibling_stream = StreamId::new(3);
                let processed = Rc::new(RefCell::new(Vec::new()));
                let completed = Rc::new(RefCell::new(Vec::new()));
                let source_drops = Rc::new(Cell::new(0));
                let backing_drops = Arc::new(AtomicUsize::new(0));
                let mut upload = HttpStreamSource::new_session(
                    stream,
                    UploadCompletion {
                        events: Rc::clone(&completed),
                        drops: Rc::clone(&source_drops),
                        processed: Rc::clone(&processed),
                    },
                    65_535,
                    Duration::ZERO,
                )
                .unwrap();
                let producer = upload.producer();
                assert!(producer.headers(
                    committed_test_headers(b"/cancel-held", false).into_headers(),
                    false
                ));
                let header = upload.read_header_event().await.unwrap();
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let mut settings = BufQueue::new(4096);
                peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                    .unwrap();
                let mut initial = settings.coalesce(settings.chain_length()).unwrap().to_vec();
                initial.extend(eligibility_frame(&mut peer, EgressFrame::SettingsAck));
                let reply = Arc::new(tokio::sync::Notify::new());
                let finish = Arc::new(tokio::sync::Notify::new());
                let release = Arc::new(tokio::sync::Notify::new());
                let mut response_head = HttpMessage::new();
                response_head.set_status_code(200).unwrap();
                response_head.set_http_version(1, 1);
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(
                        Arc::clone(&reply),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::Headers {
                                stream: sibling_stream,
                                message: &response_head,
                                end_stream: true,
                                extra_headers: None,
                            },
                        ),
                    ),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig {
                        read_timeout: Duration::ZERO,
                        stream_read_timeout: Duration::ZERO,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        ..Http2SessionConfig::default()
                    },
                )
                .unwrap();
                let cancellation = CancellationSource::new();
                let probe = Rc::new(EligibilityProbe::default());
                let mut response: Option<UniplexResponse> = None;
                let mut sibling: Option<UniplexResponse> = None;
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(5),
                        async {
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).frames.settings_acks == 1
                            })
                            .await;
                            response = Some(
                                handle
                                    .send_prepared_request_with_cancellation(
                                        UniplexHandle::prepare_request_with_headers(
                                            header,
                                            HttpSourceHolder::new(Box::new(upload)),
                                            handle.reserve_request().unwrap(),
                                        ),
                                        cancellation.token(),
                                    )
                                    .unwrap(),
                            );
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).frames.header_streams == [stream]
                            })
                            .await;
                            let before = wire.borrow().writes.clone();
                            wire.borrow_mut().write_gate = Some(Arc::clone(&release));
                            let prefix = Bytes::from_owner(DropTrackedBytes {
                                bytes: b"p".to_vec(),
                                drops: Arc::clone(&backing_drops),
                            });
                            assert_eq!(
                                producer.body(BufQueue::from_bytes(prefix, 1).unwrap(), 0, false),
                                FlowControlState::Open
                            );
                            complete_credit_wait(|| wire.borrow().write_gate_pending.is_some())
                                .await;
                            assert_eq!(*processed.borrow(), [1]);
                            let gate = EligibilityGate::new(
                                EligibilityPhase::Headers,
                                Arc::new(tokio::sync::Notify::new()),
                            );
                            gate.release();
                            let mut source = EligibilitySource::new(
                                EligibilityPhase::Headers,
                                gate,
                                Rc::clone(&probe),
                                Bytes::from_static(b"s"),
                                true,
                            );
                            for _ in 0..140 {
                                source.events.push_front(HttpBodyEvent::suspend(Box::pin(
                                    std::future::ready(
                                        roxygen_core::events::ResumeStatus::Signalled,
                                    ),
                                )));
                            }
                            let header = source.header.take().unwrap();
                            sibling = Some(
                                handle
                                    .send_prepared_request(
                                        UniplexHandle::prepare_request_with_headers(
                                            header,
                                            HttpSourceHolder::new(Box::new(source)),
                                            handle.reserve_request().unwrap(),
                                        ),
                                    )
                                    .unwrap(),
                            );
                            assert_eq!(sibling.as_ref().unwrap().stream, sibling_stream);
                            complete_credit_wait(|| probe.body_reads.get() != 0).await;
                            assert!(
                                probe.body_reads.get() < 140,
                                "ready resumes must yield before exhausting the finite script"
                            );
                            assert_eq!(probe.source_drops.get(), 0);
                            cancellation.request_cancellation();
                            complete_credit_wait(|| {
                                source_drops.get() == 1 && probe.source_drops.get() == 1
                            })
                            .await;
                            assert_eq!(completed.borrow().len(), 1);
                            assert_eq!(completed.borrow()[0].1, Some(HttpErrorCode::CANCEL));
                            assert_eq!(probe.body_reads.get(), 143);
                            assert_eq!(probe.reads_dropped.get(), 143);
                            assert_eq!(probe.cancelled_reads.get(), 0);
                            assert_eq!(wire.borrow().writes, before);
                            assert_eq!(backing_drops.load(Ordering::SeqCst), 0);
                            release.notify_one();
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire)
                                    .completed
                                    .contains(&sibling_stream)
                            })
                            .await;
                            let mut failed = response.take().unwrap();
                            let completion = failed.completion.clone();
                            let Err(error) = failed.source.read_header_event().await else {
                                panic!("cancelled upload response")
                            };
                            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                            drop(failed);
                            completion.wait().await.unwrap();
                            reply.notify_one();
                            let mut healthy = sibling.take().unwrap();
                            let completion = healthy.completion.clone();
                            assert!(healthy.source.read_header_event().await.unwrap().eom());
                            drop(healthy);
                            completion.wait().await.unwrap();
                            let decoded = complete_credit_wire(true, &wire);
                            assert_eq!(
                                decoded.frames.resets,
                                [(stream, H2ErrorCode::INTERNAL_ERROR)]
                            );
                            assert!(decoded.frames.goaways.is_empty());
                            assert_eq!(decoded.frames.body_data, b"ps");
                        },
                    ))
                    .await;
                    release.notify_one();
                    reply.notify_one();
                    finish.notify_one();
                    drop(response.take());
                    drop(sibling.take());
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, exercise) =
                    tokio::join!(eligibility_catch_unwind(session.run()), exercise);
                let finished = handle.is_finished();
                drop(handle);
                drop(producer);
                drop(transport);
                if let Err(payload) = exercise {
                    resume_unwind(payload);
                }
                if let Err(payload) = run {
                    resume_unwind(payload);
                }
                exercise
                    .unwrap()
                    .expect("finite held-write fairness and cancellation");
                run.unwrap()
                    .expect("cancelled source leaves sibling and held write joined");
                assert!(finished);
                assert_eq!(source_drops.get(), 1);
                assert_eq!(probe.source_drops.get(), 1);
                assert_eq!(backing_drops.load(Ordering::SeqCst), 1);
                assert!(wire.borrow().transport_dropped);
            })
        });
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum HeldCapacityCase {
        PositiveCredit,
        StreamBlocked,
        ConnectionBlocked,
        Completed,
    }

    #[test]
    fn held_physical_write_preserves_source_capacity_deadline_and_completed_eom() {
        for case in [
            HeldCapacityCase::PositiveCredit,
            HeldCapacityCase::StreamBlocked,
            HeldCapacityCase::ConnectionBlocked,
            HeldCapacityCase::Completed,
        ] {
            held_write_capacity_case(case);
        }
    }

    fn held_write_capacity_case(case: HeldCapacityCase) {
        use roxygen_core::stream_source::FlowControlState;
        use roxygen_core::stream_source::HttpStreamSource;

        run_on_worker(move |context| {
            Box::pin(async move {
                const K: usize = 65_535;
                let stream = StreamId::new(1);
                let duration = Duration::from_millis(250);
                let prefix_len = if case == HeldCapacityCase::ConnectionBlocked {
                    K
                } else {
                    1
                };
                let terminal = case == HeldCapacityCase::Completed;
                let prefix_drops = Arc::new(AtomicUsize::new(0));
                let completed = Rc::new(RefCell::new(Vec::new()));
                let source_drops = Rc::new(Cell::new(0));
                let processed = Rc::new(RefCell::new(Vec::new()));
                let mut upload = HttpStreamSource::new_session(
                    stream,
                    UploadCompletion {
                        events: Rc::clone(&completed),
                        drops: Rc::clone(&source_drops),
                        processed: Rc::clone(&processed),
                    },
                    3 * K as u32,
                    Duration::ZERO,
                )
                .unwrap();
                let producer = upload.producer();
                let mut head = committed_test_headers(b"/held-capacity", false).into_headers();
                head.headers_mut().set(
                    HttpHeaderCode::ContentLength,
                    (prefix_len + if terminal { K } else { 2 * K })
                        .to_string()
                        .as_bytes(),
                );
                assert!(producer.headers(head, false));
                let header = upload.read_header_event().await.unwrap();
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                peer.egress_settings_mut().set(
                    SettingsId::InitialWindowSize,
                    if case == HeldCapacityCase::StreamBlocked {
                        1
                    } else {
                        2 * K as u64
                    },
                );
                let mut settings = BufQueue::new(4096);
                peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                    .unwrap();
                let mut initial = settings.coalesce(settings.chain_length()).unwrap().to_vec();
                initial.extend(eligibility_frame(&mut peer, EgressFrame::SettingsAck));
                let credit = Arc::new(tokio::sync::Notify::new());
                let reply = Arc::new(tokio::sync::Notify::new());
                let finish = Arc::new(tokio::sync::Notify::new());
                let release = Arc::new(tokio::sync::Notify::new());
                let mut reads = vec![FixtureRead::Data(initial)];
                if terminal {
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&credit),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::WindowUpdate {
                                stream: StreamId::UNSPECIFIED,
                                amount: 1,
                            },
                        ),
                    ));
                    let mut response = HttpMessage::new();
                    response.set_status_code(200).unwrap();
                    response.set_http_version(1, 1);
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&reply),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::Headers {
                                stream,
                                message: &response,
                                end_stream: true,
                                extra_headers: None,
                            },
                        ),
                    ));
                }
                reads.push(FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()));
                let (transport, wire) = FixtureTransport::new(reads);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig {
                        maximum_write_size: 256 * 1024,
                        write_timeout: duration,
                        read_timeout: Duration::ZERO,
                        stream_read_timeout: Duration::ZERO,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        ..Http2SessionConfig::default()
                    },
                )
                .unwrap();
                let mut response: Option<UniplexResponse> = None;
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(5),
                        async {
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).frames.settings_acks == 1
                            })
                            .await;
                            response = Some(
                                handle
                                    .send_prepared_request(
                                        UniplexHandle::prepare_request_with_headers(
                                            header,
                                            HttpSourceHolder::new(Box::new(upload)),
                                            handle.reserve_request().unwrap(),
                                        ),
                                    )
                                    .unwrap(),
                            );
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).frames.header_streams == [stream]
                            })
                            .await;
                            let before = wire.borrow().writes.clone();
                            wire.borrow_mut().write_gate = Some(Arc::clone(&release));
                            let prefix = Bytes::from_owner(DropTrackedBytes {
                                bytes: vec![b'p'; prefix_len],
                                drops: Arc::clone(&prefix_drops),
                            });
                            for offset in (0..prefix_len).step_by(16 * 1024) {
                                assert_eq!(
                                    producer.body(
                                        BufQueue::from_bytes(
                                            prefix.slice(
                                                offset..(offset + 16 * 1024).min(prefix_len)
                                            ),
                                            16 * 1024
                                        )
                                        .unwrap(),
                                        0,
                                        false
                                    ),
                                    FlowControlState::Open
                                );
                            }
                            drop(prefix);
                            complete_credit_wait(|| wire.borrow().write_gate_pending.is_some())
                                .await;
                            assert_eq!(processed.borrow().iter().sum::<usize>(), prefix_len);
                            assert_eq!(prefix_drops.load(Ordering::SeqCst), 0);
                            let expected_stream_credit = if case == HeldCapacityCase::StreamBlocked
                            {
                                0
                            } else {
                                (2 * K - prefix_len) as u32
                            };
                            assert_eq!(
                                handle.http2_stream_send_credit(stream).unwrap(),
                                expected_stream_credit
                            );
                            let supplied_at = Instant::now();
                            let suffix = vec![b's'; K];
                            for (index, chunk) in suffix.chunks(16 * 1024).enumerate() {
                                assert_eq!(
                                    producer.body(
                                        queue(chunk),
                                        0,
                                        terminal && (index + 1) * 16 * 1024 >= K
                                    ),
                                    FlowControlState::Open
                                );
                            }
                            complete_credit_wait(|| {
                                processed.borrow().iter().sum::<usize>() == prefix_len + K
                            })
                            .await;
                            if terminal {
                                complete_credit_wait(|| source_drops.get() == 1).await;
                                tokio::time::sleep(duration * 2).await;
                                assert_eq!(completed.borrow().len(), 1);
                                assert_eq!(completed.borrow()[0].1, None);
                                assert_eq!(
                                    handle.h2_egress_owners_for_test(stream).unwrap().queued,
                                    1
                                );
                            } else {
                                complete_credit_wait(|| {
                                    source_drops.get() == 1
                                        && handle.h2_egress_owners_for_test(stream).unwrap().queued
                                            == 0
                                })
                                .await;
                                let events = completed.borrow();
                                assert_eq!(events.len(), 1);
                                assert_eq!(events[0].1, Some(HttpErrorCode::READ_TIMEOUT));
                                assert!(events[0].2 >= supplied_at + duration);
                            }
                            assert_eq!(prefix_drops.load(Ordering::SeqCst), 0);
                            assert_eq!(wire.borrow().writes, before);
                            let held = complete_credit_wire(true, &wire);
                            assert_eq!(held.frames.body_bytes, 0);
                            assert!(held.frames.resets.is_empty());
                            assert!(held.completed.is_empty());
                            assert!(
                                response
                                    .as_ref()
                                    .unwrap()
                                    .completion
                                    .wait()
                                    .now_or_never()
                                    .is_none()
                            );
                            release.notify_one();
                            if terminal {
                                credit.notify_one();
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire).completed == [stream]
                                })
                                .await;
                                let mut expected = vec![b'p'; prefix_len];
                                expected.extend_from_slice(&suffix);
                                assert_eq!(
                                    complete_credit_wire(true, &wire).frames.body_data,
                                    expected
                                );
                                reply.notify_one();
                                let mut healthy = response.take().unwrap();
                                let completion = healthy.completion.clone();
                                assert!(healthy.source.read_header_event().await.unwrap().eom());
                                drop(healthy);
                                completion.wait().await.unwrap();
                            } else {
                                complete_credit_wait(|| {
                                    !complete_credit_wire(true, &wire).frames.resets.is_empty()
                                })
                                .await;
                                let decoded = complete_credit_wire(true, &wire);
                                assert_eq!(decoded.frames.body_data, vec![b'p'; prefix_len]);
                                assert_eq!(
                                    decoded.frames.resets,
                                    [(stream, H2ErrorCode::INTERNAL_ERROR)]
                                );
                                let mut failed = response.take().unwrap();
                                let completion = failed.completion.clone();
                                let Err(error) = failed.source.read_header_event().await else {
                                    panic!("capacity error must not produce response headers")
                                };
                                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                                drop(failed);
                                completion.wait().await.unwrap();
                            }
                            assert!(complete_credit_wire(true, &wire).frames.goaways.is_empty());
                        },
                    ))
                    .await;
                    release.notify_one();
                    credit.notify_one();
                    reply.notify_one();
                    finish.notify_one();
                    drop(response.take());
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, exercise) =
                    tokio::join!(eligibility_catch_unwind(session.run()), exercise);
                let finished = handle.is_finished();
                drop(handle);
                drop(producer);
                drop(transport);
                if let Err(payload) = exercise {
                    resume_unwind(payload);
                }
                if let Err(payload) = run {
                    resume_unwind(payload);
                }
                exercise
                    .unwrap()
                    .unwrap_or_else(|error| panic!("held capacity {case:?}: {error}"));
                run.unwrap()
                    .expect("stream-local source retirement joins while preserving held write");
                assert!(finished);
                assert_eq!(source_drops.get(), 1);
                assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
                assert!(wire.borrow().transport_dropped);
            })
        });
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum FullBufferCase {
        Prefilled,
        LateIngress,
        ExhaustedEmptyStream,
        CompletedFull,
        HiddenEom,
        DisabledTimeout,
    }

    #[test]
    fn upstream_full_buffer_timeout_survives_ping_progress() {
        upstream_full_buffer_case(FullBufferCase::Prefilled);
    }

    #[test]
    fn upstream_partial_buffer_refills_from_late_ingress() {
        upstream_full_buffer_case(FullBufferCase::LateIngress);
    }

    #[test]
    fn upstream_exhausted_empty_stream_prefetches_to_capacity() {
        upstream_full_buffer_case(FullBufferCase::ExhaustedEmptyStream);
    }

    #[test]
    fn upstream_completed_full_buffer_has_no_source_capacity_timeout() {
        upstream_full_buffer_case(FullBufferCase::CompletedFull);
    }

    #[test]
    fn upstream_eom_behind_full_buffer_does_not_end_production_early() {
        upstream_full_buffer_case(FullBufferCase::HiddenEom);
    }

    #[test]
    fn upstream_zero_write_timeout_keeps_full_buffer_alive() {
        upstream_full_buffer_case(FullBufferCase::DisabledTimeout);
    }

    fn upstream_full_buffer_case(case: FullBufferCase) {
        use roxygen_core::stream_source::FlowControlState;
        use roxygen_core::stream_source::HttpStreamSource;

        run_on_worker(move |context| {
            Box::pin(async move {
                const K: u32 = 65_535;
                let sibling_stream = StreamId::new(1);
                let stream = StreamId::new(3);
                let duration = Duration::from_secs(5);
                let timeout_disabled = case == FullBufferCase::DisabledTimeout;
                let completes = case == FullBufferCase::CompletedFull || timeout_disabled;
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let initial_window = if case == FullBufferCase::ExhaustedEmptyStream {
                    1
                } else {
                    2 * K
                };
                peer.egress_settings_mut()
                    .set(SettingsId::InitialWindowSize, u64::from(initial_window));
                let mut settings = BufQueue::new(4096);
                peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                    .unwrap();
                let mut initial = settings.coalesce(settings.chain_length()).unwrap().to_vec();
                initial.extend(eligibility_frame(&mut peer, EgressFrame::SettingsAck));
                let ping_count = 6;
                let ping_gates: Vec<_> = (0..ping_count)
                    .map(|_| Arc::new(tokio::sync::Notify::new()))
                    .collect();
                let admit_credit = Arc::new(tokio::sync::Notify::new());
                let restore_connection = (case == FullBufferCase::ExhaustedEmptyStream)
                    .then(|| Arc::new(tokio::sync::Notify::new()));
                let mut reads = vec![FixtureRead::Data(initial)];
                if let Some(gate) = &restore_connection {
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(gate),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::WindowUpdate {
                                stream: StreamId::UNSPECIFIED,
                                amount: 1,
                            },
                        ),
                    ));
                }
                for (index, gate) in ping_gates.iter().enumerate() {
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(gate),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::PingRequest {
                                data: Some(index as u64 + 1),
                            },
                        ),
                    ));
                }
                if completes {
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&admit_credit),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::WindowUpdate {
                                stream: StreamId::UNSPECIFIED,
                                // A separate late EOM still needs positive connection credit.
                                amount: K + u32::from(timeout_disabled),
                            },
                        ),
                    ));
                }
                let release_target = Arc::new(tokio::sync::Notify::new());
                let release_sibling = Arc::new(tokio::sync::Notify::new());
                let finish_input = Arc::new(tokio::sync::Notify::new());
                let mut message = HttpMessage::new();
                message.set_status_code(200).unwrap();
                message.set_http_version(1, 1);
                if completes {
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&release_target),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::Headers {
                                stream,
                                message: &message,
                                end_stream: true,
                                extra_headers: None,
                            },
                        ),
                    ));
                }
                reads.push(FixtureRead::WaitData(
                    Arc::clone(&release_sibling),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::Headers {
                            stream: sibling_stream,
                            message: &message,
                            end_stream: true,
                            extra_headers: None,
                        },
                    ),
                ));
                reads.push(FixtureRead::WaitDataEof(
                    Arc::clone(&finish_input),
                    Vec::new(),
                ));
                let (transport, wire) = FixtureTransport::new(reads);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig {
                        write_timeout: if timeout_disabled {
                            Duration::ZERO
                        } else {
                            duration
                        },
                        read_timeout: Duration::ZERO,
                        stream_read_timeout: Duration::ZERO,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        ..Http2SessionConfig::default()
                    },
                )
                .unwrap();
                let control = handle.observe_h2_source_timers_for_test(stream).unwrap();
                let completed = Rc::new(RefCell::new(Vec::new()));
                let source_drops = Rc::new(Cell::new(0));
                let processed = Rc::new(RefCell::new(Vec::new()));
                let mut response: Option<UniplexResponse> = None;
                let mut sibling: Option<UniplexResponse> = None;
                let stage = Cell::new("settings acknowledgement");
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(30),
                        async {
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).frames.settings_acks == 1
                            })
                            .await;
                            let mut upload = HttpStreamSource::new_session(
                                stream,
                                UploadCompletion {
                                    events: Rc::clone(&completed),
                                    drops: Rc::clone(&source_drops),
                                    processed: Rc::clone(&processed),
                                },
                                3 * K,
                                Duration::ZERO,
                            )
                            .unwrap();
                            let producer = upload.producer();
                            let mut head =
                                committed_test_headers(b"/full-buffer", false).into_headers();
                            head.headers_mut().set(
                                HttpHeaderCode::ContentLength,
                                match case {
                                    FullBufferCase::CompletedFull
                                    | FullBufferCase::DisabledTimeout => 2 * K,
                                    FullBufferCase::HiddenEom => 2 * K + 1,
                                    _ => 3 * K,
                                }
                                .to_string()
                                .as_bytes(),
                            );
                            assert!(producer.headers(head, false));
                            let initial_bytes = match case {
                                FullBufferCase::Prefilled
                                | FullBufferCase::CompletedFull
                                | FullBufferCase::DisabledTimeout => 2 * K,
                                FullBufferCase::LateIngress => K + 1,
                                FullBufferCase::ExhaustedEmptyStream => 0,
                                FullBufferCase::HiddenEom => 2 * K + 1,
                            };
                            let payload = vec![b'u'; initial_bytes as usize];
                            for (index, chunk) in payload.chunks(16 * 1024).enumerate() {
                                let eom = case == FullBufferCase::CompletedFull
                                    && (index + 1) * 16 * 1024 >= payload.len();
                                assert_eq!(
                                    producer.body(queue(chunk), 0, eom),
                                    FlowControlState::Open
                                );
                            }
                            let header = upload.read_header_event().await.unwrap();
                            assert!(!header.eom());
                            assert_eq!(
                                upload.input_finished(),
                                case == FullBufferCase::CompletedFull
                            );
                            sibling = Some(
                                handle
                                    .send_prepared_request(
                                        UniplexHandle::prepare_request_with_headers(
                                            committed_test_headers(b"/sibling", true),
                                            HttpSourceHolder::default(),
                                            handle.reserve_request().unwrap(),
                                        ),
                                    )
                                    .unwrap(),
                            );
                            response = Some(
                                handle
                                    .send_prepared_request(
                                        UniplexHandle::prepare_request_with_headers(
                                            header,
                                            HttpSourceHolder::new(Box::new(upload)),
                                            handle.reserve_request().unwrap(),
                                        ),
                                    )
                                    .unwrap(),
                            );
                            assert_eq!(sibling.as_ref().unwrap().stream, sibling_stream);
                            assert_eq!(response.as_ref().unwrap().stream, stream);
                            let mut admitted_at = None;
                            if let Some(restore_connection) = &restore_connection {
                                stage
                                    .set("exhausting one byte of stream credit with empty staging");
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire)
                                        .frames
                                        .header_streams
                                        .contains(&stream)
                                })
                                .await;
                                assert!(processed.borrow().is_empty());
                                assert_eq!(
                                    producer.body(queue(b"u"), 0, false),
                                    FlowControlState::Open
                                );
                                let exhausted = eligibility_flow_snapshot(&control, |snapshot| {
                                    snapshot.connection_credit == K - 1
                                        && snapshot.stream_credit == 0
                                        && snapshot.flow_deadline.is_some()
                                })
                                .await;
                                assert_eq!(exhausted.blocked, Some((vec![stream], false)));
                                assert_eq!(exhausted.buffer_timeout, None);
                                assert_eq!(*processed.borrow(), vec![1]);
                                let decoded = complete_credit_wire(true, &wire);
                                assert_eq!(decoded.frames.body_data, vec![b'u']);
                                assert!(decoded.frames.resets.is_empty());
                                assert!(!decoded.completed.contains(&stream));
                                restore_connection.notify_one();
                                let restored = eligibility_flow_snapshot(&control, |snapshot| {
                                    snapshot.sequence > exhausted.sequence
                                        && snapshot.connection_credit == K
                                        && snapshot.stream_credit == 0
                                        && snapshot.flow_deadline.is_some()
                                })
                                .await;
                                assert_eq!(restored.buffer_timeout, None);
                                admitted_at = Some(Instant::now());
                                for chunk in vec![b'u'; 2 * K as usize].chunks(16 * 1024) {
                                    assert_eq!(
                                        producer.body(queue(chunk), 0, false),
                                        FlowControlState::Open
                                    );
                                }
                            }
                            if case == FullBufferCase::LateIngress {
                                stage.set("partially staged DATA before late ingress");
                                let partial = eligibility_flow_snapshot(&control, |snapshot| {
                                    snapshot.flow_deadline.is_some()
                                        && snapshot.blocked == Some((vec![stream], true))
                                })
                                .await;
                                assert_eq!(
                                    (partial.connection_credit, partial.stream_credit),
                                    (0, K)
                                );
                                assert_eq!(partial.buffer_timeout, None);
                                assert_eq!(*processed.borrow(), vec![K as usize, 1]);
                                assert_eq!(
                                    complete_credit_wire(true, &wire).frames.body_bytes,
                                    K as usize
                                );
                                admitted_at = Some(Instant::now());
                                for chunk in vec![b'u'; K as usize].chunks(16 * 1024) {
                                    assert_eq!(
                                        producer.body(queue(chunk), 0, false),
                                        FlowControlState::Open
                                    );
                                }
                            }
                            let connection_blocked = case != FullBufferCase::ExhaustedEmptyStream;
                            let (sent, credits) = if connection_blocked {
                                (K as usize, (0, K))
                            } else {
                                (1, (K, 0))
                            };
                            stage.set("full egress queue at send-credit exhaustion");
                            let first = eligibility_flow_snapshot(&control, |snapshot| {
                                (timeout_disabled || snapshot.flow_deadline.is_some())
                                    && snapshot.blocked == Some((vec![stream], connection_blocked))
                                    && processed.borrow().iter().sum::<usize>() - sent == K as usize
                            })
                            .await;
                            assert_eq!(first.blocked, Some((vec![stream], connection_blocked)));
                            assert_eq!((first.connection_credit, first.stream_credit), credits);
                            if timeout_disabled {
                                assert_eq!(first.flow_deadline, None);
                            }
                            let buffer_deadline = if completes {
                                assert_eq!(first.buffer_timeout, None);
                                None
                            } else {
                                let (buffer_stream, deadline) = first.buffer_timeout.unwrap();
                                assert_eq!(buffer_stream, stream);
                                if let Some(admitted_at) = admitted_at {
                                    assert!(deadline - duration >= admitted_at);
                                }
                                Some(deadline)
                            };
                            let expected_reads = match case {
                                FullBufferCase::Prefilled
                                | FullBufferCase::CompletedFull
                                | FullBufferCase::DisabledTimeout
                                | FullBufferCase::HiddenEom => vec![K as usize, K as usize],
                                FullBufferCase::LateIngress => vec![K as usize, 1, K as usize - 1],
                                FullBufferCase::ExhaustedEmptyStream => vec![1, K as usize - 1, 1],
                            };
                            assert_eq!(*processed.borrow(), expected_reads);
                            assert_eq!(processed.borrow().iter().sum::<usize>() - sent, K as usize);
                            let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                            assert_eq!((owners.queued, owners.complete), (1, 0));
                            assert_eq!(owners.egress_complete, Some(false));
                            let decoded = complete_credit_wire(true, &wire);
                            assert_eq!(decoded.frames.body_data, vec![b'u'; sent]);
                            assert!(decoded.frames.body_streams.iter().all(|id| *id == stream));
                            assert!(!decoded.completed.contains(&stream));
                            assert!(decoded.frames.resets.is_empty());
                            if case == FullBufferCase::CompletedFull {
                                let events = completed.borrow();
                                assert_eq!(events.len(), 1);
                                assert_eq!((events[0].0, events[0].1), (stream, None));
                                assert_eq!(source_drops.get(), 1);
                            } else {
                                assert!(completed.borrow().is_empty());
                            }
                            if case == FullBufferCase::HiddenEom {
                                assert!(producer.eom());
                            }
                            let epoch = if timeout_disabled {
                                tokio::time::Instant::now()
                            } else {
                                tokio::time::Instant::from_std(
                                    buffer_deadline.unwrap_or(first.flow_deadline.unwrap())
                                        - duration,
                                )
                            };
                            let mut latest = first;
                            stage.set("PING acknowledgements across retained buffer deadline");
                            for (index, gate) in ping_gates.iter().enumerate() {
                                let phase_index = index;
                                tokio::time::sleep_until(
                                    epoch + duration / 4 * (phase_index as u32 + 1),
                                )
                                .await;
                                gate.notify_one();
                                let ping = index as u64 + 1;
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire)
                                        .frames
                                        .ping_replies
                                        .contains(&ping)
                                })
                                .await;
                                if phase_index < 3 || completes {
                                    let next = eligibility_flow_snapshot(&control, |snapshot| {
                                        snapshot.sequence > latest.sequence
                                            && (timeout_disabled
                                                || snapshot.flow_deadline > latest.flow_deadline)
                                    })
                                    .await;
                                    assert_eq!(
                                        next.blocked,
                                        Some((vec![stream], connection_blocked))
                                    );
                                    assert_eq!(
                                        (next.connection_credit, next.stream_credit),
                                        credits
                                    );
                                    assert_eq!(
                                        next.buffer_timeout,
                                        buffer_deadline.map(|deadline| (stream, deadline))
                                    );
                                    if timeout_disabled {
                                        assert_eq!(next.flow_deadline, None);
                                        assert!(completed.borrow().is_empty());
                                        assert_eq!(source_drops.get(), 0);
                                    }
                                    if let Some(deadline) = buffer_deadline {
                                        assert!(next.flow_deadline.unwrap() > deadline);
                                    }
                                    assert_eq!(*processed.borrow(), expected_reads);
                                    latest = next;
                                }
                                let decoded = complete_credit_wire(true, &wire);
                                assert!(decoded.frames.goaways.is_empty());
                                assert!(decoded.frames.errors.is_empty());
                            }
                            let completed_at = if timeout_disabled {
                                assert!(completed.borrow().is_empty());
                                assert!(tokio::time::Instant::now() >= epoch + duration);
                                None
                            } else {
                                let (completed_stream, completed_error, completed_at) = {
                                    let events = completed.borrow();
                                    assert_eq!(events.len(), 1);
                                    events[0]
                                };
                                assert_eq!(
                                    (completed_stream, completed_error),
                                    (
                                        stream,
                                        (case != FullBufferCase::CompletedFull)
                                            .then_some(HttpErrorCode::READ_TIMEOUT)
                                    )
                                );
                                if let Some(deadline) = buffer_deadline {
                                    assert!(completed_at >= deadline);
                                    assert!(completed_at < latest.flow_deadline.unwrap());
                                }
                                Some(completed_at)
                            };
                            let expected_resets = if completes {
                                Vec::new()
                            } else {
                                vec![(stream, H2ErrorCode::INTERNAL_ERROR)]
                            };
                            let decoded = complete_credit_wire(true, &wire);
                            assert_eq!(
                                decoded.frames.ping_replies,
                                (1..=ping_count as u64).collect::<Vec<_>>()
                            );
                            assert_eq!(decoded.frames.resets, expected_resets);
                            assert_eq!(decoded.frames.body_bytes, sent);
                            assert!(!decoded.completed.contains(&stream));
                            assert_eq!(source_drops.get(), usize::from(!timeout_disabled));
                            let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                            assert_eq!(
                                (owners.queued, owners.complete),
                                if completes { (1, 0) } else { (0, 0) }
                            );
                            assert!(
                                response
                                    .as_ref()
                                    .unwrap()
                                    .completion
                                    .wait()
                                    .now_or_never()
                                    .is_none()
                            );
                            stage.set("retiring target response and completing sibling");
                            if completes {
                                if timeout_disabled {
                                    assert!(producer.eom());
                                } else {
                                    assert!(completed_at.unwrap() < Instant::now());
                                }
                                admit_credit.notify_one();
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire)
                                        .completed
                                        .contains(&stream)
                                })
                                .await;
                                let decoded = complete_credit_wire(true, &wire);
                                assert_eq!(decoded.frames.body_data, vec![b'u'; 2 * K as usize]);
                                assert!(decoded.frames.resets.is_empty());
                                release_target.notify_one();
                                let mut healthy = response.take().unwrap();
                                let completion = healthy.completion.clone();
                                let mut header = healthy.source.read_header_event().await.unwrap();
                                assert!(header.eom());
                                assert_eq!(header.headers_mut().status_code().unwrap(), 200);
                                drop(header);
                                drop(healthy);
                                completion.wait().await.unwrap();
                                let events = completed.borrow();
                                assert_eq!(events.len(), 1);
                                assert_eq!((events[0].0, events[0].1), (stream, None));
                            } else {
                                let mut failed = response.take().unwrap();
                                let completion = failed.completion.clone();
                                let error = match failed.source.read_header_event().await {
                                    Err(error) => error,
                                    Ok(_) => panic!("full-buffer expiry produced response headers"),
                                };
                                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                                drop(failed);
                                completion.wait().await.unwrap();
                            }
                            release_sibling.notify_one();
                            let mut healthy = sibling.take().unwrap();
                            let completion = healthy.completion.clone();
                            let mut header = healthy.source.read_header_event().await.unwrap();
                            assert!(header.eom());
                            assert_eq!(header.headers_mut().status_code().unwrap(), 200);
                            drop(header);
                            drop(healthy);
                            completion.wait().await.unwrap();
                            let decoded = complete_credit_wire(true, &wire);
                            assert!(decoded.frames.goaways.is_empty());
                            assert_eq!(decoded.frames.resets, expected_resets);
                            assert_eq!(wire.borrow().close, FixtureClose::Open);
                        },
                    ))
                    .await;
                    drop(response.take());
                    drop(sibling.take());
                    for gate in &ping_gates {
                        gate.notify_one();
                    }
                    if let Some(restore_connection) = &restore_connection {
                        restore_connection.notify_one();
                    }
                    admit_credit.notify_one();
                    release_target.notify_one();
                    release_sibling.notify_one();
                    finish_input.notify_one();
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, exercise) =
                    tokio::join!(eligibility_catch_unwind(session.run()), exercise);
                let finished = handle.is_finished();
                drop(handle);
                drop(transport);
                if let Err(payload) = exercise {
                    resume_unwind(payload);
                }
                if let Err(payload) = run {
                    resume_unwind(payload);
                }
                exercise.unwrap().unwrap_or_else(|error| {
                    panic!("upstream full-buffer phase {}: {error:?}", stage.get())
                });
                run.unwrap()
                    .expect("stream-local full-buffer timeout joins cleanly");
                assert!(finished);
                assert_eq!(source_drops.get(), 1);
                assert_eq!(wire.borrow().close, FixtureClose::ShutdownWrite);
                assert!(wire.borrow().transport_dropped);
            })
        });
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TerminalCreditCase {
        Attached,
        LateEom,
        LateTrailers,
        DataBeforeTrailers,
        ConnectionBlocked,
        ConnectionBlockedEmptyTrailers,
    }

    #[test]
    fn upstream_terminal_events_finish_at_zero_stream_credit() {
        for case in [
            TerminalCreditCase::Attached,
            TerminalCreditCase::LateEom,
            TerminalCreditCase::LateTrailers,
            TerminalCreditCase::DataBeforeTrailers,
        ] {
            upstream_terminal_credit_case(case);
        }
    }

    #[test]
    fn upstream_admitted_terminal_waits_for_connection_credit() {
        upstream_terminal_credit_case(TerminalCreditCase::ConnectionBlocked);
    }

    #[test]
    fn upstream_empty_body_before_trailers_resumes_at_zero_stream_credit() {
        upstream_terminal_credit_case(TerminalCreditCase::ConnectionBlockedEmptyTrailers);
    }

    fn upstream_terminal_credit_case(case: TerminalCreditCase) {
        use roxygen_core::stream_source::FlowControlState;
        use roxygen_core::stream_source::HttpStreamSource;

        run_on_worker(move |context| {
            Box::pin(async move {
                const K: u32 = 65_535;
                let stream = StreamId::new(1);
                let connection_blocked = matches!(
                    case,
                    TerminalCreditCase::ConnectionBlocked
                        | TerminalCreditCase::ConnectionBlockedEmptyTrailers
                );
                let with_trailers = matches!(
                    case,
                    TerminalCreditCase::LateTrailers
                        | TerminalCreditCase::DataBeforeTrailers
                        | TerminalCreditCase::ConnectionBlockedEmptyTrailers
                );
                let seed = if connection_blocked { K } else { 1 };
                let extra = u32::from(case == TerminalCreditCase::DataBeforeTrailers);
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                peer.egress_settings_mut()
                    .set(SettingsId::InitialWindowSize, u64::from(seed));
                let mut settings = BufQueue::new(4096);
                peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                    .unwrap();
                let mut initial = settings.coalesce(settings.chain_length()).unwrap().to_vec();
                initial.extend(eligibility_frame(&mut peer, EgressFrame::SettingsAck));
                let ping = Arc::new(tokio::sync::Notify::new());
                let credit = Arc::new(tokio::sync::Notify::new());
                let respond = Arc::new(tokio::sync::Notify::new());
                let finish = Arc::new(tokio::sync::Notify::new());
                let mut reads = vec![
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(
                        Arc::clone(&ping),
                        eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(1) }),
                    ),
                ];
                if case == TerminalCreditCase::DataBeforeTrailers || connection_blocked {
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&credit),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::WindowUpdate {
                                stream: if connection_blocked {
                                    StreamId::UNSPECIFIED
                                } else {
                                    stream
                                },
                                amount: 1,
                            },
                        ),
                    ));
                }
                let mut success = HttpMessage::new();
                success.set_status_code(200).unwrap();
                success.set_http_version(1, 1);
                reads.push(FixtureRead::WaitData(
                    Arc::clone(&respond),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::Headers {
                            stream,
                            message: &success,
                            end_stream: true,
                            extra_headers: None,
                        },
                    ),
                ));
                reads.push(FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()));
                let (transport, wire) = FixtureTransport::new(reads);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig {
                        write_timeout: Duration::from_secs(5),
                        read_timeout: Duration::ZERO,
                        stream_read_timeout: Duration::ZERO,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig {
                        double_goaway_drain: false,
                        ..Http2SessionConfig::default()
                    },
                )
                .unwrap();
                let control = handle.observe_h2_source_timers_for_test(stream).unwrap();
                let completed = Rc::new(RefCell::new(Vec::new()));
                let source_drops = Rc::new(Cell::new(0));
                let processed = Rc::new(RefCell::new(Vec::new()));
                let mut response = None;
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(30),
                        async {
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).frames.settings_acks == 1
                            })
                            .await;
                            let mut upload = HttpStreamSource::new_session(
                                stream,
                                UploadCompletion {
                                    events: Rc::clone(&completed),
                                    drops: Rc::clone(&source_drops),
                                    processed: Rc::clone(&processed),
                                },
                                3 * K,
                                Duration::ZERO,
                            )
                            .unwrap();
                            let producer = upload.producer();
                            let mut head =
                                committed_test_headers(b"/terminal-credit", false).into_headers();
                            head.headers_mut().set(
                                HttpHeaderCode::ContentLength,
                                (seed + extra).to_string().as_bytes(),
                            );
                            assert!(producer.headers(head, false));
                            let header = upload.read_header_event().await.unwrap();
                            if case == TerminalCreditCase::Attached {
                                assert_eq!(
                                    producer.body(queue(b"u"), 0, true),
                                    FlowControlState::Open
                                );
                            }
                            response = Some(
                                handle
                                    .send_prepared_request(
                                        UniplexHandle::prepare_request_with_headers(
                                            header,
                                            HttpSourceHolder::new(Box::new(upload)),
                                            handle.reserve_request().unwrap(),
                                        ),
                                    )
                                    .unwrap(),
                            );
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire)
                                    .frames
                                    .header_streams
                                    .contains(&stream)
                            })
                            .await;
                            if case != TerminalCreditCase::Attached {
                                for chunk in vec![b'u'; seed as usize].chunks(16 * 1024) {
                                    assert_eq!(
                                        producer.body(queue(chunk), 0, false),
                                        FlowControlState::Open
                                    );
                                }
                                let exhausted = eligibility_flow_snapshot(&control, |snapshot| {
                                    snapshot.stream_credit == 0
                                        && snapshot.connection_credit == K - seed
                                        && processed.borrow().iter().sum::<usize>() == seed as usize
                                })
                                .await;
                                assert_eq!(exhausted.buffer_timeout, None);
                                assert_eq!(
                                    complete_credit_wire(true, &wire).frames.body_bytes,
                                    seed as usize
                                );
                                assert!(completed.borrow().is_empty());
                                if extra != 0 {
                                    assert_eq!(
                                        producer.body(queue(b"u"), 0, false),
                                        FlowControlState::Open
                                    );
                                }
                                if case == TerminalCreditCase::ConnectionBlockedEmptyTrailers {
                                    assert_eq!(
                                        producer.body(BufQueue::new(0), 0, false),
                                        FlowControlState::Open
                                    );
                                }
                                if with_trailers {
                                    let mut trailers = HttpHeaders::new();
                                    trailers.add("x-source-finish", "exact");
                                    assert!(producer.trailers(trailers));
                                } else {
                                    assert!(producer.eom());
                                }
                            }
                            ping.notify_one();
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).frames.ping_replies == [1]
                            })
                            .await;
                            tokio::time::timeout(
                                Duration::from_secs(2),
                                complete_credit_wait(|| !completed.borrow().is_empty()),
                            )
                            .await
                            .expect("terminal source event is admitted without stream credit");
                            {
                                let events = completed.borrow();
                                assert_eq!(events.len(), 1);
                                assert_eq!((events[0].0, events[0].1), (stream, None));
                            }
                            assert_eq!(source_drops.get(), 1);
                            if case == TerminalCreditCase::DataBeforeTrailers || connection_blocked
                            {
                                let decoded = complete_credit_wire(true, &wire);
                                assert_eq!(decoded.frames.body_bytes, seed as usize);
                                assert!(!decoded.completed.contains(&stream));
                                assert!(decoded.trailers.is_empty());
                                if connection_blocked {
                                    let blocked = eligibility_flow_snapshot(&control, |snapshot| {
                                        snapshot.connection_credit == 0
                                            && snapshot.blocked == Some((vec![stream], true))
                                    })
                                    .await;
                                    assert!(blocked.flow_deadline.is_some());
                                    assert_eq!(blocked.buffer_timeout, None);
                                }
                                credit.notify_one();
                            }
                            tokio::time::timeout(
                                Duration::from_secs(2),
                                complete_credit_wait(|| {
                                    complete_credit_wire(true, &wire)
                                        .completed
                                        .contains(&stream)
                                }),
                            )
                            .await
                            .expect("terminal wire event follows the reference credit gate");
                            let decoded = complete_credit_wire(true, &wire);
                            assert_eq!(
                                decoded.frames.body_data,
                                vec![b'u'; (seed + extra) as usize]
                            );
                            assert_eq!(
                                decoded.completed.iter().filter(|id| **id == stream).count(),
                                1
                            );
                            assert_eq!(
                                decoded.trailers,
                                if with_trailers {
                                    vec![(stream, b"exact".to_vec())]
                                } else {
                                    Vec::new()
                                }
                            );
                            assert!(decoded.frames.resets.is_empty());
                            assert!(decoded.frames.goaways.is_empty());
                            assert!(decoded.frames.errors.is_empty());
                            respond.notify_one();
                            let mut healthy = response.take().unwrap();
                            let completion = healthy.completion.clone();
                            let mut header = healthy.source.read_header_event().await.unwrap();
                            assert!(header.eom());
                            assert_eq!(header.headers_mut().status_code().unwrap(), 200);
                            drop(header);
                            drop(healthy);
                            completion.wait().await.unwrap();
                            assert_eq!(handle.h2_egress_owners_for_test(stream).unwrap().queued, 0);
                            assert_eq!(completed.borrow().len(), 1);
                        },
                    ))
                    .await;
                    drop(response.take());
                    ping.notify_one();
                    credit.notify_one();
                    respond.notify_one();
                    finish.notify_one();
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, exercise) =
                    tokio::join!(eligibility_catch_unwind(session.run()), exercise);
                let finished = handle.is_finished();
                drop(handle);
                drop(transport);
                if let Err(payload) = exercise {
                    resume_unwind(payload);
                }
                if let Err(payload) = run {
                    resume_unwind(payload);
                }
                exercise
                    .unwrap()
                    .unwrap_or_else(|error| panic!("terminal credit {case:?}: {error:?}"));
                run.unwrap().expect("terminal credit session joins cleanly");
                assert!(finished);
                assert_eq!(source_drops.get(), 1);
                assert_eq!(wire.borrow().close, FixtureClose::ShutdownWrite);
                assert!(wire.borrow().transport_dropped);
            })
        });
    }

    struct CallbackWakeHandler(Rc<tokio::sync::Notify>);

    #[test]
    fn sleeping_incremental_sources_keep_order_across_reversed_wakes() {
        run_on_worker(|context| {
            Box::pin(async move {
                let changed = Arc::new(tokio::sync::Notify::new());
                let gates: Vec<_> = (0..3)
                    .map(|_| EligibilityGate::new(EligibilityPhase::Headers, Arc::clone(&changed)))
                    .collect();
                let probes: Vec<_> = (0..3)
                    .map(|_| Rc::new(EligibilityProbe::default()))
                    .collect();
                let mut sources = VecDeque::new();
                for (gate, probe) in gates.iter().zip(&probes) {
                    let mut source = EligibilitySource::new(
                        EligibilityPhase::Headers,
                        Arc::clone(gate),
                        Rc::clone(probe),
                        Bytes::from_static(b"source-after-wake"),
                        false,
                    );
                    source
                        .header
                        .as_mut()
                        .unwrap()
                        .headers_mut()
                        .set_http_priority(HttpPriority::from_urgency_and_incremental(3, true));
                    sources.push_back(HttpSourceHolder::new(Box::new(source)));
                }
                let mut peer = Http2Codec::new(TransportDirection::Upstream);
                let mut initial = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
                initial.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Settings { settings: &[] },
                ));
                let mut requests = Vec::new();
                for _ in 0..3 {
                    let stream = peer.create_stream();
                    let mut request = HttpMessage::new();
                    request.set_method(HttpMethod::Get).unwrap();
                    request.set_url(b"/sleeping-order", false).unwrap();
                    request.set_http_version(1, 1);
                    request
                        .headers_mut()
                        .set(HttpHeaderCode::Host, b"example.com");
                    request.set_http_priority(HttpPriority::from_urgency_and_incremental(3, true));
                    requests.push(eligibility_frame(
                        &mut peer,
                        EgressFrame::Headers {
                            stream,
                            message: &request,
                            end_stream: true,
                            extra_headers: None,
                        },
                    ));
                }
                initial.extend_from_slice(&requests[0]);
                let admit_second = Arc::new(tokio::sync::Notify::new());
                let admit_third = Arc::new(tokio::sync::Notify::new());
                let finish_input = Arc::new(tokio::sync::Notify::new());
                let (inner, state) = FixtureTransport::new([
                    FixtureRead::SegmentedData(queue(&initial)),
                    FixtureRead::WaitData(Arc::clone(&admit_second), requests[1].clone()),
                    FixtureRead::WaitData(Arc::clone(&admit_third), requests[2].clone()),
                    FixtureRead::WaitDataEof(Arc::clone(&finish_input), Vec::new()),
                ]);
                let transport = Rc::new(EligibilityTransport {
                    inner,
                    changed: Arc::clone(&changed),
                    reads_started: Cell::new(0),
                    backing_start: 0,
                    backing_length: 0,
                    shared_written: Cell::new(0),
                    backing_drops: Arc::default(),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport.clone(),
                    Some(Rc::new(EligibilitySequenceHandler(RefCell::new(sources)))),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .unwrap();
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(3),
                        async {
                            eligibility_wait(&changed, || {
                                gates[0].polls.load(Ordering::SeqCst) != 0
                            })
                            .await;
                            admit_second.notify_one();
                            eligibility_wait(&changed, || {
                                gates[1].polls.load(Ordering::SeqCst) != 0
                            })
                            .await;
                            admit_third.notify_one();
                            eligibility_wait(&changed, || {
                                gates[2].polls.load(Ordering::SeqCst) != 0
                            })
                            .await;
                            assert!(eligibility_wire(&state).statuses.is_empty());
                            for gate in &gates {
                                assert_eq!(gate.polls.load(Ordering::SeqCst), 1);
                            }
                            for gate in [
                                Arc::clone(&gates[2]),
                                Arc::clone(&gates[0]),
                                Arc::clone(&gates[1]),
                            ] {
                                gate.release();
                            }
                            eligibility_wait(&changed, || {
                                eligibility_wire(&state).completed.len() == 3
                            })
                            .await;
                            let wire = eligibility_wire(&state);
                            assert_eq!(
                                wire.statuses,
                                [
                                    (StreamId::new(1), 200),
                                    (StreamId::new(3), 200),
                                    (StreamId::new(5), 200),
                                ]
                            );
                            for stream in [StreamId::new(1), StreamId::new(3), StreamId::new(5)] {
                                assert_eq!(wire.body[&stream], b"source-after-wake");
                                assert!(wire.completed.contains(&stream));
                            }
                            assert!(wire.resets.is_empty());
                        },
                    ))
                    .await;
                    for gate in &gates {
                        gate.release();
                    }
                    admit_second.notify_one();
                    admit_third.notify_one();
                    finish_input.notify_one();
                    outcome
                };
                let (run, exercise) = tokio::join!(session.run(), exercise);
                if let Err(payload) = exercise {
                    std::panic::resume_unwind(payload);
                }
                exercise
                    .unwrap()
                    .expect("sleeping sources resume in scheduler order");
                run.expect("source wake session joins cleanly");
                for probe in probes {
                    assert_eq!(probe.header_reads.get(), 1);
                    assert_eq!(probe.body_reads.get(), 3);
                    assert_eq!(probe.reads_dropped.get(), 4);
                    assert_eq!(probe.cancelled_reads.get(), 0);
                    assert_eq!(probe.source_drops.get(), 1);
                }
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
                drop(transport);
                assert!(state.borrow().transport_dropped);
            })
        });
    }

    impl HttpHandler for CallbackWakeHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let release = Rc::clone(&self.0);
            Box::pin(async move {
                assert!(request.read_header_event().await?.eom());
                release.notified().await;
                Ok(HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_fixed_response(200, Some(queue(b"callback-response")))
                        .unwrap(),
                )))
            })
        }
    }

    #[test]
    fn reader_drained_handler_callback_wakes_idle_writer() {
        run_on_worker(|context| {
            Box::pin(async move {
                let changed = Arc::new(tokio::sync::Notify::new());
                let release_handler = Rc::new(tokio::sync::Notify::new());
                let finish_input = Arc::new(tokio::sync::Notify::new());
                let mut peer = Http2Codec::new(TransportDirection::Upstream);
                let mut initial = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
                initial.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Settings { settings: &[] },
                ));
                initial.extend(eligibility_request(&mut peer, 3));
                let (inner, state) = FixtureTransport::new([
                    FixtureRead::SegmentedData(queue(&initial)),
                    FixtureRead::WaitDataEof(Arc::clone(&finish_input), Vec::new()),
                ]);
                let transport = Rc::new(EligibilityTransport {
                    inner,
                    changed: Arc::clone(&changed),
                    reads_started: Cell::new(0),
                    backing_start: 0,
                    backing_length: 0,
                    shared_written: Cell::new(0),
                    backing_drops: Arc::default(),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport.clone(),
                    Some(Rc::new(CallbackWakeHandler(Rc::clone(&release_handler)))),
                    UniplexConfig {
                        read_timeout: Duration::ZERO,
                        stream_read_timeout: Duration::ZERO,
                        write_timeout: Duration::ZERO,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig::default(),
                )
                .unwrap();
                let control = handle
                    .observe_h2_source_timers_for_test(StreamId::new(1))
                    .unwrap();
                control.pause_next_writer_wait.set(true);
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(3),
                        async {
                            eligibility_wait(&control.changed, || control.writer_wait_paused.get())
                                .await;
                            assert!(eligibility_wire(&state).statuses.is_empty());
                            release_handler.notify_one();
                            while !handle.drain_reader_handler_callback_for_test().unwrap() {
                                tokio::task::yield_now().await;
                            }
                            let reads = transport.reads_started.get();
                            control.resume_writer_wait.notify_one();
                            eligibility_wait(&changed, || {
                                eligibility_wire(&state)
                                    .completed
                                    .contains(&StreamId::new(1))
                            })
                            .await;
                            let wire = eligibility_wire(&state);
                            assert_eq!(wire.statuses, [(StreamId::new(1), 200)]);
                            assert_eq!(wire.body[&StreamId::new(1)], b"callback-response");
                            assert!(wire.resets.is_empty());
                            assert_eq!(transport.reads_started.get(), reads);
                        },
                    ))
                    .await;
                    control.pause_next_writer_wait.set(false);
                    control.resume_writer_wait.notify_one();
                    release_handler.notify_one();
                    finish_input.notify_one();
                    outcome
                };
                let (run, exercise) = tokio::join!(session.run(), exercise);
                run.expect("callback wake session joins");
                exercise
                    .unwrap()
                    .expect("response progresses without further peer input");
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    #[test]
    fn pending_source_wakeup_and_cancellation() {
        for phase in [
            EligibilityPhase::Headers,
            EligibilityPhase::Body,
            EligibilityPhase::Resume,
        ] {
            run_on_worker(move |context| {
                Box::pin(response_source_eligibility_case(context, phase, true, true))
            });
        }
        for phase in [EligibilityPhase::Body, EligibilityPhase::Resume] {
            run_on_worker(move |context| {
                Box::pin(request_source_eligibility_cancellation(context, phase))
            });
        }
        for case in [
            EligibilityTimerCase::RetiredWake,
            EligibilityTimerCase::SynchronousWake,
            EligibilityTimerCase::OrdinaryWork,
            EligibilityTimerCase::ConnectionToStream,
        ] {
            run_on_worker(move |context| Box::pin(source_eligibility_timer_case(context, case)));
        }
    }

    async fn request_source_eligibility_cancellation(
        context: WorkerContext,
        phase: EligibilityPhase,
    ) {
        let changed = Arc::new(tokio::sync::Notify::new());
        let gate = EligibilityGate::new(phase, Arc::clone(&changed));
        let probe = Rc::new(EligibilityProbe::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let bytes = Bytes::from_owner(DropTrackedBytes {
            bytes: vec![b'u'; 100],
            drops: Arc::clone(&drops),
        });
        let backing_start = bytes.as_ptr() as usize;
        let source =
            EligibilitySource::new(phase, Arc::clone(&gate), Rc::clone(&probe), bytes, true);
        let (inner, state) = FixtureTransport::new([]);
        let transport = Rc::new(EligibilityTransport {
            inner,
            changed: Arc::clone(&changed),
            reads_started: Cell::new(0),
            backing_start,
            backing_length: 100,
            shared_written: Cell::new(0),
            backing_drops: Arc::clone(&drops),
        });
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Upstream,
            transport.clone(),
            None,
            UniplexConfig::default(),
            Http2SessionConfig::default(),
        )
        .unwrap();
        let cancellation = CancellationSource::new();
        let mut response = handle
            .send_request(
                HttpSourceHolder::new(Box::new(source)),
                cancellation.token(),
            )
            .await
            .expect("request headers admitted")
            .source;
        let exercise = async {
            let outcome =
                eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(3), async {
                    eligibility_wait(&changed, || gate.polls.load(Ordering::SeqCst) != 0).await;
                    assert_eq!(probe.header_reads.get(), 1);
                    assert_eq!(probe.body_reads.get(), 1);
                    let ping = handle.send_ping().expect("unrelated writer notification");
                    eligibility_wait(&changed, || {
                        decode_client_frames(&state.borrow().writes.concat())
                            .ping_requests
                            .contains(&ping)
                    })
                    .await;
                    assert_eq!(gate.polls.load(Ordering::SeqCst), 1);
                    assert!(cancellation.request_cancellation());
                    let error = match response.read_header_event().await {
                        Err(error) => error,
                        Ok(_) => panic!("cancelled request produced response headers"),
                    };
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    assert!(!gate.released.load(Ordering::SeqCst));
                    assert_eq!(probe.body_reads.get(), 1);
                    assert_eq!(transport.shared_written.get(), 0);
                }))
                .await;
            if !matches!(&outcome, Ok(Ok(()))) {
                cancellation.request_cancellation();
                gate.release();
            }
            drop(response);
            handle.initiate_drain().expect("cancelled request drains");
            outcome
        };
        let (run, exercise) = tokio::join!(session.run(), exercise);
        if let Err(payload) = exercise {
            std::panic::resume_unwind(payload);
        }
        exercise
            .unwrap()
            .expect("source-independent request cancellation");
        run.expect("cancelled source native session joins");
        assert_eq!(probe.source_drops.get(), 1);
        assert_eq!(probe.reads_dropped.get(), 2);
        assert_eq!(
            probe.cancelled_reads.get(),
            usize::from(phase == EligibilityPhase::Body)
        );
        assert_eq!(
            probe.resume_drops.get(),
            usize::from(phase == EligibilityPhase::Resume)
        );
        assert_eq!(
            probe.cancelled_resumes.get(),
            usize::from(phase == EligibilityPhase::Resume)
        );
        assert_eq!(
            probe.stops.borrow().as_slice(),
            [Some(HttpErrorCode::CANCEL)]
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            decode_client_frames(&state.borrow().writes.concat()).resets,
            [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
        );
        assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
        drop(transport);
        assert!(state.borrow().transport_dropped);
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum EligibilityTimerCase {
        RetiredWake,
        SynchronousWake,
        OrdinaryWork,
        ConnectionToStream,
    }

    #[derive(Default)]
    struct EligibilityPollAction {
        handle: RefCell<Option<UniplexHandle>>,
        armed: Cell<bool>,
        notifications: Cell<usize>,
    }

    struct EligibilityActionSource {
        source: EligibilitySource,
        action: Rc<EligibilityPollAction>,
    }

    impl HttpSource for EligibilityActionSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.source.read_header_event()
        }

        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            let mut read = self.source.read_body_event(maximum);
            let action = Rc::clone(&self.action);
            Box::pin(std::future::poll_fn(move |context| {
                if action.armed.replace(false) {
                    let handle = action
                        .handle
                        .borrow_mut()
                        .take()
                        .expect("one priority action");
                    assert_eq!(action.notifications.replace(1), 0);
                    assert!(
                        handle
                            .update_http2_priority(
                                StreamId::new(3),
                                HttpPriority::from_urgency_and_incremental(2, false),
                            )
                            .expect("ordinary work from the actual pending source poll")
                    );
                }
                read.as_mut().poll(context)
            }))
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.source.stop_reading(error);
        }
    }

    struct EligibilitySequenceHandler(RefCell<VecDeque<HttpSourceHolder>>);

    impl HttpHandler for EligibilitySequenceHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let source = self.0.borrow_mut().pop_front().expect("two real requests");
            Box::pin(async move {
                assert!(request.read_header_event().await?.eom());
                Ok(source)
            })
        }
    }

    struct EligibilityTimerCleanup {
        control: H2SourceTimerTestControl,
        source_gate: Arc<EligibilityGate>,
        input_gates: Vec<Arc<tokio::sync::Notify>>,
        preserve_pending_source: bool,
    }

    impl Drop for EligibilityTimerCleanup {
        fn drop(&mut self) {
            self.control.capture_next_arm.set(false);
            self.control.pause_next_source_yield.set(false);
            self.control.pause_next_source_service.set(false);
            self.control.resume_source_yield.notify_one();
            if !self.preserve_pending_source {
                self.source_gate.release();
            }
            for gate in &self.input_gates {
                gate.notify_one();
            }
        }
    }

    async fn eligibility_flow_snapshot(
        control: &H2SourceTimerTestControl,
        predicate: impl Fn(&H2SourceFlowSnapshot) -> bool,
    ) -> H2SourceFlowSnapshot {
        loop {
            let notified = control.changed.notified();
            let snapshot = control.snapshot.borrow().clone();
            if let Some(snapshot) = snapshot
                && predicate(&snapshot)
            {
                return snapshot;
            }
            notified.await;
        }
    }

    async fn eligibility_advance_to(deadline: Instant) {
        let target = tokio::time::Instant::from_std(deadline);
        let now = tokio::time::Instant::now();
        assert!(target >= now, "timer phases must advance monotonically");
        tokio::time::advance(target - now).await;
    }

    async fn source_eligibility_timer_case(context: WorkerContext, case: EligibilityTimerCase) {
        tokio::time::pause();
        let changed = Arc::new(tokio::sync::Notify::new());
        let phase = if case == EligibilityTimerCase::SynchronousWake {
            EligibilityPhase::SynchronousWake
        } else {
            EligibilityPhase::Body
        };
        let source_gate = EligibilityGate::new(phase, Arc::clone(&changed));
        let source_probe = Rc::new(EligibilityProbe::default());
        let action = Rc::new(EligibilityPollAction::default());
        let mut pending_source = EligibilitySource::new(
            phase,
            Arc::clone(&source_gate),
            Rc::clone(&source_probe),
            Bytes::from_static(b"pending-source"),
            false,
        );
        pending_source
            .header
            .as_mut()
            .unwrap()
            .headers_mut()
            .set_http_priority(HttpPriority::from_urgency_and_incremental(0, false));
        let buffered_probe = Rc::new(EligibilityProbe::default());
        let buffered_gate = EligibilityGate::new(EligibilityPhase::Headers, Arc::clone(&changed));
        buffered_gate.release();
        let backing_drops = Arc::new(AtomicUsize::new(0));
        let expected: Vec<u8> = (0..70_000).map(|offset| (offset % 251) as u8).collect();
        let bytes = Bytes::from_owner(DropTrackedBytes {
            bytes: expected.clone(),
            drops: Arc::clone(&backing_drops),
        });
        let backing_start = bytes.as_ptr() as usize;
        let buffered_source = EligibilitySource::new(
            EligibilityPhase::Headers,
            buffered_gate,
            Rc::clone(&buffered_probe),
            bytes,
            false,
        );
        let handler = Rc::new(EligibilitySequenceHandler(RefCell::new(VecDeque::from([
            HttpSourceHolder::new(Box::new(EligibilityActionSource {
                source: pending_source,
                action: Rc::clone(&action),
            })),
            HttpSourceHolder::new(Box::new(buffered_source)),
        ]))));
        let mut peer = Http2Codec::new(TransportDirection::Upstream);
        let mut initial = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
        let mut settings = BufQueue::new(1 << 20);
        peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
            .unwrap();
        initial.extend_from_slice(&settings.coalesce(settings.chain_length()).unwrap());
        initial.extend(eligibility_request(&mut peer, 0));
        let mut sibling = Vec::new();
        if case == EligibilityTimerCase::RetiredWake {
            sibling.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::CANCEL,
                },
            ));
        }
        sibling.extend(eligibility_request(&mut peer, 3));
        let admit_sibling = Arc::new(tokio::sync::Notify::new());
        let admit_credit = Arc::new(tokio::sync::Notify::new());
        let admit_cleanup = Arc::new(tokio::sync::Notify::new());
        let mut reads = vec![
            FixtureRead::SegmentedData(queue(&initial)),
            FixtureRead::WaitData(Arc::clone(&admit_sibling), sibling),
        ];
        if case == EligibilityTimerCase::ConnectionToStream {
            reads.push(FixtureRead::WaitData(
                Arc::clone(&admit_credit),
                eligibility_frame(
                    &mut peer,
                    EgressFrame::WindowUpdate {
                        stream: StreamId::UNSPECIFIED,
                        amount: 65_535,
                    },
                ),
            ));
        }
        let cleanup_input = if case == EligibilityTimerCase::RetiredWake {
            Vec::new()
        } else {
            eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::CANCEL,
                },
            )
        };
        reads.push(FixtureRead::WaitDataEof(
            Arc::clone(&admit_cleanup),
            cleanup_input,
        ));
        let (inner, state) = FixtureTransport::new(reads);
        let transport = Rc::new(EligibilityTransport {
            inner,
            changed: Arc::clone(&changed),
            reads_started: Cell::new(0),
            backing_start,
            backing_length: expected.len(),
            shared_written: Cell::new(0),
            backing_drops: Arc::clone(&backing_drops),
        });
        let write_timeout = Duration::from_secs(60);
        let mut rate_limits = Http2RateLimitConfig::default();
        for limit in [
            &mut rate_limits.control_frames,
            &mut rate_limits.header_frames,
            &mut rate_limits.resets,
            &mut rate_limits.direct_errors,
        ] {
            limit.interval = Duration::from_secs(600);
        }
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Downstream,
            transport.clone(),
            Some(handler),
            UniplexConfig {
                write_timeout,
                read_timeout: Duration::ZERO,
                stream_read_timeout: Duration::ZERO,
                ..UniplexConfig::default()
            },
            Http2SessionConfig {
                rate_limits,
                ..Http2SessionConfig::default()
            },
        )
        .unwrap();
        let control = handle
            .observe_h2_source_timers_for_test(StreamId::new(3))
            .unwrap();
        if case == EligibilityTimerCase::OrdinaryWork {
            *action.handle.borrow_mut() = Some(handle.clone());
        }
        let mut cleanup = EligibilityTimerCleanup {
            control: control.clone(),
            source_gate: Arc::clone(&source_gate),
            input_gates: vec![
                Arc::clone(&admit_sibling),
                Arc::clone(&admit_credit),
                admit_cleanup,
            ],
            preserve_pending_source: false,
        };
        let exercise = async {
            let outcome =
                eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(300), async {
                    eligibility_wait(&changed, || source_gate.polls.load(Ordering::SeqCst) != 0)
                        .await;
                    let source_waker = source_gate.waker.lock().unwrap().as_ref().unwrap().clone();
                    admit_sibling.notify_one();
                    let first = eligibility_flow_snapshot(&control, |snapshot| {
                        snapshot.flow_deadline.is_some()
                            && snapshot.blocked == Some((vec![StreamId::new(3)], true))
                    })
                    .await;
                    assert_eq!(first.connection_credit, 0, "{case:?}");
                    assert_eq!(first.stream_credit, 0, "{case:?}");
                    assert_eq!(first.buffer_timeout, None, "sub-limit remainder: {case:?}");
                    let first_deadline = first.flow_deadline.unwrap();
                    assert_eq!(buffered_probe.header_reads.get(), 1);
                    assert_eq!(buffered_probe.body_reads.get(), 3);
                    assert_eq!(buffered_probe.reads_dropped.get(), 4);
                    assert_eq!(buffered_probe.cancelled_reads.get(), 0);
                    assert_eq!(buffered_probe.source_drops.get(), 1);
                    let owners = handle.h2_egress_owners_for_test(StreamId::new(3)).unwrap();
                    assert_eq!((owners.queued, owners.complete), (1, 0));
                    assert_eq!(owners.egress_complete, Some(false));
                    assert_eq!(source_probe.header_reads.get(), 1);
                    assert_eq!(source_probe.body_reads.get(), 1);
                    assert_eq!(
                        source_probe.source_drops.get(),
                        usize::from(case == EligibilityTimerCase::RetiredWake)
                    );
                    let first_wire = state.borrow().writes.clone();
                    let first_read_count = transport.reads_started.get();
                    let decoded = eligibility_wire(&state);
                    assert_eq!(
                        decoded.body.get(&StreamId::new(3)).unwrap(),
                        &expected[..65_535]
                    );
                    assert!(!decoded.completed.contains(&StreamId::new(3)));
                    assert!(decoded.resets.is_empty());
                    assert_eq!(transport.shared_written.get(), 65_535);
                    assert_eq!(backing_drops.load(Ordering::SeqCst), 0);
                    let deadline = match case {
                        EligibilityTimerCase::RetiredWake => {
                            assert_eq!(source_probe.cancelled_reads.get(), 1);
                            let polls = source_gate.polls.load(Ordering::SeqCst);
                            eligibility_advance_to(
                                first_deadline.checked_sub(write_timeout / 2).unwrap(),
                            )
                            .await;
                            source_waker.wake_by_ref();
                            let repeated = eligibility_flow_snapshot(&control, |snapshot| {
                                snapshot.sequence > first.sequence
                                    && snapshot.flow_deadline.is_some()
                            })
                            .await;
                            assert_eq!(repeated.flow_deadline, Some(first_deadline));
                            assert_eq!(repeated.blocked, first.blocked);
                            assert_eq!(repeated.connection_credit, 0);
                            assert_eq!(repeated.stream_credit, 0);
                            assert_eq!(repeated.buffer_timeout, None);
                            assert_eq!(source_gate.polls.load(Ordering::SeqCst), polls);
                            first_deadline
                        }
                        EligibilityTimerCase::SynchronousWake => {
                            let polls = source_gate.polls.load(Ordering::SeqCst);
                            let repeated = eligibility_flow_snapshot(&control, |snapshot| {
                                snapshot.sequence > first.sequence
                                    && source_gate.polls.load(Ordering::SeqCst) > polls
                                    && snapshot.flow_deadline.is_some()
                            })
                            .await;
                            assert_eq!(repeated.flow_deadline, Some(first_deadline));
                            assert_eq!(repeated.blocked, first.blocked);
                            assert_eq!(repeated.buffer_timeout, None);
                            first_deadline
                        }
                        EligibilityTimerCase::OrdinaryWork => {
                            eligibility_advance_to(
                                first_deadline.checked_sub(write_timeout / 2).unwrap(),
                            )
                            .await;
                            action.armed.set(true);
                            let expected_deadline =
                                (tokio::time::Instant::now() + write_timeout).into_std();
                            source_waker.wake_by_ref();
                            let rearmed = eligibility_flow_snapshot(&control, |snapshot| {
                                snapshot.sequence > first.sequence
                                    && snapshot.flow_deadline.is_some()
                            })
                            .await;
                            assert_eq!(action.notifications.get(), 1);
                            assert!(action.handle.borrow().is_none());
                            assert_eq!(rearmed.flow_deadline, Some(expected_deadline));
                            assert_eq!(rearmed.blocked, first.blocked);
                            assert_eq!(rearmed.connection_credit, 0);
                            assert_eq!(rearmed.stream_credit, 0);
                            assert_eq!(rearmed.buffer_timeout, None);
                            assert!(expected_deadline > first_deadline);
                            eligibility_advance_to(first_deadline + Duration::from_millis(1)).await;
                            source_waker.wake_by_ref();
                            let after_old_deadline =
                                eligibility_flow_snapshot(&control, |snapshot| {
                                    snapshot.sequence > rearmed.sequence
                                        && snapshot.flow_deadline.is_some()
                                })
                                .await;
                            assert_eq!(after_old_deadline.flow_deadline, Some(expected_deadline));
                            assert_eq!(action.notifications.get(), 1);
                            assert_eq!(buffered_probe.source_drops.get(), 1);
                            assert_eq!(state.borrow().close, FixtureClose::Open);
                            expected_deadline
                        }
                        EligibilityTimerCase::ConnectionToStream => {
                            eligibility_advance_to(
                                first_deadline.checked_sub(write_timeout / 2).unwrap(),
                            )
                            .await;
                            control.pause_next_source_service.set(true);
                            source_waker.wake_by_ref();
                            eligibility_wait(&control.changed, || {
                                control.source_yield_paused.get()
                            })
                            .await;
                            assert!(tokio::time::Instant::now().into_std() < first_deadline);
                            assert_eq!(transport.reads_started.get(), 3);
                            admit_credit.notify_one();
                            eligibility_wait(&changed, || transport.reads_started.get() == 4).await;
                            assert_eq!(
                                handle.http2_stream_send_credit(StreamId::new(3)).unwrap(),
                                0
                            );
                            assert!(control.source_yield_paused.get());
                            assert!(tokio::time::Instant::now().into_std() < first_deadline);
                            eligibility_advance_to(first_deadline + Duration::from_millis(1)).await;
                            assert!(control.source_yield_paused.get());
                            assert_eq!(buffered_probe.source_drops.get(), 1);
                            let expected_deadline =
                                (tokio::time::Instant::now() + write_timeout).into_std();
                            let sequence_before_resume = control
                                .snapshot
                                .borrow()
                                .as_ref()
                                .expect("last arm before writer resume")
                                .sequence;
                            *control.first_arm.borrow_mut() = None;
                            control.capture_next_arm.set(true);
                            control.resume_source_yield.notify_one();
                            eligibility_wait(&control.changed, || {
                                control.first_arm.borrow().is_some()
                            })
                            .await;
                            let first_after_resume = control
                                .first_arm
                                .borrow()
                                .clone()
                                .expect("immutable first post-resume arm");
                            assert!(!control.capture_next_arm.get());
                            assert_eq!(
                                first_after_resume.sequence,
                                sequence_before_resume.checked_add(1).unwrap()
                            );
                            assert_eq!(
                                first_after_resume.blocked,
                                Some((vec![StreamId::new(3)], false))
                            );
                            assert_eq!(first_after_resume.connection_credit, 65_535);
                            assert_eq!(first_after_resume.stream_credit, 0);
                            assert_eq!(first_after_resume.buffer_timeout, None);
                            assert_eq!(first_after_resume.flow_deadline, Some(expected_deadline));
                            let rearmed = eligibility_flow_snapshot(&control, |snapshot| {
                                snapshot.sequence > first.sequence
                                    && snapshot.blocked == Some((vec![StreamId::new(3)], false))
                                    && snapshot.flow_deadline.is_some()
                            })
                            .await;
                            assert!(rearmed.sequence >= first_after_resume.sequence);
                            assert_eq!(
                                control.first_arm.borrow().as_ref().unwrap().sequence,
                                first_after_resume.sequence
                            );
                            assert!(!control.source_yield_paused.get());
                            assert_eq!(rearmed.connection_credit, 65_535);
                            assert_eq!(rearmed.stream_credit, 0);
                            assert_eq!(rearmed.buffer_timeout, None);
                            assert_eq!(rearmed.flow_deadline, Some(expected_deadline));
                            assert_eq!(buffered_probe.source_drops.get(), 1);
                            assert_eq!(state.borrow().close, FixtureClose::Open);
                            expected_deadline
                        }
                    };
                    assert_eq!(
                        state.borrow().writes,
                        first_wire,
                        "no wire progress before timeout: {case:?}"
                    );
                    assert_eq!(
                        transport.reads_started.get(),
                        first_read_count
                            + usize::from(case == EligibilityTimerCase::ConnectionToStream)
                    );
                    assert_eq!(source_probe.body_reads.get(), 1);
                    assert_eq!(buffered_probe.body_reads.get(), 3);
                    assert!(!source_gate.released.load(Ordering::SeqCst));
                    eligibility_advance_to(deadline + Duration::from_millis(1)).await;
                    if case == EligibilityTimerCase::ConnectionToStream {
                        eligibility_wait(&changed, || !eligibility_wire(&state).resets.is_empty())
                            .await;
                        assert_eq!(
                            eligibility_wire(&state).resets,
                            [(StreamId::new(3), H2ErrorCode::FLOW_CONTROL_ERROR)]
                        );
                        assert_eq!(state.borrow().close, FixtureClose::Open);
                        assert_eq!(source_probe.source_drops.get(), 0);
                    } else {
                        complete_credit_wait(|| state.borrow().close == FixtureClose::Reset).await;
                    }
                    assert!(
                        tokio::time::Instant::now().into_std()
                            <= deadline + Duration::from_millis(2)
                    );
                    assert_eq!(transport.shared_written.get(), 65_535);
                    assert!(!source_gate.released.load(Ordering::SeqCst));
                    assert_eq!(
                        action.notifications.get(),
                        usize::from(case == EligibilityTimerCase::OrdinaryWork)
                    );
                }))
                .await;
            cleanup.preserve_pending_source = matches!(&outcome, Ok(Ok(())));
            drop(cleanup);
            outcome
        };
        let (run, exercise) = tokio::join!(session.run(), exercise);
        if let Err(payload) = exercise {
            std::panic::resume_unwind(payload);
        }
        exercise
            .unwrap()
            .unwrap_or_else(|error| panic!("timer phase timed out for {case:?}: {error}"));
        if case == EligibilityTimerCase::ConnectionToStream {
            run.expect("stream-only flow timeout preserves the native connection");
            assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            assert_eq!(
                eligibility_wire(&state).resets,
                [(StreamId::new(3), H2ErrorCode::FLOW_CONTROL_ERROR)]
            );
        } else {
            let error = run.expect_err("unchanged connection stall must time out");
            assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
            assert_eq!(error.message(), b"HTTP/2 connection flow-control timeout");
            assert_eq!(state.borrow().close, FixtureClose::Reset);
        }
        assert!(!control.source_yield_paused.get());
        assert!(!source_gate.released.load(Ordering::SeqCst));
        assert_eq!(source_probe.header_reads.get(), 1);
        assert_eq!(source_probe.body_reads.get(), 1);
        assert_eq!(source_probe.reads_dropped.get(), 2);
        assert_eq!(source_probe.cancelled_reads.get(), 1);
        assert_eq!(source_probe.source_drops.get(), 1);
        assert_eq!(buffered_probe.header_reads.get(), 1);
        assert_eq!(buffered_probe.body_reads.get(), 3);
        assert_eq!(buffered_probe.reads_dropped.get(), 4);
        assert_eq!(buffered_probe.cancelled_reads.get(), 0);
        assert_eq!(buffered_probe.source_drops.get(), 1);
        assert_eq!(backing_drops.load(Ordering::SeqCst), 1);
        let decoded = eligibility_wire(&state);
        assert_eq!(
            decoded.body.get(&StreamId::new(3)).unwrap(),
            &expected[..65_535]
        );
        assert!(!decoded.completed.contains(&StreamId::new(3)));
        assert!(decoded.trailers.is_empty());
        drop(transport);
        assert!(state.borrow().transport_dropped);
    }

    #[derive(Default)]
    struct ZeroAckReadStats {
        buffered: Cell<i64>,
        deltas: RefCell<Vec<i64>>,
        changed: tokio::sync::Notify,
    }

    impl Http2SessionStats for ZeroAckReadStats {
        fn record_buffered_read_bytes(&self, delta: i64) {
            self.buffered.set(self.buffered.get() + delta);
            self.deltas.borrow_mut().push(delta);
            self.changed.notify_one();
        }
    }

    struct ZeroAckFlowCleanup {
        control: H2SourceTimerTestControl,
        peer_eof: Arc<tokio::sync::Notify>,
    }

    impl Drop for ZeroAckFlowCleanup {
        fn drop(&mut self) {
            self.control.pause_next_writer_wait.set(false);
            self.control.resume_writer_wait.notify_one();
            self.peer_eof.notify_one();
        }
    }

    async fn zero_ack_response_read_flow_deadline_case(context: WorkerContext) {
        tokio::time::pause();
        let stream = StreamId::new(1);
        let mut peer = Http2Codec::new(TransportDirection::Downstream);
        let mut settings = BufQueue::new(4096);
        peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
            .unwrap();
        let mut input = settings.coalesce(settings.chain_length()).unwrap().to_vec();
        let mut message = HttpMessage::new();
        message.set_http_version(1, 1);
        message.set_status_code(200).unwrap();
        input.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
        ));
        input.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Body {
                stream,
                body: queue(&[b'r'; 128]),
                padding: None,
                end_stream: false,
            },
        ));
        let peer_eof = Arc::new(tokio::sync::Notify::new());
        let (transport, wire) = FixtureTransport::new([
            FixtureRead::Data(input),
            FixtureRead::WaitDataEof(Arc::clone(&peer_eof), Vec::new()),
        ]);
        let stats = Rc::new(ZeroAckReadStats::default());
        let mut rate_limits = Http2RateLimitConfig::default();
        for limit in [
            &mut rate_limits.control_frames,
            &mut rate_limits.header_frames,
            &mut rate_limits.resets,
            &mut rate_limits.direct_errors,
        ] {
            limit.interval = Duration::from_secs(600);
        }
        let write_timeout = Duration::from_secs(60);
        let observation_margin = Duration::from_millis(100);
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Upstream,
            transport.clone(),
            None,
            UniplexConfig {
                write_timeout,
                read_timeout: Duration::ZERO,
                stream_read_timeout: Duration::ZERO,
                ..UniplexConfig::default()
            },
            Http2SessionConfig {
                connection_receive_window: 65_535,
                stream_receive_window: 65_535,
                double_goaway_drain: false,
                stats: Some(stats.clone()),
                rate_limits,
                ..Http2SessionConfig::default()
            },
        )
        .unwrap();
        let control = handle.observe_h2_source_timers_for_test(stream).unwrap();
        let cleanup = ZeroAckFlowCleanup {
            control: control.clone(),
            peer_eof,
        };
        let upload_probe = Rc::new(EligibilityProbe::default());
        let upload_gate = EligibilityGate::new(
            EligibilityPhase::Headers,
            Arc::new(tokio::sync::Notify::new()),
        );
        upload_gate.release();
        let upload = EligibilitySource::new(
            EligibilityPhase::Headers,
            upload_gate,
            Rc::clone(&upload_probe),
            Bytes::from(vec![b'u'; 70_000]),
            true,
        );
        let mut response = handle
            .send_request(
                HttpSourceHolder::new(Box::new(upload)),
                CancellationSource::new().token(),
            )
            .await
            .unwrap()
            .source;
        let exercise = async {
            let outcome =
                eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(300), async {
                    let setup = eligibility_flow_snapshot(&control, |snapshot| {
                        snapshot.blocked == Some((vec![stream], true))
                            && snapshot.flow_deadline.is_some()
                    })
                    .await;
                    let mut header = response.read_header_event().await.unwrap();
                    assert!(!header.eom());
                    assert_eq!(header.headers_mut().status_code().unwrap(), 200);
                    drop(header);
                    // Retire the header read's exchange guard before fixing the observed interval.
                    control.pause_next_writer_wait.set(true);
                    let first = eligibility_flow_snapshot(&control, |snapshot| {
                        snapshot.sequence > setup.sequence && control.writer_wait_paused.get()
                    })
                    .await;
                    assert_eq!(first.blocked, Some((vec![stream], true)));
                    assert_eq!((first.connection_credit, first.stream_credit), (0, 0));
                    assert_eq!(first.buffer_timeout, None);
                    let deadline = first.flow_deadline.expect("blocked upload timer");
                    assert_eq!(
                        deadline,
                        (tokio::time::Instant::now() + write_timeout).into_std()
                    );
                    assert_eq!(stats.buffered.get(), 128);
                    assert_eq!(&*stats.deltas.borrow(), &[128]);
                    assert_eq!(upload_probe.body_reads.get(), 3);
                    assert_eq!(upload_probe.reads_dropped.get(), 4);
                    assert_eq!(upload_probe.cancelled_reads.get(), 0);
                    assert_eq!(upload_probe.source_drops.get(), 1);
                    let before = wire.borrow().writes.clone();
                    let decoded = complete_credit_wire(true, &wire);
                    assert_eq!(decoded.frames.body_bytes, 65_535);
                    assert!(decoded.frames.window_updates.is_empty());
                    assert!(decoded.frames.errors.is_empty());
                    assert!(!decoded.completed.contains(&stream));
                    control.resume_writer_wait.notify_one();

                    eligibility_advance_to(deadline - write_timeout / 2).await;
                    let event = response.read_body_event(128).await.unwrap();
                    assert!(!event.eom());
                    assert_eq!(event.as_body().map(BufQueue::chain_length), Some(128));
                    drop(event);
                    eligibility_wait(&stats.changed, || stats.buffered.get() == 0).await;
                    assert_eq!(&*stats.deltas.borrow(), &[128, -128]);
                    assert!(tokio::time::Instant::now().into_std() < deadline);
                    assert_eq!(wire.borrow().writes, before);
                    assert_eq!(handle.http2_stream_send_credit(stream).unwrap(), 0);
                    let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                    assert_eq!(owners.egress_complete, Some(false));
                    assert_eq!((owners.queued, owners.complete), (1, 0));
                    assert_eq!(upload_probe.source_drops.get(), 1);
                    assert_eq!(wire.borrow().close, FixtureClose::Open);

                    let suspended = response.read_body_event(1).await.unwrap();
                    assert!(!suspended.eom());
                    let mut resume = match suspended.into_kind() {
                        HttpBodyEventKind::Suspend(resume) => resume,
                        _ => panic!("empty nonterminal response must suspend"),
                    };
                    assert!(
                        poll_fn(|cx| Poll::Ready(resume.as_mut().poll(cx)))
                            .await
                            .is_pending()
                    );
                    let read = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline + observation_margin),
                        async {
                            assert_eq!(resume.await, ResumeStatus::Signalled);
                            match response.read_body_event(1).await {
                                Err(error) => error,
                                Ok(_) => {
                                    panic!("blocked upload must fail its pending response read")
                                }
                            }
                        },
                    )
                    .await;
                    let observed = tokio::time::Instant::now().into_std();
                    (read, observed, deadline, control.snapshot.borrow().clone())
                }))
                .await;
            let retired = catch_unwind(AssertUnwindSafe(|| drop(response)));
            drop(cleanup);
            (outcome, retired)
        };
        let (run, (exercise, retired)) =
            tokio::join!(eligibility_catch_unwind(session.run()), exercise,);
        let cleanup_checks = catch_unwind(AssertUnwindSafe(|| {
            drop(handle);
            drop(transport);
            assert_eq!(upload_probe.body_reads.get(), 3);
            assert_eq!(upload_probe.reads_dropped.get(), 4);
            assert_eq!(upload_probe.cancelled_reads.get(), 0);
            assert_eq!(upload_probe.source_drops.get(), 1);
            assert!(wire.borrow().transport_dropped);
        }));
        if let Err(payload) = exercise {
            resume_unwind(payload);
        }
        let (read, observed, deadline, last_snapshot) = exercise
            .unwrap()
            .expect("zero-ACK flow deadline fixture setup completed");
        if let Err(payload) = run {
            resume_unwind(payload);
        }
        if let Err(payload) = retired {
            resume_unwind(payload);
        }
        if let Err(payload) = cleanup_checks {
            resume_unwind(payload);
        }
        let error = read.unwrap_or_else(|_| panic!(
            "zero-ACK response consumption must preserve original deadline {deadline:?}; last snapshot: {last_snapshot:?}",
        ));
        assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
        assert!(observed >= deadline);
        assert!(observed <= deadline + observation_margin);
        assert_eq!(
            run.unwrap().unwrap_err().code(),
            HttpErrorCode::FLOW_CONTROL_ERROR
        );
        assert_eq!(wire.borrow().close, FixtureClose::Reset);
        let decoded = complete_credit_wire(true, &wire);
        assert_eq!(decoded.frames.body_bytes, 65_535);
        assert!(decoded.frames.window_updates.is_empty());
        assert!(decoded.frames.errors.is_empty());
        assert!(!decoded.completed.contains(&stream));
        assert!(
            decoded
                .frames
                .goaways
                .iter()
                .any(|(_, code)| *code == H2ErrorCode::FLOW_CONTROL_ERROR)
        );
    }

    #[test]
    fn zero_ack_response_read_preserves_blocked_upload_flow_deadline() {
        let result = run_on_worker(|context| {
            Box::pin(eligibility_catch_unwind(
                zero_ack_response_read_flow_deadline_case(context),
            ))
        });
        if let Err(payload) = result {
            resume_unwind(payload);
        }
    }
    async fn mixed_zero_ack_priority_flow_deadline_case(context: WorkerContext) {
        tokio::time::pause();
        let stream = StreamId::new(1);
        let mut peer = Http2Codec::new(TransportDirection::Downstream);
        let mut settings = BufQueue::new(4096);
        peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
            .unwrap();
        let mut input = settings.coalesce(settings.chain_length()).unwrap().to_vec();
        let mut message = HttpMessage::new();
        message.set_http_version(1, 1);
        message.set_status_code(200).unwrap();
        input.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
        ));
        input.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Body {
                stream,
                body: queue(&[b'r'; 128]),
                padding: None,
                end_stream: false,
            },
        ));
        let peer_eof = Arc::new(tokio::sync::Notify::new());
        let (transport, wire) = FixtureTransport::new([
            FixtureRead::Data(input),
            FixtureRead::WaitDataEof(Arc::clone(&peer_eof), Vec::new()),
        ]);
        let stats = Rc::new(ZeroAckReadStats::default());
        let mut rate_limits = Http2RateLimitConfig::default();
        for limit in [
            &mut rate_limits.control_frames,
            &mut rate_limits.header_frames,
            &mut rate_limits.resets,
            &mut rate_limits.direct_errors,
        ] {
            limit.interval = Duration::from_secs(600);
        }
        let write_timeout = Duration::from_secs(60);
        let observation_margin = Duration::from_millis(100);
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Upstream,
            transport.clone(),
            None,
            UniplexConfig {
                write_timeout,
                read_timeout: Duration::ZERO,
                stream_read_timeout: Duration::ZERO,
                ..UniplexConfig::default()
            },
            Http2SessionConfig {
                connection_receive_window: 65_535,
                stream_receive_window: 65_535,
                double_goaway_drain: false,
                stats: Some(stats.clone()),
                rate_limits,
                ..Http2SessionConfig::default()
            },
        )
        .unwrap();
        let control = handle.observe_h2_source_timers_for_test(stream).unwrap();
        let cleanup = ZeroAckFlowCleanup {
            control: control.clone(),
            peer_eof,
        };
        let upload_probe = Rc::new(EligibilityProbe::default());
        let upload_gate = EligibilityGate::new(
            EligibilityPhase::Headers,
            Arc::new(tokio::sync::Notify::new()),
        );
        upload_gate.release();
        let upload = EligibilitySource::new(
            EligibilityPhase::Headers,
            upload_gate,
            Rc::clone(&upload_probe),
            Bytes::from(vec![b'u'; 70_000]),
            true,
        );
        let mut response = handle
            .send_request(
                HttpSourceHolder::new(Box::new(upload)),
                CancellationSource::new().token(),
            )
            .await
            .unwrap()
            .source;
        let exercise = async {
            let outcome =
                eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(300), async {
                    let setup = eligibility_flow_snapshot(&control, |snapshot| {
                        snapshot.blocked == Some((vec![stream], true))
                            && snapshot.flow_deadline.is_some()
                    })
                    .await;
                    let mut header = response.read_header_event().await.unwrap();
                    assert!(!header.eom());
                    assert_eq!(header.headers_mut().status_code().unwrap(), 200);
                    drop(header);
                    // Retire the header read's exchange guard before fixing the observed interval.
                    control.pause_next_writer_wait.set(true);
                    let first = eligibility_flow_snapshot(&control, |snapshot| {
                        snapshot.sequence > setup.sequence && control.writer_wait_paused.get()
                    })
                    .await;
                    assert_eq!(first.blocked, Some((vec![stream], true)));
                    assert_eq!((first.connection_credit, first.stream_credit), (0, 0));
                    assert_eq!(first.buffer_timeout, None);
                    let deadline = first.flow_deadline.expect("blocked upload timer");
                    assert_eq!(
                        deadline,
                        (tokio::time::Instant::now() + write_timeout).into_std()
                    );
                    assert_eq!(stats.buffered.get(), 128);
                    assert_eq!(&*stats.deltas.borrow(), &[128]);
                    assert_eq!(upload_probe.body_reads.get(), 3);
                    assert_eq!(upload_probe.reads_dropped.get(), 4);
                    assert_eq!(upload_probe.cancelled_reads.get(), 0);
                    assert_eq!(upload_probe.source_drops.get(), 1);
                    let before = wire.borrow().writes.clone();
                    let decoded = complete_credit_wire(true, &wire);
                    assert_eq!(decoded.frames.body_bytes, 65_535);
                    assert!(decoded.frames.window_updates.is_empty());
                    assert!(decoded.frames.errors.is_empty());
                    assert!(!decoded.completed.contains(&stream));
                    control.resume_writer_wait.notify_one();

                    eligibility_advance_to(deadline - write_timeout / 2).await;
                    let action_sequence = control.snapshot.borrow().as_ref().unwrap().sequence;
                    let updated_deadline = (tokio::time::Instant::now() + write_timeout).into_std();
                    let event = {
                        let mut read = response.read_body_event(128);
                        match poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await {
                            Poll::Ready(Ok(event)) => event,
                            Poll::Ready(Err(error)) => {
                                panic!("buffered response read failed: {error:?}")
                            }
                            Poll::Pending => {
                                panic!("buffered response read must be ready before priority work")
                            }
                        }
                    };
                    assert!(!event.eom());
                    assert_eq!(event.as_body().map(BufQueue::chain_length), Some(128));
                    drop(event);
                    // The outer Ready poll cannot yield between BytesProcessed and this Work signal.
                    assert!(
                        handle
                            .update_http2_priority(
                                stream,
                                HttpPriority::from_urgency_and_incremental(2, false),
                            )
                            .unwrap()
                    );
                    assert_eq!(updated_deadline, deadline + write_timeout / 2);
                    eligibility_wait(&stats.changed, || stats.buffered.get() == 0).await;
                    assert_eq!(&*stats.deltas.borrow(), &[128, -128]);
                    assert!(tokio::time::Instant::now().into_std() < deadline);
                    assert_eq!(wire.borrow().writes, before);
                    assert_eq!(handle.http2_stream_send_credit(stream).unwrap(), 0);
                    let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                    assert_eq!(owners.egress_complete, Some(false));
                    assert_eq!((owners.queued, owners.complete), (1, 0));
                    assert_eq!(upload_probe.source_drops.get(), 1);
                    assert_eq!(wire.borrow().close, FixtureClose::Open);

                    let suspended = response.read_body_event(1).await.unwrap();
                    assert!(!suspended.eom());
                    let mut resume = match suspended.into_kind() {
                        HttpBodyEventKind::Suspend(resume) => resume,
                        _ => panic!("empty nonterminal response must suspend"),
                    };
                    assert!(
                        poll_fn(|cx| Poll::Ready(resume.as_mut().poll(cx)))
                            .await
                            .is_pending()
                    );
                    let rearmed = eligibility_flow_snapshot(&control, |snapshot| {
                        snapshot.sequence > action_sequence && snapshot.flow_deadline.is_some()
                    })
                    .await;
                    assert_eq!(rearmed.flow_deadline, Some(updated_deadline));
                    assert_eq!(rearmed.blocked, first.blocked);
                    assert_eq!((rearmed.connection_credit, rearmed.stream_credit), (0, 0));
                    assert_eq!(rearmed.buffer_timeout, None);
                    let at_original_deadline = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline + observation_margin),
                        resume.as_mut(),
                    )
                    .await;
                    assert!(
                        at_original_deadline.is_err(),
                        "genuine priority work must rearm the wait"
                    );
                    assert!(
                        tokio::time::Instant::now().into_std() >= deadline + observation_margin
                    );
                    assert!(tokio::time::Instant::now().into_std() < updated_deadline);
                    assert_eq!(wire.borrow().close, FixtureClose::Open);
                    assert_eq!(wire.borrow().writes, before);
                    assert_eq!(&*stats.deltas.borrow(), &[128, -128]);
                    assert_eq!(upload_probe.source_drops.get(), 1);
                    let owners = handle.h2_egress_owners_for_test(stream).unwrap();
                    assert_eq!(owners.egress_complete, Some(false));
                    assert_eq!((owners.queued, owners.complete), (1, 0));
                    assert_eq!(handle.http2_stream_send_credit(stream).unwrap(), 0);
                    assert_eq!(
                        control.snapshot.borrow().as_ref().unwrap().flow_deadline,
                        Some(updated_deadline)
                    );
                    let read = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(updated_deadline + observation_margin),
                        async {
                            assert_eq!(resume.await, ResumeStatus::Signalled);
                            match response.read_body_event(1).await {
                                Err(error) => error,
                                Ok(_) => {
                                    panic!("blocked upload must fail its pending response read")
                                }
                            }
                        },
                    )
                    .await;
                    let observed = tokio::time::Instant::now().into_std();
                    (
                        read,
                        observed,
                        updated_deadline,
                        control.snapshot.borrow().clone(),
                    )
                }))
                .await;
            let retired = catch_unwind(AssertUnwindSafe(|| drop(response)));
            drop(cleanup);
            (outcome, retired)
        };
        let (run, (exercise, retired)) =
            tokio::join!(eligibility_catch_unwind(session.run()), exercise,);
        let cleanup_checks = catch_unwind(AssertUnwindSafe(|| {
            drop(handle);
            drop(transport);
            assert_eq!(upload_probe.body_reads.get(), 3);
            assert_eq!(upload_probe.reads_dropped.get(), 4);
            assert_eq!(upload_probe.cancelled_reads.get(), 0);
            assert_eq!(upload_probe.source_drops.get(), 1);
            assert!(wire.borrow().transport_dropped);
        }));
        if let Err(payload) = exercise {
            resume_unwind(payload);
        }
        let (read, observed, updated_deadline, last_snapshot) = exercise
            .unwrap()
            .expect("mixed zero-ACK/priority flow deadline fixture setup completed");
        if let Err(payload) = run {
            resume_unwind(payload);
        }
        if let Err(payload) = retired {
            resume_unwind(payload);
        }
        if let Err(payload) = cleanup_checks {
            resume_unwind(payload);
        }
        let error = read.unwrap_or_else(|_| panic!(
            "mixed zero-ACK/priority work must fail by its rearmed deadline {updated_deadline:?}; last snapshot: {last_snapshot:?}",
        ));
        assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
        assert!(observed >= updated_deadline);
        assert!(observed <= updated_deadline + observation_margin);
        assert_eq!(
            run.unwrap().unwrap_err().code(),
            HttpErrorCode::FLOW_CONTROL_ERROR
        );
        assert_eq!(wire.borrow().close, FixtureClose::Reset);
        let decoded = complete_credit_wire(true, &wire);
        assert_eq!(decoded.frames.body_bytes, 65_535);
        assert!(decoded.frames.window_updates.is_empty());
        assert!(decoded.frames.errors.is_empty());
        assert!(!decoded.completed.contains(&stream));
        assert!(
            decoded
                .frames
                .goaways
                .iter()
                .any(|(_, code)| *code == H2ErrorCode::FLOW_CONTROL_ERROR)
        );
    }

    #[test]
    fn zero_ack_response_read_with_priority_work_rearms_blocked_upload_flow_deadline() {
        let result = run_on_worker(|context| {
            Box::pin(eligibility_catch_unwind(
                mixed_zero_ack_priority_flow_deadline_case(context),
            ))
        });
        if let Err(payload) = result {
            resume_unwind(payload);
        }
    }

    #[derive(Default)]
    struct DecodedServerFrames {
        resets: Vec<(StreamId, H2ErrorCode)>,
        goaways: Vec<(StreamId, H2ErrorCode)>,
        control_frame_order: Vec<(u64, u8)>,
        header_goaway_order: Vec<(StreamId, u64)>,
        wire_offset: u64,
        header_end_offsets: Vec<(StreamId, u64)>,
        header_frame_lengths: Vec<(StreamId, u64)>,
        errors: Vec<(StreamId, HttpException)>,
        ping_requests: Vec<u64>,
        ping_replies: Vec<u64>,
        settings_acks: usize,
        settings: Vec<roxygen_codec::CodecSetting>,
        window_updates: Vec<(StreamId, u32)>,
        header_streams: Vec<StreamId>,
        header_statuses: Vec<(StreamId, u16)>,
        body_streams: Vec<StreamId>,
        body_bytes: usize,
        body_data: Vec<u8>,
        padding: u64,
        pushes: Vec<(StreamId, StreamId)>,
    }

    impl HttpCodecCallback for DecodedServerFrames {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            if let CodecEvent::FrameHeader {
                stream,
                frame_type,
                length,
                ..
            } = &event
            {
                self.wire_offset += FRAME_HEADER_SIZE as u64 + *length;
                if *frame_type == u64::from(FrameType::HEADERS.as_u8()) {
                    self.header_end_offsets.push((*stream, self.wire_offset));
                    self.header_frame_lengths
                        .push((*stream, FRAME_HEADER_SIZE as u64 + *length));
                }
            }
            if let CodecEvent::FrameHeader {
                stream, frame_type, ..
            } = &event
                && (*frame_type == u64::from(FrameType::HEADERS.as_u8())
                    || *frame_type == u64::from(FrameType::GOAWAY.as_u8()))
            {
                self.header_goaway_order.push((*stream, *frame_type));
            }
            match event {
                CodecEvent::Error { stream, error, .. } => {
                    self.errors.push((stream, error.clone()));
                }
                CodecEvent::Abort { stream, code } => self.resets.push((stream, code)),
                CodecEvent::Goaway {
                    last_good_stream,
                    code,
                    ..
                } => self.goaways.push((last_good_stream, code)),
                CodecEvent::PingRequest { data } => self.ping_requests.push(data),
                CodecEvent::PingReply { data } => self.ping_replies.push(data),
                CodecEvent::SettingsAck => {
                    self.settings_acks = self.settings_acks.saturating_add(1)
                }
                CodecEvent::Settings { settings } => {
                    self.settings.extend_from_slice(&settings);
                }
                CodecEvent::WindowUpdate { stream, amount } => {
                    self.window_updates.push((stream, amount));
                }
                CodecEvent::HeadersComplete {
                    stream,
                    mut message,
                } => {
                    self.header_streams.push(stream);
                    if let Ok(status) = message.status_code() {
                        self.header_statuses.push((stream, status));
                    }
                }
                CodecEvent::Body {
                    stream,
                    body,
                    padding,
                } => {
                    self.body_streams.push(stream);
                    self.body_bytes = self.body_bytes.saturating_add(body.chain_length());
                    let length = body.chain_length();
                    self.body_data.extend_from_slice(
                        body.coalesce(length)
                            .expect("decoded body coalesces")
                            .as_ref(),
                    );
                    self.padding = self.padding.saturating_add(u64::from(padding));
                }
                CodecEvent::FrameHeader {
                    frame_type, flags, ..
                } if frame_type == u64::from(FrameType::GOAWAY.as_u8())
                    || frame_type == u64::from(FrameType::PING.as_u8()) =>
                {
                    self.control_frame_order.push((frame_type, flags));
                }
                CodecEvent::FrameHeader {
                    frame_type, length, ..
                } if frame_type == u64::from(FrameType::PADDING.as_u8()) => {
                    self.padding = self
                        .padding
                        .saturating_add(length.saturating_add(FRAME_HEADER_SIZE as u64));
                }
                CodecEvent::PushMessageBegin {
                    stream,
                    associated_stream,
                    ..
                } => self.pushes.push((stream, associated_stream)),
                _ => {}
            }
        }
    }

    fn decode_server_frames(wire: &[u8]) -> DecodedServerFrames {
        decode_server_frames_with_requests(wire, 1)
    }

    fn decode_server_frames_with_requests(
        wire: &[u8],
        local_requests: usize,
    ) -> DecodedServerFrames {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        codec.egress_settings_mut().set(SettingsId::EnablePush, 1);
        for ordinal in 0..local_requests {
            assert_eq!(
                codec.create_stream(),
                StreamId::new((ordinal as u64).saturating_mul(2).saturating_add(1))
            );
        }
        let mut decoded = DecodedServerFrames::default();
        let mut output = BufQueue::new(1 << 20);
        let input = BufQueue::copy_from_slice(wire, wire.len()).expect("frame input fits");
        let result = codec.on_ingress(&input, &mut decoded, &mut output);
        assert_eq!(result.consumed, wire.len());
        assert!(result.egress_results.iter().all(Result::is_ok));
        decoded
    }

    fn decode_client_frames(wire: &[u8]) -> DecodedServerFrames {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut decoded = DecodedServerFrames {
            wire_offset: CONNECTION_PREFACE.len() as u64,
            ..DecodedServerFrames::default()
        };
        let mut output = BufQueue::new(1 << 20);
        let input = BufQueue::copy_from_slice(wire, wire.len()).expect("frame input fits");
        let result = codec.on_ingress(&input, &mut decoded, &mut output);
        assert_eq!(result.consumed, wire.len());
        assert!(result.egress_results.iter().all(Result::is_ok));
        decoded
    }

    fn assert_real_local_and_peer_ping_lifecycle() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::DataEof(peer_ping_exchange_bytes())]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                let local_token = handle.send_ping().expect("local ping queued");
                session.run().await.expect("ping session completes");
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 0);
                assert_eq!(decoded.ping_requests, [local_token]);
                assert_eq!(decoded.ping_replies, [12_345]);
                let events = events.borrow();
                assert!(
                    !events
                        .iter()
                        .any(|event| matches!(event, LifecycleEvent::Http2PingRequest { .. }))
                );
                let reply_sent = events
                    .iter()
                    .position(|event| {
                        *event
                            == LifecycleEvent::Http2PingReplySent {
                                latency: Duration::ZERO,
                            }
                    })
                    .expect("peer ping reply-sent lifecycle");
                let reply_received = events
                    .iter()
                    .position(|event| *event == LifecycleEvent::Http2PingReplyReceived)
                    .expect("local ping reply-received lifecycle");
                assert!(reply_sent < reply_received);
            })
        });
    }

    fn assert_real_peer_reset_is_not_echoed(code: H2ErrorCode) {
        let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::Reset(code));
        let outcome = run_downstream_bytes(input, 200, 0);
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.invocations, 1);
        assert!(decode_server_frames(&outcome.wire).resets.is_empty());
    }

    fn assert_real_peer_no_error_reset_cancels_ingress() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (initial, reset) = peer_incomplete_get_then_reset_parts(H2ErrorCode::NO_ERROR);
                let release_reset = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitDataEof(Arc::clone(&release_reset), reset),
                ]);
                let observed = Rc::new(Cell::new(None));
                let handler: Rc<dyn HttpHandler> = Rc::new(TimeoutReadingHandler {
                    observed: Rc::clone(&observed),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                tokio::task::spawn_local(async move {
                    tokio::task::yield_now().await;
                    release_reset.notify_one();
                });
                session
                    .run()
                    .await
                    .expect("peer reset remains stream-local");
                assert_eq!(observed.get(), Some(HttpErrorCode::CANCEL));
                assert!(
                    decode_server_frames(&state.borrow().writes.concat())
                        .resets
                        .is_empty()
                );
            })
        });
    }

    fn assert_real_peer_goaway(code: H2ErrorCode) {
        let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::Goaway(code));
        let outcome = run_downstream_bytes(input, 200, 0);
        if code == H2ErrorCode::NO_ERROR {
            assert_eq!(outcome.error, None);
            assert_eq!(
                decode_server_frames(&outcome.wire).goaways,
                [(StreamId::new(1), H2ErrorCode::NO_ERROR)]
            );
        } else {
            assert_eq!(outcome.error, Some(HttpErrorCode::INTERNAL_ERROR));
            assert_eq!(outcome.close, FixtureClose::Reset);
        }
    }

    fn assert_real_downstream_peer_goaway_without_streams() {
        run_on_worker(|context| {
            Box::pin(async move {
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(
                    peer_idle_goaway_bytes(H2ErrorCode::NO_ERROR),
                )]);
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("idle peer GOAWAY did not close session")
                    .expect("idle peer GOAWAY is graceful");
                assert_eq!(invocations.get(), 0);
                assert_eq!(
                    decode_server_frames(&state.borrow().writes.concat()).goaways,
                    [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
                );
            })
        });
    }

    pub(super) fn assert_goaway_callback_control_order() {
        struct GoawayOrderObserver {
            events: Rc<RefCell<Vec<LifecycleEvent>>>,
            handle: UniplexHandle,
        }

        impl LifecycleObserver for GoawayOrderObserver {
            fn on_event(&self, event: LifecycleEvent) {
                self.events.borrow_mut().push(event);
                if event == LifecycleEvent::DrainStarted(DrainTrigger::PeerGoaway) {
                    assert!(!self.handle.initiate_drain().expect("reentrant drain"));
                    assert!(self.handle.reserve_request().is_err());
                }
            }
        }

        for direction in [TransportDirection::Upstream, TransportDirection::Downstream] {
            let (result, wire, events, close, dropped, invocations, input_length) =
                run_on_worker(move |context| {
                    Box::pin(async move {
                        let peer_direction = if direction.is_upstream() {
                            TransportDirection::Downstream
                        } else {
                            TransportDirection::Upstream
                        };
                        let mut codec = Http2Codec::new(peer_direction);
                        let mut callback = IgnoreCodecEvents;
                        let mut input = BufQueue::new(256);
                        codec
                            .generate(&mut input, EgressFrame::ConnectionPreface, &mut callback)
                            .expect("peer preface");
                        codec
                            .generate_configured_settings(&mut input, &mut callback)
                            .expect("peer settings");
                        for frame in [
                            EgressFrame::PingRequest { data: Some(11) },
                            EgressFrame::Goaway {
                                last_stream: StreamId::new(0x7fff_ffff),
                                code: H2ErrorCode::NO_ERROR,
                                debug_data: None,
                            },
                            EgressFrame::PingRequest { data: Some(22) },
                            EgressFrame::Goaway {
                                last_stream: StreamId::UNSPECIFIED,
                                code: H2ErrorCode::NO_ERROR,
                                debug_data: None,
                            },
                            EgressFrame::PingRequest { data: Some(33) },
                        ] {
                            codec
                                .generate(&mut input, frame, &mut callback)
                                .expect("peer control");
                        }
                        let input_length = input.chain_length();
                        let (transport, state) =
                            FixtureTransport::new([FixtureRead::SegmentedData(input)]);
                        let invocations = Rc::new(Cell::new(0));
                        let handler = direction.is_downstream().then(|| {
                            Rc::new(FixtureHandler {
                                status: 200,
                                response_body: 0,
                                response_error: None,
                                invocations: Rc::clone(&invocations),
                                request_body: Rc::new(Cell::new(0)),
                            }) as Rc<dyn HttpHandler>
                        });
                        let (session, handle) = UniplexSession::new_http2(
                            context,
                            direction,
                            transport.clone(),
                            handler,
                            UniplexConfig::default(),
                            Http2SessionConfig::default(),
                        )
                        .expect("H2 control-order session");
                        let events = Rc::new(RefCell::new(Vec::new()));
                        session
                            .add_lifecycle_observer(Rc::new(GoawayOrderObserver {
                                events: Rc::clone(&events),
                                handle,
                            }))
                            .expect("control-order observer");
                        let result =
                            tokio::time::timeout(Duration::from_secs(1), session.run()).await;
                        let wire = state.borrow().writes.concat();
                        let events = events.borrow().clone();
                        let close = state.borrow().close;
                        drop(transport);
                        let dropped = state.borrow().transport_dropped;
                        (
                            result,
                            wire,
                            events,
                            close,
                            dropped,
                            invocations.get(),
                            input_length,
                        )
                    })
                });
            let summary = result
                .expect("control-order session timed out")
                .expect("healthy peer controls complete successfully");
            assert_eq!(summary.bytes_read, input_length as u64);
            assert_eq!(summary.bytes_written, wire.len() as u64);
            assert!(!summary.reset);
            assert_eq!(close, FixtureClose::ShutdownWrite);
            assert!(dropped);
            assert_eq!(invocations, 0);
            let decoded = if direction.is_upstream() {
                decode_client_frames(&wire)
            } else {
                decode_server_frames_with_requests(&wire, 0)
            };
            assert!(
                decoded.errors.is_empty(),
                "codec errors: {:?}",
                decoded.errors
            );
            assert!(decoded.resets.is_empty());
            assert!(decoded.ping_requests.is_empty());
            assert_eq!(decoded.ping_replies, [11, 22, 33]);
            assert_eq!(
                decoded.goaways,
                [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
            );
            assert_eq!(
                decoded.control_frame_order,
                [
                    (u64::from(FrameType::PING.as_u8()), 1),
                    (u64::from(FrameType::GOAWAY.as_u8()), 0),
                    (u64::from(FrameType::PING.as_u8()), 1),
                    (u64::from(FrameType::PING.as_u8()), 1),
                ]
            );
            let controls: Vec<_> = events
                .iter()
                .copied()
                .filter(|event| {
                    matches!(
                        event,
                        LifecycleEvent::Http2Goaway { .. }
                            | LifecycleEvent::DrainStarted(_)
                            | LifecycleEvent::Http2PingReplySent { .. }
                    )
                })
                .collect();
            let reply = LifecycleEvent::Http2PingReplySent {
                latency: Duration::ZERO,
            };
            assert_eq!(
                controls,
                [
                    reply,
                    LifecycleEvent::Http2Goaway {
                        last_good_stream: StreamId::new(0x7fff_ffff),
                        code: HttpErrorCode::NO_ERROR,
                    },
                    LifecycleEvent::DrainStarted(DrainTrigger::PeerGoaway),
                    reply,
                    LifecycleEvent::Http2Goaway {
                        last_good_stream: StreamId::UNSPECIFIED,
                        code: HttpErrorCode::NO_ERROR,
                    },
                    reply,
                ]
            );
            assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
        }
    }

    pub(super) fn assert_first_wire_maximum_goaway_retires_idle_upstream() {
        for last_stream in [StreamId::new(0x7fff_ffff), StreamId::new(1)] {
            let (result, wire, close, dropped) = run_on_worker(move |context| {
                Box::pin(async move {
                    let input = server_goaway_bytes(last_stream, H2ErrorCode::NO_ERROR, None);
                    let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                    let (session, _) = UniplexSession::new_http2(
                        context,
                        TransportDirection::Upstream,
                        transport.clone(),
                        None,
                        UniplexConfig::default(),
                        Http2SessionConfig::default(),
                    )
                    .expect("idle upstream H2 session");
                    let result = tokio::time::timeout(Duration::from_secs(1), session.run()).await;
                    let wire = state.borrow().writes.concat();
                    let close = state.borrow().close;
                    drop(transport);
                    let dropped = state.borrow().transport_dropped;
                    (result, wire, close, dropped)
                })
            });
            let summary = result
                .expect("first GOAWAY must retire idle upstream without EOF")
                .expect("healthy idle peer GOAWAY succeeds");
            assert!(!summary.reset);
            assert_eq!(summary.bytes_written, wire.len() as u64);
            assert_eq!(close, FixtureClose::ShutdownWrite);
            assert!(dropped);
            let decoded = decode_client_frames(&wire);
            assert!(
                decoded.errors.is_empty(),
                "codec errors: {:?}",
                decoded.errors
            );
            assert_eq!(
                decoded.goaways,
                [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
            );
            assert_eq!(
                decoded.control_frame_order,
                [(u64::from(FrameType::GOAWAY.as_u8()), 0)]
            );
        }
    }

    pub(super) fn assert_incoming_wire_maximum_goaway_keeps_timed_final_drain() {
        fn decode_maximum_stream(wire: &[u8]) -> DecodedServerFrames {
            let mut codec = Http2Codec::new(TransportDirection::Upstream);
            codec.set_next_egress_stream_id(0x7fff_ffff);
            assert_eq!(codec.create_stream(), StreamId::new(0x7fff_ffff));
            let mut decoded = DecodedServerFrames::default();
            let mut output = BufQueue::new(1 << 20);
            let result = codec.on_ingress(&queue(wire), &mut decoded, &mut output);
            assert_eq!(result.consumed, wire.len());
            assert!(result.egress_results.iter().all(Result::is_ok));
            assert!(
                decoded.errors.is_empty(),
                "codec errors: {:?}",
                decoded.errors
            );
            decoded
        }

        let (run, exercise, wire, events, close, dropped) = run_on_worker(|context| {
            Box::pin(async move {
                tokio::time::pause();
                let maximum_stream = StreamId::new(0x7fff_ffff);
                let mut peer = Http2Codec::new(TransportDirection::Upstream);
                peer.set_next_egress_stream_id(0x7fff_ffff);
                assert_eq!(peer.create_stream(), maximum_stream);
                let mut initial = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
                let mut settings = BufQueue::new(256);
                peer.generate_configured_settings(&mut settings, &mut IgnoreCodecEvents)
                    .expect("maximum-stream peer settings");
                initial.extend_from_slice(
                    &settings
                        .coalesce(settings.chain_length())
                        .expect("peer settings bytes"),
                );
                let mut request = roxygen_core::message::HttpMessage::new();
                request.set_method(HttpMethod::Post).expect("POST method");
                request
                    .set_url(b"/retained-maximum", false)
                    .expect("request URL");
                request.set_http_version(1, 1);
                request
                    .headers_mut()
                    .add(HttpHeaderCode::Host, b"example.com");
                request
                    .headers_mut()
                    .add(HttpHeaderCode::ContentLength, b"1");
                initial.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Headers {
                        stream: maximum_stream,
                        message: &request,
                        end_stream: false,
                        extra_headers: None,
                    },
                ));
                let mut controls = eligibility_frame(
                    &mut peer,
                    EgressFrame::Goaway {
                        last_stream: StreamId::UNSPECIFIED,
                        code: H2ErrorCode::NO_ERROR,
                        debug_data: None,
                    },
                );
                controls.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::PingRequest { data: Some(44) },
                ));
                let body = eligibility_frame(
                    &mut peer,
                    EgressFrame::Body {
                        stream: maximum_stream,
                        body: queue(b"x"),
                        padding: None,
                        end_stream: true,
                    },
                );
                let release_goaway = Arc::new(tokio::sync::Notify::new());
                let release_body = Arc::new(tokio::sync::Notify::new());
                let changed = Arc::new(tokio::sync::Notify::new());
                let (inner, state) = FixtureTransport::new([
                    FixtureRead::SegmentedData(queue(&initial)),
                    FixtureRead::WaitData(Arc::clone(&release_goaway), controls),
                    FixtureRead::WaitData(Arc::clone(&release_body), body),
                ]);
                let transport = Rc::new(EligibilityTransport {
                    inner,
                    changed: Arc::clone(&changed),
                    reads_started: Cell::new(0),
                    backing_start: 0,
                    backing_length: 0,
                    shared_written: Cell::new(0),
                    backing_drops: Arc::default(),
                });
                let held = Rc::new(RefCell::new(None));
                let invocations = Rc::new(Cell::new(0));
                let goaway_timeout = Duration::from_millis(5);
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport.clone(),
                    Some(Rc::new(HoldFirstRequestHandler {
                        invocations: Rc::clone(&invocations),
                        held: Rc::clone(&held),
                    })),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout,
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("retained maximum-stream session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("maximum-stream lifecycle observer");
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(1),
                        async {
                            eligibility_wait(&changed, || {
                                held.borrow().is_some()
                                    && decode_maximum_stream(&state.borrow().writes.concat())
                                        .header_statuses
                                        == [(maximum_stream, 200)]
                            })
                            .await;
                            assert_eq!(invocations.get(), 1);
                            assert!(events.borrow().iter().any(|event| {
                                matches!(event, LifecycleEvent::RequestBegin { stream, .. }
                                    if *stream == maximum_stream)
                            }));
                            release_goaway.notify_one();
                            eligibility_wait(&changed, || {
                                decode_maximum_stream(&state.borrow().writes.concat()).ping_replies
                                    == [44]
                            })
                            .await;
                            let first = decode_maximum_stream(&state.borrow().writes.concat());
                            assert_eq!(first.goaways, [(maximum_stream, H2ErrorCode::NO_ERROR)]);
                            assert_eq!(
                                first.control_frame_order,
                                [
                                    (u64::from(FrameType::GOAWAY.as_u8()), 0),
                                    (u64::from(FrameType::PING.as_u8()), 1),
                                ]
                            );
                            assert!(held.borrow().is_some());
                            assert_eq!(state.borrow().close, FixtureClose::Open);
                            let after_deadline =
                                tokio::time::Instant::from_std(Instant::now() + goaway_timeout)
                                    + Duration::from_millis(1);
                            let advance_by = after_deadline
                                .saturating_duration_since(tokio::time::Instant::now());
                            tokio::time::advance(advance_by).await;
                            eligibility_wait(&changed, || {
                                decode_maximum_stream(&state.borrow().writes.concat())
                                    .control_frame_order
                                    .iter()
                                    .filter(|(frame_type, _)| {
                                        *frame_type == u64::from(FrameType::GOAWAY.as_u8())
                                    })
                                    .count()
                                    >= 2
                            })
                            .await;
                            let second = decode_maximum_stream(&state.borrow().writes.concat());
                            assert_eq!(
                                second.control_frame_order,
                                [
                                    (u64::from(FrameType::GOAWAY.as_u8()), 0),
                                    (u64::from(FrameType::PING.as_u8()), 1),
                                    (u64::from(FrameType::GOAWAY.as_u8()), 0),
                                ]
                            );
                            assert_eq!(second.goaways, [(maximum_stream, H2ErrorCode::NO_ERROR)]);
                            assert!(second.resets.is_empty());
                            assert!(held.borrow().is_some());
                            assert_eq!(state.borrow().close, FixtureClose::Open);
                            assert!(!events.borrow().iter().any(|event| matches!(
                                event,
                                LifecycleEvent::TransactionDetached { .. }
                            )));
                            release_body.notify_one();
                            let mut request = held.borrow_mut().take().expect("retained request");
                            loop {
                                let body = request
                                    .read_body_event(u32::MAX)
                                    .await
                                    .expect("request body");
                                let eom = body.eom();
                                match body.into_kind() {
                                    HttpBodyEventKind::Suspend(resume) => {
                                        assert_eq!(resume.await, ResumeStatus::Signalled);
                                    }
                                    HttpBodyEventKind::Body(body) => {
                                        assert!(eom);
                                        assert_eq!(body.chain_length(), 1);
                                        assert_eq!(
                                            body.coalesce(1).expect("request byte").as_ref(),
                                            b"x"
                                        );
                                        break;
                                    }
                                    _ => panic!("unexpected retained request body event"),
                                }
                            }
                            drop(request);
                        },
                    ))
                    .await;
                    tokio::time::resume();
                    release_goaway.notify_one();
                    release_body.notify_one();
                    drop(held.borrow_mut().take());
                    outcome
                };
                let (run, exercise) = tokio::join!(
                    tokio::time::timeout(Duration::from_secs(1), session.run()),
                    exercise,
                );
                let wire = state.borrow().writes.concat();
                let events = events.borrow().clone();
                let close = state.borrow().close;
                drop(transport);
                let dropped = state.borrow().transport_dropped;
                (run, exercise, wire, events, close, dropped)
            })
        });
        let summary = run
            .expect("maximum-stream session cleanup timed out")
            .expect("maximum-stream session completes successfully");
        exercise
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            .expect("maximum-stream timed drain exercise timed out");
        assert!(!summary.reset);
        assert_eq!(summary.bytes_written, wire.len() as u64);
        assert_eq!(close, FixtureClose::ShutdownWrite);
        assert!(dropped);
        assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
        let decoded = decode_maximum_stream(&wire);
        assert!(decoded.resets.is_empty());
        assert_eq!(decoded.header_statuses, [(StreamId::new(0x7fff_ffff), 200)]);
        assert_eq!(decoded.ping_replies, [44]);
        assert_eq!(
            decoded.control_frame_order,
            [
                (u64::from(FrameType::GOAWAY.as_u8()), 0),
                (u64::from(FrameType::PING.as_u8()), 1),
                (u64::from(FrameType::GOAWAY.as_u8()), 0),
            ]
        );
    }

    fn assert_real_upstream_peer_goaway_without_streams() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = server_goaway_bytes(StreamId::new(1), H2ErrorCode::NO_ERROR, None);
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("peer GOAWAY did not close idle upstream session")
                    .expect("NO_ERROR GOAWAY is graceful");
                assert_eq!(
                    decode_client_frames(&state.borrow().writes.concat()).goaways,
                    [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
                );
            })
        });
    }

    fn assert_real_upstream_peer_goaway_with_open_stream() {
        run_on_worker(|context| {
            Box::pin(async move {
                let release = Arc::new(tokio::sync::Notify::new());
                let input = server_goaway_bytes(StreamId::new(1), H2ErrorCode::NO_ERROR, Some(100));
                let (transport, _) =
                    FixtureTransport::new([FixtureRead::WaitData(Arc::clone(&release), input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut run = Box::pin(session.run());
                let first = HttpFixedSource::make_get_request(b"/one", None).expect("first GET");
                let second = HttpFixedSource::make_get_request(b"/two", None).expect("second GET");
                let mut first_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(first)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("first request")
                    .source;
                let mut second_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(second)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("second request")
                    .source;
                release.notify_one();
                let consume = async move {
                    let headers = first_response
                        .read_header_event()
                        .await
                        .expect("accepted response headers");
                    assert!(!headers.eom());
                    let body = first_response
                        .read_body_event(u32::MAX)
                        .await
                        .expect("accepted response body");
                    assert_eq!(body.as_body().expect("body event").chain_length(), 100);
                    assert!(body.eom());
                    let error = match second_response.read_header_event().await {
                        Ok(_) => panic!("stream beyond peer GOAWAY was answered"),
                        Err(error) => error,
                    };
                    assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                };
                let (result, ()) = tokio::join!(run.as_mut(), consume);
                result.expect("accepted stream completed after GOAWAY");
            })
        });
    }

    fn assert_real_upstream_peer_goaway_error(before_request_egress: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let release = Arc::new(tokio::sync::Notify::new());
                let input =
                    server_goaway_bytes(StreamId::new(1), H2ErrorCode::PROTOCOL_ERROR, None);
                let (transport, _) =
                    FixtureTransport::new([FixtureRead::WaitData(Arc::clone(&release), input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut run = Box::pin(session.run());
                if before_request_egress {
                    release.notify_one();
                    let run_error = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                        .await
                        .expect("error GOAWAY processing timed out")
                        .expect_err("error GOAWAY fails the session");
                    assert_eq!(run_error.code(), HttpErrorCode::PROTOCOL_ERROR);
                    let request = HttpFixedSource::make_fixed_request(
                        b"/",
                        HttpMethod::Post,
                        Some(queue(&[b'p'; 25_000])),
                    )
                    .expect("POST request");
                    let error = match handle
                        .send_request(
                            HttpSourceHolder::new(Box::new(request)),
                            CancellationSource::new().token(),
                        )
                        .await
                    {
                        Ok(_) => panic!("request admitted after error GOAWAY"),
                        Err(error) => error,
                    };
                    assert_eq!(error.code(), HttpErrorCode::DROPPED);
                } else {
                    let request =
                        HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                    let mut response = handle
                        .send_request(
                            HttpSourceHolder::new(Box::new(request)),
                            CancellationSource::new().token(),
                        )
                        .await
                        .expect("request admitted")
                        .source;
                    release.notify_one();
                    let (run_result, header_result) =
                        tokio::join!(run.as_mut(), response.read_header_event());
                    let error = match header_result {
                        Ok(_) => panic!("error GOAWAY produced response headers"),
                        Err(error) => error,
                    };
                    assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
                    let run_error = run_result.expect_err("error GOAWAY fails the session");
                    assert_eq!(run_error.code(), HttpErrorCode::PROTOCOL_ERROR);
                }
            })
        });
    }

    fn assert_real_egress_error_resets_stream(code: HttpErrorCode) {
        let outcome = run_downstream_handler_error(code);
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.invocations, 1);
        assert_eq!(
            decode_server_frames(&outcome.wire).resets,
            [(StreamId::new(1), H2ErrorCode::CANCEL)]
        );
    }

    struct LifecycleProbe {
        events: Rc<RefCell<Vec<LifecycleEvent>>>,
    }

    impl LifecycleObserver for LifecycleProbe {
        fn on_event(&self, event: LifecycleEvent) {
            self.events.borrow_mut().push(event);
        }
    }

    struct RegisteredLifecycleProbe {
        attached: Cell<usize>,
        events: Rc<RefCell<Vec<LifecycleEvent>>>,
        registration: RefCell<Option<LifecycleObserverRegistration>>,
    }

    impl RegisteredLifecycleProbe {
        fn new(events: Rc<RefCell<Vec<LifecycleEvent>>>) -> Self {
            Self {
                attached: Cell::new(0),
                events,
                registration: RefCell::new(None),
            }
        }

        fn remove(&self) -> bool {
            self.registration
                .borrow()
                .as_ref()
                .is_some_and(LifecycleObserverRegistration::remove)
        }
    }

    impl LifecycleObserver for RegisteredLifecycleProbe {
        fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
            self.attached.set(self.attached.get().saturating_add(1));
            self.registration.replace(Some(registration));
        }

        fn on_event(&self, event: LifecycleEvent) {
            self.events.borrow_mut().push(event);
        }
    }

    struct SelfRemovingAttachedObserver {
        attached: Cell<usize>,
        removed: Cell<bool>,
        events: Rc<RefCell<Vec<LifecycleEvent>>>,
    }

    impl LifecycleObserver for SelfRemovingAttachedObserver {
        fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
            self.attached.set(self.attached.get().saturating_add(1));
            self.removed.set(registration.remove());
        }

        fn on_event(&self, event: LifecycleEvent) {
            self.events.borrow_mut().push(event);
        }
    }

    struct AddOnAttachedObserver {
        attached: Cell<usize>,
        events: Rc<RefCell<Vec<LifecycleEvent>>>,
        registration: RefCell<Option<LifecycleObserverRegistration>>,
        added: Rc<dyn LifecycleObserver>,
        added_once: Cell<bool>,
    }

    impl LifecycleObserver for AddOnAttachedObserver {
        fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
            self.attached.set(self.attached.get().saturating_add(1));
            self.registration.replace(Some(registration));
        }

        fn on_event(&self, event: LifecycleEvent) {
            self.events.borrow_mut().push(event);
            if event == LifecycleEvent::TransactionAttached && !self.added_once.replace(true) {
                let registration = self
                    .registration
                    .borrow()
                    .as_ref()
                    .cloned()
                    .expect("observer is attached before event delivery");
                registration.add_observer(Rc::clone(&self.added));
            }
        }
    }

    struct RemoveOnEventObserver {
        attached: Cell<usize>,
        events: Rc<RefCell<Vec<LifecycleEvent>>>,
        registration: RefCell<Option<LifecycleObserverRegistration>>,
        target: Rc<RegisteredLifecycleProbe>,
        handle: UniplexHandle,
        remove_on: fn(&LifecycleEvent) -> bool,
        removed: Cell<bool>,
    }

    impl LifecycleObserver for RemoveOnEventObserver {
        fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
            self.attached.set(self.attached.get().saturating_add(1));
            self.registration.replace(Some(registration));
        }

        fn on_event(&self, event: LifecycleEvent) {
            self.events.borrow_mut().push(event);
            if (self.remove_on)(&event) && !self.removed.get() {
                assert_eq!(
                    self.handle.protocol().expect("reentrant protocol query"),
                    CodecProtocol::Http2
                );
                self.removed.set(self.target.remove());
            }
        }
    }

    struct ContextHoldingHandler {
        held: Rc<RefCell<Option<HandlerSessionContext>>>,
    }

    impl HttpHandler for ContextHoldingHandler {
        fn handle_request(
            &self,
            context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let held = Rc::clone(&self.held);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                held.replace(Some(context));
                let response =
                    HttpFixedSource::make_fixed_response(200, None).expect("fixed response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    struct GracefulEarlyResponseHandler {
        status: u16,
        fail: bool,
        invocations: Rc<Cell<usize>>,
    }

    struct StallWriteHandler {
        transport: Rc<RefCell<FixtureTransportState>>,
        invocations: Rc<Cell<usize>>,
    }

    impl HttpHandler for StallWriteHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let transport = Rc::clone(&self.transport);
            let invocations = Rc::clone(&self.invocations);
            Box::pin(async move {
                invocations.set(invocations.get().saturating_add(1));
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                transport.borrow_mut().stall_writes = true;
                let response = HttpFixedSource::make_fixed_response(200, Some(queue(b"body")))
                    .expect("fixed response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    struct ContinueThenPendingSource {
        first: bool,
    }

    impl HttpSource for ContinueThenPendingSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            if self.first {
                self.first = false;
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(100).expect("continue status");
                message.set_http_version(1, 1);
                let event = HttpHeaderEvent::new(message, false).expect("continue headers");
                return Box::pin(std::future::ready(Ok(event)));
            }
            Box::pin(std::future::pending())
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::PROTOCOL_ERROR,
                b"informational response has no body",
            ))))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    struct TimeoutReadingHandler {
        observed: Rc<Cell<Option<HttpErrorCode>>>,
    }

    impl HttpHandler for TimeoutReadingHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let observed = Rc::clone(&self.observed);
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(!headers.eom());
                tokio::task::spawn_local(async move {
                    loop {
                        match request.read_body_event(u32::MAX).await {
                            Ok(event) => {
                                if let HttpBodyEventKind::Suspend(resume) = event.into_kind() {
                                    let _ = resume.await;
                                }
                            }
                            Err(error) => {
                                observed.set(Some(error.code()));
                                break;
                            }
                        }
                    }
                });
                Ok(HttpSourceHolder::new(Box::new(ContinueThenPendingSource {
                    first: true,
                })))
            })
        }
    }

    impl HttpHandler for GracefulEarlyResponseHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let status = self.status;
            let fail = self.fail;
            let invocations = Rc::clone(&self.invocations);
            Box::pin(async move {
                invocations.set(invocations.get().saturating_add(1));
                let headers = request.read_header_event().await?;
                assert!(!headers.eom());
                if fail {
                    request.stop_reading(None);
                    return Err(HttpError::with_message(
                        HttpErrorCode::REFUSED_STREAM,
                        b"handler declined the request body",
                    ));
                }
                let response =
                    HttpFixedSource::make_fixed_response(status, None).expect("fixed response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    fn event_position(
        events: &[LifecycleEvent],
        predicate: impl Fn(&LifecycleEvent) -> bool,
        label: &str,
    ) -> usize {
        events
            .iter()
            .position(predicate)
            .unwrap_or_else(|| panic!("missing lifecycle event {label}: {events:?}"))
    }

    fn event_count(
        events: &[LifecycleEvent],
        predicate: impl Fn(&LifecycleEvent) -> bool,
    ) -> usize {
        events.iter().filter(|event| predicate(event)).count()
    }

    fn assert_real_h2_lifecycle_order() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let input_length = input.len();
                let (transport, transport_state) =
                    FixtureTransport::new([FixtureRead::DataEof(input)]);
                let handler = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let observer = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&events)));
                let _registration = session
                    .add_lifecycle_observer(observer.clone())
                    .expect("lifecycle observer");
                session.run().await.expect("session completes");
                assert_eq!(observer.attached.get(), 1);
                let events = events.borrow();
                let read = event_position(
                    &events,
                    |event| {
                        *event
                            == LifecycleEvent::Http2Read {
                                bytes: input_length,
                                stream: None,
                            }
                    },
                    "HTTP/2 transport read",
                );
                let settings = event_position(
                    &events,
                    |event| matches!(event, LifecycleEvent::Http2Settings { .. }),
                    "HTTP/2 settings",
                );
                let ingress_message = event_position(
                    &events,
                    |event| {
                        *event
                            == LifecycleEvent::Http2IngressMessage {
                                stream: StreamId::new(1),
                            }
                    },
                    "HTTP/2 ingress message",
                );
                let request_begin = events
                    .iter()
                    .position(|event| matches!(event, LifecycleEvent::RequestBegin { .. }))
                    .expect("request begin");
                let activated = event_position(
                    &events,
                    |event| *event == LifecycleEvent::ActivateConnection,
                    "activate connection",
                );
                let attached = event_position(
                    &events,
                    |event| *event == LifecycleEvent::TransactionAttached,
                    "transaction attached",
                );
                let request_end = events
                    .iter()
                    .position(|event| matches!(event, LifecycleEvent::RequestEnd { .. }))
                    .expect("request end");
                let detached = event_position(
                    &events,
                    |event| matches!(event, LifecycleEvent::TransactionDetached { .. }),
                    "transaction detached",
                );
                let deactivated = event_position(
                    &events,
                    |event| *event == LifecycleEvent::DeactivateConnection,
                    "deactivate connection",
                );
                let destroy = events
                    .iter()
                    .position(|event| matches!(event, LifecycleEvent::Destroy))
                    .expect("destroy");
                let eof = event_position(
                    &events,
                    |event| *event == LifecycleEvent::Http2IngressEof,
                    "HTTP/2 ingress EOF",
                );
                let written = events
                    .iter()
                    .filter_map(|event| match event {
                        LifecycleEvent::Http2Write { bytes } => Some(*bytes),
                        _ => None,
                    })
                    .sum::<usize>();
                assert_eq!(
                    written,
                    transport_state
                        .borrow()
                        .writes
                        .iter()
                        .map(Vec::len)
                        .sum::<usize>()
                );
                assert!(written > 0);
                assert!(read < settings);
                assert!(settings < request_begin);
                assert!(request_begin < activated && activated < attached);
                assert!(attached < ingress_message && ingress_message < request_end);
                assert!(request_end < detached);
                assert!(read < eof && eof < destroy);
                assert!(detached < deactivated && deactivated < destroy);
                assert_eq!(destroy, events.len() - 1);
            })
        });
    }

    fn assert_real_h2_reset_and_error_lifecycle_order() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(
                    HttpMethod::Get,
                    &[],
                    None,
                    PeerControl::Reset(H2ErrorCode::CANCEL),
                );
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let _registration = session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                session
                    .run()
                    .await
                    .expect("peer reset remains stream-local");
                let events = events.borrow();
                let begin = event_position(
                    &events,
                    |event| matches!(event, LifecycleEvent::RequestBegin { .. }),
                    "request begin",
                );
                let message = event_position(
                    &events,
                    |event| matches!(event, LifecycleEvent::Http2IngressMessage { .. }),
                    "ingress message",
                );
                let reset = event_position(
                    &events,
                    |event| {
                        *event
                            == LifecycleEvent::PeerReset {
                                stream: StreamId::new(1),
                                code: HttpErrorCode::CANCEL,
                            }
                    },
                    "peer reset",
                );
                let detached = event_position(
                    &events,
                    |event| matches!(event, LifecycleEvent::TransactionDetached { .. }),
                    "transaction detached",
                );
                let eof = event_position(
                    &events,
                    |event| *event == LifecycleEvent::Http2IngressEof,
                    "ingress EOF",
                );
                assert!(begin < message && message < reset);
                assert!(reset < eof && eof < detached);
            })
        });

        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(
                    HttpMethod::Get,
                    &[],
                    None,
                    PeerControl::WindowUpdate {
                        connection: true,
                        amount: 0x7fff_ffff,
                    },
                );
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let _registration = session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                let error = session
                    .run()
                    .await
                    .expect_err("connection window overflow fails the session");
                assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                let events = events.borrow();
                let message = event_position(
                    &events,
                    |event| matches!(event, LifecycleEvent::Http2IngressMessage { .. }),
                    "ingress message",
                );
                let ingress_error = event_position(
                    &events,
                    |event| {
                        *event
                            == LifecycleEvent::Http2IngressError {
                                stream: StreamId::UNSPECIFIED,
                                code: HttpErrorCode::FLOW_CONTROL_ERROR,
                            }
                    },
                    "ingress error",
                );
                let drain = event_position(
                    &events,
                    |event| matches!(event, LifecycleEvent::DrainStarted(_)),
                    "connection-error drain",
                );
                assert!(message < ingress_error && ingress_error < drain);
            })
        });
    }

    fn assert_typed_lifecycle_observer_registration() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, _) = FixtureTransport::new([]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let observer = Rc::new(SelfRemovingAttachedObserver {
                    attached: Cell::new(0),
                    removed: Cell::new(false),
                    events: Rc::clone(&events),
                });
                let _id = session
                    .add_lifecycle_observer(observer.clone())
                    .expect("typed observer registration");
                assert_eq!(observer.attached.get(), 1);
                assert!(observer.removed.get());

                let mut reservation = handle.reserve_request().expect("request reservation");
                assert!(reservation.cancel());
                assert!(events.borrow().is_empty());
                assert!(handle.initiate_drain().expect("drain accepted"));
                session.run().await.expect("drained session completes");
            })
        });
    }

    fn assert_real_upstream_lifecycle_snapshot_semantics() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, _) = FixtureTransport::new([]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");

                let added_events = Rc::new(RefCell::new(Vec::new()));
                let added = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&added_events)));
                let adding_events = Rc::new(RefCell::new(Vec::new()));
                let adding = Rc::new(AddOnAttachedObserver {
                    attached: Cell::new(0),
                    events: Rc::clone(&adding_events),
                    registration: RefCell::new(None),
                    added: added.clone(),
                    added_once: Cell::new(false),
                });
                session
                    .add_lifecycle_observer(adding.clone())
                    .expect("adding observer");
                let mut reservation = handle.reserve_request().expect("first reservation");
                assert!(reservation.cancel());
                assert_eq!(adding.attached.get(), 1);
                assert_eq!(added.attached.get(), 1);
                assert_eq!(
                    event_count(&added_events.borrow(), |event| {
                        *event == LifecycleEvent::TransactionAttached
                    }),
                    0
                );
                assert_eq!(
                    added_events.borrow().as_slice(),
                    [
                        LifecycleEvent::TransactionDetached { stream: None },
                        LifecycleEvent::DeactivateConnection,
                    ]
                );
                assert!(
                    adding
                        .registration
                        .borrow()
                        .as_ref()
                        .is_some_and(LifecycleObserverRegistration::remove)
                );
                assert!(added.remove());

                let target_events = Rc::new(RefCell::new(Vec::new()));
                let target = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&target_events)));
                session
                    .add_lifecycle_observer(target.clone())
                    .expect("target observer");
                let remover_events = Rc::new(RefCell::new(Vec::new()));
                let remover = Rc::new(RemoveOnEventObserver {
                    attached: Cell::new(0),
                    events: Rc::clone(&remover_events),
                    registration: RefCell::new(None),
                    target: Rc::clone(&target),
                    handle: handle.clone(),
                    remove_on: |event| *event == LifecycleEvent::TransactionAttached,
                    removed: Cell::new(false),
                });
                session
                    .add_lifecycle_observer(remover.clone())
                    .expect("removing observer");
                let mut reservation = handle.reserve_request().expect("second reservation");
                assert!(reservation.cancel());
                assert!(remover.removed.get());
                assert_eq!(
                    target_events.borrow().as_slice(),
                    [
                        LifecycleEvent::ActivateConnection,
                        LifecycleEvent::TransactionAttached,
                    ]
                );
                assert_eq!(
                    remover_events.borrow().as_slice(),
                    [
                        LifecycleEvent::ActivateConnection,
                        LifecycleEvent::TransactionAttached,
                        LifecycleEvent::TransactionDetached { stream: None },
                        LifecycleEvent::DeactivateConnection,
                    ]
                );
                assert!(
                    remover
                        .registration
                        .borrow()
                        .as_ref()
                        .is_some_and(LifecycleObserverRegistration::remove)
                );

                let self_events = Rc::new(RefCell::new(Vec::new()));
                let self_removing = Rc::new(SelfRemovingAttachedObserver {
                    attached: Cell::new(0),
                    removed: Cell::new(false),
                    events: Rc::clone(&self_events),
                });
                session
                    .add_lifecycle_observer(self_removing.clone())
                    .expect("self-removing observer");
                assert_eq!(self_removing.attached.get(), 1);
                assert!(self_removing.removed.get());
                let mut reservation = handle.reserve_request().expect("third reservation");
                assert!(reservation.cancel());
                assert!(self_events.borrow().is_empty());

                assert!(handle.initiate_drain().expect("drain accepted"));
                session.run().await.expect("drained session completes");
            })
        });
    }

    fn assert_real_upstream_deactivation_waits_for_last_reservation() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = server_response_bytes(0, 0);
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let observer = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&events)));
                session
                    .add_lifecycle_observer(observer)
                    .expect("lifecycle observer");

                let mut held = handle.reserve_request().expect("held reservation");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed GET");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut run = Box::pin(session.run());
                let mut consume = Box::pin(async move {
                    let headers = response
                        .read_header_event()
                        .await
                        .expect("response headers");
                    assert!(headers.eom());
                });
                tokio::select! {
                    () = consume.as_mut() => {}
                    result = run.as_mut() => {
                        panic!("session ended with a held reservation: {result:?}")
                    }
                }
                drop(consume);
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if event_count(&events.borrow(), |event| {
                            matches!(event, LifecycleEvent::TransactionDetached { .. })
                        }) == 1
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before stream detach: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("stream detaches while reservation remains");
                assert_eq!(
                    event_count(&events.borrow(), |event| {
                        *event == LifecycleEvent::DeactivateConnection
                    }),
                    0
                );

                assert!(held.cancel());
                assert_eq!(
                    event_count(&events.borrow(), |event| {
                        matches!(event, LifecycleEvent::TransactionDetached { .. })
                    }),
                    2
                );
                assert_eq!(
                    event_count(&events.borrow(), |event| {
                        *event == LifecycleEvent::DeactivateConnection
                    }),
                    1
                );
                run.await.expect("session completes after last reservation");
            })
        });
    }

    fn assert_real_downstream_lifecycle_removal_and_reentry() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 100,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let persistent_events = Rc::new(RefCell::new(Vec::new()));
                let persistent =
                    Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&persistent_events)));
                session
                    .add_lifecycle_observer(persistent.clone())
                    .expect("persistent observer");
                let removed_events = Rc::new(RefCell::new(Vec::new()));
                let removed = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&removed_events)));
                session
                    .add_lifecycle_observer(removed.clone())
                    .expect("removable observer");
                let remover_events = Rc::new(RefCell::new(Vec::new()));
                let remover = Rc::new(RemoveOnEventObserver {
                    attached: Cell::new(0),
                    events: Rc::clone(&remover_events),
                    registration: RefCell::new(None),
                    target: Rc::clone(&removed),
                    handle,
                    remove_on: |event| matches!(event, LifecycleEvent::Http2Settings { .. }),
                    removed: Cell::new(false),
                });
                session
                    .add_lifecycle_observer(remover.clone())
                    .expect("removing observer");
                session.run().await.expect("session completes");
                assert!(remover.removed.get());
                assert_eq!(
                    event_count(&removed_events.borrow(), |event| {
                        matches!(event, LifecycleEvent::Http2Settings { .. })
                    }),
                    1
                );
                assert_eq!(
                    event_count(&removed_events.borrow(), |event| {
                        matches!(event, LifecycleEvent::RequestBegin { .. })
                    }),
                    0
                );
                assert_eq!(
                    event_count(&persistent_events.borrow(), |event| {
                        matches!(event, LifecycleEvent::RequestBegin { .. })
                    }),
                    1
                );
                assert_eq!(
                    event_count(&remover_events.borrow(), |event| {
                        matches!(event, LifecycleEvent::Destroy)
                    }),
                    1
                );
            })
        });
    }

    fn assert_real_multiple_downstream_lifecycle_observers() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 100,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let first_events = Rc::new(RefCell::new(Vec::new()));
                let first = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&first_events)));
                let second_events = Rc::new(RefCell::new(Vec::new()));
                let second = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&second_events)));
                session
                    .add_lifecycle_observer(first.clone())
                    .expect("first observer");
                session
                    .add_lifecycle_observer(second.clone())
                    .expect("second observer");
                session.run().await.expect("session completes");
                assert_eq!(first.attached.get(), 1);
                assert_eq!(second.attached.get(), 1);
                assert_eq!(&*first_events.borrow(), &*second_events.borrow());
                for events in [&first_events, &second_events] {
                    let events = events.borrow();
                    assert_eq!(
                        event_count(&events, |event| {
                            *event == LifecycleEvent::TransactionAttached
                        }),
                        1
                    );
                    assert_eq!(
                        event_count(&events, |event| {
                            matches!(event, LifecycleEvent::TransactionDetached { .. })
                        }),
                        1
                    );
                    assert_eq!(
                        event_count(&events, |event| {
                            *event == LifecycleEvent::DeactivateConnection
                        }),
                        1
                    );
                    assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
                }
            })
        });
    }

    struct DrainCleanupTransport {
        inner: Rc<FixtureTransport>,
        changed: Arc<tokio::sync::Notify>,
        shutdown_calls: Cell<usize>,
        reset_calls: Cell<usize>,
        close_calls: Cell<usize>,
        drops: Arc<AtomicUsize>,
        owner: std::thread::ThreadId,
    }

    impl Drop for DrainCleanupTransport {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().id(), self.owner);
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl UniplexTransport for DrainCleanupTransport {
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
            let write = self
                .inner
                .write(context, data, flags, timeout, cancellation);
            let changed = Arc::clone(&self.changed);
            Box::pin(async move {
                let result = write.await;
                changed.notify_one();
                result
            })
        }

        fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.shutdown_calls.set(self.shutdown_calls.get() + 1);
            let result = self.inner.shutdown_write(context);
            self.changed.notify_one();
            result
        }

        fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.close_calls.set(self.close_calls.get() + 1);
            let result = self.inner.close(context);
            self.changed.notify_one();
            result
        }

        fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.reset_calls.set(self.reset_calls.get() + 1);
            let result = self.inner.close_with_reset(context);
            self.changed.notify_one();
            result
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

    struct DrainCleanupLifecycle {
        events: Rc<RefCell<Vec<LifecycleEvent>>>,
        changed: Arc<tokio::sync::Notify>,
    }

    impl LifecycleObserver for DrainCleanupLifecycle {
        fn on_event(&self, event: LifecycleEvent) {
            self.events.borrow_mut().push(event);
            self.changed.notify_one();
        }
    }

    struct DrainCleanupContext {
        held: Rc<RefCell<Option<HandlerSessionContext>>>,
        close_input: Arc<tokio::sync::Notify>,
    }

    impl Drop for DrainCleanupContext {
        fn drop(&mut self) {
            let held = self.held.borrow_mut().take();
            drop(held);
            self.close_input.notify_one();
        }
    }

    #[test]
    fn drain_during_cleanup_is_idempotent() {
        for fail_read in [false, true] {
            let drops = Arc::new(AtomicUsize::new(0));
            let worker_drops = Arc::clone(&drops);
            run_on_worker(move |context| {
                Box::pin(async move {
                    let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                    let changed = Arc::new(tokio::sync::Notify::new());
                    let close_input = Arc::new(tokio::sync::Notify::new());
                    let mut reads = vec![FixtureRead::SegmentedData(queue(&input))];
                    if fail_read {
                        reads.push(FixtureRead::WaitData(Arc::clone(&close_input), Vec::new()));
                        reads.push(FixtureRead::Error(HttpErrorCode::TRANSPORT_READ_ERROR));
                    } else {
                        reads.push(FixtureRead::WaitDataEof(
                            Arc::clone(&close_input),
                            Vec::new(),
                        ));
                    }
                    let (inner, state) = FixtureTransport::new(reads);
                    let transport = Rc::new(DrainCleanupTransport {
                        inner,
                        changed: Arc::clone(&changed),
                        shutdown_calls: Cell::new(0),
                        reset_calls: Cell::new(0),
                        close_calls: Cell::new(0),
                        drops: Arc::clone(&worker_drops),
                        owner: std::thread::current().id(),
                    });
                    let cleanup = DrainCleanupContext {
                        held: Rc::new(RefCell::new(None)),
                        close_input,
                    };
                    let handler = Rc::new(ContextHoldingHandler {
                        held: Rc::clone(&cleanup.held),
                    });
                    let mut rate_limits = Http2RateLimitConfig::default();
                    for limit in [
                        &mut rate_limits.control_frames,
                        &mut rate_limits.header_frames,
                        &mut rate_limits.resets,
                        &mut rate_limits.direct_errors,
                    ] {
                        limit.interval = Duration::from_secs(30);
                    }
                    let (session, handle) = UniplexSession::new_http2(
                        context,
                        TransportDirection::Downstream,
                        transport.clone(),
                        Some(handler),
                        UniplexConfig::default(),
                        Http2SessionConfig {
                            rate_limits,
                            ..Http2SessionConfig::default()
                        },
                    )
                    .expect("real downstream H2 session");
                    let events = Rc::new(RefCell::new(Vec::new()));
                    session
                        .add_lifecycle_observer(Rc::new(DrainCleanupLifecycle {
                            events: Rc::clone(&events),
                            changed: Arc::clone(&changed),
                        }))
                        .expect("native lifecycle observer");
                    let mut run = session.run();
                    let exercise = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(3),
                        async {
                            std::future::poll_fn(|task_context| {
                                assert!(run.as_mut().poll(task_context).is_pending());
                                std::task::Poll::Ready(())
                            })
                            .await;
                            eligibility_wait(&changed, || {
                                cleanup.held.borrow().is_some()
                                    && events.borrow().iter().any(|event| {
                                        matches!(event, LifecycleEvent::TransactionDetached { .. })
                                    })
                                    && eligibility_wire(&state)
                                        .completed
                                        .contains(&StreamId::new(1))
                            })
                            .await;
                            assert_eq!(handle.closing_state_for_test(), (false, false));
                            assert_eq!(state.borrow().close, FixtureClose::Open);
                            assert_eq!(
                                eligibility_wire(&state).statuses,
                                [(StreamId::new(1), 200)]
                            );
                            cleanup.close_input.notify_one();
                            let expected_close = if fail_read {
                                FixtureClose::Reset
                            } else {
                                FixtureClose::ShutdownWrite
                            };
                            eligibility_wait(&changed, || {
                                handle.closing_state_for_test().0
                                    && state.borrow().close == expected_close
                            })
                            .await;
                            assert_eq!(handle.closing_state_for_test(), (true, false));
                            assert!(!handle.is_finished());
                            assert!(cleanup.held.borrow().is_some());
                            let wire_before = state.borrow().writes.clone();
                            let events_before = events.borrow().clone();
                            let shutdown_before = transport.shutdown_calls.get();
                            let reset_before = transport.reset_calls.get();
                            let close_before = transport.close_calls.get();
                            assert_eq!(reset_before, usize::from(fail_read));
                            assert_eq!(shutdown_before, usize::from(!fail_read));
                            assert_eq!(close_before, 0);
                            assert!(!events_before.contains(&LifecycleEvent::Destroy));
                            assert_eq!(worker_drops.load(Ordering::SeqCst), 0);
                            for _ in 0..3 {
                                std::future::poll_fn(|task_context| {
                                    assert!(run.as_mut().poll(task_context).is_pending());
                                    std::task::Poll::Ready(())
                                })
                                .await;
                                assert!(
                                    !handle.initiate_drain().expect("closing drain is a no-op")
                                );
                                assert_eq!(handle.closing_state_for_test(), (true, false));
                                assert_eq!(
                                    handle
                                        .protocol()
                                        .expect_err("other live commands still reject closing")
                                        .code(),
                                    HttpErrorCode::DROPPED
                                );
                                assert_eq!(state.borrow().close, expected_close);
                                assert_eq!(state.borrow().writes, wire_before);
                                assert_eq!(*events.borrow(), events_before);
                                assert_eq!(transport.shutdown_calls.get(), shutdown_before);
                                assert_eq!(transport.reset_calls.get(), reset_before);
                                assert_eq!(transport.close_calls.get(), close_before);
                                assert!(cleanup.held.borrow().is_some());
                            }
                        },
                    ))
                    .await;
                    drop(cleanup);
                    let result = tokio::time::timeout(Duration::from_secs(3), run).await;
                    if let Err(payload) = exercise {
                        std::panic::resume_unwind(payload);
                    }
                    exercise
                        .unwrap()
                        .expect("closing phase reached with a real held context");
                    let result = result.expect("native cleanup joins after context release");
                    if fail_read {
                        let error = result.expect_err("preserve the real transport read failure");
                        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
                        assert_eq!(error.message(), b"scripted H2 read error");
                    } else {
                        let summary = result.expect("clean EOF retains native success");
                        assert_eq!(summary.bytes_read, input.len() as u64);
                        assert_eq!(
                            summary.bytes_written,
                            state.borrow().writes.concat().len() as u64
                        );
                        assert_eq!(summary.bytes_scheduled, summary.bytes_written);
                        assert!(!summary.reset);
                    }
                    assert_eq!(handle.closing_state_for_test(), (true, true));
                    assert!(
                        !handle
                            .initiate_drain()
                            .expect("completed drain is also a no-op")
                    );
                    assert_eq!(transport.reset_calls.get(), usize::from(fail_read));
                    assert_eq!(transport.shutdown_calls.get(), usize::from(!fail_read));
                    assert_eq!(transport.close_calls.get(), 0);
                    assert_eq!(
                        events
                            .borrow()
                            .iter()
                            .filter(|event| **event == LifecycleEvent::Destroy)
                            .count(),
                        1
                    );
                    assert_eq!(events.borrow().last(), Some(&LifecycleEvent::Destroy));
                    assert!(!state.borrow().transport_dropped);
                    drop(transport);
                    assert!(state.borrow().transport_dropped);
                    assert_eq!(worker_drops.load(Ordering::SeqCst), 1);
                })
            });
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    fn assert_real_handler_context_delays_destroy() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let held = Rc::new(RefCell::new(None));
                let handler: Rc<dyn HttpHandler> = Rc::new(ContextHoldingHandler {
                    held: Rc::clone(&held),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let stream_detached = events.borrow().iter().any(|event| {
                            matches!(event, LifecycleEvent::TransactionDetached { .. })
                        });
                        if held.borrow().is_some()
                            && stream_detached
                            && !state.borrow().writes.is_empty()
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session destroyed while handler context was held: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("response completes with held handler context");
                assert_eq!(
                    event_count(&events.borrow(), |event| {
                        *event == LifecycleEvent::DeactivateConnection
                    }),
                    1
                );
                assert_eq!(
                    event_count(&events.borrow(), |event| {
                        *event == LifecycleEvent::Destroy
                    }),
                    0
                );
                drop(held.borrow_mut().take());
                run.await.expect("session destroys after context release");
                assert_eq!(events.borrow().last(), Some(&LifecycleEvent::Destroy));
            })
        });
    }

    fn assert_real_peer_zero_closes_admission() {
        run_on_worker(|context| {
            Box::pin(async move {
                let reopen = Arc::new(tokio::sync::Notify::new());
                let finish = Arc::new(tokio::sync::Notify::new());
                let (transport, _) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(0)),
                    FixtureRead::WaitData(Arc::clone(&reopen), server_settings_bytes(10)),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let _registration = session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                let run = session.run();
                tokio::pin!(run);
                loop {
                    if events
                        .borrow()
                        .iter()
                        .any(|event| matches!(event, LifecycleEvent::Http2Settings { .. }))
                    {
                        break;
                    }
                    tokio::select! {
                        result = run.as_mut() => panic!("session ended before SETTINGS: {result:?}"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
                let error = match handle.reserve_request() {
                    Ok(_) => panic!("peer zero concurrency admitted a request"),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                assert!(events.borrow().contains(&LifecycleEvent::Http2StreamsFull));
                assert!(events.borrow().contains(&LifecycleEvent::Http2Setting {
                    setting: roxygen_codec::CodecSetting {
                        identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                        value: 0,
                    },
                }));
                reopen.notify_one();
                loop {
                    if events
                        .borrow()
                        .contains(&LifecycleEvent::Http2StreamsNotFull)
                    {
                        break;
                    }
                    tokio::select! {
                        result = run.as_mut() => panic!("session ended before admission reopened: {result:?}"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
                assert!(events.borrow().contains(&LifecycleEvent::Http2Setting {
                    setting: roxygen_codec::CodecSetting {
                        identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                        value: 10,
                    },
                }));
                assert_eq!(
                    event_count(&events.borrow(), |event| {
                        *event == LifecycleEvent::Http2StreamsFull
                    }),
                    1
                );
                assert_eq!(
                    event_count(&events.borrow(), |event| {
                        *event == LifecycleEvent::Http2StreamsNotFull
                    }),
                    1
                );
                handle
                    .reserve_request()
                    .expect("peer nonzero concurrency reopens admission")
                    .cancel();
                finish.notify_one();
                assert!(handle.initiate_drain().expect("drain accepted"));
                run.await.expect("drained session completes");
            })
        });
    }

    #[test]
    fn local_stream_limit_survives_peer_settings_and_rejects_zero() {
        run_on_worker(|context| {
            Box::pin(async move {
                let finish = Arc::new(tokio::sync::Notify::new());
                let (transport, _) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(10)),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        max_concurrent_outgoing_streams: 1,
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let _registration = session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");

                assert!(
                    handle
                        .set_max_concurrent_outgoing_streams(2)
                        .expect("live limit update is accepted")
                );
                let run = session.run();
                tokio::pin!(run);
                loop {
                    if events
                        .borrow()
                        .iter()
                        .any(|event| matches!(event, LifecycleEvent::Http2Settings { .. }))
                    {
                        break;
                    }
                    tokio::select! {
                        result = run.as_mut() => panic!("session ended before SETTINGS: {result:?}"),
                        _ = tokio::task::yield_now() => {},
                    }
                }

                let mut first = handle.reserve_request().expect("first request is admitted");
                let mut second = handle
                    .reserve_request()
                    .expect("updated local limit survives peer SETTINGS");
                let error = match handle.reserve_request() {
                    Ok(_) => panic!("local two-stream limit admitted a third request"),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                assert!(
                    !handle
                        .set_max_concurrent_outgoing_streams(0)
                        .expect("zero limit is rejected without an error")
                );
                first.cancel();
                second.cancel();

                let mut first = handle
                    .reserve_request()
                    .expect("rejected zero leaves the first slot available");
                let mut second = handle
                    .reserve_request()
                    .expect("rejected zero leaves the second slot available");
                assert!(handle.reserve_request().is_err());
                first.cancel();
                second.cancel();

                finish.notify_one();
                assert!(handle.initiate_drain().expect("drain accepted"));
                run.await.expect("drained session completes");
            })
        });
    }

    fn assert_real_upstream_push_limit_rejects_promised_stream() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, _) =
                    FixtureTransport::new([FixtureRead::DataEof(server_push_bytes(false, None))]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        settings: vec![roxygen_codec::CodecSetting {
                            identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                            value: 0,
                        }],
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("parent request admitted")
                    .source;
                let consume = async move {
                    let mut headers = response
                        .read_header_event()
                        .await
                        .expect("parent response headers");
                    assert_eq!(headers.headers_mut().status_code().expect("status"), 200);
                    let mut rejected_pushes = 0;
                    loop {
                        let event = response
                            .read_body_event(u32::MAX)
                            .await
                            .expect("parent response body event");
                        let eom = event.eom();
                        if let HttpBodyEventKind::PushPromise(mut push) = event.into_kind() {
                            rejected_pushes += 1;
                            assert_eq!(push.promise_mut().path().expect("push path"), b"/push");
                            let mut pushed = push.move_push_source().expect("rejected push source");
                            let error = match pushed.read_header_event().await {
                                Ok(_) => panic!("over-limit pushed stream produced headers"),
                                Err(error) => error,
                            };
                            assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                        }
                        if eom {
                            break;
                        }
                    }
                    rejected_pushes
                };
                let (run, rejected_pushes) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(session.run(), consume)
                })
                .await
                .expect("push-limit session timed out");
                run.expect("push-limit rejection remains stream-local");
                assert_eq!(rejected_pushes, 1);
            })
        });
    }

    fn assert_real_downstream_ingress_stream_limit() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        settings: vec![roxygen_codec::CodecSetting {
                            identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                            value: 0,
                        }],
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                session
                    .run()
                    .await
                    .expect("over-limit request is refused without failing the connection");
                assert_eq!(invocations.get(), 0);
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(
                    decoded.resets,
                    [(StreamId::new(1), H2ErrorCode::REFUSED_STREAM)]
                );
                assert!(decoded.header_statuses.is_empty());
            })
        });
    }

    fn assert_real_downstream_request_between_goaways() {
        run_on_worker(|context| {
            Box::pin(async move {
                let release_request = Arc::new(tokio::sync::Notify::new());
                let request = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::WaitDataEof(
                    Arc::clone(&release_request),
                    request,
                )]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_secs(1),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                assert!(handle.initiate_drain().expect("local drain begins"));
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let goaways = decode_server_frames_with_requests(
                            &state.borrow().writes.concat(),
                            0,
                        )
                        .goaways;
                        if goaways.len() == 1 {
                            assert_eq!(
                                goaways[0],
                                (
                                    StreamId::new(u64::from(
                                        roxygen_http2::constants::MAX_STREAM_ID,
                                    )),
                                    H2ErrorCode::NO_ERROR,
                                )
                            );
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => panic!("session ended before initial GOAWAY: {result:?}"),
                            _ = tokio::task::yield_now() => {},
                        }
                    }
                })
                .await
                .expect("initial GOAWAY was written");
                release_request.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("request between GOAWAY stages timed out")
                    .expect("graceful drain completes");
                assert_eq!(invocations.get(), 1);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                assert_eq!(
                    decoded.goaways,
                    [
                        (
                            StreamId::new(u64::from(roxygen_http2::constants::MAX_STREAM_ID)),
                            H2ErrorCode::NO_ERROR,
                        ),
                        (StreamId::new(1), H2ErrorCode::NO_ERROR),
                    ]
                );
            })
        });
    }

    fn assert_real_final_goaway_keeps_active_ingress_reading() {
        run_on_worker(|context| {
            Box::pin(async move {
                let body = [b'p'; 32];
                let (headers, body_frame) = peer_padded_post_parts(&body, 0);
                let release_body = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(headers),
                    FixtureRead::WaitDataEof(Arc::clone(&release_body), body_frame),
                ]);
                let invocations = Rc::new(Cell::new(0));
                let request_body = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::clone(&request_body),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_millis(5),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while invocations.get() == 0 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before POST admission: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("POST headers were not admitted");
                assert!(handle.initiate_drain().expect("graceful drain begins"));
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let goaways =
                            decode_server_frames_with_requests(&state.borrow().writes.concat(), 1)
                                .goaways;
                        if goaways.len() == 2 {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before final GOAWAY: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("final GOAWAY was not written");
                release_body.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("active POST did not complete after final GOAWAY")
                    .expect("active POST completes during final drain");
                assert_eq!(request_body.get(), body.len());
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                assert_eq!(decoded.goaways.len(), 2);
            })
        });
    }

    fn assert_real_close_when_idle_is_single_stage() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::task::yield_now().await;
                assert!(handle.close_when_idle().expect("close when idle accepted"));
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("close when idle timed out")
                    .expect("close when idle completes");
                assert_eq!(
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 0).goaways,
                    [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
                );
            })
        });
    }

    fn assert_real_close_when_idle_waits_for_active_stream() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let handle_slot = Rc::new(RefCell::new(None));
                let handler: Rc<dyn HttpHandler> = Rc::new(CloseWhenIdleHandler {
                    handle: Rc::clone(&handle_slot),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                handle_slot.replace(Some(handle));
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("close-when-idle with a stream timed out")
                    .expect("active response completes before close");
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                assert_eq!(decoded.body_bytes, 100);
                assert_eq!(decoded.goaways, [(StreamId::new(1), H2ErrorCode::NO_ERROR)]);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_stream_after_final_goaway_is_not_admitted() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (first, second) = peer_request_parts();
                let release_second = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(first),
                    FixtureRead::WaitDataEof(Arc::clone(&release_second), second),
                ]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 100,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_millis(5),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while invocations.get() != 1
                        || !decode_server_frames_with_requests(&state.borrow().writes.concat(), 1)
                            .header_streams
                            .contains(&StreamId::new(1))
                    {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before first response: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("first response was written");
                assert!(handle.initiate_drain().expect("drain accepted"));
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let goaways =
                            decode_server_frames_with_requests(&state.borrow().writes.concat(), 1)
                                .goaways;
                        if goaways.last() == Some(&(StreamId::new(1), H2ErrorCode::NO_ERROR)) {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before final GOAWAY: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("final GOAWAY was written");
                release_second.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("post-GOAWAY request cleanup timed out")
                    .expect("post-GOAWAY request is ignored stream-locally");
                assert_eq!(invocations.get(), 1);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert!(!decoded.header_streams.contains(&StreamId::new(3)));
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_upstream_single_goaway() {
        let outcome = run_on_worker(|context| {
            Box::pin(async move {
                eligibility_catch_unwind(async move {
                    let (transport, state) =
                        FixtureTransport::new([FixtureRead::Data(server_settings_bytes(100))]);
                    let (session, handle) = UniplexSession::new_http2(
                        context,
                        TransportDirection::Upstream,
                        transport.clone(),
                        None,
                        UniplexConfig::default(),
                        Http2SessionConfig {
                            double_goaway_drain: false,
                            ..Http2SessionConfig::default()
                        },
                    )
                    .expect("frozen single-drain upstream configuration");
                    assert!(handle.initiate_drain().expect("single drain accepted"));
                    let run = tokio::time::timeout(Duration::from_secs(1), session.run()).await;
                    let wire = state.borrow().writes.concat();
                    let close = state.borrow().close;
                    let finished = handle.is_finished();
                    drop(handle);
                    drop(transport);
                    let dropped = state.borrow().transport_dropped;
                    (run, wire, close, finished, dropped)
                })
                .await
            })
        });
        let (run, wire, close, finished, dropped) =
            outcome.unwrap_or_else(|payload| resume_unwind(payload));
        let summary = run
            .expect("single drain completes without peer EOF")
            .expect("single-drain native Run succeeds");
        assert!(!summary.reset);
        assert_eq!(summary.bytes_written, wire.len() as u64);
        assert_eq!(summary.bytes_scheduled, summary.bytes_written);
        assert_eq!(close, FixtureClose::ShutdownWrite);
        assert!(finished && dropped);
        let decoded = decode_client_frames(&wire);
        assert!(
            decoded.errors.is_empty(),
            "codec errors: {:?}",
            decoded.errors
        );
        assert!(decoded.resets.is_empty());
        assert!(decoded.header_streams.is_empty());
        assert_eq!(
            decoded.goaways,
            [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
        );
        assert_eq!(
            decoded.control_frame_order,
            [(u64::from(FrameType::GOAWAY.as_u8()), 0)]
        );
    }

    pub(super) fn assert_upstream_double_drain_retires_without_final_goaway() {
        let outcome = run_on_worker(|context| {
            Box::pin(async move {
                tokio::time::pause();
                let release_response = Arc::new(tokio::sync::Notify::new());
                let outcome = eligibility_catch_unwind(async {
                    let started = tokio::time::Instant::now();
                    let input = server_response_bytes(3, 0);
                    let input_length = input.len();
                    assert!(input_length <= 4096);
                    let changed = Arc::new(tokio::sync::Notify::new());
                    let (inner, state) = FixtureTransport::new([FixtureRead::WaitData(
                        Arc::clone(&release_response),
                        input,
                    )]);
                    let transport = Rc::new(EligibilityTransport {
                        inner,
                        changed: Arc::clone(&changed),
                        reads_started: Cell::new(0),
                        backing_start: 0,
                        backing_length: 0,
                        shared_written: Cell::new(0),
                        backing_drops: Arc::default(),
                    });
                    let (session, handle) = UniplexSession::new_http2(
                        context,
                        TransportDirection::Upstream,
                        transport.clone(),
                        None,
                        UniplexConfig::default(),
                        Http2SessionConfig {
                            double_goaway_drain: true,
                            ..Http2SessionConfig::default()
                        },
                    )
                    .expect("explicit double-drain upstream session");
                    let events = Rc::new(RefCell::new(Vec::new()));
                    session
                        .add_lifecycle_observer(Rc::new(LifecycleProbe {
                            events: Rc::clone(&events),
                        }))
                        .expect("passive upstream lifecycle observer");
                    let probe = Rc::new(EligibilityProbe::default());
                    let prepared = committed_test_request(
                        &handle,
                        committed_test_headers(b"/terminal-drain", true),
                        &probe,
                    );
                    let mut response = Some(
                        handle
                            .send_prepared_request(prepared)
                            .expect("real request commits before local drain"),
                    );
                    assert_eq!(
                        response.as_ref().expect("retained response").stream,
                        StreamId::new(1)
                    );
                    assert!(state.borrow().writes.is_empty());
                    assert!(
                        handle
                            .initiate_drain()
                            .expect("drain follows committed request")
                    );
                    let exercise = async {
                        let outcome = eligibility_catch_unwind(tokio::time::timeout(
                            Duration::from_secs(1),
                            async {
                                eligibility_wait(&changed, || {
                                    let wire = state.borrow().writes.concat();
                                    assert!(wire.len() <= 4096);
                                    let decoded = decode_client_frames(&wire);
                                    decoded.header_streams == [StreamId::new(1)]
                                        && decoded.goaways
                                            == [(StreamId::new(0x7fff_ffff), H2ErrorCode::NO_ERROR)]
                                })
                                .await;
                                let initial = decode_client_frames(&state.borrow().writes.concat());
                                assert!(initial.errors.is_empty());
                                assert!(initial.resets.is_empty());
                                assert_eq!(
                                    initial.header_goaway_order,
                                    [
                                        (StreamId::new(1), u64::from(FrameType::HEADERS.as_u8())),
                                        (
                                            StreamId::UNSPECIFIED,
                                            u64::from(FrameType::GOAWAY.as_u8())
                                        ),
                                    ]
                                );
                                assert_eq!(state.borrow().close, FixtureClose::Open);
                                assert!(!state.borrow().transport_dropped);
                                assert!(!handle.is_finished());
                                assert!(!events.borrow().iter().any(|event| matches!(
                                    event,
                                    LifecycleEvent::TransactionDetached { .. }
                                )));
                                assert_eq!(tokio::time::Instant::now(), started);
                                release_response.notify_one();
                                let mut response =
                                    response.take().expect("owned committed response");
                                let completion = response.completion.clone();
                                let mut headers =
                                    response.source.read_header_event().await.expect(
                                        "valid response headers after accepted initial GOAWAY",
                                    );
                                assert_eq!(
                                    headers
                                        .headers_mut()
                                        .status_code()
                                        .expect("response status"),
                                    200
                                );
                                assert!(!headers.eom());
                                let mut received = Vec::with_capacity(3);
                                loop {
                                    let event = response
                                        .source
                                        .read_body_event(3)
                                        .await
                                        .expect("complete real response body");
                                    let eom = event.eom();
                                    match event.into_kind() {
                                        HttpBodyEventKind::Body(body) => {
                                            assert!(received.len() + body.chain_length() <= 3);
                                            for segment in body.segments() {
                                                received.extend_from_slice(segment);
                                            }
                                        }
                                        HttpBodyEventKind::Suspend(resume) => {
                                            assert_eq!(resume.await, ResumeStatus::Signalled);
                                        }
                                        _ => panic!("unexpected response event"),
                                    }
                                    if eom {
                                        break;
                                    }
                                }
                                assert_eq!(received.as_slice(), b"sss");
                                drop(response);
                                completion.wait().await.expect("actual exchange retirement");
                                completion.wait().await.expect("sticky exchange retirement");
                                assert_eq!(tokio::time::Instant::now(), started);
                            },
                        ))
                        .await;
                        release_response.notify_one();
                        drop(response.take());
                        outcome
                    };
                    let (run, exercise) = tokio::join!(
                        tokio::time::timeout(Duration::from_secs(1), session.run()),
                        exercise,
                    );
                    let elapsed = tokio::time::Instant::now().duration_since(started);
                    let wire = state.borrow().writes.concat();
                    let events = events.borrow().clone();
                    let close = state.borrow().close;
                    let finished = handle.is_finished();
                    assert!(!state.borrow().destroy_on_shutdown);
                    assert!(state.borrow().registrations.is_empty());
                    drop(handle);
                    drop(transport);
                    let dropped = state.borrow().transport_dropped;
                    let source_counts = (
                        probe.header_reads.get(),
                        probe.body_reads.get(),
                        probe.source_drops.get(),
                        probe.cancelled_reads.get(),
                    );
                    (
                        run,
                        exercise,
                        wire,
                        events,
                        close,
                        finished,
                        dropped,
                        source_counts,
                        input_length,
                        elapsed,
                    )
                })
                .await;
                release_response.notify_one();
                tokio::time::resume();
                outcome
            })
        });
        let (
            run,
            exercise,
            wire,
            events,
            close,
            finished,
            dropped,
            source_counts,
            input_length,
            elapsed,
        ) = outcome.unwrap_or_else(|payload| resume_unwind(payload));
        exercise
            .unwrap_or_else(|payload| resume_unwind(payload))
            .expect("bounded committed-stream exercise");
        let summary = run
            .expect("upstream retires without awaiting EOF or its GOAWAY timer")
            .expect("healthy upstream Run succeeds");
        assert_eq!(elapsed, Duration::ZERO);
        assert!(!summary.reset);
        assert_eq!(summary.bytes_read, input_length as u64);
        assert_eq!(summary.bytes_written, wire.len() as u64);
        assert_eq!(summary.bytes_scheduled, summary.bytes_written);
        assert_eq!(close, FixtureClose::ShutdownWrite);
        assert!(finished && dropped);
        assert_eq!(source_counts, (0, 0, 1, 0));
        assert_eq!(
            event_count(&events, |event| matches!(
                event,
                LifecycleEvent::TransactionDetached { .. }
            )),
            1
        );
        assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
        assert!(wire.len() <= 4096);
        let decoded = decode_client_frames(&wire);
        assert!(
            decoded.errors.is_empty(),
            "codec errors: {:?}",
            decoded.errors
        );
        assert!(decoded.resets.is_empty());
        assert_eq!(decoded.header_streams, [StreamId::new(1)]);
        assert_eq!(decoded.body_bytes, 0);
        assert_eq!(
            decoded.goaways,
            [(StreamId::new(0x7fff_ffff), H2ErrorCode::NO_ERROR)]
        );
        assert_eq!(
            decoded.control_frame_order,
            [(u64::from(FrameType::GOAWAY.as_u8()), 0)]
        );
        assert_eq!(
            decoded.header_goaway_order,
            [
                (StreamId::new(1), u64::from(FrameType::HEADERS.as_u8())),
                (StreamId::UNSPECIFIED, u64::from(FrameType::GOAWAY.as_u8())),
            ]
        );
    }

    fn assert_cancelled_stream_lifecycle(events: &[LifecycleEvent]) {
        assert_eq!(
            event_count(events, |event| {
                *event == LifecycleEvent::TransactionAttached
            }),
            1
        );
        assert_eq!(
            event_count(events, |event| {
                matches!(event, LifecycleEvent::TransactionDetached { .. })
            }),
            1
        );
        assert_eq!(
            event_count(events, |event| {
                *event == LifecycleEvent::DeactivateConnection
            }),
            1
        );
        assert_eq!(
            event_count(events, |event| *event == LifecycleEvent::Destroy),
            1
        );
    }

    fn assert_real_upstream_peer_abort_stops_body() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = server_reset_bytes(H2ErrorCode::NO_ERROR);
                let release_reset = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([FixtureRead::WaitDataEof(
                    Arc::clone(&release_reset),
                    input,
                )]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                let body_reads = Rc::new(Cell::new(0));
                let stops = Rc::new(RefCell::new(Vec::new()));
                let request = PendingRequestSource::post(
                    1_000,
                    false,
                    Rc::clone(&body_reads),
                    Rc::clone(&stops),
                );
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while body_reads.get() == 0 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before upload body read: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("upload body read started");
                release_reset.notify_one();
                let mut response_read = Box::pin(response.read_header_event());
                let response_error = tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::select! {
                        result = response_read.as_mut() => match result {
                            Ok(_) => panic!("peer reset unexpectedly produced response headers"),
                            Err(error) => error,
                        },
                        result = run.as_mut() => {
                            panic!("session ended before reset reached response: {result:?}")
                        }
                    }
                })
                .await
                .expect("response reset timed out");
                drop(response_read);
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("peer-reset cleanup timed out")
                    .expect("peer reset is stream-local");
                assert_eq!(response_error.code(), HttpErrorCode::CANCEL);
                assert_eq!(stops.borrow().as_slice(), [Some(HttpErrorCode::CANCEL)]);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
                assert!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .resets
                        .is_empty()
                );
                assert_eq!(
                    event_count(&events.borrow(), |event| matches!(
                        event,
                        LifecycleEvent::PeerReset {
                            stream,
                            code: HttpErrorCode::CANCEL,
                        } if *stream == StreamId::new(1)
                    )),
                    1
                );
                assert_cancelled_stream_lifecycle(&events.borrow());
            })
        });
    }

    #[derive(Clone, Copy)]
    enum SessionAbortMode {
        CancelRun,
        DropOwner,
    }

    fn assert_real_upstream_session_abort(active_stream: bool, mode: SessionAbortMode) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                let mut response = if active_stream {
                    let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed GET");
                    Some(
                        handle
                            .send_request(
                                HttpSourceHolder::new(Box::new(request)),
                                CancellationSource::new().token(),
                            )
                            .await
                            .expect("request admitted")
                            .source,
                    )
                } else {
                    None
                };

                match mode {
                    SessionAbortMode::CancelRun => {
                        let mut run = Box::pin(session.run());
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("pending session completed before cancellation: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                        drop(run);
                    }
                    SessionAbortMode::DropOwner => drop(session),
                }
                let response_error = if let Some(response) = response.as_mut() {
                    let result =
                        tokio::time::timeout(Duration::from_secs(1), response.read_header_event())
                            .await
                            .expect("aborted response read timed out");
                    Some(match result {
                        Ok(_) => panic!("aborted session produced response headers"),
                        Err(error) => error.code(),
                    })
                } else {
                    None
                };
                drop(response);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !handle.is_finished() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("driver-owned abort finishes");
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                assert!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .resets
                        .is_empty()
                );
                if let Some(error) = response_error {
                    assert_eq!(error, HttpErrorCode::CORO_CANCELLED);
                    assert_cancelled_stream_lifecycle(&events.borrow());
                } else {
                    assert_eq!(
                        event_count(&events.borrow(), |event| {
                            *event == LifecycleEvent::TransactionAttached
                        }),
                        0
                    );
                    assert_eq!(
                        event_count(&events.borrow(), |event| {
                            *event == LifecycleEvent::Destroy
                        }),
                        1
                    );
                }
            })
        });
    }

    fn assert_real_cancel_before_request_headers() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let body_reads = Rc::new(Cell::new(0));
                let stops = Rc::new(RefCell::new(Vec::new()));
                let source =
                    PendingRequestSource::post(10, true, Rc::clone(&body_reads), Rc::clone(&stops));
                let cancellation = CancellationSource::new();
                let mut send = Box::pin(handle.send_request(
                    HttpSourceHolder::new(Box::new(source)),
                    cancellation.token(),
                ));
                tokio::select! {
                    _ = send.as_mut() => panic!("pending header read completed"),
                    _ = tokio::task::yield_now() => {}
                }
                assert!(cancellation.request_cancellation());
                let error = match tokio::time::timeout(Duration::from_secs(1), send.as_mut())
                    .await
                    .expect("header cancellation timed out")
                {
                    Ok(_) => panic!("cancelled header read admitted a request"),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                drop(send);
                assert_eq!(body_reads.get(), 0);
                assert_eq!(stops.borrow().as_slice(), [None]);
                assert!(handle.initiate_drain().expect("drain accepted"));
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("pre-header cancellation cleanup timed out")
                    .expect("drained session completes");
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert!(decoded.header_streams.is_empty());
                assert!(decoded.resets.is_empty());
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_cancel_during_request_body() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let body_reads = Rc::new(Cell::new(0));
                let stops = Rc::new(RefCell::new(Vec::new()));
                let source = PendingRequestSource::post(
                    10,
                    false,
                    Rc::clone(&body_reads),
                    Rc::clone(&stops),
                );
                let cancellation = CancellationSource::new();
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(source)),
                        cancellation.token(),
                    )
                    .await
                    .expect("request headers admitted")
                    .source;
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while body_reads.get() == 0 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before request body read: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("request body read started");
                assert!(cancellation.request_cancellation());
                let mut response_read = Box::pin(response.read_header_event());
                let error = tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::select! {
                        result = response_read.as_mut() => match result {
                            Ok(_) => panic!("cancelled request produced response headers"),
                            Err(error) => error,
                        },
                        result = run.as_mut() => {
                            panic!("session ended before response cancellation: {result:?}")
                        }
                    }
                })
                .await
                .expect("response cancellation timed out");
                drop(response_read);
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert!(handle.initiate_drain().expect("drain request"));
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("body cancellation cleanup timed out")
                    .expect("drained session completes");
                assert_eq!(stops.borrow().as_slice(), [Some(HttpErrorCode::CANCEL)]);
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(
                    decoded.resets,
                    [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
                );
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_downstream_graceful_ingress_termination(fail: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = peer_incomplete_post_bytes(2_000);
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(GracefulEarlyResponseHandler {
                    status: 401,
                    fail,
                    invocations: Rc::clone(&invocations),
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_millis(5),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                let expected_status = if fail { 500 } else { 401 };
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let wire = state.borrow().writes.concat();
                        let decoded = decode_server_frames(&wire);
                        if decoded
                            .header_statuses
                            .contains(&(StreamId::new(1), expected_status))
                            && decoded
                                .resets
                                .contains(&(StreamId::new(1), H2ErrorCode::NO_ERROR))
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before early response/reset: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("early response and reset were written");
                assert!(handle.initiate_drain().expect("drain accepted"));
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("early-response cleanup timed out")
                    .expect("drained session completes");
                assert_eq!(invocations.get(), 1);
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert!(
                    decoded
                        .header_statuses
                        .contains(&(StreamId::new(1), expected_status))
                );
                assert_eq!(decoded.resets, [(StreamId::new(1), H2ErrorCode::NO_ERROR)]);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_idle_timeout(direction: TransportDirection) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let handler: Option<Rc<dyn HttpHandler>> = direction.is_downstream().then(|| {
                    Rc::new(FixtureHandler {
                        status: 200,
                        response_body: 0,
                        response_error: None,
                        invocations: Rc::new(Cell::new(0)),
                        request_body: Rc::new(Cell::new(0)),
                    }) as Rc<dyn HttpHandler>
                });
                let config = UniplexConfig {
                    read_timeout: Duration::from_millis(5),
                    ..UniplexConfig::default()
                };
                let http2 = Http2SessionConfig {
                    goaway_timeout: Duration::from_millis(1),
                    ..Http2SessionConfig::default()
                };
                let (session, _) = UniplexSession::new_http2(
                    context, direction, transport, handler, config, http2,
                )
                .expect("idle H2 session");
                let started = Instant::now();
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("idle timeout session hung")
                    .expect("idle timeout drains gracefully");
                assert!(started.elapsed() >= Duration::from_millis(5));
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
                let wire = state.borrow().writes.concat();
                let goaways = if direction.is_upstream() {
                    decode_client_frames(&wire).goaways
                } else {
                    decode_server_frames_with_requests(&wire, 0).goaways
                };
                assert!(!goaways.is_empty());
                assert!(
                    goaways
                        .iter()
                        .all(|(_, code)| *code == H2ErrorCode::NO_ERROR)
                );
            })
        });
    }

    fn assert_real_transport_write_timeout(direction: TransportDirection) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let reads = direction.is_downstream().then(|| {
                    FixtureRead::Data(peer_request_bytes(
                        HttpMethod::Get,
                        &[],
                        None,
                        PeerControl::None,
                    ))
                });
                let (transport, state) = FixtureTransport::new(reads);
                let invocations = Rc::new(Cell::new(0));
                let handler: Option<Rc<dyn HttpHandler>> = direction.is_downstream().then(|| {
                    Rc::new(StallWriteHandler {
                        transport: Rc::clone(&state),
                        invocations: Rc::clone(&invocations),
                    }) as Rc<dyn HttpHandler>
                });
                if direction.is_upstream() {
                    state.borrow_mut().stall_writes = true;
                }
                let config = UniplexConfig {
                    write_timeout: Duration::from_millis(5),
                    ..UniplexConfig::default()
                };
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    direction,
                    transport,
                    handler,
                    config,
                    Http2SessionConfig::default(),
                )
                .expect("H2 session");
                let mut response = if direction.is_upstream() {
                    let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed GET");
                    Some(
                        handle
                            .send_request(
                                HttpSourceHolder::new(Box::new(request)),
                                CancellationSource::new().token(),
                            )
                            .await
                            .expect("request admitted")
                            .source,
                    )
                } else {
                    None
                };
                let mut run = Box::pin(session.run());
                if let Some(response) = response.as_mut() {
                    let mut response_read = Box::pin(response.read_header_event());
                    let error = tokio::time::timeout(Duration::from_secs(1), async {
                        tokio::select! {
                            result = response_read.as_mut() => match result {
                                Ok(_) => panic!("write-failed request produced response headers"),
                                Err(error) => error,
                            },
                            result = run.as_mut() => {
                                panic!("write-failed session ended before response error: {result:?}")
                            }
                        }
                    })
                    .await
                    .expect("write-failed response read timed out");
                    assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
                    drop(response_read);
                } else {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while invocations.get() == 0 {
                            tokio::select! {
                                result = run.as_mut() => {
                                    panic!("write-failed session ended before handler: {result:?}")
                                }
                                _ = tokio::task::yield_now() => {}
                            }
                        }
                    })
                    .await
                    .expect("downstream handler did not run");
                }
                drop(response);
                let error = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("write timeout session hung")
                    .expect_err("write timeout fails the session");
                assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
                if direction.is_downstream() {
                    assert_eq!(invocations.get(), 1);
                }
                assert_eq!(state.borrow().close, FixtureClose::Reset);
            })
        });
    }

    #[derive(Clone, Copy)]
    enum RequestTimeoutCase {
        PendingPost,
        CompletePost,
        CompletePostCustom,
        CompleteGet,
    }

    fn assert_real_upstream_response_timeout(case: RequestTimeoutCase) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let stream_timeout = if matches!(case, RequestTimeoutCase::CompletePostCustom) {
                    Duration::from_secs(1)
                } else {
                    Duration::from_millis(5)
                };
                let config = UniplexConfig {
                    read_timeout: Duration::from_millis(100),
                    stream_read_timeout: stream_timeout,
                    ..UniplexConfig::default()
                };
                let http2 = Http2SessionConfig {
                    goaway_timeout: Duration::from_millis(1),
                    ..Http2SessionConfig::default()
                };
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    config,
                    http2,
                )
                .expect("upstream H2 session");
                let body_reads = Rc::new(Cell::new(0));
                let stops = Rc::new(RefCell::new(Vec::new()));
                let request: Box<dyn HttpSource> = match case {
                    RequestTimeoutCase::PendingPost => Box::new(PendingRequestSource::post(
                        100,
                        false,
                        Rc::clone(&body_reads),
                        Rc::clone(&stops),
                    )),
                    RequestTimeoutCase::CompletePost | RequestTimeoutCase::CompletePostCustom => {
                        Box::new(
                            HttpFixedSource::make_fixed_request(
                                b"/",
                                HttpMethod::Post,
                                Some(queue(&[b'x'; 100])),
                            )
                            .expect("fixed POST"),
                        )
                    }
                    RequestTimeoutCase::CompleteGet => {
                        Box::new(HttpFixedSource::make_get_request(b"/", None).expect("fixed GET"))
                    }
                };
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(request),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                if matches!(case, RequestTimeoutCase::CompletePostCustom) {
                    response.set_read_timeout(Duration::from_millis(5));
                }
                let mut run = Box::pin(session.run());
                let mut response_read = Box::pin(response.read_header_event());

                if matches!(case, RequestTimeoutCase::PendingPost) {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while body_reads.get() == 0 {
                            tokio::select! {
                                _ = response_read.as_mut() => {
                                    panic!("response timeout armed before request EOM")
                                }
                                result = run.as_mut() => {
                                    panic!("session ended with pending request body: {result:?}")
                                }
                                _ = tokio::task::yield_now() => {}
                            }
                        }
                    })
                    .await
                    .expect("request body read started");
                    tokio::select! {
                        _ = response_read.as_mut() => panic!("pending request armed response timer"),
                        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                    }
                    response.stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
                    let error = match response_read.as_mut().await {
                        Ok(_) => panic!("cancelled response produced headers"),
                        Err(error) => error,
                    };
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                } else {
                    let error = tokio::time::timeout(Duration::from_millis(100), async {
                        tokio::select! {
                            result = response_read.as_mut() => match result {
                                Ok(_) => panic!("silent peer produced response headers"),
                                Err(error) => error,
                            },
                            result = run.as_mut() => {
                                panic!("session ended before response timeout: {result:?}")
                            }
                        }
                    })
                    .await
                    .expect("response read timeout did not fire");
                    assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                }
                drop(response_read);
                drop(response);
                let _ = handle.initiate_drain();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("response-timeout cleanup hung")
                    .expect("response timeout is stream-local");
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.resets, [(StreamId::new(1), H2ErrorCode::CANCEL)]);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
                if matches!(case, RequestTimeoutCase::PendingPost) {
                    assert_eq!(stops.borrow().as_slice(), [Some(HttpErrorCode::CANCEL)]);
                }
            })
        });
    }

    fn assert_real_downstream_request_read_timeout() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_incomplete_post_bytes(100);
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let observed = Rc::new(Cell::new(None));
                let handler: Rc<dyn HttpHandler> = Rc::new(TimeoutReadingHandler {
                    observed: Rc::clone(&observed),
                });
                let config = UniplexConfig {
                    read_timeout: Duration::from_millis(100),
                    stream_read_timeout: Duration::from_millis(5),
                    ..UniplexConfig::default()
                };
                let http2 = Http2SessionConfig {
                    goaway_timeout: Duration::from_millis(1),
                    ..Http2SessionConfig::default()
                };
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    config,
                    http2,
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let decoded = decode_server_frames(&state.borrow().writes.concat());
                        if observed.get() == Some(HttpErrorCode::READ_TIMEOUT)
                            && decoded.header_statuses.contains(&(StreamId::new(1), 408))
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before automatic 408: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("request read timeout/408 hung");
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(
                    decoded.header_statuses,
                    [(StreamId::new(1), 100), (StreamId::new(1), 408)]
                );
                let _ = handle.initiate_drain();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("read-timeout cleanup hung")
                    .expect("read timeout remains stream-local");
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_partial_headers_use_connection_timeout() {
        run_on_worker(|context| {
            Box::pin(async move {
                let mut input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                input.pop().expect("request has a final header byte");
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let config = UniplexConfig {
                    read_timeout: Duration::from_millis(10),
                    stream_read_timeout: Duration::from_millis(1),
                    ..UniplexConfig::default()
                };
                let http2 = Http2SessionConfig {
                    goaway_timeout: Duration::from_millis(1),
                    ..Http2SessionConfig::default()
                };
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    config,
                    http2,
                )
                .expect("downstream H2 session");
                let started = Instant::now();
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("partial-header timeout hung")
                    .expect("idle partial headers drain gracefully");
                assert!(started.elapsed() >= Duration::from_millis(10));
                assert_eq!(invocations.get(), 0);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 0);
                assert!(decoded.header_streams.is_empty());
                assert!(decoded.goaways.iter().any(|(stream, code)| {
                    *stream == StreamId::UNSPECIFIED && *code == H2ErrorCode::NO_ERROR
                }));
            })
        });
    }

    fn poll_h2_timeout_once(
        timer: Pin<&mut (impl Future<Output = ()> + ?Sized)>,
    ) -> std::task::Poll<()> {
        timer.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    #[test]
    fn flow_timeout_retains_legacy_overflow_fallback() {
        use crate::uniplex::h2_flow_timeout_for_test;

        run_on_worker(|_| {
            Box::pin(async {
                tokio::time::pause();
                assert!(
                    tokio::time::Instant::now()
                        .checked_add(Duration::MAX)
                        .is_none()
                );
                {
                    let (deadline, flow_timeout) = h2_flow_timeout_for_test(true, Duration::MAX)
                        .expect("overflow retains an armed flow timer");
                    let legacy_timeout = tokio::time::sleep(Duration::MAX);
                    assert_eq!(deadline, legacy_timeout.deadline().into_std());
                    assert!(deadline > tokio::time::Instant::now().into_std());
                    let mut flow_timeout = std::pin::pin!(flow_timeout);
                    let mut legacy_timeout = std::pin::pin!(legacy_timeout);
                    assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_pending());
                    assert!(poll_h2_timeout_once(legacy_timeout.as_mut()).is_pending());
                    tokio::time::advance(Duration::from_secs(1)).await;
                    assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_pending());
                    assert!(poll_h2_timeout_once(legacy_timeout.as_mut()).is_pending());
                }
                tokio::task::yield_now().await;
            })
        });
    }

    #[test]
    fn flow_timeout_retains_tokio_clock_origin() {
        use crate::uniplex::h2_flow_timeout_for_test;

        run_on_worker(|_| {
            Box::pin(async {
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(60)).await;
                let duration = Duration::from_millis(200);
                let first_deadline;
                {
                    let origin = tokio::time::Instant::now();
                    let (deadline, flow_timeout) = h2_flow_timeout_for_test(true, duration)
                        .expect("blocked work arms a relative flow timer");
                    first_deadline = deadline;
                    assert_eq!(first_deadline, (origin + duration).into_std());
                    let mut flow_timeout = std::pin::pin!(flow_timeout);
                    assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_pending());
                    tokio::time::advance(Duration::from_millis(100)).await;
                    assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_pending());
                }
                let origin = tokio::time::Instant::now();
                let (deadline, flow_timeout) = h2_flow_timeout_for_test(true, duration)
                    .expect("eligible work rearms after the previous wait is dropped");
                let rearmed_deadline = deadline;
                assert_eq!(rearmed_deadline, (origin + duration).into_std());
                assert!(rearmed_deadline > first_deadline);
                let mut flow_timeout = std::pin::pin!(flow_timeout);
                assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_pending());
                tokio::time::timeout(Duration::from_secs(1), flow_timeout.as_mut())
                    .await
                    .expect("the retained rearmed timer completes");
                assert_eq!(deadline, rearmed_deadline);
                assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_ready());
                assert!(tokio::time::Instant::now().into_std() >= rearmed_deadline);
            })
        });
    }

    #[test]
    fn timeout_arbitration_uses_tokio_expiration_clock() {
        use crate::uniplex::WriteWakeReason;
        use crate::uniplex::arbitrate_h2_timeout_at_current_time_for_test;
        use crate::uniplex::h2_flow_timeout_for_test;

        run_on_worker(|_| {
            Box::pin(async {
                tokio::time::pause();
                let stream = StreamId::new(3);
                let buffer_deadline = Instant::now() + Duration::from_secs(60);
                let buffer_timeout =
                    tokio::time::sleep_until(tokio::time::Instant::from_std(buffer_deadline));
                let (flow_deadline, flow_timeout) =
                    h2_flow_timeout_for_test(true, Duration::from_secs(120))
                        .expect("blocked work arms a flow timer");
                assert!(buffer_deadline < flow_deadline);
                let selected = arbitrate_h2_timeout_at_current_time_for_test(
                    WriteWakeReason::FlowTimeout,
                    Some(flow_deadline),
                    Some((stream, buffer_deadline)),
                );
                assert!(matches!(selected, WriteWakeReason::FlowTimeout));
                let mut flow_timeout = std::pin::pin!(flow_timeout);
                let mut buffer_timeout = std::pin::pin!(buffer_timeout);
                assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_pending());
                assert!(poll_h2_timeout_once(buffer_timeout.as_mut()).is_pending());
                tokio::time::advance(Duration::from_secs(121)).await;
                tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::join!(flow_timeout.as_mut(), buffer_timeout.as_mut());
                })
                .await
                .expect("both retained waits expire on Tokio time");
                assert!(poll_h2_timeout_once(flow_timeout.as_mut()).is_ready());
                assert!(buffer_timeout.is_elapsed());
                assert!(Instant::now() < buffer_deadline);
                assert!(tokio::time::Instant::now().into_std() >= flow_deadline);
                let selected = arbitrate_h2_timeout_at_current_time_for_test(
                    WriteWakeReason::FlowTimeout,
                    Some(flow_deadline),
                    Some((stream, buffer_deadline)),
                );
                assert!(
                    matches!(selected, WriteWakeReason::BufferTimeout(selected_stream) if selected_stream == stream),
                    "Tokio-expired earlier buffer deadline must win: {selected:?}"
                );
            })
        });
    }

    #[test]
    fn zero_write_timeout_keeps_flow_wait_disabled() {
        use crate::uniplex::h2_flow_timeout_for_test;

        assert!(h2_flow_timeout_for_test(true, Duration::ZERO).is_none());
        assert!(h2_flow_timeout_for_test(false, Duration::MAX).is_none());
        run_on_worker(|_| {
            Box::pin(async {
                tokio::time::pause();
                for _ in 0..2 {
                    assert!(h2_flow_timeout_for_test(true, Duration::ZERO).is_none());
                    assert!(h2_flow_timeout_for_test(false, Duration::from_millis(200)).is_none());
                    tokio::time::advance(Duration::from_secs(1)).await;
                }
            })
        });
    }

    #[test]
    fn timeout_arbitration_prefers_strictly_earlier_expired_buffer() {
        use crate::uniplex::WriteWakeReason;
        use crate::uniplex::arbitrate_h2_timeout_for_test;

        let start = Instant::now();
        let stream = StreamId::new(3);
        let buffer_deadline = start + Duration::from_millis(200);
        let flow_deadline = start + Duration::from_millis(350);
        for now in [flow_deadline, flow_deadline + Duration::from_secs(1)] {
            let selected = arbitrate_h2_timeout_for_test(
                WriteWakeReason::FlowTimeout,
                Some(flow_deadline),
                Some((stream, buffer_deadline)),
                now,
            );
            assert!(
                matches!(selected, WriteWakeReason::BufferTimeout(selected_stream) if selected_stream == stream),
                "earlier expired buffer deadline must win: {selected:?}"
            );
        }
    }

    #[test]
    fn timeout_arbitration_preserves_reverse_ties_and_other_wakes() {
        use crate::uniplex::WriteWakeReason;
        use crate::uniplex::arbitrate_h2_timeout_for_test;

        let start = Instant::now();
        let stream = StreamId::new(3);
        let early = start + Duration::from_millis(100);
        let middle = start + Duration::from_millis(200);
        let late = start + Duration::from_millis(300);
        let expired = start + Duration::from_secs(1);
        for (case, flow_deadline, buffer_timeout, now) in [
            ("reverse", Some(early), Some((stream, late)), expired),
            ("tie", Some(middle), Some((stream, middle)), expired),
            ("unexpired buffer", Some(late), Some((stream, early)), start),
            ("absent or ineligible buffer", Some(middle), None, expired),
            ("unarmed flow", None, Some((stream, early)), expired),
        ] {
            let selected = arbitrate_h2_timeout_for_test(
                WriteWakeReason::FlowTimeout,
                flow_deadline,
                buffer_timeout,
                now,
            );
            assert!(
                matches!(selected, WriteWakeReason::FlowTimeout),
                "{case} preserves selected flow timeout: {selected:?}"
            );
        }
        for buffer_deadline in [early, middle, late] {
            for selected in [
                WriteWakeReason::Work,
                WriteWakeReason::BufferTimeout(StreamId::new(5)),
                WriteWakeReason::GoawayTimeout,
            ] {
                let expected_kind = std::mem::discriminant(&selected);
                let result = arbitrate_h2_timeout_for_test(
                    selected,
                    Some(middle),
                    Some((stream, buffer_deadline)),
                    expired,
                );
                assert_eq!(std::mem::discriminant(&result), expected_kind);
                if let WriteWakeReason::BufferTimeout(selected_stream) = result {
                    assert_eq!(selected_stream, StreamId::new(5));
                }
            }
        }
        for code in [HttpErrorCode::WRITE_TIMEOUT, HttpErrorCode::CORO_CANCELLED] {
            let selected = arbitrate_h2_timeout_for_test(
                WriteWakeReason::ByteEventError(HttpError::with_message(
                    code,
                    b"preserved selected error",
                )),
                Some(middle),
                Some((stream, early)),
                expired,
            );
            let WriteWakeReason::ByteEventError(error) = selected else {
                panic!("selected byte-event error was replaced: {selected:?}");
            };
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), b"preserved selected error");
        }
    }

    fn assert_real_transaction_buffer_timeout() {
        use roxygen_core::stream_source::FlowControlState;
        use roxygen_core::stream_source::HttpStreamSource;

        run_on_worker(|context| {
            Box::pin(async move {
                const K: u32 = 65_535;
                let stream = StreamId::new(3);
                let (initial, update) = peer_two_requests_with_flow_update();
                let release_update = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitDataEof(Arc::clone(&release_update), update),
                ]);
                let completed = Rc::new(RefCell::new(Vec::new()));
                let source_drops = Rc::new(Cell::new(0));
                let processed = Rc::new(RefCell::new(Vec::new()));
                let source = HttpStreamSource::new_session(
                    stream,
                    UploadCompletion {
                        events: Rc::clone(&completed),
                        drops: Rc::clone(&source_drops),
                        processed: Rc::clone(&processed),
                    },
                    3 * K,
                    Duration::ZERO,
                )
                .unwrap();
                let producer = source.producer();
                let mut response = HttpMessage::new();
                response.set_status_code(200).unwrap();
                response.set_http_version(1, 1);
                response.headers_mut().set(
                    HttpHeaderCode::ContentLength,
                    (3 * K).to_string().as_bytes(),
                );
                assert!(producer.headers(response, false));
                for chunk in vec![b'r'; (2 * K) as usize].chunks(16 * 1024) {
                    assert_eq!(
                        producer.body(queue(chunk), 0, false),
                        FlowControlState::Open
                    );
                }
                assert!(!source.input_finished());
                let sibling = HttpFixedSource::make_fixed_response(
                    200,
                    Some(queue(&vec![b'r'; (2 * K) as usize])),
                )
                .unwrap();
                let handler = Rc::new(EligibilitySequenceHandler(RefCell::new(VecDeque::from([
                    HttpSourceHolder::new(Box::new(sibling)),
                    HttpSourceHolder::new(Box::new(source)),
                ]))));
                let config = UniplexConfig {
                    write_timeout: Duration::from_millis(200),
                    ..UniplexConfig::default()
                };
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport.clone(),
                    Some(handler.clone()),
                    config,
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let phases = handle
                    .observe_h2_timeout_arms_for_test(stream)
                    .expect("observe native H2 timeout arms");
                let control = handle
                    .observe_h2_source_timers_for_test(stream)
                    .expect("control native H2 writer waits");
                control.pause_next_writer_wait.set(true);
                let clock_paused = Cell::new(false);
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(3),
                        async {
                            let (before_credit, first_wait) =
                                tokio::time::timeout(Duration::from_secs(1), async {
                                    let mut previous_sequence = 0;
                                    loop {
                                        let held =
                                            eligibility_flow_snapshot(&control, |snapshot| {
                                                snapshot.sequence > previous_sequence
                                                    && control.writer_wait_paused.get()
                                            })
                                            .await;
                                        let decoded = decode_server_frames_with_requests(
                                            &state.borrow().writes.concat(),
                                            2,
                                        );
                                        if decoded.header_streams.contains(&StreamId::new(1))
                                            && decoded.header_streams.contains(&stream)
                                            && let Some(snapshot) = phases.get().blocked
                                        {
                                            break (snapshot, held);
                                        }
                                        previous_sequence = held.sequence;
                                        control.pause_next_writer_wait.set(true);
                                        control.resume_writer_wait.notify_one();
                                    }
                                })
                                .await
                                .expect("both buffered responses did not reach a held writer wait");
                            tokio::time::pause();
                            clock_paused.set(true);
                            let paused_at = tokio::time::Instant::now().into_std();
                            assert!(
                                paused_at < before_credit.buffer_deadline
                                    && paused_at < before_credit.flow_deadline,
                                "the initial held wait must be captured before either deadline"
                            );
                            assert_eq!(
                                first_wait.buffer_timeout,
                                Some((stream, before_credit.buffer_deadline))
                            );
                            assert_eq!(before_credit.connection_credit, 0);
                            assert_eq!(before_credit.stream_credit, 0);
                            assert!(before_credit.buffer_deadline <= before_credit.flow_deadline);
                            assert_eq!(*processed.borrow(), vec![K as usize, K as usize]);
                            assert!(completed.borrow().is_empty());
                            assert_eq!(source_drops.get(), 0);
                            assert!(handler.0.borrow().is_empty());
                            let before_wire = eligibility_wire(&state);
                            assert_eq!(before_wire.body[&stream].len(), K as usize);
                            assert!(!before_wire.completed.contains(&stream));
                            control.pause_next_writer_wait.set(true);
                            release_update.notify_one();
                            control.resume_writer_wait.notify_one();
                            let (after_credit, rearmed_wait) = {
                                let mut previous_sequence = first_wait.sequence;
                                loop {
                                    let held = eligibility_flow_snapshot(&control, |snapshot| {
                                        snapshot.sequence > previous_sequence
                                            && control.writer_wait_paused.get()
                                    })
                                    .await;
                                    if let Some(snapshot) = phases.get().rearmed {
                                        break (snapshot, held);
                                    }
                                    previous_sequence = held.sequence;
                                    control.pause_next_writer_wait.set(true);
                                    control.resume_writer_wait.notify_one();
                                }
                            };
                            assert_eq!(tokio::time::Instant::now().into_std(), paused_at);
                            assert!(after_credit.generation > before_credit.generation);
                            assert!(after_credit.flow_deadline > before_credit.flow_deadline);
                            assert_eq!(after_credit.buffer_deadline, before_credit.buffer_deadline);
                            assert!(after_credit.buffer_deadline < after_credit.flow_deadline);
                            assert!(after_credit.connection_credit > 0);
                            assert_eq!(after_credit.stream_credit, 0);
                            assert_eq!(
                                rearmed_wait.flow_deadline,
                                Some(after_credit.flow_deadline)
                            );
                            assert_eq!(
                                rearmed_wait.buffer_timeout,
                                Some((stream, before_credit.buffer_deadline))
                            );
                            assert!(completed.borrow().is_empty());
                            assert_eq!(source_drops.get(), 0);
                            tokio::time::resume();
                            clock_paused.set(false);
                            tokio::time::sleep_until(tokio::time::Instant::from_std(
                                after_credit.flow_deadline,
                            ))
                            .await;
                            assert!(control.writer_wait_paused.get());
                            assert!(
                                tokio::time::Instant::now().into_std()
                                    >= after_credit.flow_deadline
                            );
                            assert!(completed.borrow().is_empty());
                            assert_eq!(source_drops.get(), 0);
                            assert!(eligibility_wire(&state).resets.is_empty());
                            let released_at = Instant::now();
                            assert!(released_at >= after_credit.flow_deadline);
                            control.resume_writer_wait.notify_one();
                            tokio::time::timeout(
                                Duration::from_secs(2),
                                complete_credit_wait(|| {
                                    let wire = eligibility_wire(&state);
                                    !completed.borrow().is_empty()
                                        && wire.resets == [(stream, H2ErrorCode::INTERNAL_ERROR)]
                                        && wire.completed.contains(&StreamId::new(1))
                                }),
                            )
                            .await
                            .expect("transaction buffer timeout hung");
                            let completed_at = {
                                let events = completed.borrow();
                                assert_eq!(events.len(), 1);
                                assert_eq!(
                                    (events[0].0, events[0].1),
                                    (stream, Some(HttpErrorCode::READ_TIMEOUT))
                                );
                                events[0].2
                            };
                            assert!(completed_at >= before_credit.buffer_deadline);
                            assert!(completed_at >= after_credit.flow_deadline);
                            assert!(completed_at >= released_at);
                            assert_eq!(*processed.borrow(), vec![K as usize, K as usize]);
                            assert_eq!(source_drops.get(), 1);
                            let decoded = decode_server_frames_with_requests(
                                &state.borrow().writes.concat(),
                                2,
                            );
                            let mut headers = decoded.header_streams;
                            headers.sort();
                            assert_eq!(headers, [StreamId::new(1), stream]);
                            assert!(decoded.body_streams.contains(&StreamId::new(1)));
                            assert_eq!(decoded.resets, [(stream, H2ErrorCode::INTERNAL_ERROR)]);
                            let wire = eligibility_wire(&state);
                            assert_eq!(wire.body[&StreamId::new(1)], vec![b'r'; (2 * K) as usize]);
                            assert_eq!(wire.body[&stream], vec![b'r'; K as usize]);
                            assert_eq!(wire.completed, [StreamId::new(1)]);
                        },
                    ))
                    .await;
                    if clock_paused.replace(false) {
                        tokio::time::resume();
                    }
                    control.pause_next_writer_wait.set(false);
                    control.resume_writer_wait.notify_one();
                    release_update.notify_one();
                    let _ = handle.initiate_drain();
                    outcome
                };
                let (run, exercise) =
                    tokio::join!(eligibility_catch_unwind(session.run()), exercise);
                let finished = handle.is_finished();
                let sources_consumed = handler.0.borrow().is_empty();
                drop(handle);
                drop(handler);
                drop(producer);
                drop(transport);
                if let Err(payload) = exercise {
                    resume_unwind(payload);
                }
                if let Err(payload) = run {
                    resume_unwind(payload);
                }
                exercise
                    .unwrap()
                    .expect("bounded transaction buffer timeout fixture");
                run.unwrap().expect("one stream timeout stays stream-local");
                assert!(finished);
                assert!(sources_consumed);
                assert_eq!(source_drops.get(), 1);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
                assert!(state.borrow().transport_dropped);
            })
        });
    }

    #[derive(Clone, Copy)]
    enum NativeResolution {
        Unsupported,
        Deliver,
        Timeout,
        Immediate,
        Pending,
    }

    struct ByteEventOutcome {
        delivered: Vec<HttpByteEvent>,
        cancelled: Vec<(HttpByteEvent, HttpErrorCode)>,
        write_flags: Vec<ByteEventWriteFlags>,
        error: Option<HttpErrorCode>,
        close: FixtureClose,
    }

    fn run_registered_response(
        kind: RegisteredResponseKind,
        resolution: NativeResolution,
    ) -> ByteEventOutcome {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let capability = if matches!(resolution, NativeResolution::Unsupported) {
                    ByteEventCapability::Unsupported
                } else {
                    ByteEventCapability::TransportEvents
                };
                let (transport, transport_state) = FixtureTransport::with_capability(
                    [FixtureRead::DataEof(input)],
                    capability,
                    matches!(resolution, NativeResolution::Immediate),
                );
                if matches!(kind, RegisteredResponseKind::Many(_)) {
                    transport_state.borrow_mut().native_event_limit = Some(96);
                }
                let probe = Rc::new(ByteEventProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(RegisteredResponseHandler {
                    probe: Rc::clone(&probe),
                    kind,
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                if matches!(resolution, NativeResolution::Timeout) {
                    session
                        .set_byte_event_timeout(Duration::from_millis(1))
                        .expect("byte-event timeout");
                }
                let mut run = Box::pin(session.run());
                if matches!(resolution, NativeResolution::Deliver) {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        loop {
                            let ready = {
                                let state = transport_state.borrow();
                                state.close == FixtureClose::ShutdownWrite
                                    && !state.registrations.is_empty()
                            };
                            if ready {
                                break;
                            }
                            tokio::select! {
                                result = run.as_mut() => {
                                    panic!("session completed before native delivery: {result:?}")
                                }
                                _ = tokio::task::yield_now() => {}
                            }
                        }
                    })
                    .await
                    .expect("native byte-event boundary was registered");
                    let offset = transport_state
                        .borrow()
                        .writes
                        .iter()
                        .map(Vec::len)
                        .sum::<usize>() as u64;
                    handle
                        .deliver_socket_byte_event(SocketByteEventType::Tx, offset)
                        .expect("TX delivery");
                    handle
                        .deliver_socket_byte_event(SocketByteEventType::Ack, offset)
                        .expect("ACK delivery");
                }
                let result = tokio::time::timeout(Duration::from_secs(2), run.as_mut())
                    .await
                    .expect("registered response session timed out");
                let delivered = std::mem::take(&mut *probe.delivered.borrow_mut());
                let cancelled = std::mem::take(&mut *probe.cancelled.borrow_mut());
                let state = transport_state.borrow();
                ByteEventOutcome {
                    delivered,
                    cancelled,
                    write_flags: state.write_flags.clone(),
                    error: result.err().map(|error| error.code()),
                    close: state.close,
                }
            })
        })
    }

    fn assert_real_simple_byte_events() {
        let outcome =
            run_registered_response(RegisteredResponseKind::Simple, NativeResolution::Deliver);
        assert_eq!(outcome.error, None);
        assert!(outcome.cancelled.is_empty());
        assert_eq!(outcome.delivered.len(), 6);
        assert_eq!(outcome.close, FixtureClose::ShutdownWrite);
        let header = outcome
            .delivered
            .iter()
            .filter(|event| event.field_section_info.is_some())
            .collect::<Vec<_>>();
        assert_eq!(header.len(), 4);
        assert!(header.into_iter().all(|event| {
            event.stream_id == Some(StreamId::new(1))
                && event.body_offset == 0
                && event.field_section_info.is_some_and(|info| {
                    info.section_type == roxygen_core::byte_event::FieldSectionType::Headers
                        && info.final_headers
                })
                && event.stream_offset <= event.transport_offset
        }));
        let body = outcome
            .delivered
            .iter()
            .filter(|event| event.field_section_info.is_none())
            .collect::<Vec<_>>();
        assert_eq!(body.len(), 2);
        assert!(body.into_iter().all(|event| {
            event.body_offset == 100
                && event.eom
                && event.field_section_info.is_none()
                && event.stream_offset <= event.transport_offset
        }));
        assert!(
            outcome
                .write_flags
                .iter()
                .any(|flags| flags.timestamp_tx && flags.timestamp_ack)
        );
    }

    fn assert_real_trailer_byte_events() {
        let outcome = run_registered_response(
            RegisteredResponseKind::Trailers,
            NativeResolution::Unsupported,
        );
        assert_eq!(outcome.error, None);
        assert!(outcome.cancelled.is_empty());
        assert_eq!(outcome.delivered.len(), 2);
        assert_eq!(outcome.delivered[0].body_offset, 100);
        assert!(!outcome.delivered[0].eom);
        let trailer = &outcome.delivered[1];
        assert_eq!(trailer.body_offset, 100);
        assert!(trailer.eom);
        assert!(trailer.field_section_info.is_some_and(|info| {
            info.section_type == roxygen_core::byte_event::FieldSectionType::Trailers
                && info.final_headers
        }));
        assert!(trailer.transport_offset > outcome.delivered[0].transport_offset);
    }

    fn assert_real_byte_event_timeout() {
        let outcome = run_registered_response(
            RegisteredResponseKind::NativeOnly,
            NativeResolution::Timeout,
        );
        assert_eq!(outcome.error, None);
        assert!(outcome.delivered.is_empty());
        assert_eq!(outcome.cancelled.len(), 1);
        assert_eq!(outcome.cancelled[0].0.event_type, ByteEventType::NicTx);
        assert_eq!(outcome.cancelled[0].1, HttpErrorCode::READ_TIMEOUT);
    }

    fn assert_real_immediate_tx_ack_events() {
        let outcome =
            run_registered_response(RegisteredResponseKind::Simple, NativeResolution::Immediate);
        assert_eq!(outcome.error, None);
        assert!(outcome.cancelled.is_empty());
        assert_eq!(
            outcome
                .delivered
                .iter()
                .filter(|event| matches!(
                    event.event_type,
                    ByteEventType::NicTx | ByteEventType::CumulativeAck
                ))
                .count(),
            2
        );
    }

    fn assert_real_too_many_byte_events() {
        let outcome =
            run_registered_response(RegisteredResponseKind::Many(100), NativeResolution::Pending);
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.close, FixtureClose::ShutdownWrite);
        assert_eq!(outcome.delivered.len(), 96);
        assert_eq!(outcome.cancelled.len(), 104);
        assert!(
            outcome
                .cancelled
                .iter()
                .all(|(_, code)| *code == HttpErrorCode::CANCEL)
        );
    }

    fn assert_real_dead_byte_event_callback_is_ignored() {
        let drops = Arc::new(AtomicUsize::new(0));
        let drops_for_worker = Arc::clone(&drops);
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let handler: Rc<dyn HttpHandler> = Rc::new(DeadByteEventCallbackHandler {
                    drops: drops_for_worker,
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                session.run().await.expect("dead callback is ignored");
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    fn assert_real_write_error_cancels_byte_events() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let events = Rc::new(ByteEventProbe::default());
                let callback = Rc::new(WriteFailProbe {
                    probe: Rc::clone(&events),
                    transport: Rc::clone(&state),
                });
                let handler: Rc<dyn HttpHandler> =
                    Rc::new(WriteFailureByteEventHandler { probe: callback });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let error = session
                    .run()
                    .await
                    .expect_err("write failure reaches session");
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                assert_eq!(events.delivered.borrow().len(), 1);
                let cancelled = events.cancelled.borrow();
                assert_eq!(cancelled.len(), 2);
                assert!(cancelled.iter().all(|(event, code)| {
                    event.event_type == ByteEventType::KernelWrite
                        && *code == HttpErrorCode::TRANSPORT_WRITE_ERROR
                }));
            })
        });
    }

    fn assert_real_protocol_error_cancels_byte_events() {
        run_on_worker(|context| {
            Box::pin(async move {
                let mut input = peer_request_bytes(
                    HttpMethod::Get,
                    &[],
                    None,
                    PeerControl::WindowUpdate {
                        connection: true,
                        amount: 0x7fff_ffff,
                    },
                );
                let connection_error = input.split_off(input.len() - 13);
                let release_error = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::with_capability(
                    [
                        FixtureRead::Data(input),
                        FixtureRead::WaitDataEof(Arc::clone(&release_error), connection_error),
                    ],
                    ByteEventCapability::TransportEvents,
                    false,
                );
                let probe = Rc::new(ByteEventProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(ProtocolErrorByteEventHandler {
                    probe: Rc::clone(&probe),
                    release_error,
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let error = session
                    .run()
                    .await
                    .expect_err("connection WINDOW_UPDATE overflow fails session");
                assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                let cancelled = probe.cancelled.borrow();
                assert_eq!(cancelled.len(), 1);
                assert_eq!(cancelled[0].0.event_type, ByteEventType::NicTx);
                assert_eq!(cancelled[0].1, HttpErrorCode::FLOW_CONTROL_ERROR);
            })
        });
    }

    fn assert_real_pending_byte_events_cancel_on_run_drop() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::with_capability(
                    [FixtureRead::Data(input)],
                    ByteEventCapability::TransportEvents,
                    false,
                );
                let probe = Rc::new(ByteEventProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(RegisteredResponseHandler {
                    probe: Rc::clone(&probe),
                    kind: RegisteredResponseKind::NativeOnly,
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if !state.borrow().registrations.is_empty() {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session completed before registration: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("pending byte event registered");
                drop(run);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !handle.is_finished() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("run cancellation completed");
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                let cancelled = probe.cancelled.borrow();
                assert_eq!(cancelled.len(), 1);
                assert_eq!(cancelled[0].0.event_type, ByteEventType::NicTx);
                assert_eq!(cancelled[0].1, HttpErrorCode::CORO_CANCELLED);
            })
        });
    }

    fn assert_real_stream_reset_cancels_only_its_byte_events(
        wire_code: H2ErrorCode,
        expected_code: HttpErrorCode,
    ) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (initial, reset) = peer_two_requests_then_reset_parts(wire_code);
                let release_reset = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::with_capability(
                    [
                        FixtureRead::Data(initial),
                        FixtureRead::WaitDataEof(Arc::clone(&release_reset), reset),
                    ],
                    ByteEventCapability::TransportEvents,
                    false,
                );
                let probe = Rc::new(ByteEventProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(RegisteredResponseHandler {
                    probe: Rc::clone(&probe),
                    kind: RegisteredResponseKind::NativeOnly,
                });
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while state.borrow().registrations.len() != 2 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session completed before both registrations: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("both stream registrations were installed");

                let boundary = state.borrow().writes.iter().map(Vec::len).sum::<usize>() as u64;
                let event_handle = handle.clone();
                tokio::task::spawn_local(async move {
                    release_reset.notify_one();
                    tokio::task::yield_now().await;
                    event_handle
                        .deliver_socket_byte_event(SocketByteEventType::Tx, boundary)
                        .expect("ready socket event");
                });
                tokio::time::timeout(Duration::from_secs(1), async {
                    while probe.cancelled.borrow().is_empty() || probe.delivered.borrow().is_empty()
                    {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session completed before reset cancellation: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("reset stream byte event was cancelled");
                {
                    let cancelled = probe.cancelled.borrow();
                    assert_eq!(cancelled.len(), 1);
                    assert_eq!(cancelled[0].0.stream_id, Some(StreamId::new(1)));
                    assert_eq!(cancelled[0].0.event_type, ByteEventType::NicTx);
                    assert_eq!(cancelled[0].1, expected_code);
                }
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("sibling completion timed out")
                    .expect("peer stream reset remains stream-local");
                let delivered = probe.delivered.borrow();
                assert_eq!(delivered.len(), 1);
                assert_eq!(delivered[0].stream_id, Some(StreamId::new(3)));
                assert_eq!(delivered[0].event_type, ByteEventType::NicTx);
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_byte_event_observer_outlives_transport() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::with_capability(
                    [FixtureRead::DataEof(input)],
                    ByteEventCapability::TransportEvents,
                    false,
                );
                state.borrow_mut().destroy_on_shutdown = true;
                let probe = Rc::new(LifetimeByteEventProbe {
                    transport: Rc::clone(&state),
                    transport_dropped_first: Cell::new(None),
                });
                let handler: Rc<dyn HttpHandler> = Rc::new(LifetimeByteEventHandler {
                    probe: Rc::clone(&probe),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                session
                    .set_byte_event_timeout(Duration::from_secs(60))
                    .expect("byte-event timeout");
                tokio::time::timeout(Duration::from_secs(2), session.run())
                    .await
                    .expect("session cleanup timed out")
                    .expect("session cleanup completes");
                assert_eq!(probe.transport_dropped_first.get(), Some(true));
                assert!(state.borrow().transport_dropped);
            })
        });
    }

    fn assert_real_downstream_push(
        peer_supports_push: bool,
        configured_limit: u32,
        pushed_error: Option<HttpErrorCode>,
    ) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = peer_request_bytes_with_push_and_limit(
                    HttpMethod::Get,
                    &[],
                    None,
                    PeerControl::None,
                    peer_supports_push,
                    (configured_limit == 0).then_some(0),
                );
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(PushResponseHandler {
                    pushed_error,
                    invocations: Rc::clone(&invocations),
                });
                let config = Http2SessionConfig {
                    max_concurrent_outgoing_streams: configured_limit.max(1),
                    ..Http2SessionConfig::default()
                };
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    config,
                )
                .expect("downstream H2 session");
                session.run().await.expect("push session completes");
                assert_eq!(invocations.get(), 1);
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                if peer_supports_push && configured_limit != 0 {
                    assert_eq!(decoded.pushes, [(StreamId::new(2), StreamId::new(1))]);
                    if pushed_error.is_some() {
                        assert_eq!(decoded.resets, [(StreamId::new(2), H2ErrorCode::CANCEL)]);
                    } else {
                        assert!(decoded.resets.is_empty());
                        assert!(decoded.header_streams.contains(&StreamId::new(2)));
                    }
                } else {
                    assert!(decoded.pushes.is_empty());
                    assert!(!decoded.header_streams.contains(&StreamId::new(2)));
                }
            })
        });
    }

    fn peer_push_reset_and_goaway_bytes() -> Vec<u8> {
        vec![
            0, 0, 4, 3, 0, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0,
            0,
        ]
    }

    fn assert_real_downstream_pushed_stream_peer_reset() {
        run_on_worker(|context| {
            Box::pin(async move {
                let release_reset = Arc::new(tokio::sync::Notify::new());
                let request = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(request),
                    FixtureRead::WaitDataEof(
                        Arc::clone(&release_reset),
                        peer_push_reset_and_goaway_bytes(),
                    ),
                ]);
                let stops = Rc::new(RefCell::new(Vec::new()));
                let handler: Rc<dyn HttpHandler> = Rc::new(PendingPushResponseHandler {
                    stops: Rc::clone(&stops),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        goaway_timeout: Duration::from_millis(1),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                let promise_written = tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let decoded =
                            decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                        if decoded.pushes == [(StreamId::new(2), StreamId::new(1))] {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before pushed response headers: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .is_ok();
                release_reset.notify_one();
                let run_result = match tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        let state = state.borrow();
                        let wire = state.writes.concat();
                        let decoded = decode_server_frames_with_requests(&wire, 1);
                        panic!(
                            "pushed reset cleanup timed out: {error:?}; unread={}; promise={promise_written}; wire={}; headers={:?}; pushes={:?}",
                            state.reads.len(),
                            wire.len(),
                            decoded.header_streams,
                            decoded.pushes,
                        )
                    }
                };
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert!(
                    promise_written,
                    "push promise missing: writes={}, headers={:?}, pushes={:?}, run={run_result:?}",
                    state.borrow().writes.len(),
                    decoded.header_streams,
                    decoded.pushes,
                );
                run_result.expect("pushed reset remains stream-local");
                assert_eq!(&*stops.borrow(), &[Some(HttpErrorCode::CANCEL)]);
                assert!(decoded.resets.is_empty());
                assert_eq!(
                    decoded.header_statuses,
                    [(StreamId::new(1), 200), (StreamId::new(2), 200)]
                );
            })
        });
    }

    fn assert_real_ingress_flow_control_error(connection_error: bool) {
        let body = vec![b'x'; if connection_error { 65_536 } else { 128 }];
        let input = peer_request_bytes(
            HttpMethod::Post,
            &body,
            (!connection_error).then_some(7),
            PeerControl::None,
        );
        let outcome = run_downstream_bytes_with_config(
            input,
            200,
            0,
            UniplexConfig::default().write_timeout,
            if connection_error { 131_072 } else { 64 },
            65_535,
        );
        if connection_error {
            assert_eq!(outcome.error, Some(HttpErrorCode::FLOW_CONTROL_ERROR));
            assert_eq!(outcome.close, FixtureClose::Reset);
        } else {
            assert_eq!(outcome.error, None);
            assert_eq!(
                decode_server_frames(&outcome.wire).resets,
                [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
            );
        }
    }

    fn assert_real_downstream_stream_window_recovery() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (initial, updates) = peer_two_requests_with_small_stream_windows();
                let release_updates = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitDataEof(Arc::clone(&release_updates), updates),
                ]);
                let invocations = Rc::new(Cell::new(0));
                let stats = Rc::new(StatsProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 100,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let decoded = decode_server_frames_with_requests(
                            &state.borrow().writes.concat(),
                            2,
                        );
                        if decoded.header_streams.contains(&StreamId::new(1))
                            && decoded.header_streams.contains(&StreamId::new(3))
                            && decoded.body_bytes == 20
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => panic!("session ended before both streams stalled: {result:?}"),
                            _ = tokio::task::yield_now() => {},
                        }
                    }
                })
                .await
                .expect("both responses did not reach their stream windows");
                assert_eq!(stats.transaction_stalled.get(), 2);
                release_updates.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("window recovery timed out")
                    .expect("window recovery completes cleanly");
                assert_eq!(invocations.get(), 2);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                assert_eq!(decoded.body_bytes, 200);
                assert!(decoded.resets.is_empty());
            })
        });
    }

    fn assert_real_upstream_response_flow_control_error(connection_error: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let release_headers = Arc::new(tokio::sync::Notify::new());
                let release_body = Arc::new(tokio::sync::Notify::new());
                let (headers, body) = server_response_flow_parts(70_000);
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::WaitData(Arc::clone(&release_headers), headers),
                    FixtureRead::WaitDataEof(Arc::clone(&release_body), body),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        connection_receive_window: if connection_error { 65_535 } else { 70_000 },
                        stream_receive_window: if connection_error { 70_000 } else { 65_535 },
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                release_headers.notify_one();
                let consume = async move {
                    assert!(
                        !response
                            .read_header_event()
                            .await
                            .expect("response headers")
                            .eom()
                    );
                    release_body.notify_one();
                    loop {
                        match response.read_body_event(u32::MAX).await {
                            Ok(event) if event.eom() => panic!("oversized response completed"),
                            Ok(_) => {}
                            Err(error) => break error,
                        }
                    }
                };
                let (run_result, source_error) = tokio::join!(session.run(), consume);
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                if connection_error {
                    let run_error = run_result
                        .expect_err("connection overflow fails session")
                        .code();
                    assert_eq!(
                        (run_error, source_error.code()),
                        (
                            HttpErrorCode::FLOW_CONTROL_ERROR,
                            HttpErrorCode::FLOW_CONTROL_ERROR,
                        ),
                        "{source_error:?}"
                    );
                    assert_eq!(state.borrow().close, FixtureClose::Reset);
                } else {
                    run_result.expect("stream overflow remains stream-local");
                    assert_eq!(
                        source_error.code(),
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        "{source_error:?}"
                    );
                    assert_eq!(
                        decoded.resets,
                        [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
                    );
                }
            })
        });
    }

    fn assert_real_upstream_window_update_overflow(connection_error: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let release = Arc::new(tokio::sync::Notify::new());
                let input = server_window_update_bytes(if connection_error {
                    StreamId::UNSPECIFIED
                } else {
                    StreamId::new(1)
                });
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::WaitDataEof(Arc::clone(&release), input)]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut response = if connection_error {
                    None
                } else {
                    let request =
                        HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                    Some(
                        handle
                            .send_request(
                                HttpSourceHolder::new(Box::new(request)),
                                CancellationSource::new().token(),
                            )
                            .await
                            .expect("request admitted")
                            .source,
                    )
                };
                release.notify_one();
                let read_error = async {
                    match response.as_mut() {
                        Some(response) => match response.read_header_event().await {
                            Ok(_) => panic!("overflowing WINDOW_UPDATE produced response"),
                            Err(error) => Some(error.code()),
                        },
                        None => None,
                    }
                };
                let (run_result, response_error) = tokio::join!(session.run(), read_error);
                assert_eq!(
                    response_error,
                    (!connection_error).then_some(HttpErrorCode::FLOW_CONTROL_ERROR)
                );
                assert_eq!(
                    run_result
                        .expect_err("WINDOW_UPDATE overflow fails session")
                        .code(),
                    HttpErrorCode::FLOW_CONTROL_ERROR
                );
                assert_eq!(state.borrow().close, FixtureClose::Reset);
            })
        });
    }

    #[test]
    fn real_detached_stream_window_update_preserves_connection() {
        struct DetachGates {
            closed: Cell<usize>,
            release_credit: Arc<tokio::sync::Notify>,
            release_eof: Arc<tokio::sync::Notify>,
        }

        impl Http2SessionStats for DetachGates {
            fn record_transaction_closed(&self) {
                let closed = self.closed.get() + 1;
                self.closed.set(closed);
                match closed {
                    1 => self.release_credit.notify_one(),
                    2 => self.release_eof.notify_one(),
                    _ => {}
                }
            }
        }

        run_on_worker(|context| {
            Box::pin(async move {
                let mut peer = Http2Codec::new(TransportDirection::Upstream);
                let mut callback = IgnoreCodecEvents;
                let [initial, delayed] = [(1, b"/one".as_slice()), (3, b"/two".as_slice())].map(
                    |(expected_stream, path)| {
                        let mut output = BufQueue::new(1 << 20);
                        if expected_stream == 1 {
                            peer.generate(
                                &mut output,
                                EgressFrame::ConnectionPreface,
                                &mut callback,
                            )
                            .expect("client preface");
                            peer.generate_configured_settings(&mut output, &mut callback)
                                .expect("client settings");
                        } else {
                            peer.generate(
                                &mut output,
                                EgressFrame::WindowUpdate {
                                    stream: StreamId::new(1),
                                    amount: 1,
                                },
                                &mut callback,
                            )
                            .expect("late positive credit for stream 1");
                        }
                        let stream = peer.create_stream();
                        assert_eq!(stream, StreamId::new(expected_stream));
                        let mut request = roxygen_core::message::HttpMessage::new();
                        request.set_method(HttpMethod::Get).expect("request method");
                        request.set_url(path, false).expect("request URL");
                        request.set_http_version(1, 1);
                        request
                            .headers_mut()
                            .set(HttpHeaderCode::Host, b"example.com");
                        peer.generate(
                            &mut output,
                            EgressFrame::Headers {
                                stream,
                                message: &request,
                                end_stream: true,
                                extra_headers: None,
                            },
                            &mut callback,
                        )
                        .expect("request headers");
                        output
                            .coalesce(output.chain_length())
                            .expect("bounded peer input")
                            .to_vec()
                    },
                );
                let gates = Rc::new(DetachGates {
                    closed: Cell::new(0),
                    release_credit: Arc::new(tokio::sync::Notify::new()),
                    release_eof: Arc::new(tokio::sync::Notify::new()),
                });
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(Arc::clone(&gates.release_credit), delayed),
                    FixtureRead::WaitDataEof(Arc::clone(&gates.release_eof), Vec::new()),
                ]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 1,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(Rc::clone(&gates) as Rc<dyn Http2SessionStats>),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                session
                    .run()
                    .await
                    .expect("late detached-stream credit preserves successful joined close");
                assert_eq!(invocations.get(), 2);
                assert_eq!(gates.closed.get(), 2);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                assert_eq!(
                    decoded.header_statuses,
                    [(StreamId::new(1), 200), (StreamId::new(3), 200)]
                );
                assert_eq!(decoded.body_data, b"rr");
                assert!(decoded.resets.is_empty());
                assert!(
                    decoded
                        .goaways
                        .iter()
                        .all(|(_, code)| *code == H2ErrorCode::NO_ERROR)
                );
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_real_egress_flow_timeout(connection_blocked: bool) {
        let input = peer_request_bytes(
            HttpMethod::Get,
            &[],
            None,
            PeerControl::WindowUpdate {
                connection: !connection_blocked,
                amount: 65_535,
            },
        );
        let outcome = run_downstream_bytes_with_config(
            input,
            200,
            96 << 10,
            Duration::from_millis(1),
            65_535,
            65_535,
        );
        let decoded = decode_server_frames(&outcome.wire);
        if connection_blocked {
            assert_eq!(outcome.error, Some(HttpErrorCode::FLOW_CONTROL_ERROR));
            assert_eq!(outcome.close, FixtureClose::Reset);
        } else {
            assert_eq!(outcome.error, None);
            assert_eq!(
                decoded.resets,
                [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
            );
        }
    }

    fn assert_real_reset_releases_connection_flow_control() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (first_bytes, reset_and_second) =
                    server_reset_release_connection_credit_parts();
                let release_reset = Arc::new(tokio::sync::Notify::new());
                let (transport, _) = FixtureTransport::new([
                    FixtureRead::Data(first_bytes),
                    FixtureRead::WaitDataEof(Arc::clone(&release_reset), reset_and_second),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let first =
                    HttpFixedSource::make_get_request(b"/first", None).expect("first request");
                let second =
                    HttpFixedSource::make_get_request(b"/second", None).expect("second request");
                let mut first_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(first)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("first request admitted")
                    .source;
                let mut second_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(second)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("second request admitted")
                    .source;
                let consume = async move {
                    assert!(
                        !first_response
                            .read_header_event()
                            .await
                            .expect("first response headers")
                            .eom()
                    );
                    let mut first_body = 0;
                    let first_error = loop {
                        match first_response.read_body_event(u32::MAX).await {
                            Ok(event) => {
                                first_body += event.as_body().map_or(0, BufQueue::chain_length);
                                if first_body == 65_535 {
                                    release_reset.notify_one();
                                }
                            }
                            Err(error) => break error,
                        }
                    };
                    assert_eq!(first_body, 65_535);
                    assert_eq!(first_error.code(), HttpErrorCode::CANCEL);

                    assert!(
                        !second_response
                            .read_header_event()
                            .await
                            .expect("second response headers")
                            .eom()
                    );
                    let second_body = second_response
                        .read_body_event(u32::MAX)
                        .await
                        .expect("second response body");
                    assert!(second_body.eom());
                    assert_eq!(second_body.as_body().map_or(0, BufQueue::chain_length), 100);
                };
                let (run, ()) = tokio::join!(session.run(), consume);
                run.expect("reset credit lets the unrelated response complete");
            })
        });
    }

    fn assert_real_late_body_after_reset_releases_connection_credit() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (headers, body, final_body) = server_response_then_late_body_parts();
                let release_body = Arc::new(tokio::sync::Notify::new());
                let release_final_body = Arc::new(tokio::sync::Notify::new());
                let finish = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(headers),
                    FixtureRead::WaitData(Arc::clone(&release_body), body),
                    FixtureRead::WaitData(Arc::clone(&release_final_body), final_body),
                    FixtureRead::WaitDataEof(Arc::clone(&finish), Vec::new()),
                ]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut run = Box::pin(session.run());
                let mut header_read = Box::pin(response.read_header_event());
                let headers = tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::select! {
                        headers = header_read.as_mut() => headers.expect("response headers"),
                        result = run.as_mut() => {
                            panic!("session ended before response headers: {result:?}")
                        }
                    }
                })
                .await
                .expect("response headers timed out");
                drop(header_read);
                assert!(!headers.eom());
                response.stop_reading(None);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !decode_client_frames(&state.borrow().writes.concat())
                        .resets
                        .contains(&(StreamId::new(1), H2ErrorCode::CANCEL))
                    {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before local reset: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("local reset was written");
                release_body.notify_one();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while state.borrow().reads.len() != 2 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before subthreshold late body: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("subthreshold late body was not consumed");
                assert!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .window_updates
                        .iter()
                        .all(|(stream, _)| !stream.is_unspecified())
                );
                release_final_body.notify_one();
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let returned = decode_client_frames(&state.borrow().writes.concat())
                            .window_updates
                            .into_iter()
                            .filter_map(|(stream, amount)| {
                                stream.is_unspecified().then_some(amount)
                            })
                            .sum::<u32>();
                        if returned == 65_535 / 2 + 1 {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before credit return: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("late body connection credit was returned");
                finish.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("late body cleanup timed out")
                    .expect("late reset-stream body is discarded");
                let returned = decode_client_frames(&state.borrow().writes.concat())
                    .window_updates
                    .into_iter()
                    .filter_map(|(stream, amount)| stream.is_unspecified().then_some(amount))
                    .sum::<u32>();
                assert_eq!(returned, 65_535 / 2 + 1);
            })
        });
    }

    fn assert_real_upstream_egress_queues_while_transport_write_is_blocked() {
        run_on_worker(|context| {
            Box::pin(async move {
                let release_responses = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([FixtureRead::WaitDataEof(
                    Arc::clone(&release_responses),
                    server_two_empty_responses_bytes(),
                )]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while state.borrow().writes.len() != 1 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before preface write: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("preface/settings write completed");

                let release_write = Arc::new(tokio::sync::Notify::new());
                state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                let first =
                    HttpFixedSource::make_get_request(b"/first", None).expect("first request");
                let mut first_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(first)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("first request admitted")
                    .source;
                tokio::select! {
                    result = run.as_mut() => panic!("session ended while first write blocked: {result:?}"),
                    _ = tokio::task::yield_now() => {}
                }
                let second =
                    HttpFixedSource::make_get_request(b"/second", None).expect("second request");
                let mut second_response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(second)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("second request admitted")
                    .source;
                tokio::select! {
                    result = run.as_mut() => panic!("session ended while writes blocked: {result:?}"),
                    _ = tokio::task::yield_now() => {}
                }
                assert_eq!(state.borrow().writes.len(), 1);

                release_write.notify_one();
                release_responses.notify_one();
                let consume = async move {
                    assert!(
                        first_response
                            .read_header_event()
                            .await
                            .expect("first response")
                            .eom()
                    );
                    assert!(
                        second_response
                            .read_header_event()
                            .await
                            .expect("second response")
                            .eom()
                    );
                };
                let (result, ()) = tokio::join!(run.as_mut(), consume);
                result.expect("queued writes and responses complete");
                assert!(state.borrow().writes.len() >= 3);
                assert_eq!(
                    decode_client_frames(&state.borrow().writes.concat()).header_streams,
                    [StreamId::new(1), StreamId::new(3)]
                );
            })
        });
    }

    fn assert_real_stream_flow_timeout_resets_every_blocked_stream() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_two_requests_with_open_connection_window();
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 65_536,
                    response_error: None,
                    invocations: Rc::clone(&invocations),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig {
                        write_timeout: Duration::from_millis(10),
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                tokio::time::timeout(Duration::from_secs(1), session.run())
                    .await
                    .expect("two-stream flow timeout hung")
                    .expect("stream-window timeouts remain stream-local");
                assert_eq!(invocations.get(), 2);
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                let mut resets = decoded.resets;
                resets.sort_by_key(|(stream, _)| stream.get());
                assert_eq!(
                    resets,
                    [
                        (StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR),
                        (StreamId::new(3), H2ErrorCode::FLOW_CONTROL_ERROR),
                    ]
                );
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
            })
        });
    }

    fn assert_no_flow_timeout_without_buffered_body() {
        run_on_worker(|context| {
            Box::pin(async move {
                let request = peer_two_requests_bytes();
                let update = peer_connection_window_update(65_535);
                let release_update = Arc::new(tokio::sync::Notify::new());
                let release_body = Arc::new(tokio::sync::Notify::new());
                let release_eof = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(request),
                    FixtureRead::WaitData(Arc::clone(&release_update), update),
                    FixtureRead::WaitDataEof(Arc::clone(&release_eof), Vec::new()),
                ]);
                let handler: Rc<dyn HttpHandler> = Rc::new(ExhaustConnectionThenDelayHandler {
                    invocation: Rc::new(Cell::new(0)),
                    release_body: Arc::clone(&release_body),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig {
                        write_timeout: Duration::from_millis(5),
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        let decoded =
                            decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                        if decoded.body_bytes == 65_535 && decoded.header_statuses.len() == 2 {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before first response byte: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("first response byte was not written");
                tokio::select! {
                    result = run.as_mut() => {
                        panic!("unproduced response body triggered a flow timeout: {result:?}")
                    }
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                }
                assert_eq!(
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2).resets,
                    []
                );
                release_update.notify_one();
                tokio::task::yield_now().await;
                release_body.notify_one();
                tokio::task::yield_now().await;
                release_eof.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("released response did not finish")
                    .expect("no-body flow wait completes normally");
            })
        });
    }

    fn assert_buffered_write_stats_decrease_per_serialized_chunk() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (request, update) = peer_request_with_stream_window(10, 90, false);
                let release_update = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(request),
                    FixtureRead::WaitDataEof(Arc::clone(&release_update), update),
                ]);
                let stats = Rc::new(StatsProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(BufferedResponseHandler {
                    invocations: Rc::new(Cell::new(0)),
                    body_size: 100,
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig {
                        write_timeout: Duration::from_secs(1),
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if decode_server_frames_with_requests(&state.borrow().writes.concat(), 1)
                            .body_bytes
                            == 10
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before first body chunk: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("first body chunk was not serialized");
                let deltas = stats.buffered_write_deltas.borrow().clone();
                assert!(stats.buffered_write.get() > 0, "deltas={deltas:?}");
                assert!(deltas.contains(&-10), "deltas={deltas:?}");
                release_update.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("window-released response did not finish")
                    .expect("window-released response completes");
                assert_eq!(stats.buffered_write.get(), 0);
            })
        });
    }

    #[test]
    fn absent_session_stats_do_not_retain_egress_diagnostics() {
        run_on_worker(|context| {
            Box::pin(async move {
                // The final source event needs one byte of credit before its tail stalls.
                let (request, update) = peer_request_with_stream_window(513, 99, false);
                let release_update = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(request),
                    FixtureRead::WaitDataEof(Arc::clone(&release_update), update),
                ]);
                let mut message = HttpMessage::new();
                message.set_status_code(200).expect("response status");
                message.set_http_version(1, 1);
                message
                    .headers_mut()
                    .set(HttpHeaderCode::ContentLength, b"611");
                let response = HttpSourceHolder::new(Box::new(EventSequenceSource {
                    header: Some(HttpHeaderEvent::new(message, false).expect("response headers")),
                    body: (0..512)
                        .map(|_| HttpBodyEvent::body(queue(b"x"), false))
                        .chain(std::iter::once(HttpBodyEvent::body(
                            queue(&[b'x'; 100]),
                            true,
                        )))
                        .collect(),
                }));
                let handler: Rc<dyn HttpHandler> =
                    Rc::new(EligibilitySequenceHandler(RefCell::new(VecDeque::from([
                        response,
                    ]))));
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if decode_server_frames_with_requests(&state.borrow().writes.concat(), 1)
                            .body_bytes
                            == 513
                            && handle
                                .buffered_egress_stats_for_test(StreamId::new(1))
                                .is_ok_and(|snapshot| snapshot.buffered_bytes == 99)
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before exhausting stream credit: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("response did not reach its buffered tail");
                assert_eq!(
                    handle
                        .buffered_egress_stats_for_test(StreamId::new(1))
                        .expect("response stream remains open"),
                    BufferedEgressStatsSnapshot {
                        buffered_bytes: 99,
                        delta_capacity: None,
                        tracked_streams: 0,
                        content_length_mismatch: Some((611, 612)),
                    }
                );
                release_update.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("window-released response did not finish")
                    .expect("egress length mismatch remains diagnostic");
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert_eq!(decoded.body_bytes, 612);
                assert!(decoded.resets.is_empty());
            })
        });
    }

    struct WindowNotification {
        opened: Cell<usize>,
        closed: Cell<usize>,
    }

    #[derive(Default)]
    struct StatsProbe {
        opened: Cell<usize>,
        closed: Cell<usize>,
        reused: Cell<usize>,
        served: RefCell<Vec<u64>>,
        buffered_read: Cell<i64>,
        buffered_write: Cell<i64>,
        buffered_read_deltas: RefCell<Vec<i64>>,
        buffered_write_deltas: RefCell<Vec<i64>>,
        content_length_mismatches: Cell<usize>,
        ingress_get_bodies: Cell<usize>,
        transaction_stalled: Cell<usize>,
        rate_samples: RefCell<Vec<crate::http2::rate_limit::RateLimitSample>>,
    }

    impl Http2SessionStats for StatsProbe {
        fn record_transaction_opened(&self) {
            self.opened.set(self.opened.get() + 1);
        }

        fn record_transaction_closed(&self) {
            self.closed.set(self.closed.get() + 1);
        }

        fn record_session_reused(&self) {
            self.reused.set(self.reused.get() + 1);
        }

        fn record_transactions_served(&self, transactions: u64) {
            self.served.borrow_mut().push(transactions);
        }

        fn record_buffered_read_bytes(&self, delta: i64) {
            self.buffered_read_deltas.borrow_mut().push(delta);
            self.buffered_read
                .set(self.buffered_read.get().saturating_add(delta));
        }

        fn record_buffered_write_bytes(&self, delta: i64) {
            self.buffered_write_deltas.borrow_mut().push(delta);
            self.buffered_write
                .set(self.buffered_write.get().saturating_add(delta));
        }

        fn record_content_length_mismatch(&self) {
            self.content_length_mismatches
                .set(self.content_length_mismatches.get().saturating_add(1));
        }

        fn record_ingress_get_body(&self) {
            self.ingress_get_bodies
                .set(self.ingress_get_bodies.get().saturating_add(1));
        }

        fn record_transaction_stalled(&self) {
            self.transaction_stalled
                .set(self.transaction_stalled.get().saturating_add(1));
        }

        fn record_rate_limit(&self, sample: crate::http2::rate_limit::RateLimitSample) {
            self.rate_samples.borrow_mut().push(sample);
        }
    }

    #[test]
    fn real_session_stats_balance_buffers_and_finalize_served_count() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let invocations = Rc::new(Cell::new(0));
                let stats = Rc::new(StatsProbe::default());
                let handler = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 3,
                    response_error: None,
                    invocations,
                    request_body: Rc::new(Cell::new(0)),
                });
                let config = Http2SessionConfig {
                    stats: Some(stats.clone()),
                    ..Http2SessionConfig::default()
                };
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    config,
                )
                .expect("downstream H2 session");
                session.run().await.expect("session completes");
                assert_eq!(stats.opened.get(), 1);
                assert_eq!(stats.closed.get(), 1);
                assert_eq!(stats.reused.get(), 0);
                assert_eq!(&*stats.served.borrow(), &[1]);
                assert_eq!(stats.buffered_read.get(), 0);
                assert_eq!(stats.buffered_write.get(), 0);
                assert!(
                    stats
                        .buffered_write_deltas
                        .borrow()
                        .iter()
                        .any(|delta| *delta > 0)
                );
                assert!(
                    stats
                        .buffered_write_deltas
                        .borrow()
                        .iter()
                        .any(|delta| *delta < 0)
                );
            })
        });
    }

    #[test]
    fn real_session_stats_track_application_read_and_error_semantics() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, b"abc", None, PeerControl::None);
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let stats = Rc::new(StatsProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                session.run().await.expect("GET body session completes");
                assert_eq!(stats.ingress_get_bodies.get(), 1);
                assert_eq!(stats.buffered_read.get(), 0);
                assert_eq!(&*stats.buffered_read_deltas.borrow(), &[3, -3]);

                let input = peer_content_length_mismatch_request();
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let stats = Rc::new(StatsProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(ErrorScenarioHandler {
                    case: ErrorHandlerCase::ReadBodyThenRespond {
                        status: 200,
                        body: b"ignored",
                    },
                    probe: Rc::new(ErrorHandlerProbe::default()),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 mismatch session");
                session
                    .run()
                    .await
                    .expect("content mismatch remains stream-local");
                assert_eq!(stats.content_length_mismatches.get(), 1);
            })
        });
    }

    #[test]
    fn discarded_padded_data_returns_each_connection_credit_byte_once() {
        run_on_worker(|context| {
            Box::pin(async move {
                let body = [b'x'; 40_000];
                let (initial, padded) = peer_padded_post_parts(&body, 7);
                let first_frame_length = FRAME_HEADER_SIZE + 16_000;
                let first = padded[..first_frame_length].to_vec();
                let remaining = padded[first_frame_length..].to_vec();
                let release_first = Arc::new(tokio::sync::Notify::new());
                let release_remaining = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(Arc::clone(&release_first), first),
                    FixtureRead::WaitDataEof(Arc::clone(&release_remaining), remaining),
                ]);
                let probe = Rc::new(ErrorHandlerProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(ErrorScenarioHandler {
                    case: ErrorHandlerCase::StopWithoutResponse,
                    probe,
                });
                let stats = Rc::new(StatsProbe::default());
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if decode_server_frames(&state.borrow().writes.concat())
                            .header_statuses
                            .contains(&(StreamId::new(1), 500))
                        {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => panic!("session ended before discard: {result:?}"),
                            _ = tokio::task::yield_now() => {},
                        }
                    }
                })
                .await
                .expect("handler discarded the request");
                release_first.notify_one();
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if stats.buffered_read_deltas.borrow().len() == 2 {
                            break;
                        }
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before subthreshold discard: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("subthreshold discarded DATA was not observed");
                assert_eq!(
                    stats.buffered_read_deltas.borrow().as_slice(),
                    [16_000, -16_000]
                );
                assert!(
                    decode_server_frames(&state.borrow().writes.concat())
                        .window_updates
                        .iter()
                        .all(|(stream, _)| !stream.is_unspecified())
                );
                release_remaining.notify_one();
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("padded discard cleanup timed out")
                    .expect("padded discard remains stream-local");
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                let returned = decoded
                    .window_updates
                    .iter()
                    .filter_map(|(stream, amount)| stream.is_unspecified().then_some(*amount))
                    .sum::<u32>();
                assert_eq!(returned, 40_008);
                assert_eq!(
                    stats.buffered_read_deltas.borrow().as_slice(),
                    [16_000, -16_000, 16_000, -16_000, 8_000, -8_000]
                );
            })
        });
    }

    /// Reads each request body `read_size` bytes at a time, counting the bytes in `consumed`,
    /// then answers 200 without a body.
    struct ChunkedBodyReader {
        read_size: u32,
        consumed: Rc<Cell<usize>>,
    }

    impl HttpHandler for ChunkedBodyReader {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let read_size = self.read_size;
            let consumed = Rc::clone(&self.consumed);
            Box::pin(async move {
                if !request.read_header_event().await?.eom() {
                    loop {
                        let event = request.read_body_event(read_size).await?;
                        let eom = event.eom();
                        match event.into_kind() {
                            roxygen_core::events::HttpBodyEventKind::Body(body) => {
                                consumed.set(consumed.get() + body.chain_length());
                            }
                            roxygen_core::events::HttpBodyEventKind::Suspend(resume) => {
                                let _ = resume.await;
                                continue;
                            }
                            _ => {}
                        }
                        if eom {
                            break;
                        }
                    }
                }
                let response =
                    HttpFixedSource::make_fixed_response(200, None).expect("fixture response");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    /// Each written WINDOW_UPDATE frame, header and payload, in wire order.
    fn window_update_frames(wire: &[u8]) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        let mut rest = wire;
        while !rest.is_empty() {
            let length = u32::from_be_bytes([0, rest[0], rest[1], rest[2]]) as usize;
            let (frame, tail) = rest.split_at(FRAME_HEADER_SIZE + length);
            if frame[3] == FrameType::WINDOW_UPDATE.as_u8() {
                frames.push(frame.to_vec());
            }
            rest = tail;
        }
        frames
    }

    /// The WINDOW_UPDATE frames written for a 60,000-byte upload read 8 KiB at a time, recorded
    /// at `62d2959bb6d3`, before HTTP/1.1 streams could declare a resume threshold (`DIV-044`).
    /// HTTP/2 streams declare none, so the frames stay byte for byte those of the parent. The
    /// first three 16,000-byte DATA frames are read in full before the last arrives, so stream
    /// credit is still owed while they are read.
    #[test]
    fn upload_window_updates_are_unchanged_by_http1_resume_thresholds() {
        let frames = run_on_worker(|context| {
            Box::pin(async move {
                let (initial, data) = peer_padded_post_parts(&[b'w'; 60_000], 0);
                let (first, last) = data.split_at(3 * (FRAME_HEADER_SIZE + 16_000));
                let release_last = Arc::new(tokio::sync::Notify::new());
                let release_eof = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data([initial.as_slice(), first].concat()),
                    FixtureRead::WaitData(Arc::clone(&release_last), last.to_vec()),
                    FixtureRead::WaitDataEof(Arc::clone(&release_eof), Vec::new()),
                ]);
                let consumed = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(ChunkedBodyReader {
                    read_size: 8 << 10,
                    consumed: Rc::clone(&consumed),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig {
                        maximum_read_size: 64 << 10,
                        read_allocation_size: 64 << 10,
                        ..UniplexConfig::default()
                    },
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while consumed.get() != 48_000 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before the first frames were read: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("the first three DATA frames were read");
                release_last.notify_one();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !decode_server_frames(&state.borrow().writes.concat())
                        .header_statuses
                        .contains(&(StreamId::new(1), 200))
                    {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before the response: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("the upload was read and answered");
                release_eof.notify_one();
                let _ = tokio::time::timeout(Duration::from_secs(5), run.as_mut())
                    .await
                    .expect("the session ends at EOF");
                window_update_frames(&state.borrow().writes.concat())
            })
        });
        assert_eq!(
            frames,
            [
                // Length 4, type 8, no flags, stream 1, increment 32,768.
                [0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 0x80, 0],
                // The same on stream 0, the connection.
                [0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 0, 0x80, 0],
            ],
            "stream and connection WINDOW_UPDATE frames, byte for byte and in wire order: each \
             half of the 65,535-byte window is returned after four 8 KiB reads"
        );
    }

    impl ConnectionFlowControlCallback for WindowNotification {
        fn on_connection_send_window_open(&self) {
            self.opened.set(self.opened.get() + 1);
        }

        fn on_connection_send_window_closed(&self) {
            self.closed.set(self.closed.get() + 1);
        }
    }

    #[test]
    fn configured_preface_settings_and_window_update_are_in_wire_order() {
        let mut output = BufQueue::new(1 << 20);
        let mut callback = IgnoreCodecEvents;
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let preface = codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("client preface");
        assert_eq!(preface.bytes_written, 24);
        let settings = codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("initial settings");
        assert!(settings.bytes_written >= 9);
        let notification = Rc::new(WindowNotification {
            opened: Cell::new(0),
            closed: Cell::new(0),
        });
        let controlled = FlowControlledCodec::new(
            Box::new(codec),
            notification.clone(),
            &mut output,
            &mut callback,
            131_070,
        )
        .expect("initial connection window");
        assert_eq!(controlled.receive_window().capacity(), 131_070);
        let bytes = output
            .coalesce(output.chain_length())
            .expect("bounded initial output");
        assert!(bytes.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
        assert_eq!(bytes[27], 4);
        let window_header = 24 + settings.bytes_written;
        assert_eq!(bytes[window_header + 3], 8);
        assert_eq!(notification.opened.get(), 0);
        assert_eq!(notification.closed.get(), 0);
    }

    #[test]
    fn priority_scheduler_orders_nonincremental_before_incremental() {
        let mut scheduler = Http2PriorityScheduler::new();
        let incremental = StreamId::new(1);
        let ordered = StreamId::new(3);
        assert!(scheduler.insert(
            incremental,
            HttpPriority::from_urgency_and_incremental(2, true),
        ));
        assert!(scheduler.insert(
            ordered,
            HttpPriority::from_urgency_and_incremental(2, false),
        ));
        assert!(scheduler.enqueue(incremental));
        assert!(scheduler.enqueue(ordered));
        assert_eq!(scheduler.next(), Some(ordered));
        assert!(scheduler.dequeue(ordered));
        assert_eq!(scheduler.next(), Some(incremental));
    }

    #[test]
    fn priority_scheduler_rotates_only_after_incremental_bytes() {
        let mut scheduler = Http2PriorityScheduler::new();
        let first = StreamId::new(1);
        let second = StreamId::new(3);
        for stream in [first, second] {
            assert!(scheduler.insert(stream, HttpPriority::from_urgency_and_incremental(3, true),));
            assert!(scheduler.enqueue(stream));
        }
        assert_eq!(scheduler.next(), Some(first));
        assert!(scheduler.consume(first, 0));
        assert_eq!(scheduler.next(), Some(first));
        assert!(scheduler.consume(first, 1));
        assert_eq!(scheduler.next(), Some(second));
    }

    #[test]
    fn priority_scheduler_orders_shared_urgency_by_order_id() {
        let mut scheduler = Http2PriorityScheduler::new();
        let late = StreamId::new(1);
        let early = StreamId::new(3);
        let middle = StreamId::new(5);
        for (stream, order_id) in [(late, 9), (early, 1), (middle, 5)] {
            assert!(scheduler.insert(stream, HttpPriority::new(1, false, order_id, false, 0)));
        }

        assert!(scheduler.enqueue(late));
        assert_eq!(
            scheduler.next(),
            Some(late),
            "a lone stream heads its urgency"
        );
        assert!(scheduler.enqueue(early));
        assert!(scheduler.enqueue(middle));
        assert_eq!(scheduler.next(), Some(early), "lowest order id goes first");
        assert_eq!(
            scheduler.next_matching(|stream| stream != early),
            Some(middle),
            "ineligible streams are skipped in order-id order"
        );

        assert!(scheduler.dequeue(early));
        assert!(scheduler.dequeue(middle));
        assert_eq!(scheduler.next(), Some(late));
        assert_eq!(scheduler.next_matching(|stream| stream != late), None);
        assert!(scheduler.dequeue(late));
        assert_eq!(scheduler.next(), None, "a drained urgency selects nothing");

        assert!(scheduler.enqueue(middle));
        assert_eq!(
            scheduler.next(),
            Some(middle),
            "a drained urgency accepts new work"
        );
        assert!(scheduler.remove(middle));
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn priority_scheduler_dequeues_incremental_streams_anywhere() {
        let mut scheduler = Http2PriorityScheduler::new();
        let first = StreamId::new(1);
        let second = StreamId::new(3);
        let third = StreamId::new(5);
        for stream in [first, second, third] {
            assert!(scheduler.insert(stream, HttpPriority::from_urgency_and_incremental(3, true)));
            assert!(scheduler.enqueue(stream));
        }

        assert!(scheduler.dequeue(second));
        assert_eq!(
            scheduler.next(),
            Some(first),
            "removing a later stream keeps the head"
        );
        assert_eq!(
            scheduler.next_matching(|stream| stream != first),
            Some(third)
        );
        assert!(scheduler.dequeue(first));
        assert_eq!(
            scheduler.next(),
            Some(third),
            "removing the head promotes the next"
        );
        assert!(
            !scheduler.dequeue(first),
            "a dequeued stream is not queued twice"
        );
        assert!(scheduler.dequeue(third));
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn priority_scheduler_preserves_occupancy_and_callback_order() {
        let mut scheduler = Http2PriorityScheduler::new();
        let urgent = StreamId::new(1);
        let ordered = StreamId::new(3);
        let earlier = StreamId::new(5);
        let incremental = StreamId::new(7);
        let tail = StreamId::new(9);
        assert!(scheduler.is_empty());
        for (stream, priority) in [
            (tail, HttpPriority::new(7, true, 0, false, 0)),
            (incremental, HttpPriority::new(6, true, 0, false, 0)),
            (ordered, HttpPriority::new(6, false, 20, false, 0)),
            (earlier, HttpPriority::new(6, false, 10, false, 0)),
            (urgent, HttpPriority::new(1, false, 0, false, 0)),
        ] {
            assert!(scheduler.insert(stream, priority));
        }
        assert!(!scheduler.is_empty());
        assert_eq!(scheduler.next(), None);
        for stream in [tail, incremental, ordered, earlier, urgent] {
            assert!(scheduler.enqueue(stream));
        }

        let mut visited = Vec::new();
        assert_eq!(
            scheduler.next_matching(|stream| {
                visited.push(stream);
                false
            }),
            None
        );
        assert_eq!(visited, [urgent, earlier, ordered, incremental, tail]);
        assert_eq!(scheduler.next(), Some(urgent));
        visited.clear();
        assert_eq!(
            scheduler.next_matching(|stream| {
                visited.push(stream);
                stream == incremental
            }),
            Some(incremental)
        );
        assert_eq!(visited, [urgent, earlier, ordered, incremental]);

        assert!(scheduler.pause(urgent, true));
        assert!(!scheduler.enqueue(urgent));
        assert_eq!(scheduler.next(), Some(earlier));
        assert!(scheduler.pause(earlier, true));
        assert_eq!(scheduler.next(), Some(ordered));
        assert!(scheduler.update(earlier, HttpPriority::new(6, false, 30, true, 0)));
        assert!(!scheduler.enqueue(earlier));
        assert!(scheduler.pause(earlier, false));
        visited.clear();
        assert_eq!(
            scheduler.next_matching(|stream| {
                visited.push(stream);
                stream == incremental
            }),
            Some(incremental)
        );
        assert_eq!(visited, [ordered, earlier, incremental]);

        assert!(scheduler.dequeue(incremental));
        assert_eq!(scheduler.next(), Some(ordered));
        assert!(scheduler.enqueue(incremental));
        assert!(scheduler.remove(ordered));
        assert_eq!(scheduler.next(), Some(earlier));
        assert!(scheduler.remove(earlier));
        assert_eq!(scheduler.next(), Some(incremental));
        assert!(scheduler.remove(incremental));
        assert_eq!(scheduler.next(), Some(tail));
        assert!(scheduler.pause(urgent, false));
        assert_eq!(scheduler.next(), Some(urgent));
        assert!(scheduler.dequeue(urgent));
        assert_eq!(scheduler.next(), Some(tail));
        assert!(scheduler.remove(tail));
        assert_eq!(scheduler.next(), None);
        assert_eq!(
            scheduler.next_matching(|_| panic!("unqueued entries are not eligible candidates")),
            None
        );
        assert!(!scheduler.is_empty());
        assert!(scheduler.remove(urgent));
        assert!(scheduler.is_empty());
    }

    /// A reference model of `Http2PriorityScheduler`'s selection order, kept as a flat list and
    /// sorted at each query: lower urgency first; at one urgency, non-incremental streams by
    /// `(order_id, stream)`, then incremental streams in the order they were queued or last
    /// rotated. Paused streams keep their enqueue intent but are not selectable.
    #[derive(Default)]
    struct SchedulerModel {
        entries: Vec<SchedulerModelEntry>,
        next_sequence: u64,
    }

    struct SchedulerModelEntry {
        stream: StreamId,
        priority: HttpPriority,
        enqueued: bool,
        sequence: u64,
    }

    impl SchedulerModel {
        fn entry(&mut self, stream: StreamId) -> Option<&mut SchedulerModelEntry> {
            self.entries.iter_mut().find(|entry| entry.stream == stream)
        }

        fn sequence(&mut self) -> u64 {
            self.next_sequence += 1;
            self.next_sequence
        }

        fn insert(&mut self, stream: StreamId, priority: HttpPriority) -> bool {
            if self.entry(stream).is_some() {
                return false;
            }
            self.entries.push(SchedulerModelEntry {
                stream,
                priority,
                enqueued: false,
                sequence: 0,
            });
            true
        }

        fn update(&mut self, stream: StreamId, priority: HttpPriority) -> bool {
            let sequence = self.sequence();
            let Some(entry) = self.entry(stream) else {
                return false;
            };
            entry.priority = priority;
            entry.sequence = sequence;
            true
        }

        fn enqueue(&mut self, stream: StreamId) -> bool {
            let sequence = self.sequence();
            let Some(entry) = self.entry(stream) else {
                return false;
            };
            if entry.enqueued || entry.priority.paused {
                return false;
            }
            entry.enqueued = true;
            entry.sequence = sequence;
            true
        }

        fn consume(&mut self, stream: StreamId, bytes: usize) -> bool {
            let sequence = self.sequence();
            let Some(entry) = self.entry(stream) else {
                return false;
            };
            if entry.enqueued && bytes != 0 && entry.priority.incremental && !entry.priority.paused
            {
                entry.sequence = sequence;
            }
            entry.enqueued
        }

        fn dequeue(&mut self, stream: StreamId) -> bool {
            let Some(entry) = self.entry(stream) else {
                return false;
            };
            std::mem::replace(&mut entry.enqueued, false)
        }

        fn remove(&mut self, stream: StreamId) -> bool {
            let before = self.entries.len();
            self.entries.retain(|entry| entry.stream != stream);
            self.entries.len() != before
        }

        /// The selectable streams in selection order.
        fn order(&self) -> Vec<StreamId> {
            let mut queued: Vec<_> = self
                .entries
                .iter()
                .filter(|entry| entry.enqueued && !entry.priority.paused)
                .collect();
            queued.sort_by_key(|entry| {
                let within = if entry.priority.incremental {
                    (1, 0, 0, entry.sequence)
                } else {
                    (0, entry.priority.order_id, entry.stream.get(), 0)
                };
                (entry.priority.urgency.min(7), within)
            });
            queued.into_iter().map(|entry| entry.stream).collect()
        }
    }

    /// The scheduler selects and visits streams in the reference model's order through every
    /// sequence of operations a session issues, from one queued stream to many: inserting,
    /// enqueueing, pausing, priority updates, incremental rotation, dequeueing and removal.
    #[test]
    fn priority_scheduler_matches_its_reference_model() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut random = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        for round in 0..64 {
            let mut scheduler = Http2PriorityScheduler::new();
            let mut model = SchedulerModel::default();
            let streams = 1 + random(6);
            for step in 0..400 {
                let stream = StreamId::new(2 * random(streams) + 1);
                let priority = HttpPriority::new(
                    random(3) as u8 * 3,
                    random(2) == 0,
                    random(4) as u32,
                    random(8) == 0,
                    0,
                );
                let (operation, scheduled, modeled) = match random(9) {
                    0 | 1 => (
                        "insert",
                        scheduler.insert(stream, priority),
                        model.insert(stream, priority),
                    ),
                    2..=4 => ("enqueue", scheduler.enqueue(stream), model.enqueue(stream)),
                    5 => (
                        "update",
                        scheduler.update(stream, priority),
                        model.update(stream, priority),
                    ),
                    6 => {
                        let bytes = random(2) as usize;
                        (
                            "consume",
                            scheduler.consume(stream, bytes),
                            model.consume(stream, bytes),
                        )
                    }
                    7 => ("dequeue", scheduler.dequeue(stream), model.dequeue(stream)),
                    _ => ("remove", scheduler.remove(stream), model.remove(stream)),
                };
                let at = format!("round {round}, step {step}: {operation} {stream:?}");
                assert_eq!(scheduled, modeled, "{at}");
                let order = model.order();
                assert_eq!(scheduler.next(), order.first().copied(), "{at}");
                let mut visited = Vec::new();
                assert_eq!(
                    scheduler.next_matching(|candidate| {
                        visited.push(candidate);
                        false
                    }),
                    None,
                    "{at}"
                );
                assert_eq!(visited, order, "{at}");
                if let Some(&last) = order.last() {
                    assert_eq!(
                        scheduler.next_matching(|candidate| candidate == last),
                        Some(last),
                        "{at}"
                    );
                }
            }
        }
    }

    /// A lone queued stream, ordered or incremental, stays out of the urgency queues through
    /// selection, consumption and a priority update; a second queued stream moves both into them.
    #[test]
    fn priority_scheduler_keeps_a_lone_queued_stream_out_of_the_urgency_queues() {
        let mut scheduler = Http2PriorityScheduler::new();
        let ordered = StreamId::new(1);
        let incremental = StreamId::new(3);
        let priority = HttpPriority::from_urgency_and_incremental;
        assert!(scheduler.insert(ordered, priority(3, false)));
        assert!(scheduler.insert(incremental, priority(3, true)));

        assert!(scheduler.enqueue(ordered));
        assert_eq!(scheduler.next(), Some(ordered));
        assert!(scheduler.consume(ordered, 1));
        assert!(scheduler.update(ordered, priority(1, false)));
        assert_eq!(
            scheduler.urgency_queue_entries_for_test(),
            0,
            "the only queued stream is held outside the urgency queues"
        );

        assert!(scheduler.enqueue(incremental));
        assert_eq!(
            scheduler.urgency_queue_entries_for_test(),
            2,
            "a second queued stream moves both streams into the urgency queues"
        );
        assert_eq!(scheduler.next(), Some(ordered));
        assert!(scheduler.dequeue(ordered));
        assert!(scheduler.dequeue(incremental));
        assert_eq!(scheduler.urgency_queue_entries_for_test(), 0);

        assert!(scheduler.enqueue(incremental));
        assert_eq!(scheduler.next(), Some(incremental));
        assert_eq!(
            scheduler.urgency_queue_entries_for_test(),
            0,
            "a lone incremental stream is held outside the urgency queues too"
        );
        assert!(scheduler.remove(incremental));
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn stream_window_updates_preserve_signed_accounting() {
        let stream = StreamId::new(1);
        let mut state = Http2DriverState::new(
            Http2SessionConfig::default(),
            roxygen_runtime::host::Host::tokio(),
        );
        state.insert_stream(stream, HttpPriority::default());
        assert!(state.reserve_stream_send(stream, 65_535));
        assert_eq!(state.stream_send_credit(stream), 0);
        assert!(state.free_stream_send(stream, 1));
        assert_eq!(state.stream_send_credit(stream), 1);
        assert!(state.update_initial_send_window(32_768));
        assert_eq!(state.stream_send_credit(stream), 0);
    }

    #[test]
    fn absent_session_stats_preserve_bookkeeping_without_queueing_events() {
        let mut state = Http2DriverState::new(
            Http2SessionConfig {
                rate_limits: Http2RateLimitConfig {
                    header_frames: RateLimitConfig::new(u64::MAX, Duration::from_secs(3600), 0),
                    resets: RateLimitConfig::new(2, Duration::from_secs(3600), 1),
                    ..Http2RateLimitConfig::default()
                },
                ..Http2SessionConfig::default()
            },
            Host::tokio(),
        );
        state.record_transaction_opened();
        state.record_transaction_opened();
        assert_eq!(state.transactions_opened, 2);
        let stream = StreamId::new(1);
        state.insert_stream(stream, HttpPriority::default());
        assert!(state.reserve_stream_send(stream, 65_535));
        assert!(state.mark_stream_stalled(stream));
        assert!(!state.mark_stream_stalled(stream));
        assert_eq!(state.stream_send_credit(stream), 0);
        assert!(state.free_stream_send(stream, 1));
        assert_eq!(state.stream_send_credit(stream), 1);
        assert!(!state.streams[&stream].stalled);

        assert!(!state.record_rate(RateLimitKind::HeaderFrames));
        assert!(state.next_rate_limit_deadline().is_none());
        assert!(!state.record_rate(RateLimitKind::Resets));
        let reset_deadline = state.next_rate_limit_deadline().expect("reset deadline");
        assert!(!state.record_rate(RateLimitKind::HeaderFrames));
        assert_eq!(state.next_rate_limit_deadline(), Some(reset_deadline));
        assert!(!state.record_rate(RateLimitKind::Resets));
        assert!(state.record_rate(RateLimitKind::Resets));
        assert!(state.record_rate(RateLimitKind::Resets));
        state.expire_rate_limits(state.next_rate_limit_deadline().expect("reset deadline"));
        assert!(state.next_rate_limit_deadline().is_none());
        assert!(!state.record_rate(RateLimitKind::Resets));
        state.finalize_stats();
        state.finalize_stats();
        assert!(state.stats_finalized);
        assert!(state.pending_stats.is_empty());
        assert_eq!(state.pending_stats.capacity(), 0);
    }

    #[test]
    fn unbounded_rate_limits_without_stats_do_not_start_deadlines() {
        for (maximum, minimum) in [(u64::MAX, 0), (0, u64::MAX)] {
            let limit = RateLimitConfig::new(maximum, Duration::from_secs(3600), minimum);
            let mut state = Http2DriverState::new(
                Http2SessionConfig {
                    rate_limits: Http2RateLimitConfig {
                        control_frames: limit,
                        header_frames: limit,
                        resets: limit,
                        direct_errors: limit,
                    },
                    ..Http2SessionConfig::default()
                },
                Host::tokio(),
            );
            for kind in [
                RateLimitKind::ControlFrames,
                RateLimitKind::HeaderFrames,
                RateLimitKind::Resets,
                RateLimitKind::DirectErrors,
            ] {
                assert!(!state.record_rate(kind));
                assert!(!state.record_rate(kind));
                assert!(state.next_rate_limit_deadline().is_none());
            }
            assert!(state.pending_stats.is_empty());
        }
    }

    #[test]
    fn unbounded_rate_limits_with_stats_preserve_interval_samples() {
        let mut state = Http2DriverState::new(
            Http2SessionConfig {
                rate_limits: Http2RateLimitConfig {
                    header_frames: RateLimitConfig::new(u64::MAX, Duration::from_secs(3600), 0),
                    ..Http2RateLimitConfig::default()
                },
                stats: Some(Rc::new(StatsProbe::default())),
                ..Http2SessionConfig::default()
            },
            Host::tokio(),
        );
        for count in [2, 1] {
            for _ in 0..count {
                assert!(!state.record_rate(RateLimitKind::HeaderFrames));
            }
            let deadline = state.next_rate_limit_deadline().expect("sampling deadline");
            state.expire_rate_limits(deadline);
            assert!(state.next_rate_limit_deadline().is_none());
            let [Http2StatsEvent::RateLimit(sample)] = state.pending_stats.as_slice() else {
                panic!("one sample for each completed header interval");
            };
            assert_eq!(
                *sample,
                RateLimitSample {
                    kind: RateLimitKind::HeaderFrames,
                    count,
                    limit: u64::MAX,
                    breached: false,
                },
            );
            state.pending_stats.clear();
        }
    }

    #[test]
    fn rate_limits_are_sticky_until_interval_rollover() {
        let now = Instant::now();
        let limit = RateLimitConfig::new(2, Duration::from_secs(1), 1);
        for kind in [
            RateLimitKind::ControlFrames,
            RateLimitKind::HeaderFrames,
            RateLimitKind::Resets,
            RateLimitKind::DirectErrors,
        ] {
            let mut limiter = Http2RateLimiter::new(
                Http2RateLimitConfig {
                    control_frames: limit,
                    header_frames: limit,
                    resets: limit,
                    direct_errors: limit,
                },
                now,
            );
            assert!(!limiter.record(kind, now).0);
            assert!(!limiter.record(kind, now).0);
            let (breached, sample) = limiter.record(kind, now);
            assert!(breached);
            assert_eq!(sample.expect("threshold breach sample").count, 3);
            let (breached, sample) = limiter.record(kind, now);
            assert!(breached);
            assert_eq!(sample.expect("repeated breach sample").count, 4);
            let samples = limiter.expire(now + Duration::from_secs(1));
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].kind, kind);
            assert_eq!(samples[0].count, 4);
            assert!(samples[0].breached);
            assert!(limiter.next_deadline().is_none());
            assert!(limiter.expire(now + Duration::from_secs(2)).is_empty());
        }
    }

    #[test]
    fn rate_limit_config_is_unbounded_uses_the_effective_limit() {
        let interval = Duration::from_secs(1);
        for (maximum, minimum, unbounded) in [
            (u64::MAX, 0, true),
            (0, u64::MAX, true),
            (u64::MAX, u64::MAX, true),
            (u64::MAX - 1, 0, false),
            (100, 50, false),
            (0, 0, false),
        ] {
            let limit = RateLimitConfig::new(maximum, interval, minimum);
            assert_eq!(limit.is_unbounded(), unbounded, "{limit:?}");
            let limiter = Http2RateLimiter::new(
                Http2RateLimitConfig {
                    header_frames: limit,
                    ..Http2RateLimitConfig::default()
                },
                Instant::now(),
            );
            assert_eq!(
                limiter.is_unbounded(RateLimitKind::HeaderFrames),
                unbounded,
                "{limit:?}"
            );
        }
        let unlimited = RateLimitConfig::new(u64::MAX, interval, 0);
        let all = Http2RateLimitConfig {
            control_frames: unlimited,
            header_frames: unlimited,
            resets: unlimited,
            direct_errors: unlimited,
        };
        assert!(all.is_unbounded());
        assert!(!Http2RateLimitConfig::default().is_unbounded());
        assert!(
            !Http2RateLimitConfig {
                direct_errors: RateLimitConfig::new(100, interval, 50),
                ..all
            }
            .is_unbounded()
        );
    }

    fn assert_real_reset_rate_limit_cancels_unrelated_active_request() {
        run_on_worker(|context| {
            Box::pin(async move {
                let mut rate_limits = Http2RateLimitConfig::default();
                // Keep the reset burst in one accounting window under instrumentation.
                rate_limits.resets.interval = Duration::from_secs(10);
                let (initial, resets) = peer_reset_rate_limit_bytes();
                let release = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(Arc::clone(&release), resets),
                ]);
                let held = Rc::new(RefCell::new(None));
                let invocations = Rc::new(Cell::new(0));
                let handler: Rc<dyn HttpHandler> = Rc::new(HoldFirstRequestHandler {
                    invocations: Rc::clone(&invocations),
                    held: Rc::clone(&held),
                });
                let stats = Rc::new(StatsProbe::default());
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        rate_limits,
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let mut run = Box::pin(session.run());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while held.borrow().is_none() {
                        tokio::select! {
                            result = run.as_mut() => panic!("session ended before held request: {result:?}"),
                            _ = tokio::task::yield_now() => {},
                        }
                    }
                })
                .await
                .expect("first request source was retained");
                release.notify_one();
                let mut request = held.borrow_mut().take().expect("held request source");
                let mut body_read = Box::pin(async move {
                    loop {
                        match request.read_body_event(u32::MAX).await {
                            Ok(event) => match event.into_kind() {
                                HttpBodyEventKind::Suspend(resume) => {
                                    let _ = resume.await;
                                }
                                _ => panic!("rate breach produced request body"),
                            },
                            Err(error) => break (error, request),
                        }
                    }
                });
                let error = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::select! {
                        (error, request) = body_read.as_mut() => (error, request),
                        result = run.as_mut() => panic!("session ended before source cancellation: {result:?}"),
                    }
                })
                .await
                .expect("active request was cancelled");
                assert_eq!(error.0.code(), HttpErrorCode::CANCEL);
                drop(body_read);
                drop(error.1);
                let run_error = tokio::time::timeout(Duration::from_secs(2), run.as_mut())
                    .await
                    .expect("rate-limit shutdown timed out")
                    .expect_err("rate-limit breach fails the connection");
                assert_eq!(run_error.code(), HttpErrorCode::CANCEL);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                let samples = stats.rate_samples.borrow();
                assert!(samples.iter().any(|sample| {
                    sample.kind == RateLimitKind::Resets
                        && sample.count == 1_001
                        && sample.limit == 1_000
                        && sample.breached
                }));
                assert!(invocations.get() >= 1);
            })
        });
    }

    #[test]
    fn control_rate_limit_counts_only_semantic_request_callbacks() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::Data(peer_control_rate_limit_bytes())]);
                let stats = Rc::new(StatsProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let limits = RateLimitConfig::new(3, Duration::from_secs(60), 0);
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        rate_limits: Http2RateLimitConfig {
                            control_frames: limits,
                            ..Http2RateLimitConfig::default()
                        },
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let error = session
                    .run()
                    .await
                    .expect_err("fourth semantic control request breaches the limit");
                assert_eq!(error.code(), HttpErrorCode::CANCEL);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                assert_eq!(
                    decode_server_frames(&state.borrow().writes.concat()).ping_replies,
                    [11]
                );
                assert!(stats.rate_samples.borrow().iter().any(|sample| {
                    sample.kind == RateLimitKind::ControlFrames
                        && sample.count == 4
                        && sample.limit == 3
                        && sample.breached
                }));
            })
        });
    }

    #[test]
    fn header_rate_limit_suppresses_the_crossing_headers_callback() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::DataEof(peer_two_requests_bytes())]);
                let stats = Rc::new(StatsProbe::default());
                let events = Rc::new(RefCell::new(Vec::new()));
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        rate_limits: Http2RateLimitConfig {
                            header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                            ..Http2RateLimitConfig::default()
                        },
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let _registration = session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                session
                    .run()
                    .await
                    .expect("header rate breach gracefully drains");
                assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert!(decoded.resets.is_empty());
                assert_eq!(
                    decoded.goaways,
                    [(
                        StreamId::new(u64::from(roxygen_http2::constants::MAX_STREAM_ID)),
                        H2ErrorCode::NO_ERROR,
                    )]
                );
                assert_eq!(
                    event_count(&events.borrow(), |event| matches!(
                        event,
                        LifecycleEvent::Http2IngressMessage { .. }
                    )),
                    1
                );
                assert!(stats.rate_samples.borrow().iter().any(|sample| {
                    sample.kind == RateLimitKind::HeaderFrames
                        && sample.count == 2
                        && sample.limit == 1
                        && sample.breached
                }));
            })
        });
    }

    fn assert_failed_write_has_no_http2_write_lifecycle(short_write: bool) {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(Vec::new())]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");
                let events = Rc::new(RefCell::new(Vec::new()));
                let _registration = session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .expect("lifecycle observer");
                if short_write {
                    state.borrow_mut().short_write = true;
                } else {
                    state.borrow_mut().write_error = Some(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                }
                let error = session
                    .run()
                    .await
                    .expect_err("failed transport write fails the session");
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert!(
                    !events
                        .borrow()
                        .iter()
                        .any(|event| matches!(event, LifecycleEvent::Http2Write { .. }))
                );
            })
        });
    }

    #[test]
    fn http2_write_lifecycle_excludes_transport_error() {
        assert_failed_write_has_no_http2_write_lifecycle(false);
    }

    #[test]
    fn http2_write_lifecycle_excludes_short_success() {
        assert_failed_write_has_no_http2_write_lifecycle(true);
    }

    #[derive(Default)]
    struct RateSamplingProbe {
        created: Cell<usize>,
        started: Cell<usize>,
        returned: Cell<usize>,
        dropped: Cell<usize>,
        abandoned: Cell<usize>,
        timeout: Cell<Option<Duration>>,
        returned_error: Cell<Option<HttpErrorCode>>,
        returned_bytes: Cell<usize>,
        sample: Cell<Option<RateLimitSample>>,
        pending_at_sample: Cell<bool>,
        on_sample: RefCell<Option<Box<dyn FnOnce()>>>,
        changed: tokio::sync::Notify,
        release: Arc<tokio::sync::Notify>,
    }

    impl Http2SessionStats for RateSamplingProbe {
        fn record_rate_limit(&self, sample: RateLimitSample) {
            if sample.kind == RateLimitKind::ControlFrames && self.sample.get().is_none() {
                self.sample.set(Some(sample));
                self.pending_at_sample.set(
                    self.created.get() == 1 && self.returned.get() == 0 && self.dropped.get() == 0,
                );
                self.changed.notify_one();
                let on_sample = self.on_sample.borrow_mut().take();
                if let Some(on_sample) = on_sample {
                    on_sample();
                }
            }
        }
    }

    struct RateSamplingRead {
        read: LocalTransportFuture<Result<UniplexRead, HttpError>>,
        probe: Rc<RateSamplingProbe>,
        complete: bool,
    }

    impl RateSamplingRead {
        async fn wait(
            mut self,
            timeout: Duration,
            cancellation: CancellationToken,
        ) -> Result<UniplexRead, HttpError> {
            self.probe.started.set(self.probe.started.get() + 1);
            self.probe.timeout.set(Some(timeout));
            self.probe.changed.notify_one();
            let baton = CancellableBaton::new();
            let result = tokio::select! {
                biased;
                result = self.read.as_mut() => result,
                status = baton.timed_wait(timeout, cancellation) => Err(HttpError::with_message(
                    if status == WaitStatus::TimedOut {
                        HttpErrorCode::READ_TIMEOUT
                    } else {
                        HttpErrorCode::CORO_CANCELLED
                    },
                    b"held sampling read interrupted",
                )),
            };
            self.complete = true;
            self.probe.returned.set(self.probe.returned.get() + 1);
            self.probe
                .returned_error
                .set(result.as_ref().err().map(HttpError::code));
            self.probe
                .returned_bytes
                .set(result.as_ref().map_or(0, UniplexRead::bytes_read));
            self.probe.changed.notify_one();
            result
        }
    }

    impl Drop for RateSamplingRead {
        fn drop(&mut self) {
            self.probe.dropped.set(self.probe.dropped.get() + 1);
            if !self.complete {
                self.probe.abandoned.set(self.probe.abandoned.get() + 1);
            }
            self.probe.changed.notify_one();
        }
    }

    struct RateSamplingTransport {
        inner: Rc<FixtureTransport>,
        probe: Rc<RateSamplingProbe>,
        reads: Cell<usize>,
    }

    impl UniplexTransport for RateSamplingTransport {
        fn read(
            &self,
            context: &WorkerContext,
            maximum: usize,
            allocation_size: usize,
            timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
            self.reads.set(self.reads.get() + 1);
            let read = self.inner.read(
                context,
                maximum,
                allocation_size,
                timeout,
                cancellation.clone(),
            );
            if self.reads.get() != 2 {
                return read;
            }
            self.probe.created.set(self.probe.created.get() + 1);
            Box::pin(
                RateSamplingRead {
                    read,
                    probe: Rc::clone(&self.probe),
                    complete: false,
                }
                .wait(timeout, cancellation),
            )
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

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum SamplingCompletion {
        Data,
        Cancel,
        CloseWithData,
        CloseWithEof,
    }

    async fn assert_rate_sampling_preserves_read(
        context: WorkerContext,
        completion: SamplingCompletion,
        close_from_sample: bool,
    ) {
        let cancel = completion != SamplingCompletion::Data;
        let (initial, mut remaining) = peer_request_parts();
        if completion == SamplingCompletion::CloseWithEof {
            remaining.clear();
        }
        let initial_length = initial.len();
        let remaining_length = remaining.len();
        let probe = Rc::new(RateSamplingProbe::default());
        let (inner, state) = FixtureTransport::new([
            FixtureRead::Data(initial),
            FixtureRead::WaitDataEof(Arc::clone(&probe.release), remaining),
        ]);
        let transport = Rc::new(RateSamplingTransport {
            inner,
            probe: Rc::clone(&probe),
            reads: Cell::new(0),
        });
        let invocations = Rc::new(Cell::new(0));
        let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
            status: 200,
            response_body: 0,
            response_error: None,
            invocations: Rc::clone(&invocations),
            request_body: Rc::new(Cell::new(0)),
        });
        let config = UniplexConfig::default();
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Downstream,
            transport.clone(),
            Some(handler),
            config,
            Http2SessionConfig {
                stats: Some(probe.clone()),
                ..Http2SessionConfig::default()
            },
        )
        .expect("sampling H2 session");
        let events = Rc::new(RefCell::new(Vec::new()));
        let _registration = session
            .add_lifecycle_observer(Rc::new(LifecycleProbe {
                events: Rc::clone(&events),
            }))
            .expect("lifecycle observer");
        let sample_close = Rc::new(RefCell::new(None));
        if close_from_sample {
            let handle = handle.clone();
            let sample_close = Rc::clone(&sample_close);
            let release = Arc::clone(&probe.release);
            *probe.on_sample.borrow_mut() = Some(Box::new(move || {
                if completion != SamplingCompletion::Cancel {
                    release.notify_one();
                }
                *sample_close.borrow_mut() = Some(handle.close_when_idle());
            }));
        }
        let control = async {
            let sampled = tokio::time::timeout(Duration::from_secs(5), async {
                while probe.sample.get().is_none() {
                    probe.changed.notified().await;
                }
            })
            .await;
            let closed = if (cancel && !close_from_sample) || sampled.is_err() {
                Some(handle.close_when_idle())
            } else {
                None
            };
            if !cancel || sampled.is_err() {
                probe.release.notify_one();
            }
            (sampled, closed.or_else(|| sample_close.borrow_mut().take()))
        };
        let (result, (sampled, closed)) = tokio::join!(session.run(), control);
        sampled.expect("actual control-frame sampling expired while the read was held");
        assert!(handle.is_finished(), "cancel={cancel}");
        assert_eq!(transport.reads.get(), 2, "cancel={cancel}");
        assert_eq!(probe.created.get(), 1, "cancel={cancel}");
        assert_eq!(probe.started.get(), 1, "cancel={cancel}");
        assert_eq!(probe.returned.get(), 1, "cancel={cancel}");
        assert_eq!(probe.dropped.get(), 1, "cancel={cancel}");
        assert_eq!(probe.abandoned.get(), 0, "cancel={cancel}");
        assert!(probe.pending_at_sample.get(), "cancel={cancel}");
        assert_eq!(probe.timeout.get(), Some(config.read_timeout));
        assert_eq!(
            probe.sample.get(),
            Some(RateLimitSample {
                kind: RateLimitKind::ControlFrames,
                count: 1,
                limit: Http2RateLimitConfig::default().control_frames.maximum,
                breached: false,
            }),
        );
        if let Some(closed) = closed {
            assert!(closed.expect("close-when-idle keeps real cancellation"));
        }
        assert_eq!(
            probe.returned_error.get(),
            (completion == SamplingCompletion::Cancel).then_some(HttpErrorCode::CORO_CANCELLED),
        );
        let summary = result.expect("delivered bytes or local cancellation drain normally");
        let delivered = if completion == SamplingCompletion::Cancel {
            0
        } else {
            remaining_length
        };
        assert_eq!(probe.returned_bytes.get(), delivered);
        assert_eq!(summary.bytes_read, (initial_length + delivered) as u64);
        let observed_reads: Vec<_> = events
            .borrow()
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::Http2Read { bytes, .. } => Some(*bytes),
                _ => None,
            })
            .collect();
        let expected_reads = if completion == SamplingCompletion::Cancel {
            vec![initial_length]
        } else {
            vec![initial_length, delivered]
        };
        assert_eq!(observed_reads, expected_reads);
        if completion == SamplingCompletion::CloseWithEof {
            let events = events.borrow();
            assert_eq!(
                event_count(&events, |event| matches!(
                    event,
                    LifecycleEvent::Http2IngressEof
                )),
                1,
            );
            let read = event_position(
                &events,
                |event| {
                    matches!(
                        event,
                        LifecycleEvent::Http2Read {
                            bytes: 0,
                            stream: None
                        }
                    )
                },
                "zero-byte transport read",
            );
            let eof = event_position(
                &events,
                |event| matches!(event, LifecycleEvent::Http2IngressEof),
                "ingress EOF",
            );
            assert!(read < eof);
        }
        assert!(!summary.reset);
        assert_eq!(state.borrow().close, FixtureClose::ShutdownWrite);
        assert_eq!(invocations.get(), if cancel { 1 } else { 2 });
    }

    #[test]
    fn rate_sampling_preserves_pending_transport_read() {
        for completion in [SamplingCompletion::Data, SamplingCompletion::Cancel] {
            run_on_worker(move |context| {
                Box::pin(assert_rate_sampling_preserves_read(
                    context, completion, false,
                ))
            });
        }
    }

    #[test]
    fn rate_sampling_close_retires_pending_transport_read() {
        run_on_worker(|context| {
            Box::pin(assert_rate_sampling_preserves_read(
                context,
                SamplingCompletion::Cancel,
                true,
            ))
        });
    }

    #[test]
    fn rate_sampling_close_preserves_ready_transport_data() {
        run_on_worker(|context| {
            Box::pin(assert_rate_sampling_preserves_read(
                context,
                SamplingCompletion::CloseWithData,
                true,
            ))
        });
    }

    #[test]
    fn rate_sampling_close_preserves_ready_transport_eof() {
        run_on_worker(|context| {
            Box::pin(assert_rate_sampling_preserves_read(
                context,
                SamplingCompletion::CloseWithEof,
                true,
            ))
        });
    }

    #[test]
    fn direct_error_rate_limit_suppresses_the_crossing_semantic_error() {
        run_on_worker(|context| {
            Box::pin(async move {
                let input = peer_duplicate_content_length_requests(51);
                let (transport, state) = FixtureTransport::new(input);
                let stats = Rc::new(StatsProbe::default());
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stats: Some(stats.clone()),
                        rate_limits: Http2RateLimitConfig {
                            direct_errors: RateLimitConfig::new(10, Duration::from_secs(60), 50),
                            ..Http2RateLimitConfig::default()
                        },
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("downstream H2 session");
                let error = match session.run().await {
                    Ok(summary) => panic!(
                        "the 51st direct semantic error did not breach: {summary:?}; samples={:?}",
                        stats.rate_samples.borrow(),
                    ),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::CANCEL);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
                assert!(stats.rate_samples.borrow().iter().any(|sample| {
                    sample.kind == RateLimitKind::DirectErrors
                        && sample.count == 51
                        && sample.limit == 50
                        && sample.breached
                }));
            })
        });
    }

    #[test]
    fn h2_lifecycle_reset_and_error_order_is_source_ordered() {
        assert_real_h2_reset_and_error_lifecycle_order();
    }

    #[test]
    fn h2_config_rejects_zero_and_conflicting_limits_and_emits_effective_windows() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (invalid_transport, _) = FixtureTransport::new([]);
                let invalid = UniplexSession::new_http2(
                    context.clone(),
                    TransportDirection::Upstream,
                    invalid_transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        max_concurrent_outgoing_streams: 0,
                        ..Http2SessionConfig::default()
                    },
                );
                let Err(error) = invalid else {
                    panic!("a local zero outgoing-stream cap must be rejected")
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);

                let (zero_transport, _) = FixtureTransport::new([]);
                let zero = UniplexSession::new_http2(
                    context.clone(),
                    TransportDirection::Upstream,
                    zero_transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stream_receive_window: 0,
                        ..Http2SessionConfig::default()
                    },
                );
                let Err(error) = zero else {
                    panic!("a zero Proxygen HTTP/2 receive window must be rejected")
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);

                let (conflict_transport, _) = FixtureTransport::new([]);
                let conflict = UniplexSession::new_http2(
                    context.clone(),
                    TransportDirection::Upstream,
                    conflict_transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        settings: vec![roxygen_codec::CodecSetting {
                            identifier: SettingsId::InitialWindowSize.as_u16(),
                            value: 128,
                        }],
                        stream_receive_window: 64,
                        ..Http2SessionConfig::default()
                    },
                );
                let Err(error) = conflict else {
                    panic!("conflicting advertised and internal windows must be rejected")
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);

                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
                let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
                    status: 200,
                    response_body: 0,
                    response_error: None,
                    invocations: Rc::new(Cell::new(0)),
                    request_body: Rc::new(Cell::new(0)),
                });
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        stream_receive_window: 64,
                        connection_receive_window: 131_072,
                        ..Http2SessionConfig::default()
                    },
                )
                .expect("nonzero HTTP/2 receive windows are representable");
                session
                    .run()
                    .await
                    .expect("configured-window session completes");
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 1);
                assert!(decoded.settings.contains(&roxygen_codec::CodecSetting {
                    identifier: SettingsId::InitialWindowSize.as_u16(),
                    value: 64,
                }));
                assert!(
                    decoded
                        .window_updates
                        .contains(&(StreamId::UNSPECIFIED, 65_537))
                );
            })
        });
    }

    #[test]
    fn h2_http_checks_reject_trace_bodies_in_both_directions() {
        let mut trace = roxygen_core::message::HttpMessage::new();
        trace.set_method(HttpMethod::Trace).expect("TRACE method");
        trace.set_url(b"/trace", false).expect("TRACE URL");
        trace.set_http_version(1, 1);
        trace
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        trace.headers_mut().set(HttpHeaderCode::ContentLength, b"1");
        let outcome = run_downstream_error_case(
            vec![FixtureRead::DataEof(peer_request_message_bytes(
                trace,
                Some(queue(b"x")),
                true,
            ))],
            ErrorHandlerCase::ReadBodyThenRespond {
                status: 200,
                body: b"ignored",
            },
            UniplexConfig::default(),
            None,
        );
        let decoded = decode_server_frames(&outcome.wire);
        assert_eq!(decoded.header_statuses, [(StreamId::new(1), 400)]);
        assert_eq!(
            outcome.request_error,
            Some(HttpErrorCode::HEADER_PARSE_ERROR)
        );
        assert!(outcome.partial_message);

        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let mut request = HttpFixedSource::make_fixed_request(
                    b"/trace",
                    HttpMethod::Trace,
                    Some(queue(b"x")),
                )
                .expect("TRACE request source");
                request
                    .message_mut()
                    .expect("TRACE headers")
                    .headers_mut()
                    .set(HttpHeaderCode::ContentLength, b"1");
                let error = match handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                {
                    Ok(_) => panic!("TRACE request body was admitted"),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::HEADER_PARSE_ERROR);
                let run = Box::pin(session.run());
                tokio::task::yield_now().await;
                assert!(handle.close_when_idle().expect("close rejected session"));
                run.await.expect("rejected TRACE session closes cleanly");
                assert!(
                    decode_client_frames(&state.borrow().writes.concat())
                        .header_streams
                        .is_empty()
                );
            })
        });
    }

    #[test]
    fn h2_stream_reset_cancels_only_matching_byte_events() {
        assert_real_stream_reset_cancels_only_its_byte_events(
            H2ErrorCode::NO_ERROR,
            HttpErrorCode::CANCEL,
        );
    }

    #[test]
    fn h2_non_cancel_peer_reset_preserves_exact_byte_event_error() {
        assert_real_stream_reset_cancels_only_its_byte_events(
            H2ErrorCode::ENHANCE_YOUR_CALM,
            HttpErrorCode::ENHANCE_YOUR_CALM,
        );
    }

    /// The HTTP/2 timer tests on a `HostedWorker`, whose wheel timer backs the
    /// session's timed batons and host sleeps: idle and transport write timeouts in
    /// both directions, upstream response and downstream request read timeouts (408),
    /// custom timeout responses, buffer and flow-control timeouts and their
    /// arbitration, GOAWAY drains, the reset rate limit's window, and byte events.
    #[test]
    fn h2_timers_on_a_hosted_worker() {
        crate::uniplex_tests::on_hosted_workers(|| {
            for direction in [TransportDirection::Upstream, TransportDirection::Downstream] {
                assert_real_idle_timeout(direction);
                assert_real_transport_write_timeout(direction);
            }
            for case in [
                RequestTimeoutCase::PendingPost,
                RequestTimeoutCase::CompletePost,
                RequestTimeoutCase::CompletePostCustom,
                RequestTimeoutCase::CompleteGet,
            ] {
                assert_real_upstream_response_timeout(case);
            }
            assert_real_downstream_request_read_timeout();
            assert_real_partial_headers_use_connection_timeout();
            assert_real_custom_timeout_response(408);
            assert_real_custom_timeout_response(500);
            assert_real_transaction_buffer_timeout();
            complete_transfer_credit_preserves_zero_window_buffer_timeout();
            upstream_full_buffer_case(FullBufferCase::Prefilled);
            upstream_full_buffer_case(FullBufferCase::DisabledTimeout);
            flow_timeout_retains_tokio_clock_origin();
            timeout_arbitration_uses_tokio_expiration_clock();
            assert_real_egress_flow_timeout(false);
            assert_real_egress_flow_timeout(true);
            assert_real_stream_flow_timeout_resets_every_blocked_stream();
            assert_no_flow_timeout_without_buffered_body();
            assert_real_downstream_request_between_goaways();
            assert_real_upstream_single_goaway();
            assert_upstream_double_drain_retires_without_final_goaway();
            assert_first_wire_maximum_goaway_retires_idle_upstream();
            assert_incoming_wire_maximum_goaway_keeps_timed_final_drain();
            assert_real_reset_rate_limit_cancels_unrelated_active_request();
            assert_real_byte_event_timeout();
        });
    }

    #[test]
    fn h2_stream_flow_timeout_resets_all_blocked_streams() {
        assert_real_stream_flow_timeout_resets_every_blocked_stream();
    }

    #[test]
    fn h2_zero_window_without_buffered_body_does_not_timeout() {
        assert_no_flow_timeout_without_buffered_body();
    }

    #[test]
    fn h2_buffered_write_stats_decrease_as_each_chunk_is_serialized() {
        assert_buffered_write_stats_decrease_per_serialized_chunk();
    }

    #[test]
    fn h2_final_goaway_keeps_an_admitted_post_readable() {
        assert_real_final_goaway_keeps_active_ingress_reading();
    }

    #[test]
    fn h2_transport_read_error_resets_session() {
        assert_real_read_failure_resets_session();
    }

    macro_rules! mapped_session_case {
        (fn h2_downstream_session_test_ping() => MappedContract::Basic) => {
            #[test]
            fn h2_downstream_session_test_ping() {
                assert_real_local_and_peer_ping_lifecycle();
            }
        };
        (fn httpupstream_session_test_simple() => MappedContract::Basic) => {
            #[test]
            fn httpupstream_session_test_simple() {
                assert_real_upstream_exchange(0, 100);
            }
        };
        (fn httpupstream_session_test_simple_post() => MappedContract::Body) => {
            #[test]
            fn httpupstream_session_test_simple_post() {
                assert_real_upstream_exchange(100, 100);
            }
        };
        (fn httpupstream_session_test_large_post() => MappedContract::Body) => {
            #[test]
            fn httpupstream_session_test_large_post() {
                assert_real_upstream_exchange(96 << 10, 100);
            }
        };
        (fn httpupstream_session_test_padding() => MappedContract::Padding) => {
            #[test]
            fn httpupstream_session_test_padding() {
                assert_real_upstream_padding();
            }
        };
        (fn h2_qupstream_session_test_receive_full_response_prior_to_request_eom() => MappedContract::Body) => {
            #[test]
            fn h2_qupstream_session_test_receive_full_response_prior_to_request_eom() {
                assert_real_upstream_response_followed_by_reset(true);
            }
        };
        (fn h2_upstream_session_test_rst_stream_after_eom() => MappedContract::Reset) => {
            #[test]
            fn h2_upstream_session_test_rst_stream_after_eom() {
                assert_real_upstream_response_followed_by_reset(false);
            }
        };
        (fn h2_qupstream_session_test_ingress_reset_stream() => MappedContract::Reset) => {
            #[test]
            fn h2_qupstream_session_test_ingress_reset_stream() {
                assert_real_upstream_response_reset();
            }
        };
        (fn h2_downstream_session_test_reset_rate_limit() => MappedContract::Reset) => {
            #[test]
            fn h2_downstream_session_test_reset_rate_limit() {
                assert_real_reset_rate_limit_cancels_unrelated_active_request();
            }
        };
        (fn h2_qdownstream_session_test_reset_stream() => MappedContract::Reset) => {
            #[test]
            fn h2_qdownstream_session_test_reset_stream() {
                assert_real_peer_reset_is_not_echoed(H2ErrorCode::CANCEL);
            }
        };
        (fn h2_qdownstream_session_test_reset_egress_ingress_open() => MappedContract::Reset) => {
            #[test]
            fn h2_qdownstream_session_test_reset_egress_ingress_open() {
                assert_real_egress_error_resets_stream(HttpErrorCode::CANCEL);
            }
        };
        (fn h2_qdownstream_session_test_reset_stream_awaiting_headers() => MappedContract::Reset) => {
            #[test]
            fn h2_qdownstream_session_test_reset_stream_awaiting_headers() {
                assert_real_egress_error_resets_stream(HttpErrorCode::CANCEL);
            }
        };
        (fn h2_downstream_session_test_reset_stream_no_error() => MappedContract::Reset) => {
            #[test]
            fn h2_downstream_session_test_reset_stream_no_error() {
                assert_real_peer_no_error_reset_cancels_ingress();
            }
        };
        (fn httpupstream_session_test_send_goaway() => MappedContract::Goaway) => {
            #[test]
            fn httpupstream_session_test_send_goaway() {
                assert_real_upstream_single_goaway();
            }
        };
        (fn httpdownstream_session_test_send_goaway() => MappedContract::Goaway) => {
            #[test]
            fn httpdownstream_session_test_send_goaway() {
                assert_real_downstream_request_between_goaways();
            }
        };
        (fn httpdownstream_session_test_close_when_idle_no_streams() => MappedContract::Goaway) => {
            #[test]
            fn httpdownstream_session_test_close_when_idle_no_streams() {
                assert_real_close_when_idle_is_single_stage();
            }
        };
        (fn h2_qupstream_session_test_receive_goaway() => MappedContract::Goaway) => {
            #[test]
            fn h2_qupstream_session_test_receive_goaway() {
                assert_real_upstream_peer_goaway_without_streams();
            }
        };
        (fn h2_qupstream_session_test_receive_goaway_with_open_stream() => MappedContract::Goaway) => {
            #[test]
            fn h2_qupstream_session_test_receive_goaway_with_open_stream() {
                assert_real_upstream_peer_goaway_with_open_stream();
            }
        };
        (fn h2_upstream_session_test_receive_goaway_with_error() => MappedContract::Goaway) => {
            #[test]
            fn h2_upstream_session_test_receive_goaway_with_error() {
                assert_real_upstream_peer_goaway_error(false);
            }
        };
        (fn h2_upstream_session_test_receive_goaway_before_request_egress() => MappedContract::Goaway) => {
            #[test]
            fn h2_upstream_session_test_receive_goaway_before_request_egress() {
                assert_real_upstream_peer_goaway_error(true);
            }
        };
        (fn h2_qdownstream_session_test_receive_goaway() => MappedContract::Goaway) => {
            #[test]
            fn h2_qdownstream_session_test_receive_goaway() {
                assert_real_downstream_peer_goaway_without_streams();
            }
        };
        (fn httpdownstream_session_test_close_when_idle_with_stream() => MappedContract::Goaway) => {
            #[test]
            fn httpdownstream_session_test_close_when_idle_with_stream() {
                assert_real_close_when_idle_waits_for_active_stream();
            }
        };
        (fn h2_qdownstream_session_test_receive_goaway_with_open_stream() => MappedContract::Goaway) => {
            #[test]
            fn h2_qdownstream_session_test_receive_goaway_with_open_stream() {
                assert_real_peer_goaway(H2ErrorCode::NO_ERROR);
            }
        };
        (fn h2_downstream_session_test_receive_goaway_error_with_open_stream() => MappedContract::Goaway) => {
            #[test]
            fn h2_downstream_session_test_receive_goaway_error_with_open_stream() {
                assert_real_peer_goaway(H2ErrorCode::INTERNAL_ERROR);
            }
        };
        (fn h2_qdownstream_session_test_stream_after_goaway() => MappedContract::Goaway) => {
            #[test]
            fn h2_qdownstream_session_test_stream_after_goaway() {
                assert_real_stream_after_final_goaway_is_not_admitted();
            }
        };
        (fn h2_qupstream_session_test_push() => MappedContract::Push) => {
            #[test]
            fn h2_qupstream_session_test_push() {
                assert_real_upstream_push(false);
                assert_real_upstream_push(true);
            }
        };
        (fn h2_qupstream_session_test_push_egress_rst_stream() => MappedContract::Push) => {
            #[test]
            fn h2_qupstream_session_test_push_egress_rst_stream() {
                assert_real_upstream_abandoned_push_resets_child();
            }
        };
        (fn h2_qupstream_session_test_push_ingress_rst_stream() => MappedContract::Push) => {
            #[test]
            fn h2_qupstream_session_test_push_ingress_rst_stream() {
                assert_real_upstream_pushed_stream_reset();
            }
        };
        (fn h2_upstream_session_test_push_parent_reset() => MappedContract::Push) => {
            #[test]
            fn h2_upstream_session_test_push_parent_reset() {
                assert_real_push_parent_reset_before_promise_completion();
            }
        };
        (fn h2_upstream_session_test_push_promise_parse_error() => MappedContract::Push) => {
            #[test]
            fn h2_upstream_session_test_push_promise_parse_error() {
                assert_real_push_promise_parse_error();
            }
        };
        (fn h2_qdownstream_session_test_push() => MappedContract::Push) => {
            #[test]
            fn h2_qdownstream_session_test_push() {
                assert_real_downstream_push(true, 100, None);
            }
        };
        (fn h2_qdownstream_session_test_push_egress_rst_stream() => MappedContract::Push) => {
            #[test]
            fn h2_qdownstream_session_test_push_egress_rst_stream() {
                assert_real_downstream_push(true, 100, Some(HttpErrorCode::CANCEL));
            }
        };
        (fn h2_qdownstream_session_test_push_ingress_rst_stream() => MappedContract::Push) => {
            #[test]
            fn h2_qdownstream_session_test_push_ingress_rst_stream() {
                assert_real_downstream_pushed_stream_peer_reset();
            }
        };
        (fn h2_qdownstream_session_test_egress_push_stream_limit_exceeded() => MappedContract::Push) => {
            #[test]
            fn h2_qdownstream_session_test_egress_push_stream_limit_exceeded() {
                assert_real_downstream_push(true, 0, None);
            }
        };
        (fn h2_downstream_session_test_egress_push_not_supported() => MappedContract::Push) => {
            #[test]
            fn h2_downstream_session_test_egress_push_not_supported() {
                assert_real_downstream_push(false, 100, None);
            }
        };
        (fn h2_upstream_session_test_conn_flow_control_on_body_error() => MappedContract::FlowControl) => {
            #[test]
            fn h2_upstream_session_test_conn_flow_control_on_body_error() {
                assert_real_upstream_response_flow_control_error(true);
            }
        };
        (fn h2_upstream_session_test_stream_flow_control_on_body_error() => MappedContract::FlowControl) => {
            #[test]
            fn h2_upstream_session_test_stream_flow_control_on_body_error() {
                assert_real_upstream_response_flow_control_error(false);
            }
        };
        (fn h2_upstream_session_test_conn_flow_control_on_window_update_error() => MappedContract::FlowControl) => {
            #[test]
            fn h2_upstream_session_test_conn_flow_control_on_window_update_error() {
                assert_real_upstream_window_update_overflow(true);
            }
        };
        (fn h2_upstream_session_test_stream_flow_control_on_window_update_error() => MappedContract::FlowControl) => {
            #[test]
            fn h2_upstream_session_test_stream_flow_control_on_window_update_error() {
                assert_real_upstream_window_update_overflow(false);
            }
        };
        (fn h2_upstream_session_test_reset_release_conn_flow_control() => MappedContract::FlowControl) => {
            #[test]
            fn h2_upstream_session_test_reset_release_conn_flow_control() {
                assert_real_reset_releases_connection_flow_control();
            }
        };
        (fn h2_upstream_session_test_free_conn_flow_control_after_rst() => MappedContract::FlowControl) => {
            #[test]
            fn h2_upstream_session_test_free_conn_flow_control_after_rst() {
                assert_real_late_body_after_reset_releases_connection_credit();
            }
        };
        (fn h2_upstream_session_test_egress_while_writes_blocked() => MappedContract::FlowControl) => {
            #[test]
            fn h2_upstream_session_test_egress_while_writes_blocked() {
                assert_real_upstream_egress_queues_while_transport_write_is_blocked();
            }
        };
        (fn h2_downstream_session_test_egress_stream_flow_control() => MappedContract::FlowControl) => {
            #[test]
            fn h2_downstream_session_test_egress_stream_flow_control() {
                assert_real_downstream_stream_window_recovery();
            }
        };
        (fn h2_downstream_session_test_ingress_stream_flow_control_error() => MappedContract::FlowControl) => {
            #[test]
            fn h2_downstream_session_test_ingress_stream_flow_control_error() {
                assert_real_ingress_flow_control_error(false);
            }
        };
        (fn h2_qdownstream_session_test_stream_flow_control_timeout() => MappedContract::FlowControl) => {
            #[test]
            fn h2_qdownstream_session_test_stream_flow_control_timeout() {
                assert_real_egress_flow_timeout(false);
            }
        };
        (fn h2_downstream_session_test_conn_flow_control_timeout() => MappedContract::FlowControl) => {
            #[test]
            fn h2_downstream_session_test_conn_flow_control_timeout() {
                assert_real_egress_flow_timeout(true);
            }
        };
        (fn httpupstream_session_test_stream_ingress_complete_no_rst() => MappedContract::Basic) => {
            #[test]
            fn httpupstream_session_test_stream_ingress_complete_no_rst() {
                assert_real_upstream_exchange(0, 0);
            }
        };
        (fn httpupstream_session_test_send_request_headers_available() => MappedContract::Basic) => {
            #[test]
            fn httpupstream_session_test_send_request_headers_available() {
                assert_real_upstream_prepared_headers();
            }
        };
        (fn httpdownstream_session_test_simple() => MappedContract::Basic) => {
            #[test]
            fn httpdownstream_session_test_simple() {
                assert_real_simple_downstream(0, None);
            }
        };
        (fn httpdownstream_session_test_head_response() => MappedContract::Basic) => {
            #[test]
            fn httpdownstream_session_test_head_response() {
                assert_real_head_has_no_data();
            }
        };
        (fn httpdownstream_session_test_two_requests() => MappedContract::Basic) => {
            #[test]
            fn httpdownstream_session_test_two_requests() {
                assert_real_two_downstream_stream_stats();
            }
        };
        (fn httpdownstream_session_test_stream_id_seq_no() => MappedContract::Basic) => {
            #[test]
            fn httpdownstream_session_test_stream_id_seq_no() {
                assert_real_downstream_stream_sequence_numbers();
            }
        };
        (fn httpdownstream_session_test_simple_post() => MappedContract::Body) => {
            #[test]
            fn httpdownstream_session_test_simple_post() {
                assert_real_simple_downstream(1_024, None);
            }
        };
        (fn httpdownstream_session_test_padded_request() => MappedContract::Padding) => {
            #[test]
            fn httpdownstream_session_test_padded_request() {
                assert_real_exact_padded_downstream_request();
            }
        };
        (fn httpupstream_session_test_reserve_request() => MappedContract::Admission) => {
            #[test]
            fn httpupstream_session_test_reserve_request() {
                peer_zero_max_concurrent_streams_closes_and_reopens_real_core_admission();
            }
        };
        (fn h2_qupstream_session_test_egress_stream_limit_exceeded() => MappedContract::Admission) => {
            #[test]
            fn h2_qupstream_session_test_egress_stream_limit_exceeded() {
                assert_real_peer_zero_closes_admission();
            }
        };
        (fn h2_upstream_session_test_ingress_stream_limit_exceeded() => MappedContract::Admission) => {
            #[test]
            fn h2_upstream_session_test_ingress_stream_limit_exceeded() {
                assert_real_upstream_push_limit_rejects_promised_stream();
            }
        };
        (fn httpupstream_session_test_supports_more_transactions() => MappedContract::Admission) => {
            #[test]
            fn httpupstream_session_test_supports_more_transactions() {
                assert_real_peer_zero_closes_admission();
                peer_zero_max_concurrent_streams_closes_and_reopens_real_core_admission();
            }
        };
        (fn h2_downstream_session_test_streams_full() => MappedContract::Admission) => {
            #[test]
            fn h2_downstream_session_test_streams_full() {
                assert_real_downstream_peer_stream_limit_transitions();
            }
        };
        (fn h2_downstream_session_test_ingress_stream_limit_exceeded() => MappedContract::Admission) => {
            #[test]
            fn h2_downstream_session_test_ingress_stream_limit_exceeded() {
                assert_real_downstream_ingress_stream_limit();
            }
        };
        (fn h2_downstream_session_test_basic_rfc9218_priority_urgency() => MappedContract::Priority) => {
            #[test]
            fn h2_downstream_session_test_basic_rfc9218_priority_urgency() {
                assert_real_priority_contract();
            }
        };
        (fn httpdownstream_session_test_simple_byte_events() => MappedContract::ByteEvents) => {
            #[test]
            fn httpdownstream_session_test_simple_byte_events() {
                assert_real_simple_byte_events();
            }
        };
        (fn httpdownstream_session_test_trailer_byte_events() => MappedContract::ByteEvents) => {
            #[test]
            fn httpdownstream_session_test_trailer_byte_events() {
                assert_real_trailer_byte_events();
            }
        };
        (fn httpdownstream_session_test_byte_event_errors() => MappedContract::ByteEvents) => {
            #[test]
            fn httpdownstream_session_test_byte_event_errors() {
                assert_real_dead_byte_event_callback_is_ignored();
            }
        };
        (fn httpdownstream_session_test_byte_events_cancel_on_write_error() => MappedContract::ByteEvents) => {
            #[test]
            fn httpdownstream_session_test_byte_events_cancel_on_write_error() {
                assert_real_write_error_cancels_byte_events();
            }
        };
        (fn h2_downstream_session_test_byte_events_cancel_on_protocol_error() => MappedContract::ByteEvents) => {
            #[test]
            fn h2_downstream_session_test_byte_events_cancel_on_protocol_error() {
                assert_real_protocol_error_cancels_byte_events();
            }
        };
        (fn h12_downstream_session_test_byte_event_timeout() => MappedContract::ByteEvents) => {
            #[test]
            fn h12_downstream_session_test_byte_event_timeout() {
                assert_real_byte_event_timeout();
            }
        };
        (fn h12_downstream_session_test_too_many_byte_events() => MappedContract::ByteEvents) => {
            #[test]
            fn h12_downstream_session_test_too_many_byte_events() {
                assert_real_too_many_byte_events();
            }
        };
        (fn h12_downstream_session_test_cancel_pending_byte_events() => MappedContract::ByteEvents) => {
            #[test]
            fn h12_downstream_session_test_cancel_pending_byte_events() {
                assert_real_pending_byte_events_cancel_on_run_drop();
            }
        };
        (fn h12_downstream_session_test_byte_event_observer_outlives_transport() => MappedContract::ByteEvents) => {
            #[test]
            fn h12_downstream_session_test_byte_event_observer_outlives_transport() {
                assert_real_byte_event_observer_outlives_transport();
            }
        };
        (fn h12_downstream_session_test_tx_ack_before_schedule_byte_events() => MappedContract::ByteEvents) => {
            #[test]
            fn h12_downstream_session_test_tx_ack_before_schedule_byte_events() {
                assert_real_immediate_tx_ack_events();
            }
        };
        (fn httpupstream_session_test_lifecycle_observer_test_nullptr() => MappedContract::Lifecycle) => {
            #[test]
            fn httpupstream_session_test_lifecycle_observer_test_nullptr() {
                assert_typed_lifecycle_observer_registration();
            }
        };
        (fn httpupstream_session_test_lifecycle_observer_tests() => MappedContract::Lifecycle) => {
            #[test]
            fn httpupstream_session_test_lifecycle_observer_tests() {
                assert_real_upstream_lifecycle_snapshot_semantics();
            }
        };
        (fn h2_qupstream_session_test_on_deactivate_connection_lifecycle() => MappedContract::Lifecycle) => {
            #[test]
            fn h2_qupstream_session_test_on_deactivate_connection_lifecycle() {
                assert_real_upstream_deactivation_waits_for_last_reservation();
            }
        };
        (fn httpdownstream_session_test_lifecycle_observer() => MappedContract::Lifecycle) => {
            #[test]
            fn httpdownstream_session_test_lifecycle_observer() {
                assert_real_h2_lifecycle_order();
            }
        };
        (fn httpdownstream_session_test_lifecycle_observer_remove_callback() => MappedContract::Lifecycle) => {
            #[test]
            fn httpdownstream_session_test_lifecycle_observer_remove_callback() {
                assert_real_downstream_lifecycle_removal_and_reentry();
            }
        };
        (fn httpdownstream_session_test_multi_lifecycle_observer() => MappedContract::Lifecycle) => {
            #[test]
            fn httpdownstream_session_test_multi_lifecycle_observer() {
                assert_real_multiple_downstream_lifecycle_observers();
            }
        };
        (fn httpdownstream_session_test_hold_context() => MappedContract::Lifecycle) => {
            #[test]
            fn httpdownstream_session_test_hold_context() {
                assert_real_handler_context_delays_destroy();
            }
        };
        (fn h2_qupstream_session_test_stop_reading_on_abort() => MappedContract::Cancellation) => {
            #[test]
            fn h2_qupstream_session_test_stop_reading_on_abort() {
                assert_real_upstream_peer_abort_stops_body();
            }
        };
        (fn httpupstream_session_test_session_cancelled_no_streams() => MappedContract::Cancellation) => {
            #[test]
            fn httpupstream_session_test_session_cancelled_no_streams() {
                assert_real_upstream_session_abort(false, SessionAbortMode::CancelRun);
            }
        };
        (fn httpupstream_session_test_session_cancelled_with_stream() => MappedContract::Cancellation) => {
            #[test]
            fn httpupstream_session_test_session_cancelled_with_stream() {
                assert_real_upstream_session_abort(true, SessionAbortMode::CancelRun);
            }
        };
        (fn httpupstream_session_test_session_drop_no_streams() => MappedContract::Cancellation) => {
            #[test]
            fn httpupstream_session_test_session_drop_no_streams() {
                assert_real_upstream_session_abort(false, SessionAbortMode::DropOwner);
            }
        };
        (fn httpupstream_session_test_session_drop_with_stream() => MappedContract::Cancellation) => {
            #[test]
            fn httpupstream_session_test_session_drop_with_stream() {
                assert_real_upstream_session_abort(true, SessionAbortMode::DropOwner);
            }
        };
        (fn httpupstream_session_test_cancel_before_sending_headers() => MappedContract::Cancellation) => {
            #[test]
            fn httpupstream_session_test_cancel_before_sending_headers() {
                assert_real_cancel_before_request_headers();
            }
        };
        (fn httpupstream_session_test_cancel_during_post_body() => MappedContract::Cancellation) => {
            #[test]
            fn httpupstream_session_test_cancel_during_post_body() {
                assert_real_cancel_during_request_body();
            }
        };
        (fn httpdownstream_session_test_handler_gracefully_terminate_ingress_fixed_response() => MappedContract::Cancellation) => {
            #[test]
            fn httpdownstream_session_test_handler_gracefully_terminate_ingress_fixed_response() {
                assert_real_downstream_graceful_ingress_termination(false);
            }
        };
        (fn httpdownstream_session_test_handler_gracefully_terminate_ingress_yield_error() => MappedContract::Cancellation) => {
            #[test]
            fn httpdownstream_session_test_handler_gracefully_terminate_ingress_yield_error() {
                assert_real_downstream_graceful_ingress_termination(true);
            }
        };
        (fn httpupstream_session_test_idle_timeout_no_streams() => MappedContract::Timeout) => {
            #[test]
            fn httpupstream_session_test_idle_timeout_no_streams() {
                assert_real_idle_timeout(TransportDirection::Upstream);
            }
        };
        (fn h12_upstream_session_test_write_timeout() => MappedContract::Timeout) => {
            #[test]
            fn h12_upstream_session_test_write_timeout() {
                assert_real_transport_write_timeout(TransportDirection::Upstream);
            }
        };
        (fn httpupstream_session_test_post_body_read_timeout() => MappedContract::Timeout) => {
            #[test]
            fn httpupstream_session_test_post_body_read_timeout() {
                assert_real_upstream_response_timeout(RequestTimeoutCase::PendingPost);
            }
        };
        (fn httpupstream_session_test_post_read_timeout() => MappedContract::Timeout) => {
            #[test]
            fn httpupstream_session_test_post_read_timeout() {
                assert_real_upstream_response_timeout(RequestTimeoutCase::CompletePost);
            }
        };
        (fn httpupstream_session_test_post_read_custom_timeout() => MappedContract::Timeout) => {
            #[test]
            fn httpupstream_session_test_post_read_custom_timeout() {
                assert_real_upstream_response_timeout(RequestTimeoutCase::CompletePostCustom);
            }
        };
        (fn httpupstream_session_test_get_read_timeout() => MappedContract::Timeout) => {
            #[test]
            fn httpupstream_session_test_get_read_timeout() {
                assert_real_upstream_response_timeout(RequestTimeoutCase::CompleteGet);
            }
        };
        (fn httpdownstream_session_test_idle_timeout_no_streams() => MappedContract::Timeout) => {
            #[test]
            fn httpdownstream_session_test_idle_timeout_no_streams() {
                assert_real_idle_timeout(TransportDirection::Downstream);
            }
        };
        (fn h12_downstream_session_test_write_timeout() => MappedContract::Timeout) => {
            #[test]
            fn h12_downstream_session_test_write_timeout() {
                assert_real_transport_write_timeout(TransportDirection::Downstream);
            }
        };
        (fn httpdownstream_session_test_read_timeout() => MappedContract::Timeout) => {
            #[test]
            fn httpdownstream_session_test_read_timeout() {
                assert_real_downstream_request_read_timeout();
            }
        };
        (fn h2_downstream_session_test_headers_read_timeout() => MappedContract::Timeout) => {
            #[test]
            fn h2_downstream_session_test_headers_read_timeout() {
                assert_real_partial_headers_use_connection_timeout();
            }
        };
        (fn h2_downstream_session_test_transaction_buffer_timeout() => MappedContract::Timeout) => {
            #[test]
            fn h2_downstream_session_test_transaction_buffer_timeout() {
                assert_real_transaction_buffer_timeout();
            }
        };
        (fn httpupstream_session_test_body_error() => MappedContract::Error) => {
            #[test]
            fn httpupstream_session_test_body_error() {
                assert_real_upstream_request_body_error();
            }
        };
        (fn h2_upstream_session_test_write_error_with_complete_streams() => MappedContract::Error) => {
            #[test]
            fn h2_upstream_session_test_write_error_with_complete_streams() {
                assert_real_upstream_write_error_after_complete_response();
            }
        };
        (fn h2_qupstream_session_test_codec_httperror() => MappedContract::Error) => {
            #[test]
            fn h2_qupstream_session_test_codec_httperror() {
                assert_real_upstream_codec_http_error();
            }
        };
        (fn h12_upstream_session_test_close_on_response_without_request() => MappedContract::Error) => {
            #[test]
            fn h12_upstream_session_test_close_on_response_without_request() {
                assert_real_unsolicited_upstream_response();
            }
        };
        (fn httpdownstream_session_test_test_eof() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_test_eof() {
                assert_real_clean_downstream_eof();
            }
        };
        (fn h12_downstream_session_test_body_error() => MappedContract::Error) => {
            #[test]
            fn h12_downstream_session_test_body_error() {
                assert_real_downstream_response_body_error();
            }
        };
        (fn httpdownstream_session_test_invalid_peer_certificate() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_invalid_peer_certificate() {
                assert_real_peer_identity_policy();
            }
        };
        (fn h2_qdownstream_session_test_stop_reading_default500_source() => MappedContract::Error) => {
            #[test]
            fn h2_qdownstream_session_test_stop_reading_default500_source() {
                assert_real_stop_reading_default_500();
            }
        };
        (fn httpdownstream_session_test_content_length_mismatch() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_content_length_mismatch() {
                assert_real_content_length_mismatch();
            }
        };
        (fn handler_runtime_error_returns_500() => MappedContract::Error) => {
            #[test]
            fn handler_runtime_error_returns_500() {
                assert_real_handler_runtime_error();
            }
        };
        (fn h2_qdownstream_session_test_state_machine_error() => MappedContract::Error) => {
            #[test]
            fn h2_qdownstream_session_test_state_machine_error() {
                assert_real_state_machine_error();
            }
        };
        (fn httpdownstream_session_test_eofwith_stream() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_eofwith_stream() {
                assert_real_eof_with_stream();
            }
        };
        (fn h12_downstream_session_test_web_socket_upgrade() => MappedContract::Error) => {
            #[test]
            fn h12_downstream_session_test_web_socket_upgrade() {
                assert_real_h2_websocket_upgrade();
            }
        };
        (fn h2_downstream_session_test_codec_error() => MappedContract::Error) => {
            #[test]
            fn h2_downstream_session_test_codec_error() {
                assert_real_codec_connection_error();
            }
        };
        (fn h2_downstream_session_test_codec_httperror() => MappedContract::Error) => {
            #[test]
            fn h2_downstream_session_test_codec_httperror() {
                assert_real_downstream_codec_http_error();
            }
        };
        (fn httpdownstream_session_test_handler_no_response() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_handler_no_response() {
                assert_real_handler_no_response();
            }
        };
        (fn httpdownstream_session_test_response_headers_error() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_response_headers_error() {
                assert_real_response_headers_error();
            }
        };
        (fn httpdownstream_session_test_ingress_error_custom_response() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_ingress_error_custom_response() {
                assert_real_custom_timeout_response(408);
            }
        };
        (fn httpdownstream_session_test_ingress_error_custom_response_mismatch() => MappedContract::Error) => {
            #[test]
            fn httpdownstream_session_test_ingress_error_custom_response_mismatch() {
                assert_real_custom_timeout_response(500);
            }
        };
    }

    mapped_session_case! { fn httpupstream_session_test_simple() => MappedContract::Basic }
    mapped_session_case! { fn httpupstream_session_test_simple_post() => MappedContract::Body }
    mapped_session_case! { fn httpupstream_session_test_large_post() => MappedContract::Body }
    mapped_session_case! { fn httpupstream_session_test_padding() => MappedContract::Padding }
    mapped_session_case! { fn h2_qupstream_session_test_receive_full_response_prior_to_request_eom() => MappedContract::Body }
    mapped_session_case! { fn h2_qupstream_session_test_stop_reading_on_abort() => MappedContract::Cancellation }
    mapped_session_case! { fn h2_upstream_session_test_rst_stream_after_eom() => MappedContract::Reset }
    mapped_session_case! { fn httpupstream_session_test_reserve_request() => MappedContract::Admission }
    mapped_session_case! { fn h2_qupstream_session_test_ingress_reset_stream() => MappedContract::Reset }
    mapped_session_case! { fn h2_qupstream_session_test_egress_stream_limit_exceeded() => MappedContract::Admission }
    mapped_session_case! { fn h2_upstream_session_test_ingress_stream_limit_exceeded() => MappedContract::Admission }
    mapped_session_case! { fn h2_qupstream_session_test_push() => MappedContract::Push }
    mapped_session_case! { fn h2_qupstream_session_test_push_egress_rst_stream() => MappedContract::Push }
    mapped_session_case! { fn h2_qupstream_session_test_push_ingress_rst_stream() => MappedContract::Push }
    mapped_session_case! { fn h2_upstream_session_test_push_parent_reset() => MappedContract::Push }
    mapped_session_case! { fn h2_qupstream_session_test_receive_goaway() => MappedContract::Goaway }
    mapped_session_case! { fn httpupstream_session_test_send_goaway() => MappedContract::Goaway }
    mapped_session_case! { fn httpupstream_session_test_lifecycle_observer_test_nullptr() => MappedContract::Lifecycle }
    mapped_session_case! { fn httpupstream_session_test_lifecycle_observer_tests() => MappedContract::Lifecycle }
    mapped_session_case! { fn httpupstream_session_test_body_error() => MappedContract::Error }
    mapped_session_case! { fn h2_qupstream_session_test_receive_goaway_with_open_stream() => MappedContract::Goaway }
    mapped_session_case! { fn h2_upstream_session_test_receive_goaway_with_error() => MappedContract::Goaway }
    mapped_session_case! { fn h2_upstream_session_test_receive_goaway_before_request_egress() => MappedContract::Goaway }
    mapped_session_case! { fn httpupstream_session_test_idle_timeout_no_streams() => MappedContract::Timeout }
    mapped_session_case! { fn h12_upstream_session_test_write_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn h2_upstream_session_test_write_error_with_complete_streams() => MappedContract::Error }
    mapped_session_case! { fn httpupstream_session_test_post_body_read_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn httpupstream_session_test_post_read_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn httpupstream_session_test_post_read_custom_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn httpupstream_session_test_get_read_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn h2_upstream_session_test_conn_flow_control_on_body_error() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_upstream_session_test_stream_flow_control_on_body_error() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_upstream_session_test_conn_flow_control_on_window_update_error() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_upstream_session_test_stream_flow_control_on_window_update_error() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_qupstream_session_test_codec_httperror() => MappedContract::Error }
    mapped_session_case! { fn h2_qupstream_session_test_on_deactivate_connection_lifecycle() => MappedContract::Lifecycle }
    mapped_session_case! { fn h2_upstream_session_test_push_promise_parse_error() => MappedContract::Push }
    mapped_session_case! { fn h2_upstream_session_test_reset_release_conn_flow_control() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_upstream_session_test_free_conn_flow_control_after_rst() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_upstream_session_test_egress_while_writes_blocked() => MappedContract::FlowControl }
    mapped_session_case! { fn httpupstream_session_test_session_cancelled_no_streams() => MappedContract::Cancellation }
    mapped_session_case! { fn httpupstream_session_test_session_cancelled_with_stream() => MappedContract::Cancellation }
    mapped_session_case! { fn httpupstream_session_test_session_drop_no_streams() => MappedContract::Cancellation }
    mapped_session_case! { fn httpupstream_session_test_session_drop_with_stream() => MappedContract::Cancellation }
    mapped_session_case! { fn httpupstream_session_test_stream_ingress_complete_no_rst() => MappedContract::Basic }
    mapped_session_case! { fn httpupstream_session_test_cancel_before_sending_headers() => MappedContract::Cancellation }
    mapped_session_case! { fn httpupstream_session_test_cancel_during_post_body() => MappedContract::Cancellation }
    mapped_session_case! { fn httpupstream_session_test_supports_more_transactions() => MappedContract::Admission }
    mapped_session_case! { fn httpupstream_session_test_send_request_headers_available() => MappedContract::Basic }
    mapped_session_case! { fn h12_upstream_session_test_close_on_response_without_request() => MappedContract::Error }

    #[test]
    fn httpupstream_session_test_early_data_handler_receives_settings() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (transport, state) =
                    FixtureTransport::new([FixtureRead::DataEof(server_response_bytes(0, 0))]);
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("GET request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted after peer SETTINGS")
                    .source;
                let consume = async move {
                    let mut headers = response.read_header_event().await.expect("response");
                    assert_eq!(headers.headers_mut().status_code().expect("status"), 200);
                    assert!(headers.eom());
                };
                let (run, ()) = tokio::join!(session.run(), consume);
                run.expect("SETTINGS session remains usable");
                let decoded = decode_client_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                assert_eq!(decoded.settings_acks, 1);
            })
        });
    }

    mapped_session_case! { fn httpdownstream_session_test_test_eof() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_simple() => MappedContract::Basic }
    mapped_session_case! { fn httpdownstream_session_test_padded_request() => MappedContract::Padding }
    mapped_session_case! { fn httpdownstream_session_test_head_response() => MappedContract::Basic }
    mapped_session_case! { fn httpdownstream_session_test_simple_byte_events() => MappedContract::ByteEvents }
    mapped_session_case! { fn httpdownstream_session_test_trailer_byte_events() => MappedContract::ByteEvents }
    mapped_session_case! { fn httpdownstream_session_test_byte_event_errors() => MappedContract::ByteEvents }
    mapped_session_case! { fn httpdownstream_session_test_byte_events_cancel_on_write_error() => MappedContract::ByteEvents }
    mapped_session_case! { fn h2_downstream_session_test_byte_events_cancel_on_protocol_error() => MappedContract::ByteEvents }
    mapped_session_case! { fn h12_downstream_session_test_byte_event_timeout() => MappedContract::ByteEvents }
    mapped_session_case! { fn h12_downstream_session_test_too_many_byte_events() => MappedContract::ByteEvents }
    mapped_session_case! { fn h12_downstream_session_test_cancel_pending_byte_events() => MappedContract::ByteEvents }
    mapped_session_case! { fn h12_downstream_session_test_byte_event_observer_outlives_transport() => MappedContract::ByteEvents }
    mapped_session_case! { fn h12_downstream_session_test_tx_ack_before_schedule_byte_events() => MappedContract::ByteEvents }
    mapped_session_case! { fn h12_downstream_session_test_body_error() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_two_requests() => MappedContract::Basic }
    mapped_session_case! { fn h2_downstream_session_test_reset_rate_limit() => MappedContract::Reset }
    mapped_session_case! { fn httpdownstream_session_test_stream_id_seq_no() => MappedContract::Basic }
    mapped_session_case! { fn httpdownstream_session_test_invalid_peer_certificate() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_lifecycle_observer() => MappedContract::Lifecycle }
    mapped_session_case! { fn httpdownstream_session_test_lifecycle_observer_remove_callback() => MappedContract::Lifecycle }
    mapped_session_case! { fn httpdownstream_session_test_multi_lifecycle_observer() => MappedContract::Lifecycle }
    mapped_session_case! { fn h2_qdownstream_session_test_reset_stream() => MappedContract::Reset }
    mapped_session_case! { fn h2_qdownstream_session_test_reset_egress_ingress_open() => MappedContract::Reset }
    mapped_session_case! { fn h2_qdownstream_session_test_reset_stream_awaiting_headers() => MappedContract::Reset }
    mapped_session_case! { fn httpdownstream_session_test_hold_context() => MappedContract::Lifecycle }
    mapped_session_case! { fn h2_downstream_session_test_reset_stream_no_error() => MappedContract::Reset }
    mapped_session_case! { fn h2_qdownstream_session_test_stop_reading_default500_source() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_content_length_mismatch() => MappedContract::Error }
    mapped_session_case! { fn handler_runtime_error_returns_500() => MappedContract::Error }
    mapped_session_case! { fn h2_qdownstream_session_test_state_machine_error() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_simple_post() => MappedContract::Body }
    mapped_session_case! { fn httpdownstream_session_test_handler_gracefully_terminate_ingress_fixed_response() => MappedContract::Cancellation }
    mapped_session_case! { fn httpdownstream_session_test_handler_gracefully_terminate_ingress_yield_error() => MappedContract::Cancellation }
    mapped_session_case! { fn h2_qdownstream_session_test_push() => MappedContract::Push }
    mapped_session_case! { fn h2_qdownstream_session_test_push_egress_rst_stream() => MappedContract::Push }
    mapped_session_case! { fn h2_qdownstream_session_test_push_ingress_rst_stream() => MappedContract::Push }
    mapped_session_case! { fn h2_qdownstream_session_test_egress_push_stream_limit_exceeded() => MappedContract::Push }
    mapped_session_case! { fn h2_downstream_session_test_egress_push_not_supported() => MappedContract::Push }
    mapped_session_case! { fn h2_downstream_session_test_ping() => MappedContract::Basic }
    mapped_session_case! { fn h2_downstream_session_test_streams_full() => MappedContract::Admission }
    mapped_session_case! { fn h2_downstream_session_test_egress_stream_flow_control() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_downstream_session_test_ingress_stream_flow_control_error() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_downstream_session_test_ingress_stream_limit_exceeded() => MappedContract::Admission }
    mapped_session_case! { fn h2_qdownstream_session_test_receive_goaway() => MappedContract::Goaway }
    mapped_session_case! { fn httpdownstream_session_test_send_goaway() => MappedContract::Goaway }
    mapped_session_case! { fn httpdownstream_session_test_close_when_idle_no_streams() => MappedContract::Goaway }
    mapped_session_case! { fn httpdownstream_session_test_close_when_idle_with_stream() => MappedContract::Goaway }
    mapped_session_case! { fn httpdownstream_session_test_eofwith_stream() => MappedContract::Error }
    mapped_session_case! { fn h2_qdownstream_session_test_receive_goaway_with_open_stream() => MappedContract::Goaway }
    mapped_session_case! { fn h2_downstream_session_test_receive_goaway_error_with_open_stream() => MappedContract::Goaway }
    mapped_session_case! { fn httpdownstream_session_test_idle_timeout_no_streams() => MappedContract::Timeout }
    mapped_session_case! { fn h12_downstream_session_test_web_socket_upgrade() => MappedContract::Error }
    mapped_session_case! { fn h12_downstream_session_test_write_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn httpdownstream_session_test_read_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn h2_downstream_session_test_headers_read_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn h2_qdownstream_session_test_stream_flow_control_timeout() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_downstream_session_test_conn_flow_control_timeout() => MappedContract::FlowControl }
    mapped_session_case! { fn h2_downstream_session_test_transaction_buffer_timeout() => MappedContract::Timeout }
    mapped_session_case! { fn h2_downstream_session_test_codec_error() => MappedContract::Error }
    mapped_session_case! { fn h2_downstream_session_test_codec_httperror() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_handler_no_response() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_response_headers_error() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_ingress_error_custom_response() => MappedContract::Error }
    mapped_session_case! { fn httpdownstream_session_test_ingress_error_custom_response_mismatch() => MappedContract::Error }
    mapped_session_case! { fn h2_qdownstream_session_test_stream_after_goaway() => MappedContract::Goaway }
    mapped_session_case! { fn h2_downstream_session_test_basic_rfc9218_priority_urgency() => MappedContract::Priority }

    #[test]
    fn callback_egress_error_trims_consumed_prefix_with_buffered_suffix() {
        run_on_worker(|context| {
            Box::pin(async move {
                let prefix_drops = Arc::new(AtomicUsize::new(0));
                let mut prefix = CONNECTION_PREFACE.to_vec();
                prefix.extend_from_slice(&[0, 0, 0, FrameType::SETTINGS.as_u8(), 0, 0, 0, 0, 0]);
                let prefix = Bytes::from_owner(DropTrackedBytes {
                    bytes: prefix,
                    drops: Arc::clone(&prefix_drops),
                });
                let suffix = Bytes::from_static(&[0]);
                let mut input = BufQueue::new(prefix.len() + suffix.len());
                input.push_bytes(prefix).expect("preface and SETTINGS fit");
                input.push_bytes(suffix).expect("buffered suffix fits");
                let (transport, state) = FixtureTransport::new([FixtureRead::SegmentedData(input)]);
                state.borrow_mut().expected_drops_before_reset =
                    Some((Arc::clone(&prefix_drops), 1));
                let transport: Rc<dyn UniplexTransport> = transport;
                let config = UniplexConfig {
                    buffer_limit: 34,
                    ingress_buffer_limit: 34,
                    maximum_read_size: 34,
                    read_allocation_size: 34,
                    maximum_write_size: 34,
                    ..UniplexConfig::default()
                };
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(Rc::new(FixtureHandler {
                        status: 200,
                        response_body: 0,
                        response_error: None,
                        invocations: Rc::new(Cell::new(0)),
                        request_body: Rc::new(Cell::new(0)),
                    })),
                    config,
                    Http2SessionConfig::default(),
                )
                .expect("downstream H2 session");

                assert!(session.run().await.is_err());
                assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
            })
        });
    }

    #[test]
    fn ingress_bytes_processed_error_trims_consumed_prefix_with_buffered_suffix() {
        run_on_worker(|context| {
            Box::pin(async move {
                let (headers, first_body, final_body) = server_response_then_late_body_parts();
                let mut fill = Vec::with_capacity(39_993);
                fill.extend_from_slice(&[0, 0, 0, FrameType::SETTINGS.as_u8(), 0, 0, 0, 0, 0]);
                for opaque in 0_u64..2_352 {
                    fill.extend_from_slice(&[0, 0, 8, FrameType::PING.as_u8(), 0, 0, 0, 0, 0]);
                    fill.extend_from_slice(&opaque.to_be_bytes());
                }
                assert_eq!(fill.len(), 39_993);

                let mut prefix_bytes = first_body;
                prefix_bytes.extend_from_slice(&final_body);

                let prefix_drops = Arc::new(AtomicUsize::new(0));
                let prefix = Bytes::from_owner(DropTrackedBytes {
                    bytes: prefix_bytes,
                    drops: Arc::clone(&prefix_drops),
                });
                let suffix = Bytes::from_static(&[0]);
                let mut input = BufQueue::new(prefix.len() + suffix.len());
                input.push_bytes(prefix).expect("PING and DATA prefix fits");
                input.push_bytes(suffix).expect("buffered suffix fits");

                let release_fill = Arc::new(tokio::sync::Notify::new());
                let release_body = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::Data(headers),
                    FixtureRead::WaitData(Arc::clone(&release_fill), fill),
                    FixtureRead::WaitSegmentedData(Arc::clone(&release_body), input),
                ]);
                state.borrow_mut().expected_drops_before_reset =
                    Some((Arc::clone(&prefix_drops), 1));
                let transport: Rc<dyn UniplexTransport> = transport;
                let config = UniplexConfig {
                    buffer_limit: 40_000,
                    ingress_buffer_limit: 40_000,
                    maximum_read_size: 40_000,
                    read_allocation_size: 40_000,
                    maximum_write_size: 40_000,
                    write_timeout: Duration::from_secs(1),
                    ..UniplexConfig::default()
                };
                let (session, handle) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    config,
                    Http2SessionConfig::default(),
                )
                .expect("upstream H2 session");
                let request = HttpFixedSource::make_get_request(b"/", None).expect("fixed request");
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut run = Box::pin(session.run());

                let mut header_read = Box::pin(response.read_header_event());
                tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::select! {
                        result = header_read.as_mut() => {
                            assert!(!result.expect("response headers").eom());
                        }
                        result = run.as_mut() => {
                            panic!("session ended before response headers: {result:?}")
                        }
                    }
                })
                .await
                .expect("response headers timed out");
                drop(header_read);
                state.borrow_mut().stall_writes = true;
                response.stop_reading(None);

                tokio::time::timeout(Duration::from_secs(1), async {
                    while state.borrow().stalled_writes_started == 0 {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended before the reset write stalled: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("reset write did not stall");
                release_fill.notify_one();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !state.borrow().reads.is_empty() {
                        tokio::select! {
                            result = run.as_mut() => {
                                panic!("session ended while filling egress: {result:?}")
                            }
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("PING replies did not fill egress");
                release_body.notify_one();

                let error = tokio::time::timeout(
                    config.write_timeout + Duration::from_secs(1),
                    run.as_mut(),
                )
                .await
                .expect("ingress credit failure timed out")
                .expect_err("connection WINDOW_UPDATE must exceed the full egress buffer");
                assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
                assert_eq!(
                    error.message(),
                    BufQueueError::LimitExceeded {
                        limit: 7,
                        current: 0,
                        incoming: 13,
                    }
                    .to_string()
                    .as_bytes(),
                    "the 13-byte connection WINDOW_UPDATE must exceed the seven remaining egress bytes",
                );
                assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
                assert_eq!(state.borrow().close, FixtureClose::Reset);
            })
        });
    }

    #[test]
    fn peer_zero_max_concurrent_streams_closes_and_reopens_real_core_admission() {
        run_on_worker(|context| {
            Box::pin(async move {
                let mut core = SessionCore::new(
                    TransportDirection::Upstream,
                    CodecProtocol::Http2,
                    context.handle().clone(),
                )
                .expect("H2 session core");
                assert!(core.set_max_concurrent_outgoing_streams(0));
                assert!(!core.supports_more_transactions());
                let refused = match core.reserve_request() {
                    Ok(_) => panic!("zero peer concurrency unexpectedly admitted a request"),
                    Err(error) => error,
                };
                assert_eq!(
                    refused.code(),
                    roxygen_core::error::HttpErrorCode::REFUSED_STREAM
                );
                assert!(core.set_max_concurrent_outgoing_streams(2));
                let reservation = core.reserve_request().expect("peer reopened admission");
                assert!(core.supports_more_transactions());
                drop(reservation);
                core.initiate_drain();
                core.finalize().await.expect("core finalization");
            })
        });
    }
}

#[test]
fn committed_request_headers_precede_local_drain() {
    h2::assert_committed_request_drain_order(true);
}

#[test]
fn local_drain_still_refuses_uncommitted_request() {
    h2::assert_committed_request_drain_order(false);
}

#[test]
fn committed_headers_precede_reentrant_stats_work() {
    h2::assert_committed_header_stats_reentry();
}

#[test]
fn committed_headers_register_events_before_lifecycle_reentry() {
    h2::assert_committed_header_lifecycle_reentry();
}

#[test]
fn committed_header_encoding_error_survives_cancel_callback_panic() {
    h2::assert_committed_header_panic_retirement(true);
}

#[test]
fn committed_header_callback_panic_fails_retained_session() {
    h2::assert_committed_header_panic_retirement(false);
}

#[test]
fn goaway_callback_preserves_control_order_in_both_directions() {
    h2::assert_goaway_callback_control_order();
}

#[test]
fn first_wire_maximum_goaway_retires_idle_upstream_without_eof() {
    h2::assert_first_wire_maximum_goaway_retires_idle_upstream();
}

#[test]
fn upstream_double_drain_retires_without_final_goaway() {
    h2::assert_upstream_double_drain_retires_without_final_goaway();
}

#[test]
fn incoming_wire_maximum_goaway_keeps_timed_final_drain() {
    h2::assert_incoming_wire_maximum_goaway_keeps_timed_final_drain();
}
