use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::collections::btree_map;
use std::error::Error;
use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::exchange::ExchangeCompletionError;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::fixed_source::get_error_response;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::handler::ResponseCounters;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::keep_alive::SessionHandleError;
use roxygen_runtime::task_group::TaskGroupError;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tokio::time;

use crate::external_host_tests::ManualHost;
use crate::session::IncomingRequest;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::session::OutgoingRequest;
use crate::session::ResetOrigin;
use crate::session::SessionCore;
use crate::session::SessionTaskError;
use crate::session::SessionTaskSet;
use crate::session::StreamAction;
use crate::session::small_map::Entry;
use crate::session::small_map::SmallMap;
use crate::session::small_map::SmallSet;
use crate::stream_state::EgressEvent;
use crate::stream_state::EgressTransfer;
use crate::stream_state::ResponseState;
use crate::stream_state::StreamCallbackEvent;
use crate::stream_state::StreamCallbacks;
use crate::uniplex::drop_egress_transfers_for_test;

struct EventRecorder {
    events: Rc<RefCell<Vec<LifecycleEvent>>>,
}

impl LifecycleObserver for EventRecorder {
    fn on_event(&self, event: LifecycleEvent) {
        self.events.borrow_mut().push(event);
    }
}

struct PanickingEventObserver {
    panic_on: LifecycleEvent,
    events: Rc<RefCell<Vec<LifecycleEvent>>>,
}

struct PanickingAttachObserver {
    attached: Rc<Cell<usize>>,
    events: Rc<Cell<usize>>,
}

impl LifecycleObserver for PanickingAttachObserver {
    fn on_attached(&self) {
        self.attached.set(self.attached.get() + 1);
        panic!("scripted observer attachment panic");
    }

    fn on_event(&self, _event: LifecycleEvent) {
        self.events.set(self.events.get() + 1);
    }
}

impl LifecycleObserver for PanickingEventObserver {
    fn on_event(&self, event: LifecycleEvent) {
        self.events.borrow_mut().push(event);
        if event == self.panic_on {
            panic!("scripted lifecycle observer panic");
        }
    }
}

struct ScriptedSource {
    headers: VecDeque<HttpHeaderEvent>,
    bodies: VecDeque<HttpBodyEvent>,
}

impl ScriptedSource {
    fn with_body(header: HttpHeaderEvent, body: HttpBodyEvent) -> Self {
        Self {
            headers: VecDeque::from([header]),
            bodies: VecDeque::from([body]),
        }
    }

    fn with_headers_and_body(
        headers: impl IntoIterator<Item = HttpHeaderEvent>,
        body: HttpBodyEvent,
    ) -> Self {
        Self {
            headers: headers.into_iter().collect(),
            bodies: VecDeque::from([body]),
        }
    }
}

impl HttpSource for ScriptedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let result = self.headers.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"header script exhausted")
        });
        Box::pin(std::future::ready(result))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let result = self.bodies.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"body script exhausted")
        });
        Box::pin(std::future::ready(result))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

#[derive(Default)]
struct StopLog {
    stops: Cell<usize>,
    drops: Cell<usize>,
}

struct NeverSource {
    log: Rc<StopLog>,
}

struct PanickingStopSource {
    log: Rc<StopLog>,
}

impl HttpSource for NeverSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.log.stops.set(self.log.stops.get() + 1);
    }
}

impl Drop for NeverSource {
    fn drop(&mut self) {
        self.log.drops.set(self.log.drops.get() + 1);
    }
}

impl HttpSource for PanickingStopSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        let stops = self.log.stops.get();
        self.log.stops.set(stops + 1);
        if stops == 0 {
            panic!("scripted egress stop panic");
        }
    }
}

impl Drop for PanickingStopSource {
    fn drop(&mut self) {
        self.log.drops.set(self.log.drops.get() + 1);
    }
}

struct ErrorSource {
    code: HttpErrorCode,
}

impl HttpSource for ErrorSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            self.code,
            b"scripted egress failure",
        ))))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            self.code,
            b"scripted egress failure",
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct ContextResponseSource {
    inner: HttpFixedSource,
    _context: Option<HandlerSessionContext>,
    drop_log: Option<Rc<RefCell<Vec<&'static str>>>>,
}

impl HttpSource for ContextResponseSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.inner.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.inner.stop_reading(error);
    }
}

impl Drop for ContextResponseSource {
    fn drop(&mut self) {
        if let Some(log) = &self.drop_log {
            log.borrow_mut().push("handler-response-drop");
        }
    }
}

struct ScriptedHandler {
    status: u16,
    retain_context: bool,
    requests: Rc<RefCell<Vec<(StreamId, usize)>>>,
    drop_log: Option<Rc<RefCell<Vec<&'static str>>>>,
}

struct IngressTimeoutHandler {
    status: u16,
    body_size: usize,
}

struct PropagatingIngressTimeoutHandler;

impl HttpHandler for PropagatingIngressTimeoutHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            request_source.read_header_event().await?;
            loop {
                match request_source.read_body_event(u32::MAX).await {
                    Ok(event) => match event.into_kind() {
                        roxygen_core::events::HttpBodyEventKind::Suspend(resume) => {
                            let _ = resume.await;
                        }
                        _ => panic!("incomplete ingress body produced data before timeout"),
                    },
                    Err(error) => return Err(error),
                }
            }
        })
    }
}

impl HttpHandler for IngressTimeoutHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let status = self.status;
        let body_size = self.body_size;
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            assert!(!headers.eom());
            let error = loop {
                match request_source.read_body_event(u32::MAX).await {
                    Ok(event) => match event.into_kind() {
                        roxygen_core::events::HttpBodyEventKind::Suspend(resume) => {
                            let _ = resume.await;
                        }
                        _ => panic!("incomplete ingress body produced data before timeout"),
                    },
                    Err(error) => break error,
                }
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(status, (body_size != 0).then(body_100))
                    .unwrap(),
            )))
        })
    }
}

struct ProposingHandler {
    inner: HttpHandlerRef,
    counters: Arc<ResponseCounters>,
    status: u16,
}

impl HttpHandler for ProposingHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        assert!(
            context
                .response_proposer()
                .unwrap()
                .propose(Arc::clone(&self.counters), self.status)
        );
        self.inner.handle_request(context, source)
    }
}

fn proposing(
    inner: HttpHandlerRef,
    counters: &Arc<ResponseCounters>,
    status: u16,
) -> HttpHandlerRef {
    Rc::new(ProposingHandler {
        inner,
        counters: Arc::clone(counters),
        status,
    })
}

struct ControlledHandler {
    ready: Option<Rc<CancellableBaton>>,
    release: Rc<CancellableBaton>,
    panic_after_release: bool,
}

impl HttpHandler for ControlledHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let release = Rc::clone(&self.release);
        let ready = self.ready.clone();
        let panic_after_release = self.panic_after_release;
        Box::pin(async move {
            request_source.read_header_event().await?;
            if let Some(ready) = ready {
                ready.signal();
            }
            assert_eq!(
                release.wait(CancellationToken::never()).await,
                WaitStatus::Signalled
            );
            if panic_after_release {
                panic!("scripted handler panic");
            }
            drop(context);
            Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
                response_message(200),
                Some(body_100()),
            ))))
        })
    }
}

struct PanickingHandler;

impl HttpHandler for PanickingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move { panic!("scripted handler panic") })
    }
}

struct ConstructionPanickingHandler;

impl HttpHandler for ConstructionPanickingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        panic!("scripted handler construction panic")
    }
}

struct DropPanickingHandlerFuture;

impl Future for DropPanickingHandlerFuture {
    type Output = Result<HttpSourceHolder, HttpError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
            response_message(200),
            Some(body_100()),
        )))))
    }
}

impl Drop for DropPanickingHandlerFuture {
    fn drop(&mut self) {
        panic!("scripted handler future drop panic");
    }
}

struct DropPanickingHandler;

impl HttpHandler for DropPanickingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(DropPanickingHandlerFuture)
    }
}

struct DestructorPanickingHandler;

impl HttpHandler for DestructorPanickingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            HttpFixedSource::new(response_message(200), Some(body_100())),
        )))))
    }
}

impl Drop for DestructorPanickingHandler {
    fn drop(&mut self) {
        panic!("scripted handler destructor panic");
    }
}

impl HttpHandler for ScriptedHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let status = self.status;
        let retain_context = self.retain_context;
        let requests = Rc::clone(&self.requests);
        let drop_log = self.drop_log.clone();
        Box::pin(async move {
            let id = request_source.get_stream_id().ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, b"missing stream ID")
            })?;
            let headers = request_source.read_header_event().await?;
            let mut body_bytes = 0;
            if !headers.eom() {
                loop {
                    let body = request_source.read_body_event(65_535).await?;
                    body_bytes += body.as_body().map_or(0, BufQueue::chain_length);
                    if body.eom() {
                        break;
                    }
                }
            }
            requests.borrow_mut().push((id, body_bytes));
            let retained_context = if retain_context {
                Some(context)
            } else {
                drop(context);
                None
            };
            Ok(HttpSourceHolder::new(Box::new(ContextResponseSource {
                inner: HttpFixedSource::new(response_message(status), Some(body_100())),
                _context: retained_context,
                drop_log,
            })))
        })
    }
}

struct TaskDrop {
    label: &'static str,
    log: Rc<RefCell<Vec<&'static str>>>,
    owner: ThreadId,
}

impl Drop for TaskDrop {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.log.borrow_mut().push(self.label);
    }
}

struct DestroyRecorder {
    log: Rc<RefCell<Vec<&'static str>>>,
    owner: ThreadId,
}

struct WorkerDestroyRecorder {
    worker: WorkerHandle,
    events: Rc<RefCell<Vec<&'static str>>>,
}

impl LifecycleObserver for WorkerDestroyRecorder {
    fn on_event(&self, event: LifecycleEvent) {
        if event == LifecycleEvent::Destroy {
            assert_eq!(std::thread::current().id(), self.worker.owner_thread());
            assert_eq!(self.worker.state(), WorkerState::Open);
            self.events.borrow_mut().push("destroy-on-worker");
        }
    }
}

impl LifecycleObserver for DestroyRecorder {
    fn on_event(&self, event: LifecycleEvent) {
        if event == LifecycleEvent::Destroy {
            assert_eq!(std::thread::current().id(), self.owner);
            self.log.borrow_mut().push("session-destroy");
        }
    }
}

fn request_message(content_length: Option<&[u8]>) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Get).unwrap();
    message.set_url(b"/", false).unwrap();
    message.set_http_version(1, 1);
    if let Some(content_length) = content_length {
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, content_length);
    }
    message
}

fn response_message(status: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    message.set_http_version(1, 1);
    message
}

fn header(message: HttpMessage, eom: bool) -> HttpHeaderEvent {
    HttpHeaderEvent::new(message, eom).unwrap()
}

fn body_100() -> BufQueue {
    get_error_response(200, [b'x'; 100])
        .unwrap()
        .body()
        .clone_shared()
}

fn fixed_request() -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_get_request(b"/", None).unwrap(),
    ))
}

fn handler(status: u16) -> (HttpHandlerRef, Rc<RefCell<Vec<(StreamId, usize)>>>) {
    let requests = Rc::new(RefCell::new(Vec::new()));
    (
        Rc::new(ScriptedHandler {
            status,
            retain_context: false,
            requests: Rc::clone(&requests),
            drop_log: None,
        }),
        requests,
    )
}

fn run_local(test: impl Future<Output = ()> + 'static) {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
        .block_on(LocalSet::new().run_until(test));
}

fn new_session(
    direction: TransportDirection,
    protocol: CodecProtocol,
) -> (SessionCore, WorkerDriver) {
    let (worker, driver) = WorkerDriver::new(64).expect("worker identity");
    (
        SessionCore::new(direction, protocol, worker).expect("session bound to worker"),
        driver,
    )
}

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn draining_dropped_downstream_source_retains_exchange_until_eom_or_abort() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        for terminal in ["eom", "peer_reset", "prepared_abort"] {
            run_local(async move {
                let (mut session, _driver) = new_session(TransportDirection::Downstream, protocol);
                assert!(session.set_drain_request_body_on_drop(true));
                let events = Rc::new(RefCell::new(Vec::new()));
                session.add_lifecycle_observer(Rc::new(EventRecorder {
                    events: Rc::clone(&events),
                }));
                let stream = StreamId::new(1);
                let mut incoming = session.begin_incoming_stream(stream).unwrap();
                let ingress = incoming.source_mut().ingress_completion().unwrap();
                let mut exchange = incoming.completion().wait();
                assert!(
                    session
                        .ingress_headers(stream, request_message(Some(b"3")), false)
                        .unwrap()
                );
                incoming.source_mut().read_header_event().await.unwrap();
                drop(incoming);
                session.process_pending_events().unwrap();
                let (_, response) = session.response_headers(stream, true, true).unwrap();
                assert!(!response.detached);
                assert_eq!(response.action, StreamAction::None);
                assert_eq!(session.num_transactions(), 1);
                assert!(ingress.snapshot().is_none());
                assert!(poll_once(exchange.as_mut()).is_pending());

                match terminal {
                    "eom" => {
                        session
                            .ingress_body(
                                stream,
                                BufQueue::copy_from_slice(b"abc", 3).unwrap(),
                                0,
                                false,
                            )
                            .unwrap();
                        assert!(session.ingress_eom(stream).unwrap());
                    }
                    "peer_reset" => {
                        session.peer_reset(stream, HttpErrorCode::CANCEL).unwrap();
                    }
                    _ => {
                        session
                            .abort_stream_without_wire(
                                stream,
                                HttpError::new(HttpErrorCode::CANCEL),
                            )
                            .unwrap();
                    }
                }
                session.process_pending_events().unwrap();
                assert_eq!(session.num_transactions(), 0, "{protocol:?} {terminal}");
                assert!(matches!(poll_once(exchange.as_mut()), Poll::Ready(Ok(()))));
                if terminal == "eom" {
                    assert!(ingress.snapshot().unwrap().is_ok());
                    assert_eq!(ingress.body_bytes_received(), 3);
                } else {
                    assert_eq!(
                        ingress.snapshot().unwrap().unwrap_err().code(),
                        HttpErrorCode::CANCEL
                    );
                }
                assert_eq!(
                    events
                        .borrow()
                        .iter()
                        .filter(|event| {
                            **event
                                == LifecycleEvent::TransactionDetached {
                                    stream: Some(stream),
                                }
                        })
                        .count(),
                    1
                );
                session.finalize().await.unwrap();
            });
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("actual session I/O cause")]
struct SessionIoCause(Arc<()>);

fn session_io_failure(identity: &Arc<()>) -> HttpError {
    HttpError::with_message(
        HttpErrorCode::TRANSPORT_READ_ERROR,
        b"actual session I/O cause",
    )
    .with_cause(Arc::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        SessionIoCause(Arc::clone(identity)),
    )))
}

fn assert_session_cause(error: &HttpError, identity: &Arc<()>) {
    let cause = error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.kind(), std::io::ErrorKind::TimedOut);
    assert!(Arc::ptr_eq(
        &cause
            .get_ref()
            .unwrap()
            .downcast_ref::<SessionIoCause>()
            .unwrap()
            .0,
        identity,
    ));
}

fn session_cause_case(after_head: bool) {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        for boundary in 0..3 {
            run_local(async move {
                let (mut session, _driver) = new_session(TransportDirection::Upstream, protocol);
                let id = StreamId::new(1);
                let mut outgoing = prepare_bodyless_request(&mut session, id, HttpMethod::Get)
                    .await
                    .unwrap();
                let egress = session.egress_finished(id);
                let mut completion = outgoing.completion.wait();
                let initially_pending = poll_once(completion.as_mut()).is_pending();
                let prefix = if after_head {
                    let headers_accepted =
                        session.ingress_headers(id, response_message(200), false);
                    let header = outgoing.response_source.read_header_event().await;
                    let body_accepted = session.ingress_body(id, body_100(), 0, false);
                    let body = outgoing.response_source.read_body_event(100).await;
                    Some((headers_accepted, header, body_accepted, body))
                } else {
                    None
                };
                let identity = Arc::new(());
                let original = session_io_failure(&identity);
                let details = match boundary {
                    0 => {
                        session.fail_connection(original.clone());
                        b"connection error".as_slice()
                    }
                    1 => {
                        session
                            .reset_stream_state(id, original.clone(), ResetOrigin::Ingress)
                            .unwrap();
                        b"actual session I/O cause".as_slice()
                    }
                    _ => {
                        session
                            .abort_stream_without_wire(id, original.clone())
                            .unwrap();
                        b"session stopped servicing the stream".as_slice()
                    }
                };
                let terminal = session.stream(id).unwrap().terminal_error().cloned();
                let error = if after_head {
                    outgoing.response_source.read_body_event(100).await.err()
                } else {
                    outgoing.response_source.read_header_event().await.err()
                };
                outgoing.response_source.stop_reading(None);
                drop(outgoing);
                let updates = session.process_pending_events();
                let counters = (
                    session.num_transactions(),
                    session.num_outgoing_streams(),
                    session.pending_exchange_count(),
                );
                let receipt = poll_once(completion.as_mut());
                let finalized = session.finalize().await;
                finalized.unwrap();
                assert!(initially_pending);
                assert!(!egress.unwrap().detached);
                if let Some((headers_accepted, header, body_accepted, body)) = prefix {
                    assert!(headers_accepted.unwrap());
                    assert!(!header.unwrap().eom());
                    body_accepted.unwrap();
                    let body = body.unwrap();
                    assert!(!body.eom());
                    let roxygen_core::events::HttpBodyEventKind::Body(bytes) = body.into_kind()
                    else {
                        panic!("actual prefix body")
                    };
                    assert_eq!(
                        bytes
                            .into_bytes_segments()
                            .into_iter()
                            .flat_map(|segment| segment.to_vec())
                            .collect::<Vec<_>>(),
                        vec![b'x'; 100],
                    );
                }
                let error = error.expect("selected source error");
                assert_session_cause(&error, &identity);
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
                let mut diagnostic =
                    b"HTTP source aborted err=TRANSPORT_READ_ERROR, details=".to_vec();
                diagnostic.extend_from_slice(details);
                assert_eq!(error.message(), diagnostic);
                let terminal = terminal.unwrap();
                assert_session_cause(&terminal, &identity);
                assert!(std::ptr::eq(
                    original.source().unwrap(),
                    error.source().unwrap()
                ));
                assert!(
                    updates
                        .unwrap()
                        .iter()
                        .any(|update| update.stream == id && update.detached)
                );
                assert_eq!(counters, (0, 0, 0));
                assert_eq!(receipt, Poll::Ready(Ok(())));
            });
        }
    }
}

#[test]
fn connection_failure_retains_cause_before_headers() {
    session_cause_case(false);
}

#[test]
fn connection_failure_retains_cause_after_body_prefix() {
    session_cause_case(true);
}

#[test]
fn connection_cause_does_not_replace_selected_stream_error() {
    run_local(async {
        let (mut session, _driver) =
            new_session(TransportDirection::Upstream, CodecProtocol::Http2);
        let first_id = StreamId::new(1);
        let second_id = StreamId::new(3);
        let mut first = prepare_bodyless_request(&mut session, first_id, HttpMethod::Get)
            .await
            .unwrap();
        let mut second = prepare_bodyless_request(&mut session, second_id, HttpMethod::Get)
            .await
            .unwrap();
        session.egress_finished(first_id).unwrap();
        session.egress_finished(second_id).unwrap();
        let mut first_completion = first.completion.wait();
        let mut second_completion = second.completion.wait();
        session.peer_reset(first_id, HttpErrorCode::CANCEL).unwrap();
        let selected = first.response_source.read_header_event().await.err();
        session
            .ingress_headers(second_id, response_message(200), false)
            .unwrap();
        let header = second.response_source.read_header_event().await;
        session
            .ingress_body(second_id, body_100(), 0, false)
            .unwrap();
        let prefix = second.response_source.read_body_event(100).await;
        let identity = Arc::new(());
        session.fail_connection(session_io_failure(&identity));
        let first_terminal = session.stream(first_id).unwrap().terminal_error().cloned();
        let second_error = second.response_source.read_body_event(100).await.err();
        first.response_source.stop_reading(None);
        second.response_source.stop_reading(None);
        drop(first);
        drop(second);
        let updates = session.process_pending_events();
        let counters = (
            session.num_transactions(),
            session.num_outgoing_streams(),
            session.pending_exchange_count(),
        );
        let first_receipt = poll_once(first_completion.as_mut());
        let second_receipt = poll_once(second_completion.as_mut());
        let finalized = session.finalize().await;
        finalized.unwrap();
        let selected = selected.expect("peer reset selected before connection failure");
        assert_eq!(selected.code(), HttpErrorCode::CANCEL);
        assert!(selected.source().is_none());
        let terminal = first_terminal.unwrap();
        assert_eq!(terminal.code(), HttpErrorCode::CANCEL);
        assert!(terminal.source().is_none());
        assert!(!header.unwrap().eom());
        let prefix = prefix.unwrap();
        assert!(!prefix.eom());
        assert!(
            matches!(prefix.into_kind(), roxygen_core::events::HttpBodyEventKind::Body(bytes) if bytes.chain_length() == 100)
        );
        assert_session_cause(&second_error.unwrap(), &identity);
        assert_eq!(
            updates
                .unwrap()
                .iter()
                .filter(|update| update.detached)
                .count(),
            2
        );
        assert_eq!(counters, (0, 0, 0));
        assert_eq!(first_receipt, Poll::Ready(Ok(())));
        assert_eq!(second_receipt, Poll::Ready(Ok(())));
    });
}

#[derive(Default)]
struct RetainedOperationLog {
    ready: Cell<bool>,
    created: Cell<usize>,
    polls: Cell<usize>,
    retired: Cell<usize>,
    cancelled: Cell<usize>,
}

impl RetainedOperationLog {
    fn new(ready: bool) -> Rc<Self> {
        Rc::new(Self {
            ready: Cell::new(ready),
            ..Self::default()
        })
    }
}

struct RetainedOperation<T> {
    output: Option<Box<T>>,
    log: Rc<RetainedOperationLog>,
    source_cancelled: Rc<Cell<bool>>,
}

impl<T> RetainedOperation<T> {
    fn new(output: T, log: Rc<RetainedOperationLog>, source_cancelled: Rc<Cell<bool>>) -> Self {
        log.created.set(log.created.get() + 1);
        Self {
            output: Some(Box::new(output)),
            log,
            source_cancelled,
        }
    }
}

impl<T> Future for RetainedOperation<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<T> {
        self.log.polls.set(self.log.polls.get() + 1);
        if self.log.ready.get() {
            Poll::Ready(*self.output.take().expect("one completed source operation"))
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for RetainedOperation<T> {
    fn drop(&mut self) {
        self.log.retired.set(self.log.retired.get() + 1);
        if self.output.is_some() {
            self.log.cancelled.set(self.log.cancelled.get() + 1);
            self.source_cancelled.set(true);
        }
    }
}

struct RetainedStep<T> {
    output: T,
    log: Rc<RetainedOperationLog>,
}

struct RetainedSource {
    headers: VecDeque<RetainedStep<HttpHeaderEvent>>,
    bodies: VecDeque<RetainedStep<HttpBodyEvent>>,
    cancellation: Rc<Cell<bool>>,
    lifetime: Rc<StopLog>,
}

impl RetainedSource {
    fn new(
        headers: impl IntoIterator<Item = RetainedStep<HttpHeaderEvent>>,
        bodies: impl IntoIterator<Item = RetainedStep<HttpBodyEvent>>,
    ) -> Self {
        Self {
            headers: headers.into_iter().collect(),
            bodies: bodies.into_iter().collect(),
            cancellation: Rc::new(Cell::new(false)),
            lifetime: Rc::new(StopLog::default()),
        }
    }

    fn read<T: 'static>(
        steps: &mut VecDeque<RetainedStep<T>>,
        cancellation: Rc<Cell<bool>>,
    ) -> LocalSourceFuture<Result<T, HttpError>> {
        if cancellation.get() {
            return Box::pin(std::future::ready(Err(HttpError::new(
                HttpErrorCode::CORO_CANCELLED,
            ))));
        }
        match steps.pop_front() {
            Some(step) => Box::pin(RetainedOperation::new(
                Ok(step.output),
                step.log,
                cancellation,
            )),
            None => Box::pin(std::future::ready(Err(HttpError::new(
                HttpErrorCode::INVALID_STATE_TRANSITION,
            )))),
        }
    }
}

impl HttpSource for RetainedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Self::read(&mut self.headers, Rc::clone(&self.cancellation))
    }
    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Self::read(&mut self.bodies, Rc::clone(&self.cancellation))
    }
    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.lifetime.stops.set(self.lifetime.stops.get() + 1);
        self.cancellation.set(true);
    }
}

impl Drop for RetainedSource {
    fn drop(&mut self) {
        self.lifetime.drops.set(self.lifetime.drops.get() + 1);
    }
}

struct RetainedPanic(Arc<()>);

struct PanickingRetainedResume {
    operation: RetainedOperation<roxygen_core::events::ResumeStatus>,
    armed: Rc<Cell<bool>>,
    marker: Arc<()>,
}

impl Future for PanickingRetainedResume {
    type Output = roxygen_core::events::ResumeStatus;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.operation).poll(context)
    }
}

impl Drop for PanickingRetainedResume {
    fn drop(&mut self) {
        if self.armed.get() {
            std::panic::panic_any(RetainedPanic(Arc::clone(&self.marker)));
        }
    }
}

struct PanickingRetainedSource {
    source: RetainedSource,
    armed: Rc<Cell<bool>>,
    stop_marker: Option<Arc<()>>,
}

impl HttpSource for PanickingRetainedSource {
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
        if !self.armed.get() {
            return;
        }
        if let Some(marker) = self.stop_marker.take() {
            std::panic::panic_any(RetainedPanic(marker));
        }
    }
}

fn retention_case(test: impl FnOnce(&mut SessionCore, StreamId) + 'static) {
    run_local(async move {
        let (mut session, driver) =
            new_session(TransportDirection::Downstream, CodecProtocol::Http2);
        let id = StreamId::new(1);
        let mut incoming = session.begin_incoming_stream(id).unwrap();
        let result = catch_unwind(AssertUnwindSafe(|| {
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            let mut ingress = incoming.source_mut().read_header_event();
            assert!(matches!(poll_once(ingress.as_mut()), Poll::Ready(Ok(event)) if event.eom()));
            drop(ingress);
            drop(incoming);
            test(&mut session, id);
        }));
        let mut reset_error = None;
        for remaining in session.stream_ids() {
            if let Err(error) =
                session.abort_stream_without_wire(remaining, HttpError::new(HttpErrorCode::CANCEL))
            {
                reset_error.get_or_insert(error);
            }
        }
        let callback_result = session.process_pending_events().map(drop);
        let finalized = session.finalize().await;
        drop(driver);
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
        if let Some(error) = reset_error {
            panic!("remaining stream reset failed: {error}");
        }
        callback_result.unwrap();
        finalized.unwrap();
    });
}

fn retained_poll(
    transfer: &mut EgressTransfer,
    credit: u32,
    direct: bool,
) -> Poll<Result<Option<EgressEvent>, HttpError>> {
    if direct {
        transfer.poll_next_event_with_limit(&mut Context::from_waker(Waker::noop()), credit)
    } else {
        let mut outer = Box::pin(transfer.next_event_with_limit(credit));
        poll_once(outer.as_mut())
    }
}

fn retained_pending(transfer: &mut EgressTransfer, credit: u32, log: &RetainedOperationLog) {
    retained_pending_mode(transfer, credit, log, false);
}

fn retained_headers(transfer: &mut EgressTransfer, status: u16, eom: bool) {
    retained_headers_mode(transfer, status, eom, false);
}

fn retained_pending_mode(
    transfer: &mut EgressTransfer,
    credit: u32,
    log: &RetainedOperationLog,
    direct: bool,
) {
    for _attempt in 0..3 {
        assert!(retained_poll(transfer, credit, direct).is_pending());
        assert_eq!(log.created.get(), 1);
        assert_eq!(log.retired.get(), 0);
        assert_eq!(log.cancelled.get(), 0);
    }
}

fn retained_ready_mode(
    transfer: &mut EgressTransfer,
    credit: u32,
    direct: bool,
) -> Option<EgressEvent> {
    match retained_poll(transfer, credit, direct) {
        Poll::Ready(result) => result.expect("released egress read succeeds"),
        Poll::Pending => panic!("released egress operation must not wait for another source read"),
    }
}

fn retained_headers_mode(transfer: &mut EgressTransfer, status: u16, eom: bool, direct: bool) {
    let Some(EgressEvent::Headers(mut event)) = retained_ready_mode(transfer, 0, direct) else {
        panic!("ordered header event")
    };
    assert_eq!(event.headers_mut().status_code().unwrap(), status);
    assert_eq!(event.eom(), eom);
}

#[derive(Default)]
struct RetainedWakes(AtomicUsize);

impl Wake for RetainedWakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn body_only_progress_preserves_headers_pending_child_and_terminal_serialization() {
    retention_case(|session, id| {
        let headers = RetainedOperationLog::new(true);
        let empty = RetainedOperationLog::new(true);
        let body = RetainedOperationLog::new(false);
        let mut steps = Vec::new();
        for _ in 0..129 {
            steps.push(RetainedStep {
                output: HttpBodyEvent::body(BufQueue::new(0), false),
                log: Rc::clone(&empty),
            });
        }
        steps.push(RetainedStep {
            output: HttpBodyEvent::body(BufQueue::copy_from_slice(b"x", 1).unwrap(), true),
            log: Rc::clone(&body),
        });
        let source = RetainedSource::new(
            [RetainedStep {
                output: header(response_message(200), false),
                log: Rc::clone(&headers),
            }],
            steps,
        );
        let lifetime = Rc::clone(&source.lifetime);
        let mut transfer = session
            .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
            .unwrap();
        let wakes = Arc::new(RetainedWakes::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        for _ in 0..2 {
            assert!(matches!(
                transfer.poll_body_source(&mut context),
                Poll::Ready(Ok(None))
            ));
            assert_eq!(headers.created.get(), 0);
        }
        retained_headers(&mut transfer, 200, false);
        for turn in 1..=2 {
            assert!(transfer.poll_body_source(&mut context).is_pending());
            assert_eq!(empty.created.get(), turn * 64);
            assert_eq!(empty.retired.get(), turn * 64);
            assert_eq!(wakes.0.load(Ordering::SeqCst), turn);
        }
        for _ in 0..3 {
            assert!(transfer.poll_body_source(&mut context).is_pending());
            assert_eq!(
                (body.created.get(), body.retired.get(), body.cancelled.get()),
                (1, 0, 0)
            );
        }
        body.ready.set(true);
        assert!(matches!(
            transfer.poll_body_source(&mut context),
            Poll::Ready(Ok(None))
        ));
        assert_eq!(
            (body.created.get(), body.retired.get(), body.cancelled.get()),
            (1, 1, 0)
        );
        assert_eq!((lifetime.stops.get(), lifetime.drops.get()), (0, 1));
        assert!(transfer.has_buffered_events());
        assert!(!transfer.is_complete());
        assert!(!transfer.can_read_source());
        assert_eq!(transfer.buffer_timeout_deadline(), None);
        let Poll::Ready(Ok(Some(EgressEvent::Body(event)))) =
            transfer.poll_next_event_with_credit(&mut context, Some(1))
        else {
            panic!("only serialization consumes the admitted terminal event")
        };
        assert_eq!(event.as_body().unwrap().front().unwrap().as_ref(), b"x");
        assert!(event.eom());
        assert!(transfer.is_complete());
    });
}

#[test]
fn body_capacity_timeout_retains_overflow_safe_host_deadline() {
    let (mut pool, clock, host) = ManualHost::create();
    let (worker, driver) = WorkerDriver::with_host(16, host.clone()).unwrap();
    let mut session = SessionCore::with_host(
        TransportDirection::Downstream,
        CodecProtocol::Http2,
        worker,
        host.clone(),
    )
    .unwrap();
    assert!(session.set_egress_write_timeout(Duration::MAX));
    let id = StreamId::new(1);
    let mut incoming = session.begin_incoming_stream(id).unwrap();
    session
        .ingress_headers(id, request_message(None), true)
        .unwrap();
    pool.run_until(incoming.source_mut().read_header_event())
        .unwrap();
    drop(incoming);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let source = HttpFixedSource::make_fixed_response(
            200,
            Some(BufQueue::copy_from_slice(&vec![b'x'; 65_536], 65_536).unwrap()),
        )
        .unwrap();
        let mut transfer = session
            .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
            .unwrap();
        retained_headers(&mut transfer, 200, false);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            transfer.poll_body_source(&mut context),
            Poll::Ready(Ok(None))
        ));
        assert!(host.now().checked_add(Duration::MAX).is_none());
        let deadline = transfer
            .buffer_timeout_deadline()
            .expect("full buffer arms timeout");
        assert_eq!(deadline, host.deadline_after(Duration::MAX));
        clock.advance(Duration::from_secs(1));
        for _ in 0..3 {
            assert!(matches!(
                transfer.poll_body_source(&mut context),
                Poll::Ready(Ok(None))
            ));
            assert_eq!(transfer.buffer_timeout_deadline(), Some(deadline));
        }
        assert!(matches!(
            transfer.poll_next_event_with_credit(&mut context, Some(1)),
            Poll::Ready(Ok(Some(EgressEvent::Body(_))))
        ));
        assert_eq!(transfer.buffer_timeout_deadline(), None);
    }));
    session
        .abort_stream_without_wire(id, HttpError::new(HttpErrorCode::CANCEL))
        .unwrap();
    session.process_pending_events().unwrap();
    pool.run_until(session.finalize()).unwrap();
    drop(driver);
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn body_capacity_timeout_releases_queue_before_reentrant_reset() {
    struct ReentrantStop {
        inner: RetainedSource,
        session: std::rc::Weak<RefCell<SessionCore>>,
        id: StreamId,
        reset: Rc<Cell<bool>>,
    }
    impl HttpSource for ReentrantStop {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.inner.read_header_event()
        }
        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.inner.read_body_event(maximum)
        }
        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.inner.stop_reading(error);
            let session = self
                .session
                .upgrade()
                .expect("the timeout retains its stream owner");
            self.reset.set(
                session
                    .borrow_mut()
                    .reset_stream_state(
                        self.id,
                        HttpError::new(HttpErrorCode::CANCEL),
                        ResetOrigin::Ingress,
                    )
                    .is_ok(),
            );
        }
    }
    run_local(async move {
        let (mut core, driver) = new_session(TransportDirection::Downstream, CodecProtocol::Http2);
        let id = StreamId::new(1);
        let mut incoming = core.begin_incoming_stream(id).unwrap();
        core.ingress_headers(id, request_message(None), true)
            .unwrap();
        let mut ingress = incoming.source_mut().read_header_event();
        assert!(matches!(poll_once(ingress.as_mut()), Poll::Ready(Ok(event)) if event.eom()));
        drop(ingress);
        drop(incoming);
        let session = Rc::new(RefCell::new(core));
        let reset = Rc::new(Cell::new(false));
        let reads = RetainedOperationLog::new(true);
        let source = RetainedSource::new(
            [RetainedStep {
                output: header(response_message(200), false),
                log: RetainedOperationLog::new(true),
            }],
            [RetainedStep {
                output: HttpBodyEvent::body(
                    BufQueue::copy_from_slice(&vec![b'x'; 65_535], 65_535).unwrap(),
                    false,
                ),
                log: Rc::clone(&reads),
            }],
        );
        let lifetime = Rc::clone(&source.lifetime);
        let mut transfer = session
            .borrow_mut()
            .install_egress_source(
                id,
                HttpSourceHolder::new(Box::new(ReentrantStop {
                    inner: source,
                    session: Rc::downgrade(&session),
                    id,
                    reset: Rc::clone(&reset),
                })),
                true,
            )
            .unwrap();
        let result = catch_unwind(AssertUnwindSafe(|| {
            retained_headers(&mut transfer, 200, false);
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                transfer.poll_body_source(&mut context),
                Poll::Ready(Ok(None))
            ));
            assert!(transfer.buffer_timeout_deadline().is_some());
            transfer.timeout_buffer_space();
        }));
        drop(transfer);
        let callbacks = session.borrow_mut().process_pending_events().map(drop);
        let session = Rc::try_unwrap(session)
            .ok()
            .expect("source owner released")
            .into_inner();
        let finalized = session.finalize().await;
        drop(driver);
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
        callbacks.unwrap();
        finalized.unwrap();
        assert!(reset.get());
        assert_eq!((lifetime.stops.get(), lifetime.drops.get()), (1, 1));
        assert_eq!(
            (
                reads.created.get(),
                reads.retired.get(),
                reads.cancelled.get()
            ),
            (1, 1, 0)
        );
    });
}

#[test]
fn body_source_error_retires_staged_bytes_before_original_completion() {
    use roxygen_core::byte_event::ByteEventType;
    use roxygen_core::byte_event::HttpByteEvent;
    use roxygen_core::byte_event::HttpByteEventCallback;
    use roxygen_core::byte_event::HttpByteEventRegistration;

    struct ErrorSource {
        prefix: RetainedSource,
        error: Option<HttpError>,
        gate: Rc<RetainedOperationLog>,
    }
    impl HttpSource for ErrorSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.prefix.read_header_event()
        }
        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            if !self.prefix.bodies.is_empty() {
                return self.prefix.read_body_event(maximum);
            }
            Box::pin(RetainedOperation::new(
                Err(self.error.take().expect("one source error")),
                Rc::clone(&self.gate),
                Rc::clone(&self.prefix.cancellation),
            ))
        }
        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.prefix.stop_reading(error);
        }
    }
    struct Registration {
        ordinal: usize,
        errors: Rc<RefCell<Vec<(usize, HttpError)>>>,
        reenter: Option<(std::rc::Weak<RefCell<SessionCore>>, StreamId, bool)>,
        reset: Rc<Cell<bool>>,
        replacement: Rc<RefCell<Option<EgressTransfer>>>,
        replacement_lifetime: Rc<StopLog>,
        panic_armed: Rc<Cell<bool>>,
    }
    impl HttpByteEventCallback for Registration {
        fn on_byte_event(&self, _event: HttpByteEvent) {
            panic!("staged bytes have not reached a transport");
        }
        fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
            self.errors.borrow_mut().push((self.ordinal, error));
            if let Some((session, id, replace)) = &self.reenter {
                let session = session.upgrade().unwrap();
                if *replace {
                    let mut source = RetainedSource::new(
                        [RetainedStep {
                            output: header(response_message(211), true),
                            log: RetainedOperationLog::new(true),
                        }],
                        [],
                    );
                    source.lifetime = Rc::clone(&self.replacement_lifetime);
                    let transfer = session
                        .borrow_mut()
                        .install_egress_source(*id, HttpSourceHolder::new(Box::new(source)), true)
                        .unwrap();
                    self.replacement.replace(Some(transfer));
                } else {
                    self.reset.set(
                        session
                            .borrow_mut()
                            .reset_stream_state(
                                *id,
                                HttpError::new(HttpErrorCode::CANCEL),
                                ResetOrigin::Ingress,
                            )
                            .is_ok(),
                    );
                }
            }
            if self.panic_armed.get() {
                std::panic::panic_any(self.ordinal);
            }
        }
    }

    for (reenter, panic) in [(0, false), (1, false), (2, false), (0, true)] {
        run_local(async move {
            let (mut core, driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http2);
            let id = StreamId::new(1);
            let mut incoming = core.begin_incoming_stream(id).unwrap();
            core.ingress_headers(id, request_message(None), true)
                .unwrap();
            let mut ingress = incoming.source_mut().read_header_event();
            assert!(matches!(poll_once(ingress.as_mut()), Poll::Ready(Ok(event)) if event.eom()));
            drop(ingress);
            drop(incoming);
            let session = Rc::new(RefCell::new(core));
            let errors = Rc::new(RefCell::new(Vec::new()));
            let reset = Rc::new(Cell::new(false));
            let panic_armed = Rc::new(Cell::new(false));
            let replacement = Rc::new(RefCell::new(None));
            let replacement_lifetime = Rc::new(StopLog::default());
            let callbacks: Vec<_> = (0..2)
                .map(|ordinal| {
                    Rc::new(Registration {
                        ordinal,
                        errors: Rc::clone(&errors),
                        reenter: (reenter != 0 && ordinal == 0)
                            .then(|| (Rc::downgrade(&session), id, reenter == 2)),
                        reset: Rc::clone(&reset),
                        replacement: Rc::clone(&replacement),
                        replacement_lifetime: Rc::clone(&replacement_lifetime),
                        panic_armed: Rc::clone(&panic_armed),
                    })
                })
                .collect();
            let mut body = HttpBodyEvent::body(
                BufQueue::copy_from_slice(b"partialbody", 11).unwrap(),
                false,
            );
            for callback in &callbacks {
                body.byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::KernelWrite.bit(),
                        callback,
                    ));
            }
            let prefix = RetainedSource::new(
                [RetainedStep {
                    output: header(response_message(200), false),
                    log: RetainedOperationLog::new(true),
                }],
                [RetainedStep {
                    output: body,
                    log: RetainedOperationLog::new(true),
                }],
            );
            let lifetime = Rc::clone(&prefix.lifetime);
            let gate = RetainedOperationLog::new(false);
            let identity = Arc::new(());
            let source = ErrorSource {
                prefix,
                error: Some(session_io_failure(&identity)),
                gate: Rc::clone(&gate),
            };
            let mut transfer = session
                .borrow_mut()
                .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
                .unwrap();
            let result = catch_unwind(AssertUnwindSafe(|| {
                retained_headers(&mut transfer, 200, false);
                let mut context = Context::from_waker(Waker::noop());
                assert!(transfer.poll_body_source(&mut context).is_pending());
                assert_eq!(
                    session.borrow().stream(id).unwrap().buffered_egress_bytes(),
                    11
                );
                assert_eq!(
                    session
                        .borrow()
                        .stream(id)
                        .unwrap()
                        .take_buffered_egress_deltas(),
                    [11]
                );
                assert_eq!((gate.created.get(), gate.retired.get()), (1, 0));
                assert!(errors.borrow().is_empty());
                gate.ready.set(true);
                panic_armed.set(panic);
                let error =
                    catch_unwind(AssertUnwindSafe(|| transfer.poll_body_source(&mut context)));
                if panic {
                    let Err(payload) = error else {
                        panic!("first registration panic is resumed after retirement");
                    };
                    assert_eq!(payload.downcast_ref::<usize>(), Some(&0));
                } else {
                    let Poll::Ready(Err(error)) = error.unwrap() else {
                        panic!("the original source error completes the retained read");
                    };
                    assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
                    assert_session_cause(&error, &identity);
                }
                assert!(transfer.is_complete());
                assert!(!transfer.has_buffered_events());
                assert_eq!((lifetime.stops.get(), lifetime.drops.get()), (0, 1));
                assert_eq!(
                    (gate.created.get(), gate.retired.get(), gate.cancelled.get()),
                    (1, 1, 0)
                );
                assert_eq!(
                    session.borrow().stream(id).unwrap().buffered_egress_bytes(),
                    0
                );
                let deltas = session
                    .borrow()
                    .stream(id)
                    .unwrap()
                    .take_buffered_egress_deltas();
                assert_eq!(
                    deltas
                        .into_iter()
                        .filter(|delta| *delta != 0)
                        .collect::<Vec<_>>(),
                    [-11]
                );
                assert_eq!(reset.get(), reenter == 1);
                if reenter == 2 {
                    let updates = session.borrow_mut().process_pending_events().unwrap();
                    assert!(
                        updates
                            .iter()
                            .all(|update| update.action == StreamAction::None)
                    );
                    let mut next = replacement.borrow_mut().take().unwrap();
                    assert_ne!(next.generation(), transfer.generation());
                    assert!(next.generation_is_current());
                    assert_eq!(
                        (
                            replacement_lifetime.stops.get(),
                            replacement_lifetime.drops.get()
                        ),
                        (0, 0)
                    );
                    retained_headers(&mut next, 211, true);
                    session
                        .borrow_mut()
                        .response_headers(id, true, true)
                        .unwrap();
                    assert_eq!(
                        (
                            replacement_lifetime.stops.get(),
                            replacement_lifetime.drops.get()
                        ),
                        (0, 1)
                    );
                    drop(next);
                }
                let errors = errors.borrow();
                assert_eq!(
                    errors
                        .iter()
                        .map(|(ordinal, _)| *ordinal)
                        .collect::<Vec<_>>(),
                    [0, 1]
                );
                for (_, error) in errors.iter() {
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    assert_eq!(error.message(), b"Sent reset");
                    assert!(error.source().is_none());
                }
            }));
            panic_armed.set(false);
            drop(callbacks);
            drop(transfer);
            drop(replacement.borrow_mut().take());
            let updates = session.borrow_mut().process_pending_events();
            let duplicate = session.borrow_mut().process_pending_events();
            let remaining = session.borrow().stream_ids();
            let mut reset_error = None;
            for stream in remaining {
                if let Err(error) = session
                    .borrow_mut()
                    .abort_stream_without_wire(stream, HttpError::new(HttpErrorCode::CANCEL))
                {
                    reset_error.get_or_insert(error);
                }
            }
            let cleanup = session.borrow_mut().process_pending_events();
            let core = Rc::try_unwrap(session)
                .ok()
                .expect("callbacks retain only a weak session")
                .into_inner();
            let finalized = core.finalize().await;
            drop(driver);
            if let Err(payload) = result {
                std::panic::resume_unwind(payload);
            }
            let updates = updates.unwrap();
            assert_eq!(
                updates
                    .iter()
                    .filter(|update| matches!(
                        update.action,
                        StreamAction::Reset {
                            code: HttpErrorCode::TRANSPORT_READ_ERROR,
                            origin: ResetOrigin::Source
                        }
                    ))
                    .count(),
                usize::from(reenter == 0)
            );
            assert!(duplicate.unwrap().is_empty());
            assert!(reset_error.is_none());
            cleanup.unwrap();
            finalized.unwrap();
        });
    }
}

#[test]
fn egress_ready_empty_reads_yield_without_restarting_pending_child() {
    retention_case(|session, id| {
        let empty = RetainedOperationLog::new(true);
        let body = RetainedOperationLog::new(false);
        let mut steps = Vec::new();
        for _ in 0..129 {
            steps.push(RetainedStep {
                output: HttpBodyEvent::body(BufQueue::new(0), false),
                log: Rc::clone(&empty),
            });
        }
        steps.push(RetainedStep {
            output: HttpBodyEvent::body(BufQueue::copy_from_slice(b"x", 1).unwrap(), true),
            log: Rc::clone(&body),
        });
        let source = RetainedSource::new(
            [RetainedStep {
                output: header(response_message(200), false),
                log: RetainedOperationLog::new(true),
            }],
            steps,
        );
        let lifetime = Rc::clone(&source.lifetime);
        let mut transfer = session
            .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
            .unwrap();
        retained_headers(&mut transfer, 200, false);
        let wakes = Arc::new(RetainedWakes::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        for turn in 1..=2 {
            assert!(
                transfer
                    .poll_next_event_with_credit(&mut context, None)
                    .is_pending()
            );
            assert_eq!(empty.created.get(), turn * 64);
            assert_eq!(empty.polls.get(), turn * 64);
            assert_eq!(empty.retired.get(), turn * 64);
            assert_eq!(body.created.get(), 0);
            assert_eq!(wakes.0.load(Ordering::SeqCst), turn);
        }
        for _ in 0..3 {
            {
                let mut next = std::pin::pin!(transfer.next_event_with_credit(None));
                assert!(next.as_mut().poll(&mut context).is_pending());
            }
            assert_eq!(empty.created.get(), 129);
            assert_eq!(body.created.get(), 1);
            assert_eq!(body.retired.get(), 0);
            assert_eq!(body.cancelled.get(), 0);
            assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
        }
        body.ready.set(true);
        let Poll::Ready(Ok(Some(EgressEvent::Body(event)))) =
            transfer.poll_next_event_with_credit(&mut context, Some(1))
        else {
            panic!("the retained read completes after two cooperative boundaries")
        };
        assert_eq!(event.as_body().unwrap().front().unwrap().as_ref(), b"x");
        assert!(event.eom());
        assert!(transfer.is_complete());
        assert_eq!(empty.cancelled.get(), 0);
        assert_eq!(
            (body.created.get(), body.retired.get(), body.cancelled.get()),
            (1, 1, 0)
        );
        assert_eq!((lifetime.stops.get(), lifetime.drops.get()), (0, 1));
    });
}

#[test]
fn egress_event_prefetch_cap_reopens_after_dequeue_and_preserves_order() {
    use roxygen_core::byte_event::ByteEventType;
    use roxygen_core::byte_event::HttpByteEvent;
    use roxygen_core::byte_event::HttpByteEventCallback;
    use roxygen_core::byte_event::HttpByteEventRegistration;
    use roxygen_core::events::HttpBodyEventKind;

    struct Registration {
        ordinal: usize,
        cancelled: Rc<RefCell<Vec<(usize, Option<StreamId>, HttpErrorCode)>>>,
    }
    impl HttpByteEventCallback for Registration {
        fn on_byte_event(&self, _event: HttpByteEvent) {
            panic!("direct transfer test does not issue byte-event completions");
        }
        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancelled
                .borrow_mut()
                .push((self.ordinal, event.stream_id, error.code()));
        }
    }

    for cancel in [false, true] {
        retention_case(move |session, id| {
            let reads = RetainedOperationLog::new(true);
            let cancelled = Rc::new(RefCell::new(Vec::new()));
            let mut callbacks = Vec::new();
            let mut steps = Vec::new();
            for ordinal in 0..300 {
                let event = if ordinal % 2 == 0 {
                    HttpBodyEvent::padding((ordinal + 1) as u16, false)
                } else {
                    let callback = Rc::new(Registration {
                        ordinal,
                        cancelled: Rc::clone(&cancelled),
                    });
                    let mut event = HttpBodyEvent::body(BufQueue::new(0), false);
                    event
                        .byte_event_registrations_mut()
                        .push(HttpByteEventRegistration::new(
                            None,
                            ByteEventType::KernelWrite.bit(),
                            &callback,
                        ));
                    callbacks.push(callback);
                    event
                };
                steps.push(RetainedStep {
                    output: event,
                    log: Rc::clone(&reads),
                });
            }
            steps.push(RetainedStep {
                output: HttpBodyEvent::body(BufQueue::copy_from_slice(b"ab", 2).unwrap(), false),
                log: Rc::clone(&reads),
            });
            let mut fields = roxygen_core::headers::HttpHeaders::new();
            fields.add("x-order", "last");
            steps.push(RetainedStep {
                output: HttpBodyEvent::trailers(fields),
                log: Rc::clone(&reads),
            });
            let source = RetainedSource::new(
                [RetainedStep {
                    output: header(response_message(200), false),
                    log: RetainedOperationLog::new(true),
                }],
                steps,
            );
            let lifetime = Rc::clone(&source.lifetime);
            let mut transfer = session
                .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
                .unwrap();
            retained_headers(&mut transfer, 200, false);
            let wakes = Arc::new(RetainedWakes::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            for turn in 1..=3 {
                assert!(
                    transfer
                        .poll_next_event_with_credit(&mut context, None)
                        .is_pending()
                );
                assert_eq!(reads.created.get(), turn * 64);
            }
            for _ in 0..3 {
                assert!(matches!(
                    transfer.poll_next_event_with_credit(&mut context, None),
                    Poll::Ready(Ok(None))
                ));
                assert_eq!(
                    (reads.created.get(), reads.polls.get(), reads.retired.get()),
                    (256, 256, 256)
                );
                assert!(!transfer.can_read_source());
                assert!(!transfer.has_buffered_body_bytes());
                assert_eq!(transfer.buffer_timeout_deadline(), None);
                assert_eq!(wakes.0.load(Ordering::SeqCst), 3);
            }
            assert!(cancelled.borrow().is_empty());
            assert_eq!(lifetime.drops.get(), 0);
            if cancel {
                session
                    .reset_stream_state(
                        id,
                        HttpError::new(HttpErrorCode::CANCEL),
                        ResetOrigin::Ingress,
                    )
                    .unwrap();
                assert!(transfer.cancellation_requested());
                drop(transfer);
                assert_eq!(
                    (
                        reads.created.get(),
                        reads.retired.get(),
                        reads.cancelled.get()
                    ),
                    (256, 256, 0)
                );
                assert_eq!((lifetime.stops.get(), lifetime.drops.get()), (1, 1));
                let observed = cancelled.borrow();
                assert_eq!(observed.len(), 150);
                assert_eq!(
                    observed
                        .iter()
                        .filter(|(_, stream, _)| stream.is_some())
                        .copied()
                        .collect::<Vec<_>>(),
                    (1..256)
                        .step_by(2)
                        .map(|n| (n, Some(id), HttpErrorCode::CANCEL))
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    observed
                        .iter()
                        .filter(|(_, stream, _)| stream.is_none())
                        .copied()
                        .collect::<Vec<_>>(),
                    (257..300)
                        .step_by(2)
                        .map(|n| (n, None, HttpErrorCode::CANCEL))
                        .collect::<Vec<_>>()
                );
                return;
            }
            for ordinal in 0..300 {
                let Poll::Ready(Ok(Some(EgressEvent::Body(event)))) =
                    transfer.poll_next_event_with_credit(&mut context, Some(0))
                else {
                    panic!("actual empty and padding events remain ordered at zero stream credit")
                };
                if ordinal == 0 {
                    assert_eq!(reads.created.get(), 256);
                    assert!(transfer.can_read_source());
                } else if ordinal == 1 {
                    assert_eq!(reads.created.get(), 257);
                }
                if ordinal % 2 == 0 {
                    assert!(
                        matches!(event.kind(), HttpBodyEventKind::Padding(n) if usize::from(*n) == ordinal + 1)
                    );
                } else {
                    assert_eq!(event.as_body().unwrap().chain_length(), 0);
                    assert_eq!(event.byte_event_registrations().len(), 1);
                }
                assert!(!event.eom());
                drop(event);
            }
            assert_eq!(reads.created.get(), 302);
            assert!(matches!(
                transfer.poll_next_event_with_credit(&mut context, Some(0)),
                Poll::Ready(Ok(None))
            ));
            for byte in *b"ab" {
                let Poll::Ready(Ok(Some(EgressEvent::Body(event)))) =
                    transfer.poll_next_event_with_credit(&mut context, Some(1))
                else {
                    panic!("positive DATA waits for actual stream credit")
                };
                assert_eq!(event.as_body().unwrap().front().unwrap().as_ref(), &[byte]);
                assert!(!event.eom());
            }
            let Poll::Ready(Ok(Some(EgressEvent::Body(event)))) =
                transfer.poll_next_event_with_credit(&mut context, Some(0))
            else {
                panic!("trailers follow the full DATA payload")
            };
            let HttpBodyEventKind::Trailers(fields) = event.kind() else {
                panic!("terminal trailers")
            };
            assert_eq!(fields.get_single_or_empty("x-order"), b"last");
            assert!(event.eom());
            assert!(transfer.is_complete());
            assert_eq!(
                (
                    reads.created.get(),
                    reads.retired.get(),
                    reads.cancelled.get()
                ),
                (302, 302, 0)
            );
            assert_eq!(
                *cancelled.borrow(),
                (1..300)
                    .step_by(2)
                    .map(|n| (n, Some(id), HttpErrorCode::CANCEL))
                    .collect::<Vec<_>>()
            );
            assert_eq!((lifetime.stops.get(), lifetime.drops.get()), (0, 1));
            drop(callbacks);
        });
    }
}

#[test]
fn egress_ready_resumes_yield_with_owned_resume_and_preserve_cancellation() {
    for cancel in [false, true] {
        retention_case(move |session, id| {
            let reads = RetainedOperationLog::new(true);
            let terminal = RetainedOperationLog::new(false);
            let mut resumes = Vec::new();
            let mut steps = vec![RetainedStep {
                output: HttpBodyEvent::body(BufQueue::new(0), false),
                log: Rc::clone(&reads),
            }];
            for _ in 0..70 {
                let log = RetainedOperationLog::new(true);
                steps.push(RetainedStep {
                    output: HttpBodyEvent::suspend(Box::pin(RetainedOperation::new(
                        roxygen_core::events::ResumeStatus::Signalled,
                        Rc::clone(&log),
                        Rc::new(Cell::new(false)),
                    ))),
                    log: Rc::clone(&reads),
                });
                resumes.push(log);
            }
            steps.push(RetainedStep {
                output: HttpBodyEvent::body(BufQueue::copy_from_slice(b"x", 1).unwrap(), true),
                log: Rc::clone(&terminal),
            });
            let source = RetainedSource::new(
                [RetainedStep {
                    output: header(response_message(200), false),
                    log: RetainedOperationLog::new(true),
                }],
                steps,
            );
            let lifetime = Rc::clone(&source.lifetime);
            let mut transfer = session
                .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
                .unwrap();
            retained_headers(&mut transfer, 200, false);
            let wakes = Arc::new(RetainedWakes::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            assert!(
                transfer
                    .poll_next_event_with_credit(&mut context, None)
                    .is_pending()
            );
            assert_eq!(reads.created.get(), 33);
            assert_eq!(resumes.iter().map(|log| log.polls.get()).sum::<usize>(), 31);
            assert_eq!(
                (
                    resumes[31].created.get(),
                    resumes[31].polls.get(),
                    resumes[31].retired.get()
                ),
                (1, 0, 0)
            );
            assert_eq!(terminal.created.get(), 0);
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
            if cancel {
                session
                    .reset_stream_state(
                        id,
                        HttpError::new(HttpErrorCode::CANCEL),
                        ResetOrigin::Ingress,
                    )
                    .unwrap();
                assert!(transfer.cancellation_requested());
                let Poll::Ready(Err(error)) =
                    transfer.poll_next_event_with_credit(&mut context, None)
                else {
                    panic!("the retained resume observes cancellation")
                };
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(terminal.created.get(), 0);
                assert_eq!(reads.created.get(), 33);
                assert_eq!(
                    (
                        resumes[31].polls.get(),
                        resumes[31].retired.get(),
                        resumes[31].cancelled.get()
                    ),
                    (1, 1, 0)
                );
                assert!(resumes[32..].iter().all(|log| log.polls.get() == 0
                    && log.retired.get() == 1
                    && log.cancelled.get() == 1));
            } else {
                for _ in 0..3 {
                    assert!(
                        transfer
                            .poll_next_event_with_credit(&mut context, None)
                            .is_pending()
                    );
                }
                assert_eq!(reads.created.get(), 71);
                assert!(resumes.iter().all(|log| log.polls.get() == 1
                    && log.retired.get() == 1
                    && log.cancelled.get() == 0));
                assert_eq!((terminal.created.get(), terminal.retired.get()), (1, 0));
                terminal.ready.set(true);
                let Poll::Ready(Ok(Some(EgressEvent::Body(event)))) =
                    transfer.poll_next_event_with_credit(&mut context, Some(1))
                else {
                    panic!("terminal DATA follows all resumed operations")
                };
                assert_eq!(event.as_body().unwrap().front().unwrap().as_ref(), b"x");
                assert!(event.eom());
                assert_eq!(
                    (
                        terminal.created.get(),
                        terminal.retired.get(),
                        terminal.cancelled.get()
                    ),
                    (1, 1, 0)
                );
            }
            drop(transfer);
            assert_eq!(
                (lifetime.stops.get(), lifetime.drops.get()),
                (usize::from(cancel), 1)
            );
            assert!(
                resumes
                    .iter()
                    .all(|log| log.created.get() == 1 && log.retired.get() == 1)
            );
        });
    }
}

fn egress_pending_header_survives_scheduler_yield_case(direct: bool) {
    retention_case(move |session, id| {
        let interim = RetainedOperationLog::new(false);
        let final_head = RetainedOperationLog::new(false);
        let mut message = response_message(209);
        message.headers_mut().set("x-retained", "exact");
        let source = RetainedSource::new(
            [
                RetainedStep {
                    output: header(response_message(103), false),
                    log: Rc::clone(&interim),
                },
                RetainedStep {
                    output: header(message, true),
                    log: Rc::clone(&final_head),
                },
            ],
            [],
        );
        let mut transfer = session
            .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
            .unwrap();
        retained_pending_mode(&mut transfer, 8, &interim, direct);
        assert_eq!(final_head.created.get(), 0);
        interim.ready.set(true);
        retained_headers_mode(&mut transfer, 103, false, direct);
        assert_eq!(interim.retired.get(), 1);
        retained_pending_mode(&mut transfer, 0, &final_head, direct);
        final_head.ready.set(true);
        let Some(EgressEvent::Headers(mut event)) = retained_ready_mode(&mut transfer, 0, direct)
        else {
            panic!("final headers")
        };
        assert_eq!(event.headers_mut().status_code().unwrap(), 209);
        assert_eq!(
            event.headers().headers().get_single_or_empty("x-retained"),
            b"exact"
        );
        assert!(event.eom());
        assert!(transfer.is_complete());
        assert!(retained_ready_mode(&mut transfer, 8, direct).is_none());
        assert_eq!(final_head.retired.get(), 1);
        assert_eq!(interim.cancelled.get() + final_head.cancelled.get(), 0);
    });
}

fn egress_pending_body_survives_scheduler_yield_and_credit_change_case(direct: bool) {
    retention_case(move |session, id| {
        let head = RetainedOperationLog::new(true);
        let body = RetainedOperationLog::new(false);
        let trailers = RetainedOperationLog::new(false);
        let bytes = BufQueue::copy_from_slice(b"abcdefgh", 8).unwrap();
        let backing = bytes.front().unwrap().clone();
        let mut fields = roxygen_core::headers::HttpHeaders::new();
        fields.add("x-retained", "trailers");
        let source = RetainedSource::new(
            [RetainedStep {
                output: header(response_message(200), false),
                log: head,
            }],
            [
                RetainedStep {
                    output: HttpBodyEvent::body(bytes, false),
                    log: Rc::clone(&body),
                },
                RetainedStep {
                    output: HttpBodyEvent::trailers(fields),
                    log: Rc::clone(&trailers),
                },
            ],
        );
        let mut transfer = session
            .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
            .unwrap();
        retained_headers_mode(&mut transfer, 200, false, direct);
        retained_pending_mode(&mut transfer, 8, &body, direct);
        retained_pending_mode(&mut transfer, 3, &body, direct);
        retained_pending_mode(&mut transfer, 0, &body, direct);
        body.ready.set(true);
        assert!(retained_poll(&mut transfer, 0, direct).is_pending());
        assert!(transfer.has_buffered_body_bytes());
        assert_eq!(body.retired.get(), 1);
        let mut offset = 0;
        for (credit, expected) in [
            (3, b"abc".as_slice()),
            (2, b"de".as_slice()),
            (3, b"fgh".as_slice()),
        ] {
            assert!(retained_poll(&mut transfer, 0, direct).is_pending());
            let Some(EgressEvent::Body(event)) = retained_ready_mode(&mut transfer, credit, direct)
            else {
                panic!("ordered data")
            };
            let data = event.as_body().expect("data before trailers");
            assert_eq!(data.chain_length(), expected.len());
            assert_eq!(data.segments().len(), 1);
            assert_eq!(data.front().unwrap().as_ref(), expected);
            assert_eq!(
                data.front().unwrap().as_ptr(),
                backing.as_ptr().wrapping_add(offset)
            );
            assert!(!event.eom());
            offset += expected.len();
            assert_eq!(body.created.get(), 1);
            assert_eq!(trailers.created.get(), 1);
            assert_eq!(trailers.retired.get(), 0);
            assert_eq!(trailers.cancelled.get(), 0);
        }
        assert_eq!(offset, 8);
        retained_pending_mode(&mut transfer, 0, &trailers, direct);
        trailers.ready.set(true);
        let Some(EgressEvent::Body(event)) = retained_ready_mode(&mut transfer, 0, direct) else {
            panic!("trailers after all data")
        };
        let roxygen_core::events::HttpBodyEventKind::Trailers(fields) = event.kind() else {
            panic!("trailer kind")
        };
        assert_eq!(fields.get_single_or_empty("x-retained"), b"trailers");
        assert!(event.eom());
        assert!(transfer.is_complete());
        assert_eq!(trailers.retired.get(), 1);
        assert_eq!(body.cancelled.get() + trailers.cancelled.get(), 0);
    });
}

fn egress_pending_resume_survives_scheduler_yield_case(direct: bool) {
    retention_case(move |session, id| {
        let head = RetainedOperationLog::new(true);
        let suspended = RetainedOperationLog::new(true);
        let resume = RetainedOperationLog::new(false);
        let body = RetainedOperationLog::new(false);
        let resumed = RetainedOperation::new(
            roxygen_core::events::ResumeStatus::Signalled,
            Rc::clone(&resume),
            Rc::new(Cell::new(false)),
        );
        let bytes = BufQueue::copy_from_slice(b"resumed", 7).unwrap();
        let backing = bytes.front().unwrap().clone();
        let source = RetainedSource::new(
            [RetainedStep {
                output: header(response_message(200), false),
                log: head,
            }],
            [
                RetainedStep {
                    output: HttpBodyEvent::suspend(Box::pin(resumed)),
                    log: Rc::clone(&suspended),
                },
                RetainedStep {
                    output: HttpBodyEvent::body(bytes, true),
                    log: Rc::clone(&body),
                },
            ],
        );
        let mut transfer = session
            .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
            .unwrap();
        retained_headers_mode(&mut transfer, 200, false, direct);
        retained_pending_mode(&mut transfer, 7, &resume, direct);
        assert_eq!(suspended.created.get(), 1);
        assert_eq!(suspended.retired.get(), 1);
        assert_eq!(body.created.get(), 0);
        resume.ready.set(true);
        retained_pending_mode(&mut transfer, 2, &body, direct);
        assert_eq!(resume.retired.get(), 1);
        assert_eq!(resume.cancelled.get(), 0);
        body.ready.set(true);
        let mut offset = 0;
        for credit in [2, 5] {
            let Some(EgressEvent::Body(event)) = retained_ready_mode(&mut transfer, credit, direct)
            else {
                panic!("body after resumed producer")
            };
            let data = event.as_body().unwrap().front().unwrap();
            assert_eq!(data.as_ptr(), backing.as_ptr().wrapping_add(offset));
            assert_eq!(data.as_ref(), &backing[offset..offset + credit as usize]);
            offset += credit as usize;
            assert_eq!(event.eom(), offset == backing.len());
        }
        assert!(transfer.is_complete());
        assert_eq!(body.created.get(), 1);
        assert_eq!(body.retired.get(), 1);
        assert_eq!(body.cancelled.get(), 0);
    });
}

#[test]
fn egress_pending_header_survives_scheduler_yield() {
    egress_pending_header_survives_scheduler_yield_case(false);
}

#[test]
fn direct_egress_pending_header_survives_scheduler_yield() {
    egress_pending_header_survives_scheduler_yield_case(true);
}

#[test]
fn egress_pending_body_survives_scheduler_yield_and_credit_change() {
    egress_pending_body_survives_scheduler_yield_and_credit_change_case(false);
}

#[test]
fn direct_egress_pending_body_survives_scheduler_yield_and_credit_change() {
    egress_pending_body_survives_scheduler_yield_and_credit_change_case(true);
}

#[test]
fn egress_pending_resume_survives_scheduler_yield() {
    egress_pending_resume_survives_scheduler_yield_case(false);
}

#[test]
fn direct_egress_pending_resume_survives_scheduler_yield() {
    egress_pending_resume_survives_scheduler_yield_case(true);
}

#[derive(Clone, Copy)]
enum RetainedPanicPath {
    StopAndResume,
    OuterUnwind,
    ResumeOnly,
}

fn retained_resume_panic_case(path: RetainedPanicPath) {
    retention_case(move |session, id| {
        let stop_marker = Arc::new(());
        let resume_marker = Arc::new(());
        let external_marker = Arc::new(());
        let armed = Rc::new(Cell::new(false));
        let head = RetainedOperationLog::new(true);
        let suspended = RetainedOperationLog::new(true);
        let resume = RetainedOperationLog::new(false);
        let source = RetainedSource::new(
            [RetainedStep {
                output: header(response_message(200), false),
                log: Rc::clone(&head),
            }],
            [RetainedStep {
                output: HttpBodyEvent::suspend(Box::pin(PanickingRetainedResume {
                    operation: RetainedOperation::new(
                        roxygen_core::events::ResumeStatus::Signalled,
                        Rc::clone(&resume),
                        Rc::new(Cell::new(false)),
                    ),
                    armed: Rc::clone(&armed),
                    marker: Arc::clone(&resume_marker),
                })),
                log: Rc::clone(&suspended),
            }],
        );
        let lifetime = Rc::clone(&source.lifetime);
        let source = PanickingRetainedSource {
            source,
            armed: Rc::clone(&armed),
            stop_marker: matches!(
                path,
                RetainedPanicPath::StopAndResume | RetainedPanicPath::OuterUnwind
            )
            .then(|| Arc::clone(&stop_marker)),
        };
        let completion = session.exchange_tracker(id).unwrap().completion();
        let mut transfer = session
            .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
            .unwrap();
        retained_headers(&mut transfer, 200, false);
        retained_pending(&mut transfer, 7, &resume);
        assert!(resume.polls.get() >= 3);
        assert_eq!(suspended.created.get(), 1);
        assert_eq!(suspended.retired.get(), 1);
        assert_eq!(suspended.cancelled.get(), 0);
        assert_eq!(lifetime.stops.get(), 0);
        assert_eq!(lifetime.drops.get(), 0);

        let peer_id = StreamId::new(3);
        let mut incoming = session.begin_incoming_stream(peer_id).unwrap();
        let peer_completion = incoming.completion().clone();
        assert!(
            session
                .ingress_headers(peer_id, request_message(None), true)
                .unwrap()
        );
        let mut ingress = incoming.source_mut().read_header_event();
        assert!(matches!(poll_once(ingress.as_mut()), Poll::Ready(Ok(event)) if event.eom()));
        drop(ingress);
        drop(incoming);
        let peer_read = RetainedOperationLog::new(false);
        let peer_source = RetainedSource::new(
            [RetainedStep {
                output: header(response_message(200), false),
                log: Rc::clone(&peer_read),
            }],
            [],
        );
        let peer_lifetime = Rc::clone(&peer_source.lifetime);
        let mut peer_transfer = session
            .install_egress_source(peer_id, HttpSourceHolder::new(Box::new(peer_source)), true)
            .unwrap();
        retained_pending(&mut peer_transfer, 7, &peer_read);

        let mut confirmation = completion.wait();
        let mut peer_confirmation = peer_completion.wait();
        assert!(poll_once(confirmation.as_mut()).is_pending());
        assert!(poll_once(peer_confirmation.as_mut()).is_pending());
        let external_payload = Arc::clone(&external_marker);
        armed.set(true);
        let result = catch_unwind(AssertUnwindSafe(move || {
            let transfers = vec![transfer, peer_transfer];
            if matches!(path, RetainedPanicPath::OuterUnwind) {
                let _transfers = transfers;
                std::panic::panic_any(RetainedPanic(external_payload));
            }
            drop_egress_transfers_for_test(transfers);
        }));
        let payload = result.expect_err("the exact first panic must propagate");
        let actual = payload
            .downcast_ref::<RetainedPanic>()
            .expect("retirement must preserve the original typed payload");
        let expected = match path {
            RetainedPanicPath::StopAndResume => &stop_marker,
            RetainedPanicPath::OuterUnwind => &external_marker,
            RetainedPanicPath::ResumeOnly => &resume_marker,
        };
        assert!(Arc::ptr_eq(&actual.0, expected));
        drop(payload);
        assert_eq!(resume.created.get(), 1);
        assert_eq!(resume.retired.get(), 1);
        assert_eq!(resume.cancelled.get(), 1);
        assert_eq!(lifetime.stops.get(), 1);
        assert_eq!(lifetime.drops.get(), 1);
        assert_eq!(peer_read.created.get(), 1);
        assert_eq!(peer_read.retired.get(), 1);
        assert_eq!(peer_read.cancelled.get(), 1);
        assert_eq!(peer_lifetime.stops.get(), 1);
        assert_eq!(peer_lifetime.drops.get(), 1);
        session.process_pending_events().unwrap();
        assert!(session.stream(id).is_err());
        assert!(session.stream(peer_id).is_err());
        assert_eq!(
            poll_once(confirmation.as_mut()),
            Poll::Ready(Err(ExchangeCompletionError::CleanupUnconfirmed))
        );
        let peer_result = if matches!(path, RetainedPanicPath::OuterUnwind) {
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        } else {
            Ok(())
        };
        assert_eq!(
            poll_once(peer_confirmation.as_mut()),
            Poll::Ready(peer_result)
        );
    });
}

#[test]
fn egress_pending_read_retires_on_transfer_cancellation() {
    for phase in 0..3 {
        for action in 0..3 {
            retention_case(move |session, id| {
                let head = RetainedOperationLog::new(phase != 0);
                let body = RetainedOperationLog::new(phase == 2);
                let resume = RetainedOperationLog::new(false);
                let event = if phase == 2 {
                    HttpBodyEvent::suspend(Box::pin(RetainedOperation::new(
                        roxygen_core::events::ResumeStatus::Signalled,
                        Rc::clone(&resume),
                        Rc::new(Cell::new(false)),
                    )))
                } else {
                    HttpBodyEvent::body(BufQueue::copy_from_slice(b"pending", 7).unwrap(), true)
                };
                let source = RetainedSource::new(
                    [RetainedStep {
                        output: header(response_message(200), false),
                        log: Rc::clone(&head),
                    }],
                    [RetainedStep {
                        output: event,
                        log: Rc::clone(&body),
                    }],
                );
                let lifetime = Rc::clone(&source.lifetime);
                let mut transfer = session
                    .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
                    .unwrap();
                let generation = transfer.generation();
                if phase != 0 {
                    retained_headers(&mut transfer, 200, false);
                }
                let active = match phase {
                    0 => head,
                    1 => body,
                    _ => resume,
                };
                retained_pending(&mut transfer, 7, &active);
                assert_eq!(lifetime.stops.get(), 0);
                assert_eq!(lifetime.drops.get(), 0);
                let mut replacement = None;
                let mut next_lifetime = None;
                if action == 1 {
                    session
                        .reset_stream_state(
                            id,
                            HttpError::new(HttpErrorCode::CANCEL),
                            ResetOrigin::Ingress,
                        )
                        .unwrap();
                } else if action == 2 {
                    let source = RetainedSource::new(
                        [RetainedStep {
                            output: header(response_message(211), true),
                            log: RetainedOperationLog::new(true),
                        }],
                        [],
                    );
                    next_lifetime = Some(Rc::clone(&source.lifetime));
                    replacement = Some(
                        session
                            .install_egress_source(
                                id,
                                HttpSourceHolder::new(Box::new(source)),
                                true,
                            )
                            .unwrap(),
                    );
                }
                if action != 0 {
                    let phase_name = match phase {
                        0 => "headers",
                        1 => "body",
                        _ => "resume",
                    };
                    let action_name = if action == 1 { "reset" } else { "replacement" };
                    let expected = if phase == 1 {
                        HttpErrorCode::CANCEL
                    } else {
                        HttpErrorCode::CORO_CANCELLED
                    };
                    assert!(
                        transfer.cancellation_requested(),
                        "phase={phase_name} action={action_name}: cancellation was not requested"
                    );
                    let mut outer = Box::pin(transfer.next_event());
                    let error = match poll_once(outer.as_mut()) {
                        Poll::Ready(Err(error)) => error,
                        Poll::Ready(Ok(_)) => panic!(
                            "phase={phase_name} action={action_name}: cancelled transfer returned success"
                        ),
                        Poll::Pending => panic!(
                            "phase={phase_name} action={action_name}: cancelled transfer remained pending"
                        ),
                    };
                    assert_eq!(
                        error.code(),
                        expected,
                        "phase={phase_name} action={action_name}: cancellation code"
                    );
                    drop(outer);
                }
                drop(transfer);
                assert_eq!(active.created.get(), 1);
                assert_eq!(active.retired.get(), 1);
                assert_eq!(active.cancelled.get(), 1);
                assert_eq!(lifetime.stops.get(), 1);
                assert_eq!(lifetime.drops.get(), 1);
                if let Some(mut replacement) = replacement {
                    let lifetime = next_lifetime.unwrap();
                    assert_ne!(replacement.generation(), generation);
                    assert_eq!(lifetime.stops.get(), 0);
                    assert_eq!(lifetime.drops.get(), 0);
                    retained_headers(&mut replacement, 211, true);
                    assert!(replacement.is_complete());
                }
            });
        }
    }
    for path in [
        RetainedPanicPath::StopAndResume,
        RetainedPanicPath::OuterUnwind,
        RetainedPanicPath::ResumeOnly,
    ] {
        retained_resume_panic_case(path);
    }
}

async fn prepare_bodyless_request(
    session: &mut SessionCore,
    id: StreamId,
    method: HttpMethod,
) -> Result<OutgoingRequest, String> {
    let mut message = request_message(None);
    message
        .set_method(method)
        .map_err(|error| error.to_string())?;
    let reservation = session
        .reserve_request()
        .map_err(|error| error.to_string())?;
    let prepared = SessionCore::prepare_request(
        HttpSourceHolder::new(Box::new(HttpFixedSource::new(message, None))),
        reservation,
        CancellationToken::never(),
    )
    .await
    .map_err(|error| error.to_string())?;
    session
        .send_prepared_request(prepared, id)
        .map_err(|error| error.to_string())
}

fn require_head_observation(condition: bool, reason: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(reason.to_owned())
    }
}

fn retire_bodyless_request(
    session: &mut SessionCore,
    id: StreamId,
    mut outgoing: OutgoingRequest,
) -> Result<(), String> {
    let terminal = !outgoing.response_source.readable();
    let buffered = session
        .stream(id)
        .map(|stream| stream.buffered_ingress_bytes());
    let mut completion = outgoing.completion.wait();
    if !terminal {
        outgoing
            .response_source
            .stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
    }
    drop(outgoing);
    let updates = session.process_pending_events();
    let transactions = session.num_transactions();
    let outgoing_streams = session.num_outgoing_streams();
    let second_publication = session.publish_exchange_completions();
    let receipt = poll_once(completion.as_mut());
    let pending_exchanges = session.pending_exchange_count();
    require_head_observation(terminal, "response did not terminate before cleanup")?;
    require_head_observation(
        buffered.map_err(|error| error.to_string())? == 0,
        "response retained unexpected ingress bytes",
    )?;
    require_head_observation(
        updates
            .map_err(|error| error.to_string())?
            .iter()
            .any(|update| update.stream == id && update.detached),
        "native source completion did not detach its stream",
    )?;
    require_head_observation(transactions == 0, "transaction credit was not returned")?;
    require_head_observation(
        outgoing_streams == 0,
        "outgoing stream credit was not returned",
    )?;
    require_head_observation(
        second_publication == 0,
        "direct-core publication was not idempotent",
    )?;
    require_head_observation(
        receipt == Poll::Ready(Ok(())),
        &format!("actual exchange completion was not confirmed: {receipt:?}"),
    )?;
    require_head_observation(
        pending_exchanges == 0,
        "exchange owner remained after publication",
    )
}

#[derive(Debug)]
struct HeadResponseOutcome {
    response: Result<(), String>,
    retirement: Result<(), String>,
}

async fn observe_bodyless_response(
    session: &mut SessionCore,
    id: StreamId,
    method: HttpMethod,
    content_length: &[u8],
    header_eom: bool,
    informational: bool,
) -> Result<HeadResponseOutcome, String> {
    let mut outgoing = prepare_bodyless_request(session, id, method).await?;
    let response: Result<(), String> = async {
        let egress = session
            .egress_finished(id)
            .map_err(|error| error.to_string())?;
        require_head_observation(!egress.detached, "stream detached before its response")?;
        require_head_observation(
            outgoing
                .headers
                .headers_mut()
                .method()
                .map_err(|error| error.to_string())?
                == Some(method),
            "request method changed",
        )?;
        require_head_observation(outgoing.headers.eom(), "request header lost EOM")?;
        require_head_observation(
            outgoing.body.is_none(),
            "bodyless request gained a producer",
        )?;
        require_head_observation(
            session.num_outgoing_streams() == 1,
            "request credit was not retained",
        )?;
        let mut completion = outgoing.completion.wait();
        require_head_observation(
            poll_once(completion.as_mut()).is_pending(),
            "exchange completed before response ingress",
        )?;
        if informational {
            for status in [100, 103] {
                require_head_observation(
                    session
                        .ingress_headers(id, response_message(status), false)
                        .map_err(|error| error.to_string())?,
                    "informational header ingress was rejected",
                )?;
                let mut headers = outgoing
                    .response_source
                    .read_header_event()
                    .await
                    .map_err(|error| error.to_string())?;
                require_head_observation(
                    headers
                        .headers_mut()
                        .status_code()
                        .map_err(|error| error.to_string())?
                        == status,
                    "informational header order/status changed",
                )?;
                require_head_observation(!headers.eom(), "informational header gained EOM")?;
            }
        }
        let mut response = response_message(200);
        response
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, content_length);
        require_head_observation(
            session
                .ingress_headers(id, response, header_eom)
                .map_err(|error| error.to_string())?,
            "final header ingress was rejected",
        )?;
        let header_result = outgoing.response_source.read_header_event().await;
        let error = if method == HttpMethod::Get && header_eom {
            match header_result {
                Err(error) => Some(error),
                Ok(_) => return Err("GET still requires its advertised payload".to_owned()),
            }
        } else {
            let mut headers = header_result.map_err(|error| error.to_string())?;
            require_head_observation(
                headers
                    .headers_mut()
                    .status_code()
                    .map_err(|error| error.to_string())?
                    == 200,
                "final response status changed",
            )?;
            require_head_observation(headers.eom() == header_eom, "final header EOM changed")?;
            require_head_observation(
                headers
                    .headers()
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::ContentLength)
                    == content_length,
                "representation Content-Length changed",
            )?;
            if header_eom {
                None
            } else {
                require_head_observation(
                    session.ingress_eom(id).map_err(|error| error.to_string())?,
                    "separate EOM was rejected",
                )?;
                let body = outgoing.response_source.read_body_event(u32::MAX).await;
                if method == HttpMethod::Get {
                    match body {
                        Err(error) => Some(error),
                        Ok(_) => return Err("GET still rejects truncation at later EOM".to_owned()),
                    }
                } else {
                    let terminal = body.map_err(|error| error.to_string())?;
                    require_head_observation(terminal.eom(), "separate EOM was not terminal")?;
                    require_head_observation(
                        terminal
                            .as_body()
                            .is_some_and(|body| body.chain_length() == 0),
                        "HEAD separate EOM contains unexpected payload or event type",
                    )?;
                    None
                }
            }
        };
        if method == HttpMethod::Get {
            let error = error
                .ok_or_else(|| "GET did not detect missing representation bytes".to_owned())?;
            require_head_observation(
                error.code() == HttpErrorCode::CONTENT_LENGTH_MISMATCH,
                &format!("GET returned wrong cause: {error:?}"),
            )?;
            require_head_observation(
                error.message()
                    == b"Content-Length/body mismatch on ingress: expected= 12, actual= 0",
                &format!("GET mismatch diagnostic changed: {error:?}"),
            )?;
        } else {
            require_head_observation(error.is_none(), "HEAD received a fabricated payload error")?;
        }
        Ok(())
    }
    .await;
    let retirement = retire_bodyless_request(session, id, outgoing);
    Ok(HeadResponseOutcome {
        response,
        retirement,
    })
}

fn assert_head_response_outcome(outcome: Result<HeadResponseOutcome, String>) {
    let outcome = outcome.expect("bodyless request was prepared and committed");
    assert!(outcome.response.is_ok(), "{outcome:?}");
    assert!(outcome.retirement.is_ok(), "{outcome:?}");
}

#[test]
fn head_response_keeps_representation_length_without_body() {
    run_local(async {
        for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
            for header_eom in [true, false] {
                for content_length in [b"12".as_slice(), b"4194304".as_slice()] {
                    let (mut session, driver) = new_session(TransportDirection::Upstream, protocol);
                    let outcome = observe_bodyless_response(
                        &mut session,
                        StreamId::new(1),
                        HttpMethod::Head,
                        content_length,
                        header_eom,
                        true,
                    )
                    .await;
                    let finalization = session.finalize().await;
                    drop(driver);
                    assert!(
                        finalization.is_ok(),
                        "{protocol:?}, header_eom={header_eom}: {finalization:?}; {outcome:?}"
                    );
                    assert_head_response_outcome(outcome);
                }
            }
        }
    });
}

#[test]
fn head_response_exemption_does_not_leak_to_get() {
    run_local(async {
        for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
            let (mut session, driver) = new_session(TransportDirection::Upstream, protocol);
            let mut outcomes = Vec::new();
            for (stream, method, header_eom) in [
                (1, HttpMethod::Head, true),
                (3, HttpMethod::Get, true),
                (5, HttpMethod::Head, false),
                (7, HttpMethod::Get, false),
            ] {
                outcomes.push(
                    observe_bodyless_response(
                        &mut session,
                        StreamId::new(stream),
                        method,
                        b"12",
                        header_eom,
                        false,
                    )
                    .await,
                );
            }
            let finalization = session.finalize().await;
            drop(driver);
            assert!(
                finalization.is_ok(),
                "{protocol:?}: {finalization:?}; {outcomes:?}"
            );
            assert_eq!(outcomes.len(), 4);
            for outcome in outcomes {
                assert_head_response_outcome(outcome);
            }
        }
    });
}

async fn complete_upstream_response(
    session: &mut SessionCore,
    id: StreamId,
    mut outgoing: OutgoingRequest,
) {
    assert!(
        session
            .ingress_headers(id, response_message(200), false)
            .unwrap()
    );
    assert_eq!(
        session.ingress_body(id, body_100(), 0, true).unwrap(),
        roxygen_core::stream_source::FlowControlState::Open
    );
    let headers = outgoing
        .response_source
        .read_header_event()
        .await
        .expect("response headers");
    assert!(!headers.eom());
    let body = outgoing
        .response_source
        .read_body_event(65_535)
        .await
        .expect("response body");
    assert_eq!(body.as_body().unwrap().chain_length(), 100);
    assert!(body.eom());
    let updates = session.process_pending_events().unwrap();
    assert!(updates.iter().any(|update| update.detached));
}

async fn drain_response(
    session: &mut SessionCore,
    id: StreamId,
    transfer: &mut EgressTransfer,
) -> (u16, usize) {
    let event = transfer
        .next_event()
        .await
        .expect("response source succeeds")
        .expect("response header event");
    let EgressEvent::Headers(event) = event else {
        panic!("expected response headers");
    };
    let eom = event.eom();
    let mut message = event.into_headers();
    let status = message.status_code().unwrap();
    let (state, update) = session
        .response_headers(id, true, eom)
        .expect("response headers are serialized");
    assert!(!update.detached);
    let mut body_bytes = 0;
    if !eom {
        assert_eq!(state, ResponseState::Body);
        loop {
            let event = transfer.next_event().await.unwrap().unwrap();
            let EgressEvent::Body(event) = event else {
                panic!("expected response body");
            };
            body_bytes += event.as_body().map_or(0, BufQueue::chain_length);
            if event.eom() {
                break;
            }
        }
        assert!(!session.egress_finished(id).unwrap().detached);
    } else {
        assert_eq!(state, ResponseState::Done);
    }
    let updates = session.process_pending_events().unwrap();
    assert!(updates.iter().any(|update| update.detached));
    (status, body_bytes)
}

async fn serve_request(
    session: &mut SessionCore,
    handler: &HttpHandlerRef,
    request: IncomingRequest,
    with_body_event: bool,
) {
    let id = request.stream();
    assert!(
        session
            .ingress_headers(id, request_message(None), !with_body_event)
            .unwrap()
    );
    session
        .start_request_handler(request, Rc::clone(handler))
        .expect("handler task starts");
    if with_body_event {
        tokio::task::yield_now().await;
        assert!(session.process_pending_events().unwrap().is_empty());
        assert_eq!(
            session.ingress_body(id, body_100(), 0, true).unwrap(),
            roxygen_core::stream_source::FlowControlState::Open
        );
    }
    tokio::task::yield_now().await;
    let mut updates = session.process_pending_events().unwrap();
    let mut transfer = updates
        .iter_mut()
        .find_map(|update| update.replacement.take())
        .expect("handler returns a response source");
    assert_eq!(drain_response(session, id, &mut transfer).await, (200, 100));
}

#[test]
fn upstream_capacity_tracks_both_protocol_ends_instead_of_response_ownership() {
    run_local(async {
        for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
            for response_first in [false, true] {
                let (mut session, _driver) = new_session(TransportDirection::Upstream, protocol);
                let _ = session.set_max_concurrent_outgoing_streams(1);
                let events = Rc::new(RefCell::new(Vec::new()));
                session.add_lifecycle_observer(Rc::new(EventRecorder {
                    events: Rc::clone(&events),
                }));
                let activity = session.outgoing_transaction_activity();
                let id = StreamId::new(1);
                let prepared = SessionCore::prepare_request(
                    fixed_request(),
                    session.reserve_request().unwrap(),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
                let outgoing = session.send_prepared_request(prepared, id).unwrap();
                if response_first {
                    assert!(
                        session
                            .ingress_headers(id, response_message(200), true)
                            .unwrap()
                    );
                } else {
                    session.egress_finished(id).unwrap();
                }
                assert_eq!(
                    activity.available(),
                    0,
                    "one open protocol half still occupies capacity"
                );
                assert!(session.reserve_request().is_err());
                if response_first {
                    session.egress_finished(id).unwrap();
                } else {
                    assert!(
                        session
                            .ingress_headers(id, response_message(200), true)
                            .unwrap()
                    );
                }
                assert_eq!(activity.available(), 1);
                assert_eq!(session.num_outgoing_streams(), 0);
                assert_eq!(
                    session.num_incoming_streams(),
                    0,
                    "retained local streams remain classified as local"
                );
                assert_eq!(
                    session.num_transactions(),
                    1,
                    "source lifetime is independent of protocol capacity"
                );
                assert!(outgoing.response_source.readable());
                assert!(session.stream(id).unwrap().outgoing_capacity_released());
                let replacement = session
                    .reserve_request()
                    .expect("completed unread response permits a new request");
                session.egress_finished(id).unwrap();
                session.peer_reset(id, HttpErrorCode::CANCEL).unwrap();
                drop(outgoing);
                session.process_pending_events().unwrap();
                assert_eq!(
                    session.num_outgoing_streams(),
                    1,
                    "late reset and detach cannot release the replacement reservation"
                );
                assert_eq!(
                    events
                        .borrow()
                        .iter()
                        .filter(|event| **event
                            == LifecycleEvent::OutgoingCapacityReleased { stream: id })
                        .count(),
                    1
                );
                assert_eq!(
                    events
                        .borrow()
                        .iter()
                        .filter(|event| **event
                            == LifecycleEvent::TransactionDetached { stream: Some(id) })
                        .count(),
                    1
                );
                drop(replacement);
                assert_eq!(session.num_transactions(), 0);
                session.finalize().await.unwrap();
            }
        }
    });
}

#[test]
fn upstream_capacity_does_not_treat_content_length_errors_as_successful_eom() {
    run_local(async {
        for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
            for (expected, actual) in [
                (1, None),
                (3, Some(2)),
                (1, Some(2)),
                (2, Some(2)),
                (0, None),
            ] {
                let (mut session, _driver) = new_session(TransportDirection::Upstream, protocol);
                let _ = session.set_max_concurrent_outgoing_streams(1);
                let id = StreamId::new(1);
                let prepared = SessionCore::prepare_request(
                    fixed_request(),
                    session.reserve_request().unwrap(),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
                let outgoing = session.send_prepared_request(prepared, id).unwrap();
                session.egress_finished(id).unwrap();
                let mut message = response_message(200);
                message.headers_mut().set(
                    HttpHeaderCode::ContentLength,
                    expected.to_string().as_bytes(),
                );
                assert!(
                    session
                        .ingress_headers(id, message, actual.is_none())
                        .unwrap()
                );
                if let Some(actual) = actual {
                    let mut body = BufQueue::new(actual);
                    body.push_vec(vec![b'x'; actual]).unwrap();
                    assert_ne!(
                        session.ingress_body(id, body, 0, true).unwrap(),
                        roxygen_core::stream_source::FlowControlState::Error
                    );
                }
                let success = expected == actual.unwrap_or(0);
                let stream = session.stream(id).unwrap();
                assert!(
                    stream.terminal_error().is_none(),
                    "SourceComplete error has not been reconciled yet"
                );
                assert_eq!(stream.ingress_producer().ingress_eom_succeeded(), success);
                assert_eq!(stream.outgoing_capacity_released(), success);
                assert_eq!(
                    session.outgoing_transaction_activity().available(),
                    u32::from(success)
                );
                if !success {
                    assert_eq!(
                        outgoing
                            .response_source
                            .ingress_completion()
                            .unwrap()
                            .snapshot()
                            .unwrap()
                            .unwrap_err()
                            .code(),
                        HttpErrorCode::CONTENT_LENGTH_MISMATCH
                    );
                }
                drop(outgoing);
                session.process_pending_events().unwrap();
                assert_eq!(session.num_transactions(), 0);
                assert_eq!(session.outgoing_transaction_activity().available(), 1);
                session.finalize().await.unwrap();
            }
        }
    });
}

#[test]
fn prepared_reset_reports_the_removed_stream_once_after_accounting() {
    run_local(async {
        for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
            let (mut session, _driver) = new_session(TransportDirection::Downstream, protocol);
            let events = Rc::new(RefCell::new(Vec::new()));
            session.add_lifecycle_observer(Rc::new(EventRecorder {
                events: Rc::clone(&events),
            }));
            let id = StreamId::new(1);
            let mut request = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            assert!(
                request
                    .source_mut()
                    .read_header_event()
                    .await
                    .unwrap()
                    .eom()
            );
            drop(request);
            session.process_pending_events().unwrap();
            assert_eq!(session.num_transactions(), 1);
            let retirement = session
                .prepare_abort_stream_without_wire(id, HttpError::new(HttpErrorCode::CANCEL))
                .unwrap();
            assert_eq!(session.num_transactions(), 0);
            assert!(session.stream(id).is_err());
            assert!(
                events
                    .borrow()
                    .iter()
                    .all(|event| !matches!(event, LifecycleEvent::TransactionDetached { .. }))
            );
            assert!(retirement.retire().is_ok());
            session.process_pending_events().unwrap();
            session.peer_reset(id, HttpErrorCode::CANCEL).unwrap();
            assert_eq!(
                events
                    .borrow()
                    .iter()
                    .filter(
                        |event| **event == LifecycleEvent::TransactionDetached { stream: Some(id) }
                    )
                    .count(),
                1
            );
            session.finalize().await.unwrap();
        }
    });
}

#[test]
fn prepared_reset_waits_for_every_evacuated_egress_generation() {
    run_local(async {
        for finish_old_first in [false, true] {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http2);
            let id = StreamId::new(1);
            let mut request = session.begin_incoming_stream(id).unwrap();
            let mut completion = request.completion().wait();
            session
                .ingress_headers(id, request_message(None), true)
                .unwrap();
            request.source_mut().read_header_event().await.unwrap();
            drop(request);
            session.process_pending_events().unwrap();

            let old_log = Rc::new(StopLog::default());
            let old = session
                .install_egress_source(
                    id,
                    HttpSourceHolder::new(Box::new(NeverSource {
                        log: Rc::clone(&old_log),
                    })),
                    true,
                )
                .unwrap();
            let current_log = Rc::new(StopLog::default());
            let current = session
                .install_egress_source(
                    id,
                    HttpSourceHolder::new(Box::new(NeverSource {
                        log: Rc::clone(&current_log),
                    })),
                    true,
                )
                .unwrap();
            assert!(old.cancellation_requested());
            assert!(!current.cancellation_requested());

            let retirement = session
                .prepare_abort_stream_without_wire(id, HttpError::new(HttpErrorCode::CANCEL))
                .unwrap();
            assert!(!session.stream(id).unwrap().egress_task_complete());
            assert!(!session.stream(id).unwrap().egress_cancel_requested());
            assert!(!current.cancellation_requested());
            assert!(current.generation_is_current());
            assert_eq!(current_log.stops.get(), 0);
            assert!(retirement.retire().is_ok());
            assert!(current.cancellation_requested());
            assert_eq!((old_log.stops.get(), old_log.drops.get()), (1, 1));
            assert_eq!((current_log.stops.get(), current_log.drops.get()), (1, 1));
            session.process_pending_events().unwrap();
            assert!(!session.stream(id).unwrap().egress_task_complete());
            assert!(poll_once(completion.as_mut()).is_pending());

            let (first, last) = if finish_old_first {
                (old, current)
            } else {
                (current, old)
            };
            drop(first);
            let updates = session.process_pending_events().unwrap();
            assert!(updates.iter().all(|update| !update.detached));
            assert!(!session.stream(id).unwrap().egress_task_complete());
            assert_eq!(last.generation_is_current(), finish_old_first);
            assert_eq!(session.num_transactions(), 1);
            assert!(poll_once(completion.as_mut()).is_pending());

            drop(last);
            let updates = session.process_pending_events().unwrap();
            assert_eq!(updates.iter().filter(|update| update.detached).count(), 1);
            assert!(session.stream(id).is_err());
            assert_eq!(session.num_transactions(), 0);
            assert_eq!(poll_once(completion.as_mut()), Poll::Ready(Ok(())));
            assert_eq!((old_log.stops.get(), old_log.drops.get()), (1, 1));
            assert_eq!((current_log.stops.get(), current_log.drops.get()), (1, 1));
            session.finalize().await.unwrap();
        }
    });
}

#[test]
fn outgoing_eof_admission_closure_rejects_prepared_requests_without_draining_ingress() {
    run_local(async {
        let (mut session, _driver) =
            new_session(TransportDirection::Upstream, CodecProtocol::Http2);
        let events = Rc::new(RefCell::new(Vec::new()));
        session.add_lifecycle_observer(Rc::new(EventRecorder {
            events: Rc::clone(&events),
        }));
        let prepared = SessionCore::prepare_request(
            fixed_request(),
            session.reserve_request().unwrap(),
            CancellationToken::never(),
        )
        .await
        .unwrap();
        session.close_outgoing_admission();
        session.close_outgoing_admission();
        assert!(!session.is_draining());
        assert!(
            session
                .send_prepared_request(prepared, StreamId::new(1))
                .is_err()
        );
        assert!(session.set_max_concurrent_outgoing_streams(1_000));
        assert_eq!(session.outgoing_transaction_activity().available(), 0);
        assert!(session.reserve_request().is_err());
        let push = session
            .begin_pushed_stream(StreamId::new(2))
            .expect("final-byte push is still accepted before ingress drain");
        assert!(
            session
                .ingress_headers(StreamId::new(2), response_message(200), true)
                .unwrap()
        );
        drop(push);
        session.process_pending_events().unwrap();
        assert_eq!(session.num_transactions(), 0);
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| **event == LifecycleEvent::OutgoingAdmissionClosed)
                .count(),
            1
        );
        session.finalize().await.unwrap();
    });
}

#[test]
fn failed_upstream_request_returns_capacity_once_during_detach() {
    run_local(async {
        for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
            let (mut session, _driver) = new_session(TransportDirection::Upstream, protocol);
            let _ = session.set_max_concurrent_outgoing_streams(1);
            let events = Rc::new(RefCell::new(Vec::new()));
            session.add_lifecycle_observer(Rc::new(EventRecorder {
                events: Rc::clone(&events),
            }));
            let id = StreamId::new(1);
            let prepared = SessionCore::prepare_request(
                fixed_request(),
                session.reserve_request().unwrap(),
                CancellationToken::never(),
            )
            .await
            .unwrap();
            let outgoing = session.send_prepared_request(prepared, id).unwrap();
            session
                .reset_stream_state(
                    id,
                    HttpError::new(HttpErrorCode::CANCEL),
                    ResetOrigin::Source,
                )
                .unwrap();
            drop(outgoing);
            session.process_pending_events().unwrap();
            session.peer_reset(id, HttpErrorCode::CANCEL).unwrap();
            assert_eq!(session.num_outgoing_streams(), 0);
            assert_eq!(session.num_transactions(), 0);
            assert_eq!(session.outgoing_transaction_activity().available(), 1);
            assert_eq!(
                events
                    .borrow()
                    .iter()
                    .filter(
                        |event| **event == LifecycleEvent::OutgoingCapacityReleased { stream: id }
                    )
                    .count(),
                1
            );
            session.finalize().await.unwrap();
        }
    });
}

mod h1 {
    use super::*;

    #[test]
    fn httpupstream_session_test_reserve_request() {
        run_local(async {
            let events = Rc::new(RefCell::new(Vec::new()));
            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            session.add_lifecycle_observer(Rc::new(EventRecorder {
                events: Rc::clone(&events),
            }));
            assert!(!session.set_max_concurrent_outgoing_streams(1));

            let mut reservation = session.reserve_request().expect("first reservation");
            assert_eq!(session.num_outgoing_streams(), 1);
            assert_eq!(
                session
                    .reserve_request()
                    .err()
                    .expect("the ordered session is full")
                    .code(),
                HttpErrorCode::REFUSED_STREAM
            );
            assert!(reservation.cancel());
            assert!(!reservation.cancel());

            let reservation = session.reserve_request().expect("replacement reservation");
            let id = StreamId::new(1);
            let prepared = SessionCore::prepare_request(
                fixed_request(),
                reservation,
                CancellationToken::never(),
            )
            .await
            .expect("request headers are prepared");
            let outgoing = session
                .send_prepared_request(prepared, id)
                .expect("reservation becomes a request stream");
            assert_eq!(outgoing.sequence_number, 0);
            assert!(outgoing.body.is_none());
            assert!(!session.supports_more_transactions());
            assert!(!session.stream(id).unwrap().egress_complete());
            assert!(
                events
                    .borrow()
                    .iter()
                    .all(|event| !matches!(event, LifecycleEvent::RequestEnd { .. }))
            );
            assert!(!session.egress_finished(id).unwrap().detached);

            complete_upstream_response(&mut session, id, outgoing).await;
            assert_eq!(session.num_transactions(), 0);
            assert_eq!(
                *events.borrow(),
                vec![
                    LifecycleEvent::ActivateConnection,
                    LifecycleEvent::TransactionAttached,
                    LifecycleEvent::TransactionDetached { stream: None },
                    LifecycleEvent::DeactivateConnection,
                    LifecycleEvent::ActivateConnection,
                    LifecycleEvent::TransactionAttached,
                    LifecycleEvent::RequestEnd { stream: id },
                    LifecycleEvent::OutgoingCapacityReleased { stream: id },
                    LifecycleEvent::TransactionDetached { stream: Some(id) },
                    LifecycleEvent::DeactivateConnection,
                ]
            );
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn lifecycle_panics_run_remaining_observers_after_safe_detach_and_destroy() {
        run_local(async {
            let events = Rc::new(RefCell::new(Vec::new()));
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            session.add_lifecycle_observer(Rc::new(EventRecorder {
                events: Rc::clone(&events),
            }));
            let detach_panicker = session.add_lifecycle_observer(Rc::new(PanickingEventObserver {
                panic_on: LifecycleEvent::TransactionDetached {
                    stream: Some(StreamId::new(1)),
                },
                events: Rc::clone(&events),
            }));
            events.borrow_mut().clear();

            let id = StreamId::new(1);
            let request = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            let (handler, _) = handler(200);
            session.start_request_handler(request, handler).unwrap();
            time::timeout(Duration::from_secs(1), async {
                while !session.has_pending_handler_completion() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let mut updates = session.process_pending_events().unwrap();
            let mut transfer = updates
                .iter_mut()
                .find_map(|update| update.replacement.take())
                .unwrap();
            let EgressEvent::Headers(headers) = transfer.next_event().await.unwrap().unwrap()
            else {
                panic!("expected response headers");
            };
            session.response_headers(id, true, headers.eom()).unwrap();
            let body = transfer.next_event().await.unwrap().unwrap();
            assert!(matches!(body, EgressEvent::Body(_)));
            assert!(!session.egress_finished(id).unwrap().detached);
            let detached = session.transaction_detach_wait();
            assert!(
                catch_unwind(AssertUnwindSafe(|| { session.process_pending_events() })).is_err()
            );
            assert_eq!(session.num_transactions(), 0);
            time::timeout(Duration::from_secs(1), detached)
                .await
                .expect("detach wake was committed before observer panic");
            assert!(
                events
                    .borrow()
                    .contains(&LifecycleEvent::TransactionDetached { stream: Some(id) })
            );
            assert!(session.remove_lifecycle_observer(detach_panicker));

            events.borrow_mut().clear();
            session.add_lifecycle_observer(Rc::new(PanickingEventObserver {
                panic_on: LifecycleEvent::Destroy,
                events: Rc::clone(&events),
            }));
            let join = tokio::task::spawn_local(session.finalize()).await;
            assert!(join.is_err());
            assert_eq!(
                events
                    .borrow()
                    .iter()
                    .filter(|event| **event == LifecycleEvent::Destroy)
                    .count(),
                2,
                "the observer after the panicker still receives Destroy",
            );
        });
    }

    #[test]
    fn lifecycle_attachment_panics_leave_no_reservation_observer_or_stream() {
        run_local(async {
            let (upstream, _upstream_driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let reservation_panicker =
                upstream.add_lifecycle_observer(Rc::new(PanickingEventObserver {
                    panic_on: LifecycleEvent::TransactionAttached,
                    events: Rc::new(RefCell::new(Vec::new())),
                }));
            assert!(catch_unwind(AssertUnwindSafe(|| upstream.reserve_request())).is_err());
            assert_eq!(upstream.num_transactions(), 0);
            assert!(upstream.remove_lifecycle_observer(reservation_panicker));
            upstream.finalize().await.unwrap();

            let (mut downstream, _downstream_driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let attached = Rc::new(Cell::new(0));
            let orphan_events = Rc::new(Cell::new(0));
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    downstream.add_lifecycle_observer(Rc::new(PanickingAttachObserver {
                        attached: Rc::clone(&attached),
                        events: Rc::clone(&orphan_events),
                    }))
                }))
                .is_err()
            );
            assert_eq!(attached.get(), 1);

            let stream_panicker =
                downstream.add_lifecycle_observer(Rc::new(PanickingEventObserver {
                    panic_on: LifecycleEvent::TransactionAttached,
                    events: Rc::new(RefCell::new(Vec::new())),
                }));
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    downstream.begin_incoming_stream(StreamId::new(1))
                }))
                .is_err()
            );
            assert_eq!(downstream.num_transactions(), 0);
            assert!(downstream.remove_lifecycle_observer(stream_panicker));
            downstream.process_pending_events().unwrap();
            assert_eq!(orphan_events.get(), 0);
            downstream.finalize().await.unwrap();
        });
    }

    #[test]
    fn httpupstream_session_test_supports_more_transactions() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let request = ScriptedSource::with_body(
                header(request_message(Some(b"100")), false),
                HttpBodyEvent::body(body_100(), true),
            );
            let reservation = session.reserve_request().expect("request capacity");
            let id = StreamId::new(1);
            let prepared = SessionCore::prepare_request(
                HttpSourceHolder::new(Box::new(request)),
                reservation,
                CancellationToken::never(),
            )
            .await
            .unwrap();
            let mut outgoing = session.send_prepared_request(prepared, id).unwrap();
            assert!(!session.supports_more_transactions());
            let body = outgoing
                .body
                .as_mut()
                .expect("non-EOM headers retain the body source")
                .next_event()
                .await
                .unwrap()
                .unwrap();
            let EgressEvent::Body(event) = body else {
                panic!("expected request body");
            };
            assert_eq!(event.as_body().unwrap().chain_length(), 100);
            assert!(event.eom());
            session.process_pending_events().unwrap();
            session.egress_finished(id).unwrap();

            complete_upstream_response(&mut session, id, outgoing).await;
            assert!(session.supports_more_transactions());
            assert!(session.initiate_drain());
            assert!(!session.initiate_drain());
            assert!(!session.supports_more_transactions());
            assert_eq!(
                session
                    .reserve_request()
                    .err()
                    .expect("draining rejects admission")
                    .code(),
                HttpErrorCode::REFUSED_STREAM
            );
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn h2_capacity_counts_reserved_and_active_streams() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http2);
            assert!(session.set_max_concurrent_outgoing_streams(2));
            let first = session.reserve_request().unwrap();
            let second = session.reserve_request().unwrap();
            assert_eq!(session.num_outgoing_streams(), 2);
            assert_eq!(
                session
                    .reserve_request()
                    .err()
                    .expect("reservations consume multiplexed capacity")
                    .code(),
                HttpErrorCode::REFUSED_STREAM
            );
            drop(first);
            drop(second);
            assert!(session.supports_more_transactions());
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn httpdownstream_session_test_two_requests() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let (handler, requests) = handler(200);
            let first_id = StreamId::new(1);
            let second_id = StreamId::new(2);
            let first = session.begin_incoming_stream(first_id).unwrap();
            let second = session.begin_incoming_stream(second_id).unwrap();
            assert_eq!(session.num_incoming_streams(), 2);
            serve_request(&mut session, &handler, first, true).await;
            assert_eq!(session.num_incoming_streams(), 1);
            serve_request(&mut session, &handler, second, false).await;
            assert_eq!(
                *requests.borrow(),
                vec![(StreamId::new(1), 100), (StreamId::new(2), 0)]
            );
            assert_eq!(session.num_incoming_streams(), 0);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn httpdownstream_session_test_stream_id_seq_no() {
        run_local(async {
            let (mut h1, _h1_driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let (handler, _) = handler(200);
            let first = h1.begin_incoming_stream(StreamId::new(101)).unwrap();
            let second = h1.begin_incoming_stream(StreamId::new(7)).unwrap();
            assert_eq!(first.sequence_number(), 100);
            assert_eq!(second.sequence_number(), 6);
            serve_request(&mut h1, &handler, first, false).await;
            serve_request(&mut h1, &handler, second, false).await;
            h1.finalize().await.unwrap();

            let (mut h2, _h2_driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http2);
            let first = h2.begin_incoming_stream(StreamId::new(1)).unwrap();
            let second = h2.begin_incoming_stream(StreamId::new(3)).unwrap();
            assert_eq!(first.sequence_number(), 0);
            assert_eq!(second.sequence_number(), 1);
            serve_request(&mut h2, &handler, first, false).await;
            serve_request(&mut h2, &handler, second, false).await;
            h2.finalize().await.unwrap();
        });
    }
}

mod session {
    use super::*;

    #[test]
    fn session_lifetime_is_bound_to_one_worker_identity() {
        run_local(async {
            let (worker, driver) = WorkerDriver::new(8).unwrap();
            let (other, other_driver) = WorkerDriver::new(8).unwrap();
            assert_eq!(worker.owner_thread(), other.owner_thread());
            assert!(!worker.same_worker(&other));
            let events = Rc::new(RefCell::new(Vec::new()));
            let session = SessionCore::new(
                TransportDirection::Upstream,
                CodecProtocol::Http11,
                worker.clone(),
            )
            .unwrap();
            assert_eq!(session.worker_id(), worker.id());
            assert!(matches!(
                session.acquire_session_handle(other.clone()),
                Err(SessionHandleError::WrongOwnerWorker { .. })
            ));
            let mut handle = session
                .acquire_session_handle(worker.clone())
                .expect("matching worker handle");
            handle.reset();
            session.add_lifecycle_observer(Rc::new(WorkerDestroyRecorder {
                worker: worker.clone(),
                events: Rc::clone(&events),
            }));
            session.finalize().await.unwrap();
            assert_eq!(*events.borrow(), vec!["destroy-on-worker"]);
            assert_eq!(worker.state(), WorkerState::Open);
            drop(driver);
            drop(other_driver);
        });
    }

    #[test]
    fn request_preparation_cancels_without_borrowing_the_session() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let reservation = session.reserve_request().unwrap();
            let cancellation = CancellationSource::new();
            let stop_log = Rc::new(StopLog::default());
            let mut send = SessionCore::prepare_request(
                HttpSourceHolder::new(Box::new(NeverSource {
                    log: Rc::clone(&stop_log),
                })),
                reservation,
                cancellation.token(),
            );
            assert!(poll_once(send.as_mut()).is_pending());
            assert!(session.initiate_drain());
            assert!(cancellation.request_cancellation());
            assert_eq!(
                send.await
                    .err()
                    .expect("pending request header is cancelled")
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(stop_log.stops.get(), 1);
            assert_eq!(stop_log.drops.get(), 1);
            assert_eq!(session.num_transactions(), 0);
            session.finalize().await.unwrap();
        });
    }

    struct ReadyHeadSource {
        reads: Rc<Cell<usize>>,
    }

    impl HttpSource for ReadyHeadSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.reads.set(self.reads.get() + 1);
            Box::pin(std::future::ready(Ok(header(request_message(None), true))))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::pending())
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    /// Proxygen's `sendRequest` reads the request head and then checks its
    /// cancellation token; it waits on the token only while the head is pending.
    #[test]
    fn request_preparation_checks_its_token_after_a_ready_head_and_waits_only_on_a_pending_one() {
        run_local(async {
            let (session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let before = crate::session::head_cancellation_waits_built_for_test();
            for cancelled in [false, true] {
                let cancellation = CancellationSource::new();
                if cancelled {
                    cancellation.request_cancellation();
                }
                let reads = Rc::new(Cell::new(0));
                let prepared = SessionCore::prepare_request(
                    HttpSourceHolder::new(Box::new(ReadyHeadSource {
                        reads: Rc::clone(&reads),
                    })),
                    session.reserve_request().unwrap(),
                    cancellation.token(),
                )
                .await;
                assert_eq!(reads.get(), 1, "the head is read before the token check");
                match prepared {
                    Ok(prepared) => {
                        assert!(!cancelled);
                        drop(prepared);
                    }
                    Err(error) => {
                        assert!(cancelled);
                        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                        assert_eq!(error.message(), b"request header read cancelled");
                    }
                }
                assert_eq!(
                    crate::session::head_cancellation_waits_built_for_test(),
                    before,
                    "a ready head builds no cancellation wait"
                );
            }

            let cancellation = CancellationSource::new();
            let stop_log = Rc::new(StopLog::default());
            let mut send = SessionCore::prepare_request(
                HttpSourceHolder::new(Box::new(NeverSource {
                    log: Rc::clone(&stop_log),
                })),
                session.reserve_request().unwrap(),
                cancellation.token(),
            );
            assert!(poll_once(send.as_mut()).is_pending());
            assert!(poll_once(send.as_mut()).is_pending());
            assert_eq!(
                crate::session::head_cancellation_waits_built_for_test(),
                before + 1,
                "a pending head builds one wait"
            );
            assert!(cancellation.request_cancellation());
            let error = send.await.err().expect("the pending head is cancelled");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.message(), b"request header read cancelled");
            assert_eq!(stop_log.stops.get(), 1);
            assert_eq!(stop_log.drops.get(), 1);
            assert_eq!(session.num_transactions(), 0);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn read_write_tasks_are_siblings_and_join_before_destroy() {
        run_local(async {
            run_sibling_order(true).await;
            run_sibling_order(false).await;
        });
    }

    async fn run_sibling_order(read_finishes_first: bool) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let owner = std::thread::current().id();
        let (mut session, _driver) =
            new_session(TransportDirection::Upstream, CodecProtocol::Http11);
        session.add_lifecycle_observer(Rc::new(DestroyRecorder {
            log: Rc::clone(&log),
            owner,
        }));
        let read_log = Rc::clone(&log);
        let write_log = Rc::clone(&log);
        session
            .start_read_write_tasks(
                move |cancellation| async move {
                    let _drop = TaskDrop {
                        label: "read-drop",
                        log: Rc::clone(&read_log),
                        owner,
                    };
                    read_log.borrow_mut().push("read-start");
                    if read_finishes_first {
                        read_log.borrow_mut().push("read-end");
                    } else {
                        let wait = CancellableBaton::new();
                        assert_eq!(wait.wait(cancellation).await, WaitStatus::Cancelled);
                        read_log.borrow_mut().push("read-cancelled");
                    }
                },
                move |cancellation| async move {
                    let _drop = TaskDrop {
                        label: "write-drop",
                        log: Rc::clone(&write_log),
                        owner,
                    };
                    write_log.borrow_mut().push("write-start");
                    if read_finishes_first {
                        let wait = CancellableBaton::new();
                        assert_eq!(wait.wait(cancellation).await, WaitStatus::Cancelled);
                        write_log.borrow_mut().push("write-cancelled");
                    } else {
                        write_log.borrow_mut().push("write-end");
                    }
                },
            )
            .unwrap();
        time::timeout(Duration::from_secs(1), session.join_read_write_tasks())
            .await
            .expect("sibling completion is bounded")
            .unwrap();
        assert_eq!(session.pending_task_count(), 0);
        session.finalize().await.unwrap();

        let log = log.borrow();
        assert!(position(&log, "read-drop") < position(&log, "session-destroy"));
        assert!(position(&log, "write-drop") < position(&log, "session-destroy"));
        if read_finishes_first {
            assert!(position(&log, "read-end") < position(&log, "write-cancelled"));
        } else {
            assert!(position(&log, "write-end") < position(&log, "read-cancelled"));
        }
    }

    #[test]
    fn task_panic_explicit_cancel_and_dropped_join_are_bounded() {
        run_local(async {
            let cancelled = Rc::new(Cell::new(false));
            let (mut panic_session, _panic_driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let sibling_cancelled = Rc::clone(&cancelled);
            panic_session
                .start_read_write_tasks(
                    |_| async { panic!("scripted read panic") },
                    move |cancellation| async move {
                        let wait = CancellableBaton::new();
                        assert_eq!(wait.wait(cancellation).await, WaitStatus::Cancelled);
                        sibling_cancelled.set(true);
                    },
                )
                .unwrap();
            assert_eq!(
                time::timeout(
                    Duration::from_secs(1),
                    panic_session.join_read_write_tasks(),
                )
                .await
                .expect("panic joins both siblings"),
                Err(SessionTaskError::TaskGroup(TaskGroupError::TaskPanicked))
            );
            assert!(cancelled.get());
            panic_session.finalize().await.unwrap();

            let cancelled_count = Rc::new(Cell::new(0));
            let (mut cancel_session, _cancel_driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let read_count = Rc::clone(&cancelled_count);
            let write_count = Rc::clone(&cancelled_count);
            cancel_session
                .start_read_write_tasks(
                    move |cancellation| async move {
                        let wait = CancellableBaton::new();
                        assert_eq!(wait.wait(cancellation).await, WaitStatus::Cancelled);
                        read_count.set(read_count.get() + 1);
                    },
                    move |cancellation| async move {
                        let wait = CancellableBaton::new();
                        assert_eq!(wait.wait(cancellation).await, WaitStatus::Cancelled);
                        write_count.set(write_count.get() + 1);
                    },
                )
                .unwrap();
            let mut abandoned_join = Box::pin(cancel_session.join_read_write_tasks());
            assert!(poll_once(abandoned_join.as_mut()).is_pending());
            drop(abandoned_join);
            time::timeout(
                Duration::from_secs(1),
                cancel_session.cancel_and_join_read_write_tasks(),
            )
            .await
            .expect("explicit cancellation is bounded")
            .unwrap();
            assert_eq!(cancelled_count.get(), 2);
            cancel_session.finalize().await.unwrap();
        });
    }

    #[test]
    fn handler_source_transfer_retains_session_until_response_release() {
        run_local(async {
            let log = Rc::new(RefCell::new(Vec::new()));
            let requests = Rc::new(RefCell::new(Vec::new()));
            let handler: HttpHandlerRef = Rc::new(ScriptedHandler {
                status: 200,
                retain_context: true,
                requests: Rc::clone(&requests),
                drop_log: Some(Rc::clone(&log)),
            });
            let owner = std::thread::current().id();
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            session.add_lifecycle_observer(Rc::new(DestroyRecorder {
                log: Rc::clone(&log),
                owner,
            }));
            let id = StreamId::new(1);
            let incoming = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            session
                .start_request_handler(incoming, Rc::clone(&handler))
                .unwrap();
            assert_eq!(session.num_keep_alives(), 3);
            tokio::task::yield_now().await;
            let mut updates = session.process_pending_events().unwrap();
            let mut transfer = updates
                .iter_mut()
                .find_map(|update| update.replacement.take())
                .expect("handler response is ready");
            assert_eq!(session.num_keep_alives(), 2);
            assert_eq!(
                drain_response(&mut session, id, &mut transfer).await,
                (200, 100)
            );
            assert_eq!(session.num_keep_alives(), 1);
            assert_eq!(*requests.borrow(), vec![(id, 0)]);
            drop(transfer);
            session.finalize().await.unwrap();
            let log = log.borrow();
            assert!(position(&log, "handler-response-drop") < position(&log, "session-destroy"));
        });
    }

    #[test]
    fn pending_handler_does_not_borrow_session_and_is_cancelled_with_stream() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http2);
            let (handler, _) = handler(200);
            let first_id = StreamId::new(1);
            let first = session.begin_incoming_stream(first_id).unwrap();
            assert!(
                session
                    .ingress_headers(first_id, request_message(None), false)
                    .unwrap()
            );
            session
                .start_request_handler(first, Rc::clone(&handler))
                .unwrap();
            tokio::task::yield_now().await;
            assert!(session.process_pending_events().unwrap().is_empty());

            let second_id = StreamId::new(3);
            let second = session.begin_incoming_stream(second_id).unwrap();
            assert!(
                session
                    .ingress_headers(second_id, request_message(None), false)
                    .unwrap()
            );
            session
                .start_request_handler(second, Rc::clone(&handler))
                .unwrap();
            tokio::task::yield_now().await;
            assert!(
                !session
                    .peer_reset(first_id, HttpErrorCode::CANCEL)
                    .unwrap()
                    .unwrap()
                    .detached
            );
            tokio::task::yield_now().await;
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .any(|update| update.stream == first_id && update.detached)
            );

            assert_eq!(
                session.fail_connection(HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR)),
                vec![second_id]
            );
            tokio::task::yield_now().await;
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .any(|update| update.stream == second_id && update.detached)
            );
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn handler_completion_is_a_detach_gate_and_completed_tasks_are_reaped() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let ready = Rc::new(CancellableBaton::new());
            let release = Rc::new(CancellableBaton::new());
            let handler: HttpHandlerRef = Rc::new(ControlledHandler {
                ready: Some(Rc::clone(&ready)),
                release: Rc::clone(&release),
                panic_after_release: false,
            });
            let id = StreamId::new(1);
            let request = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            session.start_request_handler(request, handler).unwrap();
            assert_eq!(
                time::timeout(
                    Duration::from_secs(1),
                    ready.wait(CancellationToken::never()),
                )
                .await
                .expect("handler reaches its explicit wait barrier"),
                WaitStatus::Signalled
            );
            session.process_pending_events().unwrap();
            assert!(!session.has_pending_events());
            assert_eq!(session.pending_handler_task_count(), 1);
            assert!(!session.egress_finished(id).unwrap().detached);

            let mut callback_wait = session.pending_event_wait(CancellationToken::never());
            let waiter_polled = Rc::new(Cell::new(false));
            let waiter_poll_probe = Rc::clone(&waiter_polled);
            let waiter = tokio::task::spawn_local(async move {
                poll_fn(|context| {
                    let result = callback_wait.as_mut().poll(context);
                    if result.is_pending() {
                        waiter_poll_probe.set(true);
                    }
                    result
                })
                .await
            });
            time::timeout(Duration::from_secs(1), async {
                while !waiter_polled.get() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("callback waiter receives its first real task poll");
            assert!(!session.has_pending_events());
            release.signal();
            assert_eq!(
                time::timeout(Duration::from_secs(1), waiter)
                    .await
                    .expect("handler completion wakes the suspended waiter")
                    .expect("callback waiter task joins"),
                WaitStatus::Signalled
            );
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .any(|update| update.stream == id && update.detached)
            );
            assert_eq!(session.pending_handler_task_count(), 0);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn queued_handler_completion_wakes_a_late_uniplex_waiter() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let (handler, _) = handler(200);
            let id = StreamId::new(1);
            let request = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            session.start_request_handler(request, handler).unwrap();
            time::timeout(Duration::from_secs(1), async {
                while !session.has_pending_handler_completion() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("handler completion is queued before the late wait begins");

            assert_eq!(
                time::timeout(
                    Duration::from_secs(1),
                    session.pending_event_wait(CancellationToken::never()),
                )
                .await
                .expect("a queued callback retains a sticky wake"),
                WaitStatus::Signalled
            );
            let mut updates = session.process_pending_events().unwrap();
            let mut transfer = updates
                .iter_mut()
                .find_map(|update| update.replacement.take())
                .expect("handler completion installs its response");
            assert_eq!(
                drain_response(&mut session, id, &mut transfer).await,
                (200, 100)
            );
            drop(transfer);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn callback_wait_prioritizes_cancellation_over_queued_events() {
        run_local(async {
            for poll_before_signal in [false, true] {
                let callbacks = StreamCallbacks::new();
                let cancellation = CancellationSource::new();
                let mut wait = Box::pin(callbacks.wait(cancellation.token()));
                let mut context = Context::from_waker(Waker::noop());
                if poll_before_signal {
                    assert!(wait.as_mut().poll(&mut context).is_pending());
                }
                callbacks.push(StreamCallbackEvent::WindowOpen(StreamId::new(1)));
                cancellation.request_cancellation();
                assert_eq!(
                    wait.as_mut().poll(&mut context),
                    Poll::Ready(WaitStatus::Cancelled)
                );
                assert!(matches!(
                    callbacks.pop_front(),
                    Some(StreamCallbackEvent::WindowOpen(_))
                ));
            }
        });
    }

    #[test]
    fn uncancellable_callback_wait_preserves_event_and_retirement_wakes() {
        struct Wakes(AtomicUsize);
        impl Wake for Wakes {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        run_local(async {
            let callbacks = StreamCallbacks::new();
            callbacks.push(StreamCallbackEvent::WindowOpen(StreamId::new(1)));
            callbacks.pop_front();
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            let mut abandoned = Box::pin(callbacks.wait(CancellationToken::never()));
            assert!(abandoned.as_mut().poll(&mut context).is_pending());
            drop(abandoned);

            let mut wait = Box::pin(callbacks.wait(CancellationToken::never()));
            assert!(wait.as_mut().poll(&mut context).is_pending());
            let before = wakes.0.load(Ordering::SeqCst);
            callbacks.wake_retirement();
            assert!(wakes.0.load(Ordering::SeqCst) > before);
            assert_eq!(
                wait.as_mut().poll(&mut context),
                Poll::Ready(WaitStatus::Signalled)
            );
            drop(wait);
            callbacks.acknowledge_retirement();

            let mut wait = Box::pin(callbacks.wait(CancellationToken::never()));
            assert!(wait.as_mut().poll(&mut context).is_pending());
            let before = wakes.0.load(Ordering::SeqCst);
            callbacks.push(StreamCallbackEvent::WindowOpen(StreamId::new(1)));
            assert!(wakes.0.load(Ordering::SeqCst) > before);
            assert_eq!(
                wait.as_mut().poll(&mut context),
                Poll::Ready(WaitStatus::Signalled)
            );
        });
    }

    #[test]
    fn shared_response_acceptance_keeps_conversion_status_and_rejects_late_or_reset_results() {
        struct ConversionError;
        impl HttpHandler for ConversionError {
            fn handle_request(
                &self,
                _context: HandlerSessionContext,
                mut source: HttpSourceHolder,
            ) -> LocalHandlerFuture {
                Box::pin(async move {
                    source.read_header_event().await?;
                    Err(HttpError::with_message(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        b"conversion",
                    ))
                })
            }
        }

        run_local(async {
            for gate in 0..3 {
                let (mut session, _driver) =
                    new_session(TransportDirection::Downstream, CodecProtocol::Http2);
                let counters = Arc::new(ResponseCounters::default());
                let id = StreamId::new(1);
                let request = session.begin_incoming_stream(id).unwrap();
                assert!(
                    session
                        .ingress_headers(id, request_message(None), true)
                        .unwrap()
                );
                session
                    .start_request_handler(
                        request,
                        proposing(Rc::new(ConversionError), &counters, 200),
                    )
                    .unwrap();
                time::timeout(Duration::from_secs(1), async {
                    while !session.has_pending_handler_completion() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(counters.snapshot(), [0; 6]);
                if gate == 1 {
                    assert_eq!(
                        session.response_headers(id, true, false).unwrap().0,
                        ResponseState::Body
                    );
                } else if gate == 2 {
                    session.peer_reset(id, HttpErrorCode::CANCEL).unwrap();
                }
                let mut updates = session.process_pending_events().unwrap();
                if gate == 0 {
                    let update = updates
                        .iter_mut()
                        .find(|update| update.action == StreamAction::SendErrorResponse(500))
                        .unwrap();
                    let mut transfer = update.replacement.take().unwrap();
                    assert_eq!(
                        drain_response(&mut session, id, &mut transfer).await,
                        (500, b"conversion".len())
                    );
                    drop(transfer);
                    assert_eq!(counters.snapshot(), [1, 0, 1, 0, 0, 0]);
                } else {
                    assert!(updates.iter().all(|update| update.replacement.is_none()));
                    if gate == 1 {
                        assert!(updates.iter().any(|update| update.action
                            == StreamAction::Reset {
                                code: HttpErrorCode::INVALID_STATE_TRANSITION,
                                origin: ResetOrigin::Source,
                            }));
                    }
                    assert_eq!(counters.snapshot(), [0; 6]);
                }
                session.finalize().await.unwrap();
            }
        });
    }

    #[test]
    fn handler_panic_selects_500_before_headers_and_source_reset_after_headers() {
        run_local(async {
            assert_handler_panic_before_headers(Rc::new(PanickingHandler)).await;
            assert_handler_panic_before_headers(Rc::new(ConstructionPanickingHandler)).await;
            assert_handler_panic_before_headers(Rc::new(DropPanickingHandler)).await;
            assert_handler_panic_before_headers(Rc::new(DestructorPanickingHandler)).await;

            let (mut late, _late_driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let ready = Rc::new(CancellableBaton::new());
            let release = Rc::new(CancellableBaton::new());
            let handler: HttpHandlerRef = Rc::new(ControlledHandler {
                ready: Some(Rc::clone(&ready)),
                release: Rc::clone(&release),
                panic_after_release: true,
            });
            let late_id = StreamId::new(1);
            let request = late.begin_incoming_stream(late_id).unwrap();
            assert!(
                late.ingress_headers(late_id, request_message(None), true)
                    .unwrap()
            );
            late.start_request_handler(request, handler).unwrap();
            assert_eq!(
                time::timeout(
                    Duration::from_secs(1),
                    ready.wait(CancellationToken::never()),
                )
                .await
                .expect("late panic handler reaches its wait barrier"),
                WaitStatus::Signalled
            );
            late.process_pending_events().unwrap();
            assert_eq!(
                late.response_headers(late_id, true, false).unwrap().0,
                ResponseState::Body
            );
            release.signal();
            time::timeout(Duration::from_secs(1), async {
                while !late.has_pending_handler_completion() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("late handler panic callback is queued");
            assert!(late.process_pending_events().unwrap().iter().any(|update| {
                update.detached
                    && update.action
                        == StreamAction::Reset {
                            code: HttpErrorCode::INTERNAL_ERROR,
                            origin: ResetOrigin::Source,
                        }
            }));
            late.finalize().await.unwrap();
        });
    }

    async fn assert_handler_panic_before_headers(handler: HttpHandlerRef) {
        let counters = Arc::new(ResponseCounters::default());
        let handler = proposing(handler, &counters, 200);
        let (mut session, _driver) =
            new_session(TransportDirection::Downstream, CodecProtocol::Http11);
        let id = StreamId::new(1);
        let request = session.begin_incoming_stream(id).unwrap();
        assert!(
            session
                .ingress_headers(id, request_message(None), true)
                .unwrap()
        );
        session.start_request_handler(request, handler).unwrap();
        time::timeout(Duration::from_secs(1), async {
            while !session.has_pending_handler_completion() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panicking handler completion is queued");
        let mut updates = session.process_pending_events().unwrap();
        assert!(
            updates
                .iter()
                .all(|update| !matches!(update.action, StreamAction::Reset { .. }))
        );
        let update = updates
            .iter_mut()
            .find(|update| update.action == StreamAction::SendErrorResponse(500))
            .expect("handler panic selects a 500 response");
        let mut replacement = update.replacement.take().unwrap();
        let (status, body_bytes) = drain_response(&mut session, id, &mut replacement).await;
        assert_eq!(status, 500);
        assert!(body_bytes > 0);
        drop(replacement);
        session.finalize().await.unwrap();
        assert_eq!(counters.snapshot(), [0; 6]);
    }

    #[test]
    fn admitted_downstream_request_starts_after_graceful_drain() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let (handler, _) = handler(200);
            let id = StreamId::new(1);
            let request = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            assert!(session.initiate_drain());
            assert_eq!(
                session
                    .begin_incoming_stream(StreamId::new(2))
                    .err()
                    .expect("drain closes new admission")
                    .code(),
                HttpErrorCode::REFUSED_STREAM
            );
            session
                .start_request_handler(request, handler)
                .expect("already-admitted work survives drain");
            time::timeout(Duration::from_secs(1), async {
                while !session.has_pending_handler_completion() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("admitted handler completion is queued after drain");
            let mut updates = session.process_pending_events().unwrap();
            let mut transfer = updates
                .iter_mut()
                .find_map(|update| update.replacement.take())
                .expect("admitted handler returns a response");
            assert_eq!(
                drain_response(&mut session, id, &mut transfer).await,
                (200, 100)
            );
            drop(transfer);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn observed_cancellation_precedes_a_ready_request_source_error() {
        run_local(async {
            let (session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let reservation = session.reserve_request().unwrap();
            let cancellation = CancellationSource::new();
            let prepared = SessionCore::prepare_request(
                HttpSourceHolder::new(Box::new(ErrorSource {
                    code: HttpErrorCode::PROTOCOL_ERROR,
                })),
                reservation,
                cancellation.token(),
            );
            assert!(cancellation.request_cancellation());
            assert_eq!(
                prepared
                    .await
                    .err()
                    .expect("observed cancellation wins the completion race")
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(session.num_transactions(), 0);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn typed_incoming_request_rejects_cross_session_wiring() {
        run_local(async {
            let (mut first, _first_driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let (mut second, _second_driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let request = first.begin_incoming_stream(StreamId::new(1)).unwrap();
            let (handler, _) = handler(200);
            assert_eq!(
                second
                    .start_request_handler(request, handler)
                    .expect_err("request identity is session-bound")
                    .code(),
                HttpErrorCode::INVALID_STATE_TRANSITION
            );
            first.process_pending_events().unwrap();
            assert!(
                first
                    .reset_stream_state(
                        StreamId::new(1),
                        HttpError::new(HttpErrorCode::CANCEL),
                        ResetOrigin::Ingress,
                    )
                    .unwrap()
                    .detached
            );
            first.finalize().await.unwrap();
            second.finalize().await.unwrap();
        });
    }

    #[test]
    fn informational_headers_preserve_state_and_are_rejected_after_final() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let id = StreamId::new(1);
            let mut incoming = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), true)
                    .unwrap()
            );
            incoming.source_mut().read_header_event().await.unwrap();

            let source = ScriptedSource::with_headers_and_body(
                [
                    header(response_message(100), false),
                    header(response_message(200), false),
                ],
                HttpBodyEvent::body(BufQueue::new(0), true),
            );
            let mut transfer = session
                .install_egress_source(id, HttpSourceHolder::new(Box::new(source)), true)
                .unwrap();
            let EgressEvent::Headers(mut informational) =
                transfer.next_event().await.unwrap().unwrap()
            else {
                panic!("expected informational headers");
            };
            assert_eq!(informational.headers_mut().status_code().unwrap(), 100);
            assert_eq!(
                session.response_headers(id, false, false).unwrap().0,
                ResponseState::Headers
            );

            let EgressEvent::Headers(mut final_headers) =
                transfer.next_event().await.unwrap().unwrap()
            else {
                panic!("expected final headers");
            };
            assert_eq!(final_headers.headers_mut().status_code().unwrap(), 200);
            assert_eq!(
                session.response_headers(id, true, false).unwrap().0,
                ResponseState::Body
            );
            assert_eq!(
                session
                    .response_headers(id, false, false)
                    .err()
                    .expect("informational headers after final are invalid")
                    .code(),
                HttpErrorCode::INVALID_STATE_TRANSITION
            );
            let body = transfer.next_event().await.unwrap().unwrap();
            assert!(matches!(body, EgressEvent::Body(event) if event.eom()));
            assert!(!session.egress_finished(id).unwrap().detached);
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .any(|update| update.detached)
            );
            drop(transfer);

            let terminal_id = StreamId::new(2);
            let terminal_request = session.begin_incoming_stream(terminal_id).unwrap();
            assert_eq!(
                session.response_headers(terminal_id, true, true).unwrap().0,
                ResponseState::Done
            );
            assert_eq!(
                session
                    .response_headers(terminal_id, false, false)
                    .unwrap()
                    .0,
                ResponseState::Done
            );
            drop(terminal_request);
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .any(|update| update.detached)
            );
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn peer_and_connection_resets_abort_live_ingress_and_detach() {
        run_local(async {
            let events = Rc::new(RefCell::new(Vec::new()));
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http2);
            session.add_lifecycle_observer(Rc::new(EventRecorder {
                events: Rc::clone(&events),
            }));
            let id = StreamId::new(1);
            let mut incoming = session.begin_incoming_stream(id).unwrap();
            let mut read = incoming.source_mut().read_header_event();
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(
                !session
                    .peer_reset(id, HttpErrorCode::PROTOCOL_ERROR)
                    .unwrap()
                    .unwrap()
                    .detached
            );
            assert_eq!(
                read.await.err().expect("peer reset wakes ingress").code(),
                HttpErrorCode::PROTOCOL_ERROR
            );
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .any(|update| update.detached)
            );
            assert!(
                session
                    .peer_reset(id, HttpErrorCode::CANCEL)
                    .unwrap()
                    .is_none()
            );
            assert!(
                session
                    .peer_reset(StreamId::new(99), HttpErrorCode::CANCEL)
                    .unwrap()
                    .is_none()
            );

            let internal = StreamId::new(3);
            let internal_source = session.begin_incoming_stream(internal).unwrap();
            session
                .reset_stream_state(
                    internal,
                    HttpError::new(HttpErrorCode::INTERNAL_ERROR),
                    ResetOrigin::Ingress,
                )
                .unwrap();
            drop(internal_source);
            session.process_pending_events().unwrap();
            let reset_events: Vec<_> = events
                .borrow()
                .iter()
                .copied()
                .filter(|event| matches!(event, LifecycleEvent::PeerReset { .. }))
                .collect();
            assert_eq!(reset_events.len(), 3);

            let mut first = session.begin_incoming_stream(StreamId::new(5)).unwrap();
            let mut second = session.begin_incoming_stream(StreamId::new(7)).unwrap();
            let mut first_future = first.source_mut().read_header_event();
            let mut second_future = second.source_mut().read_header_event();
            assert!(poll_once(first_future.as_mut()).is_pending());
            assert!(poll_once(second_future.as_mut()).is_pending());
            assert_eq!(
                session.fail_connection(HttpError::with_message(
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                    b"connection failed",
                )),
                vec![StreamId::new(5), StreamId::new(7)]
            );
            assert_eq!(
                first_future
                    .await
                    .err()
                    .expect("connection failure wakes first ingress")
                    .code(),
                HttpErrorCode::TRANSPORT_READ_ERROR
            );
            assert_eq!(
                second_future
                    .await
                    .err()
                    .expect("connection failure wakes second ingress")
                    .code(),
                HttpErrorCode::TRANSPORT_READ_ERROR
            );
            let updates = session.process_pending_events().unwrap();
            assert_eq!(updates.iter().filter(|update| update.detached).count(), 2);
            assert_eq!(session.num_transactions(), 0);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn current_egress_failure_aborts_pending_ingress_before_reset() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http2);
            let id = StreamId::new(1);
            let mut incoming = session.begin_incoming_stream(id).unwrap();
            let mut ingress = incoming.source_mut().read_header_event();
            assert!(poll_once(ingress.as_mut()).is_pending());
            let mut egress = session
                .install_egress_source(
                    id,
                    HttpSourceHolder::new(Box::new(ErrorSource {
                        code: HttpErrorCode::INTERNAL_ERROR,
                    })),
                    true,
                )
                .unwrap();
            assert_eq!(
                egress
                    .next_event()
                    .await
                    .err()
                    .expect("egress source fails")
                    .code(),
                HttpErrorCode::INTERNAL_ERROR
            );
            let updates = session.process_pending_events().unwrap();
            assert!(updates.iter().any(|update| {
                update.action
                    == StreamAction::Reset {
                        code: HttpErrorCode::INTERNAL_ERROR,
                        origin: ResetOrigin::Source,
                    }
            }));
            assert_eq!(
                ingress
                    .await
                    .err()
                    .expect("egress failure wakes ingress")
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .any(|update| update.detached)
            );
            drop(egress);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn source_completion_reset_rules_are_directional_and_protocol_specific() {
        run_local(async {
            let (mut upstream, _upstream_driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            let request = ScriptedSource::with_headers_and_body(
                [header(request_message(Some(b"100")), true)],
                HttpBodyEvent::body(BufQueue::new(0), true),
            );
            let reservation = upstream.reserve_request().unwrap();
            let id = StreamId::new(1);
            let prepared = SessionCore::prepare_request(
                HttpSourceHolder::new(Box::new(request)),
                reservation,
                CancellationToken::never(),
            )
            .await
            .unwrap();
            let outgoing = upstream.send_prepared_request(prepared, id).unwrap();
            assert_eq!(
                upstream
                    .stream(id)
                    .unwrap()
                    .egress_content_length_mismatch(),
                Some((100, 0))
            );
            assert!(!upstream.egress_finished(id).unwrap().detached);
            drop(outgoing.response_source);
            let updates = upstream.process_pending_events().unwrap();
            assert!(updates.iter().any(|update| {
                update.action
                    == StreamAction::Reset {
                        code: HttpErrorCode::CANCEL,
                        origin: ResetOrigin::Ingress,
                    }
                    && update.detached
            }));
            upstream.finalize().await.unwrap();

            exercise_downstream_no_error(CodecProtocol::Http11, StreamAction::None).await;
            exercise_downstream_no_error(
                CodecProtocol::Http2,
                StreamAction::Reset {
                    code: HttpErrorCode::NO_ERROR,
                    origin: ResetOrigin::Ingress,
                },
            )
            .await;
        });
    }

    async fn exercise_downstream_no_error(protocol: CodecProtocol, expected_finish: StreamAction) {
        let (mut session, _driver) = new_session(TransportDirection::Downstream, protocol);
        let id = StreamId::new(1);
        let request = session.begin_incoming_stream(id).unwrap();
        let mut response = session
            .install_egress_source(
                id,
                HttpSourceHolder::new(Box::new(HttpFixedSource::new(response_message(200), None))),
                true,
            )
            .unwrap();
        drop(request);
        let updates = session.process_pending_events().unwrap();
        assert!(
            updates
                .iter()
                .all(|update| update.action == StreamAction::None && !update.detached)
        );
        let event = response.next_event().await.unwrap().unwrap();
        assert!(matches!(event, EgressEvent::Headers(event) if event.eom()));
        let (_, finish) = session.response_headers(id, true, true).unwrap();
        assert_eq!(finish.action, expected_finish);
        assert!(!finish.detached);
        assert!(
            session
                .process_pending_events()
                .unwrap()
                .iter()
                .any(|update| update.detached)
        );
        drop(response);
        session.finalize().await.unwrap();
    }

    #[test]
    fn source_failure_replaces_cancelled_producer_for_custom_and_implicit_errors() {
        run_local(async {
            exercise_error_replacement(Some(422), HttpErrorCode::CONTENT_LENGTH_MISMATCH, 422)
                .await;
            exercise_error_replacement(None, HttpErrorCode::READ_TIMEOUT, 408).await;
        });
    }

    #[test]
    fn panicking_egress_stop_still_completes_generation_and_finalizes() {
        run_local(async {
            let events = Rc::new(RefCell::new(Vec::new()));
            let first_log = Rc::new(StopLog::default());
            let second_log = Rc::new(StopLog::default());
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            session.add_lifecycle_observer(Rc::new(EventRecorder {
                events: Rc::clone(&events),
            }));
            let first_id = StreamId::new(1);
            let first_request = session.begin_incoming_stream(first_id).unwrap();
            assert!(
                session
                    .ingress_headers(first_id, request_message(None), true)
                    .unwrap()
            );
            let first_transfer = session
                .install_egress_source(
                    first_id,
                    HttpSourceHolder::new(Box::new(PanickingStopSource {
                        log: Rc::clone(&first_log),
                    })),
                    true,
                )
                .unwrap();
            let second_id = StreamId::new(2);
            let second_request = session.begin_incoming_stream(second_id).unwrap();
            assert!(
                session
                    .ingress_headers(second_id, request_message(None), true)
                    .unwrap()
            );
            let second_transfer = session
                .install_egress_source(
                    second_id,
                    HttpSourceHolder::new(Box::new(NeverSource {
                        log: Rc::clone(&second_log),
                    })),
                    true,
                )
                .unwrap();
            drop(first_request);
            drop(second_request);
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    drop_egress_transfers_for_test(vec![first_transfer, second_transfer]);
                }))
                .is_err()
            );
            assert_eq!(first_log.stops.get(), 1);
            assert_eq!(second_log.stops.get(), 1);
            assert!(
                session
                    .process_pending_events()
                    .unwrap()
                    .iter()
                    .filter(|update| update.detached)
                    .count()
                    >= 2
            );
            session.finalize().await.unwrap();
            assert_eq!(first_log.drops.get(), 1);
            assert_eq!(second_log.drops.get(), 1);
            assert!(events.borrow().contains(&LifecycleEvent::Destroy));
        });
    }

    #[test]
    fn downstream_timeout_preserves_matching_handler_response_and_replaces_mismatch() {
        run_local(async {
            assert_eq!(exercise_timeout_handler_response(408).await, (408, 100));
            assert_eq!(exercise_timeout_handler_response(500).await, (408, 0));
        });
    }

    #[test]
    fn downstream_timeout_propagated_by_handler_maps_to_408() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let id = StreamId::new(1);
            let mut request = session.begin_incoming_stream(id).unwrap();
            request
                .source_mut()
                .set_read_timeout(Duration::from_millis(5));
            assert!(
                session
                    .ingress_headers(id, request_message(Some(b"100")), false)
                    .unwrap()
            );
            session
                .start_request_handler(request, Rc::new(PropagatingIngressTimeoutHandler))
                .unwrap();

            let mut transfer = time::timeout(Duration::from_secs(1), async {
                loop {
                    let mut updates = session.process_pending_events().unwrap();
                    if let Some(transfer) = updates
                        .iter_mut()
                        .find_map(|update| update.replacement.take())
                    {
                        break transfer;
                    }
                    session.pending_event_wait(CancellationToken::never()).await;
                }
            })
            .await
            .expect("propagated ingress timeout selected a response");
            assert_eq!(
                drain_response(&mut session, id, &mut transfer).await,
                (408, 0)
            );
            drop(transfer);
            session.finalize().await.unwrap();
        });
    }

    async fn exercise_timeout_handler_response(status: u16) -> (u16, usize) {
        let (mut session, _driver) =
            new_session(TransportDirection::Downstream, CodecProtocol::Http11);
        let counters = Arc::new(ResponseCounters::default());
        let id = StreamId::new(1);
        let mut request = session.begin_incoming_stream(id).unwrap();
        request
            .source_mut()
            .set_read_timeout(Duration::from_millis(5));
        assert!(
            session
                .ingress_headers(id, request_message(Some(b"100")), false)
                .unwrap()
        );
        session
            .start_request_handler(
                request,
                proposing(
                    Rc::new(IngressTimeoutHandler {
                        status,
                        body_size: 100,
                    }),
                    &counters,
                    status,
                ),
            )
            .unwrap();

        let mut transfer = time::timeout(Duration::from_secs(1), async {
            loop {
                let mut updates = session.process_pending_events().unwrap();
                if let Some(transfer) = updates
                    .iter_mut()
                    .find_map(|update| update.replacement.take())
                {
                    break transfer;
                }
                let wait = session.pending_event_wait(CancellationToken::never());
                let _ = wait.await;
            }
        })
        .await
        .expect("timed-out ingress handler produced a validated response");
        let response = drain_response(&mut session, id, &mut transfer).await;
        drop(transfer);
        session.finalize().await.unwrap();
        assert_eq!(
            counters.snapshot(),
            if status == 408 {
                [1, 0, 0, 0, 1, 0]
            } else {
                [1, 0, 0, 0, 0, 1]
            }
        );
        response
    }

    #[test]
    fn custom_error_replaces_a_pending_handler_response() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let (handler, _) = handler(200);
            let id = StreamId::new(1);
            let request = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(None), false)
                    .unwrap()
            );
            session.start_request_handler(request, handler).unwrap();
            tokio::task::yield_now().await;
            assert!(session.process_pending_events().unwrap().is_empty());
            session.set_error_response_status(id, 422).unwrap();
            tokio::task::yield_now().await;
            let mut updates = session.process_pending_events().unwrap();
            let update = updates
                .iter_mut()
                .find(|update| update.action == StreamAction::SendErrorResponse(422))
                .expect("handler cancellation selects the custom response");
            let mut replacement = update.replacement.take().unwrap();
            assert_eq!(
                drain_response(&mut session, id, &mut replacement).await,
                (422, 0)
            );
            drop(replacement);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn forced_error_response_survives_later_handler_completion_in_same_batch() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Downstream, CodecProtocol::Http11);
            let ready = Rc::new(CancellableBaton::new());
            let release = Rc::new(CancellableBaton::new());
            let handler: HttpHandlerRef = Rc::new(ControlledHandler {
                ready: Some(Rc::clone(&ready)),
                release: Rc::clone(&release),
                panic_after_release: false,
            });
            let counters = Arc::new(ResponseCounters::default());
            let handler = proposing(handler, &counters, 200);
            let id = StreamId::new(1);
            let request = session.begin_incoming_stream(id).unwrap();
            assert!(
                session
                    .ingress_headers(id, request_message(Some(b"1")), false)
                    .unwrap()
            );
            session.start_request_handler(request, handler).unwrap();
            assert_eq!(
                time::timeout(
                    Duration::from_secs(1),
                    ready.wait(CancellationToken::never())
                )
                .await
                .expect("handler consumed its request headers"),
                WaitStatus::Signalled,
            );
            session
                .set_error_response_status_without_handler_cancel_for_test(id, 422)
                .unwrap();
            session
                .stream(id)
                .unwrap()
                .ingress_producer()
                .abort(HttpErrorCode::PROTOCOL_ERROR, b"scripted ingress failure");
            release.signal();
            time::timeout(Duration::from_secs(1), async {
                while !session.has_pending_handler_completion() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("source and readable handler completion are queued together");
            let mut updates = session.process_pending_events().unwrap();
            assert_eq!(
                updates
                    .iter()
                    .filter(|update| update.replacement.is_some())
                    .count(),
                1,
            );
            let update = updates
                .iter_mut()
                .find(|update| update.action == StreamAction::SendErrorResponse(422))
                .expect("forced error source remains authoritative");
            let mut replacement = update.replacement.take().unwrap();
            assert_eq!(
                drain_response(&mut session, id, &mut replacement).await,
                (422, 0),
            );
            drop(replacement);
            session.finalize().await.unwrap();
            assert_eq!(counters.snapshot(), [0; 6]);
        });
    }

    async fn exercise_error_replacement(
        custom_status: Option<u16>,
        source_error: HttpErrorCode,
        expected_status: u16,
    ) {
        let (mut session, _driver) =
            new_session(TransportDirection::Downstream, CodecProtocol::Http11);
        let id = StreamId::new(1);
        let mut incoming = session.begin_incoming_stream(id).unwrap();
        let stop_log = Rc::new(StopLog::default());
        let mut old_transfer = session
            .install_egress_source(
                id,
                HttpSourceHolder::new(Box::new(NeverSource {
                    log: Rc::clone(&stop_log),
                })),
                true,
            )
            .unwrap();
        let old_generation = old_transfer.generation();
        let mut old_read = Some(Box::pin(old_transfer.next_event()));
        assert!(poll_once(old_read.as_mut().unwrap().as_mut()).is_pending());

        if let Some(status) = custom_status {
            session.set_error_response_status(id, status).unwrap();
            assert!(session.stream(id).unwrap().ingress_cancel_requested());
            assert_eq!(
                old_read
                    .take()
                    .unwrap()
                    .await
                    .err()
                    .expect("custom status wakes the old producer first")
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
            let early = session.process_pending_events().unwrap();
            assert!(
                early
                    .iter()
                    .all(|update| update.action == StreamAction::None)
            );
            assert_eq!(
                session.stream(id).unwrap().error_response_status(),
                Some(status)
            );
            assert!(
                session
                    .ingress_headers(id, request_message(Some(b"1")), true)
                    .unwrap()
            );
            assert_eq!(
                incoming
                    .source_mut()
                    .read_header_event()
                    .await
                    .err()
                    .expect("content-length mismatch reaches the consumer")
                    .code(),
                source_error
            );
        } else {
            incoming
                .source_mut()
                .set_read_timeout(Duration::from_millis(1));
            let mut read = incoming.source_mut().read_header_event();
            assert!(poll_once(read.as_mut()).is_pending());
            time::sleep(Duration::from_millis(2)).await;
            assert_eq!(
                read.await
                    .err()
                    .expect("timeout reaches the consumer")
                    .code(),
                source_error
            );
        }

        let mut updates = session.process_pending_events().unwrap();
        let update = updates
            .iter_mut()
            .find(|update| update.action == StreamAction::SendErrorResponse(expected_status))
            .expect("source error creates a replacement response");
        let mut replacement = update.replacement.take().expect("replacement producer");
        assert_ne!(replacement.generation(), old_generation);
        assert_eq!(stop_log.stops.get(), 1);
        assert_eq!(stop_log.drops.get(), 1);

        if let Some(old_read) = old_read.take() {
            assert_eq!(
                old_read
                    .await
                    .err()
                    .expect("replacement wakes the old egress read")
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
        }
        drop(old_read);
        assert!(old_transfer.cancellation_requested());
        let stale_updates = session.process_pending_events().unwrap();
        assert!(
            stale_updates
                .iter()
                .all(|update| update.action == StreamAction::None)
        );
        drop(old_transfer);
        assert_eq!(
            drain_response(&mut session, id, &mut replacement).await,
            (expected_status, 0)
        );
        drop(replacement);
        session.finalize().await.unwrap();
    }

    fn position(log: &[&'static str], value: &'static str) -> usize {
        log.iter()
            .position(|entry| *entry == value)
            .expect("event is present")
    }

    #[derive(Default)]
    struct CountingWake(AtomicUsize);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn callback_wait_rearms_after_a_wake_without_pending_events() {
        run_local(async {
            for cancellable in [false, true] {
                let callbacks = StreamCallbacks::new();
                let cancellation = CancellationSource::new();
                let token = if cancellable {
                    cancellation.token()
                } else {
                    CancellationToken::never()
                };
                let wakes = Arc::new(CountingWake::default());
                let waker = Waker::from(Arc::clone(&wakes));
                let mut context = Context::from_waker(&waker);
                let mut wait = Box::pin(callbacks.wait(token));
                assert!(wait.as_mut().poll(&mut context).is_pending());

                // A wake whose event another consumer drained leaves nothing pending.
                callbacks.push(StreamCallbackEvent::WindowOpen(StreamId::new(1)));
                assert!(callbacks.pop_front().is_some());
                assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
                assert!(wait.as_mut().poll(&mut context).is_pending());
                assert!(
                    !callbacks.has_stored_wake_for_test(),
                    "the stale wake was consumed, not left for the next wait"
                );

                // The re-armed wait still observes the next event.
                callbacks.push(StreamCallbackEvent::WindowOpen(StreamId::new(1)));
                assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
                assert_eq!(
                    wait.as_mut().poll(&mut context),
                    Poll::Ready(WaitStatus::Signalled)
                );
                assert!(callbacks.pop_front().is_some());
                drop(wait);

                if cancellable {
                    // After a stale wake, a cancelled token ends the wait.
                    let mut wait = Box::pin(callbacks.wait(cancellation.token()));
                    assert!(wait.as_mut().poll(&mut context).is_pending());
                    callbacks.push(StreamCallbackEvent::WindowOpen(StreamId::new(1)));
                    assert!(callbacks.pop_front().is_some());
                    assert!(cancellation.request_cancellation());
                    assert_eq!(
                        wait.as_mut().poll(&mut context),
                        Poll::Ready(WaitStatus::Cancelled)
                    );
                }
            }
        });
    }

    #[test]
    fn callback_wait_checks_cancellation_before_consuming_a_permit() {
        run_local(async {
            let callbacks = StreamCallbacks::new();
            let cancellation = CancellationSource::new();
            let mut context = Context::from_waker(Waker::noop());

            callbacks.push(StreamCallbackEvent::WindowOpen(StreamId::new(1)));
            assert!(callbacks.has_stored_wake_for_test());
            assert!(cancellation.request_cancellation());
            let mut cancelled = Box::pin(callbacks.wait(cancellation.token()));
            assert_eq!(
                cancelled.as_mut().poll(&mut context),
                Poll::Ready(WaitStatus::Cancelled)
            );
            drop(cancelled);
            assert!(
                callbacks.has_stored_wake_for_test(),
                "a cancelled wait leaves the stored permit"
            );

            let mut signalled = Box::pin(callbacks.wait(CancellationToken::never()));
            assert_eq!(
                signalled.as_mut().poll(&mut context),
                Poll::Ready(WaitStatus::Signalled)
            );
            drop(signalled);
            assert!(
                !callbacks.has_stored_wake_for_test(),
                "a wait that finds a pending event consumes exactly its permit"
            );
            assert!(callbacks.pop_front().is_some());
            assert!(callbacks.pop_front().is_none());

            let mut idle = Box::pin(callbacks.wait(CancellationToken::never()));
            assert!(idle.as_mut().poll(&mut context).is_pending());
        });
    }

    struct ClosureDrop {
        label: &'static str,
        log: Rc<RefCell<Vec<&'static str>>>,
    }

    impl Drop for ClosureDrop {
        fn drop(&mut self) {
            self.log.borrow_mut().push(self.label);
        }
    }

    #[test]
    fn start_read_write_tasks_after_fail_connection_fails_closed_then_already_started() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            session.fail_connection(HttpError::new(HttpErrorCode::CONNECT_ERROR));
            let log = Rc::new(RefCell::new(Vec::new()));
            let read = ClosureDrop {
                label: "read",
                log: Rc::clone(&log),
            };
            let write = ClosureDrop {
                label: "write",
                log: Rc::clone(&log),
            };
            let started = session.start_read_write_tasks(
                move |_| {
                    let _read = read;
                    async {}
                },
                move |_| {
                    let _write = write;
                    async {}
                },
            );
            assert_eq!(
                started,
                Err(SessionTaskError::TaskGroup(TaskGroupError::Closed))
            );
            assert_eq!(
                *log.borrow(),
                ["read", "write"],
                "the rejected spawn drops the read closure before the write closure"
            );
            assert_eq!(
                session.start_read_write_tasks(|_| async {}, |_| async {}),
                Err(SessionTaskError::AlreadyStarted)
            );
            assert_eq!(session.pending_task_count(), 0);
            session.finalize().await.unwrap();
        });
    }

    #[test]
    fn start_read_write_tasks_after_join_with_nothing_started_fails_closed() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            session.join_read_write_tasks().await.unwrap();
            assert_eq!(
                session.start_read_write_tasks(|_| async {}, |_| async {}),
                Err(SessionTaskError::TaskGroup(TaskGroupError::Closed))
            );
            session.finalize().await.unwrap();

            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            session.cancel_and_join_read_write_tasks().await.unwrap();
            assert_eq!(
                session.start_read_write_tasks(|_| async {}, |_| async {}),
                Err(SessionTaskError::TaskGroup(TaskGroupError::Closed))
            );
            session.finalize().await.unwrap();
        });
    }

    async fn header_only_mismatch(content_length: Option<&'static [u8]>) -> Option<(u64, u64)> {
        let (mut upstream, _driver) =
            new_session(TransportDirection::Upstream, CodecProtocol::Http11);
        let request = ScriptedSource::with_headers_and_body(
            [header(request_message(content_length), true)],
            HttpBodyEvent::body(BufQueue::new(0), true),
        );
        let reservation = upstream.reserve_request().unwrap();
        let id = StreamId::new(1);
        let prepared = SessionCore::prepare_request(
            HttpSourceHolder::new(Box::new(request)),
            reservation,
            CancellationToken::never(),
        )
        .await
        .unwrap();
        let outgoing = upstream.send_prepared_request(prepared, id).unwrap();
        let mismatch = upstream
            .stream(id)
            .unwrap()
            .egress_content_length_mismatch();
        upstream.egress_finished(id).unwrap();
        drop(outgoing.response_source);
        upstream.process_pending_events().unwrap();
        upstream.finalize().await.unwrap();
        mismatch
    }

    #[test]
    fn header_only_request_without_content_length_records_no_mismatch() {
        run_local(async {
            assert_eq!(header_only_mismatch(None).await, None);
            assert_eq!(header_only_mismatch(Some(b"100")).await, Some((100, 0)));
        });
    }

    #[test]
    fn header_only_request_with_zero_content_length_records_no_mismatch() {
        run_local(async {
            assert_eq!(header_only_mismatch(Some(b"0")).await, None);
            assert_eq!(header_only_mismatch(Some(b"7")).await, Some((7, 0)));
        });
    }

    #[test]
    fn unused_session_task_set_cancel_returns_true_then_false() {
        run_local(async {
            let mut tasks = SessionTaskSet::new();
            assert!(tasks.cancel(), "the first cancellation request is new");
            assert!(!tasks.cancel(), "a repeated cancellation request is not");
            assert!(tasks.is_empty());
        });
    }

    #[test]
    fn upstream_session_finalizes_and_drops_without_creating_task_groups() {
        run_local(async {
            let (mut session, _driver) =
                new_session(TransportDirection::Upstream, CodecProtocol::Http11);
            assert_eq!(session.task_groups_created_for_test(), (false, false));
            let mut reservation = session.reserve_request().unwrap();
            assert!(reservation.cancel());
            session.process_pending_events().unwrap();
            assert_eq!(session.pending_task_count(), 0);
            assert_eq!(session.pending_handler_task_count(), 0);
            assert_eq!(session.task_groups_created_for_test(), (false, false));
            session.finalize().await.unwrap();
        });
    }

    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % bound
        }
    }

    const SMALL_MAP_KEYS: u32 = 6;

    fn assert_same_map(small: &SmallMap<u32, u64>, tree: &BTreeMap<u32, u64>) {
        assert_eq!(small.len(), tree.len());
        assert_eq!(small.is_empty(), tree.is_empty());
        assert!(small.iter().eq(tree.iter()));
        assert!(small.iter().rev().eq(tree.iter().rev()));
        assert!(small.keys().eq(tree.keys()));
        assert!(small.values().rev().eq(tree.values().rev()));
        assert_eq!(small.keys().next(), tree.keys().next());
        for key in 0..SMALL_MAP_KEYS {
            assert_eq!(small.get(&key), tree.get(&key));
            assert_eq!(small.contains_key(&key), tree.contains_key(&key));
            if let Some(value) = tree.get(&key) {
                assert_eq!(small[&key], *value);
            }
        }
    }

    fn rebuilt_values(entries: &[(u32, u64)], rotation: usize) -> (Vec<u64>, Vec<u64>) {
        let mut small = SmallMap::new();
        let mut tree = BTreeMap::new();
        for index in 0..entries.len() {
            let (key, value) = entries[(index + rotation) % entries.len()];
            assert_eq!(small.insert(key, value), tree.insert(key, value));
        }
        (small.into_values().collect(), tree.into_values().collect())
    }

    #[test]
    fn small_map_matches_btree_map_under_generated_operations() {
        let mut small = SmallMap::<u32, u64>::default();
        let mut tree = BTreeMap::<u32, u64>::new();
        assert!(small.is_inline_for_test());

        // Out-of-order inserts keep ascending order inline; the third key grows a
        // tree that stays a tree through removal down to zero and growth again.
        for (key, smallest, inline) in [(4, 4, true), (1, 1, true), (2, 1, false)] {
            assert_eq!(
                small.insert(key, u64::from(key)),
                tree.insert(key, u64::from(key))
            );
            assert_eq!(small.keys().next(), Some(&smallest));
            assert_eq!(small.is_inline_for_test(), inline);
            assert_same_map(&small, &tree);
        }
        for key in [2, 4, 1] {
            assert_eq!(small.remove(&key), tree.remove(&key));
            assert!(!small.is_inline_for_test());
            assert_same_map(&small, &tree);
        }
        for key in [5, 0, 3] {
            assert_eq!(small.insert(key, 9), tree.insert(key, 9));
            assert!(!small.is_inline_for_test());
            assert_same_map(&small, &tree);
        }
        let (taken_small, taken_tree) = (std::mem::take(&mut small), std::mem::take(&mut tree));
        assert!(small.is_inline_for_test());
        assert!(taken_small.into_values().eq(taken_tree.into_values()));

        let mut random = Lcg(0x5eed);
        let mut grown = false;
        for step in 0..20_000_u64 {
            let key = random.next(u64::from(SMALL_MAP_KEYS)) as u32;
            let value = random.next(1_000);
            match random.next(11) {
                0 | 1 => assert_eq!(small.insert(key, value), tree.insert(key, value)),
                2 | 3 => assert_eq!(small.remove(&key), tree.remove(&key)),
                4 => {
                    let small_value = small.entry(key).or_insert_with(|| value);
                    let tree_value = tree.entry(key).or_insert_with(|| value);
                    assert_eq!(*small_value, *tree_value);
                    *small_value += 1;
                    *tree_value += 1;
                }
                5 => {
                    *small.entry(key).or_default() += 2;
                    *tree.entry(key).or_default() += 2;
                }
                6 => match (small.entry(key), tree.entry(key)) {
                    (
                        Entry::Occupied(mut small_entry),
                        btree_map::Entry::Occupied(mut tree_entry),
                    ) => {
                        assert_eq!(small_entry.get(), tree_entry.get());
                        *small_entry.get_mut() += 3;
                        *tree_entry.get_mut() += 3;
                    }
                    (Entry::Vacant(_), btree_map::Entry::Vacant(_)) => {}
                    _ => panic!("entry occupancy differs at step {step}"),
                },
                7 => match (small.entry(key), tree.entry(key)) {
                    (Entry::Occupied(small_entry), btree_map::Entry::Occupied(tree_entry)) => {
                        assert_eq!(small_entry.remove(), tree_entry.remove());
                    }
                    (Entry::Vacant(_), btree_map::Entry::Vacant(_)) => {}
                    _ => panic!("entry occupancy differs at step {step}"),
                },
                8 => {
                    if let (Some(small_value), Some(tree_value)) =
                        (small.get_mut(&key), tree.get_mut(&key))
                    {
                        *small_value += 4;
                        *tree_value += 4;
                    }
                    assert_eq!(small.get(&key), tree.get(&key));
                }
                9 => {
                    let modulus = random.next(3) + 2;
                    let mut small_visits = Vec::new();
                    let mut tree_visits = Vec::new();
                    small.retain(|key, value| {
                        small_visits.push(*key);
                        *value += 1;
                        u64::from(*key) % modulus != 0
                    });
                    tree.retain(|key, value| {
                        tree_visits.push(*key);
                        *value += 1;
                        u64::from(*key) % modulus != 0
                    });
                    assert_eq!(small_visits, tree_visits);
                }
                _ => {
                    if random.next(8) == 0 {
                        let (taken_small, taken_tree) =
                            (std::mem::take(&mut small), std::mem::take(&mut tree));
                        assert!(taken_small.into_values().eq(taken_tree.into_values()));
                        grown = false;
                    } else {
                        let entries: Vec<_> = tree.iter().map(|(k, v)| (*k, *v)).collect();
                        if !entries.is_empty() {
                            let (small_values, tree_values) =
                                rebuilt_values(&entries, step as usize % entries.len());
                            assert_eq!(small_values, tree_values);
                        }
                    }
                }
            }
            grown |= tree.len() > 2;
            assert_eq!(small.is_inline_for_test(), !grown, "step {step}");
            assert_same_map(&small, &tree);
        }
    }

    #[test]
    fn small_set_insert_and_remove_return_btree_set_results() {
        let mut small = SmallSet::new();
        let mut tree = BTreeSet::new();
        let mut random = Lcg(0xb7);
        for _ in 0..5_000 {
            let value = StreamId::new(random.next(5) * 2 + 1);
            if random.next(2) == 0 {
                assert_eq!(small.insert(value), tree.insert(value));
            } else {
                assert_eq!(small.remove(&value), tree.remove(&value));
            }
            assert_eq!(small.len(), tree.len());
            for candidate in (1..10).step_by(2).map(StreamId::new) {
                assert_eq!(small.contains(&candidate), tree.contains(&candidate));
            }
        }
    }

    struct RecordedDrop {
        key: u32,
        log: Rc<RefCell<Vec<u32>>>,
        panic: bool,
    }

    impl Drop for RecordedDrop {
        fn drop(&mut self) {
            self.log.borrow_mut().push(self.key);
            if self.panic {
                panic!("recorded drop panic");
            }
        }
    }

    fn recorded_maps(
        keys: &[u32],
        panic_key: Option<u32>,
    ) -> (
        SmallMap<u32, RecordedDrop>,
        Rc<RefCell<Vec<u32>>>,
        BTreeMap<u32, RecordedDrop>,
        Rc<RefCell<Vec<u32>>>,
    ) {
        let small_log = Rc::new(RefCell::new(Vec::new()));
        let tree_log = Rc::new(RefCell::new(Vec::new()));
        let mut small = SmallMap::new();
        let mut tree = BTreeMap::new();
        for &key in keys {
            let panic = panic_key == Some(key);
            assert!(
                small
                    .insert(
                        key,
                        RecordedDrop {
                            key,
                            log: Rc::clone(&small_log),
                            panic,
                        },
                    )
                    .is_none()
            );
            assert!(
                tree.insert(
                    key,
                    RecordedDrop {
                        key,
                        log: Rc::clone(&tree_log),
                        panic,
                    },
                )
                .is_none()
            );
        }
        (small, small_log, tree, tree_log)
    }

    #[test]
    fn small_map_drops_remaining_entries_in_ascending_order_after_a_panic() {
        for keys in [&[7, 3][..], &[9, 1, 5, 3, 7][..]] {
            let first = *keys.iter().min().unwrap();
            let (small, small_log, tree, tree_log) = recorded_maps(keys, Some(first));
            assert!(catch_unwind(AssertUnwindSafe(|| drop(small))).is_err());
            assert!(catch_unwind(AssertUnwindSafe(|| drop(tree))).is_err());
            let mut ascending = keys.to_vec();
            ascending.sort_unstable();
            assert_eq!(*small_log.borrow(), ascending);
            assert_eq!(*small_log.borrow(), *tree_log.borrow());

            // A partly consumed iterator keeps dropping after a panicking value.
            let second = ascending[1];
            let (small, small_log, tree, tree_log) = recorded_maps(keys, Some(second));
            let mut small_values = small.into_values();
            let mut tree_values = tree.into_values();
            drop(small_values.next());
            drop(tree_values.next());
            assert!(catch_unwind(AssertUnwindSafe(|| drop(small_values))).is_err());
            assert!(catch_unwind(AssertUnwindSafe(|| drop(tree_values))).is_err());
            assert_eq!(*small_log.borrow(), ascending);
            assert_eq!(*small_log.borrow(), *tree_log.borrow());
        }
    }

    #[derive(Debug, PartialEq)]
    enum RetainEvent {
        Visit(u32),
        Drop(u32),
    }

    struct RetainValue {
        key: u32,
        log: Rc<RefCell<Vec<RetainEvent>>>,
        panic: bool,
    }

    impl Drop for RetainValue {
        fn drop(&mut self) {
            self.log.borrow_mut().push(RetainEvent::Drop(self.key));
            if self.panic {
                panic!("retained value drop panic");
            }
        }
    }

    fn retain_log<M>(
        insert: impl Fn(&mut M, u32, RetainValue),
        retain: impl FnOnce(&mut M, &mut dyn FnMut(&u32, &mut RetainValue) -> bool),
        keys: &[u32],
        mut map: M,
        drop_panic: Option<u32>,
        predicate_panic: Option<u32>,
        remaining: impl Fn(&M) -> Vec<u32>,
    ) -> (bool, Vec<RetainEvent>, Vec<u32>) {
        let log = Rc::new(RefCell::new(Vec::new()));
        for &key in keys {
            insert(
                &mut map,
                key,
                RetainValue {
                    key,
                    log: Rc::clone(&log),
                    panic: drop_panic == Some(key),
                },
            );
        }
        let visit_log = Rc::clone(&log);
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            retain(&mut map, &mut |key, _value| {
                visit_log.borrow_mut().push(RetainEvent::Visit(*key));
                if predicate_panic == Some(*key) {
                    panic!("retain predicate panic");
                }
                key % 2 == 0
            });
        }))
        .is_err();
        let left = remaining(&map);
        let events = std::mem::take(&mut *log.borrow_mut());
        drop(map);
        (panicked, events, left)
    }

    #[test]
    fn small_map_retain_drops_removed_entries_in_order_and_survives_a_panic() {
        for keys in [&[4, 1][..], &[3][..], &[5, 2, 1, 4, 3, 6][..]] {
            for (drop_panic, predicate_panic) in [
                (None, None),
                (Some(3), None),
                (Some(1), None),
                (None, Some(4)),
                (None, Some(5)),
            ] {
                let small = retain_log(
                    |map: &mut SmallMap<u32, RetainValue>, key, value| {
                        assert!(map.insert(key, value).is_none());
                    },
                    |map, keep| map.retain(|key, value| keep(key, value)),
                    keys,
                    SmallMap::new(),
                    drop_panic,
                    predicate_panic,
                    |map| map.keys().copied().collect(),
                );
                let tree = retain_log(
                    |map: &mut BTreeMap<u32, RetainValue>, key, value| {
                        assert!(map.insert(key, value).is_none());
                    },
                    |map, keep| map.retain(|key, value| keep(key, value)),
                    keys,
                    BTreeMap::new(),
                    drop_panic,
                    predicate_panic,
                    |map| map.keys().copied().collect(),
                );
                assert_eq!(small, tree, "keys {keys:?}");
            }
        }
    }

    #[test]
    fn small_map_entry_closure_panic_leaves_the_map_unchanged() {
        for keys in [&[][..], &[2_u32][..], &[2, 4][..], &[2, 4, 6][..]] {
            let mut small = SmallMap::new();
            for &key in keys {
                small.insert(key, u64::from(key));
            }
            let inline = small.is_inline_for_test();
            let before: Vec<_> = small.iter().map(|(k, v)| (*k, *v)).collect();
            for key in [1, 3, 5, 7] {
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        small
                            .entry(key)
                            .or_insert_with(|| panic!("entry value panic"));
                    }))
                    .is_err()
                );
                let after: Vec<_> = small.iter().map(|(k, v)| (*k, *v)).collect();
                assert_eq!(after, before);
                assert_eq!(small.is_inline_for_test(), inline);
                assert!(!small.contains_key(&key));
            }
        }
    }

    #[test]
    #[ignore = "microbenchmark; run in @fbcode//mode/opt with --run-ignored"]
    fn small_map_many_stream_operation_cost() {
        fn measure(label: &str, streams: u64, mut run: impl FnMut() -> u64) {
            let rounds = (200_000 / streams).max(10);
            let start = std::time::Instant::now();
            let mut checksum = 0_u64;
            for _ in 0..rounds {
                checksum = checksum.wrapping_add(run());
            }
            let elapsed = start.elapsed();
            println!(
                "{label} streams={streams} ns_per_stream={:.1} checksum={checksum}",
                elapsed.as_nanos() as f64 / (rounds * streams) as f64
            );
        }

        for streams in [1_u64, 2, 3, 100, 1_000] {
            measure("small_map", streams, || {
                let mut map = SmallMap::new();
                for id in (0..streams).map(|id| id * 2 + 1) {
                    map.insert(StreamId::new(id), id);
                }
                let mut sum = map.values().sum::<u64>();
                for id in (0..streams).map(|id| id * 2 + 1) {
                    sum += map.get(&StreamId::new(id)).copied().unwrap_or_default();
                }
                for id in (0..streams).map(|id| id * 2 + 1) {
                    sum += map.remove(&StreamId::new(id)).unwrap_or_default();
                }
                std::hint::black_box(sum)
            });
            measure("btree_map", streams, || {
                let mut map = BTreeMap::new();
                for id in (0..streams).map(|id| id * 2 + 1) {
                    map.insert(StreamId::new(id), id);
                }
                let mut sum = map.values().sum::<u64>();
                for id in (0..streams).map(|id| id * 2 + 1) {
                    sum += map.get(&StreamId::new(id)).copied().unwrap_or_default();
                }
                for id in (0..streams).map(|id| id * 2 + 1) {
                    sum += map.remove(&StreamId::new(id)).unwrap_or_default();
                }
                std::hint::black_box(sum)
            });
        }
    }
}
