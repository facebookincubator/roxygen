// Owned HTTP/1.1 downstream driver tests. Each byte comparison runs `UniplexSession` HTTP/1.1
// with the same input, the same zero-timeout `UniplexConfig` and the same scripted response,
// whose fixed `date` keeps the clock out of the comparison.

use std::task::Context;
use std::task::Waker;

use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::stream_source::HttpStreamSourceOwner;

use super::*;
use crate::external_host_tests::YieldMode;
use crate::external_host_tests::run_on_host;
use crate::external_host_tests::run_on_host_with;
use crate::owned::OwnedApplication;
use crate::owned::OwnedExchange;
use crate::owned_h1::Http1DriverConfig;
use crate::owned_h1::Http1UpgradePolicy;
use crate::owned_h1::OwnedH1Handle;
use crate::owned_h1::OwnedH1Options;
use crate::owned_h1::OwnedH1Session;
use crate::uniplex::UniplexWriteCompletion;
use crate::uniplex::UniplexWriteQueue;

const DATE: &[u8] = b"Tue, 03 Nov 2026 00:00:00 GMT";

/// The configuration both sessions share in every comparison.
fn h1_config() -> UniplexConfig {
    UniplexConfig {
        read_timeout: Duration::ZERO,
        stream_read_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        drain_request_body_on_drop: true,
        ..UniplexConfig::default()
    }
}

/// One scripted response, replayed by both the owned exchange and the Uniplex handler.
#[derive(Clone)]
struct Response {
    status: u16,
    body: Vec<u8>,
    content_length: bool,
    /// A `Content-Length` sent in place of the body's length, as a HEAD response sends one.
    declared_length: Option<usize>,
    /// A trailer section sent after the body, which then does not end the response.
    trailers: Vec<(&'static str, &'static str)>,
    /// Informational statuses sent, in order, before the request body is read.
    interim: Vec<u16>,
    /// The head is sent before the request body is read, and the body after it ends, as a
    /// tunnel's answer is.
    tunnel: bool,
    /// The head accepts a WebSocket upgrade.
    websocket: bool,
}

impl Response {
    fn fixed(status: u16, body: &[u8]) -> Self {
        Self {
            status,
            body: body.to_vec(),
            content_length: true,
            declared_length: None,
            trailers: Vec::new(),
            interim: Vec::new(),
            tunnel: false,
            websocket: false,
        }
    }

    /// Accepts a tunnel with `status`, then sends `body` once the tunnel's ingress ends.
    fn tunnel(status: u16, body: &[u8], websocket: bool) -> Self {
        Self {
            content_length: false,
            tunnel: true,
            websocket,
            ..Self::fixed(status, body)
        }
    }

    fn interim_events(&self) -> VecDeque<HttpHeaderEvent> {
        self.interim
            .iter()
            .map(|status| {
                let mut message = HttpMessage::new();
                message.set_status_code(*status).unwrap();
                message.set_http_version(1, 1);
                message.headers_mut().set(HttpHeaderCode::Date, DATE);
                HttpHeaderEvent::new(message, false).unwrap()
            })
            .collect()
    }

    /// A body-less response that declares `length`, as a response to HEAD does.
    fn declaring(status: u16, length: usize) -> Self {
        Self {
            declared_length: Some(length),
            ..Self::fixed(status, b"")
        }
    }

    fn events(&self) -> (HttpHeaderEvent, VecDeque<HttpBodyEvent>) {
        let mut message = HttpMessage::new();
        message.set_status_code(self.status).unwrap();
        message.set_http_version(1, 1);
        if self.content_length {
            message.headers_mut().set(
                HttpHeaderCode::ContentLength,
                self.declared_length
                    .unwrap_or(self.body.len())
                    .to_string()
                    .as_bytes(),
            );
        }
        message.headers_mut().set(HttpHeaderCode::Date, DATE);
        if self.websocket {
            message.set_egress_websocket_upgrade();
        }
        let header_eom = self.body.is_empty() && self.trailers.is_empty();
        let header = HttpHeaderEvent::new(message, header_eom).unwrap();
        let mut body = VecDeque::new();
        if !self.body.is_empty() {
            body.push_back(HttpBodyEvent::body(
                queue(&self.body),
                self.trailers.is_empty(),
            ));
        }
        if !self.trailers.is_empty() {
            let mut trailers = HttpHeaders::new();
            for (name, value) in &self.trailers {
                trailers.add(name.as_bytes(), value.as_bytes());
            }
            body.push_back(HttpBodyEvent::trailers(trailers));
        }
        (header, body)
    }
}

/// What the application of one exchange observed.
#[derive(Default)]
struct H1Probe {
    method: RefCell<Option<HttpMethod>>,
    url: RefCell<Vec<u8>>,
    /// The request head's fields, in order.
    fields: RefCell<Vec<(Vec<u8>, Vec<u8>)>>,
    /// The error a request body read returned.
    request_error: Cell<Option<HttpErrorCode>>,
    request_eom: Cell<bool>,
    request_body: RefCell<Vec<u8>>,
    request_finished: Cell<bool>,
    /// Each request trailers event: its fields, and whether it carried EOM.
    request_trailers: RefCell<Vec<(Vec<(Vec<u8>, Vec<u8>)>, bool)>>,
    response_complete: Cell<usize>,
    completed: Cell<usize>,
    cancelled: RefCell<Vec<(HttpErrorCode, bool)>>,
    dropped: Cell<usize>,
    headers_encoded: RefCell<Vec<u32>>,
    headers_polled: Cell<usize>,
    /// Each byte event registered on the response: `None` if it fired, else the cancel code.
    byte_events: RefCell<Vec<Option<HttpErrorCode>>>,
    /// The error of each `stop_reading` call on the response source `ScriptHandler` returns.
    response_stopped: RefCell<Vec<Option<HttpErrorCode>>>,
    /// The application's `cancel_in`, for the `byte_event` callback.
    cancel_in: CancelIn,
}

impl HttpByteEventCallback for H1Probe {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        self.byte_events.borrow_mut().push(None);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.byte_events.borrow_mut().push(Some(error.code()));
        cancel_from(&self.cancel_in, "byte_event");
    }
}

thread_local! {
    /// Exchange lifecycle events on this test thread, in order: (event, exchange ordinal).
    static H1_EVENTS: RefCell<Vec<(&'static str, usize)>> = const { RefCell::new(Vec::new()) };
}

fn note(event: &'static str, ordinal: usize) {
    H1_EVENTS.with(|events| events.borrow_mut().push((event, ordinal)));
}

/// Takes this thread's exchange lifecycle events.
fn take_events() -> Vec<(&'static str, usize)> {
    H1_EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

struct H1App {
    responses: Vec<Response>,
    probes: Rc<RefCell<Vec<Rc<H1Probe>>>>,
    /// Each exchange starts reading its request body only once this is notified.
    body_gate: Option<Rc<tokio::sync::Notify>>,
    /// Each exchange drops its request body unread and answers at once.
    respond_early: bool,
    /// Each response head asks to be told its encoded size, and every response event registers
    /// a transport-write byte event.
    instrument: bool,
    /// Once set, each exchange cancels the session through this handle from the named
    /// callback: `new_exchange`, `request`, `poll_headers`, `poll_body`, `response_complete` or
    /// (instrumented) `byte_event` or `headers_encoded`.
    cancel_in: CancelIn,
    /// Once set, each exchange drains the session through this handle from the named callback:
    /// `request` or `poll_headers`.
    drain_in: CancelIn,
    /// The first exchange panics from this point of its retirement: `complete` or `drop`.
    panic_in: Option<&'static str>,
}

type CancelIn = Rc<RefCell<Option<(&'static str, OwnedH1Handle)>>>;

/// The handle in `slot` if `callback` is the one it names.
fn handle_for(slot: &CancelIn, callback: &str) -> Option<OwnedH1Handle> {
    slot.borrow()
        .as_ref()
        .filter(|(name, _)| *name == callback)
        .map(|(_, handle)| handle.clone())
}

/// Cancels the session if `callback` is the one `cancel_in` names.
fn cancel_from(cancel_in: &CancelIn, callback: &str) {
    if let Some(handle) = handle_for(cancel_in, callback) {
        handle.cancel();
    }
}

/// Drains the session if `callback` is the one `drain_in` names.
fn drain_from(drain_in: &CancelIn, callback: &str) {
    if let Some(handle) = handle_for(drain_in, callback) {
        assert!(handle.initiate_drain().unwrap());
    }
}

/// A sendable copy of one exchange's observations, taken after the run.
#[derive(Clone, Debug)]
struct Observed {
    method: Option<HttpMethod>,
    url: Vec<u8>,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
    request_error: Option<HttpErrorCode>,
    request_eom: bool,
    request_body: Vec<u8>,
    request_finished: bool,
    request_trailers: Vec<(Vec<(Vec<u8>, Vec<u8>)>, bool)>,
    response_complete: usize,
    completed: usize,
    cancelled: Vec<(HttpErrorCode, bool)>,
    dropped: usize,
    headers_encoded: Vec<u32>,
    headers_polled: usize,
    byte_events: Vec<Option<HttpErrorCode>>,
    response_stopped: Vec<Option<HttpErrorCode>>,
}

impl H1Probe {
    fn observed(&self) -> Observed {
        Observed {
            method: *self.method.borrow(),
            url: self.url.borrow().clone(),
            fields: self.fields.borrow().clone(),
            request_error: self.request_error.get(),
            request_eom: self.request_eom.get(),
            request_body: self.request_body.borrow().clone(),
            request_finished: self.request_finished.get(),
            request_trailers: self.request_trailers.borrow().clone(),
            response_complete: self.response_complete.get(),
            completed: self.completed.get(),
            cancelled: self.cancelled.borrow().clone(),
            dropped: self.dropped.get(),
            headers_encoded: self.headers_encoded.borrow().clone(),
            headers_polled: self.headers_polled.get(),
            byte_events: self.byte_events.borrow().clone(),
            response_stopped: self.response_stopped.borrow().clone(),
        }
    }
}

/// Records a request head as either application receives it.
fn record_head(probe: &H1Probe, head: &mut HttpMessage, eom: bool) {
    *probe.method.borrow_mut() = head.method().unwrap();
    *probe.url.borrow_mut() = head.url().unwrap().to_vec();
    *probe.fields.borrow_mut() = head
        .headers()
        .iter()
        .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
        .collect();
    probe.request_eom.set(eom);
    probe.request_finished.set(eom);
}

/// Reads a request body to its end as either application does, recording each event.
async fn read_request_body(probe: &H1Probe, body: &mut HttpSourceHolder) -> Result<(), HttpError> {
    loop {
        let event = match body.read_body_event(u32::MAX).await {
            Ok(event) => event,
            Err(error) => {
                probe.request_error.set(Some(error.code()));
                return Err(error);
            }
        };
        if let Some(bytes) = event.as_body() {
            let length = bytes.chain_length();
            probe
                .request_body
                .borrow_mut()
                .extend_from_slice(&bytes.coalesce(length).unwrap());
        }
        if let HttpBodyEventKind::Trailers(trailers) = event.kind() {
            let fields = trailers
                .iter()
                .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
                .collect();
            probe
                .request_trailers
                .borrow_mut()
                .push((fields, event.eom()));
        }
        if event.eom() {
            probe.request_finished.set(true);
            return Ok(());
        }
    }
}

impl H1App {
    fn new(responses: Vec<Response>) -> (Self, Rc<RefCell<Vec<Rc<H1Probe>>>>) {
        let probes = Rc::new(RefCell::new(Vec::new()));
        (
            Self {
                responses,
                probes: Rc::clone(&probes),
                body_gate: None,
                respond_early: false,
                instrument: false,
                cancel_in: Rc::default(),
                drain_in: Rc::default(),
                panic_in: None,
            },
            probes,
        )
    }
}

struct H1Exchange {
    ordinal: usize,
    body_gate: Option<Rc<tokio::sync::Notify>>,
    respond_early: bool,
    tunnel: bool,
    cancel_in: CancelIn,
    drain_in: CancelIn,
    panic_in: Option<&'static str>,
    probe: Rc<H1Probe>,
    upload: Option<LocalSourceFuture<Result<(), HttpError>>>,
    interim: VecDeque<HttpHeaderEvent>,
    header: Option<HttpHeaderEvent>,
    body: VecDeque<HttpBodyEvent>,
}

impl OwnedApplication for H1App {
    type Exchange = H1Exchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> H1Exchange {
        let probe = Rc::new(H1Probe {
            cancel_in: Rc::clone(&self.cancel_in),
            ..H1Probe::default()
        });
        let ordinal = self.probes.borrow().len();
        self.probes.borrow_mut().push(Rc::clone(&probe));
        let response = &self.responses[ordinal.min(self.responses.len() - 1)];
        let (mut header, mut body) = response.events();
        if self.instrument {
            let encoded = Rc::clone(&probe);
            let cancel_in = Rc::clone(&self.cancel_in);
            header.set_egress_headers_callback(move |size| {
                encoded.headers_encoded.borrow_mut().push(size.compressed);
                cancel_from(&cancel_in, "headers_encoded");
            });
            let write = ByteEventType::TransportWrite.bit();
            header
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(None, write, &probe));
            for event in &mut body {
                event
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(None, write, &probe));
            }
        }
        note("new", ordinal);
        cancel_from(&self.cancel_in, "new_exchange");
        H1Exchange {
            ordinal,
            body_gate: self.body_gate.clone(),
            respond_early: self.respond_early,
            tunnel: response.tunnel,
            cancel_in: Rc::clone(&self.cancel_in),
            drain_in: Rc::clone(&self.drain_in),
            panic_in: self.panic_in.filter(|_| ordinal == 0),
            probe,
            upload: None,
            interim: response.interim_events(),
            header: Some(header),
            body,
        }
    }
}

impl H1Exchange {
    /// Polls the request body read to its end; `Pending` while it is still being read.
    fn poll_upload(&mut self, context: &mut Context<'_>) -> Poll<Result<(), HttpError>> {
        let Some(upload) = &mut self.upload else {
            return Poll::Ready(Ok(()));
        };
        let result = std::task::ready!(upload.as_mut().poll(context));
        self.upload.take();
        Poll::Ready(result)
    }
}

impl OwnedExchange for H1Exchange {
    fn request(&mut self, mut head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        record_head(&self.probe, &mut head, eom);
        cancel_from(&self.cancel_in, "request");
        drain_from(&self.drain_in, "request");
        let mut body = body.into_source_holder();
        if eom || self.respond_early {
            return;
        }
        let probe = Rc::clone(&self.probe);
        let gate = self.body_gate.clone();
        self.upload = Some(Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            read_request_body(&probe, &mut body).await
        }));
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if let Some(interim) = self.interim.pop_front() {
            return Poll::Ready(Ok(interim));
        }
        if !self.tunnel {
            std::task::ready!(self.poll_upload(context))?;
        }
        self.probe
            .headers_polled
            .set(self.probe.headers_polled.get() + 1);
        cancel_from(&self.cancel_in, "poll_headers");
        drain_from(&self.drain_in, "poll_headers");
        Poll::Ready(Ok(self
            .header
            .take()
            .expect("the response head is polled once")))
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        std::task::ready!(self.poll_upload(context))?;
        cancel_from(&self.cancel_in, "poll_body");
        assert_ne!(
            maximum, 0,
            "the body is polled only with room for some of it"
        );
        let event = self
            .body
            .pop_front()
            .expect("the body is not polled after its terminal event");
        let maximum = maximum as usize;
        let oversized = event
            .as_body()
            .filter(|bytes| bytes.chain_length() > maximum)
            .map(|bytes| bytes.coalesce(bytes.chain_length()).unwrap().to_vec());
        let Some(bytes) = oversized else {
            return Poll::Ready(Ok(event));
        };
        // As the contract asks: at most `maximum` bytes now, the rest on later polls.
        self.body
            .push_front(HttpBodyEvent::body(queue(&bytes[maximum..]), event.eom()));
        Poll::Ready(Ok(HttpBodyEvent::body(queue(&bytes[..maximum]), false)))
    }

    fn cancel(&mut self, error: &HttpError, peer_reset: bool) {
        self.probe
            .cancelled
            .borrow_mut()
            .push((error.code(), peer_reset));
    }

    fn response_complete(&mut self) {
        self.probe
            .response_complete
            .set(self.probe.response_complete.get() + 1);
        cancel_from(&self.cancel_in, "response_complete");
    }

    fn complete(self) {
        note("complete", self.ordinal);
        self.probe.completed.set(self.probe.completed.get() + 1);
        if self.panic_in == Some("complete") {
            panic!("exchange panicked at retirement");
        }
    }
}

impl Drop for H1Exchange {
    fn drop(&mut self) {
        note("drop", self.ordinal);
        self.probe.dropped.set(self.probe.dropped.get() + 1);
        if self.panic_in == Some("drop") && !std::thread::panicking() {
            panic!("exchange panicked at retirement");
        }
    }
}

/// The Uniplex side of a comparison: replays the script in order, reading each request as
/// `H1Exchange` does and recording what it receives.
struct ScriptHandler {
    responses: Vec<Response>,
    served: Cell<usize>,
    /// Answers without reading the request body, dropping it.
    respond_early: bool,
    probes: Rc<RefCell<Vec<Rc<H1Probe>>>>,
}

impl ScriptHandler {
    fn new(responses: Vec<Response>) -> Self {
        Self {
            responses,
            served: Cell::new(0),
            respond_early: false,
            probes: Rc::default(),
        }
    }
}

impl HttpHandler for ScriptHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let ordinal = self.served.replace(self.served.get() + 1);
        let response = &self.responses[ordinal.min(self.responses.len() - 1)];
        let (header, body) = response.events();
        let interim = response.interim_events();
        let tunnel = response.tunnel;
        let respond_early = self.respond_early;
        let probe = Rc::new(H1Probe::default());
        self.probes.borrow_mut().push(Rc::clone(&probe));
        Box::pin(async move {
            let mut head = match request_source.read_header_event().await {
                Ok(head) => head,
                Err(error) => {
                    probe.request_error.set(Some(error.code()));
                    return Err(error);
                }
            };
            let eom = head.eom();
            record_head(&probe, head.headers_mut(), eom);
            if eom || respond_early {
                return Ok(HttpSourceHolder::new(Box::new(StopProbe {
                    inner: EventSource {
                        header: Some(header),
                        body,
                    },
                    probe,
                })));
            }
            if interim.is_empty() && !tunnel {
                read_request_body(&probe, &mut request_source).await?;
                return Ok(HttpSourceHolder::new(Box::new(StopProbe {
                    inner: EventSource {
                        header: Some(header),
                        body,
                    },
                    probe,
                })));
            }
            Ok(HttpSourceHolder::new(Box::new(ScriptSource {
                request: Some(request_source),
                probe,
                interim,
                tunnel,
                header: Some(header),
                body,
            })))
        })
    }
}

/// Sends its informational heads at once, then reads the request body to its end before the
/// final head (or, for a tunnel, before the body), as `H1Exchange` does.
struct ScriptSource {
    request: Option<HttpSourceHolder>,
    probe: Rc<H1Probe>,
    interim: VecDeque<HttpHeaderEvent>,
    tunnel: bool,
    header: Option<HttpHeaderEvent>,
    body: VecDeque<HttpBodyEvent>,
}

impl ScriptSource {
    /// Reads the request body to its end, then yields `then`.
    fn after_request<T: 'static>(
        &mut self,
        then: Result<T, HttpError>,
    ) -> LocalSourceFuture<Result<T, HttpError>> {
        let request = self.request.take();
        let probe = Rc::clone(&self.probe);
        Box::pin(async move {
            if let Some(mut request) = request {
                read_request_body(&probe, &mut request).await?;
            }
            then
        })
    }
}

impl HttpSource for ScriptSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        if let Some(interim) = self.interim.pop_front() {
            return Box::pin(std::future::ready(Ok(interim)));
        }
        let header = self.header.take().expect("the final head is read once");
        if self.tunnel {
            return Box::pin(std::future::ready(Ok(header)));
        }
        self.after_request(Ok(header))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let event = self.body.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response body exhausted")
        });
        self.after_request(event)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.response_stopped.borrow_mut().push(error);
        if let Some(request) = &mut self.request {
            request.stop_reading(error);
        }
    }
}

/// An `EventSource` that records each `stop_reading` call in its probe.
struct StopProbe {
    inner: EventSource,
    probe: Rc<H1Probe>,
}

impl HttpSource for StopProbe {
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
        self.probe.response_stopped.borrow_mut().push(error);
        self.inner.stop_reading(error);
    }
}

struct Wire {
    output: Vec<u8>,
    close: CloseKind,
    error: Option<HttpErrorCode>,
}

fn wire_of(
    state: &Rc<RefCell<TransportState>>,
    result: Result<crate::uniplex::UniplexRunSummary, HttpError>,
) -> Wire {
    let state = state.borrow();
    Wire {
        output: state.writes.concat(),
        close: state.close,
        error: result.err().map(|error| error.code()),
    }
}

fn owned_options(context: WorkerContext, transport: Rc<dyn UniplexTransport>) -> OwnedH1Options {
    OwnedH1Options {
        context,
        transport,
        io: h1_config(),
        http1: Http1DriverConfig::default(),
    }
}

/// Serves `reads` with the owned driver on a `ManualHost`.
fn owned_wire(reads: Vec<ReadAction>, responses: Vec<Response>) -> (Wire, Vec<Observed>) {
    owned_wire_with(reads, responses, false)
}

/// As `owned_wire`; `instrument` sets `H1App::instrument`.
fn owned_wire_with(
    reads: Vec<ReadAction>,
    responses: Vec<Response>,
    instrument: bool,
) -> (Wire, Vec<Observed>) {
    owned_wire_configured(reads, responses, instrument, Http1DriverConfig::default())
}

/// As `owned_wire_with`, with the driver's HTTP/1.1 configuration `http1`.
fn owned_wire_configured(
    reads: Vec<ReadAction>,
    responses: Vec<Response>,
    instrument: bool,
    http1: Http1DriverConfig,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let (mut application, probes) = H1App::new(responses);
            application.instrument = instrument;
            let mut options = owned_options(context, transport);
            options.http1 = http1;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let result = session.run().await;
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// Serves `reads` with `UniplexSession` HTTP/1.1 on a `ManualHost`.
fn uniplex_wire(reads: Vec<ReadAction>, responses: Vec<Response>) -> Wire {
    uniplex_wire_observed(reads, responses).0
}

/// As `uniplex_wire`, with what the handler observed of each request.
fn uniplex_wire_observed(
    reads: Vec<ReadAction>,
    responses: Vec<Response>,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let handler = Rc::new(ScriptHandler::new(responses));
            let erased: HttpHandlerRef = handler.clone();
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(erased),
                h1_config(),
            )
            .unwrap();
            let result = session.run().await;
            let observed = handler
                .probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

#[test]
fn owned_h1_serves_a_get_on_a_manual_host() {
    let reads = || {
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ]
    };
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert!(
        owned.output.ends_with(b"\r\n\r\nhello"),
        "{:?}",
        owned.output
    );
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let probe = &probes[0];
    assert_eq!(probe.method, Some(HttpMethod::Get));
    assert_eq!(probe.url, b"/");
    assert!(probe.request_eom);
    assert_eq!(probe.response_complete, 1);
    assert_eq!(probe.completed, 1);
    assert_eq!(probe.dropped, 1);
    assert!(probe.cancelled.is_empty());
}

/// `count` keep-alive GETs in one read; the last one asks to close the connection.
fn pipelined_gets(count: usize) -> Vec<u8> {
    (0..count)
        .flat_map(|index| {
            let close = if index + 1 == count {
                "Connection: close\r\n"
            } else {
                ""
            };
            format!("GET /{index} HTTP/1.1\r\nHost: a\r\n{close}\r\n").into_bytes()
        })
        .collect()
}

#[test]
fn owned_h1_spawns_no_task_per_request() {
    let (spawned, completed) = run_on_host_with(|host, context| {
        Box::pin(async move {
            let (transport, _state) =
                ScriptedTransport::new([ReadAction::Data(pipelined_gets(100))]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let before = host.spawns();
            session.run().await.unwrap();
            let completed: usize = probes
                .borrow()
                .iter()
                .map(|probe| probe.completed.get())
                .sum();
            (host.spawns() - before, completed)
        })
    });
    assert_eq!(
        completed, 100,
        "every request was served on the one connection"
    );
    assert_eq!(spawned, 0, "the driver spawns no task for any request");
}

#[test]
fn keep_alive_serves_three_sequential_requests() {
    let reads = vec![
        ReadAction::Data(b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
        ReadAction::Data(b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
        ReadAction::Data(b"GET /2 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec()),
    ];
    let (owned, probes) = owned_wire(reads, vec![Response::fixed(200, b"hello")]);
    assert_eq!(owned.error, None);
    let urls: Vec<_> = probes.iter().map(|probe| probe.url.clone()).collect();
    assert_eq!(urls, [b"/0".to_vec(), b"/1".to_vec(), b"/2".to_vec()]);
    assert!(probes.iter().all(|probe| probe.completed == 1));
    assert_eq!(
        owned
            .output
            .windows(b"HTTP/1.1 200".len())
            .filter(|window| *window == b"HTTP/1.1 200")
            .count(),
        3,
        "three responses on the one transport"
    );
    assert!(owned.output.ends_with(b"\r\n\r\nhello"));
    assert_eq!(owned.close, CloseKind::ShutdownWrite);
}

#[test]
fn serial_exchanges_do_not_overlap() {
    take_events();
    let reads = vec![ReadAction::Data(
        b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\nGET /1 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"
            .to_vec(),
    )];
    let ((owned, probes), events) = run_on_host(move |context| {
        Box::pin(async move {
            take_events();
            let (transport, state) = ScriptedTransport::new(reads);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let result = session.run().await;
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            ((wire_of(&state, result), observed), take_events())
        })
    });
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 2);
    assert_eq!(
        events,
        [
            ("new", 0),
            ("complete", 0),
            ("drop", 0),
            ("new", 1),
            ("complete", 1),
            ("drop", 1)
        ],
        "the second request's exchange is created only after the first retires"
    );
}

fn patterned(length: usize) -> Vec<u8> {
    (0..length).map(|index| b'a' + (index % 26) as u8).collect()
}

#[test]
fn content_length_request_body_reaches_the_exchange() {
    let body = patterned(16 << 10);
    let mut request = format!(
        "POST /upload HTTP/1.1\r\nHost: a\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    let reads = request
        .chunks(4096)
        .map(|chunk| ReadAction::Data(chunk.to_vec()))
        .collect();
    let (owned, probes) = owned_wire(reads, vec![Response::fixed(200, b"ok")]);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let probe = &probes[0];
    assert!(!probe.request_eom, "the head arrives before its body");
    assert_eq!(probe.request_body, body, "16 KiB arrive intact");
    assert!(probe.request_finished, "the body ends with EOM");
    assert_eq!(probe.completed, 1);
}

/// Serves one byte string honoring each read's `maximum`, then parks until cancelled; records
/// writes and how the session closed.
struct ByteTransport {
    pending: RefCell<VecDeque<u8>>,
    eof: bool,
    reads: Cell<usize>,
    state: Rc<RefCell<TransportState>>,
}

impl ByteTransport {
    fn new(bytes: Vec<u8>, eof: bool) -> (Rc<Self>, Rc<RefCell<TransportState>>) {
        let (_, state) = ScriptedTransport::new([]);
        (
            Rc::new(Self {
                pending: RefCell::new(bytes.into()),
                eof,
                reads: Cell::new(0),
                state: Rc::clone(&state),
            }),
            state,
        )
    }
}

impl UniplexTransport for ByteTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.reads.set(self.reads.get() + 1);
        let mut pending = self.pending.borrow_mut();
        if !pending.is_empty() || self.eof {
            let take = maximum.min(pending.len());
            let bytes: Vec<u8> = pending.drain(..take).collect();
            let eof = self.eof && pending.is_empty();
            return Box::pin(std::future::ready(Ok(UniplexRead::new(queue(&bytes), eof))));
        }
        let baton = CancellableBaton::with_host(context.host().clone());
        Box::pin(async move {
            let _ = baton.wait(cancellation).await;
            Err(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"byte transport read cancelled",
            ))
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
        let length = data.chain_length();
        let bytes = data.coalesce(length).unwrap().to_vec();
        self.state.borrow_mut().writes.push(bytes);
        Box::pin(std::future::ready(Ok(WriteInfo {
            bytes_written: length,
        })))
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
        self.state.borrow_mut().close = CloseKind::Reset;
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

#[test]
fn request_body_beyond_the_buffer_limit_pauses_ingress() {
    let body = patterned(256 << 10);
    let expected = body.clone();
    let (error, observed) = run_on_host(move |context| {
        Box::pin(async move {
            let mut request = format!(
                "POST /upload HTTP/1.1\r\nHost: a\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .into_bytes();
            request.extend_from_slice(&body);
            let (transport, state) = ByteTransport::new(request, false);
            let gate = Rc::new(tokio::sync::Notify::new());
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"ok")]);
            application.body_gate = Some(Rc::clone(&gate));
            let mut options = owned_options(context.clone(), transport.clone());
            options.io.buffer_limit = 64 << 10;
            options.io.ingress_buffer_limit = 16 << 10;
            options.io.maximum_read_size = 16 << 10;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let release = async {
                // Lets the session read until ingress stalls behind the unread body.
                for _ in 0..64 {
                    context.host().yield_to_io().await;
                }
                assert!(
                    transport.pending.borrow().len() > 128 << 10,
                    "ingress stopped reading while the body was unread"
                );
                gate.notify_one();
            };
            let (result, ()) = futures::join!(session.run(), release);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            drop(state);
            (result.err().map(|error| error.code()), observed)
        })
    });
    assert_eq!(error, None, "the body never exceeds the request window");
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].request_body, expected, "256 KiB arrive intact");
    assert!(observed[0].request_finished);
    assert_eq!(observed[0].completed, 1);
}

#[test]
fn chunked_request_body_reaches_the_exchange() {
    let reads = vec![
        ReadAction::Data(
            b"POST /c HTTP/1.1\r\nHost: a\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n"
                .to_vec(),
        ),
        ReadAction::Data(b"5\r\nhello\r\n".to_vec()),
        ReadAction::Data(b"1\r\n,\r\n6\r\n world\r\n".to_vec()),
        ReadAction::Data(b"0\r\n\r\n".to_vec()),
    ];
    let (owned, probes) = owned_wire(reads, vec![Response::fixed(200, b"ok")]);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let probe = &probes[0];
    assert!(!probe.request_eom);
    assert_eq!(
        probe.request_body, b"hello, world",
        "every chunk arrives in order"
    );
    assert!(
        probe.request_finished,
        "the last chunk ends the body with EOM"
    );
    assert_eq!(probe.completed, 1);
}

#[test]
fn response_without_length_is_chunked_on_http11() {
    let reads = || {
        vec![
            ReadAction::Data(b"GET /stream HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ]
    };
    let responses = vec![Response {
        content_length: false,
        ..Response::fixed(200, b"event: one\n\n")
    }];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert_eq!(owned.error, None);
    assert_eq!(probes[0].completed, 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(text.contains("Transfer-Encoding: chunked\r\n"), "{text:?}");
    assert!(
        text.ends_with("\r\n\r\nc\r\nevent: one\n\n\r\n0\r\n\r\n"),
        "{text:?}"
    );
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
}

/// Serves one request that selects connection close; no peer EOF follows.
fn assert_closes_after_the_response(request: &'static [u8]) {
    let reads = || vec![ReadAction::Data(request.to_vec())];
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].completed, 1);
    assert!(
        owned.output.ends_with(b"\r\n\r\nhello"),
        "the response is written first"
    );
    assert_eq!(
        owned.close,
        CloseKind::ShutdownWrite,
        "then the transport closes"
    );
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.close, uniplex.close);
}

#[test]
fn http10_request_closes_after_the_response() {
    assert_closes_after_the_response(b"GET / HTTP/1.0\r\nHost: a\r\n\r\n");
}

#[test]
fn connection_close_request_closes_after_the_response() {
    assert_closes_after_the_response(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
}

#[test]
fn a_closing_request_admits_no_pipelined_request() {
    for first in [
        &b"GET /0 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"[..],
        &b"GET /0 HTTP/1.0\r\nHost: a\r\n\r\n"[..],
    ] {
        let mut read = first.to_vec();
        read.extend_from_slice(b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n");
        let reads = || vec![ReadAction::Data(read.clone())];
        let responses = vec![Response::fixed(200, b"hello")];
        let (owned, probes) = owned_wire(reads(), responses.clone());
        let label = String::from_utf8_lossy(first);
        assert_eq!(owned.error, None, "{label:?}");
        let urls: Vec<_> = probes.iter().map(|probe| probe.url.clone()).collect();
        assert_eq!(
            urls,
            [b"/0".to_vec()],
            "{label:?}: only the closing request is admitted"
        );
        assert_eq!(probes[0].completed, 1, "{label:?}");
        assert_eq!(
            owned
                .output
                .windows(b" 200 ".len())
                .filter(|window| *window == b" 200 ")
                .count(),
            1,
            "{label:?}: one response is written: {:?}",
            String::from_utf8_lossy(&owned.output)
        );
        assert!(owned.output.ends_with(b"\r\n\r\nhello"), "{label:?}");
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "{label:?}");
        let uniplex = uniplex_wire(reads(), responses);
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label:?}"
        );
        assert_eq!(owned.close, uniplex.close, "{label:?}");
        assert_eq!(owned.error, uniplex.error, "{label:?}");
    }
}

#[test]
fn peer_eof_mid_request_cancels_the_exchange() {
    let eof: fn() -> ReadAction = || ReadAction::Eof;
    let read_error: fn() -> ReadAction = || ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR);
    for (last_read, cause) in [
        (eof, HttpErrorCode::TRANSPORT_EOF),
        (read_error, HttpErrorCode::TRANSPORT_READ_ERROR),
    ] {
        let reads = || {
            vec![
                ReadAction::Data(
                    b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc".to_vec(),
                ),
                last_read(),
            ]
        };
        let (owned, probes) = owned_wire(reads(), vec![Response::fixed(200, b"hello")]);
        let (uniplex, uniplex_requests) =
            uniplex_wire_observed(reads(), vec![Response::fixed(200, b"hello")]);
        assert_eq!(probes.len(), 1);
        let probe = &probes[0];
        assert_eq!(
            probe.cancelled,
            [(cause, true)],
            "peer EOF or a failed read cancels the exchange once, as a peer reset"
        );
        assert_same_requests(
            &probes,
            &uniplex_requests,
            &format!("{cause:?}: the head and body read before the end reach the exchange"),
        );
        assert_eq!(probe.completed, 0);
        assert_eq!(probe.dropped, 1);
        assert_eq!(owned.error, Some(cause));
        assert_eq!(owned.close, CloseKind::Reset);
        assert_eq!(
            without_dates(&owned.output),
            without_dates(&uniplex.output),
            "{cause:?}: the error response, if any, is the one UniplexSession writes"
        );
        assert_eq!(owned.close, uniplex.close, "{cause:?}");
        assert_eq!(owned.error, uniplex.error, "{cause:?}");
    }
}

/// Serves `reads` with the owned driver (`owned`) or `UniplexSession` under the I/O
/// configuration `io`.
fn configured_wire(
    owned: bool,
    reads: Vec<ReadAction>,
    response: Response,
    io: UniplexConfig,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let (result, probes) = if owned {
                let (application, probes) = H1App::new(vec![response]);
                let mut options = owned_options(context, transport);
                options.io = io;
                let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                (session.run().await, probes)
            } else {
                let handler = Rc::new(ScriptHandler::new(vec![response]));
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    io,
                )
                .unwrap();
                (session.run().await, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// A read error that follows request body bytes the parser has not reached, because the unread
/// body is at `ingress_buffer_limit`, fails the connection only after those bytes reach the
/// exchange, as `UniplexSession` delivers them before it sees the failed read.
#[test]
fn bytes_read_before_a_read_error_reach_the_exchange_as_uniplex() {
    let body = patterned(4096);
    let mut first = b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 4096\r\n\r\n".to_vec();
    first.extend_from_slice(&body[..1536]);
    let reads = || {
        vec![
            ReadAction::Data(first.clone()),
            ReadAction::Data(body[1536..3072].to_vec()),
            ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
        ]
    };
    let io = UniplexConfig {
        ingress_buffer_limit: 1024,
        ..h1_config()
    };
    let response = Response::fixed(200, b"ok");
    let (owned, owned_requests) = configured_wire(true, reads(), response.clone(), io.clone());
    let (uniplex, uniplex_requests) = configured_wire(false, reads(), response, io);
    assert_eq!(owned_requests.len(), 1);
    assert_eq!(uniplex_requests.len(), 1);
    assert_eq!(
        uniplex_requests[0].request_body,
        body[..3072],
        "UniplexSession delivers both reads' body bytes"
    );
    assert_eq!(
        (
            owned_requests[0].method,
            owned_requests[0].request_body.len()
        ),
        (
            uniplex_requests[0].method,
            uniplex_requests[0].request_body.len()
        ),
        "the request head and body bytes read before the error reach the exchange"
    );
    assert_same_requests(&owned_requests, &uniplex_requests, "read error");
    // The owned exchange learns of the failure through `cancel`; the handler through its body.
    assert_eq!(
        owned_requests[0].cancelled,
        [(HttpErrorCode::TRANSPORT_READ_ERROR, true)]
    );
    assert_eq!(
        uniplex_requests[0].request_error,
        Some(HttpErrorCode::TRANSPORT_READ_ERROR)
    );
    assert_eq!(owned.error, uniplex.error);
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(without_dates(&owned.output), without_dates(&uniplex.output));
}

/// Serves `reads` with both sessions under `io` and asserts the same requests, output, close
/// and result; returns the owned run and what each application observed.
fn assert_read_error_matches_uniplex(
    reads: impl Fn() -> Vec<ReadAction>,
    response: Response,
    io: UniplexConfig,
    label: &str,
) -> (Wire, Vec<Observed>, Vec<Observed>) {
    let (owned, owned_requests) = configured_wire(true, reads(), response.clone(), io.clone());
    let (uniplex, uniplex_requests) = configured_wire(false, reads(), response, io);
    assert_same_requests(&owned_requests, &uniplex_requests, label);
    assert_eq!(
        without_dates(&owned.output),
        without_dates(&uniplex.output),
        "{label}: the responses written before the failure"
    );
    assert_eq!(owned.close, uniplex.close, "{label}");
    assert_eq!(owned.error, uniplex.error, "{label}");
    (owned, owned_requests, uniplex_requests)
}

/// A read error that follows a pipelined request, read while the request before it awaits its
/// response, fails the connection only after that response, as `UniplexSession` issues no read
/// while it holds a pipelined request back: a keep-alive request's successor is then parsed and
/// admitted, and a closing request's connection closes without seeing the failed read.
#[test]
fn a_pipelined_request_read_before_a_read_error_is_served_as_uniplex() {
    let read_error = Some(HttpErrorCode::TRANSPORT_READ_ERROR);
    for (first, admitted, error) in [
        (&b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\n"[..], 2, read_error),
        (
            &b"GET /0 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"[..],
            1,
            None,
        ),
    ] {
        let mut read = first.to_vec();
        read.extend_from_slice(b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n");
        let reads = || {
            vec![
                ReadAction::Data(read.clone()),
                ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
            ]
        };
        let label = String::from_utf8_lossy(first).into_owned();
        let (owned, owned_requests, uniplex_requests) = assert_read_error_matches_uniplex(
            reads,
            Response::fixed(200, b"hello"),
            h1_config(),
            &label,
        );
        assert_eq!(owned_requests.len(), admitted, "{label}");
        assert_eq!(
            owned_requests[0].completed, 1,
            "{label}: the first request is served"
        );
        assert!(owned_requests[0].cancelled.is_empty(), "{label}");
        assert_eq!(owned.error, error, "{label}");
        if admitted == 2 {
            assert_eq!(
                owned_requests[1].cancelled,
                [(HttpErrorCode::TRANSPORT_READ_ERROR, true)],
                "{label}: the failure cancels the pipelined request before its response"
            );
            assert_eq!(owned_requests[1].headers_polled, 0, "{label}");
            // `UniplexSession` gives the handler no cause: it drops the pipelined request's
            // response unread, and the read error is the run's error.
            assert!(uniplex_requests[0].response_stopped.is_empty(), "{label}");
            assert_eq!(
                uniplex_requests[1].response_stopped,
                [None],
                "{label}: UniplexSession also ends the pipelined request unserved"
            );
        }
    }
}

/// A read error that follows bytes read behind an upgrade or CONNECT head fails the connection
/// only after the final response resolves the upgrade, as `UniplexSession` issues no read while
/// the upgrade is pending: an accepted tunnel's head is written before the reset, and a refused
/// upgrade closes without seeing the failed read.
#[test]
fn bytes_behind_a_pending_upgrade_precede_a_read_error_as_uniplex() {
    let mut refused = WEBSOCKET_HEAD.to_vec();
    refused.extend_from_slice(b"GET /later HTTP/1.1\r\nHost: a\r\n\r\n");
    let refusal = Response {
        tunnel: true,
        ..Response::fixed(400, b"no")
    };
    let read_error = Some(HttpErrorCode::TRANSPORT_READ_ERROR);
    let cases = [
        (
            [CONNECT_HEAD, &[b'x'; 10][..]].concat(),
            Response::tunnel(200, &[b'y'; 10], false),
            "CONNECT accepted",
            read_error,
        ),
        (
            [WEBSOCKET_HEAD, &[b'x'; 10][..]].concat(),
            Response::tunnel(200, &[b'y'; 10], true),
            "WebSocket accepted",
            read_error,
        ),
        (refused, refusal, "WebSocket refused", None),
    ];
    for (read, response, label, error) in cases {
        let reads = || {
            vec![
                ReadAction::Data(read.clone()),
                ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
            ]
        };
        let (owned, owned_requests, _) =
            assert_read_error_matches_uniplex(reads, response, h1_config(), label);
        assert_eq!(owned_requests.len(), 1, "{label}");
        assert_eq!(owned.error, error, "{label}");
        if error.is_some() {
            assert!(
                owned.output.starts_with(b"HTTP/1.1 "),
                "{label}: the accepting head is written before the reset"
            );
            assert_eq!(
                owned_requests[0].cancelled,
                [(HttpErrorCode::TRANSPORT_READ_ERROR, true)],
                "{label}"
            );
        }
    }
}

#[test]
fn drain_closes_after_the_in_flight_response() {
    let (owned, probes) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc".to_vec(),
            )]);
            let gate = Rc::new(tokio::sync::Notify::new());
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.body_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let drain = async {
                while probes.borrow().is_empty() {
                    context.host().yield_to_io().await;
                }
                assert!(
                    handle.initiate_drain().unwrap(),
                    "the first drain request changes state"
                );
                assert!(!handle.initiate_drain().unwrap(), "a second one does not");
                gate.notify_one();
            };
            let (result, ()) = futures::join!(session.run(), drain);
            assert!(
                !handle.initiate_drain().unwrap(),
                "a finished session ignores drain"
            );
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    });
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].completed, 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(
        text.contains("\r\nConnection: close\r\n"),
        "the in-flight response carries Connection: close: {text:?}"
    );
    assert!(text.ends_with("\r\n\r\nhello"));
    assert_eq!(
        owned.close,
        CloseKind::ShutdownWrite,
        "then the transport closes"
    );
}

#[test]
fn turn_budget_yields_to_host_io() {
    for mode in [YieldMode::Wake, YieldMode::Immediate, YieldMode::Deferred] {
        let (yields, completed_yields, completed) = run_on_host_with(move |host, context| {
            Box::pin(async move {
                host.set_yield_mode(mode);
                let (transport, _state) =
                    ScriptedTransport::new([ReadAction::Data(pipelined_gets(100))]);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                session.run().await.unwrap();
                let completed: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.completed.get())
                    .sum();
                (host.yields(), host.completed_yields(), completed)
            })
        });
        assert_eq!(completed, 100);
        assert!(
            yields >= 1,
            "always-ready input longer than one turn budget yields to host I/O"
        );
        assert_eq!(
            completed_yields, yields,
            "the driver resumes only through a completed yield"
        );
    }
}

#[test]
fn cancel_tears_down_cleanly() {
    let (owned, probes) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc".to_vec(),
            )]);
            let gate = Rc::new(tokio::sync::Notify::new());
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.body_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let cancel = async {
                while probes.borrow().is_empty() {
                    context.host().yield_to_io().await;
                }
                handle.cancel();
            };
            let (result, ()) = futures::join!(session.run(), cancel);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    });
    assert_eq!(
        owned.error,
        Some(HttpErrorCode::CORO_CANCELLED),
        "run completes"
    );
    assert_eq!(probes.len(), 1);
    assert_eq!(
        probes[0].cancelled,
        [(HttpErrorCode::CORO_CANCELLED, false)],
        "the live exchange is cancelled once"
    );
    assert_eq!(probes[0].completed, 0);
    assert_eq!(
        probes[0].dropped, 1,
        "the exchange is dropped before run completes"
    );
    assert_eq!(owned.close, CloseKind::Reset);
    assert!(owned.output.is_empty());
}

#[test]
fn owned_h1_rejects_unsupported_config() {
    type Change = fn(&mut UniplexConfig);
    let cases: [(&str, Change, ByteEventCapability); 9] = [
        (
            "read_timeout",
            |io| io.read_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "stream_read_timeout",
            |io| io.stream_read_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "write_timeout",
            |io| io.write_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "drain_request_body_on_drop",
            |io| io.drain_request_body_on_drop = false,
            ByteEventCapability::Unsupported,
        ),
        (
            "wait_for_http1_response_on_drain",
            |io| io.wait_for_http1_response_on_drain = true,
            ByteEventCapability::Unsupported,
        ),
        (
            "byte-event-capable transport",
            |_| {},
            ByteEventCapability::TransportEvents,
        ),
        (
            "zero maximum_write_size",
            |io| io.maximum_write_size = 0,
            ByteEventCapability::Unsupported,
        ),
        (
            "maximum_write_size above buffer_limit",
            |io| io.maximum_write_size = io.buffer_limit + 1,
            ByteEventCapability::Unsupported,
        ),
        (
            "maximum_write_size with no room beside chunk framing",
            |io| io.maximum_write_size = 17,
            ByteEventCapability::Unsupported,
        ),
    ];
    for (name, change, capability) in cases {
        let rejected = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, _state) = ScriptedTransport::with_capability([], capability);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let mut options = owned_options(context, transport);
                change(&mut options.io);
                let rejected = match OwnedH1Session::new(application, options) {
                    Ok(_) => None,
                    Err(error) => Some(error.code()),
                };
                assert!(probes.borrow().is_empty());
                rejected
            })
        });
        assert_eq!(
            rejected,
            Some(HttpErrorCode::INVALID_STATE_TRANSITION),
            "{name} is rejected"
        );
    }
}

#[test]
fn response_head_reports_its_encoded_size() {
    let (owned, probes) = owned_wire_with(
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ],
        vec![Response::fixed(200, b"hello")],
        true,
    );
    assert_eq!(owned.error, None);
    let head = owned
        .output
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response head")
        + 4;
    assert_eq!(probes[0].headers_encoded, vec![head as u32]);
}

#[test]
fn response_byte_event_registrations_are_refused() {
    let (owned, probes) = owned_wire_with(
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ],
        vec![Response::fixed(200, b"hello")],
        true,
    );
    assert_eq!(owned.error, None);
    assert!(owned.output.ends_with(b"\r\n\r\nhello"));
    assert_eq!(
        probes[0].byte_events,
        vec![Some(HttpErrorCode::INVALID_STATE_TRANSITION); 2],
        "the head's and the body's registrations are each cancelled with the documented error"
    );
}

#[test]
fn request_body_ending_at_the_ingress_limit_completes() {
    let body = patterned(4096);
    let expected = body.clone();
    let observed = run_on_host(move |context| {
        Box::pin(async move {
            let mut request = format!(
                "POST /upload HTTP/1.1\r\nHost: a\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .into_bytes();
            request.extend_from_slice(&body);
            let (transport, _state) = ScriptedTransport::new([ReadAction::Data(request)]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"ok")]);
            let mut options = owned_options(context, transport);
            options.io.ingress_buffer_limit = body.len() as u32;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let result = session.run().await;
            assert_eq!(result.err().map(|error| error.code()), None);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            observed
        })
    });
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].request_body, expected);
    assert!(
        observed[0].request_finished,
        "the last body bytes reach the ingress limit, and the request still ends with EOM"
    );
    assert_eq!(observed[0].completed, 1);
}

#[test]
fn ready_reads_count_against_the_turn_budget() {
    let request = pipelined_gets(5);
    let cases: [(&str, Vec<ReadAction>); 2] = [
        (
            "empty reads",
            (0..200)
                .map(|_| ReadAction::Data(Vec::new()))
                .chain([ReadAction::Data(request.clone())])
                .collect(),
        ),
        (
            "one-byte reads",
            request
                .iter()
                .map(|byte| ReadAction::Data(vec![*byte]))
                .collect(),
        ),
    ];
    for (name, reads) in cases {
        let (yields, completed) = run_on_host_with(move |host, context| {
            Box::pin(async move {
                let (transport, _state) = ScriptedTransport::new(reads);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                session.run().await.unwrap();
                let completed: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.completed.get())
                    .sum();
                (host.yields(), completed)
            })
        });
        assert_eq!(completed, 5, "{name}: every request is served");
        assert!(
            yields >= 1,
            "{name}: more ready reads than one turn budget yield to host I/O"
        );
    }
}

#[test]
fn cancel_from_an_exchange_callback_stops_the_response() {
    for (callback, response) in [
        ("request", Response::fixed(204, b"")),
        ("poll_headers", Response::fixed(204, b"")),
        ("poll_body", Response::fixed(200, b"hello")),
    ] {
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                    b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
                )]);
                let (application, probes) = H1App::new(vec![response]);
                let slot = Rc::clone(&application.cancel_in);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *slot.borrow_mut() = Some((callback, handle));
                let result = session.run().await;
                slot.borrow_mut().take();
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        assert_eq!(
            owned.error,
            Some(HttpErrorCode::CORO_CANCELLED),
            "{callback}"
        );
        assert_eq!(probes.len(), 1);
        assert_eq!(
            probes[0].response_complete, 0,
            "{callback}: a response polled after the exchange cancelled the session is not serialized"
        );
        if callback == "request" {
            assert_eq!(
                probes[0].headers_polled, 0,
                "an exchange that cancelled the session is not polled again"
            );
        }
        assert_eq!(
            probes[0].cancelled,
            [(HttpErrorCode::CORO_CANCELLED, false)],
            "{callback}"
        );
        assert_eq!(probes[0].completed, 0);
        assert!(owned.output.is_empty(), "{callback}");
        assert_eq!(owned.close, CloseKind::Reset);
    }
}

#[test]
fn response_body_polls_fit_the_egress_buffer() {
    let body = patterned(16 << 10);
    for content_length in [true, false] {
        let response = Response {
            content_length,
            ..Response::fixed(200, &body)
        };
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                    b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
                )]);
                let (application, probes) = H1App::new(vec![response]);
                let mut options = owned_options(context, transport);
                options.io.buffer_limit = 16 << 10;
                options.io.maximum_write_size = 16 << 10;
                options.io.ingress_buffer_limit = 4 << 10;
                options.io.maximum_read_size = 4 << 10;
                options.io.read_allocation_size = 4 << 10;
                let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                let result = session.run().await;
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        assert_eq!(
            owned.error, None,
            "content_length {content_length}: a body as large as the write size is served"
        );
        assert_eq!(probes[0].completed, 1);
        let head = owned
            .output
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("a response head")
            + 4;
        let wire_body = &owned.output[head..];
        if content_length {
            assert_eq!(wire_body, body.as_slice());
        } else {
            assert!(wire_body.ends_with(b"0\r\n\r\n"));
            assert!(wire_body.len() > body.len());
        }
        assert_eq!(owned.close, CloseKind::ShutdownWrite);
    }
}

#[test]
fn drain_closes_an_idle_keep_alive_connection() {
    let (owned, probes) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
            )]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let drain = async {
                while probes
                    .borrow()
                    .first()
                    .is_none_or(|probe| probe.completed.get() == 0)
                {
                    context.host().yield_to_io().await;
                }
                assert!(handle.initiate_drain().unwrap());
            };
            let (result, ()) = futures::join!(session.run(), drain);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    });
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(
        !text.contains("Connection: close"),
        "the response was written before the drain: {text:?}"
    );
    assert!(text.ends_with("\r\n\r\nhello"));
    assert_eq!(
        owned.close,
        CloseKind::ShutdownWrite,
        "the idle keep-alive connection closes on drain"
    );
}

#[test]
fn drain_before_a_request_admits_no_exchange() {
    let reads = || {
        vec![ReadAction::Data(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
        )]
    };
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, probes) = run_on_host({
        let reads = reads();
        let responses = responses.clone();
        move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new(reads);
                let (application, probes) = H1App::new(responses);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                assert!(handle.initiate_drain().unwrap());
                let result = session.run().await;
                let observed = probes.borrow().len();
                (wire_of(&state, result), observed)
            })
        }
    });
    let (uniplex, served) = run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads());
            let handler = Rc::new(ScriptHandler::new(responses));
            let erased: HttpHandlerRef = handler.clone();
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(erased),
                h1_config(),
            )
            .unwrap();
            assert!(handle.initiate_drain().unwrap());
            let result = session.run().await;
            (wire_of(&state, result), handler.served.get())
        })
    });
    assert_eq!(uniplex.error, None);
    assert_eq!(
        served, 0,
        "UniplexSession admits no request after an idle drain"
    );
    assert_eq!(probes, 0, "nor does the owned driver");
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.error, None);
    assert_eq!(owned.close, uniplex.close);
}

#[test]
fn request_chunked_trailers_reach_the_exchange() {
    // A body then trailers, and trailers right after the head.
    for (chunks, body) in [
        (
            &b"5\r\nhello\r\n0\r\nx-checksum: 1\r\n\r\n"[..],
            &b"hello"[..],
        ),
        (&b"0\r\nx-checksum: 1\r\n\r\n"[..], &b""[..]),
    ] {
        let mut request =
            b"POST /c HTTP/1.1\r\nHost: a\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n"
                .to_vec();
        request.extend_from_slice(chunks);
        let reads = || vec![ReadAction::Data(request.clone())];
        let label = String::from_utf8_lossy(chunks);
        let responses = vec![Response::fixed(200, b"ok")];
        let (owned, probes) = owned_wire(reads(), responses.clone());
        let uniplex = uniplex_wire(reads(), responses);
        assert_eq!(owned.error, None, "{label:?}");
        assert_eq!(probes.len(), 1, "{label:?}");
        let probe = &probes[0];
        assert!(
            !probe.request_eom,
            "{label:?}: the trailers follow the head"
        );
        assert_eq!(probe.request_body, body, "{label:?}");
        assert_eq!(
            probe.request_trailers,
            [(vec![(b"x-checksum".to_vec(), b"1".to_vec())], true)],
            "{label:?}: the trailer section arrives as one trailers event carrying EOM"
        );
        assert!(probe.request_finished, "{label:?}");
        assert_eq!(probe.completed, 1, "{label:?}");
        assert!(probe.cancelled.is_empty(), "{label:?}");
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label:?}"
        );
        assert_eq!(owned.close, uniplex.close, "{label:?}");
    }
}

#[test]
fn owned_h1_rejects_a_cancelled_context() {
    let rejected = run_on_host(|context| {
        Box::pin(async move {
            context.handle().begin_shutdown();
            assert!(context.is_cancelled());
            let (transport, _state) = ScriptedTransport::new([]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let rejected = match OwnedH1Session::new(application, owned_options(context, transport))
            {
                Ok(_) => None,
                Err(error) => Some(error.code()),
            };
            assert!(probes.borrow().is_empty());
            rejected
        })
    });
    assert_eq!(rejected, Some(HttpErrorCode::INVALID_STATE_TRANSITION));
}

/// The first exchange panics in `callback` at retirement: `run` resumes the panic after the
/// connection fails, and no later request is parsed.
fn assert_retirement_panic_fails_the_connection(callback: &'static str, request: Vec<u8>) {
    let (panic, output, close, probes) = run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(request)]);
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.panic_in = Some(callback);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let panic = futures::FutureExt::catch_unwind(AssertUnwindSafe(session.run()))
                .await
                .err()
                .and_then(|payload| payload.downcast_ref::<&str>().map(|text| text.to_string()));
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            let state = state.borrow();
            (panic, state.writes.concat(), state.close, observed)
        })
    });
    assert_eq!(
        panic.as_deref(),
        Some("exchange panicked at retirement"),
        "{callback}: run resumes the panic"
    );
    assert_eq!(
        probes.len(),
        1,
        "{callback}: no request is parsed after a retirement panic"
    );
    assert!(
        output.ends_with(b"\r\n\r\nhello"),
        "{callback}: the response was written before retirement"
    );
    assert_eq!(close, CloseKind::Reset, "{callback}: the connection fails");
}

#[test]
fn a_panic_in_complete_fails_the_connection() {
    // A second request is already read when the first exchange's `complete` panics.
    assert_retirement_panic_fails_the_connection("complete", pipelined_gets(2));
}

#[test]
fn a_panic_dropping_the_exchange_fails_the_idle_connection() {
    // A keep-alive connection with nothing more to read.
    assert_retirement_panic_fails_the_connection(
        "drop",
        b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
    );
}

/// Each byte of `bytes` in its own ready read.
fn one_byte_reads(bytes: &[u8]) -> Vec<ReadAction> {
    bytes
        .iter()
        .map(|byte| ReadAction::Data(vec![*byte]))
        .collect()
}

#[test]
fn cancel_from_a_driver_callback_fails_the_connection() {
    // One-byte reads use up the turn's budget before the first request is parsed, so its
    // response would be written in the same turn.
    for callback in [
        "new_exchange",
        "byte_event",
        "headers_encoded",
        "response_complete",
    ] {
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new(one_byte_reads(&pipelined_gets(3)));
                let (mut application, probes) = H1App::new(vec![Response::fixed(204, b"")]);
                application.instrument = matches!(callback, "byte_event" | "headers_encoded");
                let slot = Rc::clone(&application.cancel_in);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *slot.borrow_mut() = Some((callback, handle));
                let result = session.run().await;
                slot.borrow_mut().take();
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        assert_eq!(
            owned.error,
            Some(HttpErrorCode::CORO_CANCELLED),
            "{callback}"
        );
        assert_eq!(probes.len(), 1, "{callback}");
        if callback == "new_exchange" {
            assert!(
                probes[0].method.is_none(),
                "an exchange that cancelled the session from its creation gets no request"
            );
        }
        assert_eq!(
            probes[0].response_complete,
            usize::from(callback == "response_complete"),
            "{callback}: the response is not serialized after the cancel"
        );
        assert_eq!(
            probes[0].cancelled,
            [(HttpErrorCode::CORO_CANCELLED, false)],
            "{callback}"
        );
        assert_eq!(probes[0].completed, 0, "{callback}");
        assert!(
            owned.output.is_empty(),
            "{callback}: nothing is written after the cancel"
        );
        assert_eq!(owned.close, CloseKind::Reset, "{callback}");
    }
}

#[test]
fn drain_from_an_exchange_callback_closes_after_that_response() {
    for callback in ["request", "poll_headers"] {
        let (owned, completed) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                    b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
                )]);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let slot = Rc::clone(&application.drain_in);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *slot.borrow_mut() = Some((callback, handle));
                let result = session.run().await;
                slot.borrow_mut().take();
                let completed: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.completed.get())
                    .sum();
                (wire_of(&state, result), completed)
            })
        });
        assert_eq!(owned.error, None, "{callback}");
        assert_eq!(completed, 1, "{callback}");
        let text = String::from_utf8_lossy(&owned.output);
        assert!(
            text.contains("\r\nConnection: close\r\n"),
            "{callback}: a drain before the response head makes it carry Connection: close: {text:?}"
        );
        assert!(text.ends_with("\r\n\r\nhello"), "{callback}");
        assert_eq!(
            owned.close,
            CloseKind::ShutdownWrite,
            "{callback}: then the transport closes"
        );
    }
}

/// How one fenced write completes.
#[derive(Clone, Copy, Debug)]
enum WriteOutcome {
    Full,
    /// Success that reports one byte fewer than requested.
    Short,
    /// Failure with `WRITE_TIMEOUT` and no bytes written.
    Fail,
}

impl WriteOutcome {
    fn result(self, requested: usize) -> Result<WriteInfo, TransportWriteError> {
        match self {
            Self::Full => Ok(WriteInfo {
                bytes_written: requested,
            }),
            Self::Short => Ok(WriteInfo {
                bytes_written: requested - 1,
            }),
            Self::Fail => Err(TransportWriteError::new(
                HttpError::with_message(HttpErrorCode::WRITE_TIMEOUT, b"fenced write failure"),
                0,
            )),
        }
    }
}

/// Holds every write's physical completion until `open`; each write takes the next scripted
/// outcome (then `Full`).
#[derive(Default)]
struct WriteFence {
    open: Cell<bool>,
    outcomes: RefCell<VecDeque<WriteOutcome>>,
    wakers: RefCell<Vec<Waker>>,
    /// Queued writes not yet completed: requested bytes and outcome, in write order.
    receipts: RefCell<VecDeque<(usize, WriteOutcome)>>,
    direct_writes: Cell<usize>,
    queued_writes: Cell<usize>,
    /// What the write queue's `is_saturated` answers: `false` once set (the default answers
    /// `true`, as `UniplexWriteQueue`'s default does).
    unsaturated: Cell<bool>,
    saturation_queries: Cell<usize>,
    /// Run once, from inside the transport, by the named callback: `read` (a read completes),
    /// `write_complete` (a write completes) or `can_write` (the write queue's capacity check).
    reentry: RefCell<Option<(&'static str, Box<dyn Fn()>)>>,
}

impl WriteFence {
    /// Runs the reentry hook if `callback` is the one it names.
    fn reenter(&self, callback: &str) {
        let hook = self
            .reentry
            .borrow_mut()
            .take_if(|(name, _)| *name == callback);
        if let Some((_, hook)) = hook {
            hook();
        }
    }

    fn open(&self) {
        self.open.set(true);
        let wakers = std::mem::take(&mut *self.wakers.borrow_mut());
        for waker in wakers {
            waker.wake();
        }
    }

    fn poll_open(&self, context: &mut Context<'_>) -> Poll<()> {
        if self.open.get() {
            return Poll::Ready(());
        }
        self.wakers.borrow_mut().push(context.waker().clone());
        Poll::Pending
    }

    fn next_outcome(&self) -> WriteOutcome {
        self.outcomes
            .borrow_mut()
            .pop_front()
            .unwrap_or(WriteOutcome::Full)
    }
}

fn record_write(state: &RefCell<TransportState>, data: BufQueue) -> usize {
    let length = data.chain_length();
    let bytes = data.coalesce(length).unwrap().to_vec();
    state.borrow_mut().writes.push(bytes);
    length
}

/// A write queue whose receipts complete, in order, once the fence opens.
struct FenceQueue {
    fence: Rc<WriteFence>,
    state: Rc<RefCell<TransportState>>,
}

impl UniplexWriteQueue for FenceQueue {
    fn can_write(&self, _bytes: usize) -> bool {
        self.fence.reenter("can_write");
        self.fence.receipts.borrow().len() < 4
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        let length = record_write(&self.state, data);
        self.fence
            .queued_writes
            .set(self.fence.queued_writes.get() + 1);
        let outcome = self.fence.next_outcome();
        self.fence
            .receipts
            .borrow_mut()
            .push_back((length, outcome));
        Ok(())
    }

    fn poll_complete(&mut self, context: &mut Context<'_>) -> Poll<UniplexWriteCompletion> {
        if self.fence.poll_open(context).is_pending() {
            return Poll::Pending;
        }
        let Some((requested_bytes, outcome)) = self.fence.receipts.borrow_mut().pop_front() else {
            self.fence.wakers.borrow_mut().push(context.waker().clone());
            return Poll::Pending;
        };
        self.fence.reenter("write_complete");
        Poll::Ready(UniplexWriteCompletion {
            requested_bytes,
            result: outcome.result(requested_bytes),
        })
    }

    fn is_idle(&self) -> bool {
        self.fence.receipts.borrow().is_empty()
    }

    fn is_saturated(&self) -> bool {
        self.fence
            .saturation_queries
            .set(self.fence.saturation_queries.get() + 1);
        !self.fence.unsaturated.get()
    }
}

/// Reads through a [`ScriptedTransport`]; writes complete through the fence, directly or, when
/// `queued`, through a [`FenceQueue`].
struct FencedTransport {
    inner: Rc<ScriptedTransport>,
    state: Rc<RefCell<TransportState>>,
    fence: Rc<WriteFence>,
    queued: bool,
}

impl FencedTransport {
    fn new(
        reads: impl IntoIterator<Item = ReadAction>,
        queued: bool,
    ) -> (Rc<Self>, Rc<RefCell<TransportState>>, Rc<WriteFence>) {
        let (inner, state) = ScriptedTransport::new(reads);
        let fence = Rc::new(WriteFence::default());
        (
            Rc::new(Self {
                inner,
                state: Rc::clone(&state),
                fence: Rc::clone(&fence),
                queued,
            }),
            state,
            fence,
        )
    }
}

impl UniplexTransport for FencedTransport {
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn UniplexWriteQueue>>, HttpError> {
        Ok(self.queued.then(|| {
            Box::new(FenceQueue {
                fence: Rc::clone(&self.fence),
                state: Rc::clone(&self.state),
            }) as Box<dyn UniplexWriteQueue>
        }))
    }

    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let read = self
            .inner
            .read(context, maximum, allocation_size, timeout, cancellation);
        let fence = Rc::clone(&self.fence);
        Box::pin(async move {
            let result = read.await;
            fence.reenter("read");
            result
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
        let length = record_write(&self.state, data);
        self.fence
            .direct_writes
            .set(self.fence.direct_writes.get() + 1);
        let outcome = self.fence.next_outcome();
        let fence = Rc::clone(&self.fence);
        Box::pin(poll_fn(move |context| {
            fence.poll_open(context).map(|()| {
                fence.reenter("write_complete");
                outcome.result(length)
            })
        }))
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

const CLOSING_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";

#[test]
fn response_retires_only_after_its_write_completes() {
    for queued in [false, true] {
        let (owned, held, completed, direct_writes, queued_writes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state, fence) =
                    FencedTransport::new([ReadAction::Data(CLOSING_GET.to_vec())], queued);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let release = async {
                    for _ in 0..64 {
                        if !state.borrow().writes.is_empty() {
                            break;
                        }
                        context.host().yield_to_io().await;
                    }
                    // Leaves the driver turns in which it could wrongly retire the exchange.
                    for _ in 0..8 {
                        context.host().yield_to_io().await;
                    }
                    let probe = Rc::clone(&probes.borrow()[0]);
                    let held = (probe.response_complete.get(), probe.completed.get());
                    fence.open();
                    held
                };
                let (result, held) = futures::join!(session.run(), release);
                let completed = probes.borrow()[0].completed.get();
                (
                    wire_of(&state, result),
                    held,
                    completed,
                    fence.direct_writes.get(),
                    fence.queued_writes.get(),
                )
            })
        });
        assert_eq!(
            held,
            (1, 0),
            "queued {queued}: a complete response retires only once its write completes"
        );
        assert_eq!(owned.error, None, "queued {queued}");
        assert_eq!(completed, 1, "queued {queued}");
        assert!(owned.output.ends_with(b"\r\n\r\nhello"), "queued {queued}");
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "queued {queued}");
        if queued {
            assert_eq!(direct_writes, 0, "a write queue carries every write");
            assert!(queued_writes >= 1);
        } else {
            assert_eq!(queued_writes, 0);
            assert!(direct_writes >= 1);
        }
    }
}

#[test]
fn a_failed_or_short_write_fails_the_connection() {
    for queued in [false, true] {
        for (outcome, code) in [
            (WriteOutcome::Short, HttpErrorCode::TRANSPORT_WRITE_ERROR),
            (WriteOutcome::Fail, HttpErrorCode::WRITE_TIMEOUT),
        ] {
            let (owned, probes) = run_on_host(move |context| {
                Box::pin(async move {
                    let (transport, state, fence) =
                        FencedTransport::new([ReadAction::Data(CLOSING_GET.to_vec())], queued);
                    fence.open.set(true);
                    fence.outcomes.borrow_mut().push_back(outcome);
                    let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                    let (session, _handle) =
                        OwnedH1Session::new(application, owned_options(context, transport))
                            .unwrap();
                    let result = session.run().await;
                    let observed: Vec<_> = probes
                        .borrow()
                        .iter()
                        .map(|probe| probe.observed())
                        .collect();
                    (wire_of(&state, result), observed)
                })
            });
            let case = format!("queued {queued}, {outcome:?}");
            assert_eq!(owned.error, Some(code), "{case}");
            assert_eq!(owned.close, CloseKind::Reset, "{case}");
            assert_eq!(probes.len(), 1, "{case}");
            assert_eq!(
                probes[0].cancelled,
                [(code, false)],
                "{case}: the exchange whose write failed is cancelled, not retired"
            );
            assert_eq!(probes[0].completed, 0, "{case}");
            assert_eq!(probes[0].dropped, 1, "{case}");
        }
    }
}

/// Serves a keep-alive GET with the owned driver (`owned`) or `UniplexSession` and fails the
/// next read while the write of `response` is held at the fence; the write then completes with
/// `outcome`. Also returns how the transport was closed before that write completed.
fn read_error_during_write_wire(
    owned: bool,
    queued: bool,
    outcome: WriteOutcome,
    response: Response,
) -> (Wire, Vec<Observed>, CloseKind) {
    run_on_host(move |context| {
        Box::pin(async move {
            let failed = Arc::new(tokio::sync::Notify::new());
            let (transport, state, fence) = FencedTransport::new(
                [
                    ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
                    ReadAction::WaitError(Arc::clone(&failed), HttpErrorCode::TRANSPORT_READ_ERROR),
                ],
                queued,
            );
            fence.outcomes.borrow_mut().push_back(outcome);
            let script = async {
                for _ in 0..64 {
                    if !state.borrow().writes.is_empty() {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                assert!(
                    !state.borrow().writes.is_empty(),
                    "the response write starts"
                );
                failed.notify_one();
                // Leaves the session turns in which it observes the failed read.
                for _ in 0..8 {
                    context.host().yield_to_io().await;
                }
                let close = state.borrow().close;
                fence.open();
                close
            };
            let (result, close, probes) = if owned {
                let (application, probes) = H1App::new(vec![response]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let (result, close) = futures::join!(session.run(), script);
                (result, close, probes)
            } else {
                let handler = Rc::new(ScriptHandler::new(vec![response]));
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                let (result, close) = futures::join!(session.run(), script);
                (result, close, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed, close)
        })
    })
}

/// A read error that arrives while a response's write is outstanding fails the connection as
/// `UniplexSession` does: the transport resets only once the write completes, and the read
/// error stays the run's error even when that write then fails too.
#[test]
fn a_read_error_during_a_response_write_fails_the_connection_as_uniplex() {
    for queued in [false, true] {
        for outcome in [WriteOutcome::Full, WriteOutcome::Fail] {
            let case = format!("queued {queued}, {outcome:?}");
            let response = Response::fixed(204, b"");
            let (owned, owned_requests, owned_held) =
                read_error_during_write_wire(true, queued, outcome, response.clone());
            let (uniplex, uniplex_requests, uniplex_held) =
                read_error_during_write_wire(false, queued, outcome, response);
            assert_same_requests(&owned_requests, &uniplex_requests, &case);
            assert_eq!(
                (owned_held, uniplex_held),
                (CloseKind::Open, CloseKind::Open),
                "{case}: neither session resets while the write is outstanding"
            );
            assert_eq!(
                without_dates(&owned.output),
                without_dates(&uniplex.output),
                "{case}: the response written before the reset"
            );
            assert_eq!(owned.close, uniplex.close, "{case}");
            assert_eq!(owned.error, uniplex.error, "{case}: the run's error");
            assert_eq!(
                owned.error,
                Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                "{case}"
            );
            assert_eq!(owned.close, CloseKind::Reset, "{case}");
            assert_eq!(
                owned_requests[0].cancelled,
                [(HttpErrorCode::TRANSPORT_READ_ERROR, true)],
                "{case}: the failed read cancels the exchange as the peer's failure"
            );
        }
    }
}

/// As `a_read_error_during_a_response_write_fails_the_connection_as_uniplex`, with a response
/// body. Both sessions write only what is serialized when the read fails, and with no
/// per-stream task the owned driver has serialized the ready body with its head, while
/// `UniplexSession`'s stream has serialized only the head (`DIV-029`).
#[test]
fn a_read_error_during_a_response_write_writes_the_serialized_body() {
    for queued in [false, true] {
        let case = format!("queued {queued}");
        let response = Response::fixed(200, b"hello");
        let (owned, owned_requests, _) =
            read_error_during_write_wire(true, queued, WriteOutcome::Full, response.clone());
        let (uniplex, uniplex_requests, _) =
            read_error_during_write_wire(false, queued, WriteOutcome::Full, response);
        assert_same_requests(&owned_requests, &uniplex_requests, &case);
        assert_eq!(owned.close, uniplex.close, "{case}");
        assert_eq!(owned.error, uniplex.error, "{case}");
        assert_eq!(
            owned.error,
            Some(HttpErrorCode::TRANSPORT_READ_ERROR),
            "{case}"
        );
        let head = without_dates(&uniplex.output);
        assert!(
            head.ends_with("\r\nContent-Length: 5\r\n\r\n"),
            "{case}: UniplexSession writes the head it serialized: {head:?}"
        );
        assert_eq!(
            without_dates(&owned.output),
            format!("{head}hello"),
            "{case}: the owned driver writes the head and body it serialized"
        );
    }
}

#[test]
fn cancel_from_a_transport_callback_fails_the_connection() {
    // A keep-alive GET: only the cancel ends the connection. For `can_write`, one-byte reads
    // use up the turn's budget before the first request is parsed, so its response is written
    // in the turn whose capacity check cancels.
    for (callback, queued) in [
        ("read", false),
        ("write_complete", false),
        ("write_complete", true),
        ("can_write", true),
    ] {
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (reads, response) = if callback == "can_write" {
                    (
                        one_byte_reads(&pipelined_gets(3)),
                        Response::fixed(204, b""),
                    )
                } else {
                    (
                        vec![ReadAction::Data(
                            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
                        )],
                        Response::fixed(200, b"hello"),
                    )
                };
                let (transport, state, fence) = FencedTransport::new(reads, queued);
                fence.open.set(true);
                let (application, probes) = H1App::new(vec![response]);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *fence.reentry.borrow_mut() = Some((callback, Box::new(move || handle.cancel())));
                let result = session.run().await;
                fence.reentry.borrow_mut().take();
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        let case = format!("{callback}, queued {queued}");
        assert_eq!(owned.error, Some(HttpErrorCode::CORO_CANCELLED), "{case}");
        assert_eq!(owned.close, CloseKind::Reset, "{case}");
        if callback == "read" {
            assert!(
                probes.is_empty(),
                "{case}: no exchange is created after the read that cancelled the session"
            );
            assert!(owned.output.is_empty(), "{case}: nothing is written");
            continue;
        }
        assert_eq!(probes.len(), 1, "{case}");
        assert_eq!(
            probes[0].cancelled,
            [(HttpErrorCode::CORO_CANCELLED, false)],
            "{case}: the live exchange is cancelled, not retired"
        );
        assert_eq!(probes[0].completed, 0, "{case}");
        assert_eq!(probes[0].dropped, 1, "{case}");
        if callback == "write_complete" {
            assert!(
                owned.output.ends_with(b"\r\n\r\nhello"),
                "{case}: the response was written before the cancel"
            );
        } else {
            assert!(
                owned.output.is_empty(),
                "{case}: nothing is written after the cancel"
            );
        }
    }
}

#[test]
fn drain_from_a_read_admits_no_exchange() {
    // The read that carries a keep-alive GET drains the idle session from inside the transport.
    let request = || {
        vec![ReadAction::Data(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
        )]
    };
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, admitted) = run_on_host({
        let responses = responses.clone();
        move |context| {
            Box::pin(async move {
                let (transport, state, fence) = FencedTransport::new(request(), false);
                fence.open.set(true);
                let (application, probes) = H1App::new(responses);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *fence.reentry.borrow_mut() = Some((
                    "read",
                    Box::new(move || assert!(handle.initiate_drain().unwrap())),
                ));
                let result = session.run().await;
                fence.reentry.borrow_mut().take();
                let admitted = probes.borrow().len();
                (wire_of(&state, result), admitted)
            })
        }
    });
    let (uniplex, served) = run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state, fence) = FencedTransport::new(request(), false);
            fence.open.set(true);
            let handler = Rc::new(ScriptHandler::new(responses));
            let erased: HttpHandlerRef = handler.clone();
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(erased),
                h1_config(),
            )
            .unwrap();
            *fence.reentry.borrow_mut() = Some((
                "read",
                Box::new(move || assert!(handle.initiate_drain().unwrap())),
            ));
            let result = session.run().await;
            fence.reentry.borrow_mut().take();
            (wire_of(&state, result), handler.served.get())
        })
    });
    assert_eq!(uniplex.error, None);
    assert_eq!(
        served, 0,
        "UniplexSession admits no request read with the drain"
    );
    assert_eq!(admitted, 0, "nor does the owned driver");
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.error, None);
    assert_eq!(owned.close, uniplex.close);
}

#[test]
fn drain_from_a_write_completion_admits_no_read_ahead_request() {
    // Two keep-alive GETs in one read; the first response's write completion drains.
    for queued in [false, true] {
        let (owned, urls) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state, fence) = FencedTransport::new(
                    [ReadAction::Data(pipelined_gets(3)[..56].to_vec())],
                    queued,
                );
                fence.open.set(true);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *fence.reentry.borrow_mut() = Some((
                    "write_complete",
                    Box::new(move || assert!(handle.initiate_drain().unwrap())),
                ));
                let result = session.run().await;
                fence.reentry.borrow_mut().take();
                let urls: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.url.borrow().clone())
                    .collect();
                (wire_of(&state, result), urls)
            })
        });
        assert_eq!(owned.error, None, "queued {queued}");
        assert_eq!(
            urls,
            [b"/0".to_vec()],
            "queued {queued}: only the first request is admitted"
        );
        let text = String::from_utf8_lossy(&owned.output);
        assert!(text.ends_with("\r\n\r\nhello"), "queued {queued}: {text:?}");
        assert_eq!(text.matches("HTTP/1.1 200").count(), 1, "queued {queued}");
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "queued {queued}");
    }
}

#[test]
fn head_204_and_304_responses_have_no_body() {
    const CLOSING_HEAD: &[u8] = b"HEAD / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
    const CLOSING_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
    // (request, response, whether the wire must end at the head). The last case is an
    // application that sends body bytes for HEAD: both sessions write what it sends.
    for (request, response, bodiless) in [
        (CLOSING_HEAD, Response::declaring(200, 5), true),
        (CLOSING_GET, Response::fixed(204, b""), true),
        (CLOSING_GET, Response::declaring(304, 5), true),
        (CLOSING_HEAD, Response::fixed(200, b"hello"), false),
    ] {
        let reads = || vec![ReadAction::Data(request.to_vec())];
        let label = format!(
            "{} {} {bodiless}",
            String::from_utf8_lossy(&request[..4]),
            response.status
        );
        let (owned, probes) = owned_wire(reads(), vec![response.clone()]);
        let uniplex = uniplex_wire(reads(), vec![response]);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(probes[0].completed, 1, "{label}");
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label}"
        );
        assert!(
            !bodiless || owned.output.ends_with(b"\r\n\r\n"),
            "{label}: no body follows the head: {:?}",
            String::from_utf8_lossy(&owned.output)
        );
        assert_eq!(owned.close, uniplex.close, "{label}");
        assert_eq!(owned.error, uniplex.error, "{label}");
    }
}

#[test]
fn close_delimited_response_to_http10() {
    // Without a length, an HTTP/1.0 client can find the body's end only at the close, keep-alive
    // or not.
    for request in [
        &b"GET / HTTP/1.0\r\nHost: a\r\n\r\n"[..],
        &b"GET / HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\n\r\n"[..],
    ] {
        let reads = || vec![ReadAction::Data(request.to_vec())];
        let responses = vec![Response {
            content_length: false,
            ..Response::fixed(200, b"hello")
        }];
        let label = String::from_utf8_lossy(request);
        let (owned, probes) = owned_wire(reads(), responses.clone());
        let uniplex = uniplex_wire(reads(), responses);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(probes[0].completed, 1, "{label}");
        let text = String::from_utf8_lossy(&owned.output);
        assert!(
            !text.contains("Content-Length") && !text.contains("Transfer-Encoding"),
            "{label}: the body is delimited by the close: {text:?}"
        );
        assert!(text.ends_with("\r\n\r\nhello"), "{label}: {text:?}");
        assert_eq!(
            owned.close,
            CloseKind::ShutdownWrite,
            "{label}: then the transport closes"
        );
        assert_eq!(text, String::from_utf8_lossy(&uniplex.output), "{label}");
        assert_eq!(owned.close, uniplex.close, "{label}");
    }
}

/// Yields until the transport has written something, then returns what it wrote.
async fn first_writes(context: &WorkerContext, state: &Rc<RefCell<TransportState>>) -> Vec<u8> {
    for _ in 0..64 {
        if !state.borrow().writes.is_empty() {
            break;
        }
        context.host().yield_to_io().await;
    }
    state.borrow().writes.concat()
}

/// A POST whose last 7 body bytes arrive only after the first response is written, then a
/// closing GET. Returns the wire, what was written before the rest of the body arrived, and
/// how many requests the application saw.
fn early_response_run(owned: bool) -> (Wire, Vec<u8>, usize) {
    run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(
                    b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc".to_vec(),
                ),
                ReadAction::WaitData(Arc::clone(&release), b"defghij".to_vec()),
                ReadAction::Data(
                    b"GET /next HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
                ),
            ]);
            let responses = vec![Response::fixed(200, b"ok"), Response::fixed(200, b"next")];
            let releaser = async {
                let written = first_writes(&context, &state).await;
                release.notify_one();
                written
            };
            if owned {
                let (mut application, probes) = H1App::new(responses);
                application.respond_early = true;
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let (result, written) = futures::join!(session.run(), releaser);
                let seen = probes.borrow().len();
                (wire_of(&state, result), written, seen)
            } else {
                let handler = Rc::new(ScriptHandler {
                    respond_early: true,
                    ..ScriptHandler::new(responses)
                });
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                let (result, written) = futures::join!(session.run(), releaser);
                (wire_of(&state, result), written, handler.served.get())
            }
        })
    })
}

#[test]
fn early_response_before_the_request_body_ends() {
    let (owned, owned_early, owned_seen) = early_response_run(true);
    let (uniplex, uniplex_early, uniplex_seen) = early_response_run(false);
    let early = String::from_utf8_lossy(&owned_early);
    assert!(
        early.ends_with("\r\n\r\nok"),
        "the response is written before the request body ends: {early:?}"
    );
    assert_eq!(early, String::from_utf8_lossy(&uniplex_early));
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, uniplex.error);
    assert_eq!(
        (owned_seen, uniplex_seen),
        (2, 2),
        "the unread body is drained and the connection serves the next request"
    );
}

#[test]
fn response_trailers_become_a_chunked_trailer_section() {
    let reads = || {
        vec![ReadAction::Data(
            b"GET /rpc HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
        )]
    };
    let responses = vec![Response {
        content_length: false,
        trailers: vec![("grpc-status", "0")],
        ..Response::fixed(200, b"hello")
    }];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert_eq!(owned.error, None);
    assert_eq!(probes[0].completed, 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(text.contains("Transfer-Encoding: chunked\r\n"), "{text:?}");
    assert!(
        text.ends_with("\r\n\r\n5\r\nhello\r\n0\r\ngrpc-status: 0\r\n\r\n"),
        "the body, then the trailer section ends the chunked body: {text:?}"
    );
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
}

/// An `Expect: 100-continue` POST whose body is sent only once the session wrote something.
/// Returns the wire and what was written before the body arrived.
fn expect_continue_run(owned: bool) -> (Wire, Vec<u8>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(
                    b"POST /u HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\n"
                        .to_vec(),
                ),
                ReadAction::WaitData(Arc::clone(&release), b"hello".to_vec()),
            ]);
            let responses = vec![Response {
                interim: vec![100],
                ..Response::fixed(200, b"ok")
            }];
            let releaser = async {
                let written = first_writes(&context, &state).await;
                release.notify_one();
                written
            };
            let result = if owned {
                let (application, _probes) = H1App::new(responses);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                futures::join!(session.run(), releaser)
            } else {
                let handler: HttpHandlerRef = Rc::new(ScriptHandler::new(responses));
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    h1_config(),
                )
                .unwrap();
                futures::join!(session.run(), releaser)
            };
            (wire_of(&state, result.0), result.1)
        })
    })
}

#[test]
fn expect_continue_matches_uniplex() {
    let (owned, owned_early) = expect_continue_run(true);
    let (uniplex, uniplex_early) = expect_continue_run(false);
    let early = String::from_utf8_lossy(&owned_early);
    assert_eq!(
        early, "HTTP/1.1 100 \r\nDate: Tue, 03 Nov 2026 00:00:00 GMT\r\n\r\n",
        "the interim response is written before the request body is sent"
    );
    assert_eq!(early, String::from_utf8_lossy(&uniplex_early));
    let text = String::from_utf8_lossy(&owned.output);
    assert!(text.ends_with("\r\n\r\nok"), "{text:?}");
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, uniplex.error);
}

const CONNECT_HEAD: &[u8] = b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\n\r\n";
const WEBSOCKET_HEAD: &[u8] = b"GET /ws HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

/// Asserts that both applications observed the same requests.
fn assert_same_requests(owned: &[Observed], uniplex: &[Observed], label: &str) {
    let view = |observed: &[Observed]| {
        observed
            .iter()
            .map(|probe| {
                (
                    probe.method,
                    probe.url.clone(),
                    probe.fields.clone(),
                    probe.request_eom,
                    probe.request_body.clone(),
                    probe.request_trailers.clone(),
                    probe.request_finished,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        view(owned),
        view(uniplex),
        "{label}: the requests either application saw"
    );
}

#[test]
fn upgrade_under_proxygen_policy_matches_uniplex() {
    assert_eq!(
        Http1DriverConfig::default().upgrade_policy,
        Http1UpgradePolicy::Proxygen,
        "the default upgrade policy is Proxygen's, which these runs use"
    );
    for (head, response) in [
        (CONNECT_HEAD, Response::tunnel(200, &[b'y'; 100], false)),
        (WEBSOCKET_HEAD, Response::tunnel(200, &[b'y'; 100], true)),
    ] {
        let mut request = head.to_vec();
        request.extend_from_slice(&[b'x'; 100]);
        let reads = || vec![ReadAction::Data(request.clone()), ReadAction::Eof];
        let label = String::from_utf8_lossy(&head[..7]).into_owned();
        let (owned, owned_requests) = owned_wire(reads(), vec![response.clone()]);
        let (uniplex, uniplex_requests) = uniplex_wire_observed(reads(), vec![response]);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(
            owned_requests[0].request_body, [b'x'; 100],
            "{label}: the bytes after the head reach the exchange as its request body"
        );
        assert!(
            owned_requests[0].request_finished,
            "{label}: EOF ends that body"
        );
        assert!(
            owned.output.ends_with(&[b'y'; 100]),
            "{label}: the response body follows the accepting head unframed"
        );
        assert_same_requests(&owned_requests, &uniplex_requests, &label);
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label}"
        );
        assert_eq!(owned.close, uniplex.close, "{label}");
        assert_eq!(owned.error, uniplex.error, "{label}");
    }
}

#[test]
fn refused_upgrade_matches_uniplex() {
    // A refused WebSocket upgrade with a request read behind it, and a refused CONNECT.
    let mut websocket = WEBSOCKET_HEAD.to_vec();
    websocket.extend_from_slice(b"GET /later HTTP/1.1\r\nHost: a\r\n\r\n");
    for (request, status) in [(websocket, 400), (CONNECT_HEAD.to_vec(), 403)] {
        let reads = || vec![ReadAction::Data(request.clone())];
        let label = String::from_utf8_lossy(&request[..7]).into_owned();
        // The head answers before the request body is read: the refusal is what ends that body.
        let response = Response {
            tunnel: true,
            ..Response::fixed(status, b"no")
        };
        let (owned, owned_requests) = owned_wire(reads(), vec![response.clone()]);
        let (uniplex, uniplex_requests) = uniplex_wire_observed(reads(), vec![response]);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(
            owned_requests.len(),
            1,
            "{label}: nothing after the request is admitted"
        );
        assert!(owned_requests[0].request_finished, "{label}");
        assert_eq!(owned_requests[0].completed, 1, "{label}");
        assert!(owned.output.ends_with(b"\r\n\r\nno"), "{label}");
        assert!(
            String::from_utf8_lossy(&owned.output).contains("\r\nConnection: close\r\n"),
            "{label}: the codec made the connection non-reusable at the request head"
        );
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "{label}");
        assert_same_requests(&owned_requests, &uniplex_requests, &label);
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label}"
        );
        assert_eq!(owned.close, uniplex.close, "{label}");
        assert_eq!(owned.error, uniplex.error, "{label}");
    }
}

/// Pins today's behavior, shared with `UniplexSession`, for a 1xx before the final response to
/// a downstream CONNECT: the upgrade is resolved by the final head only, so a 1xx then a 2xx
/// leaves the tunnel working. Proxygen completes the request without an upgrade instead; that
/// defect is the queued `repair.http1_downstream_upgrade_interim_response`, which will change
/// this expectation for both sessions.
#[test]
fn interim_before_upgrade_final_is_pinned() {
    let mut request = CONNECT_HEAD.to_vec();
    request.extend_from_slice(&[b'x'; 10]);
    let reads = || vec![ReadAction::Data(request.clone()), ReadAction::Eof];
    let response = Response {
        interim: vec![100],
        ..Response::tunnel(200, &[b'y'; 10], false)
    };
    let (owned, owned_requests) = owned_wire(reads(), vec![response.clone()]);
    let (uniplex, uniplex_requests) = uniplex_wire_observed(reads(), vec![response]);
    assert_eq!(owned.error, None);
    assert_eq!(
        owned_requests[0].request_body, [b'x'; 10],
        "the bytes after the CONNECT head still reach the exchange as tunnel bytes"
    );
    let text = String::from_utf8_lossy(&owned.output);
    assert!(
        text.starts_with("HTTP/1.1 100 \r\n"),
        "the interim head is written first: {text:?}"
    );
    assert!(text.ends_with("yyyyyyyyyy"), "{text:?}");
    assert_same_requests(&owned_requests, &uniplex_requests, "CONNECT");
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, uniplex.error);
}

/// What one fenced retirement run observed: the response-complete and complete counts while
/// the response's write was still held, the counts after the run, the saturation queries,
/// and the wire.
struct FencedRetirement {
    held: (usize, usize),
    completed: usize,
    queries: usize,
    wire: Wire,
}

/// Serves `CLOSING_GET` through a write queue whose completions are held, answering
/// `is_saturated` with `!unsaturated`; `instrument` sets `H1App::instrument`.
fn fenced_retirement(unsaturated: bool, instrument: bool) -> FencedRetirement {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state, fence) =
                FencedTransport::new([ReadAction::Data(CLOSING_GET.to_vec())], true);
            fence.unsaturated.set(unsaturated);
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.instrument = instrument;
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let release = async {
                for _ in 0..64 {
                    if !state.borrow().writes.is_empty() {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                // Leaves the driver turns in which it could retire the exchange.
                for _ in 0..8 {
                    context.host().yield_to_io().await;
                }
                let probe = Rc::clone(&probes.borrow()[0]);
                let held = (probe.response_complete.get(), probe.completed.get());
                fence.open();
                held
            };
            let (result, held) = futures::join!(session.run(), release);
            let completed = probes.borrow()[0].completed.get();
            FencedRetirement {
                held,
                completed,
                queries: fence.saturation_queries.get(),
                wire: wire_of(&state, result),
            }
        })
    })
}

#[test]
fn unsaturated_h1_stream_retires_at_serialization() {
    let run = fenced_retirement(true, false);
    assert_eq!(
        run.held,
        (1, 1),
        "an unsaturated queue lets the exchange retire before its write completes"
    );
    assert_eq!(run.completed, 1, "it retires once");
    assert_eq!(
        run.queries, 1,
        "the queue is asked once, at END_STREAM serialization"
    );
    assert_eq!(run.wire.error, None);
    assert!(run.wire.output.ends_with(b"\r\n\r\nhello"));
    assert_eq!(
        run.wire.close,
        CloseKind::ShutdownWrite,
        "the connection still closes only after its write completes"
    );
}

#[test]
fn saturated_h1_stream_keeps_its_write_fence() {
    for (label, unsaturated, instrument, queries) in [
        ("a saturated queue", false, false, 1),
        ("a byte-event registration", true, true, 0),
    ] {
        let run = fenced_retirement(unsaturated, instrument);
        assert_eq!(
            run.held,
            (1, 0),
            "{label}: the exchange keeps its write fence"
        );
        assert_eq!(
            run.completed, 1,
            "{label}: it retires once its write completes"
        );
        assert_eq!(
            run.queries, queries,
            "{label}: only an otherwise eligible exchange asks the queue"
        );
        assert_eq!(run.wire.error, None, "{label}");
        assert_eq!(run.wire.close, CloseKind::ShutdownWrite, "{label}");
    }
}

/// Downstream entries of `FROZEN_INGRESS_ARTIFACTS` in `differential/tests/http1.rs` (the
/// `fbcode//roxygen/differential:http1` test crate) at revision `63764d2bca63`, copied
/// fragment by fragment: that constant is private to its crate. Each entry: the source
/// artifact's name and its fragments.
const COPIED_FROZEN_ARTIFACTS: &[(&str, &[&[u8]])] = &[
    (
        "fragmented_chunked_request_with_trailer",
        &[
            b"POST /resource?x=1 HTTP/1.1\r\nHost: example.com\r\n",
            b"Transfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n",
            b"0\r\nX-Trailer: done\r\n\r\n",
        ],
    ),
    (
        "pipelined_requests",
        &[b"GET /one HTTP/1.1\r\n\r\nGET /two HTTP/1.1\r\n\r\n"],
    ),
    (
        "malformed_header",
        &[b"GET /bad HTTP/1.1\r\nMalformed\r\n\r\n"],
    ),
];

#[test]
fn copied_frozen_artifacts_match_their_source() {
    // Each source artifact's fragment lengths and the FNV-1a 64 hash of its bytes, computed
    // from `differential/tests/http1.rs` at `63764d2bca63`.
    let expected: [(&str, &[usize], u64); 3] = [
        (
            "fragmented_chunked_request_with_trailer",
            &[48, 39, 22],
            0x7e66_e9ab_c18e_df79,
        ),
        ("pipelined_requests", &[42], 0xae81_04a6_2eb2_6587),
        ("malformed_header", &[32], 0xaf05_6c6a_4ad6_69f6),
    ];
    let fnv1a = |fragments: &[&[u8]]| {
        fragments
            .iter()
            .flat_map(|fragment| fragment.iter())
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    };
    let copied: Vec<_> = COPIED_FROZEN_ARTIFACTS
        .iter()
        .map(|(name, fragments)| {
            let lengths: Vec<usize> = fragments.iter().map(|part| part.len()).collect();
            (*name, lengths, fnv1a(fragments))
        })
        .collect();
    let expected: Vec<_> = expected
        .iter()
        .map(|(name, lengths, hash)| (*name, lengths.to_vec(), *hash))
        .collect();
    assert_eq!(copied, expected);
}

/// One corpus vector: the input bytes, and the script both applications replay.
struct Vector {
    name: String,
    input: Vec<u8>,
    responses: Vec<Response>,
}

impl Vector {
    fn new(name: &str, input: &[u8], responses: Vec<Response>) -> Self {
        Self {
            name: name.to_owned(),
            input: input.to_vec(),
            responses,
        }
    }
}

/// How a vector's input is cut into reads.
#[derive(Clone, Copy, Debug)]
enum Split {
    Whole,
    Every7,
    OneByte,
}

impl Split {
    fn reads(self, input: &[u8]) -> Vec<Vec<u8>> {
        match self {
            Self::Whole => vec![input.to_vec()],
            Self::Every7 => input.chunks(7).map(<[u8]>::to_vec).collect(),
            Self::OneByte => input.chunks(1).map(<[u8]>::to_vec).collect(),
        }
    }
}

/// A consumer-shaped `POST /generate`: 18 header lines and a 766-byte head, with field lengths
/// from a downstream consumer's traffic profile (a 14-byte host, a 53-byte request ID, a
/// 41-byte routing key and its copy, filler to the total) and generic names.
fn consumer_shaped_post() -> Vec<u8> {
    let body_length = 1536;
    let mut lines = vec![
        "host: c1.example.net".to_owned(),
        "content-type: application/json".to_owned(),
        "accept: text/event-stream".to_owned(),
        format!("content-length: {body_length}"),
        format!("x-request-id: {}", "r".repeat(53)),
        format!("x-routing-key: {}", "k".repeat(41)),
        format!("x-routing-key-copy: {}", "k".repeat(41)),
        "x-timeout-ms: 3600000".to_owned(),
    ];
    let start = "POST /generate HTTP/1.1\r\n";
    let fixed: usize = start.len() + lines.iter().map(|line| line.len() + 2).sum::<usize>() + 2;
    let fillers = 18 - lines.len();
    // Each filler line is `x-pad-req-N: <value>\r\n`.
    let mut remaining = 766 - fixed;
    for index in 0..fillers {
        let name = format!("x-pad-req-{index}: ");
        let left = fillers - index;
        let line = remaining / left;
        remaining -= line;
        lines.push(format!("{name}{}", "p".repeat(line - name.len() - 2)));
    }
    let mut head = start.to_owned();
    for line in &lines {
        head.push_str(line);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    assert_eq!(head.len(), 766, "the consumer-shaped head is 766 bytes");
    assert_eq!(lines.len(), 18, "of 18 header lines");
    let mut request = head.into_bytes();
    request.extend(std::iter::repeat_n(b'j', body_length));
    request
}

/// A GET whose head is `length` bytes, padded by one filler field.
fn head_of_length(length: usize) -> Vec<u8> {
    let start = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\nx-pad-req: ";
    let end = b"\r\n\r\n";
    let mut head = start.to_vec();
    head.extend(std::iter::repeat_n(b'p', length - start.len() - end.len()));
    head.extend_from_slice(end);
    head
}

fn corpus() -> Vec<Vector> {
    let ok = || vec![Response::fixed(200, b"ok")];
    let mut vectors: Vec<Vector> = COPIED_FROZEN_ARTIFACTS
        .iter()
        .map(|(name, fragments)| Vector::new(name, &fragments.concat(), ok()))
        .collect();
    let event_stream = Response {
        content_length: false,
        ..Response::fixed(200, b"data: one\n\ndata: two\n\n")
    };
    vectors.extend([
        Vector::new("consumer_shaped_post", &consumer_shaped_post(), vec![event_stream.clone()]),
        Vector::new(
            "event_stream_response",
            b"GET /events HTTP/1.1\r\nHost: a\r\nAccept: text/event-stream\r\n\r\n",
            vec![event_stream],
        ),
        Vector::new(
            "head",
            b"HEAD / HTTP/1.1\r\nHost: a\r\n\r\n",
            vec![Response::declaring(200, 5)],
        ),
        Vector::new("no_content", b"GET / HTTP/1.1\r\nHost: a\r\n\r\n", vec![Response::fixed(204, b"")]),
        Vector::new(
            "not_modified",
            b"GET / HTTP/1.1\r\nHost: a\r\nIf-None-Match: \"v1\"\r\n\r\n",
            vec![Response::declaring(304, 5)],
        ),
        Vector::new("http10", b"GET / HTTP/1.0\r\nHost: a\r\n\r\n", ok()),
        Vector::new(
            "http10_keep_alive",
            b"GET / HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "connection_close",
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "two_pipelined_requests",
            b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\nPOST /1 HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc",
            vec![Response::fixed(200, b"zero"), Response::fixed(200, b"one")],
        ),
        Vector::new(
            "content_length_and_chunked",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "duplicate_content_length",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\nabcd",
            ok(),
        ),
        Vector::new(
            "obs_fold",
            b"GET / HTTP/1.1\r\nHost: a\r\nX-Folded: one\r\n two\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "absolute_form_with_host",
            b"GET http://a.example/path?q=1 HTTP/1.1\r\nHost: b.example\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "connect",
            &[CONNECT_HEAD, b"tunnel bytes"].concat(),
            vec![Response::tunnel(200, b"tunnel reply", false)],
        ),
        Vector::new(
            "websocket_upgrade",
            &[WEBSOCKET_HEAD, b"frame bytes"].concat(),
            vec![Response::tunnel(200, b"frame reply", true)],
        ),
        Vector::new(
            "expect_continue",
            b"POST /u HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello",
            vec![Response {
                interim: vec![100],
                ..Response::fixed(200, b"ok")
            }],
        ),
        Vector::new(
            "chunked_with_trailers",
            b"POST /c HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nx-checksum: 1\r\n\r\n",
            ok(),
        ),
        Vector::new("head_at_the_limit", &head_of_length(80 << 10), ok()),
        Vector::new("head_over_the_limit", &head_of_length((80 << 10) + 1), ok()),
        Vector::new("malformed_request_line", b"GET/ HTTP/1.1\r\nHost: a\r\n\r\n", ok()),
    ]);
    vectors
}

/// `Date` values come from the clock in error responses: compare them as one placeholder.
fn without_dates(output: &[u8]) -> String {
    String::from_utf8_lossy(output)
        .split("\r\n")
        .map(|line| {
            if line.starts_with("Date: ") {
                "Date: <date>"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\r\n")
}

/// The cause an application was given for an exchange that did not finish: the cancel cause
/// (owned) or the error a request read returned (Uniplex).
fn cause(probe: &Observed) -> Option<HttpErrorCode> {
    probe
        .cancelled
        .first()
        .map(|(code, _)| *code)
        .or(probe.request_error)
}

/// What one corpus run produced, in comparable form.
#[derive(Debug, PartialEq)]
struct CorpusRun {
    output: String,
    close: CloseKind,
    error: Option<HttpErrorCode>,
    requests: Vec<(
        Option<HttpMethod>,
        Vec<u8>,
        Vec<(Vec<u8>, Vec<u8>)>,
        bool,
        Vec<u8>,
        Vec<(Vec<(Vec<u8>, Vec<u8>)>, bool)>,
        bool,
        Option<HttpErrorCode>,
    )>,
}

/// Runs `vector` cut by `split` through one session. Peer EOF follows the input once the
/// session has read all of it and settled, so EOF cannot race a response.
fn corpus_run(owned: bool, vector: &Vector, split: Split) -> CorpusRun {
    corpus_run_configured(owned, vector, split, Http1DriverConfig::default())
}

/// As `corpus_run`, with the owned driver's HTTP/1.1 configuration `http1`.
fn corpus_run_configured(
    owned: bool,
    vector: &Vector,
    split: Split,
    http1: Http1DriverConfig,
) -> CorpusRun {
    let reads = split.reads(&vector.input);
    let responses = vector.responses.clone();
    let (wire, observed) = run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new(
                reads
                    .into_iter()
                    .map(ReadAction::Data)
                    .chain([ReadAction::WaitEof(Arc::clone(&release))]),
            );
            let done = Cell::new(false);
            let releaser = async {
                let mut settled = 0;
                while !done.get() && settled < 64 {
                    if state.borrow().reads.is_empty() {
                        settled += 1;
                    }
                    context.host().yield_to_io().await;
                }
                release.notify_one();
            };
            if owned {
                let (application, probes) = H1App::new(responses);
                let mut options = owned_options(context.clone(), transport);
                options.http1 = http1;
                let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                let (result, ()) = futures::join!(run, releaser);
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            } else {
                let handler = Rc::new(ScriptHandler::new(responses));
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                let (result, ()) = futures::join!(run, releaser);
                let observed: Vec<_> = handler
                    .probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            }
        })
    });
    CorpusRun {
        output: without_dates(&wire.output),
        close: wire.close,
        error: wire.error,
        requests: observed
            .iter()
            .map(|probe| {
                (
                    probe.method,
                    probe.url.clone(),
                    probe.fields.clone(),
                    probe.request_eom,
                    probe.request_body.clone(),
                    probe.request_trailers.clone(),
                    probe.request_finished,
                    cause(probe),
                )
            })
            .collect(),
    }
}

/// Every corpus vector, fed whole, in 7-byte reads and one byte at a time, writes the same
/// bytes (dates aside) and shows the application the same events under both sessions. No
/// vector differs; DIV-029 would list any that did, each with its own assertion here.
#[test]
fn owned_h1_matches_uniplex_on_the_corpus() {
    let mut differences = Vec::new();
    for vector in corpus() {
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let owned = corpus_run(true, &vector, split);
            let uniplex = corpus_run(false, &vector, split);
            if owned != uniplex {
                differences.push(format!(
                    "{} {split:?}:\n  owned:   {owned:?}\n  uniplex: {uniplex:?}",
                    vector.name
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

fn ordinary_request_policy() -> Http1DriverConfig {
    Http1DriverConfig {
        upgrade_policy: Http1UpgradePolicy::AsOrdinaryRequest,
        ..Http1DriverConfig::default()
    }
}

/// The header fields of a request head as it reads on the wire.
fn wire_fields(head: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    String::from_utf8_lossy(head)
        .split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.as_bytes().to_vec(), value.trim().as_bytes().to_vec())
        })
        .collect()
}

/// Under `AsOrdinaryRequest` (`DIV-030`), a WebSocket upgrade request and a CONNECT request
/// are handed over as ordinary requests: no pause for an upgrade, the request behind each is
/// parsed and answered on the same connection, every response is framed, and no tunnel is
/// carried. The wire equals the default policy's for two ordinary GETs with the same responses.
#[test]
fn upgrade_as_ordinary_request_keeps_parsing_http() {
    const LATER: &[u8] = b"GET /later HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
    let responses = || vec![Response::fixed(200, b"one"), Response::fixed(200, b"two")];
    let ordinary = [b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(), LATER].concat();
    let (reference, reference_requests) = owned_wire(vec![ReadAction::Data(ordinary)], responses());
    assert_eq!(reference_requests.len(), 2);
    assert_eq!(reference.error, None);
    let reference_output = String::from_utf8_lossy(&reference.output).into_owned();
    assert!(
        reference_output.ends_with("\r\n\r\ntwo"),
        "{reference_output}"
    );
    assert_eq!(reference_output.matches("Content-Length: 3\r\n").count(), 2);
    for (head, method, url) in [
        (WEBSOCKET_HEAD, HttpMethod::Get, b"/ws".as_slice()),
        (CONNECT_HEAD, HttpMethod::Connect, b"h:443".as_slice()),
    ] {
        let label = String::from_utf8_lossy(&head[..7]).into_owned();
        let reads = vec![ReadAction::Data([head, LATER].concat())];
        let (owned, requests) =
            owned_wire_configured(reads, responses(), false, ordinary_request_policy());
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(requests.len(), 2, "{label}: the request behind is parsed");
        assert_eq!(requests[0].method, Some(method), "{label}");
        assert_eq!(requests[0].url, url, "{label}");
        assert_eq!(
            requests[0].fields,
            wire_fields(head),
            "{label}: headers unchanged"
        );
        assert!(
            requests[0].request_body.is_empty(),
            "{label}: no tunnel bytes"
        );
        assert!(requests[0].request_finished, "{label}");
        assert_eq!(requests[1].url, b"/later", "{label}");
        assert!(requests[1].request_finished, "{label}");
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&reference.output),
            "{label}: both responses framed, on one connection"
        );
        assert_eq!(owned.close, reference.close, "{label}");
    }
}

/// The corpus's `connect` and `websocket_upgrade` vectors under `AsOrdinaryRequest`
/// (`DIV-030`), every split: the head reaches the exchange as an ordinary request, and the bytes
/// behind it are parsed as the next request, which they fail as a malformed start line, with
/// the error response and close of the corpus's `malformed_request_line` vector. Apart from the
/// head, the run equals the default policy's on the same bytes behind an ordinary GET head, with
/// the same response. With the default policy the vectors still equal `UniplexSession`'s.
#[test]
fn owned_h1_corpus_upgrade_vectors_parse_as_ordinary_requests() {
    const ORDINARY_HEAD: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
    let corpus = corpus();
    let malformed = corpus
        .iter()
        .find(|vector| vector.name == "malformed_request_line")
        .unwrap();
    for vector in corpus
        .iter()
        .filter(|vector| matches!(vector.name.as_str(), "connect" | "websocket_upgrade"))
    {
        let head = if vector.name == "connect" {
            CONNECT_HEAD
        } else {
            WEBSOCKET_HEAD
        };
        let behind = &vector.input[head.len()..];
        let option_vector = Vector::new(
            &vector.name,
            &vector.input,
            vec![Response::fixed(200, b"ok")],
        );
        let ordinary_vector = Vector::new(
            "ordinary",
            &[ORDINARY_HEAD, behind].concat(),
            vec![Response::fixed(200, b"ok")],
        );
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{} {split:?}", vector.name);
            let mut option =
                corpus_run_configured(true, &option_vector, split, ordinary_request_policy());
            assert_eq!(
                option.requests.len(),
                2,
                "{label}: the bytes behind are a request"
            );
            let first = &mut option.requests[0];
            assert_eq!(first.2, wire_fields(head), "{label}: headers unchanged");
            assert!(
                first.4.is_empty(),
                "{label}: the bytes behind are not its body"
            );
            assert!(first.6, "{label}: the request is finished");
            let refused = corpus_run(true, malformed, split);
            assert!(
                option.output.ends_with(&refused.output),
                "{label}: {:?} ends with the malformed start line's {:?}",
                option.output,
                refused.output
            );
            assert_eq!(option.close, refused.close, "{label}");
            assert_eq!(option.error, refused.error, "{label}");
            assert_eq!(option.requests[1].7, refused.requests[0].7, "{label}");
            // Apart from the head itself, the run is the ordinary request's.
            let ordinary = corpus_run(true, &ordinary_vector, split);
            let reference_head = &ordinary.requests[0];
            let first = &mut option.requests[0];
            first.0 = reference_head.0;
            first.1 = reference_head.1.clone();
            first.2 = reference_head.2.clone();
            assert_eq!(option, ordinary, "{label}");
            assert_eq!(
                corpus_run(true, vector, split),
                corpus_run(false, vector, split),
                "{label}: default policy"
            );
        }
    }
}

/// A complete request followed by bytes that end, at peer EOF, inside the next request head
/// (`DIV-029`): `UniplexSession` answers the request with `Connection: close`, writes no error
/// response and shuts down its write side; the owned driver answers it keep-alive, then writes
/// the codec's 400 and resets. The requests either application sees and the session error are
/// the same. A malformed head that is complete before EOF gives the same wire in both.
#[test]
fn eof_inside_a_head_behind_a_request_differs_from_uniplex() {
    const OK: &str =
        "HTTP/1.1 200 \r\nDate: <date>\r\nConnection: keep-alive\r\nContent-Length: 2\r\n\r\nok";
    const OK_CLOSE: &str =
        "HTTP/1.1 200 \r\nDate: <date>\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok";
    const BAD_REQUEST: &str = "HTTP/1.1 400 Bad Request\r\nDate: <date>\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
    for (behind, differs) in [
        (b"tunnel bytes".as_slice(), true),
        (b"GET/ HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(), false),
    ] {
        let vector = Vector::new(
            "malformed_behind",
            &[b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(), behind].concat(),
            vec![Response::fixed(200, b"ok")],
        );
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!(
                "{} {split:?}",
                String::from_utf8_lossy(behind).escape_debug()
            );
            let owned = corpus_run(true, &vector, split);
            let uniplex = corpus_run(false, &vector, split);
            assert_eq!(owned.requests, uniplex.requests, "{label}");
            assert_eq!(owned.error, uniplex.error, "{label}");
            assert_eq!(owned.output, format!("{OK}{BAD_REQUEST}"), "{label}: owned");
            assert_eq!(owned.close, CloseKind::Reset, "{label}: owned");
            if differs {
                assert_eq!(uniplex.output, OK_CLOSE, "{label}: uniplex");
                assert_eq!(uniplex.close, CloseKind::ShutdownWrite, "{label}: uniplex");
            } else {
                assert_eq!(uniplex, owned, "{label}");
            }
        }
    }
}

/// Serves `reads`, then peer EOF once the session has read them all and settled (as
/// `corpus_run`), with the owned driver (`owned`) or `UniplexSession`; each application drops
/// its request body unread and answers at once.
fn dropping_bodies_wire(
    owned: bool,
    reads: Vec<Vec<u8>>,
    responses: Vec<Response>,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new(
                reads
                    .into_iter()
                    .map(ReadAction::Data)
                    .chain([ReadAction::WaitEof(Arc::clone(&release))]),
            );
            let done = Cell::new(false);
            let releaser = async {
                let mut settled = 0;
                while !done.get() && settled < 64 {
                    if state.borrow().reads.is_empty() {
                        settled += 1;
                    }
                    context.host().yield_to_io().await;
                }
                release.notify_one();
            };
            let (result, probes) = if owned {
                let (mut application, probes) = H1App::new(responses);
                application.respond_early = true;
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                (futures::join!(run, releaser).0, probes)
            } else {
                let handler = Rc::new(ScriptHandler {
                    respond_early: true,
                    ..ScriptHandler::new(responses)
                });
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                (futures::join!(run, releaser).0, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// A chunked request whose trailer section ends it retires once that section is read, when
/// its exchange drops the body unread, however the input is cut, and a request behind it on
/// the connection is served; both sessions write the same bytes.
#[test]
fn a_dropped_request_body_with_trailers_retires() {
    const TRAILERS: &[u8] =
        b"POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nx: v\r\n\r\n";
    const BEHIND: &[u8] = b"GET /2 HTTP/1.1\r\nHost: a\r\n\r\n";
    for (input, requests) in [(TRAILERS.to_vec(), 1), ([TRAILERS, BEHIND].concat(), 2)] {
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{requests} request(s), {split:?}");
            let responses = vec![Response::fixed(200, b"ok")];
            let (owned, probes) =
                dropping_bodies_wire(true, split.reads(&input), responses.clone());
            let (uniplex, _) = dropping_bodies_wire(false, split.reads(&input), responses);
            assert_eq!(probes.len(), requests, "{label}");
            for probe in &probes {
                assert_eq!(probe.response_complete, 1, "{label}");
                assert_eq!(probe.completed, 1, "{label}: the exchange retires once");
                assert_eq!(probe.dropped, 1, "{label}");
                assert!(probe.cancelled.is_empty(), "{label}");
            }
            let output = String::from_utf8_lossy(&owned.output);
            assert_eq!(output.matches("HTTP/1.1 200 ").count(), requests, "{label}");
            assert_eq!(output, String::from_utf8_lossy(&uniplex.output), "{label}");
            assert_eq!(owned.error, uniplex.error, "{label}");
            assert_eq!(owned.close, uniplex.close, "{label}");
        }
    }
}

/// Serves `reads` with the owned driver (`owned`) or `UniplexSession`; with `respond_early`,
/// each application answers at once and drops its request body unread.
fn early_wire(
    owned: bool,
    reads: Vec<ReadAction>,
    response: Response,
    respond_early: bool,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let (result, probes) = if owned {
                let (mut application, probes) = H1App::new(vec![response]);
                application.respond_early = respond_early;
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                (session.run().await, probes)
            } else {
                let handler = Rc::new(ScriptHandler {
                    respond_early,
                    ..ScriptHandler::new(vec![response])
                });
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                (session.run().await, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// Peer EOF read while an upgrade or CONNECT head waits for its final response, with no tunnel
/// bytes, is delivered once that response resolves the upgrade: an accepted tunnel's request
/// body ends and a refused request ends, so the exchange retires and `run` returns, with the
/// same bytes, close and result as `UniplexSession`, however the input is cut.
#[test]
fn a_pending_upgrade_keeps_a_peer_eof() {
    let mut cases = Vec::new();
    for (head, websocket, refusal) in [(CONNECT_HEAD, false, 403), (WEBSOCKET_HEAD, true, 400)] {
        let name = String::from_utf8_lossy(&head[..7]).into_owned();
        let accepting = Response {
            websocket,
            ..Response::fixed(200, &[b'r'; 64])
        };
        cases.push((head, format!("{name} early fixed"), accepting, true));
        cases.push((
            head,
            format!("{name} early tunnel"),
            Response::tunnel(200, &[b'r'; 64], websocket),
            true,
        ));
        cases.push((
            head,
            format!("{name} body read first"),
            Response::tunnel(200, &[b'r'; 64], websocket),
            false,
        ));
        cases.push((
            head,
            format!("{name} refused"),
            Response {
                tunnel: true,
                ..Response::fixed(refusal, b"no")
            },
            false,
        ));
    }
    for (head, name, response, respond_early) in cases {
        let reads = |split: &str| match split {
            "whole" => vec![ReadAction::Data(head.to_vec()), ReadAction::Eof],
            "whole with EOF" => vec![ReadAction::DataEof(head.to_vec())],
            _ => one_byte_reads(head)
                .into_iter()
                .chain([ReadAction::Eof])
                .collect(),
        };
        for split in ["whole", "whole with EOF", "one byte"] {
            let label = format!("{name}, {split}");
            let (owned, owned_requests) =
                early_wire(true, reads(split), response.clone(), respond_early);
            let (uniplex, uniplex_requests) =
                early_wire(false, reads(split), response.clone(), respond_early);
            assert_eq!(owned_requests.len(), 1, "{label}");
            assert_eq!(owned_requests[0].response_complete, 1, "{label}");
            assert_eq!(
                owned_requests[0].completed, 1,
                "{label}: the exchange retires once"
            );
            assert_same_requests(&owned_requests, &uniplex_requests, &label);
            assert_eq!(
                String::from_utf8_lossy(&owned.output),
                String::from_utf8_lossy(&uniplex.output),
                "{label}"
            );
            assert_eq!(owned.close, uniplex.close, "{label}");
            assert_eq!(owned.error, uniplex.error, "{label}");
        }
    }
}

/// Writes and closes through a [`ScriptedTransport`]; each read observes its cancellation only
/// when polled, and arranges no wake for it.
struct PolledCancellationTransport(Rc<ScriptedTransport>);

impl UniplexTransport for PolledCancellationTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        Box::pin(std::future::poll_fn(move |_| {
            if cancellation.is_cancellation_requested() {
                Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"read cancelled",
                )))
            } else {
                Poll::Pending
            }
        }))
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.0.write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.0.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.0.register_byte_event(context, registration)
    }
}

/// The idle close polls the outstanding read again after requesting its cancellation, so a read
/// that observes cancellation only when polled still completes the close.
#[test]
fn idle_close_polls_the_read_again_after_cancelling_it() {
    run_on_host(|context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let transport = Rc::new(PolledCancellationTransport(inner));
            let (application, probes) = H1App::new(Vec::new());
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.initiate_drain().unwrap());
            });
            run.unwrap();
            assert!(probes.borrow().is_empty());
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

/// An idle close parks while its outstanding read has yet to complete, and finishes once that
/// read completes.
#[test]
fn idle_close_parks_until_the_read_completes() {
    run_on_host_with(|host, context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // The read ignores cancellation: it completes only when the test releases it.
            let (transport, state) =
                ScriptedTransport::new([ReadAction::WaitData(Arc::clone(&gate), Vec::new())]);
            let (application, probes) = H1App::new(Vec::new());
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.initiate_drain().unwrap());
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                let yields = host.yields();
                for _ in 0..1000 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    host.yields(),
                    yields,
                    "the idle close kept polling while its read was outstanding"
                );
                assert_eq!(state.borrow().close, CloseKind::Open);
                gate.notify_one();
            });
            run.unwrap();
            assert!(probes.borrow().is_empty());
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}
