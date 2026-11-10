use std::sync::atomic::AtomicBool;
use std::task::Context;
use std::task::Waker;

use futures::future::Either;
use futures::future::select;
use roxygen_codec::CodecSetting;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_http2::hpack::HeaderName;
use roxygen_http2::hpack::HpackDecoder;
use roxygen_http2::hpack::HpackEncodeBuffer;
use roxygen_http2::hpack::HpackEncoder;
use roxygen_http2::hpack::HpackHeader;
use roxygen_runtime::tokio_host::ManagedWorkerError;

use super::*;
use crate::external_host_tests::ManualHost;
use crate::external_host_tests::YieldMode;
use crate::external_host_tests::after_forced_teardown;
use crate::external_host_tests::forced_teardown_of_scope;
use crate::http2::StreamReceiveWindowMode;
use crate::owned_h2::OwnedH2Application;
use crate::owned_h2::OwnedH2Exchange;
use crate::owned_h2::OwnedH2Handle;
use crate::owned_h2::OwnedH2Options;
use crate::owned_h2::OwnedH2Session;
use crate::owned_h2::OwnedH2UpstreamHandle;
use crate::owned_h2::OwnedH2UpstreamRequest;
use crate::owned_h2::OwnedH2UpstreamSession;
use crate::owned_h2::OwnedH2UpstreamSubmission;
use crate::owned_h2::stream_seed_order;
use crate::owned_h2::take_discarded_streams;
use crate::uniplex::UniplexRunSummary;
use crate::uniplex_tests::ParkedEnd;

#[derive(Clone, Copy)]
enum Reply {
    Body(usize),
    Push,
    /// The body bytes, then a trailers event carrying these fields and EOM.
    BodyThenTrailers(usize, &'static [(&'static str, &'static str)]),
}

#[derive(Default)]
struct Probe {
    requested: Cell<usize>,
    request_eom: Cell<bool>,
    request_bytes: Cell<usize>,
    request_trailer: RefCell<Option<Vec<u8>>>,
    produced_bytes: Cell<usize>,
    body_polls: Cell<usize>,
    body_event_ends: RefCell<Vec<u64>>,
    response_complete: Cell<usize>,
    completed: Cell<usize>,
    dropped: Cell<usize>,
    push_stops: Cell<usize>,
    cancelled: RefCell<Vec<(HttpErrorCode, bool)>>,
    ingress: RefCell<Option<IngressCompletion>>,
    held_body: RefCell<Option<HttpSourceHolder>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CancelAt {
    HeaderPoll,
    BodyPoll,
    HeaderEncoded,
    ResumePoll,
    ResumeDrop,
}

struct ReentrantCancel {
    at: CancelAt,
    handle: RefCell<Option<OwnedH2Handle>>,
    transport: Rc<RefCell<FixtureTransportState>>,
    writes_at_cancel: Cell<Option<usize>>,
}

impl ReentrantCancel {
    fn fire(&self, at: CancelAt) {
        if self.at == at {
            self.writes_at_cancel
                .set(Some(self.transport.borrow().writes.len()));
            self.handle.borrow().as_ref().unwrap().cancel();
        }
    }
}

struct CancelResume(Rc<ReentrantCancel>);

impl Future for CancelResume {
    type Output = ResumeStatus;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<ResumeStatus> {
        self.0.fire(CancelAt::ResumePoll);
        Poll::Ready(ResumeStatus::Signalled)
    }
}

impl Drop for CancelResume {
    fn drop(&mut self) {
        self.0.fire(CancelAt::ResumeDrop);
    }
}

#[derive(Clone, Copy)]
enum CallbackPanicAt {
    HeaderPoll,
    HeaderEncoded,
}

#[derive(Default)]
struct OwnerPollGate {
    ready: Cell<bool>,
    polls: Cell<usize>,
    self_wake_once: Cell<bool>,
    informational: Cell<bool>,
    panic_first: Cell<bool>,
    wakers: RefCell<Vec<Option<Waker>>>,
    pending_visits: Option<RefCell<Vec<usize>>>,
    control_read: RefCell<Option<Arc<tokio::sync::Notify>>>,
}

impl OwnerPollGate {
    fn release(&self) {
        self.ready.set(true);
        for waker in self.wakers.borrow().iter() {
            waker.as_ref().unwrap().wake_by_ref();
        }
    }
}

#[derive(Default)]
struct StreamOrderProbe {
    cancelled: RefCell<Vec<u64>>,
    dropped: RefCell<Vec<(u64, bool)>>,
    panic: Cell<bool>,
}

struct Application {
    replies: Vec<Reply>,
    response_priorities: Vec<Option<HttpPriority>>,
    response_date: Option<&'static [u8]>,
    retain_request: bool,
    /// Holds the request body and drains it inside the poll that produces the response's
    /// final body event.
    drain_request_last: bool,
    /// Drops the request body unread as soon as the request head arrives.
    drop_request_body: bool,
    /// Awaits the resume of each suspend event while reading the request body, as
    /// `FixtureHandler` does, instead of reading on at once.
    await_request_resume: bool,
    /// Answers with the response head at once and reads the request body before the first
    /// response body event, instead of before the head.
    upload_after_head: bool,
    /// Fails the response head with this code instead of producing it.
    response_head_error: Option<HttpErrorCode>,
    /// Answers a request body read timeout with its own 408 head, as the server's handler
    /// adapter answers a handler's `READ_TIMEOUT` result, instead of failing the head with it.
    answer_read_timeout: bool,
    /// When a request body read times out, awaits this cleanup (a sleep, if nonzero), then
    /// answers with this status and its reply's body, reporting itself a running handler until
    /// it answers.
    late_answer: Option<(u16, Duration)>,
    hold_headers: bool,
    empty_terminal: Option<Arc<tokio::sync::Notify>>,
    body_chunk: usize,
    reentrant_cancel: Option<Rc<ReentrantCancel>>,
    callback_panic: Option<(CallbackPanicAt, Arc<tokio::sync::Notify>)>,
    owner_poll_gate: Option<Rc<OwnerPollGate>>,
    byte_events: Option<Rc<ByteEventProbe>>,
    /// Registers only `TransportWrite` instead of `TransportWrite` and `KernelWrite`.
    transport_write_events: bool,
    probes: Rc<RefCell<Vec<Rc<Probe>>>>,
    stream_order: Option<Rc<StreamOrderProbe>>,
}

impl Application {
    fn new(replies: Vec<Reply>) -> (Self, Rc<RefCell<Vec<Rc<Probe>>>>) {
        let probes = Rc::new(RefCell::new(Vec::new()));
        (
            Self {
                replies,
                response_priorities: Vec::new(),
                response_date: None,
                retain_request: false,
                drain_request_last: false,
                drop_request_body: false,
                await_request_resume: false,
                upload_after_head: false,
                response_head_error: None,
                answer_read_timeout: false,
                late_answer: None,
                hold_headers: false,
                empty_terminal: None,
                body_chunk: 4096,
                reentrant_cancel: None,
                callback_panic: None,
                owner_poll_gate: None,
                byte_events: None,
                transport_write_events: false,
                probes: Rc::clone(&probes),
                stream_order: None,
            },
            probes,
        )
    }
}

struct Exchange {
    reply: Reply,
    response_priority: Option<HttpPriority>,
    response_date: Option<&'static [u8]>,
    remaining: usize,
    suspended: bool,
    terminal: Option<LocalSourceFuture<()>>,
    retain_request: bool,
    drain_request_last: bool,
    drop_request_body: bool,
    await_request_resume: bool,
    upload_after_head: bool,
    response_head_error: Option<HttpErrorCode>,
    answer_read_timeout: bool,
    late_answer: Option<(u16, Duration)>,
    cleanup: Option<Pin<Box<tokio::time::Sleep>>>,
    late_status: Option<u16>,
    answered: bool,
    drained_request: Option<HttpSourceHolder>,
    hold_headers: bool,
    body_chunk: usize,
    reentrant_cancel: Option<Rc<ReentrantCancel>>,
    callback_panic: Option<(CallbackPanicAt, LocalSourceFuture<()>)>,
    owner_poll_gate: Option<(usize, Rc<OwnerPollGate>)>,
    byte_events: Option<Rc<ByteEventProbe>>,
    transport_write_events: bool,
    upload: Option<LocalSourceFuture<Result<(), HttpError>>>,
    probe: Rc<Probe>,
    stream_order: Option<(StreamId, Rc<StreamOrderProbe>)>,
}

impl OwnedH2Application for Application {
    type Exchange = Exchange;

    fn new_exchange(&self, stream: StreamId, ingress: IngressCompletion) -> Exchange {
        let probe = Rc::new(Probe::default());
        *probe.ingress.borrow_mut() = Some(ingress);
        let ordinal = self.probes.borrow().len();
        self.probes.borrow_mut().push(Rc::clone(&probe));
        let reply = self.replies[ordinal];
        let terminal = self.empty_terminal.as_ref().map(|gate| {
            let gate = Arc::clone(gate);
            Box::pin(async move { gate.notified().await }) as LocalSourceFuture<()>
        });
        Exchange {
            reply,
            response_priority: self.response_priorities.get(ordinal).copied().flatten(),
            response_date: self.response_date,
            remaining: match reply {
                Reply::Body(size) | Reply::BodyThenTrailers(size, _) => size,
                Reply::Push => 0,
            },
            suspended: false,
            terminal,
            retain_request: self.retain_request,
            drain_request_last: self.drain_request_last,
            drop_request_body: self.drop_request_body,
            await_request_resume: self.await_request_resume,
            upload_after_head: self.upload_after_head,
            response_head_error: self.response_head_error,
            answer_read_timeout: self.answer_read_timeout,
            late_answer: self.late_answer,
            cleanup: None,
            late_status: None,
            answered: false,
            drained_request: None,
            hold_headers: self.hold_headers,
            body_chunk: self.body_chunk,
            reentrant_cancel: self.reentrant_cancel.clone(),
            callback_panic: self.callback_panic.as_ref().map(|(at, gate)| {
                let gate = Arc::clone(gate);
                (
                    *at,
                    Box::pin(async move { gate.notified().await }) as LocalSourceFuture<()>,
                )
            }),
            owner_poll_gate: self.owner_poll_gate.as_ref().map(|gate| {
                gate.wakers.borrow_mut().push(None);
                (ordinal, Rc::clone(gate))
            }),
            byte_events: self.byte_events.clone(),
            transport_write_events: self.transport_write_events,
            upload: None,
            probe,
            stream_order: self
                .stream_order
                .as_ref()
                .map(|probe| (stream, Rc::clone(probe))),
        }
    }
}

impl OwnedH2Exchange for Exchange {
    fn request(&mut self, mut head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        let mut body = body.into_source_holder();
        assert!(head.method().unwrap().is_some());
        self.probe.requested.set(self.probe.requested.get() + 1);
        self.probe.request_eom.set(eom);
        if eom || self.drop_request_body {
            drop(body);
        } else if self.drain_request_last {
            self.drained_request = Some(body);
        } else if self.retain_request {
            *self.probe.held_body.borrow_mut() = Some(body);
        } else {
            let probe = Rc::clone(&self.probe);
            let await_resume = self.await_request_resume;
            self.upload = Some(Box::pin(async move {
                loop {
                    let event = body.read_body_event(4096).await?;
                    if let Some(bytes) = event.as_body() {
                        probe
                            .request_bytes
                            .set(probe.request_bytes.get() + bytes.chain_length());
                    }
                    if let HttpBodyEventKind::Trailers(trailers) = event.kind() {
                        *probe.request_trailer.borrow_mut() =
                            Some(trailers.get_single_or_empty(b"x-check").to_vec());
                    }
                    if event.eom() {
                        return Ok(());
                    }
                    if await_resume && let HttpBodyEventKind::Suspend(resume) = event.into_kind() {
                        let _ = resume.await;
                    }
                }
            }));
        }
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if self.hold_headers {
            return Poll::Pending;
        }
        if let Some(code) = self.response_head_error {
            return Poll::Ready(Err(HttpError::with_message(
                code,
                b"scripted response head error",
            )));
        }
        if let Some((ordinal, gate)) = &self.owner_poll_gate {
            gate.polls.set(gate.polls.get() + 1);
            gate.wakers.borrow_mut()[*ordinal] = Some(context.waker().clone());
            if !gate.ready.get() {
                return Poll::Pending;
            }
            if gate.self_wake_once.replace(false) {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            if gate.informational.replace(false) {
                let mut response = HttpMessage::new();
                response.set_status_code(103).unwrap();
                response.set_http_version(1, 1);
                return Poll::Ready(Ok(HttpHeaderEvent::new(response, false).unwrap()));
            }
            if *ordinal == 0 && gate.panic_first.replace(false) {
                context.waker().wake_by_ref();
                gate.wakers.borrow()[1].as_ref().unwrap().wake_by_ref();
                panic_any("first ready owner");
            }
            if let Some(visits) = &gate.pending_visits {
                visits.borrow_mut().push(*ordinal);
                return Poll::Pending;
            }
        }
        if !self.upload_after_head
            && let Some(upload) = &mut self.upload
        {
            match upload.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error))
                    if self.answer_read_timeout && error.code() == HttpErrorCode::READ_TIMEOUT =>
                {
                    self.upload.take();
                    let mut response = HttpMessage::new();
                    response.set_status_code(408).unwrap();
                    response.set_http_version(1, 1);
                    response.set_wants_keepalive(false);
                    response
                        .headers_mut()
                        .set(HttpHeaderCode::ContentLength, b"0");
                    return Poll::Ready(Ok(HttpHeaderEvent::new(response, true).unwrap()));
                }
                Poll::Ready(Err(error))
                    if self.late_answer.is_some()
                        && error.code() == HttpErrorCode::READ_TIMEOUT =>
                {
                    let (status, cleanup) = self.late_answer.expect("checked by the guard");
                    self.upload.take();
                    self.late_status = Some(status);
                    if !cleanup.is_zero() {
                        self.cleanup = Some(Box::pin(tokio::time::sleep(cleanup)));
                    }
                }
                Poll::Ready(result) => {
                    self.upload.take();
                    result?;
                }
            }
        }
        if let Some(cleanup) = &mut self.cleanup {
            std::task::ready!(cleanup.as_mut().poll(context));
            self.cleanup.take();
        }
        let status = self.late_status.unwrap_or(200);
        let panic_at = if let Some((at, ready)) = &mut self.callback_panic {
            if ready.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            Some(*at)
        } else {
            None
        };
        self.callback_panic.take();
        if matches!(panic_at, Some(CallbackPanicAt::HeaderPoll)) {
            panic_any("application header poll");
        }
        let mut response = HttpMessage::new();
        response.set_status_code(status).unwrap();
        response.set_http_version(1, 1);
        if let Some(priority) = self.response_priority {
            response.set_http_priority(priority);
        }
        if let Reply::Body(size) = self.reply {
            response
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, size.to_string().as_bytes());
        }
        if let Some(date) = self.response_date {
            response.headers_mut().set(HttpHeaderCode::Date, date);
        }
        let mut headers =
            HttpHeaderEvent::new(response, matches!(self.reply, Reply::Body(0))).unwrap();
        if let Some(cancel) = &self.reentrant_cancel {
            cancel.fire(CancelAt::HeaderPoll);
            if cancel.at == CancelAt::HeaderEncoded {
                let cancel = Rc::clone(cancel);
                headers.set_egress_headers_callback(move |_| cancel.fire(CancelAt::HeaderEncoded));
            }
        }
        if matches!(panic_at, Some(CallbackPanicAt::HeaderEncoded)) {
            headers.set_egress_headers_callback(|_| panic_any("application encoded headers"));
        }
        self.answered = true;
        Poll::Ready(Ok(headers))
    }

    fn handler_active(&self) -> bool {
        self.late_answer.is_some() && !self.answered
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.probe.body_polls.set(self.probe.body_polls.get() + 1);
        if self.upload_after_head
            && let Some(upload) = &mut self.upload
        {
            match upload.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.upload.take();
                    result?;
                }
            }
        }
        if let Some((_, gate)) = &self.owner_poll_gate
            && let Some(ready) = gate.control_read.borrow_mut().take()
        {
            ready.notify_one();
        }
        if let Some(cancel) = &self.reentrant_cancel {
            cancel.fire(CancelAt::BodyPoll);
            if !self.suspended && matches!(cancel.at, CancelAt::ResumePoll | CancelAt::ResumeDrop) {
                self.suspended = true;
                return Poll::Ready(Ok(HttpBodyEvent::suspend(Box::pin(CancelResume(
                    Rc::clone(cancel),
                )))));
            }
        }
        match self.reply {
            Reply::Body(_) => {
                if self.remaining == 0 {
                    let terminal = self
                        .terminal
                        .as_mut()
                        .expect("the body must not be polled after its terminal event");
                    if terminal.as_mut().poll(context).is_pending() {
                        return Poll::Pending;
                    }
                    self.terminal.take();
                    return Poll::Ready(Ok(HttpBodyEvent::body(queue(&[]), true)));
                }
                let size = self.remaining.min(maximum as usize).min(self.body_chunk);
                assert!(size > 0, "a data source needs positive production capacity");
                self.remaining -= size;
                self.probe
                    .produced_bytes
                    .set(self.probe.produced_bytes.get() + size);
                let mut event = HttpBodyEvent::body(
                    queue(&vec![b'r'; size]),
                    self.remaining == 0 && self.terminal.is_none(),
                );
                if let Some(callback) = &self.byte_events {
                    self.probe
                        .body_event_ends
                        .borrow_mut()
                        .push(self.probe.produced_bytes.get() as u64);
                    event
                        .byte_event_registrations_mut()
                        .push(HttpByteEventRegistration::new(
                            None,
                            if self.transport_write_events {
                                ByteEventType::TransportWrite.bit()
                            } else {
                                ByteEventType::TransportWrite.bit()
                                    | ByteEventType::KernelWrite.bit()
                            },
                            callback,
                        ));
                }
                if event.eom()
                    && let Some(mut body) = self.drained_request.take()
                {
                    loop {
                        let mut read = body.read_body_event_unboxed(u32::MAX);
                        let Poll::Ready(read) = Pin::new(&mut read).poll(context) else {
                            panic!("the drained request body has fully arrived");
                        };
                        let read = read.expect("the drained request body reads cleanly");
                        if let Some(bytes) = read.as_body() {
                            self.probe
                                .request_bytes
                                .set(self.probe.request_bytes.get() + bytes.chain_length());
                        }
                        if read.eom() {
                            break;
                        }
                    }
                }
                Poll::Ready(Ok(event))
            }
            Reply::BodyThenTrailers(_, fields) => {
                if self.remaining == 0 {
                    let mut trailers = HttpHeaders::new();
                    for (name, value) in fields {
                        trailers.add(*name, *value);
                    }
                    return Poll::Ready(Ok(HttpBodyEvent::trailers(trailers)));
                }
                let size = self.remaining.min(maximum as usize).min(self.body_chunk);
                self.remaining -= size;
                self.probe
                    .produced_bytes
                    .set(self.probe.produced_bytes.get() + size);
                Poll::Ready(Ok(HttpBodyEvent::body(queue(&vec![b'r'; size]), false)))
            }
            Reply::Push => {
                let mut promise = HttpMessage::new();
                promise.set_method(HttpMethod::Get).unwrap();
                promise.set_url(b"/push", false).unwrap();
                promise.set_http_version(1, 1);
                promise
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
                Poll::Ready(Ok(HttpBodyEvent::push_promise(
                    promise,
                    Some(Box::new(PushSource(Rc::clone(&self.probe)))),
                    false,
                )))
            }
        }
    }

    fn cancel(&mut self, error: &HttpError, peer_reset: bool) {
        self.probe
            .cancelled
            .borrow_mut()
            .push((error.code(), peer_reset));
        self.upload.take();
        self.terminal.take();
        self.callback_panic.take();
        if let Some((stream, probe)) = &self.stream_order {
            probe.cancelled.borrow_mut().push(stream.get());
            if probe.panic.get() && stream.get() == 7 {
                panic_any(("ordered owner cancellation", stream.get()));
            }
        }
    }

    fn response_complete(&mut self) {
        self.probe
            .response_complete
            .set(self.probe.response_complete.get() + 1);
    }

    fn complete(self) {
        self.probe.completed.set(self.probe.completed.get() + 1);
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        self.probe.dropped.set(self.probe.dropped.get() + 1);
        if let Some((stream, probe)) = &self.stream_order {
            probe
                .dropped
                .borrow_mut()
                .push((stream.get(), std::thread::panicking()));
            if probe.panic.get() {
                panic_any(("ordered owner drop", stream.get()));
            }
        }
    }
}

struct PushSource(Rc<Probe>);

impl HttpSource for PushSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        panic!("unsupported push must not start its source")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        panic!("unsupported push must not read its source")
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.0.push_stops.set(self.0.push_stops.get() + 1);
    }
}

fn options(context: WorkerContext, transport: Rc<dyn UniplexTransport>) -> OwnedH2Options {
    let unlimited = RateLimitConfig::new(u64::MAX, Duration::from_millis(100), 0);
    OwnedH2Options {
        context,
        transport,
        io: UniplexConfig {
            read_timeout: Duration::ZERO,
            stream_read_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            drain_request_body_on_drop: true,
            ingress_buffer_limit: 1 << 20,
            ..UniplexConfig::default()
        },
        http2: Http2SessionConfig {
            goaway_timeout: Duration::ZERO,
            rate_limits: Http2RateLimitConfig {
                control_frames: unlimited,
                header_frames: unlimited,
                resets: unlimited,
                direct_errors: unlimited,
            },
            ..Http2SessionConfig::default()
        },
    }
}

fn ready_owner_requests(count: usize) -> (Http2Codec, Vec<u8>) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut output = BufQueue::new(1 << 20);
    peer.generate(
        &mut output,
        EgressFrame::ConnectionPreface,
        &mut IgnoreCodecEvents,
    )
    .unwrap();
    peer.generate_configured_settings(&mut output, &mut IgnoreCodecEvents)
        .unwrap();
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Get).unwrap();
    request.set_url(b"/ready", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    request.set_http_priority(HttpPriority::from_urgency_and_incremental(0, true));
    for _ in 0..count {
        let stream = peer.create_stream();
        peer.generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &request,
                end_stream: true,
                extra_headers: None,
            },
            &mut IgnoreCodecEvents,
        )
        .unwrap();
    }
    let bytes = output.coalesce(output.chain_length()).unwrap().to_vec();
    (peer, bytes)
}

async fn exercise(
    session: OwnedH2Session<Application>,
    handle: &OwnedH2Handle,
    gates: &[Arc<tokio::sync::Notify>],
    operation: impl Future<Output = ()>,
) {
    let run = async {
        let outcome =
            eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), operation)).await;
        gates.iter().for_each(|gate| gate.notify_one());
        handle.cancel();
        outcome
    };
    let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), run)
    })
    .await
    .expect("the cancelled connection joins its transport operations");
    match outcome {
        Ok(result) => result.expect("owned H2 exchange made progress"),
        Err(payload) => resume_unwind(payload),
    }
    assert_eq!(
        result.expect_err("exercise ends by cancellation").code(),
        HttpErrorCode::CORO_CANCELLED
    );
}

async fn pending_owners_after_low_stream_retirement(
    context: WorkerContext,
    order: Rc<StreamOrderProbe>,
) -> (
    LocalTransportFuture<Result<UniplexRunSummary, HttpError>>,
    OwnedH2Handle,
) {
    let release_reset = Arc::new(tokio::sync::Notify::new());
    let (mut peer, input) = ready_owner_requests(4);
    let reset = eligibility_frame(
        &mut peer,
        EgressFrame::Reset {
            stream: StreamId::new(1),
            code: H2ErrorCode::CANCEL,
        },
    );
    let (transport, _) = FixtureTransport::new([
        FixtureRead::SegmentedData(queue(&input)),
        FixtureRead::WaitData(Arc::clone(&release_reset), reset),
    ]);
    let (mut application, probes) = Application::new(vec![Reply::Body(0); 4]);
    application.hold_headers = true;
    application.stream_order = Some(Rc::clone(&order));
    assert_seed_reorders(&[1, 3, 5, 7]);
    let (session, handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    let session = session.with_stream_seed(ORDER_SEED);
    let mut run: LocalTransportFuture<Result<UniplexRunSummary, HttpError>> =
        Box::pin(session.run());
    tokio::time::timeout(Duration::from_secs(5), async {
        poll_fn(|context| {
            assert!(run.as_mut().poll(context).is_pending());
            let probes = probes.borrow();
            if probes.len() == 4 && probes.iter().all(|probe| probe.requested.get() == 1) {
                Poll::Ready(())
            } else {
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
        release_reset.notify_one();
        poll_fn(|context| {
            assert!(run.as_mut().poll(context).is_pending());
            if probes.borrow()[0].dropped.get() == 1 {
                Poll::Ready(())
            } else {
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    })
    .await
    .expect("the low stream retires while the other responses remain pending");
    assert_eq!(order.cancelled.borrow().as_slice(), &[1]);
    assert_eq!(order.dropped.borrow().as_slice(), &[(1, false)]);
    order.cancelled.borrow_mut().clear();
    order.dropped.borrow_mut().clear();
    drop(probes);
    (run, handle)
}

#[test]
fn connection_cancellation_keeps_stream_order_after_retirement() {
    run_on_worker(|context| {
        Box::pin(async move {
            let order = Rc::new(StreamOrderProbe::default());
            let (run, handle) =
                pending_owners_after_low_stream_retirement(context, Rc::clone(&order)).await;
            handle.cancel();
            let error = tokio::time::timeout(Duration::from_secs(5), run)
                .await
                .expect("connection cancellation retires all remaining owners")
                .expect_err("connection was cancelled");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(order.cancelled.borrow().as_slice(), &[3, 5, 7]);
            assert_eq!(
                order.dropped.borrow().as_slice(),
                &[(3, false), (5, false), (7, false)]
            );
        })
    });
}

#[test]
fn abandoned_streams_drop_in_order_and_preserve_first_panic() {
    run_on_worker(|context| {
        Box::pin(async move {
            let order = Rc::new(StreamOrderProbe::default());
            let (run, handle) =
                pending_owners_after_low_stream_retirement(context, Rc::clone(&order)).await;
            drop(handle);
            order.panic.set(true);
            let panic = catch_unwind(AssertUnwindSafe(|| drop(run)))
                .expect_err("the first cancellation panic survives owner destruction");
            assert_eq!(
                panic.downcast_ref::<(&'static str, u64)>(),
                Some(&("ordered owner cancellation", 7))
            );
            assert_eq!(order.cancelled.borrow().as_slice(), &[3, 5, 7]);
            assert_eq!(
                order.dropped.borrow().as_slice(),
                &[(7, false), (3, true), (5, true)]
            );
        })
    });
}

#[test]
fn terminal_head_and_small_bodies_round_trip_over_fragmented_ingress() {
    for size in [0, 1, 1024] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = peer_request_bytes(
                    HttpMethod::Post,
                    &vec![b'q'; size],
                    None,
                    PeerControl::None,
                );
                let reads = input
                    .chunks(7)
                    .map(|chunk| FixtureRead::Data(chunk.to_vec()));
                let (transport, state) =
                    FixtureTransport::new(reads.chain([FixtureRead::DataEof(Vec::new())]));
                let (application, probes) = Application::new(vec![Reply::Body(size)]);
                let (session, _) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                tokio::time::timeout(Duration::from_secs(5), session.run())
                    .await
                    .unwrap()
                    .unwrap();
                let probes = probes.borrow();
                assert_eq!(probes.len(), 1);
                let probe = &probes[0];
                assert_eq!(probe.requested.get(), 1);
                assert_eq!(probe.request_eom.get(), size == 0);
                assert_eq!(probe.request_bytes.get(), size);
                assert_eq!(probe.response_complete.get(), 1);
                assert_eq!(probe.completed.get(), 1);
                assert_eq!(probe.dropped.get(), 1);
                assert!(probe.cancelled.borrow().is_empty());
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                assert_eq!(decoded.body_data, vec![b'r'; size]);
                assert!(decoded.resets.is_empty());
                assert!(decoded.errors.is_empty());
            })
        });
    }
}

/// H2-L6: after a read that drained the transport, the driver polls its reader again only once
/// the reader's waker fires, so each read of a request that arrives in 64-byte reads is polled
/// once, after one poll that waited for the first bytes.
#[test]
fn owned_h2_polls_a_drained_reader_once_per_read() {
    let (counts, read_count) = run_on_worker(move |context| {
        Box::pin(async move {
            let input =
                peer_request_bytes(HttpMethod::Post, &vec![b'q'; 1024], None, PeerControl::None);
            let reads = input
                .chunks(64)
                .map(|chunk| FixtureRead::Data(chunk.to_vec()))
                .chain([FixtureRead::DataEof(Vec::new())]);
            let read_count = input.chunks(64).count() + 1;
            let (fixture, _state) = FixtureTransport::new(reads);
            let (transport, gate) = crate::uniplex_tests::GatedReaderTransport::new(fixture);
            let (application, probes) = Application::new(vec![Reply::Body(1024)]);
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let (result, counts) = crate::uniplex_tests::GatedReaderTransport::run_gated(
                &gate,
                tokio::time::timeout(Duration::from_secs(5), session.run()),
            )
            .await;
            result.unwrap().unwrap();
            assert_eq!(probes.borrow()[0].completed.get(), 1);
            (counts, read_count)
        })
    });
    assert_eq!(
        counts,
        (read_count + 1, read_count),
        "(polls, reads): each read polled once, after one poll that waited",
    );
}

/// What an owned H2 session showed whose drained reader stayed parked while it answered four
/// GETs, read in one read, with 256-byte bodies in 32-byte events, before `end` ended it.
struct ParkedH2 {
    /// Read starts the driver evaluated from the first read's wait until every exchange
    /// completed.
    read_start_checks: usize,
    /// The body bytes each exchange produced.
    produced: Vec<usize>,
    run: Result<(), HttpErrorCode>,
    close: FixtureClose,
}

/// Yields until `ready` holds.
async fn yield_until_ready(mut ready: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if ready() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the awaited condition never held");
}

fn parked_h2(end: ParkedEnd) -> ParkedH2 {
    run_on_worker(move |context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut reads = vec![FixtureRead::Data(retirement_requests(&mut peer, 4))];
            match end {
                ParkedEnd::PeerEof => reads.push(FixtureRead::DataEof(Vec::new())),
                ParkedEnd::ReadError(code) => reads.push(FixtureRead::Error(code)),
                ParkedEnd::Close | ParkedEnd::Cancel => {}
            }
            let (fixture, state) = FixtureTransport::new(reads);
            let (transport, gate) = crate::uniplex_tests::GatedReaderTransport::new(fixture);
            let (mut application, probes) = Application::new(vec![Reply::Body(256); 4]);
            application.body_chunk = 32;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let (run, read_start_checks) = futures::join!(
                tokio::time::timeout(Duration::from_secs(5), session.run()),
                async {
                    yield_until_ready(|| gate.waiting()).await;
                    let before = crate::owned_h2::read_start_checks();
                    gate.open_one();
                    yield_until_ready(|| {
                        let probes = probes.borrow();
                        gate.waiting()
                            && probes.len() == 4
                            && probes.iter().all(|probe| probe.completed.get() == 1)
                    })
                    .await;
                    let checks = crate::owned_h2::read_start_checks() - before;
                    match end {
                        ParkedEnd::PeerEof | ParkedEnd::ReadError(_) => gate.open_one(),
                        ParkedEnd::Close => assert!(handle.initiate_drain().unwrap()),
                        ParkedEnd::Cancel => handle.cancel(),
                    }
                    checks
                }
            );
            let run = run
                .expect("the parked session ends")
                .map(|_| ())
                .map_err(|error| error.code());
            let produced = probes
                .borrow()
                .iter()
                .map(|probe| probe.produced_bytes.get())
                .collect();
            ParkedH2 {
                read_start_checks,
                produced,
                run,
                close: state.borrow().close,
            }
        })
    })
}

/// H2-L6b: while a drained reader waits for its waker ("parked"), each driver turn leaves the
/// read phase without evaluating a read start. Only the turn that parks the drained reader's
/// next read evaluates one (it stamps the read's `read_idle` deadline); the turns that produce
/// four responses in 32 body events evaluate none; without the fast exit each would evaluate one.
#[test]
fn owned_h2_a_parked_reader_evaluates_no_read_start_while_responses_are_produced() {
    let parked = parked_h2(ParkedEnd::PeerEof);
    assert_eq!(
        parked.produced, [256; 4],
        "four 256-byte bodies (32 events) were produced while the reader was parked"
    );
    assert_eq!(
        parked.read_start_checks, 1,
        "only the turn that parks the reader evaluates a read start"
    );
    assert_eq!(parked.run, Ok(()));
}

/// H2-L6b: a parked reader still ends. The next read's EOF or failure wakes it, and the driver's
/// own close and abort mark its read wake in the same call (`close_idle`, `reset_transport`), so
/// each parked session ends as it did before the fast exit.
#[test]
fn owned_h2_a_parked_reader_still_ends_on_its_wake_close_and_abort() {
    for (end, run, close) in [
        (ParkedEnd::PeerEof, Ok(()), FixtureClose::Closed),
        (
            ParkedEnd::ReadError(HttpErrorCode::TRANSPORT_READ_ERROR),
            Err(HttpErrorCode::TRANSPORT_READ_ERROR),
            FixtureClose::Reset,
        ),
        (
            ParkedEnd::ReadError(HttpErrorCode::DROPPED),
            Err(HttpErrorCode::DROPPED),
            FixtureClose::Reset,
        ),
        (ParkedEnd::Close, Ok(()), FixtureClose::Closed),
        (
            ParkedEnd::Cancel,
            Err(HttpErrorCode::CORO_CANCELLED),
            FixtureClose::Reset,
        ),
    ] {
        let parked = parked_h2(end);
        assert_eq!(parked.produced, [256; 4], "{end:?}");
        assert_eq!((parked.run, parked.close), (run, close), "{end:?}");
    }
}

/// The driver reads through the transport's reader when it offers one, and writes the same bytes
/// as when it reads through `read`.
#[test]
fn owned_h2_reads_through_the_transport_reader() {
    let serve = |reader: bool| {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = peer_request_bytes(
                    HttpMethod::Post,
                    &vec![b'q'; 1024],
                    None,
                    PeerControl::None,
                );
                let reads = input
                    .chunks(7)
                    .map(|chunk| FixtureRead::Data(chunk.to_vec()))
                    .chain([FixtureRead::DataEof(Vec::new())]);
                let (fixture, state) = FixtureTransport::new(reads);
                let read_count = input.chunks(7).count() + 1;
                let (transport, counts): (Rc<dyn UniplexTransport>, _) = if reader {
                    let (transport, counts) = crate::uniplex_tests::ReaderTransport::new(fixture);
                    (transport, Some(counts))
                } else {
                    (fixture, None)
                };
                let (application, probes) = Application::new(vec![Reply::Body(1024)]);
                let (session, _) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                tokio::time::timeout(Duration::from_secs(5), session.run())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(probes.borrow()[0].completed.get(), 1);
                let counts = counts.map(|counts| (counts.direct.get(), counts.reader.get()));
                (state.borrow().writes.concat(), counts, read_count)
            })
        })
    };
    let (direct, _, _) = serve(false);
    let (through_reader, counts, read_count) = serve(true);
    assert_eq!(
        counts,
        Some((0, read_count)),
        "every read goes through the reader"
    );
    assert!(
        through_reader == direct,
        "the reader path writes the same bytes"
    );
}

#[test]
fn held_write_allows_bounded_body_progress_without_completing_the_exchange() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (mut input, _) = peer_request_with_stream_window(1 << 20, 1, false);
            input.extend(peer_connection_window_update(1 << 20));
            let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
            let release_write = Arc::new(tokio::sync::Notify::new());
            state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let (mut application, probes) = Application::new(vec![Reply::Body(256 << 10)]);
            application.body_chunk = usize::MAX;
            let byte_events = Rc::new(ByteEventProbe::default());
            application.byte_events = Some(Rc::clone(&byte_events));
            let mut config = options(context, transport);
            config.io.buffer_limit = 16 << 10;
            config.io.maximum_write_size = 4096;
            config.io.maximum_read_size = 4096;
            config.io.read_allocation_size = 4096;
            config.io.ingress_buffer_limit = 16 << 10;
            let buffer_limit = config.io.buffer_limit;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            exercise(session, &handle, &[Arc::clone(&release_write)], async {
            complete_credit_wait(|| state.borrow().write_gate_pending.is_some() && probes.borrow().len() == 1).await;
            let probe = Rc::clone(&probes.borrow()[0]);
            complete_credit_wait(|| probe.produced_bytes.get() > 4096).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            let produced = probe.produced_bytes.get();
            assert!(produced <= 2 * buffer_limit, "one held write, bounded staging, and one pending body event must not drain a 256 KiB source");
            assert!(state.borrow().writes.is_empty());
            assert!(byte_events.delivered.borrow().is_empty());
            assert_eq!(probe.completed.get(), 0);
            assert_eq!(probe.dropped.get(), 0);
            release_write.notify_one();
            complete_credit_wait(|| probe.completed.get() == 1).await;
            assert_eq!(probe.response_complete.get(), 1);
            assert_eq!(probe.dropped.get(), 1);
            assert!(probe.cancelled.borrow().is_empty());
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert_eq!(decoded.body_data, vec![b'r'; 256 << 10]);
            assert!(decoded.errors.is_empty());
            assert!(byte_events.cancelled.borrow().is_empty());
            let wire = state.borrow().writes.concat();
            let original_ends = probe.body_event_ends.borrow();
            let mut expected_offsets = Vec::new();
            let mut offset = 0;
            let mut body_offset = 0;
            while offset < wire.len() {
                let length = (usize::from(wire[offset]) << 16)
                    | (usize::from(wire[offset + 1]) << 8)
                    | usize::from(wire[offset + 2]);
                let is_data = wire[offset + 3] == FrameType::DATA.as_u8();
                offset += FRAME_HEADER_SIZE + length;
                if is_data {
                    body_offset += length as u64;
                    if original_ends.contains(&body_offset) {
                        expected_offsets.push((body_offset, offset as u64));
                    }
                }
            }
            assert_eq!(expected_offsets.len(), original_ends.len());
            for kind in [ByteEventType::TransportWrite, ByteEventType::KernelWrite] {
                let events = byte_events.delivered.borrow();
                let actual: Vec<_> = events.iter().filter(|event| event.event_type == kind)
                    .map(|event| (event.body_offset, event.transport_offset)).collect();
                assert_eq!(actual, expected_offsets, "a split body's observer remains on the original event's last byte");
                assert_eq!(events.iter().filter(|event| event.event_type == kind && event.eom).count(), 1);
            }
        }).await;
        })
    });
}

async fn zero_credit_case(context: WorkerContext, connection: bool) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    peer.egress_settings_mut().set(
        SettingsId::InitialWindowSize,
        if connection { 131_072 } else { 0 },
    );
    let mut preface = BufQueue::new(4096);
    peer.generate(
        &mut preface,
        EgressFrame::ConnectionPreface,
        &mut IgnoreCodecEvents,
    )
    .unwrap();
    peer.generate_configured_settings(&mut preface, &mut IgnoreCodecEvents)
        .unwrap();
    let mut initial = preface.coalesce(preface.chain_length()).unwrap().to_vec();
    initial.extend(eligibility_request(&mut peer, 0));
    let mut siblings = eligibility_request(&mut peer, 1);
    siblings.extend(eligibility_request(&mut peer, 2));
    let mut control = eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(42) });
    for stream in [StreamId::new(1), StreamId::new(3)] {
        if !connection && stream == StreamId::new(3) {
            control.extend(eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate { stream, amount: 1 },
            ));
        }
        control.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Reset {
                stream,
                code: H2ErrorCode::CANCEL,
            },
        ));
        for amount in [i32::MAX as u32, 1] {
            control.extend(eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate { stream, amount },
            ));
        }
    }
    if connection {
        control.extend(eligibility_frame(
            &mut peer,
            EgressFrame::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 1,
            },
        ));
    }
    let credit = eligibility_frame(
        &mut peer,
        EgressFrame::WindowUpdate {
            stream: if connection {
                StreamId::UNSPECIFIED
            } else {
                StreamId::new(5)
            },
            amount: 1,
        },
    );
    let send_siblings = Arc::new(tokio::sync::Notify::new());
    let send_control = Arc::new(tokio::sync::Notify::new());
    let send_credit = Arc::new(tokio::sync::Notify::new());
    let (transport, state) = FixtureTransport::new([
        FixtureRead::Data(initial),
        FixtureRead::WaitData(Arc::clone(&send_siblings), siblings),
        FixtureRead::WaitData(Arc::clone(&send_control), control),
        FixtureRead::WaitData(Arc::clone(&send_credit), credit),
    ]);
    let (application, probes) = Application::new(vec![
        Reply::Body(if connection { 65_536 } else { 1 }),
        Reply::Body(1),
        Reply::Body(1),
    ]);
    let (session, handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    exercise(
        session,
        &handle,
        &[
            Arc::clone(&send_siblings),
            Arc::clone(&send_control),
            Arc::clone(&send_credit),
        ],
        async {
            complete_credit_wait(|| {
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                decoded.header_streams == [StreamId::new(1)]
                    && decoded.body_bytes == if connection { 65_535 } else { 0 }
            })
            .await;
            send_siblings.notify_one();
            complete_credit_wait(|| {
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
                decoded.header_streams.len() == 3
                    && decoded.body_bytes == if connection { 65_535 } else { 0 }
            })
            .await;
            let first = Rc::clone(&probes.borrow()[0]);
            let second = Rc::clone(&probes.borrow()[1]);
            let sibling = Rc::clone(&probes.borrow()[2]);
            assert_eq!(first.completed.get(), 0);
            assert_eq!(second.completed.get(), 0);
            assert_eq!(sibling.completed.get(), 0);
            send_control.notify_one();
            complete_credit_wait(|| {
                first.cancelled.borrow().len() == 1
                    && second.cancelled.borrow().len() == 1
                    && decode_server_frames_with_requests(&state.borrow().writes.concat(), 3)
                        .ping_replies
                        == [42]
                    && (!connection || sibling.completed.get() == 1)
            })
            .await;
            for reset in [&first, &second] {
                assert_eq!(
                    reset.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CANCEL, true)]
                );
            }
            assert!(sibling.cancelled.borrow().is_empty());
            let blocked = decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
            assert_eq!(blocked.body_bytes, if connection { 65_536 } else { 0 });
            assert!(
                !blocked.body_streams.contains(&StreamId::new(3)),
                "a second reset's pending DATA is ineligible before its owner is polled"
            );
            assert!(blocked.resets.is_empty(), "a peer reset must not be echoed");
            send_credit.notify_one();
            complete_credit_wait(|| {
                sibling.completed.get() == 1
                    && first.dropped.get() == 1
                    && second.dropped.get() == 1
            })
            .await;
            let decoded = decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
            assert_eq!(decoded.body_bytes, if connection { 65_536 } else { 1 });
            assert_eq!(decoded.body_streams.last(), Some(&StreamId::new(5)));
            assert!(!decoded.body_streams.contains(&StreamId::new(3)));
            for reset in [&first, &second] {
                assert_eq!(reset.completed.get(), 0);
                assert_eq!(reset.response_complete.get(), 0);
                assert_eq!(reset.dropped.get(), 1);
            }
            assert_eq!(sibling.response_complete.get(), 1);
            assert_eq!(sibling.dropped.get(), 1);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        },
    )
    .await;
}

#[test]
fn zero_stream_credit_preserves_ping_reset_and_sibling_progress() {
    run_on_worker(|context| Box::pin(zero_credit_case(context, false)));
}

#[test]
fn zero_connection_credit_preserves_ping_reset_and_sibling_progress() {
    run_on_worker(|context| Box::pin(zero_credit_case(context, true)));
}

#[test]
fn early_response_and_producer_eom_do_not_retire_an_unconsumed_raw_read() {
    for (incomplete, late_stop) in [(false, false), (true, false), (true, true)] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let send_eom = Arc::new(tokio::sync::Notify::new());
                let input = if incomplete {
                    peer_incomplete_post_bytes(1)
                } else {
                    peer_request_bytes(HttpMethod::Post, b"x", None, PeerControl::None)
                };
                let mut reads = vec![FixtureRead::Data(input)];
                if incomplete {
                    let mut peer = Http2Codec::new(TransportDirection::Upstream);
                    let stream = peer.create_stream();
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&send_eom),
                        eligibility_frame(&mut peer, EgressFrame::EndOfMessage { stream }),
                    ));
                }
                let (transport, state) = FixtureTransport::new(reads);
                let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
                application.retain_request = true;
                let (session, handle) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                exercise(session, &handle, &[Arc::clone(&send_eom)], async {
                    complete_credit_wait(|| {
                        decode_server_frames(&state.borrow().writes.concat()).header_statuses
                            == [(StreamId::new(1), 200)]
                    })
                    .await;
                    let probe = Rc::clone(&probes.borrow()[0]);
                    let completion = probe.ingress.borrow().as_ref().unwrap().clone();
                    assert_eq!(completion.snapshot().is_none(), incomplete);
                    assert_eq!(completion.body_bytes_received(), 1);
                    assert_eq!(probe.response_complete.get(), 1);
                    assert_eq!(probe.completed.get(), 0);
                    assert_eq!(probe.dropped.get(), 0);
                    let mut body = probe.held_body.borrow_mut().take().unwrap();
                    let raw_read = body.read_body_event(1);
                    for _ in 0..4 {
                        tokio::task::yield_now().await;
                    }
                    assert_eq!(
                        probe.completed.get(),
                        0,
                        "admitting a read is not consuming its terminal event"
                    );
                    assert_eq!(probe.dropped.get(), 0);
                    let event = raw_read.await.unwrap();
                    assert_eq!(event.eom(), !incomplete);
                    assert_eq!(event.as_body().unwrap().coalesce(1).unwrap().as_ref(), b"x");
                    if incomplete {
                        match body.read_body_event(1).await.unwrap().into_kind() {
                            HttpBodyEventKind::Suspend(resume) => drop(resume),
                            _ => panic!("an empty request body first yields its suspend event"),
                        }
                        let mut terminal = body.read_body_event(1);
                        poll_fn(|context| {
                            assert!(terminal.as_mut().poll(context).is_pending());
                            Poll::Ready(())
                        })
                        .await;
                        send_eom.notify_one();
                        completion.wait().await.unwrap();
                        assert_eq!(
                            probe.completed.get(),
                            0,
                            "producer EOM cannot consume a retained terminal read"
                        );
                        if late_stop {
                            body.stop_reading(Some(HttpErrorCode::CANCEL));
                            for _ in 0..4 {
                                tokio::task::yield_now().await;
                            }
                            assert!(completion.snapshot().unwrap().is_ok());
                            assert_eq!(probe.response_complete.get(), 1);
                            assert_eq!(probe.completed.get(), 0);
                            assert_eq!(probe.dropped.get(), 0);
                            assert!(probe.cancelled.borrow().is_empty());
                            assert_eq!(
                                terminal
                                    .await
                                    .err()
                                    .expect("the retained terminal read observes the late stop")
                                    .code(),
                                HttpErrorCode::CANCEL
                            );
                        } else {
                            let event = terminal.await.unwrap();
                            assert!(event.eom());
                            assert!(event.as_body().unwrap().is_empty());
                        }
                    }
                    drop(body);
                    complete_credit_wait(|| probe.completed.get() == 1).await;
                    assert_eq!(probe.dropped.get(), 1);
                    assert!(probe.cancelled.borrow().is_empty());
                    assert!(completion.snapshot().unwrap().is_ok());
                    let decoded = decode_server_frames(&state.borrow().writes.concat());
                    assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                    assert!(decoded.resets.is_empty());
                    assert!(decoded.errors.is_empty());
                })
                .await;
            })
        });
    }
}

#[test]
fn closed_protocol_updates_preserve_terminal_write_ownership_and_sibling_progress() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut preface = BufQueue::new(4096);
            peer.generate(
                &mut preface,
                EgressFrame::ConnectionPreface,
                &mut IgnoreCodecEvents,
            )
            .unwrap();
            peer.generate_configured_settings(&mut preface, &mut IgnoreCodecEvents)
                .unwrap();
            let mut input = preface.coalesce(preface.chain_length()).unwrap().to_vec();
            input.extend(eligibility_request(&mut peer, 0));
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::new(1),
                    amount: 1,
                },
            ));
            let mut closed_updates = eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::new(1),
                    amount: i32::MAX as u32,
                },
            );
            closed_updates.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: i32::MAX as u64,
                    }],
                },
            ));
            closed_updates.extend(eligibility_frame(
                &mut peer,
                EgressFrame::PingRequest { data: Some(43) },
            ));
            closed_updates.extend(eligibility_request(&mut peer, 0));
            let release_write = Arc::new(tokio::sync::Notify::new());
            let send_closed_updates = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&send_closed_updates), closed_updates),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let (application, probes) = Application::new(vec![Reply::Body(0); 2]);
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(
                session,
                &handle,
                &[Arc::clone(&release_write), Arc::clone(&send_closed_updates)],
                async {
                    complete_credit_wait(|| {
                        state.borrow().write_gate_pending.is_some()
                            && probes
                                .borrow()
                                .first()
                                .is_some_and(|probe| probe.response_complete.get() == 1)
                    })
                    .await;
                    let first = Rc::clone(&probes.borrow()[0]);
                    assert_eq!(first.completed.get(), 0);
                    assert_eq!(first.dropped.get(), 0);
                    send_closed_updates.notify_one();
                    complete_credit_wait(|| {
                        probes
                            .borrow()
                            .get(1)
                            .is_some_and(|probe| probe.response_complete.get() == 1)
                    })
                    .await;
                    let sibling = Rc::clone(&probes.borrow()[1]);
                    assert_eq!(first.completed.get(), 0);
                    assert_eq!(first.dropped.get(), 0);
                    assert_eq!(sibling.completed.get(), 0);
                    assert!(state.borrow().writes.is_empty());
                    release_write.notify_one();
                    complete_credit_wait(|| {
                        first.completed.get() == 1 && sibling.completed.get() == 1
                    })
                    .await;
                    assert_eq!(first.response_complete.get(), 1);
                    assert_eq!(first.dropped.get(), 1);
                    assert_eq!(sibling.dropped.get(), 1);
                    assert!(first.cancelled.borrow().is_empty());
                    assert!(sibling.cancelled.borrow().is_empty());
                    let decoded =
                        decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                    assert_eq!(
                        decoded.header_statuses,
                        [(StreamId::new(1), 200), (StreamId::new(3), 200)]
                    );
                    assert_eq!(decoded.ping_replies, [43]);
                    assert!(decoded.resets.is_empty());
                    assert!(decoded.errors.is_empty());
                },
            )
            .await;
        })
    });
}

#[test]
fn duplicate_content_length_emits_400_without_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_duplicate_content_length_request();
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (application, probes) = Application::new(vec![Reply::Body(0)]);
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .unwrap();
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert_eq!(decoded.header_statuses, [(StreamId::new(1), 400)]);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
            let probes = probes.borrow();
            assert_eq!(
                probes.len(),
                1,
                "rejected heads still create their accounting owner"
            );
            assert_eq!(probes[0].requested.get(), 0);
            assert_eq!(probes[0].cancelled.borrow().len(), 1);
            assert_eq!(probes[0].completed.get(), 0);
            assert_eq!(probes[0].dropped.get(), 1);
        })
    });
}

#[test]
fn content_length_mismatch_cancels_once_with_the_typed_error() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(
                peer_content_length_mismatch_request(),
            )]);
            let (application, probes) = Application::new(vec![Reply::Body(0)]);
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .unwrap();
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert_eq!(
                decoded.resets,
                [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
            );
            assert!(decoded.errors.is_empty());
            let probes = probes.borrow();
            assert_eq!(probes.len(), 1);
            assert_eq!(
                probes[0].cancelled.borrow().as_slice(),
                &[(HttpErrorCode::CONTENT_LENGTH_MISMATCH, false)]
            );
            assert_eq!(probes[0].completed.get(), 0);
            assert_eq!(probes[0].dropped.get(), 1);
        })
    });
}

/// Records each request's method as sent (`HttpMessage::request_method`) and answers it 200,
/// with no body.
struct MethodApplication(Rc<RefCell<Vec<String>>>);

struct MethodExchange(Rc<RefCell<Vec<String>>>);

impl OwnedH2Application for MethodApplication {
    type Exchange = MethodExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> MethodExchange {
        MethodExchange(Rc::clone(&self.0))
    }
}

/// A 200 head that ends the response.
fn bodiless_ok() -> HttpHeaderEvent {
    let mut response = HttpMessage::new();
    response.set_status_code(200).unwrap();
    response.set_http_version(1, 1);
    HttpHeaderEvent::new(response, true).unwrap()
}

impl OwnedH2Exchange for MethodExchange {
    fn request(&mut self, head: Box<HttpMessage>, _body: HttpStreamSourceOwner, _eom: bool) {
        self.0
            .borrow_mut()
            .push(head.request_method().unwrap().to_owned());
    }

    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        Poll::Ready(Ok(bodiless_ok()))
    }

    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        unreachable!("the head ends the response")
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

/// `MethodApplication` as a `UniplexSession` handler.
struct MethodHandler(Rc<RefCell<Vec<String>>>);

impl HttpHandler for MethodHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let seen = Rc::clone(&self.0);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            seen.borrow_mut()
                .push(head.headers().request_method().unwrap().to_owned());
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("a 200 response"),
            )))
        })
    }
}

/// The client preface, empty SETTINGS, and one bodiless request per method on streams 1, 3,
/// 5, ..., each `:method` encoded as given.
fn method_requests(methods: &[&[u8]]) -> Vec<u8> {
    const END_HEADERS: u8 = 0x4;
    let mut wire = CONNECTION_PREFACE.to_vec();
    wire.extend_from_slice(&[0, 0, 0, FrameType::SETTINGS.as_u8(), 0, 0, 0, 0, 0]);
    let mut encoder = HpackEncoder::new(false);
    for (index, method) in methods.iter().enumerate() {
        let stream = 1 + 2 * u32::try_from(index).unwrap();
        let block = encoder.encode(&[
            HpackHeader::new(":method", method),
            HpackHeader::new(":scheme", "http"),
            HpackHeader::new(":path", "/generate"),
            HpackHeader::new(":authority", "example.com"),
        ]);
        wire.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes()[1..]);
        wire.extend_from_slice(&[FrameType::HEADERS.as_u8(), END_HEADERS | END_STREAM]);
        wire.extend_from_slice(&stream.to_be_bytes());
        wire.extend_from_slice(&block);
    }
    wire
}

/// What one method run produced: the methods the application read, as sent, in sorted order;
/// each response head's status, by stream; the RST_STREAM and GOAWAY frames written; and the
/// session's error. The order is normalized because the two sessions start and answer their
/// streams in different orders.
#[derive(Debug, PartialEq)]
struct MethodRun {
    methods: Vec<String>,
    statuses: Vec<(StreamId, u16)>,
    resets: Vec<(StreamId, H2ErrorCode)>,
    goaways: Vec<(StreamId, H2ErrorCode)>,
    error: Option<HttpErrorCode>,
}

/// Serves `methods` (`method_requests`), then peer EOF, with the owned driver (`owned`) or
/// `UniplexSession`, both configured as `options` with `exact_method_tokens` set to `exact`.
fn method_run(owned: bool, exact: bool, methods: &[&[u8]]) -> MethodRun {
    let input = method_requests(methods);
    let requests = methods.len();
    run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let mut config = options(context, transport);
            config.io.exact_method_tokens = exact;
            let seen = Rc::new(RefCell::new(Vec::new()));
            let result = if owned {
                let (session, _) =
                    OwnedH2Session::new(MethodApplication(Rc::clone(&seen)), config).unwrap();
                tokio::time::timeout(Duration::from_secs(5), session.run()).await
            } else {
                let OwnedH2Options {
                    context,
                    transport,
                    io,
                    http2,
                } = config;
                let handler: Rc<dyn HttpHandler> = Rc::new(MethodHandler(Rc::clone(&seen)));
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    io,
                    http2,
                )
                .unwrap();
                tokio::time::timeout(Duration::from_secs(5), session.run()).await
            };
            let error = result
                .expect("the run ends")
                .err()
                .map(|error| error.code());
            let mut decoded =
                decode_server_frames_with_requests(&state.borrow().writes.concat(), requests);
            decoded.header_statuses.sort_by_key(|(stream, _)| *stream);
            let mut methods = seen.take();
            methods.sort();
            MethodRun {
                methods,
                statuses: decoded.header_statuses,
                resets: decoded.resets,
                goaways: decoded.goaways,
                error,
            }
        })
    })
}

/// Under `exact_method_tokens` (`DIV-040`), the owned driver hands each token `:method` to its
/// exchange as sent: `post`, `Post` and `PURGE` as extension methods, kept byte for byte. The
/// application reads what `UniplexSession` reads under the option, and every stream is
/// answered as there.
#[test]
fn exact_method_tokens_reach_each_stream_as_sent_as_uniplex() {
    const METHODS: [&[u8]; 4] = [b"POST", b"post", b"Post", b"PURGE"];
    let owned = method_run(true, true, &METHODS);
    assert_eq!(owned.methods, ["POST", "PURGE", "Post", "post"]);
    assert_eq!(
        owned.statuses,
        [1, 3, 5, 7].map(|stream| (StreamId::new(stream), 200))
    );
    assert!(owned.resets.is_empty());
    assert!(owned.goaways.is_empty());
    assert_eq!(owned, method_run(false, true, &METHODS));
}

/// Under `exact_method_tokens`, a `:method` that is not a token, the empty one included, is
/// still malformed: the owned driver answers the stream with the session's 400 and hands no
/// request to its exchange, as `UniplexSession` does under the option.
#[test]
fn exact_method_tokens_still_make_a_non_token_method_malformed_as_uniplex() {
    for method in [&b"P@ST"[..], b"(GET)", b"POST ", b"", b"PO\x7fST"] {
        let label = String::from_utf8_lossy(method).escape_debug().to_string();
        let owned = method_run(true, true, &[method]);
        assert!(owned.methods.is_empty(), "{label}: {:?}", owned.methods);
        assert_eq!(owned.statuses, [(StreamId::new(1), 400)], "{label}");
        assert!(owned.resets.is_empty(), "{label}");
        assert!(owned.goaways.is_empty(), "{label}");
        assert_eq!(owned, method_run(false, true, &[method]), "{label}");
    }
}

/// Without `exact_method_tokens`, the owned driver keeps the reference reading, as
/// `UniplexSession` does: recognized methods are canonicalized without regard to case and
/// extension methods are uppercased.
#[test]
fn request_methods_are_canonicalized_by_default_as_uniplex() {
    const METHODS: [&[u8]; 4] = [b"POST", b"post", b"Post", b"purge"];
    let owned = method_run(true, false, &METHODS);
    assert_eq!(owned.methods, ["POST", "POST", "POST", "PURGE"]);
    assert_eq!(owned, method_run(false, false, &METHODS));
}

/// Never answers, and records each exchange dropped while its response is owed.
struct HeldApplication(Rc<Cell<usize>>);

struct HeldExchange(Rc<Cell<usize>>);

impl OwnedH2Application for HeldApplication {
    type Exchange = HeldExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> HeldExchange {
        HeldExchange(Rc::clone(&self.0))
    }
}

impl OwnedH2Exchange for HeldExchange {
    fn request(&mut self, _head: Box<HttpMessage>, _body: HttpStreamSourceOwner, _eom: bool) {}

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
        unreachable!("no head is produced")
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

impl Drop for HeldExchange {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// `HeldApplication` as a `UniplexSession` handler.
struct HeldStreamHandler(Rc<Cell<usize>>);

struct AbandonProbe(Rc<Cell<usize>>);

impl Drop for AbandonProbe {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

impl HttpHandler for HeldStreamHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let probe = AbandonProbe(Rc::clone(&self.0));
        Box::pin(async move {
            let _held = (probe, request);
            std::future::pending::<Result<HttpSourceHolder, HttpError>>().await
        })
    }
}

/// One frame of an EOF-abort script.
fn eof_frame(frame_type: FrameType, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut frame = u32::try_from(payload.len()).unwrap().to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[frame_type.as_u8(), flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// The client preface and empty SETTINGS, then `frames`.
fn eof_script(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut wire = CONNECTION_PREFACE.to_vec();
    wire.extend(eof_frame(FrameType::SETTINGS, 0, 0, &[]));
    wire.extend(frames.concat());
    wire
}

/// A request head on `stream`: a GET that ends the stream, or a POST whose body
/// follows. Without `end_headers` the header block is left unfinished.
fn eof_request(stream: u32, get: bool, end_headers: bool) -> Vec<u8> {
    const END_HEADERS: u8 = 0x4;
    let block = HpackEncoder::new(false).encode(&[
        HpackHeader::new(":method", if get { "GET" } else { "POST" }),
        HpackHeader::new(":scheme", "http"),
        HpackHeader::new(":path", "/generate"),
        HpackHeader::new(":authority", "example.com"),
    ]);
    let flags = if get { END_STREAM } else { 0 } | if end_headers { END_HEADERS } else { 0 };
    eof_frame(FrameType::HEADERS, flags, stream, &block)
}

fn eof_reset(stream: u32) -> Vec<u8> {
    eof_frame(
        FrameType::RST_STREAM,
        0,
        stream,
        &H2ErrorCode::CANCEL.as_u32().to_be_bytes(),
    )
}

/// What one held-request EOF run produced: the exchanges or handlers dropped while
/// their responses were owed; each response head's status, by stream; the RST_STREAM
/// and GOAWAY frames written; and the session's error.
#[derive(Debug, PartialEq)]
struct EofRun {
    abandoned: usize,
    statuses: Vec<(StreamId, u16)>,
    resets: Vec<(StreamId, H2ErrorCode)>,
    goaways: Vec<(StreamId, H2ErrorCode)>,
    error: Option<HttpErrorCode>,
}

/// Serves `input`, read with EOF, with the owned driver (`owned`) or `UniplexSession`,
/// both configured as `options` with `abort_streams_on_eof`, then `configure`, and
/// applications that hold every response.
fn eof_run(owned: bool, input: &[u8], configure: fn(&mut OwnedH2Options)) -> EofRun {
    let input = input.to_vec();
    run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let mut config = options(context, transport);
            config.http2.abort_streams_on_eof = true;
            configure(&mut config);
            let abandoned = Rc::new(Cell::new(0));
            let result = if owned {
                let (session, _) =
                    OwnedH2Session::new(HeldApplication(Rc::clone(&abandoned)), config).unwrap();
                tokio::time::timeout(Duration::from_secs(5), session.run()).await
            } else {
                let OwnedH2Options {
                    context,
                    transport,
                    io,
                    http2,
                } = config;
                let handler: Rc<dyn HttpHandler> =
                    Rc::new(HeldStreamHandler(Rc::clone(&abandoned)));
                let (session, _) = UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    io,
                    http2,
                )
                .unwrap();
                tokio::time::timeout(Duration::from_secs(5), session.run()).await
            };
            let error = result
                .expect("the run ends")
                .err()
                .map(|error| error.code());
            let decoded = decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
            EofRun {
                abandoned: abandoned.get(),
                statuses: decoded.header_statuses,
                resets: decoded.resets,
                goaways: decoded.goaways,
                error,
            }
        })
    })
}

/// Under `abort_streams_on_eof` (`DIV-055`), the owned driver judges an EOF read with
/// the bytes before it as `UniplexSession` does, each case read in one segment: held
/// requests are aborted with one GOAWAY(INTERNAL_ERROR) and no response, a request
/// whose head or body is incomplete is open, a stream the peer reset is not open even
/// before the owned driver applies the reset, and a flood breach parsed with the EOF
/// keeps its own error.
#[test]
fn http2_eof_abort_matches_uniplex() {
    fn unlimited(_config: &mut OwnedH2Options) {}
    fn one_reset(config: &mut OwnedH2Options) {
        config.http2.rate_limits.resets = RateLimitConfig::new(1, Duration::from_secs(10), 0);
    }
    let aborted = |abandoned, last| EofRun {
        abandoned,
        statuses: Vec::new(),
        resets: Vec::new(),
        goaways: vec![(StreamId::new(last), H2ErrorCode::INTERNAL_ERROR)],
        error: Some(HttpErrorCode::TRANSPORT_EOF),
    };
    let cases: [(&str, Vec<u8>, fn(&mut OwnedH2Options), EofRun); 6] = [
        (
            "two held GETs",
            eof_script(&[eof_request(1, true, true), eof_request(3, true, true)]),
            unlimited,
            aborted(2, 3),
        ),
        (
            "a POST whose body is incomplete",
            eof_script(&[
                eof_request(1, false, true),
                eof_frame(FrameType::DATA, 0, 1, b"abc"),
            ]),
            unlimited,
            aborted(1, 1),
        ),
        (
            "a HEADERS block without END_HEADERS",
            eof_script(&[eof_request(1, true, false)]),
            unlimited,
            aborted(0, 1),
        ),
        (
            "a held GET the peer reset",
            eof_script(&[eof_request(1, true, true), eof_reset(1)]),
            unlimited,
            EofRun {
                abandoned: 1,
                statuses: Vec::new(),
                resets: Vec::new(),
                goaways: Vec::new(),
                error: None,
            },
        ),
        (
            "a held GET beside one the peer reset",
            eof_script(&[
                eof_request(1, true, true),
                eof_request(3, true, true),
                eof_reset(3),
            ]),
            unlimited,
            aborted(2, 3),
        ),
        (
            "a reset flood beside a held GET",
            eof_script(&[
                eof_request(1, true, true),
                eof_request(3, true, true),
                eof_request(5, true, true),
                eof_reset(3),
                eof_reset(5),
            ]),
            one_reset,
            EofRun {
                abandoned: 3,
                statuses: Vec::new(),
                resets: Vec::new(),
                goaways: vec![(StreamId::new(5), H2ErrorCode::CANCEL)],
                error: Some(HttpErrorCode::CANCEL),
            },
        ),
    ];
    for (label, input, configure, expected) in cases {
        let owned = eof_run(true, &input, configure);
        assert_eq!(owned, expected, "{label}");
        assert_eq!(eof_run(false, &input, configure), owned, "{label}");
    }
}

/// `UniplexSession` under `abort_streams_on_eof` (`DIV-055`): a REFUSED_STREAM that finds no
/// room in the egress buffer behind a held write records its error without failing the
/// connection, and the peer's EOF still aborts the held stream.
#[test]
fn uniplex_eof_aborts_a_held_stream_after_a_refusal_found_no_room() {
    run_on_worker(|context| {
        Box::pin(async move {
            let eof = Arc::new(tokio::sync::Notify::new());
            let write_gate = Arc::new(tokio::sync::Notify::new());
            // Behind the held first write, the SETTINGS ACK (9 bytes) and 14 PING replies (17
            // bytes each) leave 9 of the 256 bytes, too few for the 13-byte RST_STREAM.
            let pings = eof_frame(FrameType::PING, 0, 0, &[0; 8]).repeat(14);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(eof_script(&[eof_request(1, true, true)])),
                FixtureRead::Data(pings),
                FixtureRead::Data(eof_request(3, true, true)),
                FixtureRead::WaitDataEof(Arc::clone(&eof), Vec::new()),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&write_gate));
            let OwnedH2Options {
                context,
                transport,
                mut io,
                mut http2,
            } = options(context, transport);
            io.buffer_limit = 256;
            io.ingress_buffer_limit = 256;
            io.maximum_read_size = 256;
            io.read_allocation_size = 256;
            http2.settings.push(CodecSetting {
                identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                value: 1,
            });
            http2.abort_streams_on_eof = true;
            let abandoned = Rc::new(Cell::new(0));
            let handler: Rc<dyn HttpHandler> = Rc::new(HeldStreamHandler(Rc::clone(&abandoned)));
            let (session, _) = UniplexSession::new_http2(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                io,
                http2,
            )
            .unwrap();
            let script = async {
                wait_until(|| {
                    state.borrow().reads.is_empty() && state.borrow().write_gate_pending.is_some()
                })
                .await;
                assert_eq!(abandoned.get(), 0, "stream 1 is held until the EOF");
                eof.notify_one();
                for _ in 0..256 {
                    tokio::task::yield_now().await;
                }
                write_gate.notify_one();
            };
            let (run, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(session.run(), script)
            })
            .await
            .expect("the EOF ends the run");
            let decoded = decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
            assert_eq!(decoded.ping_replies.len(), 14);
            assert!(
                decoded.resets.is_empty(),
                "stream 3's REFUSED_STREAM found no room"
            );
            assert_eq!(abandoned.get(), 1, "the EOF aborts held stream 1");
            assert_eq!(state.borrow().close, FixtureClose::Reset);
            assert_eq!(
                run.err().map(|error| error.code()),
                Some(HttpErrorCode::PROTOCOL_ERROR),
                "the session keeps the error the refusal recorded"
            );
        })
    });
}

#[test]
fn unsupported_config_is_rejected_before_transport_or_application_work() {
    run_on_worker(|context| {
        Box::pin(async move {
            for case in 0..6 {
                let (transport, state) = FixtureTransport::new([]);
                let mut config = options(context.clone(), transport);
                match case {
                    0 => {
                        config.http2.ping_probes = Some(crate::experimental::PingProbeConfig {
                            interval: Duration::from_secs(1),
                            ..crate::experimental::PingProbeConfig::default()
                        })
                    }
                    1 => config.http2.stats = Some(Rc::new(StatsProbe::default())),
                    2 => config.http2.stream_receive_window = 0,
                    3 => state.borrow_mut().capability = ByteEventCapability::TransportEvents,
                    4 => config.io.deliver_ingress_before_abort = true,
                    5 => config.io.verbatim_http2_header_values = true,
                    _ => unreachable!(),
                }
                let (application, probes) = Application::new(vec![]);
                let result = OwnedH2Session::new(application, config);
                let error = match result {
                    Err(error) => error,
                    Ok(_) => panic!("unsupported configuration {case} was silently accepted"),
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                assert!(probes.borrow().is_empty());
                assert!(state.borrow().writes.is_empty());
            }
        })
    });
}

#[test]
fn unsupported_push_retires_its_source_and_leaves_the_sibling_usable() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) =
                FixtureTransport::new([FixtureRead::DataEof(peer_two_requests_bytes())]);
            let (application, probes) = Application::new(vec![Reply::Push, Reply::Body(1)]);
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .unwrap();
            let probes = probes.borrow();
            assert_eq!(probes.len(), 2);
            assert_eq!(probes[0].push_stops.get(), 1);
            assert_eq!(
                probes[0].cancelled.borrow().as_slice(),
                &[(HttpErrorCode::PROTOCOL_ERROR, false)]
            );
            assert_eq!(probes[0].completed.get(), 0);
            assert_eq!(probes[0].dropped.get(), 1);
            assert_eq!(probes[1].completed.get(), 1);
            assert_eq!(probes[1].dropped.get(), 1);
            assert!(probes[1].cancelled.borrow().is_empty());
            let decoded = decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
            assert!(decoded.pushes.is_empty());
            assert_eq!(
                decoded.resets,
                [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
            );
            assert_eq!(decoded.body_streams, [StreamId::new(3)]);
            assert_eq!(decoded.body_data, b"r");
            assert!(decoded.errors.is_empty());
            assert!(
                decoded
                    .goaways
                    .iter()
                    .all(|(_, code)| *code == H2ErrorCode::NO_ERROR)
            );
        })
    });
}

#[test]
fn header_stats_shutdown_skips_encoded_callback_without_byte_events() {
    use roxygen_core::http::HttpHeaderSize;
    use roxygen_http2::hpack::HeaderCodecStats;
    use roxygen_runtime::worker::WorkerHandle;

    struct ShutdownOnEncode {
        worker: WorkerHandle,
        encodes: Cell<usize>,
    }

    impl HeaderCodecStats for ShutdownOnEncode {
        fn record_encode(&self, _size: HttpHeaderSize) {
            self.encodes.set(self.encodes.get() + 1);
            self.worker.begin_shutdown();
        }

        fn record_decode(&self, _size: HttpHeaderSize) {}
        fn record_decode_error(&self) {}
        fn record_decode_too_large(&self) {}
    }

    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (transport, state) =
                FixtureTransport::new([FixtureRead::SegmentedData(queue(&input))]);
            let cancel = Rc::new(ReentrantCancel {
                at: CancelAt::HeaderEncoded,
                handle: RefCell::new(None),
                transport: state,
                writes_at_cancel: Cell::new(None),
            });
            let stats = Rc::new(ShutdownOnEncode {
                worker: context.handle().clone(),
                encodes: Cell::new(0),
            });
            let mut config = options(context, transport);
            config.http2.header_codec_stats = Some(stats.clone());
            let (mut application, probes) = Application::new(vec![Reply::Body(1)]);
            application.reentrant_cancel = Some(Rc::clone(&cancel));
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            *cancel.handle.borrow_mut() = Some(handle);
            let error = tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(stats.encodes.get(), 1);
            assert_eq!(cancel.writes_at_cancel.get(), None);
            let probes = probes.borrow();
            assert_eq!(probes.len(), 1);
            assert_eq!(probes[0].body_polls.get(), 0);
            assert_eq!(probes[0].completed.get(), 0);
            assert_eq!(probes[0].dropped.get(), 1);
        })
    });
}

#[test]
fn reentrant_cancellation_stops_writes_before_returning_from_the_driver_turn() {
    for at in [
        CancelAt::HeaderPoll,
        CancelAt::BodyPoll,
        CancelAt::HeaderEncoded,
        CancelAt::ResumePoll,
        CancelAt::ResumeDrop,
    ] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
                let cancel = Rc::new(ReentrantCancel {
                    at,
                    handle: RefCell::new(None),
                    transport: Rc::clone(&state),
                    writes_at_cancel: Cell::new(None),
                });
                let (mut application, probes) = Application::new(vec![Reply::Body(1)]);
                application.reentrant_cancel = Some(Rc::clone(&cancel));
                let (session, handle) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                *cancel.handle.borrow_mut() = Some(handle);
                let error = tokio::time::timeout(Duration::from_secs(5), session.run())
                    .await
                    .unwrap()
                    .unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(
                    Some(state.borrow().writes.len()),
                    cancel.writes_at_cancel.get()
                );
                let probes = probes.borrow();
                assert_eq!(probes.len(), 1);
                assert_eq!(
                    probes[0].body_polls.get(),
                    usize::from(matches!(
                        at,
                        CancelAt::BodyPoll | CancelAt::ResumePoll | CancelAt::ResumeDrop,
                    )),
                    "a completed or dropped resume must not permit another body poll after cancellation"
                );
                assert_eq!(
                    probes[0].cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CORO_CANCELLED, false)]
                );
                assert_eq!(probes[0].response_complete.get(), 0);
                assert_eq!(probes[0].completed.get(), 0);
                assert_eq!(probes[0].dropped.get(), 1);
            })
        });
    }
}

#[test]
fn application_callback_panics_cancel_once_and_join_native_io_before_resuming() {
    for (at, expected_panic) in [
        (CallbackPanicAt::HeaderPoll, "application header poll"),
        (
            CallbackPanicAt::HeaderEncoded,
            "application encoded headers",
        ),
    ] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let release_callback = Arc::new(tokio::sync::Notify::new());
                let release_write = Arc::new(tokio::sync::Notify::new());
                let release_read = Arc::new(tokio::sync::Notify::new());
                let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
                let (transport, state) = FixtureTransport::new([
                    FixtureRead::SegmentedData(queue(&input)),
                    FixtureRead::WaitDataEof(Arc::clone(&release_read), Vec::new()),
                ]);
                state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                let (mut application, probes) = Application::new(vec![Reply::Body(1)]);
                application.callback_panic = Some((at, Arc::clone(&release_callback)));
                let (session, handle) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                let resumed = Cell::new(false);
                let run = async {
                    let result = eligibility_catch_unwind(session.run()).await;
                    resumed.set(true);
                    result
                };
                let scenario = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(5),
                        async {
                            complete_credit_wait(|| {
                                let state = state.borrow();
                                state.write_gate_pending.is_some_and(|length| length > 0)
                                    && state.reads.is_empty()
                                    && probes.borrow().len() == 1
                            })
                            .await;
                            let probe = Rc::clone(&probes.borrow()[0]);
                            assert!(state.borrow().writes.is_empty());
                            release_callback.notify_one();
                            complete_credit_wait(|| !probe.cancelled.borrow().is_empty()).await;
                            assert_eq!(
                                probe.cancelled.borrow().as_slice(),
                                &[(HttpErrorCode::INTERNAL_ERROR, false)]
                            );
                            assert!(!resumed.get(), "held I/O retains the original panic");
                            assert!(state.borrow().writes.is_empty());
                            release_write.notify_one();
                            complete_credit_wait(|| state.borrow().writes.len() == 1).await;
                            assert!(
                                !resumed.get(),
                                "write completion alone cannot abandon the admitted read"
                            );
                        },
                    ))
                    .await;
                    release_callback.notify_one();
                    release_write.notify_one();
                    release_read.notify_one();
                    if !matches!(&outcome, Ok(Ok(()))) {
                        handle.cancel();
                    }
                    outcome
                };
                let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::join!(run, scenario)
                })
                .await
                .expect("callback unwind joins the admitted read and write");
                match outcome {
                    Ok(result) => result.expect("callback panic reaches joined cleanup"),
                    Err(payload) => resume_unwind(payload),
                }
                let panic = result.expect_err("the original application panic is resumed");
                assert_eq!(panic.downcast_ref::<&str>(), Some(&expected_panic));
                let probes = probes.borrow();
                assert_eq!(probes.len(), 1);
                let probe = &probes[0];
                assert_eq!(probe.requested.get(), 1);
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::INTERNAL_ERROR, false)]
                );
                assert_eq!(probe.response_complete.get(), 0);
                assert_eq!(probe.completed.get(), 0);
                assert_eq!(probe.dropped.get(), 1);
                assert!(!handle.initiate_drain().unwrap());
                let state = state.borrow();
                assert_eq!(state.writes.len(), 1);
                assert_eq!(Some(state.writes[0].len()), state.write_gate_pending);
                assert!(state.transport_dropped);
                assert_eq!(state.close, FixtureClose::Reset);
            })
        });
    }
}

#[test]
fn batched_owner_panic_preserves_unvisited_wakes_and_joins_native_io() {
    run_on_worker(|context| {
        Box::pin(async move {
            let release_write = Arc::new(tokio::sync::Notify::new());
            let release_read = Arc::new(tokio::sync::Notify::new());
            let gate = Rc::new(OwnerPollGate {
                panic_first: Cell::new(true),
                ..OwnerPollGate::default()
            });
            let (_, input) = ready_owner_requests(17);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::WaitDataEof(Arc::clone(&release_read), Vec::new()),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let (mut application, probes) = Application::new(vec![Reply::Body(1); 17]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let resumed = Cell::new(false);
            let run = async {
                let result = eligibility_catch_unwind(session.run()).await;
                resumed.set(true);
                result
            };
            let scenario = async {
                let outcome =
                    eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                        complete_credit_wait(|| {
                            let state = state.borrow();
                            let wakers = gate.wakers.borrow();
                            state.write_gate_pending.is_some_and(|length| length > 0)
                                && state.reads.is_empty()
                                && wakers.len() == 17
                                && wakers.iter().all(Option::is_some)
                        })
                        .await;
                        assert!(state.borrow().writes.is_empty());
                        gate.release();
                        complete_credit_wait(|| {
                            probes
                                .borrow()
                                .iter()
                                .all(|probe| !probe.cancelled.borrow().is_empty())
                        })
                        .await;
                        for probe in probes.borrow().iter() {
                            assert_eq!(
                                probe.cancelled.borrow().as_slice(),
                                &[(HttpErrorCode::INTERNAL_ERROR, false)]
                            );
                        }
                        assert!(!resumed.get(), "held I/O retains the original panic");
                        assert!(state.borrow().writes.is_empty());
                        release_write.notify_one();
                        complete_credit_wait(|| state.borrow().writes.len() == 1).await;
                        assert!(
                            !resumed.get(),
                            "write completion alone cannot abandon the admitted read"
                        );
                    }))
                    .await;
                release_write.notify_one();
                release_read.notify_one();
                if !matches!(&outcome, Ok(Ok(()))) {
                    handle.cancel();
                }
                outcome
            };
            let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(run, scenario)
            })
            .await
            .expect("every queued owner and both native operations retire after panic");
            match outcome {
                Ok(result) => result.expect("ready owner panic reaches joined cleanup"),
                Err(payload) => resume_unwind(payload),
            }
            let panic = result.expect_err("the original owner panic is resumed");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"first ready owner"));
            let probes = probes.borrow();
            assert_eq!(probes.len(), 17);
            for probe in probes.iter() {
                assert_eq!(probe.requested.get(), 1);
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::INTERNAL_ERROR, false)]
                );
                assert_eq!(probe.body_polls.get(), 0);
                assert_eq!(probe.response_complete.get(), 0);
                assert_eq!(probe.completed.get(), 0);
                assert_eq!(probe.dropped.get(), 1);
            }
            let state = state.borrow();
            assert_eq!(state.writes.len(), 1);
            assert_eq!(Some(state.writes[0].len()), state.write_gate_pending);
            assert!(state.transport_dropped);
            assert_eq!(state.close, FixtureClose::Reset);
        })
    });
}

#[test]
fn zero_data_request_preserves_trailers_instead_of_becoming_header_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut preface = BufQueue::new(4096);
            peer.generate(
                &mut preface,
                EgressFrame::ConnectionPreface,
                &mut IgnoreCodecEvents,
            )
            .unwrap();
            peer.generate_configured_settings(&mut preface, &mut IgnoreCodecEvents)
                .unwrap();
            let mut input = preface.coalesce(preface.chain_length()).unwrap().to_vec();
            let stream = peer.create_stream();
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Post).unwrap();
            request.set_url(b"/trailers", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
            ));
            let mut trailers = HttpHeaders::new();
            trailers.set(b"x-check", b"preserved");
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Trailers {
                    stream,
                    trailers: &trailers,
                },
            ));
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (application, probes) = Application::new(vec![Reply::Body(0)]);
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .unwrap();
            let probes = probes.borrow();
            assert_eq!(probes.len(), 1);
            assert!(!probes[0].request_eom.get());
            assert_eq!(probes[0].request_bytes.get(), 0);
            assert_eq!(
                probes[0].request_trailer.borrow().as_deref(),
                Some(b"preserved".as_slice())
            );
            assert_eq!(probes[0].completed.get(), 1);
            assert_eq!(probes[0].dropped.get(), 1);
            assert!(probes[0].cancelled.borrow().is_empty());
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert_eq!(decoded.header_statuses, [(stream, 200)]);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

/// The client preface and a POST's HEADERS without END_STREAM, then its trailers HEADERS frame
/// (END_STREAM).
fn trailers_only_request() -> (StreamId, Vec<u8>, Vec<u8>) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut preface = BufQueue::new(4096);
    peer.generate(
        &mut preface,
        EgressFrame::ConnectionPreface,
        &mut IgnoreCodecEvents,
    )
    .unwrap();
    peer.generate_configured_settings(&mut preface, &mut IgnoreCodecEvents)
        .unwrap();
    let mut head = preface.coalesce(preface.chain_length()).unwrap().to_vec();
    let stream = peer.create_stream();
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Post).unwrap();
    request.set_url(b"/trailers", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    head.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    ));
    let mut trailers = HttpHeaders::new();
    trailers.set(b"x-check", b"preserved");
    let trailers = eligibility_frame(
        &mut peer,
        EgressFrame::Trailers {
            stream,
            trailers: &trailers,
        },
    );
    (stream, head, trailers)
}

#[test]
fn a_dropped_request_body_with_trailers_retires() {
    // `split`: the trailers arrive in a later read, after the exchange has dropped the body.
    for split in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (stream, head, trailers) = trailers_only_request();
                let release_trailers = Arc::new(tokio::sync::Notify::new());
                let reads = if split {
                    vec![
                        FixtureRead::Data(head),
                        FixtureRead::WaitDataEof(Arc::clone(&release_trailers), trailers),
                    ]
                } else {
                    let mut input = head;
                    input.extend(trailers);
                    vec![FixtureRead::DataEof(input)]
                };
                let (transport, state) = FixtureTransport::new(reads);
                let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
                application.drop_request_body = true;
                let (session, handle) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                let probe_reached = |condition: fn(&Probe) -> bool| {
                    let probes = Rc::clone(&probes);
                    move || {
                        probes
                            .borrow()
                            .first()
                            .is_some_and(|probe| condition(probe))
                    }
                };
                let scenario = async {
                    if split {
                        let requested = drive_until(
                            DRIVE_LIMIT,
                            probe_reached(|probe| probe.requested.get() == 1),
                        )
                        .await;
                        assert!(
                            requested,
                            "split={split}: the request head reaches the exchange"
                        );
                        assert_eq!(
                            probes.borrow()[0].completed.get(),
                            0,
                            "split={split}: the request stays open until its trailers arrive"
                        );
                        release_trailers.notify_one();
                    }
                    let completed = drive_until(
                        DRIVE_LIMIT,
                        probe_reached(|probe| probe.completed.get() == 1),
                    )
                    .await;
                    if !completed {
                        handle.cancel();
                    }
                    completed
                };
                let (result, completed) = tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::join!(session.run(), scenario)
                })
                .await
                .unwrap_or_else(|_| panic!("split={split}: run returns after EOF"));
                assert!(
                    completed,
                    "split={split}: a request whose body was dropped retires on its trailers"
                );
                result.unwrap();
                let probes = probes.borrow();
                assert_eq!(probes.len(), 1);
                assert!(!probes[0].request_eom.get());
                assert_eq!(probes[0].request_trailer.borrow().as_deref(), None);
                assert_eq!(probes[0].response_complete.get(), 1);
                assert_eq!(probes[0].completed.get(), 1);
                assert_eq!(probes[0].dropped.get(), 1);
                assert!(probes[0].cancelled.borrow().is_empty());
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.header_statuses, [(stream, 200)]);
                assert!(decoded.resets.is_empty());
                assert!(decoded.errors.is_empty());
            })
        });
    }
}

#[test]
fn two_stream_local_header_failures_preserve_later_valid_stream_progress() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut preface = BufQueue::new(4096);
            peer.generate(
                &mut preface,
                EgressFrame::ConnectionPreface,
                &mut IgnoreCodecEvents,
            )
            .unwrap();
            peer.generate_configured_settings(&mut preface, &mut IgnoreCodecEvents)
                .unwrap();
            let mut input = preface.coalesce(preface.chain_length()).unwrap().to_vec();
            for method in [HttpMethod::Trace, HttpMethod::Trace, HttpMethod::Get] {
                let stream = peer.create_stream();
                let mut request = HttpMessage::new();
                request.set_method(method).unwrap();
                request.set_url(b"/same-read", false).unwrap();
                request.set_http_version(1, 1);
                request
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
                if method == HttpMethod::Trace {
                    request
                        .headers_mut()
                        .set(HttpHeaderCode::ContentLength, b"1");
                }
                input.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                ));
            }
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (application, probes) = Application::new(vec![Reply::Body(0); 3]);
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .unwrap();
            let mut decoded =
                decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
            decoded.header_statuses.sort_by_key(|(stream, _)| *stream);
            assert_eq!(
                decoded.header_statuses,
                [
                    (StreamId::new(1), 400),
                    (StreamId::new(3), 400),
                    (StreamId::new(5), 200)
                ]
            );
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
            let probes = probes.borrow();
            assert_eq!(probes.len(), 3);
            for rejected in &probes[..2] {
                assert_eq!(rejected.requested.get(), 0);
                assert_eq!(
                    rejected.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::HEADER_PARSE_ERROR, false)]
                );
                let ingress = rejected
                    .ingress
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .snapshot()
                    .unwrap();
                assert_eq!(
                    ingress.unwrap_err().code(),
                    HttpErrorCode::HEADER_PARSE_ERROR
                );
                assert_eq!(rejected.completed.get(), 0);
                assert_eq!(rejected.dropped.get(), 1);
            }
            assert_eq!(probes[2].requested.get(), 1);
            assert_eq!(probes[2].completed.get(), 1);
            assert_eq!(probes[2].dropped.get(), 1);
            assert!(probes[2].cancelled.borrow().is_empty());
        })
    });
}

#[test]
fn late_peer_reset_overrides_local_cause_without_repeating_source_retirement() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let release_write = Arc::new(tokio::sync::Notify::new());
            let release_reset = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let reset = eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::CANCEL,
                },
            );
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&release_reset), reset),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let (application, probes) = Application::new(vec![Reply::Push]);
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(
                session,
                &handle,
                &[Arc::clone(&release_write), Arc::clone(&release_reset)],
                async {
                    complete_credit_wait(|| {
                        state.borrow().write_gate_pending.is_some()
                            && probes
                                .borrow()
                                .first()
                                .is_some_and(|probe| probe.cancelled.borrow().len() == 1)
                    })
                    .await;
                    let probe = Rc::clone(&probes.borrow()[0]);
                    assert_eq!(
                        probe.cancelled.borrow().as_slice(),
                        &[(HttpErrorCode::PROTOCOL_ERROR, false)]
                    );
                    assert_eq!(probe.push_stops.get(), 1);
                    assert_eq!(probe.dropped.get(), 0);
                    release_reset.notify_one();
                    complete_credit_wait(|| probe.cancelled.borrow().len() == 2).await;
                    assert_eq!(
                        probe.cancelled.borrow().as_slice(),
                        &[
                            (HttpErrorCode::PROTOCOL_ERROR, false),
                            (HttpErrorCode::CANCEL, true),
                        ]
                    );
                    assert_eq!(probe.push_stops.get(), 1);
                    assert_eq!(probe.dropped.get(), 0);
                    release_write.notify_one();
                    complete_credit_wait(|| probe.dropped.get() == 1).await;
                    assert_eq!(probe.push_stops.get(), 1);
                    assert_eq!(probe.completed.get(), 0);
                    assert_eq!(
                        decode_server_frames(&state.borrow().writes.concat()).resets,
                        [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)]
                    );
                },
            )
            .await;
        })
    });
}

async fn run_single_read_to_eof(
    context: WorkerContext,
    input: Vec<u8>,
) -> (Rc<Probe>, Rc<OwnerPollGate>, DecodedServerFrames) {
    let gate = Rc::new(OwnerPollGate {
        ready: Cell::new(true),
        ..OwnerPollGate::default()
    });
    let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
    let (mut application, probes) = Application::new(vec![Reply::Body(4)]);
    application.owner_poll_gate = Some(Rc::clone(&gate));
    let (session, _) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), session.run())
        .await
        .expect("a single scripted read runs to EOF")
        .unwrap();
    let probe = {
        let probes = probes.borrow();
        assert_eq!(probes.len(), 1, "the HEADERS frame creates one exchange");
        Rc::clone(&probes[0])
    };
    let decoded = decode_server_frames(&state.borrow().writes.concat());
    (probe, gate, decoded)
}

#[test]
fn same_read_peer_reset_retires_the_exchange_before_its_start() {
    run_on_worker(|context| {
        Box::pin(async move {
            for (method, body) in [
                (HttpMethod::Get, b"".as_slice()),
                (HttpMethod::Post, b"body"),
            ] {
                let input =
                    peer_request_bytes(method, body, None, PeerControl::Reset(H2ErrorCode::CANCEL));
                let (probe, gate, decoded) = run_single_read_to_eof(context.clone(), input).await;

                // The whole read is parsed before any owner is visited, so the
                // reset is recorded before the deferred start can run.
                assert_eq!(probe.requested.get(), 0, "the exchange is never started");
                assert_eq!(gate.polls.get(), 0, "response headers are never polled");
                assert_eq!(probe.request_bytes.get(), 0);
                assert_eq!(probe.body_polls.get(), 0);
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CANCEL, true)]
                );
                assert_eq!(probe.response_complete.get(), 0);
                assert_eq!(probe.completed.get(), 0);
                assert_eq!(probe.dropped.get(), 1);
                let completion = probe.ingress.borrow().as_ref().unwrap().clone();
                assert!(completion.snapshot().unwrap().is_ok());
                assert_eq!(completion.body_bytes_received(), body.len() as u64);

                assert!(decoded.header_streams.is_empty());
                assert!(decoded.body_streams.is_empty());
                assert!(decoded.resets.is_empty(), "a peer reset is not echoed");
                assert!(decoded.errors.is_empty());
            }
        })
    });
}

#[test]
fn split_read_peer_reset_cancels_the_started_exchange() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let release_reset = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let reset = eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::CANCEL,
                },
            );
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&release_reset), reset),
            ]);
            let gate = Rc::new(OwnerPollGate::default());
            let (mut application, probes) = Application::new(vec![Reply::Body(4)]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(session, &handle, &[Arc::clone(&release_reset)], async {
                complete_credit_wait(|| gate.polls.get() > 0).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                assert_eq!(probe.requested.get(), 1, "the exchange started");
                assert!(probe.cancelled.borrow().is_empty());

                release_reset.notify_one();
                complete_credit_wait(|| probe.dropped.get() == 1).await;
                assert_eq!(probe.requested.get(), 1);
                assert_eq!(gate.polls.get(), 1);
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CANCEL, true)]
                );
                assert_eq!(probe.response_complete.get(), 0);
                assert_eq!(probe.completed.get(), 0);
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert!(decoded.header_streams.is_empty());
                assert!(decoded.body_streams.is_empty());
                assert!(decoded.resets.is_empty(), "a peer reset is not echoed");
                assert!(decoded.errors.is_empty());
            })
            .await;
        })
    });
}

/// A client's RST_STREAM(NO_ERROR) after its whole request reaches the exchange as
/// `CANCEL`. Only upstream streams keep `NO_ERROR` after a complete response (`DIV-050`).
#[test]
fn client_no_error_reset_after_its_whole_request_cancels_the_exchange() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let release_reset = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let reset = eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::NO_ERROR,
                },
            );
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&release_reset), reset),
            ]);
            let gate = Rc::new(OwnerPollGate::default());
            let (mut application, probes) = Application::new(vec![Reply::Body(4)]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(session, &handle, &[Arc::clone(&release_reset)], async {
                complete_credit_wait(|| gate.polls.get() > 0).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                assert_eq!(probe.requested.get(), 1, "the exchange started");
                assert!(probe.request_eom.get(), "the whole request arrived");

                release_reset.notify_one();
                complete_credit_wait(|| probe.dropped.get() == 1).await;
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CANCEL, true)]
                );
                assert_eq!(probe.completed.get(), 0);
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert!(decoded.header_streams.is_empty());
                assert!(decoded.resets.is_empty(), "a peer reset is not echoed");
                assert!(decoded.errors.is_empty());
            })
            .await;
        })
    });
}

/// When the origin's RST_STREAM(NO_ERROR) reaches the forwarding case's upstream session.
#[derive(Clone, Copy, Eq, PartialEq)]
enum OriginReset {
    /// In the read that carries the whole response.
    WithTheResponse,
    /// In a later read, once the client already has the whole forwarded response.
    AfterTheForwardedResponse,
}

#[derive(Default)]
struct ForwardProbe {
    cancelled: RefCell<Vec<(HttpErrorCode, bool)>>,
    response_complete: Cell<usize>,
    completed: Cell<usize>,
}

/// Forwards each request to `upstream`, with the request's ingress body as the upload, and
/// answers with the upstream response.
struct ForwardApplication {
    upstream: OwnedH2UpstreamHandle,
    probe: Rc<ForwardProbe>,
}

/// The upstream response source, and the request control kept for its lifetime.
type Forwarded = (HttpSourceHolder, OwnedH2UpstreamRequest);

struct ForwardExchange {
    upstream: OwnedH2UpstreamHandle,
    submission: Option<OwnedH2UpstreamSubmission>,
    response: Option<Forwarded>,
    head: Option<LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>>,
    body: Option<LocalSourceFuture<Result<HttpBodyEvent, HttpError>>>,
    probe: Rc<ForwardProbe>,
}

impl OwnedH2Application for ForwardApplication {
    type Exchange = ForwardExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> ForwardExchange {
        ForwardExchange {
            upstream: self.upstream.clone(),
            submission: None,
            response: None,
            head: None,
            body: None,
            probe: Rc::clone(&self.probe),
        }
    }
}

impl OwnedH2Exchange for ForwardExchange {
    fn request(&mut self, _head: Box<HttpMessage>, _body: HttpStreamSourceOwner, _eom: bool) {
        panic!("the owned driver supplies the ingress body")
    }

    fn request_body(&mut self, mut head: Box<HttpMessage>, body: HttpIngressBody, eom: bool) {
        assert_eq!(head.method().unwrap(), Some(HttpMethod::Post));
        assert!(!eom, "the upload is still open");
        let submission = self.upstream.reserve_request().unwrap().submit(
            committed_test_headers(b"/forward", false),
            body,
            CancellationToken::never(),
        );
        self.submission = Some(submission);
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if let Some(submission) = &mut self.submission {
            let response = std::task::ready!(Pin::new(submission).poll(context))?;
            self.submission = None;
            let (_, source, control) = response.into_source_parts();
            self.response = Some((source, control));
        }
        let (source, _) = self.response.as_mut().expect("the upstream response");
        let head = self.head.get_or_insert_with(|| source.read_header_event());
        let mut head = std::task::ready!(head.as_mut().poll(context))?;
        self.head = None;
        let mut response = HttpMessage::new();
        response
            .set_status_code(head.headers_mut().status_code().unwrap())
            .unwrap();
        response.set_http_version(1, 1);
        Poll::Ready(Ok(HttpHeaderEvent::new(response, head.eom()).unwrap()))
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let (source, _) = self.response.as_mut().expect("the upstream response");
        let body = self
            .body
            .get_or_insert_with(|| source.read_body_event(maximum));
        let event = std::task::ready!(body.as_mut().poll(context));
        self.body = None;
        Poll::Ready(event)
    }

    fn cancel(&mut self, error: &HttpError, peer_reset: bool) {
        self.probe
            .cancelled
            .borrow_mut()
            .push((error.code(), peer_reset));
        self.head = None;
        self.body = None;
        self.response = None;
        self.submission = None;
    }

    fn response_complete(&mut self) {
        self.probe
            .response_complete
            .set(self.probe.response_complete.get() + 1);
    }

    fn complete(self) {
        self.probe.completed.set(self.probe.completed.get() + 1);
    }
}

/// The origin's `200` on stream 1 with body `reply` and trailer `grpc-status: 0`, and the
/// RST_STREAM(NO_ERROR) it sends after them.
fn origin_trailered_response_then_no_error_reset() -> (Vec<u8>, Vec<u8>) {
    let mut origin = Http2Codec::new(TransportDirection::Downstream);
    let origin_stream = StreamId::new(1);
    let mut status = HttpMessage::new();
    status.set_status_code(200).unwrap();
    status.set_http_version(1, 1);
    let mut response = eligibility_frame(
        &mut origin,
        EgressFrame::Headers {
            stream: origin_stream,
            message: &status,
            end_stream: false,
            extra_headers: None,
        },
    );
    response.extend(eligibility_frame(
        &mut origin,
        EgressFrame::Body {
            stream: origin_stream,
            body: queue(b"reply"),
            padding: None,
            end_stream: false,
        },
    ));
    let mut trailers = HttpHeaders::new();
    trailers.add("grpc-status", "0");
    response.extend(eligibility_frame(
        &mut origin,
        EgressFrame::Trailers {
            stream: origin_stream,
            trailers: &trailers,
        },
    ));
    let reset = eligibility_frame(
        &mut origin,
        EgressFrame::Reset {
            stream: origin_stream,
            code: H2ErrorCode::NO_ERROR,
        },
    );
    (response, reset)
}

/// The forwarding cases' owned upstream session, on a fixture transport that reads `reads`.
fn forwarding_upstream(
    context: WorkerContext,
    reads: Vec<FixtureRead>,
) -> (
    OwnedH2UpstreamSession,
    OwnedH2UpstreamHandle,
    Rc<RefCell<FixtureTransportState>>,
) {
    let (upstream_transport, upstream_wire) = FixtureTransport::new(reads);
    let mut upstream_config = options(context, upstream_transport);
    upstream_config.io.drain_request_body_on_drop = false;
    upstream_config.http2.settings = vec![CodecSetting {
        identifier: SettingsId::EnablePush.as_u16(),
        value: 0,
    }];
    upstream_config.http2.double_goaway_drain = false;
    let (upstream, upstream_handle) = OwnedH2UpstreamSession::new(upstream_config).unwrap();
    (upstream, upstream_handle, upstream_wire)
}

/// A downstream session forwards a POST whose upload stays open to an upstream session,
/// with the downstream request's ingress body as the upstream upload. The origin sends
/// the whole response with trailers, then RST_STREAM(NO_ERROR). The client gets the
/// response head, body and trailers, then RST_STREAM(NO_ERROR) (`DIV-050`).
#[test]
fn forwarded_response_survives_an_origin_no_error_reset_after_its_trailers() {
    forwarded_origin_no_error_reset_case(OriginReset::WithTheResponse);
}

/// As above, but the origin's reset arrives after the client got the whole forwarded
/// response. The `NO_ERROR` stop reaches the client's stream after its END_STREAM, so,
/// as for any request source stopped with `NO_ERROR` once its response ended, the client
/// gets no reset, RST_STREAM(CANCEL) included, and the rest of its upload is read and
/// discarded on a healthy connection (`DIV-050`).
#[test]
fn forwarded_response_then_a_late_origin_no_error_reset_sends_the_client_no_reset() {
    forwarded_origin_no_error_reset_case(OriginReset::AfterTheForwardedResponse);
}

fn forwarded_origin_no_error_reset_case(origin_reset: OriginReset) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let mut client = Http2Codec::new(TransportDirection::Upstream);
            let mut request = eligibility_frame(&mut client, EgressFrame::ConnectionPreface);
            request.extend(transcript_settings(&mut client));
            let stream = client.create_stream();
            let mut head = HttpMessage::new();
            head.set_method(HttpMethod::Post).unwrap();
            head.set_url(b"/forward", false).unwrap();
            head.set_http_version(1, 1);
            head.headers_mut().set(HttpHeaderCode::Host, b"example.com");
            request.extend(eligibility_frame(
                &mut client,
                EgressFrame::Headers {
                    stream,
                    message: &head,
                    end_stream: false,
                    extra_headers: None,
                },
            ));
            request.extend(eligibility_frame(
                &mut client,
                EgressFrame::Body {
                    stream,
                    body: queue(b"up"),
                    padding: None,
                    end_stream: false,
                },
            ));
            let upload_rest = Arc::new(tokio::sync::Notify::new());
            let mut client_reads = vec![FixtureRead::Data(request)];
            if origin_reset == OriginReset::AfterTheForwardedResponse {
                // The rest of the client's upload, then a PING whose ACK shows that the
                // session has read it.
                let mut rest = eligibility_frame(
                    &mut client,
                    EgressFrame::Body {
                        stream,
                        body: queue(b"rest"),
                        padding: None,
                        end_stream: true,
                    },
                );
                rest.extend(eligibility_frame(
                    &mut client,
                    EgressFrame::PingRequest { data: Some(45) },
                ));
                client_reads.push(FixtureRead::WaitData(Arc::clone(&upload_rest), rest));
            }
            let (downstream_transport, downstream_wire) = FixtureTransport::new(client_reads);

            let (mut response, reset) = origin_trailered_response_then_no_error_reset();
            let respond = Arc::new(tokio::sync::Notify::new());
            let late_reset = Arc::new(tokio::sync::Notify::new());
            let mut reads = vec![FixtureRead::Data(server_settings_bytes(1))];
            match origin_reset {
                OriginReset::WithTheResponse => {
                    response.extend(reset);
                    reads.push(FixtureRead::WaitData(Arc::clone(&respond), response));
                }
                OriginReset::AfterTheForwardedResponse => {
                    reads.push(FixtureRead::WaitData(Arc::clone(&respond), response));
                    reads.push(FixtureRead::WaitData(Arc::clone(&late_reset), reset));
                }
            }
            let (upstream, upstream_handle, upstream_wire) =
                forwarding_upstream(context.clone(), reads);
            let probe = Rc::new(ForwardProbe::default());
            let application = ForwardApplication {
                upstream: upstream_handle.clone(),
                probe: Rc::clone(&probe),
            };
            let (downstream, downstream_handle) =
                OwnedH2Session::new(application, options(context, downstream_transport)).unwrap();

            let operation = async {
                // The origin answers once the upstream request and its first upload bytes
                // are on the wire, while the client's upload is still open.
                wait_for_written(&upstream_wire, |wire| {
                    wire.count(FrameType::HEADERS, 0) == 1 && wire.count(FrameType::DATA, 0) == 1
                })
                .await;
                respond.notify_one();
                let headers = FrameType::HEADERS.as_u8();
                if origin_reset == OriginReset::AfterTheForwardedResponse {
                    wait_for_written(&downstream_wire, |wire| {
                        wire.count(FrameType::HEADERS, END_STREAM) == 1
                    })
                    .await;
                    settle_turns(DRIVE_LIMIT).await;
                    assert_eq!(
                        stream_frame_kinds(&downstream_wire.borrow().writes.concat(), 1),
                        [
                            (headers, END_HEADERS),
                            (FrameType::DATA.as_u8(), 0),
                            (headers, END_HEADERS | END_STREAM),
                        ],
                        "the client has the whole response before the origin's reset"
                    );
                    assert_eq!(probe.completed.get(), 0, "the upload is still open");
                    late_reset.notify_one();
                    assert!(
                        writer_drive_until(|| {
                            probe.completed.get() + probe.cancelled.borrow().len() != 0
                        })
                        .await,
                        "the exchange ends once the origin's reset stops its upload"
                    );
                    assert_eq!(probe.completed.get(), 1, "the exchange completes");
                    upload_rest.notify_one();
                    wait_for_written(&downstream_wire, |wire| {
                        wire.count(FrameType::PING, ACK) == 1
                    })
                    .await;
                } else {
                    wait_for_written(&downstream_wire, |wire| {
                        wire.count(FrameType::RST_STREAM, 0) == 1
                    })
                    .await;
                }
                settle_turns(DRIVE_LIMIT).await;
                let wire = downstream_wire.borrow().writes.concat();
                let decoded = decode_server_frames(&wire);
                let mut client_stream = vec![
                    (headers, END_HEADERS),
                    (FrameType::DATA.as_u8(), 0),
                    (headers, END_HEADERS | END_STREAM),
                ];
                if origin_reset == OriginReset::WithTheResponse {
                    assert_eq!(
                        decoded.resets,
                        [(StreamId::new(1), H2ErrorCode::NO_ERROR)],
                        "the client's stream ends with RST_STREAM(NO_ERROR), not CANCEL"
                    );
                    client_stream.push((FrameType::RST_STREAM.as_u8(), 0));
                } else {
                    assert!(
                        decoded.resets.is_empty(),
                        "a stop after the client's stream ended sends no reset: {:?}",
                        decoded.resets
                    );
                    assert_eq!(
                        decoded.ping_replies,
                        [45],
                        "the rest of the upload was read"
                    );
                    assert!(decoded.goaways.is_empty());
                }
                assert_eq!(
                    stream_frame_kinds(&wire, 1),
                    client_stream,
                    "the response head, body and trailers, then any reset"
                );
                assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                assert_eq!(decoded.body_data, b"reply");
                assert_eq!(
                    trailer_fields(&wire, 1).expect("trailers end the client's stream"),
                    [HpackHeader::new("grpc-status", "0")]
                );
                assert!(decoded.errors.is_empty());
                assert!(probe.cancelled.borrow().is_empty());
                assert_eq!(probe.response_complete.get(), 1);
                let upstream_resets =
                    decode_client_frames(&upstream_wire.borrow().writes.concat()).resets;
                assert!(
                    upstream_resets.is_empty(),
                    "the origin's reset is not echoed"
                );
            };
            let script = async {
                let outcome = eligibility_catch_unwind(tokio::time::timeout(
                    Duration::from_secs(5),
                    operation,
                ))
                .await;
                // The scripted reads wait on these, so a failed check still lets both
                // sessions join.
                for gate in [&respond, &late_reset, &upload_rest] {
                    gate.notify_one();
                }
                downstream_handle.cancel();
                upstream_handle.cancel();
                outcome
            };
            let (downstream_run, upstream_run, outcome) =
                tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::join!(downstream.run(), upstream.run(), script)
                })
                .await
                .expect("both cancelled sessions join");
            match outcome {
                Ok(result) => result.expect("the forwarded exchange made progress"),
                Err(payload) => resume_unwind(payload),
            }
            for run in [downstream_run.map(drop), upstream_run.map(drop)] {
                assert_eq!(
                    run.expect_err("the forwarding fixture ends by cancellation")
                        .code(),
                    HttpErrorCode::CORO_CANCELLED
                );
            }
        })
    });
}

/// As `forwarded_response_survives_an_origin_no_error_reset_after_its_trailers`, but the
/// client is on an owned HTTP/1.1 downstream session. HTTP/1.1 cannot reset one request, so
/// the client gets the whole response with `Connection: close`, and the connection then
/// closes: its write side first, then its read side at the client's EOF (`DIV-054`).
#[test]
fn forwarded_response_reaches_an_http1_client_after_an_origin_no_error_reset() {
    use crate::owned_h1::OwnedH1Options;
    use crate::owned_h1::OwnedH1Session;

    run_on_worker(move |context| {
        Box::pin(async move {
            // The upload stays open until the client, having read the response, sends EOF.
            let client_eof = Arc::new(tokio::sync::Notify::new());
            let (downstream_transport, downstream_wire) = FixtureTransport::new([
                FixtureRead::Data(
                    b"POST /forward HTTP/1.1\r\nHost: example.com\r\n\
                      Transfer-Encoding: chunked\r\n\r\n2\r\nup\r\n"
                        .to_vec(),
                ),
                FixtureRead::WaitDataEof(Arc::clone(&client_eof), Vec::new()),
            ]);
            let (response, reset) = origin_trailered_response_then_no_error_reset();
            let respond = Arc::new(tokio::sync::Notify::new());
            let (upstream, upstream_handle, upstream_wire) = forwarding_upstream(
                context.clone(),
                vec![
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(Arc::clone(&respond), [response, reset].concat()),
                ],
            );
            let probe = Rc::new(ForwardProbe::default());
            let application = ForwardApplication {
                upstream: upstream_handle.clone(),
                probe: Rc::clone(&probe),
            };
            let OwnedH2Options {
                context,
                transport,
                io,
                ..
            } = options(context, downstream_transport);
            let (downstream, downstream_handle) = OwnedH1Session::new(
                application,
                OwnedH1Options {
                    context,
                    transport,
                    io,
                    http1: Default::default(),
                },
            )
            .unwrap();
            let downstream_done = Cell::new(false);
            let downstream_run = async {
                let result = downstream.run().await;
                downstream_done.set(true);
                result
            };

            let operation = async {
                wait_for_written(&upstream_wire, |wire| {
                    wire.count(FrameType::HEADERS, 0) == 1 && wire.count(FrameType::DATA, 0) == 1
                })
                .await;
                respond.notify_one();
                assert!(
                    writer_drive_until(|| downstream_wire.borrow().close != FixtureClose::Open)
                        .await,
                    "the client's connection closes"
                );
                assert!(
                    !downstream_done.get(),
                    "the connection still reads after its write side shuts down"
                );
                let output = String::from_utf8(downstream_wire.borrow().writes.concat()).unwrap();
                assert_eq!(output.matches("HTTP/1.1 ").count(), 1, "{output:?}");
                assert!(output.starts_with("HTTP/1.1 200 "), "{output:?}");
                assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
                assert!(
                    output.ends_with("\r\n\r\n5\r\nreply\r\n0\r\ngrpc-status: 0\r\n\r\n"),
                    "the whole response, trailers included: {output:?}"
                );
                assert_eq!(downstream_wire.borrow().close, FixtureClose::ShutdownWrite);
                assert!(probe.cancelled.borrow().is_empty());
                assert_eq!(probe.response_complete.get(), 1);
                assert_eq!(probe.completed.get(), 1);
                let upstream_resets =
                    decode_client_frames(&upstream_wire.borrow().writes.concat()).resets;
                assert!(
                    upstream_resets.is_empty(),
                    "the origin's reset is not echoed"
                );
                client_eof.notify_one();
                assert!(
                    writer_drive_until(|| downstream_done.get()).await,
                    "the client's EOF ends the connection"
                );
                assert_eq!(downstream_wire.borrow().close, FixtureClose::ShutdownWrite);
            };
            let script = async {
                let outcome = eligibility_catch_unwind(tokio::time::timeout(
                    Duration::from_secs(5),
                    operation,
                ))
                .await;
                // The origin's and the client's reads wait on them, so a failed check still
                // lets both sessions join.
                respond.notify_one();
                client_eof.notify_one();
                downstream_handle.cancel();
                upstream_handle.cancel();
                outcome
            };
            let (downstream_run, upstream_run, outcome) =
                tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::join!(downstream_run, upstream.run(), script)
                })
                .await
                .expect("both sessions join");
            match outcome {
                Ok(result) => result.expect("the forwarded exchange made progress"),
                Err(payload) => resume_unwind(payload),
            }
            downstream_run.expect("the client's connection closes without error");
            assert_eq!(
                upstream_run
                    .map(drop)
                    .expect_err("the upstream session ends by cancellation")
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
        })
    });
}

#[test]
fn same_read_client_goaway_below_the_stream_does_not_preempt_its_start() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (mut peer, mut input) = ready_owner_requests(1);
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Goaway {
                    last_stream: StreamId::UNSPECIFIED,
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
            ));
            let (probe, gate, decoded) = run_single_read_to_eof(context, input).await;

            // A client GOAWAY's last stream bounds server-initiated streams, so
            // it does not refuse client stream 1 parsed earlier in the read.
            assert_eq!(probe.requested.get(), 1);
            assert_eq!(gate.polls.get(), 1);
            assert!(probe.cancelled.borrow().is_empty());
            assert_eq!(probe.response_complete.get(), 1);
            assert_eq!(probe.completed.get(), 1);
            assert_eq!(probe.dropped.get(), 1);
            assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
            assert_eq!(decoded.body_data, b"rrrr");
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
            assert_eq!(
                decoded.goaways,
                [(StreamId::new(1), H2ErrorCode::NO_ERROR)],
                "the server's GOAWAY reports stream 1 as processed"
            );
        })
    });
}

#[test]
fn peer_reset_discards_a_malformed_head_response_parked_behind_a_held_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut preface = BufQueue::new(4096);
            peer.generate(
                &mut preface,
                EgressFrame::ConnectionPreface,
                &mut IgnoreCodecEvents,
            )
            .unwrap();
            peer.generate_configured_settings(&mut preface, &mut IgnoreCodecEvents)
                .unwrap();
            let mut initial = preface.coalesce(preface.chain_length()).unwrap().to_vec();
            initial.extend(eligibility_request(&mut peer, 0));
            let rejected_stream = peer.create_stream();
            assert_eq!(rejected_stream, StreamId::new(3));
            let mut malformed = HttpMessage::new();
            malformed.set_method(HttpMethod::Trace).unwrap();
            malformed.set_url(b"/parked-error", false).unwrap();
            malformed.set_http_version(1, 1);
            malformed
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            malformed
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"1");
            let malformed = eligibility_frame(
                &mut peer,
                EgressFrame::Headers {
                    stream: rejected_stream,
                    message: &malformed,
                    end_stream: true,
                    extra_headers: None,
                },
            );
            let mut reset_and_sibling = eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: rejected_stream,
                    code: H2ErrorCode::CANCEL,
                },
            );
            reset_and_sibling.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: rejected_stream,
                    code: H2ErrorCode::REFUSED_STREAM,
                },
            ));
            reset_and_sibling.extend(eligibility_request(&mut peer, 1));
            let release_write = Arc::new(tokio::sync::Notify::new());
            let send_malformed = Arc::new(tokio::sync::Notify::new());
            let send_reset = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(initial),
                FixtureRead::WaitData(Arc::clone(&send_malformed), malformed),
                FixtureRead::WaitData(Arc::clone(&send_reset), reset_and_sibling),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let (application, probes) =
                Application::new(vec![Reply::Body(4096), Reply::Body(0), Reply::Body(1)]);
            let mut config = options(context, transport);
            config.io.buffer_limit = 16 << 10;
            config.io.ingress_buffer_limit = 16 << 10;
            config.io.maximum_write_size = 128;
            config.io.maximum_read_size = 4096;
            config.io.read_allocation_size = 4096;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            exercise(
                session,
                &handle,
                &[
                    Arc::clone(&release_write),
                    Arc::clone(&send_malformed),
                    Arc::clone(&send_reset),
                ],
                async {
                    complete_credit_wait(|| {
                        state.borrow().write_gate_pending.is_some()
                            && probes
                                .borrow()
                                .first()
                                .is_some_and(|probe| probe.produced_bytes.get() == 4096)
                    })
                    .await;
                    let first = Rc::clone(&probes.borrow()[0]);
                    assert_eq!(first.response_complete.get(), 0);
                    send_malformed.notify_one();
                    complete_credit_wait(|| {
                        probes
                            .borrow()
                            .get(1)
                            .is_some_and(|probe| !probe.cancelled.borrow().is_empty())
                    })
                    .await;
                    let rejected = Rc::clone(&probes.borrow()[1]);
                    assert_eq!(
                        rejected.cancelled.borrow().as_slice(),
                        &[(HttpErrorCode::HEADER_PARSE_ERROR, false)]
                    );
                    assert_eq!(rejected.requested.get(), 0);
                    assert_eq!(rejected.response_complete.get(), 0);
                    send_reset.notify_one();
                    complete_credit_wait(|| {
                        rejected.cancelled.borrow().len() == 2 && probes.borrow().len() == 3
                    })
                    .await;
                    let sibling = Rc::clone(&probes.borrow()[2]);
                    assert_eq!(
                        rejected.cancelled.borrow().as_slice(),
                        &[
                            (HttpErrorCode::HEADER_PARSE_ERROR, false),
                            (HttpErrorCode::CANCEL, true),
                        ]
                    );
                    assert_eq!(rejected.response_complete.get(), 0);
                    assert!(state.borrow().writes.is_empty());
                    release_write.notify_one();
                    complete_credit_wait(|| {
                        first.completed.get() == 1
                            && sibling.completed.get() == 1
                            && rejected.dropped.get() == 1
                    })
                    .await;
                    assert_eq!(first.dropped.get(), 1);
                    assert_eq!(sibling.dropped.get(), 1);
                    assert!(first.cancelled.borrow().is_empty());
                    assert!(sibling.cancelled.borrow().is_empty());
                    assert_eq!(rejected.completed.get(), 0);
                    assert_eq!(rejected.response_complete.get(), 0);
                    let mut decoded =
                        decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
                    decoded.header_statuses.sort_by_key(|(stream, _)| *stream);
                    assert_eq!(
                        decoded.header_statuses,
                        [(StreamId::new(1), 200), (StreamId::new(5), 200)]
                    );
                    assert_eq!(decoded.body_data, vec![b'r'; 4097]);
                    assert!(decoded.resets.is_empty());
                    assert!(decoded.errors.is_empty());
                },
            )
            .await;
        })
    });
}

#[test]
fn cancellation_waker_panic_waits_for_both_native_io_completions() {
    use std::task::Wake;
    use std::task::Waker;

    #[derive(Default)]
    struct IoReceipt {
        started: [Cell<bool>; 2],
        cancelled: [Cell<bool>; 2],
        completed: [Cell<bool>; 2],
        allow_completion: Cell<bool>,
        native_wakers: [RefCell<Option<Waker>>; 2],
        dropped: [Cell<usize>; 2],
        closed: Cell<bool>,
    }

    #[derive(Default)]
    struct WakeReceipt {
        calls: [AtomicUsize; 2],
        total: AtomicUsize,
    }

    struct CancelWake {
        direction: usize,
        receipt: Arc<WakeReceipt>,
        driver: Waker,
    }

    impl Wake for CancelWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.receipt.calls[self.direction].fetch_add(1, Ordering::SeqCst);
            let ordinal = self.receipt.total.fetch_add(1, Ordering::SeqCst);
            self.driver.wake_by_ref();
            if ordinal == 0 {
                panic_any("first native cancellation waker");
            }
        }
    }

    struct IoDrop {
        direction: usize,
        receipt: Rc<IoReceipt>,
    }

    impl Drop for IoDrop {
        fn drop(&mut self) {
            let count = &self.receipt.dropped[self.direction];
            count.set(count.get() + 1);
        }
    }

    struct CancellationTransport {
        inner: Rc<FixtureTransport>,
        receipt: Rc<IoReceipt>,
        wakes: Arc<WakeReceipt>,
    }

    impl CancellationTransport {
        fn pending(&self, direction: usize, token: CancellationToken) -> LocalTransportFuture<()> {
            let receipt = Rc::clone(&self.receipt);
            let wakes = Arc::clone(&self.wakes);
            Box::pin(async move {
                let _drop = IoDrop {
                    direction,
                    receipt: Rc::clone(&receipt),
                };
                let baton = CancellableBaton::new();
                let mut wait = std::pin::pin!(baton.wait(token.clone()));
                let result = poll_fn(|context| {
                    let waker = Waker::from(Arc::new(CancelWake {
                        direction,
                        receipt: Arc::clone(&wakes),
                        driver: context.waker().clone(),
                    }));
                    let result = wait.as_mut().poll(&mut Context::from_waker(&waker));
                    if result.is_pending() {
                        receipt.started[direction].set(true);
                    }
                    result
                })
                .await;
                assert_eq!(result, WaitStatus::Cancelled);
                assert!(token.is_cancellation_requested());
                receipt.cancelled[direction].set(true);
                poll_fn(|context| {
                    if receipt.allow_completion.get() {
                        Poll::Ready(())
                    } else {
                        *receipt.native_wakers[direction].borrow_mut() =
                            Some(context.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
                receipt.completed[direction].set(true);
            })
        }
    }

    impl UniplexTransport for CancellationTransport {
        fn read(
            &self,
            _context: &WorkerContext,
            _maximum: usize,
            _allocation: usize,
            _timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
            let pending = self.pending(0, cancellation);
            Box::pin(async move {
                pending.await;
                Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"native read completed cancellation",
                ))
            })
        }

        fn write(
            &self,
            _context: &WorkerContext,
            data: BufQueue,
            _flags: ByteEventWriteFlags,
            _timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
            let pending = self.pending(1, cancellation);
            Box::pin(async move {
                let _owned_write = data;
                pending.await;
                Err(TransportWriteError::new(
                    HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"native write completed cancellation",
                    ),
                    0,
                ))
            })
        }

        fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.receipt.closed.set(true);
            self.inner.shutdown_write(context)
        }

        fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.receipt.closed.set(true);
            self.inner.close(context)
        }

        fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.receipt.closed.set(true);
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

    run_on_worker(|context| {
        Box::pin(async move {
            let (inner, state) = FixtureTransport::new([]);
            let receipt = Rc::new(IoReceipt::default());
            let wakes = Arc::new(WakeReceipt::default());
            let transport = Rc::new(CancellationTransport {
                inner,
                receipt: Rc::clone(&receipt),
                wakes: Arc::clone(&wakes),
            });
            let (application, probes) = Application::new(vec![]);
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let cancel = async {
                complete_credit_wait(|| receipt.started.iter().all(Cell::get)).await;
                handle.cancel();
                complete_credit_wait(|| {
                    receipt.closed.get() && receipt.cancelled.iter().all(Cell::get)
                })
                .await;
                assert_eq!([receipt.dropped[0].get(), receipt.dropped[1].get()], [0, 0]);
                assert!(receipt.completed.iter().all(|completed| !completed.get()));
                receipt.allow_completion.set(true);
                for waker in &receipt.native_wakers {
                    if let Some(waker) = waker.borrow_mut().take() {
                        waker.wake();
                    }
                }
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(eligibility_catch_unwind(session.run()), cancel)
            })
            .await
            .expect("panicking cancellation still joins both real IO futures");
            let panic = result.expect_err("the original cancellation waker panic is resumed");
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"first native cancellation waker")
            );
            assert!(receipt.completed.iter().all(Cell::get));
            assert_eq!([receipt.dropped[0].get(), receipt.dropped[1].get()], [1, 1]);
            assert_eq!(
                [
                    wakes.calls[0].load(Ordering::SeqCst),
                    wakes.calls[1].load(Ordering::SeqCst)
                ],
                [1, 1]
            );
            assert_eq!(state.borrow().close, FixtureClose::Reset);
            assert!(probes.borrow().is_empty());
        })
    });
}

#[test]
fn abandoning_a_started_run_signals_io_and_reports_the_missing_join() {
    struct ObserveCancellation {
        inner: Rc<FixtureTransport>,
        read_token: RefCell<Option<CancellationToken>>,
        write_token: RefCell<Option<CancellationToken>>,
    }

    impl UniplexTransport for ObserveCancellation {
        fn read(
            &self,
            context: &WorkerContext,
            maximum: usize,
            allocation: usize,
            timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
            *self.read_token.borrow_mut() = Some(cancellation.clone());
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
            *self.write_token.borrow_mut() = Some(cancellation.clone());
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

    run_on_worker(|context| {
        Box::pin(async move {
            let (inner, state) = FixtureTransport::new([]);
            state.borrow_mut().write_gate = Some(Arc::new(tokio::sync::Notify::new()));
            let transport = Rc::new(ObserveCancellation {
                inner,
                read_token: RefCell::new(None),
                write_token: RefCell::new(None),
            });
            let (application, probes) = Application::new(vec![]);
            let (session, _) =
                OwnedH2Session::new(application, options(context.clone(), transport.clone()))
                    .unwrap();
            let mut run = Box::pin(session.run());
            poll_fn(|context| {
                assert!(run.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(state.borrow().write_gate_pending.is_some());
            let read = transport.read_token.borrow().as_ref().unwrap().clone();
            let write = transport.write_token.borrow().as_ref().unwrap().clone();
            assert!(!read.is_cancellation_requested());
            assert!(!write.is_cancellation_requested());
            let panic = catch_unwind(AssertUnwindSafe(|| drop(run)))
                .expect_err("dropping a started runner must report unjoined native IO");
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap();
            assert_eq!(
                message,
                "started owned H2 runner dropped before application and native I/O cleanup joined"
            );
            assert!(read.is_cancellation_requested());
            assert!(write.is_cancellation_requested());
            assert_eq!(state.borrow().close, FixtureClose::Reset);
            assert!(state.borrow().writes.is_empty());
            assert!(probes.borrow().is_empty());

            let (transport, _) = FixtureTransport::new([]);
            let (application, _) = Application::new(vec![]);
            let (unstarted, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            assert!(catch_unwind(AssertUnwindSafe(|| drop(unstarted.run()))).is_ok());
        })
    });
}

/// `DIV-043`: a worker whose teardown deadline has passed destroys a started owned HTTP/2
/// downstream, here waiting for its client's preface, before its application and native I/O
/// cleanup joined. That destruction is an abandonment: the driver still resets the transport,
/// and the worker reports a clean overrun instead of a panic.
#[test]
fn a_forced_worker_teardown_abandons_an_owned_h2_downstream() {
    let (inspected_tx, inspected_rx) = std::sync::mpsc::channel();
    let completion = forced_teardown_of_scope(move |context| {
        // The read is never notified and ignores cancellation, so the driver's native I/O cleanup
        // cannot join before the forced teardown destroys it.
        let (transport, state) = FixtureTransport::new([FixtureRead::WaitData(
            Arc::new(tokio::sync::Notify::new()),
            Vec::new(),
        )]);
        let (application, _probes) = Application::new(vec![]);
        let (session, handle) =
            OwnedH2Session::new(application, options(context, transport)).unwrap();
        after_forced_teardown(move || inspected_tx.send(state.borrow().close).unwrap());
        Box::pin(async move {
            let _ = session.run().await;
            drop(handle);
        })
    });
    assert_eq!(
        completion,
        Err(ManagedWorkerError::TeardownDeadlineExceeded),
        "the forced destruction of the unjoined driver is a clean overrun"
    );
    assert_eq!(
        inspected_rx.recv().unwrap(),
        FixtureClose::Reset,
        "the abandoned driver reset its transport"
    );
}

/// `DIV-051` with `DIV-043`: a lifecycle observer on an owned HTTP/2 downstream that a forced
/// `HostedWorker` teardown destroys sees its connection's destroy exactly once, as the last
/// event: `Drop` reports it before the forced-teardown check. No panic escapes, and the worker
/// reports a clean overrun.
#[test]
fn a_forced_worker_teardown_reports_an_owned_h2_destroy_to_its_observer_once() {
    let (events_tx, events_rx) = std::sync::mpsc::channel();
    let completion = forced_teardown_of_scope(move |context| {
        let (transport, _state) = FixtureTransport::new([FixtureRead::WaitData(
            Arc::new(tokio::sync::Notify::new()),
            Vec::new(),
        )]);
        let (application, _probes) = Application::new(vec![]);
        let (mut session, handle) =
            OwnedH2Session::new(application, options(context, transport)).unwrap();
        let events = Rc::new(RefCell::new(Vec::new()));
        session
            .add_lifecycle_observer(Rc::new(LifecycleProbe {
                events: Rc::clone(&events),
            }))
            .unwrap();
        after_forced_teardown(move || events_tx.send(events.take()).unwrap());
        Box::pin(async move {
            let _ = session.run().await;
            drop(handle);
        })
    });
    assert_eq!(
        completion,
        Err(ManagedWorkerError::TeardownDeadlineExceeded),
        "the forced destruction of the observed driver is a clean overrun"
    );
    let events = events_rx.recv().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == LifecycleEvent::Destroy)
            .count(),
        1,
        "the observer sees one destroy: {events:?}"
    );
    assert_eq!(
        events.last(),
        Some(&LifecycleEvent::Destroy),
        "the destroy is the last event: {events:?}"
    );
}

#[test]
fn synthesized_error_head_waits_for_the_admitted_ingress_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Post).unwrap();
            request.set_url(b"/incomplete", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            request
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"1");
            let input = peer_request_message_bytes(request, None, false);
            let send_eof = Arc::new(tokio::sync::Notify::new());
            let release_write = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitDataEof(Arc::clone(&send_eof), Vec::new()),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.retain_request = true;
            application.hold_headers = true;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let scenario = async {
                let outcome =
                    eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                        complete_credit_wait(|| {
                            probes
                                .borrow()
                                .first()
                                .is_some_and(|probe| probe.held_body.borrow().is_some())
                        })
                        .await;
                        let probe = Rc::clone(&probes.borrow()[0]);
                        let mut body = probe.held_body.borrow_mut().take().unwrap();
                        match body.read_body_event(1).await.unwrap().into_kind() {
                            HttpBodyEventKind::Suspend(resume) => drop(resume),
                            _ => panic!("an empty request body first yields its suspend event"),
                        }
                        let mut read = std::pin::pin!(body.read_body_event_unboxed(1));
                        poll_fn(|context| {
                            assert!(read.as_mut().poll(context).is_pending());
                            Poll::Ready(())
                        })
                        .await;
                        send_eof.notify_one();
                        complete_credit_wait(|| probe.cancelled.borrow().len() == 1).await;
                        for _ in 0..4 {
                            tokio::task::yield_now().await;
                        }
                        assert!(
                            decode_server_frames(&state.borrow().writes.concat())
                                .header_streams
                                .is_empty()
                        );
                        assert_eq!(probe.dropped.get(), 0);
                        state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                        let error = match read.await {
                            Err(error) => error,
                            Ok(_) => panic!("incomplete request must report transport EOF"),
                        };
                        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
                        drop(body);
                        complete_credit_wait(|| state.borrow().write_gate_pending.is_some()).await;
                        assert_eq!(probe.dropped.get(), 0);
                        assert_eq!(probe.completed.get(), 0);
                        release_write.notify_one();
                        complete_credit_wait(|| probe.dropped.get() == 1).await;
                        assert_eq!(
                            probe.cancelled.borrow().as_slice(),
                            &[(HttpErrorCode::TRANSPORT_EOF, false)]
                        );
                        assert_eq!(probe.completed.get(), 0);
                        let decoded = decode_server_frames(&state.borrow().writes.concat());
                        assert_eq!(decoded.header_statuses, [(StreamId::new(1), 500)]);
                        assert!(decoded.errors.is_empty());
                    }))
                    .await;
                send_eof.notify_one();
                release_write.notify_one();
                if !matches!(&outcome, Ok(Ok(()))) {
                    handle.cancel();
                }
                outcome
            };
            let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(session.run(), scenario)
            })
            .await
            .expect("EOF error response retires its input and write owners");
            match outcome {
                Ok(result) => result.expect("EOF error response made progress"),
                Err(payload) => resume_unwind(payload),
            }
            result.unwrap();
        })
    });
}

#[test]
fn empty_terminal_data_survives_a_negative_stream_window() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut preface = BufQueue::new(4096);
            peer.generate(
                &mut preface,
                EgressFrame::ConnectionPreface,
                &mut IgnoreCodecEvents,
            )
            .unwrap();
            peer.generate_configured_settings(&mut preface, &mut IgnoreCodecEvents)
                .unwrap();
            let mut input = preface.coalesce(preface.chain_length()).unwrap().to_vec();
            input.extend(eligibility_request(&mut peer, 0));
            let mut shrink = eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: 0,
                    }],
                },
            );
            shrink.extend(eligibility_frame(
                &mut peer,
                EgressFrame::PingRequest { data: Some(44) },
            ));
            shrink.extend(eligibility_request(&mut peer, 1));
            let send_shrink = Arc::new(tokio::sync::Notify::new());
            let send_terminal = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&send_shrink), shrink),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(1), Reply::Body(0)]);
            application.empty_terminal = Some(Arc::clone(&send_terminal));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(
                session,
                &handle,
                &[Arc::clone(&send_shrink), Arc::clone(&send_terminal)],
                async {
                    complete_credit_wait(|| {
                        decode_server_frames_with_requests(&state.borrow().writes.concat(), 2)
                            .body_bytes
                            == 1
                    })
                    .await;
                    let first = Rc::clone(&probes.borrow()[0]);
                    assert_eq!(first.response_complete.get(), 0);
                    send_shrink.notify_one();
                    complete_credit_wait(|| {
                        let wire =
                            decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                        wire.ping_replies == [44]
                            && probes
                                .borrow()
                                .get(1)
                                .is_some_and(|probe| probe.completed.get() == 1)
                    })
                    .await;
                    assert_eq!(first.completed.get(), 0);
                    assert!(first.cancelled.borrow().is_empty());
                    send_terminal.notify_one();
                    complete_credit_wait(|| first.completed.get() == 1).await;
                    assert_eq!(first.response_complete.get(), 1);
                    assert_eq!(first.dropped.get(), 1);
                    assert!(first.cancelled.borrow().is_empty());
                    let decoded =
                        decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
                    assert_eq!(decoded.body_data, b"r");
                    assert!(decoded.resets.is_empty());
                    assert!(decoded.errors.is_empty());
                    let wire = eligibility_wire(&state);
                    assert!(wire.completed.contains(&StreamId::new(1)));
                    assert!(wire.completed.contains(&StreamId::new(3)));
                },
            )
            .await;
        })
    });
}

#[test]
fn ready_small_response_batches_headers_and_body_into_one_transport_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (application, probes) = Application::new(vec![Reply::Body(1024)]);
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .unwrap();
            let probes = probes.borrow();
            assert_eq!(probes.len(), 1);
            let probe = &probes[0];
            assert_eq!(probe.completed.get(), 1);
            assert_eq!(probe.dropped.get(), 1);
            assert_eq!(probe.body_polls.get(), 1);
            assert!(probe.cancelled.borrow().is_empty());
            let state = state.borrow();
            let decoded = decode_server_frames(&state.writes.concat());
            assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
            assert_eq!(decoded.body_data, vec![b'r'; 1024]);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
            let mut header_writes = Vec::new();
            let mut body_writes = Vec::new();
            for (write, bytes) in state.writes.iter().enumerate() {
                let mut offset = 0;
                while offset < bytes.len() {
                    assert!(bytes.len() - offset >= FRAME_HEADER_SIZE);
                    let length = (usize::from(bytes[offset]) << 16)
                        | (usize::from(bytes[offset + 1]) << 8)
                        | usize::from(bytes[offset + 2]);
                    let stream =
                        u32::from_be_bytes(bytes[offset + 5..offset + 9].try_into().unwrap())
                            & 0x7fff_ffff;
                    if stream == 1 {
                        if bytes[offset + 3] == FrameType::HEADERS.as_u8() {
                            header_writes.push(write);
                        } else if bytes[offset + 3] == FrameType::DATA.as_u8() {
                            body_writes.push(write);
                        }
                    }
                    offset += FRAME_HEADER_SIZE + length;
                    assert!(
                        offset <= bytes.len(),
                        "this small fixture has complete frames"
                    );
                }
            }
            assert_eq!(header_writes.len(), 1);
            assert_eq!(body_writes.len(), 1);
            assert_eq!(
                header_writes, body_writes,
                "the already-ready 1 KiB body shares its header's transport write"
            );
        })
    });
}

#[test]
fn ready_owner_suffix_precedes_later_cross_thread_wake_after_yield() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    struct RecordingWake(AtomicBool);
    impl Wake for RecordingWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            let release_write = Arc::new(tokio::sync::Notify::new());
            let release_read = Arc::new(tokio::sync::Notify::new());
            let gate = Rc::new(OwnerPollGate {
                pending_visits: Some(RefCell::new(Vec::new())),
                ..OwnerPollGate::default()
            });
            let (_, input) = ready_owner_requests(66);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::WaitDataEof(Arc::clone(&release_read), Vec::new()),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let (mut application, probes) = Application::new(vec![Reply::Body(0); 66]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let wake = Arc::new(RecordingWake(AtomicBool::new(false)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut run = Box::pin(session.run());
            let mut finished = None;
            let outcome = eligibility_catch_unwind(async {
                let mut poll_pending = || {
                    wake.0.store(false, Ordering::SeqCst);
                    if let Poll::Ready(result) = run.as_mut().poll(&mut Context::from_waker(&waker))
                    {
                        finished = Some(result);
                        panic!("held native I/O keeps the session running");
                    }
                };
                for _ in 0..16 {
                    poll_pending();
                    if !wake.0.load(Ordering::SeqCst) {
                        break;
                    }
                }
                assert!(
                    !wake.0.load(Ordering::SeqCst),
                    "initial owner readiness settles"
                );
                assert!(state.borrow().reads.is_empty());
                assert!(
                    state
                        .borrow()
                        .write_gate_pending
                        .is_some_and(|length| length > 0)
                );
                let mut wakers: Vec<_> = gate
                    .wakers
                    .borrow()
                    .iter()
                    .map(|waker| waker.as_ref().expect("each owner registered").clone())
                    .collect();
                assert_eq!(wakers.len(), 66);
                let late = wakers.pop().unwrap();
                gate.ready.set(true);
                for waker in &wakers {
                    waker.wake_by_ref();
                }
                poll_pending();
                let prefix = gate.pending_visits.as_ref().unwrap().borrow().clone();
                assert!(
                    (1..65).contains(&prefix.len()),
                    "the ready batch spans a yield"
                );
                assert_eq!(prefix, (0..prefix.len()).collect::<Vec<_>>());
                // The budget yield wakes the session once this task returns to the scheduler.
                tokio::task::yield_now().await;
                assert!(
                    wake.0.load(Ordering::SeqCst),
                    "the suffix requests another poll"
                );

                let unvisited = wakers[prefix.len()].clone();
                wake.0.store(false, Ordering::SeqCst);
                std::thread::spawn(move || {
                    unvisited.wake();
                    late.wake();
                })
                .join()
                .expect("cross-thread owner wakes complete");
                assert!(
                    wake.0.load(Ordering::SeqCst),
                    "the late owner wakes its session"
                );
                for _ in 0..16 {
                    poll_pending();
                    if !wake.0.load(Ordering::SeqCst) {
                        break;
                    }
                }
                assert!(!wake.0.load(Ordering::SeqCst), "the ready owners settle");
                assert_eq!(
                    *gate.pending_visits.as_ref().unwrap().borrow(),
                    (0..66).collect::<Vec<_>>()
                );
            })
            .await;
            handle.cancel();
            release_write.notify_one();
            release_read.notify_one();
            let result = match finished {
                Some(result) => result,
                None => tokio::time::timeout(Duration::from_secs(5), run.as_mut())
                    .await
                    .expect("all owners and both native operations join"),
            };
            drop(run);
            if let Err(payload) = outcome {
                resume_unwind(payload);
            }
            assert_eq!(result.unwrap_err().code(), HttpErrorCode::CORO_CANCELLED);
            let probes = probes.borrow();
            assert_eq!(probes.len(), 66);
            for probe in probes.iter() {
                assert_eq!(probe.requested.get(), 1);
                assert_eq!(probe.body_polls.get(), 0);
                assert_eq!(probe.response_complete.get(), 0);
                assert_eq!(probe.completed.get(), 0);
                assert_eq!(probe.dropped.get(), 1);
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CORO_CANCELLED, false)]
                );
            }
            assert!(state.borrow().transport_dropped);
            assert_eq!(state.borrow().close, FixtureClose::Reset);
        })
    });
}

#[test]
fn continuously_ready_body_flushes_and_yields_within_one_turn_budget() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::DataEof(Vec::new()),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(1024)]);
            application.body_chunk = 1;
            let mut config = options(context, transport);
            config.io.buffer_limit = 64 << 10;
            config.io.ingress_buffer_limit = 64 << 10;
            config.io.maximum_write_size = 64 << 10;
            config.io.maximum_read_size = 4096;
            config.io.read_allocation_size = 4096;
            let (session, _) = OwnedH2Session::new(application, config).unwrap();
            let mut run = Box::pin(session.run());
            let first = poll_fn(|context| Poll::Ready(run.as_mut().poll(context))).await;
            let yielded = first.is_pending();
            let produced_at_yield = probes
                .borrow()
                .first()
                .map_or(0, |probe| probe.body_polls.get());
            let written_at_yield = decode_server_frames(&state.borrow().writes.concat()).body_bytes;
            let result = match first {
                Poll::Ready(result) => result,
                Poll::Pending => tokio::time::timeout(Duration::from_secs(5), run)
                    .await
                    .unwrap(),
            };
            result.unwrap();
            assert!(yielded, "a ready source longer than one budget must yield");
            assert!(
                (1..=64).contains(&produced_at_yield),
                "one poll must retain the bounded work budget: {produced_at_yield}"
            );
            assert!(
                written_at_yield > 0,
                "the last productive turn must flush staged DATA before yielding"
            );
            assert!(written_at_yield <= produced_at_yield);
            let probes = probes.borrow();
            assert_eq!(probes.len(), 1);
            assert_eq!(probes[0].body_polls.get(), 1024);
            assert_eq!(probes[0].completed.get(), 1);
            assert_eq!(probes[0].dropped.get(), 1);
            assert!(probes[0].cancelled.borrow().is_empty());
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
            assert_eq!(decoded.body_data, vec![b'r'; 1024]);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

#[test]
fn continuously_ready_owners_do_not_starve_control_input() {
    run_on_worker(|context| {
        Box::pin(async move {
            let release_control = Arc::new(tokio::sync::Notify::new());
            let release_eof = Arc::new(tokio::sync::Notify::new());
            let gate = Rc::new(OwnerPollGate::default());
            let (mut peer, input) = ready_owner_requests(17);
            let mut control =
                eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(43) });
            control.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(33),
                    code: H2ErrorCode::CANCEL,
                },
            ));
            let (transport, state) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::WaitData(Arc::clone(&release_control), control),
                FixtureRead::WaitDataEof(Arc::clone(&release_eof), Vec::new()),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(1024); 17]);
            application.body_chunk = 1;
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let mut config = options(context, transport);
            config.io.buffer_limit = 64 << 10;
            config.io.ingress_buffer_limit = 64 << 10;
            config.io.maximum_write_size = 64 << 10;
            config.io.maximum_read_size = 4096;
            config.io.read_allocation_size = 4096;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            let mut run = Box::pin(session.run());
            let mut finished = None;
            let outcome = eligibility_catch_unwind(async {
                if let Poll::Ready(result) =
                    poll_fn(|context| Poll::Ready(run.as_mut().poll(context))).await
                {
                    finished = Some(result);
                    return;
                }
                assert_eq!(gate.wakers.borrow().len(), 17);
                assert!(gate.wakers.borrow().iter().all(Option::is_some));
                assert_eq!(state.borrow().reads.len(), 1);
                gate.release();
                if let Poll::Ready(result) =
                    poll_fn(|context| Poll::Ready(run.as_mut().poll(context))).await
                {
                    finished = Some(result);
                    return;
                }
                let polls_at_yield: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.body_polls.get())
                    .sum();
                assert!((1..=64).contains(&polls_at_yield));
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 17);
                assert!(decoded.body_bytes > 0);
                assert!(decoded.ping_replies.is_empty());
                assert!(
                    probes
                        .borrow()
                        .iter()
                        .all(|probe| probe.produced_bytes.get() < 1024)
                );
                *gate.control_read.borrow_mut() = Some(Arc::clone(&release_control));
                if let Poll::Ready(result) =
                    poll_fn(|context| Poll::Ready(run.as_mut().poll(context))).await
                {
                    finished = Some(result);
                    return;
                }
                let polls_after_control: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.body_polls.get())
                    .sum();
                assert!((1..=64).contains(&(polls_after_control - polls_at_yield)));
                assert!(gate.control_read.borrow().is_none());
                assert_eq!(
                    probes.borrow()[16].cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CANCEL, true)]
                );
                assert!(
                    probes
                        .borrow()
                        .iter()
                        .all(|probe| probe.produced_bytes.get() < 1024)
                );
                let decoded =
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 17);
                assert_eq!(decoded.ping_replies, [43]);
                assert!(decoded.errors.is_empty());
            })
            .await;
            release_control.notify_one();
            release_eof.notify_one();
            if outcome.is_err() {
                handle.cancel();
            }
            let result = match finished {
                Some(result) => result,
                None => tokio::time::timeout(Duration::from_secs(5), run)
                    .await
                    .expect("ready responses and control input complete"),
            };
            if let Err(payload) = outcome {
                resume_unwind(payload);
            }
            result.unwrap();
            let probes = probes.borrow();
            assert_eq!(probes.len(), 17);
            for probe in &probes[..16] {
                assert_eq!(probe.produced_bytes.get(), 1024);
                assert_eq!(probe.response_complete.get(), 1);
                assert_eq!(probe.completed.get(), 1);
                assert_eq!(probe.dropped.get(), 1);
                assert!(probe.cancelled.borrow().is_empty());
            }
            assert_eq!(probes[16].completed.get(), 0);
            assert_eq!(probes[16].dropped.get(), 1);
            assert_eq!(
                probes[16].cancelled.borrow().as_slice(),
                &[(HttpErrorCode::CANCEL, true)]
            );
            let decoded = decode_server_frames_with_requests(&state.borrow().writes.concat(), 17);
            assert_eq!(decoded.ping_replies, [43]);
            assert!((16 * 1024..17 * 1024).contains(&decoded.body_bytes));
            assert!(decoded.body_data.iter().all(|byte| *byte == b'r'));
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

struct PollCountingTransport {
    inner: Rc<FixtureTransport>,
    reads: Rc<RefCell<Vec<usize>>>,
    writes: Rc<RefCell<Vec<usize>>>,
    wake_first: bool,
    read_panic: Option<Rc<ReadPollPanic>>,
}

#[derive(Default)]
struct ReadPollPanic {
    token: RefCell<Option<CancellationToken>>,
    completed: Cell<bool>,
    drops: Cell<usize>,
}

struct ReadPollDrop(Rc<ReadPollPanic>);

impl Drop for ReadPollDrop {
    fn drop(&mut self) {
        self.0.drops.set(self.0.drops.get() + 1);
    }
}

impl PollCountingTransport {
    fn counted<T: 'static>(
        &self,
        counts: &Rc<RefCell<Vec<usize>>>,
        mut operation: LocalTransportFuture<T>,
        read_panic: Option<Rc<ReadPollPanic>>,
    ) -> LocalTransportFuture<T> {
        let index = counts.borrow().len();
        counts.borrow_mut().push(0);
        let counts = Rc::clone(counts);
        let wake_first = self.wake_first;
        let read_panic = read_panic.map(ReadPollDrop);
        Box::pin(poll_fn(move |context| {
            let ordinal = {
                let mut counts = counts.borrow_mut();
                counts[index] += 1;
                counts[index]
            };
            if read_panic.is_some() {
                match ordinal {
                    1 => panic_any("first native read poll"),
                    2 => panic_any("second native read poll"),
                    _ => {}
                }
            }
            if wake_first && ordinal == 1 {
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                let result = operation.as_mut().poll(context);
                if result.is_ready()
                    && let Some(probe) = &read_panic
                {
                    probe.0.completed.set(true);
                }
                result
            }
        }))
    }
}

impl UniplexTransport for PollCountingTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        if let Some(probe) = &self.read_panic {
            *probe.token.borrow_mut() = Some(cancellation.clone());
        }
        self.counted(
            &self.reads,
            self.inner
                .read(context, maximum, allocation_size, timeout, cancellation),
            self.read_panic.clone(),
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
        self.counted(
            &self.writes,
            self.inner
                .write(context, data, flags, timeout, cancellation),
            None,
        )
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

#[test]
fn pending_native_io_is_not_repolled_by_ready_body_work() {
    run_on_worker(|context| {
        Box::pin(async move {
            let finish_read = Arc::new(tokio::sync::Notify::new());
            let finish_write = Arc::new(tokio::sync::Notify::new());
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (inner, wire) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::WaitDataEof(Arc::clone(&finish_read), Vec::new()),
            ]);
            wire.borrow_mut().write_gate = Some(Arc::clone(&finish_write));
            let reads = Rc::new(RefCell::new(Vec::new()));
            let writes = Rc::new(RefCell::new(Vec::new()));
            let transport = Rc::new(PollCountingTransport {
                inner,
                reads: Rc::clone(&reads),
                writes: Rc::clone(&writes),
                wake_first: false,
                read_panic: None,
            });
            let (mut application, probes) = Application::new(vec![Reply::Body(1024)]);
            application.body_chunk = 1;
            let mut config = options(context, transport);
            config.io.maximum_write_size = 128;
            let (session, _) = OwnedH2Session::new(application, config).unwrap();
            let mut run = Box::pin(session.run());
            let first = poll_fn(|context| Poll::Ready(run.as_mut().poll(context))).await;
            let yielded = first.is_pending();
            let pending_read_polls = reads.borrow().get(1).copied();
            let pending_write_polls = writes.borrow().first().copied();
            let write_waiting = wire.borrow().write_gate_pending.is_some();
            let body_polls = probes
                .borrow()
                .first()
                .map_or(0, |probe| probe.body_polls.get());
            finish_read.notify_one();
            finish_write.notify_one();
            let result = match first {
                Poll::Ready(result) => result,
                Poll::Pending => tokio::time::timeout(Duration::from_secs(5), run)
                    .await
                    .unwrap(),
            };
            result.unwrap();
            assert!(yielded && write_waiting);
            assert!(body_polls > 1, "ready body work advances while I/O is held");
            assert_eq!(
                (pending_read_polls, pending_write_polls),
                (Some(1), Some(1)),
                "the same retained read and write receive no wake before the snapshot"
            );
            let probe = Rc::clone(&probes.borrow()[0]);
            assert_eq!(probe.completed.get(), 1);
            assert_eq!(probe.dropped.get(), 1);
            assert!(probe.cancelled.borrow().is_empty());
            let decoded = decode_server_frames(&wire.borrow().writes.concat());
            assert_eq!(decoded.body_data, vec![b'r'; 1024]);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

#[test]
fn native_io_wake_during_poll_preserves_same_turn_progress_and_new_operations() {
    run_on_worker(|context| {
        Box::pin(async move {
            let finish_read = Arc::new(tokio::sync::Notify::new());
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (inner, wire) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::SegmentedData(queue(b"")),
                FixtureRead::WaitDataEof(Arc::clone(&finish_read), Vec::new()),
            ]);
            let reads = Rc::new(RefCell::new(Vec::new()));
            let writes = Rc::new(RefCell::new(Vec::new()));
            let transport = Rc::new(PollCountingTransport {
                inner,
                reads: Rc::clone(&reads),
                writes: Rc::clone(&writes),
                wake_first: true,
                read_panic: None,
            });
            let (mut application, probes) = Application::new(vec![Reply::Body(1024)]);
            application.body_chunk = 1;
            let mut config = options(context, transport);
            config.io.maximum_write_size = 128;
            let (session, _) = OwnedH2Session::new(application, config).unwrap();
            let mut run = Box::pin(session.run());
            let first = poll_fn(|context| Poll::Ready(run.as_mut().poll(context))).await;
            let yielded = first.is_pending();
            let first_reads = reads.borrow().clone();
            let first_writes = writes.borrow().clone();
            finish_read.notify_one();
            let result = match first {
                Poll::Ready(result) => result,
                Poll::Pending => tokio::time::timeout(Duration::from_secs(5), run)
                    .await
                    .unwrap(),
            };
            result.unwrap();
            assert!(yielded);
            assert!(
                first_reads.first().is_some_and(|polls| *polls >= 2)
                    && first_reads.get(1).is_some_and(|polls| *polls >= 2),
                "an in-poll wake permits repoll and the next read's first poll: {first_reads:?}"
            );
            assert!(
                first_writes.first().is_some_and(|polls| *polls >= 2),
                "an in-poll write wake survives while body work progresses: {first_writes:?}"
            );
            let probe = Rc::clone(&probes.borrow()[0]);
            assert_eq!(probe.completed.get(), 1);
            assert_eq!(probe.dropped.get(), 1);
            assert!(probe.cancelled.borrow().is_empty());
            let decoded = decode_server_frames(&wire.borrow().writes.concat());
            assert_eq!(decoded.body_data, vec![b'r'; 1024]);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

#[test]
fn repeated_native_read_poll_panics_still_join_before_resuming_the_first_panic() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (inner, wire) = FixtureTransport::new([]);
            let reads = Rc::new(RefCell::new(Vec::new()));
            let probe = Rc::new(ReadPollPanic::default());
            let transport = Rc::new(PollCountingTransport {
                inner,
                reads: Rc::clone(&reads),
                writes: Rc::new(RefCell::new(Vec::new())),
                wake_first: false,
                read_panic: Some(Rc::clone(&probe)),
            });
            let (application, streams) = Application::new(Vec::new());
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                eligibility_catch_unwind(session.run()),
            )
            .await
            .expect("repeated pre-registration read panics still join cancellation");
            let panic = result.expect_err("the first transport poll panic is resumed");
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"first native read poll")
            );
            assert_eq!(&*reads.borrow(), &[3]);
            assert!(
                probe
                    .token
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .is_cancellation_requested()
            );
            assert!(probe.completed.get());
            assert_eq!(probe.drops.get(), 1);
            assert!(!handle.initiate_drain().unwrap());
            assert_eq!(wire.borrow().close, FixtureClose::Reset);
            assert!(streams.borrow().is_empty());
        })
    });
}

#[test]
fn response_header_priorities_preserve_wire_order_and_paused_cleanup() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: 0,
                    }],
                },
            ));
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Get).unwrap();
            request.set_url(b"/priority", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            for urgency in [6, 7, 1, 1] {
                let stream = peer.create_stream();
                request
                    .set_http_priority(HttpPriority::from_urgency_and_incremental(urgency, true));
                input.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Headers {
                        stream,
                        message: &request,
                        end_stream: true,
                        extra_headers: None,
                    },
                ));
            }
            let credit = eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: 1,
                    }],
                },
            );
            let mut reset = eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(7),
                    code: H2ErrorCode::CANCEL,
                },
            );
            reset.extend(eligibility_frame(
                &mut peer,
                EgressFrame::PingRequest { data: Some(97) },
            ));
            let send_credit = Arc::new(tokio::sync::Notify::new());
            let send_reset = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&send_credit), credit),
                FixtureRead::WaitData(Arc::clone(&send_reset), reset),
            ]);
            let owner_gate = Rc::new(OwnerPollGate {
                ready: Cell::new(true),
                ..OwnerPollGate::default()
            });
            let (mut application, probes) = Application::new(vec![Reply::Body(1); 4]);
            application.owner_poll_gate = Some(Rc::clone(&owner_gate));
            application.response_priorities = vec![
                Some(HttpPriority::new(2, false, 20, false, 0)),
                Some(HttpPriority::new(2, false, 10, false, 0)),
                None,
                Some(HttpPriority::new(0, true, 0, true, 0)),
            ];
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let decode = || {
                let bytes = wire.borrow().writes.concat();
                let mut codec = Http2Codec::new(TransportDirection::Upstream);
                for id in [1, 3, 5, 7] {
                    assert_eq!(codec.create_stream(), StreamId::new(id));
                }
                let mut decoded = CompleteCreditWire::default();
                let mut output = BufQueue::new(4096);
                let result = codec.on_ingress(&queue(&bytes), &mut decoded, &mut output);
                assert_eq!(result.consumed, bytes.len());
                assert!(result.egress_results.iter().all(Result::is_ok));
                decoded
            };
            exercise(
                session,
                &handle,
                &[Arc::clone(&send_credit), Arc::clone(&send_reset)],
                async {
                    complete_credit_wait(|| {
                        let probes = probes.borrow();
                        probes.len() == 4
                            && probes.iter().all(|probe| probe.produced_bytes.get() == 1)
                            && decode().frames.header_streams.len() == 4
                    })
                    .await;
                    let probes = probes.borrow().clone();
                    assert_eq!(decode().frames.body_bytes, 0);
                    assert!(probes.iter().all(|probe| probe.completed.get() == 0));
                    let ready_owner = owner_gate.wakers.borrow()[0].as_ref().unwrap().clone();
                    send_credit.notify_one();
                    ready_owner.wake_by_ref();
                    complete_credit_wait(|| {
                        probes[..3].iter().all(|probe| probe.completed.get() == 1)
                    })
                    .await;
                    let expected = [StreamId::new(5), StreamId::new(3), StreamId::new(1)];
                    let sent = decode();
                    assert_eq!(sent.frames.body_streams, expected);
                    assert_eq!(sent.frames.body_data, b"rrr");
                    assert_eq!(sent.completed, expected);
                    assert_eq!(probes[3].response_complete.get(), 0);
                    assert_eq!(probes[3].completed.get(), 0);
                    assert_eq!(probes[3].dropped.get(), 0);
                    send_reset.notify_one();
                    complete_credit_wait(|| {
                        probes[3].dropped.get() == 1 && decode().frames.ping_replies == [97]
                    })
                    .await;
                    for probe in &probes[..3] {
                        assert_eq!(probe.response_complete.get(), 1);
                        assert_eq!(probe.completed.get(), 1);
                        assert_eq!(probe.dropped.get(), 1);
                        assert!(probe.cancelled.borrow().is_empty());
                    }
                    assert_eq!(probes[3].completed.get(), 0);
                    assert_eq!(
                        probes[3].cancelled.borrow().as_slice(),
                        &[(HttpErrorCode::CANCEL, true)]
                    );
                    let sent = decode();
                    assert_eq!(sent.frames.body_streams, expected);
                    assert_eq!(sent.completed, expected);
                    assert!(sent.frames.resets.is_empty());
                    assert!(sent.frames.errors.is_empty());
                },
            )
            .await;
        })
    });
}

#[test]
fn reentrant_parent_wake_preserves_queued_owners_after_panic() {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    struct ParentWakePanic(Arc<()>);

    struct ReentrantWake {
        delegate: Waker,
        armed: Arc<AtomicBool>,
        notifications: Arc<AtomicUsize>,
        second: Arc<Mutex<Option<Waker>>>,
        marker: Arc<()>,
    }

    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.notifications.fetch_add(1, Ordering::SeqCst);
            if self.armed.swap(false, Ordering::SeqCst) {
                let second = self.second.lock().unwrap().take();
                if let Some(second) = second {
                    second.wake_by_ref();
                }
                panic_any(ParentWakePanic(Arc::clone(&self.marker)));
            }
            self.delegate.wake_by_ref();
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            let release_eof = Arc::new(tokio::sync::Notify::new());
            let gate = Rc::new(OwnerPollGate::default());
            let (mut peer, input) = ready_owner_requests(2);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::WaitDataEof(Arc::clone(&release_eof), Vec::new()),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(1); 2]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let armed = Arc::new(AtomicBool::new(false));
            let notifications = Arc::new(AtomicUsize::new(0));
            let second = Arc::new(Mutex::new(None));
            let marker = Arc::new(());
            let scenario = async {
                let outcome =
                    eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                        complete_credit_wait(|| {
                            let state = state.borrow();
                            let wakers = gate.wakers.borrow();
                            state.reads.is_empty()
                                && !state.writes.is_empty()
                                && wakers.len() == 2
                                && wakers.iter().all(Option::is_some)
                        })
                        .await;
                        let first = {
                            let wakers = gate.wakers.borrow();
                            *second.lock().unwrap() = Some(wakers[1].as_ref().unwrap().clone());
                            wakers[0].as_ref().unwrap().clone()
                        };
                        gate.ready.set(true);
                        let before = notifications.load(Ordering::SeqCst);
                        armed.store(true, Ordering::SeqCst);
                        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            first.wake_by_ref();
                        }))
                        .expect_err("the parent notification panic escapes the external wake");
                        assert!(Arc::ptr_eq(
                            &panic.downcast_ref::<ParentWakePanic>().unwrap().0,
                            &marker,
                        ));
                        assert!(!armed.load(Ordering::SeqCst));
                        assert_eq!(
                            notifications.load(Ordering::SeqCst) - before,
                            2,
                            "the second owner must enqueue and notify during parent reentry"
                        );
                        drop(panic);
                        complete_credit_wait(|| {
                            probes
                                .borrow()
                                .iter()
                                .all(|probe| probe.completed.get() == 1 && probe.dropped.get() == 1)
                        })
                        .await;
                        let probes = probes.borrow();
                        assert_eq!(probes.len(), 2);
                        for probe in probes.iter() {
                            assert_eq!(probe.requested.get(), 1);
                            assert_eq!(probe.response_complete.get(), 1);
                            assert_eq!(probe.produced_bytes.get(), 1);
                            assert!(probe.cancelled.borrow().is_empty());
                        }
                    }))
                    .await;
                armed.store(false, Ordering::SeqCst);
                let remaining_waker = second.lock().unwrap().take();
                drop(remaining_waker);
                gate.ready.set(true);
                gate.wakers
                    .borrow()
                    .iter()
                    .flatten()
                    .for_each(Waker::wake_by_ref);
                release_eof.notify_one();
                if !matches!(&outcome, Ok(Ok(()))) {
                    handle.cancel();
                }
                outcome
            };
            let mut run = Box::pin(session.run());
            let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(
                    poll_fn(|context| {
                        let waker = Waker::from(Arc::new(ReentrantWake {
                            delegate: context.waker().clone(),
                            armed: Arc::clone(&armed),
                            notifications: Arc::clone(&notifications),
                            second: Arc::clone(&second),
                            marker: Arc::clone(&marker),
                        }));
                        run.as_mut().poll(&mut Context::from_waker(&waker))
                    }),
                    scenario,
                )
            })
            .await
            .expect("both queued owners and native I/O join after parent wake reentry");
            drop(run);
            gate.wakers.borrow_mut().clear();
            match outcome {
                Ok(result) => result.expect("queued owners progress without a fallback wake"),
                Err(payload) => resume_unwind(payload),
            }
            result.unwrap();
            let wire = state.borrow().writes.concat();
            let mut decoded = CompleteCreditWire::default();
            let mut output = BufQueue::new(1 << 20);
            let parsed = peer.on_ingress(&queue(&wire), &mut decoded, &mut output);
            assert_eq!(parsed.consumed, wire.len());
            assert!(parsed.egress_results.iter().all(Result::is_ok));
            assert_eq!(
                decoded.frames.header_statuses,
                [(StreamId::new(1), 200), (StreamId::new(3), 200)]
            );
            assert_eq!(
                decoded.frames.body_streams,
                [StreamId::new(1), StreamId::new(3)]
            );
            assert_eq!(decoded.frames.body_data, vec![b'r'; 2]);
            assert_eq!(decoded.completed, [StreamId::new(1), StreamId::new(3)]);
            assert!(decoded.frames.resets.is_empty());
            assert!(decoded.frames.errors.is_empty());
            assert!(state.borrow().transport_dropped);
        })
    });
}

#[test]
fn pending_headers_ignore_credit_notifications_without_blocking_control() {
    run_on_worker(|context| {
        Box::pin(async move {
            let gate = Rc::new(OwnerPollGate::default());
            let ping_gates: Vec<_> = (0..3)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect();
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut reads = vec![FixtureRead::Data(peer_incomplete_post_bytes(3 * 16_384))];
            for (ordinal, release) in ping_gates.iter().enumerate() {
                reads.push(FixtureRead::WaitData(
                    Arc::clone(release),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::PingRequest {
                            data: Some(40 + ordinal as u64),
                        },
                    ),
                ));
            }
            let (transport, state) = FixtureTransport::new(reads);
            let (mut application, probes) = Application::new(vec![Reply::Body(1)]);
            application.retain_request = true;
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(session, &handle, &ping_gates, async {
                complete_credit_wait(|| {
                    gate.polls.get() == 1
                        && probes.borrow()[0]
                            .ingress
                            .borrow()
                            .as_ref()
                            .unwrap()
                            .body_bytes_received()
                            == 3 * 16_384
                })
                .await;
                let probe = Rc::clone(&probes.borrow()[0]);
                let mut body = probe.held_body.borrow_mut().take().unwrap();
                for (ordinal, release) in ping_gates.iter().enumerate() {
                    let event = body.read_body_event(16_384).await.unwrap();
                    assert_eq!(event.as_body().unwrap().chain_length(), 16_384);
                    assert!(!event.eom());
                    release.notify_one();
                    complete_credit_wait(|| {
                        decode_server_frames(&state.borrow().writes.concat())
                            .ping_replies
                            .len()
                            == ordinal + 1
                    })
                    .await;
                    assert_eq!(gate.polls.get(), 1);
                    assert_eq!(probe.body_polls.get(), 0);
                }
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.ping_replies, [40, 41, 42]);
                assert!(
                    decoded
                        .window_updates
                        .contains(&(StreamId::UNSPECIFIED, 32_768))
                );
                assert!(decoded.header_statuses.is_empty());
                assert!(decoded.errors.is_empty());
                drop(body);
                handle.cancel();
                complete_credit_wait(|| probe.dropped.get() == 1).await;
                assert_eq!(gate.polls.get(), 1);
                assert_eq!(probe.response_complete.get(), 0);
                assert_eq!(probe.completed.get(), 0);
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CORO_CANCELLED, false)]
                );
            })
            .await;
            assert!(state.borrow().transport_dropped);
        })
    });
}

#[test]
fn pending_headers_preserve_coalesced_and_self_wakes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let gate = Rc::new(OwnerPollGate::default());
            let input = peer_request_bytes(HttpMethod::Post, b"x", None, PeerControl::None);
            let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
            let (mut application, probes) = Application::new(vec![Reply::Body(2)]);
            application.retain_request = true;
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(session, &handle, &[], async {
                complete_credit_wait(|| {
                    gate.polls.get() == 1
                        && probes.borrow()[0]
                            .ingress
                            .borrow()
                            .as_ref()
                            .unwrap()
                            .body_bytes_received()
                            == 1
                })
                .await;
                let probe = Rc::clone(&probes.borrow()[0]);
                let mut body = probe.held_body.borrow_mut().take().unwrap();
                let event = body.read_body_event(1).await.unwrap();
                assert_eq!(event.as_body().unwrap().chain_length(), 1);
                assert!(event.eom());
                // The read queued credit/completion before this future wake, with no yield.
                gate.self_wake_once.set(true);
                gate.release();
                drop(body);
                complete_credit_wait(|| probe.completed.get() == 1 && probe.dropped.get() == 1)
                    .await;
                assert_eq!(gate.polls.get(), 3);
                assert_eq!(probe.response_complete.get(), 1);
                assert_eq!(probe.produced_bytes.get(), 2);
                assert!(probe.cancelled.borrow().is_empty());
                let decoded = complete_credit_wire(false, &state);
                assert_eq!(decoded.frames.header_statuses, [(StreamId::new(1), 200)]);
                assert_eq!(decoded.frames.body_data, b"rr");
                assert_eq!(decoded.completed, [StreamId::new(1)]);
                assert!(decoded.frames.errors.is_empty());
            })
            .await;
        })
    });
}

#[test]
fn informational_headers_continue_without_an_external_wake() {
    run_on_worker(|context| {
        Box::pin(async move {
            let gate = Rc::new(OwnerPollGate {
                ready: Cell::new(true),
                informational: Cell::new(true),
                ..OwnerPollGate::default()
            });
            let (_, input) = ready_owner_requests(1);
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (mut application, probes) = Application::new(vec![Reply::Body(2)]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            application.body_chunk = 1;
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .expect("informational headers and ready body events continue")
                .unwrap();
            assert_eq!(gate.polls.get(), 2);
            let probe = Rc::clone(&probes.borrow()[0]);
            assert_eq!(probe.body_polls.get(), 2);
            assert_eq!(probe.completed.get(), 1);
            assert_eq!(probe.dropped.get(), 1);
            assert!(probe.cancelled.borrow().is_empty());
            let decoded = complete_credit_wire(false, &state);
            assert_eq!(
                decoded.frames.header_statuses,
                [(StreamId::new(1), 103), (StreamId::new(1), 200)]
            );
            assert_eq!(decoded.frames.body_data, b"rr");
            assert_eq!(decoded.completed, [StreamId::new(1)]);
            assert!(decoded.frames.errors.is_empty());
        })
    });
}

#[test]
fn cross_thread_stream_wake_resumes_after_ready_queue_drain() {
    use std::thread;

    run_on_worker(|context| {
        Box::pin(async move {
            let release_eof = Arc::new(tokio::sync::Notify::new());
            let terminal = Arc::new(tokio::sync::Notify::new());
            let gate = Rc::new(OwnerPollGate::default());
            let (_, input) = ready_owner_requests(1);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&input)),
                FixtureRead::WaitDataEof(Arc::clone(&release_eof), Vec::new()),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(2)]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            application.empty_terminal = Some(Arc::clone(&terminal));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let run = tokio::task::spawn_local(session.run());
            let outcome =
                eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                    complete_credit_wait(|| {
                        let state = state.borrow();
                        let wakers = gate.wakers.borrow();
                        state.reads.is_empty()
                            && !state.writes.is_empty()
                            && wakers.len() == 1
                            && wakers[0].is_some()
                    })
                    .await;
                    let probe = Rc::clone(&probes.borrow()[0]);
                    assert_eq!(probe.body_polls.get(), 0);
                    let waker = gate.wakers.borrow()[0].as_ref().unwrap().clone();
                    gate.ready.set(true);
                    thread::spawn(move || waker.wake())
                        .join()
                        .expect("the external header wake returns");

                    complete_credit_wait(|| {
                        probe.produced_bytes.get() == 2 && probe.body_polls.get() >= 2
                    })
                    .await;
                    let decoded = complete_credit_wire(false, &state);
                    assert_eq!(decoded.frames.header_statuses, [(StreamId::new(1), 200)]);
                    assert_eq!(decoded.frames.body_data, vec![b'r'; 2]);
                    assert!(decoded.completed.is_empty());
                    assert_eq!(probe.response_complete.get(), 0);
                    assert_eq!(probe.completed.get(), 0);
                    assert_eq!(probe.dropped.get(), 0);
                    assert!(probe.cancelled.borrow().is_empty());

                    let terminal = Arc::clone(&terminal);
                    thread::spawn(move || terminal.notify_one())
                        .join()
                        .expect("the external terminal-body wake returns");
                    complete_credit_wait(|| probe.completed.get() == 1 && probe.dropped.get() == 1)
                        .await;
                    assert_eq!(probe.requested.get(), 1);
                    assert_eq!(probe.produced_bytes.get(), 2);
                    assert_eq!(probe.response_complete.get(), 1);
                    assert!(probe.cancelled.borrow().is_empty());
                    assert_eq!(
                        complete_credit_wire(false, &state).completed,
                        [StreamId::new(1)]
                    );
                }))
                .await;
            if !matches!(&outcome, Ok(Ok(()))) {
                gate.ready.set(true);
                gate.wakers
                    .borrow()
                    .iter()
                    .flatten()
                    .for_each(Waker::wake_by_ref);
                terminal.notify_one();
                handle.cancel();
            }
            release_eof.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(5), run)
                .await
                .expect("the independently scheduled driver joins its native I/O");
            gate.wakers.borrow_mut().clear();
            match outcome {
                Ok(result) => result.expect("both cross-thread stream wakes make progress"),
                Err(payload) => resume_unwind(payload),
            }
            result.expect("the driver does not panic").unwrap();
            let decoded = complete_credit_wire(false, &state);
            assert_eq!(decoded.frames.header_statuses, [(StreamId::new(1), 200)]);
            assert_eq!(decoded.frames.body_data, vec![b'r'; 2]);
            assert_eq!(decoded.completed, [StreamId::new(1)]);
            assert!(decoded.frames.resets.is_empty());
            assert!(decoded.frames.errors.is_empty());
            assert!(state.borrow().transport_dropped);
        })
    });
}

#[test]
#[ignore = "manual fixed-work dispatch mechanism measurement"]
fn measure_owned_h2_dispatch() {
    use std::env;
    use std::hint::black_box;

    const CONCURRENCY: usize = 16;
    const BODY_BYTES: usize = 1024;
    const WARMUP: usize = 64;
    const RESPONSE_DATE: &[u8] = b"Thu, 01 Jan 1970 00:00:00 GMT";

    #[derive(Default)]
    struct DispatchWire {
        decoded: CompleteCreditWire,
        date_headers: usize,
    }

    impl HttpCodecCallback for DispatchWire {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            if let CodecEvent::HeadersComplete { message, .. } = &event {
                assert_eq!(message.headers().number_of_values(HttpHeaderCode::Date), 1);
                assert_eq!(
                    message.headers().get_single_or_empty(HttpHeaderCode::Date),
                    RESPONSE_DATE,
                );
                self.date_headers += 1;
            }
            self.decoded.on_event(event);
        }
    }

    fn process_cpu_ns() -> u64 {
        let mut stamp = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes one timespec to an aligned, initialized
        // local whose exclusive borrow remains valid for the entire call.
        let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut stamp) };
        assert_eq!(result, 0, "the process CPU clock must be available");
        u64::try_from(stamp.tv_sec)
            .expect("nonnegative CPU seconds")
            .checked_mul(1_000_000_000)
            .and_then(|seconds| seconds.checked_add(u64::try_from(stamp.tv_nsec).ok()?))
            .expect("process CPU time fits in nanoseconds")
    }

    let validation_only = env::var_os("COMPONENT_BENCH_VALIDATE").is_some_and(|value| value == "1");
    assert!(
        !cfg!(debug_assertions) || validation_only,
        "use the optimized test image or explicit validation mode"
    );
    let arm = env::var("COMPONENT_BENCH_ARM").expect("COMPONENT_BENCH_ARM");
    assert!(matches!(arm.as_str(), "baseline" | "candidate"));
    let pair = env::var("COMPONENT_BENCH_PAIR")
        .expect("COMPONENT_BENCH_PAIR")
        .parse::<u32>()
        .expect("integer pair");
    assert!((1..=10).contains(&pair));
    let case = env::var("COMPONENT_BENCH_CASE").expect("COMPONENT_BENCH_CASE");
    let blocked = match case.as_str() {
        "ready_c16" => false,
        "blocked_c16" => true,
        _ => panic!("expected ready_c16 or blocked_c16"),
    };
    let operations = 2048;

    run_on_worker(move |context| {
        Box::pin(async move {
            let total = WARMUP + operations;
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Post).unwrap();
            request.set_url(b"/dispatch-component", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            request
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"1024");
            request.set_http_priority(HttpPriority::from_urgency_and_incremental(0, true));
            let payload = vec![b'q'; BODY_BYTES];
            let mut reads = Vec::new();
            let mut waves = Vec::new();
            for wave in 0..total / CONCURRENCY {
                let mut input = BufQueue::new(1 << 20);
                if wave == 0 {
                    peer.generate(
                        &mut input,
                        EgressFrame::ConnectionPreface,
                        &mut IgnoreCodecEvents,
                    )
                    .unwrap();
                    peer.generate_configured_settings(&mut input, &mut IgnoreCodecEvents)
                        .unwrap();
                }
                peer.generate(
                    &mut input,
                    EgressFrame::WindowUpdate {
                        stream: StreamId::UNSPECIFIED,
                        amount: (CONCURRENCY * BODY_BYTES) as u32,
                    },
                    &mut IgnoreCodecEvents,
                )
                .unwrap();
                if blocked {
                    peer.generate(
                        &mut input,
                        EgressFrame::Settings {
                            settings: &[CodecSetting {
                                identifier: SettingsId::InitialWindowSize.as_u16(),
                                value: 0,
                            }],
                        },
                        &mut IgnoreCodecEvents,
                    )
                    .unwrap();
                }
                for _ in 0..CONCURRENCY {
                    let stream = peer.create_stream();
                    peer.generate(
                        &mut input,
                        EgressFrame::Headers {
                            stream,
                            message: &request,
                            end_stream: false,
                            extra_headers: None,
                        },
                        &mut IgnoreCodecEvents,
                    )
                    .unwrap();
                    peer.generate(
                        &mut input,
                        EgressFrame::Body {
                            stream,
                            body: queue(&payload),
                            padding: None,
                            end_stream: true,
                        },
                        &mut IgnoreCodecEvents,
                    )
                    .unwrap();
                }
                let admit = Arc::new(tokio::sync::Notify::new());
                reads.push(FixtureRead::WaitSegmentedData(
                    Arc::clone(&admit),
                    black_box(input),
                ));
                let release = blocked.then(|| {
                    let release = Arc::new(tokio::sync::Notify::new());
                    let mut credit = BufQueue::new(4096);
                    peer.generate(
                        &mut credit,
                        EgressFrame::Settings {
                            settings: &[CodecSetting {
                                identifier: SettingsId::InitialWindowSize.as_u16(),
                                value: BODY_BYTES as u64,
                            }],
                        },
                        &mut IgnoreCodecEvents,
                    )
                    .unwrap();
                    reads.push(FixtureRead::WaitSegmentedData(Arc::clone(&release), credit));
                    release
                });
                waves.push((admit, release));
            }
            let eof = Arc::new(tokio::sync::Notify::new());
            reads.push(FixtureRead::WaitDataEof(Arc::clone(&eof), Vec::new()));
            let (transport, state) = FixtureTransport::new(black_box(reads));
            let owner_gate = Rc::new(OwnerPollGate {
                ready: Cell::new(true),
                ..OwnerPollGate::default()
            });
            let (mut application, probes) = Application::new(vec![Reply::Body(BODY_BYTES); total]);
            application.body_chunk = BODY_BYTES;
            application.response_date = Some(RESPONSE_DATE);
            application.owner_poll_gate = Some(Rc::clone(&owner_gate));
            let (session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let feeder = async {
                let mut start = None;
                for (wave, (admit, release)) in waves.iter().enumerate() {
                    let begin = wave * CONCURRENCY;
                    let end = begin + CONCURRENCY;
                    if begin == WARMUP {
                        let state = state.borrow();
                        let warmup_writes = state.writes.len();
                        let warmup_bytes = state.writes.iter().map(Vec::len).sum::<usize>();
                        start = Some((
                            Instant::now(),
                            process_cpu_ns(),
                            warmup_writes,
                            warmup_bytes,
                        ));
                    }
                    admit.notify_one();
                    if let Some(release) = release {
                        while {
                            let probes = probes.borrow();
                            probes.len() < end
                                || probes[begin..end]
                                    .iter()
                                    .any(|probe| probe.produced_bytes.get() != BODY_BYTES)
                        } {
                            tokio::task::yield_now().await;
                        }
                        let other_owner = owner_gate.wakers.borrow()[end - 1]
                            .as_ref()
                            .unwrap()
                            .clone();
                        release.notify_one();
                        other_owner.wake_by_ref();
                    }
                    while {
                        let probes = probes.borrow();
                        probes.len() < end
                            || probes[begin..end]
                                .iter()
                                .any(|probe| probe.completed.get() != 1 || probe.dropped.get() != 1)
                    } {
                        tokio::task::yield_now().await;
                    }
                    owner_gate.wakers.borrow_mut()[begin..end]
                        .iter_mut()
                        .for_each(|waker| drop(waker.take()));
                    probes.borrow()[begin..end].iter().for_each(|probe| {
                        drop(probe.ingress.borrow_mut().take());
                    });
                }
                eof.notify_one();
                start.expect("the measured waves were reached")
            };
            let (run, (start, cpu_start, warmup_writes, warmup_bytes)) =
                tokio::time::timeout(Duration::from_secs(30), async {
                    tokio::join!(session.run(), feeder)
                })
                .await
                .expect("fixed work and session teardown complete");
            let cpu_ns = process_cpu_ns().checked_sub(cpu_start).unwrap();
            let elapsed_ns = start.elapsed().as_nanos();
            let summary = black_box(run.expect("session completes successfully"));

            let probes = probes.borrow();
            assert_eq!(probes.len(), total);
            assert!(probes.iter().all(|probe| {
                probe.requested.get() == 1
                    && probe.request_bytes.get() == BODY_BYTES
                    && probe.produced_bytes.get() == BODY_BYTES
                    && probe.body_polls.get() == 1
                    && probe.response_complete.get() == 1
                    && probe.completed.get() == 1
                    && probe.dropped.get() == 1
                    && probe.cancelled.borrow().is_empty()
            }));
            let state = state.borrow();
            assert!(state.transport_dropped);
            assert_ne!(state.close, FixtureClose::Reset);
            let wire = state.writes.concat();
            let mut decoder = DispatchWire::default();
            let mut output = BufQueue::new(1 << 20);
            let mut input = BufQueue::copy_from_slice(&wire, wire.len()).unwrap();
            let mut consumed = 0;
            while !input.is_empty() {
                let parsed = peer.on_ingress(&input, &mut decoder, &mut output);
                assert!(parsed.consumed > 0, "the peer decoder must make progress");
                input.trim_start(parsed.consumed).unwrap();
                consumed += parsed.consumed;
                assert!(parsed.egress_results.iter().all(Result::is_ok));
            }
            assert_eq!(consumed, wire.len());
            let DispatchWire {
                decoded,
                date_headers,
            } = decoder;
            assert_eq!(date_headers, total);
            assert_eq!(decoded.frames.header_statuses.len(), total);
            assert!(
                decoded
                    .frames
                    .header_statuses
                    .iter()
                    .all(|(_, status)| *status == 200)
            );
            assert_eq!(decoded.frames.body_bytes, total * BODY_BYTES);
            assert!(decoded.frames.body_data.iter().all(|byte| *byte == b'r'));
            assert_eq!(decoded.frames.body_streams.len(), total);
            if blocked {
                for (wave, streams) in decoded
                    .frames
                    .body_streams
                    .as_chunks::<CONCURRENCY>()
                    .0
                    .iter()
                    .enumerate()
                {
                    let first = StreamId::new((2 * wave * CONCURRENCY + 1) as u64);
                    let woken = StreamId::new((2 * (wave + 1) * CONCURRENCY - 1) as u64);
                    assert_eq!(streams[0], first);
                    assert_ne!(streams[0], woken);
                }
            }
            assert_eq!(decoded.completed.len(), total);
            assert!(decoded.frames.resets.is_empty());
            assert!(decoded.frames.errors.is_empty());
            let checksum = black_box(wire.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
            }));
            let write_submissions = state.writes.len() - warmup_writes;
            let written_bytes = wire.len() - warmup_bytes;
            black_box(summary);
            println!(
                "COMPONENT_BENCH_JSON {{\"schema\":\"component-bench-v1\",\"event\":\"batch\",\"mechanism\":\"dispatch\",\"arm\":\"{arm}\",\"pair\":{pair},\"case\":\"{case}\",\"operations\":{operations},\"warmup_operations\":{WARMUP},\"elapsed_ns\":{elapsed_ns},\"cpu_ns\":{cpu_ns},\"cpu_clock\":\"process\",\"checksum\":{checksum},\"assertions_passed\":true,\"validation_only\":{validation_only},\"counts\":{{\"sessions\":1,\"lifetime_requests\":{total},\"completed_requests\":{total},\"dropped_exchanges\":{total},\"fixed_date_headers\":{date_headers},\"max_concurrency\":{CONCURRENCY},\"request_body_bytes\":{BODY_BYTES},\"response_body_bytes\":{BODY_BYTES},\"measured_write_submissions\":{write_submissions},\"measured_written_bytes\":{written_bytes}}}}}"
            );
        })
    });
}

#[derive(Default)]
pub(super) struct QueuedWriteProbe {
    pub(super) accepted: Vec<Vec<u8>>,
    held: VecDeque<BufQueue>,
    pub(super) released: usize,
    pub(super) retired: usize,
    blocked_nonempty: usize,
    pub(super) complete_inline: bool,
    acquired: bool,
    pub(super) dropped: usize,
    waker: Option<Waker>,
    /// Released writes complete with `TRANSPORT_WRITE_ERROR`.
    pub(super) fail_released: bool,
    /// A [`ReportingWriteQueue`]'s `start_write` reports saturation from then on.
    pub(super) saturate_on_write: bool,
    /// The write timeout the queue was acquired with; the fixture never times a write.
    pub(super) write_timeout: Duration,
}

pub(super) fn release_queued_writes(probe: &Rc<RefCell<QueuedWriteProbe>>, count: usize) {
    let (waker, retired) = {
        let mut probe = probe.borrow_mut();
        assert!(probe.released + count <= probe.accepted.len());
        probe.released += count;
        let retired: Vec<_> = probe.held.drain(..count).collect();
        (probe.waker.take(), retired)
    };
    drop(retired);
    if let Some(waker) = waker {
        waker.wake();
    }
}

pub(super) fn finish_queued_writes(probe: &Rc<RefCell<QueuedWriteProbe>>) {
    let count = {
        let mut probe = probe.borrow_mut();
        probe.complete_inline = true;
        probe.accepted.len() - probe.released
    };
    release_queued_writes(probe, count);
}

pub(super) struct QueuedFixtureTransport {
    inner: Rc<FixtureTransport>,
    pub(super) probe: Rc<RefCell<QueuedWriteProbe>>,
    /// `Some` selects a [`ReportingWriteQueue`] that answers `is_saturated` from the cell.
    saturation: Option<Rc<Cell<bool>>>,
    pub(super) saturation_queries: Rc<Cell<usize>>,
    panic_on_saturation_query: bool,
}

impl QueuedFixtureTransport {
    pub(super) fn new(reads: impl IntoIterator<Item = FixtureRead>) -> Rc<Self> {
        Self::reporting(reads, None, false)
    }

    /// `saturation` and `panic_on_query` configure the [`ReportingWriteQueue`]; `None`
    /// keeps the [`FixtureWriteQueue`] and its default answer.
    pub(super) fn reporting(
        reads: impl IntoIterator<Item = FixtureRead>,
        saturation: Option<Rc<Cell<bool>>>,
        panic_on_query: bool,
    ) -> Rc<Self> {
        let (inner, _) = FixtureTransport::new(reads);
        Rc::new(Self {
            inner,
            probe: Rc::new(RefCell::new(QueuedWriteProbe::default())),
            saturation,
            saturation_queries: Rc::default(),
            panic_on_saturation_query: panic_on_query,
        })
    }
}

pub(super) struct FixtureWriteQueue {
    probe: Rc<RefCell<QueuedWriteProbe>>,
    cancellation: CancellationToken,
}

/// Forwards to a [`FixtureWriteQueue`] and answers `is_saturated` from its cell, counting
/// queries; it can panic when queried.
struct ReportingWriteQueue {
    inner: FixtureWriteQueue,
    saturation: Rc<Cell<bool>>,
    queries: Rc<Cell<usize>>,
    panic_on_query: bool,
}

impl crate::uniplex::UniplexWriteQueue for ReportingWriteQueue {
    fn can_write(&self, bytes: usize) -> bool {
        self.inner.can_write(bytes)
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        if self.inner.probe.borrow().saturate_on_write {
            self.saturation.set(true);
        }
        self.inner.start_write(data)
    }

    fn poll_complete(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<crate::uniplex::UniplexWriteCompletion> {
        self.inner.poll_complete(context)
    }

    fn is_idle(&self) -> bool {
        self.inner.is_idle()
    }

    fn is_saturated(&self) -> bool {
        self.queries.set(self.queries.get() + 1);
        assert!(
            !self.panic_on_query,
            "this session must not query saturation"
        );
        self.saturation.get()
    }
}

impl crate::uniplex::UniplexWriteQueue for FixtureWriteQueue {
    fn can_write(&self, bytes: usize) -> bool {
        let mut probe = self.probe.borrow_mut();
        let pending = probe.accepted.len() - probe.retired;
        let admitted = pending < 2 && (bytes != 0 || pending == 0);
        if !admitted && bytes != 0 {
            probe.blocked_nonempty += 1;
        }
        admitted
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        assert!(self.can_write(data.chain_length()));
        let bytes = data
            .segments()
            .flat_map(|segment| segment.iter().copied())
            .collect();
        let complete_inline = {
            let mut probe = self.probe.borrow_mut();
            probe.accepted.push(bytes);
            probe.held.push_back(data);
            probe.complete_inline
        };
        if complete_inline {
            release_queued_writes(&self.probe, 1);
        }
        Ok(())
    }

    fn poll_complete(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<crate::uniplex::UniplexWriteCompletion> {
        let waker = context.waker().clone();
        let old_waker = self.probe.borrow_mut().waker.replace(waker);
        drop(old_waker);
        let requested_bytes = {
            let mut probe = self.probe.borrow_mut();
            if probe.retired == probe.released {
                return Poll::Pending;
            }
            let bytes = probe.accepted[probe.retired].len();
            probe.retired += 1;
            bytes
        };
        let result = if self.cancellation.is_cancellation_requested() {
            Err(TransportWriteError::new(
                HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"queued fixture cancelled"),
                requested_bytes,
            ))
        } else if self.probe.borrow().fail_released {
            Err(TransportWriteError::new(
                HttpError::with_message(
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    b"queued fixture write failed",
                ),
                0,
            ))
        } else {
            Ok(WriteInfo {
                bytes_written: requested_bytes,
            })
        };
        Poll::Ready(crate::uniplex::UniplexWriteCompletion {
            requested_bytes,
            result,
        })
    }

    fn is_idle(&self) -> bool {
        let probe = self.probe.borrow();
        probe.retired == probe.accepted.len()
    }
}

impl Drop for FixtureWriteQueue {
    fn drop(&mut self) {
        self.probe.borrow_mut().dropped += 1;
    }
}

impl UniplexTransport for QueuedFixtureTransport {
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn crate::uniplex::UniplexWriteQueue>>, HttpError> {
        self.probe.borrow_mut().write_timeout = timeout;
        assert!(!std::mem::replace(
            &mut self.probe.borrow_mut().acquired,
            true
        ));
        let queue = FixtureWriteQueue {
            probe: Rc::clone(&self.probe),
            cancellation,
        };
        Ok(Some(match &self.saturation {
            None => Box::new(queue),
            Some(saturation) => Box::new(ReportingWriteQueue {
                inner: queue,
                saturation: Rc::clone(saturation),
                queries: Rc::clone(&self.saturation_queries),
                panic_on_query: self.panic_on_saturation_query,
            }),
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
        self.inner
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        _context: &WorkerContext,
        _data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        panic!("the opt-in fixture must use its queued writer")
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

async fn exercise_queued<A: OwnedH2Application>(
    session: OwnedH2Session<A>,
    handle: &OwnedH2Handle,
    probe: &Rc<RefCell<QueuedWriteProbe>>,
    operation: impl Future<Output = ()>,
) {
    let scenario = async {
        let result =
            eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), operation)).await;
        handle.cancel();
        finish_queued_writes(probe);
        result
    };
    let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), scenario)
    })
    .await
    .expect("queued writes and native reads are joined");
    match outcome {
        Ok(result) => result.expect("queued write scenario made progress"),
        Err(payload) => resume_unwind(payload),
    }
    assert_eq!(result.unwrap_err().code(), HttpErrorCode::CORO_CANCELLED);
    let probe = probe.borrow();
    assert_eq!(probe.retired, probe.accepted.len());
    assert!(probe.held.is_empty());
    assert_eq!(probe.dropped, 1);
}

#[test]
fn queued_writes_bound_admission_and_retire_observers_and_eom_in_order() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let transport = QueuedFixtureTransport::new([FixtureRead::Data(input)]);
            let queue = Rc::clone(&transport.probe);
            let (mut application, probes) = Application::new(vec![Reply::Body(8192)]);
            application.body_chunk = 512;
            let events = Rc::new(ByteEventProbe::default());
            application.byte_events = Some(Rc::clone(&events));
            let mut config = options(context, transport);
            config.io.maximum_write_size = 1024;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            exercise_queued(session, &handle, &queue, async {
                complete_credit_wait(|| {
                    let queue = queue.borrow();
                    queue.accepted.len() == 2 && queue.blocked_nonempty > 0
                })
                .await;
                let probe = Rc::clone(&probes.borrow()[0]);
                assert_eq!(queue.borrow().retired, 0);
                assert!(!events.delivered.borrow().is_empty());
                assert!(
                    events
                        .delivered
                        .borrow()
                        .iter()
                        .all(|event| event.event_type == ByteEventType::TransportWrite)
                );
                assert_eq!(probe.completed.get(), 0);
                assert_eq!(probe.dropped.get(), 0);
                release_queued_writes(&queue, 1);
                complete_credit_wait(|| queue.borrow().accepted.len() == 3).await;
                assert_eq!(queue.borrow().retired, 1);
                assert_eq!(queue.borrow().released, 1);
                assert_eq!(probe.completed.get(), 0);
                release_queued_writes(&queue, 2);
                complete_credit_wait(|| queue.borrow().retired == 3).await;
                while probe.completed.get() == 0 {
                    complete_credit_wait(|| {
                        let queue = queue.borrow();
                        probe.completed.get() != 0 || queue.accepted.len() > queue.released
                    })
                    .await;
                    if probe.completed.get() != 0 {
                        break;
                    }
                    let expected = queue.borrow().released + 1;
                    release_queued_writes(&queue, 1);
                    complete_credit_wait(|| queue.borrow().retired >= expected).await;
                    let (accepted_offset, completed_offset) = {
                        let queue = queue.borrow();
                        let sum = |writes: &[Vec<u8>]| {
                            writes.iter().map(|bytes| bytes.len() as u64).sum::<u64>()
                        };
                        (sum(&queue.accepted), sum(&queue.accepted[..queue.retired]))
                    };
                    assert!(
                        events
                            .delivered
                            .borrow()
                            .iter()
                            .all(|event| event.transport_offset
                                <= match event.event_type {
                                    ByteEventType::TransportWrite => accepted_offset,
                                    ByteEventType::KernelWrite => completed_offset,
                                    _ => panic!("the native byte bridge has no TX/ACK observer"),
                                })
                    );
                    if !events
                        .delivered
                        .borrow()
                        .iter()
                        .any(|event| event.event_type == ByteEventType::KernelWrite && event.eom)
                    {
                        assert_eq!(probe.completed.get(), 0);
                    }
                }
                assert_eq!(probe.response_complete.get(), 1);
                assert_eq!(probe.completed.get(), 1);
                assert_eq!(probe.dropped.get(), 1);
                assert!(probe.cancelled.borrow().is_empty());
                assert!(events.cancelled.borrow().is_empty());
                let wire = queue.borrow().accepted.concat();
                assert_eq!(decode_server_frames(&wire).body_data, vec![b'r'; 8192]);
                let mut expected = Vec::new();
                let mut offset = 0;
                let mut body_offset = 0;
                while offset < wire.len() {
                    let length = (usize::from(wire[offset]) << 16)
                        | (usize::from(wire[offset + 1]) << 8)
                        | usize::from(wire[offset + 2]);
                    let data = wire[offset + 3] == FrameType::DATA.as_u8();
                    offset += FRAME_HEADER_SIZE + length;
                    if data {
                        body_offset += length as u64;
                        if probe.body_event_ends.borrow().contains(&body_offset) {
                            expected.push((body_offset, offset as u64));
                        }
                    }
                }
                assert_eq!(expected.len(), probe.body_event_ends.borrow().len());
                for kind in [ByteEventType::TransportWrite, ByteEventType::KernelWrite] {
                    let events = events.delivered.borrow();
                    let actual: Vec<_> = events
                        .iter()
                        .filter(|event| event.event_type == kind)
                        .map(|event| (event.body_offset, event.transport_offset))
                        .collect();
                    assert_eq!(actual, expected);
                    assert_eq!(
                        events
                            .iter()
                            .filter(|event| event.event_type == kind && event.eom)
                            .count(),
                        1
                    );
                }
            })
            .await;
        })
    });
}

#[test]
fn queued_write_cancellation_joins_both_accepted_writes_before_retiring_the_owner() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let transport = QueuedFixtureTransport::new([FixtureRead::Data(input)]);
            let queue = Rc::clone(&transport.probe);
            let wire = Rc::clone(&transport.inner.state);
            let (application, probes) = Application::new(vec![Reply::Body(8192)]);
            let mut config = options(context, transport);
            config.io.maximum_write_size = 1024;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            exercise_queued(session, &handle, &queue, async {
                complete_credit_wait(|| queue.borrow().accepted.len() == 2).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                handle.cancel();
                complete_credit_wait(|| probe.cancelled.borrow().len() == 1).await;
                assert_eq!(probe.dropped.get(), 0);
                assert_eq!(queue.borrow().dropped, 0);
                assert_eq!(queue.borrow().retired, 0);
                release_queued_writes(&queue, 1);
                complete_credit_wait(|| queue.borrow().retired == 1).await;
                assert_eq!(queue.borrow().accepted.len(), 2);
                assert_eq!(queue.borrow().dropped, 0);
                assert_eq!(probe.dropped.get(), 0);
                release_queued_writes(&queue, 1);
                complete_credit_wait(|| probe.dropped.get() == 1).await;
                assert_eq!(queue.borrow().retired, 2);
                assert_eq!(probe.completed.get(), 0);
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CORO_CANCELLED, false)]
                );
                assert_eq!(wire.borrow().close, FixtureClose::Reset);
            })
            .await;
        })
    });
}

struct ZeroBarrierApplication {
    inner: Application,
    first: Arc<tokio::sync::Notify>,
    empty: Arc<tokio::sync::Notify>,
    emitted: Rc<Cell<bool>>,
}

struct ZeroBarrierExchange {
    inner: Exchange,
    first: Option<LocalSourceFuture<()>>,
    empty: LocalSourceFuture<()>,
    emitted: Rc<Cell<bool>>,
}

impl OwnedH2Application for ZeroBarrierApplication {
    type Exchange = ZeroBarrierExchange;

    fn new_exchange(&self, stream: StreamId, ingress: IngressCompletion) -> Self::Exchange {
        let first = Arc::clone(&self.first);
        let empty = Arc::clone(&self.empty);
        ZeroBarrierExchange {
            inner: self.inner.new_exchange(stream, ingress),
            first: Some(Box::pin(async move { first.notified().await })),
            empty: Box::pin(async move { empty.notified().await }),
            emitted: Rc::clone(&self.emitted),
        }
    }
}

impl OwnedH2Exchange for ZeroBarrierExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        self.inner.request(head, body, eom);
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        self.inner.poll_headers(context)
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if let Some(first) = &mut self.first {
            if first.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            self.first.take();
            return self.inner.poll_body(context, maximum);
        }
        if self.emitted.get() || self.empty.as_mut().poll(context).is_pending() {
            return Poll::Pending;
        }
        self.emitted.set(true);
        let mut event = HttpBodyEvent::padding(0, false);
        event
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                None,
                ByteEventType::TransportWrite.bit(),
                self.inner.byte_events.as_ref().unwrap(),
            ));
        Poll::Ready(Ok(event))
    }

    fn cancel(&mut self, error: &HttpError, peer_reset: bool) {
        self.inner.cancel(error, peer_reset);
    }

    fn response_complete(&mut self) {
        self.inner.response_complete();
    }

    fn complete(self) {
        self.inner.complete();
    }
}

#[test]
fn queued_zero_byte_barrier_waits_for_all_prior_accepts_to_retire() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let transport =
                QueuedFixtureTransport::new([FixtureRead::SegmentedData(queue(&input))]);
            let queue = Rc::clone(&transport.probe);
            let (mut inner, _) = Application::new(vec![Reply::Body(2)]);
            inner.body_chunk = 1;
            inner.byte_events = Some(Rc::new(ByteEventProbe::default()));
            let first = Arc::new(tokio::sync::Notify::new());
            let empty = Arc::new(tokio::sync::Notify::new());
            let emitted = Rc::new(Cell::new(false));
            let application = ZeroBarrierApplication {
                inner,
                first: Arc::clone(&first),
                empty: Arc::clone(&empty),
                emitted: Rc::clone(&emitted),
            };
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise_queued(session, &handle, &queue, async {
                complete_credit_wait(|| queue.borrow().accepted.len() == 1).await;
                first.notify_one();
                complete_credit_wait(|| queue.borrow().accepted.len() == 2).await;
                assert_eq!(
                    decode_server_frames(&queue.borrow().accepted.concat()).body_data,
                    b"r"
                );
                empty.notify_one();
                complete_credit_wait(|| emitted.get()).await;
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(queue.borrow().accepted.len(), 2);
                release_queued_writes(&queue, 1);
                complete_credit_wait(|| queue.borrow().retired == 1).await;
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(queue.borrow().accepted.len(), 2);
                release_queued_writes(&queue, 1);
                complete_credit_wait(|| queue.borrow().accepted.len() == 3).await;
                assert_eq!(queue.borrow().retired, 2);
                assert!(queue.borrow().accepted[2].is_empty());
                release_queued_writes(&queue, 1);
                complete_credit_wait(|| queue.borrow().retired == 3).await;
            })
            .await;
        })
    });
}

fn padding_post_headers(peer: &mut Http2Codec) -> (StreamId, Vec<u8>) {
    let stream = peer.create_stream();
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Post).unwrap();
    request.set_url(b"/padded", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    let bytes = eligibility_frame(
        peer,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    );
    (stream, bytes)
}

fn padding_data(
    peer: &mut Http2Codec,
    stream: StreamId,
    length: usize,
    padding: Option<u8>,
) -> Vec<u8> {
    eligibility_frame(
        peer,
        EgressFrame::Body {
            stream,
            body: queue(&vec![b'p'; length]),
            padding,
            end_stream: false,
        },
    )
}

#[test]
fn owned_h2_padding_does_not_widen_the_connection_receive_window() {
    run_on_worker(|context| {
        Box::pin(async move {
            const PADDED_FRAMES: usize = 3;
            const PADDED_BODY: usize = 11_000;
            let send_overrun = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut first = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
            let mut output = BufQueue::new(1 << 20);
            peer.generate_configured_settings(&mut output, &mut IgnoreCodecEvents)
                .unwrap();
            first.extend_from_slice(&output.coalesce(output.chain_length()).unwrap());
            let (padded, headers) = padding_post_headers(&mut peer);
            first.extend(headers);
            for _ in 0..PADDED_FRAMES {
                first.extend(padding_data(&mut peer, padded, PADDED_BODY, Some(255)));
            }
            let (bulk, headers) = padding_post_headers(&mut peer);
            let mut overrun = headers;
            let mut remaining = 65_535usize;
            while remaining != 0 {
                let length = remaining.min(16_384);
                overrun.extend(padding_data(&mut peer, bulk, length, None));
                remaining -= length;
            }
            overrun.extend(padding_data(&mut peer, padded, 1, None));
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(first),
                FixtureRead::WaitData(Arc::clone(&send_overrun), overrun),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0); 2]);
            application.retain_request = true;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let acknowledged = (PADDED_BODY + 256) * PADDED_FRAMES;
            let operation = async {
                complete_credit_wait(|| {
                    probes
                        .borrow()
                        .first()
                        .is_some_and(|probe| probe.held_body.borrow().is_some())
                })
                .await;
                let probe = Rc::clone(&probes.borrow()[0]);
                let mut body = probe.held_body.borrow_mut().take().unwrap();
                let mut consumed = 0;
                while consumed < PADDED_BODY * PADDED_FRAMES {
                    let event = body.read_body_event(PADDED_BODY as u32).await.unwrap();
                    if let Some(bytes) = event.as_body() {
                        consumed += bytes.chain_length();
                    }
                }
                let connection_updates = || {
                    decode_server_frames(&state.borrow().writes.concat())
                        .window_updates
                        .into_iter()
                        .filter(|(stream, _)| stream.is_unspecified())
                        .collect::<Vec<_>>()
                };
                complete_credit_wait(|| !connection_updates().is_empty()).await;
                assert_eq!(
                    connection_updates(),
                    [(StreamId::UNSPECIFIED, acknowledged as u32)]
                );
                send_overrun.notify_one();
                complete_credit_wait(|| {
                    !decode_server_frames(&state.borrow().writes.concat())
                        .goaways
                        .is_empty()
                        || probe
                            .ingress
                            .borrow()
                            .as_ref()
                            .unwrap()
                            .body_bytes_received()
                            == (PADDED_BODY * PADDED_FRAMES + 1) as u64
                })
                .await;
                drop(body);
                handle.cancel();
            };
            let (_, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(
                    session.run(),
                    eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(5),
                        operation
                    ))
                )
            })
            .await
            .expect("the connection joins its transport operations");
            match outcome {
                Ok(result) => result.expect("the padded exchange made progress"),
                Err(payload) => resume_unwind(payload),
            }
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert!(
                decoded
                    .goaways
                    .iter()
                    .any(|(_, code)| *code == H2ErrorCode::FLOW_CONTROL_ERROR),
                "one byte beyond the granted connection window must be a FLOW_CONTROL_ERROR, \
                 goaways={:?}",
                decoded.goaways
            );
        })
    });
}

/// What the server wrote, the request body bytes each POST's application read, and how the run
/// ended, when the peer sends one POST per entry of `bodies`, each body in 16,384-byte DATA frames
/// without END_STREAM, and then, once every POST has read its body or been reset, ends the
/// POSTs, sends a GET and closes.
async fn receive_window_case(
    context: WorkerContext,
    configure: impl FnOnce(&mut Http2SessionConfig),
    bodies: &[usize],
) -> (DecodedServerFrames, Vec<usize>, Result<(), HttpErrorCode>) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut posts = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    posts.extend(transcript_settings(&mut peer));
    let mut streams = Vec::new();
    for &length in bodies {
        let (stream, headers) = padding_post_headers(&mut peer);
        posts.extend(headers);
        for offset in (0..length).step_by(16_384) {
            posts.extend(padding_data(
                &mut peer,
                stream,
                (length - offset).min(16_384),
                None,
            ));
        }
        streams.push(stream);
    }
    let mut finish: Vec<u8> = streams
        .iter()
        .flat_map(|&stream| {
            eligibility_frame(
                &mut peer,
                EgressFrame::Body {
                    stream,
                    body: queue(&[]),
                    padding: None,
                    end_stream: true,
                },
            )
        })
        .collect();
    let mut get = HttpMessage::new();
    get.set_method(HttpMethod::Get).unwrap();
    get.set_url(b"/after", false).unwrap();
    get.set_http_version(1, 1);
    get.headers_mut().set(HttpHeaderCode::Host, b"example.com");
    let get_stream = peer.create_stream();
    finish.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Headers {
            stream: get_stream,
            message: &get,
            end_stream: true,
            extra_headers: None,
        },
    ));
    let finish_gate = Arc::new(tokio::sync::Notify::new());
    let (transport, state) = FixtureTransport::new([
        FixtureRead::Data(posts),
        FixtureRead::WaitDataEof(Arc::clone(&finish_gate), finish),
    ]);
    let requests = bodies.len() + 1;
    let (application, probes) = Application::new(vec![Reply::Body(0); requests]);
    let mut config = options(context, transport);
    config.http2.connection_receive_window = 1 << 20;
    configure(&mut config.http2);
    let (session, _handle) = OwnedH2Session::new(application, config).unwrap();
    let script = async {
        let settled = || {
            let resets =
                decode_server_frames_with_requests(&state.borrow().writes.concat(), requests)
                    .resets;
            let probes = probes.borrow();
            probes.len() == bodies.len()
                && streams.iter().zip(bodies).zip(probes.iter()).all(
                    |((stream, &length), probe)| {
                        probe.request_bytes.get() == length
                            || resets.iter().any(|(reset, _)| reset == stream)
                    },
                )
        };
        let _ = tokio::time::timeout(TRANSCRIPT_WAIT, complete_credit_wait(settled)).await;
        settle_turns(16).await;
        finish_gate.notify_one();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), script)
    })
    .await
    .expect("the receive-window fixture finishes at EOF");
    let written = decode_server_frames_with_requests(&state.borrow().writes.concat(), requests);
    let consumed = probes
        .borrow()
        .iter()
        .map(|probe| probe.request_bytes.get())
        .collect();
    (
        written,
        consumed,
        run.map(drop).map_err(|error| error.code()),
    )
}

/// The `INITIAL_WINDOW_SIZE` values of the server's SETTINGS, in order.
fn advertised_windows(written: &DecodedServerFrames) -> Vec<u64> {
    written
        .settings
        .iter()
        .filter(|setting| setting.identifier == SettingsId::InitialWindowSize.as_u16())
        .map(|setting| setting.value)
        .collect()
}

fn sorted_statuses(written: &DecodedServerFrames) -> Vec<(StreamId, u16)> {
    let mut statuses = written.header_statuses.clone();
    statuses.sort_unstable();
    statuses
}

#[test]
fn preserved_stream_window_is_advertised_and_enforced() {
    let (written, consumed, run) = run_on_worker(|context| {
        Box::pin(receive_window_case(
            context,
            |http2| {
                http2.stream_receive_window = 262_144;
                http2.stream_receive_window_mode = StreamReceiveWindowMode::PreserveConfigured;
            },
            &[200_000, 262_145],
        ))
    });
    assert_eq!(
        advertised_windows(&written),
        [262_144],
        "the configured window is advertised once"
    );
    assert_eq!(
        written.settings.last().map(|setting| setting.identifier),
        Some(SettingsId::InitialWindowSize.as_u16()),
        "it follows the configured settings, as UniplexSession appends it"
    );
    assert_eq!(
        consumed[0], 200_000,
        "a body above the default window stays within the advertised one"
    );
    assert!(
        written
            .window_updates
            .iter()
            .any(|&(stream, amount)| stream == StreamId::new(1) && amount >= 131_072),
        "consuming more than half the window returns stream credit: {:?}",
        written.window_updates
    );
    assert_eq!(
        written.resets,
        [(StreamId::new(3), H2ErrorCode::FLOW_CONTROL_ERROR)],
        "one byte beyond the advertised window resets only that stream"
    );
    assert_eq!(
        sorted_statuses(&written),
        [(StreamId::new(1), 200), (StreamId::new(5), 200)],
        "the connection keeps serving after the reset"
    );
    assert!(written.goaways.is_empty());
    assert_eq!(run, Ok(()));
}

#[test]
fn mirrored_advertised_window_replaces_the_default_source_window() {
    let (written, consumed, run) = run_on_worker(|context| {
        Box::pin(receive_window_case(
            context,
            |http2| {
                http2.settings = vec![CodecSetting {
                    identifier: SettingsId::InitialWindowSize.as_u16(),
                    value: 131_072,
                }];
                assert_eq!(
                    http2.stream_receive_window_mode,
                    StreamReceiveWindowMode::MirrorAdvertisedSettings
                );
                assert_eq!(http2.stream_receive_window, 65_535);
            },
            &[131_072, 131_073],
        ))
    });
    assert_eq!(advertised_windows(&written), [131_072]);
    assert_eq!(
        consumed[0], 131_072,
        "the advertised window is the source window"
    );
    assert_eq!(
        written.resets,
        [(StreamId::new(3), H2ErrorCode::FLOW_CONTROL_ERROR)]
    );
    assert_eq!(
        sorted_statuses(&written),
        [(StreamId::new(1), 200), (StreamId::new(5), 200)]
    );
    assert_eq!(run, Ok(()));
}

#[test]
fn default_config_advertises_no_initial_window() {
    let (written, consumed, run) =
        run_on_worker(|context| Box::pin(receive_window_case(context, |_| {}, &[65_535, 65_536])));
    assert!(
        advertised_windows(&written).is_empty(),
        "the default 65,535-byte window needs no setting: {:?}",
        written.settings
    );
    assert_eq!(consumed[0], 65_535);
    assert_eq!(
        written.resets,
        [(StreamId::new(3), H2ErrorCode::FLOW_CONTROL_ERROR)]
    );
    assert_eq!(
        sorted_statuses(&written),
        [(StreamId::new(1), 200), (StreamId::new(5), 200)]
    );
    assert_eq!(run, Ok(()));
}

#[test]
fn conflicting_receive_windows_are_refused() {
    use StreamReceiveWindowMode::MirrorAdvertisedSettings as Mirror;
    use StreamReceiveWindowMode::PreserveConfigured as Preserve;

    run_on_worker(|context| {
        Box::pin(async move {
            // (mode, advertised INITIAL_WINDOW_SIZE, configured window)
            let refused = [
                (Preserve, Some(131_072), 65_535),
                (Preserve, Some(131_072), 262_144),
                (Mirror, Some(131_072), 262_144),
                (Mirror, Some(0), 65_535),
                (Mirror, Some(1 << 31), 65_535),
                (Mirror, Some(u64::from(u32::MAX) + 1), 65_535),
                (Mirror, None, 0),
                (Preserve, None, 1 << 31),
            ];
            for (mode, advertised, configured) in refused {
                let (transport, state) = FixtureTransport::new([]);
                let mut config = options(context.clone(), transport);
                config.http2.stream_receive_window_mode = mode;
                config.http2.stream_receive_window = configured;
                config.http2.settings = advertised
                    .map(|value| CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value,
                    })
                    .into_iter()
                    .collect();
                let (application, probes) = Application::new(vec![]);
                let Err(error) = OwnedH2Session::new(application, config) else {
                    panic!("{mode:?} {advertised:?} {configured} was accepted");
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                assert_eq!(
                    error.message(),
                    b"owned H2 requires matching advertised and source receive windows"
                );
                assert!(probes.borrow().is_empty());
                assert!(state.borrow().writes.is_empty());
            }
        })
    });
}

/// Waits up to `TRANSCRIPT_WAIT` for `condition`; on timeout the caller's assertions report the
/// difference instead.
async fn wait_until(condition: impl Fn() -> bool) {
    let _ = tokio::time::timeout(TRANSCRIPT_WAIT, complete_credit_wait(condition)).await;
}

/// How the peer finishes the header block of stream 3, which straddles the server's final
/// GOAWAY.
#[derive(Clone, Copy, Debug)]
enum StraddledBlock {
    /// A POST whose body never ends.
    OpenPost,
    /// A POST whose 10-byte body ends.
    EndedPost,
    /// A field section the codec rejects as malformed, which the peer then resets.
    MalformedThenReset,
}

/// Holds stream 1, a GET, sends the first half of stream 3's header block, drains the session,
/// and sends the rest of the block once the final GOAWAY is out, so the draining server refuses
/// stream 3 itself. Returns what the server wrote, its transcript digest, the refused streams it
/// still remembered after its last parse and the most it remembered at once, and how the run
/// ended.
async fn straddled_refusal(
    context: WorkerContext,
    block: StraddledBlock,
    rate_limits: Option<Http2RateLimitConfig>,
) -> (
    DecodedServerFrames,
    (usize, u64),
    (usize, usize),
    Result<(), HttpErrorCode>,
) {
    take_discarded_streams();
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut first = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    first.extend(transcript_settings(&mut peer));
    first.extend(eligibility_request(&mut peer, 3));
    let (stream, headers) = padding_post_headers(&mut peer);
    let fragment = match block {
        StraddledBlock::OpenPost | StraddledBlock::EndedPost => {
            raw_frames(&headers).remove(0).payload
        }
        // The last block on the connection, so the fresh encoder cannot desynchronize HPACK.
        StraddledBlock::MalformedThenReset => HpackEncoder::new(true).encode(&[
            HpackHeader::new(":method", "POST"),
            HpackHeader::new(":scheme", "https"),
            HpackHeader::new(":authority", "example.com"),
            HpackHeader::new(":path", "/malformed"),
            HpackHeader::new(":bogus", "x"),
        ]),
    };
    let (head, tail) = fragment.split_at(fragment.len() / 2);
    first.extend(raw_frame(
        FrameType::HEADERS,
        0,
        u32::try_from(stream.get()).unwrap(),
        head,
    ));
    let mut rest = raw_frame(
        FrameType::CONTINUATION,
        END_HEADERS,
        u32::try_from(stream.get()).unwrap(),
        tail,
    );
    rest.extend(eligibility_frame(
        &mut peer,
        match block {
            StraddledBlock::OpenPost => EgressFrame::Body {
                stream,
                body: queue(&[b'p'; 10]),
                padding: None,
                end_stream: false,
            },
            StraddledBlock::EndedPost => EgressFrame::Body {
                stream,
                body: queue(&[b'p'; 10]),
                padding: None,
                end_stream: true,
            },
            StraddledBlock::MalformedThenReset => EgressFrame::Reset {
                stream,
                code: H2ErrorCode::CANCEL,
            },
        },
    ));
    let send_rest = Arc::new(tokio::sync::Notify::new());
    let eof = Arc::new(tokio::sync::Notify::new());
    let (transport, state) = FixtureTransport::new([
        FixtureRead::Data(first),
        FixtureRead::WaitData(Arc::clone(&send_rest), rest),
        FixtureRead::WaitDataEof(Arc::clone(&eof), Vec::new()),
    ]);
    let gate = Rc::new(OwnerPollGate::default());
    let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
    application.owner_poll_gate = Some(Rc::clone(&gate));
    application.response_date = Some(TRANSCRIPT_DATE);
    let mut config = options(context, transport);
    if let Some(rate_limits) = rate_limits {
        config.http2.rate_limits = rate_limits;
    }
    let (session, handle) = OwnedH2Session::new(application, config).unwrap();
    let written = || decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
    let script = async {
        wait_until(|| gate.polls.get() > 0).await;
        assert!(handle.initiate_drain().unwrap());
        wait_until(|| written().goaways.len() == 2).await;
        send_rest.notify_one();
        wait_until(|| !written().resets.is_empty()).await;
        gate.release();
        wait_until(|| probes.borrow()[0].completed.get() == 1).await;
        eof.notify_one();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), script)
    })
    .await
    .expect("the straddled-refusal fixture finishes at EOF");
    (
        written(),
        written_transcript(&state).digest(),
        take_discarded_streams(),
        run.map(drop).map_err(|error| error.code()),
    )
}

#[test]
fn refused_stream_is_forgotten_after_its_header_block() {
    // (how stream 3's block ends, refused streams still remembered after the last parse)
    let cases = [
        (StraddledBlock::OpenPost, 0),
        (StraddledBlock::EndedPost, 0),
        // The block never completes, so the record stays and the codec error queued behind the
        // refusal stays ignored instead of resetting stream 3 again. Only a block that straddles
        // the final GOAWAY reaches this refusal, so a connection keeps at most one such record.
        (StraddledBlock::MalformedThenReset, 1),
    ];
    for (block, remembered) in cases {
        let (written, transcript, discarded, run) =
            run_on_worker(move |context| Box::pin(straddled_refusal(context, block, None)));
        assert_eq!(
            written.goaways,
            [
                (StreamId::new(0x7fff_ffff), H2ErrorCode::NO_ERROR),
                (StreamId::new(3), H2ErrorCode::NO_ERROR)
            ],
            "{block:?}: the final GOAWAY covers the stream whose block it interrupted"
        );
        assert_eq!(
            written.resets,
            [(StreamId::new(3), H2ErrorCode::REFUSED_STREAM)],
            "{block:?}: the draining server refuses stream 3 once"
        );
        assert!(written.errors.is_empty(), "{block:?}");
        assert_eq!(
            written.header_statuses,
            [(StreamId::new(1), 200)],
            "{block:?}"
        );
        assert_eq!(run, Ok(()), "{block:?}");
        assert_eq!(discarded, (remembered, 1), "{block:?}");
        assert_eq!(
            transcript,
            (6, 4_075_595_476_242_011_935),
            "{block:?}: transcript recorded with the driver that kept every refused stream \
             until END_STREAM"
        );
    }
}

#[test]
fn concurrency_refusals_leave_no_stream_record() {
    const REFUSED: usize = 5_000;
    const READS: usize = 2;
    const DATA: usize = 10;
    let (written, discarded, run) = run_on_worker(|context| {
        Box::pin(async move {
            take_discarded_streams();
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut first = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
            first.extend(transcript_settings(&mut peer));
            first.extend(eligibility_request(&mut peer, 3));
            // Each read carries 2,500 POSTs without END_STREAM, each with two DATA frames:
            // 50,000 bytes, within the connection's 65,535-byte window.
            let refusals: Vec<Vec<u8>> = (0..READS)
                .map(|_| {
                    (0..REFUSED / READS)
                        .flat_map(|_| {
                            let (stream, mut bytes) = padding_post_headers(&mut peer);
                            bytes.extend(padding_data(&mut peer, stream, DATA, None));
                            bytes.extend(padding_data(&mut peer, stream, DATA, None));
                            bytes
                        })
                        .collect()
                })
                .collect();
            let after = eligibility_request(&mut peer, 3);
            let send_after = Arc::new(tokio::sync::Notify::new());
            let reads = std::iter::once(FixtureRead::Data(first))
                .chain(refusals.into_iter().map(FixtureRead::Data))
                .chain([FixtureRead::WaitDataEof(Arc::clone(&send_after), after)]);
            let (transport, state) = FixtureTransport::new(reads);
            let gate = Rc::new(OwnerPollGate::default());
            let (mut application, probes) = Application::new(vec![Reply::Body(0); 2]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let mut config = options(context, transport);
            config.http2.settings.push(CodecSetting {
                identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                value: 1,
            });
            let (session, _handle) = OwnedH2Session::new(application, config).unwrap();
            let streams = REFUSED + 2;
            let written =
                || decode_server_frames_with_requests(&state.borrow().writes.concat(), streams);
            let script = async {
                wait_until(|| gate.polls.get() > 0 && written().resets.len() == REFUSED).await;
                gate.release();
                wait_until(|| probes.borrow()[0].completed.get() == 1).await;
                send_after.notify_one();
            };
            let (run, ()) = tokio::time::timeout(Duration::from_secs(20), async {
                tokio::join!(session.run(), script)
            })
            .await
            .expect("the concurrency-refusal fixture finishes at EOF");
            (
                written(),
                take_discarded_streams(),
                run.map(drop).map_err(|error| error.code()),
            )
        })
    });
    assert_eq!(
        written.resets,
        (0..REFUSED as u64)
            .map(|index| (StreamId::new(3 + 2 * index), H2ErrorCode::REFUSED_STREAM))
            .collect::<Vec<_>>(),
        "every POST beyond the limit is refused once"
    );
    assert_eq!(
        discarded,
        (0, 0),
        "the codec refuses streams beyond MAX_CONCURRENT_STREAMS before the driver sees them"
    );
    assert_eq!(
        written
            .window_updates
            .iter()
            .filter(|(stream, _)| stream.is_unspecified())
            .map(|&(_, amount)| amount as usize)
            .sum::<usize>(),
        REFUSED * 2 * DATA,
        "the refused streams' DATA returns all of its connection credit"
    );
    assert_eq!(
        written.header_statuses,
        [
            (StreamId::new(1), 200),
            (StreamId::new(3 + 2 * REFUSED as u64), 200)
        ],
        "the held stream and a later one are served"
    );
    assert!(written.errors.is_empty() && written.goaways.is_empty());
    assert_eq!(run, Ok(()));
}

#[test]
fn owned_h2_round_trip_runs_without_tokio_runtime() {
    for size in [0, 1, 1024] {
        crate::external_host_tests::run_on_host(move |context| {
            Box::pin(async move {
                assert!(tokio::runtime::Handle::try_current().is_err());
                let input = peer_request_bytes(
                    HttpMethod::Post,
                    &vec![b'q'; size],
                    None,
                    PeerControl::None,
                );
                let reads = input
                    .chunks(7)
                    .map(|chunk| FixtureRead::Data(chunk.to_vec()));
                let (transport, state) =
                    FixtureTransport::new(reads.chain([FixtureRead::DataEof(Vec::new())]));
                let (application, probes) = Application::new(vec![Reply::Body(size)]);
                let (session, _) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                session.run().await.unwrap();
                let probes = probes.borrow();
                assert_eq!(probes.len(), 1);
                let probe = &probes[0];
                assert_eq!(probe.requested.get(), 1);
                assert_eq!(probe.request_eom.get(), size == 0);
                assert_eq!(probe.request_bytes.get(), size);
                assert_eq!(probe.response_complete.get(), 1);
                assert_eq!(probe.completed.get(), 1);
                assert_eq!(probe.dropped.get(), 1);
                assert!(probe.cancelled.borrow().is_empty());
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
                assert_eq!(decoded.body_data, vec![b'r'; size]);
                assert!(decoded.resets.is_empty());
                assert!(decoded.errors.is_empty());
            })
        });
    }
}

/// Ends a scripted `WaitData` read on transport cancellation, as a parked socket read does. The
/// shared fixture's `WaitData` waits only for its notification.
struct CancellableWaitData(Rc<FixtureTransport>);

impl UniplexTransport for CancellableWaitData {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let parked = {
            let mut state = self.0.state.borrow_mut();
            match state.reads.front() {
                Some(FixtureRead::WaitData(..)) => state.reads.pop_front(),
                _ => None,
            }
        };
        let Some(FixtureRead::WaitData(ready, bytes)) = parked else {
            return self
                .0
                .read(context, maximum, allocation, timeout, cancellation);
        };
        let baton = CancellableBaton::with_host(context.host().clone());
        Box::pin(async move {
            match select(
                std::pin::pin!(ready.notified()),
                std::pin::pin!(baton.wait(cancellation)),
            )
            .await
            {
                Either::Left(_) => Ok(UniplexRead::new(queue(&bytes), false)),
                Either::Right(_) => Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"scripted H2 read cancelled",
                )),
            }
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

    fn peer_identity(&self, context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        self.0.peer_identity(context)
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

#[test]
fn owned_h2_scope_joins_on_worker_shutdown_with_cleanup_cancel() {
    let done = Arc::new(AtomicBool::new(false));
    let finished = Arc::clone(&done);
    crate::external_host_tests::run_on_host(move |context| {
        Box::pin(async move {
            let (_, preface) = ready_owner_requests(0);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(preface),
                FixtureRead::WaitData(Arc::new(tokio::sync::Notify::new()), Vec::new()),
            ]);
            let (application, probes) = Application::new(Vec::new());
            let transport = Rc::new(CancellableWaitData(transport));
            let (session, handle) =
                OwnedH2Session::new(application, options(context.clone(), transport)).unwrap();
            // Dropping the registration leaves the cleanup armed.
            let _ = context
                .register_cleanup(move || handle.cancel())
                .expect("the worker is open");
            context
                .register_scope(async move {
                    let _ = session.run().await;
                    finished.store(true, Ordering::SeqCst);
                })
                .and_then(|scope| scope.activate())
                .expect("the worker is open");
            // Return only once the session is parked on the never-notified read: the turn that
            // issued it left the session woken, so let that wake run before shutdown starts.
            complete_credit_wait(|| state.borrow().reads.is_empty()).await;
            context.host().yield_to_io().await;
            assert!(
                !state.borrow().writes.is_empty(),
                "the server SETTINGS was written"
            );
            assert!(probes.borrow().is_empty());
        })
    });
    assert!(
        done.load(Ordering::SeqCst),
        "worker shutdown joins the session scope after the cleanup cancels it"
    );
}

/// Runs one continuously ready 1024-chunk response body, longer than one turn budget, to
/// completion and checks the response.
async fn continuously_ready_body_round_trip(context: WorkerContext) {
    let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
    let (transport, state) = FixtureTransport::new([
        FixtureRead::SegmentedData(queue(&input)),
        FixtureRead::DataEof(Vec::new()),
    ]);
    let (mut application, probes) = Application::new(vec![Reply::Body(1024)]);
    application.body_chunk = 1;
    let mut config = options(context, transport);
    config.io.buffer_limit = 64 << 10;
    config.io.ingress_buffer_limit = 64 << 10;
    config.io.maximum_write_size = 64 << 10;
    config.io.maximum_read_size = 4096;
    config.io.read_allocation_size = 4096;
    let (session, _) = OwnedH2Session::new(application, config).unwrap();
    session.run().await.unwrap();
    let probes = probes.borrow();
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].body_polls.get(), 1024);
    assert_eq!(probes[0].completed.get(), 1);
    assert_eq!(probes[0].dropped.get(), 1);
    assert!(probes[0].cancelled.borrow().is_empty());
    let decoded = decode_server_frames(&state.borrow().writes.concat());
    assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
    assert_eq!(decoded.body_data, vec![b'r'; 1024]);
    assert!(decoded.resets.is_empty());
    assert!(decoded.errors.is_empty());
}

#[test]
fn budget_exhaustion_yields_to_host_io_without_tokio() {
    crate::external_host_tests::run_on_host_with(|host, context| {
        Box::pin(async move {
            continuously_ready_body_round_trip(context).await;
            assert!(
                host.yields() >= 1,
                "a ready source longer than one budget must yield to host I/O: yields={}",
                host.yields()
            );
        })
    });
}

#[test]
fn budget_exhaustion_self_wakes_when_host_yield_completes_immediately() {
    crate::external_host_tests::run_on_host_with(|host, context| {
        Box::pin(async move {
            host.set_yield_mode(YieldMode::Immediate);
            continuously_ready_body_round_trip(context).await;
            assert!(
                host.yields() >= 1,
                "the driver still asks the host to yield: yields={}",
                host.yields()
            );
        })
    });
}

#[test]
fn budget_exhaustion_holds_the_driver_until_the_host_yield_completes() {
    crate::external_host_tests::run_on_host_with(|host, context| {
        Box::pin(async move {
            host.set_yield_mode(YieldMode::Deferred);
            continuously_ready_body_round_trip(context).await;
            assert!(host.yields() >= 1, "the driver yields at budget exhaustion");
            assert_eq!(
                host.completed_yields(),
                host.yields(),
                "the driver resumes only through a completed yield, never re-yielding over one"
            );
        })
    });
}

/// 64-bit FNV-1a, for wire-identity goldens.
pub(super) fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Which way a recorded frame crossed the session's transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WireDirection {
    /// Scripted peer bytes the session read.
    Read,
    /// Bytes the session wrote.
    Written,
}

/// One HTTP/2 frame: its header fields and the FNV-1a digest of its payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WireFrame {
    pub(super) direction: WireDirection,
    pub(super) frame_type: u8,
    pub(super) flags: u8,
    pub(super) stream: u32,
    pub(super) length: u32,
    pub(super) payload: u64,
}

impl WireFrame {
    fn digest_bytes(&self) -> impl Iterator<Item = u8> {
        [self.direction as u8, self.frame_type, self.flags]
            .into_iter()
            .chain(self.stream.to_be_bytes())
            .chain(self.length.to_be_bytes())
            .chain(self.payload.to_be_bytes())
    }
}

/// The frames of one direction of an HTTP/2 connection, in wire order.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct WireTranscript {
    /// Whether the bytes start with the client connection preface.
    pub(super) preface: bool,
    pub(super) frames: Vec<WireFrame>,
}

impl WireTranscript {
    /// Splits `wire` into whole frames after an optional client connection
    /// preface.
    pub(super) fn new(direction: WireDirection, wire: &[u8]) -> Self {
        let preface = wire.starts_with(CONNECTION_PREFACE);
        let mut rest = if preface {
            &wire[CONNECTION_PREFACE.len()..]
        } else {
            wire
        };
        let mut frames = Vec::new();
        while !rest.is_empty() {
            assert!(
                rest.len() >= FRAME_HEADER_SIZE,
                "the wire ends inside a frame header"
            );
            let length = u32::from_be_bytes([0, rest[0], rest[1], rest[2]]);
            let end = FRAME_HEADER_SIZE + length as usize;
            assert!(rest.len() >= end, "the wire ends inside a frame payload");
            frames.push(WireFrame {
                direction,
                frame_type: rest[3],
                flags: rest[4],
                stream: u32::from_be_bytes([rest[5], rest[6], rest[7], rest[8]]) & 0x7fff_ffff,
                length,
                payload: fnv1a64(&rest[FRAME_HEADER_SIZE..end]),
            });
            rest = &rest[end..];
        }
        Self { preface, frames }
    }

    /// Frame count and FNV-1a digest over every recorded field.
    pub(super) fn digest(&self) -> (usize, u64) {
        let bytes: Vec<u8> = std::iter::once(u8::from(self.preface))
            .chain(self.frames.iter().flat_map(WireFrame::digest_bytes))
            .collect();
        (self.frames.len(), fnv1a64(&bytes))
    }

    /// Number of `frame_type` frames with every bit of `flags` set.
    pub(super) fn count(&self, frame_type: FrameType, flags: u8) -> usize {
        self.frames
            .iter()
            .filter(|frame| frame.frame_type == frame_type.as_u8() && frame.flags & flags == flags)
            .count()
    }
}

const TRANSCRIPT_BODY: usize = 128;
const TRANSCRIPT_PADDING: u8 = 200;
const TRANSCRIPT_PING: u64 = 0x0102_0304_0506_0708;
const TRANSCRIPT_DATE: &[u8] = b"Thu, 01 Jan 1970 00:00:00 GMT";
const TRANSCRIPT_WAIT: Duration = Duration::from_secs(5);
const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const PADDED: u8 = 0x8;

pub(super) fn written_transcript(state: &RefCell<FixtureTransportState>) -> WireTranscript {
    WireTranscript::new(WireDirection::Written, &state.borrow().writes.concat())
}

/// Waits for a written-wire condition; on timeout the caller's transcript
/// assertion reports the difference instead.
async fn wait_for_written(
    state: &RefCell<FixtureTransportState>,
    condition: impl Fn(&WireTranscript) -> bool,
) {
    let _ = tokio::time::timeout(
        TRANSCRIPT_WAIT,
        complete_credit_wait(|| condition(&written_transcript(state))),
    )
    .await;
}

fn transcript_settings(peer: &mut Http2Codec) -> Vec<u8> {
    let mut output = BufQueue::new(1 << 20);
    peer.generate_configured_settings(&mut output, &mut IgnoreCodecEvents)
        .unwrap();
    output.coalesce(output.chain_length()).unwrap().to_vec()
}

fn transcript_data(peer: &mut Http2Codec, stream: StreamId, byte: u8, padded: bool) -> Vec<u8> {
    eligibility_frame(
        peer,
        EgressFrame::Body {
            stream,
            body: queue(&[byte; TRANSCRIPT_BODY]),
            padding: padded.then_some(TRANSCRIPT_PADDING),
            end_stream: true,
        },
    )
}

/// Client bytes: preface, SETTINGS and three POSTs with 128-byte bodies (the
/// second DATA padded by 200 bytes); then a PING; then a GOAWAY.
fn downstream_transcript_input() -> [Vec<u8>; 3] {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut requests = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    requests.extend(transcript_settings(&mut peer));
    requests.extend((0..3).flat_map(|index| {
        let stream = peer.create_stream();
        let mut request = HttpMessage::new();
        request.set_method(HttpMethod::Post).unwrap();
        request
            .set_url(format!("/transcript/{index}").as_bytes(), false)
            .unwrap();
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        request
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"128");
        let headers = eligibility_frame(
            &mut peer,
            EgressFrame::Headers {
                stream,
                message: &request,
                end_stream: false,
                extra_headers: None,
            },
        );
        [
            headers,
            transcript_data(&mut peer, stream, b'q', index == 1),
        ]
        .concat()
    }));
    let ping = eligibility_frame(
        &mut peer,
        EgressFrame::PingRequest {
            data: Some(TRANSCRIPT_PING),
        },
    );
    let goaway = eligibility_frame(
        &mut peer,
        EgressFrame::Goaway {
            last_stream: StreamId::new(0),
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        },
    );
    [requests, ping, goaway]
}

/// The `index`th of three 200 responses with 128-byte bodies, on stream
/// `1 + 2 * index`; the second DATA is padded by 200 bytes.
fn transcript_response(peer: &mut Http2Codec, index: u64) -> Vec<u8> {
    let stream = StreamId::new(1 + 2 * index);
    let mut response = HttpMessage::new();
    response.set_status_code(200).unwrap();
    response.set_http_version(1, 1);
    response
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"128");
    response
        .headers_mut()
        .set(HttpHeaderCode::Date, TRANSCRIPT_DATE);
    let headers = eligibility_frame(
        peer,
        EgressFrame::Headers {
            stream,
            message: &response,
            end_stream: false,
            extra_headers: None,
        },
    );
    [headers, transcript_data(peer, stream, b'r', index == 1)].concat()
}

/// Server bytes: SETTINGS; then three 200 responses with 128-byte bodies (the
/// second DATA padded by 200 bytes); then a PING; then a GOAWAY.
fn upstream_transcript_input() -> [Vec<u8>; 4] {
    let [settings, first, second, third, ping, goaway] = sequential_upstream_input();
    [settings, [first, second, third].concat(), ping, goaway]
}

/// [`upstream_transcript_input`] with each response separate.
fn sequential_upstream_input() -> [Vec<u8>; 6] {
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let settings = transcript_settings(&mut peer);
    let [first, second, third] = [0, 1, 2].map(|index| transcript_response(&mut peer, index));
    let ping = eligibility_frame(
        &mut peer,
        EgressFrame::PingRequest {
            data: Some(TRANSCRIPT_PING),
        },
    );
    let goaway = eligibility_frame(
        &mut peer,
        EgressFrame::Goaway {
            last_stream: StreamId::new(5),
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        },
    );
    [settings, first, second, third, ping, goaway]
}

/// Read and written transcripts, and how the session's run ended.
type TranscriptRun = (WireTranscript, WireTranscript, Result<(), HttpErrorCode>);

/// Runs the downstream fixture.
async fn downstream_transcripts(context: WorkerContext) -> TranscriptRun {
    let [requests, ping, goaway] = downstream_transcript_input();
    let read = WireTranscript::new(
        WireDirection::Read,
        &[requests.as_slice(), &ping, &goaway].concat(),
    );
    let ping_gate = Arc::new(tokio::sync::Notify::new());
    let goaway_gate = Arc::new(tokio::sync::Notify::new());
    let (transport, state) = FixtureTransport::new([
        FixtureRead::Data(requests),
        FixtureRead::WaitData(Arc::clone(&ping_gate), ping),
        FixtureRead::WaitDataEof(Arc::clone(&goaway_gate), goaway),
    ]);
    let (mut application, _probes) = Application::new(vec![Reply::Body(TRANSCRIPT_BODY); 3]);
    application.response_date = Some(TRANSCRIPT_DATE);
    let (session, _handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    let script = async {
        wait_for_written(&state, |wire| {
            wire.count(FrameType::DATA, END_STREAM) + wire.count(FrameType::RST_STREAM, 0) == 3
        })
        .await;
        ping_gate.notify_one();
        wait_for_written(&state, |wire| wire.count(FrameType::PING, ACK) == 1).await;
        goaway_gate.notify_one();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), script)
    })
    .await
    .expect("the downstream transcript fixture finishes at EOF");
    let run = run.map(drop).map_err(|error| error.code());
    (read, written_transcript(&state), run)
}

/// Runs the upstream fixture.
async fn upstream_transcripts(context: WorkerContext) -> TranscriptRun {
    let [settings, responses, ping, goaway] = upstream_transcript_input();
    let read = WireTranscript::new(
        WireDirection::Read,
        &[settings.as_slice(), &responses, &ping, &goaway].concat(),
    );
    let responses_gate = Arc::new(tokio::sync::Notify::new());
    let ping_gate = Arc::new(tokio::sync::Notify::new());
    let goaway_gate = Arc::new(tokio::sync::Notify::new());
    let (transport, state) = FixtureTransport::new([
        FixtureRead::Data(settings),
        FixtureRead::WaitData(Arc::clone(&responses_gate), responses),
        FixtureRead::WaitData(Arc::clone(&ping_gate), ping),
        FixtureRead::WaitDataEof(Arc::clone(&goaway_gate), goaway),
    ]);
    let mut config = options(context, transport);
    config.io.drain_request_body_on_drop = false;
    config.http2.settings = vec![CodecSetting {
        identifier: SettingsId::EnablePush.as_u16(),
        value: 0,
    }];
    config.http2.double_goaway_drain = false;
    let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
    let submissions: Vec<_> = (0..3)
        .map(|index| {
            let mut head = committed_test_headers(format!("/transcript/{index}").as_bytes(), false);
            head.headers_mut()
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"128");
            let body = HttpFixedSource::make_fixed_body(queue(&[b'u'; TRANSCRIPT_BODY]));
            handle.reserve_request().unwrap().submit(
                head,
                HttpSourceHolder::new(Box::new(body)),
                CancellationToken::never(),
            )
        })
        .collect();
    let script = async {
        wait_for_written(&state, |wire| wire.count(FrameType::DATA, END_STREAM) == 3).await;
        responses_gate.notify_one();
        for submission in submissions {
            let Ok(response) = submission.await else {
                continue;
            };
            let (_, mut source, _request) = response.into_source_parts();
            if source.read_header_event().await.is_err() {
                continue;
            }
            while let Ok(event) = source.read_body_event(u32::MAX).await
                && !event.eom()
            {}
        }
        ping_gate.notify_one();
        wait_for_written(&state, |wire| wire.count(FrameType::PING, ACK) == 1).await;
        goaway_gate.notify_one();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), script)
    })
    .await
    .expect("the upstream transcript fixture finishes at EOF");
    let run = run.map(drop).map_err(|error| error.code());
    (read, written_transcript(&state), run)
}

/// Both fixtures carry three exchanges with 128-byte bodies, one padded DATA,
/// a PING with its ACK, and a peer GOAWAY.
pub(super) fn assert_transcript_scenario(read: &WireTranscript, written: &WireTranscript) {
    for transcript in [read, written] {
        assert_eq!(transcript.count(FrameType::HEADERS, 0), 3);
        assert_eq!(transcript.count(FrameType::DATA, END_STREAM), 3);
        assert_eq!(transcript.count(FrameType::RST_STREAM, 0), 0);
    }
    let padded = read.frames.iter().filter(|frame| {
        frame.frame_type == FrameType::DATA.as_u8()
            && frame.flags & PADDED != 0
            && frame.length as usize == 1 + TRANSCRIPT_BODY + usize::from(TRANSCRIPT_PADDING)
    });
    assert_eq!(padded.count(), 1);
    assert_eq!(read.count(FrameType::PING, 0), 1);
    assert_eq!(written.count(FrameType::PING, ACK), 1);
    assert_eq!(read.count(FrameType::GOAWAY, 0), 1);
}

#[test]
fn owned_h2_transcripts_are_unchanged() {
    let (read, written, run) = run_on_worker(|context| Box::pin(downstream_transcripts(context)));
    assert_eq!(
        [read.digest(), written.digest()],
        [
            (9, 11_231_806_118_693_533_887),
            (10, 7_227_848_528_293_173_899)
        ],
        "downstream transcripts recorded at 775385963702; read: {read:#?}; written: {written:#?}"
    );
    assert_transcript_scenario(&read, &written);
    assert_eq!(run, Ok(()), "the downstream session closes cleanly at EOF");
    let (read, written, run) = run_on_worker(|context| Box::pin(upstream_transcripts(context)));
    assert_eq!(
        [read.digest(), written.digest()],
        [
            (9, 3_188_096_049_290_309_849),
            (10, 2_096_194_806_515_092_526)
        ],
        "upstream transcripts recorded at 775385963702; read: {read:#?}; written: {written:#?}"
    );
    assert_transcript_scenario(&read, &written);
    assert_eq!(run, Ok(()), "the upstream session closes cleanly at EOF");
}

/// Receipts an [`InlineWriteQueue`] holds before `can_write` refuses more.
const INLINE_OUTSTANDING_WRITES: usize = 8;

/// What an [`InlineWriteQueue`] was given and still owes the session.
#[derive(Default)]
pub(super) struct InlineWriteState {
    /// The bytes of each accepted write, in order.
    pub(super) writes: Vec<Vec<u8>>,
    /// The requested size of each completed receipt not yet polled.
    receipts: VecDeque<usize>,
    /// The waker of the last `poll_complete` that found no receipt.
    writer: Option<Waker>,
    /// `is_saturated` calls, and whether one panics.
    saturation_queries: usize,
    panic_on_saturation_query: bool,
}

/// A capacity-only queued writer: it admits writes whatever their size
/// until `INLINE_OUTSTANDING_WRITES` receipts are held, completes each write
/// inside `start_write` as an inline physical write does, and wakes only the
/// waker of the last `poll_complete` that found no completed receipt.
struct InlineWriteQueue(Rc<RefCell<InlineWriteState>>);

impl crate::uniplex::UniplexWriteQueue for InlineWriteQueue {
    fn can_write(&self, _bytes: usize) -> bool {
        self.0.borrow().receipts.len() < INLINE_OUTSTANDING_WRITES
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        let bytes: Vec<u8> = data
            .segments()
            .flat_map(|segment| segment.iter().copied())
            .collect();
        let writer = {
            let mut state = self.0.borrow_mut();
            state.receipts.push_back(bytes.len());
            state.writes.push(bytes);
            state.writer.take()
        };
        if let Some(writer) = writer {
            writer.wake();
        }
        Ok(())
    }

    fn poll_complete(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<crate::uniplex::UniplexWriteCompletion> {
        let mut state = self.0.borrow_mut();
        if let Some(requested_bytes) = state.receipts.pop_front() {
            return Poll::Ready(crate::uniplex::UniplexWriteCompletion {
                requested_bytes,
                result: Ok(WriteInfo {
                    bytes_written: requested_bytes,
                }),
            });
        }
        let replaced = state.writer.replace(context.waker().clone());
        drop(state);
        drop(replaced);
        Poll::Pending
    }

    fn is_idle(&self) -> bool {
        self.0.borrow().receipts.is_empty()
    }

    /// The default answer, counted.
    fn is_saturated(&self) -> bool {
        let mut state = self.0.borrow_mut();
        state.saturation_queries += 1;
        assert!(
            !state.panic_on_saturation_query,
            "this session must not query saturation"
        );
        true
    }
}

/// Reads through a [`FixtureTransport`] and writes through an
/// [`InlineWriteQueue`].
pub(super) struct InlineWriteTransport {
    inner: Rc<FixtureTransport>,
    pub(super) state: Rc<RefCell<InlineWriteState>>,
}

impl InlineWriteTransport {
    pub(super) fn new(reads: impl IntoIterator<Item = FixtureRead>) -> Rc<Self> {
        let (inner, _) = FixtureTransport::new(reads);
        Rc::new(Self {
            inner,
            state: Rc::default(),
        })
    }
}

impl UniplexTransport for InlineWriteTransport {
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn crate::uniplex::UniplexWriteQueue>>, HttpError> {
        assert!(timeout.is_zero());
        Ok(Some(Box::new(InlineWriteQueue(Rc::clone(&self.state)))))
    }

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
        _context: &WorkerContext,
        _data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        panic!("the inline fixture writes only through its queue")
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

pub(super) fn inline_transcript(state: &RefCell<InlineWriteState>) -> WireTranscript {
    WireTranscript::new(WireDirection::Written, &state.borrow().writes.concat())
}

/// Waits for `condition` on the transcript `written` returns; on timeout the
/// caller's transcript assertion reports the difference instead.
async fn wait_for_transcript(
    written: impl Fn() -> WireTranscript,
    condition: impl Fn(&WireTranscript) -> bool,
) {
    let _ = tokio::time::timeout(
        TRANSCRIPT_WAIT,
        complete_credit_wait(|| condition(&written())),
    )
    .await;
}

/// Runs three upstream POSTs one after another over the inline queued writer,
/// each 128-byte request written before its response is read, then a PING once
/// the session is idle, then a GOAWAY and EOF. Also returns whether the
/// session published idleness before the PING.
async fn unarmed_upstream_transcripts(context: WorkerContext) -> (TranscriptRun, bool) {
    let (run, idle, _) = unarmed_upstream_transcripts_with(context, false).await;
    (run, idle)
}

/// [`unarmed_upstream_transcripts`], whose write queue panics if `panic_on_saturation_query`
/// and the session queries saturation. Also returns the number of queries.
pub(super) async fn unarmed_upstream_transcripts_with(
    context: WorkerContext,
    panic_on_saturation_query: bool,
) -> (TranscriptRun, bool, usize) {
    let [settings, first, second, third, ping, goaway] = sequential_upstream_input();
    let read = WireTranscript::new(
        WireDirection::Read,
        &[settings.as_slice(), &first, &second, &third, &ping, &goaway].concat(),
    );
    let response_gates: [_; 3] = std::array::from_fn(|_| Arc::new(tokio::sync::Notify::new()));
    let ping_gate = Arc::new(tokio::sync::Notify::new());
    let goaway_gate = Arc::new(tokio::sync::Notify::new());
    let transport = InlineWriteTransport::new([
        FixtureRead::Data(settings),
        FixtureRead::WaitData(Arc::clone(&response_gates[0]), first),
        FixtureRead::WaitData(Arc::clone(&response_gates[1]), second),
        FixtureRead::WaitData(Arc::clone(&response_gates[2]), third),
        FixtureRead::WaitData(Arc::clone(&ping_gate), ping),
        FixtureRead::WaitDataEof(Arc::clone(&goaway_gate), goaway),
    ]);
    let state = Rc::clone(&transport.state);
    state.borrow_mut().panic_on_saturation_query = panic_on_saturation_query;
    let written = || inline_transcript(&state);
    let mut config = options(context, transport);
    config.io.drain_request_body_on_drop = false;
    config.http2.settings = vec![CodecSetting {
        identifier: SettingsId::EnablePush.as_u16(),
        value: 0,
    }];
    config.http2.double_goaway_drain = false;
    let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
    let idle_before_ping = Cell::new(false);
    let script = async {
        wait_for_transcript(written, |wire| wire.count(FrameType::SETTINGS, ACK) == 1).await;
        for (index, gate) in response_gates.iter().enumerate() {
            let mut head = committed_test_headers(format!("/transcript/{index}").as_bytes(), false);
            head.headers_mut()
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"128");
            let body = HttpFixedSource::make_fixed_body(queue(&[b'u'; TRANSCRIPT_BODY]));
            let submission = handle.reserve_request().unwrap().submit(
                head,
                HttpSourceHolder::new(Box::new(body)),
                CancellationToken::never(),
            );
            wait_for_transcript(written, |wire| {
                wire.count(FrameType::DATA, END_STREAM) == index + 1
            })
            .await;
            gate.notify_one();
            let Ok(response) = submission.await else {
                continue;
            };
            let (_, mut source, _request) = response.into_source_parts();
            if source.read_header_event().await.is_err() {
                continue;
            }
            while let Ok(event) = source.read_body_event(u32::MAX).await
                && !event.eom()
            {}
        }
        idle_before_ping.set(
            tokio::time::timeout(TRANSCRIPT_WAIT, complete_credit_wait(|| handle.is_idle()))
                .await
                .is_ok(),
        );
        ping_gate.notify_one();
        wait_for_transcript(written, |wire| wire.count(FrameType::PING, ACK) == 1).await;
        goaway_gate.notify_one();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), script)
    })
    .await
    .expect("the unarmed upstream transcript fixture finishes at EOF");
    let run = run.map(drop).map_err(|error| error.code());
    let queries = state.borrow().saturation_queries;
    ((read, written(), run), idle_before_ping.get(), queries)
}

#[test]
fn upstream_transcript_is_unchanged_by_unarmed_writes() {
    let ((read, written, run), idle_before_ping) =
        run_on_worker(|context| Box::pin(unarmed_upstream_transcripts(context)));
    assert_eq!(
        [read.digest(), written.digest()],
        [
            (9, 3_188_096_049_290_309_849),
            (10, 18_252_902_388_575_904_042)
        ],
        "unarmed upstream transcripts recorded at da1228fe4a74; read: {read:#?}; written: {written:#?}"
    );
    assert_transcript_scenario(&read, &written);
    assert!(idle_before_ping, "the session is idle before the PING");
    assert_eq!(run, Ok(()), "the upstream session closes cleanly at EOF");
}

/// Polls its future only on the first poll and after the future woke its own
/// waker, as a separately scheduled task is polled, however often the enclosing
/// task is polled. Under `tokio::join!` with a script that yields, a session
/// would otherwise be polled at every yield, which hides lost wakes.
pub(super) struct OwnWakes<F> {
    inner: Pin<Box<F>>,
    wake: Arc<OwnWake>,
}

struct OwnWake {
    woken: AtomicBool,
    wakes: AtomicUsize,
    outer: std::sync::Mutex<Option<Waker>>,
}

/// How many times an [`OwnWakes`] future has woken its own waker.
#[derive(Clone)]
pub(super) struct WakeCount(Arc<OwnWake>);

impl WakeCount {
    pub(super) fn get(&self) -> usize {
        self.0.wakes.load(Ordering::SeqCst)
    }
}

impl std::task::Wake for OwnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.woken.store(true, Ordering::SeqCst);
        let outer = self.outer.lock().unwrap().clone();
        if let Some(outer) = outer {
            outer.wake();
        }
    }
}

impl<F: Future> OwnWakes<F> {
    pub(super) fn new(inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
            wake: Arc::new(OwnWake {
                woken: AtomicBool::new(true),
                wakes: AtomicUsize::new(0),
                outer: std::sync::Mutex::new(None),
            }),
        }
    }

    pub(super) fn wake_count(&self) -> WakeCount {
        WakeCount(Arc::clone(&self.wake))
    }
}

impl<F: Future> Future for OwnWakes<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let previous = self
            .wake
            .outer
            .lock()
            .unwrap()
            .replace(context.waker().clone());
        drop(previous);
        if !self.wake.woken.swap(false, Ordering::SeqCst) {
            return Poll::Pending;
        }
        let waker = Waker::from(Arc::clone(&self.wake));
        self.inner.as_mut().poll(&mut Context::from_waker(&waker))
    }
}

/// Opens the response gate of [`KernelWriteExchange`] when its body's kernel
/// write is reported.
#[derive(Default)]
struct KernelWriteGate {
    written: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl HttpByteEventCallback for KernelWriteGate {
    fn on_byte_event(&self, event: HttpByteEvent) {
        if event.event_type == ByteEventType::KernelWrite {
            self.written.set(true);
            let waker = self.waker.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {}
}

struct KernelWriteApplication(Rc<KernelWriteGate>);

/// Answers with one body byte that registers a kernel-write event, and ends the
/// response only once that event is reported. The request stays open.
struct KernelWriteExchange {
    gate: Rc<KernelWriteGate>,
    sent: bool,
    _request: Option<HttpSourceHolder>,
}

impl OwnedH2Application for KernelWriteApplication {
    type Exchange = KernelWriteExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> KernelWriteExchange {
        KernelWriteExchange {
            gate: Rc::clone(&self.0),
            sent: false,
            _request: None,
        }
    }
}

impl OwnedH2Exchange for KernelWriteExchange {
    fn request(&mut self, _head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        self._request = Some(body.into_source_holder());
    }

    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        let mut response = HttpMessage::new();
        response.set_status_code(200).unwrap();
        response.set_http_version(1, 1);
        Poll::Ready(Ok(HttpHeaderEvent::new(response, false).unwrap()))
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if !self.sent {
            self.sent = true;
            let mut event = HttpBodyEvent::body(queue(b"a"), false);
            event
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::KernelWrite.bit(),
                    &self.gate,
                ));
            return Poll::Ready(Ok(event));
        }
        if !self.gate.written.get() {
            *self.gate.waker.borrow_mut() = Some(context.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(HttpBodyEvent::body(queue(&[]), true)))
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

#[test]
fn kernel_write_waiter_arms_the_writer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
            input.extend(transcript_settings(&mut peer));
            let (_stream, headers) = padding_post_headers(&mut peer);
            input.extend(headers);
            let transport = InlineWriteTransport::new([FixtureRead::Data(input)]);
            let state = Rc::clone(&transport.state);
            let gate = Rc::new(KernelWriteGate::default());
            let (session, handle) = OwnedH2Session::new(
                KernelWriteApplication(Rc::clone(&gate)),
                options(context, transport),
            )
            .unwrap();
            let script = async {
                let ended = tokio::time::timeout(
                    TRANSCRIPT_WAIT,
                    complete_credit_wait(|| {
                        inline_transcript(&state).count(FrameType::DATA, END_STREAM) == 1
                    }),
                )
                .await
                .is_ok();
                handle.cancel();
                ended
            };
            let (run, ended) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(OwnWakes::new(session.run()), script)
            })
            .await
            .expect("the cancelled session joins its transport operations");
            assert!(
                ended,
                "while the request is still open, the response's kernel-write event arms the \
                 writer, so the response ends"
            );
            assert!(gate.written.get());
            assert_eq!(run.unwrap_err().code(), HttpErrorCode::CORO_CANCELLED);
        })
    });
}

/// Per-stream control for [`ScanApplication`]: each exchange holds its response
/// until the test releases it.
#[derive(Default)]
struct ScanControl {
    released: RefCell<std::collections::BTreeSet<StreamId>>,
    wakers: RefCell<std::collections::BTreeMap<StreamId, Waker>>,
    completed: Cell<usize>,
    cancelled: Cell<usize>,
}

impl ScanControl {
    fn release(&self, stream: StreamId) {
        self.released.borrow_mut().insert(stream);
        let waker = self.wakers.borrow_mut().remove(&stream);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct ScanApplication(Rc<ScanControl>);

/// Holds its request body and its response until released; a released exchange
/// stops reading its request with `NO_ERROR` and answers 200 with no body, so a
/// stream whose request has not ended retires without ending or resetting it.
struct ScanExchange {
    stream: StreamId,
    control: Rc<ScanControl>,
    request: Option<HttpSourceHolder>,
}

impl OwnedH2Application for ScanApplication {
    type Exchange = ScanExchange;

    fn new_exchange(&self, stream: StreamId, _ingress: IngressCompletion) -> ScanExchange {
        ScanExchange {
            stream,
            control: Rc::clone(&self.0),
            request: None,
        }
    }
}

impl OwnedH2Exchange for ScanExchange {
    fn request(&mut self, _head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        self.request = Some(body.into_source_holder());
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if !self.control.released.borrow().contains(&self.stream) {
            self.control
                .wakers
                .borrow_mut()
                .insert(self.stream, context.waker().clone());
            return Poll::Pending;
        }
        if let Some(mut request) = self.request.take() {
            request.stop_reading(Some(HttpErrorCode::NO_ERROR));
        }
        let mut response = HttpMessage::new();
        response.set_status_code(200).unwrap();
        response.set_http_version(1, 1);
        response
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"0");
        Poll::Ready(Ok(HttpHeaderEvent::new(response, true).unwrap()))
    }

    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        unreachable!("the response ends with its head")
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {
        self.control.cancelled.set(self.control.cancelled.get() + 1);
    }

    fn response_complete(&mut self) {}

    fn complete(self) {
        self.control.completed.set(self.control.completed.get() + 1);
    }
}

/// One step of `awaiting_ingress_counter_matches_a_scan`.
#[derive(Clone, Copy)]
enum ScanStep {
    /// Releases the client bytes behind this read gate.
    Ingress(usize),
    /// Releases this stream's response.
    Respond(StreamId),
}

/// A deterministic sequence of `operations` create (HEADERS without
/// END_STREAM), END_STREAM (empty DATA), client reset and response steps over at
/// most 64 open streams, then responses for the streams still open. Returns the
/// client bytes (the first is the connection preface, read at once; each later
/// one sits behind a gate), the steps, and the numbers of streams created,
/// ended, reset by the client and answered.
fn scan_steps(operations: usize) -> (Vec<Vec<u8>>, Vec<ScanStep>, [usize; 4]) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut preface = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    preface.extend(transcript_settings(&mut peer));
    let mut chunks = vec![preface];
    let mut steps = Vec::new();
    let mut open: Vec<(StreamId, bool)> = Vec::new();
    let [mut created, mut ended, mut reset, mut answered] = [0; 4];
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = |bound: usize| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) as usize % bound
    };
    for _ in 0..operations {
        let kind = next(4);
        let unended: Vec<usize> = (0..open.len()).filter(|index| !open[*index].1).collect();
        let create =
            open.is_empty() || (kind == 0 && open.len() < 64) || (kind == 1 && unended.is_empty());
        if create {
            let (stream, headers) = padding_post_headers(&mut peer);
            chunks.push(headers);
            steps.push(ScanStep::Ingress(chunks.len() - 1));
            open.push((stream, false));
            created += 1;
            continue;
        }
        match kind {
            1 => {
                let index = unended[next(unended.len())];
                open[index].1 = true;
                ended += 1;
                chunks.push(eligibility_frame(
                    &mut peer,
                    EgressFrame::Body {
                        stream: open[index].0,
                        body: queue(&[]),
                        padding: None,
                        end_stream: true,
                    },
                ));
                steps.push(ScanStep::Ingress(chunks.len() - 1));
            }
            2 => {
                let (stream, _) = open.swap_remove(next(open.len()));
                chunks.push(eligibility_frame(
                    &mut peer,
                    EgressFrame::Reset {
                        stream,
                        code: H2ErrorCode::CANCEL,
                    },
                ));
                steps.push(ScanStep::Ingress(chunks.len() - 1));
                reset += 1;
            }
            _ => {
                let (stream, _) = open.swap_remove(next(open.len()));
                steps.push(ScanStep::Respond(stream));
                answered += 1;
            }
        }
    }
    for (stream, _) in open {
        steps.push(ScanStep::Respond(stream));
        answered += 1;
    }
    (chunks, steps, [created, ended, reset, answered])
}

#[test]
fn awaiting_ingress_counter_matches_a_scan() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (chunks, steps, [created, ended, reset, answered]) = scan_steps(500);
            assert_eq!(created, reset + answered, "every created stream ends");
            assert_eq!(
                [created, ended, reset, answered],
                [211, 79, 99, 112],
                "the deterministic sequence covers every kind of step"
            );
            let mut chunks = chunks.into_iter();
            let mut reads = vec![FixtureRead::Data(chunks.next().unwrap())];
            let gates: Vec<_> = chunks
                .map(|chunk| {
                    let gate = Arc::new(tokio::sync::Notify::new());
                    reads.push(FixtureRead::WaitData(Arc::clone(&gate), chunk));
                    gate
                })
                .collect();
            let transport = InlineWriteTransport::new(reads);
            let control = Rc::new(ScanControl::default());
            let (session, handle) = OwnedH2Session::new(
                ScanApplication(Rc::clone(&control)),
                options(context, transport),
            )
            .unwrap();
            let script = async {
                for step in steps {
                    match step {
                        ScanStep::Ingress(chunk) => gates[chunk - 1].notify_one(),
                        ScanStep::Respond(stream) => control.release(stream),
                    }
                    for _ in 0..8 {
                        tokio::task::yield_now().await;
                    }
                }
                let retired = tokio::time::timeout(
                    TRANSCRIPT_WAIT,
                    complete_credit_wait(|| {
                        control.completed.get() + control.cancelled.get() == created
                    }),
                )
                .await
                .is_ok();
                handle.cancel();
                retired
            };
            let (run, retired) = tokio::time::timeout(Duration::from_secs(20), async {
                tokio::join!(OwnWakes::new(session.run()), script)
            })
            .await
            .expect("the cancelled session joins its transport operations");
            assert!(
                retired,
                "every stream retires: {} completed and {} cancelled of {created}",
                control.completed.get(),
                control.cancelled.get()
            );
            assert_eq!(
                (control.completed.get(), control.cancelled.get()),
                (answered, reset)
            );
            assert_eq!(run.unwrap_err().code(), HttpErrorCode::CORO_CANCELLED);
        })
    });
}

/// Scheduler turns a writer-arming check may take before it reports a lost wake.
pub(super) const WRITER_DRIVE_LIMIT: usize = 20_000;

/// Scheduler turns that let a session settle before a check that nothing more happens.
pub(super) const WRITER_SETTLE: usize = 100;

/// Yields up to `WRITER_DRIVE_LIMIT` times for `condition`, so a lost wake fails instead of
/// hanging.
pub(super) async fn writer_drive_until(condition: impl Fn() -> bool) -> bool {
    for _ in 0..WRITER_DRIVE_LIMIT {
        if condition() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    condition()
}

pub(super) async fn writer_settle() {
    for _ in 0..WRITER_SETTLE {
        tokio::task::yield_now().await;
    }
}

/// The session polls and wakes a [`CountedWakes`] observed.
pub(super) struct WakeCounts {
    woken: AtomicBool,
    polling: AtomicBool,
    polls: AtomicUsize,
    wakes: AtomicUsize,
    in_poll_wakes: AtomicUsize,
    outer: std::sync::Mutex<Option<Waker>>,
}

impl WakeCounts {
    pub(super) fn polls(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }

    pub(super) fn wakes(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }

    /// Wakes that arrived inside the future's own poll, which a host defers to its next loop
    /// iteration.
    pub(super) fn in_poll_wakes(&self) -> usize {
        self.in_poll_wakes.load(Ordering::SeqCst)
    }

    /// Whether the future is being polled now.
    pub(super) fn polling(&self) -> bool {
        self.polling.load(Ordering::SeqCst)
    }
}

impl std::task::Wake for WakeCounts {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        if self.polling.load(Ordering::SeqCst) {
            self.in_poll_wakes.fetch_add(1, Ordering::SeqCst);
        }
        self.woken.store(true, Ordering::SeqCst);
        let outer = self.outer.lock().unwrap().clone();
        if let Some(outer) = outer {
            outer.wake();
        }
    }
}

/// [`OwnWakes`] that counts the polls of its future and the wakes of its own waker, as a host
/// counts the polls and wakes of a separately scheduled task.
pub(super) struct CountedWakes<F> {
    inner: Pin<Box<F>>,
    counts: Arc<WakeCounts>,
}

impl<F: Future> CountedWakes<F> {
    pub(super) fn new(inner: F) -> (Self, Arc<WakeCounts>) {
        let counts = Arc::new(WakeCounts {
            woken: AtomicBool::new(true),
            polling: AtomicBool::new(false),
            polls: AtomicUsize::new(0),
            wakes: AtomicUsize::new(0),
            in_poll_wakes: AtomicUsize::new(0),
            outer: std::sync::Mutex::new(None),
        });
        (
            Self {
                inner: Box::pin(inner),
                counts: Arc::clone(&counts),
            },
            counts,
        )
    }
}

impl<F: Future> Future for CountedWakes<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let previous = self
            .counts
            .outer
            .lock()
            .unwrap()
            .replace(context.waker().clone());
        drop(previous);
        if !self.counts.woken.swap(false, Ordering::SeqCst) {
            return Poll::Pending;
        }
        self.counts.polls.fetch_add(1, Ordering::SeqCst);
        let waker = Waker::from(Arc::clone(&self.counts));
        self.counts.polling.store(true, Ordering::SeqCst);
        let polled = self.inner.as_mut().poll(&mut Context::from_waker(&waker));
        self.counts.polling.store(false, Ordering::SeqCst);
        polled
    }
}

/// How a [`ScriptedWriteQueue`] answers one accepted write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ScriptedWrite {
    /// Physically complete at the transport's next flush, which runs once the session's poll
    /// returns, as a hosting event loop writes inline when it flushes after its callbacks.
    Complete,
    /// Physically outstanding until the test completes or fails it.
    Pending,
}

/// Where one accepted write of a [`ScriptedWriteQueue`] stands.
enum ScriptedReceipt {
    /// Waits for the transport's next flush.
    Flushing,
    /// Waits for the test.
    Pending,
    /// Physically complete with this result.
    Settled(Result<(), HttpError>),
}

/// What a [`ScriptedWriteQueue`] was given and still owes the session.
pub(super) struct ScriptedWriteState {
    /// The bytes of each accepted write, in order.
    pub(super) writes: Vec<Vec<u8>>,
    script: VecDeque<ScriptedWrite>,
    after_script: ScriptedWrite,
    /// Unconsumed receipts the queue holds before `can_write` refuses more.
    capacity: usize,
    /// Accepted writes whose receipts are unconsumed, oldest first, with their requested
    /// sizes.
    receipts: VecDeque<(usize, ScriptedReceipt)>,
    /// A flush is scheduled.
    flush_scheduled: bool,
    /// The waker of the last `poll_complete` that found no completed receipt.
    writer: Option<Waker>,
    /// Runs inside every `start_write`, before the queue wakes a writer.
    pub(super) on_write: Option<Box<dyn FnMut()>>,
    /// The fixture's tasks (the flushes and the test's own), joined after the session.
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// The queue's `is_saturated` answer; `true`, the trait's default, unless the test clears
    /// it.
    pub(super) saturated: bool,
    /// The write timeout the queue was acquired with; the fixture never times a write.
    pub(super) write_timeout: Duration,
}

impl ScriptedWriteState {
    /// Answers every write after the script with `answer`.
    pub(super) fn set_after_script(&mut self, answer: ScriptedWrite) {
        self.after_script = answer;
    }

    /// Accepted writes that are not yet physically complete.
    pub(super) fn pending(&self) -> usize {
        self.receipts
            .iter()
            .filter(|(_, receipt)| matches!(receipt, ScriptedReceipt::Pending))
            .count()
    }

    /// Whole frames of `frame_type` with every bit of `flags` set in the writes so far.
    pub(super) fn frames(&self, frame_type: FrameType, flags: u8) -> usize {
        let wire = self.writes.concat();
        let mut rest = wire.strip_prefix(CONNECTION_PREFACE).unwrap_or(&wire);
        let mut count = 0;
        while rest.len() >= FRAME_HEADER_SIZE {
            let end =
                FRAME_HEADER_SIZE + u32::from_be_bytes([0, rest[0], rest[1], rest[2]]) as usize;
            if rest.len() < end {
                break;
            }
            if rest[3] == frame_type.as_u8() && rest[4] & flags == flags {
                count += 1;
            }
            rest = &rest[end..];
        }
        count
    }
}

/// Settles the oldest `count` physically outstanding writes with `result`, then wakes the
/// registered writer.
fn settle_scripted_writes(
    state: &RefCell<ScriptedWriteState>,
    count: usize,
    result: impl Fn() -> Result<(), HttpError>,
) {
    let writer = {
        let mut state = state.borrow_mut();
        let mut outstanding = state
            .receipts
            .iter_mut()
            .filter(|(_, receipt)| matches!(receipt, ScriptedReceipt::Pending));
        for _ in 0..count {
            outstanding
                .next()
                .expect("the test settles only outstanding writes")
                .1 = ScriptedReceipt::Settled(result());
        }
        state.writer.take()
    };
    if let Some(writer) = writer {
        writer.wake();
    }
}

/// Completes the oldest `count` physically outstanding writes.
pub(super) fn complete_scripted_writes(state: &RefCell<ScriptedWriteState>, count: usize) {
    settle_scripted_writes(state, count, || Ok(()));
}

/// Fails the oldest physically outstanding write with `error`.
pub(super) fn fail_scripted_write(state: &RefCell<ScriptedWriteState>, error: HttpError) {
    settle_scripted_writes(state, 1, || Err(error.clone()));
}

/// Completes every outstanding write now, and every later write at its flush, so a cancelled
/// session joins its writer.
pub(super) fn finish_scripted_writes(state: &RefCell<ScriptedWriteState>) {
    let count = {
        let mut state = state.borrow_mut();
        state.script.clear();
        state.after_script = ScriptedWrite::Complete;
        state.pending()
    };
    complete_scripted_writes(state, count);
}

/// Runs `task` on its own task, recorded so [`join_scripted_tasks`] joins it.
pub(super) fn spawn_scripted_task(
    state: &RefCell<ScriptedWriteState>,
    task: impl Future<Output = ()> + 'static,
) {
    let handle = tokio::task::spawn_local(task);
    state.borrow_mut().tasks.push(handle);
}

/// Joins every task [`spawn_scripted_task`] recorded, including any a joined task recorded.
pub(super) async fn join_scripted_tasks(state: &RefCell<ScriptedWriteState>) {
    loop {
        let tasks = std::mem::take(&mut state.borrow_mut().tasks);
        if tasks.is_empty() {
            return;
        }
        for task in tasks {
            task.await.expect("a fixture task completes");
        }
    }
}

/// The transport's flush: completes every write accepted for it, then wakes the registered
/// writer.
fn flush_scripted_writes(state: &RefCell<ScriptedWriteState>) {
    let writer = {
        let mut state = state.borrow_mut();
        state.flush_scheduled = false;
        for (_, receipt) in &mut state.receipts {
            if matches!(receipt, ScriptedReceipt::Flushing) {
                *receipt = ScriptedReceipt::Settled(Ok(()));
            }
        }
        state.writer.take()
    };
    if let Some(writer) = writer {
        writer.wake();
    }
}

/// A capacity-only queued writer: it admits writes whatever their size until `capacity`
/// receipts are unconsumed, completes each write at the next flush or holds it physically
/// outstanding as its script says, delivers receipts in write order, and wakes only the waker
/// of the last `poll_complete` that found no completed receipt.
struct ScriptedWriteQueue(Rc<RefCell<ScriptedWriteState>>);

impl crate::uniplex::UniplexWriteQueue for ScriptedWriteQueue {
    fn can_write(&self, _bytes: usize) -> bool {
        let state = self.0.borrow();
        state.receipts.len() < state.capacity
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        let bytes: Vec<u8> = data
            .segments()
            .flat_map(|segment| segment.iter().copied())
            .collect();
        let (flush, on_write) = {
            let mut state = self.0.borrow_mut();
            let receipt = match state.script.pop_front().unwrap_or(state.after_script) {
                ScriptedWrite::Complete => ScriptedReceipt::Flushing,
                ScriptedWrite::Pending => ScriptedReceipt::Pending,
            };
            let flush = matches!(receipt, ScriptedReceipt::Flushing) && !state.flush_scheduled;
            state.flush_scheduled |= flush;
            state.receipts.push_back((bytes.len(), receipt));
            state.writes.push(bytes);
            (flush, state.on_write.take())
        };
        if flush {
            let state = Rc::clone(&self.0);
            spawn_scripted_task(&self.0, async move { flush_scripted_writes(&state) });
        }
        if let Some(mut on_write) = on_write {
            on_write();
            self.0.borrow_mut().on_write = Some(on_write);
        }
        Ok(())
    }

    fn poll_complete(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<crate::uniplex::UniplexWriteCompletion> {
        let mut state = self.0.borrow_mut();
        if let Some((_, ScriptedReceipt::Settled(_))) = state.receipts.front() {
            let Some((requested_bytes, ScriptedReceipt::Settled(result))) =
                state.receipts.pop_front()
            else {
                unreachable!("the front receipt is settled")
            };
            let result = match result {
                Ok(()) => Ok(WriteInfo {
                    bytes_written: requested_bytes,
                }),
                Err(error) => Err(TransportWriteError::new(error, 0)),
            };
            return Poll::Ready(crate::uniplex::UniplexWriteCompletion {
                requested_bytes,
                result,
            });
        }
        let replaced = state.writer.replace(context.waker().clone());
        drop(state);
        drop(replaced);
        Poll::Pending
    }

    fn is_idle(&self) -> bool {
        self.0.borrow().receipts.is_empty()
    }

    fn is_saturated(&self) -> bool {
        self.0.borrow().saturated
    }
}

/// Reads through a [`FixtureTransport`], then, when its reads run out, fails one read once its
/// gate opens; writes through a [`ScriptedWriteQueue`].
pub(super) struct ScriptedWriteTransport {
    inner: Rc<FixtureTransport>,
    reads: Rc<RefCell<FixtureTransportState>>,
    failing_read: RefCell<Option<(Arc<tokio::sync::Notify>, HttpErrorCode)>>,
    pub(super) state: Rc<RefCell<ScriptedWriteState>>,
}

impl ScriptedWriteTransport {
    pub(super) fn new(
        reads: impl IntoIterator<Item = FixtureRead>,
        script: impl IntoIterator<Item = ScriptedWrite>,
        after_script: ScriptedWrite,
        capacity: usize,
        failing_read: Option<(Arc<tokio::sync::Notify>, HttpErrorCode)>,
    ) -> Rc<Self> {
        let (inner, reads) = FixtureTransport::new(reads);
        Rc::new(Self {
            inner,
            reads,
            failing_read: RefCell::new(failing_read),
            state: Rc::new(RefCell::new(ScriptedWriteState {
                writes: Vec::new(),
                script: script.into_iter().collect(),
                after_script,
                capacity,
                receipts: VecDeque::new(),
                flush_scheduled: false,
                writer: None,
                on_write: None,
                tasks: Vec::new(),
                saturated: true,
                write_timeout: Duration::ZERO,
            })),
        })
    }
}

impl UniplexTransport for ScriptedWriteTransport {
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn crate::uniplex::UniplexWriteQueue>>, HttpError> {
        self.state.borrow_mut().write_timeout = timeout;
        Ok(Some(Box::new(ScriptedWriteQueue(Rc::clone(&self.state)))))
    }

    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        if self.reads.borrow().reads.is_empty()
            && let Some((gate, code)) = self.failing_read.borrow_mut().take()
        {
            return Box::pin(async move {
                gate.notified().await;
                Err(HttpError::with_message(code, b"scripted read failure"))
            });
        }
        self.inner
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        _context: &WorkerContext,
        _data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        panic!("the scripted fixture writes only through its queue")
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

/// Opens the gated [`ArmingExchange`] answers once the test releases them.
#[derive(Default)]
struct ResponseGate {
    open: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl ResponseGate {
    fn release(&self) {
        self.open.set(true);
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Every exchange holds its request body unread, so the body source never wakes the session,
/// and answers `200` with a fixed date and no body, ending the response with its head. The
/// exchange for `held` never answers; with a `gate`, every other answer waits for it. Counts
/// the exchanges that retire after answering.
struct ArmingApplication {
    completes: Rc<Cell<usize>>,
    gate: Option<Rc<ResponseGate>>,
    held: Option<StreamId>,
}

struct ArmingExchange {
    held: bool,
    gate: Option<Rc<ResponseGate>>,
    completes: Rc<Cell<usize>>,
    _ingress: IngressCompletion,
    _body: Option<HttpStreamSourceOwner>,
}

impl ArmingApplication {
    fn new(completes: &Rc<Cell<usize>>) -> Self {
        Self {
            completes: Rc::clone(completes),
            gate: None,
            held: None,
        }
    }
}

impl OwnedH2Application for ArmingApplication {
    type Exchange = ArmingExchange;

    fn new_exchange(&self, stream: StreamId, ingress: IngressCompletion) -> ArmingExchange {
        ArmingExchange {
            held: self.held == Some(stream),
            gate: self.gate.clone(),
            completes: Rc::clone(&self.completes),
            _ingress: ingress,
            _body: None,
        }
    }
}

impl OwnedH2Exchange for ArmingExchange {
    fn request(&mut self, _head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        self._body = Some(body);
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if self.held {
            return Poll::Pending;
        }
        if let Some(gate) = &self.gate
            && !gate.open.get()
        {
            *gate.waker.borrow_mut() = Some(context.waker().clone());
            return Poll::Pending;
        }
        let mut response = HttpMessage::new();
        response.set_status_code(200).unwrap();
        response.set_http_version(1, 1);
        // A fixed date keeps the encoded head, and so `ANSWER_BYTES`, constant.
        response
            .headers_mut()
            .set(HttpHeaderCode::Date, TRANSCRIPT_DATE);
        Poll::Ready(Ok(HttpHeaderEvent::new(response, true).unwrap()))
    }

    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        unreachable!("the head ends the response")
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {
        if !self.held {
            self.completes.set(self.completes.get() + 1);
        }
    }
}

/// Bytes the session writes to answer with [`ArmingExchange`]: one HEADERS frame.
const ANSWER_BYTES: usize = 34;

/// The client connection preface and SETTINGS.
fn arming_preface(peer: &mut Http2Codec) -> Vec<u8> {
    let mut preface = eligibility_frame(peer, EgressFrame::ConnectionPreface);
    preface.extend(transcript_settings(peer));
    preface
}

/// A GET that ends its stream with its HEADERS.
fn arming_get(peer: &mut Http2Codec) -> (StreamId, Vec<u8>) {
    let stream = peer.create_stream();
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Get).unwrap();
    request.set_url(b"/", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    let bytes = eligibility_frame(
        peer,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: true,
            extra_headers: None,
        },
    );
    (stream, bytes)
}

/// Runs a downstream session for `application` over `transport` beside `script`, polling the
/// session only on its own wakes. Then cancels it, opens `gates` and finishes every write so
/// the session joins, and returns what the script observed.
async fn run_downstream_arming<T>(
    context: WorkerContext,
    application: ArmingApplication,
    transport: Rc<ScriptedWriteTransport>,
    configure: impl FnOnce(&mut OwnedH2Options),
    gates: &[Arc<tokio::sync::Notify>],
    script: impl AsyncFnOnce(&RefCell<ScriptedWriteState>, &WakeCounts) -> T,
) -> T {
    let state = Rc::clone(&transport.state);
    let mut config = options(context, transport);
    configure(&mut config);
    let (session, handle) = OwnedH2Session::new(application, config).unwrap();
    let (run, counts) = CountedWakes::new(session.run());
    let script = async {
        let observed = script(&state, &counts).await;
        handle.cancel();
        gates.iter().for_each(|gate| gate.notify_one());
        finish_scripted_writes(&state);
        observed
    };
    let (_run, observed) = tokio::time::timeout(Duration::from_secs(10), async {
        let joined = tokio::join!(run, script);
        join_scripted_tasks(&state).await;
        joined
    })
    .await
    .expect("the cancelled session joins its transport operations and the fixture's tasks");
    observed
}

/// Once the server SETTINGS and the SETTINGS ACK are written, opens `gate`, behind which the
/// client's requests wait, and returns the number of writes before them.
async fn release_requests(
    state: &RefCell<ScriptedWriteState>,
    gate: &tokio::sync::Notify,
) -> Option<usize> {
    let ready = writer_drive_until(|| state.borrow().frames(FrameType::SETTINGS, ACK) == 1).await;
    let writes = state.borrow().writes.len();
    gate.notify_one();
    ready.then_some(writes)
}

#[test]
fn stream_retirement_waits_for_physical_completion() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let preface = arming_preface(&mut peer);
            let (_, get) = arming_get(&mut peer);
            let requests = Arc::new(tokio::sync::Notify::new());
            let transport = ScriptedWriteTransport::new(
                [
                    FixtureRead::Data(preface),
                    FixtureRead::WaitData(Arc::clone(&requests), get),
                ],
                [],
                ScriptedWrite::Complete,
                INLINE_OUTSTANDING_WRITES,
                None,
            );
            let completes = Rc::new(Cell::new(0));
            let (before, answered, held, retired) = run_downstream_arming(
                context,
                ArmingApplication::new(&completes),
                transport,
                |_| {},
                &[Arc::clone(&requests)],
                async |state, _counts| {
                    let before = release_requests(state, &requests).await;
                    state.borrow_mut().set_after_script(ScriptedWrite::Pending);
                    let answered = writer_drive_until(|| {
                        state.borrow().frames(FrameType::HEADERS, END_STREAM) == 1
                    })
                    .await;
                    writer_settle().await;
                    let held = (
                        state.borrow().writes.len(),
                        state.borrow().pending(),
                        completes.get(),
                    );
                    complete_scripted_writes(state, 1);
                    let retired = writer_drive_until(|| completes.get() == 1).await;
                    (before, answered, held, retired)
                },
            )
            .await;
            let before = before.expect("the session exchanges SETTINGS");
            assert!(answered, "the session answers the GET");
            assert_eq!(
                held,
                (before + 1, 1, 0),
                "the response's write is pending, and the consumed, response-complete stream \
                 waits for its physical write"
            );
            assert!(retired, "the stream retires once its write completes");
        })
    });
}

/// While another request stays open, so some stream still awaits ingress, a stream that was
/// consumed before it answered retires once its response's pending write completes. The
/// answer comes from the application's own wake, so no self-wake follows the write.
#[test]
fn retirement_behind_an_open_request_waits_for_physical_completion() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let preface = arming_preface(&mut peer);
            let (held, open) = padding_post_headers(&mut peer);
            assert_eq!(held, StreamId::new(1), "the held request is stream 1");
            let (_, get) = arming_get(&mut peer);
            let requests = Arc::new(tokio::sync::Notify::new());
            let transport = ScriptedWriteTransport::new(
                [
                    FixtureRead::Data(preface),
                    FixtureRead::WaitData(Arc::clone(&requests), [open, get].concat()),
                ],
                [],
                ScriptedWrite::Complete,
                INLINE_OUTSTANDING_WRITES,
                None,
            );
            let completes = Rc::new(Cell::new(0));
            let gate = Rc::new(ResponseGate::default());
            let application = ArmingApplication {
                gate: Some(Rc::clone(&gate)),
                held: Some(held),
                ..ArmingApplication::new(&completes)
            };
            let (before, gated, answered, held_write, retired) = run_downstream_arming(
                context,
                application,
                transport,
                |_| {},
                &[Arc::clone(&requests)],
                async |state, _counts| {
                    let before = release_requests(state, &requests).await;
                    writer_settle().await;
                    let gated = (gate.waker.borrow().is_some(), state.borrow().writes.len());
                    state.borrow_mut().set_after_script(ScriptedWrite::Pending);
                    gate.release();
                    let answered = writer_drive_until(|| {
                        state.borrow().frames(FrameType::HEADERS, END_STREAM) == 1
                    })
                    .await;
                    writer_settle().await;
                    let held_write = (
                        state.borrow().writes.len(),
                        state.borrow().pending(),
                        completes.get(),
                    );
                    complete_scripted_writes(state, 1);
                    let retired = writer_drive_until(|| completes.get() == 1).await;
                    (before, gated, answered, held_write, retired)
                },
            )
            .await;
            let before = before.expect("the session exchanges SETTINGS");
            assert_eq!(
                gated,
                (true, before),
                "the GET waits for the gate, and nothing is written"
            );
            assert!(answered, "the session answers the GET");
            assert_eq!(
                held_write,
                (before + 1, 1, 0),
                "the response's write is pending, and the consumed, response-complete stream \
                 waits for its physical write"
            );
            assert!(retired, "the stream retires once its write completes");
        })
    });
}

/// The response's write starts unarmed while its request is still open. Later in the same
/// session poll the request's END_STREAM is read, so no stream awaits ingress, and the write's
/// completion must wake the session.
#[test]
fn ingress_end_after_an_unarmed_write_arms_the_writer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let preface = arming_preface(&mut peer);
            let (stream, open) = padding_post_headers(&mut peer);
            let end = eligibility_frame(
                &mut peer,
                EgressFrame::Body {
                    stream,
                    body: queue(&[]),
                    padding: None,
                    end_stream: true,
                },
            );
            let requests = Arc::new(tokio::sync::Notify::new());
            // One read takes the open request. The next, which completes when first polled,
            // takes its END_STREAM in the turn after the one that answers it.
            let transport = ScriptedWriteTransport::new(
                [
                    FixtureRead::Data(preface),
                    FixtureRead::WaitData(Arc::clone(&requests), open),
                    FixtureRead::SegmentedData(queue(&end)),
                ],
                [],
                ScriptedWrite::Complete,
                INLINE_OUTSTANDING_WRITES,
                None,
            );
            let completes = Rc::new(Cell::new(0));
            let (before, answered, response, woken) = run_downstream_arming(
                context,
                ArmingApplication::new(&completes),
                transport,
                // A write limit of exactly the answer's egress starts that write, whole, in the
                // turn that serializes it, before the turn that reads the END_STREAM.
                |config| config.io.maximum_write_size = ANSWER_BYTES,
                &[Arc::clone(&requests)],
                async |state, counts| {
                    let before = release_requests(state, &requests).await;
                    state.borrow_mut().set_after_script(ScriptedWrite::Pending);
                    let answered = writer_drive_until(|| {
                        state.borrow().frames(FrameType::HEADERS, END_STREAM) == 1
                    })
                    .await;
                    writer_settle().await;
                    let response = {
                        let state = state.borrow();
                        (
                            state.writes.len(),
                            state.writes.last().map_or(0, Vec::len),
                            state.pending(),
                        )
                    };
                    let polls = counts.polls();
                    complete_scripted_writes(state, 1);
                    writer_settle().await;
                    (before, answered, response, counts.polls() > polls)
                },
            )
            .await;
            let before = before.expect("the session exchanges SETTINGS");
            assert!(answered, "the session answers the POST");
            assert_eq!(
                response,
                (before + 1, ANSWER_BYTES, 1),
                "the response's HEADERS, which end it, are one pending write"
            );
            assert!(
                woken,
                "with no stream awaiting ingress, the write's completion wakes the session"
            );
        })
    });
}

#[test]
fn scripted_write_fixture_joins_its_tasks() {
    run_on_worker(|_context| {
        Box::pin(async move {
            let transport = ScriptedWriteTransport::new([], [], ScriptedWrite::Complete, 1, None);
            let state = Rc::clone(&transport.state);
            drop(transport);
            let mut writer = ScriptedWriteQueue(Rc::clone(&state));
            crate::uniplex::UniplexWriteQueue::start_write(&mut writer, queue(b"w")).unwrap();
            drop(writer);
            let ran = Rc::new(Cell::new(false));
            let marked = Rc::clone(&ran);
            spawn_scripted_task(&state, async move {
                tokio::task::yield_now().await;
                marked.set(true);
            });
            join_scripted_tasks(&state).await;
            assert!(
                ran.get(),
                "a recorded task runs to completion before the join returns"
            );
            assert!(
                matches!(
                    state.borrow().receipts.front(),
                    Some((1, ScriptedReceipt::Settled(Ok(()))))
                ),
                "the write's flush task completes it before the join returns"
            );
            assert_eq!(
                Rc::strong_count(&state),
                1,
                "no fixture task holds the fixture after the join"
            );
        })
    });
}

#[test]
fn downstream_arming_harness_joins_fixture_tasks() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let transport = ScriptedWriteTransport::new(
                [FixtureRead::Data(arming_preface(&mut peer))],
                [],
                ScriptedWrite::Complete,
                INLINE_OUTSTANDING_WRITES,
                None,
            );
            let teardown = Arc::new(tokio::sync::Notify::new());
            let ran = Rc::new(Cell::new(false));
            let completes = Rc::new(Cell::new(0));
            let settled = run_downstream_arming(
                context,
                ArmingApplication::new(&completes),
                transport,
                |_| {},
                &[Arc::clone(&teardown)],
                async |state, _counts| {
                    let settled =
                        writer_drive_until(|| state.borrow().frames(FrameType::SETTINGS, ACK) == 1)
                            .await;
                    let marked = Rc::clone(&ran);
                    let teardown = Arc::clone(&teardown);
                    spawn_scripted_task(state, async move {
                        teardown.notified().await;
                        writer_settle().await;
                        marked.set(true);
                    });
                    settled
                },
            )
            .await;
            assert!(settled, "the session exchanges SETTINGS");
            assert!(
                ran.get(),
                "the harness joins a task that runs on past the session's teardown"
            );
        })
    });
}

/// Streams the stream-table order tests open.
const ORDER_STREAMS: [u64; 5] = [1, 3, 5, 7, 9];

/// A fixed stream-table seed under which the table does not iterate the
/// order tests' streams in id order.
pub(super) const ORDER_SEED: u64 = 0x5EED_0048;

/// Asserts the order tests' precondition: under `ORDER_SEED`, a table holding
/// `streams` does not iterate them in id order.
pub(super) fn assert_seed_reorders(streams: &[u64]) {
    assert_ne!(
        stream_seed_order(ORDER_SEED, streams),
        streams,
        "precondition: the seeded stream table iterates out of id order"
    );
}

/// Client preface and SETTINGS (`settings`, or the codec's configured set
/// when empty), then [`order_followups`].
fn order_requests(
    peer: &mut Http2Codec,
    settings: &[CodecSetting],
    count: usize,
    complete: bool,
) -> Vec<u8> {
    let mut requests = eligibility_frame(peer, EgressFrame::ConnectionPreface);
    if settings.is_empty() {
        requests.extend(transcript_settings(peer));
    } else {
        requests.extend(eligibility_frame(peer, EgressFrame::Settings { settings }));
    }
    requests.extend(order_followups(peer, count, complete));
    requests
}

/// One request per new stream: GETs ending the stream, or POSTs whose bodies
/// never arrive.
fn order_followups(peer: &mut Http2Codec, count: usize, complete: bool) -> Vec<u8> {
    let mut requests = Vec::new();
    for _ in 0..count {
        let stream = peer.create_stream();
        let mut request = HttpMessage::new();
        request
            .set_method(if complete {
                HttpMethod::Get
            } else {
                HttpMethod::Post
            })
            .unwrap();
        request
            .set_url(format!("/order/{}", stream.get()).as_bytes(), false)
            .unwrap();
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        requests.extend(eligibility_frame(
            peer,
            EgressFrame::Headers {
                stream,
                message: &request,
                end_stream: complete,
                extra_headers: None,
            },
        ));
    }
    requests
}

/// Bytes of the 65,535-byte response that takes the connection's whole send
/// credit ahead of the 64 consumer-shaped exchanges.
const CREDIT_FILLER: usize = 65_535;

#[test]
fn sixty_four_stream_transcript_is_unchanged() {
    let (written, run) = run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let filler = order_requests(&mut peer, &[], 1, true);
            let exchanges = order_followups(&mut peer, 64, true);
            let credit = eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: (64 * TRANSCRIPT_BODY) as u32,
                },
            );
            let goaway = eligibility_frame(
                &mut peer,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(0),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
            );
            let exchanges_gate = Arc::new(tokio::sync::Notify::new());
            let credit_gate = Arc::new(tokio::sync::Notify::new());
            let goaway_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(filler),
                FixtureRead::WaitData(Arc::clone(&exchanges_gate), exchanges),
                FixtureRead::WaitData(Arc::clone(&credit_gate), credit),
                FixtureRead::WaitDataEof(Arc::clone(&goaway_gate), goaway),
            ]);
            let mut replies = vec![Reply::Body(CREDIT_FILLER)];
            replies.extend([Reply::Body(TRANSCRIPT_BODY); 64]);
            let (mut application, probes) = Application::new(replies);
            application.response_date = Some(TRANSCRIPT_DATE);
            assert_seed_reorders(&(0..65).map(|index| 1 + 2 * index).collect::<Vec<_>>());
            let (session, _handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let session = session.with_stream_seed(ORDER_SEED);
            let script = async {
                wait_for_written(&state, |wire| wire.count(FrameType::DATA, END_STREAM) == 1).await;
                exchanges_gate.notify_one();
                wait_for_written(&state, |wire| wire.count(FrameType::HEADERS, 0) == 65).await;
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                let blocked = written_transcript(&state);
                assert_eq!(
                    (
                        blocked.count(FrameType::HEADERS, 0),
                        blocked.count(FrameType::DATA, END_STREAM),
                        probes.borrow()[1..]
                            .iter()
                            .map(|probe| probe.produced_bytes.get())
                            .sum::<usize>(),
                    ),
                    (65, 1, 64 * TRANSCRIPT_BODY),
                    "every consumer-shaped response is produced and waits on connection send credit"
                );
                credit_gate.notify_one();
                wait_for_written(&state, |wire| wire.count(FrameType::DATA, END_STREAM) == 65)
                    .await;
                goaway_gate.notify_one();
            };
            let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(session.run(), script)
            })
            .await
            .expect("the 64-stream fixture finishes at EOF");
            (
                written_transcript(&state),
                run.map(drop).map_err(|error| error.code()),
            )
        })
    });
    assert_eq!(
        written.digest(),
        (149, 9_009_706_708_975_363_477),
        "written transcript recorded at 3ef2675c725b; written: {written:#?}"
    );
    assert_eq!(written.count(FrameType::HEADERS, 0), 65);
    assert_eq!(written.count(FrameType::DATA, END_STREAM), 65);
    assert_eq!(written.count(FrameType::RST_STREAM, 0), 0);
    assert_eq!(run, Ok(()), "the session closes cleanly at EOF");
}

/// Runs `ORDER_STREAMS.len()` requests (`complete` GETs or bodiless POSTs)
/// whose owners hold their responses, logging cancellations and drops in
/// `order`. Once every request has reached its owner, `failure` delivers peer
/// bytes, or the handle cancels the connection when `failure` is `None`.
/// With `eof`, the requests arrive with EOF instead. Returns the written
/// transcript and how the run ended.
async fn ordered_owner_failure(
    context: WorkerContext,
    order: Rc<StreamOrderProbe>,
    reads: Vec<u8>,
    failure: Option<Vec<u8>>,
    eof: bool,
) -> (WireTranscript, Result<(), HttpErrorCode>) {
    let gate = Arc::new(tokio::sync::Notify::new());
    let mut script = vec![if eof {
        FixtureRead::DataEof(reads)
    } else {
        FixtureRead::Data(reads)
    }];
    if let Some(bytes) = &failure {
        script.push(FixtureRead::WaitData(Arc::clone(&gate), bytes.clone()));
    }
    let (transport, state) = FixtureTransport::new(script);
    let (mut application, probes) = Application::new(vec![Reply::Body(0); ORDER_STREAMS.len()]);
    application.hold_headers = true;
    application.stream_order = Some(order);
    assert_seed_reorders(&ORDER_STREAMS);
    let (session, handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    let session = session.with_stream_seed(ORDER_SEED);
    let operation = async {
        if eof {
            return;
        }
        complete_credit_wait(|| {
            let probes = probes.borrow();
            probes.len() == ORDER_STREAMS.len()
                && probes.iter().all(|probe| probe.requested.get() == 1)
        })
        .await;
        if failure.is_some() {
            gate.notify_one();
        } else {
            handle.cancel();
        }
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), operation)
    })
    .await
    .expect("the failed connection retires every owner");
    drop(probes);
    (
        written_transcript(&state),
        run.map(drop).map_err(|error| error.code()),
    )
}

/// As `ordered_owner_failure` with `eof`, on a session with
/// `Http2SessionConfig::abort_streams_on_eof`.
async fn ordered_owner_eof_abort(
    context: WorkerContext,
    order: Rc<StreamOrderProbe>,
    reads: Vec<u8>,
) -> (WireTranscript, Result<(), HttpErrorCode>) {
    let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(reads)]);
    let (mut application, probes) = Application::new(vec![Reply::Body(0); ORDER_STREAMS.len()]);
    application.hold_headers = true;
    application.stream_order = Some(order);
    assert_seed_reorders(&ORDER_STREAMS);
    let mut config = options(context, transport);
    config.http2.abort_streams_on_eof = true;
    let (session, _handle) = OwnedH2Session::new(application, config).unwrap();
    let session = session.with_stream_seed(ORDER_SEED);
    let run = tokio::time::timeout(Duration::from_secs(10), session.run())
        .await
        .expect("the EOF retires every owner");
    drop(probes);
    (
        written_transcript(&state),
        run.map(drop).map_err(|error| error.code()),
    )
}

/// With `abort_streams_on_eof` (`DIV-055`), an EOF read with requests whose owners
/// hold their responses fails the connection with `TRANSPORT_EOF`: complete GETs and
/// incomplete POSTs alike are cancelled in id order, with no 500 for the incomplete
/// ones, and one GOAWAY carries INTERNAL_ERROR.
#[test]
fn eof_abort_cancels_open_streams_in_id_order_without_500s_when_enabled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let order = Rc::new(StreamOrderProbe::default());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut requests = order_requests(&mut peer, &[], 1, true);
            for complete in [false, true, false, true] {
                requests.extend(order_followups(&mut peer, 1, complete));
            }
            let (written, run) =
                ordered_owner_eof_abort(context, Rc::clone(&order), requests).await;
            assert_eq!(run, Err(HttpErrorCode::TRANSPORT_EOF));
            assert_eq!(order.cancelled.borrow().as_slice(), &ORDER_STREAMS);
            assert_eq!(
                written.count(FrameType::HEADERS, 0),
                0,
                "no response head is written"
            );
            assert_eq!(written.count(FrameType::GOAWAY, 0), 1);
        })
    });
}

/// With `abort_streams_on_eof`, a stream whose response's END_STREAM is serialized is
/// not open, even while the write that carries it is held: an EOF read then takes the
/// default path, and both responses are written whole.
#[test]
fn eof_after_every_response_end_is_serialized_closes_cleanly_when_enabled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let eof = Arc::new(tokio::sync::Notify::new());
            let write_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(peer_two_requests_bytes()),
                FixtureRead::WaitDataEof(Arc::clone(&eof), Vec::new()),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&write_gate));
            let (application, probes) = Application::new(vec![Reply::Body(TRANSCRIPT_BODY); 2]);
            let mut config = options(context, transport);
            config.http2.abort_streams_on_eof = true;
            let (session, _handle) = OwnedH2Session::new(application, config).unwrap();
            let script = async {
                complete_credit_wait(|| {
                    let probes = probes.borrow();
                    probes.len() == 2
                        && probes
                            .iter()
                            .all(|probe| probe.response_complete.get() == 1)
                        && state.borrow().write_gate_pending.is_some()
                })
                .await;
                eof.notify_one();
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                write_gate.notify_one();
            };
            let (run, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(session.run(), script)
            })
            .await
            .expect("the session ends at EOF");
            assert_eq!(
                run.map(drop).map_err(|error| error.code()),
                Ok(()),
                "the EOF closes the connection cleanly"
            );
            let written = written_transcript(&state);
            assert_eq!(written.count(FrameType::HEADERS, 0), 2);
            assert_eq!(written.count(FrameType::DATA, END_STREAM), 2);
            assert!(
                probes
                    .borrow()
                    .iter()
                    .all(|probe| probe.cancelled.borrow().is_empty())
            );
        })
    });
}

/// With `abort_streams_on_eof`, a request block begun above the final GOAWAY's last stream
/// is not open: the codec drops its events when it ends. Its EOF, while the write carrying
/// stream 1's whole response is held, then closes the connection cleanly.
#[test]
fn eof_inside_a_request_head_above_the_final_goaway_closes_cleanly_when_enabled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let eof = Arc::new(tokio::sync::Notify::new());
            let write_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(eof_script(&[eof_request(1, true, true)])),
                FixtureRead::WaitDataEof(Arc::clone(&eof), eof_request(3, true, false)),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&write_gate));
            let (application, probes) = Application::new(vec![Reply::Body(TRANSCRIPT_BODY)]);
            let mut config = options(context, transport);
            config.http2.abort_streams_on_eof = true;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            let script = async {
                complete_credit_wait(|| {
                    probes
                        .borrow()
                        .first()
                        .is_some_and(|probe| probe.response_complete.get() == 1)
                        && state.borrow().write_gate_pending.is_some()
                })
                .await;
                assert!(handle.initiate_drain().unwrap());
                for _ in 0..256 {
                    tokio::task::yield_now().await;
                }
                eof.notify_one();
                for _ in 0..256 {
                    tokio::task::yield_now().await;
                }
                write_gate.notify_one();
            };
            let (run, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(session.run(), script)
            })
            .await
            .expect("the session ends at EOF");
            assert_eq!(
                run.map(drop).map_err(|error| error.code()),
                Ok(()),
                "the EOF closes the connection cleanly"
            );
            let decoded = decode_server_frames_with_requests(&state.borrow().writes.concat(), 3);
            assert_eq!(
                decoded.goaways,
                [
                    (StreamId::new(0x7fff_ffff), H2ErrorCode::NO_ERROR),
                    (StreamId::new(1), H2ErrorCode::NO_ERROR)
                ],
                "the final GOAWAY precedes stream 3's block"
            );
            assert_eq!(
                written_transcript(&state).count(FrameType::DATA, END_STREAM),
                1
            );
            assert!(probes.borrow()[0].cancelled.borrow().is_empty());
            assert_ne!(state.borrow().close, FixtureClose::Reset);
        })
    });
}

/// Serialized output is kept only best-effort (`DIV-055`). The owned driver serializes into
/// its egress buffer while a write is held, so an endless response can fill the buffer and
/// leave no room for the abort's GOAWAY. The driver then aborts at once: the exchange is
/// cancelled, the transport is reset while the write is still held, and nothing serialized
/// after that write is written. (`UniplexSession` serializes only between writes, and
/// ignores a GOAWAY that does not fit.)
#[test]
fn eof_abort_without_room_for_its_goaway_resets_at_once() {
    run_on_worker(|context| {
        Box::pin(async move {
            let eof = Arc::new(tokio::sync::Notify::new());
            let write_gate = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(order_requests(&mut peer, &[], 1, true)),
                FixtureRead::WaitDataEof(Arc::clone(&eof), Vec::new()),
            ]);
            state.borrow_mut().write_gate = Some(Arc::clone(&write_gate));
            let (application, probes) = Application::new(vec![Reply::Body(1 << 20)]);
            let mut config = options(context, transport);
            config.io.buffer_limit = 1 << 15;
            config.io.ingress_buffer_limit = 1 << 14;
            config.io.maximum_write_size = 1 << 15;
            config.io.maximum_read_size = 1 << 14;
            config.io.read_allocation_size = 4096;
            config.http2.abort_streams_on_eof = true;
            let (session, _handle) = OwnedH2Session::new(application, config).unwrap();
            let script = async {
                let mut before = 0;
                loop {
                    for _ in 0..64 {
                        tokio::task::yield_now().await;
                    }
                    let now = probes
                        .borrow()
                        .first()
                        .map_or(0, |probe| probe.produced_bytes.get());
                    if now != 0 && now == before && state.borrow().write_gate_pending.is_some() {
                        break;
                    }
                    before = now;
                }
                eof.notify_one();
                for _ in 0..256 {
                    tokio::task::yield_now().await;
                }
                assert!(
                    state.borrow().writes.is_empty(),
                    "the first write is still held"
                );
                assert_eq!(
                    state.borrow().close,
                    FixtureClose::Reset,
                    "the transport is reset before the held write completes"
                );
                write_gate.notify_one();
            };
            let (run, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(session.run(), script)
            })
            .await
            .expect("the run ends once the held write completes");
            assert_eq!(
                run.err().map(|error| error.code()),
                Some(HttpErrorCode::TRANSPORT_EOF)
            );
            assert_eq!(
                probes.borrow()[0].cancelled.borrow().as_slice(),
                &[(HttpErrorCode::TRANSPORT_EOF, false)]
            );
            assert_eq!(
                state.borrow().writes.len(),
                1,
                "only the held write is written"
            );
        })
    });
}

#[test]
fn eof_fails_incomplete_streams_in_id_order() {
    run_on_worker(|context| {
        Box::pin(async move {
            let order = Rc::new(StreamOrderProbe::default());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let requests = order_requests(&mut peer, &[], ORDER_STREAMS.len(), false);
            let (written, run) =
                ordered_owner_failure(context, Rc::clone(&order), requests, None, true).await;
            assert_eq!(
                run,
                Ok(()),
                "the session closes at EOF once every stream failed"
            );
            assert_eq!(order.cancelled.borrow().as_slice(), &ORDER_STREAMS);
            let headers: Vec<_> = written
                .frames
                .iter()
                .filter(|frame| frame.frame_type == FrameType::HEADERS.as_u8())
                .map(|frame| u64::from(frame.stream))
                .collect();
            assert_eq!(
                headers, ORDER_STREAMS,
                "the 500 responses go out in id order"
            );
        })
    });
}

#[test]
fn connection_failure_fails_streams_in_id_order() {
    for peer_error in [true, false] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let order = Rc::new(StreamOrderProbe::default());
                let mut peer = Http2Codec::new(TransportDirection::Upstream);
                let requests = order_requests(&mut peer, &[], ORDER_STREAMS.len(), true);
                // A connection WINDOW_UPDATE past 2^31 - 1 fails the connection
                // (`fail_connection`); handle cancellation aborts it (`abort`).
                let failure = peer_error.then(|| {
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::WindowUpdate {
                            stream: StreamId::UNSPECIFIED,
                            amount: i32::MAX as u32,
                        },
                    )
                });
                let (_, run) =
                    ordered_owner_failure(context, Rc::clone(&order), requests, failure, false)
                        .await;
                assert_eq!(
                    run,
                    Err(if peer_error {
                        HttpErrorCode::FLOW_CONTROL_ERROR
                    } else {
                        HttpErrorCode::CORO_CANCELLED
                    })
                );
                assert_eq!(order.cancelled.borrow().as_slice(), &ORDER_STREAMS);
            })
        });
    }
}

#[test]
fn wake_all_polls_owners_in_id_order() {
    run_on_worker(|context| {
        Box::pin(async move {
            let order = Rc::new(StreamOrderProbe::default());
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let requests = order_requests(&mut peer, &[], ORDER_STREAMS.len(), true);
            let (_, run) =
                ordered_owner_failure(context, Rc::clone(&order), requests, None, false).await;
            assert_eq!(run, Err(HttpErrorCode::CORO_CANCELLED));
            // `abort` wakes every owner from `reset_transport` before it resets
            // them, so the owner polls that retire them follow the wake order.
            assert_eq!(
                order.dropped.borrow().as_slice(),
                ORDER_STREAMS.map(|stream| (stream, false))
            );
        })
    });
}

/// Four GETs under a peer initial stream window of 0, so their 128-byte
/// responses wait on stream credit; then `update` (peer bytes); then, unless
/// the update fails the connection, a GOAWAY and EOF.
async fn initial_window_case(
    context: WorkerContext,
    overflow: bool,
) -> (WireTranscript, Result<(), HttpErrorCode>) {
    let initial = |value| CodecSetting {
        identifier: SettingsId::InitialWindowSize.as_u16(),
        value,
    };
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let requests = order_requests(&mut peer, &[initial(0)], 4, true);
    let mut update = Vec::new();
    if overflow {
        update.extend(eligibility_frame(
            &mut peer,
            EgressFrame::WindowUpdate {
                stream: StreamId::new(5),
                amount: i32::MAX as u32,
            },
        ));
    }
    update.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Settings {
            settings: &[initial(if overflow { 1 } else { TRANSCRIPT_BODY as u64 })],
        },
    ));
    let goaway = eligibility_frame(
        &mut peer,
        EgressFrame::Goaway {
            last_stream: StreamId::new(0),
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        },
    );
    let update_gate = Arc::new(tokio::sync::Notify::new());
    let goaway_gate = Arc::new(tokio::sync::Notify::new());
    let (transport, state) = FixtureTransport::new([
        FixtureRead::Data(requests),
        FixtureRead::WaitData(Arc::clone(&update_gate), update),
        FixtureRead::WaitDataEof(Arc::clone(&goaway_gate), goaway),
    ]);
    let (mut application, _probes) = Application::new(vec![Reply::Body(TRANSCRIPT_BODY); 4]);
    application.response_date = Some(TRANSCRIPT_DATE);
    assert_seed_reorders(&[1, 3, 5, 7]);
    let (session, _handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    let session = session.with_stream_seed(ORDER_SEED);
    let script = async {
        wait_for_written(&state, |wire| wire.count(FrameType::HEADERS, 0) == 4).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            written_transcript(&state).count(FrameType::DATA, 0),
            0,
            "every response waits on stream credit"
        );
        update_gate.notify_one();
        if overflow {
            return;
        }
        wait_for_written(&state, |wire| wire.count(FrameType::DATA, END_STREAM) == 4).await;
        goaway_gate.notify_one();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), script)
    })
    .await
    .expect("the initial-window fixture finishes");
    (
        written_transcript(&state),
        run.map(drop).map_err(|error| error.code()),
    )
}

#[test]
fn initial_window_change_reaches_every_stream() {
    let (written, run) = run_on_worker(|context| Box::pin(initial_window_case(context, false)));
    let data: Vec<_> = written
        .frames
        .iter()
        .filter(|frame| frame.frame_type == FrameType::DATA.as_u8())
        .map(|frame| (frame.stream, frame.length, frame.flags & END_STREAM))
        .collect();
    assert_eq!(
        data,
        [1, 3, 5, 7].map(|stream| (stream, TRANSCRIPT_BODY as u32, END_STREAM)),
        "the new initial window reaches every stream"
    );
    assert_eq!(run, Ok(()));
    let (written, run) = run_on_worker(|context| Box::pin(initial_window_case(context, true)));
    assert_eq!(
        written.digest(),
        (8, 8_117_510_642_420_668_309),
        "overflow transcript recorded at 3ef2675c725b; written: {written:#?}"
    );
    assert_eq!(run, Err(HttpErrorCode::FLOW_CONTROL_ERROR));
}

thread_local! {
    /// `yield_to_io` futures that [`FoldHost`] holds until [`release_held_yields`].
    static HELD_YIELDS: RefCell<Option<Vec<Rc<HeldYield>>>> = const { RefCell::new(None) };
}

#[derive(Default)]
struct HeldYield {
    released: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

/// Holds every later `yield_to_io` of a [`FoldHost`] on this thread until
/// [`release_held_yields`], so a budget yield wakes nothing by itself.
fn hold_yields() {
    HELD_YIELDS.set(Some(Vec::new()));
}

/// Completes the held yields and stops holding new ones.
fn release_held_yields() {
    for held in HELD_YIELDS.take().unwrap_or_default() {
        held.released.set(true);
        if let Some(waker) = held.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

/// The Tokio host, with yields that a test can hold, that runs wakes in the current iteration.
struct FoldHost;

impl roxygen_runtime::host::LocalHost for FoldHost {
    fn runs_wakes_this_iteration(&self) -> bool {
        true
    }

    fn spawn_local(&self, future: roxygen_runtime::host::LocalFuture) {
        roxygen_runtime::host::TokioHost.spawn_local(future);
    }

    fn now(&self) -> Instant {
        roxygen_runtime::host::TokioHost.now()
    }

    fn sleep_until(&self, deadline: Instant) -> roxygen_runtime::host::LocalFuture {
        roxygen_runtime::host::TokioHost.sleep_until(deadline)
    }

    fn yield_to_io(&self) -> roxygen_runtime::host::LocalFuture {
        let held = HELD_YIELDS.with_borrow_mut(|held| {
            held.as_mut().map(|held| {
                let entry = Rc::new(HeldYield::default());
                held.push(Rc::clone(&entry));
                entry
            })
        });
        let Some(held) = held else {
            return roxygen_runtime::host::TokioHost.yield_to_io();
        };
        Box::pin(poll_fn(move |context| {
            if held.released.get() {
                Poll::Ready(())
            } else {
                *held.waker.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }
        }))
    }
}

/// [`run_on_worker`] on a worker whose host is a [`FoldHost`].
fn run_on_fold_worker<T: Send + 'static>(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
) -> T {
    let result = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
        .block_on(LocalSet::new().run_until(async move {
            let host = Host::new(Rc::new(FoldHost));
            let (worker, driver) = WorkerDriver::with_host(16, host).expect("worker driver");
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
        }));
    release_held_yields();
    result
}

/// Counts wakes delivered to the session's own waker and marks the session runnable; the
/// task's waker still runs.
struct CountingWake {
    delegate: Waker,
    wakes: Arc<AtomicUsize>,
    woken: Arc<AtomicBool>,
}

impl std::task::Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.woken.store(true, Ordering::SeqCst);
        self.delegate.wake_by_ref();
    }
}

#[derive(Default)]
struct SessionCounts {
    wakes: Arc<AtomicUsize>,
    polls: Cell<usize>,
}

impl SessionCounts {
    fn wakes(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }
}

/// Polls the session only after its own waker fired, as a host does. `tokio::join!` alone
/// polls every branch whenever any branch wakes, which would hide a lost session wake.
fn gated_run(
    session: OwnedH2Session<Application>,
    counts: &SessionCounts,
) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> + '_ {
    let mut run = Box::pin(session.run());
    let woken = Arc::new(AtomicBool::new(true));
    poll_fn(move |context| {
        if !woken.swap(false, Ordering::SeqCst) {
            return Poll::Pending;
        }
        counts.polls.set(counts.polls.get() + 1);
        let waker = Waker::from(Arc::new(CountingWake {
            delegate: context.waker().clone(),
            wakes: Arc::clone(&counts.wakes),
            woken: Arc::clone(&woken),
        }));
        run.as_mut().poll(&mut Context::from_waker(&waker))
    })
}

/// Runs the wake-gated `session` beside `scenario`, counting its polls and wakes, then
/// cancels it.
async fn counted_exercise(
    session: OwnedH2Session<Application>,
    handle: &OwnedH2Handle,
    counts: &SessionCounts,
    scenario: impl Future<Output = ()>,
) {
    let session = gated_run(session, counts);
    let scenario = async {
        let outcome =
            eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), scenario)).await;
        handle.cancel();
        outcome
    };
    let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session, scenario)
    })
    .await
    .expect("the cancelled connection joins its transport operations");
    match outcome {
        Ok(result) => result.expect("the scenario made progress"),
        Err(payload) => resume_unwind(payload),
    }
    assert_eq!(
        result
            .expect_err("the scenario ends by cancellation")
            .code(),
        HttpErrorCode::CORO_CANCELLED
    );
}

/// Reads a retained request body to its terminal event without yielding, so every read
/// runs outside the session's poll. The whole body must already be buffered.
fn read_buffered_body(body: &mut HttpSourceHolder) -> usize {
    let mut bytes = 0;
    loop {
        let event = body
            .read_body_event(1 << 16)
            .now_or_never()
            .expect("the request body is already buffered")
            .expect("the buffered request body reads");
        bytes += event.as_body().map_or(0, BufQueue::chain_length);
        if event.eom() {
            return bytes;
        }
    }
}

fn held_body_buffered(probes: &RefCell<Vec<Rc<Probe>>>, ordinal: usize, bytes: u64) -> bool {
    probes.borrow().get(ordinal).is_some_and(|probe| {
        probe.held_body.borrow().is_some()
            && probe
                .ingress
                .borrow()
                .as_ref()
                .is_some_and(|ingress| ingress.body_bytes_received() == bytes)
    })
}

/// Scheduler turns a liveness check may take before it reports a lost wake.
pub(super) const DRIVE_LIMIT: usize = 64;

/// Yields up to `limit` times for `condition`, so a lost wake fails instead of hanging.
pub(super) async fn drive_until(limit: usize, condition: impl Fn() -> bool) -> bool {
    for _ in 0..limit {
        if condition() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    condition()
}

pub(super) async fn settle_turns(turns: usize) {
    for _ in 0..turns {
        tokio::task::yield_now().await;
    }
}

type WindowUpdates = Vec<(StreamId, u32)>;

/// `(body bytes, maximum DATA payload, padding per DATA frame)`.
type PeerBody = (usize, usize, Option<u8>);

/// The client preface plus one POST per body. Each body ends with END_STREAM and goes out as
/// DATA frames of at most the given payload, each padded when padding is given.
fn peer_posts(bodies: &[PeerBody]) -> Vec<u8> {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut output = BufQueue::new(1 << 20);
    codec
        .generate(
            &mut output,
            EgressFrame::ConnectionPreface,
            &mut IgnoreCodecEvents,
        )
        .unwrap();
    codec
        .generate_configured_settings(&mut output, &mut IgnoreCodecEvents)
        .unwrap();
    for &(length, frame, padding) in bodies {
        let stream = codec.create_stream();
        let mut request = HttpMessage::new();
        request.set_method(HttpMethod::Post).unwrap();
        request.set_url(b"/credit", false).unwrap();
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
                    end_stream: false,
                    extra_headers: None,
                },
                &mut IgnoreCodecEvents,
            )
            .unwrap();
        let mut remaining = length;
        while remaining != 0 {
            let chunk = remaining.min(frame);
            remaining -= chunk;
            codec
                .generate(
                    &mut output,
                    EgressFrame::Body {
                        stream,
                        body: queue(&vec![b'c'; chunk]),
                        padding,
                        end_stream: remaining == 0,
                    },
                    &mut IgnoreCodecEvents,
                )
                .unwrap();
        }
    }
    output.coalesce(output.chain_length()).unwrap().to_vec()
}

/// Reads the retained bodies outside the session's poll, one group of back-to-back reads per
/// step. Returns the WINDOW_UPDATEs on the wire after each step has had 16 scheduler turns to
/// settle, and the written transcript after the last step.
fn window_updates_after_reads(
    bodies: Vec<PeerBody>,
    reads: Vec<Vec<usize>>,
) -> (Vec<WindowUpdates>, (usize, u64)) {
    run_on_fold_worker(move |context| {
        Box::pin(async move {
            let (transport, state) =
                FixtureTransport::new([FixtureRead::Data(peer_posts(&bodies))]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0); bodies.len()]);
            application.retain_request = true;
            application.hold_headers = true;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let observed = RefCell::new(Vec::new());
            let written = Cell::new((0, 0));
            let counts = SessionCounts::default();
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    !state.borrow().writes.is_empty()
                        && bodies.iter().enumerate().all(|(ordinal, (length, ..))| {
                            held_body_buffered(&probes, ordinal, *length as u64)
                        })
                })
                .await;
                let mut held = Vec::new();
                for group in &reads {
                    for &ordinal in group {
                        let probe = Rc::clone(&probes.borrow()[ordinal]);
                        let mut body = probe.held_body.borrow_mut().take().unwrap();
                        assert_eq!(read_buffered_body(&mut body), bodies[ordinal].0);
                        held.push(body);
                    }
                    settle_turns(16).await;
                    observed.borrow_mut().push(
                        decode_server_frames_with_requests(
                            &state.borrow().writes.concat(),
                            bodies.len(),
                        )
                        .window_updates,
                    );
                }
                written.set(written_transcript(&state).digest());
                drop(held);
            })
            .await;
            (observed.into_inner(), written.get())
        })
    })
}

fn connection_update(amount: u32) -> (StreamId, u32) {
    (StreamId::UNSPECIFIED, amount)
}

#[test]
fn window_update_offsets_are_unchanged_around_the_threshold() {
    // Default 65,535-byte windows: the connection update goes out once more than 32,767 bytes
    // of credit are pending. Every body ends with END_STREAM, so no stream credit is returned.
    type Case = (
        &'static str,
        Vec<PeerBody>,
        Vec<Vec<usize>>,
        Vec<WindowUpdates>,
        (usize, u64),
    );
    let cases: Vec<Case> = vec![
        (
            "(a) one 30,000 B body stays below the threshold",
            vec![(30_000, 16_384, None)],
            vec![vec![0]],
            vec![vec![]],
            (2, 7_026_372_957_147_746_794),
        ),
        (
            "(b) one 40,000 B body crosses it",
            vec![(40_000, 16_384, None)],
            vec![vec![0]],
            vec![vec![connection_update(40_000)]],
            (3, 10_471_453_527_199_536_510),
        ),
        (
            "(c) two 20,000 B bodies cross it together, at the second read",
            vec![(20_000, 16_384, None), (20_000, 16_384, None)],
            vec![vec![0], vec![1]],
            vec![vec![], vec![connection_update(40_000)]],
            (3, 10_471_453_527_199_536_510),
        ),
        (
            // 12 frames of 2,500 B, each charged 255 B of padding plus its pad-length byte when
            // it arrives: 30,000 + 12 * 256 = 33,072.
            "(d) received padding plus a 30,000 B body cross it",
            vec![(30_000, 2_500, Some(255))],
            vec![vec![0]],
            vec![vec![connection_update(33_072)]],
            (3, 1_153_870_922_662_416_260),
        ),
        (
            // Both reads happen before the session runs; the first crosses on its own.
            "(e) a body read right after a crossing read keeps the woken order",
            vec![(40_000, 16_384, None), (20_000, 16_384, None)],
            vec![vec![0, 1]],
            vec![vec![connection_update(40_000)]],
            (3, 10_471_453_527_199_536_510),
        ),
    ];
    let mismatches: Vec<String> = cases
        .into_iter()
        .filter_map(|(name, bodies, reads, updates, digest)| {
            let observed = window_updates_after_reads(bodies, reads);
            (observed != (updates.clone(), digest)).then(|| {
                format!(
                    "{name}: expected {:?}, observed {observed:?}",
                    (updates, digest)
                )
            })
        })
        .collect();
    assert!(
        mismatches.is_empty(),
        "transcripts recorded at 7eddd4a0506c: {mismatches:#?}"
    );
}

/// The client preface and SETTINGS, then D (a POST with a 1,000 B body and END_STREAM) and
/// the head of R, a POST whose body is still to come. Also returns R's 40,000 B of DATA.
fn deferred_and_rejected_posts() -> (StreamId, StreamId, Vec<u8>, Vec<u8>) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    input.extend(transcript_settings(&mut peer));
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Post).unwrap();
    request.set_url(b"/credit", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    let deferring = peer.create_stream();
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Headers {
            stream: deferring,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    ));
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Body {
            stream: deferring,
            body: queue(&[b'd'; 1_000]),
            padding: None,
            end_stream: true,
        },
    ));
    let rejected = peer.create_stream();
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Headers {
            stream: rejected,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    ));
    let mut rejected_data = Vec::new();
    for chunk in [16_384, 16_384, 7_232] {
        rejected_data.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Body {
                stream: rejected,
                body: queue(&vec![b'r'; chunk]),
                padding: None,
                end_stream: false,
            },
        ));
    }
    (deferring, rejected, input, rejected_data)
}

#[test]
fn folded_credit_precedes_rejected_data_parsed_in_the_next_poll() {
    let (updates, written) = run_on_fold_worker(|context| {
        Box::pin(async move {
            let (_, rejected, input, rejected_data) = deferred_and_rejected_posts();
            let send_rejected = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&send_rejected), rejected_data),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0); 2]);
            application.retain_request = true;
            application.hold_headers = true;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            let observed = RefCell::new((Vec::new(), (0, 0)));
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    !state.borrow().writes.is_empty()
                        && held_body_buffered(&probes, 0, 1_000)
                        && held_body_buffered(&probes, 1, 0)
                })
                .await;
                let r = Rc::clone(&probes.borrow()[1]);
                let mut r = r.held_body.borrow_mut().take().unwrap();
                r.stop_reading(Some(HttpErrorCode::CANCEL));
                let reset = drive_until(DRIVE_LIMIT, || {
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2).resets
                        == [(rejected, H2ErrorCode::CANCEL)]
                })
                .await;
                assert!(reset, "R is reset, so its later DATA is rejected");
                settle_turns(16).await;
                // D's 1,000 B are read while the session is idle, and the session settles
                // before R's 40,000 B arrive in the next read.
                let d = Rc::clone(&probes.borrow()[0]);
                let mut d = d.held_body.borrow_mut().take().unwrap();
                assert_eq!(read_buffered_body(&mut d), 1_000);
                settle_turns(16).await;
                send_rejected.notify_one();
                settle_turns(16).await;
                *observed.borrow_mut() = (
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2)
                        .window_updates,
                    written_transcript(&state).digest(),
                );
                drop((d, r));
            })
            .await;
            observed.into_inner()
        })
    });
    assert_eq!(
        (updates, written),
        (
            vec![connection_update(41_000)],
            (4, 4_371_259_458_476_961_168)
        ),
        "transcript recorded at 7eddd4a0506c"
    );
}

#[test]
fn credit_below_headroom_does_not_wake_an_idle_session() {
    run_on_fold_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Post, &[b'q'; 128], None, PeerControl::None);
            let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.retain_request = true;
            application.hold_headers = true;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    held_body_buffered(&probes, 0, 128) && !state.borrow().writes.is_empty()
                })
                .await;
                settle_turns(16).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                let mut body = probe.held_body.borrow_mut().take().unwrap();
                let before = counts.wakes();
                assert_eq!(read_buffered_body(&mut body), 128);
                assert_eq!(
                    counts.wakes() - before,
                    0,
                    "128 bytes of credit and the source's completion, with no response yet, \
                     need no session poll"
                );
                drop(body);
            })
            .await;
        })
    });
}

#[test]
fn credit_behind_a_queued_owner_is_not_folded() {
    let (wakes, updates, written) = run_on_fold_worker(|context| {
        Box::pin(async move {
            let gate = Rc::new(OwnerPollGate {
                pending_visits: Some(RefCell::new(Vec::new())),
                ..OwnerPollGate::default()
            });
            // X (40,000 B) and D (20,000 B).
            let bodies = [(40_000, 16_384, None), (20_000, 16_384, None)];
            let (transport, state) =
                FixtureTransport::new([FixtureRead::Data(peer_posts(&bodies))]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0); 2]);
            application.retain_request = true;
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            let observed = RefCell::new((0, Vec::new(), (0, 0)));
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    !state.borrow().writes.is_empty()
                        && held_body_buffered(&probes, 0, 40_000)
                        && held_body_buffered(&probes, 1, 20_000)
                        && gate.wakers.borrow().iter().all(Option::is_some)
                })
                .await;
                settle_turns(16).await;
                // Before the session runs: X's owner is queued by its header wake, then D's
                // 20,000 B are read, then X's 40,000 B. D's credit must keep its place behind
                // X, so X's poll returns 40,000 alone and crosses the threshold.
                gate.ready.set(true);
                let x_waker = gate.wakers.borrow()[0].clone().unwrap();
                x_waker.wake_by_ref();
                let d = Rc::clone(&probes.borrow()[1]);
                let mut d = d.held_body.borrow_mut().take().unwrap();
                let before = counts.wakes();
                assert_eq!(read_buffered_body(&mut d), 20_000);
                let wakes = counts.wakes() - before;
                let x = Rc::clone(&probes.borrow()[0]);
                let mut x = x.held_body.borrow_mut().take().unwrap();
                assert_eq!(read_buffered_body(&mut x), 40_000);
                settle_turns(32).await;
                assert_eq!(*gate.pending_visits.as_ref().unwrap().borrow(), [0]);
                *observed.borrow_mut() = (
                    wakes,
                    decode_server_frames_with_requests(&state.borrow().writes.concat(), 2)
                        .window_updates,
                    written_transcript(&state).digest(),
                );
                drop((x, d));
            })
            .await;
            observed.into_inner()
        })
    });
    assert_eq!(
        (wakes, updates, written),
        (
            1,
            vec![connection_update(40_000)],
            (3, 10_471_453_527_199_536_510)
        ),
        "D's read wakes the session as before; transcript recorded at 7eddd4a0506c"
    );
}

/// The client preface and SETTINGS, then a POST with a 5-byte body and END_STREAM. Also
/// returns 4 bytes of DATA for that stream after its END_STREAM.
fn post_then_late_data() -> (StreamId, Vec<u8>, Vec<u8>) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    input.extend(transcript_settings(&mut peer));
    let stream = peer.create_stream();
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Post).unwrap();
    request.set_url(b"/late", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    ));
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Body {
            stream,
            body: queue(b"hello"),
            padding: None,
            end_stream: true,
        },
    ));
    // DATA after the client's END_STREAM is a stream error for a live stream, and is ignored
    // once both directions are complete.
    let late = eligibility_frame(
        &mut peer,
        EgressFrame::Body {
            stream,
            body: queue(b"late"),
            padding: None,
            end_stream: false,
        },
    );
    (stream, input, late)
}

#[test]
fn late_request_data_after_a_folded_end_is_ignored() {
    let written = run_on_fold_worker(|context| {
        Box::pin(async move {
            let (stream, input, late) = post_then_late_data();
            let send_late = Arc::new(tokio::sync::Notify::new());
            let release_write = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&send_late), late),
            ]);
            let gate = Rc::new(OwnerPollGate::default());
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.response_date = Some(TRANSCRIPT_DATE);
            application.retain_request = true;
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            let written = Cell::new((0, 0));
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    gate.polls.get() == 1
                        && held_body_buffered(&probes, 0, 5)
                        && !state.borrow().writes.is_empty()
                })
                .await;
                settle_turns(16).await;
                // The request ends while the session is idle and no response is pending.
                let probe = Rc::clone(&probes.borrow()[0]);
                let mut body = probe.held_body.borrow_mut().take().unwrap();
                assert_eq!(read_buffered_body(&mut body), 5);
                drop(body);
                settle_turns(16).await;
                // The terminal 200's write is held, so the exchange stays live while the late
                // DATA arrives.
                state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                gate.release();
                complete_credit_wait(|| {
                    probe.response_complete.get() == 1
                        && state.borrow().write_gate_pending.is_some()
                })
                .await;
                send_late.notify_one();
                settle_turns(16).await;
                release_write.notify_one();
                let settled = drive_until(DRIVE_LIMIT, || {
                    probe.completed.get() == 1 || !probe.cancelled.borrow().is_empty()
                })
                .await;
                assert!(settled, "the exchange either completes or is reset");
                settle_turns(16).await;
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(
                    (decoded.resets, probe.cancelled.borrow().clone()),
                    (vec![], vec![]),
                    "late DATA for a completed exchange is ignored, not reset"
                );
                assert_eq!(probe.completed.get(), 1);
                assert_eq!(decoded.header_statuses, [(stream, 200)]);
                written.set(written_transcript(&state).digest());
            })
            .await;
            written.get()
        })
    });
    assert_eq!(
        written,
        (3, 10_517_397_198_046_951_649),
        "transcript recorded at 7eddd4a0506c"
    );
}

/// Two POST heads, then 20,000 B with END_STREAM for each in one later read. Each exchange
/// reads its whole body, and so completes its source, inside its own owner poll. Returns the
/// WINDOW_UPDATEs and the written transcript once both exchanges are complete.
async fn in_poll_credit_updates(context: WorkerContext) -> (WindowUpdates, (usize, u64)) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut heads = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    heads.extend(transcript_settings(&mut peer));
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Post).unwrap();
    request.set_url(b"/credit", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    let streams = [peer.create_stream(), peer.create_stream()];
    for stream in streams {
        heads.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Headers {
                stream,
                message: &request,
                end_stream: false,
                extra_headers: None,
            },
        ));
    }
    let mut bodies = Vec::new();
    for stream in streams {
        for (chunk, end_stream) in [(16_384, false), (3_616, true)] {
            bodies.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Body {
                    stream,
                    body: queue(&vec![b'c'; chunk]),
                    padding: None,
                    end_stream,
                },
            ));
        }
    }
    let send_bodies = Arc::new(tokio::sync::Notify::new());
    let (transport, state) = FixtureTransport::new([
        FixtureRead::Data(heads),
        FixtureRead::WaitData(Arc::clone(&send_bodies), bodies),
    ]);
    let (mut application, probes) = Application::new(vec![Reply::Body(0); 2]);
    application.response_date = Some(TRANSCRIPT_DATE);
    let (session, handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    let counts = SessionCounts::default();
    let observed = RefCell::new((Vec::new(), (0, 0)));
    counted_exercise(session, &handle, &counts, async {
        complete_credit_wait(|| !state.borrow().writes.is_empty() && probes.borrow().len() == 2)
            .await;
        settle_turns(16).await;
        send_bodies.notify_one();
        let completed = drive_until(DRIVE_LIMIT, || {
            probes
                .borrow()
                .iter()
                .all(|probe| probe.request_bytes.get() == 20_000 && probe.completed.get() == 1)
        })
        .await;
        assert!(completed, "both exchanges read their bodies and complete");
        settle_turns(16).await;
        *observed.borrow_mut() = (
            decode_server_frames_with_requests(&state.borrow().writes.concat(), 2).window_updates,
            written_transcript(&state).digest(),
        );
    })
    .await;
    observed.into_inner()
}

#[test]
fn completion_inside_the_owner_poll_is_not_folded() {
    assert_eq!(
        run_on_fold_worker(|context| Box::pin(in_poll_credit_updates(context))),
        (
            vec![connection_update(40_000)],
            (5, 12_085_380_293_361_417_345)
        ),
        "credit and completions inside owner polls keep their order; transcript recorded at \
         7eddd4a0506c"
    );
    let written = run_on_fold_worker(|context| {
        Box::pin(async move {
            let (stream, input, late) = post_then_late_data();
            let send_late = Arc::new(tokio::sync::Notify::new());
            let release_write = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&send_late), late),
            ]);
            // Holding the first write keeps write completion from polling the owner before the
            // late DATA arrives.
            state.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            // The exchange reads the whole buffered request inside its header poll, so the
            // request source completes during the owner poll that returns the terminal 200.
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.response_date = Some(TRANSCRIPT_DATE);
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            let written = Cell::new((0, 0));
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    probes.borrow().first().is_some_and(|probe| {
                        probe.request_bytes.get() == 5 && probe.response_complete.get() == 1
                    }) && state.borrow().write_gate_pending.is_some()
                })
                .await;
                let probe = Rc::clone(&probes.borrow()[0]);
                settle_turns(16).await;
                send_late.notify_one();
                settle_turns(16).await;
                release_write.notify_one();
                let settled = drive_until(DRIVE_LIMIT, || {
                    probe.completed.get() == 1 || !probe.cancelled.borrow().is_empty()
                })
                .await;
                assert!(settled, "the exchange either completes or is reset");
                settle_turns(16).await;
                let decoded = decode_server_frames(&state.borrow().writes.concat());
                assert_eq!(
                    (decoded.resets, probe.cancelled.borrow().clone()),
                    (vec![], vec![]),
                    "late DATA for a completed exchange is ignored, not reset"
                );
                assert_eq!(probe.completed.get(), 1);
                assert_eq!(decoded.header_statuses, [(stream, 200)]);
                written.set(written_transcript(&state).digest());
            })
            .await;
            written.get()
        })
    });
    assert_eq!(
        written,
        (3, 10_517_397_198_046_951_649),
        "transcript recorded at 7eddd4a0506c"
    );
}

/// Counts wakes of a session polled by hand.
#[derive(Default)]
struct HandWake {
    woken: AtomicBool,
    wakes: AtomicUsize,
}

impl std::task::Wake for HandWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.woken.store(true, Ordering::SeqCst);
    }
}

#[test]
fn credit_during_a_budget_yield_is_not_folded() {
    let (wakes, updates, written) = run_on_fold_worker(|context| {
        Box::pin(async move {
            let gate = Rc::new(OwnerPollGate {
                pending_visits: Some(RefCell::new(Vec::new())),
                ..OwnerPollGate::default()
            });
            // 64 one-byte bodies fill one turn budget of owner polls; then A (40,000 B) and
            // B (20,000 B). Nothing is ever read from the 64.
            let mut bodies = vec![(1, 16_384, None); 64];
            bodies.extend([(40_000, 16_384, None), (20_000, 16_384, None)]);
            let (transport, state) =
                FixtureTransport::new([FixtureRead::Data(peer_posts(&bodies))]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0); bodies.len()]);
            application.retain_request = true;
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let wake = Arc::new(HandWake::default());
            let waker = Waker::from(Arc::clone(&wake));
            let mut run = Box::pin(session.run());
            let updates = || decode_server_frames(&state.borrow().writes.concat()).window_updates;
            let outcome = eligibility_catch_unwind(async {
                let mut poll = || {
                    wake.woken.store(false, Ordering::SeqCst);
                    assert!(
                        run.as_mut()
                            .poll(&mut Context::from_waker(&waker))
                            .is_pending(),
                        "the held bodies keep the session running"
                    );
                };
                let mut settled = false;
                for _ in 0..DRIVE_LIMIT {
                    poll();
                    if !wake.woken.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                        if !wake.woken.load(Ordering::SeqCst) {
                            settled = true;
                            break;
                        }
                    }
                }
                assert!(settled, "the requests settle");
                assert!(bodies.iter().enumerate().all(|(ordinal, (length, ..))| {
                    held_body_buffered(&probes, ordinal, *length as u64)
                }));
                let wakers: Vec<_> = gate
                    .wakers
                    .borrow()
                    .iter()
                    .map(|waker| waker.as_ref().expect("each owner registered").clone())
                    .collect();
                assert_eq!(wakers.len(), 66);

                // Queue the 64, then read A outside the poll: its 40,000 B cross the 32,767 B
                // threshold, so its owner is queued behind them with that credit.
                gate.ready.set(true);
                for waker in &wakers[..64] {
                    waker.wake_by_ref();
                }
                let a = Rc::clone(&probes.borrow()[64]);
                let mut a = a.held_body.borrow_mut().take().unwrap();
                assert_eq!(read_buffered_body(&mut a), 40_000);
                wakers[64].wake_by_ref();

                // One poll spends its turn budget on the 64 and yields, with A still queued;
                // the held yield wakes nothing.
                hold_yields();
                poll();
                assert_eq!(
                    *gate.pending_visits.as_ref().unwrap().borrow(),
                    (0..64).collect::<Vec<_>>()
                );
                assert!(updates().is_empty());
                assert!(!wake.woken.load(Ordering::SeqCst));

                // B is read during the yield. A's credit still waits in its owner, so B's
                // 20,000 must reach the codec behind it.
                let b = Rc::clone(&probes.borrow()[65]);
                let mut b = b.held_body.borrow_mut().take().unwrap();
                let before = wake.wakes.load(Ordering::SeqCst);
                assert_eq!(read_buffered_body(&mut b), 20_000);
                let wakes = wake.wakes.load(Ordering::SeqCst) - before;
                release_held_yields();
                let mut settled = false;
                for _ in 0..DRIVE_LIMIT {
                    poll();
                    if !wake.woken.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                        if !wake.woken.load(Ordering::SeqCst) {
                            settled = true;
                            break;
                        }
                    }
                }
                assert!(settled, "A and B settle");
                assert_eq!(
                    *gate.pending_visits.as_ref().unwrap().borrow(),
                    (0..65).collect::<Vec<_>>()
                );
                drop((a, b));
                (wakes, updates())
            })
            .await;
            let written = written_transcript(&state).digest();
            handle.cancel();
            let result = tokio::time::timeout(Duration::from_secs(5), run.as_mut())
                .await
                .expect("the cancelled session joins");
            drop(run);
            let (wakes, updates) = match outcome {
                Ok(observed) => observed,
                Err(payload) => resume_unwind(payload),
            };
            assert_eq!(result.unwrap_err().code(), HttpErrorCode::CORO_CANCELLED);
            (wakes, updates, written)
        })
    });
    assert_eq!(
        (wakes, updates, written),
        (
            1,
            vec![connection_update(40_000)],
            (3, 10_471_453_527_199_536_510)
        ),
        "B's read wakes the session as before; transcript recorded at 7eddd4a0506c"
    );
}

/// Reads a retained 128 B body outside the poll of an idle session on `context`'s host and
/// returns the session wakes the read caused.
async fn small_body_read_wakes(context: WorkerContext) -> usize {
    let input = peer_request_bytes(HttpMethod::Post, &[b'q'; 128], None, PeerControl::None);
    let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
    let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
    application.retain_request = true;
    application.hold_headers = true;
    let (session, handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    let counts = SessionCounts::default();
    let wakes = Cell::new(usize::MAX);
    counted_exercise(session, &handle, &counts, async {
        complete_credit_wait(|| {
            held_body_buffered(&probes, 0, 128) && !state.borrow().writes.is_empty()
        })
        .await;
        settle_turns(16).await;
        let probe = Rc::clone(&probes.borrow()[0]);
        let mut body = probe.held_body.borrow_mut().take().unwrap();
        let before = counts.wakes();
        assert_eq!(read_buffered_body(&mut body), 128);
        wakes.set(counts.wakes() - before);
        drop(body);
    })
    .await;
    wakes.get()
}

#[test]
fn host_without_the_hook_never_folds() {
    assert_eq!(
        run_on_worker(|context| Box::pin(small_body_read_wakes(context))),
        1,
        "a host that does not run wakes in this iteration keeps the wake"
    );
}

#[test]
fn response_headers_after_folded_credit_still_wake_the_session() {
    run_on_fold_worker(|context| {
        Box::pin(async move {
            let gate = Rc::new(OwnerPollGate::default());
            let input = peer_request_bytes(HttpMethod::Post, &[b'q'; 128], None, PeerControl::None);
            let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.retain_request = true;
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    gate.polls.get() == 1
                        && held_body_buffered(&probes, 0, 128)
                        && !state.borrow().writes.is_empty()
                })
                .await;
                settle_turns(16).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                let mut body = probe.held_body.borrow_mut().take().unwrap();
                assert_eq!(read_buffered_body(&mut body), 128);
                drop(body);
                let polls = counts.polls.get();
                gate.release();
                let responded = drive_until(DRIVE_LIMIT, || {
                    decode_server_frames(&state.borrow().writes.concat()).header_statuses
                        == [(StreamId::new(1), 200)]
                        && probe.completed.get() == 1
                })
                .await;
                assert!(
                    responded,
                    "the response wake must reach the owner after its credit was folded"
                );
                let used = counts.polls.get() - polls;
                assert!(
                    used <= 2,
                    "the response and retirement take at most 2 session polls, took {used}"
                );
                assert_eq!(gate.polls.get(), 2);
                assert!(probe.cancelled.borrow().is_empty());
            })
            .await;
        })
    });
}

/// [`WireTranscript::digest`] of `wire` with the `date` value in each HEADERS frame replaced by
/// [`TRANSCRIPT_DATE`] (same literal encoding, frame length adjusted): the codec adds the current
/// `date` to a response the session synthesizes. Every other byte is digested as written.
fn dateless_digest(wire: &[u8]) -> (usize, u64) {
    let literal = |value: &[u8]| {
        let mut buffer = HpackEncodeBuffer::new(true);
        buffer.encode_literal(value);
        buffer.into_bytes()
    };
    let mut decoder = HpackDecoder::new();
    let mut normalized = Vec::with_capacity(wire.len());
    let mut rest = wire;
    while !rest.is_empty() {
        let length = u32::from_be_bytes([0, rest[0], rest[1], rest[2]]) as usize;
        let (frame, tail) = rest.split_at(FRAME_HEADER_SIZE + length);
        rest = tail;
        let (header, payload) = frame.split_at(FRAME_HEADER_SIZE);
        if header[3] != FrameType::HEADERS.as_u8() {
            normalized.extend_from_slice(frame);
            continue;
        }
        let mut payload = payload.to_vec();
        let block = decoder
            .decode(&payload)
            .expect("a written header block decodes");
        let date = HeaderName::from_code(HttpHeaderCode::Date);
        for field in block.headers.iter().filter(|field| field.name == date) {
            let written = literal(&field.value);
            let at = payload
                .windows(written.len())
                .position(|bytes| bytes == written.as_slice())
                .expect("the date is a literal in its header block");
            payload.splice(at..at + written.len(), literal(TRANSCRIPT_DATE));
        }
        normalized.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes()[1..]);
        normalized.extend_from_slice(&header[3..]);
        normalized.extend_from_slice(&payload);
    }
    WireTranscript::new(WireDirection::Written, &normalized).digest()
}

/// One HEADERS frame on stream 1 that ends the stream and carries `headers`.
fn headers_frame(headers: &[HpackHeader]) -> Vec<u8> {
    const END_HEADERS: u8 = 0x4;
    let block = HpackEncoder::new(true).encode(headers);
    let mut frame = u32::try_from(block.len()).unwrap().to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[
        FrameType::HEADERS.as_u8(),
        END_HEADERS | END_STREAM,
        0,
        0,
        0,
        1,
    ]);
    frame.extend_from_slice(&block);
    frame
}

#[test]
fn dateless_digest_ignores_only_the_date() {
    let digest = dateless_digest;
    let response = |date: &str, content_length: &str| {
        headers_frame(&[
            HpackHeader::new(":status", "500"),
            HpackHeader::new("content-length", content_length),
            HpackHeader::new("date", date),
        ])
    };
    let early = response("Mon, 01 Jan 2001 00:00:00 GMT", "0");
    let late = response("Wed, 29 Sep 2099 19:59:59 GMT", "0");
    assert_ne!(
        early.len(),
        late.len(),
        "the two dates' Huffman literals differ in length"
    );
    assert_eq!(
        digest(&early),
        digest(&late),
        "the date alone does not change the digest"
    );
    assert_ne!(
        digest(&early),
        digest(&response("Mon, 01 Jan 2001 00:00:00 GMT", "1")),
        "any other header byte changes the digest"
    );
}

#[test]
fn deferred_completion_still_sends_a_pending_error_response() {
    let written = run_on_fold_worker(|context| {
        Box::pin(async move {
            let send_eof = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(peer_incomplete_post_bytes(5)),
                FixtureRead::WaitDataEof(Arc::clone(&send_eof), Vec::new()),
            ]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.retain_request = true;
            application.hold_headers = true;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            let finished = RefCell::new(None);
            let run = async {
                let result = gated_run(session, &counts).await;
                *finished.borrow_mut() = Some(result);
            };
            let scenario = async {
                let outcome =
                    eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                        complete_credit_wait(|| held_body_buffered(&probes, 0, 5)).await;
                        let probe = Rc::clone(&probes.borrow()[0]);
                        let mut body = probe.held_body.borrow_mut().take().unwrap();
                        let event = body
                            .read_body_event(16)
                            .now_or_never()
                            .expect("the partial body is buffered")
                            .expect("the partial body reads");
                        assert_eq!(event.as_body().map(BufQueue::chain_length), Some(5));
                        assert!(!event.eom());
                        match body
                            .read_body_event(16)
                            .now_or_never()
                            .expect("the drained body yields its suspend event")
                            .expect("the suspend event reads")
                            .into_kind()
                        {
                            HttpBodyEventKind::Suspend(resume) => drop(resume),
                            _ => panic!("a drained incomplete body first yields its suspend event"),
                        }
                        {
                            let mut read = std::pin::pin!(body.read_body_event_unboxed(16));
                            poll_fn(|context| {
                                assert!(read.as_mut().poll(context).is_pending());
                                Poll::Ready(())
                            })
                            .await;
                            send_eof.notify_one();
                            complete_credit_wait(|| probe.cancelled.borrow().len() == 1).await;
                            settle_turns(16).await;
                            assert!(
                                decode_server_frames(&state.borrow().writes.concat())
                                    .header_streams
                                    .is_empty(),
                                "the error response waits for the admitted read"
                            );
                        }
                        let ended = drive_until(DRIVE_LIMIT, || finished.borrow().is_some()).await;
                        assert!(
                            ended,
                            "dropping the read completes the source, which must wake the owner \
                             of the pending 500"
                        );
                        let decoded = decode_server_frames(&state.borrow().writes.concat());
                        assert_eq!(decoded.header_statuses, [(StreamId::new(1), 500)]);
                        assert!(decoded.errors.is_empty());
                        assert_eq!(
                            probe.cancelled.borrow().as_slice(),
                            &[(HttpErrorCode::TRANSPORT_EOF, false)]
                        );
                        assert_eq!(probe.completed.get(), 0);
                        assert_eq!(probe.dropped.get(), 1);
                        drop(body);
                    }))
                    .await;
                if !matches!(&outcome, Ok(Ok(()))) {
                    handle.cancel();
                }
                outcome
            };
            let ((), outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(run, scenario)
            })
            .await
            .expect("the session and the scenario join");
            match outcome {
                Ok(result) => result.expect("the error response made progress"),
                Err(payload) => resume_unwind(payload),
            }
            finished
                .into_inner()
                .expect("the session finished")
                .expect("EOF closes the connection once the error response retires");
            dateless_digest(&state.borrow().writes.concat())
        })
    });
    assert_eq!(
        written,
        (3, 9_837_321_972_108_583_545),
        "transcript recorded at 7eddd4a0506c"
    );
}

#[test]
fn errored_request_body_still_wakes_the_session() {
    run_on_fold_worker(|context| {
        Box::pin(async move {
            let (transport, state) =
                FixtureTransport::new([FixtureRead::Data(peer_incomplete_post_bytes(5))]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.retain_request = true;
            application.hold_headers = true;
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let counts = SessionCounts::default();
            counted_exercise(session, &handle, &counts, async {
                complete_credit_wait(|| {
                    held_body_buffered(&probes, 0, 5) && !state.borrow().writes.is_empty()
                })
                .await;
                settle_turns(16).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                let mut body = probe.held_body.borrow_mut().take().unwrap();
                body.stop_reading(Some(HttpErrorCode::CANCEL));
                let reset = drive_until(DRIVE_LIMIT, || {
                    decode_server_frames(&state.borrow().writes.concat()).resets
                        == [(StreamId::new(1), H2ErrorCode::CANCEL)]
                })
                .await;
                assert!(
                    reset,
                    "a failed request body must wake its owner to reset the stream"
                );
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::CANCEL, false)]
                );
                drop(body);
            })
            .await;
        })
    });
}

#[test]
fn owner_poll_without_eom_reads_no_write_state() {
    run_on_worker(|context| {
        Box::pin(async move {
            let gate = Rc::new(OwnerPollGate::default());
            let (_, input) = ready_owner_requests(1);
            let (transport, _state) = FixtureTransport::new([FixtureRead::Data(input)]);
            let (mut application, _probes) = Application::new(vec![Reply::Body(0)]);
            application.owner_poll_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(session, &handle, &[], async {
                complete_credit_wait(|| gate.polls.get() == 1).await;
                let before = crate::owned_h2::owner_write_state_reads();
                gate.wakers.borrow()[0].as_ref().unwrap().wake_by_ref();
                complete_credit_wait(|| gate.polls.get() == 2).await;
                assert_eq!(
                    crate::owned_h2::owner_write_state_reads() - before,
                    0,
                    "an owner poll whose headers stay pending serializes no EOM or reset, so it \
                     reads no write state"
                );
            })
            .await;
        })
    });
}

/// One input of [`early_retirement_transcript`]; each runs in its own session.
#[derive(Clone, Copy, Debug)]
enum RetirementInput {
    /// Three GETs with 128-byte responses.
    Gets,
    /// A POST with a 40,000-byte body that the application drains inside the poll that
    /// produces the response's final body event.
    DrainedPost,
    /// A WINDOW_UPDATE, a PRIORITY, a DATA and a HEADERS frame on stream 1 while the write
    /// holding its response's END_STREAM is held.
    EndedStreamFrames,
    /// `MAX_CONCURRENT_STREAMS` 2, and a third request while two responses are held.
    ConcurrencyLimit,
    /// A GET, then a PING, then a GOAWAY.
    PingGoaway,
}

const RETIREMENT_POST_BODY: usize = 40_000;

/// Raw client frame bytes, so that frames on an ended stream bypass the peer codec's
/// stream state.
fn raw_frame(frame_type: FrameType, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = payload.len() as u32;
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend([frame_type.as_u8(), flags]);
    frame.extend(stream.to_be_bytes());
    frame.extend(payload);
    frame
}

/// The client preface and SETTINGS, then `gets` GETs on streams 1, 3, ...
fn retirement_requests(peer: &mut Http2Codec, gets: usize) -> Vec<u8> {
    let mut bytes = eligibility_frame(peer, EgressFrame::ConnectionPreface);
    bytes.extend(transcript_settings(peer));
    for _ in 0..gets {
        bytes.extend(retirement_get(peer));
    }
    bytes
}

fn retirement_get(peer: &mut Http2Codec) -> Vec<u8> {
    let stream = peer.create_stream();
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Get).unwrap();
    request.set_url(b"/retirement", false).unwrap();
    request.set_http_version(1, 1);
    request
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    eligibility_frame(
        peer,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: true,
            extra_headers: None,
        },
    )
}

/// Whether a written chunk carries a frame with END_STREAM on any stream.
fn carries_end_stream(write: &[u8]) -> bool {
    let wire = WireTranscript::new(WireDirection::Written, write);
    wire.count(FrameType::DATA, END_STREAM) + wire.count(FrameType::HEADERS, END_STREAM) != 0
}

/// Yields until `condition` holds, releasing held writes front to back every four yields
/// and stopping at the first write `hold` keeps. `false` if the bound is hit.
async fn pump_queued(
    probe: &Rc<RefCell<QueuedWriteProbe>>,
    hold: impl Fn(&[u8]) -> bool,
    condition: impl Fn() -> bool,
) -> bool {
    for _ in 0..1000 {
        if condition() {
            return true;
        }
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        let releasable = {
            let probe = probe.borrow();
            probe.accepted[probe.released..]
                .iter()
                .take_while(|write| !hold(write))
                .count()
        };
        if releasable != 0 {
            release_queued_writes(probe, releasable);
        }
    }
    condition()
}

fn queued_transcript(probe: &RefCell<QueuedWriteProbe>) -> WireTranscript {
    WireTranscript::new(WireDirection::Written, &probe.borrow().accepted.concat())
}

/// Runs `input` over a [`QueuedFixtureTransport`] whose writes complete only when the
/// script releases them. Returns the written transcript, how the session ended, and how
/// many exchanges had completed when the frames held behind an END_STREAM write arrived.
async fn early_retirement_transcript(
    context: WorkerContext,
    input: RetirementInput,
    saturation: Option<Rc<Cell<bool>>>,
) -> (WireTranscript, Result<(), HttpErrorCode>, usize) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let gate = Arc::new(tokio::sync::Notify::new());
    let end = Arc::new(tokio::sync::Notify::new());
    let (reads, replies) = match input {
        RetirementInput::Gets => (
            vec![
                FixtureRead::Data(retirement_requests(&mut peer, 3)),
                FixtureRead::WaitDataEof(Arc::clone(&end), Vec::new()),
            ],
            3,
        ),
        RetirementInput::DrainedPost => {
            let mut bytes = retirement_requests(&mut peer, 0);
            let stream = peer.create_stream();
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Post).unwrap();
            request.set_url(b"/retirement", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            request.headers_mut().set(
                HttpHeaderCode::ContentLength,
                RETIREMENT_POST_BODY.to_string().as_bytes(),
            );
            bytes.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
            ));
            for (index, chunk) in vec![b'q'; RETIREMENT_POST_BODY].chunks(16_000).enumerate() {
                bytes.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Body {
                        stream,
                        body: queue(chunk),
                        padding: None,
                        end_stream: (index + 1) * 16_000 >= RETIREMENT_POST_BODY,
                    },
                ));
            }
            (
                vec![
                    FixtureRead::Data(bytes),
                    FixtureRead::WaitDataEof(Arc::clone(&end), Vec::new()),
                ],
                1,
            )
        }
        RetirementInput::EndedStreamFrames => {
            // `:method GET`, `:scheme http`, `:path /` and a literal `:authority` without
            // indexing: no dynamic-table change.
            let mut block = vec![0x82, 0x86, 0x84, 0x01, 11];
            block.extend(b"example.com");
            let frames = [
                raw_frame(FrameType::WINDOW_UPDATE, 0, 1, &1000u32.to_be_bytes()),
                raw_frame(FrameType::PRIORITY, 0, 1, &[0, 0, 0, 0, 15]),
                raw_frame(FrameType::DATA, 0, 1, b"late"),
                raw_frame(FrameType::HEADERS, 0x4 | END_STREAM, 1, &block),
            ]
            .concat();
            (
                vec![
                    FixtureRead::Data(retirement_requests(&mut peer, 1)),
                    FixtureRead::WaitData(Arc::clone(&gate), frames),
                    FixtureRead::WaitDataEof(Arc::clone(&end), Vec::new()),
                ],
                1,
            )
        }
        RetirementInput::ConcurrencyLimit => {
            let requests = retirement_requests(&mut peer, 2);
            let third = retirement_get(&mut peer);
            (
                vec![
                    FixtureRead::Data(requests),
                    FixtureRead::WaitData(Arc::clone(&gate), third),
                    FixtureRead::WaitDataEof(Arc::clone(&end), Vec::new()),
                ],
                3,
            )
        }
        RetirementInput::PingGoaway => {
            let requests = retirement_requests(&mut peer, 1);
            let ping = eligibility_frame(
                &mut peer,
                EgressFrame::PingRequest {
                    data: Some(TRANSCRIPT_PING),
                },
            );
            let goaway = eligibility_frame(
                &mut peer,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(0),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
            );
            (
                vec![
                    FixtureRead::Data(requests),
                    FixtureRead::WaitData(Arc::clone(&gate), ping),
                    FixtureRead::WaitDataEof(Arc::clone(&end), goaway),
                ],
                1,
            )
        }
    };
    let transport = QueuedFixtureTransport::reporting(reads, saturation, false);
    let queue = Rc::clone(&transport.probe);
    let (mut application, probes) = Application::new(vec![Reply::Body(TRANSCRIPT_BODY); replies]);
    application.response_date = Some(TRANSCRIPT_DATE);
    application.drain_request_last = matches!(input, RetirementInput::DrainedPost);
    let mut config = options(context, transport);
    if matches!(input, RetirementInput::ConcurrencyLimit) {
        config.http2.settings.push(CodecSetting {
            identifier: SettingsId::MaxConcurrentStreams.as_u16(),
            value: 2,
        });
    }
    let (session, _handle) = OwnedH2Session::new(application, config).unwrap();
    let completed = |count: usize| {
        let probes = probes.borrow();
        probes.len() == count && probes.iter().all(|probe| probe.completed.get() == 1)
    };
    let ended =
        |count: usize| queued_transcript(&queue).count(FrameType::DATA, END_STREAM) == count;
    let completed_while_held = Cell::new(0);
    let held_arrival = || {
        completed_while_held.set(
            probes
                .borrow()
                .iter()
                .filter(|probe| probe.completed.get() == 1)
                .count(),
        );
        gate.notify_one();
    };
    let script = async {
        match input {
            RetirementInput::Gets | RetirementInput::DrainedPost => {
                pump_queued(&queue, |_| false, || completed(replies)).await;
            }
            RetirementInput::EndedStreamFrames => {
                pump_queued(&queue, carries_end_stream, || ended(1)).await;
                held_arrival();
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                }
                pump_queued(&queue, |_| false, || completed(1)).await;
            }
            RetirementInput::ConcurrencyLimit => {
                pump_queued(&queue, carries_end_stream, || ended(2)).await;
                held_arrival();
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                }
                pump_queued(&queue, |_| false, || completed(3)).await;
            }
            RetirementInput::PingGoaway => {
                pump_queued(&queue, |_| false, || completed(1)).await;
                gate.notify_one();
                pump_queued(
                    &queue,
                    |_| false,
                    || queued_transcript(&queue).count(FrameType::PING, ACK) == 1,
                )
                .await;
            }
        }
        end.notify_one();
        finish_queued_writes(&queue);
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), script)
    })
    .await
    .expect("the early-retirement fixture finishes");
    let run = run.map(drop).map_err(|error| error.code());
    (queued_transcript(&queue), run, completed_while_held.get())
}

const RETIREMENT_INPUTS: [RetirementInput; 5] = [
    RetirementInput::Gets,
    RetirementInput::DrainedPost,
    RetirementInput::EndedStreamFrames,
    RetirementInput::ConcurrencyLimit,
    RetirementInput::PingGoaway,
];

#[test]
fn downstream_transcripts_are_unchanged_by_early_retirement() {
    // `None` keeps the default answer, so every stream keeps its fence; `Some(false)` lets
    // eligible streams retire at END_STREAM serialization.
    for (unsaturated, completed_while_held) in [(false, [0, 0, 0, 0, 0]), (true, [0, 0, 1, 2, 0])] {
        let runs: Vec<_> = RETIREMENT_INPUTS
            .into_iter()
            .map(|input| {
                let (written, run, completed) = run_on_worker(move |context| {
                    let saturation = unsaturated.then(|| Rc::new(Cell::new(false)));
                    Box::pin(early_retirement_transcript(context, input, saturation))
                });
                (written.digest(), run, completed, written)
            })
            .collect();
        assert_eq!(
            runs.iter()
                .map(|(_, _, completed, _)| *completed)
                .collect::<Vec<_>>(),
            completed_while_held,
            "exchanges completed when the held-back input arrives (unsaturated: {unsaturated})"
        );
        let recorded: Vec<_> = runs
            .iter()
            .map(|(digest, run, _, _)| (*digest, *run))
            .collect();
        assert_eq!(
            recorded,
            [
                ((8, 234_198_259_703_760_995), Ok(())),
                ((5, 12_245_000_780_145_702_999), Ok(())),
                ((5, 5_501_662_039_820_430_337), Ok(())),
                ((8, 15_476_332_832_309_872_086), Ok(())),
                ((6, 11_756_098_851_954_770_615), Ok(())),
            ],
            "downstream transcripts recorded at 9d58c4aa0c67 (unsaturated: {unsaturated}); \
             written (type, flags, stream, length): {:?}",
            runs.iter()
                .map(|(_, _, _, written)| written
                    .frames
                    .iter()
                    .map(|frame| (frame.frame_type, frame.flags, frame.stream, frame.length))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn write_queue_defaults_to_saturated() {
    use crate::uniplex::UniplexWriteQueue;
    let queue = FixtureWriteQueue {
        probe: Rc::default(),
        cancellation: CancellationToken::never(),
    };
    assert!(
        queue.is_saturated(),
        "a queue that cannot report saturation keeps every stream's write fence"
    );
}

/// A downstream session over a [`ReportingWriteQueue`] answering from `saturation`, whose
/// writes the script releases; each exchange answers 128 bytes.
fn reporting_session(
    context: WorkerContext,
    reads: Vec<FixtureRead>,
    replies: usize,
    saturation: &Rc<Cell<bool>>,
    configure: impl FnOnce(&mut Application),
) -> (
    OwnedH2Session<Application>,
    OwnedH2Handle,
    Rc<RefCell<QueuedWriteProbe>>,
    Rc<RefCell<Vec<Rc<Probe>>>>,
) {
    let transport = QueuedFixtureTransport::reporting(reads, Some(Rc::clone(saturation)), false);
    let queue = Rc::clone(&transport.probe);
    let (mut application, probes) = Application::new(vec![Reply::Body(TRANSCRIPT_BODY); replies]);
    application.response_date = Some(TRANSCRIPT_DATE);
    configure(&mut application);
    let (session, handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    (session, handle, queue, probes)
}

/// [`reporting_session`] for one GET.
fn reporting_get(
    context: WorkerContext,
    saturated: bool,
) -> (
    OwnedH2Session<Application>,
    OwnedH2Handle,
    Rc<RefCell<QueuedWriteProbe>>,
    Rc<RefCell<Vec<Rc<Probe>>>>,
) {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    reporting_session(
        context,
        vec![FixtureRead::Data(retirement_requests(&mut peer, 1))],
        1,
        &Rc::new(Cell::new(saturated)),
        |_| {},
    )
}

/// Releases every held write.
fn release_all_queued_writes(queue: &Rc<RefCell<QueuedWriteProbe>>) {
    let held = {
        let queue = queue.borrow();
        queue.accepted.len() - queue.released
    };
    release_queued_writes(queue, held);
}

/// Whether the queue has accepted a write carrying stream `stream`'s END_STREAM.
fn end_stream_accepted(queue: &RefCell<QueuedWriteProbe>, stream: u32) -> bool {
    queued_transcript(queue).frames.iter().any(|frame| {
        frame.stream == stream
            && frame.flags & END_STREAM != 0
            && (frame.frame_type == FrameType::DATA.as_u8()
                || frame.frame_type == FrameType::HEADERS.as_u8())
    })
}

#[test]
fn unsaturated_downstream_stream_retires_before_its_write_completes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, queue, probes) = reporting_get(context, false);
            exercise_queued(session, &handle, &queue, async {
                complete_credit_wait(|| end_stream_accepted(&queue, 1)).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                drive_until(DRIVE_LIMIT, || probe.completed.get() == 1).await;
                assert_eq!(queue.borrow().released, 0, "every write is still held");
                assert_eq!(
                    probe.completed.get(),
                    1,
                    "the unsaturated stream completes before its END_STREAM is written"
                );
                assert!(probe.cancelled.borrow().is_empty());
                assert_eq!(probe.dropped.get(), 1, "the retired exchange is dropped");
                assert_eq!(queue.borrow().dropped, 0, "the write queue lives on");
            })
            .await;
        })
    });
}

#[test]
fn saturated_downstream_stream_keeps_its_write_fence() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, queue, probes) = reporting_get(context, true);
            exercise_queued(session, &handle, &queue, async {
                complete_credit_wait(|| end_stream_accepted(&queue, 1)).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                settle_turns(DRIVE_LIMIT).await;
                assert_eq!(
                    probe.completed.get(),
                    0,
                    "a saturated queue keeps the stream behind its write fence"
                );
                release_all_queued_writes(&queue);
                let completed = drive_until(DRIVE_LIMIT, || probe.completed.get() == 1).await;
                assert!(
                    completed,
                    "the stream retires once the receipt covering its END_STREAM is consumed"
                );
                assert_eq!(queue.borrow().retired, queue.borrow().accepted.len());
                assert!(probe.cancelled.borrow().is_empty());
            })
            .await;
        })
    });
}

#[test]
fn own_write_saturating_the_transport_does_not_hold_the_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let preface = retirement_requests(&mut peer, 0);
            let mut first = Vec::new();
            let stream = peer.create_stream();
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Post).unwrap();
            request.set_url(b"/retirement", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            first.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
            ));
            first.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Body {
                    stream,
                    body: queue(b"body"),
                    padding: None,
                    end_stream: true,
                },
            ));
            let second = retirement_get(&mut peer);
            let first_gate = Arc::new(tokio::sync::Notify::new());
            let gate = Arc::new(tokio::sync::Notify::new());
            let saturation = Rc::new(Cell::new(false));
            let (session, handle, queue, probes) = reporting_session(
                context,
                vec![
                    FixtureRead::Data(preface),
                    FixtureRead::WaitData(Arc::clone(&first_gate), first),
                    FixtureRead::WaitData(Arc::clone(&gate), second),
                ],
                2,
                &saturation,
                |application| application.retain_request = true,
            );
            exercise_queued(session, &handle, &queue, async {
                // The connection's own SETTINGS and their ACK are written first.
                let settled = pump_queued(
                    &queue,
                    |_| false,
                    || {
                        let queue_state = queue.borrow();
                        queue_state.released == queue_state.accepted.len()
                            && queued_transcript(&queue).count(FrameType::SETTINGS, ACK) == 1
                    },
                )
                .await;
                assert!(settled, "the connection preface exchange completes");
                let released = queue.borrow().released;
                queue.borrow_mut().saturate_on_write = true;
                first_gate.notify_one();
                complete_credit_wait(|| end_stream_accepted(&queue, 1)).await;
                assert!(
                    saturation.get(),
                    "the write carrying END_STREAM saturated the queue"
                );
                let first = Rc::clone(&probes.borrow()[0]);
                assert_eq!(
                    first.completed.get(),
                    0,
                    "the request body is not consumed yet"
                );
                let mut body = first.held_body.borrow_mut().take().unwrap();
                while !body.read_body_event(u32::MAX).await.unwrap().eom() {}
                drop(body);
                drive_until(DRIVE_LIMIT, || first.completed.get() == 1).await;
                assert_eq!(
                    queue.borrow().released,
                    released,
                    "the write carrying END_STREAM is still held"
                );
                assert_eq!(
                    first.completed.get(),
                    1,
                    "saturation is sampled when END_STREAM is serialized, before the stream's \
                     own write saturates the queue"
                );
                gate.notify_one();
                complete_credit_wait(|| end_stream_accepted(&queue, 3)).await;
                let second = Rc::clone(&probes.borrow()[1]);
                settle_turns(DRIVE_LIMIT).await;
                assert_eq!(
                    second.completed.get(),
                    0,
                    "a stream whose END_STREAM is serialized behind a saturating write keeps \
                     its fence"
                );
                release_all_queued_writes(&queue);
                let completed = drive_until(DRIVE_LIMIT, || second.completed.get() == 1).await;
                assert!(completed, "the fenced stream retires at its receipt");
            })
            .await;
        })
    });
}

#[test]
fn byte_event_stream_keeps_its_write_fence() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let events = Rc::new(ByteEventProbe::default());
            let (session, handle, queue, probes) = reporting_session(
                context,
                vec![FixtureRead::Data(retirement_requests(&mut peer, 1))],
                1,
                &Rc::new(Cell::new(false)),
                |application| {
                    application.byte_events = Some(Rc::clone(&events));
                    application.transport_write_events = true;
                },
            );
            exercise_queued(session, &handle, &queue, async {
                complete_credit_wait(|| end_stream_accepted(&queue, 1)).await;
                let probe = Rc::clone(&probes.borrow()[0]);
                settle_turns(DRIVE_LIMIT).await;
                assert_eq!(
                    probe.completed.get(),
                    0,
                    "a stream that registered a byte event keeps its write fence"
                );
                release_all_queued_writes(&queue);
                let completed = drive_until(DRIVE_LIMIT, || probe.completed.get() == 1).await;
                assert!(completed, "the stream retires at its receipt");
                assert!(
                    events
                        .delivered
                        .borrow()
                        .iter()
                        .any(|event| event.event_type == ByteEventType::TransportWrite),
                    "the byte event is delivered before the exchange completes"
                );
                assert!(events.cancelled.borrow().is_empty());
            })
            .await;
        })
    });
}

#[test]
fn peer_reset_after_retirement_is_ignored() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let requests = retirement_requests(&mut peer, 1);
            let reset = raw_frame(
                FrameType::RST_STREAM,
                0,
                1,
                &H2ErrorCode::CANCEL.as_u32().to_be_bytes(),
            );
            let next = retirement_get(&mut peer);
            let reset_gate = Arc::new(tokio::sync::Notify::new());
            let next_gate = Arc::new(tokio::sync::Notify::new());
            let (session, handle, queue, probes) = reporting_session(
                context,
                vec![
                    FixtureRead::Data(requests),
                    FixtureRead::WaitData(Arc::clone(&reset_gate), reset),
                    FixtureRead::WaitData(Arc::clone(&next_gate), next),
                ],
                2,
                &Rc::new(Cell::new(false)),
                |_| {},
            );
            exercise_queued(session, &handle, &queue, async {
                complete_credit_wait(|| end_stream_accepted(&queue, 1)).await;
                let first = Rc::clone(&probes.borrow()[0]);
                drive_until(DRIVE_LIMIT, || first.completed.get() == 1).await;
                assert_eq!(queue.borrow().released, 0, "every write is still held");
                assert_eq!(
                    first.completed.get(),
                    1,
                    "the stream retired at END_STREAM serialization"
                );
                let before_reset = queue.borrow().accepted.len();
                reset_gate.notify_one();
                settle_turns(DRIVE_LIMIT).await;
                release_all_queued_writes(&queue);
                next_gate.notify_one();
                let answered = pump_queued(
                    &queue,
                    |_| false,
                    || {
                        probes
                            .borrow()
                            .get(1)
                            .is_some_and(|probe| probe.completed.get() == 1)
                    },
                )
                .await;
                assert!(answered, "a following request on stream 3 is answered");
                assert!(
                    first.cancelled.borrow().is_empty(),
                    "the peer's RST_STREAM does not reach the retired exchange"
                );
                assert_eq!(first.completed.get(), 1);
                let after_reset = WireTranscript::new(
                    WireDirection::Written,
                    &queue.borrow().accepted[before_reset..].concat(),
                );
                assert!(
                    after_reset.frames.iter().all(|frame| frame.stream != 1),
                    "nothing is written for the retired stream after the RST: {after_reset:?}"
                );
            })
            .await;
        })
    });
}

#[test]
fn write_failure_after_retirement_leaves_the_stream_complete() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut requests = retirement_requests(&mut peer, 1);
            let open = peer.create_stream();
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Post).unwrap();
            request.set_url(b"/open", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            requests.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Headers {
                    stream: open,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
            ));
            let (session, _handle, queue, probes) = reporting_session(
                context,
                vec![FixtureRead::Data(requests)],
                2,
                &Rc::new(Cell::new(false)),
                |_| {},
            );
            let ended = Cell::new(false);
            let run = async {
                let result = session.run().await;
                ended.set(true);
                result
            };
            let script = async {
                complete_credit_wait(|| end_stream_accepted(&queue, 1)).await;
                let first = Rc::clone(&probes.borrow()[0]);
                drive_until(DRIVE_LIMIT, || first.completed.get() == 1).await;
                assert_eq!(queue.borrow().released, 0, "every write is still held");
                assert_eq!(
                    first.completed.get(),
                    1,
                    "the stream retired at END_STREAM serialization"
                );
                queue.borrow_mut().fail_released = true;
                pump_queued(&queue, |_| false, || ended.get()).await;
            };
            let (result, ()) =
                tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(run, script) })
                    .await
                    .expect("the failed connection ends");
            assert_eq!(
                result.expect_err("the held write failed").code(),
                HttpErrorCode::TRANSPORT_WRITE_ERROR
            );
            let probes = probes.borrow();
            assert_eq!(probes[0].completed.get(), 1);
            assert!(
                probes[0].cancelled.borrow().is_empty(),
                "the failed write does not reach the retired exchange"
            );
            assert_eq!(probes[1].completed.get(), 0);
            assert_eq!(
                probes[1].cancelled.borrow().as_slice(),
                &[(HttpErrorCode::TRANSPORT_WRITE_ERROR, false)],
                "the open stream is reset by the failure"
            );
        })
    });
}

/// A downstream session with no stream left does not arm the writer for the completion of a
/// write nothing waits for (`DIV-028`). Every write completes at the transport's flush, after
/// the session's poll, and the queue is unsaturated, so each GET's stream retires when its
/// END_STREAM is serialized. The third of three sequential GETs then takes two polls and two
/// wakes: the read's wake, and the request source's self-wake inside the poll that reads the
/// GET. No wake follows the response's write. The receipt it leaves is consumed before the
/// session ends at EOF.
#[test]
fn retired_downstream_stream_needs_no_write_completion_poll() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let gates: [Arc<tokio::sync::Notify>; 4] =
                std::array::from_fn(|_| Arc::new(tokio::sync::Notify::new()));
            let mut reads = vec![FixtureRead::Data(arming_preface(&mut peer))];
            for gate in &gates[..3] {
                let (_, get) = arming_get(&mut peer);
                reads.push(FixtureRead::WaitData(Arc::clone(gate), get));
            }
            reads.push(FixtureRead::WaitDataEof(Arc::clone(&gates[3]), Vec::new()));
            let transport = ScriptedWriteTransport::new(
                reads,
                [],
                ScriptedWrite::Complete,
                INLINE_OUTSTANDING_WRITES,
                None,
            );
            transport.state.borrow_mut().saturated = false;
            let state = Rc::clone(&transport.state);
            let completes = Rc::new(Cell::new(0));
            let (session, _handle) = OwnedH2Session::new(
                ArmingApplication::new(&completes),
                options(context, transport),
            )
            .unwrap();
            let (run, counts) = CountedWakes::new(session.run());
            let script = async {
                let counted = || (counts.polls(), counts.wakes(), counts.in_poll_wakes());
                let mut answered = Vec::new();
                let mut per_request = Vec::new();
                for (index, gate) in gates[..3].iter().enumerate() {
                    let before = counted();
                    gate.notify_one();
                    answered.push(
                        writer_drive_until(|| {
                            completes.get() == index + 1
                                && state.borrow().frames(FrameType::HEADERS, END_STREAM)
                                    == index + 1
                        })
                        .await,
                    );
                    writer_settle().await;
                    let after = counted();
                    per_request.push((after.0 - before.0, after.1 - before.1, after.2 - before.2));
                }
                gates[3].notify_one();
                (answered, per_request)
            };
            let (result, (answered, per_request)) =
                tokio::time::timeout(Duration::from_secs(10), async {
                    let joined = tokio::join!(run, script);
                    join_scripted_tasks(&state).await;
                    joined
                })
                .await
                .expect("the session ends at EOF");
            assert_eq!(answered, [true; 3], "every GET is answered and retires");
            assert_eq!(
                per_request[2],
                (2, 2, 1),
                "the third GET's session (polls, wakes, in-poll wakes): no wake follows its \
                 response's write"
            );
            let summary = result.expect("EOF at idle ends the session cleanly");
            assert_eq!(
                summary.bytes_written, summary.bytes_scheduled,
                "the receipt the last retired stream left is consumed before the session ends"
            );
            assert_eq!(completes.get(), 3);
        })
    });
}

const END_HEADERS: u8 = 0x4;
const GRPC_OK: &[(&str, &str)] = &[("grpc-status", "0")];

/// One written frame with its payload bytes.
#[derive(Debug)]
struct RawFrame {
    frame_type: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

/// Splits written bytes into whole frames, keeping each payload.
fn raw_frames(wire: &[u8]) -> Vec<RawFrame> {
    let mut frames = Vec::new();
    let mut rest = wire;
    while !rest.is_empty() {
        let length = u32::from_be_bytes([0, rest[0], rest[1], rest[2]]) as usize;
        let (frame, tail) = rest.split_at(FRAME_HEADER_SIZE + length);
        frames.push(RawFrame {
            frame_type: frame[3],
            flags: frame[4],
            stream: u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]) & 0x7fff_ffff,
            payload: frame[FRAME_HEADER_SIZE..].to_vec(),
        });
        rest = tail;
    }
    frames
}

/// Every HEADERS frame of `wire` in order, as `(stream, flags, block, fields)`, decoded
/// with one decoder so later blocks see the dynamic table earlier blocks built.
fn decoded_header_blocks(wire: &[u8]) -> Vec<(u32, u8, Vec<u8>, Vec<HpackHeader>)> {
    let mut decoder = HpackDecoder::new();
    raw_frames(wire)
        .into_iter()
        .filter(|frame| frame.frame_type == FrameType::HEADERS.as_u8())
        .map(|frame| {
            assert_eq!(
                frame.flags & !(END_STREAM | END_HEADERS),
                0,
                "a written HEADERS frame carries no padding or priority"
            );
            assert_ne!(frame.flags & END_HEADERS, 0, "no CONTINUATION follows");
            let fields = decoder
                .decode(&frame.payload)
                .expect("a written header block decodes")
                .headers;
            (frame.stream, frame.flags, frame.payload, fields)
        })
        .collect()
}

/// `(type, flags)` of each frame written on `stream`.
fn stream_frame_kinds(wire: &[u8], stream: u32) -> Vec<(u8, u8)> {
    raw_frames(wire)
        .iter()
        .filter(|frame| frame.stream == stream)
        .map(|frame| (frame.frame_type, frame.flags))
        .collect()
}

/// The fields of the HEADERS frame that ends `stream`.
fn trailer_fields(wire: &[u8], stream: u32) -> Option<Vec<HpackHeader>> {
    decoded_header_blocks(wire)
        .into_iter()
        .find(|(id, flags, _, _)| *id == stream && flags & END_STREAM != 0)
        .map(|(_, _, _, fields)| fields)
}

/// Runs one GET on stream 1 whose exchange answers `reply`, and returns the bytes written
/// once the exchange has completed and the session has settled.
async fn single_reply_wire(context: WorkerContext, reply: Reply) -> Vec<u8> {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let (transport, state) =
        FixtureTransport::new([FixtureRead::Data(retirement_requests(&mut peer, 1))]);
    let (application, probes) = Application::new(vec![reply]);
    let (session, handle) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    exercise(session, &handle, &[], async {
        complete_credit_wait(|| {
            probes
                .borrow()
                .first()
                .is_some_and(|probe| probe.completed.get() == 1)
        })
        .await;
        settle_turns(DRIVE_LIMIT).await;
    })
    .await;
    let probe = Rc::clone(&probes.borrow()[0]);
    assert_eq!(probe.response_complete.get(), 1);
    assert!(probe.cancelled.borrow().is_empty());
    state.borrow().writes.concat()
}

#[test]
fn response_trailers_end_the_stream_on_their_headers_frame() {
    let wire = run_on_worker(|context| {
        Box::pin(single_reply_wire(
            context,
            Reply::BodyThenTrailers(5, GRPC_OK),
        ))
    });
    let headers = FrameType::HEADERS.as_u8();
    assert_eq!(
        stream_frame_kinds(&wire, 1),
        [
            (headers, END_HEADERS),
            (FrameType::DATA.as_u8(), 0),
            (headers, END_HEADERS | END_STREAM),
        ],
        "head, body, then trailers carrying END_STREAM, and nothing after them"
    );
    let blocks: Vec<_> = decoded_header_blocks(&wire)
        .into_iter()
        .filter(|(stream, _, _, _)| *stream == 1)
        .collect();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[1].3, [HpackHeader::new("grpc-status", "0")]);
}

const GRPC_MESSAGE: &str = "upstream request timed out after 15000ms";
const GRPC_FAILURE: &[(&str, &str)] = &[("grpc-status", "9"), ("grpc-message", GRPC_MESSAGE)];

#[test]
fn response_trailers_keep_field_order_and_values() {
    assert_eq!(GRPC_MESSAGE.len(), 40);
    let wire = run_on_worker(|context| {
        Box::pin(single_reply_wire(
            context,
            Reply::BodyThenTrailers(5, GRPC_FAILURE),
        ))
    });
    assert_eq!(
        trailer_fields(&wire, 1).expect("a HEADERS frame ends the stream"),
        [
            HpackHeader::new("grpc-status", "9"),
            HpackHeader::new("grpc-message", GRPC_MESSAGE),
        ]
    );
}

fn completions(probes: &RefCell<Vec<Rc<Probe>>>) -> Vec<usize> {
    probes
        .borrow()
        .iter()
        .map(|probe| probe.completed.get())
        .collect()
}

/// Two GETs over a [`ReportingWriteQueue`] reporting `saturated`: stream 1 ends on
/// trailers, stream 3 on a DATA END_STREAM. Returns the exchanges' completions while every
/// write carrying an END_STREAM is held, then after every write is released.
async fn trailers_retirement(context: WorkerContext, saturated: bool) -> [Vec<usize>; 2] {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let (session, handle, queue, probes) = reporting_session(
        context,
        vec![FixtureRead::Data(retirement_requests(&mut peer, 2))],
        2,
        &Rc::new(Cell::new(saturated)),
        |application| {
            application.replies = vec![
                Reply::BodyThenTrailers(5, GRPC_OK),
                Reply::Body(TRANSCRIPT_BODY),
            ];
        },
    );
    let held = RefCell::new(Vec::new());
    let released = RefCell::new(Vec::new());
    exercise_queued(session, &handle, &queue, async {
        let ended = pump_queued(&queue, carries_end_stream, || {
            end_stream_accepted(&queue, 1) && end_stream_accepted(&queue, 3)
        })
        .await;
        assert!(ended, "both streams' END_STREAM writes are accepted");
        settle_turns(DRIVE_LIMIT).await;
        {
            let queue = queue.borrow();
            let first_end = queue
                .accepted
                .iter()
                .position(|write| carries_end_stream(write))
                .unwrap();
            assert!(
                queue.released <= first_end,
                "no write carrying an END_STREAM has completed"
            );
        }
        *held.borrow_mut() = completions(&probes);
        release_all_queued_writes(&queue);
        drive_until(DRIVE_LIMIT, || completions(&probes) == [1, 1]).await;
        *released.borrow_mut() = completions(&probes);
    })
    .await;
    let trailers = queued_transcript(&queue)
        .frames
        .into_iter()
        .filter(|frame| frame.stream == 1 && frame.flags & END_STREAM != 0)
        .map(|frame| frame.frame_type)
        .collect::<Vec<_>>();
    assert_eq!(
        trailers,
        [FrameType::HEADERS.as_u8()],
        "stream 1 ends on its trailers HEADERS frame"
    );
    for probe in probes.borrow().iter() {
        assert!(probe.cancelled.borrow().is_empty());
        assert_eq!(probe.dropped.get(), 1);
    }
    [held.into_inner(), released.into_inner()]
}

#[test]
fn trailers_end_stream_retires_early_when_unsaturated() {
    let unsaturated = run_on_worker(|context| Box::pin(trailers_retirement(context, false)));
    assert_eq!(
        unsaturated,
        [vec![1, 1], vec![1, 1]],
        "unsaturated: the trailers-ended stream retires at serialization, as the DATA-ended \
         sibling does"
    );
    let saturated = run_on_worker(|context| Box::pin(trailers_retirement(context, true)));
    assert_eq!(
        saturated,
        [vec![0, 0], vec![1, 1]],
        "saturated: both streams wait for the physical write"
    );
}

#[test]
fn response_trailers_wait_behind_flow_controlled_data() {
    const WINDOW: usize = 16;
    const BODY: usize = 40;
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            peer.egress_settings_mut()
                .set(SettingsId::InitialWindowSize, WINDOW as u64);
            let input = retirement_requests(&mut peer, 1);
            let credit = eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::new(1),
                    amount: 1000,
                },
            );
            let send_credit = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&send_credit), credit),
            ]);
            let (application, probes) =
                Application::new(vec![Reply::BodyThenTrailers(BODY, GRPC_OK)]);
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let headers = FrameType::HEADERS.as_u8();
            let data = FrameType::DATA.as_u8();
            exercise(session, &handle, &[Arc::clone(&send_credit)], async {
                complete_credit_wait(|| {
                    decode_server_frames(&state.borrow().writes.concat()).body_bytes == WINDOW
                })
                .await;
                settle_turns(DRIVE_LIMIT).await;
                let blocked = state.borrow().writes.concat();
                assert_eq!(
                    stream_frame_kinds(&blocked, 1),
                    [(headers, END_HEADERS), (data, 0)],
                    "no trailers frame is written while body bytes wait for credit"
                );
                let probe = Rc::clone(&probes.borrow()[0]);
                assert_eq!(probe.response_complete.get(), 0);
                send_credit.notify_one();
                complete_credit_wait(|| probe.completed.get() == 1).await;
            })
            .await;
            let wire = state.borrow().writes.concat();
            assert_eq!(
                stream_frame_kinds(&wire, 1),
                [
                    (headers, END_HEADERS),
                    (data, 0),
                    (data, 0),
                    (headers, END_HEADERS | END_STREAM),
                ],
                "the window's DATA, the remaining DATA, then the trailers"
            );
            let lengths: Vec<_> = raw_frames(&wire)
                .into_iter()
                .filter(|frame| frame.stream == 1 && frame.frame_type == data)
                .map(|frame| frame.payload.len())
                .collect();
            assert_eq!(lengths, [WINDOW, BODY - WINDOW]);
            assert_eq!(
                trailer_fields(&wire, 1).unwrap(),
                [HpackHeader::new("grpc-status", "0")]
            );
        })
    });
}

#[test]
fn trailer_blocks_share_the_dynamic_table() {
    let wire = run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let first = retirement_requests(&mut peer, 1);
            let second = retirement_get(&mut peer);
            let send_second = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(first),
                FixtureRead::WaitData(Arc::clone(&send_second), second),
            ]);
            let (application, probes) = Application::new(vec![
                Reply::BodyThenTrailers(5, GRPC_OK),
                Reply::BodyThenTrailers(5, GRPC_OK),
            ]);
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            exercise(session, &handle, &[Arc::clone(&send_second)], async {
                complete_credit_wait(|| completions(&probes) == [1]).await;
                send_second.notify_one();
                complete_credit_wait(|| completions(&probes) == [1, 1]).await;
            })
            .await;
            state.borrow().writes.concat()
        })
    });
    let trailers: Vec<_> = decoded_header_blocks(&wire)
        .into_iter()
        .filter(|(_, flags, _, _)| flags & END_STREAM != 0)
        .collect();
    assert_eq!(
        trailers
            .iter()
            .map(|(stream, ..)| *stream)
            .collect::<Vec<_>>(),
        [1, 3],
        "each response ends on its trailers block, in order"
    );
    for (_, _, _, fields) in &trailers {
        assert_eq!(fields, &[HpackHeader::new("grpc-status", "0")]);
    }
    let (first, second) = (&trailers[0].2, &trailers[1].2);
    assert_eq!(
        second.len(),
        1,
        "the second block is one representation: {second:02x?}"
    );
    assert_eq!(
        second[0] & 0x80,
        0x80,
        "the second block is an indexed field (RFC 7541 §6.1): {second:02x?}"
    );
    assert!(
        second[0] & 0x7f > 61,
        "the index refers to the dynamic table: {second:02x?}"
    );
    assert_eq!(
        first[0] & 0xc0,
        0x40,
        "the first block inserts the field (literal with incremental indexing, RFC 7541 §6.2.1): {first:02x?}"
    );
}

#[test]
fn downstream_trailers_transcript_is_unchanged_by_early_retirement() {
    let wire = run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut input = arming_preface(&mut peer);
            for _ in 0..2 {
                input.extend(arming_get(&mut peer).1);
            }
            let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
            let (mut application, probes) =
                Application::new(vec![Reply::BodyThenTrailers(5, GRPC_OK), Reply::Body(5)]);
            application.response_date = Some(TRANSCRIPT_DATE);
            let (session, handle) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let wire = RefCell::new(Vec::new());
            exercise(session, &handle, &[], async {
                complete_credit_wait(|| completions(&probes) == [1, 1]).await;
                settle_turns(DRIVE_LIMIT).await;
                for probe in probes.borrow().iter() {
                    assert_eq!(probe.response_complete.get(), 1);
                    assert!(probe.cancelled.borrow().is_empty());
                }
                *wire.borrow_mut() = state.borrow().writes.concat();
            })
            .await;
            wire.into_inner()
        })
    });
    let (headers, data) = (FrameType::HEADERS.as_u8(), FrameType::DATA.as_u8());
    assert_eq!(
        stream_frame_kinds(&wire, 1),
        [
            (headers, END_HEADERS),
            (data, 0),
            (headers, END_HEADERS | END_STREAM),
        ],
        "stream 1: head, body, then trailers carrying END_STREAM"
    );
    assert_eq!(
        stream_frame_kinds(&wire, 3),
        [(headers, END_HEADERS), (data, END_STREAM)],
        "stream 3: head, then its body carrying END_STREAM"
    );
    let written = WireTranscript::new(WireDirection::Written, &wire);
    assert_eq!(
        written.digest(),
        (7, 17_566_401_009_799_498_029),
        "written transcript recorded before streams retired at END_STREAM framing; written: {written:#?}"
    );
}

/// A wrapper typed only against the protocol-neutral contract.
struct GenericApplication(Application);

struct GenericExchange(Exchange);

impl crate::owned::OwnedApplication for GenericApplication {
    type Exchange = GenericExchange;

    fn new_exchange(&self, stream: StreamId, ingress: IngressCompletion) -> GenericExchange {
        GenericExchange(self.0.new_exchange(stream, ingress))
    }
}

impl crate::owned::OwnedExchange for GenericExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        self.0.request(head, body, eom);
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        self.0.poll_headers(context)
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.0.poll_body(context, maximum)
    }

    fn cancel(&mut self, error: &HttpError, peer_reset: bool) {
        self.0.cancel(error, peer_reset);
    }

    fn response_complete(&mut self) {
        self.0.response_complete();
    }

    fn complete(self) {
        self.0.complete();
    }
}

#[test]
fn generic_exchange_contract_drives_owned_h2() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (transport, state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (application, probes) = Application::new(vec![Reply::Body(3)]);
            let (session, _) =
                OwnedH2Session::new(GenericApplication(application), options(context, transport))
                    .unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .expect("the generic exchange finishes its one GET")
                .unwrap();
            let probes = probes.borrow();
            assert_eq!(probes.len(), 1);
            let probe = &probes[0];
            assert_eq!(probe.requested.get(), 1);
            assert!(probe.request_eom.get());
            assert_eq!(probe.response_complete.get(), 1);
            assert_eq!(probe.completed.get(), 1, "complete runs exactly once");
            assert!(probe.cancelled.borrow().is_empty());
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert_eq!(decoded.header_statuses, [(StreamId::new(1), 200)]);
            assert_eq!(decoded.body_data, b"rrr");
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

#[test]
fn h2_contract_names_are_aliases() {
    fn generic_exchange<E: crate::owned::OwnedExchange>() {}
    fn h2_exchange<E: OwnedH2Exchange>() {}
    fn generic_application<A: crate::owned::OwnedApplication>() {}
    fn h2_application<A: OwnedH2Application>() {}

    generic_exchange::<Exchange>();
    h2_exchange::<GenericExchange>();
    generic_application::<Application>();
    h2_application::<GenericApplication>();
}

/// A draining idle connection's close parks while its outstanding read has yet to complete, and
/// finishes once that read completes.
#[test]
fn drain_close_parks_until_the_read_completes() {
    crate::external_host_tests::run_on_host_with(|host, context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // The read ignores cancellation: it completes only when the test releases it.
            let (transport, state) =
                FixtureTransport::new([FixtureRead::WaitData(Arc::clone(&gate), Vec::new())]);
            let (application, probes) = Application::new(Vec::new());
            let mut config = options(context, transport);
            config.http2.double_goaway_drain = false;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
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
                    "the drain's close kept polling while its read was outstanding"
                );
                assert_eq!(state.borrow().close, FixtureClose::Open);
                gate.notify_one();
            });
            run.unwrap();
            assert!(probes.borrow().is_empty());
            assert_eq!(state.borrow().close, FixtureClose::Closed);
            let decoded = decode_server_frames(&state.borrow().writes.concat());
            assert_eq!(
                decoded.goaways,
                [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
            );
        })
    });
}

/// Writes and closes through a [`FixtureTransport`]; each read observes its cancellation only
/// when polled, and arranges no wake for it.
pub(super) struct PolledCancellationTransport(pub(super) Rc<FixtureTransport>);

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

/// A draining idle connection's close polls the outstanding read again after requesting its
/// cancellation, so a read that observes cancellation only when polled still completes the close.
#[test]
fn drain_close_polls_the_read_again_after_cancelling_it() {
    crate::external_host_tests::run_on_host(|context| {
        Box::pin(async move {
            let (inner, state) = FixtureTransport::new([]);
            let transport = Rc::new(PolledCancellationTransport(inner));
            let (application, probes) = Application::new(Vec::new());
            let mut config = options(context, transport);
            config.http2.double_goaway_drain = false;
            let (session, handle) = OwnedH2Session::new(application, config).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.initiate_drain().unwrap());
            });
            run.unwrap();
            assert!(probes.borrow().is_empty());
            assert_eq!(state.borrow().close, FixtureClose::Closed);
        })
    });
}

/// Runs one downstream connection fed `input` then EOF, observed by a recording probe, each of
/// `requests` requests answered with an empty 200. Returns the events and the run's outcome.
async fn owned_lifecycle(
    context: WorkerContext,
    input: Vec<u8>,
    requests: usize,
) -> (Vec<LifecycleEvent>, Result<(), HttpErrorCode>) {
    let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
    let (application, _) = Application::new(vec![Reply::Body(0); requests]);
    let (mut session, _) = OwnedH2Session::new(application, options(context, transport)).unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    session
        .add_lifecycle_observer(Rc::new(LifecycleProbe {
            events: Rc::clone(&events),
        }))
        .unwrap();
    let run = tokio::time::timeout(Duration::from_secs(5), session.run())
        .await
        .expect("the observed connection finishes at EOF");
    (events.take(), run.map(drop).map_err(|error| error.code()))
}

/// The same connection under `UniplexSession` with its default configuration.
async fn uniplex_lifecycle(
    context: WorkerContext,
    input: Vec<u8>,
) -> (Vec<LifecycleEvent>, Result<(), HttpErrorCode>) {
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
    .unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    session
        .add_lifecycle_observer(Rc::new(LifecycleProbe {
            events: Rc::clone(&events),
        }))
        .unwrap();
    let run = tokio::time::timeout(Duration::from_secs(5), session.run())
        .await
        .expect("the observed connection finishes at EOF");
    (events.take(), run.map(drop).map_err(|error| error.code()))
}

#[test]
fn owned_h2_lifecycle_order_matches_uniplex_get() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let input_length = input.len();
            let (transport, transport_state) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (application, _probes) = Application::new(vec![Reply::Body(0)]);
            let (mut session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let events = Rc::new(RefCell::new(Vec::new()));
            let observer = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&events)));
            session.add_lifecycle_observer(observer.clone()).unwrap();
            assert_eq!(
                observer.attached.get(),
                1,
                "attachment runs at registration"
            );
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .expect("session completes");
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
            let request_begin = event_position(
                &events,
                |event| {
                    *event
                        == LifecycleEvent::RequestBegin {
                            stream: StreamId::new(1),
                            sequence_number: 0,
                        }
                },
                "request begin",
            );
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
            let request_end = event_position(
                &events,
                |event| {
                    *event
                        == LifecycleEvent::RequestEnd {
                            stream: StreamId::new(1),
                        }
                },
                "request end",
            );
            let detached = event_position(
                &events,
                |event| {
                    *event
                        == LifecycleEvent::TransactionDetached {
                            stream: Some(StreamId::new(1)),
                        }
                },
                "transaction detached",
            );
            let deactivated = event_position(
                &events,
                |event| *event == LifecycleEvent::DeactivateConnection,
                "deactivate connection",
            );
            let eof = event_position(
                &events,
                |event| *event == LifecycleEvent::Http2IngressEof,
                "HTTP/2 ingress EOF",
            );
            let destroy = event_position(
                &events,
                |event| *event == LifecycleEvent::Destroy,
                "destroy",
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
            let drain = event_position(
                &events,
                |event| matches!(event, LifecycleEvent::DrainStarted(_)),
                "close drain",
            );
            assert_eq!(
                events[drain],
                LifecycleEvent::DrainStarted(DrainTrigger::CloseWhenIdle),
                "a connection that closes at EOF reports the close as `UniplexSession` does"
            );
            assert!(deactivated < drain && drain < destroy);
        })
    });
}

/// The events both drivers report alike: everything but writes, whose coalescing differs,
/// flow-window transitions, and the detach and deactivation, which the owned driver reports at
/// retirement (`DIV-051`) and which are returned separately.
fn comparable_lifecycle(events: &[LifecycleEvent]) -> (Vec<LifecycleEvent>, Vec<LifecycleEvent>) {
    events
        .iter()
        .copied()
        .filter(|event| {
            !matches!(
                event,
                LifecycleEvent::Http2Write { .. }
                    | LifecycleEvent::Http2FlowWindowOpen
                    | LifecycleEvent::Http2FlowWindowClosed
            )
        })
        .partition(|event| {
            !matches!(
                event,
                LifecycleEvent::TransactionDetached { .. } | LifecycleEvent::DeactivateConnection
            )
        })
}

#[test]
fn owned_h2_lifecycle_sequence_equals_uniplex() {
    let cases: [(&str, fn() -> Vec<u8>, usize); 4] = [
        (
            "GET",
            || peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None),
            1,
        ),
        ("two requests", peer_two_requests_bytes, 2),
        (
            "POST with body",
            || peer_request_bytes(HttpMethod::Post, &[b'q'; 1024], None, PeerControl::None),
            1,
        ),
        (
            "peer RST",
            || {
                peer_request_bytes(
                    HttpMethod::Get,
                    &[],
                    None,
                    PeerControl::Reset(H2ErrorCode::CANCEL),
                )
            },
            1,
        ),
    ];
    for (name, input, requests) in cases {
        let (owned, owned_run) =
            run_on_worker(move |context| Box::pin(owned_lifecycle(context, input(), requests)));
        let (uniplex, uniplex_run) =
            run_on_worker(move |context| Box::pin(uniplex_lifecycle(context, input())));
        assert_eq!(owned_run, uniplex_run, "{name}: run outcome");
        let (owned_sequence, owned_detached) = comparable_lifecycle(&owned);
        let (uniplex_sequence, uniplex_detached) = comparable_lifecycle(&uniplex);
        assert_eq!(
            owned_sequence, uniplex_sequence,
            "{name}: owned {owned:?}\nuniplex {uniplex:?}"
        );
        assert_eq!(
            owned_detached, uniplex_detached,
            "{name}: owned {owned:?}\nuniplex {uniplex:?}"
        );
        assert_eq!(owned.last(), Some(&LifecycleEvent::Destroy), "{name}");
    }
}

#[test]
fn owned_h2_reset_and_error_lifecycle_order() {
    let (events, run) = run_on_worker(|context| {
        Box::pin(owned_lifecycle(
            context,
            peer_request_bytes(
                HttpMethod::Get,
                &[],
                None,
                PeerControl::Reset(H2ErrorCode::CANCEL),
            ),
            1,
        ))
    });
    assert_eq!(run, Ok(()), "a peer reset stays stream-local");
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
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            LifecycleEvent::RequestEnd { .. }
        )),
        0,
        "a reset stream reports no request end"
    );

    let (events, run) = run_on_worker(|context| {
        Box::pin(owned_lifecycle(
            context,
            peer_request_bytes(
                HttpMethod::Get,
                &[],
                None,
                PeerControl::WindowUpdate {
                    connection: true,
                    amount: 0x7fff_ffff,
                },
            ),
            1,
        ))
    });
    assert_eq!(run, Err(HttpErrorCode::FLOW_CONTROL_ERROR));
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
        |event| *event == LifecycleEvent::DrainStarted(DrainTrigger::ConnectionError),
        "connection-error drain",
    );
    assert!(message < ingress_error && ingress_error < drain);
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            LifecycleEvent::Http2IngressError { .. }
        )),
        1
    );
    assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
}

/// A client preface and SETTINGS followed by `frames`.
fn peer_control_bytes(frames: &[EgressFrame<'_>]) -> Vec<u8> {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    input.extend(transcript_settings(&mut peer));
    for frame in frames {
        let frame = match frame {
            EgressFrame::SettingsAck => EgressFrame::SettingsAck,
            EgressFrame::PingRequest { data } => EgressFrame::PingRequest { data: *data },
            EgressFrame::PingReply { data } => EgressFrame::PingReply { data: *data },
            _ => unreachable!("only control frames"),
        };
        input.extend(eligibility_frame(&mut peer, frame));
    }
    input
}

#[test]
fn owned_h2_control_frame_events() {
    let (events, run) = run_on_worker(|context| {
        Box::pin(owned_lifecycle(
            context,
            peer_control_bytes(&[
                EgressFrame::SettingsAck,
                EgressFrame::PingRequest { data: Some(12_345) },
                EgressFrame::PingReply { data: 54_321 },
            ]),
            0,
        ))
    });
    assert_eq!(run, Ok(()));
    let settings = events
        .iter()
        .find_map(|event| match event {
            LifecycleEvent::Http2Settings { count } => Some(*count),
            _ => None,
        })
        .expect("the peer's SETTINGS are reported");
    assert_eq!(
        event_count(&events, |event| matches!(
            event,
            LifecycleEvent::Http2Setting { .. }
        )),
        settings,
        "each setting follows its frame's report"
    );
    let acknowledged = event_position(
        &events,
        |event| *event == LifecycleEvent::Http2SettingsAck,
        "settings acknowledgement",
    );
    let reply_sent = event_position(
        &events,
        |event| {
            *event
                == LifecycleEvent::Http2PingReplySent {
                    latency: Duration::ZERO,
                }
        },
        "PING reply sent",
    );
    let reply_received = event_position(
        &events,
        |event| *event == LifecycleEvent::Http2PingReplyReceived,
        "PING reply received",
    );
    assert!(acknowledged < reply_sent && reply_sent < reply_received);
    assert!(!events.iter().any(|event| matches!(
        event,
        LifecycleEvent::Http2StreamsFull | LifecycleEvent::Http2StreamsNotFull
    )));

    let (events, run) = run_on_worker(|context| {
        Box::pin(owned_lifecycle(
            context,
            peer_stream_limit_transition_bytes(),
            0,
        ))
    });
    assert_eq!(run, Ok(()));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                LifecycleEvent::Http2StreamsFull | LifecycleEvent::Http2StreamsNotFull
            ))
            .copied()
            .collect::<Vec<_>>(),
        [
            LifecycleEvent::Http2StreamsFull,
            LifecycleEvent::Http2StreamsNotFull
        ]
    );

    let (events, _) = run_on_worker(|context| {
        Box::pin(owned_lifecycle(
            context,
            peer_idle_goaway_bytes(H2ErrorCode::NO_ERROR),
            0,
        ))
    });
    let goaway = event_position(
        &events,
        |event| {
            *event
                == LifecycleEvent::Http2Goaway {
                    last_good_stream: StreamId::UNSPECIFIED,
                    code: HttpErrorCode::NO_ERROR,
                }
        },
        "peer GOAWAY",
    );
    let drain = event_position(
        &events,
        |event| *event == LifecycleEvent::DrainStarted(DrainTrigger::PeerGoaway),
        "peer GOAWAY drain",
    );
    assert!(goaway < drain);
}

#[test]
fn owned_h2_peer_reset_of_unknown_stream_is_reported() {
    let (events, run) = run_on_worker(|context| {
        let mut input = peer_control_bytes(&[]);
        // RST_STREAM(9, CANCEL) for a stream the peer never opened.
        input.extend([0, 0, 4, 3, 0, 0, 0, 0, 9, 0, 0, 0, 8]);
        Box::pin(owned_lifecycle(context, input, 0))
    });
    assert_eq!(run, Ok(()));
    assert_eq!(
        event_count(&events, |event| *event
            == LifecycleEvent::PeerReset {
                stream: StreamId::new(9),
                code: HttpErrorCode::CANCEL,
            }),
        1
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, LifecycleEvent::RequestBegin { .. }))
    );
    assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
}

#[test]
fn owned_h2_drain_started_once() {
    // (drain locally before the run, peer GOAWAY code, the one drain reported)
    let cases = [
        (true, H2ErrorCode::NO_ERROR, DrainTrigger::Local),
        (false, H2ErrorCode::NO_ERROR, DrainTrigger::PeerGoaway),
        (true, H2ErrorCode::PROTOCOL_ERROR, DrainTrigger::Local),
        (
            false,
            H2ErrorCode::PROTOCOL_ERROR,
            DrainTrigger::ConnectionError,
        ),
    ];
    for (local, code, trigger) in cases {
        let events = run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, _) =
                    FixtureTransport::new([FixtureRead::DataEof(peer_idle_goaway_bytes(code))]);
                let (application, _) = Application::new(Vec::new());
                let (mut session, handle) =
                    OwnedH2Session::new(application, options(context, transport)).unwrap();
                let events = Rc::new(RefCell::new(Vec::new()));
                session
                    .add_lifecycle_observer(Rc::new(LifecycleProbe {
                        events: Rc::clone(&events),
                    }))
                    .unwrap();
                if local {
                    assert!(handle.initiate_drain().unwrap());
                }
                let _ = tokio::time::timeout(Duration::from_secs(5), session.run())
                    .await
                    .expect("the drained connection finishes");
                events.take()
            })
        });
        let drains: Vec<_> = events
            .iter()
            .filter(|event| matches!(event, LifecycleEvent::DrainStarted(_)))
            .copied()
            .collect();
        assert_eq!(
            drains,
            [LifecycleEvent::DrainStarted(trigger)],
            "local={local} {code:?}: {events:?}"
        );
        assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
    }
}

#[test]
fn owned_h2_unrun_session_reports_destroy_and_drops_observer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = FixtureTransport::new([]);
            let (application, probes) = Application::new(Vec::new());
            let (mut session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let events = Rc::new(RefCell::new(Vec::new()));
            let observer = Rc::new(LifecycleProbe {
                events: Rc::clone(&events),
            });
            session.add_lifecycle_observer(observer.clone()).unwrap();
            assert_eq!(Rc::strong_count(&observer), 2);
            drop(session);
            assert_eq!(events.borrow().as_slice(), [LifecycleEvent::Destroy]);
            assert_eq!(
                Rc::strong_count(&observer),
                1,
                "the dropped session releases its observer"
            );
            assert!(probes.borrow().is_empty());
            assert!(state.borrow().writes.is_empty());
        })
    });
}

/// Panics the first time it sees `TransactionAttached`.
struct PanicOnAttach {
    panicked: Cell<bool>,
}

impl LifecycleObserver for PanicOnAttach {
    fn on_event(&self, event: LifecycleEvent) {
        if event == LifecycleEvent::TransactionAttached && !self.panicked.replace(true) {
            panic_any("lifecycle observer");
        }
    }
}

#[test]
fn owned_h2_observer_panic_aborts_after_cleanup() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (transport, state) = FixtureTransport::new([FixtureRead::Data(input)]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0)]);
            application.hold_headers = true;
            let (mut session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let events = Rc::new(RefCell::new(Vec::new()));
            session
                .add_lifecycle_observer(Rc::new(LifecycleProbe {
                    events: Rc::clone(&events),
                }))
                .unwrap();
            session
                .add_lifecycle_observer(Rc::new(PanicOnAttach {
                    panicked: Cell::new(false),
                }))
                .unwrap();
            let panic = tokio::time::timeout(
                Duration::from_secs(5),
                eligibility_catch_unwind(session.run()),
            )
            .await
            .expect("the panicked connection joins its I/O")
            .expect_err("the run resumes the observer's panic after cleanup");
            assert_eq!(
                panic.downcast_ref::<&'static str>(),
                Some(&"lifecycle observer")
            );
            assert_eq!(state.borrow().close, FixtureClose::Reset);
            let probes = probes.borrow();
            assert_eq!(
                probes[0].cancelled.borrow().as_slice(),
                [(HttpErrorCode::INTERNAL_ERROR, false)]
            );
            assert_eq!(probes[0].dropped.get(), 1);
            let events = events.borrow();
            let drain = event_position(
                &events,
                |event| *event == LifecycleEvent::DrainStarted(DrainTrigger::ConnectionError),
                "connection-error drain",
            );
            let detached = event_position(
                &events,
                |event| matches!(event, LifecycleEvent::TransactionDetached { .. }),
                "transaction detached",
            );
            assert!(drain < detached);
            assert_eq!(
                events.last(),
                Some(&LifecycleEvent::Destroy),
                "the other observer still sees the teardown"
            );
        })
    });
}

/// Records each event it sees, tagged with its own number.
struct TaggedProbe {
    tag: u8,
    seen: Rc<RefCell<Vec<(u8, LifecycleEvent)>>>,
}

impl LifecycleObserver for TaggedProbe {
    fn on_event(&self, event: LifecycleEvent) {
        self.seen.borrow_mut().push((self.tag, event));
    }
}

#[test]
fn owned_h2_registration_adds_and_removes_observers() {
    run_on_worker(|context| {
        Box::pin(async move {
            let input = peer_request_bytes(HttpMethod::Get, &[], None, PeerControl::None);
            let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input)]);
            let (application, _) = Application::new(vec![Reply::Body(0)]);
            let (mut session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            let removed_events = Rc::new(RefCell::new(Vec::new()));
            let removed = Rc::new(RegisteredLifecycleProbe::new(Rc::clone(&removed_events)));
            let self_removed_events = Rc::new(RefCell::new(Vec::new()));
            let self_removed = Rc::new(SelfRemovingAttachedObserver {
                attached: Cell::new(0),
                removed: Cell::new(false),
                events: Rc::clone(&self_removed_events),
            });
            let seen = Rc::new(RefCell::new(Vec::new()));
            let first = session
                .add_lifecycle_observer(Rc::new(TaggedProbe {
                    tag: 1,
                    seen: Rc::clone(&seen),
                }))
                .unwrap();
            let second = session.add_lifecycle_observer(removed.clone()).unwrap();
            let third = session
                .add_lifecycle_observer(self_removed.clone())
                .unwrap();
            let fourth = session
                .add_lifecycle_observer(Rc::new(TaggedProbe {
                    tag: 2,
                    seen: Rc::clone(&seen),
                }))
                .unwrap();
            assert!(first < second && second < third && third < fourth);
            assert!(self_removed.removed.get());
            assert!(removed.remove(), "a registration removes its observer");
            assert!(!removed.remove(), "a removed observer is removed once");
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .expect("session completes");
            assert!(removed_events.borrow().is_empty());
            assert!(self_removed_events.borrow().is_empty());
            assert_eq!(Rc::strong_count(&removed), 1);
            let seen = seen.borrow();
            assert_eq!(seen.len() % 2, 0);
            for pair in seen.chunks(2) {
                assert_eq!(
                    [pair[0].0, pair[1].0],
                    [2, 1],
                    "the newest observer sees each event first"
                );
                assert_eq!(pair[0].1, pair[1].1);
            }
            assert_eq!(
                seen.last().map(|(_, event)| *event),
                Some(LifecycleEvent::Destroy)
            );
        })
    });
}

#[test]
fn owned_h2_default_path_has_no_observation() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _) = FixtureTransport::new([]);
            let (application, _) = Application::new(Vec::new());
            let (mut session, _) =
                OwnedH2Session::new(application, options(context, transport)).unwrap();
            assert!(!session.observed(), "no observer, no observation state");
            session
                .add_lifecycle_observer(Rc::new(LifecycleProbe {
                    events: Rc::default(),
                }))
                .unwrap();
            assert!(session.observed());
        })
    });
}

// Opt-in flood limits (`DIV-052`).

/// Every category unbounded except the ones the caller sets.
fn unbounded_rates() -> Http2RateLimitConfig {
    let unlimited = RateLimitConfig::new(u64::MAX, Duration::from_millis(100), 0);
    Http2RateLimitConfig {
        control_frames: unlimited,
        header_frames: unlimited,
        resets: unlimited,
        direct_errors: unlimited,
    }
}

/// What one exchange's probe recorded.
struct ExchangeOutcome {
    requested: usize,
    request_bytes: usize,
    completed: usize,
    dropped: usize,
    cancelled: Vec<(HttpErrorCode, bool)>,
}

fn exchange_outcomes(probes: &RefCell<Vec<Rc<Probe>>>) -> Vec<ExchangeOutcome> {
    probes
        .borrow()
        .iter()
        .map(|probe| ExchangeOutcome {
            requested: probe.requested.get(),
            request_bytes: probe.request_bytes.get(),
            completed: probe.completed.get(),
            dropped: probe.dropped.get(),
            cancelled: probe.cancelled.borrow().clone(),
        })
        .collect()
}

/// Runs one downstream connection fed `reads` with `rate_limits`, each of `requests` requests
/// answered with an empty 200. Returns the server's frames, the exchanges' outcomes, the run's
/// outcome and how the transport closed.
async fn rate_limited_run(
    context: WorkerContext,
    reads: Vec<FixtureRead>,
    rate_limits: Http2RateLimitConfig,
    requests: usize,
) -> (
    DecodedServerFrames,
    Vec<ExchangeOutcome>,
    Result<(), HttpErrorCode>,
    FixtureClose,
) {
    let (transport, state) = FixtureTransport::new(reads);
    let (application, probes) = Application::new(vec![Reply::Body(0); requests]);
    let mut config = options(context, transport);
    config.http2.rate_limits = rate_limits;
    let (session, _) = OwnedH2Session::new(application, config).unwrap();
    let run = tokio::time::timeout(Duration::from_secs(10), session.run())
        .await
        .expect("the rate-limited connection finishes");
    let written = decode_server_frames_with_requests(&state.borrow().writes.concat(), requests);
    let close = state.borrow().close;
    (
        written,
        exchange_outcomes(&probes),
        run.map(drop).map_err(|error| error.code()),
        close,
    )
}

/// A client preface and SETTINGS, then `count` GET requests, each ending its stream.
fn peer_get_requests(count: usize) -> Vec<u8> {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    input.extend(transcript_settings(&mut peer));
    for _ in 0..count {
        input.extend(eligibility_request(&mut peer, 3));
    }
    input
}

#[test]
fn owned_h2_builds_no_rate_limiter_unless_a_category_is_bounded() {
    let bounded = RateLimitConfig::new(10, Duration::from_secs(1), 0);
    // (configuration, whether the downstream session counts)
    let cases = [
        (unbounded_rates(), false),
        (
            Http2RateLimitConfig {
                header_frames: RateLimitConfig::new(0, Duration::from_secs(1), u64::MAX),
                ..unbounded_rates()
            },
            false,
        ),
        (Http2RateLimitConfig::default(), true),
        (
            Http2RateLimitConfig {
                resets: bounded,
                ..unbounded_rates()
            },
            true,
        ),
        (
            Http2RateLimitConfig {
                direct_errors: RateLimitConfig::new(u64::MAX - 1, Duration::from_secs(1), 0),
                ..unbounded_rates()
            },
            true,
        ),
    ];
    for (index, (rate_limits, counts)) in cases.into_iter().enumerate() {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state) = FixtureTransport::new([]);
                let mut config = options(context, transport);
                config.http2.rate_limits = rate_limits;
                let (application, _) = Application::new(Vec::new());
                let (session, _) = OwnedH2Session::new(application, config)
                    .unwrap_or_else(|error| panic!("case {index} was refused: {error:?}"));
                assert_eq!(session.limits_rates(), counts, "case {index}");
                drop(session);
                assert!(state.borrow().writes.is_empty());
            })
        });
    }
}

#[test]
fn finite_rate_limits_are_accepted_downstream() {
    let (written, probes, run, _) = run_on_worker(|context| {
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::DataEof(peer_two_requests_bytes())],
            Http2RateLimitConfig::default(),
            2,
        ))
    });
    assert_eq!(run, Ok(()));
    assert_eq!(
        sorted_statuses(&written),
        [(StreamId::new(1), 200), (StreamId::new(3), 200)]
    );
    assert!(written.resets.is_empty() && written.goaways.is_empty());
    assert!(probes.iter().all(|probe| probe.completed == 1));
}

#[test]
fn owned_control_rate_limit_counts_only_semantic_request_callbacks() {
    let (written, _, run, close) = run_on_worker(|context| {
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::Data(peer_control_rate_limit_bytes())],
            Http2RateLimitConfig {
                control_frames: RateLimitConfig::new(3, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            1,
        ))
    });
    // SETTINGS, PRIORITY_UPDATE and the first PING count; SETTINGS ACK, the PING ACK and
    // WINDOW_UPDATE do not, so the second PING is the fourth control frame.
    assert_eq!(run, Err(HttpErrorCode::CANCEL));
    assert_eq!(close, FixtureClose::Reset);
    assert_eq!(written.ping_replies, [11]);
    assert_eq!(written.goaways, [(StreamId::new(1), H2ErrorCode::CANCEL)]);
}

#[test]
fn owned_settings_flood_trips_control_limit_before_acknowledging() {
    let (written, _, run, close) = run_on_worker(|context| {
        let mut peer = Http2Codec::new(TransportDirection::Upstream);
        let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
        for _ in 0..5 {
            input.extend(transcript_settings(&mut peer));
        }
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::Data(input)],
            Http2RateLimitConfig {
                control_frames: RateLimitConfig::new(2, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            0,
        ))
    });
    assert_eq!(run, Err(HttpErrorCode::CANCEL));
    assert_eq!(close, FixtureClose::Reset);
    assert_eq!(
        written.settings_acks, 2,
        "the third SETTINGS breaches the limit before it is acknowledged"
    );
    assert_eq!(
        written.goaways,
        [(StreamId::UNSPECIFIED, H2ErrorCode::CANCEL)]
    );
}

#[test]
fn owned_rfc7540_priority_frames_are_not_counted() {
    let (written, _, run, _) = run_on_worker(|context| {
        let mut input = peer_control_bytes(&[]);
        for _ in 0..10 {
            // PRIORITY(3): exclusive bit clear, depends on stream 0, weight 16.
            input.extend([0, 0, 5, 2, 0, 0, 0, 0, 3, 0, 0, 0, 0, 15]);
        }
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::DataEof(input)],
            Http2RateLimitConfig {
                control_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            0,
        ))
    });
    assert_eq!(run, Ok(()), "only the peer's SETTINGS counts");
    assert!(written.goaways.is_empty());
}

#[test]
fn owned_reset_rate_limit_cancels_unrelated_active_request() {
    let (written, probes, run, close) = run_on_worker(|context| {
        let (initial, resets) = peer_reset_rate_limit_bytes();
        let mut rate_limits = Http2RateLimitConfig::default();
        // Keep the reset burst in one accounting window under instrumentation.
        rate_limits.resets.interval = Duration::from_secs(10);
        Box::pin(async move {
            let (transport, state) =
                FixtureTransport::new([FixtureRead::Data(initial), FixtureRead::Data(resets)]);
            let (mut application, probes) = Application::new(vec![Reply::Body(0); 2]);
            application.retain_request = true;
            let mut config = options(context, transport);
            config.http2.rate_limits = rate_limits;
            let (session, _) = OwnedH2Session::new(application, config).unwrap();
            let run = tokio::time::timeout(Duration::from_secs(10), session.run())
                .await
                .expect("the reset flood ends the connection");
            let written = decode_server_frames_with_requests(&state.borrow().writes.concat(), 2);
            let close = state.borrow().close;
            (
                written,
                exchange_outcomes(&probes),
                run.map(drop).map_err(|error| error.code()),
                close,
            )
        })
    });
    assert_eq!(run, Err(HttpErrorCode::CANCEL));
    assert_eq!(close, FixtureClose::Reset);
    assert_eq!(written.goaways, [(StreamId::new(3), H2ErrorCode::CANCEL)]);
    assert_eq!(probes.len(), 2);
    assert_eq!(
        probes[0].cancelled.as_slice(),
        [(HttpErrorCode::CANCEL, false)],
        "the held POST is cancelled by the connection failure"
    );
    assert_eq!(probes[0].completed, 0);
    assert_eq!(
        probes[1].cancelled.first(),
        Some(&(HttpErrorCode::CANCEL, false)),
        "the reset GET, which never started, is cancelled by the connection failure"
    );
    assert_eq!(probes[1].completed, 0);
}

/// A client preface and SETTINGS, then `count` GETs, each reset by the client right after its
/// HEADERS.
fn peer_rapid_reset_bytes(count: usize) -> Vec<u8> {
    let mut peer = Http2Codec::new(TransportDirection::Upstream);
    let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
    input.extend(transcript_settings(&mut peer));
    for index in 0..count {
        input.extend(eligibility_request(&mut peer, 3));
        input.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Reset {
                stream: StreamId::new(1 + 2 * index as u64),
                code: H2ErrorCode::CANCEL,
            },
        ));
    }
    input
}

#[test]
fn owned_rapid_reset_flood_trips_the_reset_limit() {
    const PAIRS: usize = 200;
    let (written, probes, run, close) = run_on_worker(|context| {
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::DataEof(peer_rapid_reset_bytes(PAIRS))],
            Http2RateLimitConfig {
                resets: RateLimitConfig::new(100, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            PAIRS,
        ))
    });
    assert_eq!(run, Err(HttpErrorCode::CANCEL));
    assert_eq!(close, FixtureClose::Reset);
    assert_eq!(written.goaways.len(), 1);
    assert_eq!(written.goaways[0].1, H2ErrorCode::CANCEL);
    assert!(
        probes.len() <= 101,
        "{} exchanges started before the 101st reset",
        probes.len()
    );
    assert!(probes.iter().all(|probe| probe.completed == 0));
    assert!(written.header_statuses.is_empty());

    let (written, probes, run, _) = run_on_worker(|context| {
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::DataEof(peer_rapid_reset_bytes(PAIRS))],
            unbounded_rates(),
            PAIRS,
        ))
    });
    assert_eq!(run, Ok(()), "without a reset limit the flood is served");
    assert_eq!(probes.len(), PAIRS);
    assert!(written.goaways.is_empty());
}

#[test]
fn owned_header_rate_limit_drains_without_resetting_the_crossing_stream() {
    let (written, probes, run, _) = run_on_worker(|context| {
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::DataEof(peer_two_requests_bytes())],
            Http2RateLimitConfig {
                header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            2,
        ))
    });
    assert_eq!(run, Ok(()), "a header breach drains gracefully");
    assert!(written.resets.is_empty());
    assert_eq!(
        written.goaways,
        [(StreamId::new(3), H2ErrorCode::NO_ERROR)],
        "one final GOAWAY covers the crossing stream, as Proxygen's RateLimitFilter sends"
    );
    assert_eq!(written.header_statuses, [(StreamId::new(1), 200)]);
    assert_eq!(probes[0].completed, 1);
    assert_eq!(probes[1].requested, 0, "the crossing head is not delivered");
    assert_eq!(
        probes[1].cancelled.as_slice(),
        [(HttpErrorCode::CANCEL, false)]
    );
    assert_eq!(probes[1].dropped, 1);
}

#[test]
fn owned_header_rate_limit_stays_breached_for_the_rest_of_the_window() {
    let (written, probes, run, _) = run_on_worker(|context| {
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::DataEof(peer_get_requests(3))],
            Http2RateLimitConfig {
                header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            3,
        ))
    });
    assert_eq!(run, Ok(()));
    assert!(written.resets.is_empty());
    assert_eq!(written.goaways, [(StreamId::new(5), H2ErrorCode::NO_ERROR)]);
    assert_eq!(written.header_statuses, [(StreamId::new(1), 200)]);
    for probe in &probes[1..] {
        assert_eq!(probe.cancelled.as_slice(), [(HttpErrorCode::CANCEL, false)]);
        assert_eq!(probe.requested, 0);
    }
}

#[test]
fn owned_header_rate_limit_later_batch_stream_is_dropped_by_codec() {
    const BODY: usize = 10;
    let (written, probes, run, _) = run_on_worker(|context| {
        let mut peer = Http2Codec::new(TransportDirection::Upstream);
        let mut first = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
        first.extend(transcript_settings(&mut peer));
        let (upload, headers) = padding_post_headers(&mut peer);
        first.extend(headers);
        first.extend(eligibility_request(&mut peer, 3));
        let mut second = eligibility_request(&mut peer, 3);
        second.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Body {
                stream: upload,
                body: queue(&[b'q'; BODY]),
                padding: None,
                end_stream: true,
            },
        ));
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::Data(first), FixtureRead::DataEof(second)],
            Http2RateLimitConfig {
                header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            2,
        ))
    });
    assert_eq!(run, Ok(()));
    assert_eq!(
        probes.len(),
        2,
        "stream 5, above the final GOAWAY, never reaches the driver"
    );
    assert!(written.resets.is_empty());
    assert_eq!(written.goaways, [(StreamId::new(3), H2ErrorCode::NO_ERROR)]);
    assert_eq!(probes[0].request_bytes, BODY);
    assert_eq!(probes[0].completed, 1);
    assert_eq!(written.header_statuses, [(StreamId::new(1), 200)]);
    assert_eq!(
        probes[1].cancelled.as_slice(),
        [(HttpErrorCode::CANCEL, false)]
    );
}

#[test]
fn owned_header_rate_limit_skips_forbidden_body_requests() {
    let (written, _, run, _) = run_on_worker(|context| {
        let mut peer = Http2Codec::new(TransportDirection::Upstream);
        let mut input = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
        input.extend(transcript_settings(&mut peer));
        let mut trace = HttpMessage::new();
        trace.set_method(HttpMethod::Trace).unwrap();
        trace.set_url(b"/trace", false).unwrap();
        trace.set_http_version(1, 1);
        trace
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        trace.headers_mut().set(HttpHeaderCode::ContentLength, b"0");
        let stream = peer.create_stream();
        input.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Headers {
                stream,
                message: &trace,
                end_stream: true,
                extra_headers: None,
            },
        ));
        input.extend(eligibility_request(&mut peer, 3));
        Box::pin(rate_limited_run(
            context,
            vec![FixtureRead::DataEof(input)],
            Http2RateLimitConfig {
                header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                ..unbounded_rates()
            },
            2,
        ))
    });
    assert_eq!(run, Ok(()));
    assert!(
        written.goaways.is_empty(),
        "the rejected TRACE is not counted, so the GET does not breach"
    );
    assert_eq!(
        sorted_statuses(&written),
        [(StreamId::new(1), 400), (StreamId::new(3), 200)]
    );
}

#[test]
fn owned_header_rate_limit_during_a_drain_sends_no_extra_goaway() {
    // Stream 3's header block straddles the final GOAWAY of a local drain, so the driver refuses
    // it, and its completed block is the second counted one.
    let header_limit = Http2RateLimitConfig {
        header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
        ..unbounded_rates()
    };
    let (written, transcript, _, run) = run_on_worker(move |context| {
        Box::pin(straddled_refusal(
            context,
            StraddledBlock::EndedPost,
            Some(header_limit),
        ))
    });
    assert_eq!(run, Ok(()));
    assert_eq!(
        written.goaways,
        [
            (StreamId::new(0x7fff_ffff), H2ErrorCode::NO_ERROR),
            (StreamId::new(3), H2ErrorCode::NO_ERROR)
        ],
        "the breach during the drain adds no GOAWAY"
    );
    assert_eq!(
        written.resets,
        [(StreamId::new(3), H2ErrorCode::REFUSED_STREAM)]
    );
    assert_eq!(
        transcript,
        (6, 4_075_595_476_242_011_935),
        "the wire equals the unlimited run's (`refused_stream_is_forgotten_after_its_header_block`)"
    );
}

#[test]
fn owned_direct_error_rate_limit_suppresses_the_crossing_semantic_error() {
    let (written, _, run, close) = run_on_worker(|context| {
        Box::pin(rate_limited_run(
            context,
            peer_duplicate_content_length_requests(51),
            Http2RateLimitConfig {
                direct_errors: RateLimitConfig::new(10, Duration::from_secs(60), 50),
                ..unbounded_rates()
            },
            51,
        ))
    });
    assert_eq!(
        run,
        Err(HttpErrorCode::CANCEL),
        "the 51st direct semantic error breaches the limit"
    );
    assert_eq!(close, FixtureClose::Reset);
    assert!(written.header_statuses.len() <= 50);
    assert!(
        written
            .header_statuses
            .iter()
            .all(|&(stream, _)| stream != StreamId::new(101))
    );
}

/// Polls `run` until `condition` holds; `run` must not finish first.
async fn poll_until<F: Future>(run: &mut Pin<&mut F>, condition: impl Fn() -> bool) {
    std::future::poll_fn(|context| {
        assert!(
            run.as_mut().poll(context).is_pending(),
            "the connection ended early"
        );
        if condition() {
            Poll::Ready(())
        } else {
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

#[test]
fn owned_rate_limit_windows_roll_on_the_next_event_without_a_timer() {
    let (replies, timers, run) = crate::external_host_tests::run_on_host_with(|host, context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let ping = |peer: &mut Http2Codec, data| {
                eligibility_frame(peer, EgressFrame::PingRequest { data: Some(data) })
            };
            let mut first = eligibility_frame(&mut peer, EgressFrame::ConnectionPreface);
            first.extend(transcript_settings(&mut peer));
            first.extend(ping(&mut peer, 1));
            let mut second = ping(&mut peer, 2);
            second.extend(ping(&mut peer, 3));
            let third = ping(&mut peer, 4);
            let release_second = Arc::new(tokio::sync::Notify::new());
            let release_third = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = FixtureTransport::new([
                FixtureRead::Data(first),
                FixtureRead::WaitData(Arc::clone(&release_second), second),
                FixtureRead::WaitDataEof(Arc::clone(&release_third), third),
            ]);
            let (application, _) = Application::new(Vec::new());
            let mut config = options(context, transport);
            config.http2.rate_limits = Http2RateLimitConfig {
                control_frames: RateLimitConfig::new(2, Duration::from_secs(1), 0),
                ..unbounded_rates()
            };
            let (session, _) = OwnedH2Session::new(application, config).unwrap();
            let replies = || {
                let written = state.borrow().writes.concat();
                decode_server_frames_with_requests(&written, 0).ping_replies
            };
            let mut run = std::pin::pin!(session.run());
            // SETTINGS and the first PING fill the 1 s window.
            poll_until(&mut run, || replies().len() == 1).await;
            host.advance(Duration::from_secs(1));
            release_second.notify_one();
            // The next PING finds the window elapsed and starts a new one.
            poll_until(&mut run, || replies().len() == 3).await;
            release_third.notify_one();
            let run = run.await.map(drop).map_err(|error| error.code());
            (replies(), host.timers_created(), run)
        })
    });
    assert_eq!(replies, [1, 2, 3]);
    assert_eq!(
        run,
        Err(HttpErrorCode::CANCEL),
        "the third PING of the second window breaches it"
    );
    assert_eq!(timers, 0, "the windows need no timer");
}

#[test]
fn owned_unbounded_categories_read_no_clock() {
    // Control frames only: SETTINGS, two PINGs and SETTINGS ACK.
    let input = || {
        peer_control_bytes(&[
            EgressFrame::PingRequest { data: Some(1) },
            EgressFrame::PingRequest { data: Some(2) },
            EgressFrame::SettingsAck,
        ])
    };
    let clock_reads = |rate_limits: Http2RateLimitConfig| {
        crate::external_host_tests::run_on_host_with(move |host, context| {
            Box::pin(async move {
                let (transport, _) = FixtureTransport::new([FixtureRead::DataEof(input())]);
                let (application, _) = Application::new(Vec::new());
                let mut config = options(context, transport);
                config.http2.rate_limits = rate_limits;
                let (session, _) = OwnedH2Session::new(application, config).unwrap();
                let before = host.now_reads();
                session.run().await.unwrap();
                host.now_reads() - before
            })
        })
    };
    let unlimited = clock_reads(unbounded_rates());
    let headers_bounded = clock_reads(Http2RateLimitConfig {
        header_frames: RateLimitConfig::new(10, Duration::from_secs(1), 0),
        ..unbounded_rates()
    });
    let control_bounded = clock_reads(Http2RateLimitConfig {
        control_frames: RateLimitConfig::new(10, Duration::from_secs(1), 0),
        ..unbounded_rates()
    });
    assert_eq!(
        headers_bounded, unlimited,
        "frames of an unbounded category read no clock"
    );
    assert_eq!(
        control_bounded,
        unlimited + 3,
        "each counted control frame reads the clock once"
    );
}

#[test]
fn owned_header_rate_limit_reports_drain_and_suppresses_ingress_message() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _) =
                FixtureTransport::new([FixtureRead::DataEof(peer_two_requests_bytes())]);
            let (application, _) = Application::new(vec![Reply::Body(0); 2]);
            let mut config = options(context, transport);
            config.http2.rate_limits = Http2RateLimitConfig {
                header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                ..unbounded_rates()
            };
            let (mut session, _) = OwnedH2Session::new(application, config).unwrap();
            let events = Rc::new(RefCell::new(Vec::new()));
            session
                .add_lifecycle_observer(Rc::new(LifecycleProbe {
                    events: Rc::clone(&events),
                }))
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .unwrap()
                .expect("a header breach drains gracefully");
            let events = events.borrow();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, LifecycleEvent::Http2IngressMessage { .. }))
                    .copied()
                    .collect::<Vec<_>>(),
                [LifecycleEvent::Http2IngressMessage {
                    stream: StreamId::new(1)
                }],
                "the crossing head is not reported"
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, LifecycleEvent::DrainStarted(_)))
                    .copied()
                    .collect::<Vec<_>>(),
                [LifecycleEvent::DrainStarted(DrainTrigger::HeaderRateLimit)]
            );
            // `UniplexSession` aborts the crossing stream without queuing a codec failure.
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, LifecycleEvent::Http2IngressError { .. })),
                "a header breach reports no ingress error: {events:?}"
            );
        })
    });
}

/// The read idle timeout of the connection-timer tests.
pub(super) const IDLE: Duration = Duration::from_secs(10);

/// The GOAWAY timeout of the connection-timer tests: the grace between a drain's two GOAWAYs.
const GRACE: Duration = Duration::from_secs(1);

/// The last-stream id of a graceful drain's first GOAWAY, `2^31 - 1` on the wire.
const GOAWAY_MAX: StreamId = StreamId::new(0x7fff_ffff);

/// Serves the fixture's scripted reads and requires each to carry no timeout: the owned driver
/// times its connection itself. A parked `WaitData` read, and a read past the script, end on
/// transport cancellation.
pub(super) struct UntimedReads(pub(super) Rc<FixtureTransport>);

impl UniplexTransport for UntimedReads {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        assert!(
            timeout.is_zero(),
            "owned H2 reads carry no transport timeout"
        );
        let parked = {
            let mut state = self.0.state.borrow_mut();
            match state.reads.front() {
                None | Some(FixtureRead::WaitData(..)) => Some(state.reads.pop_front()),
                Some(_) => None,
            }
        };
        let Some(parked) = parked else {
            return self
                .0
                .read(context, maximum, allocation, timeout, cancellation);
        };
        let baton = CancellableBaton::with_host(context.host().clone());
        Box::pin(async move {
            let ready = match parked {
                Some(FixtureRead::WaitData(ready, bytes)) => Some((ready, bytes)),
                _ => None,
            };
            if let Some((ready, bytes)) = ready {
                if let Either::Left(_) = select(
                    std::pin::pin!(ready.notified()),
                    std::pin::pin!(baton.wait(cancellation)),
                )
                .await
                {
                    return Ok(UniplexRead::new(queue(&bytes), false));
                }
            } else {
                baton.wait(cancellation).await;
            }
            Err(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"scripted H2 read cancelled",
            ))
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

/// A downstream session over `reads` with the given read idle and GOAWAY timeouts.
fn timed_session(
    context: WorkerContext,
    reads: impl IntoIterator<Item = FixtureRead>,
    replies: Vec<Reply>,
    read_timeout: Duration,
    goaway_timeout: Duration,
) -> (
    OwnedH2Session<Application>,
    OwnedH2Handle,
    Rc<RefCell<FixtureTransportState>>,
    Rc<RefCell<Vec<Rc<Probe>>>>,
) {
    let (fixture, state) = FixtureTransport::new(reads);
    let (application, probes) = Application::new(replies);
    let mut config = options(context, Rc::new(UntimedReads(fixture)));
    config.io.read_timeout = read_timeout;
    config.http2.goaway_timeout = goaway_timeout;
    let (session, handle) = OwnedH2Session::new(application, config).expect("timed owned H2");
    (session, handle, state, probes)
}

/// The frames written so far, decoded by a client that sent `requests` requests.
fn written_frames(state: &RefCell<FixtureTransportState>, requests: usize) -> DecodedServerFrames {
    decode_server_frames_with_requests(&state.borrow().writes.concat(), requests)
}

fn written_goaways(state: &RefCell<FixtureTransportState>) -> Vec<(StreamId, H2ErrorCode)> {
    written_frames(state, 1).goaways
}

pub(super) type HostCase = fn(Rc<ManualHost>, WorkerContext) -> Pin<Box<dyn Future<Output = ()>>>;

/// Runs `case` on a host that drives a wheel timer, as hosted workers do, and on one without a
/// wheel, where the driver's timeouts fall back to host sleeps.
pub(super) fn on_both_hosts(case: HostCase) {
    crate::external_host_tests::run_on_wheel_host_with(case);
    crate::external_host_tests::run_on_host_with(case);
}

/// An idle connection drains at its read timeout, not a millisecond earlier, as Proxygen's read
/// timeout calls `initiateDrain`. With a zero GOAWAY timeout both GOAWAYs go out at once, then
/// the connection closes.
#[test]
fn idle_connection_drains_at_its_read_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (_, preface) = ready_owner_requests(0);
            let (session, _handle, state, probes) = timed_session(
                context,
                [FixtureRead::Data(preface)],
                Vec::new(),
                IDLE,
                Duration::ZERO,
            );
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                assert!(written_goaways(&state).is_empty());
                host.advance(IDLE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(written_goaways(&state).is_empty(), "the drain came early");
                assert_eq!(state.borrow().close, FixtureClose::Open);
                host.advance(Duration::from_millis(1));
            });
            run.expect("an idle drain closes cleanly");
            assert_eq!(
                written_goaways(&state),
                [
                    (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                    (StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)
                ]
            );
            assert_eq!(state.borrow().close, FixtureClose::Closed);
            assert!(probes.borrow().is_empty());
        })
    });
}

/// A drained reader's next read parks for its waker, and the parked read is outstanding for the
/// read idle timeout, with its deadline stamped when it parks: a reader that drains `IDLE / 2`
/// after its read started drains the idle connection `IDLE` after it parked.
#[test]
fn idle_connection_with_a_parked_reader_drains_at_its_read_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (_, preface) = ready_owner_requests(0);
            let (fixture, state) = FixtureTransport::new([FixtureRead::Data(preface)]);
            let (transport, gate) =
                crate::uniplex_tests::GatedReaderTransport::new(Rc::new(UntimedReads(fixture)));
            let (application, probes) = Application::new(Vec::new());
            let mut config = options(context, transport);
            config.io.read_timeout = IDLE;
            config.http2.goaway_timeout = Duration::ZERO;
            let (session, _handle) =
                OwnedH2Session::new(application, config).expect("timed owned H2");
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                assert!(gate.waiting(), "the first read waits for the preface");
                host.advance(IDLE / 2);
                gate.open_one();
                settle_turns(32).await;
                assert!(gate.waiting(), "the drained reader waits for its waker");
                host.advance(IDLE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(written_goaways(&state).is_empty(), "the drain came early");
                assert_eq!(state.borrow().close, FixtureClose::Open);
                host.advance(Duration::from_millis(1));
                settle_turns(32).await;
                assert_eq!(
                    written_goaways(&state),
                    [
                        (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                        (StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)
                    ],
                    "the parked read timed out"
                );
            });
            run.expect("an idle drain closes cleanly");
            assert_eq!(state.borrow().close, FixtureClose::Closed);
            assert!(probes.borrow().is_empty());
        })
    });
}

/// Request bytes that arrive in the turn the read idle timeout fires win: the connection serves
/// them and stays open, and the next read gets a fresh deadline.
#[test]
fn data_at_the_read_deadline_beats_the_idle_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, preface) = ready_owner_requests(0);
            let request = eligibility_request(&mut peer, 0);
            let gate = Arc::new(tokio::sync::Notify::new());
            let (session, _handle, state, probes) = timed_session(
                context,
                [
                    FixtureRead::Data(preface),
                    FixtureRead::WaitData(Arc::clone(&gate), request),
                ],
                vec![Reply::Body(0)],
                IDLE,
                Duration::ZERO,
            );
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                host.advance(IDLE);
                gate.notify_one();
                settle_turns(64).await;
                let frames = written_frames(&state, 1);
                assert!(frames.goaways.is_empty(), "the expiry beat same-turn data");
                assert_eq!(frames.header_statuses, [(StreamId::new(1), 200)]);
                host.advance(IDLE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(
                    written_goaways(&state).is_empty(),
                    "the next read kept the old deadline"
                );
                host.advance(Duration::from_millis(1));
            });
            run.expect("the later idle drain closes cleanly");
            assert_eq!(
                written_goaways(&state),
                [
                    (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                    (StreamId::new(1), H2ErrorCode::NO_ERROR)
                ]
            );
            assert_eq!(probes.borrow()[0].completed.get(), 1);
        })
    });
}

/// An idle drain keeps Proxygen's grace: it sends GOAWAY(2^31-1), serves a request that arrives
/// before the GOAWAY timeout, sends the final GOAWAY exactly at that timeout, then closes.
/// `UniplexSession` refuses streams during an idle drain instead (`DIV-028`).
#[test]
fn idle_drain_keeps_grace_until_the_goaway_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, preface) = ready_owner_requests(0);
            let request = eligibility_request(&mut peer, 0);
            let gate = Arc::new(tokio::sync::Notify::new());
            let (session, _handle, state, probes) = timed_session(
                context,
                [
                    FixtureRead::Data(preface),
                    FixtureRead::WaitData(Arc::clone(&gate), request),
                ],
                vec![Reply::Body(0)],
                IDLE,
                GRACE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                host.advance(IDLE);
                settle_turns(32).await;
                assert_eq!(
                    written_goaways(&state),
                    [(GOAWAY_MAX, H2ErrorCode::NO_ERROR)]
                );
                host.advance(GRACE / 2);
                gate.notify_one();
                settle_turns(64).await;
                assert_eq!(
                    written_frames(&state, 1).header_statuses,
                    [(StreamId::new(1), 200)],
                    "a request inside the grace is served"
                );
                host.advance(GRACE / 2 - Duration::from_millis(1));
                settle_turns(32).await;
                assert_eq!(
                    written_goaways(&state).len(),
                    1,
                    "the final GOAWAY came early"
                );
                assert_eq!(state.borrow().close, FixtureClose::Open);
                host.advance(Duration::from_millis(1));
            });
            run.expect("the idle drain closes cleanly");
            assert_eq!(
                written_goaways(&state),
                [
                    (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                    (StreamId::new(1), H2ErrorCode::NO_ERROR)
                ]
            );
            assert_eq!(state.borrow().close, FixtureClose::Closed);
            assert_eq!(probes.borrow()[0].completed.get(), 1);
        })
    });
}

/// A local drain honors a nonzero downstream GOAWAY timeout. While the final GOAWAY waits, turns
/// read no clock, and on a wheel host the wait creates no host sleep.
#[test]
fn local_drain_waits_the_goaway_timeout_without_reading_the_clock() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let wheel = context.host().wheel_timer().is_some();
            let (mut peer, preface) = ready_owner_requests(0);
            let gates: Vec<_> = (0..3)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect();
            let pings = gates.iter().enumerate().map(|(index, gate)| {
                FixtureRead::WaitData(
                    Arc::clone(gate),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::PingRequest {
                            data: Some(index as u64 + 1),
                        },
                    ),
                )
            });
            let reads: Vec<_> = [FixtureRead::Data(preface)]
                .into_iter()
                .chain(pings)
                .collect();
            let (session, handle, state, probes) =
                timed_session(context, reads, Vec::new(), Duration::ZERO, GRACE);
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                assert!(handle.initiate_drain().unwrap());
                settle_turns(32).await;
                assert_eq!(
                    written_goaways(&state),
                    [(GOAWAY_MAX, H2ErrorCode::NO_ERROR)]
                );
                let (now_reads, sleeps) = (host.now_reads(), host.sleeps());
                for gate in &gates {
                    gate.notify_one();
                    settle_turns(32).await;
                }
                assert_eq!(written_frames(&state, 1).ping_replies, [1, 2, 3]);
                assert_eq!(
                    host.now_reads() - now_reads,
                    0,
                    "a pending GOAWAY read the clock"
                );
                assert_eq!(host.sleeps() - sleeps, 0);
                if wheel {
                    assert_eq!(host.sleeps(), 0, "the GOAWAY wait used a host sleep");
                }
                host.advance(GRACE - Duration::from_millis(1));
                settle_turns(32).await;
                assert_eq!(
                    written_goaways(&state).len(),
                    1,
                    "the final GOAWAY came early"
                );
                host.advance(Duration::from_millis(1));
            });
            run.expect("the drain closes cleanly");
            assert_eq!(
                written_goaways(&state),
                [
                    (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                    (StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)
                ]
            );
            assert_eq!(state.borrow().close, FixtureClose::Closed);
            assert!(probes.borrow().is_empty());
        })
    });
}

/// An active connection keeps one wheel slot for its read idle timeout: a hundred requests, each
/// arriving well inside the timeout, restamp it without relinking, the wheel re-slots it about
/// once per timeout period, and it never fires.
#[test]
fn active_connection_restamps_one_read_idle_slot() {
    const REQUESTS: usize = 100;
    const SPACING: Duration = Duration::from_secs(6);
    crate::external_host_tests::run_on_wheel_host_with(|host, context| {
        Box::pin(async move {
            let (mut peer, preface) = ready_owner_requests(0);
            let gates: Vec<_> = (0..REQUESTS)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect();
            let requests = gates.iter().map(|gate| {
                FixtureRead::WaitData(Arc::clone(gate), eligibility_request(&mut peer, 0))
            });
            let reads: Vec<_> = [FixtureRead::Data(preface)]
                .into_iter()
                .chain(requests)
                .collect();
            let (session, handle, state, probes) = timed_session(
                context,
                reads,
                vec![Reply::Body(0); REQUESTS],
                IDLE,
                Duration::ZERO,
            );
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                for (served, gate) in gates.iter().enumerate() {
                    host.advance(SPACING);
                    gate.notify_one();
                    complete_credit_wait(|| {
                        written_frames(&state, REQUESTS).header_statuses.len() == served + 1
                    })
                    .await;
                }
                assert!(
                    written_frames(&state, REQUESTS).goaways.is_empty(),
                    "an active connection drained"
                );
                let stats = host.wheel_stats();
                assert_eq!(
                    stats.live_slots, 1,
                    "one slot for the connection's read idle"
                );
                assert_eq!(stats.fired, 0);
                assert_eq!(stats.relinked, 1, "restamps are stores, not relinks");
                let periods = (SPACING.as_secs() * REQUESTS as u64).div_ceil(IDLE.as_secs());
                assert!(
                    stats.reslotted <= 2 * periods,
                    "{} re-slots over {periods} timeout periods",
                    stats.reslotted
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
            assert!(
                probes
                    .borrow()
                    .iter()
                    .all(|probe| probe.completed.get() == 1)
            );
        })
    });
}

/// The clock reads of the zero-config guard: a GET round trip, as on the parent commit, and that
/// round trip plus a local drain with a zero GOAWAY timeout, one fewer than the parent's 3: the
/// final GOAWAY no longer rereads the clock to find the zero timeout passed.
const ZERO_CONFIG_ROUND_TRIP_NOW_READS: usize = 1;
const ZERO_CONFIG_DRAIN_NOW_READS: usize = 2;

/// With every timeout zero the driver installs no timer: a request round trip and a drain take no
/// wheel slot, create no host sleep, and read the clock no more often than before the driver
/// honored timeouts.
#[test]
fn zero_timeouts_take_no_slot_sleep_or_clock_read() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let wheel = context.host().wheel_timer().is_some();
            let (mut peer, preface) = ready_owner_requests(0);
            let request = eligibility_request(&mut peer, 0);
            let (session, handle, state, probes) = timed_session(
                context,
                [FixtureRead::Data([preface, request].concat())],
                vec![Reply::Body(0)],
                Duration::ZERO,
                Duration::ZERO,
            );
            let reads_before = host.now_reads();
            let (run, round_trip) = futures::join!(session.run(), async {
                complete_credit_wait(|| written_frames(&state, 1).header_statuses.len() == 1).await;
                settle_turns(32).await;
                let round_trip = host.now_reads() - reads_before;
                assert!(handle.initiate_drain().unwrap());
                round_trip
            });
            run.expect("the drained connection closes cleanly");
            let drain = host.now_reads() - reads_before;
            assert_eq!(round_trip, ZERO_CONFIG_ROUND_TRIP_NOW_READS);
            assert_eq!(drain, ZERO_CONFIG_DRAIN_NOW_READS);
            assert_eq!(host.sleeps(), 0, "a zero-config session made a host sleep");
            if wheel {
                let stats = host.wheel_stats();
                assert_eq!(stats.scheduled, 0);
                assert_eq!(stats.live_slots, 0);
            }
            assert_eq!(
                written_goaways(&state),
                [
                    (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                    (StreamId::new(1), H2ErrorCode::NO_ERROR)
                ]
            );
            assert_eq!(probes.borrow()[0].completed.get(), 1);
        })
    });
}

/// Runs `test` on a caller-driven worker over a current-thread runtime whose clock is paused, so
/// Tokio sleeps, the fixture's read timeouts and a host without a wheel all advance together.
pub(super) fn run_on_paused_worker<T: Send + 'static>(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
) -> T {
    Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("paused current-thread runtime")
        .block_on(LocalSet::new().run_until(async move {
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
        }))
}

#[derive(Clone, Copy, Debug)]
enum DrainStart {
    /// The connection idles until its read timeout drains it.
    Idle,
    /// The application drains the connection at once.
    Local,
}

/// Scripted reads that both drivers time the same way. A read waits for its gate, if any, and a
/// nonzero read timeout ends a read still waiting with `READ_TIMEOUT`, keeping its bytes for the
/// next read, as a socket read does. Past the script, a read waits for cancellation or its
/// timeout.
pub(super) struct GatedReads {
    pub(super) inner: Rc<FixtureTransport>,
    pub(super) script: Rc<RefCell<VecDeque<(Option<Arc<tokio::sync::Notify>>, Vec<u8>)>>>,
}

impl UniplexTransport for GatedReads {
    fn read(
        &self,
        context: &WorkerContext,
        _maximum: usize,
        _allocation: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let front = self.script.borrow().front().cloned();
        let script = Rc::clone(&self.script);
        let baton = CancellableBaton::with_host(context.host().clone());
        Box::pin(async move {
            let ready = async {
                match &front {
                    Some((Some(gate), _)) => gate.notified().await,
                    Some((None, _)) => {}
                    None => std::future::pending().await,
                }
            };
            let expired = async {
                if timeout.is_zero() {
                    std::future::pending::<()>().await;
                }
                tokio::time::sleep(timeout).await;
            };
            tokio::select! {
                biased;
                () = ready => {
                    let (_, bytes) = script.borrow_mut().pop_front().expect("the scripted read");
                    Ok(UniplexRead::new(queue(&bytes), false))
                }
                () = expired => Err(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"scripted H2 read timeout",
                )),
                _ = baton.wait(cancellation) => Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"scripted H2 read cancelled",
                )),
            }
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

/// What a downstream driver wrote while it drained an idle connection.
struct DrainWire {
    /// `(milliseconds since start, last stream)` of each GOAWAY, sampled every millisecond of
    /// paused time until the session ended.
    goaways: Vec<(u64, StreamId)>,
    header_statuses: Vec<(StreamId, u16)>,
    resets: Vec<(StreamId, H2ErrorCode)>,
}

/// Drains an idle connection on the owned driver or on `UniplexSession`. A GET arrives at
/// `request_at`, if given.
async fn drain_wire(
    context: WorkerContext,
    owned: bool,
    start: DrainStart,
    read_timeout: Duration,
    goaway_timeout: Duration,
    request_at: Option<Duration>,
) -> DrainWire {
    let (mut peer, preface) = ready_owner_requests(0);
    let gate = Arc::new(tokio::sync::Notify::new());
    let mut script = VecDeque::from([(None, preface)]);
    if request_at.is_some() {
        script.push_back((Some(Arc::clone(&gate)), eligibility_request(&mut peer, 0)));
    }
    let (inner, state) = FixtureTransport::new([]);
    let transport = Rc::new(GatedReads {
        inner,
        script: Rc::new(RefCell::new(script)),
    });
    let begun = tokio::time::Instant::now();
    let done = Rc::new(Cell::new(false));
    let session: Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>>>> = if owned {
        let (application, _probes) = Application::new(vec![Reply::Body(0)]);
        let mut config = options(context, transport);
        config.io.read_timeout = read_timeout;
        config.http2.goaway_timeout = goaway_timeout;
        let (session, handle) = OwnedH2Session::new(application, config).expect("owned H2");
        if let DrainStart::Local = start {
            assert!(handle.initiate_drain().unwrap());
        }
        Box::pin(session.run())
    } else {
        let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
            status: 200,
            response_body: 0,
            response_error: None,
            invocations: Rc::new(Cell::new(0)),
            request_body: Rc::new(Cell::new(0)),
        });
        let config = UniplexConfig {
            read_timeout,
            ..UniplexConfig::default()
        };
        let http2 = Http2SessionConfig {
            goaway_timeout,
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
        .expect("Uniplex H2");
        if let DrainStart::Local = start {
            assert!(handle.initiate_drain().unwrap());
        }
        Box::pin(session.run())
    };
    let monitor = async {
        let mut timeline = Vec::new();
        loop {
            let at = begun.elapsed();
            if request_at.is_some_and(|request_at| at >= request_at) {
                gate.notify_one();
            }
            let goaways = written_goaways(&state);
            timeline.extend(goaways[timeline.len()..].iter().map(|(stream, code)| {
                assert_eq!(*code, H2ErrorCode::NO_ERROR);
                (at.as_millis() as u64, *stream)
            }));
            if done.get() {
                return timeline;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let run = async {
        let result = session.await;
        done.set(true);
        result
    };
    let (result, goaways) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(run, monitor)
    })
    .await
    .expect("the drain ends");
    result.expect("the drain closes cleanly");
    let frames = written_frames(&state, 1);
    DrainWire {
        goaways,
        header_statuses: frames.header_statuses,
        resets: frames.resets,
    }
}

/// Asserts the same GOAWAYs, each within the millisecond the timelines sample at.
fn assert_same_goaways(owned: &[(u64, StreamId)], uniplex: &[(u64, StreamId)]) {
    assert_eq!(
        owned.iter().map(|(_, stream)| *stream).collect::<Vec<_>>(),
        uniplex
            .iter()
            .map(|(_, stream)| *stream)
            .collect::<Vec<_>>(),
    );
    for ((owned_at, _), (uniplex_at, _)) in owned.iter().zip(uniplex) {
        assert!(
            owned_at.abs_diff(*uniplex_at) <= 1,
            "owned {owned:?}, uniplex {uniplex:?}"
        );
    }
}

/// Downstream drains of an idle connection, owned against `UniplexSession` on the same paused
/// clock: a local drain, and an idle drain with a zero and with a nonzero GOAWAY timeout, write
/// the same GOAWAYs at the same instants, and a request inside a local drain's grace is served
/// by both. A request inside an idle drain's grace is served by the owned driver, as Proxygen's
/// read timeout (`initiateDrain`) keeps the grace, while `UniplexSession` keeps grace only for a
/// local drain and refuses it (`DIV-028`).
#[test]
fn owned_drain_goaways_match_uniplex_apart_from_idle_grace() {
    let half = GRACE / 2;
    let cases = [
        (DrainStart::Local, Duration::ZERO, GRACE, None),
        (DrainStart::Idle, IDLE, Duration::ZERO, None),
        (DrainStart::Idle, IDLE, GRACE, None),
        (DrainStart::Local, Duration::ZERO, GRACE, Some(half)),
        (DrainStart::Idle, IDLE, GRACE, Some(IDLE + half)),
    ];
    for (start, read_timeout, goaway_timeout, request_at) in cases {
        let (owned, uniplex) = run_on_paused_worker(move |context| {
            Box::pin(async move {
                let owned = drain_wire(
                    context.clone(),
                    true,
                    start,
                    read_timeout,
                    goaway_timeout,
                    request_at,
                )
                .await;
                let uniplex = drain_wire(
                    context,
                    false,
                    start,
                    read_timeout,
                    goaway_timeout,
                    request_at,
                )
                .await;
                (owned, uniplex)
            })
        });
        assert_same_goaways(&owned.goaways, &uniplex.goaways);
        let idle_grace = matches!(start, DrainStart::Idle) && request_at.is_some();
        if !idle_grace {
            assert_eq!(owned.header_statuses, uniplex.header_statuses);
            assert_eq!(owned.resets, uniplex.resets);
            continue;
        }
        assert_eq!(owned.header_statuses, [(StreamId::new(1), 200)]);
        assert!(owned.resets.is_empty());
        // Until `UniplexSession` keeps grace for an idle drain, it refuses the request, and with
        // PROTOCOL_ERROR rather than REFUSED_STREAM.
        assert!(uniplex.header_statuses.is_empty());
        assert_eq!(
            uniplex.resets,
            [(StreamId::new(1), H2ErrorCode::PROTOCOL_ERROR)]
        );
    }
}

/// Records `session`'s lifecycle events.
fn observe_lifecycle(
    session: &mut OwnedH2Session<Application>,
) -> Rc<RefCell<Vec<LifecycleEvent>>> {
    let events = Rc::new(RefCell::new(Vec::new()));
    session
        .add_lifecycle_observer(Rc::new(LifecycleProbe {
            events: Rc::clone(&events),
        }))
        .unwrap();
    events
}

fn drains_started(events: &[LifecycleEvent]) -> Vec<LifecycleEvent> {
    events
        .iter()
        .filter(|event| matches!(event, LifecycleEvent::DrainStarted(_)))
        .copied()
        .collect()
}

/// An observer sees an idle drain as `DrainStarted(IdleTimeout)`, once: the read idle expiries
/// that follow while the drain waits out its GOAWAY timeout report nothing more.
#[test]
fn an_idle_drain_reports_one_idle_timeout_drain() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (_, preface) = ready_owner_requests(0);
            let (mut session, _handle, state, _probes) = timed_session(
                context,
                [FixtureRead::Data(preface)],
                Vec::new(),
                IDLE,
                IDLE * 2 + IDLE / 2,
            );
            let events = observe_lifecycle(&mut session);
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                for _ in 0..3 {
                    host.advance(IDLE);
                    settle_turns(32).await;
                }
                assert_eq!(
                    written_goaways(&state),
                    [(GOAWAY_MAX, H2ErrorCode::NO_ERROR)],
                    "two read idle expiries during the drain sent nothing"
                );
                host.advance(IDLE / 2);
            });
            run.expect("an idle drain closes cleanly");
            assert_eq!(
                written_goaways(&state),
                [
                    (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                    (StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)
                ]
            );
            let events = events.take();
            assert_eq!(
                drains_started(&events),
                [LifecycleEvent::DrainStarted(DrainTrigger::IdleTimeout)],
                "{events:?}"
            );
            assert_eq!(events.last(), Some(&LifecycleEvent::Destroy));
        })
    });
}

/// On a connection with a read idle timeout and a GOAWAY timeout, an observer sees a header-rate
/// breach as `DrainStarted(HeaderRateLimit)`, and C7's one final GOAWAY is all the wire gets: it
/// disarms nothing that could send a second one, and a later read idle expiry starts no drain.
/// The accepted stream's response holds its end after one body byte while the clock passes the
/// GOAWAY timeout and then the read idle timeout, so the connection is still in its final drain
/// when both would fire.
#[test]
fn a_header_rate_limit_trip_on_a_timed_connection_reports_one_drain() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (fixture, state) =
                FixtureTransport::new([FixtureRead::Data(peer_two_requests_bytes())]);
            let (mut application, probes) = Application::new(vec![Reply::Body(1); 2]);
            let release = Arc::new(tokio::sync::Notify::new());
            application.empty_terminal = Some(Arc::clone(&release));
            let mut config = options(context, Rc::new(UntimedReads(fixture)));
            config.io.read_timeout = IDLE;
            config.http2.goaway_timeout = GRACE;
            config.http2.rate_limits = Http2RateLimitConfig {
                header_frames: RateLimitConfig::new(1, Duration::from_secs(60), 0),
                ..unbounded_rates()
            };
            let (mut session, _) = OwnedH2Session::new(application, config).unwrap();
            let events = observe_lifecycle(&mut session);
            let done = Cell::new(false);
            let run = async {
                let result = session.run().await;
                done.set(true);
                result
            };
            let (run, ()) = futures::join!(run, async {
                settle_turns(64).await;
                let final_goaway = [(StreamId::new(3), H2ErrorCode::NO_ERROR)];
                assert_eq!(written_frames(&state, 2).goaways, final_goaway);
                host.advance(GRACE);
                settle_turns(32).await;
                host.advance(IDLE);
                settle_turns(32).await;
                assert!(
                    !done.get(),
                    "the open response keeps the final drain running"
                );
                assert_eq!(probes.borrow()[0].completed.get(), 0);
                assert_eq!(
                    written_frames(&state, 2).goaways,
                    final_goaway,
                    "the GOAWAY and read idle expiries during the final drain sent nothing"
                );
                release.notify_one();
            });
            run.expect("a header breach drains gracefully");
            let written = written_frames(&state, 2);
            assert_eq!(written.goaways, [(StreamId::new(3), H2ErrorCode::NO_ERROR)]);
            assert!(written.resets.is_empty());
            assert_eq!(written.header_statuses, [(StreamId::new(1), 200)]);
            assert_eq!(probes.borrow()[0].completed.get(), 1);
            let events = events.take();
            assert_eq!(
                drains_started(&events),
                [LifecycleEvent::DrainStarted(DrainTrigger::HeaderRateLimit)],
                "{events:?}"
            );
        })
    });
}

/// A connection without an observer creates no `Observation` (C4's zero-config path), on both
/// hosts: not for requests served to EOF or a local drain with every timeout zero, nor for an
/// idle drain on a timed connection, whose `DrainStarted(IdleTimeout)` has nobody to go to.
/// Adding an observer creates exactly one.
#[test]
fn unobserved_connections_create_no_observation() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let created = crate::owned_h2::observations_created();
            for drain in [false, true] {
                let (transport, _) =
                    FixtureTransport::new([FixtureRead::DataEof(peer_two_requests_bytes())]);
                let (application, probes) = Application::new(vec![Reply::Body(0); 2]);
                let (session, handle) =
                    OwnedH2Session::new(application, options(context.clone(), transport)).unwrap();
                if drain {
                    assert!(handle.initiate_drain().unwrap());
                }
                session.run().await.expect("the connection closes cleanly");
                if !drain {
                    assert_eq!(probes.borrow().len(), 2);
                }
            }

            let (_, preface) = ready_owner_requests(0);
            let (session, _handle, state, _probes) = timed_session(
                context.clone(),
                [FixtureRead::Data(preface)],
                Vec::new(),
                IDLE,
                Duration::ZERO,
            );
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                host.advance(IDLE);
            });
            run.expect("an idle drain closes cleanly");
            assert_eq!(written_goaways(&state).len(), 2);
            assert_eq!(
                crate::owned_h2::observations_created(),
                created,
                "an unobserved connection created an Observation"
            );

            let (mut session, ..) = timed_session(context, [], Vec::new(), IDLE, Duration::ZERO);
            observe_lifecycle(&mut session);
            observe_lifecycle(&mut session);
            assert_eq!(crate::owned_h2::observations_created(), created + 1);
        })
    });
}

/// The write timeout of the write-timer tests: Proxygen's `writeTimeout_`, which bounds a stalled
/// transport write and the writer's wait for flow-control credit.
const WRITE: Duration = Duration::from_secs(1);

/// How a downstream driver's session ended once its transport stopped accepting writes.
#[derive(Debug, Eq, PartialEq)]
struct StalledWriteEnd {
    /// Milliseconds of paused time from the start to the end of the session.
    ended_at: u128,
    error: Option<HttpErrorCode>,
    close: FixtureClose,
}

/// Runs a downstream driver, the owned one or `UniplexSession`, whose transport accepts no write:
/// the fixture ends a stalled write with `WRITE_TIMEOUT` once its timeout passes.
async fn stalled_write_end(context: WorkerContext, owned: bool) -> StalledWriteEnd {
    let (_, preface) = ready_owner_requests(0);
    let (transport, state) = FixtureTransport::new([FixtureRead::Data(preface)]);
    state.borrow_mut().stall_writes = true;
    let begun = tokio::time::Instant::now();
    let result = if owned {
        let (application, _probes) = Application::new(Vec::new());
        let mut config = options(context, transport);
        config.io.write_timeout = WRITE;
        let (session, _handle) = OwnedH2Session::new(application, config).expect("owned H2");
        session.run().await
    } else {
        let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
            status: 200,
            response_body: 0,
            response_error: None,
            invocations: Rc::new(Cell::new(0)),
            request_body: Rc::new(Cell::new(0)),
        });
        let config = UniplexConfig {
            write_timeout: WRITE,
            ..UniplexConfig::default()
        };
        let (session, _handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Downstream,
            transport,
            Some(handler),
            config,
            Http2SessionConfig::default(),
        )
        .expect("Uniplex H2");
        session.run().await
    };
    let ended_at = begun.elapsed().as_millis();
    assert_eq!(state.borrow().stalled_writes_started, 1);
    let close = state.borrow().close;
    StalledWriteEnd {
        ended_at,
        error: result.err().map(|error| error.code()),
        close,
    }
}

/// The owned driver hands its write timeout to the transport, which arms it only while a write
/// pends: a transport that accepts nothing fails the connection with `WRITE_TIMEOUT` and resets
/// it, at the instant and in the way `UniplexSession` does.
#[test]
fn owned_write_stall_fails_the_connection_like_uniplex() {
    let (owned, uniplex) = run_on_paused_worker(|context| {
        Box::pin(async move {
            let owned = stalled_write_end(context.clone(), true).await;
            let uniplex = stalled_write_end(context, false).await;
            (owned, uniplex)
        })
    });
    assert_eq!(
        uniplex,
        StalledWriteEnd {
            ended_at: WRITE.as_millis(),
            error: Some(HttpErrorCode::WRITE_TIMEOUT),
            close: FixtureClose::Reset,
        }
    );
    assert_eq!(owned, uniplex);
}

/// Response bytes the flow-control timer tests send on a stream: more than the 65,535 bytes of
/// credit the peer leaves on the blocked window.
const FLOW_BODY: usize = 96 << 10;

/// A downstream session over `reads` answering with `replies`, its other timeouts zero and
/// `write_timeout` as given.
fn write_timed_session(
    context: WorkerContext,
    reads: impl IntoIterator<Item = FixtureRead>,
    replies: Vec<Reply>,
    write_timeout: Duration,
) -> (
    OwnedH2Session<Application>,
    OwnedH2Handle,
    Rc<RefCell<FixtureTransportState>>,
    Rc<RefCell<Vec<Rc<Probe>>>>,
) {
    let (fixture, state) = FixtureTransport::new(reads);
    let (application, probes) = Application::new(replies);
    let mut config = options(context, Rc::new(UntimedReads(fixture)));
    config.io.write_timeout = write_timeout;
    let (session, handle) = OwnedH2Session::new(application, config).expect("write-timed owned H2");
    (session, handle, state, probes)
}

/// The client preface, a GET on stream 1, and a WINDOW_UPDATE of 65,535 that widens the
/// connection window (`connection_blocked == false`, so the stream's window blocks the response)
/// or the stream's (`connection_blocked`, so the connection's window does).
fn flow_blocked_get(connection_blocked: bool) -> (Http2Codec, Vec<u8>) {
    let (mut peer, mut input) = ready_owner_requests(1);
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::WindowUpdate {
            stream: if connection_blocked {
                StreamId::new(1)
            } else {
                StreamId::UNSPECIFIED
            },
            amount: 65_535,
        },
    ));
    (peer, input)
}

/// A response blocked on its stream's send window is reset with FLOW_CONTROL_ERROR when the
/// write timeout passes with no activity, not a millisecond earlier, as Proxygen's
/// `handleWriteEventTimeout` resets every flow-blocked stream; the connection stays open.
#[test]
fn stream_flow_control_timeout_resets_the_blocked_stream() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (_, input) = flow_blocked_get(false);
            let (session, handle, state, probes) = write_timed_session(
                context,
                [FixtureRead::Data(input)],
                vec![Reply::Body(FLOW_BODY)],
                WRITE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| written_frames(&state, 1).body_bytes == 65_535).await;
                settle_turns(32).await;
                host.advance(WRITE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(
                    written_frames(&state, 1).resets.is_empty(),
                    "the flow-control timeout came early"
                );
                host.advance(Duration::from_millis(1));
                settle_turns(32).await;
                let frames = written_frames(&state, 1);
                assert_eq!(
                    frames.resets,
                    [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
                );
                assert_eq!(frames.body_bytes, 65_535);
                assert!(frames.goaways.is_empty(), "the connection stays open");
                assert_eq!(
                    probes.borrow()[0].cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::FLOW_CONTROL_ERROR, false)]
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
            assert_eq!(state.borrow().close, FixtureClose::Closed);
        })
    });
}

/// A response blocked on the connection's send window, with its body waiting, fails the
/// connection with FLOW_CONTROL_ERROR when the write timeout passes with no activity, not a
/// millisecond earlier: Proxygen's `handleWriteEventTimeout` raises a connection error when the
/// connection is out of credit and a stream has queued body.
#[test]
fn connection_flow_control_timeout_fails_the_connection() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (_, input) = flow_blocked_get(true);
            let (session, _handle, state, probes) = write_timed_session(
                context,
                [FixtureRead::Data(input)],
                vec![Reply::Body(FLOW_BODY)],
                WRITE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| written_frames(&state, 1).body_bytes == 65_535).await;
                settle_turns(32).await;
                host.advance(WRITE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(
                    written_goaways(&state).is_empty(),
                    "the flow-control timeout came early"
                );
                assert_eq!(state.borrow().close, FixtureClose::Open);
                host.advance(Duration::from_millis(1));
            });
            let error = run.expect_err("the flow-control timeout fails the connection");
            assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
            let frames = written_frames(&state, 1);
            assert_eq!(
                frames.goaways,
                [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
            );
            assert!(frames.resets.is_empty());
            assert_eq!(frames.body_bytes, 65_535);
            assert_eq!(state.borrow().close, FixtureClose::Reset);
            assert_eq!(
                probes.borrow()[0].cancelled.borrow().as_slice(),
                &[(HttpErrorCode::FLOW_CONTROL_ERROR, false)]
            );
        })
    });
}

/// Activity restarts the flow-control timeout, as each write event restarts Proxygen's writer
/// wait and Uniplex's `note_flow_timeout_activity` restamps its flow deadline: a PING halfway
/// through the timeout, and the PING reply written for it, move the stream reset to a full write
/// timeout after the PING.
#[test]
fn activity_restarts_the_flow_control_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, input) = flow_blocked_get(false);
            let gate = Arc::new(tokio::sync::Notify::new());
            let ping = eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(7) });
            let (session, handle, state, _probes) = write_timed_session(
                context,
                [
                    FixtureRead::Data(input),
                    FixtureRead::WaitData(Arc::clone(&gate), ping),
                ],
                vec![Reply::Body(FLOW_BODY)],
                WRITE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| written_frames(&state, 1).body_bytes == 65_535).await;
                settle_turns(32).await;
                host.advance(WRITE / 2);
                gate.notify_one();
                complete_credit_wait(|| written_frames(&state, 1).ping_replies == [7]).await;
                settle_turns(32).await;
                host.advance(WRITE / 2);
                settle_turns(32).await;
                assert!(
                    written_frames(&state, 1).resets.is_empty(),
                    "the PING did not restart the flow-control timeout"
                );
                host.advance(WRITE / 2 - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(written_frames(&state, 1).resets.is_empty());
                host.advance(Duration::from_millis(1));
                settle_turns(32).await;
                assert_eq!(
                    written_frames(&state, 1).resets,
                    [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// A drain requested in the turn that observes the flow-control expiry wins, as Proxygen's
/// `initiateDrain` signals the writer's write event: its GOAWAYs go out, the stream stays, and
/// the still-blocked stream is reset a full write timeout later, after which the drained
/// connection closes.
#[test]
fn a_drain_in_the_expiry_turn_restarts_the_flow_control_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (_, input) = flow_blocked_get(false);
            let (session, handle, state, _probes) = write_timed_session(
                context,
                [FixtureRead::Data(input)],
                vec![Reply::Body(FLOW_BODY)],
                WRITE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| written_frames(&state, 1).body_bytes == 65_535).await;
                settle_turns(32).await;
                host.advance(WRITE);
                assert!(handle.initiate_drain().unwrap());
                settle_turns(32).await;
                let frames = written_frames(&state, 1);
                assert_eq!(
                    frames.goaways,
                    [
                        (GOAWAY_MAX, H2ErrorCode::NO_ERROR),
                        (StreamId::new(1), H2ErrorCode::NO_ERROR)
                    ]
                );
                assert!(frames.resets.is_empty(), "the expiry beat the drain");
                host.advance(WRITE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(written_frames(&state, 1).resets.is_empty());
                host.advance(Duration::from_millis(1));
            });
            run.expect("the drained connection closes once the stream is reset");
            assert_eq!(
                written_frames(&state, 1).resets,
                [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
            );
            assert_eq!(state.borrow().close, FixtureClose::Closed);
        })
    });
}

/// What a downstream driver wrote while its one response waited on flow control: each RST_STREAM
/// and GOAWAY with the millisecond of paused time it was first seen at, and how the session
/// ended.
#[derive(Debug)]
struct FlowWire {
    resets: Vec<(u64, StreamId, H2ErrorCode)>,
    goaways: Vec<(u64, StreamId, H2ErrorCode)>,
    header_statuses: Vec<(StreamId, u16)>,
    body_bytes: usize,
    error: Option<HttpErrorCode>,
    close: FixtureClose,
}

/// Serves `flow_blocked_get(connection_blocked)` with a response of `body` bytes on the owned
/// driver or on `UniplexSession`, both with `WRITE` as the write timeout and a zero GOAWAY
/// timeout. A PING arrives at `ping_at`, if given, and the application drains the connection at
/// `drain_at` if it is still open.
async fn flow_wire(
    context: WorkerContext,
    owned: bool,
    connection_blocked: bool,
    body: usize,
    ping_at: Option<Duration>,
    drain_at: Duration,
) -> FlowWire {
    let (mut peer, input) = flow_blocked_get(connection_blocked);
    let gate = Arc::new(tokio::sync::Notify::new());
    let mut script = VecDeque::from([(None, input)]);
    if ping_at.is_some() {
        script.push_back((
            Some(Arc::clone(&gate)),
            eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(7) }),
        ));
    }
    let (inner, state) = FixtureTransport::new([]);
    let transport = Rc::new(GatedReads {
        inner,
        script: Rc::new(RefCell::new(script)),
    });
    let begun = tokio::time::Instant::now();
    let done = Rc::new(Cell::new(false));
    type Run = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>>>>;
    let (session, drain): (Run, Box<dyn Fn()>) = if owned {
        let (application, _probes) = Application::new(vec![Reply::Body(body)]);
        let mut config = options(context, transport);
        config.io.write_timeout = WRITE;
        let (session, handle) = OwnedH2Session::new(application, config).expect("owned H2");
        (
            Box::pin(session.run()),
            Box::new(move || assert!(handle.initiate_drain().unwrap())),
        )
    } else {
        let handler: Rc<dyn HttpHandler> = Rc::new(FixtureHandler {
            status: 200,
            response_body: body,
            response_error: None,
            invocations: Rc::new(Cell::new(0)),
            request_body: Rc::new(Cell::new(0)),
        });
        let config = UniplexConfig {
            write_timeout: WRITE,
            ..UniplexConfig::default()
        };
        let http2 = Http2SessionConfig {
            goaway_timeout: Duration::ZERO,
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
        .expect("Uniplex H2");
        (
            Box::pin(session.run()),
            Box::new(move || assert!(handle.initiate_drain().unwrap())),
        )
    };
    let monitor = async {
        let (mut resets, mut goaways) = (Vec::new(), Vec::new());
        let mut drained = false;
        loop {
            let at = begun.elapsed();
            if ping_at.is_some_and(|ping_at| at >= ping_at) {
                gate.notify_one();
            }
            if !drained && !done.get() && at >= drain_at {
                drained = true;
                drain();
            }
            let ms = at.as_millis() as u64;
            let frames = written_frames(&state, 1);
            resets.extend(
                frames.resets[resets.len()..]
                    .iter()
                    .map(|(stream, code)| (ms, *stream, *code)),
            );
            goaways.extend(
                frames.goaways[goaways.len()..]
                    .iter()
                    .map(|(stream, code)| (ms, *stream, *code)),
            );
            if done.get() {
                return (resets, goaways);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let run = async {
        let result = session.await;
        done.set(true);
        result
    };
    let (result, (resets, goaways)) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(run, monitor)
    })
    .await
    .expect("the flow-blocked session ends");
    let frames = written_frames(&state, 1);
    let close = state.borrow().close;
    FlowWire {
        resets,
        goaways,
        header_statuses: frames.header_statuses,
        body_bytes: frames.body_bytes,
        error: result.err().map(|error| error.code()),
        close,
    }
}

/// Asserts `timeline` holds `expected`, each frame seen within the millisecond the timelines
/// sample at.
fn assert_timeline(
    timeline: &[(u64, StreamId, H2ErrorCode)],
    expected: &[(Duration, StreamId, H2ErrorCode)],
) {
    let at = |expected: &[(Duration, StreamId, H2ErrorCode)]| -> Vec<(u64, StreamId, H2ErrorCode)> {
        expected
            .iter()
            .map(|(at, stream, code)| (at.as_millis() as u64, *stream, *code))
            .collect()
    };
    assert_same_timeline(timeline, &at(expected));
}

/// Asserts the same frames, each seen within the millisecond the timelines sample at.
fn assert_same_timeline(
    owned: &[(u64, StreamId, H2ErrorCode)],
    uniplex: &[(u64, StreamId, H2ErrorCode)],
) {
    let frames = |timeline: &[(u64, StreamId, H2ErrorCode)]| -> Vec<(StreamId, H2ErrorCode)> {
        timeline
            .iter()
            .map(|(_, stream, code)| (*stream, *code))
            .collect()
    };
    assert_eq!(
        frames(owned),
        frames(uniplex),
        "owned {owned:?}, uniplex {uniplex:?}"
    );
    for ((owned_at, ..), (uniplex_at, ..)) in owned.iter().zip(uniplex) {
        assert!(
            owned_at.abs_diff(*uniplex_at) <= 1,
            "owned {owned:?}, uniplex {uniplex:?}"
        );
    }
}

/// Flow-control timeouts, owned against `UniplexSession` on the same paused clock: a response
/// blocked on its stream window is reset with FLOW_CONTROL_ERROR at the write timeout and the
/// connection drains later, and one blocked on the connection window fails the connection with
/// GOAWAY(FLOW_CONTROL_ERROR) and a transport reset at the write timeout. Both drivers write the
/// same frames at the same instants and end with the same result. (A drained connection then
/// closes differently, which is not a timer property: the owned driver closes the transport
/// where `UniplexSession` shuts down its write side.)
#[test]
fn owned_flow_control_timeouts_match_uniplex() {
    let drain_at = 3 * WRITE;
    for connection_blocked in [false, true] {
        let (owned, uniplex) = run_on_paused_worker(move |context| {
            Box::pin(async move {
                let owned = flow_wire(
                    context.clone(),
                    true,
                    connection_blocked,
                    FLOW_BODY,
                    None,
                    drain_at,
                )
                .await;
                let uniplex = flow_wire(
                    context,
                    false,
                    connection_blocked,
                    FLOW_BODY,
                    None,
                    drain_at,
                )
                .await;
                (owned, uniplex)
            })
        });
        assert_same_timeline(&owned.resets, &uniplex.resets);
        assert_same_timeline(&owned.goaways, &uniplex.goaways);
        assert_eq!(owned.header_statuses, uniplex.header_statuses);
        assert_eq!(owned.body_bytes, uniplex.body_bytes);
        assert_eq!(owned.error, uniplex.error);
        let timeout = [(WRITE, StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)];
        if connection_blocked {
            assert_eq!(owned.close, uniplex.close);
            assert!(owned.resets.is_empty());
            assert_timeline(&owned.goaways, &timeout);
            assert_eq!(owned.error, Some(HttpErrorCode::FLOW_CONTROL_ERROR));
            assert_eq!(owned.close, FixtureClose::Reset);
        } else {
            assert_timeline(&owned.resets, &timeout);
            assert_eq!(owned.error, None);
        }
    }
}

/// `UniplexSession`'s H2 buffer timeout, on the owned axis (`DIV-028`). A response blocked on its
/// stream window fills Uniplex's egress body queue, which arms a buffer deadline that activity
/// does not move; a PING halfway through the write timeout moves only the flow deadline, so
/// Uniplex's buffer timeout fires first and fails the stream's source with READ_TIMEOUT ("timed
/// out waiting for buffer space"). The owned driver reads response body only within credit and
/// has no egress body queue to time: the PING restarts its flow-control timeout, which resets the
/// stream with FLOW_CONTROL_ERROR a full write timeout after the PING.
#[test]
fn uniplex_buffer_timeout_is_a_flow_control_timeout_on_the_owned_driver() {
    // Enough that what the window holds back fills Uniplex's 65,535-byte egress body queue.
    const BUFFER_BODY: usize = 256 << 10;
    let drain_at = 3 * WRITE;
    let ping_at = WRITE / 2;
    let (owned, uniplex) = run_on_paused_worker(move |context| {
        Box::pin(async move {
            let owned = flow_wire(
                context.clone(),
                true,
                false,
                BUFFER_BODY,
                Some(ping_at),
                drain_at,
            )
            .await;
            let uniplex =
                flow_wire(context, false, false, BUFFER_BODY, Some(ping_at), drain_at).await;
            (owned, uniplex)
        })
    });
    assert_timeline(
        &owned.resets,
        &[(
            ping_at + WRITE,
            StreamId::new(1),
            H2ErrorCode::FLOW_CONTROL_ERROR,
        )],
    );
    // Uniplex's buffer timeout fails the source with READ_TIMEOUT, which resets the stream with
    // INTERNAL_ERROR, at the buffer deadline the PING did not move.
    assert_timeline(
        &uniplex.resets,
        &[(WRITE, StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)],
    );
    assert_eq!(owned.header_statuses, uniplex.header_statuses);
    assert_eq!(owned.body_bytes, uniplex.body_bytes);
    assert_same_timeline(&owned.goaways, &uniplex.goaways);
    assert_eq!(owned.error, None);
    assert_eq!(uniplex.error, None);
}

/// `streams` GETs whose responses each need `extra` bytes beyond the 65,535-byte stream window,
/// with the connection window widened so only the stream windows block.
fn st6b_stream_blocked_gets(streams: usize) -> (Http2Codec, Vec<u8>) {
    let (mut peer, mut input) = ready_owner_requests(streams);
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::WindowUpdate {
            stream: StreamId::UNSPECIFIED,
            amount: i32::MAX as u32 - 65_535,
        },
    ));
    (peer, input)
}

/// A SETTINGS change of the initial window adjusts every stream's window at once (the table's
/// `iter_mut`, which refreshes the whole flow index). Streams that the change unblocks by one
/// byte and that then block again keep the flow slot, restamped at the change: they are reset a
/// full write timeout after it, not a millisecond earlier.
#[test]
fn flow_index_follows_an_initial_window_change_to_every_stream() {
    const STREAMS: usize = 4;
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, input) = st6b_stream_blocked_gets(STREAMS);
            let grow = eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: 65_536,
                    }],
                },
            );
            let gate = Arc::new(tokio::sync::Notify::new());
            let (session, handle, state, _probes) = write_timed_session(
                context,
                [
                    FixtureRead::Data(input),
                    FixtureRead::WaitData(Arc::clone(&gate), grow),
                ],
                vec![Reply::Body(65_535 + 2); STREAMS],
                WRITE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| {
                    written_frames(&state, STREAMS).body_bytes == 65_535 * STREAMS
                })
                .await;
                settle_turns(32).await;
                host.advance(WRITE / 2);
                let refreshes = crate::owned_h2::FLOW_FULL_REFRESHES.with(Cell::get);
                gate.notify_one();
                complete_credit_wait(|| {
                    written_frames(&state, STREAMS).body_bytes == 65_536 * STREAMS
                })
                .await;
                settle_turns(32).await;
                assert!(
                    crate::owned_h2::FLOW_FULL_REFRESHES.with(Cell::get) > refreshes,
                    "the flow index was refreshed in full"
                );
                host.advance(WRITE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(
                    written_frames(&state, STREAMS).resets.is_empty(),
                    "the window change restamps the flow slot"
                );
                host.advance(Duration::from_millis(1));
                settle_turns(32).await;
                let frames = written_frames(&state, STREAMS);
                assert_eq!(
                    frames.resets,
                    (0..STREAMS as u64)
                        .map(|index| (
                            StreamId::new(2 * index + 1),
                            H2ErrorCode::FLOW_CONTROL_ERROR
                        ))
                        .collect::<Vec<_>>()
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// More stream accesses between two idle checks than the flow index tracks one by one (here,
/// interleaved WINDOW_UPDATEs in one read) fall back to a full refresh, which still finds every
/// stream that blocks again; they are reset a full write timeout after the read.
#[test]
fn flow_index_overflow_refreshes_every_stream() {
    const STREAMS: usize = 8;
    const ROUNDS: u32 = 10;
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, input) = st6b_stream_blocked_gets(STREAMS);
            let mut updates = Vec::new();
            for _ in 0..ROUNDS {
                for index in 0..STREAMS as u64 {
                    updates.extend(eligibility_frame(
                        &mut peer,
                        EgressFrame::WindowUpdate {
                            stream: StreamId::new(2 * index + 1),
                            amount: 1,
                        },
                    ));
                }
            }
            let gate = Arc::new(tokio::sync::Notify::new());
            let (session, handle, state, _probes) = write_timed_session(
                context,
                [
                    FixtureRead::Data(input),
                    FixtureRead::WaitData(Arc::clone(&gate), updates),
                ],
                vec![Reply::Body(65_535 + 100); STREAMS],
                WRITE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| {
                    written_frames(&state, STREAMS).body_bytes == 65_535 * STREAMS
                })
                .await;
                settle_turns(32).await;
                host.advance(WRITE / 2);
                let refreshes = crate::owned_h2::FLOW_FULL_REFRESHES.with(Cell::get);
                gate.notify_one();
                complete_credit_wait(|| {
                    written_frames(&state, STREAMS).body_bytes
                        == (65_535 + ROUNDS as usize) * STREAMS
                })
                .await;
                settle_turns(32).await;
                assert!(
                    crate::owned_h2::FLOW_FULL_REFRESHES.with(Cell::get) > refreshes,
                    "the flow index was refreshed in full"
                );
                host.advance(WRITE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(
                    written_frames(&state, STREAMS).resets.is_empty(),
                    "the read restamps the flow slot"
                );
                host.advance(Duration::from_millis(1));
                settle_turns(32).await;
                assert_eq!(
                    written_frames(&state, STREAMS).resets,
                    (0..STREAMS as u64)
                        .map(|index| (
                            StreamId::new(2 * index + 1),
                            H2ErrorCode::FLOW_CONTROL_ERROR
                        ))
                        .collect::<Vec<_>>()
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// A SETTINGS change to an initial window that the open streams have already used up blocks
/// every one of them on its own window, though none has a body waiting and none is polled or
/// serialized again (the window change reaches them only through the stream table's `iter_mut`):
/// each is reset with FLOW_CONTROL_ERROR a full write timeout after the change.
#[test]
fn initial_window_shrink_blocks_idle_open_streams_at_the_flow_timeout() {
    const STREAMS: usize = 4;
    const BODY: usize = 1_000;
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, input) = st6b_stream_blocked_gets(STREAMS);
            let shrink = eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: BODY as u64,
                    }],
                },
            );
            let gate = Arc::new(tokio::sync::Notify::new());
            let terminal = Arc::new(tokio::sync::Notify::new());
            let (fixture, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&gate), shrink),
            ]);
            let (mut application, _probes) = Application::new(vec![Reply::Body(BODY); STREAMS]);
            application.empty_terminal = Some(Arc::clone(&terminal));
            let mut config = options(context, Rc::new(UntimedReads(fixture)));
            config.io.write_timeout = WRITE;
            let (session, handle) = OwnedH2Session::new(application, config).expect("owned H2");
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| {
                    written_frames(&state, STREAMS).body_bytes == BODY * STREAMS
                })
                .await;
                settle_turns(32).await;
                host.advance(WRITE * 2);
                settle_turns(32).await;
                assert!(
                    written_frames(&state, STREAMS).resets.is_empty(),
                    "open streams with send credit are not flow-blocked"
                );
                gate.notify_one();
                settle_turns(32).await;
                host.advance(WRITE - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(
                    written_frames(&state, STREAMS).resets.is_empty(),
                    "the flow-control timeout came early"
                );
                host.advance(Duration::from_millis(1));
                settle_turns(32).await;
                assert_eq!(
                    written_frames(&state, STREAMS).resets,
                    (0..STREAMS as u64)
                        .map(|index| (
                            StreamId::new(2 * index + 1),
                            H2ErrorCode::FLOW_CONTROL_ERROR
                        ))
                        .collect::<Vec<_>>()
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// A connection WINDOW_UPDATE whose credit the stream's own window cuts short exhausts that
/// window in a serialization that polls no owner, since body stays pending: the stream turns
/// from connection-blocked to blocked on its own window, and the flow-control timeout resets it
/// with FLOW_CONTROL_ERROR a full write timeout after the update, not a millisecond earlier.
#[test]
fn connection_credit_cut_short_by_the_stream_window_arms_the_stream_flow_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, mut input) = ready_owner_requests(1);
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::new(1),
                    amount: 50,
                },
            ));
            let credit = eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 1_000,
                },
            );
            let gate = Arc::new(tokio::sync::Notify::new());
            let (session, handle, state, _probes) = write_timed_session(
                context,
                [
                    FixtureRead::Data(input),
                    FixtureRead::WaitData(Arc::clone(&gate), credit),
                ],
                vec![Reply::Body(65_535 + 100)],
                WRITE,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| written_frames(&state, 1).body_bytes == 65_535).await;
                settle_turns(32).await;
                host.advance(WRITE / 2);
                gate.notify_one();
                complete_credit_wait(|| written_frames(&state, 1).body_bytes == 65_535 + 50).await;
                settle_turns(32).await;
                host.advance(WRITE - Duration::from_millis(1));
                settle_turns(32).await;
                let frames = written_frames(&state, 1);
                assert!(
                    frames.resets.is_empty(),
                    "the flow-control timeout came early"
                );
                assert!(
                    frames.goaways.is_empty(),
                    "the connection is no longer blocked"
                );
                host.advance(Duration::from_millis(1));
                settle_turns(32).await;
                let frames = written_frames(&state, 1);
                assert_eq!(
                    frames.resets,
                    [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
                );
                assert!(frames.goaways.is_empty());
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// A mass unblock: one read of WINDOW_UPDATE frames, in ascending stream order, unblocks every
/// idle stream that a SETTINGS change blocked on its own window. The next idle check refreshes
/// the flow index in one full pass instead of taking the streams out of its sets one by one, and
/// finds what a pass over the streams finds: nothing blocked, so no stream is reset.
#[test]
fn flow_index_refreshes_in_full_after_a_mass_unblock() {
    const STREAMS: usize = 100;
    const BODY: usize = 1_000;
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (mut peer, input) = st6b_stream_blocked_gets(STREAMS);
            let shrink = eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: BODY as u64,
                    }],
                },
            );
            let mut unblock = Vec::new();
            for index in 0..STREAMS as u64 {
                unblock.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::WindowUpdate {
                        stream: StreamId::new(2 * index + 1),
                        amount: 1,
                    },
                ));
            }
            let shrink_gate = Arc::new(tokio::sync::Notify::new());
            let unblock_gate = Arc::new(tokio::sync::Notify::new());
            let terminal = Arc::new(tokio::sync::Notify::new());
            let (fixture, state) = FixtureTransport::new([
                FixtureRead::Data(input),
                FixtureRead::WaitData(Arc::clone(&shrink_gate), shrink),
                FixtureRead::WaitData(Arc::clone(&unblock_gate), unblock),
            ]);
            let (mut application, _probes) = Application::new(vec![Reply::Body(BODY); STREAMS]);
            application.empty_terminal = Some(Arc::clone(&terminal));
            let mut config = options(context, Rc::new(UntimedReads(fixture)));
            config.io.write_timeout = WRITE;
            let (session, handle) = OwnedH2Session::new(application, config).expect("owned H2");
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| {
                    written_frames(&state, STREAMS).body_bytes == BODY * STREAMS
                })
                .await;
                settle_turns(32).await;
                shrink_gate.notify_one();
                settle_turns(32).await;
                let refreshes = crate::owned_h2::FLOW_FULL_REFRESHES.with(Cell::get);
                unblock_gate.notify_one();
                settle_turns(32).await;
                assert_eq!(
                    crate::owned_h2::FLOW_FULL_REFRESHES.with(Cell::get) - refreshes,
                    1,
                    "the check after the unblock refreshes the flow index in one full pass"
                );
                host.advance(WRITE * 2);
                settle_turns(32).await;
                assert!(
                    written_frames(&state, STREAMS).resets.is_empty(),
                    "no stream is blocked after the unblock"
                );
                terminal.notify_waiters();
                settle_turns(32).await;
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// A hundred streams that block one after another hold one wheel slot for the connection's
/// flow-control timeout: each arrival restamps it later, which is a store, not a relink, the
/// wheel re-slots it about once per timeout period, and its one fire resets every blocked stream.
#[test]
fn staggered_blocked_streams_hold_one_flow_slot() {
    const STREAMS: usize = 100;
    const SPACING: Duration = Duration::from_millis(250);
    crate::external_host_tests::run_on_wheel_host_with(|host, context| {
        Box::pin(async move {
            let (mut peer, mut preface) = ready_owner_requests(0);
            preface.extend(eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: i32::MAX as u32 - 65_535,
                },
            ));
            let gates: Vec<_> = (0..STREAMS)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect();
            let requests = gates.iter().map(|gate| {
                FixtureRead::WaitData(Arc::clone(gate), eligibility_request(&mut peer, 0))
            });
            let reads: Vec<_> = [FixtureRead::Data(preface)]
                .into_iter()
                .chain(requests)
                .collect();
            // One byte more than each stream's 65,535-byte window.
            let (session, handle, state, probes) =
                write_timed_session(context, reads, vec![Reply::Body(65_536); STREAMS], WRITE);
            let (run, ()) = futures::join!(session.run(), async {
                settle_turns(32).await;
                for (opened, gate) in gates.iter().enumerate() {
                    host.advance(SPACING);
                    gate.notify_one();
                    complete_credit_wait(|| {
                        written_frames(&state, STREAMS).body_bytes == 65_535 * (opened + 1)
                    })
                    .await;
                    settle_turns(8).await;
                }
                let stats = host.wheel_stats();
                assert_eq!(
                    stats.live_slots, 1,
                    "one slot for the connection's flow timeout"
                );
                assert_eq!(stats.fired, 0);
                assert_eq!(stats.relinked, 1, "restamps are stores, not relinks");
                let periods =
                    (SPACING.as_millis() * STREAMS as u128).div_ceil(WRITE.as_millis()) as u64;
                assert!(
                    stats.reslotted <= 2 * periods,
                    "{} re-slots over {periods} timeout periods",
                    stats.reslotted
                );
                assert!(written_frames(&state, STREAMS).resets.is_empty());
                host.advance(WRITE);
                complete_credit_wait(|| written_frames(&state, STREAMS).resets.len() == STREAMS)
                    .await;
                assert_eq!(host.wheel_stats().fired, 1, "one fire resets every stream");
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
            let resets = written_frames(&state, STREAMS).resets;
            assert!(
                resets
                    .iter()
                    .all(|(_, code)| *code == H2ErrorCode::FLOW_CONTROL_ERROR)
            );
            assert_eq!(
                resets
                    .iter()
                    .map(|(stream, _)| stream.get())
                    .collect::<Vec<_>>(),
                (0..STREAMS as u64)
                    .map(|index| 2 * index + 1)
                    .collect::<Vec<_>>()
            );
            assert!(
                probes
                    .borrow()
                    .iter()
                    .all(|probe| probe.cancelled.borrow().as_slice()
                        == [(HttpErrorCode::FLOW_CONTROL_ERROR, false)])
            );
        })
    });
}

/// A flow-blocked stream that gets credit and retires at its END_STREAM serialization, before its
/// write completes (`DIV-028`), leaves nothing for the flow-control timeout: the slot is disarmed
/// and never fires.
#[test]
fn flow_slot_disarms_when_the_blocked_stream_retires_early() {
    crate::external_host_tests::run_on_wheel_host_with(|host, context| {
        Box::pin(async move {
            let (mut peer, input) = flow_blocked_get(false);
            let gate = Arc::new(tokio::sync::Notify::new());
            let credit = eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::new(1),
                    amount: 1,
                },
            );
            // The peer's EOF waits for the drain, since a read past the script needs Tokio.
            let end = Arc::new(tokio::sync::Notify::new());
            let transport = QueuedFixtureTransport::reporting(
                [
                    FixtureRead::Data(input),
                    FixtureRead::WaitData(Arc::clone(&gate), credit),
                    FixtureRead::WaitDataEof(Arc::clone(&end), Vec::new()),
                ],
                Some(Rc::new(Cell::new(false))),
                false,
            );
            let queue = Rc::clone(&transport.probe);
            // One byte more than the stream's 65,535-byte window.
            let (application, probes) = Application::new(vec![Reply::Body(65_536)]);
            let mut config = options(context, transport);
            config.io.write_timeout = WRITE;
            let (session, handle) = OwnedH2Session::new(application, config).expect("owned H2");
            assert_eq!(queue.borrow().write_timeout, WRITE);
            let (run, ()) = futures::join!(session.run(), async {
                // The queue holds two writes until they are released.
                while queued_body_bytes(&queue) != 65_535 {
                    release_all_queued_writes(&queue);
                    tokio::task::yield_now().await;
                }
                release_all_queued_writes(&queue);
                settle_turns(32).await;
                assert_eq!(
                    host.wheel_stats().armed,
                    1,
                    "the blocked stream arms the slot"
                );
                host.advance(WRITE / 2);
                gate.notify_one();
                let probe = Rc::clone(&probes.borrow()[0]);
                drive_until(DRIVE_LIMIT, || probe.completed.get() == 1).await;
                assert!(end_stream_accepted(&queue, 1));
                assert!(
                    queue.borrow().released < queue.borrow().accepted.len(),
                    "the stream retired before its END_STREAM was written"
                );
                assert_eq!(
                    host.wheel_stats().armed,
                    0,
                    "the retired stream disarmed the slot"
                );
                release_all_queued_writes(&queue);
                host.advance(WRITE);
                settle_turns(32).await;
                assert_eq!(host.wheel_stats().fired, 0);
                assert!(probe.cancelled.borrow().is_empty());
                assert!(
                    queued_transcript(&queue)
                        .frames
                        .iter()
                        .all(|frame| frame.frame_type != FrameType::RST_STREAM.as_u8())
                );
                assert!(handle.initiate_drain().unwrap());
                finish_queued_writes(&queue);
                end.notify_one();
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// The DATA payload bytes the queue has accepted.
fn queued_body_bytes(queue: &RefCell<QueuedWriteProbe>) -> usize {
    queued_transcript(queue)
        .frames
        .iter()
        .filter(|frame| frame.frame_type == FrameType::DATA.as_u8())
        .map(|frame| frame.length as usize)
        .sum()
}

/// The clock reads of a flow-blocked GET, a PING, the credit that unblocks the response and a
/// drain, with every timeout zero: 2, as on the parent commit, which had no flow-control timer.
const ZERO_CONFIG_BLOCKED_NOW_READS: usize = 2;

/// With a zero write timeout a flow-blocked response arms nothing: no slot, no host sleep, and no
/// clock read beyond the parent's, however long it stays blocked and however often the
/// connection polls meanwhile.
#[test]
fn zero_write_timeout_blocked_stream_takes_no_slot_sleep_or_clock_read() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let wheel = context.host().wheel_timer().is_some();
            let (mut peer, input) = flow_blocked_get(false);
            let gate = Arc::new(tokio::sync::Notify::new());
            let ping = eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(7) });
            let credit_gate = Arc::new(tokio::sync::Notify::new());
            let credit = eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::new(1),
                    amount: (FLOW_BODY - 65_535) as u32,
                },
            );
            let reads_before = host.now_reads();
            let (session, handle, state, probes) = write_timed_session(
                context,
                [
                    FixtureRead::Data(input),
                    FixtureRead::WaitData(Arc::clone(&gate), ping),
                    FixtureRead::WaitData(Arc::clone(&credit_gate), credit),
                ],
                vec![Reply::Body(FLOW_BODY)],
                Duration::ZERO,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| written_frames(&state, 1).body_bytes == 65_535).await;
                settle_turns(32).await;
                host.advance(10 * WRITE);
                gate.notify_one();
                complete_credit_wait(|| written_frames(&state, 1).ping_replies == [7]).await;
                settle_turns(32).await;
                host.advance(10 * WRITE);
                settle_turns(32).await;
                assert!(written_frames(&state, 1).resets.is_empty());
                credit_gate.notify_one();
                let probe = Rc::clone(&probes.borrow()[0]);
                complete_credit_wait(|| probe.completed.get() == 1).await;
                assert_eq!(written_frames(&state, 1).body_bytes, FLOW_BODY);
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
            assert_eq!(
                host.now_reads() - reads_before,
                ZERO_CONFIG_BLOCKED_NOW_READS
            );
            assert_eq!(host.sleeps(), 0, "a zero-config session made a host sleep");
            if wheel {
                let stats = host.wheel_stats();
                assert_eq!(stats.scheduled, 0);
                assert_eq!(stats.live_slots, 0);
            }
            assert!(probes.borrow()[0].cancelled.borrow().is_empty());
        })
    });
}

/// The stream read timeout of the stream-read-timeout tests.
const STREAM_READ: Duration = Duration::from_secs(1);

/// The cleanup a handler awaits after its request body read times out, before it answers.
const CLEANUP: Duration = Duration::from_millis(30);

/// How the request body that stalls is consumed.
#[derive(Clone, Copy, Debug)]
enum Stall {
    /// The application reads the request body before answering.
    Handler,
    /// The application holds the request body, which the test reads outside the session's poll,
    /// and never produces its response head.
    HeldHeadUnsent,
    /// The application holds the request body, which the test reads outside the session's poll,
    /// and answers a header-only 200 at once.
    HeldHeadSent,
    /// The application answers with its head at once and reads the request body before its
    /// one-byte response body.
    BodyAfterHead,
    /// No request body stalls: the application's response head fails with `READ_TIMEOUT`, as a
    /// response source whose head read times out does on `UniplexSession`.
    HeadError,
    /// As `Handler`, but the owned application answers the read timeout with its own 408 head,
    /// in the poll that observed it, as the server's handler adapter does.
    AnsweredHandler,
    /// The handler reads the request body; when a read times out it awaits this cleanup (a
    /// sleep, if nonzero), then answers this status with a 4-byte body.
    CleanupThenAnswer(u16, Duration),
}

/// Reads the request body as `FixtureHandler`; when a read times out, awaits `cleanup` (a sleep,
/// if nonzero) and answers `status` with the body `rrrr`.
struct CleanupThenAnswerHandler {
    status: u16,
    cleanup: Duration,
}

impl HttpHandler for CleanupThenAnswerHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let (status, cleanup) = (self.status, self.cleanup);
        Box::pin(async move {
            request.read_header_event().await?;
            loop {
                match request.read_body_event(u32::MAX).await {
                    Ok(event) if event.eom() => break,
                    Ok(event) => {
                        if let HttpBodyEventKind::Suspend(resume) = event.into_kind() {
                            let _ = resume.await;
                        }
                    }
                    Err(error) if error.code() == HttpErrorCode::READ_TIMEOUT => {
                        if !cleanup.is_zero() {
                            tokio::time::sleep(cleanup).await;
                        }
                        let response =
                            HttpFixedSource::make_fixed_response(status, Some(queue(b"rrrr")))
                                .expect("fixture response");
                        return Ok(HttpSourceHolder::new(Box::new(response)));
                    }
                    Err(error) => return Err(error),
                }
            }
            panic!("the request body stalls");
        })
    }
}

/// What a downstream driver wrote for one request whose body stalls.
#[derive(Debug)]
struct StreamReadWire {
    /// `(milliseconds since start, stream, status)` of each response head, sampled every
    /// millisecond of paused time.
    statuses: Vec<(u64, StreamId, u16)>,
    resets: Vec<(u64, StreamId, H2ErrorCode)>,
    goaways: Vec<(u64, StreamId, H2ErrorCode)>,
    /// Everything written, with any date normalized.
    written: (usize, u64),
    /// How the test's read of a held request body ended, and when.
    held_read: Option<(u64, HttpErrorCode, Vec<u8>)>,
    result: Result<(), HttpErrorCode>,
}

/// A response whose head never arrives, so a handler that returned it is inactive while the
/// response head is still unsent.
struct UnsentHead;

impl HttpSource for UnsentHead {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

/// Holds the request and answers with `head_sent`'s response: a header-only 200 with
/// `Content-Length: 0`, as the fixture application's, or `UnsentHead`.
struct HoldingHandler {
    held: Rc<RefCell<Option<HttpSourceHolder>>>,
    head_sent: bool,
}

impl HttpHandler for HoldingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let held = Rc::clone(&self.held);
        let head_sent = self.head_sent;
        Box::pin(async move {
            request.read_header_event().await?;
            held.replace(Some(request));
            let response: Box<dyn HttpSource> = if head_sent {
                Box::new(
                    HttpFixedSource::make_fixed_response(200, Some(queue(&[]))).expect("fixed 200"),
                )
            } else {
                Box::new(UnsentHead)
            };
            Ok(HttpSourceHolder::new(response))
        })
    }
}

/// Answers a 200 with `Content-Length: 1` at once and reads the request body, awaiting each
/// suspend's resume, before its one body byte.
struct BodyAfterHeadHandler;

struct BodyAfterHeadSource(Rc<RefCell<HttpSourceHolder>>);

impl HttpHandler for BodyAfterHeadHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            request.read_header_event().await?;
            Ok(HttpSourceHolder::new(Box::new(BodyAfterHeadSource(
                Rc::new(RefCell::new(request)),
            ))))
        })
    }
}

impl HttpSource for BodyAfterHeadSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let mut response = HttpMessage::new();
        response.set_status_code(200).unwrap();
        response.set_http_version(1, 1);
        response
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"1");
        Box::pin(std::future::ready(Ok(HttpHeaderEvent::new(
            response, false,
        )
        .unwrap())))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let request = Rc::clone(&self.0);
        Box::pin(async move {
            loop {
                let read = request.borrow_mut().read_body_event(u32::MAX);
                let event = read.await?;
                if event.eom() {
                    return Ok(HttpBodyEvent::body(queue(b"r"), true));
                }
                if let HttpBodyEventKind::Suspend(resume) = event.into_kind() {
                    let _ = resume.await;
                }
            }
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.0.borrow_mut().stop_reading(error);
    }
}

/// Reads a held request body until it fails, awaiting each suspend's resume, and returns the
/// failure and the millisecond it came.
async fn read_held_body(
    held: impl Fn() -> Option<HttpSourceHolder>,
    begun: tokio::time::Instant,
) -> (u64, HttpErrorCode, Vec<u8>) {
    let mut body = loop {
        if let Some(body) = held() {
            break body;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    loop {
        match body.read_body_event(u32::MAX).await {
            Ok(event) => {
                assert!(!event.eom(), "the stalled request body never ends");
                if let HttpBodyEventKind::Suspend(resume) = event.into_kind() {
                    let _ = resume.await;
                }
            }
            Err(error) => {
                return (
                    begun.elapsed().as_millis() as u64,
                    error.code(),
                    error.message().to_vec(),
                );
            }
        }
    }
}

/// Serves `input` on the owned driver (the fixture application) or on `UniplexSession` (a
/// handler that does the same), both with `STREAM_READ` as the stream read timeout and every
/// other timeout zero; `stall` says how the request body is consumed. The application drains
/// the connection at `drain_at` if it is still open.
async fn stream_read_wire(
    context: WorkerContext,
    owned: bool,
    stall: Stall,
    input: Vec<u8>,
    drain_at: Duration,
) -> StreamReadWire {
    let (inner, state) = FixtureTransport::new([]);
    let transport = Rc::new(GatedReads {
        inner,
        script: Rc::new(RefCell::new(VecDeque::from([(None, input)]))),
    });
    let mut config = options(context, transport);
    config.io.stream_read_timeout = STREAM_READ;
    let begun = tokio::time::Instant::now();
    let done = Rc::new(Cell::new(false));
    let held_by_test = matches!(stall, Stall::HeldHeadUnsent | Stall::HeldHeadSent);
    type Run = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>>>>;
    type Held = Box<dyn Fn() -> Option<HttpSourceHolder>>;
    let (session, drain, held): (Run, Box<dyn Fn()>, Held) = if owned {
        let reply = match stall {
            Stall::BodyAfterHead => Reply::Body(1),
            Stall::CleanupThenAnswer(..) => Reply::Body(4),
            _ => Reply::Body(0),
        };
        let (mut application, probes) = Application::new(vec![reply]);
        application.await_request_resume = true;
        application.retain_request = held_by_test;
        application.hold_headers = matches!(stall, Stall::HeldHeadUnsent);
        application.upload_after_head = matches!(stall, Stall::BodyAfterHead);
        if matches!(stall, Stall::HeadError) {
            application.response_head_error = Some(HttpErrorCode::READ_TIMEOUT);
        }
        application.answer_read_timeout = matches!(stall, Stall::AnsweredHandler);
        if let Stall::CleanupThenAnswer(status, cleanup) = stall {
            application.late_answer = Some((status, cleanup));
        }
        let (session, handle) =
            OwnedH2Session::new(application, config).expect("stream-read-timed owned H2");
        (
            Box::pin(session.run()),
            Box::new(move || assert!(handle.initiate_drain().unwrap())),
            Box::new(move || {
                let probe = probes.borrow().first().cloned()?;
                probe.held_body.borrow_mut().take()
            }),
        )
    } else {
        let held = Rc::new(RefCell::new(None));
        let handler: Rc<dyn HttpHandler> = match stall {
            Stall::Handler | Stall::AnsweredHandler => Rc::new(FixtureHandler {
                status: 200,
                response_body: 0,
                response_error: None,
                invocations: Rc::new(Cell::new(0)),
                request_body: Rc::new(Cell::new(0)),
            }),
            Stall::HeldHeadUnsent | Stall::HeldHeadSent => Rc::new(HoldingHandler {
                held: Rc::clone(&held),
                head_sent: matches!(stall, Stall::HeldHeadSent),
            }),
            Stall::BodyAfterHead => Rc::new(BodyAfterHeadHandler),
            Stall::CleanupThenAnswer(status, cleanup) => {
                Rc::new(CleanupThenAnswerHandler { status, cleanup })
            }
            Stall::HeadError => Rc::new(FixtureHandler {
                status: 200,
                response_body: 0,
                response_error: Some(HttpErrorCode::READ_TIMEOUT),
                invocations: Rc::new(Cell::new(0)),
                request_body: Rc::new(Cell::new(0)),
            }),
        };
        let OwnedH2Options {
            context,
            transport,
            io,
            http2,
        } = config;
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Downstream,
            transport,
            Some(handler),
            io,
            http2,
        )
        .expect("Uniplex H2");
        (
            Box::pin(session.run()),
            Box::new(move || assert!(handle.initiate_drain().unwrap())),
            Box::new(move || held.borrow_mut().take()),
        )
    };
    let monitor = async {
        let (mut statuses, mut resets, mut goaways) = (Vec::new(), Vec::new(), Vec::new());
        let mut drained = false;
        loop {
            let at = begun.elapsed();
            if !drained && !done.get() && at >= drain_at {
                drained = true;
                drain();
            }
            let ms = at.as_millis() as u64;
            let frames = written_frames(&state, 1);
            statuses.extend(
                frames.header_statuses[statuses.len()..]
                    .iter()
                    .map(|(stream, status)| (ms, *stream, *status)),
            );
            resets.extend(
                frames.resets[resets.len()..]
                    .iter()
                    .map(|(stream, code)| (ms, *stream, *code)),
            );
            goaways.extend(
                frames.goaways[goaways.len()..]
                    .iter()
                    .map(|(stream, code)| (ms, *stream, *code)),
            );
            if done.get() {
                return (statuses, resets, goaways);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let run = async {
        let result = session.await;
        done.set(true);
        result
    };
    let reader = async {
        if held_by_test {
            Some(read_held_body(held, begun).await)
        } else {
            None
        }
    };
    let (result, (statuses, resets, goaways), held_read) =
        tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(run, monitor, reader)
        })
        .await
        .expect("the stalled request's session ends");
    let written = dateless_digest(&state.borrow().writes.concat());
    StreamReadWire {
        statuses,
        resets,
        goaways,
        written,
        held_read,
        result: result.map(|_| ()).map_err(|error| error.code()),
    }
}

/// Runs `stall` on `UniplexSession`, then on the owned driver, on one paused clock, over a POST
/// whose 5 body bytes arrive without END_STREAM (or a GET, for `Stall::HeadError`).
fn stream_read_wires(stall: Stall) -> (StreamReadWire, StreamReadWire) {
    let input = move || match stall {
        Stall::HeadError => ready_owner_requests(1).1,
        _ => peer_incomplete_post_bytes(5),
    };
    run_on_paused_worker(move |context| {
        Box::pin(async move {
            let drain_at = 3 * STREAM_READ;
            let uniplex = stream_read_wire(context.clone(), false, stall, input(), drain_at).await;
            let owned = stream_read_wire(context, true, stall, input(), drain_at).await;
            (owned, uniplex)
        })
    })
}

/// Asserts the reader's read timed out within a millisecond of `at`.
fn assert_held_read_timed_out(wire: &StreamReadWire, at: Duration) {
    let Some((read_at, code, message)) = &wire.held_read else {
        panic!("the held body was not read: {wire:?}");
    };
    assert_eq!(*code, HttpErrorCode::READ_TIMEOUT, "{wire:?}");
    assert_eq!(message.as_slice(), b"Read timeout after 1000", "{wire:?}");
    assert!(read_at.abs_diff(at.as_millis() as u64) <= 1, "{wire:?}");
}

/// Asserts the same response heads, resets and GOAWAYs at the same instants, the same bytes and
/// the same result.
fn assert_same_stream_read_wire(owned: &StreamReadWire, uniplex: &StreamReadWire) {
    let heads = |wire: &StreamReadWire| -> Vec<(StreamId, u16)> {
        wire.statuses
            .iter()
            .map(|(_, stream, status)| (*stream, *status))
            .collect()
    };
    assert_eq!(
        heads(owned),
        heads(uniplex),
        "owned {owned:?}, uniplex {uniplex:?}"
    );
    for ((owned_at, ..), (uniplex_at, ..)) in owned.statuses.iter().zip(&uniplex.statuses) {
        assert!(
            owned_at.abs_diff(*uniplex_at) <= 1,
            "owned {owned:?}, uniplex {uniplex:?}"
        );
    }
    assert_same_timeline(&owned.resets, &uniplex.resets);
    assert_same_timeline(&owned.goaways, &uniplex.goaways);
    assert_eq!(
        owned.written, uniplex.written,
        "owned {owned:?}, uniplex {uniplex:?}"
    );
    assert_eq!(owned.result, uniplex.result);
}

/// A handler blocked on the request body is answered 408 at the stream read timeout, owned
/// against `UniplexSession` on the same paused clock: the same frames at the same instants and
/// the same bytes, then the same drain. Both handlers read the 5 buffered bytes, then await the
/// source's suspend event, whose wait times out first (the handler reads on), then the next read
/// times out: 408 at twice the stream read timeout.
#[test]
fn stalled_request_body_gets_408_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::Handler);
    let stream_read = STREAM_READ.as_millis() as u64;
    assert_eq!(
        uniplex
            .statuses
            .iter()
            .map(|(_, stream, status)| (*stream, *status))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), 408)],
        "uniplex {uniplex:?}"
    );
    assert!(
        uniplex.statuses[0].0.abs_diff(2 * stream_read) <= 1,
        "uniplex {uniplex:?}"
    );
    assert!(uniplex.resets.is_empty(), "uniplex {uniplex:?}");
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A held request body whose read times out outside the application's poll, while the response
/// head is unsent, is answered 408 as `UniplexSession` answers it when its handler has returned
/// a response that has not produced its head: the same frames and bytes, and the reader gets
/// the same read timeout.
#[test]
fn held_request_body_timeout_before_the_head_gets_408_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::HeldHeadUnsent);
    assert_held_read_timed_out(&uniplex, 2 * STREAM_READ);
    assert_held_read_timed_out(&owned, 2 * STREAM_READ);
    assert_eq!(
        uniplex
            .statuses
            .iter()
            .map(|(_, stream, status)| (*stream, *status))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), 408)],
        "uniplex {uniplex:?}"
    );
    assert!(uniplex.resets.is_empty(), "uniplex {uniplex:?}");
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A held request body whose read times out after the response went out resets the stream as
/// `UniplexSession` does.
#[test]
fn held_request_body_timeout_after_the_head_resets_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::HeldHeadSent);
    assert_held_read_timed_out(&uniplex, 2 * STREAM_READ);
    assert_held_read_timed_out(&owned, 2 * STREAM_READ);
    assert_eq!(
        uniplex
            .resets
            .iter()
            .map(|(_, stream, code)| (*stream, *code))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), H2ErrorCode::CANCEL)],
        "uniplex {uniplex:?}"
    );
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A response body that waits on a request body whose read times out after the response head
/// went out resets the stream with CANCEL, as on `UniplexSession`: the request body's failure,
/// published while the response body was polled, is handled before the poll's result (Uniplex
/// runs the source's completion callback before the egress producer's failure).
#[test]
fn request_body_timeout_after_a_sent_head_resets_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::BodyAfterHead);
    assert_eq!(
        uniplex
            .resets
            .iter()
            .map(|(_, stream, code)| (*stream, *code))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), H2ErrorCode::CANCEL)],
        "uniplex {uniplex:?}"
    );
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A `READ_TIMEOUT` response head that no request body read caused resets the stream with
/// INTERNAL_ERROR, as `UniplexSession` resets a stream whose response source fails its head read
/// with it: only the request body's read timeout is answered 408.
#[test]
fn response_head_read_timeout_resets_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::HeadError);
    assert_eq!(
        uniplex
            .resets
            .iter()
            .map(|(_, stream, code)| (*stream, *code))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), H2ErrorCode::INTERNAL_ERROR)],
        "uniplex {uniplex:?}"
    );
    assert!(uniplex.statuses.is_empty(), "uniplex {uniplex:?}");
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// An application that answers the request body's read timeout with its own 408 head in the
/// poll that observed it, as the server's handler adapter does, gets the same wire as
/// `UniplexSession`: one 408 and no RST_STREAM after it. The read timeout is handled before the
/// polled head, as the session core handles the source's completion before the handler result.
#[test]
fn application_408_for_a_stalled_request_body_matches_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::AnsweredHandler);
    assert!(uniplex.resets.is_empty(), "uniplex {uniplex:?}");
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A running handler whose request body read times out keeps running: its own 408, answered
/// after a cleanup, goes out with its body, as `UniplexSession` waits for an active handler and
/// keeps a 408 it answers.
#[test]
fn running_handlers_own_408_after_a_read_timeout_is_kept_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::CleanupThenAnswer(408, CLEANUP));
    let answered = (2 * STREAM_READ + CLEANUP).as_millis() as u64;
    assert_eq!(
        uniplex
            .statuses
            .iter()
            .map(|(_, stream, status)| (*stream, *status))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), 408)],
        "uniplex {uniplex:?}"
    );
    assert!(
        uniplex.statuses[0].0.abs_diff(answered) <= 1,
        "the handler's 408 follows its cleanup: uniplex {uniplex:?}"
    );
    assert!(uniplex.resets.is_empty(), "uniplex {uniplex:?}");
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A running handler that answers its request body's read timeout with its own 408 in the poll
/// that observed it keeps that 408 and its body, as `UniplexSession` handles the source's
/// completion while the handler is still active, before the handler's result: the driver asks
/// whether the handler ran before that poll, not after it.
#[test]
fn running_handlers_own_408_in_the_timed_out_poll_is_kept_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::CleanupThenAnswer(408, Duration::ZERO));
    assert_eq!(
        uniplex
            .statuses
            .iter()
            .map(|(_, stream, status)| (*stream, *status))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), 408)],
        "uniplex {uniplex:?}"
    );
    assert!(uniplex.resets.is_empty(), "uniplex {uniplex:?}");
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A running handler whose request body read times out and then answers anything but 408 has
/// that answer replaced with a bare 408 when it answers, as `UniplexSession` replaces an active
/// handler's response that is not the expected 408.
#[test]
fn running_handlers_non_408_after_a_read_timeout_becomes_408_like_uniplex() {
    let (owned, uniplex) = stream_read_wires(Stall::CleanupThenAnswer(200, CLEANUP));
    let answered = (2 * STREAM_READ + CLEANUP).as_millis() as u64;
    assert_eq!(
        uniplex
            .statuses
            .iter()
            .map(|(_, stream, status)| (*stream, *status))
            .collect::<Vec<_>>(),
        [(StreamId::new(1), 408)],
        "uniplex {uniplex:?}"
    );
    assert!(
        uniplex.statuses[0].0.abs_diff(answered) <= 1,
        "the replacement waits for the handler: uniplex {uniplex:?}"
    );
    assert!(uniplex.resets.is_empty(), "uniplex {uniplex:?}");
    assert_same_stream_read_wire(&owned, &uniplex);
}

/// A downstream session over `reads`, answering with `replies`, with the given stream read
/// timeout and every other timeout zero.
fn stream_read_timed_session(
    context: WorkerContext,
    reads: impl IntoIterator<Item = FixtureRead>,
    replies: Vec<Reply>,
    stream_read_timeout: Duration,
) -> (
    OwnedH2Session<Application>,
    OwnedH2Handle,
    Rc<RefCell<FixtureTransportState>>,
    Rc<RefCell<Vec<Rc<Probe>>>>,
) {
    let (fixture, state) = FixtureTransport::new(reads);
    let (application, probes) = Application::new(replies);
    let mut config = options(context, Rc::new(UntimedReads(fixture)));
    config.io.stream_read_timeout = stream_read_timeout;
    let (session, handle) =
        OwnedH2Session::new(application, config).expect("stream-read-timed owned H2");
    (session, handle, state, probes)
}

/// An application blocked on a stalled request body is answered 408 at the stream read
/// timeout, not a millisecond earlier, and its exchange is cancelled with the read timeout.
#[test]
fn stalled_request_body_gets_408_at_its_stream_read_timeout() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (session, handle, state, probes) = stream_read_timed_session(
                context,
                [FixtureRead::Data(peer_incomplete_post_bytes(5))],
                vec![Reply::Body(0)],
                STREAM_READ,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| {
                    probes
                        .borrow()
                        .first()
                        .is_some_and(|probe| probe.request_bytes.get() == 5)
                })
                .await;
                settle_turns(32).await;
                host.advance(STREAM_READ - Duration::from_millis(1));
                settle_turns(32).await;
                assert!(
                    written_frames(&state, 1).header_statuses.is_empty(),
                    "the 408 came early"
                );
                host.advance(Duration::from_millis(1));
                let probe = Rc::clone(&probes.borrow()[0]);
                complete_credit_wait(|| !probe.cancelled.borrow().is_empty()).await;
                settle_turns(32).await;
                let frames = written_frames(&state, 1);
                assert_eq!(frames.header_statuses, [(StreamId::new(1), 408)]);
                assert!(frames.resets.is_empty());
                assert_eq!(
                    probe.cancelled.borrow().as_slice(),
                    &[(HttpErrorCode::READ_TIMEOUT, false)]
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
            assert_eq!(probes.borrow()[0].completed.get(), 0);
        })
    });
}

/// Each wait for request body is timed afresh, as Proxygen's per-read timeout: a body whose
/// chunks each arrive within the stream read timeout is never answered 408, however long it
/// takes in all. On a wheel host each armed wait is a store on the stream baton's one slot, with
/// no host sleep; without a wheel each armed wait takes one host sleep, as before. The stream read
/// timeout takes no connection slot.
#[test]
fn trickling_request_body_is_timed_per_wait() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let wheel = context.host().wheel_timer().is_some();
            let gates: Vec<_> = (0..3)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect();
            let chunks = [(0, b"a"), (0, b"b"), (END_STREAM, b"c")];
            let mut request = HttpMessage::new();
            request.set_method(HttpMethod::Post).unwrap();
            request.set_url(b"/", false).unwrap();
            request.set_http_version(1, 1);
            request
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            request
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"8");
            let head = peer_request_message_bytes(request, Some(queue(b"xxxxx")), false);
            let reads = std::iter::once(FixtureRead::Data(head)).chain(
                gates.iter().zip(chunks).map(|(gate, (flags, chunk))| {
                    FixtureRead::WaitData(
                        Arc::clone(gate),
                        raw_frame(FrameType::DATA, flags, 1, chunk),
                    )
                }),
            );
            let (session, handle, state, probes) =
                stream_read_timed_session(context, reads, vec![Reply::Body(0)], STREAM_READ);
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| {
                    probes
                        .borrow()
                        .first()
                        .is_some_and(|probe| probe.request_bytes.get() == 5)
                })
                .await;
                let probe = Rc::clone(&probes.borrow()[0]);
                for (index, gate) in gates.iter().enumerate() {
                    settle_turns(32).await;
                    host.advance(STREAM_READ * 3 / 4);
                    settle_turns(32).await;
                    assert!(
                        written_frames(&state, 1).header_statuses.is_empty(),
                        "a chunk that came within the timeout was answered"
                    );
                    gate.notify_one();
                    assert!(
                        drive_until(1024, || probe.request_bytes.get() == 6 + index).await,
                        "chunk {index} was not read: {} bytes, {:?}",
                        probe.request_bytes.get(),
                        written_frames(&state, 1).header_statuses
                    );
                }
                assert!(
                    drive_until(1024, || probe.completed.get() == 1).await,
                    "the exchange did not complete: {:?}",
                    written_frames(&state, 1).header_statuses
                );
                assert_eq!(
                    written_frames(&state, 1).header_statuses,
                    [(StreamId::new(1), 200)]
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
            assert!(probes.borrow()[0].cancelled.borrow().is_empty());
            if wheel {
                let stats = host.wheel_stats();
                assert_eq!(
                    (
                        host.sleeps(),
                        stats.scheduled,
                        stats.relinked,
                        stats.live_slots
                    ),
                    (0, gates.len() as u64, 1, 0),
                    "(host sleeps, schedules, links, slots left): each armed wait for request body is a \
                     store on one slot"
                );
            } else {
                assert_eq!(
                    host.sleeps(),
                    gates.len(),
                    "one host sleep per armed wait for request body"
                );
            }
        })
    });
}

/// The clock reads of a POST whose body waits 20 s for its END_STREAM, then a drain, with every
/// timeout zero, measured on the parent commit, which did not honor stream read timeouts.
const ZERO_CONFIG_WAITING_BODY_NOW_READS: usize = 2;

/// With a zero stream read timeout a request body that the application waits on arms nothing:
/// no slot, no host sleep, and no clock read beyond the parent's, however long it waits.
#[test]
fn zero_stream_read_timeout_takes_no_slot_sleep_or_clock_read() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let wheel = context.host().wheel_timer().is_some();
            let gate = Arc::new(tokio::sync::Notify::new());
            let reads_before = host.now_reads();
            let (session, handle, state, probes) = stream_read_timed_session(
                context,
                [
                    FixtureRead::Data(peer_incomplete_post_bytes(5)),
                    FixtureRead::WaitData(
                        Arc::clone(&gate),
                        raw_frame(FrameType::DATA, END_STREAM, 1, &[]),
                    ),
                ],
                vec![Reply::Body(0)],
                Duration::ZERO,
            );
            let (run, ()) = futures::join!(session.run(), async {
                complete_credit_wait(|| {
                    probes
                        .borrow()
                        .first()
                        .is_some_and(|probe| probe.request_bytes.get() == 5)
                })
                .await;
                settle_turns(32).await;
                host.advance(20 * STREAM_READ);
                settle_turns(32).await;
                assert!(written_frames(&state, 1).header_statuses.is_empty());
                gate.notify_one();
                let probe = Rc::clone(&probes.borrow()[0]);
                complete_credit_wait(|| probe.completed.get() == 1).await;
                assert_eq!(
                    written_frames(&state, 1).header_statuses,
                    [(StreamId::new(1), 200)]
                );
                assert!(handle.initiate_drain().unwrap());
            });
            run.expect("the drained connection closes cleanly");
            assert_eq!(
                host.now_reads() - reads_before,
                ZERO_CONFIG_WAITING_BODY_NOW_READS
            );
            assert_eq!(host.sleeps(), 0, "a zero-config session made a host sleep");
            if wheel {
                let stats = host.wheel_stats();
                assert_eq!(stats.scheduled, 0);
                assert_eq!(stats.live_slots, 0);
            }
            assert!(probes.borrow()[0].cancelled.borrow().is_empty());
        })
    });
}
