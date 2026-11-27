/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The HTTP/3 session driven through the in-memory transport: a test client writes the peer's
//! streams and parses what the session sends, and a scripted application records the session's
//! calls into it.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
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
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;
use mvfstr_driver::ConnInfo;
use mvfstr_driver::QuicApplication;
use mvfstr_driver::QuicSession;
use mvfstr_driver::SinkError;
use mvfstr_proto::BufQueue;
use mvfstr_proto::ConnEvent;
use mvfstr_proto::ConnOptions;
use mvfstr_proto::Dir;
use mvfstr_proto::Side;
use mvfstr_proto::StreamId;
use mvfstr_proto::StreamReady;
use mvfstr_proto::Version;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeFuture;
use roxygen_core::events::ResumeStatus;
use roxygen_core::headers::HttpCommonHeaderTable;
use roxygen_core::headers::HttpCommonHeaders;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::ids::StreamId as HttpStreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::stream_source::HttpStreamBodyRead;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_qpack::FieldLine;
use roxygen_qpack::StringLiteral;
use roxygen_qpack::instructions::EncoderInstruction;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;

use crate::H3ApplicationBuilder;
use crate::H3Session;
use crate::allocations::count_allocations;
use crate::codec::Frame;
use crate::codec::FrameDecoder;
use crate::codec::FrameLimits;
use crate::codec::FrameType;
use crate::codec::H3ErrorCode;
use crate::codec::Role;
use crate::codec::SettingId;
use crate::codec::Settings;
use crate::codec::StreamKind;
use crate::codec::StreamType;
use crate::codec::UniStreamHeader;
use crate::codec::UniStreamHeaderDecoder;
use crate::codec::encode_frame_header;
use crate::fake::FakeConn;
use crate::fake::FakeSink;
use crate::session::Census;
use crate::transport::PullSink;
use crate::transport::StreamSink;

// ---------------------------------------------------------------------------------------------
// The scripted application.

/// What the application did, in order.
#[derive(Clone, Debug, PartialEq)]
enum Ev {
    New(u64),
    Request {
        id: u64,
        eom: bool,
    },
    Cancel {
        id: u64,
        code: HttpErrorCode,
        peer: bool,
    },
    ResponseComplete(u64),
    Complete(u64),
}

/// A flag a test opens to let the application continue.
#[derive(Clone, Default)]
struct Gate(Rc<GateInner>);

#[derive(Default)]
struct GateInner {
    open: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl Gate {
    fn open(&self) {
        self.0.open.set(true);
        if let Some(waker) = self.0.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.open.get() {
            Poll::Ready(())
        } else {
            *self.0.waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn resume(&self) -> ResumeFuture {
        let gate = self.clone();
        Box::pin(std::future::poll_fn(move |cx| {
            gate.poll(cx).map(|()| ResumeStatus::Signalled)
        }))
    }
}

/// The response an exchange produces.
#[derive(Clone)]
struct Plan {
    informational: bool,
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: Bytes,
    trailers: Option<Vec<(&'static str, &'static str)>>,
    /// A gate the head waits for.
    head_gate: Option<Gate>,
    /// A gate the first body poll suspends on.
    suspend: Option<Gate>,
    /// Whether the head fails instead.
    fail: bool,
    /// The code the body fails with instead.
    body_failure: Option<HttpErrorCode>,
    /// Empty body events produced before the body.
    empty_events: usize,
    /// Whether the exchange reports that it holds its whole response until its head is polled.
    fixed: bool,
    /// A response head whose clone is sent in place of one built from `status` and `headers`.
    head: Option<HttpMessage>,
    /// An informational head whose clone is sent in place of the built 103.
    interim: Option<HttpMessage>,
    /// Whether each head event records the size the session reports for it in `sizes`.
    record_sizes: bool,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            informational: false,
            status: 200,
            headers: vec![("content-type", "text/plain")],
            body: Bytes::from_static(b"hello"),
            trailers: None,
            head_gate: None,
            suspend: None,
            fail: false,
            body_failure: None,
            empty_events: 0,
            fixed: false,
            head: None,
            interim: None,
            record_sizes: false,
        }
    }
}

/// A request as the application received it.
struct Received {
    head: Box<HttpMessage>,
    body: Option<HttpStreamSourceOwner>,
    read: Option<HttpStreamBodyRead>,
    /// The ingress completion of an exchange that took it.
    completion: Option<IngressCompletion>,
}

#[derive(Default)]
struct AppState {
    log: Vec<Ev>,
    plan: Plan,
    requests: HashMap<u64, Received>,
    /// The ingress completions of cancelled exchanges that took them.
    cancelled: HashMap<u64, IngressCompletion>,
    /// The ingress completions that a `KeepApp` keeps beyond their exchanges.
    kept: HashMap<u64, IngressCompletion>,
    /// The `maximum` of each `poll_body` call.
    maxima: Vec<u32>,
    /// The encoded size reported for each head event that records it, with its exchange's ID.
    sizes: Vec<(u64, HttpHeaderSize)>,
}

/// The scripted application. It creates its exchanges through the default `new_exchange_with`,
/// which takes the ingress completion.
#[derive(Clone, Default)]
struct TestApp(Rc<RefCell<AppState>>);

impl TestApp {
    fn exchange(
        &self,
        stream: HttpStreamId,
        completion: Option<IngressCompletion>,
    ) -> TestExchange {
        let plan = {
            let mut state = self.0.borrow_mut();
            state.log.push(Ev::New(stream.get()));
            state.plan.clone()
        };
        TestExchange {
            id: stream.get(),
            state: Rc::clone(&self.0),
            plan,
            completion,
            sent_informational: false,
            offset: 0,
            suspended: false,
            empty_events: 0,
            head_polled: false,
        }
    }
}

impl OwnedApplication for TestApp {
    type Exchange = TestExchange;

    fn new_exchange(&self, stream: HttpStreamId, ingress: IngressCompletion) -> TestExchange {
        self.exchange(stream, Some(ingress))
    }
}

/// The scripted application creating its exchanges without the ingress completion, as the
/// server's handler application does.
#[derive(Clone, Default)]
struct SkipApp(TestApp);

impl OwnedApplication for SkipApp {
    type Exchange = TestExchange;

    fn new_exchange(&self, stream: HttpStreamId, ingress: IngressCompletion) -> TestExchange {
        self.0.new_exchange(stream, ingress)
    }

    fn new_exchange_with(
        &self,
        stream: HttpStreamId,
        _ingress: impl FnOnce() -> IngressCompletion,
    ) -> TestExchange {
        self.0.exchange(stream, None)
    }
}

/// The scripted application keeping a handle to each ingress completion it takes beyond the
/// completion's exchange, as an accounting task would.
#[derive(Clone, Default)]
struct KeepApp(TestApp);

impl OwnedApplication for KeepApp {
    type Exchange = TestExchange;

    fn new_exchange(&self, stream: HttpStreamId, ingress: IngressCompletion) -> TestExchange {
        let app = &self.0;
        app.0
            .borrow_mut()
            .kept
            .insert(stream.get(), ingress.clone());
        app.new_exchange(stream, ingress)
    }
}

struct TestExchange {
    id: u64,
    state: Rc<RefCell<AppState>>,
    plan: Plan,
    completion: Option<IngressCompletion>,
    sent_informational: bool,
    offset: usize,
    suspended: bool,
    empty_events: usize,
    head_polled: bool,
}

impl TestExchange {
    fn fields(fields: &[(&str, &str)]) -> HttpHeaders {
        let mut headers = HttpHeaders::new();
        for (name, value) in fields {
            headers.add(*name, value);
        }
        headers
    }

    /// Returns the event of a response head, which records its encoded size if the plan says so.
    fn head_event(&self, message: HttpMessage, eom: bool) -> HttpHeaderEvent {
        let mut event = HttpHeaderEvent::new(message, eom).unwrap();
        if self.plan.record_sizes {
            let state = Rc::clone(&self.state);
            let id = self.id;
            event
                .set_egress_headers_callback(move |size| state.borrow_mut().sizes.push((id, size)));
        }
        event
    }
}

impl OwnedExchange for TestExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        let mut state = self.state.borrow_mut();
        state.log.push(Ev::Request { id: self.id, eom });
        state.requests.insert(
            self.id,
            Received {
                head,
                body: Some(body),
                read: None,
                completion: self.completion.take(),
            },
        );
    }

    fn poll_headers(&mut self, cx: &mut Context<'_>) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if let Some(gate) = &self.plan.head_gate {
            std::task::ready!(gate.poll(cx));
        }
        self.head_polled = true;
        if self.plan.fail {
            return Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                b"the handler failed",
            )));
        }
        if self.plan.informational && !self.sent_informational {
            self.sent_informational = true;
            let message = self.plan.interim.clone().unwrap_or_else(|| {
                let mut message = HttpMessage::new();
                message.set_status_code(103).unwrap();
                message.headers_mut().add("link", "</style.css>");
                message
            });
            return Poll::Ready(Ok(self.head_event(message, false)));
        }
        let message = self.plan.head.clone().unwrap_or_else(|| {
            let mut message = HttpMessage::new();
            message.set_status_code(self.plan.status).unwrap();
            for (name, value) in &self.plan.headers {
                message.headers_mut().add(*name, value);
            }
            message
        });
        let eom = self.plan.body.is_empty() && self.plan.trailers.is_none();
        Poll::Ready(Ok(self.head_event(message, eom)))
    }

    fn poll_body(
        &mut self,
        _cx: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.state.borrow_mut().maxima.push(maximum);
        if let Some(code) = self.plan.body_failure {
            return Poll::Ready(Err(HttpError::with_message(code, b"the body failed")));
        }
        if let Some(gate) = &self.plan.suspend
            && !self.suspended
        {
            self.suspended = true;
            return Poll::Ready(Ok(HttpBodyEvent::suspend(gate.resume())));
        }
        if self.empty_events < self.plan.empty_events {
            self.empty_events += 1;
            return Poll::Ready(Ok(HttpBodyEvent::body(BufQueue::new(0), false)));
        }
        let left = self.plan.body.len() - self.offset;
        if left > 0 {
            let take = left.min(maximum as usize);
            let chunk = self.plan.body.slice(self.offset..self.offset + take);
            self.offset += take;
            let eom = self.offset == self.plan.body.len() && self.plan.trailers.is_none();
            let body = BufQueue::from_bytes(chunk, take).unwrap();
            return Poll::Ready(Ok(HttpBodyEvent::body(body, eom)));
        }
        let trailers = self.plan.trailers.as_ref().expect("the body ended");
        Poll::Ready(Ok(HttpBodyEvent::trailers(Self::fields(trailers))))
    }

    fn fixed_response_len(&self) -> Option<usize> {
        (self.plan.fixed && !self.head_polled).then_some(self.plan.body.len())
    }

    fn cancel(&mut self, error: &HttpError, peer_reset: bool) {
        let mut state = self.state.borrow_mut();
        state.log.push(Ev::Cancel {
            id: self.id,
            code: error.code(),
            peer: peer_reset,
        });
        if let Some(completion) = self.completion.take() {
            state.cancelled.insert(self.id, completion);
        }
    }

    fn response_complete(&mut self) {
        self.state
            .borrow_mut()
            .log
            .push(Ev::ResponseComplete(self.id));
    }

    fn complete(self) {
        self.state.borrow_mut().log.push(Ev::Complete(self.id));
    }
}

// ---------------------------------------------------------------------------------------------
// The test client.

fn client_bidi(index: u64) -> StreamId {
    StreamId::new(Side::Client, Dir::Bi, index).unwrap()
}

fn client_uni(index: u64) -> StreamId {
    StreamId::new(Side::Client, Dir::Uni, index).unwrap()
}

fn server_uni(index: u64) -> StreamId {
    StreamId::new(Side::Server, Dir::Uni, index).unwrap()
}

/// Returns the Roxygen stream ID of a request stream.
fn http_id(id: StreamId) -> u64 {
    id.into_inner() + 1
}

fn field_section(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut block = BytesMut::new();
    roxygen_qpack::Encoder::new().encode_field_section(
        fields
            .iter()
            .map(|(name, value)| FieldLine::new(name.as_bytes(), value.as_bytes())),
        &mut block,
    );
    block.to_vec()
}

fn headers_frame(fields: &[(&str, &str)]) -> Vec<u8> {
    frame(FrameType::HEADERS, &field_section(fields))
}

fn data_frame(payload: &[u8]) -> Vec<u8> {
    frame(FrameType::DATA, payload)
}

fn frame(frame_type: FrameType, payload: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    encode_frame_header(&mut out, frame_type, payload.len() as u64).unwrap();
    out.extend_from_slice(payload);
    out.to_vec()
}

const GET: &[(&str, &str)] = &[
    (":method", "GET"),
    (":scheme", "https"),
    (":authority", "example.com"),
    (":path", "/index.html"),
];

const POST: &[(&str, &str)] = &[
    (":method", "POST"),
    (":scheme", "https"),
    (":authority", "example.com"),
    (":path", "/upload"),
];

fn control_stream(settings: &Settings) -> Vec<u8> {
    let mut out = BytesMut::new();
    UniStreamHeader::Control.encode(&mut out).unwrap();
    Frame::Settings(settings.clone()).encode(&mut out).unwrap();
    out.to_vec()
}

/// A part of a response as the client decodes it.
#[derive(Clone, Debug, PartialEq)]
enum Part {
    Headers(Vec<(String, String)>),
    Data(Vec<u8>),
}

fn parse_request_stream(bytes: &[u8]) -> Vec<Part> {
    let mut decoder = FrameDecoder::new(StreamKind::Request, Role::Client, FrameLimits::default());
    let mut input = Bytes::copy_from_slice(bytes);
    let mut parts = Vec::new();
    while let Some(frame) = decoder.decode(&mut input).unwrap() {
        match frame {
            Frame::Data(data) => parts.push(Part::Data(data.to_vec())),
            Frame::Headers(block) => {
                let mut fields = Vec::new();
                roxygen_qpack::Decoder::new()
                    .decode_field_section(&block, |line| {
                        fields.push((
                            String::from_utf8(line.name.to_vec()).unwrap(),
                            String::from_utf8(line.value.to_vec()).unwrap(),
                        ));
                    })
                    .unwrap();
                parts.push(Part::Headers(fields));
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }
    assert!(input.is_empty());
    parts
}

fn body_of(parts: &[Part]) -> Vec<u8> {
    parts
        .iter()
        .filter_map(|part| match part {
            Part::Data(data) => Some(data.as_slice()),
            Part::Headers(_) => None,
        })
        .flatten()
        .copied()
        .collect()
}

fn field<'a>(fields: &'a [(String, String)], name: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, value)| value.as_str())
}

// ---------------------------------------------------------------------------------------------
// The harness: one session on a fake connection.

struct Harness<A: OwnedApplication = TestApp> {
    conn: FakeConn,
    session: H3Session<A>,
    /// The state of the scripted application, which `session` hosts directly or wrapped.
    app: TestApp,
    options: ConnOptions,
    /// Wakes of the waker `poll_session` is given.
    session_waker: Arc<CountWake>,
}

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Harness {
    fn new() -> Self {
        Self::with(H3ApplicationBuilder::new())
    }

    fn with(builder: H3ApplicationBuilder) -> Self {
        Self::with_plan(builder, Plan::default())
    }

    fn with_plan(builder: H3ApplicationBuilder, plan: Plan) -> Self {
        Self::hosting(builder, plan, |app| app)
    }

    /// Builds a harness whose peer has opened its control stream with SETTINGS.
    fn ready() -> Self {
        Self::ready_with(H3ApplicationBuilder::new(), Plan::default())
    }

    fn ready_with(builder: H3ApplicationBuilder, plan: Plan) -> Self {
        let mut harness = Self::with_plan(builder, plan);
        harness.make_ready();
        harness
    }
}

impl Harness<SkipApp> {
    /// Builds a ready harness whose application skips the ingress completion.
    fn skipping(plan: Plan) -> Self {
        let mut harness = Self::hosting(H3ApplicationBuilder::new(), plan, SkipApp);
        harness.make_ready();
        harness
    }
}

impl<A: OwnedApplication> Harness<A> {
    /// Builds a harness whose session hosts `wrap` of the scripted application.
    fn hosting(builder: H3ApplicationBuilder, plan: Plan, wrap: impl FnOnce(TestApp) -> A) -> Self
    where
        A: Clone,
    {
        let app = TestApp::default();
        app.0.borrow_mut().plan = plan;
        let made = wrap(app.clone());
        let mut application = builder
            .build(Host::tokio(), move |_| Ok(made.clone()))
            .unwrap();
        let info = ConnInfo {
            remote: "[::1]:4433".parse().unwrap(),
            local_ip: None,
            version: Version::V1,
        };
        let mut options = ConnOptions::default();
        let session = application.accept(&info, &mut options).unwrap();
        Self {
            conn: FakeConn::new(),
            session,
            app,
            options,
            session_waker: Arc::default(),
        }
    }

    /// Opens the peer's control stream with SETTINGS.
    fn make_ready(&mut self) {
        self.event(ConnEvent::Connected);
        self.receive(client_uni(0), &control_stream(&Settings::new()), false);
    }

    fn event(&mut self, event: ConnEvent) {
        self.session.core.on_event(&mut self.conn, event);
        self.session.core.check_exchanges();
    }

    fn ready_edge(&mut self, id: StreamId, ready: StreamReady) {
        self.session.core.on_stream(&mut self.conn, id, ready);
        self.session.core.check_exchanges();
    }

    /// Delivers peer bytes on `id`, opening it first if new, and the FIN if `fin`. As in the
    /// transport, the stream reports a FIN that came with the bytes before any of their edges.
    fn receive(&mut self, id: StreamId, bytes: &[u8], fin: bool) {
        let new = !self.conn.streams.contains_key(&id);
        self.conn.receive(id, bytes);
        self.conn.get(id).peer_finished |= fin;
        if new {
            self.ready_edge(id, StreamReady::Opened);
        }
        if !bytes.is_empty() {
            self.ready_edge(id, StreamReady::Readable);
        }
        if fin {
            self.ready_edge(id, StreamReady::Finished);
        }
    }

    fn poll_session(&mut self) {
        let waker = Waker::from(Arc::clone(&self.session_waker));
        let mut cx = Context::from_waker(&waker);
        let _ = self.session.core.poll_session(&mut cx, &mut self.conn);
        self.session.core.check_exchanges();
    }

    /// Counts the wakes of the waker `poll_session` was given.
    fn waker_wakes(&self) -> usize {
        self.session_waker.0.load(Ordering::SeqCst)
    }

    /// Counts the session wakes by either path.
    fn wakes(&self) -> usize {
        self.conn.session_wakes() + self.waker_wakes()
    }

    /// Calls `poll_send` once on `id` with `allowance`, and records what it pushed as sent.
    fn pull(&mut self, id: StreamId, allowance: usize) -> (Poll<()>, FakeSink) {
        let waker = self.conn.stream_waker_for(id);
        let mut cx = Context::from_waker(&waker);
        let mut sink = FakeSink::new(allowance);
        let poll = self.session.core.poll_send(&mut cx, id, &mut sink);
        self.session.core.check_exchanges();
        if let Some(stream) = self.conn.streams.get_mut(&id) {
            stream.sent.extend_from_slice(&sink.pushed);
            stream.finished |= sink.finished;
        }
        (poll, sink)
    }

    /// Calls `poll_send` until it returns `Pending`, and returns the number of `Ready` calls.
    fn pull_all(&mut self, id: StreamId, allowance: usize) -> usize {
        let mut ready = 0;
        while self.pull(id, allowance).0.is_ready() {
            ready += 1;
            assert!(ready < 10_000, "poll_send never returned Pending");
        }
        ready
    }

    fn response(&mut self, id: StreamId) -> Vec<Part> {
        parse_request_stream(&self.conn.get(id).sent)
    }

    fn log(&self) -> Vec<Ev> {
        self.app.0.borrow().log.clone()
    }

    /// Parses the server's control stream into its header and frames.
    fn control(&mut self) -> (UniStreamHeader, Vec<Frame>) {
        let sent = Bytes::copy_from_slice(&self.conn.get(server_uni(0)).sent);
        let mut input = sent;
        let header = UniStreamHeaderDecoder::new(Role::Client)
            .decode(&mut input)
            .unwrap()
            .expect("the stream header is complete");
        let mut decoder =
            FrameDecoder::new(StreamKind::Control, Role::Client, FrameLimits::default());
        let mut frames = Vec::new();
        while let Some(frame) = decoder.decode(&mut input).unwrap() {
            frames.push(frame);
        }
        (header, frames)
    }

    fn closed(&self) -> Option<u64> {
        self.conn.closed
    }

    /// Polls the application's request body once for at most `maximum` bytes.
    fn read_body(&mut self, id: StreamId, maximum: u32) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let mut state = self.app.0.borrow_mut();
        let received = state
            .requests
            .get_mut(&http_id(id))
            .expect("the request was delivered");
        let mut read = match received.read.take() {
            Some(read) => read,
            None => received
                .body
                .as_ref()
                .and_then(HttpStreamSourceOwner::source)
                .expect("the body has a source")
                .read_body_event_unboxed(maximum, CancellationToken::never()),
        };
        let mut cx = Context::from_waker(Waker::noop());
        let poll = Pin::new(&mut read).poll(&mut cx);
        if poll.is_pending() {
            received.read = Some(read);
        }
        poll
    }
}

// ---------------------------------------------------------------------------------------------
// Connection streams.

#[test]
fn settings_exchange() {
    let mut h = Harness::with(H3ApplicationBuilder::new().max_field_section_size(16 << 10));
    let transport = h
        .options
        .transport
        .clone()
        .expect("accept sets the transport");
    assert!(transport.max_streams_bidi() >= 100);
    assert!(transport.max_streams_uni() >= 3);
    assert_eq!(transport.max_stream_data_bidi_remote(), 64 << 10);

    h.event(ConnEvent::Connected);
    let (header, frames) = h.control();
    assert_eq!(header, UniStreamHeader::Control);
    let Some(Frame::Settings(settings)) = frames.first() else {
        panic!("the control stream starts with SETTINGS: {frames:?}");
    };
    assert_eq!(
        settings.get(SettingId::QPACK_MAX_TABLE_CAPACITY),
        Some(4096)
    );
    assert_eq!(settings.get(SettingId::QPACK_BLOCKED_STREAMS), Some(0));
    assert_eq!(
        settings.get(SettingId::MAX_FIELD_SECTION_SIZE),
        Some(16 << 10)
    );
    assert_eq!(settings.iter().filter(|(id, _)| id.is_grease()).count(), 1);
    assert_eq!(settings.len(), 4);
    let order = h.conn.get(server_uni(0)).order.unwrap();
    assert_eq!((order.class, order.urgency), (0, 0));

    // The peer's control stream and QPACK streams are accepted, and capacity 0 is valid.
    let mut peer = Settings::new();
    peer.set(SettingId::MAX_FIELD_SECTION_SIZE, 1000).unwrap();
    h.receive(client_uni(0), &control_stream(&peer), false);
    h.receive(
        client_uni(1),
        &[StreamType::QPACK_ENCODER.as_u64() as u8, 0x20],
        false,
    );
    h.receive(
        client_uni(2),
        &[StreamType::QPACK_DECODER.as_u64() as u8],
        false,
    );
    assert_eq!(h.closed(), None);
    for index in 0..3 {
        assert!(h.conn.get(client_uni(index)).readable.is_empty());
    }
    // MAX_PUSH_ID is accepted and ignored.
    let mut max_push = BytesMut::new();
    Frame::MaxPushId(10).encode(&mut max_push).unwrap();
    h.receive(client_uni(0), &max_push, false);
    assert_eq!(h.closed(), None);
}

/// Returns the bytes of the server's control stream and its SETTINGS, for a session built with
/// `builder`.
fn sent_settings(builder: H3ApplicationBuilder) -> (Vec<u8>, Settings) {
    let mut h = Harness::with(builder);
    h.event(ConnEvent::Connected);
    let (_, frames) = h.control();
    let Some(Frame::Settings(settings)) = frames.first() else {
        panic!("the control stream starts with SETTINGS: {frames:?}");
    };
    (h.conn.get(server_uni(0)).sent.clone(), settings.clone())
}

#[test]
fn settings_advertise_a_4096_byte_table_and_no_blocked_streams() {
    for (builder, capacity) in [
        (H3ApplicationBuilder::new(), 4096),
        (
            H3ApplicationBuilder::new().qpack_max_table_capacity(65_536),
            65_536,
        ),
    ] {
        let (_, settings) = sent_settings(builder);
        assert_eq!(
            settings.get(SettingId::QPACK_MAX_TABLE_CAPACITY),
            Some(capacity)
        );
        assert_eq!(settings.get(SettingId::QPACK_BLOCKED_STREAMS), Some(0));
    }

    // At 0, the control stream is the one sent before the table: the two QPACK settings at 0,
    // the field section size and one reserved setting.
    let (sent, settings) = sent_settings(H3ApplicationBuilder::new().qpack_max_table_capacity(0));
    let (grease, value) = settings
        .iter()
        .find(|(id, _)| id.is_grease())
        .expect("a reserved setting");
    let mut expected_settings = Settings::new();
    for (id, value) in [
        (SettingId::QPACK_MAX_TABLE_CAPACITY, 0),
        (SettingId::QPACK_BLOCKED_STREAMS, 0),
        (SettingId::MAX_FIELD_SECTION_SIZE, 64 << 10),
        (grease, value),
    ] {
        expected_settings.set(id, value).unwrap();
    }
    assert_eq!(sent, control_stream(&expected_settings));

    let built = |bytes| {
        H3ApplicationBuilder::new()
            .qpack_max_table_capacity(bytes)
            .build(Host::tokio(), |_| Ok(TestApp::default()))
            .err()
    };
    assert_eq!(built(65_536), None);
    assert_eq!(
        built(65_537),
        Some(crate::H3ConfigError::OutOfRange {
            field: "qpack_max_table_capacity"
        })
    );
}

#[test]
fn critical_stream_errors() {
    fn encoded(frame: Frame) -> Vec<u8> {
        let mut out = BytesMut::new();
        frame.encode(&mut out).unwrap();
        out.to_vec()
    }
    let settings = control_stream(&Settings::new());
    let encoder = [StreamType::QPACK_ENCODER.as_u64() as u8];
    let decoder = [StreamType::QPACK_DECODER.as_u64() as u8];
    let control_then = {
        let settings = settings.clone();
        move |frame: Frame| {
            let mut bytes = settings.clone();
            bytes.extend_from_slice(&encoded(frame));
            bytes
        }
    };
    type Step = Box<dyn Fn(&mut Harness)>;
    let cases: Vec<(&str, Step, u64)> = vec![
        (
            "control stream FIN",
            Box::new({
                let settings = settings.clone();
                move |h| h.receive(client_uni(0), &settings, true)
            }),
            H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64(),
        ),
        (
            "control stream reset",
            Box::new({
                let settings = settings.clone();
                move |h| {
                    h.receive(client_uni(0), &settings, false);
                    h.ready_edge(
                        client_uni(0),
                        StreamReady::Reset {
                            code: app(H3ErrorCode::NO_ERROR.as_u64()),
                            final_size: settings.len() as u64,
                        },
                    );
                }
            }),
            H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64(),
        ),
        (
            "encoder stream FIN",
            Box::new(move |h| h.receive(client_uni(1), &encoder, true)),
            H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64(),
        ),
        (
            "decoder stream reset",
            Box::new(move |h| {
                h.receive(client_uni(1), &decoder, false);
                h.ready_edge(
                    client_uni(1),
                    StreamReady::Reset {
                        code: app(0),
                        final_size: 1,
                    },
                );
            }),
            H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64(),
        ),
        (
            "STOP_SENDING on our control stream",
            Box::new(|h| {
                h.ready_edge(
                    server_uni(0),
                    StreamReady::Stopped {
                        code: app(H3ErrorCode::NO_ERROR.as_u64()),
                    },
                )
            }),
            H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64(),
        ),
        (
            "second control stream",
            Box::new({
                let settings = settings.clone();
                move |h| {
                    h.receive(client_uni(0), &settings, false);
                    h.receive(client_uni(1), &settings, false);
                }
            }),
            H3ErrorCode::STREAM_CREATION_ERROR.as_u64(),
        ),
        (
            "second encoder stream",
            Box::new(move |h| {
                h.receive(client_uni(1), &encoder, false);
                h.receive(client_uni(2), &encoder, false);
            }),
            H3ErrorCode::STREAM_CREATION_ERROR.as_u64(),
        ),
        (
            "second decoder stream",
            Box::new(move |h| {
                h.receive(client_uni(1), &decoder, false);
                h.receive(client_uni(2), &decoder, false);
            }),
            H3ErrorCode::STREAM_CREATION_ERROR.as_u64(),
        ),
        (
            "client push stream",
            Box::new(|h| h.receive(client_uni(1), &[StreamType::PUSH.as_u64() as u8, 0], false)),
            H3ErrorCode::STREAM_CREATION_ERROR.as_u64(),
        ),
        (
            "control stream without SETTINGS first",
            Box::new(|h| {
                let mut bytes = vec![StreamType::CONTROL.as_u64() as u8];
                bytes.extend_from_slice(&encoded(Frame::MaxPushId(1)));
                h.receive(client_uni(0), &bytes, false);
            }),
            H3ErrorCode::MISSING_SETTINGS.as_u64(),
        ),
        (
            "second SETTINGS",
            Box::new({
                let control_then = control_then.clone();
                move |h| {
                    h.receive(
                        client_uni(0),
                        &control_then(Frame::Settings(Settings::new())),
                        false,
                    )
                }
            }),
            H3ErrorCode::FRAME_UNEXPECTED.as_u64(),
        ),
        (
            "DATA on the control stream",
            Box::new({
                let control_then = control_then.clone();
                move |h| {
                    h.receive(
                        client_uni(0),
                        &control_then(Frame::Data(Bytes::from_static(b"x"))),
                        false,
                    )
                }
            }),
            H3ErrorCode::FRAME_UNEXPECTED.as_u64(),
        ),
        (
            "GOAWAY declaring a 2^30-byte payload",
            Box::new({
                let settings = settings.clone();
                move |h| {
                    let mut bytes = settings.clone();
                    bytes.extend_from_slice(&[0x07, 0xc0, 0, 0, 0, 0x40, 0, 0, 0]);
                    h.receive(client_uni(0), &bytes, false)
                }
            }),
            H3ErrorCode::EXCESSIVE_LOAD.as_u64(),
        ),
        (
            "CANCEL_PUSH for a push never promised",
            Box::new({
                let control_then = control_then.clone();
                move |h| h.receive(client_uni(0), &control_then(Frame::CancelPush(0)), false)
            }),
            H3ErrorCode::ID_ERROR.as_u64(),
        ),
        (
            "encoder stream insert",
            Box::new(move |h| h.receive(client_uni(1), &[encoder[0], 0xc0 | 17], false)),
            roxygen_qpack::QPACK_ENCODER_STREAM_ERROR,
        ),
        (
            "encoder stream capacity above the maximum",
            Box::new(move |h| h.receive(client_uni(1), &[encoder[0], 0x3f, 0xe2, 0x1f], false)),
            roxygen_qpack::QPACK_ENCODER_STREAM_ERROR,
        ),
        (
            "decoder stream insert count increment",
            Box::new(move |h| h.receive(client_uni(1), &[decoder[0], 0x00], false)),
            roxygen_qpack::QPACK_DECODER_STREAM_ERROR,
        ),
    ];
    for (name, step, code) in cases {
        let mut h = Harness::new();
        h.event(ConnEvent::Connected);
        step(&mut h);
        assert_eq!(h.closed(), Some(code), "{name}");
    }
}

#[test]
fn settings_precede_a_goaway_queued_before_the_control_stream_opened() {
    let plan = Plan {
        head_gate: Some(Gate::default()),
        ..Plan::default()
    };
    let mut h = Harness::with_plan(H3ApplicationBuilder::new(), plan);
    h.conn.uni_limit = 0;
    h.event(ConnEvent::Connected);
    assert!(
        h.conn.streams.is_empty(),
        "no unidirectional stream credit yet"
    );
    h.receive(client_bidi(0), &headers_frame(GET), true);
    h.session.core.on_quiesce(&mut h.conn);
    // A transport that has shut down cannot open a stream, so the control stream opens first.
    assert!(
        !h.conn.shutdown,
        "shutdown waits for the control stream to open"
    );

    h.conn.uni_limit = 3;
    h.event(ConnEvent::StreamsAvailable { dir: Dir::Uni });
    let (header, frames) = h.control();
    assert_eq!(header, UniStreamHeader::Control);
    assert!(
        matches!(frames.as_slice(), [Frame::Settings(_), Frame::Goaway(4)]),
        "{frames:?}"
    );
    assert!(h.conn.shutdown, "the transport shuts down after the GOAWAY");
}

#[test]
fn a_quiesce_waiting_for_the_control_stream_closes_only_after_its_goaway_can_leave() {
    let mut h = Harness::new();
    h.conn.uni_limit = 0;
    h.event(ConnEvent::Connected);
    h.session.core.on_quiesce(&mut h.conn);
    assert_eq!(
        h.session.deadline(),
        None,
        "no close is due while the GOAWAY cannot be sent"
    );
    h.conn.now += Duration::from_secs(60);
    h.poll_session();
    assert_eq!(
        h.closed(),
        None,
        "the connection waits for its control stream"
    );

    h.conn.uni_limit = 3;
    h.event(ConnEvent::StreamsAvailable { dir: Dir::Uni });
    let (_, frames) = h.control();
    assert!(
        matches!(frames.as_slice(), [Frame::Settings(_), Frame::Goaway(0)]),
        "{frames:?}"
    );
    assert_eq!(h.closed(), None, "the GOAWAY is staged, not sent");
    let deadline = h
        .session
        .deadline()
        .expect("the close waits for the GOAWAY");
    h.poll_session();
    assert_eq!(h.closed(), None, "before the deadline");
    h.conn.now = deadline;
    h.poll_session();
    assert_eq!(h.closed(), Some(H3ErrorCode::NO_ERROR.as_u64()));
}

#[test]
fn handshake_completes_with_the_peer_settings() {
    let mut h = Harness::new();
    h.event(ConnEvent::Connected);
    assert!(
        !h.session.handshake_complete(),
        "no peer control stream yet"
    );
    let control = control_stream(&Settings::new());
    h.receive(client_uni(0), &control[..1], false);
    assert!(
        !h.session.handshake_complete(),
        "the control stream has no SETTINGS yet"
    );
    h.receive(client_uni(0), &control[1..], false);
    assert!(
        h.session.handshake_complete(),
        "the peer's SETTINGS arrived"
    );
}

#[test]
fn qpack_stream_fin_inside_an_instruction_closes_a_critical_stream() {
    let mut h = Harness::ready();
    // Set Dynamic Table Capacity with a continued integer, cut off by the FIN.
    h.receive(
        client_uni(1),
        &[StreamType::QPACK_ENCODER.as_u64() as u8, 0x3f],
        true,
    );
    assert_eq!(
        h.closed(),
        Some(H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64())
    );
}

#[test]
fn closed_edge_on_a_critical_stream_closes_the_connection() {
    let mut h = Harness::ready();
    h.receive(
        client_uni(1),
        &[StreamType::QPACK_DECODER.as_u64() as u8],
        false,
    );
    h.ready_edge(client_uni(1), StreamReady::Closed);
    assert_eq!(
        h.closed(),
        Some(H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64())
    );
}

#[test]
fn control_stream_bytes_go_out_across_partial_sends_and_pulls() {
    let mut h = Harness::new();
    h.conn.uni_send_window = 4;
    h.event(ConnEvent::Connected);
    let control = server_uni(0);
    assert_eq!(
        h.conn.get(control).sent.len(),
        4,
        "the stream took its window"
    );
    h.ready_edge(control, StreamReady::Writable);
    assert_eq!(h.conn.get(control).sent.len(), 8);
    assert!(
        h.pull(control, 1 << 20).0.is_ready(),
        "the pull path drains the rest"
    );
    let (header, frames) = h.control();
    assert_eq!(header, UniStreamHeader::Control);
    assert!(
        matches!(frames.as_slice(), [Frame::Settings(_)]),
        "{frames:?}"
    );
    assert!(h.pull(control, 1 << 20).0.is_pending(), "nothing is left");
}

fn app(code: u64) -> mvfstr_proto::AppCode {
    mvfstr_proto::AppCode(mvfstr_proto::VarInt::from_u64(code).unwrap())
}

// ---------------------------------------------------------------------------------------------
// Requests and responses.

/// Returns the request head the application received, as (method, url, fields).
fn received_head<A: OwnedApplication>(
    h: &Harness<A>,
    id: StreamId,
) -> (String, String, Vec<(String, String)>) {
    let state = h.app.0.borrow();
    let head = &state.requests[&http_id(id)].head;
    let mut fields = Vec::new();
    head.headers().for_each(|name, value| {
        fields.push((
            String::from_utf8(name.to_vec()).unwrap(),
            String::from_utf8(value.to_vec()).unwrap(),
        ));
    });
    (
        head.request_method().unwrap().to_owned(),
        String::from_utf8(head.request_url().unwrap().to_vec()).unwrap(),
        fields,
    )
}

#[test]
fn get_produces_headers_data_fin() {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let request = headers_frame(GET);
    // A head whose FIN comes later is delivered at once, and the FIN's Finished edge then ends
    // the body.
    h.receive(id, &request, false);
    assert_eq!(h.log(), vec![Ev::New(1), Ev::Request { id: 1, eom: false }]);
    h.receive(id, &[], true);
    let Poll::Ready(Ok(event)) = h.read_body(id, 100) else {
        panic!("the FIN ends the request body");
    };
    assert!(event.eom() && event.as_body().is_some_and(BufQueue::is_empty));
    let (method, url, fields) = received_head(&h, id);
    assert_eq!((method.as_str(), url.as_str()), ("GET", "/index.html"));
    assert_eq!(
        field(&fields, "Host").or(field(&fields, "host")),
        Some("example.com")
    );
    assert!(h.conn.take_stream_wakes().contains(&id));

    let (poll, sink) = h.pull(id, 1 << 20);
    assert!(poll.is_ready());
    assert_eq!(
        sink.parts.last(),
        Some(&5),
        "the body goes out as a shared payload"
    );
    assert!(sink.finished);
    let parts = h.response(id);
    let [Part::Headers(head), Part::Data(body)] = parts.as_slice() else {
        panic!("expected HEADERS and DATA: {parts:?}");
    };
    assert_eq!(field(head, ":status"), Some("200"));
    assert_eq!(field(head, "content-type"), Some("text/plain"));
    assert_eq!(body, b"hello");
    assert!(h.conn.get(id).finished);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
    h.poll_session();
    assert_eq!(h.conn.get(id).consumed, request.len() as u64);
    assert!(h.pull(id, 1 << 20).0.is_pending());

    // A head that arrives with its FIN, reported by one Finished edge, is delivered complete.
    let second = client_bidi(1);
    h.receive(second, &[], false);
    h.conn.receive(second, &request);
    h.conn.get(second).peer_finished = true;
    h.ready_edge(second, StreamReady::Finished);
    assert_eq!(h.log()[4..], [Ev::New(5), Ev::Request { id: 5, eom: true }]);
    h.pull_all(second, 1 << 20);
    assert_eq!(body_of(&h.response(second)), b"hello");
    assert_eq!(h.log()[6..], [Ev::ResponseComplete(5), Ev::Complete(5)]);
}

/// A request head whose FIN came in the same packet is delivered complete at the stream's first
/// edge, and the Readable and Finished edges that follow do not add events.
#[test]
fn a_head_whose_fin_arrived_with_it_is_delivered_complete_at_the_first_edge() {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let request = headers_frame(GET);
    h.conn.receive(id, &request);
    h.conn.get(id).peer_finished = true;
    h.ready_edge(id, StreamReady::Opened);
    assert_eq!(h.log(), [Ev::New(1), Ev::Request { id: 1, eom: true }]);
    h.ready_edge(id, StreamReady::Readable);
    h.ready_edge(id, StreamReady::Finished);
    assert_eq!(h.log().len(), 2, "the later edges deliver nothing more");
    assert_eq!(h.conn.get(id).consumed, request.len() as u64);
    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), b"hello");
    assert!(h.conn.get(id).finished);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
}

/// A request head starts at the transport's reading for the phase in which it is decoded, whether
/// its FIN came with it or not, and a head whose bytes span two phases starts at the second.
#[test]
fn a_request_head_starts_at_the_phase_reading() {
    let mut h = Harness::ready();
    // Ahead of the wall clock, so that a clock read while decoding cannot match it.
    h.conn.now = Instant::now() + Duration::from_secs(3600);
    let first = h.conn.now;
    let (get, post, split) = (client_bidi(0), client_bidi(1), client_bidi(2));
    h.receive(get, &headers_frame(GET), true);
    h.receive(post, &headers_frame(POST), false);
    let request = headers_frame(GET);
    h.receive(split, &request[..request.len() - 1], false);
    h.conn.now += Duration::from_millis(5);
    let second = h.conn.now;
    h.receive(split, &request[request.len() - 1..], true);
    assert_eq!(
        h.log(),
        [
            Ev::New(1),
            Ev::Request { id: 1, eom: true },
            Ev::New(5),
            Ev::Request { id: 5, eom: false },
            Ev::New(9),
            Ev::Request { id: 9, eom: true },
        ]
    );
    let state = h.app.0.borrow();
    let start = |id| state.requests[&http_id(id)].head.start_time();
    assert_eq!(start(get), first);
    assert_eq!(start(post), first);
    assert_eq!(start(split), second);
}

/// A request body whose FIN arrived with it ends at the stream's first edge: the request is
/// delivered with a body, and the first read returns the body's bytes with its end.
#[test]
fn a_body_whose_fin_arrived_with_it_ends_at_the_first_edge() {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    h.conn
        .receive(id, &[headers_frame(POST), data_frame(b"hello")].concat());
    h.conn.get(id).peer_finished = true;
    h.ready_edge(id, StreamReady::Opened);
    assert_eq!(h.log(), [Ev::New(1), Ev::Request { id: 1, eom: false }]);
    assert_eq!(read_chunk(&mut h, id), (b"hello".to_vec(), true));
}

#[test]
fn finished_exchange_needs_no_session_wake() {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);
    let wakes = h.wakes();
    h.pull_all(id, 1 << 20);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
    h.ready_edge(id, StreamReady::Closed);
    assert_eq!(
        h.wakes(),
        wakes,
        "a request that ends normally costs no session wake"
    );
    assert_eq!(h.conn.get(id).consumed, headers_frame(GET).len() as u64);
}

#[test]
fn head_response_carries_no_content() {
    // The application answers HEAD as it answers GET, and the session does not send the content,
    // as RFC 9110 requires. RFC 9114 lets the head keep the content-length of the GET.
    let plan = Plan {
        headers: vec![("content-type", "text/plain"), ("content-length", "5")],
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    let head = [
        (":method", "HEAD"),
        (":scheme", "https"),
        (":authority", "example.com"),
        (":path", "/index.html"),
    ];
    h.receive(id, &headers_frame(&head), true);
    h.pull_all(id, 1 << 20);
    let parts = h.response(id);
    let [Part::Headers(fields)] = parts.as_slice() else {
        panic!("expected HEADERS only: {parts:?}");
    };
    assert_eq!(field(fields, ":status"), Some("200"));
    assert_eq!(field(fields, "content-length"), Some("5"));
    assert!(h.conn.get(id).finished);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
}

#[test]
fn large_response_spans_several_polls() {
    let body: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let plan = Plan {
        body: Bytes::from(body.clone()),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);

    // Each call stays within its allowance; the sink refuses anything larger.
    let mut calls = 0;
    while h.pull(id, 30_000).0.is_ready() {
        calls += 1;
    }
    assert!(calls >= 7, "{calls} calls");
    let parts = h.response(id);
    assert_eq!(body_of(&parts), body);
    assert!(h.conn.get(id).finished);
    let maxima = h.app.0.borrow().maxima.clone();
    // min(allowance, 64 KiB) less the DATA frame header.
    assert!(maxima.iter().all(|&maximum| maximum <= 30_000 - 5));
    assert_eq!(maxima[1], 30_000 - 5);

    // With a larger allowance the cap is 64 KiB less the frame header.
    let mut h = Harness::ready_with(
        H3ApplicationBuilder::new(),
        Plan {
            body: Bytes::from(body.clone()),
            ..Plan::default()
        },
    );
    h.receive(id, &headers_frame(GET), true);
    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), body);
    let maxima = h.app.0.borrow().maxima.clone();
    assert_eq!(maxima[0], (64 << 10) - 5);
}

/// Returns the bytes of the next body event the application reads, and whether it ends the body.
fn read_chunk<A: OwnedApplication>(h: &mut Harness<A>, id: StreamId) -> (Vec<u8>, bool) {
    let Poll::Ready(Ok(event)) = h.read_body(id, 1 << 20) else {
        panic!("a body event is ready");
    };
    let eom = event.eom();
    match event.into_kind() {
        HttpBodyEventKind::Body(body) => (
            body.segments()
                .flat_map(|segment| segment.to_vec())
                .collect(),
            eom,
        ),
        _ => panic!("expected a body event"),
    }
}

#[test]
fn post_credit_returned_only_on_consume() {
    let mut h = Harness::ready_with(
        H3ApplicationBuilder::new().request_body_buffer(1000),
        Plan::default(),
    );
    let id = client_bidi(0);
    let head = headers_frame(POST);
    h.receive(id, &head, false);
    assert_eq!(h.log(), vec![Ev::New(1), Ev::Request { id: 1, eom: false }]);
    let mut credited = head.len() as u64;
    assert_eq!(h.conn.get(id).consumed, credited);

    // The DATA frame header's credit is returned at once, the payload's only when consumed.
    h.receive(id, &data_frame(&[1; 600]), false);
    credited += 3;
    assert_eq!(h.conn.get(id).consumed, credited);
    h.poll_session();
    assert_eq!(h.conn.get(id).consumed, credited);

    let wakes = h.wakes();
    assert_eq!(read_chunk(&mut h, id), (vec![1; 600], false));
    assert!(h.wakes() > wakes, "consuming wakes the session");
    assert_eq!(h.conn.get(id).consumed, credited);
    h.poll_session();
    credited += 600;
    assert_eq!(h.conn.get(id).consumed, credited);

    // At most the request body buffer is handed to the body before the application reads it.
    h.receive(id, &data_frame(&[2; 700]), false);
    h.receive(id, &data_frame(&[3; 700]), true);
    credited += 3 + 3;
    assert_eq!(h.conn.get(id).consumed, credited);
    assert_eq!(
        read_chunk(&mut h, id),
        ([vec![2; 700], vec![3; 300]].concat(), false)
    );
    h.poll_session();
    credited += 1000;
    assert_eq!(h.conn.get(id).consumed, credited);
    assert_eq!(read_chunk(&mut h, id), (vec![3; 400], true));
    h.poll_session();
    credited += 400;
    assert_eq!(h.conn.get(id).consumed, credited);
    assert!(h.conn.get(id).readable.is_empty());

    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), b"hello");
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
}

#[test]
fn retired_exchange_returns_body_credit_only_as_consumed() {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let head = headers_frame(POST);
    let request = [head.clone(), data_frame(&[7; 500])].concat();
    h.receive(id, &request, true);
    h.pull_all(id, 1 << 20);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);

    // The application still holds the unread body, so its credit stays out.
    h.poll_session();
    let overhead = head.len() as u64 + 3;
    assert_eq!(h.conn.get(id).consumed, overhead);
    assert_eq!(read_all(&mut h, id), (vec![7; 500], None));
    h.poll_session();
    assert_eq!(h.conn.get(id).consumed, request.len() as u64);
}

#[test]
fn credit_cell_wakes_the_session_from_another_thread() {
    use crate::credit::CreditCell;
    use crate::credit::WorkQueue;
    use crate::fake::FakeHandle;

    let queue = Arc::new(WorkQueue::default());
    let handle = FakeHandle::default();
    let count = Arc::clone(&handle.wakes);
    queue.set_handle(Box::new(handle));
    let cell = Arc::new(CreditCell::new(client_bidi(0), Arc::clone(&queue)));
    let consumer = Arc::clone(&cell);
    std::thread::spawn(move || {
        consumer.add(10);
        consumer.add(5);
    })
    .join()
    .unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "one wake until the session drains"
    );
    let mut streams = Vec::new();
    queue.take(&mut streams);
    assert_eq!(streams, vec![client_bidi(0)]);
    assert_eq!(cell.take(), 15);

    let consumer = Arc::clone(&cell);
    std::thread::spawn(move || consumer.add(1)).join().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(cell.take(), 1);
}

#[test]
fn dropped_session_wake_is_retried_by_later_credit() {
    use crate::credit::CreditCell;
    use crate::credit::WorkQueue;
    use crate::fake::FakeHandle;

    let queue = Arc::new(WorkQueue::default());
    let handle = FakeHandle::default();
    let delivered = Arc::clone(&handle.wakes);
    handle.drop_wakes.store(1, Ordering::SeqCst);
    queue.set_handle(Box::new(handle));
    let cell = CreditCell::new(client_bidi(0), Arc::clone(&queue));
    cell.add(10);
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        0,
        "the first wake is dropped"
    );
    cell.add(5);
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        1,
        "later credit retries the dropped wake"
    );
    cell.add(1);
    assert_eq!(delivered.load(Ordering::SeqCst), 1, "one wake per drain");
}

#[test]
fn wakes_use_the_session_waker_once_polled() {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let request = [headers_frame(POST), data_frame(&[1; 600])].concat();
    h.receive(id, &request, false);
    h.poll_session();
    let handle_wakes = h.conn.session_wakes();
    let waker_wakes = h.waker_wakes();
    assert_eq!(read_chunk(&mut h, id), (vec![1; 600], false));
    assert_eq!(
        h.waker_wakes(),
        waker_wakes + 1,
        "consumption wakes the waker poll_session was given"
    );
    assert_eq!(h.conn.session_wakes(), handle_wakes, "no handle command");
    h.poll_session();
    assert_eq!(h.conn.get(id).consumed, request.len() as u64);
}

#[test]
fn malformed_requests_are_reset_before_delivery() {
    const BASE: &[(&str, &str)] = &[(":scheme", "https"), (":authority", "a.example")];
    let request = |method: &str, path: &str, extra: &[(&str, &str)]| {
        let mut fields = vec![(":method", method), (":path", path)];
        fields.extend_from_slice(BASE);
        fields.extend_from_slice(extra);
        headers_frame(&fields)
    };
    let get = |extra: &[(&str, &str)]| request("GET", "/", extra);
    let with_body = |extra: &[(&str, &str)], body: &[u8]| {
        let mut bytes = request("POST", "/", extra);
        bytes.extend_from_slice(&data_frame(body));
        bytes
    };
    let many_static_lines: Vec<(&str, &str)> = (0..10).map(|_| ("accept", "*/*")).collect();
    // (class, request stream bytes, whether the FIN arrives with them)
    let cases: Vec<(&str, Vec<u8>, bool)> = vec![
        (
            "missing :method",
            headers_frame(&[(":scheme", "https"), (":authority", "a"), (":path", "/")]),
            false,
        ),
        (
            "missing :scheme",
            headers_frame(&[(":method", "GET"), (":authority", "a"), (":path", "/")]),
            false,
        ),
        (
            "missing :path",
            headers_frame(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "a"),
            ]),
            false,
        ),
        ("empty :path", request("GET", "", &[]), false),
        (
            "missing :authority and host",
            headers_frame(&[(":method", "GET"), (":scheme", "https"), (":path", "/")]),
            false,
        ),
        (
            "empty :authority",
            headers_frame(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", ""),
                (":path", "/"),
            ]),
            false,
        ),
        (
            "host differs from :authority",
            get(&[("host", "b.example")]),
            false,
        ),
        (
            "duplicate pseudo-header",
            headers_frame(&[
                (":method", "GET"),
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "/"),
            ]),
            false,
        ),
        (
            "pseudo-header after a regular field",
            headers_frame(&[
                (":method", "GET"),
                (":scheme", "https"),
                ("accept", "*/*"),
                (":authority", "a"),
                (":path", "/"),
            ]),
            false,
        ),
        (
            "undefined pseudo-header",
            headers_frame(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "/"),
                (":foo", "bar"),
            ]),
            false,
        ),
        (
            "response pseudo-header",
            headers_frame(&[
                (":status", "200"),
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "/"),
            ]),
            false,
        ),
        (
            "extended CONNECT not offered",
            headers_frame(&[
                (":method", "CONNECT"),
                (":protocol", "websocket"),
                (":scheme", "https"),
                (":authority", "a"),
                (":path", "/"),
            ]),
            false,
        ),
        (
            "CONNECT with a path",
            headers_frame(&[
                (":method", "CONNECT"),
                (":authority", "a:443"),
                (":path", "/"),
            ]),
            false,
        ),
        ("uppercase field name", get(&[("Accept", "*/*")]), false),
        ("connection", get(&[("connection", "keep-alive")]), false),
        ("keep-alive", get(&[("keep-alive", "timeout=5")]), false),
        (
            "proxy-connection",
            get(&[("proxy-connection", "keep-alive")]),
            false,
        ),
        (
            "transfer-encoding",
            get(&[("transfer-encoding", "chunked")]),
            false,
        ),
        ("upgrade", get(&[("upgrade", "h2c")]), false),
        ("te other than trailers", get(&[("te", "gzip")]), false),
        (
            "value with a line feed",
            get(&[("x-a", "1\nx-b: 2")]),
            false,
        ),
        (
            "invalid content-length",
            get(&[("content-length", "1x")]),
            false,
        ),
        (
            "conflicting content-length",
            get(&[("content-length", "1"), ("content-length", "2")]),
            false,
        ),
        (
            "DATA beyond content-length",
            with_body(&[("content-length", "3")], b"hello"),
            false,
        ),
        (
            "DATA short of content-length",
            with_body(&[("content-length", "10")], b"hello"),
            true,
        ),
        (
            "field section over the limit",
            get(&many_static_lines),
            false,
        ),
    ];
    let message_error = H3ErrorCode::MESSAGE_ERROR.as_u64();
    for (class, bytes, fin) in cases {
        let mut h = Harness::ready_with(
            H3ApplicationBuilder::new().max_field_section_size(300),
            Plan::default(),
        );
        let id = client_bidi(0);
        h.receive(id, &[], false);
        h.conn.receive(id, &bytes);
        h.conn.get(id).peer_finished = fin;
        h.ready_edge(
            id,
            if fin {
                StreamReady::Finished
            } else {
                StreamReady::Readable
            },
        );
        let stream = h.conn.get(id);
        assert_eq!(stream.reset, Some(message_error), "{class}");
        assert_eq!(stream.stop_sending, Some(message_error), "{class}");
        assert_eq!(
            h.log(),
            vec![
                Ev::New(1),
                Ev::Cancel {
                    id: 1,
                    code: HttpErrorCode::MESSAGE_ERROR,
                    peer: false
                }
            ],
            "{class}"
        );
        assert_eq!(h.closed(), None, "{class}");

        // The connection carries on.
        let next = client_bidi(1);
        h.receive(next, &headers_frame(GET), true);
        h.pull_all(next, 1 << 20);
        assert_eq!(body_of(&h.response(next)), b"hello", "{class}");
    }
}

#[test]
fn field_values_with_surrounding_whitespace_are_malformed() {
    // RFC 9114 requires values to follow the field-content rule, which does not allow a leading
    // or trailing SP or HTAB.
    let message_error = Some(H3ErrorCode::MESSAGE_ERROR.as_u64());
    for value in [" a", "a ", "\ta", "a\t", " "] {
        let mut h = Harness::ready();
        let id = client_bidi(0);
        h.receive(
            id,
            &headers_frame(&[GET, &[("x-a", value)]].concat()),
            false,
        );
        assert_eq!(h.conn.get(id).reset, message_error, "{value:?}");
        assert_eq!(
            h.log(),
            vec![
                Ev::New(1),
                Ev::Cancel {
                    id: 1,
                    code: HttpErrorCode::MESSAGE_ERROR,
                    peer: false
                }
            ],
            "{value:?}"
        );

        let next = client_bidi(1);
        let request = [headers_frame(POST), headers_frame(&[("x-t", value)])].concat();
        h.receive(next, &request, false);
        assert_eq!(h.conn.get(next).reset, message_error, "trailer {value:?}");
    }

    // Inner whitespace and empty values are valid.
    let mut h = Harness::ready();
    let id = client_bidi(0);
    h.receive(
        id,
        &headers_frame(&[GET, &[("x-a", "a \t b"), ("x-b", "")]].concat()),
        true,
    );
    assert_eq!(h.conn.get(id).reset, None);
    assert_eq!(h.log(), vec![Ev::New(1), Ev::Request { id: 1, eom: true }]);
}

/// Returns whether a request head with `fields` is reset as malformed before delivery.
fn rejected(fields: &[(&str, &str)]) -> bool {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    h.receive(id, &headers_frame(fields), true);
    let reset = h.conn.get(id).reset == Some(H3ErrorCode::MESSAGE_ERROR.as_u64());
    assert_eq!(
        h.log().contains(&Ev::Request { id: 1, eom: true }),
        !reset,
        "{fields:?}"
    );
    reset
}

#[test]
fn method_is_case_sensitive() {
    // RFC 9110 makes methods case-sensitive, so `connect` is not CONNECT and gets the checks of
    // an ordinary request, and the application receives each method as it was sent.
    let ordinary = |method| {
        [
            (":method", method),
            (":scheme", "https"),
            (":authority", "a.example:443"),
            (":path", "/"),
        ]
    };
    for method in ["get", "Post", "connect"] {
        let mut h = Harness::ready();
        let id = client_bidi(0);
        h.receive(id, &headers_frame(&ordinary(method)), true);
        assert_eq!(h.conn.get(id).reset, None, "{method}");
        assert_eq!(received_head(&h, id).0, method);
    }
    // The CONNECT form is open only to `CONNECT`, and the asterisk form only to `OPTIONS`.
    assert!(rejected(&[
        (":method", "connect"),
        (":authority", "a.example:443")
    ]));
    assert!(rejected(&[
        (":method", "options"),
        (":scheme", "https"),
        (":authority", "a.example"),
        (":path", "*"),
    ]));
}

#[test]
fn te_trailers_is_case_insensitive() {
    // "trailers" is a literal of RFC 9110's TE grammar, and RFC 5234 makes ABNF string literals
    // case-insensitive.
    for value in ["trailers", "Trailers", "TRAILERS"] {
        assert!(!rejected(&[GET, &[("te", value)]].concat()), "{value}");
    }
    assert!(rejected(&[GET, &[("te", "trailers, gzip")]].concat()));
}

#[test]
fn path_must_be_origin_form() {
    // RFC 9114 requires `:path` to be path-absolute and an optional query, or `*` for OPTIONS.
    let request = |method, path| {
        [
            (":method", method),
            (":scheme", "https"),
            (":authority", "a.example"),
            (":path", path),
        ]
    };
    for (method, path) in [
        ("GET", "https://a.example/"),
        ("GET", "index.html"),
        ("GET", "/a#fragment"),
        ("GET", "*"),
        ("GET", "/a b"),
    ] {
        assert!(rejected(&request(method, path)), "{method} {path}");
    }
    for (method, path) in [("GET", "/a/b?c=d&e"), ("OPTIONS", "*"), ("GET", "/%7Euser")] {
        assert!(!rejected(&request(method, path)), "{method} {path}");
    }
}

#[test]
fn http_authority_has_no_userinfo() {
    // RFC 9114 does not allow userinfo in the authority of an http or https URI, whether it comes
    // from `:authority` or `host`, and whichever order the pseudo-header fields come in.
    let cases: [&[(&str, &str)]; 4] = [
        &[
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "user@a.example"),
            (":path", "/"),
        ],
        &[
            (":method", "GET"),
            (":authority", "user:pw@a.example"),
            (":scheme", "http"),
            (":path", "/"),
        ],
        &[
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            ("host", "user@a.example"),
        ],
        &[(":method", "CONNECT"), (":authority", "user@a.example:443")],
    ];
    for fields in cases {
        assert!(rejected(fields), "{fields:?}");
    }
    assert!(!rejected(&[
        (":method", "CONNECT"),
        (":authority", "a.example:443")
    ]));
    assert!(!rejected(&[
        (":method", "GET"),
        (":scheme", "https"),
        (":authority", "[2001:db8::1]:8443"),
        (":path", "/"),
    ]));
}

#[test]
fn authority_follows_the_uri_syntax() {
    // In RFC 9114, `:authority`, or `host` in its place, is the authority of the target URI in
    // RFC 3986's syntax, and RFC 9110 requires a host in an http or https URI.
    let with_authority = |authority| {
        [
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", authority),
            (":path", "/"),
        ]
    };
    let with_host = |host| {
        [
            (":method", "GET"),
            (":scheme", "https"),
            (":path", "/"),
            ("host", host),
        ]
    };
    for authority in [
        "a b",
        "a/b",
        "a?b",
        "a#b",
        "a\\b",
        "a.example:80x",
        "a:1:2",
        "[2001:db8::1",
        "[2001:db8::1]x",
        "a%zz.example",
        ":443",
        // In RFC 3986, an IP literal is an IPv6 address or an IPvFuture.
        "[foo]:443",
        "[192.0.2.1]",
        "[2001:db8::1::2]",
        "[v1]",
        "[v.a]",
        "[vg.a]:443",
        "[v1.a%20]",
    ] {
        assert!(rejected(&with_authority(authority)), "{authority}");
        assert!(rejected(&with_host(authority)), "host {authority}");
    }
    for authority in [
        "a.example",
        "a.example:8443",
        "a.example:",
        "[2001:db8::1]:8443",
        "[::1]",
        "[::ffff:192.0.2.1]:443",
        "[v1.a:b]:443",
        "[V1F.x~y]",
        "xn--bcher-kva.example",
        "a_b~c.example",
        "a%2Db.example",
    ] {
        assert!(!rejected(&with_authority(authority)), "{authority}");
        assert!(!rejected(&with_host(authority)), "host {authority}");
    }
}

#[test]
fn connect_authority_names_host_and_port() {
    // RFC 9114 requires the `:authority` of CONNECT to carry the host and port to connect to.
    let connect = |authority| [(":method", "CONNECT"), (":authority", authority)];
    for authority in ["a.example", "a.example:", "[2001:db8::1]", ":443"] {
        assert!(rejected(&connect(authority)), "{authority}");
    }
    for authority in ["a.example:443", "[2001:db8::1]:443", "192.0.2.1:8080"] {
        assert!(!rejected(&connect(authority)), "{authority}");
    }
}

const CONNECT: &[(&str, &str)] = &[(":method", "CONNECT"), (":authority", "a.example:443")];

#[test]
fn connect_stream_carries_only_data_after_the_head() {
    // RFC 9114 permits only DATA frames after the CONNECT head, and receipt of any other known
    // frame type is a connection error.
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let request = [headers_frame(CONNECT), data_frame(b"tunnel")].concat();
    h.receive(id, &request, false);
    assert_eq!(h.log(), vec![Ev::New(1), Ev::Request { id: 1, eom: false }]);
    assert_eq!(read_chunk(&mut h, id), (b"tunnel".to_vec(), false));
    h.receive(id, &headers_frame(&[("x-trailer", "1")]), false);
    assert_eq!(
        h.closed(),
        Some(H3ErrorCode::FRAME_UNEXPECTED.as_u64()),
        "HEADERS after the CONNECT head"
    );
}

#[test]
fn connect_response_carries_no_trailers() {
    // RFC 9114 allows only DATA frames on the stream once CONNECT has completed.
    let plan = Plan {
        trailers: Some(vec![("x-checksum", "abc")]),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(CONNECT), false);
    h.pull_all(id, 1 << 20);
    let parts = h.response(id);
    let [Part::Headers(head), Part::Data(body)] = parts.as_slice() else {
        panic!("expected HEADERS and DATA only: {parts:?}");
    };
    assert_eq!(field(head, ":status"), Some("200"));
    assert_eq!(body, b"hello");
    assert!(h.conn.get(id).finished);
}

#[test]
fn connect_tunnel_stays_open_to_the_client_after_the_response_ends() {
    // In RFC 9114, the proxy closes its send stream when the target closes, and the client may
    // go on sending, so the end of a tunnel's response does not stop the request stream.
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let request = [headers_frame(CONNECT), data_frame(b"up")].concat();
    h.receive(id, &request, false);
    h.pull_all(id, 1 << 20);
    assert!(h.conn.get(id).finished);
    h.poll_session();
    assert_eq!(h.conn.get(id).stop_sending, None);
    h.receive(id, &data_frame(b"stream"), true);
    let mut upload = Vec::new();
    loop {
        let (chunk, eom) = read_chunk(&mut h, id);
        upload.extend(chunk);
        if eom {
            break;
        }
    }
    assert_eq!(upload, b"upstream");
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
}

#[test]
fn peer_reset_of_a_tunnel_after_its_response_ends_cancels() {
    // In RFC 9114, a client that resets a tunnel aborts its TCP connection, also after the
    // target closed its side, so the exchange is cancelled, not completed.
    let mut h = Harness::ready();
    let id = client_bidi(0);
    h.receive(id, &headers_frame(CONNECT), false);
    h.pull_all(id, 1 << 20);
    assert!(h.conn.get(id).finished);
    h.poll_session();
    h.ready_edge(
        id,
        StreamReady::Reset {
            code: app(H3ErrorCode::CONNECT_ERROR.as_u64()),
            final_size: 100,
        },
    );
    assert_eq!(
        h.log()[2..],
        [
            Ev::ResponseComplete(1),
            Ev::Cancel {
                id: 1,
                code: HttpErrorCode::CONNECT_ERROR,
                peer: true
            }
        ]
    );
    assert!(matches!(h.read_body(id, 100), Poll::Ready(Err(_))));
    h.ready_edge(id, StreamReady::Closed);
    assert_eq!(h.log().len(), 4, "no completion after the cancel");
    assert_eq!(h.closed(), None);
}

#[test]
fn scheme_follows_the_uri_syntax() {
    // RFC 3986 defines a scheme as a letter, then letters, digits, `+`, `-` and `.`.
    let request = |scheme| {
        [
            (":method", "GET"),
            (":scheme", scheme),
            (":authority", "a.example"),
            (":path", "/"),
        ]
    };
    for scheme in ["ht_tp", "1http", "web!", "-x"] {
        assert!(rejected(&request(scheme)), "{scheme}");
    }
    for scheme in ["https", "web+app", "coap.tcp", "x-1"] {
        assert!(!rejected(&request(scheme)), "{scheme}");
    }
}

#[test]
fn http_schemes_are_case_insensitive() {
    // RFC 3986 makes schemes case-insensitive, so `HTTPS` has the rules of `https`.
    assert!(
        rejected(&[(":method", "GET"), (":scheme", "HTTPS"), (":path", "/")]),
        "no authority"
    );
    assert!(
        rejected(&[
            (":method", "GET"),
            (":scheme", "Http"),
            (":authority", "user@a.example"),
            (":path", "/"),
        ]),
        "userinfo"
    );
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let request = [
        (":method", "GET"),
        (":scheme", "HTTPS"),
        (":authority", "a.example"),
        (":path", "/"),
    ];
    h.receive(id, &headers_frame(&request), true);
    assert!(h.app.0.borrow().requests[&http_id(id)].head.is_secure());
}

#[test]
fn trailers_both_ways() {
    let plan = Plan {
        trailers: Some(vec![("x-checksum", "abc")]),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    let mut request = headers_frame(POST);
    request.extend_from_slice(&data_frame(b"body"));
    request.extend_from_slice(&headers_frame(&[("x-request-trailer", "1")]));
    h.receive(id, &request, true);

    // The request trailers end the body.
    assert_eq!(read_chunk(&mut h, id), (b"body".to_vec(), false));
    let Poll::Ready(Ok(event)) = h.read_body(id, 100) else {
        panic!("the trailers are ready");
    };
    assert!(event.eom());
    let HttpBodyEventKind::Trailers(trailers) = event.into_kind() else {
        panic!("expected trailers");
    };
    assert_eq!(trailers.get_single_or_empty("x-request-trailer"), b"1");

    // The response trailers are a final HEADERS frame before the FIN.
    h.pull_all(id, 1 << 20);
    let parts = h.response(id);
    let [
        Part::Headers(head),
        Part::Data(body),
        Part::Headers(trailers),
    ] = parts.as_slice()
    else {
        panic!("expected HEADERS, DATA and trailers: {parts:?}");
    };
    assert_eq!(field(head, ":status"), Some("200"));
    assert_eq!(body, b"hello");
    assert_eq!(trailers, &[("x-checksum".to_owned(), "abc".to_owned())]);
    assert!(h.conn.get(id).finished);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);

    // Request trailers with a pseudo-header are malformed.
    let next = client_bidi(1);
    let mut request = headers_frame(POST);
    request.extend_from_slice(&headers_frame(&[(":path", "/")]));
    h.receive(next, &request, false);
    assert_eq!(
        h.conn.get(next).reset,
        Some(H3ErrorCode::MESSAGE_ERROR.as_u64())
    );
}

#[test]
fn responses_and_their_trailers_carry_no_te() {
    // RFC 9114 allows TE only in the header section of a request.
    let plan = Plan {
        headers: vec![("te", "trailers"), ("content-type", "text/plain")],
        trailers: Some(vec![("te", "trailers"), ("x-checksum", "abc")]),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);
    h.pull_all(id, 1 << 20);
    let parts = h.response(id);
    let [Part::Headers(head), Part::Data(_), Part::Headers(trailers)] = parts.as_slice() else {
        panic!("expected HEADERS, DATA and trailers: {parts:?}");
    };
    assert_eq!(field(head, "te"), None, "response head");
    assert_eq!(field(head, "content-type"), Some("text/plain"));
    assert_eq!(field(trailers, "te"), None, "response trailers");
    assert_eq!(field(trailers, "x-checksum"), Some("abc"));
}

#[test]
fn request_trailers_with_te_are_malformed() {
    // RFC 9114 allows TE only in the header section of a request.
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let request = [headers_frame(POST), headers_frame(&[("te", "trailers")])].concat();
    h.receive(id, &request, false);
    assert_eq!(
        h.conn.get(id).reset,
        Some(H3ErrorCode::MESSAGE_ERROR.as_u64())
    );
}

#[test]
fn early_response_stops_the_request_body() {
    stop_the_request_body_after_an_early_response(Harness::ready());
    stop_the_request_body_after_an_early_response(Harness::skipping(Plan::default()));
}

fn stop_the_request_body_after_an_early_response<A: OwnedApplication>(mut h: Harness<A>) {
    let id = client_bidi(0);
    let mut request = headers_frame(POST);
    request.extend_from_slice(&data_frame(&[1; 100]));
    h.receive(id, &request, false);

    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), b"hello");
    assert!(h.conn.get(id).finished);
    assert_eq!(h.conn.get(id).stop_sending, None);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1)]);

    // STOP_SENDING needs the stream, so the session sends it at its next callback.
    let wakes = h.conn.session_wakes();
    h.poll_session();
    assert!(wakes > 0);
    assert_eq!(
        h.conn.get(id).stop_sending,
        Some(H3ErrorCode::NO_ERROR.as_u64())
    );
    assert_eq!(h.conn.get(id).reset, None);
    // The application's body read ends with the cancellation rather than waiting for more.
    let mut reads = Vec::new();
    loop {
        match h.read_body(id, 1 << 20) {
            Poll::Ready(Ok(event)) => reads.push(format!("body eom={}", event.eom())),
            Poll::Ready(Err(error)) => {
                reads.push(format!("error {:?}", error.code()));
                break;
            }
            Poll::Pending => panic!("the body read waits after the early response: {reads:?}"),
        }
        assert!(reads.len() < 3, "{reads:?}");
    }
    assert!(reads.last().unwrap().starts_with("error"), "{reads:?}");

    // The peer's reset of its upload does not cancel the finished exchange, which retires
    // once both halves end.
    h.ready_edge(
        id,
        StreamReady::Reset {
            code: app(H3ErrorCode::NO_ERROR.as_u64()),
            final_size: 1000,
        },
    );
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1)]);
    h.ready_edge(id, StreamReady::Closed);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
}

#[test]
fn peer_reset_before_the_early_response_cleanup_ends_the_body() {
    end_the_body_at_a_reset_before_the_early_response_cleanup(Harness::ready());
    end_the_body_at_a_reset_before_the_early_response_cleanup(Harness::skipping(Plan::default()));
}

fn end_the_body_at_a_reset_before_the_early_response_cleanup<A: OwnedApplication>(
    mut h: Harness<A>,
) {
    let id = client_bidi(0);
    let request = [headers_frame(POST), data_frame(&[1; 100])].concat();
    h.receive(id, &request, false);
    h.pull_all(id, 1 << 20);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1)]);

    // The peer abandons its upload before the session's next callback.
    h.ready_edge(
        id,
        StreamReady::Reset {
            code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
            final_size: 1000,
        },
    );
    h.poll_session();
    let mut reads = Vec::new();
    loop {
        match h.read_body(id, 1 << 20) {
            Poll::Ready(Ok(event)) => reads.push(format!("body eom={}", event.eom())),
            Poll::Ready(Err(error)) => {
                reads.push(format!("error {:?}", error.code()));
                break;
            }
            Poll::Pending => panic!("the body read waits after the peer's reset: {reads:?}"),
        }
        assert!(reads.len() < 3, "{reads:?}");
    }
    assert_eq!(
        h.conn.get(id).stop_sending,
        None,
        "the peer ended the upload"
    );
    h.ready_edge(id, StreamReady::Closed);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
}

// ---------------------------------------------------------------------------------------------
// Lifecycle.

fn large_plan() -> Plan {
    Plan {
        body: Bytes::from(vec![0; 100_000]),
        ..Plan::default()
    }
}

#[test]
fn peer_reset_mid_response_cancels() {
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), large_plan());
    let id = client_bidi(0);
    h.receive(id, &headers_frame(POST), false);
    assert!(h.pull(id, 10_000).0.is_ready());
    h.ready_edge(
        id,
        StreamReady::Reset {
            code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
            final_size: 50,
        },
    );
    assert_eq!(
        h.log()[2..],
        [Ev::Cancel {
            id: 1,
            code: HttpErrorCode::CANCEL,
            peer: true
        }]
    );
    assert_eq!(
        h.conn.get(id).reset,
        Some(H3ErrorCode::REQUEST_CANCELLED.as_u64())
    );
    assert!(h.pull(id, 10_000).0.is_pending());
    assert!(matches!(h.read_body(id, 100), Poll::Ready(Err(_))));

    // The peer's code selects the cancellation cause.
    let next = client_bidi(1);
    h.receive(next, &headers_frame(POST), false);
    h.ready_edge(
        next,
        StreamReady::Reset {
            code: app(H3ErrorCode::INTERNAL_ERROR.as_u64()),
            final_size: 0,
        },
    );
    assert_eq!(
        h.log().last(),
        Some(&Ev::Cancel {
            id: 5,
            code: HttpErrorCode::INTERNAL_ERROR,
            peer: true
        })
    );
}

#[test]
fn peer_stop_sending_mid_response_cancels() {
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), large_plan());
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);
    assert!(h.pull(id, 10_000).0.is_ready());
    h.ready_edge(
        id,
        StreamReady::Stopped {
            code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
        },
    );
    assert_eq!(
        h.log()[2..],
        [Ev::Cancel {
            id: 1,
            code: HttpErrorCode::CANCEL,
            peer: true
        }]
    );
    // The transport has reset the send half itself; the request had already ended.
    assert_eq!(h.conn.get(id).reset, None);
    assert_eq!(h.conn.get(id).stop_sending, None);
    assert!(h.pull(id, 10_000).0.is_pending());

    // A request still uploading is stopped too.
    let next = client_bidi(1);
    h.receive(next, &headers_frame(POST), false);
    assert!(h.pull(next, 10_000).0.is_ready());
    h.ready_edge(next, StreamReady::Stopped { code: app(0x99) });
    assert_eq!(
        h.log().last(),
        Some(&Ev::Cancel {
            id: 5,
            code: HttpErrorCode::CANCEL,
            peer: true
        })
    );
    assert_eq!(
        h.conn.get(next).stop_sending,
        Some(H3ErrorCode::REQUEST_CANCELLED.as_u64())
    );
}

#[test]
fn peer_stop_sending_after_the_upload_returns_its_credit() {
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), large_plan());
    let id = client_bidi(0);
    let request = [headers_frame(POST), data_frame(&[1; 300])].concat();
    h.receive(id, &request, true);
    assert!(h.pull(id, 10_000).0.is_ready());
    h.ready_edge(
        id,
        StreamReady::Stopped {
            code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
        },
    );
    assert_eq!(
        h.log()[2..],
        [Ev::Cancel {
            id: 1,
            code: HttpErrorCode::CANCEL,
            peer: true
        }]
    );
    // The cancelled body is not read, so its bytes are released and the stream can end.
    assert_eq!(h.conn.get(id).consumed, request.len() as u64);
    assert_eq!(h.conn.get(id).stop_sending, None, "the upload had ended");
}

#[test]
fn quiesce_sends_goaway_and_closes_after_the_last_exchange() {
    let gate = Gate::default();
    let plan = Plan {
        head_gate: Some(gate.clone()),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    h.receive(client_bidi(0), &headers_frame(GET), true);
    h.receive(client_bidi(1), &headers_frame(GET), true);
    h.session.core.on_quiesce(&mut h.conn);
    let (_, frames) = h.control();
    assert_eq!(
        frames.last(),
        Some(&Frame::Goaway(8)),
        "the first stream not accepted"
    );
    assert!(h.conn.shutdown);
    assert_eq!(h.closed(), None);

    // Later request streams are rejected.
    let late = client_bidi(2);
    h.receive(late, &headers_frame(GET), true);
    let rejected = Some(H3ErrorCode::REQUEST_REJECTED.as_u64());
    assert_eq!(h.conn.get(late).reset, rejected);
    assert_eq!(h.conn.get(late).stop_sending, rejected);
    assert!(!h.log().contains(&Ev::New(9)));

    // The connection closes with H3_NO_ERROR once the last request stream is done: a staged
    // response may still be unsent or unacknowledged, and CONNECTION_CLOSE would cut it off.
    gate.open();
    h.pull_all(client_bidi(0), 1 << 20);
    h.poll_session();
    h.pull_all(client_bidi(1), 1 << 20);
    h.poll_session();
    assert_eq!(h.closed(), None, "the responses are staged, not delivered");
    h.ready_edge(client_bidi(0), StreamReady::Closed);
    assert_eq!(h.closed(), None);
    assert_eq!(h.session.deadline(), None, "an exchange is left");
    h.ready_edge(client_bidi(1), StreamReady::Closed);
    assert_eq!(
        h.log()
            .iter()
            .filter(|ev| matches!(ev, Ev::Complete(_)))
            .count(),
        2
    );
    assert_eq!(h.closed(), None, "the GOAWAY has not had its time");
    h.conn.now = h
        .session
        .deadline()
        .expect("the close waits for the GOAWAY");
    h.poll_session();
    assert_eq!(h.closed(), Some(H3ErrorCode::NO_ERROR.as_u64()));

    // Once the GOAWAY has had its time, the end of the last request stream closes at once.
    let mut h = Harness::ready();
    h.receive(client_bidi(0), &headers_frame(GET), true);
    h.session.core.on_quiesce(&mut h.conn);
    h.pull_all(client_bidi(0), 1 << 20);
    h.conn.now += Duration::from_secs(60);
    h.poll_session();
    assert_eq!(h.closed(), None);
    h.ready_edge(client_bidi(0), StreamReady::Closed);
    assert_eq!(h.closed(), Some(H3ErrorCode::NO_ERROR.as_u64()));
}

#[test]
fn idle_quiesce_closes_after_the_goaway_can_leave() {
    // `close` enters the closing period at once, in which RFC 9000 lets only CONNECTION_CLOSE go
    // out, so closing in the callback that staged GOAWAY would drop the GOAWAY.
    let mut h = Harness::ready();
    h.session.core.on_quiesce(&mut h.conn);
    assert_eq!(h.control().1.last(), Some(&Frame::Goaway(0)));
    assert_eq!(h.closed(), None, "the GOAWAY is staged, not sent");
    let deadline = h.session.deadline().expect("the session asks to be polled");
    h.poll_session();
    assert_eq!(h.closed(), None, "before the deadline");
    h.conn.now = deadline;
    h.poll_session();
    assert_eq!(h.closed(), Some(H3ErrorCode::NO_ERROR.as_u64()));
    assert_eq!(h.session.deadline(), None);
}

#[test]
fn connection_close_cancels_exchanges() {
    let plan = Plan {
        head_gate: Some(Gate::default()),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan.clone());
    h.receive(client_bidi(0), &headers_frame(GET), true);
    h.receive(client_bidi(1), &headers_frame(POST), false);
    h.event(ConnEvent::Closed {
        reason: mvfstr_proto::CloseReason::Silent,
        initiator: mvfstr_proto::Initiator::Peer,
    });
    let cancels: Vec<_> = h
        .log()
        .into_iter()
        .filter(|ev| matches!(ev, Ev::Cancel { .. }))
        .collect();
    assert_eq!(
        cancels,
        [1, 5].map(|id| Ev::Cancel {
            id,
            code: HttpErrorCode::TRANSPORT_EOF,
            peer: true
        })
    );

    // A connection error cancels the exchanges as a local cause.
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    h.receive(client_bidi(0), &headers_frame(GET), true);
    h.receive(client_uni(0), &[], true);
    assert_eq!(
        h.closed(),
        Some(H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64())
    );
    assert_eq!(
        h.log().last(),
        Some(&Ev::Cancel {
            id: 1,
            code: HttpErrorCode::PROTOCOL_ERROR,
            peer: false
        })
    );
}

#[test]
fn connection_close_cause_follows_its_initiator() {
    use mvfstr_proto::Initiator;
    let cases = [
        (Initiator::Local, HttpErrorCode::CANCEL, false),
        (Initiator::Internal, HttpErrorCode::INTERNAL_ERROR, false),
        (Initiator::IdleTimeout, HttpErrorCode::TRANSPORT_EOF, true),
        (
            Initiator::StatelessReset,
            HttpErrorCode::TRANSPORT_EOF,
            true,
        ),
    ];
    for (initiator, code, peer) in cases {
        let plan = Plan {
            head_gate: Some(Gate::default()),
            ..Plan::default()
        };
        let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
        h.receive(client_bidi(0), &headers_frame(GET), true);
        h.event(ConnEvent::Closed {
            reason: mvfstr_proto::CloseReason::Silent,
            initiator,
        });
        assert_eq!(
            h.log().last(),
            Some(&Ev::Cancel { id: 1, code, peer }),
            "{initiator:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// QPACK, unknown types and framing.

#[test]
fn dynamic_table_reference_is_decompression_failure() {
    // A Required Insert Count above 0, and an indexed line in the dynamic table.
    for block in [vec![0x01, 0x00, 0xd1], vec![0x00, 0x00, 0x80]] {
        let mut h = Harness::ready();
        let id = client_bidi(0);
        h.receive(id, &frame(FrameType::HEADERS, &block), false);
        assert_eq!(h.closed(), Some(roxygen_qpack::QPACK_DECOMPRESSION_FAILED));
        assert_eq!(
            h.log(),
            vec![
                Ev::New(1),
                Ev::Cancel {
                    id: 1,
                    code: HttpErrorCode::COMPRESSION_ERROR,
                    peer: false
                }
            ]
        );
    }
}

/// Returns the client's encoder stream type and `instructions`.
fn encoder_stream(instructions: &[EncoderInstruction<'_>]) -> Vec<u8> {
    let mut out = vec![StreamType::QPACK_ENCODER.as_u64() as u8];
    for instruction in instructions {
        instruction.encode(&mut out);
    }
    out
}

/// An insert of `value` under the name of static entry `index`.
fn static_name_insert(index: u64, value: &[u8]) -> EncoderInstruction<'_> {
    EncoderInstruction::InsertWithNameReference {
        static_table: true,
        name_index: index,
        value: StringLiteral {
            huffman: false,
            data: value,
        },
    }
}

#[test]
fn a_section_that_would_block_closes_the_connection() {
    let mut h = Harness::ready();
    h.receive(
        client_uni(1),
        &encoder_stream(&[
            EncoderInstruction::SetDynamicTableCapacity(4096),
            static_name_insert(0, b"example.com"),
        ]),
        false,
    );
    assert_eq!(h.closed(), None);
    // Required Insert Count 1 and Base 1: `:method GET`, `:scheme https`, the entry by relative
    // index 0 and `:path /`. The entry is received but not yet acknowledged.
    h.receive(
        client_bidi(0),
        &frame(FrameType::HEADERS, &[0x02, 0x00, 0xd1, 0xd7, 0x80, 0xc1]),
        true,
    );
    assert_eq!(h.closed(), None);
    let mut literal = Harness::ready();
    literal.receive(
        client_bidi(0),
        &headers_frame(&[
            (":method", "GET"),
            (":scheme", "https"),
            (":authority", "example.com"),
            (":path", "/"),
        ]),
        true,
    );
    assert_eq!(
        received_head(&h, client_bidi(0)),
        received_head(&literal, client_bidi(0))
    );

    // Required Insert Count 2 needs an entry that has not arrived.
    h.receive(
        client_bidi(1),
        &frame(FrameType::HEADERS, &[0x03, 0x00, 0xd1, 0xd7, 0x81, 0xc1]),
        true,
    );
    assert_eq!(h.closed(), Some(roxygen_qpack::QPACK_DECOMPRESSION_FAILED));
}

/// h2load's request as nghttp3 sends it once both of its entries are acknowledged: Required
/// Insert Count 2 and Base 2, `:path` as a literal under its static name, `:scheme https`,
/// `:authority` by relative index 1, `:method GET` and `user-agent` by relative index 0.
fn h2load_dynamic_request() -> Vec<u8> {
    let mut section = vec![0x03, 0x00, 0x51, 18];
    section.extend_from_slice(b"/baseline2?a=1&b=1");
    section.extend_from_slice(&[0xd7, 0x81, 0xd1, 0x80]);
    frame(FrameType::HEADERS, &section)
}

const H2LOAD_REQUEST: &[(&str, &str)] = &[
    (":path", "/baseline2?a=1&b=1"),
    (":scheme", "https"),
    (":authority", "localhost:8443"),
    (":method", "GET"),
    ("user-agent", "h2load nghttp2/1.69.0"),
];

/// Pulls the decoder stream until it is idle, and returns the pushes it made.
fn pull_decoder<A: OwnedApplication>(h: &mut Harness<A>) -> (Vec<Poll<()>>, usize) {
    let mut polls = Vec::new();
    let mut pushes = 0;
    loop {
        let (poll, sink) = h.pull(server_uni(1), 16 << 10);
        polls.push(poll);
        pushes += sink.pushes;
        if poll.is_pending() || polls.len() > 8 {
            return (polls, pushes);
        }
    }
}

#[test]
fn an_nghttp3_shaped_connection_gets_one_decoder_write_per_phase() {
    let fixed = Plan {
        fixed: true,
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), fixed.clone());
    let decoder = server_uni(1);
    h.conn.take_stream_wakes();
    h.receive(
        client_uni(1),
        &encoder_stream(&[
            EncoderInstruction::SetDynamicTableCapacity(4096),
            static_name_insert(0, b"localhost:8443"),
            static_name_insert(95, b"h2load nghttp2/1.69.0"),
        ]),
        false,
    );
    assert_eq!(h.conn.take_stream_wakes(), vec![decoder]);
    let control_order = h.conn.get(server_uni(0)).order;
    assert_eq!(h.conn.get(decoder).order, control_order);
    // A push that sends every due byte returns Pending, so the stream waits for its waker.
    assert_eq!(pull_decoder(&mut h), (vec![Poll::Pending], 1));
    // The stream type, then an Insert Count Increment of 2.
    assert_eq!(h.conn.get(decoder).sent, [0x03, 0x02]);

    let mut literal = Harness::ready_with(H3ApplicationBuilder::new(), fixed);
    for index in 0..64 {
        h.receive(client_bidi(index), &h2load_dynamic_request(), true);
        literal.receive(client_bidi(index), &headers_frame(H2LOAD_REQUEST), true);
    }
    assert_eq!(h.closed(), None);
    assert_eq!(h.conn.take_stream_wakes(), vec![decoder]);
    assert_eq!(pull_decoder(&mut h), (vec![Poll::Pending], 1));
    // A Section Acknowledgment per request, in stream order: stream IDs 0 to 124 fit the 7-bit
    // prefix, and 128 to 252 continue in one more byte.
    let mut acks: Vec<u8> = (0..32).map(|index| 0x80 | (index * 4)).collect();
    for index in 32..64u8 {
        acks.extend([0xff, index * 4 - 127]);
    }
    assert_eq!(acks[..3], [0x80, 0x84, 0x88]);
    assert_eq!(acks[acks.len() - 2..], [0xff, 0x7d]);
    assert_eq!(h.conn.get(decoder).sent[2..], acks);
    assert_eq!(h.conn.stream_wakers.get(), 1);
    for index in 0..64 {
        assert_eq!(
            received_head(&h, client_bidi(index)),
            received_head(&literal, client_bidi(index))
        );
    }
}

#[test]
fn the_decoder_stream_is_a_critical_stream() {
    let inserts = encoder_stream(&[
        EncoderInstruction::SetDynamicTableCapacity(4096),
        static_name_insert(0, b"localhost:8443"),
        static_name_insert(95, b"h2load nghttp2/1.69.0"),
    ]);
    let decoder = server_uni(1);
    let edges = [
        StreamReady::Stopped {
            code: app(H3ErrorCode::NO_ERROR.as_u64()),
        },
        StreamReady::Reset {
            code: app(H3ErrorCode::NO_ERROR.as_u64()),
            final_size: 0,
        },
        StreamReady::Closed,
    ];
    for edge in edges {
        let mut h = Harness::ready();
        h.receive(client_uni(1), &inserts, false);
        assert!(h.conn.streams.contains_key(&decoder));
        h.ready_edge(decoder, edge);
        assert_eq!(
            h.closed(),
            Some(H3ErrorCode::CLOSED_CRITICAL_STREAM.as_u64()),
            "{edge:?}"
        );
    }

    // At the peer's stream limit, opening waits for more streams.
    let mut h = Harness::ready();
    h.conn.uni_limit = 1;
    h.receive(client_uni(1), &inserts, false);
    h.poll_session();
    h.poll_session();
    assert!(!h.conn.streams.contains_key(&decoder));
    assert_eq!(h.conn.open_refusals, 1);
    h.conn.uni_limit = 2;
    h.event(ConnEvent::StreamsAvailable { dir: Dir::Uni });
    assert_eq!(h.conn.take_stream_wakes(), vec![decoder]);
    assert_eq!(pull_decoder(&mut h), (vec![Poll::Pending], 1));
    assert_eq!(h.conn.get(decoder).sent, [0x03, 0x02]);

    // A request reset before its head is cancelled on the decoder stream, only under a nonzero
    // maximum.
    for (builder, sent) in [
        (H3ApplicationBuilder::new(), Some(vec![0x03, 0x48])),
        (
            H3ApplicationBuilder::new().qpack_max_table_capacity(0),
            None,
        ),
    ] {
        let mut h = Harness::ready_with(builder, Plan::default());
        let id = client_bidi(2);
        h.receive(id, &[FrameType::HEADERS.as_u64() as u8], false);
        h.ready_edge(
            id,
            StreamReady::Reset {
                code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
                final_size: 1,
            },
        );
        pull_decoder(&mut h);
        assert_eq!(
            h.conn
                .streams
                .get(&decoder)
                .map(|stream| stream.sent.clone()),
            sent
        );
    }

    // A peer that withholds the decoder stream's credit while it keeps sending sections. From
    // stream ID 16,512 on, each Section Acknowledgment takes 4 bytes, so with the unsent stream
    // type and Insert Count Increment, the 16,384th acknowledgment passes 64 KiB.
    let mut h = Harness::ready_with(
        H3ApplicationBuilder::new(),
        Plan {
            fixed: true,
            ..Plan::default()
        },
    );
    h.receive(client_uni(1), &inserts, false);
    let first = 4128;
    let mut index = first;
    while h.closed().is_none() {
        assert!(index < first + 20_000, "the connection stays open");
        let id = client_bidi(index);
        h.conn.now += Duration::from_micros(1);
        h.receive(id, &h2load_dynamic_request(), true);
        h.ready_edge(id, StreamReady::Closed);
        h.conn.streams.remove(&id);
        index += 1;
    }
    assert_eq!(h.closed(), Some(H3ErrorCode::EXCESSIVE_LOAD.as_u64()));
    assert_eq!(index - first, 16_384);
    assert!(h.conn.get(decoder).sent.is_empty());
}

#[test]
fn decoder_stream_bytes_an_allowance_cuts_short_lead_the_next_push() {
    let mut h = Harness::ready();
    let decoder = server_uni(1);
    h.receive(
        client_uni(1),
        &encoder_stream(&[
            EncoderInstruction::SetDynamicTableCapacity(4096),
            static_name_insert(0, b"localhost:8443"),
            static_name_insert(95, b"h2load nghttp2/1.69.0"),
        ]),
        false,
    );
    // A 1-byte allowance takes the stream type, and the Insert Count Increment of 2 waits.
    let (poll, sink) = h.pull(decoder, 1);
    assert_eq!(
        (poll, sink.pushes, &sink.pushed[..]),
        (Poll::Ready(()), 1, &[0x03][..])
    );

    // Two requests add Section Acknowledgments while the stream is still ready, so its waker
    // does not fire. A 2-byte allowance takes the increment and the first acknowledgment.
    h.conn.take_stream_wakes();
    for index in 0..2 {
        h.receive(client_bidi(index), &h2load_dynamic_request(), true);
    }
    assert!(!h.conn.take_stream_wakes().contains(&decoder));
    let (poll, sink) = h.pull(decoder, 2);
    assert_eq!(
        (poll, sink.pushes, &sink.pushed[..]),
        (Poll::Ready(()), 1, &[0x02, 0x80][..])
    );

    // A third request: one push carries the rest of the earlier bytes and then its own
    // acknowledgment.
    h.receive(client_bidi(2), &h2load_dynamic_request(), true);
    let (poll, sink) = h.pull(decoder, 16 << 10);
    assert_eq!(
        (poll, sink.pushes, &sink.pushed[..]),
        (Poll::Pending, 1, &[0x84, 0x88][..])
    );
    assert_eq!(h.conn.get(decoder).sent, [0x03, 0x02, 0x80, 0x84, 0x88]);
}

#[test]
fn request_trailers_that_reference_the_dynamic_table_are_acknowledged() {
    let mut h = Harness::ready();
    let decoder = server_uni(1);
    let raw = |data| StringLiteral {
        huffman: false,
        data,
    };
    h.receive(
        client_uni(1),
        &encoder_stream(&[
            EncoderInstruction::SetDynamicTableCapacity(4096),
            EncoderInstruction::InsertWithLiteralName {
                name: raw(b"x-request-trailer"),
                value: raw(b"1"),
            },
        ]),
        false,
    );
    pull_decoder(&mut h);
    assert_eq!(h.conn.get(decoder).sent, [0x03, 0x01]);

    // Trailers with Required Insert Count 1 (encoded 2) and Base 1: the entry by relative
    // index 0.
    let id = client_bidi(3);
    let mut request = headers_frame(POST);
    request.extend_from_slice(&data_frame(b"body"));
    request.extend_from_slice(&frame(FrameType::HEADERS, &[0x02, 0x00, 0x80]));
    h.receive(id, &request, true);
    assert_eq!(h.closed(), None);
    assert_eq!(read_chunk(&mut h, id), (b"body".to_vec(), false));
    let Poll::Ready(Ok(event)) = h.read_body(id, 100) else {
        panic!("the trailers are ready");
    };
    assert!(event.eom());
    let HttpBodyEventKind::Trailers(trailers) = event.into_kind() else {
        panic!("expected trailers");
    };
    assert_eq!(trailers.get_single_or_empty("x-request-trailer"), b"1");

    // The trailer section is acknowledged for its request stream, ID 12.
    pull_decoder(&mut h);
    assert_eq!(h.conn.get(decoder).sent, [0x03, 0x01, 0x8c]);
}

#[test]
fn request_streams_abandoned_while_ingress_is_open_are_cancelled_on_the_decoder_stream() {
    /// Returns the instructions on the decoder stream after `abandon`, if the stream opened.
    fn cancellations(
        builder: H3ApplicationBuilder,
        abandon: impl FnOnce(&mut Harness),
    ) -> Option<Vec<u8>> {
        let mut h = Harness::ready_with(builder, Plan::default());
        abandon(&mut h);
        h.poll_session();
        pull_decoder(&mut h);
        let sent = &h.conn.streams.get(&server_uni(1))?.sent;
        assert_eq!(sent[0], 0x03);
        Some(sent[1..].to_vec())
    }
    let id = client_bidi(0);
    let upload = |h: &mut Harness| h.receive(id, &headers_frame(POST), false);
    let whole = |h: &mut Harness| h.receive(id, &headers_frame(GET), true);
    let reset = |h: &mut Harness| {
        h.ready_edge(
            id,
            StreamReady::Reset {
                code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
                final_size: 100,
            },
        );
    };
    let stopped = |h: &mut Harness| {
        h.ready_edge(
            id,
            StreamReady::Stopped {
                code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
            },
        );
    };
    let new = H3ApplicationBuilder::new;
    let actual = vec![
        (
            "peer reset after the response",
            cancellations(new(), |h| {
                upload(h);
                h.pull_all(id, 1 << 20);
                reset(h);
            }),
        ),
        (
            "early response",
            cancellations(new(), |h| {
                upload(h);
                h.pull_all(id, 1 << 20);
            }),
        ),
        (
            "malformed head",
            cancellations(new(), |h| {
                h.receive(
                    id,
                    &headers_frame(&[(":scheme", "https"), (":authority", "a"), (":path", "/")]),
                    false,
                );
            }),
        ),
        (
            "peer stopped the response",
            cancellations(new(), |h| {
                upload(h);
                stopped(h);
            }),
        ),
        (
            "rejected",
            cancellations(new().max_concurrent_exchanges(1), |h| {
                upload(h);
                h.receive(client_bidi(1), &headers_frame(GET), true);
            }),
        ),
        (
            "peer reset after the whole request",
            cancellations(new(), |h| {
                whole(h);
                reset(h);
            }),
        ),
        (
            "peer stopped after the whole request",
            cancellations(new(), |h| {
                whole(h);
                stopped(h);
            }),
        ),
        (
            "early response under a zero maximum",
            cancellations(new().qpack_max_table_capacity(0), |h| {
                upload(h);
                h.pull_all(id, 1 << 20);
            }),
        ),
    ];
    let expected = vec![
        ("peer reset after the response", Some(vec![0x40])),
        ("early response", Some(vec![0x40])),
        ("malformed head", Some(vec![0x40])),
        ("peer stopped the response", Some(vec![0x40])),
        ("rejected", Some(vec![0x44])),
        ("peer reset after the whole request", None),
        ("peer stopped after the whole request", None),
        ("early response under a zero maximum", None),
    ];
    assert_eq!(actual, expected);
}

#[test]
fn unknown_frames_and_stream_types_are_ignored() {
    let mut h = Harness::ready();
    // PRIORITY_UPDATE (RFC 9218) is skipped as an unknown frame on the control stream.
    h.receive(
        client_uni(0),
        &frame(FrameType::new(0xf0700).unwrap(), b"\x00u=1"),
        false,
    );
    // A reserved and an unknown stream type are stopped and drained.
    for (index, stream_type) in [(1, StreamType::grease(3).as_u64()), (2, 0x54)] {
        let id = client_uni(index);
        let mut bytes = BytesMut::new();
        UniStreamHeader::Unknown(StreamType::new(stream_type).unwrap())
            .encode(&mut bytes)
            .unwrap();
        bytes.extend_from_slice(b"ignored");
        h.receive(id, &bytes, false);
        assert_eq!(
            h.conn.get(id).stop_sending,
            Some(H3ErrorCode::STREAM_CREATION_ERROR.as_u64())
        );
        assert!(h.conn.get(id).readable.is_empty());
        h.receive(id, b"more", true);
        assert!(h.conn.get(id).readable.is_empty());
    }
    // Reserved frame types around a request's frames are skipped.
    let id = client_bidi(0);
    let grease = |n| frame(FrameType::grease(n), b"grease");
    let request = [
        grease(1),
        headers_frame(POST),
        grease(2),
        data_frame(b"abc"),
        frame(FrameType::new(0x2a).unwrap(), b""),
        data_frame(b"def"),
        grease(3),
    ]
    .concat();
    h.receive(id, &request, true);
    assert_eq!(h.closed(), None);
    assert_eq!(read_all(&mut h, id), (b"abcdef".to_vec(), None));
    h.poll_session();
    assert_eq!(h.conn.get(id).consumed, request.len() as u64);
    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), b"hello");
}

#[test]
fn frame_legality_follows_the_codec() {
    let settings = {
        let mut out = BytesMut::new();
        Frame::Settings(Settings::new()).encode(&mut out).unwrap();
        out.to_vec()
    };
    let cases: Vec<(&str, Vec<u8>, bool, u64)> = vec![
        (
            "DATA before HEADERS",
            data_frame(b"x"),
            false,
            H3ErrorCode::FRAME_UNEXPECTED.as_u64(),
        ),
        (
            "SETTINGS on a request stream",
            settings,
            false,
            H3ErrorCode::FRAME_UNEXPECTED.as_u64(),
        ),
        (
            "HTTP/2 frame type",
            // PRIORITY (0x02), with an empty payload.
            vec![0x02, 0x00],
            false,
            H3ErrorCode::FRAME_UNEXPECTED.as_u64(),
        ),
        (
            "HEADERS after trailers",
            [
                headers_frame(POST),
                headers_frame(&[("x", "1")]),
                headers_frame(&[("y", "2")]),
            ]
            .concat(),
            false,
            H3ErrorCode::FRAME_UNEXPECTED.as_u64(),
        ),
        (
            "DATA after trailers",
            [
                headers_frame(POST),
                headers_frame(&[("x", "1")]),
                data_frame(b"z"),
            ]
            .concat(),
            false,
            H3ErrorCode::FRAME_UNEXPECTED.as_u64(),
        ),
        (
            "FIN inside a frame",
            [headers_frame(POST), data_frame(b"abc")[..3].to_vec()].concat(),
            true,
            H3ErrorCode::FRAME_ERROR.as_u64(),
        ),
    ];
    for (name, bytes, fin, code) in cases {
        let mut h = Harness::ready();
        h.receive(client_bidi(0), &bytes, fin);
        assert_eq!(h.closed(), Some(code), "{name}");
    }

    // A FIN before the request head is a stream error.
    let mut h = Harness::ready();
    let id = client_bidi(0);
    h.receive(id, &[], true);
    assert_eq!(
        h.conn.get(id).reset,
        Some(H3ErrorCode::REQUEST_INCOMPLETE.as_u64())
    );
    assert_eq!(h.closed(), None);
}

/// Reads the application's request body to its end: the data and any trailers.
fn read_all<A: OwnedApplication>(
    h: &mut Harness<A>,
    id: StreamId,
) -> (Vec<u8>, Option<Vec<(String, String)>>) {
    let mut data = Vec::new();
    loop {
        let Poll::Ready(Ok(event)) = h.read_body(id, 1 << 20) else {
            panic!("the body is readable to its end");
        };
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(body) => {
                data.extend(body.segments().flat_map(|segment| segment.to_vec()));
            }
            HttpBodyEventKind::Trailers(trailers) => {
                let mut fields = Vec::new();
                trailers.for_each(|name, value| {
                    fields.push((
                        String::from_utf8(name.to_vec()).unwrap(),
                        String::from_utf8(value.to_vec()).unwrap(),
                    ));
                });
                return (data, Some(fields));
            }
            _ => panic!("unexpected body event"),
        }
        if eom {
            return (data, None);
        }
        h.poll_session();
    }
}

#[test]
fn request_split_at_every_byte_boundary() {
    let request = [
        headers_frame(
            &[
                POST,
                &[
                    ("content-length", "11"),
                    ("cookie", "a=1"),
                    ("cookie", "b=2"),
                ],
            ]
            .concat(),
        ),
        frame(FrameType::grease(7), b"pad"),
        data_frame(b"hello "),
        data_frame(b"world"),
        headers_frame(&[("x-trailer", "t")]),
    ]
    .concat();
    let check = |h: &mut Harness, id: StreamId, split: &str| {
        assert_eq!(h.closed(), None, "{split}");
        assert_eq!(
            h.log()[..2],
            [Ev::New(1), Ev::Request { id: 1, eom: false }],
            "{split}"
        );
        let (method, url, fields) = received_head(h, id);
        assert_eq!(
            (method.as_str(), url.as_str()),
            ("POST", "/upload"),
            "{split}"
        );
        assert_eq!(field(&fields, "Cookie"), Some("a=1; b=2"), "{split}");
        assert_eq!(
            read_all(h, id),
            (
                b"hello world".to_vec(),
                Some(vec![("x-trailer".to_owned(), "t".to_owned())])
            ),
            "{split}"
        );
        h.poll_session();
        assert_eq!(h.conn.get(id).consumed, request.len() as u64, "{split}");
    };
    let id = client_bidi(0);
    for at in 1..request.len() {
        let mut h = Harness::ready();
        h.receive(id, &request[..at], false);
        h.receive(id, &request[at..], true);
        check(&mut h, id, &format!("split at {at}"));
    }
    // One byte at a time.
    let mut h = Harness::ready();
    for byte in &request {
        h.receive(id, std::slice::from_ref(byte), false);
    }
    h.receive(id, &[], true);
    check(&mut h, id, "byte by byte");
}

// ---------------------------------------------------------------------------------------------
// Response scheduling and limits.

#[test]
fn suspend_parks_the_stream_until_resumed() {
    let gate = Gate::default();
    let plan = Plan {
        suspend: Some(gate.clone()),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);
    assert!(h.pull(id, 1 << 20).0.is_ready(), "the head goes out");
    assert!(h.pull(id, 1 << 20).0.is_pending(), "the body is suspended");
    assert!(!h.conn.get(id).finished);
    h.conn.take_stream_wakes();

    gate.open();
    assert_eq!(
        h.conn.take_stream_wakes(),
        vec![id],
        "resuming wakes the stream"
    );
    assert!(h.pull(id, 1 << 20).0.is_ready());
    assert_eq!(body_of(&h.response(id)), b"hello");
    assert!(h.conn.get(id).finished);
}

#[test]
fn poll_without_progress_wakes_its_stream_again() {
    let plan = Plan {
        empty_events: 40,
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);
    assert!(h.pull(id, 1 << 20).0.is_ready(), "the head goes out");
    h.conn.take_stream_wakes();

    // A call that meets only empty events returns without progress and wakes its stream again,
    // since the stream would otherwise wait for a wake that was never registered.
    assert!(h.pull(id, 1 << 20).0.is_pending());
    assert_eq!(h.conn.take_stream_wakes(), vec![id]);
    for _ in 0..4 {
        let _ = h.pull(id, 1 << 20);
    }
    assert!(h.conn.get(id).finished);
    assert_eq!(body_of(&h.response(id)), b"hello");
}

#[test]
fn priority_header_sets_the_stream_order() {
    let mut h = Harness::ready();
    let urgent = client_bidi(0);
    h.receive(
        urgent,
        &headers_frame(&[GET, &[("priority", "u=1, i")]].concat()),
        true,
    );
    let order = h
        .conn
        .get(urgent)
        .order
        .expect("the order is set at delivery");
    assert_eq!(
        (order.class, order.urgency, order.incremental),
        (1, 1, true)
    );

    let plain = client_bidi(1);
    h.receive(plain, &headers_frame(GET), true);
    let order = h.conn.get(plain).order.unwrap();
    assert_eq!(
        (order.class, order.urgency, order.incremental),
        (1, 3, false)
    );
}

#[test]
fn priority_header_follows_rfc9218() {
    // (field lines, urgency, incremental)
    let cases: [(&[&str], u8, bool); 10] = [
        (&["i=?1, u=1"], 1, true),
        (&["i, u=2"], 2, true),
        // Out-of-range values and values of another type are ignored, not the whole field.
        (&["u=9, i"], 3, true),
        (&["u=1, i=1"], 1, false),
        (&["u=a, i=?1"], 3, true),
        // Parameters and unknown members, including inner lists, are ignored.
        (&["u=1;x=2, i;y, z=(a \"b\" 3), w=:AAEC:"], 1, true),
        // The last of duplicate members counts, across field lines too.
        (&["u=1, u=6"], 6, false),
        (&["u=1", "u=5, i"], 5, true),
        // A value that does not parse leaves the defaults.
        (&["u=1, i,"], 3, false),
        (&["u=1 i"], 3, false),
    ];
    for (lines, urgency, incremental) in cases {
        let mut h = Harness::ready();
        let id = client_bidi(0);
        let fields: Vec<(&str, &str)> = GET
            .iter()
            .copied()
            .chain(lines.iter().map(|line| ("priority", *line)))
            .collect();
        h.receive(id, &headers_frame(&fields), true);
        let order = h.conn.get(id).order.expect("the order is set at delivery");
        assert_eq!(
            (order.class, order.urgency, order.incremental),
            (1, urgency, incremental),
            "{lines:?}"
        );
    }
}

#[test]
fn informational_response_precedes_the_final_head() {
    let plan = Plan {
        informational: true,
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);
    h.pull_all(id, 1 << 20);
    let parts = h.response(id);
    let [Part::Headers(early), Part::Headers(head), Part::Data(_)] = parts.as_slice() else {
        panic!("expected 103, 200 and the body: {parts:?}");
    };
    assert_eq!(field(early, ":status"), Some("103"));
    assert_eq!(field(head, ":status"), Some("200"));
}

#[test]
fn concurrent_exchange_limit_rejects_streams() {
    let gate = Gate::default();
    let plan = Plan {
        head_gate: Some(gate.clone()),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(
        H3ApplicationBuilder::new().max_concurrent_exchanges(1),
        plan,
    );
    h.receive(client_bidi(0), &headers_frame(GET), true);
    h.receive(client_bidi(1), &headers_frame(GET), true);
    let rejected = Some(H3ErrorCode::REQUEST_REJECTED.as_u64());
    assert_eq!(h.conn.get(client_bidi(1)).reset, rejected);
    assert_eq!(h.conn.get(client_bidi(1)).stop_sending, rejected);
    assert!(!h.log().contains(&Ev::New(5)));

    gate.open();
    h.pull_all(client_bidi(0), 1 << 20);
    h.poll_session();
    h.receive(client_bidi(2), &headers_frame(GET), true);
    assert!(h.log().contains(&Ev::New(9)));
    assert_eq!(h.conn.get(client_bidi(2)).reset, None);
}

#[test]
fn builder_rejects_unusable_limits() {
    let build = |builder: H3ApplicationBuilder| {
        builder
            .build(Host::tokio(), |_| Ok(TestApp::default()))
            .err()
    };
    assert!(build(H3ApplicationBuilder::new().max_concurrent_exchanges(0)).is_some());
    assert!(build(H3ApplicationBuilder::new().request_body_buffer(0)).is_some());
    assert!(build(H3ApplicationBuilder::new().max_field_section_size(0)).is_some());
    let too_few_uni = mvfstr_proto::TransportConfig::builder()
        .max_streams_uni(2)
        .build()
        .unwrap();
    assert!(build(H3ApplicationBuilder::new().transport(too_few_uni)).is_some());
    assert!(build(H3ApplicationBuilder::new()).is_none());
}

#[test]
fn response_failure_resets_the_stream() {
    let plan = Plan {
        fail: true,
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(POST), false);
    assert!(h.pull(id, 1 << 20).0.is_pending());
    h.poll_session();
    // RFC 9114 uses H3_REQUEST_CANCELLED for a response abandoned after partial processing.
    let cancelled = Some(H3ErrorCode::REQUEST_CANCELLED.as_u64());
    assert_eq!(h.conn.get(id).reset, cancelled);
    assert_eq!(h.conn.get(id).stop_sending, cancelled);
    assert_eq!(
        h.log()[2..],
        [Ev::Cancel {
            id: 1,
            code: HttpErrorCode::INTERNAL_ERROR,
            peer: false
        }]
    );
    assert!(h.pull(id, 1 << 20).0.is_pending());
}

#[test]
fn tunnel_failure_resets_with_connect_error() {
    // In RFC 9114, an error in a tunnel's TCP connection is a stream error of type
    // H3_CONNECT_ERROR.
    let plan = Plan {
        body_failure: Some(HttpErrorCode::CONNECT_ERROR),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let reset_with = |h: &mut Harness, index: u64, request: &[(&str, &str)]| {
        let id = client_bidi(index);
        h.receive(id, &headers_frame(request), false);
        assert!(h.pull(id, 1 << 20).0.is_pending());
        h.poll_session();
        let stream = h.conn.get(id);
        assert_eq!(stream.reset, stream.stop_sending);
        stream.reset
    };
    let connect_error = Some(H3ErrorCode::CONNECT_ERROR.as_u64());
    let cancelled = Some(H3ErrorCode::REQUEST_CANCELLED.as_u64());
    assert_eq!(reset_with(&mut h, 0, CONNECT), connect_error);
    assert_eq!(
        h.log()[2..],
        [Ev::Cancel {
            id: 1,
            code: HttpErrorCode::CONNECT_ERROR,
            peer: false
        }]
    );

    // Only a tunnel has a TCP connection to report: other failures keep H3_REQUEST_CANCELLED.
    assert_eq!(reset_with(&mut h, 1, POST), cancelled);
    h.app.0.borrow_mut().plan.body_failure = Some(HttpErrorCode::INTERNAL_ERROR);
    assert_eq!(reset_with(&mut h, 2, CONNECT), cancelled);
}

#[test]
fn tiny_allowance_splits_frames_across_polls() {
    let plan = Plan {
        body: Bytes::from_static(b"a body longer than the allowance"),
        trailers: Some(vec![("x-done", "1")]),
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &headers_frame(GET), true);
    let mut calls = 0;
    while h.pull(id, [1, 2, 3][calls % 3]).0.is_ready() {
        calls += 1;
        assert!(calls < 1000);
    }
    assert!(h.conn.get(id).finished);
    let parts = h.response(id);
    assert_eq!(body_of(&parts), b"a body longer than the allowance");
    assert_eq!(
        parts.last(),
        Some(&Part::Headers(vec![("x-done".to_owned(), "1".to_owned())]))
    );
}

// ---------------------------------------------------------------------------------------------
// Fixed responses staged at delivery.

const FIXED_BODY: &[u8] = b"fixed response!";

/// A plan whose exchanges report that they hold their whole response, with `body`.
fn fixed(body: Bytes) -> Plan {
    Plan {
        fixed: true,
        body,
        ..Plan::default()
    }
}

/// Delivers `request` and its FIN to a new stream at the stream's Opened edge, as when they arrive
/// together.
fn open_with_fin<A: OwnedApplication>(h: &mut Harness<A>, id: StreamId, request: &[u8]) {
    h.conn.receive(id, request);
    h.conn.get(id).peer_finished = true;
    h.ready_edge(id, StreamReady::Opened);
}

/// Serves a GET with its FIN to an application that does not report a fixed response, through
/// the stream's wake and one `poll_send`, and returns what the stream staged and whether its FIN
/// is staged.
fn pulled(body: Bytes) -> (Vec<u8>, bool) {
    let plan = Plan {
        body,
        ..Plan::default()
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    open_with_fin(&mut h, id, &headers_frame(GET));
    assert_eq!(h.conn.take_stream_wakes(), [id]);
    assert!(h.pull(id, 1 << 20).0.is_ready());
    let stream = h.conn.get(id);
    (stream.sent.clone(), stream.finished)
}

#[test]
fn a_fixed_response_is_staged_while_its_request_is_delivered() {
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    open_with_fin(&mut h, id, &headers_frame(GET));
    let parts = h.response(id);
    let [Part::Headers(head), Part::Data(body)] = parts.as_slice() else {
        panic!("expected HEADERS and DATA when the Opened callback returns: {parts:?}");
    };
    assert_eq!(field(head, ":status"), Some("200"));
    assert_eq!(body, FIXED_BODY);
    assert!(h.conn.get(id).finished, "the FIN is staged");
    assert_eq!(h.conn.stream_wakers.get(), 0, "no stream waker is made");
    assert!(h.conn.take_stream_wakes().is_empty(), "no wake fires");
    assert_eq!(
        h.log(),
        [
            Ev::New(1),
            Ev::Request { id: 1, eom: true },
            Ev::ResponseComplete(1),
            Ev::Complete(1)
        ]
    );
}

#[test]
fn inline_and_pulled_responses_stage_the_same_bytes() {
    let body = Bytes::from_static(FIXED_BODY);
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), fixed(body.clone()));
    let id = client_bidi(0);
    open_with_fin(&mut h, id, &headers_frame(GET));
    let stream = h.conn.get(id);
    let inline = (stream.sent.clone(), stream.finished);
    let pulled = pulled(body);
    assert!(pulled.1, "the pulled response ends");
    assert_eq!(inline, pulled);
}

/// Counts the allocations of a fixed-response GET from its stream's Opened edge until its
/// exchange retires. A stream wake it fires is polled as the driver polls it, with a waker the
/// driver makes for the stream. The fake's buffers are reserved beforehand, so only the session,
/// the application and the wakers allocate.
#[test]
fn allocations_of_a_fixed_response_from_accept_to_retire() {
    // The shared header-name table is initialized outside the per-request count.
    let _ = HttpCommonHeaders::name(
        HttpHeaderCode::ContentType,
        HttpCommonHeaderTable::Lowercase,
    );
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.conn.receive(id, &headers_frame(GET));
    let stream = h.conn.get(id);
    stream.peer_finished = true;
    stream.sent.reserve(256);
    h.conn.stream_wakes.lock().unwrap().reserve(4);
    let mut sink = FakeSink::new(1 << 20);
    sink.pushed.reserve(256);
    sink.parts.reserve(4);
    let allocations = count_allocations(|| {
        h.ready_edge(id, StreamReady::Opened);
        for woken in h.conn.take_stream_wakes() {
            let waker = h.conn.stream_waker_for(woken);
            let mut cx = Context::from_waker(&waker);
            let _ = h.session.core.poll_send(&mut cx, woken, &mut sink);
        }
    });
    let stream = h.conn.get(id);
    stream.sent.extend_from_slice(&sink.pushed);
    stream.finished |= sink.finished;
    assert!(stream.finished);
    assert_eq!(body_of(&h.response(id)), FIXED_BODY);
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
    match allocations {
        Some(allocations) => {
            eprintln!("{allocations} allocations from accept to retire");
            // The wake path also makes the stream waker it fires and the waker the driver polls
            // with: 30 allocations.
            assert_eq!(allocations, 28);
        }
        None => eprintln!("the allocation count needs a statically linked std"),
    }
}

#[test]
fn a_fixed_body_over_sixteen_kib_is_woken() {
    let mut h = Harness::ready_with(
        H3ApplicationBuilder::new(),
        fixed(Bytes::from(vec![b'a'; 16_384])),
    );
    let at_cap = client_bidi(0);
    open_with_fin(&mut h, at_cap, &headers_frame(GET));
    assert!(h.conn.get(at_cap).finished, "a 16,384 B body is staged");
    assert_eq!(body_of(&h.response(at_cap)).len(), 16_384);
    assert!(h.conn.take_stream_wakes().is_empty());

    h.app.0.borrow_mut().plan.body = Bytes::from(vec![b'b'; 16_385]);
    let over = client_bidi(1);
    open_with_fin(&mut h, over, &headers_frame(GET));
    assert!(
        h.conn.get(over).sent.is_empty(),
        "nothing is staged at delivery"
    );
    assert_eq!(h.conn.take_stream_wakes(), [over]);
    assert_eq!(h.conn.stream_wakers.get(), 1);
    assert!(h.pull(over, 1 << 20).0.is_ready());
    assert!(h.conn.get(over).finished);
    assert_eq!(body_of(&h.response(over)), vec![b'b'; 16_385]);
}

#[test]
fn connect_and_handled_responses_keep_the_wake() {
    // A CONNECT whose exchange reports a fixed response, and an exchange that reports none.
    let cases: [(&str, bool, &[(&str, &str)], bool); 2] = [
        ("CONNECT", true, CONNECT, false),
        ("handled", false, GET, true),
    ];
    for (name, reports, request, fin) in cases {
        let plan = Plan {
            fixed: reports,
            ..Plan::default()
        };
        let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
        let id = client_bidi(0);
        h.receive(id, &headers_frame(request), fin);
        assert!(h.conn.get(id).sent.is_empty(), "{name}: nothing staged");
        assert_eq!(h.conn.take_stream_wakes(), [id], "{name}: one wake");
        assert_eq!(h.conn.stream_wakers.get(), 1, "{name}");
        assert!(h.pull(id, 1 << 20).0.is_ready(), "{name}");
        assert!(
            h.conn.get(id).finished,
            "{name}: one poll ends the response"
        );
        let parts = h.response(id);
        let [Part::Headers(head), Part::Data(body)] = parts.as_slice() else {
            panic!("{name}: expected HEADERS and DATA: {parts:?}");
        };
        assert_eq!(field(head, ":status"), Some("200"), "{name}");
        assert_eq!(body, b"hello", "{name}");
        assert!(h.conn.take_stream_wakes().is_empty(), "{name}");
    }
}

#[test]
fn a_stream_polled_before_delivery_is_woken_not_staged() {
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    h.receive(id, &[], false);
    assert!(h.pull(id, 1 << 20).0.is_pending(), "no request yet");
    h.receive(id, &headers_frame(GET), true);
    assert!(
        h.conn.get(id).sent.is_empty(),
        "nothing is staged at delivery"
    );
    assert_eq!(
        h.conn.take_stream_wakes(),
        [id],
        "the registered waker fires"
    );
    assert_eq!(h.conn.stream_wakers.get(), 0);
    assert!(h.pull(id, 1 << 20).0.is_ready());
    assert!(h.conn.get(id).finished);
    assert_eq!(body_of(&h.response(id)), FIXED_BODY);
}

#[test]
fn fixed_responses_past_the_phase_bound_are_woken() {
    // Each response staged at delivery is charged at least 16 KiB against 1 MiB per phase.
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    for index in 0..64 {
        let id = client_bidi(index);
        open_with_fin(&mut h, id, &headers_frame(GET));
        assert!(
            h.conn.get(id).finished,
            "stream {index} is staged at delivery"
        );
    }
    assert!(h.conn.take_stream_wakes().is_empty());
    assert_eq!(h.conn.stream_wakers.get(), 0);

    let woken = client_bidi(64);
    open_with_fin(&mut h, woken, &headers_frame(GET));
    assert!(h.conn.get(woken).sent.is_empty(), "the 65th is not staged");
    assert_eq!(h.conn.take_stream_wakes(), [woken]);
    assert_eq!(h.conn.stream_wakers.get(), 1);
    assert!(h.pull(woken, 1 << 20).0.is_ready());
    assert!(h.conn.get(woken).finished);
    assert_eq!(body_of(&h.response(woken)), FIXED_BODY);

    h.conn.now += Duration::from_millis(1);
    let next = client_bidi(65);
    open_with_fin(&mut h, next, &headers_frame(GET));
    assert!(
        h.conn.get(next).finished,
        "a new phase stages at delivery again"
    );
    assert!(h.conn.take_stream_wakes().is_empty());
}

#[test]
fn fixed_responses_are_charged_the_bytes_they_stage() {
    // A response staged at delivery is charged the bytes it staged when they pass 16 KiB, so
    // responses with large heads reach the phase's 1 MiB bound in fewer than 64 stagings.
    static PAD: [u8; 16 << 10] = [b'z'; 16 << 10];
    let pad = std::str::from_utf8(&PAD).unwrap();
    let plan = Plan {
        headers: vec![("x-pad", pad)],
        ..fixed(Bytes::from(vec![b'a'; 16 << 10]))
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    open_with_fin(&mut h, client_bidi(0), &headers_frame(GET));
    let size = h.conn.get(client_bidi(0)).sent.len();
    // Each staging that starts below 1 MiB goes ahead.
    let staged = (1usize << 20).div_ceil(size) as u64;
    assert!(staged < 64, "{size} B per response");
    for index in 1..staged {
        let id = client_bidi(index);
        open_with_fin(&mut h, id, &headers_frame(GET));
        assert!(
            h.conn.get(id).finished,
            "stream {index} is staged at delivery"
        );
    }
    assert!(h.conn.take_stream_wakes().is_empty());
    let woken = client_bidi(staged);
    open_with_fin(&mut h, woken, &headers_frame(GET));
    assert!(
        h.conn.get(woken).sent.is_empty(),
        "stream {staged} is not staged"
    );
    assert_eq!(h.conn.take_stream_wakes(), [woken]);
}

#[test]
fn a_fixed_response_cut_short_finishes_after_its_wake() {
    let body = Bytes::from_static(FIXED_BODY);
    let (expected, _) = pulled(body.clone());
    // The DATA frame's type and length take a byte each.
    let data = 2 + FIXED_BODY.len();
    let head = expected.len() - data;
    // A stream that cannot take the body's DATA frame whole stages nothing at delivery, and one
    // that takes the DATA frame but not the whole response stages the HEADERS alone, so the body
    // is polled with the pulled path's allowance and its DATA frame is not cut shorter. Each case
    // is a stream capacity and the bytes it stages at delivery.
    let cases = [
        (head + 4, 0, "ends inside the DATA frame"),
        (3, 0, "ends inside the HEADERS frame"),
        (data - 1, 0, "a byte short of the DATA frame"),
        (data, head, "takes the DATA frame alone"),
        (head + data - 1, head, "a byte short of the response"),
    ];
    for (capacity, at_delivery, name) in cases {
        let mut h = Harness::ready_with(H3ApplicationBuilder::new(), fixed(body.clone()));
        let id = client_bidi(0);
        h.conn.receive(id, &headers_frame(GET));
        let stream = h.conn.get(id);
        stream.peer_finished = true;
        stream.capacity = capacity;
        h.ready_edge(id, StreamReady::Opened);
        let staged = h.conn.get(id).sent.clone();
        assert!(!h.conn.get(id).finished, "{name}");
        assert_eq!(h.conn.take_stream_wakes(), [id], "{name}: one wake");
        assert_eq!(h.conn.stream_wakers.get(), 1, "{name}");

        assert!(h.pull(id, 1 << 20).0.is_ready(), "{name}");
        let stream = h.conn.get(id);
        assert_eq!(
            stream.sent, expected,
            "{name}: the pulled bytes, none twice"
        );
        assert!(stream.finished, "{name}");
        assert_eq!(
            staged,
            expected[..at_delivery],
            "{name}: staged at delivery"
        );
        assert_eq!(
            h.log()[2..],
            [Ev::ResponseComplete(1), Ev::Complete(1)],
            "{name}"
        );
    }

    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), fixed(body));
    let id = client_bidi(0);
    h.conn.receive(id, &headers_frame(GET));
    let stream = h.conn.get(id);
    stream.peer_finished = true;
    stream.capacity = expected.len();
    h.ready_edge(id, StreamReady::Opened);
    let stream = h.conn.get(id);
    assert_eq!(stream.sent, expected, "a capacity of the whole response");
    assert!(stream.finished);
    assert!(h.conn.take_stream_wakes().is_empty());
}

#[test]
fn an_inline_response_computes_the_allowance_once() {
    let body = Bytes::from_static(FIXED_BODY);
    let expected = pulled(body.clone());
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), fixed(body.clone()));
    let id = client_bidi(0);
    open_with_fin(&mut h, id, &headers_frame(GET));
    let inline = h.conn.get(id);
    assert_eq!(
        (inline.sent.clone(), inline.finished),
        expected,
        "at delivery"
    );

    // A response that the session's poll stages through a stream sink over the stream.
    let plan = Plan {
        body,
        ..Plan::default()
    };
    let mut p = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    open_with_fin(&mut p, id, &headers_frame(GET));
    assert_eq!(p.conn.take_stream_wakes(), [id]);
    let waker = p.conn.stream_waker_for(id);
    let mut cx = Context::from_waker(&waker);
    let mut sink = StreamSink::new(p.conn.get(id));
    assert!(p.session.core.poll_send(&mut cx, id, &mut sink).is_ready());
    p.session.core.check_exchanges();
    let polled = p.conn.get(id);
    assert_eq!((polled.sent.clone(), polled.finished), expected, "polled");
    assert_eq!(
        [h.conn.get(id).computations.get(), polled.computations.get()],
        [1, 1],
        "allowance computations of a response staged at delivery and of one polled through a \
         stream sink"
    );
}

#[test]
fn a_stream_sink_reads_the_allowance_once_and_offers_none_after_its_finish() {
    let mut conn = FakeConn::new();
    let id = client_bidi(0);
    conn.receive(id, b"");
    conn.get(id).capacity = 100;
    let mut sink = StreamSink::new(conn.get(id));
    assert_eq!(sink.allowance(), 100);
    sink.push(Bytes::from_static(b"0123456789"))
        .expect("within the allowance");
    assert_eq!(sink.allowance(), 90);
    sink.finish().expect("the stream is open");
    assert_eq!(sink.allowance(), 0, "a finished stream takes no bytes");
    assert_eq!(
        sink.push(Bytes::from_static(b"x")),
        Err(SinkError::Closed),
        "a push after the finish"
    );
    let stream = conn.get(id);
    assert_eq!(stream.computations.get(), 1, "allowance computations");
    assert_eq!(stream.sent, b"0123456789");
    assert!(stream.finished);
}

#[test]
fn an_inline_response_error_resets_as_a_polled_one_does() {
    // HTTP/3 cannot carry 101, so encoding the fixed head fails.
    let plan = Plan {
        status: 101,
        ..fixed(Bytes::from_static(FIXED_BODY))
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let id = client_bidi(0);
    let wakes = h.conn.session_wakes();
    open_with_fin(&mut h, id, &headers_frame(GET));
    // The reset is queued as `poll_send` queues it, which wakes the session, and the callback
    // runs the queue before it returns.
    assert_eq!(h.conn.session_wakes(), wakes + 1);
    let cancelled = Some(H3ErrorCode::REQUEST_CANCELLED.as_u64());
    let stream = h.conn.get(id);
    assert_eq!((stream.reset, stream.stop_sending), (cancelled, cancelled));
    assert!(stream.sent.is_empty());
    assert!(h.conn.take_stream_wakes().is_empty(), "no wake fires");
    assert_eq!(h.conn.stream_wakers.get(), 0);
    assert_eq!(
        h.log()[2..],
        [Ev::Cancel {
            id: 1,
            code: HttpErrorCode::INTERNAL_ERROR,
            peer: false
        }]
    );
    assert!(h.pull(id, 1 << 20).0.is_pending());
}

// ---------------------------------------------------------------------------------------------
// Ingress bodies built only when something can use them.

/// Counts the allocations of the Opened edge of a stream whose `request` and FIN arrived together:
/// the exchange's admission, the read of its head and its delivery, which wakes the stream. The
/// fake's buffers are reserved beforehand, so only the session, the application and the stream
/// waker allocate.
fn allocations_at_open<A: OwnedApplication>(
    h: &mut Harness<A>,
    id: StreamId,
    request: &[u8],
) -> Option<usize> {
    h.conn.receive(id, request);
    h.conn.get(id).peer_finished = true;
    h.conn.stream_wakes.lock().unwrap().reserve(4);
    count_allocations(|| h.ready_edge(id, StreamReady::Opened))
}

/// Checks a census against its pinned count, where the count is observable.
fn assert_allocations(allocations: Option<usize>, expected: usize, what: &str) {
    match allocations {
        Some(allocations) => {
            eprintln!("{what}: {allocations} allocations");
            assert_eq!(allocations, expected, "{what}");
        }
        None => eprintln!("{what}: the allocation count needs a statically linked std"),
    }
}

#[test]
fn a_request_complete_at_its_headers_builds_no_ingress_body() {
    let mut h = Harness::skipping(Plan::default());
    let id = client_bidi(0);
    let allocations = allocations_at_open(&mut h, id, &headers_frame(GET));
    assert_eq!(h.log(), [Ev::New(1), Ev::Request { id: 1, eom: true }]);
    {
        let state = h.app.0.borrow();
        let body = state.requests[&http_id(id)].body.as_ref().unwrap();
        assert!(body.source().is_none(), "the body holds no source");
    }
    // An ingress body would add 3: its credit cell, its signals and its source state.
    assert_allocations(allocations, 13, "a GET with its FIN");

    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), b"hello");
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);
}

#[test]
fn an_application_that_takes_the_completion_keeps_the_eager_body() {
    let mut h = Harness::ready();
    let id = client_bidi(0);
    let allocations = allocations_at_open(&mut h, id, &headers_frame(GET));
    assert_eq!(h.log(), [Ev::New(1), Ev::Request { id: 1, eom: true }]);
    {
        let state = h.app.0.borrow();
        let received = &state.requests[&http_id(id)];
        assert!(received.body.as_ref().unwrap().source().is_none());
        let completion = received.completion.as_ref().expect("the exchange took it");
        assert_eq!(
            completion.snapshot().map(Result::ok),
            Some(Some(h.conn.now)),
            "the completion resolves with the phase reading of the FIN"
        );
    }
    // The ingress body's credit cell, signals and source state, and the completion's handle, are
    // among these.
    assert_allocations(allocations, 17, "a GET with its FIN, completion taken");

    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), b"hello");
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);

    // The head of a request whose body follows it is validated through the body built at
    // admission, which then carries the body: its completion stays pending until the FIN.
    let post = client_bidi(1);
    h.receive(post, &headers_frame(POST), false);
    h.receive(post, &data_frame(&[7; 10]), false);
    assert_eq!(
        h.log()[4..],
        [Ev::New(5), Ev::Request { id: 5, eom: false }]
    );
    let completion = h.app.0.borrow().requests[&http_id(post)]
        .completion
        .clone()
        .expect("the exchange took it");
    assert!(completion.snapshot().is_none(), "the body is open");
    h.receive(post, &data_frame(&[7; 10]), true);
    assert!(completion.snapshot().is_some_and(|outcome| outcome.is_ok()));
    assert_eq!(completion.body_bytes_received(), 20);
    assert_eq!(read_all(&mut h, post), (vec![7; 20], None));

    // A head followed by a bare FIN: the completion resolves at the FIN with its phase reading.
    let bare = client_bidi(2);
    h.receive(bare, &headers_frame(GET), false);
    let completion = h.app.0.borrow().requests[&http_id(bare)]
        .completion
        .clone()
        .expect("the exchange took it");
    assert!(completion.snapshot().is_none(), "the body is open");
    h.receive(bare, &[], true);
    assert_eq!(
        completion.snapshot().map(Result::ok),
        Some(Some(h.conn.now))
    );
}

#[test]
fn a_connect_builds_its_body_eagerly_at_the_head() {
    let mut h = Harness::skipping(Plan::default());
    let id = client_bidi(0);
    let request = [headers_frame(CONNECT), data_frame(b"tunnel")].concat();
    h.receive(id, &request, false);
    assert_eq!(h.log(), [Ev::New(1), Ev::Request { id: 1, eom: false }]);
    assert_eq!(read_chunk(&mut h, id), (b"tunnel".to_vec(), false));
    h.receive(id, &data_frame(b"!"), true);
    assert_eq!(read_chunk(&mut h, id), (b"!".to_vec(), true));

    // A CONNECT whose FIN came with its head builds its body all the same: the body's credit
    // cell, signals and source state are among these.
    let mut h = Harness::skipping(Plan::default());
    let allocations = allocations_at_open(&mut h, id, &headers_frame(CONNECT));
    assert_eq!(h.log(), [Ev::New(1), Ev::Request { id: 1, eom: true }]);
    assert_allocations(allocations, 14, "a CONNECT with its FIN");
}

#[test]
fn a_head_without_fin_builds_the_body_at_the_head() {
    let mut h = Harness::skipping(Plan::default());
    let id = client_bidi(0);
    let head = headers_frame(POST);
    h.receive(id, &head, false);
    assert_eq!(h.log(), [Ev::New(1), Ev::Request { id: 1, eom: false }]);
    assert_eq!(h.conn.get(id).consumed, head.len() as u64);

    // The payload's credit returns through the body's credit cell as the consumer reads it.
    h.receive(id, &data_frame(&[7; 500]), true);
    h.poll_session();
    let overhead = head.len() as u64 + 3;
    assert_eq!(h.conn.get(id).consumed, overhead, "the payload is unread");
    let wakes = h.wakes();
    assert_eq!(read_chunk(&mut h, id), (vec![7; 500], true));
    assert!(h.wakes() > wakes, "consuming wakes the session");
    h.poll_session();
    assert_eq!(h.conn.get(id).consumed, overhead + 500);
    h.pull_all(id, 1 << 20);
    assert_eq!(body_of(&h.response(id)), b"hello");
    assert_eq!(h.log()[2..], [Ev::ResponseComplete(1), Ev::Complete(1)]);

    // A head followed by a bare FIN is delivered open, and the FIN ends its body.
    let bare = client_bidi(1);
    h.receive(bare, &headers_frame(GET), false);
    assert_eq!(
        h.log()[4..],
        [Ev::New(5), Ev::Request { id: 5, eom: false }]
    );
    h.receive(bare, &[], true);
    let Poll::Ready(Ok(event)) = h.read_body(bare, 100) else {
        panic!("the FIN ends the request body");
    };
    assert!(event.eom() && event.as_body().is_some_and(BufQueue::is_empty));

    // A body whose FIN arrived with its head is delivered with the body.
    let together = client_bidi(2);
    let request = [headers_frame(POST), data_frame(b"hello")].concat();
    open_with_fin(&mut h, together, &request);
    assert_eq!(
        h.log()[6..],
        [Ev::New(9), Ev::Request { id: 9, eom: false }]
    );
    assert_eq!(read_chunk(&mut h, together), (b"hello".to_vec(), true));
}

/// Resets a malformed head, with and without its FIN, and a stream that the peer resets before
/// its head, then serves a GET on the same connection.
fn cancel_before_the_head<A: OwnedApplication>(mut h: Harness<A>) -> Harness<A> {
    let malformed = headers_frame(&[(":method", "GET"), (":path", "/")]);
    let message_error = Some(H3ErrorCode::MESSAGE_ERROR.as_u64());
    for (index, fin) in [(0, false), (1, true)] {
        let id = client_bidi(index);
        h.receive(id, &malformed, fin);
        let stream = h.conn.get(id);
        assert_eq!(stream.reset, message_error, "FIN {fin}");
        assert_eq!(stream.stop_sending, message_error, "FIN {fin}");
    }
    let reset = client_bidi(2);
    h.receive(reset, &[], false);
    h.ready_edge(
        reset,
        StreamReady::Reset {
            code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
            final_size: 0,
        },
    );
    assert_eq!(
        h.conn.get(reset).reset,
        Some(H3ErrorCode::REQUEST_CANCELLED.as_u64())
    );
    let cancel = |id, code, peer| Ev::Cancel { id, code, peer };
    assert_eq!(
        h.log(),
        [
            Ev::New(1),
            cancel(1, HttpErrorCode::MESSAGE_ERROR, false),
            Ev::New(5),
            cancel(5, HttpErrorCode::MESSAGE_ERROR, false),
            Ev::New(9),
            cancel(9, HttpErrorCode::CANCEL, true),
        ]
    );

    let next = client_bidi(3);
    h.receive(next, &headers_frame(GET), true);
    h.pull_all(next, 1 << 20);
    assert_eq!(body_of(&h.response(next)), b"hello");
    assert_eq!(h.closed(), None);
    h
}

#[test]
fn malformed_heads_and_resets_before_the_head_cancel_without_a_body() {
    let h = cancel_before_the_head(Harness::skipping(Plan::default()));
    assert!(h.app.0.borrow().cancelled.is_empty());

    // An exchange that took its completion sees it resolve with the cancellation's error.
    let h = cancel_before_the_head(Harness::ready());
    let state = h.app.0.borrow();
    for (id, code) in [
        (1, HttpErrorCode::MESSAGE_ERROR),
        (5, HttpErrorCode::MESSAGE_ERROR),
        (9, HttpErrorCode::CANCEL),
    ] {
        let outcome = state.cancelled[&id].snapshot().expect("it resolves");
        assert_eq!(
            outcome.err().map(|error| error.code()),
            Some(code),
            "stream {id}"
        );
    }
}

/// Serves a request whose FIN came with its head `fields`, and returns how its stream was reset,
/// if it was, and the application's events.
fn complete_at_its_head<A: OwnedApplication>(
    mut h: Harness<A>,
    fields: &[(&str, &str)],
) -> (Option<u64>, Vec<Ev>) {
    let id = client_bidi(0);
    open_with_fin(&mut h, id, &headers_frame(fields));
    h.pull_all(id, 1 << 20);
    (h.conn.get(id).reset, h.log())
}

#[test]
fn content_length_cases_are_judged_alike_with_and_without_a_body() {
    let post = |extra: &[(&'static str, &'static str)]| [POST, extra].concat();
    let length = |value| ("content-length", value);
    let message_error = Some(H3ErrorCode::MESSAGE_ERROR.as_u64());
    let cases = [
        ("zero", post(&[length("0")]), None),
        ("five", post(&[length("5")]), message_error),
        ("two equal", post(&[length("0"), length("0")]), None),
        (
            "two different",
            post(&[length("0"), length("5")]),
            message_error,
        ),
    ];
    for (name, fields, reset) in cases {
        let skipping = complete_at_its_head(Harness::skipping(Plan::default()), &fields);
        let taking = complete_at_its_head(Harness::ready(), &fields);
        assert_eq!(skipping, taking, "{name}");
        assert_eq!(skipping.0, reset, "{name}");
    }
}

#[test]
fn peer_reset_of_a_retired_exchange_cancels_nothing() {
    reset_retired_exchanges(Harness::ready());
    reset_retired_exchanges(Harness::skipping(Plan::default()));
}

/// Retires a GET complete at its head and a POST whose body is unread, then resets both streams
/// from the peer before their `Closed` edges.
fn reset_retired_exchanges<A: OwnedApplication>(mut h: Harness<A>) {
    let get = client_bidi(0);
    open_with_fin(&mut h, get, &headers_frame(GET));
    let post = client_bidi(1);
    let request = [headers_frame(POST), data_frame(&[7; 10])].concat();
    open_with_fin(&mut h, post, &request);
    h.pull_all(get, 1 << 20);
    h.pull_all(post, 1 << 20);
    let retired = [
        Ev::New(1),
        Ev::Request { id: 1, eom: true },
        Ev::New(5),
        Ev::Request { id: 5, eom: false },
        Ev::ResponseComplete(1),
        Ev::Complete(1),
        Ev::ResponseComplete(5),
        Ev::Complete(5),
    ];
    assert_eq!(h.log(), retired);

    for id in [get, post] {
        h.ready_edge(
            id,
            StreamReady::Reset {
                code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
                final_size: 100,
            },
        );
    }
    assert_eq!(h.log(), retired, "the resets cancel nothing");
    assert_eq!(read_chunk(&mut h, post), (vec![7; 10], true));
    for id in [get, post] {
        h.ready_edge(id, StreamReady::Closed);
    }
    assert_eq!(h.log(), retired);
    assert_eq!(h.closed(), None);
}

#[test]
fn a_session_dropped_before_the_head_ends_a_taken_completion_as_dropped() {
    let mut h = Harness::hosting(H3ApplicationBuilder::new(), Plan::default(), KeepApp);
    h.make_ready();
    let id = client_bidi(0);
    let head = headers_frame(POST);
    h.receive(id, &head[..head.len() - 1], false);
    assert_eq!(h.log(), [Ev::New(1)], "the head is incomplete");
    let app = h.app.clone();
    assert!(
        app.0.borrow().kept[&1].snapshot().is_none(),
        "it is pending"
    );

    // The session goes without a `Closed` edge, as when its connection is torn down. The body's
    // owner goes before its drain owner, which then stops the draining body as dropped.
    drop(h);
    let outcome = app.0.borrow().kept[&1].snapshot().expect("it resolves");
    assert_eq!(
        outcome.err().map(|error| error.code()),
        Some(HttpErrorCode::DROPPED)
    );
}

// ---------------------------------------------------------------------------------------------
// Exchanges by stream.

/// Returns the lookup work that `step` does.
fn census_of<A: OwnedApplication>(
    h: &mut Harness<A>,
    step: impl FnOnce(&mut Harness<A>),
) -> Census {
    let before = h.session.core.census();
    step(h);
    let after = h.session.core.census();
    Census {
        visited: after.visited - before.visited,
        moved: after.moved - before.moved,
        finds: after.finds - before.finds,
        probes: after.probes - before.probes,
    }
}

/// Opens a GET stream with its FIN at each index, delivered and waiting for its response.
fn open_gets<A: OwnedApplication>(h: &mut Harness<A>, indexes: Range<u64>) {
    for index in indexes {
        h.receive(client_bidi(index), &headers_frame(GET), true);
    }
}

fn admitted<A: OwnedApplication>(h: &Harness<A>, index: u64) -> bool {
    h.log().contains(&Ev::New(http_id(client_bidi(index))))
}

#[test]
fn admission_reads_a_live_count_instead_of_walking_the_exchanges() {
    let builder = H3ApplicationBuilder::new().max_concurrent_exchanges(512);
    let mut h = Harness::ready_with(builder, Plan::default());
    open_gets(&mut h, 0..256);
    let census = census_of(&mut h, |h| open_gets(h, 256..257));
    assert!(admitted(&h, 256));
    assert_eq!(
        census.visited, 0,
        "exchanges visited to admit one next to 256"
    );

    // The default limit rejects a stream past 100 live exchanges. An exchange that retired and
    // waits for its stream's `Closed` edge does not count, and that edge does not make room for
    // another stream.
    let mut h = Harness::ready();
    open_gets(&mut h, 0..100);
    let rejected = Some(H3ErrorCode::REQUEST_REJECTED.as_u64());
    open_gets(&mut h, 100..101);
    assert_eq!(h.conn.get(client_bidi(100)).reset, rejected);
    assert!(!admitted(&h, 100));
    h.pull_all(client_bidi(0), 1 << 20);
    assert_eq!(h.log().last(), Some(&Ev::Complete(1)), "stream 0 retired");
    open_gets(&mut h, 101..102);
    assert!(admitted(&h, 101), "a retired exchange does not count");
    assert_eq!(h.conn.get(client_bidi(101)).reset, None);
    open_gets(&mut h, 102..103);
    assert_eq!(h.conn.get(client_bidi(102)).reset, rejected);
    h.ready_edge(client_bidi(0), StreamReady::Closed);
    open_gets(&mut h, 103..104);
    assert_eq!(h.conn.get(client_bidi(103)).reset, rejected);
    assert!(!admitted(&h, 102) && !admitted(&h, 103));
}

#[test]
fn closing_the_oldest_exchange_moves_no_other() {
    let builder = H3ApplicationBuilder::new().max_concurrent_exchanges(512);
    let mut h = Harness::ready_with(builder, Plan::default());
    open_gets(&mut h, 0..256);
    for index in 0..256 {
        h.pull_all(client_bidi(index), 1 << 20);
    }
    let moved: Vec<usize> = (0..256)
        .map(|index| {
            census_of(&mut h, |h| {
                h.ready_edge(client_bidi(index), StreamReady::Closed)
            })
            .moved
        })
        .collect();
    assert_eq!(
        moved, [0; 256],
        "exchanges moved by each Closed edge, oldest first"
    );
    assert_eq!(
        h.log()
            .iter()
            .filter(|ev| matches!(ev, Ev::Complete(_)))
            .count(),
        256
    );
}

#[test]
fn a_find_inside_the_window_probes_one_slot() {
    let builder = H3ApplicationBuilder::new().max_concurrent_exchanges(512);
    let mut h = Harness::ready_with(builder, Plan::default());
    for index in 0..256 {
        h.receive(client_bidi(index), &headers_frame(POST), false);
    }
    // Each edge to a present stream, and each pull, finds the stream's exchange one or more
    // times. Every find probes one slot.
    let mut steps = Vec::new();
    for index in 0..256 {
        let id = client_bidi(index);
        let mut step = |name: &'static str, step: &dyn Fn(&mut Harness)| {
            steps.push((name, index, census_of(&mut h, step)));
        };
        step("DATA", &|h| h.receive(id, &data_frame(b"x"), false));
        step("FIN", &|h| h.receive(id, &[], true));
        step("pull", &|h| assert!(h.pull(id, 1 << 20).0.is_ready()));
        let code = H3ErrorCode::REQUEST_CANCELLED.as_u64();
        match index % 3 {
            0 => step("reset", &|h| {
                h.ready_edge(
                    id,
                    StreamReady::Reset {
                        code: app(code),
                        final_size: 0,
                    },
                )
            }),
            1 => step("stop", &|h| {
                h.ready_edge(id, StreamReady::Stopped { code: app(code) })
            }),
            _ => {}
        }
        step("Closed", &|h| h.ready_edge(id, StreamReady::Closed));
    }
    for (name, index, census) in steps {
        assert!(
            census.finds > 0,
            "{name} on stream {index} finds its exchange"
        );
        assert_eq!(
            census.probes, census.finds,
            "{name} on stream {index}: one probe per find, {census:?}"
        );
    }
    assert_eq!(
        h.log()
            .iter()
            .filter(|ev| matches!(ev, Ev::Complete(_)))
            .count(),
        256
    );
}

#[test]
fn a_stream_of_another_kind_does_not_find_a_request_exchange() {
    let mut h = Harness::ready();
    // Request stream 4 has index 1, as do these streams of the three other kinds.
    open_gets(&mut h, 1..2);
    let others = [
        client_uni(1),
        server_uni(1),
        StreamId::new(Side::Server, Dir::Bi, 1).unwrap(),
    ];
    for id in others {
        let (poll, sink) = h.pull(id, 1 << 20);
        assert!(
            poll.is_pending() && sink.pushed.is_empty(),
            "a pull of {id:?} does not find an exchange"
        );
    }
    let id = client_bidi(1);
    assert!(h.pull(id, 1 << 20).0.is_ready());
    assert!(
        h.conn.get(id).finished,
        "stream 4 is answered on its own stream"
    );
}

/// Returns the exchange window's base and slots, and the exchanges present.
fn window<A: OwnedApplication>(h: &Harness<A>) -> (u64, usize, usize) {
    let shape = h.session.core.exchange_shape();
    (shape.base, shape.slots, shape.occupied)
}

#[test]
fn exchanges_closed_out_of_order_are_found_and_reaped() {
    let mut h = Harness::ready();
    // Streams 0, 4, 8, 12 and 16, at indexes 0 to 4.
    open_gets(&mut h, 0..5);
    for index in [2, 1, 3] {
        h.ready_edge(client_bidi(index), StreamReady::Closed);
        assert_eq!(window(&h).0, 0, "the window starts at stream 0");
    }
    assert_eq!(window(&h), (0, 5, 2));
    for index in [0, 4] {
        let id = client_bidi(index);
        assert!(h.pull(id, 1 << 20).0.is_ready());
        assert!(
            h.conn.get(id).finished,
            "stream {id:?} is found and answered"
        );
    }
    h.ready_edge(client_bidi(0), StreamReady::Closed);
    assert_eq!(
        window(&h),
        (4, 1, 1),
        "the front passes the empty slots of streams 4, 8 and 12"
    );
    h.ready_edge(client_bidi(4), StreamReady::Closed);
    assert_eq!(window(&h), (5, 0, 0));
    let cancel = |id| Ev::Cancel {
        id,
        code: HttpErrorCode::STREAM_CLOSED,
        peer: true,
    };
    assert_eq!(
        h.log()[10..],
        [
            cancel(9),
            cancel(5),
            cancel(13),
            Ev::ResponseComplete(1),
            Ev::Complete(1),
            Ev::ResponseComplete(17),
            Ev::Complete(17),
        ]
    );
}

#[test]
fn streams_opened_out_of_order_are_admitted_in_place() {
    let mut h = Harness::ready();
    // Stream 8's Opened edge comes before stream 4's, then stream 0's.
    for index in [2, 1, 0] {
        open_gets(&mut h, index..index + 1);
        assert!(admitted(&h, index));
    }
    let shape = h.session.core.exchange_shape();
    assert_eq!(
        (shape.base, shape.slots, shape.side),
        (0, 3, 0),
        "streams 4 and 0 take slots in front of stream 8"
    );
    for index in [1, 2, 0] {
        let id = client_bidi(index);
        assert!(h.pull(id, 1 << 20).0.is_ready());
        assert!(
            h.conn.get(id).finished,
            "stream {id:?} is found and answered"
        );
    }

    // With a window of 4 slots at streams 5 to 7 by index, streams 0 and 3 do not fit in front
    // of it and go to the side map. Stream 4 then takes the window's fourth slot.
    let mut h = Harness::ready();
    h.session.core.set_exchange_window(4);
    open_gets(&mut h, 5..8);
    let mut shapes = Vec::new();
    for index in [0, 3, 4] {
        open_gets(&mut h, index..index + 1);
        assert!(admitted(&h, index));
        let shape = h.session.core.exchange_shape();
        shapes.push((index, shape.base, shape.slots, shape.side));
    }
    assert_eq!(shapes, [(0, 5, 3, 1), (3, 5, 3, 2), (4, 4, 4, 2)]);
    for index in [0, 3, 4, 5, 6, 7] {
        let id = client_bidi(index);
        assert!(h.pull(id, 1 << 20).0.is_ready());
        assert!(
            h.conn.get(id).finished,
            "stream {id:?} is found and answered"
        );
    }
}

#[test]
fn late_streams_take_the_empty_slots_left_for_them_in_the_window() {
    // Stream 12 opens after stream 0, so the window appends empty slots up to it, or before stream
    // 0, so the window grows at the front by the slots down to it. Either way streams 4 and 8 open
    // last and take the empty slots between them.
    for order in [[0, 3, 1, 2], [3, 0, 1, 2]] {
        let mut h = Harness::ready();
        let mut shapes = Vec::new();
        for index in order {
            open_gets(&mut h, index..index + 1);
            assert!(
                admitted(&h, index),
                "stream {:?} is admitted, opened in index order {order:?}",
                client_bidi(index)
            );
            let shape = h.session.core.exchange_shape();
            shapes.push((shape.base, shape.slots, shape.side));
        }
        assert_eq!(
            shapes,
            [(order[0], 1, 0), (0, 4, 0), (0, 4, 0), (0, 4, 0)],
            "opened in index order {order:?}"
        );
        for index in 0..4 {
            let id = client_bidi(index);
            assert!(h.pull(id, 1 << 20).0.is_ready());
            assert!(
                h.conn.get(id).finished,
                "stream {id:?} is found and answered on its own stream, order {order:?}"
            );
        }
    }
}

#[test]
fn a_long_lived_exchange_moves_out_of_the_window() {
    let mut h = Harness::ready();
    h.session.core.set_exchange_window(8);
    // Two CONNECT streams stay open while 20 later requests open, are answered and close.
    for index in [0, 1] {
        h.receive(client_bidi(index), &headers_frame(CONNECT), false);
    }
    let mut slots = Vec::new();
    for index in 2..22 {
        let id = client_bidi(index);
        open_gets(&mut h, index..index + 1);
        slots.push(h.session.core.exchange_shape().slots);
        h.pull_all(id, 1 << 20);
        h.ready_edge(id, StreamReady::Closed);
        let shape = h.session.core.exchange_shape();
        assert_eq!((shape.occupied, shape.live), (2, 2), "after stream {index}");
    }
    // The ninth slot would hold stream 8 by index: the oldest CONNECT moves to the side map, and
    // stream 9 moves the other, after which the window holds each request alone.
    let expected: Vec<usize> = [3, 4, 5, 6, 7, 8, 8].into_iter().chain([1; 13]).collect();
    assert_eq!(slots, expected, "window slots after each request opens");
    let shape = h.session.core.exchange_shape();
    assert_eq!((shape.slots, shape.side), (0, 2));

    // The tunnels' DATA still reaches their exchanges.
    for index in [0, 1] {
        let id = client_bidi(index);
        h.receive(id, &data_frame(b"tunnel"), false);
        assert_eq!(read_chunk(&mut h, id), (b"tunnel".to_vec(), false));
    }
    // A reset and a Closed edge each remove one from the side map.
    h.ready_edge(
        client_bidi(0),
        StreamReady::Reset {
            code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
            final_size: 100,
        },
    );
    let shape = h.session.core.exchange_shape();
    assert_eq!((shape.side, shape.occupied, shape.live), (1, 1, 1));
    h.ready_edge(client_bidi(1), StreamReady::Closed);
    let shape = h.session.core.exchange_shape();
    assert_eq!((shape.side, shape.occupied, shape.live), (0, 0, 0));
    let log = h.log();
    assert_eq!(
        log[log.len() - 2..],
        [
            Ev::Cancel {
                id: 1,
                code: HttpErrorCode::CANCEL,
                peer: true
            },
            Ev::Cancel {
                id: 5,
                code: HttpErrorCode::STREAM_CLOSED,
                peer: true
            },
        ]
    );
}

#[test]
fn the_window_holds_1024_slots_by_default() {
    let builder = H3ApplicationBuilder::new().max_concurrent_exchanges(2048);
    let mut h = Harness::ready_with(builder, Plan::default());
    let shape = |h: &Harness| {
        let shape = h.session.core.exchange_shape();
        (shape.base, shape.slots, shape.side)
    };
    // A CONNECT at index 0 holds the window's front while requests open up to index 1,023.
    h.receive(client_bidi(0), &headers_frame(CONNECT), false);
    open_gets(&mut h, 1..1024);
    assert_eq!(shape(&h), (0, 1024, 0));
    open_gets(&mut h, 1024..1025);
    assert_eq!(shape(&h), (1, 1024, 1), "the CONNECT moves to the side map");
}

#[test]
fn a_stream_past_the_bound_restarts_an_emptied_window() {
    let mut h = Harness::ready();
    h.session.core.set_exchange_window(4);
    // A request at index 8, twice the bound past a CONNECT at index 0, moves the CONNECT to the
    // side map. The emptied window starts again at the request, not at the lowest index the bound
    // allows.
    h.receive(client_bidi(0), &headers_frame(CONNECT), false);
    open_gets(&mut h, 8..9);
    let shape = h.session.core.exchange_shape();
    assert_eq!((shape.base, shape.slots, shape.side), (8, 1, 1));
    let id = client_bidi(8);
    assert!(h.pull(id, 1 << 20).0.is_ready());
    assert!(h.conn.get(id).finished, "stream 32 is found and answered");
    let tunnel = client_bidi(0);
    h.receive(tunnel, &data_frame(b"tunnel"), false);
    assert_eq!(read_chunk(&mut h, tunnel), (b"tunnel".to_vec(), false));
}

#[test]
fn a_late_stream_below_a_side_map_exchange_joins_the_side_map() {
    let mut h = Harness::ready();
    h.session.core.set_exchange_window(4);
    // CONNECTs at indexes 0 and 3 fill the window, and a request at index 7 moves both to the
    // side map. Late Opened edges then come for indexes 2 and 1, below CONNECT 3. Index 2 would
    // fit in front of the window at index 5, and index 1 could start the emptied window again, but
    // either would put the window below CONNECT 3, so both join the side map.
    for index in [0, 3] {
        h.receive(client_bidi(index), &headers_frame(CONNECT), false);
    }
    let shape = |h: &Harness| {
        let shape = h.session.core.exchange_shape();
        (shape.base, shape.slots, shape.side)
    };
    let answer_and_close = |h: &mut Harness, index| {
        let id = client_bidi(index);
        h.pull_all(id, 1 << 20);
        h.ready_edge(id, StreamReady::Closed);
    };
    open_gets(&mut h, 7..8);
    assert_eq!(shape(&h), (7, 1, 2), "index 7 moves both CONNECTs");
    answer_and_close(&mut h, 7);
    open_gets(&mut h, 5..6);
    assert_eq!(shape(&h), (5, 1, 2));
    open_gets(&mut h, 2..3);
    assert_eq!(shape(&h), (5, 1, 3), "index 2 joins the side map");
    answer_and_close(&mut h, 5);
    assert_eq!(shape(&h), (6, 0, 3));
    open_gets(&mut h, 1..2);
    assert_eq!(shape(&h), (6, 0, 4), "index 1 joins the side map");

    for index in [0, 3] {
        let id = client_bidi(index);
        h.receive(id, &data_frame(b"tunnel"), false);
        assert_eq!(read_chunk(&mut h, id), (b"tunnel".to_vec(), false));
    }
    for index in [1, 2] {
        let id = client_bidi(index);
        assert!(h.pull(id, 1 << 20).0.is_ready());
        assert!(
            h.conn.get(id).finished,
            "stream {id:?} is found and answered"
        );
    }
    for index in [2, 1, 0, 3] {
        h.ready_edge(client_bidi(index), StreamReady::Closed);
    }
    assert_eq!(h.session.core.exchange_shape().occupied, 0);
}

#[test]
fn a_quiesced_connection_waits_for_and_stops_exchanges_in_the_side_map() {
    let mut h = Harness::ready();
    h.session.core.set_exchange_window(4);
    // CONNECTs at indexes 0 and 1 stay open, and a request at index 5 moves both to the side map.
    // Once the request closes, only the side map holds exchanges.
    for index in [0, 1] {
        h.receive(client_bidi(index), &headers_frame(CONNECT), false);
    }
    open_gets(&mut h, 5..6);
    let shape = |h: &Harness| {
        let shape = h.session.core.exchange_shape();
        (shape.slots, shape.side, shape.occupied)
    };
    assert_eq!(shape(&h), (1, 2, 3));
    let request = client_bidi(5);
    h.pull_all(request, 1 << 20);
    h.ready_edge(request, StreamReady::Closed);
    assert_eq!(shape(&h), (0, 2, 2));

    // After GOAWAY, the tunnels keep the connection open past the close delay.
    h.session.core.on_quiesce(&mut h.conn);
    assert_eq!(h.session.deadline(), None, "the tunnels are open");
    h.conn.now += Duration::from_secs(60);
    h.poll_session();
    assert_eq!(h.closed(), None, "the tunnels are open");

    // A peer STOP_SENDING on CONNECT 0 cancels its exchange and stops its upload.
    let stopped = client_bidi(0);
    h.ready_edge(
        stopped,
        StreamReady::Stopped {
            code: app(H3ErrorCode::REQUEST_CANCELLED.as_u64()),
        },
    );
    assert_eq!(
        h.log().last(),
        Some(&Ev::Cancel {
            id: 1,
            code: HttpErrorCode::CANCEL,
            peer: true
        })
    );
    assert_eq!(
        h.conn.get(stopped).stop_sending,
        Some(H3ErrorCode::REQUEST_CANCELLED.as_u64())
    );
    assert_eq!(shape(&h), (0, 1, 1));
    assert_eq!(h.closed(), None, "CONNECT 1 is open");

    // The end of the last tunnel closes the connection.
    h.ready_edge(client_bidi(1), StreamReady::Closed);
    assert_eq!(h.closed(), Some(H3ErrorCode::NO_ERROR.as_u64()));
}

#[test]
fn cancel_all_cancels_in_ascending_stream_order() {
    let mut h = Harness::ready();
    h.session.core.set_exchange_window(4);
    // Streams 2 to 5 by index fill the window. Stream 0 does not fit in front of it, stream 6
    // moves stream 2 to the side map, and stream 1 goes there between streams 0 and 2.
    for index in [2, 3, 4, 5, 0, 6, 1] {
        open_gets(&mut h, index..index + 1);
    }
    let shape = h.session.core.exchange_shape();
    assert_eq!((shape.base, shape.slots, shape.side), (3, 4, 3));
    // A retired exchange is not cancelled.
    h.pull_all(client_bidi(4), 1 << 20);
    h.event(ConnEvent::Closed {
        reason: mvfstr_proto::CloseReason::Silent,
        initiator: mvfstr_proto::Initiator::Peer,
    });
    let cancelled: Vec<u64> = h
        .log()
        .into_iter()
        .filter_map(|ev| match ev {
            Ev::Cancel { id, .. } => Some(id),
            _ => None,
        })
        .collect();
    assert_eq!(
        cancelled,
        [0, 1, 2, 3, 5, 6].map(|index| http_id(client_bidi(index)))
    );
    assert_eq!(h.session.core.exchange_shape().occupied, 0);
}

#[test]
fn the_live_count_matches_a_walk_after_every_edge() {
    // The harness checks the table's counts against a walk after every edge, pull and session
    // poll. This sequence reaches each way an exchange retires or goes.
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let counts = |h: &Harness| {
        let shape = h.session.core.exchange_shape();
        (shape.occupied, shape.live)
    };
    // A fixed response staged at delivery retires its exchange inside the Opened edge.
    open_with_fin(&mut h, client_bidi(0), &headers_frame(GET));
    assert_eq!(counts(&h), (1, 0));

    // After an early response, STOP_SENDING ends the upload. The exchange does not retire and
    // stays until its stream's `Closed` edge.
    h.app.0.borrow_mut().plan = Plan::default();
    let early = client_bidi(1);
    h.receive(early, &headers_frame(POST), false);
    h.pull_all(early, 1 << 20);
    h.poll_session();
    assert_eq!(
        h.conn.get(early).stop_sending,
        Some(H3ErrorCode::NO_ERROR.as_u64())
    );
    assert_eq!(counts(&h), (2, 1));

    // A peer reset, a peer STOP_SENDING and a malformed head each remove their exchange.
    let code = app(H3ErrorCode::REQUEST_CANCELLED.as_u64());
    h.receive(client_bidi(2), &headers_frame(POST), false);
    h.ready_edge(
        client_bidi(2),
        StreamReady::Reset {
            code,
            final_size: 0,
        },
    );
    h.receive(client_bidi(3), &headers_frame(GET), true);
    h.ready_edge(client_bidi(3), StreamReady::Stopped { code });
    h.receive(
        client_bidi(4),
        &headers_frame(&[(":method", "GET"), (":path", "/")]),
        true,
    );
    assert_eq!(
        h.conn.get(client_bidi(4)).reset,
        Some(H3ErrorCode::MESSAGE_ERROR.as_u64())
    );
    assert_eq!(counts(&h), (2, 1));

    // A stream past the GOAWAY is rejected without an exchange.
    h.session.core.on_quiesce(&mut h.conn);
    h.session.core.check_exchanges();
    h.receive(client_bidi(5), &headers_frame(GET), true);
    assert_eq!(
        h.conn.get(client_bidi(5)).reset,
        Some(H3ErrorCode::REQUEST_REJECTED.as_u64())
    );
    assert_eq!(counts(&h), (2, 1));

    // `Closed` removes the retired exchange and the early response's.
    h.ready_edge(client_bidi(0), StreamReady::Closed);
    assert_eq!(counts(&h), (1, 1));
    h.ready_edge(early, StreamReady::Closed);
    assert_eq!(counts(&h), (0, 0));
    let cancel = |id, code| Ev::Cancel {
        id,
        code,
        peer: true,
    };
    assert_eq!(
        h.log()[4..],
        [
            Ev::New(5),
            Ev::Request { id: 5, eom: false },
            Ev::ResponseComplete(5),
            Ev::New(9),
            Ev::Request { id: 9, eom: false },
            cancel(9, HttpErrorCode::CANCEL),
            Ev::New(13),
            Ev::Request { id: 13, eom: true },
            cancel(13, HttpErrorCode::CANCEL),
            Ev::New(17),
            Ev::Cancel {
                id: 17,
                code: HttpErrorCode::MESSAGE_ERROR,
                peer: false
            },
            Ev::Complete(5),
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Response heads encoded once per connection.

/// The header fields of a response head that a server sends unchanged.
const SHARED_FIELDS: &[(&str, &str)] = &[
    ("content-type", "text/plain"),
    ("server", "roxygen"),
    ("x-request-cost", "low"),
];

/// Returns a response head whose header entries are shared, so that its clones share them.
fn shared_head(status: u16, fields: &[(&str, &str)]) -> HttpMessage {
    let mut head = HttpMessage::new();
    head.set_status_code(status).unwrap();
    for (name, value) in fields {
        head.headers_mut().add(*name, value);
    }
    head.headers_mut().share_entries();
    head
}

/// Returns the fields a client decodes from a head with `status` and `fields`.
fn decoded_head(status: u16, fields: &[(&str, &str)]) -> Vec<(String, String)> {
    let status = (":status".to_owned(), status.to_string());
    let fields = fields
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()));
    std::iter::once(status).chain(fields).collect()
}

/// Returns the first frame of a response stream's bytes, which is its HEADERS frame, as sent.
fn sent_headers_frame(sent: &[u8]) -> Vec<u8> {
    let mut decoder = FrameDecoder::new(StreamKind::Request, Role::Client, FrameLimits::default());
    let mut input = Bytes::copy_from_slice(sent);
    let frame = decoder.decode(&mut input).unwrap();
    assert!(matches!(frame, Some(Frame::Headers(_))), "{frame:?}");
    sent[..sent.len() - input.len()].to_vec()
}

/// Returns the fields of the HEADERS frame that starts a response stream.
fn response_head(h: &mut Harness, id: StreamId) -> Vec<(String, String)> {
    match h.response(id).first() {
        Some(Part::Headers(fields)) => fields.clone(),
        other => panic!("expected HEADERS first: {other:?}"),
    }
}

/// Serves a fixed GET with each head in turn, on request streams from the first, and returns the
/// head encode count after each response.
fn serve_heads(h: &mut Harness, heads: &[&HttpMessage]) -> Vec<usize> {
    heads
        .iter()
        .zip(0..)
        .map(|(head, index)| {
            h.app.0.borrow_mut().plan.head = Some((*head).clone());
            let id = client_bidi(index);
            open_with_fin(h, id, &headers_frame(GET));
            assert!(h.conn.get(id).finished, "stream {index} is staged");
            h.session.core.head_encodes()
        })
        .collect()
}

#[test]
fn a_shared_response_head_is_encoded_once_per_connection() {
    let head = shared_head(200, SHARED_FIELDS);
    // Fixed responses, staged while their requests are delivered.
    let plan = Plan {
        head: Some(head.clone()),
        ..fixed(Bytes::from_static(FIXED_BODY))
    };
    let mut inline = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let inline_encodes: Vec<usize> = (0..8)
        .map(|index| {
            let id = client_bidi(index);
            open_with_fin(&mut inline, id, &headers_frame(GET));
            assert!(inline.conn.get(id).finished, "stream {index} is staged");
            inline.session.core.head_encodes()
        })
        .collect();
    // Responses pulled through each stream's wake and `poll_send`.
    let plan = Plan {
        head: Some(head),
        ..Plan::default()
    };
    let mut pulled = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let pulled_encodes: Vec<usize> = (0..8)
        .map(|index| {
            let id = client_bidi(index);
            open_with_fin(&mut pulled, id, &headers_frame(GET));
            assert_eq!(pulled.conn.take_stream_wakes(), [id]);
            assert!(pulled.pull(id, 1 << 20).0.is_ready());
            assert!(pulled.conn.get(id).finished, "stream {index} ends");
            pulled.session.core.head_encodes()
        })
        .collect();
    assert_eq!(
        [inline_encodes, pulled_encodes],
        [[1; 8], [1; 8]],
        "head encodes after each response, staged inline and pulled"
    );

    let mut first = Vec::new();
    for h in [&mut inline, &mut pulled] {
        let parts = h.response(client_bidi(0));
        let [Part::Headers(fields), Part::Data(body)] = parts.as_slice() else {
            panic!("expected HEADERS and DATA: {parts:?}");
        };
        assert_eq!(*fields, decoded_head(200, SHARED_FIELDS));
        assert_eq!(body, &h.app.0.borrow().plan.body[..]);
        let frames: Vec<Vec<u8>> = (0..8)
            .map(|index| sent_headers_frame(&h.conn.get(client_bidi(index)).sent))
            .collect();
        first.push(frames[0].clone());
        for (index, frame) in frames.iter().enumerate() {
            assert_eq!(frame, &frames[0], "the HEADERS frame of stream {index}");
        }
    }
    assert_eq!(first[0], first[1], "inline and pulled heads are alike");
}

#[test]
fn a_changed_clone_is_encoded_and_does_not_replace_the_cache() {
    let head = shared_head(200, SHARED_FIELDS);
    let mut changed = head.clone();
    changed.headers_mut().add("x-changed", "1");
    assert!(
        !changed.headers().shares_entries_with(head.headers()),
        "the added header copies the entries"
    );
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    assert_eq!(
        serve_heads(&mut h, &[&head, &changed, &head]),
        [1, 2, 2],
        "head encodes after the shared head, the changed clone and the shared head"
    );

    let mut with_change = SHARED_FIELDS.to_vec();
    with_change.push(("x-changed", "1"));
    assert_eq!(
        response_head(&mut h, client_bidi(1)),
        decoded_head(200, &with_change)
    );
    for index in [0, 2] {
        assert_eq!(
            response_head(&mut h, client_bidi(index)),
            decoded_head(200, SHARED_FIELDS),
            "stream {index}"
        );
    }
    let frame = |h: &mut Harness, index| sent_headers_frame(&h.conn.get(client_bidi(index)).sent);
    assert_eq!(frame(&mut h, 2), frame(&mut h, 0));
}

/// Counts the allocations of each of eight fixed responses on one connection, whose heads are
/// clones of one head with shared entries, from the stream's Opened edge, which stages the
/// response, until the edge returns. Each stream closes after its response, outside the count, so
/// the exchange window keeps one slot. The fake's buffers and the application's records are
/// reserved beforehand. In tests, each encode also copies every line it encodes for the field
/// tests' recorder: two allocations per line, and the growth of the recorder's list.
#[test]
fn a_cached_head_allocates_nothing_on_a_hit() {
    let plan = Plan {
        head: Some(shared_head(200, SHARED_FIELDS)),
        ..fixed(Bytes::from_static(FIXED_BODY))
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    {
        let mut state = h.app.0.borrow_mut();
        state.log.reserve(64);
        state.requests.reserve(16);
        state.maxima.reserve(16);
    }
    let counts: Option<Vec<usize>> = (0..8)
        .map(|index| {
            let id = client_bidi(index);
            h.conn.receive(id, &headers_frame(GET));
            let stream = h.conn.get(id);
            stream.peer_finished = true;
            stream.sent.reserve(256);
            let allocations = count_allocations(|| h.ready_edge(id, StreamReady::Opened));
            assert!(h.conn.get(id).finished, "stream {index} is staged");
            assert_eq!(body_of(&h.response(id)), FIXED_BODY, "stream {index}");
            h.ready_edge(id, StreamReady::Closed);
            allocations
        })
        .collect();
    assert_eq!(h.session.core.head_encodes(), 1);
    match counts {
        Some(counts) => {
            eprintln!("allocations of each response: {counts:?}");
            // The first response also makes the connection's first exchange and frame buffer, and
            // the cache's copy of the encoded section. The first hit promotes that copy's handle to
            // a shared one, and later hits allocate nothing for the head.
            assert_eq!(counts, [107, 14, 13, 13, 13, 13, 13, 13]);
        }
        None => eprintln!("the allocation count needs a statically linked std"),
    }
}

#[test]
fn a_new_status_or_new_entries_replace_the_cached_head() {
    let ok = shared_head(200, SHARED_FIELDS);
    let mut not_found = ok.clone();
    not_found.set_status_code(404).unwrap();
    assert!(not_found.headers().shares_entries_with(ok.headers()));
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan.clone());
    assert_eq!(
        serve_heads(&mut h, &[&ok, &not_found, &ok]),
        [1, 2, 3],
        "head encodes after the shared headers at 200, at 404 and at 200"
    );
    for (index, status) in [(0, 200), (1, 404), (2, 200)] {
        assert_eq!(
            response_head(&mut h, client_bidi(index)),
            decoded_head(status, SHARED_FIELDS),
            "stream {index}"
        );
    }

    let b_fields: &[(&str, &str)] = &[("content-type", "text/html")];
    let a = shared_head(200, SHARED_FIELDS);
    let b = shared_head(200, b_fields);
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    assert_eq!(
        serve_heads(&mut h, &[&a, &b, &a, &b]),
        [1, 2, 3, 4],
        "head encodes after heads A, B, A and B"
    );
    for (index, fields) in (0..).zip([SHARED_FIELDS, b_fields, SHARED_FIELDS, b_fields]) {
        assert_eq!(
            response_head(&mut h, client_bidi(index)),
            decoded_head(200, fields),
            "stream {index}"
        );
    }
}

#[test]
fn a_head_http3_cannot_carry_fails_although_its_entries_are_cached() {
    // HTTP/3 cannot carry 101, so a 101 head fails as it does without a cache, and leaves the
    // cached head in place.
    let ok = shared_head(200, SHARED_FIELDS);
    let mut switching = ok.clone();
    switching.set_status_code(101).unwrap();
    let plan = fixed(Bytes::from_static(FIXED_BODY));
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    assert_eq!(serve_heads(&mut h, &[&ok]), [1]);

    h.app.0.borrow_mut().plan.head = Some(switching);
    let id = client_bidi(1);
    open_with_fin(&mut h, id, &headers_frame(GET));
    let cancelled = Some(H3ErrorCode::REQUEST_CANCELLED.as_u64());
    let stream = h.conn.get(id);
    assert_eq!((stream.reset, stream.stop_sending), (cancelled, cancelled));
    assert!(stream.sent.is_empty());
    assert_eq!(
        h.log().last(),
        Some(&Ev::Cancel {
            id: http_id(id),
            code: HttpErrorCode::INTERNAL_ERROR,
            peer: false
        })
    );
    assert_eq!(h.session.core.head_encodes(), 2, "the 101 head is encoded");

    h.app.0.borrow_mut().plan.head = Some(ok);
    let id = client_bidi(2);
    open_with_fin(&mut h, id, &headers_frame(GET));
    assert!(h.conn.get(id).finished);
    assert_eq!(response_head(&mut h, id), decoded_head(200, SHARED_FIELDS));
    assert_eq!(
        h.session.core.head_encodes(),
        2,
        "the 200 head is still cached"
    );
}

#[test]
fn a_head_with_owned_entries_is_encoded_every_time() {
    let plan = Plan {
        headers: SHARED_FIELDS.to_vec(),
        ..fixed(Bytes::from_static(FIXED_BODY))
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    let encodes: Vec<usize> = (0..8)
        .map(|index| {
            let id = client_bidi(index);
            open_with_fin(&mut h, id, &headers_frame(GET));
            assert!(h.conn.get(id).finished, "stream {index} is staged");
            assert_eq!(
                response_head(&mut h, id),
                decoded_head(200, SHARED_FIELDS),
                "stream {index}"
            );
            h.session.core.head_encodes()
        })
        .collect();
    assert_eq!(
        encodes,
        [1, 2, 3, 4, 5, 6, 7, 8],
        "head encodes after each response built with `add`"
    );
}

#[test]
fn cached_and_encoded_heads_stage_the_same_bytes() {
    let a = shared_head(200, SHARED_FIELDS);
    let with_status = |status| {
        let mut head = a.clone();
        head.set_status_code(status).unwrap();
        head
    };
    let b = shared_head(200, &[("content-type", "text/html"), ("x-b", "1")]);
    let early_hints = shared_head(103, &[("link", "</style.css>")]);
    let head_request = [
        (":method", "HEAD"),
        (":scheme", "https"),
        (":authority", "example.com"),
        (":path", "/index.html"),
    ];
    let shared = |head: &HttpMessage, fixed: bool| Plan {
        head: Some(head.clone()),
        fixed,
        record_sizes: true,
        ..Plan::default()
    };
    let owned = Plan {
        headers: SHARED_FIELDS.to_vec(),
        record_sizes: true,
        ..Plan::default()
    };
    // Each response's plan and request: shared and owned heads, staged inline and pulled, at 200,
    // 404 and 500, with 103 interim heads, shared and owned, and a HEAD request.
    let sequence: Vec<(Plan, &[(&str, &str)])> = vec![
        (shared(&a, true), GET),
        (shared(&a, false), GET),
        (owned.clone(), GET),
        (shared(&a, true), GET),
        (shared(&with_status(404), true), GET),
        (shared(&with_status(404), false), GET),
        (shared(&a, false), &head_request),
        (
            Plan {
                informational: true,
                interim: Some(early_hints.clone()),
                ..shared(&a, false)
            },
            GET,
        ),
        (
            Plan {
                informational: true,
                ..shared(&a, false)
            },
            GET,
        ),
        (shared(&with_status(500), true), GET),
        (shared(&b, true), GET),
        (shared(&a, true), GET),
        (owned, GET),
        (shared(&a, false), GET),
    ];
    let run = |cache: bool| {
        let mut h = Harness::ready();
        h.session.core.set_head_cache(cache);
        let streams: Vec<(Vec<u8>, bool)> = sequence
            .iter()
            .zip(0..)
            .map(|((plan, request), index)| {
                h.app.0.borrow_mut().plan = plan.clone();
                let id = client_bidi(index);
                open_with_fin(&mut h, id, &headers_frame(request));
                if !plan.fixed {
                    assert_eq!(h.conn.take_stream_wakes(), [id], "stream {index}");
                    h.pull_all(id, 1 << 20);
                }
                let stream = h.conn.get(id);
                assert!(stream.finished, "stream {index} ends");
                (stream.sent.clone(), stream.finished)
            })
            .collect();
        let sizes = h.app.0.borrow().sizes.clone();
        (streams, sizes, h.session.core.head_encodes())
    };
    let (cached, cached_sizes, cached_encodes) = run(true);
    let (encoded, encoded_sizes, encoded_encodes) = run(false);
    assert_eq!(
        encoded_encodes, 16,
        "every head is encoded with the cache off"
    );
    assert_eq!(cached_encodes, 11, "head encodes with the cache on");
    for (index, (cached, encoded)) in cached.iter().zip(&encoded).enumerate() {
        assert_eq!(cached, encoded, "stream {index}");
    }
    assert_eq!(cached_sizes.len(), 16, "one size for each head");
    assert_eq!(cached_sizes, encoded_sizes);
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "the QPACK encoder must not change while a response head is cached")]
fn a_hit_after_the_encoder_changed_fails_the_debug_check() {
    let plan = Plan {
        head: Some(shared_head(200, SHARED_FIELDS)),
        ..fixed(Bytes::from_static(FIXED_BODY))
    };
    let mut h = Harness::ready_with(H3ApplicationBuilder::new(), plan);
    open_with_fin(&mut h, client_bidi(0), &headers_frame(GET));
    // Without Huffman coding the encoder writes the head's literals longer.
    h.session.core.qpack_encoder_mut().set_huffman(false);
    open_with_fin(&mut h, client_bidi(1), &headers_frame(GET));
}
