/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Sessions do not keep a copy of an ingress message. These tests see a kept copy
//! only when it shares the delivered header entries, because a handler's change to
//! the delivered headers then moves them, or when an error attaches it; a deep copy
//! kept anywhere else is invisible here. An error for HTTP/2 body bytes after the
//! end of the stream carries the request head only while the handler has not read
//! it.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

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
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_http1::Http1xCodec;
use roxygen_http2::Http2Codec;
use roxygen_http2::frame::FrameType;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;

use super::DriverState;
use super::UniplexConfig;
use super::UniplexHandle;
use super::UniplexRead;
use super::UniplexSession;
use super::UniplexTransport;
use crate::http2::Http2SessionConfig;
use crate::owned_h2::OwnedH2Application;
use crate::owned_h2::OwnedH2Exchange;
use crate::owned_h2::OwnedH2Options;
use crate::owned_h2::OwnedH2Session;
use crate::uniplex_tests::run_on_worker;

const QUEUE_LIMIT: usize = 1 << 20;
const PROBE: &[u8] = b"x-probe";

fn queue(bytes: &[u8]) -> BufQueue {
    BufQueue::from_vec(bytes.to_vec(), QUEUE_LIMIT).expect("fixture bytes fit")
}

fn queue_bytes(queue: &BufQueue) -> Vec<u8> {
    queue.segments().flatten().copied().collect()
}

/// Renders the fields that the codec sets on an ingress message.
fn render(message: &HttpMessage) -> String {
    let mut rendered = match (message.request_method(), message.response_status()) {
        (Some(method), _) => format!(
            "{method} \"{}\"",
            message.request_url().unwrap_or_default().escape_ascii()
        ),
        (None, Some(status)) => format!(
            "{status} \"{}\"",
            message
                .response_status_message()
                .unwrap_or_default()
                .escape_ascii()
        ),
        (None, None) => "kindless".to_owned(),
    };
    let (major, minor) = message.http_version();
    let _ = write!(rendered, " {major}.{minor} [");
    message.headers().for_each(|name, value| {
        let _ = write!(
            rendered,
            "\"{}\": \"{}\", ",
            name.escape_ascii(),
            value.escape_ascii()
        );
    });
    let _ = write!(
        rendered,
        "] keepalive={} chunked={} upgraded={} websocket={}",
        message.wants_keepalive(),
        message.is_chunked(),
        message.is_upgraded(),
        message.is_ingress_websocket_upgrade()
    );
    rendered
}

fn probe_address(message: &HttpMessage) -> *const u8 {
    message.headers().get_single_or_empty(PROBE).as_ptr()
}

/// Changes `message`'s headers and reports whether its probe value kept its
/// address, which it does unless another message shares the header entries.
fn edits_in_place(message: &mut HttpMessage) -> bool {
    assert!(!message.headers().get_single_or_empty(PROBE).is_empty());
    let before = probe_address(message);
    // An uncommon name that is absent still makes the removal write the entries.
    message.headers_mut().remove(b"x-absent");
    probe_address(message) == before
}

/// The check fails when another message shares the header entries.
#[test]
fn edits_in_place_sees_shared_header_entries() {
    let mut message = HttpMessage::new();
    message.headers_mut().set(PROBE, b"shared");
    assert!(edits_in_place(&mut message.clone()));
    message.headers_mut().share_entries();
    let _sibling = message.clone();
    assert!(!edits_in_place(&mut message));
}

/// Collects the messages a standalone codec delivers.
#[derive(Default)]
struct Messages(Vec<String>);

impl HttpCodecCallback for Messages {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if let CodecEvent::HeadersComplete { message, .. } = event {
            self.0.push(render(&message));
        }
    }
}

fn decode(codec: &mut dyn HttpCodec, wire: &[u8]) -> Vec<String> {
    let mut messages = Messages::default();
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let _ = codec.on_ingress(&queue(wire), &mut messages, &mut egress);
    messages.0
}

type Gate = Box<dyn Fn() -> bool>;

struct TransportState {
    reads: VecDeque<(Gate, Option<Vec<u8>>)>,
    writes: Vec<Vec<u8>>,
}

/// Delivers each scripted read once its gate opens; `None` is EOF. Writes and
/// handler progress re-check the gate.
struct ScriptTransport {
    state: Rc<RefCell<TransportState>>,
    progress: Rc<tokio::sync::Notify>,
}

impl ScriptTransport {
    fn new(progress: &Rc<tokio::sync::Notify>) -> Rc<Self> {
        Rc::new(Self {
            state: Rc::new(RefCell::new(TransportState {
                reads: VecDeque::new(),
                writes: Vec::new(),
            })),
            progress: Rc::clone(progress),
        })
    }

    fn push_read(&self, gate: impl Fn() -> bool + 'static, bytes: Option<Vec<u8>>) {
        self.state
            .borrow_mut()
            .reads
            .push_back((Box::new(gate), bytes));
    }

    fn written(&self) -> Vec<u8> {
        self.state.borrow().writes.concat()
    }
}

impl UniplexTransport for ScriptTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let next = self.state.borrow_mut().reads.pop_front();
        let progress = Rc::clone(&self.progress);
        Box::pin(async move {
            let Some((gate, bytes)) = next else {
                return Ok(UniplexRead::new(BufQueue::new(0), true));
            };
            let baton = CancellableBaton::new();
            while !gate() {
                tokio::select! {
                    _ = progress.notified() => {}
                    _ = baton.wait(cancellation.clone()) => {
                        return Err(HttpError::with_message(
                            HttpErrorCode::CORO_CANCELLED,
                            b"scripted read cancelled",
                        ));
                    }
                }
            }
            Ok(match bytes {
                Some(bytes) => UniplexRead::new(queue(&bytes), false),
                None => UniplexRead::new(BufQueue::new(0), true),
            })
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
        let bytes = queue_bytes(&data);
        let bytes_written = bytes.len();
        self.state.borrow_mut().writes.push(bytes);
        self.progress.notify_one();
        Box::pin(std::future::ready(Ok(WriteInfo { bytes_written })))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
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

/// What a handler observed when its header read finished.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Seen {
    /// The delivered message's rendering, or the header-read error's code and
    /// its attached message's rendering.
    delivered: String,
    /// For a delivered message, whether a change to its headers left them in
    /// place.
    edited_in_place: Option<bool>,
}

/// What a handler observed when a body read after its headers failed.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Failure {
    /// The body read's error code.
    body_error: u16,
    /// The stream's terminal error: its code and its attached message's
    /// rendering.
    terminal: Option<(u16, String)>,
    /// Whether the terminal error's message has the delivered message's probe
    /// address.
    shares_probe: Option<bool>,
}

#[derive(Default)]
struct Probe {
    session: RefCell<Weak<RefCell<DriverState>>>,
    seen: RefCell<Vec<(StreamId, Seen)>>,
    failures: RefCell<Vec<(StreamId, Failure)>>,
    finished: Cell<usize>,
}

/// Maps each fixture request target to its stream, records what its header
/// read returned and whether it could edit the delivered headers in place,
/// drains its body, records a failed body read, and responds with 200.
struct ProbeHandler {
    probe: Rc<Probe>,
    progress: Rc<tokio::sync::Notify>,
    streams: &'static [(&'static [u8], u64)],
}

/// The stream of the request whose target `message` carries, or the first
/// stream when no message identifies it.
fn stream_for(streams: &[(&[u8], u64)], message: Option<&HttpMessage>) -> StreamId {
    let url = message.and_then(HttpMessage::request_url);
    let stream = streams
        .iter()
        .find(|(target, _)| url == Some(*target))
        .map_or(streams[0].1, |(_, stream)| *stream);
    StreamId::new(stream)
}

impl HttpHandler for ProbeHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let probe = Rc::clone(&self.probe);
        let progress = Rc::clone(&self.progress);
        let streams = self.streams;
        Box::pin(async move {
            let mut header = request.read_header_event().await;
            let (stream, seen) = match &mut header {
                Ok(headers) => {
                    let message = headers.headers_mut();
                    let delivered = render(message);
                    (
                        stream_for(streams, Some(&*message)),
                        Seen {
                            delivered,
                            edited_in_place: Some(edits_in_place(message)),
                        },
                    )
                }
                Err(error) => {
                    let message = error.http_message();
                    (
                        stream_for(streams, message),
                        Seen {
                            delivered: format!(
                                "error {} {}",
                                error.code().as_u16(),
                                message.map_or_else(|| "none".to_owned(), render)
                            ),
                            edited_in_place: None,
                        },
                    )
                }
            };
            probe.seen.borrow_mut().push((stream, seen));
            progress.notify_one();
            if let Ok(headers) = &header
                && !headers.eom()
            {
                let session = probe.session.borrow().upgrade().expect("the session lives");
                loop {
                    match request.read_body_event(u32::MAX).await {
                        Ok(event) if event.eom() => break,
                        Ok(_) => {}
                        Err(error) => {
                            let failure = failure(&session.borrow(), stream, &error, headers);
                            probe.failures.borrow_mut().push((stream, failure));
                            break;
                        }
                    }
                }
            }
            probe.finished.set(probe.finished.get() + 1);
            progress.notify_one();
            let response =
                HttpFixedSource::make_fixed_response(200, None).expect("a fixed 200 response");
            Ok(HttpSourceHolder::new(Box::new(response)))
        })
    }
}

/// Records a failed body read with the stream's terminal error, the error
/// that the session's failure for the stream left behind.
fn failure(
    session: &DriverState,
    stream: StreamId,
    error: &HttpError,
    headers: &HttpHeaderEvent,
) -> Failure {
    let terminal = session
        .core()
        .stream_if_present(stream)
        .and_then(|state| state.terminal_error());
    Failure {
        body_error: error.code().as_u16(),
        terminal: terminal.map(|terminal| {
            (
                terminal.code().as_u16(),
                terminal
                    .http_message()
                    .map_or_else(|| "none".to_owned(), render),
            )
        }),
        shares_probe: terminal
            .and_then(HttpError::http_message)
            .map(|message| probe_address(message) == probe_address(headers.headers())),
    }
}

struct DownstreamOutcome {
    seen: Vec<(StreamId, Seen)>,
    failures: Vec<(StreamId, Failure)>,
    run_error: Option<(u16, Option<String>)>,
    written: Vec<u8>,
}

/// Runs a downstream session over `reads`, each delivered once that many
/// handlers have finished their header reads, then EOF once `handlers`
/// handlers have finished.
fn run_downstream(
    reads: Vec<(usize, Vec<u8>)>,
    http2: bool,
    streams: &'static [(&'static [u8], u64)],
    handlers: usize,
) -> DownstreamOutcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let progress = Rc::new(tokio::sync::Notify::new());
            let probe = Rc::new(Probe::default());
            let transport = ScriptTransport::new(&progress);
            for (seen, bytes) in reads {
                let probe = Rc::clone(&probe);
                transport.push_read(move || probe.seen.borrow().len() >= seen, Some(bytes));
            }
            let finished = Rc::clone(&probe);
            transport.push_read(move || finished.finished.get() >= handlers, None);
            let handler = Rc::new(ProbeHandler {
                probe: Rc::clone(&probe),
                progress: Rc::clone(&progress),
                streams,
            });
            let erased: Rc<dyn UniplexTransport> = transport.clone();
            let (session, _handle) = if http2 {
                UniplexSession::new_http2(
                    context,
                    TransportDirection::Downstream,
                    erased,
                    Some(handler),
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
            } else {
                UniplexSession::new(
                    context,
                    TransportDirection::Downstream,
                    erased,
                    Some(handler),
                    UniplexConfig::default(),
                )
            }
            .expect("a downstream session");
            *probe.session.borrow_mut() = Rc::downgrade(&session.state);
            let result = tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .expect("the session finishes");
            let seen = probe.seen.borrow().clone();
            let failures = probe.failures.borrow().clone();
            DownstreamOutcome {
                seen,
                failures,
                run_error: result
                    .err()
                    .map(|error| (error.code().as_u16(), error.http_message().map(render))),
                written: transport.written(),
            }
        })
    })
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

const FIRST: &[u8] = b"GET /first HTTP/1.1\r\nHost: example.test\r\nX-Probe: first\r\n\r\n";
const SECOND: &[u8] = b"POST /second HTTP/1.1\r\nHost: example.test\r\nX-Probe: second\r\n\
                        Content-Length: 5\r\n\r\nhello";
const FORBIDDEN: &[u8] = b"TRACE /forbidden HTTP/1.1\r\nHost: example.test\r\n\
                           X-Probe: forbidden\r\nContent-Length: 5\r\n\r\nhello";
const H1_STREAMS: &[(&[u8], u64)] = &[(b"/first", 1), (b"/second", 2), (b"/forbidden", 1)];

/// The server's 200 response to the first request, with an `x-probe` header,
/// over HTTP/2 when `http2` is set and HTTP/1.1 otherwise. Returns the head and
/// the body.
fn upstream_response_wire(http2: bool) -> (Vec<u8>, Vec<u8>) {
    if !http2 {
        return (
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Probe: upstream\r\n\r\n".to_vec(),
            b"ok".to_vec(),
        );
    }
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut callback = super::NoopCodecCallback;
    let mut head = BufQueue::new(QUEUE_LIMIT);
    codec
        .generate_configured_settings(&mut head, &mut callback)
        .expect("server settings");
    let mut response = HttpMessage::new();
    response.set_status_code(200).expect("a response status");
    response.set_http_version(1, 1);
    response.headers_mut().set(PROBE, b"upstream");
    codec
        .generate(
            &mut head,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &response,
                end_stream: false,
                extra_headers: None,
            },
            &mut callback,
        )
        .expect("response headers");
    let mut body = BufQueue::new(QUEUE_LIMIT);
    codec
        .generate(
            &mut body,
            EgressFrame::Body {
                stream: StreamId::new(1),
                body: queue(b"ok"),
                padding: None,
                end_stream: true,
            },
            &mut callback,
        )
        .expect("a response body");
    (queue_bytes(&head), queue_bytes(&body))
}

/// A standalone upstream codec's rendering of `wire`, the response to its GET
/// of `/upstream`.
fn standalone_response(http2: bool, wire: &[u8]) -> String {
    let mut callback = super::NoopCodecCallback;
    let mut output = BufQueue::new(QUEUE_LIMIT);
    let mut codec: Box<dyn HttpCodec> = if http2 {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
            .expect("a client preface");
        codec
            .generate_configured_settings(&mut output, &mut callback)
            .expect("client settings");
        Box::new(codec)
    } else {
        Box::new(Http1xCodec::new(TransportDirection::Upstream))
    };
    let stream = codec.create_stream();
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Get).expect("a method");
    request.set_url(b"/upstream", false).expect("a URL");
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
        .expect("the standalone request");
    decode(codec.as_mut(), wire)
        .into_iter()
        .next()
        .expect("the standalone response")
}

/// Sends a GET on an upstream session and records what the response's header
/// read returned, with the standalone codec's rendering of the response. The
/// body arrives after the header read.
fn upstream_response(http2: bool) -> (Seen, String) {
    let (head, body) = upstream_response_wire(http2);
    let expected = standalone_response(http2, &[head.as_slice(), &body].concat());
    let seen = run_on_worker(move |context| {
        Box::pin(async move {
            let progress = Rc::new(tokio::sync::Notify::new());
            let transport = ScriptTransport::new(&progress);
            let requested = Rc::clone(&transport.state);
            transport.push_read(move || !requested.borrow().writes.is_empty(), Some(head));
            let headers_read = Rc::new(Cell::new(false));
            let gate = Rc::clone(&headers_read);
            transport.push_read(move || gate.get(), Some(body));
            transport.push_read(|| true, None);
            let erased: Rc<dyn UniplexTransport> = transport.clone();
            let (session, handle) = if http2 {
                UniplexSession::new_http2(
                    context,
                    TransportDirection::Upstream,
                    erased,
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
            } else {
                UniplexSession::new(
                    context,
                    TransportDirection::Upstream,
                    erased,
                    None,
                    UniplexConfig::default(),
                )
            }
            .expect("an upstream session");
            let source =
                HttpFixedSource::make_get_request(b"/upstream", None).expect("a fixed GET request");
            let mut source = send(&handle, source).await.source;
            let consume = async move {
                let mut headers = source
                    .read_header_event()
                    .await
                    .expect("the response headers");
                let delivered = headers.headers_mut();
                let seen = Seen {
                    delivered: render(delivered),
                    edited_in_place: Some(edits_in_place(delivered)),
                };
                headers_read.set(true);
                progress.notify_one();
                while let Ok(event) = source.read_body_event(u32::MAX).await {
                    if event.eom() {
                        break;
                    }
                }
                seen
            };
            let (result, seen) = tokio::join!(
                tokio::time::timeout(Duration::from_secs(5), session.run()),
                consume
            );
            result
                .expect("the session finishes")
                .expect("the exchange succeeds");
            seen
        })
    });
    (seen, expected)
}

async fn send(handle: &UniplexHandle, source: HttpFixedSource) -> super::UniplexResponse {
    handle
        .send_request(
            HttpSourceHolder::new(Box::new(source)),
            CancellationSource::new().token(),
        )
        .await
        .expect("the request starts")
}

#[test]
fn http1_sessions_keep_no_ingress_message_copy() {
    let mut pipelined = FIRST.to_vec();
    pipelined.extend_from_slice(SECOND);
    let expected = decode(
        &mut Http1xCodec::new(TransportDirection::Downstream),
        &pipelined,
    );
    let outcome = run_downstream(vec![(0, pipelined)], false, H1_STREAMS, 2);
    assert_eq!(outcome.run_error, None);
    assert_eq!(
        outcome.seen,
        [
            (
                StreamId::new(1),
                Seen {
                    delivered: expected[0].clone(),
                    edited_in_place: Some(true),
                },
            ),
            (
                StreamId::new(2),
                Seen {
                    delivered: expected[1].clone(),
                    edited_in_place: Some(true),
                },
            ),
        ]
    );

    let expected = decode(
        &mut Http1xCodec::new(TransportDirection::Downstream),
        FORBIDDEN,
    );
    let head = FORBIDDEN.len() - b"hello".len();
    let outcome = run_downstream(
        vec![
            (0, FORBIDDEN[..head].to_vec()),
            (1, FORBIDDEN[head..].to_vec()),
        ],
        false,
        H1_STREAMS,
        1,
    );
    assert_eq!(
        outcome.run_error,
        Some((
            HttpErrorCode::HEADER_PARSE_ERROR.as_u16(),
            Some(expected[0].clone())
        ))
    );
    assert!(contains(&outcome.written, b"HTTP/1.1 400 "));
    assert_eq!(outcome.seen.len(), 1);
    let (stream, seen) = &outcome.seen[0];
    assert_eq!(*stream, StreamId::new(1));
    assert!(
        seen.delivered.starts_with("error "),
        "no handler receives the forbidden request's headers: {seen:?}"
    );

    let (seen, expected) = upstream_response(false);
    assert_eq!(
        seen,
        Seen {
            delivered: expected,
            edited_in_place: Some(true),
        }
    );
}

/// A client connection preface, SETTINGS, and each request's HEADERS; then
/// the DATA of the requests that have a body.
fn http2_wire(requests: &[(HttpMethod, &[u8], &[u8], Option<&[u8]>)]) -> (Vec<u8>, Vec<u8>) {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut callback = super::NoopCodecCallback;
    let mut output = BufQueue::new(QUEUE_LIMIT);
    codec
        .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
        .expect("a client preface");
    codec
        .generate_configured_settings(&mut output, &mut callback)
        .expect("client settings");
    let mut bodies = BufQueue::new(QUEUE_LIMIT);
    for (method, target, probe, body) in requests {
        let stream = codec.create_stream();
        let mut request = HttpMessage::new();
        request.set_method(*method).expect("a request method");
        request.set_url(*target, false).expect("a request URL");
        request.set_http_version(1, 1);
        let headers: &mut HttpHeaders = request.headers_mut();
        headers.set(b"host", b"example.test");
        headers.set(PROBE, *probe);
        if let Some(body) = body {
            headers.set(b"content-length", body.len().to_string().as_bytes());
        }
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: body.is_none(),
                    extra_headers: None,
                },
                &mut callback,
            )
            .expect("request headers");
        if let Some(body) = body {
            codec
                .generate(
                    &mut bodies,
                    EgressFrame::Body {
                        stream,
                        body: queue(body),
                        padding: None,
                        end_stream: true,
                    },
                    &mut callback,
                )
                .expect("a request body");
        }
    }
    (queue_bytes(&output), queue_bytes(&bodies))
}

const H2_STREAMS: &[(&[u8], u64)] = &[(b"/shared", 1), (b"/forbidden", 3)];

#[test]
fn http2_sessions_keep_no_ingress_message_copy() {
    let (headers, bodies) = http2_wire(&[
        (HttpMethod::Get, b"/shared", b"shared", None),
        (
            HttpMethod::Trace,
            b"/forbidden",
            b"forbidden",
            Some(b"hello"),
        ),
    ]);
    let expected = decode(
        &mut Http2Codec::new(TransportDirection::Downstream),
        &headers,
    );
    // The forbidden request's 400 aborts the source that holds the failure's
    // attached message as unread headers, so the header read returns that
    // message on its error.
    let outcome = run_downstream(vec![(0, headers), (2, bodies)], true, H2_STREAMS, 2);
    assert_eq!(outcome.run_error, None);
    let mut seen = outcome.seen;
    seen.sort_by_key(|(stream, _)| *stream);
    assert_eq!(
        seen,
        [
            (
                StreamId::new(1),
                Seen {
                    delivered: expected[0].clone(),
                    edited_in_place: Some(true),
                },
            ),
            (
                StreamId::new(3),
                Seen {
                    delivered: format!(
                        "error {} {}",
                        HttpErrorCode::HEADER_PARSE_ERROR.as_u16(),
                        expected[1]
                    ),
                    edited_in_place: None,
                },
            ),
        ]
    );
}

#[test]
fn http2_upstream_sessions_keep_no_ingress_message_copy() {
    let (seen, expected) = upstream_response(true);
    assert_eq!(
        seen,
        Seen {
            delivered: expected,
            edited_in_place: Some(true),
        }
    );
}

/// Trailers without END_STREAM, and DATA after them, arrive after the handler has read its
/// headers. The codec fails the stream at the trailers, so its source never sees the DATA, and
/// the failure carries the trailers the codec decoded.
#[test]
fn http2_trailers_without_end_stream_fail_after_the_head() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut callback = super::NoopCodecCallback;
    let mut output = BufQueue::new(QUEUE_LIMIT);
    codec
        .generate(&mut output, EgressFrame::ConnectionPreface, &mut callback)
        .expect("a client preface");
    codec
        .generate_configured_settings(&mut output, &mut callback)
        .expect("client settings");
    let stream = codec.create_stream();
    let mut request = HttpMessage::new();
    request
        .set_method(HttpMethod::Get)
        .expect("a request method");
    request.set_url(b"/state", false).expect("a request URL");
    request.set_http_version(1, 1);
    request.headers_mut().set(PROBE, b"state");
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
    let headers_wire = queue_bytes(&output);
    let mut trailers = HttpHeaders::new();
    trailers.set(b"x-trailer", b"done");
    let mut trailer_wire = BufQueue::new(QUEUE_LIMIT);
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
    let mut trailer_bytes = queue_bytes(&trailer_wire);
    trailer_bytes[4] &= !0x1;
    let mut body_wire = BufQueue::new(QUEUE_LIMIT);
    codec
        .generate(
            &mut body_wire,
            EgressFrame::Body {
                stream,
                body: queue(b"late body"),
                padding: None,
                end_stream: false,
            },
            &mut callback,
        )
        .expect("body after trailers");
    let mut late = trailer_bytes;
    late.extend_from_slice(&queue_bytes(&body_wire));

    let expected = decode(
        &mut Http2Codec::new(TransportDirection::Downstream),
        &headers_wire,
    );
    let outcome = run_downstream(
        vec![(0, headers_wire), (1, late)],
        true,
        &[(b"/state", 1)],
        1,
    );
    assert_eq!(outcome.run_error, None);
    assert_eq!(
        outcome.seen,
        [(
            StreamId::new(1),
            Seen {
                delivered: expected[0].clone(),
                edited_in_place: Some(true),
            },
        )]
    );
    let protocol = HttpErrorCode::PROTOCOL_ERROR.as_u16();
    assert_eq!(
        outcome.failures,
        [(
            StreamId::new(1),
            Failure {
                body_error: protocol,
                terminal: Some((
                    protocol,
                    "kindless 1.0 [\"x-trailer\": \"done\", ] keepalive=true chunked=false \
                     upgraded=false websocket=false"
                        .to_owned()
                )),
                shares_probe: Some(false),
            },
        )]
    );
}

const LATE_STREAMS: &[(&[u8], u64)] = &[(b"/late", 1)];

/// A POST of "body" on stream 1: the client preface, SETTINGS and HEADERS, then
/// DATA with END_STREAM.
fn late_body_wire() -> (Vec<u8>, Vec<u8>) {
    http2_wire(&[(HttpMethod::Post, b"/late", b"late", Some(b"body"))])
}

fn frame(kind: FrameType, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("a short payload");
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind.as_u8(), 0]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// DATA on stream 1 after its END_STREAM.
fn late_data() -> Vec<u8> {
    frame(FrameType::DATA, 1, b"late")
}

/// RST_STREAM(STREAM_CLOSED) on stream 1.
fn stream_closed_reset() -> Vec<u8> {
    frame(
        FrameType::RST_STREAM,
        1,
        &H2ErrorCode::STREAM_CLOSED.as_u32().to_be_bytes(),
    )
}

/// What the owned HTTP/2 driver's exchanges observed: each request head's
/// rendering, and each cancellation's code and attached message's rendering.
#[derive(Default)]
struct OwnedProbe {
    heads: RefCell<Vec<String>>,
    cancels: RefCell<Vec<(u16, String)>>,
    progress: Rc<tokio::sync::Notify>,
}

/// Holds each request body unread until its exchange is cancelled, and never
/// responds.
struct HoldingApplication(Rc<OwnedProbe>);

struct HoldingExchange {
    probe: Rc<OwnedProbe>,
    body: Option<HttpSourceHolder>,
}

impl OwnedH2Application for HoldingApplication {
    type Exchange = HoldingExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> HoldingExchange {
        HoldingExchange {
            probe: Rc::clone(&self.0),
            body: None,
        }
    }
}

impl OwnedH2Exchange for HoldingExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        self.probe.heads.borrow_mut().push(render(&head));
        self.body = Some(body.into_source_holder());
        self.probe.progress.notify_one();
    }

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
        unreachable!("the exchange does not produce a head")
    }

    fn cancel(&mut self, error: &HttpError, _peer_reset: bool) {
        self.probe.cancels.borrow_mut().push((
            error.code().as_u16(),
            error
                .http_message()
                .map_or_else(|| "none".to_owned(), render),
        ));
        drop(self.body.take());
        self.probe.progress.notify_one();
    }

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

struct OwnedOutcome {
    heads: Vec<String>,
    cancels: Vec<(u16, String)>,
    run_error: Option<u16>,
    written: Vec<u8>,
}

/// Runs the owned HTTP/2 driver over `head`, then `late` once an exchange has
/// its request head, then EOF once an exchange is cancelled.
fn run_owned(head: Vec<u8>, late: Vec<u8>) -> OwnedOutcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let probe = Rc::new(OwnedProbe::default());
            let transport = ScriptTransport::new(&probe.progress);
            transport.push_read(|| true, Some(head));
            let headed = Rc::clone(&probe);
            transport.push_read(move || !headed.heads.borrow().is_empty(), Some(late));
            let cancelled = Rc::clone(&probe);
            transport.push_read(move || !cancelled.cancels.borrow().is_empty(), None);
            let erased: Rc<dyn UniplexTransport> = transport.clone();
            let (session, _handle) = OwnedH2Session::new(
                HoldingApplication(Rc::clone(&probe)),
                OwnedH2Options {
                    context,
                    transport: erased,
                    io: UniplexConfig {
                        drain_request_body_on_drop: true,
                        ..UniplexConfig::default()
                    },
                    http2: Http2SessionConfig::default(),
                },
            )
            .expect("an owned session");
            let result = tokio::time::timeout(Duration::from_secs(5), session.run())
                .await
                .expect("the session finishes");
            let heads = probe.heads.borrow().clone();
            let cancels = probe.cancels.borrow().clone();
            OwnedOutcome {
                heads,
                cancels,
                run_error: result.err().map(|error| error.code().as_u16()),
                written: transport.written(),
            }
        })
    })
}

/// DATA after the request's END_STREAM, once the handler has read the request
/// head, gets RST_STREAM(STREAM_CLOSED) on both drivers, and the stream's error
/// does not carry a message: the handler has taken the head from the source, and
/// the drivers do not keep a copy of it.
#[test]
fn http2_data_after_end_stream_fails_without_the_read_head() {
    let (head, body) = late_body_wire();
    let late = [body, late_data()].concat();
    let expected = decode(&mut Http2Codec::new(TransportDirection::Downstream), &head);
    let invalid = HttpErrorCode::INVALID_STATE_TRANSITION.as_u16();

    let outcome = run_downstream(
        vec![(0, head.clone()), (1, late.clone())],
        true,
        LATE_STREAMS,
        1,
    );
    assert_eq!(outcome.run_error, None);
    assert_eq!(
        outcome.seen,
        [(
            StreamId::new(1),
            Seen {
                delivered: expected[0].clone(),
                edited_in_place: Some(true),
            },
        )]
    );
    assert_eq!(
        outcome.failures,
        [(
            StreamId::new(1),
            Failure {
                body_error: invalid,
                terminal: Some((invalid, "none".to_owned())),
                shares_probe: None,
            },
        )]
    );
    assert!(contains(&outcome.written, &stream_closed_reset()));

    let owned = run_owned(head, late);
    assert_eq!(owned.run_error, None);
    assert_eq!(owned.heads, [expected[0].clone()]);
    assert_eq!(owned.cancels, [(invalid, "none".to_owned())]);
    assert!(contains(&owned.written, &stream_closed_reset()));
}

/// The same DATA, arriving with the request before the handler reads its head,
/// fails the header read, and the source attaches the head that it still
/// holds to that error.
#[test]
fn http2_data_after_end_stream_fails_with_an_unread_head() {
    let (head, body) = late_body_wire();
    let expected = decode(&mut Http2Codec::new(TransportDirection::Downstream), &head);
    let outcome = run_downstream(
        vec![(0, [head, body, late_data()].concat())],
        true,
        LATE_STREAMS,
        1,
    );
    assert_eq!(outcome.run_error, None);
    assert_eq!(
        outcome.seen,
        [(
            StreamId::new(1),
            Seen {
                delivered: format!(
                    "error {} {}",
                    HttpErrorCode::INVALID_STATE_TRANSITION.as_u16(),
                    expected[0]
                ),
                edited_in_place: None,
            },
        )]
    );
    assert_eq!(outcome.failures, []);
    assert!(contains(&outcome.written, &stream_closed_reset()));
}
