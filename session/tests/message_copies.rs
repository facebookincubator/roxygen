/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Group-168: an HTTP/1 session keeps no copy of an ingress message, and
//! HTTP/2's copy, which only its `INVALID_STATE_TRANSITION` failure reads,
//! shares the delivered message's header entries (expectation-351).

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::rc::Rc;
use std::rc::Weak;
use std::time::Duration;

use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http1::Http1xCodec;
use roxygen_http2::Http2Codec;
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

/// What a handler saw when its header read finished.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Seen {
    /// The delivered message's rendering, or the header-read error's code and
    /// its attached message's rendering.
    delivered: String,
    /// `None` without an `IngressMeta`; otherwise whether it holds a message.
    meta_message: Option<bool>,
    /// Whether the `IngressMeta` copy's probe value has the delivered
    /// message's address.
    shares_probe: Option<bool>,
}

/// What a handler saw when a body read after its headers failed.
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
/// read returned, drains its body, records a failed body read, and answers
/// 200.
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
            let header = request.read_header_event().await;
            let (stream, delivered, message) = match &header {
                Ok(headers) => {
                    let message = headers.headers();
                    (
                        stream_for(streams, Some(message)),
                        render(message),
                        Some(message),
                    )
                }
                Err(error) => {
                    let message = error.http_message();
                    (
                        stream_for(streams, message),
                        format!(
                            "error {} {}",
                            error.code().as_u16(),
                            message.map_or_else(|| "none".to_owned(), render)
                        ),
                        message,
                    )
                }
            };
            let session = probe.session.borrow().upgrade().expect("the session lives");
            let (meta_message, shares_probe) = session
                .borrow()
                .with_ingress_message_for_test(stream, |copy| {
                    let shares = copy
                        .zip(message)
                        .map(|(copy, message)| probe_address(copy) == probe_address(message));
                    (copy.is_some(), shares)
                })
                .map_or((None, None), |(holds, shares)| (Some(holds), shares));
            probe.seen.borrow_mut().push((
                stream,
                Seen {
                    delivered,
                    meta_message,
                    shares_probe,
                },
            ));
            progress.notify_one();
            if let Ok(headers) = &header
                && !headers.eom()
            {
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

/// A 200 with an `x-probe` header, as the server of the protocol sends it in
/// answer to the first request: its head, then its body.
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
            let state = Rc::clone(&session.state);
            let source =
                HttpFixedSource::make_get_request(b"/upstream", None).expect("a fixed GET request");
            let response = send(&handle, source).await;
            let stream = response.stream;
            let mut source = response.source;
            let consume = async move {
                let headers = source
                    .read_header_event()
                    .await
                    .expect("the response headers");
                let delivered = headers.headers();
                let (meta_message, shares_probe) = state
                    .borrow()
                    .with_ingress_message_for_test(stream, |copy| {
                        let shares =
                            copy.map(|copy| probe_address(copy) == probe_address(delivered));
                        (copy.is_some(), shares)
                    })
                    .map_or((None, None), |(holds, shares)| (Some(holds), shares));
                let seen = Seen {
                    delivered: render(delivered),
                    meta_message,
                    shares_probe,
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
                    meta_message: Some(false),
                    shares_probe: None,
                },
            ),
            (
                StreamId::new(2),
                Seen {
                    delivered: expected[1].clone(),
                    meta_message: Some(false),
                    shares_probe: None,
                },
            ),
        ]
    );

    let expected = decode(
        &mut Http1xCodec::new(TransportDirection::Downstream),
        FORBIDDEN,
    );
    // The body arrives after the handler's header read, so the discarded
    // stream's `IngressMeta` is still present when the handler reads it.
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
    assert_eq!(seen.meta_message, Some(false));

    let (seen, expected) = upstream_response(false);
    assert_eq!(
        seen,
        Seen {
            delivered: expected,
            meta_message: Some(false),
            shares_probe: None,
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
fn http2_ingress_copy_shares_the_delivered_header_entries() {
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
    // The forbidden request's DATA arrives after both header reads, so its
    // discarded stream's `IngressMeta` is still present when its handler reads.
    // Its 400 aborts the source that holds the failure's attached message as
    // unread headers, so the header read returns that message on its error.
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
                    meta_message: Some(true),
                    shares_probe: Some(true),
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
                    meta_message: Some(true),
                    shares_probe: Some(true),
                },
            ),
        ]
    );
}

#[test]
fn http2_upstream_ingress_copy_shares_the_delivered_header_entries() {
    let (seen, expected) = upstream_response(true);
    assert_eq!(
        seen,
        Seen {
            delivered: expected,
            meta_message: Some(true),
            shares_probe: Some(true),
        }
    );
}

#[test]
fn http2_state_failure_carries_an_equal_message() {
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
    // Without END_STREAM on the trailers the codec accepts the DATA after them,
    // which the complete stream's source rejects.
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
    // The trailers and late DATA arrive after the handler has read its headers,
    // so no unread header in the source can stand in for the failure's message.
    // The source rejects the DATA with its own error, which carries no message,
    // so the failure's message is read from the stream's terminal error.
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
                meta_message: Some(true),
                shares_probe: Some(true),
            },
        )]
    );
    let invalid = HttpErrorCode::INVALID_STATE_TRANSITION.as_u16();
    assert_eq!(
        outcome.failures,
        [(
            StreamId::new(1),
            Failure {
                body_error: invalid,
                terminal: Some((invalid, expected[0].clone())),
                shares_probe: Some(true),
            },
        )]
    );
}
