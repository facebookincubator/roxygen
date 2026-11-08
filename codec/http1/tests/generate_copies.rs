//! HTTP/1 header generation keeps the bytes, sizes, errors, and codec state
//! recorded from the codec before `repair.session_message_copies`
//! (expectation-349). Each case drives one codec through its headers, a body,
//! an EOM, and the next exchange, and records every result with a snapshot of
//! the egress state. Readable traces pin each base message's header block and
//! the error, message-kind, CR/LF, and websocket cases; FNV-1a digests of the
//! full traces pin every base message under every header variant. A request
//! and a response with many Connection tokens also check that generation
//! sizes its output buffer once.
//!
//! The framing frames (body, chunk header and terminator, trailers, and EOM)
//! keep the chunk-size prefixes of a frozen `format!` reference and the traces
//! recorded before `repair.http1_head_egress` (expectation-432), and allocate
//! no byte buffer beyond an exact-size chunk prefix (expectation-433): none
//! for a chunk shorter than 0x1000. A body frame moves its body into the
//! output with one append, and fails whole when the output lacks room or the
//! frame's length overflows.

use std::fmt::Write as _;

use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::GeneratedFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::Method;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;

use super::Http1xCodec;
use super::MAX_REQUEST_LINE_SUFFIX;
use super::MAX_STATUS_LINE_SUFFIX;
use super::formatted_strings;
use super::websocket_accept;

const QUEUE_LIMIT: usize = 1 << 20;
const DATE: &[u8] = b"Tue, 06 Oct 2026 00:00:00 GMT";
const KEY_PLACEHOLDER: &str = "<generated-key>";
const ACCEPT_PLACEHOLDER: &str = "<derived-accept>";
const GET_REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

/// Renders each ingress event's kind and the fields generation depends on.
#[derive(Default)]
struct Events {
    lines: Vec<String>,
}

impl HttpCodecCallback for Events {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        let line = match event {
            CodecEvent::MessageBegin { stream, .. } => format!("begin {}", stream.get()),
            CodecEvent::HeadersComplete { stream, message } => {
                format!("headers {} {}", stream.get(), render_message(&message))
            }
            CodecEvent::Body { stream, body, .. } => {
                format!("body {} {}", stream.get(), body.chain_length())
            }
            CodecEvent::MessageComplete { stream, upgraded } => {
                format!("complete {} upgraded={upgraded}", stream.get())
            }
            CodecEvent::Error { stream, error, .. } => format!(
                "error {} \"{}\"",
                stream.get(),
                error.message().escape_ascii()
            ),
            _ => "other".to_owned(),
        };
        self.lines.push(line);
    }
}

fn render_message(message: &HttpMessage) -> String {
    let mut rendered = match (message.request_method(), message.response_status()) {
        (Some(method), _) => format!(
            "{method} \"{}\"",
            message.request_url().unwrap_or_default().escape_ascii()
        ),
        (None, Some(status)) => format!("status={status}"),
        (None, None) => "kindless".to_owned(),
    };
    let (major, minor) = message.http_version();
    let _ = write!(
        rendered,
        " {major}.{minor} keepalive={} upgraded={}",
        message.wants_keepalive(),
        message.is_upgraded()
    );
    rendered
}

fn render_result(result: &Result<GeneratedFrame, HttpError>) -> String {
    match result {
        Ok(frame) => format!(
            "ok written={} size={:?} offset={:?}",
            frame.bytes_written,
            frame.header_size.map(|size| (
                size.compressed,
                size.uncompressed,
                size.compressed_block
            )),
            frame.payload_offset
        ),
        Err(error) => format!(
            "error {} \"{}\"",
            error.code().as_u16(),
            error.message().escape_ascii()
        ),
    }
}

fn queue_bytes(queue: &BufQueue) -> Vec<u8> {
    queue.segments().flatten().copied().collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// On an egress websocket upgrade, replaces each `Sec-WebSocket-Key` value
/// that neither `message` nor `extra_headers` supply with a placeholder, after
/// checking that it is the standard base64 encoding of 16 bytes, and returns
/// the last value replaced. Every other value stays.
fn normalize_key(
    bytes: &[u8],
    message: Option<&HttpMessage>,
    extra_headers: Option<&HttpHeaders>,
) -> (String, Option<Vec<u8>>) {
    const PREFIX: &[u8] = b"Sec-WebSocket-Key: ";
    let Some(message) = message.filter(|message| message.is_egress_websocket_upgrade()) else {
        return (bytes.escape_ascii().to_string(), None);
    };
    let supplied: Vec<&[u8]> = message
        .headers()
        .iter()
        .chain(extra_headers.into_iter().flat_map(HttpHeaders::iter))
        .filter(|(code, _, _)| *code == HttpHeaderCode::SecWebSocketKey)
        .map(|(_, _, value)| value)
        .collect();
    let mut rendered = String::new();
    let mut generated = None;
    let mut rest = bytes;
    while let Some(index) = rest
        .windows(PREFIX.len())
        .position(|window| window == PREFIX)
    {
        let start = index + PREFIX.len();
        let end = start
            + rest[start..]
                .iter()
                .position(|byte| *byte == b'\r')
                .expect("a generated header line ends with CRLF");
        let key = &rest[start..end];
        let _ = write!(rendered, "{}", rest[..start].escape_ascii());
        if supplied.contains(&key) {
            let _ = write!(rendered, "{}", key.escape_ascii());
        } else {
            assert!(
                is_base64_of_16_bytes(key),
                "generated key {}",
                key.escape_ascii()
            );
            rendered.push_str(KEY_PLACEHOLDER);
            generated = Some(key.to_vec());
        }
        rest = &rest[end..];
    }
    let _ = write!(rendered, "{}", rest.escape_ascii());
    (rendered, generated)
}

/// The standard base64 encoding of 16 bytes: 22 characters, the last of
/// which encodes 2 data bits and 4 zero bits, then `==`.
fn is_base64_of_16_bytes(key: &[u8]) -> bool {
    key.len() == 24
        && key[..22]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'/'))
        && matches!(key[21], b'A' | b'Q' | b'g' | b'w')
        && &key[22..] == b"=="
}

/// One codec and the trace of every operation on it.
struct Trace {
    codec: Http1xCodec,
    lines: Vec<String>,
    generated_key: Option<Vec<u8>>,
    last_output: Vec<u8>,
}

impl Trace {
    fn new(direction: TransportDirection) -> Self {
        Self {
            codec: Http1xCodec::new(direction),
            lines: Vec::new(),
            generated_key: None,
            last_output: Vec::new(),
        }
    }

    fn ingress(&mut self, wire: &[u8]) {
        let mut events = Events::default();
        let input = BufQueue::from_vec(wire.to_vec(), QUEUE_LIMIT).expect("the wire fits");
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let result = self.codec.on_ingress(&input, &mut events, &mut egress);
        self.lines.push(format!(
            "ingress consumed={} [{}]",
            result.consumed,
            events.lines.join("; ")
        ));
    }

    fn generate(&mut self, label: &str, frame: EgressFrame<'_>) {
        let (message, extra_headers) = match &frame {
            EgressFrame::Headers {
                message,
                extra_headers,
                ..
            } => (Some(*message), *extra_headers),
            _ => (None, None),
        };
        let mut output = BufQueue::new(QUEUE_LIMIT);
        let result = self
            .codec
            .generate(&mut output, frame, &mut Events::default());
        self.last_output = queue_bytes(&output);
        let (bytes, key) = normalize_key(&self.last_output, message, extra_headers);
        if key.is_some() {
            self.generated_key = key;
        }
        self.lines
            .push(format!("{label} {} \"{bytes}\"", render_result(&result)));
    }

    fn headers(
        &mut self,
        stream: StreamId,
        message: &HttpMessage,
        end_stream: bool,
        extra_headers: Option<&HttpHeaders>,
    ) {
        self.generate(
            "headers",
            EgressFrame::Headers {
                stream,
                message,
                end_stream,
                extra_headers,
            },
        );
    }

    fn body(&mut self, stream: StreamId, body: &[u8], end_stream: bool) {
        self.generate(
            "body",
            EgressFrame::Body {
                stream,
                body: BufQueue::from_vec(body.to_vec(), QUEUE_LIMIT).expect("the body fits"),
                padding: None,
                end_stream,
            },
        );
    }

    fn eom(&mut self, stream: StreamId) {
        self.generate("eom", EgressFrame::EndOfMessage { stream });
    }

    fn state(&mut self) {
        let codec = &self.codec;
        let accept = if codec.websocket_accept_key.is_empty() {
            String::new()
        } else if self
            .generated_key
            .as_deref()
            .is_some_and(|key| websocket_accept(key) == codec.websocket_accept_key)
        {
            ACCEPT_PLACEHOLDER.to_owned()
        } else {
            codec.websocket_accept_key.escape_ascii().to_string()
        };
        let flag = u8::from;
        self.lines.push(format!(
            "state streams={}/{} pending={}/{} keepalive={} chunked={}/{}/{} upgrade={}/{} \
             informational={} may_chunk={} no_body={} connect={} head={} http09={} \
             upgrade_header=\"{}\" accept=\"{accept}\" reusable={} busy={}",
            codec.ingress_stream.get(),
            codec.egress_stream.get(),
            flag(codec.request_pending),
            flag(codec.response_pending),
            flag(codec.keepalive),
            flag(codec.egress_chunked),
            flag(codec.in_chunk),
            flag(codec.last_chunk_written),
            flag(codec.ingress_upgrade),
            flag(codec.egress_upgrade),
            flag(codec.informational_response),
            flag(codec.may_chunk_egress),
            flag(codec.expect_no_response_body),
            flag(codec.connect_request),
            flag(codec.head_request),
            flag(codec.expect_http_09_response),
            codec.upgrade_header.escape_ascii(),
            flag(codec.is_reusable()),
            flag(codec.is_busy()),
        ));
    }

    fn render(&self) -> String {
        self.lines.join("\n")
    }
}

fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn request(method: impl Into<Method>, url: Option<&[u8]>, version: (u8, u8)) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(method).expect("a request method");
    if let Some(url) = url {
        message.set_url(url, false).expect("a request URL");
    }
    message.set_http_version(version.0, version.1);
    message
        .headers_mut()
        .add(HttpHeaderCode::Host, b"example.test");
    message
}

fn response(status: u16, reason: Option<&[u8]>, version: (u8, u8)) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).expect("a response status");
    if let Some(reason) = reason {
        message.set_status_message(reason).expect("a reason");
    }
    message.set_http_version(version.0, version.1);
    message.headers_mut().add(HttpHeaderCode::Date, DATE);
    message
}

/// The header variants applied to every base message. The empty Content-Length
/// and Connection variants pin bytes that differ from
/// `HTTP1xCodec::generateHeader`'s (`repair.http1_generate_header_parity`).
#[derive(Clone, Copy, Debug)]
enum Variant {
    Plain,
    Close,
    KeepAlive,
    UpgradeToken,
    CustomTokens,
    ContentLength,
    Chunked,
    NotChunked,
    ExtraHeaders,
    EmptyContentLength,
    EmptyTokens,
    EmptyConnection,
}

const VARIANTS: [Variant; 12] = [
    Variant::Plain,
    Variant::Close,
    Variant::KeepAlive,
    Variant::UpgradeToken,
    Variant::CustomTokens,
    Variant::ContentLength,
    Variant::Chunked,
    Variant::NotChunked,
    Variant::ExtraHeaders,
    Variant::EmptyContentLength,
    Variant::EmptyTokens,
    Variant::EmptyConnection,
];

impl Variant {
    /// Applies the variant to `message` and returns its `extra_headers`.
    fn apply(self, message: &mut HttpMessage) -> Option<HttpHeaders> {
        let headers = message.headers_mut();
        match self {
            Self::Plain => {}
            Self::Close => headers.add(HttpHeaderCode::Connection, b"close"),
            Self::KeepAlive => headers.add(HttpHeaderCode::Connection, b"keep-alive"),
            Self::UpgradeToken => headers.add(HttpHeaderCode::Connection, b"Upgrade"),
            Self::CustomTokens => headers.add(HttpHeaderCode::Connection, b"X-Hop, x-other"),
            Self::ContentLength => headers.add(HttpHeaderCode::ContentLength, b"5"),
            Self::Chunked => {
                headers.add(HttpHeaderCode::TransferEncoding, b"chunked");
                message.set_chunked(true);
            }
            Self::NotChunked => headers.add(HttpHeaderCode::TransferEncoding, b"gzip"),
            Self::ExtraHeaders => {
                let mut extra = HttpHeaders::new();
                extra.add(b"X-Extra", b"one");
                extra.add(HttpHeaderCode::ContentLength, b"5");
                extra.add(HttpHeaderCode::Connection, b"X-Extra-Hop");
                return Some(extra);
            }
            Self::EmptyContentLength => headers.add(HttpHeaderCode::ContentLength, b""),
            Self::EmptyTokens => headers.add(HttpHeaderCode::Connection, b"X-Hop, , x-other,"),
            Self::EmptyConnection => headers.add(HttpHeaderCode::Connection, b""),
        }
        None
    }
}

#[derive(Clone, Copy, Debug)]
enum RequestCase {
    Get,
    Head,
    PostWithLength,
    PostWithoutLength,
    ConnectAuthority,
    ConnectHostFallback,
    WebSocket,
    Http10,
    Http09,
    Http0Dot10,
    Http1Dot10,
    AbsoluteUrl,
    ExtensionMethod,
}

const REQUEST_CASES: [RequestCase; 13] = [
    RequestCase::Get,
    RequestCase::Head,
    RequestCase::PostWithLength,
    RequestCase::PostWithoutLength,
    RequestCase::ConnectAuthority,
    RequestCase::ConnectHostFallback,
    RequestCase::WebSocket,
    RequestCase::Http10,
    RequestCase::Http09,
    RequestCase::Http0Dot10,
    RequestCase::Http1Dot10,
    RequestCase::AbsoluteUrl,
    RequestCase::ExtensionMethod,
];

impl RequestCase {
    fn message(self) -> HttpMessage {
        match self {
            Self::Get => request(HttpMethod::Get, Some(b"/index.html"), (1, 1)),
            Self::Head => request(HttpMethod::Head, Some(b"/index.html"), (1, 1)),
            Self::PostWithLength => {
                let mut message = request(HttpMethod::Post, Some(b"/upload"), (1, 1));
                message
                    .headers_mut()
                    .add(HttpHeaderCode::ContentLength, b"5");
                message
            }
            Self::PostWithoutLength => request(HttpMethod::Post, Some(b"/upload"), (1, 1)),
            Self::ConnectAuthority => {
                request(HttpMethod::Connect, Some(b"example.test:443"), (1, 1))
            }
            Self::ConnectHostFallback => request(HttpMethod::Connect, None, (1, 1)),
            Self::WebSocket => {
                let mut message = request(HttpMethod::Get, Some(b"/chat"), (1, 1));
                message.set_egress_websocket_upgrade();
                message
            }
            Self::Http10 => request(HttpMethod::Get, Some(b"/old"), (1, 0)),
            Self::Http09 => request(HttpMethod::Get, Some(b"/older"), (0, 9)),
            Self::Http0Dot10 => request(HttpMethod::Get, Some(b"/odd"), (0, 10)),
            Self::Http1Dot10 => request(HttpMethod::Get, Some(b"/clamped"), (1, 10)),
            Self::AbsoluteUrl => request(
                HttpMethod::Get,
                Some(b"http://example.test:8080/a/b?c=d"),
                (1, 1),
            ),
            Self::ExtensionMethod => request("PURGE", Some(b"/cache/item"), (1, 1)),
        }
    }
}

/// The base message's header line and the whole trace of one exchange.
struct Recorded {
    head: String,
    trace: String,
}

/// An upstream exchange: the request, a body, an EOM, a response, and the
/// next request, whose bytes show the keep-alive state left behind.
fn request_trace(message: &HttpMessage, extra: Option<&HttpHeaders>) -> Recorded {
    let mut trace = Trace::new(TransportDirection::Upstream);
    let stream = trace.codec.create_stream();
    trace.headers(stream, message, false, extra);
    let head = trace.lines[0].clone();
    trace.state();
    trace.body(stream, b"hello", false);
    trace.eom(stream);
    trace.state();
    trace.ingress(OK_RESPONSE);
    let next = trace.codec.create_stream();
    trace.headers(
        next,
        &request(HttpMethod::Get, Some(b"/next"), (1, 1)),
        true,
        None,
    );
    trace.state();
    Recorded {
        head,
        trace: trace.render(),
    }
}

#[derive(Clone, Copy, Debug)]
enum ResponseCase {
    Ok,
    Continue,
    SwitchingProtocols,
    NoContent,
    NotModified,
    ConnectEstablished,
    HeadResponse,
    Http10KeepAliveRequested,
    Http10KeepAliveNotRequested,
    Http0Dot10,
    Http1Dot10,
    OneDigitStatus,
    TwoDigitStatus,
    FourDigitStatus,
    FiveDigitStatus,
    WebSocketAccept,
    CustomReason,
    EmptyReason,
    Http09Request,
}

const RESPONSE_CASES: [ResponseCase; 19] = [
    ResponseCase::Ok,
    ResponseCase::Continue,
    ResponseCase::SwitchingProtocols,
    ResponseCase::NoContent,
    ResponseCase::NotModified,
    ResponseCase::ConnectEstablished,
    ResponseCase::HeadResponse,
    ResponseCase::Http10KeepAliveRequested,
    ResponseCase::Http10KeepAliveNotRequested,
    ResponseCase::Http0Dot10,
    ResponseCase::Http1Dot10,
    ResponseCase::OneDigitStatus,
    ResponseCase::TwoDigitStatus,
    ResponseCase::FourDigitStatus,
    ResponseCase::FiveDigitStatus,
    ResponseCase::WebSocketAccept,
    ResponseCase::CustomReason,
    ResponseCase::EmptyReason,
    ResponseCase::Http09Request,
];

impl ResponseCase {
    fn request(self) -> &'static [u8] {
        match self {
            Self::Continue => {
                b"POST /upload HTTP/1.1\r\nHost: example.test\r\nExpect: 100-continue\r\n\
                  Content-Length: 5\r\n\r\nhello"
            }
            Self::SwitchingProtocols => {
                b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\n\
                  Upgrade: h2c\r\n\r\n"
            }
            Self::ConnectEstablished => {
                b"CONNECT example.test:443 HTTP/1.1\r\nHost: example.test:443\r\n\r\n"
            }
            Self::HeadResponse => b"HEAD / HTTP/1.1\r\nHost: example.test\r\n\r\n",
            Self::Http10KeepAliveRequested => {
                b"GET / HTTP/1.0\r\nHost: example.test\r\nConnection: keep-alive\r\n\r\n"
            }
            Self::Http10KeepAliveNotRequested => b"GET / HTTP/1.0\r\nHost: example.test\r\n\r\n",
            Self::WebSocketAccept => {
                b"GET /chat HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\n\
                  Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
            }
            Self::Http09Request => b"GET /\r\n",
            _ => GET_REQUEST,
        }
    }

    fn message(self) -> HttpMessage {
        match self {
            Self::Ok | Self::Continue | Self::HeadResponse | Self::Http09Request => {
                response(200, Some(b"OK"), (1, 1))
            }
            Self::SwitchingProtocols => {
                let mut message = response(101, Some(b"Switching Protocols"), (1, 1));
                message.headers_mut().add(HttpHeaderCode::Upgrade, b"h2c");
                message
            }
            Self::NoContent => response(204, Some(b"No Content"), (1, 1)),
            Self::NotModified => response(304, Some(b"Not Modified"), (1, 1)),
            Self::ConnectEstablished => response(200, Some(b"Connection Established"), (1, 1)),
            Self::Http10KeepAliveRequested | Self::Http10KeepAliveNotRequested => {
                response(200, Some(b"OK"), (1, 0))
            }
            Self::Http0Dot10 => response(200, Some(b"OK"), (0, 10)),
            Self::Http1Dot10 => response(200, Some(b"OK"), (1, 10)),
            Self::OneDigitStatus => response(7, None, (1, 1)),
            Self::TwoDigitStatus => response(42, None, (1, 1)),
            Self::FourDigitStatus => response(1000, None, (1, 1)),
            Self::FiveDigitStatus => response(65535, None, (1, 1)),
            Self::WebSocketAccept => {
                let mut message = response(200, Some(b"OK"), (1, 1));
                message.set_egress_websocket_upgrade();
                message
            }
            Self::CustomReason => response(200, Some(b"Totally Fine"), (1, 1)),
            Self::EmptyReason => response(200, None, (1, 1)),
        }
    }
}

/// A downstream exchange: the request, a 100 Continue first when the request
/// expects one, the response, a body, an EOM, and the next exchange, whose
/// bytes show the keep-alive state left behind.
fn response_trace(
    case: ResponseCase,
    message: &HttpMessage,
    extra: Option<&HttpHeaders>,
) -> Recorded {
    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.ingress(case.request());
    let stream = StreamId::new(1);
    if matches!(case, ResponseCase::Continue) {
        trace.headers(
            stream,
            &response(100, Some(b"Continue"), (1, 1)),
            false,
            None,
        );
        trace.state();
    }
    trace.headers(stream, message, false, extra);
    let head = trace.lines.last().expect("a header line").clone();
    trace.state();
    trace.body(stream, b"hello", false);
    trace.eom(stream);
    trace.state();
    trace.ingress(GET_REQUEST);
    let mut next = response(200, Some(b"OK"), (1, 1));
    next.headers_mut().add(HttpHeaderCode::ContentLength, b"0");
    trace.headers(StreamId::new(2), &next, true, None);
    trace.state();
    Recorded {
        head,
        trace: trace.render(),
    }
}

/// A Connection value of 128 one-byte tokens, which generation rejoins with
/// `", "`.
fn many_tokens() -> Vec<u8> {
    vec![b"x".as_slice(); 128].join(b",".as_slice())
}

/// Generates `message` through `generate_headers` and checks that the output
/// buffer still has the capacity it was created with, which `Vec::with_capacity`
/// makes exactly the requested one, so the buffer never regrew.
fn generate_sized_once(
    codec: &mut Http1xCodec,
    stream: StreamId,
    message: &HttpMessage,
    extra_headers: Option<&HttpHeaders>,
    start_line: usize,
) -> String {
    let capacity = start_line + codec.header_capacity(message, extra_headers, true);
    let (output, _) = codec
        .generate_headers(message, stream, true, extra_headers)
        .expect("generated headers");
    assert_eq!(output.capacity(), capacity, "{}", output.escape_ascii());
    output.escape_ascii().to_string()
}

/// A request and a response whose Connection header, ordinary or in
/// `extra_headers`, has more tokens than its own bytes leave room for once
/// they are rejoined.
fn connection_token_heads(direction: TransportDirection) -> Vec<String> {
    [false, true]
        .into_iter()
        .map(|in_extra| {
            let mut codec = Http1xCodec::new(direction);
            let (stream, mut message, start_line) = if direction.is_upstream() {
                let stream = codec.create_stream();
                let message = request(HttpMethod::Get, Some(b"/tokens"), (1, 1));
                (
                    stream,
                    message,
                    b"GET".len() + b"/tokens".len() + MAX_REQUEST_LINE_SUFFIX,
                )
            } else {
                let mut events = Events::default();
                let input =
                    BufQueue::from_vec(GET_REQUEST.to_vec(), QUEUE_LIMIT).expect("the wire fits");
                let _ = codec.on_ingress(&input, &mut events, &mut BufQueue::new(QUEUE_LIMIT));
                let mut message = response(200, Some(b"OK"), (1, 1));
                message
                    .headers_mut()
                    .add(HttpHeaderCode::ContentLength, b"0");
                (
                    StreamId::new(1),
                    message,
                    b"OK".len() + MAX_STATUS_LINE_SUFFIX,
                )
            };
            let mut extra = HttpHeaders::new();
            if in_extra {
                extra.add(HttpHeaderCode::Connection, many_tokens());
            } else {
                message
                    .headers_mut()
                    .add(HttpHeaderCode::Connection, many_tokens());
            }
            generate_sized_once(
                &mut codec,
                stream,
                &message,
                in_extra.then_some(&extra),
                start_line,
            )
        })
        .collect()
}

fn check_trace(name: &str, actual: &str, expected: &str) {
    assert!(
        actual == expected,
        "{name}: the trace differs from the recorded baseline\nactual:\n{actual}\nexpected:\n{expected}"
    );
}

fn check_traces(test: &str, actual: &[String], expected: &[&[&str]]) {
    assert_eq!(actual.len(), expected.len(), "{test}: trace count");
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        check_trace(
            &format!("{test} trace {index}"),
            actual,
            &expected.join("\n"),
        );
    }
}

fn check_digests(test: &str, actual: &[(String, u64, String)], expected: &[(&str, u64)]) {
    let mismatches: Vec<String> = actual
        .iter()
        .enumerate()
        .filter(|(index, (name, digest, _))| {
            expected.get(*index) != Some(&(name.as_str(), *digest))
        })
        .map(|(_, (name, digest, trace))| format!("(\"{name}\", {digest:#018x}):\n{trace}"))
        .collect();
    assert!(
        mismatches.is_empty() && actual.len() == expected.len(),
        "{test}: {} of {} traces differ from the recorded baseline:\n{}",
        mismatches.len(),
        actual.len(),
        mismatches.join("\n")
    );
}

#[test]
fn generated_requests_match_the_recorded_baseline() {
    for (case, expected) in REQUEST_CASES.into_iter().zip(REQUEST_HEADS) {
        check_trace(
            &format!("{case:?}"),
            &request_trace(&case.message(), None).head,
            expected,
        );
    }
    let digests: Vec<_> = REQUEST_CASES
        .into_iter()
        .flat_map(|case| VARIANTS.into_iter().map(move |variant| (case, variant)))
        .map(|(case, variant)| {
            let mut message = case.message();
            let extra = variant.apply(&mut message);
            let trace = request_trace(&message, extra.as_ref()).trace;
            (format!("{case:?}/{variant:?}"), fnv1a(&trace), trace)
        })
        .collect();
    check_digests("requests", &digests, REQUEST_DIGESTS);

    let tokens = vec!["x"; 128].join(", ");
    let expected = format!(
        "GET /tokens HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: {tokens}, keep-alive\\r\\n\\r\\n"
    );
    assert_eq!(
        connection_token_heads(TransportDirection::Upstream),
        [expected.clone(), expected]
    );
}

#[test]
fn generated_responses_match_the_recorded_baseline() {
    for (case, expected) in RESPONSE_CASES.into_iter().zip(RESPONSE_HEADS) {
        check_trace(
            &format!("{case:?}"),
            &response_trace(case, &case.message(), None).head,
            expected,
        );
    }
    let digests: Vec<_> = RESPONSE_CASES
        .into_iter()
        .flat_map(|case| VARIANTS.into_iter().map(move |variant| (case, variant)))
        .map(|(case, variant)| {
            let mut message = case.message();
            let extra = variant.apply(&mut message);
            let trace = response_trace(case, &message, extra.as_ref()).trace;
            (format!("{case:?}/{variant:?}"), fnv1a(&trace), trace)
        })
        .collect();
    check_digests("responses", &digests, RESPONSE_DIGESTS);

    let tokens = vec!["x"; 128].join(", ");
    let expected = format!(
        "HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: {tokens}, \
         keep-alive\\r\\nContent-Length: 0\\r\\n\\r\\n"
    );
    assert_eq!(
        connection_token_heads(TransportDirection::Downstream),
        [expected.clone(), expected]
    );

    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.ingress(GET_REQUEST);
    let mut message = HttpMessage::new();
    message.set_status_code(200).expect("a response status");
    message.set_http_version(1, 1);
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentLength, b"0");
    trace.headers(StreamId::new(1), &message, true, None);
    let bytes = String::from_utf8(trace.last_output.clone()).expect("an ASCII response");
    let date = bytes
        .split("\r\n")
        .find_map(|line| line.strip_prefix("Date: "))
        .expect("a generated Date line");
    let parts: Vec<&str> = date.split(' ').collect();
    assert_eq!(date.len(), 29, "{date}");
    assert!(date.ends_with(" GMT"), "{date}");
    assert!(
        ["Mon,", "Tue,", "Wed,", "Thu,", "Fri,", "Sat,", "Sun,"].contains(&parts[0]),
        "{date}"
    );
    assert!(
        [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]
        .contains(&parts[2]),
        "{date}"
    );
    assert!(
        parts[1].len() == 2
            && parts[3].len() == 4
            && parts[4].len() == 8
            && [parts[1], parts[3]]
                .iter()
                .all(|part| part.bytes().all(|byte| byte.is_ascii_digit())),
        "{date}"
    );
    assert_eq!(
        bytes.replace(date, "<date>"),
        "HTTP/1.1 200 \r\nDate: <date>\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"
    );
}

fn error_traces() -> Vec<String> {
    let mut traces = Vec::new();

    let mut trace = Trace::new(TransportDirection::Upstream);
    let stream = trace.codec.create_stream();
    trace.state();
    trace.headers(
        StreamId::new(2),
        &request(HttpMethod::Get, Some(b"/"), (1, 1)),
        false,
        None,
    );
    trace.state();
    let mut no_method = HttpMessage::new();
    no_method.set_url(b"/no-method", false).expect("a URL");
    trace.headers(stream, &no_method, false, None);
    trace.state();
    trace.headers(
        stream,
        &request(HttpMethod::Post, Some(b"/"), (1, 1)),
        false,
        None,
    );
    trace.state();
    traces.push(trace.render());

    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.headers(
        StreamId::new(1),
        &response(200, Some(b"OK"), (1, 1)),
        true,
        None,
    );
    trace.state();
    trace.ingress(GET_REQUEST);
    trace.headers(
        StreamId::new(2),
        &response(200, Some(b"OK"), (1, 1)),
        true,
        None,
    );
    trace.state();
    let mut no_status = HttpMessage::new();
    no_status
        .set_status_message(b"Missing")
        .expect("a reason selects a response");
    trace.headers(StreamId::new(1), &no_status, true, None);
    trace.state();
    trace.generate(
        "goaway",
        EgressFrame::Goaway {
            last_stream: StreamId::MAX,
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        },
    );
    trace.headers(StreamId::new(0), &response(200, None, (1, 1)), true, None);
    trace.state();
    trace.headers(
        StreamId::new(1),
        &response(200, Some(b"OK"), (1, 1)),
        true,
        None,
    );
    trace.state();
    trace.headers(
        StreamId::new(1),
        &response(200, Some(b"OK"), (1, 1)),
        true,
        None,
    );
    trace.state();
    traces.push(trace.render());
    traces
}

#[test]
fn generate_errors_keep_their_state_changes() {
    check_traces("error", &error_traces(), &ERROR_TRACES);
}

fn kind_traces() -> Vec<String> {
    let mut traces = Vec::new();
    for direction in [TransportDirection::Upstream, TransportDirection::Downstream] {
        for wrong_kind in [false, true] {
            for websocket in [false, true] {
                let mut message = match (direction.is_upstream(), wrong_kind) {
                    (_, false) => HttpMessage::new(),
                    (true, true) => response(200, Some(b"OK"), (1, 1)),
                    (false, true) => request(HttpMethod::Get, Some(b"/"), (1, 1)),
                };
                if websocket {
                    message.set_egress_websocket_upgrade();
                }
                if direction.is_downstream() {
                    message.headers_mut().set(HttpHeaderCode::Date, DATE);
                }
                let mut trace = Trace::new(direction);
                let stream = if direction.is_upstream() {
                    trace.codec.create_stream()
                } else {
                    trace.ingress(GET_REQUEST);
                    StreamId::new(1)
                };
                trace.headers(stream, &message, false, None);
                trace.state();
                trace.eom(stream);
                trace.state();
                traces.push(trace.render());
            }
        }
    }
    traces
}

#[test]
fn kindless_and_wrong_kind_messages_keep_their_results() {
    check_traces("message kind", &kind_traces(), &KIND_TRACES);
}

fn cr_lf_traces() -> Vec<String> {
    let mut traces = Vec::new();

    let mut message = request(HttpMethod::Post, Some(b"/a\r\nInjected: url"), (1, 1));
    let headers = message.headers_mut();
    headers.add(b"X-Dropped", b"before\r\nInjected: header");
    headers.add(b"X-Kept", b"kept");
    headers.add(HttpHeaderCode::ContentLength, b"5\r\nInjected: length");
    headers.add(
        HttpHeaderCode::Connection,
        b"x-hop\r\nInjected: token, close",
    );
    let mut extra = HttpHeaders::new();
    extra.add(b"X-Extra-Dropped", b"before\nInjected: extra");
    extra.add(b"X-Extra-Kept", b"kept");
    let mut trace = Trace::new(TransportDirection::Upstream);
    let stream = trace.codec.create_stream();
    trace.headers(stream, &message, false, Some(&extra));
    trace.state();
    traces.push(trace.render());

    let mut message = request(HttpMethod::Connect, None, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::Host, b"proxy\r\nInjected: host:443");
    let mut trace = Trace::new(TransportDirection::Upstream);
    let stream = trace.codec.create_stream();
    trace.headers(stream, &message, false, None);
    trace.state();
    traces.push(trace.render());

    for server_upgrade in [b"h2c".as_slice(), b"websocket"] {
        let mut message = request(HttpMethod::Get, Some(b"/"), (1, 1));
        message
            .headers_mut()
            .add(HttpHeaderCode::Upgrade, b"h2c,\r\nInjected: upgrade");
        message
            .headers_mut()
            .add(HttpHeaderCode::Connection, b"Upgrade");
        let mut trace = Trace::new(TransportDirection::Upstream);
        let stream = trace.codec.create_stream();
        trace.headers(stream, &message, true, None);
        trace.state();
        let mut switching =
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: ".to_vec();
        switching.extend_from_slice(server_upgrade);
        switching.extend_from_slice(b"\r\n\r\n");
        trace.ingress(&switching);
        trace.state();
        traces.push(trace.render());
    }

    let mut message = request(HttpMethod::Get, Some(b"/chat"), (1, 1));
    message.set_egress_websocket_upgrade();
    message
        .headers_mut()
        .add(HttpHeaderCode::Upgrade, b"websocket\r\nInjected: upgrade");
    let mut trace = Trace::new(TransportDirection::Upstream);
    let stream = trace.codec.create_stream();
    trace.headers(stream, &message, true, None);
    trace.state();
    traces.push(trace.render());

    let mut message = response(200, Some(b"OK\r\nInjected: reason"), (1, 1));
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentLength, b"0");
    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.ingress(GET_REQUEST);
    trace.headers(StreamId::new(1), &message, true, None);
    trace.state();
    traces.push(trace.render());

    let mut message = HttpMessage::new();
    message.set_status_code(204).expect("a response status");
    message
        .headers_mut()
        .add(HttpHeaderCode::Date, b"Tue\r\nInjected: date");
    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.ingress(GET_REQUEST);
    trace.headers(StreamId::new(1), &message, true, None);
    trace.state();
    assert!(!contains(&trace.last_output, b"Date"));
    traces.push(trace.render());

    let mut message = response(200, Some(b"OK"), (1, 1));
    message.set_egress_websocket_upgrade();
    message
        .headers_mut()
        .add(HttpHeaderCode::Upgrade, b"websocket\nInjected: upgrade");
    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.ingress(ResponseCase::WebSocketAccept.request());
    trace.headers(StreamId::new(1), &message, false, None);
    trace.state();
    traces.push(trace.render());

    let mut message = request("GET\r\nInjected: method", Some(b"/"), (1, 1));
    message.headers_mut().add(b"X-Name\r\nInjected", b"name");
    let mut extra = HttpHeaders::new();
    extra.add(b"X-Extra-Name\nInjected", b"extra");
    let mut trace = Trace::new(TransportDirection::Upstream);
    let stream = trace.codec.create_stream();
    trace.headers(stream, &message, true, Some(&extra));
    trace.state();
    traces.push(trace.render());

    let mut message = response(200, Some(b"OK"), (1, 1));
    let headers = message.headers_mut();
    headers.add(HttpHeaderCode::ContentLength, b"0");
    headers.add(b"X-Name\r\nInjected", b"name");
    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.ingress(GET_REQUEST);
    trace.headers(StreamId::new(1), &message, true, Some(&extra));
    trace.state();
    traces.push(trace.render());

    traces
}

#[test]
fn cr_and_lf_keep_their_handling_on_every_serialization_path() {
    check_traces("CR/LF", &cr_lf_traces(), &CR_LF_TRACES);
}

fn websocket_traces() -> Vec<String> {
    let mut traces = Vec::new();
    for derived in [true, false] {
        let mut message = request(HttpMethod::Get, Some(b"/chat"), (1, 1));
        message.set_egress_websocket_upgrade();
        let mut trace = Trace::new(TransportDirection::Upstream);
        let stream = trace.codec.create_stream();
        trace.headers(stream, &message, false, None);
        trace.state();
        let key = trace
            .generated_key
            .clone()
            .expect("the request carries a generated key");
        let accept = if derived {
            websocket_accept(&key)
        } else {
            b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".to_vec()
        };
        let mut switching = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
            Connection: Upgrade\r\nSec-WebSocket-Accept: "
            .to_vec();
        switching.extend_from_slice(&accept);
        switching.extend_from_slice(b"\r\n\r\n");
        trace.ingress(&switching);
        trace.state();
        traces.push(trace.render());
    }

    let mut trace = Trace::new(TransportDirection::Downstream);
    trace.ingress(ResponseCase::WebSocketAccept.request());
    let mut message = response(200, Some(b"OK"), (1, 1));
    message.set_egress_websocket_upgrade();
    trace.headers(StreamId::new(1), &message, false, None);
    assert!(contains(
        &trace.last_output,
        b"\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"
    ));
    trace.state();
    traces.push(trace.render());

    for websocket in [false, true] {
        let mut message = request(HttpMethod::Get, Some(b"/keys"), (1, 1));
        if websocket {
            message.set_egress_websocket_upgrade();
        }
        message
            .headers_mut()
            .add(HttpHeaderCode::SecWebSocketKey, b"dGhlIHNhbXBsZSBub25jZQ==");
        let mut extra = HttpHeaders::new();
        extra.add(HttpHeaderCode::SecWebSocketKey, b"AAAAAAAAAAAAAAAAAAAAAA==");
        let mut trace = Trace::new(TransportDirection::Upstream);
        let stream = trace.codec.create_stream();
        trace.headers(stream, &message, false, Some(&extra));
        trace.state();
        traces.push(trace.render());
    }

    traces
}

#[test]
fn generated_websocket_keys_derive_the_accepted_value() {
    check_traces("websocket", &websocket_traces(), &WEBSOCKET_TRACES);
}

// Recorded from `generate_headers` before `repair.session_message_copies`.

const REQUEST_HEADS: [&str; 13] = [
    // Get
    "headers ok written=72 size=Some((72, 72, 0)) offset=None \"GET /index.html HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // Head
    "headers ok written=73 size=Some((73, 73, 0)) offset=None \"HEAD /index.html HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // PostWithLength
    "headers ok written=88 size=Some((88, 88, 0)) offset=None \"POST /upload HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\nContent-Length: 5\\r\\n\\r\\n\"",
    // PostWithoutLength
    "headers ok written=97 size=Some((97, 97, 0)) offset=None \"POST /upload HTTP/1.1\\r\\nHost: example.test\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // ConnectAuthority
    "headers ok written=81 size=Some((81, 81, 0)) offset=None \"CONNECT example.test:443 HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // ConnectHostFallback
    "headers ok written=77 size=Some((77, 77, 0)) offset=None \"CONNECT example.test HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // WebSocket
    "headers ok written=140 size=Some((140, 140, 0)) offset=None \"GET /chat HTTP/1.1\\r\\nHost: example.test\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Key: <generated-key>\\r\\nConnection: Upgrade, keep-alive\\r\\n\\r\\n\"",
    // Http10
    "headers ok written=65 size=Some((65, 65, 0)) offset=None \"GET /old HTTP/1.0\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // Http09
    "headers ok written=12 size=Some((12, 12, 0)) offset=None \"GET /older\\r\\n\"",
    // Http0Dot10
    "headers ok written=61 size=Some((61, 61, 0)) offset=None \"GET /odd HTTP/0.10\\r\\nHost: example.test\\r\\nConnection: close\\r\\n\\r\\n\"",
    // Http1Dot10
    "headers ok written=69 size=Some((69, 69, 0)) offset=None \"GET /clamped HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // AbsoluteUrl
    "headers ok written=93 size=Some((93, 93, 0)) offset=None \"GET http://example.test:8080/a/b?c=d HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // ExtensionMethod
    "headers ok written=74 size=Some((74, 74, 0)) offset=None \"PURGE /cache/item HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
];

const RESPONSE_HEADS: [&str; 19] = [
    // Ok
    "headers ok written=108 size=Some((108, 108, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // Continue
    "headers ok written=108 size=Some((108, 108, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // SwitchingProtocols
    "headers ok written=87 size=Some((87, 87, 0)) offset=None \"HTTP/1.1 101 Switching Protocols\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nUpgrade: h2c\\r\\n\\r\\n\"",
    // NoContent
    "headers ok written=88 size=Some((88, 88, 0)) offset=None \"HTTP/1.1 204 No Content\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // NotModified
    "headers ok written=90 size=Some((90, 90, 0)) offset=None \"HTTP/1.1 304 Not Modified\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // ConnectEstablished
    "headers ok written=95 size=Some((95, 95, 0)) offset=None \"HTTP/1.1 200 Connection Established\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: close\\r\\n\\r\\n\"",
    // HeadResponse
    "headers ok written=80 size=Some((80, 80, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // Http10KeepAliveRequested
    "headers ok written=75 size=Some((75, 75, 0)) offset=None \"HTTP/1.0 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: close\\r\\n\\r\\n\"",
    // Http10KeepAliveNotRequested
    "headers ok written=75 size=Some((75, 75, 0)) offset=None \"HTTP/1.0 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: close\\r\\n\\r\\n\"",
    // Http0Dot10
    "headers ok written=104 size=Some((104, 104, 0)) offset=None \"HTTP/0.10 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: close\\r\\n\\r\\n\"",
    // Http1Dot10
    "headers ok written=108 size=Some((108, 108, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // OneDigitStatus
    "headers ok written=104 size=Some((104, 104, 0)) offset=None \"HTTP/1.1 7 \\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // TwoDigitStatus
    "headers ok written=105 size=Some((105, 105, 0)) offset=None \"HTTP/1.1 42 \\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // FourDigitStatus
    "headers ok written=107 size=Some((107, 107, 0)) offset=None \"HTTP/1.1 1000 \\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // FiveDigitStatus
    "headers ok written=108 size=Some((108, 108, 0)) offset=None \"HTTP/1.1 65535 \\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // WebSocketAccept
    "headers ok written=166 size=Some((166, 166, 0)) offset=None \"HTTP/1.1 101 Switching Protocols\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\nConnection: Upgrade\\r\\n\\r\\n\"",
    // CustomReason
    "headers ok written=118 size=Some((118, 118, 0)) offset=None \"HTTP/1.1 200 Totally Fine\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // EmptyReason
    "headers ok written=106 size=Some((106, 106, 0)) offset=None \"HTTP/1.1 200 \\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
    // Http09Request
    "headers ok written=75 size=Some((75, 75, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: close\\r\\n\\r\\n\"",
];

const ERROR_TRACES: [&[&str]; 2] = [
    &[
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
        "headers error 4096 \"HTTP/1 request stream is out of order\" \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
        "headers error 4096 \"HTTP/1 request requires a method\" \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
        "headers ok written=91 size=Some((91, 91, 0)) offset=None \"POST / HTTP/1.1\\r\\nHost: example.test\\r\\nTransfer-Encoding: chunked\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=1 chunked=1/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
    ],
    &[
        "headers error 4096 \"HTTP/1 response stream is out of order\" \"\"",
        "state streams=0/0 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers error 4096 \"HTTP/1 response stream is out of order\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
        "headers error 4096 \"HTTP/1 response requires a status\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
        "goaway ok written=0 size=None offset=None \"\"",
        "headers error 4096 \"HTTP/1 response stream is out of order\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
        "headers ok written=108 size=Some((108, 108, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nTransfer-Encoding: chunked\\r\\nConnection: close\\r\\n\\r\\n0\\r\\n\\r\\n\"",
        "state streams=1/1 pending=0/0 keepalive=0 chunked=1/0/1 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=0",
        "headers error 4096 \"HTTP/1 response stream is out of order\" \"\"",
        "state streams=1/1 pending=0/0 keepalive=0 chunked=1/0/1 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=0",
    ],
];

const KIND_TRACES: [&[&str]; 8] = [
    &[
        "headers error 4096 \"HTTP/1 request requires a method\" \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
        "eom ok written=0 size=None offset=None \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
    ],
    &[
        "headers ok written=115 size=Some((115, 115, 0)) offset=None \"GET  HTTP/1.0\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Key: <generated-key>\\r\\nConnection: Upgrade, keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=1 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"websocket\" accept=\"<derived-accept>\" reusable=0 busy=1",
        "eom ok written=0 size=None offset=None \"\"",
        "state streams=0/1 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"websocket\" accept=\"<derived-accept>\" reusable=0 busy=1",
    ],
    &[
        "headers error 4096 \"HTTP/1 request requires a method\" \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
        "eom ok written=0 size=None offset=None \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
    ],
    &[
        "headers error 4096 \"HTTP/1 request requires a URL\" \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
        "eom ok written=0 size=None offset=None \"\"",
        "state streams=0/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=0 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
    ],
    &[
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers error 4096 \"HTTP/1 response requires a status\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
        "eom error 4096 \"HTTP/1 EOM stream is out of order\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
    ],
    &[
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers ok written=138 size=Some((138, 138, 0)) offset=None \"HTTP/1.0 101 Switching Protocols\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Accept: \\r\\nConnection: Upgrade\\r\\n\\r\\n\"",
        "state streams=1/1 pending=0/1 keepalive=0 chunked=0/0/0 upgrade=1/1 informational=1 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=1",
        "eom ok written=0 size=None offset=None \"\"",
        "state streams=1/1 pending=0/0 keepalive=0 chunked=0/0/0 upgrade=1/1 informational=1 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=0",
    ],
    &[
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers error 4096 \"HTTP/1 response requires a status\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
        "eom error 4096 \"HTTP/1 EOM stream is out of order\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
    ],
    &[
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers error 4096 \"HTTP/1 response requires a status\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
        "eom error 4096 \"HTTP/1 EOM stream is out of order\" \"\"",
        "state streams=1/0 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
    ],
];

const CR_LF_TRACES: [&[&str]; 10] = [
    &[
        "headers ok written=169 size=Some((169, 169, 0)) offset=None \"POST /a\\r\\nInjected: url HTTP/1.1\\r\\nHost: example.test\\r\\nX-Kept: kept\\r\\nX-Extra-Kept: kept\\r\\nConnection: x-hop\\r\\nInjected: token, close\\r\\nContent-Length: 5\\r\\nInjected: length\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=0 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=1",
    ],
    &[
        "headers ok written=70 size=Some((70, 70, 0)) offset=None \"CONNECT proxy\\r\\nInjected: host:443 HTTP/1.1\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=1 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=1 no_body=1 connect=1 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=1",
    ],
    &[
        "headers ok written=71 size=Some((71, 71, 0)) offset=None \"GET / HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: Upgrade, keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"h2c,\\r\\nInjected: upgrade\" accept=\"\" reusable=1 busy=1",
        "ingress consumed=71 [begin 1; headers 1 status=101 1.1 keepalive=true upgraded=true; complete 1 upgraded=true]",
        "state streams=1/1 pending=0/1 keepalive=0 chunked=0/0/0 upgrade=1/1 informational=1 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=1",
    ],
    &[
        "headers ok written=71 size=Some((71, 71, 0)) offset=None \"GET / HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: Upgrade, keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"h2c,\\r\\nInjected: upgrade\" accept=\"\" reusable=1 busy=1",
        "ingress consumed=77 [begin 1; error 1 \"upgrade protocol mismatch\"]",
        "state streams=1/1 pending=0/1 keepalive=0 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"h2c,\\r\\nInjected: upgrade\" accept=\"\" reusable=0 busy=1",
    ],
    &[
        "headers ok written=66 size=Some((66, 66, 0)) offset=None \"GET /chat HTTP/1.1\\r\\nHost: example.test\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"websocket\\r\\nInjected: upgrade\" accept=\"\" reusable=0 busy=1",
    ],
    &[
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers ok written=117 size=Some((117, 117, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nInjected: reason\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nConnection: keep-alive\\r\\nContent-Length: 0\\r\\n\\r\\n\"",
        "state streams=1/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
    ],
    &[
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers ok written=36 size=Some((36, 36, 0)) offset=None \"HTTP/1.0 204 \\r\\nConnection: close\\r\\n\\r\\n\"",
        "state streams=1/1 pending=0/0 keepalive=0 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=1 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=0 busy=0",
    ],
    &[
        "ingress consumed=155 [begin 1; headers 1 GET \"/chat\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers ok written=73 size=Some((73, 73, 0)) offset=None \"HTTP/1.1 101 Switching Protocols\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\n\\r\\n\"",
        "state streams=1/1 pending=0/1 keepalive=0 chunked=0/0/0 upgrade=1/1 informational=1 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\" reusable=0 busy=1",
    ],
    &[
        "headers ok written=134 size=Some((134, 134, 0)) offset=None \"GET\\r\\nINJECTED: METHOD / HTTP/1.1\\r\\nHost: example.test\\r\\nX-Name\\r\\nInjected: name\\r\\nX-Extra-Name\\nInjected: extra\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=0/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
    ],
    &[
        "ingress consumed=38 [begin 1; headers 1 GET \"/\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers ok written=153 size=Some((153, 153, 0)) offset=None \"HTTP/1.1 200 OK\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nX-Name\\r\\nInjected: name\\r\\nX-Extra-Name\\nInjected: extra\\r\\nConnection: keep-alive\\r\\nContent-Length: 0\\r\\n\\r\\n\"",
        "state streams=1/1 pending=0/0 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=0",
    ],
];

const WEBSOCKET_TRACES: [&[&str]; 5] = [
    &[
        "headers ok written=140 size=Some((140, 140, 0)) offset=None \"GET /chat HTTP/1.1\\r\\nHost: example.test\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Key: <generated-key>\\r\\nConnection: Upgrade, keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=1 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"websocket\" accept=\"<derived-accept>\" reusable=0 busy=1",
        "ingress consumed=129 [begin 1; headers 1 status=101 1.1 keepalive=true upgraded=true; complete 1 upgraded=true]",
        "state streams=1/1 pending=1/1 keepalive=0 chunked=0/0/0 upgrade=1/1 informational=1 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"<derived-accept>\" reusable=0 busy=1",
    ],
    &[
        "headers ok written=140 size=Some((140, 140, 0)) offset=None \"GET /chat HTTP/1.1\\r\\nHost: example.test\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Key: <generated-key>\\r\\nConnection: Upgrade, keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=1 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"websocket\" accept=\"<derived-accept>\" reusable=0 busy=1",
        "ingress consumed=129 [begin 1; error 1 \"invalid websocket accept key\"]",
        "state streams=1/1 pending=1/1 keepalive=0 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"websocket\" accept=\"<derived-accept>\" reusable=0 busy=1",
    ],
    &[
        "ingress consumed=155 [begin 1; headers 1 GET \"/chat\" 1.1 keepalive=true upgraded=false; complete 1 upgraded=false]",
        "headers ok written=166 size=Some((166, 166, 0)) offset=None \"HTTP/1.1 101 Switching Protocols\\r\\nDate: Tue, 06 Oct 2026 00:00:00 GMT\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\nConnection: Upgrade\\r\\n\\r\\n\"",
        "state streams=1/1 pending=0/1 keepalive=0 chunked=0/0/0 upgrade=1/1 informational=1 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\" reusable=0 busy=1",
    ],
    &[
        "headers ok written=156 size=Some((156, 156, 0)) offset=None \"GET /keys HTTP/1.1\\r\\nHost: example.test\\r\\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\\r\\nSec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\\r\\nConnection: keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=1 chunked=0/0/0 upgrade=0/0 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"\" accept=\"\" reusable=1 busy=1",
    ],
    &[
        "headers ok written=140 size=Some((140, 140, 0)) offset=None \"GET /keys HTTP/1.1\\r\\nHost: example.test\\r\\nUpgrade: websocket\\r\\nSec-WebSocket-Key: <generated-key>\\r\\nConnection: Upgrade, keep-alive\\r\\n\\r\\n\"",
        "state streams=0/1 pending=1/1 keepalive=1 chunked=0/0/0 upgrade=0/1 informational=0 may_chunk=1 no_body=0 connect=0 head=0 http09=0 upgrade_header=\"websocket\" accept=\"<derived-accept>\" reusable=0 busy=1",
    ],
];

const REQUEST_DIGESTS: &[(&str, u64)] = &[
    ("Get/Plain", 0xa3200b132996b8c4),
    ("Get/Close", 0x82093d09e5fadc89),
    ("Get/KeepAlive", 0xa3200b132996b8c4),
    ("Get/UpgradeToken", 0x509cc52adf259270),
    ("Get/CustomTokens", 0xb2f5efbdf956b202),
    ("Get/ContentLength", 0x711b848d23ca25ea),
    ("Get/Chunked", 0xd1424ca0f9a7773d),
    ("Get/NotChunked", 0xa3200b132996b8c4),
    ("Get/ExtraHeaders", 0xe53ec5ae15bcda04),
    ("Get/EmptyContentLength", 0x50aede188f0d805a),
    ("Get/EmptyTokens", 0xb2f5efbdf956b202),
    ("Get/EmptyConnection", 0xdc7859cc30583a20),
    ("Head/Plain", 0x62eb05eb2769a53b),
    ("Head/Close", 0xf9915189f574001c),
    ("Head/KeepAlive", 0x62eb05eb2769a53b),
    ("Head/UpgradeToken", 0x958272b1d155391f),
    ("Head/CustomTokens", 0x99dc7caa5c40473b),
    ("Head/ContentLength", 0x0d1d4292a6e9948b),
    ("Head/Chunked", 0xb754287cd51fd732),
    ("Head/NotChunked", 0x62eb05eb2769a53b),
    ("Head/ExtraHeaders", 0x595fc13c3277b5df),
    ("Head/EmptyContentLength", 0x2cdc0c71ecb74251),
    ("Head/EmptyTokens", 0x99dc7caa5c40473b),
    ("Head/EmptyConnection", 0x7bb0857372b5b881),
    ("PostWithLength/Plain", 0x0c3c40d4712c6c0a),
    ("PostWithLength/Close", 0x1b7436f9d9b8a9c0),
    ("PostWithLength/KeepAlive", 0x0c3c40d4712c6c0a),
    ("PostWithLength/UpgradeToken", 0x7542e644f1f6822e),
    ("PostWithLength/CustomTokens", 0x6c9df4799d6486a8),
    ("PostWithLength/ContentLength", 0x0c3c40d4712c6c0a),
    ("PostWithLength/Chunked", 0xa923596b36caefcd),
    ("PostWithLength/NotChunked", 0x0c3c40d4712c6c0a),
    ("PostWithLength/ExtraHeaders", 0x0c2ab711c17bd851),
    ("PostWithLength/EmptyContentLength", 0xfb12e1a4c98ae32e),
    ("PostWithLength/EmptyTokens", 0x6c9df4799d6486a8),
    ("PostWithLength/EmptyConnection", 0x64058388f0ec417b),
    ("PostWithoutLength/Plain", 0x8166876cbb73fe4e),
    ("PostWithoutLength/Close", 0x622a064a45f5ef30),
    ("PostWithoutLength/KeepAlive", 0x8166876cbb73fe4e),
    ("PostWithoutLength/UpgradeToken", 0xf550f3719aeb556b),
    ("PostWithoutLength/CustomTokens", 0xccacb3c24842b408),
    ("PostWithoutLength/ContentLength", 0x0c3c40d4712c6c0a),
    ("PostWithoutLength/Chunked", 0x8166876cbb73fe4e),
    ("PostWithoutLength/NotChunked", 0x8166876cbb73fe4e),
    ("PostWithoutLength/ExtraHeaders", 0x0c2ab711c17bd851),
    ("PostWithoutLength/EmptyContentLength", 0xfb12e1a4c98ae32e),
    ("PostWithoutLength/EmptyTokens", 0xccacb3c24842b408),
    ("PostWithoutLength/EmptyConnection", 0x598809ce41cd5897),
    ("ConnectAuthority/Plain", 0x0e19bde9f2b22aad),
    ("ConnectAuthority/Close", 0xd03b8c5f8790e830),
    ("ConnectAuthority/KeepAlive", 0x0e19bde9f2b22aad),
    ("ConnectAuthority/UpgradeToken", 0xc09ef46bc8dbdb63),
    ("ConnectAuthority/CustomTokens", 0xf4c6deeb31ff611b),
    ("ConnectAuthority/ContentLength", 0xe70efcd1e49ae282),
    ("ConnectAuthority/Chunked", 0xbc9f2304ddc5845f),
    ("ConnectAuthority/NotChunked", 0x0e19bde9f2b22aad),
    ("ConnectAuthority/ExtraHeaders", 0x8c396d879da09043),
    ("ConnectAuthority/EmptyContentLength", 0x65440b60b513ecfe),
    ("ConnectAuthority/EmptyTokens", 0xf4c6deeb31ff611b),
    ("ConnectAuthority/EmptyConnection", 0x5ffb35b23a333c35),
    ("ConnectHostFallback/Plain", 0x06d25aba5d6c75ad),
    ("ConnectHostFallback/Close", 0xd6918e53a3d90e5f),
    ("ConnectHostFallback/KeepAlive", 0x06d25aba5d6c75ad),
    ("ConnectHostFallback/UpgradeToken", 0xa9fd9592a414452d),
    ("ConnectHostFallback/CustomTokens", 0x84add2ae2c4efa4a),
    ("ConnectHostFallback/ContentLength", 0x6b11a088db97940d),
    ("ConnectHostFallback/Chunked", 0x82db417673f6b54e),
    ("ConnectHostFallback/NotChunked", 0x06d25aba5d6c75ad),
    ("ConnectHostFallback/ExtraHeaders", 0x5fa0f0de0bea86de),
    ("ConnectHostFallback/EmptyContentLength", 0x00fbdb51a85468b3),
    ("ConnectHostFallback/EmptyTokens", 0x84add2ae2c4efa4a),
    ("ConnectHostFallback/EmptyConnection", 0x6e0711ad0c258a80),
    ("WebSocket/Plain", 0x7d99ed81754dbf12),
    ("WebSocket/Close", 0xa2bcf9c44693f514),
    ("WebSocket/KeepAlive", 0x7d99ed81754dbf12),
    ("WebSocket/UpgradeToken", 0x7d99ed81754dbf12),
    ("WebSocket/CustomTokens", 0xaf0f0a531674b560),
    ("WebSocket/ContentLength", 0x20ae0fe9124c831b),
    ("WebSocket/Chunked", 0x69125dcc32862aab),
    ("WebSocket/NotChunked", 0x7d99ed81754dbf12),
    ("WebSocket/ExtraHeaders", 0x985ce57f713d38a5),
    ("WebSocket/EmptyContentLength", 0xfe7710fbbed0d955),
    ("WebSocket/EmptyTokens", 0xaf0f0a531674b560),
    ("WebSocket/EmptyConnection", 0x7d99ed81754dbf12),
    ("Http10/Plain", 0xea3442c71a436aff),
    ("Http10/Close", 0xa1213824d7448edd),
    ("Http10/KeepAlive", 0xea3442c71a436aff),
    ("Http10/UpgradeToken", 0xbb3a621ae3539d4d),
    ("Http10/CustomTokens", 0x60673704f5fc749e),
    ("Http10/ContentLength", 0xf567529163ffff2b),
    ("Http10/Chunked", 0xea3442c71a436aff),
    ("Http10/NotChunked", 0xea3442c71a436aff),
    ("Http10/ExtraHeaders", 0x3b3a22ab572cfd56),
    ("Http10/EmptyContentLength", 0x364e575c3b730bad),
    ("Http10/EmptyTokens", 0x60673704f5fc749e),
    ("Http10/EmptyConnection", 0xea3442c71a436aff),
    ("Http09/Plain", 0xb5789a0705803fe9),
    ("Http09/Close", 0xb5789a0705803fe9),
    ("Http09/KeepAlive", 0xb5789a0705803fe9),
    ("Http09/UpgradeToken", 0xb5789a0705803fe9),
    ("Http09/CustomTokens", 0xb5789a0705803fe9),
    ("Http09/ContentLength", 0xb5789a0705803fe9),
    ("Http09/Chunked", 0xb5789a0705803fe9),
    ("Http09/NotChunked", 0xb5789a0705803fe9),
    ("Http09/ExtraHeaders", 0xb5789a0705803fe9),
    ("Http09/EmptyContentLength", 0xb5789a0705803fe9),
    ("Http09/EmptyTokens", 0xb5789a0705803fe9),
    ("Http09/EmptyConnection", 0xb5789a0705803fe9),
    ("Http0Dot10/Plain", 0x77c13c1a9c16a540),
    ("Http0Dot10/Close", 0x77c13c1a9c16a540),
    ("Http0Dot10/KeepAlive", 0x77c13c1a9c16a540),
    ("Http0Dot10/UpgradeToken", 0x19c2a1ab7428269e),
    ("Http0Dot10/CustomTokens", 0xb52be239263ae1f8),
    ("Http0Dot10/ContentLength", 0x2aadb986a54af902),
    ("Http0Dot10/Chunked", 0x77c13c1a9c16a540),
    ("Http0Dot10/NotChunked", 0x77c13c1a9c16a540),
    ("Http0Dot10/ExtraHeaders", 0xd0579d4fb31fde4c),
    ("Http0Dot10/EmptyContentLength", 0xe32eb10cb365195f),
    ("Http0Dot10/EmptyTokens", 0xb52be239263ae1f8),
    ("Http0Dot10/EmptyConnection", 0x77c13c1a9c16a540),
    ("Http1Dot10/Plain", 0xf3eb2cd4b70043bf),
    ("Http1Dot10/Close", 0xc6472b6d575b37ad),
    ("Http1Dot10/KeepAlive", 0xf3eb2cd4b70043bf),
    ("Http1Dot10/UpgradeToken", 0x47ebdf6733b96c41),
    ("Http1Dot10/CustomTokens", 0xd2c9160aeba33946),
    ("Http1Dot10/ContentLength", 0x600a2427175bee2f),
    ("Http1Dot10/Chunked", 0x5d60683c9986058f),
    ("Http1Dot10/NotChunked", 0xf3eb2cd4b70043bf),
    ("Http1Dot10/ExtraHeaders", 0x470c0a7217a70966),
    ("Http1Dot10/EmptyContentLength", 0x0c4e64d849cae959),
    ("Http1Dot10/EmptyTokens", 0xd2c9160aeba33946),
    ("Http1Dot10/EmptyConnection", 0x14df0f9dc8e37594),
    ("AbsoluteUrl/Plain", 0x9afa4934cb42ec81),
    ("AbsoluteUrl/Close", 0xdf7db227575cc22e),
    ("AbsoluteUrl/KeepAlive", 0x9afa4934cb42ec81),
    ("AbsoluteUrl/UpgradeToken", 0xf0b587f13c8bbb20),
    ("AbsoluteUrl/CustomTokens", 0xb5fb05d36f6b31a0),
    ("AbsoluteUrl/ContentLength", 0x4576896916690e0a),
    ("AbsoluteUrl/Chunked", 0x72b03d4d8d52da8c),
    ("AbsoluteUrl/NotChunked", 0x9afa4934cb42ec81),
    ("AbsoluteUrl/ExtraHeaders", 0x09ceb979bc1bd945),
    ("AbsoluteUrl/EmptyContentLength", 0x43f0d09c30c01720),
    ("AbsoluteUrl/EmptyTokens", 0xb5fb05d36f6b31a0),
    ("AbsoluteUrl/EmptyConnection", 0xf9e44e6664af6683),
    ("ExtensionMethod/Plain", 0xc02fe2c5fdf8aafe),
    ("ExtensionMethod/Close", 0xcd1aecb82da0aa7f),
    ("ExtensionMethod/KeepAlive", 0xc02fe2c5fdf8aafe),
    ("ExtensionMethod/UpgradeToken", 0xd56deea1cf9f69c2),
    ("ExtensionMethod/CustomTokens", 0x52e081e08093be9b),
    ("ExtensionMethod/ContentLength", 0xf15955c6254143c4),
    ("ExtensionMethod/Chunked", 0xdfead4cf81ecbabf),
    ("ExtensionMethod/NotChunked", 0xc02fe2c5fdf8aafe),
    ("ExtensionMethod/ExtraHeaders", 0x54ff954641bcd69d),
    ("ExtensionMethod/EmptyContentLength", 0xf876caeb1756affc),
    ("ExtensionMethod/EmptyTokens", 0x52e081e08093be9b),
    ("ExtensionMethod/EmptyConnection", 0x30fb8614cb1d3949),
];

const RESPONSE_DIGESTS: &[(&str, u64)] = &[
    ("Ok/Plain", 0xb44074869651853a),
    ("Ok/Close", 0xdc44bf0651188d7c),
    ("Ok/KeepAlive", 0xb44074869651853a),
    ("Ok/UpgradeToken", 0x1b8af7150cf00f06),
    ("Ok/CustomTokens", 0xaa2f691cce1f5499),
    ("Ok/ContentLength", 0xee11ddc15a85ce39),
    ("Ok/Chunked", 0xb44074869651853a),
    ("Ok/NotChunked", 0xb44074869651853a),
    ("Ok/ExtraHeaders", 0x7779b478049287c0),
    ("Ok/EmptyContentLength", 0x040f3b90eb11e7c9),
    ("Ok/EmptyTokens", 0xaa2f691cce1f5499),
    ("Ok/EmptyConnection", 0xb44074869651853a),
    ("Continue/Plain", 0xca0c9b47de4dff47),
    ("Continue/Close", 0x6dd522ce5927dda1),
    ("Continue/KeepAlive", 0xca0c9b47de4dff47),
    ("Continue/UpgradeToken", 0xa07e47cb39569a8d),
    ("Continue/CustomTokens", 0x8aec001893b9bc74),
    ("Continue/ContentLength", 0xe4a3f11797cdc83e),
    ("Continue/Chunked", 0xca0c9b47de4dff47),
    ("Continue/NotChunked", 0xca0c9b47de4dff47),
    ("Continue/ExtraHeaders", 0xbfefd992df40dbbb),
    ("Continue/EmptyContentLength", 0xbd3e3dc15d52763c),
    ("Continue/EmptyTokens", 0x8aec001893b9bc74),
    ("Continue/EmptyConnection", 0xca0c9b47de4dff47),
    ("SwitchingProtocols/Plain", 0x3d2621ee1295e41b),
    ("SwitchingProtocols/Close", 0x8150d798de58e187),
    ("SwitchingProtocols/KeepAlive", 0xbf3b702357847ffa),
    ("SwitchingProtocols/UpgradeToken", 0xd7537f6ee0570825),
    ("SwitchingProtocols/CustomTokens", 0xe9e47f244cac660e),
    ("SwitchingProtocols/ContentLength", 0xd2b2b96b4844a8c4),
    ("SwitchingProtocols/Chunked", 0x4ff6508bed8565f6),
    ("SwitchingProtocols/NotChunked", 0x3d2621ee1295e41b),
    ("SwitchingProtocols/ExtraHeaders", 0xb735aaee7d23166d),
    ("SwitchingProtocols/EmptyContentLength", 0xee5deb3a5f74f0b4),
    ("SwitchingProtocols/EmptyTokens", 0x2a682a215c7f8d17),
    ("SwitchingProtocols/EmptyConnection", 0x1e32de46d08ac1a4),
    ("NoContent/Plain", 0x5d0f2b7737c75ca0),
    ("NoContent/Close", 0x30a1bef90cf3da38),
    ("NoContent/KeepAlive", 0x5d0f2b7737c75ca0),
    ("NoContent/UpgradeToken", 0x4ae66bfa15bf3650),
    ("NoContent/CustomTokens", 0x9beb06a5503f3f9c),
    ("NoContent/ContentLength", 0x15186a60d8810dc5),
    ("NoContent/Chunked", 0xb440b15d7e3946ed),
    ("NoContent/NotChunked", 0x5d0f2b7737c75ca0),
    ("NoContent/ExtraHeaders", 0x77dffd4ed5a0c0a7),
    ("NoContent/EmptyContentLength", 0xeec2f20e6d0cd16b),
    ("NoContent/EmptyTokens", 0x9beb06a5503f3f9c),
    ("NoContent/EmptyConnection", 0x5d0f2b7737c75ca0),
    ("NotModified/Plain", 0xba77271c5c7f66ee),
    ("NotModified/Close", 0xf72d0a15d878f57d),
    ("NotModified/KeepAlive", 0xba77271c5c7f66ee),
    ("NotModified/UpgradeToken", 0xe275b5347ae10347),
    ("NotModified/CustomTokens", 0x3d348dc2d6034889),
    ("NotModified/ContentLength", 0x605f2a01bb46320e),
    ("NotModified/Chunked", 0xb6bd8cb6d376833e),
    ("NotModified/NotChunked", 0xba77271c5c7f66ee),
    ("NotModified/ExtraHeaders", 0xbd0036fe1e2d70da),
    ("NotModified/EmptyContentLength", 0xd50af1615c235136),
    ("NotModified/EmptyTokens", 0x3d348dc2d6034889),
    ("NotModified/EmptyConnection", 0xba77271c5c7f66ee),
    ("ConnectEstablished/Plain", 0xf4e8ee8e5a5ce362),
    ("ConnectEstablished/Close", 0xf4e8ee8e5a5ce362),
    ("ConnectEstablished/KeepAlive", 0xf4e8ee8e5a5ce362),
    ("ConnectEstablished/UpgradeToken", 0x795762accc4b4adb),
    ("ConnectEstablished/CustomTokens", 0xdbf12fbea2e9dabe),
    ("ConnectEstablished/ContentLength", 0x98c9a31dd52ada57),
    ("ConnectEstablished/Chunked", 0xcdd58c2d9fe6003d),
    ("ConnectEstablished/NotChunked", 0xf4e8ee8e5a5ce362),
    ("ConnectEstablished/ExtraHeaders", 0xd330b1f533c964e5),
    ("ConnectEstablished/EmptyContentLength", 0x3cc1aeb862f0c99b),
    ("ConnectEstablished/EmptyTokens", 0xdbf12fbea2e9dabe),
    ("ConnectEstablished/EmptyConnection", 0xf4e8ee8e5a5ce362),
    ("HeadResponse/Plain", 0x6a16a48a303dc727),
    ("HeadResponse/Close", 0xc3d9dbf789269f66),
    ("HeadResponse/KeepAlive", 0x6a16a48a303dc727),
    ("HeadResponse/UpgradeToken", 0x983b012110c9e9cc),
    ("HeadResponse/CustomTokens", 0x3833505b4550616b),
    ("HeadResponse/ContentLength", 0xc5a6f1933c720e9e),
    ("HeadResponse/Chunked", 0xe8293dda061eaf30),
    ("HeadResponse/NotChunked", 0x6a16a48a303dc727),
    ("HeadResponse/ExtraHeaders", 0x013d412f069c897b),
    ("HeadResponse/EmptyContentLength", 0x88496e4a25dc185c),
    ("HeadResponse/EmptyTokens", 0x3833505b4550616b),
    ("HeadResponse/EmptyConnection", 0x6a16a48a303dc727),
    ("Http10KeepAliveRequested/Plain", 0x67a95dbf3158b88c),
    ("Http10KeepAliveRequested/Close", 0x67a95dbf3158b88c),
    ("Http10KeepAliveRequested/KeepAlive", 0x67a95dbf3158b88c),
    ("Http10KeepAliveRequested/UpgradeToken", 0x5367c24b11d07376),
    ("Http10KeepAliveRequested/CustomTokens", 0x90d3715a14a72515),
    ("Http10KeepAliveRequested/ContentLength", 0x8b037c9f5e81e2b0),
    ("Http10KeepAliveRequested/Chunked", 0x67a95dbf3158b88c),
    ("Http10KeepAliveRequested/NotChunked", 0x67a95dbf3158b88c),
    ("Http10KeepAliveRequested/ExtraHeaders", 0x56a0878521afdcab),
    (
        "Http10KeepAliveRequested/EmptyContentLength",
        0x15fa7a729749e2ca,
    ),
    ("Http10KeepAliveRequested/EmptyTokens", 0x90d3715a14a72515),
    (
        "Http10KeepAliveRequested/EmptyConnection",
        0x67a95dbf3158b88c,
    ),
    ("Http10KeepAliveNotRequested/Plain", 0x287eace1d92523e6),
    ("Http10KeepAliveNotRequested/Close", 0x287eace1d92523e6),
    ("Http10KeepAliveNotRequested/KeepAlive", 0x287eace1d92523e6),
    (
        "Http10KeepAliveNotRequested/UpgradeToken",
        0x5058a1f0fc097080,
    ),
    (
        "Http10KeepAliveNotRequested/CustomTokens",
        0x01b971574164458f,
    ),
    (
        "Http10KeepAliveNotRequested/ContentLength",
        0x93d64f9d19aae26a,
    ),
    ("Http10KeepAliveNotRequested/Chunked", 0x287eace1d92523e6),
    ("Http10KeepAliveNotRequested/NotChunked", 0x287eace1d92523e6),
    (
        "Http10KeepAliveNotRequested/ExtraHeaders",
        0xe5da6a9c78e21c43,
    ),
    (
        "Http10KeepAliveNotRequested/EmptyContentLength",
        0xeae35ec8d8785008,
    ),
    (
        "Http10KeepAliveNotRequested/EmptyTokens",
        0x01b971574164458f,
    ),
    (
        "Http10KeepAliveNotRequested/EmptyConnection",
        0x287eace1d92523e6,
    ),
    ("Http0Dot10/Plain", 0xd76a0a0a1363905e),
    ("Http0Dot10/Close", 0xd76a0a0a1363905e),
    ("Http0Dot10/KeepAlive", 0xd76a0a0a1363905e),
    ("Http0Dot10/UpgradeToken", 0xc9de619f6618c178),
    ("Http0Dot10/CustomTokens", 0xc1872b97a05a0f5d),
    ("Http0Dot10/ContentLength", 0xf249507d87395b09),
    ("Http0Dot10/Chunked", 0xd76a0a0a1363905e),
    ("Http0Dot10/NotChunked", 0xd76a0a0a1363905e),
    ("Http0Dot10/ExtraHeaders", 0xf27fe31e1bc9cbac),
    ("Http0Dot10/EmptyContentLength", 0x787f2ac18ef68ec1),
    ("Http0Dot10/EmptyTokens", 0xc1872b97a05a0f5d),
    ("Http0Dot10/EmptyConnection", 0xd76a0a0a1363905e),
    ("Http1Dot10/Plain", 0xb44074869651853a),
    ("Http1Dot10/Close", 0xdc44bf0651188d7c),
    ("Http1Dot10/KeepAlive", 0xb44074869651853a),
    ("Http1Dot10/UpgradeToken", 0x1b8af7150cf00f06),
    ("Http1Dot10/CustomTokens", 0xaa2f691cce1f5499),
    ("Http1Dot10/ContentLength", 0xee11ddc15a85ce39),
    ("Http1Dot10/Chunked", 0xb44074869651853a),
    ("Http1Dot10/NotChunked", 0xb44074869651853a),
    ("Http1Dot10/ExtraHeaders", 0x7779b478049287c0),
    ("Http1Dot10/EmptyContentLength", 0x040f3b90eb11e7c9),
    ("Http1Dot10/EmptyTokens", 0xaa2f691cce1f5499),
    ("Http1Dot10/EmptyConnection", 0xb44074869651853a),
    ("OneDigitStatus/Plain", 0x90125511547a51fb),
    ("OneDigitStatus/Close", 0xc916f5f61b255467),
    ("OneDigitStatus/KeepAlive", 0x90125511547a51fb),
    ("OneDigitStatus/UpgradeToken", 0x45bd849187cd1da1),
    ("OneDigitStatus/CustomTokens", 0x3b4c9696b4766c40),
    ("OneDigitStatus/ContentLength", 0x7aff80969f541728),
    ("OneDigitStatus/Chunked", 0x90125511547a51fb),
    ("OneDigitStatus/NotChunked", 0x90125511547a51fb),
    ("OneDigitStatus/ExtraHeaders", 0x8000b632b099da4b),
    ("OneDigitStatus/EmptyContentLength", 0x90b9f167a265ec16),
    ("OneDigitStatus/EmptyTokens", 0x3b4c9696b4766c40),
    ("OneDigitStatus/EmptyConnection", 0x90125511547a51fb),
    ("TwoDigitStatus/Plain", 0x6fb2d3e681affb7b),
    ("TwoDigitStatus/Close", 0x4e55d784e8b67485),
    ("TwoDigitStatus/KeepAlive", 0x6fb2d3e681affb7b),
    ("TwoDigitStatus/UpgradeToken", 0x8ae0ff29fa9adbdb),
    ("TwoDigitStatus/CustomTokens", 0xad40120b21231e60),
    ("TwoDigitStatus/ContentLength", 0xa9bb7268d39c71ca),
    ("TwoDigitStatus/Chunked", 0x6fb2d3e681affb7b),
    ("TwoDigitStatus/NotChunked", 0x6fb2d3e681affb7b),
    ("TwoDigitStatus/ExtraHeaders", 0xf0c453ba29d362df),
    ("TwoDigitStatus/EmptyContentLength", 0x4cfa5e2c7e909688),
    ("TwoDigitStatus/EmptyTokens", 0xad40120b21231e60),
    ("TwoDigitStatus/EmptyConnection", 0x6fb2d3e681affb7b),
    ("FourDigitStatus/Plain", 0x895dd439a82a37c6),
    ("FourDigitStatus/Close", 0x21ed83edac457b44),
    ("FourDigitStatus/KeepAlive", 0x895dd439a82a37c6),
    ("FourDigitStatus/UpgradeToken", 0x3de584dec9018598),
    ("FourDigitStatus/CustomTokens", 0x3e8fb09e2b1d20bd),
    ("FourDigitStatus/ContentLength", 0x0bb8a072b4f88f1b),
    ("FourDigitStatus/Chunked", 0x895dd439a82a37c6),
    ("FourDigitStatus/NotChunked", 0x895dd439a82a37c6),
    ("FourDigitStatus/ExtraHeaders", 0x929ed3ba0fcad944),
    ("FourDigitStatus/EmptyContentLength", 0xa6db082d036334ab),
    ("FourDigitStatus/EmptyTokens", 0x3e8fb09e2b1d20bd),
    ("FourDigitStatus/EmptyConnection", 0x895dd439a82a37c6),
    ("FiveDigitStatus/Plain", 0x3fffbc6599a1580a),
    ("FiveDigitStatus/Close", 0xc9fd0049d7fc89ec),
    ("FiveDigitStatus/KeepAlive", 0x3fffbc6599a1580a),
    ("FiveDigitStatus/UpgradeToken", 0x4fff25b111a8ddd6),
    ("FiveDigitStatus/CustomTokens", 0x8abc55cfb91bbcbd),
    ("FiveDigitStatus/ContentLength", 0x878edd3fa28eaf6d),
    ("FiveDigitStatus/Chunked", 0x3fffbc6599a1580a),
    ("FiveDigitStatus/NotChunked", 0x3fffbc6599a1580a),
    ("FiveDigitStatus/ExtraHeaders", 0xdb4911890b6de878),
    ("FiveDigitStatus/EmptyContentLength", 0xf96ceecf4b4e5585),
    ("FiveDigitStatus/EmptyTokens", 0x8abc55cfb91bbcbd),
    ("FiveDigitStatus/EmptyConnection", 0x3fffbc6599a1580a),
    ("WebSocketAccept/Plain", 0x06acfae18a958baa),
    ("WebSocketAccept/Close", 0x06acfae18a958baa),
    ("WebSocketAccept/KeepAlive", 0x06acfae18a958baa),
    ("WebSocketAccept/UpgradeToken", 0x06acfae18a958baa),
    ("WebSocketAccept/CustomTokens", 0xc740001573cd84db),
    ("WebSocketAccept/ContentLength", 0x6cb0caf8a4c5c732),
    ("WebSocketAccept/Chunked", 0xab91eb9593f00fbe),
    ("WebSocketAccept/NotChunked", 0x06acfae18a958baa),
    ("WebSocketAccept/ExtraHeaders", 0x451575457550100d),
    ("WebSocketAccept/EmptyContentLength", 0x85acb4d0ec97cf94),
    ("WebSocketAccept/EmptyTokens", 0xc740001573cd84db),
    ("WebSocketAccept/EmptyConnection", 0x06acfae18a958baa),
    ("CustomReason/Plain", 0x73eddde4df9950d4),
    ("CustomReason/Close", 0x183c896c8ae450fa),
    ("CustomReason/KeepAlive", 0x73eddde4df9950d4),
    ("CustomReason/UpgradeToken", 0x6ca3918bbe582602),
    ("CustomReason/CustomTokens", 0x50280960a27157eb),
    ("CustomReason/ContentLength", 0x32ea805cdf39ce82),
    ("CustomReason/Chunked", 0x73eddde4df9950d4),
    ("CustomReason/NotChunked", 0x73eddde4df9950d4),
    ("CustomReason/ExtraHeaders", 0xb67d9440674f7c2e),
    ("CustomReason/EmptyContentLength", 0x7d2309a3f2bb0982),
    ("CustomReason/EmptyTokens", 0x50280960a27157eb),
    ("CustomReason/EmptyConnection", 0x73eddde4df9950d4),
    ("EmptyReason/Plain", 0xd07865d305ae54ba),
    ("EmptyReason/Close", 0x8c2fe82ad2ba328c),
    ("EmptyReason/KeepAlive", 0xd07865d305ae54ba),
    ("EmptyReason/UpgradeToken", 0xadc62214e4047546),
    ("EmptyReason/CustomTokens", 0xe4bcad5c379714e5),
    ("EmptyReason/ContentLength", 0x0eeaabda34c24945),
    ("EmptyReason/Chunked", 0xd07865d305ae54ba),
    ("EmptyReason/NotChunked", 0xd07865d305ae54ba),
    ("EmptyReason/ExtraHeaders", 0x1c9d9297fd6f3c94),
    ("EmptyReason/EmptyContentLength", 0x7d7b4a866ccb57a5),
    ("EmptyReason/EmptyTokens", 0xe4bcad5c379714e5),
    ("EmptyReason/EmptyConnection", 0xd07865d305ae54ba),
    ("Http09Request/Plain", 0xd8680379a3f1a897),
    ("Http09Request/Close", 0xd8680379a3f1a897),
    ("Http09Request/KeepAlive", 0xd8680379a3f1a897),
    ("Http09Request/UpgradeToken", 0xc89d3894bdd9954b),
    ("Http09Request/CustomTokens", 0xd72f3873a161705e),
    ("Http09Request/ContentLength", 0xa7a5406b07ec70d9),
    ("Http09Request/Chunked", 0xd8680379a3f1a897),
    ("Http09Request/NotChunked", 0xd8680379a3f1a897),
    ("Http09Request/ExtraHeaders", 0x610fdf47d4743976),
    ("Http09Request/EmptyContentLength", 0xbef12fb5a1d33405),
    ("Http09Request/EmptyTokens", 0xd72f3873a161705e),
    ("Http09Request/EmptyConnection", 0xd8680379a3f1a897),
];

// Framing frames (`repair.http1_head_egress`): the chunk-size prefix against a
// frozen `format!` reference (expectation-432), and traces of the body, chunk,
// trailer, and EOM frames recorded before the unit's product edit.

/// The chunk-size prefix that `generate` wrote through `format!` before
/// `repair.http1_head_egress`.
fn reference_chunk_prefix(length: usize) -> Vec<u8> {
    format!("{length:x}\r\n").into_bytes()
}

/// A downstream codec that has parsed a GET and generated a chunked response
/// head.
fn chunked_response_codec() -> Http1xCodec {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let input = BufQueue::from_vec(GET_REQUEST.to_vec(), QUEUE_LIMIT).expect("the wire fits");
    let _ = codec.on_ingress(
        &input,
        &mut Events::default(),
        &mut BufQueue::new(QUEUE_LIMIT),
    );
    let message = response(200, Some(b"OK"), (1, 1));
    codec
        .generate(
            &mut BufQueue::new(QUEUE_LIMIT),
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
            &mut Events::default(),
        )
        .expect("the response head is generated");
    assert!(codec.egress_chunked, "the response is chunked");
    codec
}

/// Lengths around every hex digit-count boundary: `16^k - 1`, `16^k`, and
/// `16^k + 1` for `k` in `powers`.
fn hex_boundaries(powers: std::ops::RangeInclusive<u32>) -> impl Iterator<Item = usize> {
    powers.flat_map(|k| {
        let power = 16_usize.pow(k);
        [power - 1, power, power + 1]
    })
}

static BODY_BYTES: [u8; 65_537] = [b'b'; 65_537];

fn static_body(segments: &[&'static [u8]]) -> BufQueue {
    let mut body = BufQueue::new(QUEUE_LIMIT);
    for segment in segments {
        body.push_static(segment).expect("the body fits");
    }
    body
}

#[test]
fn chunk_size_prefixes_match_the_frozen_format() {
    let mut codec = chunked_response_codec();
    let stream = StreamId::new(1);
    for length in (1..=4_096)
        .chain(hex_boundaries(1..=15))
        .chain([usize::MAX])
    {
        let mut output = BufQueue::new(QUEUE_LIMIT);
        let frame = codec
            .generate(
                &mut output,
                EgressFrame::ChunkHeader { stream, length },
                &mut Events::default(),
            )
            .expect("the chunk header is generated");
        let expected = reference_chunk_prefix(length);
        assert_eq!(queue_bytes(&output), expected, "ChunkHeader {length}");
        assert_eq!(frame.bytes_written, expected.len(), "ChunkHeader {length}");
        codec
            .generate(
                &mut BufQueue::new(QUEUE_LIMIT),
                EgressFrame::ChunkTerminator { stream },
                &mut Events::default(),
            )
            .expect("the chunk is closed");
    }
    for length in (1..=300).chain(hex_boundaries(1..=4)) {
        let mut output = BufQueue::new(QUEUE_LIMIT);
        let frame = codec
            .generate(
                &mut output,
                EgressFrame::Body {
                    stream,
                    body: static_body(&[&BODY_BYTES[..length]]),
                    padding: None,
                    end_stream: false,
                },
                &mut Events::default(),
            )
            .expect("the body is generated");
        let prefix = reference_chunk_prefix(length);
        let mut expected = prefix.clone();
        expected.extend_from_slice(&BODY_BYTES[..length]);
        expected.extend_from_slice(b"\r\n");
        assert!(queue_bytes(&output) == expected, "Body {length}");
        assert_eq!(frame.bytes_written, expected.len(), "Body {length}");
        assert_eq!(
            frame.payload_offset,
            Some(prefix.len() as u64),
            "Body {length}"
        );
    }
}

/// The codec setups the framing traces start from.
#[derive(Clone, Copy, Debug)]
enum FramingSetup {
    /// A downstream codec that has parsed an HTTP/1.1 GET.
    Get,
    /// A downstream codec that has parsed an HTTP/1.1 HEAD.
    Head,
    /// A downstream codec that has parsed an HTTP/1.0 GET.
    Http10,
    /// An upstream codec sending a POST.
    Post,
}

/// How the head of a framing trace frames its body.
#[derive(Clone, Copy, Debug)]
enum FramingEgress {
    /// Neither Content-Length nor Transfer-Encoding: the codec chunks the
    /// body when it may.
    Implicit,
    /// `Transfer-Encoding: chunked`, with the message marked chunked.
    Chunked,
    /// `Content-Length: 5`.
    ContentLength,
}

/// The frame sequences of the framing traces, each generated after the head.
#[derive(Clone, Copy, Debug)]
enum FramingSequence {
    BodyThenBodyWithEnd,
    ChunkHeaderBodyTerminator,
    ChunkHeaderThenBodyWithEnd,
    BodyThenTrailers,
    BodyThenEndOfMessage,
    EndOfMessageAlone,
    EmptyBodyWithEnd,
    /// A body after the message's end, which generation does not refuse; a
    /// chunked one closes its chunk after the last chunk was written.
    BodyWithEndAfterEndOfMessage,
    ZeroLengthChunkHeader,
    SecondChunkHeader,
    BodyOnAnotherStream,
    BodyIntoAFullQueue,
    EndOfMessageIntoAFullQueue,
}

const FRAMING_SEQUENCES: [FramingSequence; 13] = [
    FramingSequence::BodyThenBodyWithEnd,
    FramingSequence::ChunkHeaderBodyTerminator,
    FramingSequence::ChunkHeaderThenBodyWithEnd,
    FramingSequence::BodyThenTrailers,
    FramingSequence::BodyThenEndOfMessage,
    FramingSequence::EndOfMessageAlone,
    FramingSequence::EmptyBodyWithEnd,
    FramingSequence::BodyWithEndAfterEndOfMessage,
    FramingSequence::ZeroLengthChunkHeader,
    FramingSequence::SecondChunkHeader,
    FramingSequence::BodyOnAnotherStream,
    FramingSequence::BodyIntoAFullQueue,
    FramingSequence::EndOfMessageIntoAFullQueue,
];

/// Records every framing frame's result, output segments, and egress state.
struct FramingTrace {
    codec: Http1xCodec,
    stream: StreamId,
    lines: Vec<String>,
}

impl FramingTrace {
    fn new(setup: FramingSetup, egress: FramingEgress) -> Self {
        let wire: Option<&[u8]> = match setup {
            FramingSetup::Get => Some(GET_REQUEST),
            FramingSetup::Head => Some(b"HEAD / HTTP/1.1\r\nHost: example.test\r\n\r\n"),
            FramingSetup::Http10 => Some(b"GET / HTTP/1.0\r\n\r\n"),
            FramingSetup::Post => None,
        };
        let (mut codec, stream, mut message) = match wire {
            Some(wire) => {
                let mut codec = Http1xCodec::new(TransportDirection::Downstream);
                let input = BufQueue::from_vec(wire.to_vec(), QUEUE_LIMIT).expect("the wire fits");
                let _ = codec.on_ingress(
                    &input,
                    &mut Events::default(),
                    &mut BufQueue::new(QUEUE_LIMIT),
                );
                (codec, StreamId::new(1), response(200, Some(b"OK"), (1, 1)))
            }
            None => {
                let mut codec = Http1xCodec::new(TransportDirection::Upstream);
                let stream = codec.create_stream();
                (
                    codec,
                    stream,
                    request(HttpMethod::Post, Some(b"/upload"), (1, 1)),
                )
            }
        };
        match egress {
            FramingEgress::Implicit => {}
            FramingEgress::Chunked => {
                message
                    .headers_mut()
                    .add(HttpHeaderCode::TransferEncoding, b"chunked");
                message.set_chunked(true);
            }
            FramingEgress::ContentLength => message
                .headers_mut()
                .add(HttpHeaderCode::ContentLength, b"5"),
        }
        let mut output = BufQueue::new(QUEUE_LIMIT);
        let result = codec.generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
            &mut Events::default(),
        );
        let mut trace = Self {
            codec,
            stream,
            lines: vec![format!("setup {setup:?} egress={egress:?}")],
        };
        trace.record("headers", &result, &output);
        trace
    }

    fn record(
        &mut self,
        label: &str,
        result: &Result<GeneratedFrame, HttpError>,
        output: &BufQueue,
    ) {
        let codec = &self.codec;
        let segments: Vec<usize> = output.segments().map(|segment| segment.len()).collect();
        let flag = u8::from;
        self.lines.push(format!(
            "{label} {} segments={segments:?} \"{}\" keepalive={} chunked={}/{}/{} pending={}/{}",
            render_result(result),
            queue_bytes(output).escape_ascii(),
            flag(codec.keepalive),
            flag(codec.egress_chunked),
            flag(codec.in_chunk),
            flag(codec.last_chunk_written),
            flag(codec.request_pending),
            flag(codec.response_pending),
        ));
    }

    fn frame_into(&mut self, label: &str, mut output: BufQueue, frame: EgressFrame<'_>) {
        let result = self
            .codec
            .generate(&mut output, frame, &mut Events::default());
        self.record(label, &result, &output);
    }

    fn frame(&mut self, label: &str, frame: EgressFrame<'_>) {
        self.frame_into(label, BufQueue::new(QUEUE_LIMIT), frame);
    }

    fn body(&mut self, segments: &[&'static [u8]], end_stream: bool) {
        let stream = self.stream;
        self.frame(
            "body",
            EgressFrame::Body {
                stream,
                body: static_body(segments),
                padding: None,
                end_stream,
            },
        );
    }

    /// An output queue of `limit` bytes that already holds `b"xx"`.
    fn full_queue(limit: usize) -> BufQueue {
        let mut output = BufQueue::new(limit);
        output
            .push_static(b"xx")
            .expect("the queue holds two bytes");
        output
    }

    fn run(&mut self, sequence: FramingSequence) {
        let stream = self.stream;
        self.lines.push(format!("sequence {sequence:?}"));
        match sequence {
            FramingSequence::BodyThenBodyWithEnd => {
                self.body(&[b"hel", b"lo"], false);
                self.body(&[b"world"], true);
            }
            FramingSequence::ChunkHeaderBodyTerminator => {
                self.frame("chunk", EgressFrame::ChunkHeader { stream, length: 5 });
                self.body(&[b"hel", b"lo"], false);
                self.frame("terminator", EgressFrame::ChunkTerminator { stream });
                self.frame("eom", EgressFrame::EndOfMessage { stream });
            }
            FramingSequence::ChunkHeaderThenBodyWithEnd => {
                self.frame("chunk", EgressFrame::ChunkHeader { stream, length: 5 });
                self.body(&[b"hel", b"lo"], true);
            }
            FramingSequence::BodyThenTrailers => {
                self.body(&[b"hel", b"lo"], false);
                let mut trailers = HttpHeaders::new();
                trailers.add(b"X-Trailer".as_slice(), b"t".as_slice());
                self.frame(
                    "trailers",
                    EgressFrame::Trailers {
                        stream,
                        trailers: &trailers,
                    },
                );
            }
            FramingSequence::BodyThenEndOfMessage => {
                self.body(&[b"hel", b"lo"], false);
                self.frame("eom", EgressFrame::EndOfMessage { stream });
            }
            FramingSequence::EndOfMessageAlone => {
                self.frame("eom", EgressFrame::EndOfMessage { stream });
            }
            FramingSequence::EmptyBodyWithEnd => self.body(&[], true),
            FramingSequence::BodyWithEndAfterEndOfMessage => {
                self.frame("eom", EgressFrame::EndOfMessage { stream });
                self.body(&[b"hel", b"lo"], true);
            }
            FramingSequence::ZeroLengthChunkHeader => {
                self.frame("chunk", EgressFrame::ChunkHeader { stream, length: 0 });
            }
            FramingSequence::SecondChunkHeader => {
                self.frame("chunk", EgressFrame::ChunkHeader { stream, length: 5 });
                self.frame("chunk", EgressFrame::ChunkHeader { stream, length: 5 });
            }
            FramingSequence::BodyOnAnotherStream => {
                self.frame(
                    "body",
                    EgressFrame::Body {
                        stream: StreamId::new(stream.get() + 2),
                        body: static_body(&[b"hello"]),
                        padding: None,
                        end_stream: true,
                    },
                );
            }
            FramingSequence::BodyIntoAFullQueue => {
                self.frame_into(
                    "body",
                    Self::full_queue(4),
                    EgressFrame::Body {
                        stream,
                        body: static_body(&[b"hel", b"lo"]),
                        padding: None,
                        end_stream: true,
                    },
                );
            }
            FramingSequence::EndOfMessageIntoAFullQueue => {
                self.frame_into(
                    "eom",
                    Self::full_queue(3),
                    EgressFrame::EndOfMessage { stream },
                );
            }
        }
    }
}

fn framing_traces() -> String {
    let mut traces = Vec::new();
    for setup in [
        FramingSetup::Get,
        FramingSetup::Head,
        FramingSetup::Http10,
        FramingSetup::Post,
    ] {
        for egress in [
            FramingEgress::Implicit,
            FramingEgress::Chunked,
            FramingEgress::ContentLength,
        ] {
            for sequence in FRAMING_SEQUENCES {
                let mut trace = FramingTrace::new(setup, egress);
                trace.run(sequence);
                traces.push(trace.lines.join("\n"));
            }
        }
    }
    traces.join("\n\n")
}

/// The FNV-1a digest of [`framing_traces`], recorded at the activation commit
/// of `repair.http1_head_egress` before any product edit.
const FRAMING_TRACES_DIGEST: u64 = 0xd80d_419a_1d3f_2a40;

#[test]
fn framing_frames_match_the_recorded_traces() {
    let traces = framing_traces();
    let digest = fnv1a(&traces);
    assert!(
        digest == FRAMING_TRACES_DIGEST,
        "the framing traces differ from the recorded baseline: {digest:#018x}\n{traces}"
    );
}

/// One framing frame's buffer work on a fresh, empty output queue: the
/// queue's backing bound afterwards, the byte buffers the frame created, the
/// strings it formatted through `core::fmt` (expectation-433), and the whole
/// queues it appended.
fn framing_work(
    codec: &mut Http1xCodec,
    frame: EgressFrame<'_>,
) -> (Option<usize>, usize, usize, usize) {
    let buffers = codec.egress_work.byte_buffers.get();
    let formatted = formatted_strings();
    let appended = codec.egress_work.appended_queues.get();
    let mut output = BufQueue::new(QUEUE_LIMIT);
    codec
        .generate(&mut output, frame, &mut Events::default())
        .expect("the frame is generated");
    (
        output.backing_capacity_upper_bound(),
        codec.egress_work.byte_buffers.get() - buffers,
        formatted_strings() - formatted,
        codec.egress_work.appended_queues.get() - appended,
    )
}

/// A chunk shorter than 0x1000 opens with a static line, so it allocates no
/// byte buffer; a longer one allocates its exact-size line. A body frame
/// moves its body into the output with one append and no staging queue.
#[test]
fn framing_frames_allocate_at_most_their_chunk_prefix() {
    let stream = StreamId::new(1);
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (length, end_stream, work) in [
        (128, false, (Some(0), 0, 0, 1)),
        (128, true, (Some(0), 0, 0, 1)),
        (0xfff, false, (Some(0), 0, 0, 1)),
        (0x1000, false, (Some(b"1000\r\n".len()), 1, 0, 1)),
    ] {
        let mut codec = chunked_response_codec();
        let label = format!("Body {length:#x} end_stream={end_stream}");
        actual.push((
            label.clone(),
            framing_work(
                &mut codec,
                EgressFrame::Body {
                    stream,
                    body: static_body(&[&BODY_BYTES[..length]]),
                    padding: None,
                    end_stream,
                },
            ),
        ));
        expected.push((label, work));
    }
    let mut codec = chunked_response_codec();
    for (length, work) in [
        (4, (Some(0), 0, 0, 0)),
        (0xfff, (Some(0), 0, 0, 0)),
        (0x1000, (Some(b"1000\r\n".len()), 1, 0, 0)),
        (0xf_ffff, (Some(b"fffff\r\n".len()), 1, 0, 0)),
    ] {
        let label = format!("ChunkHeader {length:#x}");
        let header = framing_work(&mut codec, EgressFrame::ChunkHeader { stream, length });
        actual.push((label.clone(), header));
        expected.push((label, work));
        let work = framing_work(&mut codec, EgressFrame::ChunkTerminator { stream });
        actual.push(("ChunkTerminator".to_owned(), work));
        expected.push(("ChunkTerminator".to_owned(), (Some(0), 0, 0, 0)));
    }
    let work = framing_work(&mut codec, EgressFrame::EndOfMessage { stream });
    actual.push(("EndOfMessage".to_owned(), work));
    expected.push(("EndOfMessage".to_owned(), (Some(0), 0, 0, 0)));
    assert_eq!(
        actual, expected,
        "per frame: the output queue's backing bound, the byte buffers, the \
         strings formatted through core::fmt, and the appended queues"
    );
}

/// A chunked body that overflows the output fails as one frame, even when its
/// chunk line alone would fit: the output keeps its bytes and bound, the error
/// counts the whole frame, and the codec can generate the frame again.
#[test]
fn body_frames_that_overflow_the_output_leave_it_unchanged() {
    let mut codec = chunked_response_codec();
    let stream = StreamId::new(1);
    let body = || EgressFrame::Body {
        stream,
        body: static_body(&[b"abcd", b"efgh"]),
        padding: None,
        end_stream: false,
    };
    let mut output = BufQueue::new(10);
    output
        .push_static(b"xx")
        .expect("the queue holds two bytes");
    let result = codec.generate(&mut output, body(), &mut Events::default());
    assert_eq!(
        render_result(&result),
        "error 2 \"buffer limit 10 exceeded: 2 existing bytes plus 13 incoming bytes\""
    );
    let segments: Vec<usize> = output.segments().map(|segment| segment.len()).collect();
    assert_eq!(segments, [2]);
    assert_eq!(queue_bytes(&output), b"xx");
    assert_eq!(output.backing_capacity_upper_bound(), Some(0));
    assert!(!codec.in_chunk && !codec.last_chunk_written);
    let mut output = BufQueue::new(QUEUE_LIMIT);
    let result = codec.generate(&mut output, body(), &mut Events::default());
    assert_eq!(
        render_result(&result),
        "ok written=13 size=None offset=Some(3)"
    );
    assert_eq!(queue_bytes(&output), b"8\r\nabcdefgh\r\n");
}

#[test]
fn a_body_frame_whose_length_overflows_fails_its_room_check_as_its_staging_queue_did() {
    let empty = BufQueue::new(usize::MAX);
    let mut holding_two = BufQueue::new(usize::MAX);
    holding_two
        .push_static(b"xx")
        .expect("the queue holds two bytes");
    let render = |result: Result<usize, HttpError>| match result {
        Ok(written) => format!("ok {written}"),
        Err(error) => format!(
            "error {} \"{}\"",
            error.code().as_u16(),
            error.message().escape_ascii()
        ),
    };
    // Output, prefix, body and suffix lengths, and the result of staging the
    // frame in one queue and then appending that queue to the output.
    let cases = [
        (&empty, 3, 8, 2, "ok 13"),
        (&empty, 3, usize::MAX - 5, 2, "ok 18446744073709551615"),
        (
            &empty,
            3,
            usize::MAX - 4,
            2,
            "error 2 \"buffer length overflow: 18446744073709551614 existing bytes plus 2 incoming bytes\"",
        ),
        (
            &empty,
            3,
            usize::MAX - 1,
            2,
            "error 2 \"buffer length overflow: 3 existing bytes plus 18446744073709551614 incoming bytes\"",
        ),
        (
            &holding_two,
            3,
            usize::MAX - 5,
            2,
            "error 2 \"buffer length overflow: 2 existing bytes plus 18446744073709551615 incoming bytes\"",
        ),
        (
            &holding_two,
            3,
            usize::MAX - 1,
            2,
            "error 2 \"buffer length overflow: 3 existing bytes plus 18446744073709551614 incoming bytes\"",
        ),
    ];
    cases
        .into_iter()
        .for_each(|(output, prefix, body, suffix, expected)| {
            assert_eq!(
                render(Http1xCodec::check_frame_room(output, prefix, body, suffix)),
                expected,
                "{} queued bytes, frame {prefix} + {body} + {suffix}",
                output.chain_length()
            );
        });
}
