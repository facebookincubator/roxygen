/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An incremental HTTP/1.0 and HTTP/1.1 codec.
//!
//! [`Http1xCodec`] implements [`HttpCodec`] for HTTP/1.x. `roxygen_session` drives it on HTTP/1
//! connections, so most applications use `roxygen_server` and `roxygen_client` instead of this
//! crate. Its parser is Rust generated from the grammar of [llhttp], the parser that Node.js
//! uses, and the codec leaves all of llhttp's leniency flags off.
//!
//! A downstream codec ([`TransportDirection::Downstream`]) parses requests and generates
//! responses. An upstream codec generates requests and parses responses. Input can arrive in
//! pieces of any size, and the codec keeps its parse state between calls. It handles chunked
//! bodies and trailers, pipelined requests, `CONNECT`, `Upgrade` including the WebSocket
//! handshake, and HTTP/0.9 messages.
//!
//! - [`Http1xCodec::new`] and [`Http1xCodec::with_options`] create a codec. Setters such as
//!   [`Http1xCodec::set_strict_validation`] change how it parses and frames messages.
//! - [`Http1xCodec::on_ingress_with_callback`] parses like [`HttpCodec::on_ingress`] but calls
//!   a callback of a concrete type instead of a `dyn HttpCodecCallback`.
//! - [`server_accepted_upgrade`] checks whether a server's `Upgrade` header names a protocol
//!   that the client offered.
//!
//! Parsing a request:
//!
//! ```
//! use roxygen_codec::CodecEvent;
//! use roxygen_codec::HttpCodec;
//! use roxygen_codec::HttpCodecCallback;
//! use roxygen_core::buf_queue::BufQueue;
//! use roxygen_core::http::TransportDirection;
//! use roxygen_http1::Http1xCodec;
//!
//! #[derive(Default)]
//! struct Targets(Vec<Vec<u8>>);
//!
//! impl HttpCodecCallback for Targets {
//!     fn on_event(&mut self, event: CodecEvent<'_>) {
//!         if let CodecEvent::HeadersComplete { message, .. } = event
//!             && let Some(target) = message.request_url()
//!         {
//!             self.0.push(target.to_vec());
//!         }
//!     }
//! }
//!
//! let wire = b"GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n";
//! let input = BufQueue::copy_from_slice(wire, wire.len())?;
//! let mut codec = Http1xCodec::new(TransportDirection::Downstream);
//! let mut targets = Targets::default();
//! let mut callback_egress = BufQueue::new(4096);
//! let result = codec.on_ingress(&input, &mut targets, &mut callback_egress);
//! assert_eq!(result.consumed, wire.len());
//! assert_eq!(targets.0, [b"/hello".to_vec()]);
//! # Ok::<(), roxygen_core::buf_queue::BufQueueError>(())
//! ```
//!
//! [llhttp]: https://github.com/nodejs/llhttp

use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::Range;
use std::rc::Rc;

mod ingress;
mod llhttp;
use base64::Engine as _;
use bytes::Bytes;
use llhttp::Error as ParserError;
use llhttp::Event as ParserEvent;
use llhttp::Parser;
use llhttp::ParserType;
use rand::Rng as _;
use roxygen_codec::CodecCallbackResult;
use roxygen_codec::CodecCapabilities;
use roxygen_codec::CodecEvent;
use roxygen_codec::CodecIngressResult;
use roxygen_codec::EgressFrame;
use roxygen_codec::GeneratedFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::HttpException;
use roxygen_codec::HttpExceptionDirection;
use roxygen_codec::ProxygenError;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::BufQueueError;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::Method;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::rfc2616;
use roxygen_core::url::UrlComponents;
use sha1::Digest as _;
use sha1::Sha1;

const MAX_HEADER_BYTES: usize = 80 * 1024;
const WEBSOCKET_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// The longest request line, not counting its method and target.
const MAX_REQUEST_LINE_SUFFIX: usize = b"  HTTP/255.255\r\n".len();
/// The longest status line, not counting its reason.
const MAX_STATUS_LINE_SUFFIX: usize = b"HTTP/255.255 65535 \r\n".len();
/// The lines `generate_headers` may add after the message's headers: chunked
/// framing, Date, Connection with its keep-alive token, and the final CRLF.
const GENERATED_LINES: usize = b"Transfer-Encoding: chunked\r\n".len()
    + b"Date: Tue, 06 Oct 2026 00:00:00 GMT\r\n".len()
    + b"Connection: , keep-alive\r\n".len()
    + b"\r\n".len();
/// The generated websocket lines and Upgrade token, not counting the stored
/// accept key.
const MAX_WEBSOCKET_LINES: usize = b"Upgrade: websocket\r\n".len()
    + b"Sec-WebSocket-Accept: \r\n".len()
    + b"Sec-WebSocket-Key: 0123456789012345678901==\r\n".len()
    + b", Upgrade".len();
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum KeepaliveRequested {
    #[default]
    Unset,
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum IngressPhase {
    #[default]
    Idle,
    Headers,
    Body,
    Trailers,
    Http09,
    Upgraded,
}

/// Why ingress parsing failed, and the HTTP status a downstream codec reports for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParseErrorKind {
    /// A malformed message head (400).
    Header,
    /// A header value rejected by strict validation (400).
    HeaderContent,
    /// A parse error after the head, in the body, chunk framing or trailers (400).
    Body,
    /// The request-target used up the head budget (414, RFC 9112).
    UriTooLong,
    /// The request head is past its budget for another reason (431, RFC 6585).
    HeaderFieldsTooLarge,
    /// A transfer coding the codec does not implement (501, RFC 9112).
    NotImplemented,
}

#[cfg(test)]
std::thread_local! {
    static DYN_INGRESS_DELIVERIES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn dyn_ingress_deliveries() -> usize {
    DYN_INGRESS_DELIVERIES.with(Cell::get)
}

/// Work counts of the egress path, checked by tests.
#[cfg(test)]
#[derive(Debug, Default)]
struct EgressWork {
    /// Vectors with a heap capacity that `push_output` queues. Strings built
    /// by `format!`, queued or not, are counted in `FORMATTED_STRINGS`.
    byte_buffers: Cell<usize>,
    /// Whole queues that `append_output` moves onto another queue.
    appended_queues: Cell<usize>,
}

#[cfg(test)]
std::thread_local! {
    /// Strings built by `format!` in this file after the counting macro below.
    static FORMATTED_STRINGS: Cell<usize> = const { Cell::new(0) };
}

/// Strings built by `format!` on this thread, checked by tests.
#[cfg(test)]
fn formatted_strings() -> usize {
    FORMATTED_STRINGS.with(Cell::get)
}

#[cfg(test)]
fn count_work(counter: &Cell<usize>, amount: usize) {
    counter.set(counter.get() + amount);
}

/// The bytes that end an egress message. Chunked egress ends with the final
/// CRLF, preceded by the last chunk if it was not written yet.
#[derive(Clone, Copy, Debug)]
enum MessageEnd {
    Nothing,
    Crlf,
    LastChunk,
}

impl MessageEnd {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::Nothing => b"",
            Self::Crlf => b"\r\n",
            Self::LastChunk => b"0\r\n\r\n",
        }
    }

    /// The bytes after a body that opened a chunk: the chunk's CRLF, then
    /// [`bytes`](Self::bytes), as one static string.
    fn after_chunk(self) -> &'static [u8] {
        match self {
            Self::Nothing => b"\r\n",
            Self::Crlf => b"\r\n\r\n",
            Self::LastChunk => b"\r\n0\r\n\r\n",
        }
    }
}

/// A worker-local, incremental HTTP/1 codec.
pub struct Http1xCodec {
    direction: TransportDirection,
    force_1_1: bool,
    strict_validation: bool,
    downstream_upgrade_as_ordinary: bool,
    unframed_bodiless_requests: bool,
    /// The last request stream parsed under `downstream_upgrade_as_ordinary`, or unspecified.
    last_ordinary_request: StreamId,
    upstream_upgrade_as_ordinary: bool,
    /// Whether the last request an upstream codec generated was sent as ordinary, so that its
    /// response is read as an ordinary response.
    ordinary_request_sent: bool,
    release_egress_after_request: bool,
    add_date_to_response: bool,
    parser: Option<Box<Parser>>,
    phase: IngressPhase,
    header_name: Vec<u8>,
    header_value: Vec<u8>,
    target: Vec<u8>,
    target_complete: bool,
    parsed_target: Option<UrlComponents>,
    reason: Vec<u8>,
    trailers: Option<HttpHeaders>,
    header_bytes: usize,
    header_reserve_hint: usize,
    protocol_bytes: usize,
    http09_request: bool,
    http09_synthetic: bool,
    pending_message_complete: bool,
    current_message: Option<HttpMessage>,
    ingress_stream: StreamId,
    egress_stream: StreamId,
    parser_paused: bool,
    upgrade_pause: bool,
    parser_error: bool,
    request_pending: bool,
    response_pending: bool,
    egress_chunked: bool,
    in_chunk: bool,
    last_chunk_written: bool,
    keepalive: bool,
    disable_keepalive_pending: bool,
    keepalive_requested: KeepaliveRequested,
    connect_request: bool,
    head_request: bool,
    expect_no_response_body: bool,
    may_chunk_egress: bool,
    informational_response: bool,
    ingress_upgrade: bool,
    ingress_upgrade_complete: bool,
    egress_upgrade: bool,
    upgrade_header: Vec<u8>,
    websocket_accept_key: Vec<u8>,
    user_agent: Vec<u8>,
    expect_http_09_response: bool,
    #[cfg(test)]
    egress_work: EgressWork,
    _worker_local: PhantomData<Rc<()>>,
}

#[derive(Clone)]
struct EgressRollback {
    egress_stream: StreamId,
    request_pending: bool,
    response_pending: bool,
    egress_chunked: bool,
    in_chunk: bool,
    last_chunk_written: bool,
    keepalive: bool,
    disable_keepalive_pending: bool,
    connect_request: bool,
    ordinary_request_sent: bool,
    head_request: bool,
    expect_no_response_body: bool,
    may_chunk_egress: bool,
    informational_response: bool,
    ingress_upgrade: bool,
    egress_upgrade: bool,
    upgrade_header: Vec<u8>,
    websocket_accept_key: Vec<u8>,
    expect_http_09_response: bool,
}

impl Http1xCodec {
    /// Creates a codec using llhttp's HTTP/1 parsing rules.
    pub fn new(direction: TransportDirection) -> Self {
        Self::with_options(direction, false, false)
    }

    /// Creates a codec with explicit HTTP/1.1 forcing and strict validation.
    pub fn with_options(
        direction: TransportDirection,
        force_1_1: bool,
        strict_validation: bool,
    ) -> Self {
        Self {
            direction,
            force_1_1,
            strict_validation,
            downstream_upgrade_as_ordinary: false,
            unframed_bodiless_requests: false,
            last_ordinary_request: StreamId::UNSPECIFIED,
            upstream_upgrade_as_ordinary: false,
            ordinary_request_sent: false,
            release_egress_after_request: false,
            add_date_to_response: true,
            parser: Some(Box::new(Parser::new(if direction.is_downstream() {
                ParserType::Request
            } else {
                ParserType::Response
            }))),
            phase: IngressPhase::Idle,
            header_name: Vec::new(),
            header_value: Vec::new(),
            target: Vec::new(),
            target_complete: false,
            parsed_target: None,
            reason: Vec::new(),
            trailers: None,
            header_bytes: 0,
            header_reserve_hint: 0,
            protocol_bytes: 0,
            http09_request: false,
            http09_synthetic: false,
            pending_message_complete: false,
            current_message: None,
            ingress_stream: StreamId::UNSPECIFIED,
            egress_stream: StreamId::UNSPECIFIED,
            parser_paused: false,
            upgrade_pause: false,
            parser_error: false,
            request_pending: false,
            response_pending: false,
            egress_chunked: false,
            in_chunk: false,
            last_chunk_written: false,
            keepalive: true,
            disable_keepalive_pending: false,
            keepalive_requested: KeepaliveRequested::Unset,
            connect_request: false,
            head_request: false,
            expect_no_response_body: false,
            may_chunk_egress: false,
            informational_response: false,
            ingress_upgrade: false,
            ingress_upgrade_complete: false,
            egress_upgrade: false,
            upgrade_header: Vec::new(),
            websocket_accept_key: Vec::new(),
            user_agent: Vec::new(),
            expect_http_09_response: false,
            #[cfg(test)]
            egress_work: EgressWork::default(),
            _worker_local: PhantomData,
        }
    }

    /// Creates a response-only codec with an explicitly known chunking policy.
    pub fn make_response_codec(may_chunk_egress: bool) -> Self {
        let mut codec = Self::new(TransportDirection::Downstream);
        codec.may_chunk_egress = may_chunk_egress;
        codec
    }

    /// Sets whether to reject ingress header values that contain a byte of 0x7f or above.
    pub fn set_strict_validation(&mut self, strict: bool) {
        self.strict_validation = strict;
    }

    /// Parses each downstream request whose head is read after this call as an ordinary
    /// request, including one with an `Upgrade` header or a `CONNECT` method, so that parsing
    /// continues behind it. Off by default. It has no effect on an upstream codec.
    ///
    /// The bytes behind such a request may already be parsed as HTTP/1.1, so generating a 101
    /// or WebSocket upgrade response to it, or to any request before it, fails.
    pub fn set_downstream_upgrade_as_ordinary(&mut self, enabled: bool) {
        self.downstream_upgrade_as_ordinary = enabled;
    }

    /// Sends each request that an upstream codec generates after this call as an ordinary
    /// request, including one with an `Upgrade` header or a `CONNECT` method, and reads its
    /// response as an ordinary response. Off by default. It has no effect on a downstream codec.
    pub fn set_upstream_upgrade_as_ordinary(&mut self, enabled: bool) {
        self.upstream_upgrade_as_ordinary = enabled;
    }

    /// Sends an upstream request without body framing when its head ends the message and it
    /// carries neither `Content-Length` nor a chunked `Transfer-Encoding`, as hyper sends a
    /// request with an empty body. Off by default. It has no effect on a downstream codec.
    ///
    /// Without the option, such a `POST` gets `Transfer-Encoding: chunked` and an empty last
    /// chunk, or `Connection: close` where chunking is not allowed. A head that carries the
    /// caller's own framing is generated as without the option, so a caller's chunked
    /// `Transfer-Encoding` with no body still closes the connection. A request that has a body
    /// is framed normally.
    pub fn set_unframed_bodiless_requests(&mut self, enabled: bool) {
        self.unframed_bodiless_requests = enabled;
    }

    /// Releases upstream egress ownership as soon as the request completes.
    pub fn set_release_egress_after_request(&mut self, release: bool) {
        self.release_egress_after_request = release;
    }

    /// Sets whether a downstream codec adds a `Date` header to a response that lacks one. On by
    /// default. It has no effect on an upstream codec.
    pub fn set_add_date_header_to_response(&mut self, enabled: bool) {
        self.add_date_to_response = enabled;
    }

    /// Resolves a downstream upgrade paused after request headers.
    pub fn resolve_downstream_upgrade(&mut self, response: &HttpMessage) -> Option<bool> {
        if !self.direction.is_downstream() {
            return None;
        }
        // As in `generate_headers`, a request parsed as ordinary, or one before it, never
        // switches protocols.
        let accepted = if self.is_at_or_before_an_ordinary_request(self.ingress_stream) {
            false
        } else if self.connect_request {
            let mut response = response.clone();
            response
                .status_code()
                .is_ok_and(|status| (200..300).contains(&status))
        } else {
            response.is_egress_websocket_upgrade()
        };
        self.ingress_upgrade = accepted;
        self.ingress_upgrade_complete = false;
        if let Some(parser) = self.parser.as_mut() {
            parser.upgrade = u8::from(accepted);
        }
        if self.phase == IngressPhase::Upgraded {
            self.ingress_upgrade_complete = accepted;
            if !accepted {
                self.phase = IngressPhase::Idle;
                if let Some(parser) = self.parser.as_mut() {
                    parser.resume_after_upgrade();
                }
            }
            return Some(accepted);
        }
        matches!(self.phase, IngressPhase::Body).then_some(accepted)
    }

    /// Whether `stream` is a request parsed under `downstream_upgrade_as_ordinary`, or one before
    /// such a request.
    fn is_at_or_before_an_ordinary_request(&self, stream: StreamId) -> bool {
        !self.last_ordinary_request.is_unspecified() && stream <= self.last_ordinary_request
    }

    /// First user-agent value observed on this connection.
    pub fn user_agent(&self) -> &[u8] {
        &self.user_agent
    }

    /// Whether an ALPN value names an HTTP/1 protocol supported by this codec.
    pub fn supports_next_protocol(protocol: &[u8]) -> bool {
        matches!(protocol, b"http/1.0" | b"http/1.1")
    }

    /// Immediately makes the connection non-reusable without emitting bytes.
    pub fn generate_immediate_goaway(&mut self) -> GeneratedFrame {
        self.keepalive = false;
        GeneratedFrame::default()
    }

    fn dispatch(
        &mut self,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
        result: &mut CodecIngressResult,
    ) {
        result.append_callback_result(callback.deliver(self, event, callback_egress));
    }

    fn dispatch_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
        result: &mut CodecCallbackResult,
    ) {
        result.append(HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            event,
        ));
    }

    fn begin_message(
        &mut self,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if self.direction.is_downstream() || !self.informational_response {
            self.ingress_stream = StreamId::new(self.ingress_stream.get().saturating_add(1));
        }
        if self.direction.is_upstream() {
            self.informational_response = false;
        } else {
            self.request_pending = true;
            self.response_pending = true;
        }
        let mut message = HttpMessage::new();
        if self.direction.is_upstream() {
            let _ = message.set_status_code(0);
        }
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::MessageBegin {
                stream: self.ingress_stream,
                message: &mut message,
            },
            result,
        );
        self.current_message = Some(message);
    }

    fn begin_http_09_response(
        &mut self,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if self.current_message.is_none() {
            self.begin_message(callback, callback_egress, result);
        }
        if self.parser_paused {
            return;
        }
        let mut message = self.current_message.take().unwrap_or_default();
        message.set_http_version(0, 9);
        let _ = message.set_status_code(200);
        message.set_ingress_header_size(HttpHeaderSize::default());
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::HeadersComplete {
                stream: self.ingress_stream,
                message: Box::new(message),
            },
            result,
        );
        self.phase = IngressPhase::Http09;
    }

    fn complete_message(
        &mut self,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
        upgraded: bool,
    ) {
        if self.direction.is_downstream() {
            self.request_pending = false;
        } else {
            self.response_pending = self.informational_response;
            if self.informational_response && !self.ingress_upgrade {
                self.phase = IngressPhase::Idle;
                return;
            }
        }
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::MessageComplete {
                stream: self.ingress_stream,
                upgraded,
            },
            result,
        );
        if upgraded {
            self.ingress_upgrade_complete = true;
            self.upgrade_pause |= !self.parser_paused;
            self.parser_paused = true;
            self.phase = IngressPhase::Upgraded;
        } else {
            self.phase = IngressPhase::Idle;
        }
    }

    fn fail(
        &mut self,
        kind: ParseErrorKind,
        message: &'static [u8],
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if self.parser_error {
            return;
        }
        self.parser_error = true;
        self.parser_paused = true;
        let mut error = HttpException::new(HttpExceptionDirection::Ingress, message);
        error.set_proxygen_error(match kind {
            ParseErrorKind::Header
            | ParseErrorKind::UriTooLong
            | ParseErrorKind::HeaderFieldsTooLarge
            | ParseErrorKind::NotImplemented => ProxygenError::PARSE_HEADER,
            ParseErrorKind::HeaderContent => ProxygenError::HEADER_CONTENT_VALIDATION,
            ParseErrorKind::Body => ProxygenError::PARSE_BODY,
        });
        if self.direction.is_downstream() && self.egress_stream < self.ingress_stream {
            error.set_http_status_code(match kind {
                ParseErrorKind::UriTooLong => 414,
                ParseErrorKind::HeaderFieldsTooLarge => 431,
                ParseErrorKind::NotImplemented => 501,
                ParseErrorKind::Header | ParseErrorKind::HeaderContent | ParseErrorKind::Body => {
                    400
                }
            });
        }
        error.set_partial_message(self.current_message.take());
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::Error {
                stream: self.ingress_stream,
                error: &error,
                new_stream: false,
            },
            result,
        );
    }

    fn fail_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecCallbackResult,
    ) {
        if self.parser_error {
            return;
        }
        self.parser_error = true;
        self.parser_paused = true;
        let mut error = HttpException::new(HttpExceptionDirection::Ingress, b"unexpected EOF");
        error.set_proxygen_error(ProxygenError::EOF);
        if self.direction.is_downstream() && self.egress_stream < self.ingress_stream {
            error.set_http_status_code(400);
        }
        error.set_partial_message(self.current_message.take());
        self.dispatch_eof(
            callback,
            callback_egress,
            CodecEvent::Error {
                stream: self.ingress_stream,
                error: &error,
                new_stream: false,
            },
            result,
        );
    }

    /// How many bytes of a body run may be parsed or forwarded before the codec calls
    /// `HttpCodecCallback::ingress_body_limit` again. It is at least one, so that parsing always
    /// progresses.
    #[inline]
    fn body_limit(&self, callback: &(impl IngressDelivery + ?Sized)) -> usize {
        callback
            .ingress_body_limit(self.ingress_stream)
            .map_or(usize::MAX, |limit| limit.max(1))
    }

    /// Forwards `input` (tunnel or HTTP/0.9 response bytes) as body events within the body
    /// limit. It stops when the input runs out or a callback pauses parsing. `result.consumed`
    /// counts the bytes forwarded.
    fn forward_body(
        &mut self,
        input: &Bytes,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        let mut offset = 0;
        while offset < input.len() {
            let end = input
                .len()
                .min(offset.saturating_add(self.body_limit(&*callback)));
            let body = if offset == 0 && end == input.len() {
                input.clone()
            } else {
                input.slice(offset..end)
            };
            result.consumed = end;
            self.emit_body(body, callback, callback_egress, result);
            offset = end;
            if self.parser_error || (self.parser_paused && !self.upgrade_pause) {
                break;
            }
        }
    }

    fn emit_body(
        &mut self,
        body: Bytes,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if body.is_empty() {
            return;
        }
        let body_length = body.len();
        let queue = BufQueue::from_bytes(body, body_length)
            .expect("an exactly sized ingress queue must accept its payload");
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::Body {
                stream: self.ingress_stream,
                body: queue,
                padding: 0,
            },
            result,
        );
    }

    fn is_egress_busy(&self) -> bool {
        (self.direction.is_downstream() && self.response_pending)
            || (self.direction.is_upstream()
                && (self.request_pending
                    || (!self.egress_upgrade
                        && self.response_pending
                        && !self.release_egress_after_request)))
    }

    fn egress_rollback(&self) -> EgressRollback {
        EgressRollback {
            egress_stream: self.egress_stream,
            request_pending: self.request_pending,
            response_pending: self.response_pending,
            egress_chunked: self.egress_chunked,
            in_chunk: self.in_chunk,
            last_chunk_written: self.last_chunk_written,
            keepalive: self.keepalive,
            disable_keepalive_pending: self.disable_keepalive_pending,
            connect_request: self.connect_request,
            ordinary_request_sent: self.ordinary_request_sent,
            head_request: self.head_request,
            expect_no_response_body: self.expect_no_response_body,
            may_chunk_egress: self.may_chunk_egress,
            informational_response: self.informational_response,
            ingress_upgrade: self.ingress_upgrade,
            egress_upgrade: self.egress_upgrade,
            upgrade_header: self.upgrade_header.clone(),
            websocket_accept_key: self.websocket_accept_key.clone(),
            expect_http_09_response: self.expect_http_09_response,
        }
    }

    fn restore_egress(&mut self, rollback: EgressRollback) {
        self.egress_stream = rollback.egress_stream;
        self.request_pending = rollback.request_pending;
        self.response_pending = rollback.response_pending;
        self.egress_chunked = rollback.egress_chunked;
        self.in_chunk = rollback.in_chunk;
        self.last_chunk_written = rollback.last_chunk_written;
        self.keepalive = rollback.keepalive;
        self.disable_keepalive_pending = rollback.disable_keepalive_pending;
        self.connect_request = rollback.connect_request;
        self.ordinary_request_sent = rollback.ordinary_request_sent;
        self.head_request = rollback.head_request;
        self.expect_no_response_body = rollback.expect_no_response_body;
        self.may_chunk_egress = rollback.may_chunk_egress;
        self.informational_response = rollback.informational_response;
        self.ingress_upgrade = rollback.ingress_upgrade;
        self.egress_upgrade = rollback.egress_upgrade;
        self.upgrade_header = rollback.upgrade_header;
        self.websocket_accept_key = rollback.websocket_accept_key;
        self.expect_http_09_response = rollback.expect_http_09_response;
    }

    #[inline(never)]
    fn generate_headers(
        &mut self,
        message: &HttpMessage,
        stream: StreamId,
        end_stream: bool,
        extra_headers: Option<&HttpHeaders>,
    ) -> Result<(Vec<u8>, HttpHeaderSize), HttpError> {
        if self.keepalive && self.disable_keepalive_pending {
            self.keepalive = false;
        }
        let upstream = self.direction.is_upstream();
        let previous_informational = self.informational_response;
        let mut request = None;
        let mut method = None;
        let mut response = (0, b"".as_slice());
        if upstream {
            if stream != self.egress_stream {
                return Err(invalid_state(b"HTTP/1 request stream is out of order"));
            }
            // A websocket upgrade needs a 101, which a request sent as ordinary cannot accept.
            if self.upstream_upgrade_as_ordinary && message.is_egress_websocket_upgrade() {
                return Err(invalid_state(
                    b"HTTP/1 egress websocket upgrade on a request sent as ordinary",
                ));
            }
            if self.upstream_upgrade_as_ordinary
                && !self.frames_ordinary_request(message, extra_headers, end_stream)
            {
                return Err(invalid_state(
                    b"HTTP/1 request sent as ordinary has ambiguous framing",
                ));
            }
            self.request_pending = true;
            self.response_pending = true;
            request = request_fields(message);
            method = request.and_then(|(name, _)| HttpMethod::from_bytes(name.as_bytes()));
            // Under the option a CONNECT is an ordinary request. It has no egress upgrade or
            // tunnel, and its response is read by its own framing.
            self.ordinary_request_sent = self.upstream_upgrade_as_ordinary;
            self.connect_request =
                method == Some(HttpMethod::Connect) && !self.ordinary_request_sent;
            self.head_request = method == Some(HttpMethod::Head);
            self.expect_no_response_body = self.connect_request || self.head_request;
        } else {
            let next = StreamId::new(self.egress_stream.get().saturating_add(1));
            if (stream != next && !(stream == self.egress_stream && previous_informational))
                || stream > self.ingress_stream
            {
                return Err(invalid_state(b"HTTP/1 response stream is out of order"));
            }
            if !previous_informational {
                self.egress_stream = next;
            }
            response = response_fields(message)
                .ok_or_else(|| invalid_state(b"HTTP/1 response requires a status"))?;
            // The bytes behind a request parsed as ordinary may already be parsed as
            // HTTP/1.1, so the connection cannot switch protocols in a response to it or to any
            // request before it.
            if self.is_at_or_before_an_ordinary_request(stream)
                && (response.0 == 101 || message.is_egress_websocket_upgrade())
            {
                return Err(invalid_state(
                    b"HTTP/1 upgrade response after a request parsed as ordinary",
                ));
            }
            self.informational_response =
                response.0 / 100 == 1 || message.is_egress_websocket_upgrade();
            self.expect_no_response_body = self.connect_request
                || self.head_request
                || rfc2616::response_body_must_be_empty(u32::from(response.0));
        }

        let version = self.egress_version(message);
        let egress_websocket = message.is_egress_websocket_upgrade();
        let status = if upstream {
            0
        } else if egress_websocket {
            101
        } else {
            response.0
        };
        if !upstream {
            if self.connect_request && (200..300).contains(&status) {
                self.egress_upgrade = true;
            } else if status == 101 {
                self.ingress_upgrade = true;
                self.egress_upgrade = true;
            } else if self.connect_request && self.ingress_upgrade {
                self.ingress_upgrade = false;
                self.keepalive = false;
            }
        } else if self.connect_request || egress_websocket {
            self.egress_upgrade = true;
        }

        self.egress_chunked = message.is_chunked() && !self.egress_upgrade;
        self.last_chunk_written = false;
        let mut output;
        if upstream {
            let method_name = if egress_websocket {
                "GET"
            } else {
                request
                    .ok_or_else(|| invalid_state(b"HTTP/1 request requires a method"))?
                    .0
            };
            if method_name.is_empty() {
                return Err(invalid_state(b"HTTP/1 request requires a method"));
            }
            let url = request
                .ok_or_else(|| invalid_state(b"HTTP/1 request requires a URL"))?
                .1;
            let target = if method == Some(HttpMethod::Connect) && url.is_empty() {
                message.headers().get_single_or_empty(HttpHeaderCode::Host)
            } else {
                url
            };
            let start_line = method_name.len() + target.len() + MAX_REQUEST_LINE_SUFFIX;
            output = Vec::with_capacity(
                start_line + self.header_capacity(message, extra_headers, end_stream),
            );
            output.extend_from_slice(method_name.as_bytes());
            output.push(b' ');
            output.extend_from_slice(target);
            if version != (0, 9) {
                output.extend_from_slice(b" HTTP/");
                push_decimal(&mut output, u16::from(version.0));
                output.push(b'.');
                push_decimal(&mut output, u16::from(version.1));
            }
            output.extend_from_slice(b"\r\n");
            self.may_chunk_egress = version.0 == 1 && version.1 >= 1;
            if !self.upgrade_header.is_empty() {
                self.upgrade_header.clear();
            }
        } else {
            if status == 0 {
                return Err(invalid_state(b"HTTP/1 response requires a status"));
            }
            if version == (0, 9) {
                if end_stream {
                    self.finish_egress_message();
                }
                return Ok((Vec::new(), HttpHeaderSize::default()));
            }
            let reason = if egress_websocket {
                HttpMessage::default_reason(101)
            } else {
                response.1
            };
            let start_line = reason.len() + MAX_STATUS_LINE_SUFFIX;
            output = Vec::with_capacity(
                start_line + self.header_capacity(message, extra_headers, end_stream),
            );
            output.extend_from_slice(b"HTTP/");
            push_decimal(&mut output, u16::from(version.0));
            output.push(b'.');
            push_decimal(&mut output, u16::from(version.1));
            output.push(b' ');
            push_decimal(&mut output, status);
            output.push(b' ');
            output.extend_from_slice(reason);
            output.extend_from_slice(b"\r\n");
        }
        if self.keepalive
            && (!message.wants_keepalive()
                || version.0 < 1
                || (!upstream
                    && version == (1, 0)
                    && self.keepalive_requested != KeepaliveRequested::Enabled))
        {
            self.keepalive = false;
        }
        self.egress_chunked &= self.may_chunk_egress;
        if version == (0, 9) {
            self.expect_http_09_response = upstream;
            if end_stream {
                self.finish_egress_message();
            }
            let size = header_size(output.len());
            return Ok((output, size));
        }

        let mut deferred_content_length = None;
        let mut transfer_encoding_written = false;
        let mut has_date = false;
        let mut has_upgrade = false;
        let mut connection_header_seen = false;
        let mut explicit_keepalive = false;
        let mut connection_tokens = Vec::<&[u8]>::new();
        let mut has_upgrade_connection_token = false;
        let extra = extra_headers.into_iter().flat_map(HttpHeaders::iter);
        for (code, name, value) in message.headers().iter().chain(extra) {
            if code == HttpHeaderCode::ContentLength {
                deferred_content_length = Some(value);
                continue;
            }
            if code == HttpHeaderCode::Connection
                && (!self.informational_response || egress_websocket)
            {
                connection_header_seen = true;
                for token in value.split(|byte| *byte == b',') {
                    let token = trim_http_whitespace(token);
                    if token.eq_ignore_ascii_case(b"upgrade") {
                        has_upgrade_connection_token = true;
                    }
                    if token.eq_ignore_ascii_case(b"close") {
                        self.keepalive = false;
                    } else if token.eq_ignore_ascii_case(b"keep-alive") {
                        explicit_keepalive = true;
                    } else if !token.is_empty() {
                        connection_tokens.push(token);
                    }
                }
                continue;
            }
            if code == HttpHeaderCode::Upgrade {
                has_upgrade = true;
                if upstream {
                    self.upgrade_header = value.to_vec();
                }
            } else if code == HttpHeaderCode::TransferEncoding && !transfer_encoding_written {
                if !value.eq_ignore_ascii_case(b"chunked") {
                    continue;
                }
                transfer_encoding_written = true;
                if !self.may_chunk_egress {
                    continue;
                }
            } else if code == HttpHeaderCode::Date {
                has_date = true;
            } else if egress_websocket
                && matches!(
                    code,
                    HttpHeaderCode::SecWebSocketKey | HttpHeaderCode::SecWebSocketAccept
                )
            {
                continue;
            }
            if holds_cr_or_lf(value) {
                continue;
            }
            output.extend_from_slice(name);
            output.extend_from_slice(b": ");
            output.extend_from_slice(value);
            output.extend_from_slice(b"\r\n");
        }

        // Only a head that ends its message and has none of the caller's framing
        // goes out unframed.
        let unframed = end_stream
            && self.unframed_bodiless_requests
            && !transfer_encoding_written
            && deferred_content_length.is_none();
        let body_check = (!upstream && !self.expect_no_response_body && !self.egress_upgrade)
            || (upstream && !unframed && (method == Some(HttpMethod::Post) || self.egress_chunked));
        self.egress_chunked &= transfer_encoding_written;
        if body_check && !self.egress_chunked && deferred_content_length.is_none() {
            if !transfer_encoding_written && self.may_chunk_egress {
                output.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
                self.egress_chunked = true;
            } else {
                self.keepalive = false;
            }
        }
        if !upstream && !has_date && self.add_date_to_response {
            output.extend_from_slice(b"Date: ");
            output.extend_from_slice(HttpMessage::date_header().as_bytes());
            output.extend_from_slice(b"\r\n");
        }
        if egress_websocket && !has_upgrade {
            output.extend_from_slice(b"Upgrade: websocket\r\n");
            if upstream {
                self.upgrade_header = b"websocket".to_vec();
                let key = websocket_key(stream);
                output.extend_from_slice(b"Sec-WebSocket-Key: ");
                output.extend_from_slice(&key);
                output.extend_from_slice(b"\r\n");
                self.websocket_accept_key = websocket_accept(&key);
            } else {
                output.extend_from_slice(b"Sec-WebSocket-Accept: ");
                output.extend_from_slice(&self.websocket_accept_key);
                output.extend_from_slice(b"\r\n");
            }
            if !has_upgrade_connection_token {
                connection_tokens.push(b"Upgrade");
            }
        }
        let omit_implicit_http11_keepalive = upstream
            && version == (1, 1)
            && self.keepalive
            && connection_header_seen
            && !explicit_keepalive
            && connection_tokens.is_empty();
        if (!self.informational_response || upstream || !connection_tokens.is_empty())
            && !omit_implicit_http11_keepalive
        {
            output.extend_from_slice(b"Connection: ");
            for (index, token) in connection_tokens.iter().enumerate() {
                if index != 0 {
                    output.extend_from_slice(b", ");
                }
                output.extend_from_slice(token);
            }
            if !self.informational_response {
                if !connection_tokens.is_empty() {
                    output.extend_from_slice(b", ");
                }
                output.extend_from_slice(if self.keepalive {
                    b"keep-alive"
                } else {
                    b"close"
                });
            }
            output.extend_from_slice(b"\r\n");
        }
        if let Some(content_length) = deferred_content_length {
            output.extend_from_slice(b"Content-Length: ");
            output.extend_from_slice(content_length);
            output.extend_from_slice(b"\r\n");
        }
        output.extend_from_slice(b"\r\n");
        if end_stream {
            self.append_eom(&mut output);
            self.finish_egress_message();
        }
        let size = header_size(output.len());
        Ok((output, size))
    }

    /// The HTTP version `generate_headers` writes for `message`.
    fn egress_version(&self, message: &HttpMessage) -> (u8, u8) {
        let mut version = message.http_version();
        if version > (1, 1) {
            version = (1, 1);
        }
        if self.force_1_1
            && version < (1, 1)
            && !(self.direction.is_downstream() && version == (0, 9))
        {
            version = (1, 1);
        }
        version
    }

    /// Whether the head of a request sent as ordinary marks exactly where the request ends, as
    /// this codec frames it.
    ///
    /// The head is checked as `generate_headers` writes it, from `message`'s headers and then
    /// `extra_headers`. It may carry at most one `Content-Length`, with a plain decimal value
    /// that must be 0 if the head ends the message. It may carry at most one written
    /// `Transfer-Encoding` line, never together with a `Content-Length`, and only when this
    /// codec chunk-frames the body. If the head does not end the message, the body must be
    /// framed by that length or by chunking.
    ///
    /// With [`set_upstream_upgrade_as_ordinary`](Self::set_upstream_upgrade_as_ordinary) on,
    /// generating a request for which this is false fails. It is always true for a request with
    /// neither an `Upgrade` header nor a `CONNECT` method.
    pub fn frames_ordinary_request(
        &self,
        message: &HttpMessage,
        extra_headers: Option<&HttpHeaders>,
        end_stream: bool,
    ) -> bool {
        let method =
            request_fields(message).and_then(|(name, _)| HttpMethod::from_bytes(name.as_bytes()));
        let headers = || {
            message
                .headers()
                .iter()
                .chain(extra_headers.into_iter().flat_map(HttpHeaders::iter))
        };
        if method != Some(HttpMethod::Connect)
            && !headers().any(|(code, _, _)| code == HttpHeaderCode::Upgrade)
        {
            return true;
        }
        let version = self.egress_version(message);
        if version == (0, 9) {
            // No header line is written, so a body cannot be framed.
            return end_stream;
        }
        let may_chunk = version.0 == 1 && version.1 >= 1;
        let mut lengths = Vec::new();
        let mut chunked_seen = false;
        let mut transfer_encodings = 0_usize;
        // This follows the header loop of `generate_headers`. The last `Content-Length` is
        // written. A `Transfer-Encoding` line before the first chunked one is dropped, as is
        // that line on HTTP/1.0, and every later line without CR or LF is written verbatim.
        for (code, _, value) in headers() {
            if code == HttpHeaderCode::ContentLength {
                lengths.push(value);
            } else if code == HttpHeaderCode::TransferEncoding {
                if !chunked_seen {
                    if !value.eq_ignore_ascii_case(b"chunked") {
                        continue;
                    }
                    chunked_seen = true;
                    if !may_chunk {
                        continue;
                    }
                }
                if !value.iter().any(|byte| matches!(*byte, b'\r' | b'\n')) {
                    transfer_encodings += 1;
                }
            }
        }
        // As in `generate_headers`, a message marked chunked is chunked under its chunked line,
        // and a POST or such a message with neither framing header gets a generated one.
        let marked = message.is_chunked() && may_chunk;
        let generated = (method == Some(HttpMethod::Post) || marked)
            && !chunked_seen
            && lengths.is_empty()
            && may_chunk;
        let chunk_framed = (marked && chunked_seen) || generated;
        transfer_encodings += usize::from(generated);
        let length = match lengths.as_slice() {
            [] => None,
            [value] => match parse_decimal_u64(value) {
                Some(length) => Some(length),
                None => return false,
            },
            _ => return false,
        };
        if transfer_encodings > 1
            || (transfer_encodings != 0 && (length.is_some() || !chunk_framed))
        {
            return false;
        }
        if end_stream {
            length.is_none_or(|length| length == 0)
        } else {
            length.is_some() || chunk_framed
        }
    }

    /// Bytes for every header line and generated line that can follow a start
    /// line. Connection tokens are rejoined with `", "`, so each comma in a
    /// Connection value counts one more byte.
    fn header_capacity(
        &self,
        message: &HttpMessage,
        extra_headers: Option<&HttpHeaders>,
        end_stream: bool,
    ) -> usize {
        let extra = extra_headers.into_iter().flat_map(HttpHeaders::iter);
        let headers: usize = message
            .headers()
            .iter()
            .chain(extra)
            .map(|(code, name, value)| {
                let separators = if code == HttpHeaderCode::Connection {
                    value.iter().filter(|byte| **byte == b',').count()
                } else {
                    0
                };
                name.len() + value.len() + b": \r\n".len() + separators
            })
            .sum();
        let websocket = if message.is_egress_websocket_upgrade() {
            MAX_WEBSOCKET_LINES + self.websocket_accept_key.len()
        } else {
            0
        };
        headers + GENERATED_LINES + websocket + if end_stream { b"0\r\n\r\n".len() } else { 0 }
    }

    fn append_eom(&mut self, output: &mut Vec<u8>) {
        output.extend_from_slice(self.message_end().bytes());
    }

    /// Ends the egress message's framing and returns the bytes that end it.
    fn message_end(&mut self) -> MessageEnd {
        if !self.egress_chunked {
            return MessageEnd::Nothing;
        }
        if self.head_request && self.direction.is_downstream() {
            self.last_chunk_written = true;
            return MessageEnd::Nothing;
        }
        if self.last_chunk_written {
            MessageEnd::Crlf
        } else {
            self.last_chunk_written = true;
            MessageEnd::LastChunk
        }
    }

    fn finish_egress_message(&mut self) {
        if self.direction.is_downstream() {
            self.response_pending = false;
        } else {
            self.request_pending = false;
        }
    }

    fn push_static_output(output: &mut BufQueue, bytes: &'static [u8]) -> Result<(), HttpError> {
        output.push_static(bytes).map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
        })
    }

    fn push_output(&self, output: &mut BufQueue, bytes: Vec<u8>) -> Result<(), HttpError> {
        #[cfg(test)]
        if bytes.capacity() != 0 {
            count_work(&self.egress_work.byte_buffers, 1);
        }
        output.push_vec(bytes).map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
        })
    }

    fn append_output(&self, output: &mut BufQueue, other: &mut BufQueue) -> Result<(), HttpError> {
        #[cfg(test)]
        count_work(&self.egress_work.appended_queues, 1);
        output.append(other).map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
        })
    }

    fn push_chunk_line(
        &self,
        output: &mut BufQueue,
        line: Cow<'static, [u8]>,
    ) -> Result<(), HttpError> {
        match line {
            Cow::Borrowed(line) => Self::push_static_output(output, line),
            Cow::Owned(line) => self.push_output(output, line),
        }
    }

    /// Fails with the error that appending `incoming` bytes to `output` would
    /// return, so that a frame queued in parts is queued whole or not at all.
    fn check_output_room(output: &BufQueue, incoming: usize) -> Result<(), HttpError> {
        let current = output.chain_length();
        let error = match current.checked_add(incoming) {
            Some(length) if length <= output.limit() => return Ok(()),
            Some(_) => BufQueueError::LimitExceeded {
                limit: output.limit(),
                current,
                incoming,
            },
            None => BufQueueError::LengthOverflow { current, incoming },
        };
        Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            error.to_string(),
        ))
    }

    /// Returns the length of a frame of `prefix`, `body` and `suffix` bytes.
    /// Fails with the error that staging the frame in one queue and appending
    /// it to `output` would return, including when the length overflows.
    fn check_frame_room(
        output: &BufQueue,
        prefix: usize,
        body: usize,
        suffix: usize,
    ) -> Result<usize, HttpError> {
        let overflow = |current, incoming| {
            HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                BufQueueError::LengthOverflow { current, incoming }.to_string(),
            )
        };
        let framed = prefix
            .checked_add(body)
            .ok_or_else(|| overflow(prefix, body))?;
        let written = framed
            .checked_add(suffix)
            .ok_or_else(|| overflow(framed, suffix))?;
        Self::check_output_room(output, written)?;
        Ok(written)
    }
}

/// Counts every string built by `format!` from here to the end of this file
/// (the `generate` arms and the mounted test modules), so that tests catch any
/// chunk prefix formatted through `core::fmt`.
#[cfg(test)]
macro_rules! format {
    ($($argument:tt)*) => {{
        $crate::FORMATTED_STRINGS.with(|count| count.set(count.get() + 1));
        ::std::format!($($argument)*)
    }};
}

/// Delivers one ingress callback and applies the controls it returns before
/// parsing continues.
trait IngressDelivery {
    fn deliver(
        &mut self,
        codec: &mut Http1xCodec,
        event: CodecEvent<'_>,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult;

    fn ingress_body_limit(&self, stream: StreamId) -> Option<usize>;
}

/// Calls a callback of a concrete type directly, with the same body as the
/// default [`HttpCodec::dispatch_ingress_callback`], which `Http1xCodec` does
/// not override.
impl<C: HttpCodecCallback> IngressDelivery for C {
    fn deliver(
        &mut self,
        codec: &mut Http1xCodec,
        event: CodecEvent<'_>,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        let actions = self.on_ingress_event(event);
        HttpCodec::apply_callback_actions(codec, actions, self, callback_egress)
    }

    fn ingress_body_limit(&self, stream: StreamId) -> Option<usize> {
        HttpCodecCallback::ingress_body_limit(self, stream)
    }
}

impl IngressDelivery for dyn HttpCodecCallback + '_ {
    fn deliver(
        &mut self,
        codec: &mut Http1xCodec,
        event: CodecEvent<'_>,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        #[cfg(test)]
        DYN_INGRESS_DELIVERIES.with(|deliveries| count_work(deliveries, 1));
        HttpCodec::dispatch_ingress_callback(codec, self, callback_egress, event)
    }

    fn ingress_body_limit(&self, stream: StreamId) -> Option<usize> {
        HttpCodecCallback::ingress_body_limit(self, stream)
    }
}

impl Http1xCodec {
    /// Same as [`HttpCodec::on_ingress`], but calls a callback of concrete type
    /// `C` directly instead of through `dyn HttpCodecCallback`.
    ///
    /// It parses with the same code, delivers the same callbacks in the same
    /// order, applies each callback's controls before parsing continues, and
    /// returns the same result.
    pub fn on_ingress_with_callback<C: HttpCodecCallback>(
        &mut self,
        input: &BufQueue,
        callback: &mut C,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        self.parse_ingress(input, callback, callback_egress)
    }
}

impl HttpCodec for Http1xCodec {
    fn protocol(&self) -> CodecProtocol {
        CodecProtocol::Http11
    }

    fn transport_direction(&self) -> TransportDirection {
        self.direction
    }

    fn capabilities(&self) -> CodecCapabilities {
        CodecCapabilities::default()
    }

    fn create_stream(&mut self) -> StreamId {
        if self.direction.is_downstream() {
            self.ingress_stream = StreamId::new(self.ingress_stream.get().saturating_add(1));
            self.ingress_stream
        } else {
            self.egress_stream = StreamId::new(self.egress_stream.get().saturating_add(1));
            self.egress_stream
        }
    }

    fn is_busy(&self) -> bool {
        self.request_pending || self.response_pending
    }

    fn set_parser_paused(&mut self, paused: bool) {
        if !self.parser_error {
            self.parser_paused = paused;
            self.upgrade_pause = false;
        }
    }

    fn is_parser_paused(&self) -> bool {
        self.parser_paused
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        self.parse_ingress(input, callback, callback_egress)
    }

    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        if self.phase == IngressPhase::Upgraded && self.ingress_upgrade {
            self.ingress_upgrade_complete = true;
        }
        if self.parser_error
            || (self.parser_paused && !(self.ingress_upgrade_complete && self.upgrade_pause))
        {
            return CodecCallbackResult::default();
        }
        self.finish_parser(callback, callback_egress)
    }

    fn is_reusable(&self) -> bool {
        self.keepalive
            && !self.egress_upgrade
            && !self.ingress_upgrade
            && !self.parser_error
            && self.websocket_accept_key.is_empty()
    }

    fn is_waiting_to_drain(&self) -> bool {
        self.disable_keepalive_pending && self.keepalive
    }

    fn close_on_egress_complete(&self) -> bool {
        !self.is_egress_busy() && !self.is_reusable()
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        self.ingress_stream
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        _callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let rollback = self.egress_rollback();
        let result = (|| -> Result<GeneratedFrame, HttpError> {
            match frame {
                EgressFrame::ConnectionPreface => Ok(GeneratedFrame::default()),
                EgressFrame::Headers {
                    stream,
                    message,
                    end_stream,
                    extra_headers,
                } => {
                    let (bytes, size) =
                        self.generate_headers(message, stream, end_stream, extra_headers)?;
                    let written = bytes.len();
                    self.push_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: Some(size),
                        payload_offset: None,
                    })
                }
                EgressFrame::Body {
                    stream,
                    mut body,
                    end_stream,
                    ..
                } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 body stream is out of order"));
                    }
                    let body_len = body.chain_length();
                    let opens_chunk = body_len != 0 && self.egress_chunked && !self.in_chunk;
                    let (prefix, payload_offset) = if opens_chunk {
                        let prefix = chunk_prefix(body_len);
                        let offset = prefix.len() as u64;
                        (prefix, Some(offset))
                    } else {
                        (Cow::Borrowed(&[][..]), (body_len != 0).then_some(0))
                    };
                    let end = if end_stream {
                        let end = self.message_end();
                        self.finish_egress_message();
                        end
                    } else {
                        MessageEnd::Nothing
                    };
                    let suffix = if opens_chunk {
                        end.after_chunk()
                    } else {
                        end.bytes()
                    };
                    let written =
                        Self::check_frame_room(output, prefix.len(), body_len, suffix.len())?;
                    self.push_chunk_line(output, prefix)?;
                    self.append_output(output, &mut body)?;
                    Self::push_static_output(output, suffix)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset,
                    })
                }
                EgressFrame::ChunkHeader { stream, length } => {
                    if stream != self.egress_stream || length == 0 {
                        return Err(invalid_state(b"invalid HTTP/1 chunk header"));
                    }
                    let bytes = if self.egress_chunked {
                        if self.in_chunk {
                            return Err(invalid_state(b"HTTP/1 chunk already open"));
                        }
                        self.in_chunk = true;
                        chunk_prefix(length)
                    } else {
                        Cow::Borrowed(&[][..])
                    };
                    let written = bytes.len();
                    self.push_chunk_line(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::ChunkTerminator { stream } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 chunk stream is out of order"));
                    }
                    let bytes: &'static [u8] = if self.egress_chunked && self.in_chunk {
                        self.in_chunk = false;
                        b"\r\n"
                    } else {
                        b""
                    };
                    let written = bytes.len();
                    Self::push_static_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::Trailers { stream, trailers } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 trailer stream is out of order"));
                    }
                    let mut bytes = Vec::new();
                    if self.egress_chunked {
                        if self.in_chunk {
                            return Err(invalid_state(b"HTTP/1 trailers inside a chunk"));
                        }
                        bytes.extend_from_slice(b"0\r\n");
                        self.last_chunk_written = true;
                        trailers.for_each(|name, value| {
                            bytes.extend_from_slice(name);
                            bytes.extend_from_slice(b": ");
                            bytes.extend_from_slice(value);
                            bytes.extend_from_slice(b"\r\n");
                        });
                        self.append_eom(&mut bytes);
                        self.finish_egress_message();
                    }
                    let written = bytes.len();
                    self.push_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::EndOfMessage { stream } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 EOM stream is out of order"));
                    }
                    let bytes = self.message_end().bytes();
                    self.finish_egress_message();
                    let written = bytes.len();
                    Self::push_static_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::Reset { .. } => {
                    self.keepalive = false;
                    Ok(GeneratedFrame::default())
                }
                EgressFrame::Goaway {
                    last_stream, code, ..
                } => {
                    if self.direction.is_upstream()
                        || self.disable_keepalive_pending
                        || last_stream != StreamId::MAX
                        || code != H2ErrorCode::NO_ERROR
                    {
                        self.keepalive = false;
                        self.disable_keepalive_pending = false;
                    } else {
                        self.disable_keepalive_pending = true;
                    }
                    Ok(GeneratedFrame::default())
                }
                EgressFrame::Padding { .. } => Ok(GeneratedFrame::default()),
                EgressFrame::PushPromise { .. }
                | EgressFrame::PingRequest { .. }
                | EgressFrame::PingReply { .. }
                | EgressFrame::Settings { .. }
                | EgressFrame::SettingsAck
                | EgressFrame::WindowUpdate { .. }
                | EgressFrame::Priority { .. }
                | EgressFrame::PushPriority { .. } => Err(HttpError::with_message(
                    HttpErrorCode::PROTOCOL_ERROR,
                    b"operation is unsupported by HTTP/1",
                )),
            }
        })();
        if result.is_err() {
            self.restore_egress(rollback);
        }
        result
    }
}

/// Returns true when the server selected any protocol advertised by the client.
pub fn server_accepted_upgrade(client: &[u8], server: &[u8]) -> bool {
    client
        .split(|byte| *byte == b',')
        .map(trim_http_whitespace)
        .filter(|token| !token.is_empty())
        .any(|client_token| {
            server
                .split(|byte| *byte == b',')
                .map(trim_http_whitespace)
                .filter(|token| !token.is_empty())
                .any(|server_token| client_token.eq_ignore_ascii_case(server_token))
        })
}

fn parse_decimal_u64(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    value.iter().try_fold(0_u64, |current, byte| {
        current.checked_mul(10)?.checked_add(u64::from(
            byte.checked_sub(b'0').filter(|digit| *digit <= 9)?,
        ))
    })
}

fn trim_http_whitespace(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
        .map_or(start, |index| index + 1);
    &value[start..end]
}

/// Whether `value` holds a CR or an LF.
#[inline]
fn holds_cr_or_lf(value: &[u8]) -> bool {
    !word_scan::all_words(value, |word| {
        // CR and LF are below 0x0e, and most words do not contain such a byte.
        word_scan::has_less(word, 0x0e) == 0
            || (word_scan::has_zero(word ^ word_scan::repeat(b'\r'))
                | word_scan::has_zero(word ^ word_scan::repeat(b'\n')))
                == 0
    })
}

fn websocket_key(_stream: StreamId) -> Vec<u8> {
    let mut raw = [0_u8; 16];
    rand::rng().fill_bytes(&mut raw);
    base64::engine::general_purpose::STANDARD
        .encode(raw)
        .into_bytes()
}

fn websocket_accept(key: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(key);
    hasher.update(WEBSOCKET_GUID);
    base64::engine::general_purpose::STANDARD
        .encode(hasher.finalize())
        .into_bytes()
}

fn invalid_state(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

/// Returns the method and URL the way `HttpMessage`'s `&mut` request accessors
/// would. A message with no kind yields an empty method and URL, and a response
/// yields `None`.
fn request_fields(message: &HttpMessage) -> Option<(&str, &[u8])> {
    (!message.is_response()).then(|| {
        (
            message.request_method().unwrap_or_default(),
            message.request_url().unwrap_or_default(),
        )
    })
}

/// Returns the status and reason the way `HttpMessage`'s `&mut` response
/// accessors would. A message with no kind yields status 0 and an empty reason,
/// and a request yields `None`.
fn response_fields(message: &HttpMessage) -> Option<(u16, &[u8])> {
    (!message.is_request()).then(|| {
        (
            message.response_status().unwrap_or_default(),
            message.response_status_message().unwrap_or_default(),
        )
    })
}

/// Chunks shorter than this open with a line from [`CHUNK_LINES`].
const STATIC_CHUNK_LINES: usize = 0x1000;

/// The width of a [`CHUNK_LINES`] entry: three hex digits, then CRLF.
const CHUNK_LINE_WIDTH: usize = 5;

/// The line that opens each chunk shorter than [`STATIC_CHUNK_LINES`], right
/// aligned in its entry, so that queueing it does not allocate.
static CHUNK_LINES: [u8; STATIC_CHUNK_LINES * CHUNK_LINE_WIDTH] = {
    let mut lines = [0; STATIC_CHUNK_LINES * CHUNK_LINE_WIDTH];
    let mut length = 0;
    while length < STATIC_CHUNK_LINES {
        write_chunk_line(&mut lines, (length + 1) * CHUNK_LINE_WIDTH, length);
        length += 1;
    }
    lines
};

/// Writes `length` as lowercase hex followed by CRLF so that the line ends at
/// `end` in `line`, and returns where it starts. Like [`push_decimal`], it does
/// not go through `core::fmt`.
const fn write_chunk_line(line: &mut [u8], end: usize, mut length: usize) -> usize {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut start = end - 2;
    line[start] = b'\r';
    line[start + 1] = b'\n';
    loop {
        start -= 1;
        line[start] = HEX_DIGITS[length % 16];
        length /= 16;
        if length == 0 {
            return start;
        }
    }
}

/// Returns the line that opens a chunk of `length` bytes. A chunk shorter than
/// [`STATIC_CHUNK_LINES`] borrows a line from [`CHUNK_LINES`]. Other chunks get
/// a buffer of exactly the line's length.
fn chunk_prefix(length: usize) -> Cow<'static, [u8]> {
    if length < STATIC_CHUNK_LINES {
        let digits = (usize::BITS - (length | 1).leading_zeros()).div_ceil(4) as usize;
        let end = (length + 1) * CHUNK_LINE_WIDTH;
        return Cow::Borrowed(&CHUNK_LINES[end - 2 - digits..end]);
    }
    let mut line = [0_u8; usize::BITS as usize / 4 + 2];
    let end = line.len();
    let start = write_chunk_line(&mut line, end, length);
    Cow::Owned(line[start..].to_vec())
}

fn push_decimal(output: &mut Vec<u8>, mut value: u16) {
    let mut digits = [0_u8; 5];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    output.extend_from_slice(&digits[start..]);
}

fn header_size(length: usize) -> HttpHeaderSize {
    let length = u32::try_from(length).unwrap_or(u32::MAX);
    HttpHeaderSize {
        compressed: length,
        uncompressed: length,
        compressed_block: 0,
    }
}

/// Word-at-a-time byte classes over little-endian `u64` loads. A class mask
/// marks each matching byte with its high bit and is nonzero exactly when some
/// byte of the word matches, but only its lowest mark is exact: a borrow out
/// of a matching byte can also mark the bytes above it.
mod word_scan {
    pub(super) const HIGH: u64 = u64::from_ne_bytes([0x80; 8]);
    const ONES: u64 = u64::from_ne_bytes([0x01; 8]);

    pub(super) const fn repeat(byte: u8) -> u64 {
        ONES * byte as u64
    }

    /// Marks the zero bytes of `word`.
    pub(super) const fn has_zero(word: u64) -> u64 {
        word.wrapping_sub(ONES) & !word & HIGH
    }

    /// Marks the bytes of `word` below `bound`, which is at most 0x80.
    pub(super) const fn has_less(word: u64, bound: u8) -> u64 {
        word.wrapping_sub(repeat(bound)) & !word & HIGH
    }

    /// Whether `test` holds for a set of words that together cover every byte
    /// of `bytes`. The set is the whole words plus the last eight bytes, or a
    /// single word built from the bytes when there are fewer than eight. A byte
    /// class that `test` checks in every byte of a word is therefore checked in
    /// every byte of `bytes`, without a per-byte loop.
    #[inline]
    pub(super) fn all_words(bytes: &[u8], test: impl Fn(u64) -> bool) -> bool {
        if let Some(last) = bytes.last_chunk::<8>() {
            let (words, _) = bytes.as_chunks::<8>();
            return words.iter().all(|word| test(u64::from_le_bytes(*word)))
                && test(u64::from_le_bytes(*last));
        }
        if let (Some(first), Some(last)) = (bytes.first_chunk::<4>(), bytes.last_chunk::<4>()) {
            let word = u64::from(u32::from_le_bytes(*first))
                | (u64::from(u32::from_le_bytes(*last)) << 32);
            return test(word);
        }
        match bytes {
            [] => true,
            [first, ..] => {
                let middle = bytes[bytes.len() / 2];
                let last = bytes[bytes.len() - 1];
                test(u64::from_le_bytes([
                    *first, middle, last, *first, *first, *first, *first, *first,
                ]))
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/codec.rs"]
mod codec_http1;

#[cfg(test)]
#[path = "../tests/generate_copies.rs"]
mod generate_copies;
