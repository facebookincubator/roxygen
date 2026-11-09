use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::borrow::Borrow;
use std::cell::Cell;
use std::fmt::Write as _;
use std::hint::black_box;
use std::ops::Range;
use std::ptr;
use std::sync::LazyLock;

use base64::Engine as _;
use bytes::Bytes;
use roxygen_codec::CodecCallbackAction;
use roxygen_codec::CodecCallbackActions;
use roxygen_codec::CodecEvent;
use roxygen_codec::CodecIngressResult;
use roxygen_codec::EgressFrame;
use roxygen_codec::GeneratedFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::OwnedEgressFrame;
use roxygen_codec::ProxygenError;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;

use super::FROZEN_HTTP_METHODS;
use super::Http1xCodec;
use super::MAX_HEADER_BYTES;
use super::ParseState;
use super::chunk_delimiter_turns;
use super::server_accepted_upgrade;

static_assertions::assert_not_impl_any!(Http1xCodec: Clone, Send, Sync);

const QUEUE_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Default)]
struct Recorder {
    callback_count: usize,
    events: Vec<&'static str>,
    messages: Vec<HttpMessage>,
    bodies: Vec<u8>,
    chunk_lengths: Vec<usize>,
    trailers: Vec<HttpHeaders>,
    completions: Vec<bool>,
    errors: Vec<(ProxygenError, Option<u32>, Option<HttpMessage>)>,
    error_messages: Vec<Vec<u8>>,
}

impl HttpCodecCallback for Recorder {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.callback_count += 1;
        match event {
            CodecEvent::MessageBegin { .. } => self.events.push("begin"),
            CodecEvent::HeadersComplete { message, .. } => {
                self.events.push("headers");
                self.messages.push(*message);
            }
            CodecEvent::Body { body, .. } => {
                self.events.push("body");
                for segment in body.segments() {
                    self.bodies.extend_from_slice(segment);
                }
            }
            CodecEvent::ChunkHeader { length, .. } => {
                self.events.push("chunk_header");
                self.chunk_lengths.push(length);
            }
            CodecEvent::ChunkComplete { .. } => self.events.push("chunk_complete"),
            CodecEvent::TrailersComplete { trailers, .. } => {
                self.events.push("trailers");
                self.trailers.push(*trailers);
            }
            CodecEvent::MessageComplete { upgraded, .. } => {
                self.events.push("complete");
                self.completions.push(upgraded);
            }
            CodecEvent::Error { error, .. } => {
                self.events.push("error");
                self.error_messages.push(error.message().to_vec());
                self.errors.push((
                    error.proxygen_error(),
                    error.http_status_code(),
                    error.partial_message().cloned(),
                ));
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct SharedBodyCapture {
    segments: Vec<Bytes>,
}

impl HttpCodecCallback for SharedBodyCapture {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if let CodecEvent::Body { body, .. } = event {
            self.segments.extend(body.into_bytes_segments());
        }
    }
}

#[derive(Default)]
struct PauseOnHeaders {
    recorder: Recorder,
}

impl HttpCodecCallback for PauseOnHeaders {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.recorder.on_event(event);
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let pause = matches!(event, CodecEvent::HeadersComplete { .. });
        self.recorder.on_event(event);
        if pause {
            vec![CodecCallbackAction::SetParserPaused(true)].into()
        } else {
            CodecCallbackActions::new()
        }
    }
}

fn queue_bytes(queue: &BufQueue) -> Vec<u8> {
    queue
        .coalesce(queue.chain_length())
        .expect("bounded test queue must coalesce")
        .to_vec()
}

fn ingress_queue(bytes: &[u8]) -> BufQueue {
    BufQueue::copy_from_slice(bytes, bytes.len()).expect("test input fits its exact queue limit")
}

fn parse(codec: &mut Http1xCodec, callback: &mut Recorder, input: &[u8]) -> usize {
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    codec
        .on_ingress(&ingress_queue(input), callback, &mut egress)
        .consumed
}

fn request(method: HttpMethod, url: &[u8], version: (u8, u8)) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(method).unwrap();
    message.set_url(url, false).unwrap();
    message.set_http_version(version.0, version.1);
    message
}

fn response(status: u16, version: (u8, u8)) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    message
        .set_status_message(HttpMessage::default_reason(status))
        .unwrap();
    message.set_http_version(version.0, version.1);
    message
}

fn generate(codec: &mut Http1xCodec, frame: EgressFrame<'_>) -> Vec<u8> {
    let mut callback = Recorder::default();
    let mut output = BufQueue::new(QUEUE_LIMIT);
    codec.generate(&mut output, frame, &mut callback).unwrap();
    queue_bytes(&output)
}

fn assert_simple_request(input: &[u8]) -> HttpMessage {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, input), input.len());
    assert_eq!(callback.events, ["begin", "headers", "complete"]);
    callback.messages.pop().unwrap()
}

fn assert_header_error(input: &[u8], strict: bool) -> Recorder {
    assert_error(input, strict, ProxygenError::PARSE_HEADER)
}

fn assert_error(input: &[u8], strict: bool, expected: ProxygenError) -> Recorder {
    let mut codec = Http1xCodec::with_options(TransportDirection::Downstream, false, strict);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, input);
    assert_eq!(callback.errors.len(), 1);
    assert_eq!(callback.errors[0].0, expected);
    assert_eq!(callback.errors[0].1, Some(400));
    callback
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[test]
fn test_simple_headers() {
    let request = b"GET /yeah HTTP/1.1\nHost: www.facebook.com\n\n";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, request), request.len());
    assert_eq!(parse(&mut codec, &mut callback, request), request.len());
    assert_eq!(callback.messages.len(), 2);
    assert_eq!(
        callback.events,
        [
            "begin", "headers", "complete", "begin", "headers", "complete"
        ]
    );
    for message in &callback.messages {
        assert_eq!(
            message.ingress_header_size().uncompressed as usize,
            request.len()
        );
        assert_eq!(
            message.ingress_header_size().compressed,
            message.ingress_header_size().uncompressed
        );
    }
}

#[test]
fn fragmented_ingress_body_shares_backing() {
    let head = b"POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\n";
    let mut first_bytes = head.to_vec();
    first_bytes.extend_from_slice(b"ab");
    let first = Bytes::from(first_bytes);
    let second = Bytes::from_static(b"cd");
    let first_body_pointer = first.as_ptr().wrapping_add(head.len());
    let second_body_pointer = second.as_ptr();
    let mut input = BufQueue::new(first.len() + second.len());
    input.push_bytes(first.clone()).expect("first segment fits");
    input
        .push_bytes(second.clone())
        .expect("second segment fits");
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = SharedBodyCapture::default();
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);

    let result = codec.on_ingress(&input, &mut callback, &mut callback_egress);

    assert_eq!(result.consumed, input.chain_length());
    assert_eq!(callback.segments.len(), 2);
    assert_eq!(callback.segments[0].as_ref(), b"ab");
    assert_eq!(callback.segments[0].as_ptr(), first_body_pointer);
    assert_eq!(callback.segments[1].as_ref(), b"cd");
    assert_eq!(callback.segments[1].as_ptr(), second_body_pointer);
    assert_eq!(input.segment_count(), 2);
    assert_eq!(
        input.front().expect("first input segment").as_ptr(),
        first.as_ptr()
    );
}

#[test]
fn frozen_method_table_accepts_fragmented_extension_methods() {
    for method in FROZEN_HTTP_METHODS {
        let target = if method == b"CONNECT".as_slice() {
            b"example.com:443".as_slice()
        } else {
            b"/resource".as_slice()
        };
        let mut wire = method.to_vec();
        wire.push(b' ');
        wire.extend_from_slice(target);
        wire.extend_from_slice(b" HTTP/1.1\r\n\r\n");
        let mut codec = Http1xCodec::new(TransportDirection::Downstream);
        let mut callback = Recorder::default();
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        for byte in wire.chunks(1) {
            assert_eq!(
                codec
                    .on_ingress(&ingress_queue(byte), &mut callback, &mut egress)
                    .consumed,
                1
            );
        }
        assert!(callback.errors.is_empty());
        assert_eq!(callback.messages.len(), 1);
        assert_eq!(
            callback.messages[0].method_string().unwrap().as_bytes(),
            method
        );
    }

    let callback = assert_header_error(b"PUBLISH / HTTP/1.1\r\n\r\n", false);
    assert_eq!(callback.events, ["begin", "error"]);
}

#[test]
fn test_simple_headers_queue() {
    let wire = b"GET /one HTTP/1.1\nHost: one\n\nGET /two HTTP/1.1\nHost: two\n\n";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, wire), wire.len());
    assert_eq!(callback.messages.len(), 2);
    assert_eq!(
        callback
            .events
            .iter()
            .filter(|event| **event == "complete")
            .count(),
        2
    );
}

#[test]
fn test_simple_headers_queue_with_pause() {
    struct PauseOnComplete(bool);
    impl HttpCodecCallback for PauseOnComplete {
        fn on_event(&mut self, _event: CodecEvent<'_>) {}
        fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
            if matches!(event, CodecEvent::MessageComplete { .. }) && !self.0 {
                self.0 = true;
                vec![CodecCallbackAction::SetParserPaused(true)].into()
            } else {
                CodecCallbackActions::new()
            }
        }
    }
    let first = b"GET /one HTTP/1.1\r\nHost: one\r\n\r\n";
    let second = b"GET /two HTTP/1.1\r\nHost: two\r\n\r\n";
    let mut wire = first.to_vec();
    wire.extend_from_slice(second);
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = PauseOnComplete(false);
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let result = codec.on_ingress(&ingress_queue(&wire), &mut callback, &mut egress);
    assert_eq!(result.consumed, first.len());
    codec.set_parser_paused(false);
    let result = codec.on_ingress(
        &ingress_queue(&wire[result.consumed..]),
        &mut callback,
        &mut egress,
    );
    assert_eq!(result.consumed, second.len());
}

#[test]
fn paused_body_defers_eof_until_resume() {
    struct PauseOnBody {
        events: Vec<&'static str>,
        paused: bool,
    }
    impl HttpCodecCallback for PauseOnBody {
        fn on_event(&mut self, _event: CodecEvent<'_>) {}

        fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
            match event {
                CodecEvent::MessageBegin { .. } => self.events.push("begin"),
                CodecEvent::HeadersComplete { .. } => self.events.push("headers"),
                CodecEvent::Body { .. } => {
                    self.events.push("body");
                    if !self.paused {
                        self.paused = true;
                        return vec![CodecCallbackAction::SetParserPaused(true)].into();
                    }
                }
                CodecEvent::MessageComplete { .. } => self.events.push("complete"),
                CodecEvent::Error { .. } => self.events.push("error"),
                _ => {}
            }
            CodecCallbackActions::new()
        }
    }

    let input = b"POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\ndata";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = PauseOnBody {
        events: Vec::new(),
        paused: false,
    };
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let result = codec.on_ingress(&ingress_queue(input), &mut callback, &mut egress);
    assert_eq!(result.consumed, input.len());
    assert_eq!(callback.events, ["begin", "headers", "body"]);

    let _ = codec.on_ingress_eof(&mut callback, &mut egress);
    assert_eq!(callback.events, ["begin", "headers", "body"]);

    codec.set_parser_paused(false);
    assert_eq!(
        codec
            .on_ingress(&BufQueue::new(0), &mut callback, &mut egress)
            .consumed,
        0
    );
    assert_eq!(callback.events, ["begin", "headers", "body", "complete"]);
}

#[test]
fn test_09_req() {
    let mut message = assert_simple_request(b"GET /yeah\r\n");
    assert_eq!(message.http_version(), (0, 9));
    assert_eq!(message.url().unwrap(), b"/yeah");
}

#[test]
fn test_09_req_vers() {
    let message = assert_simple_request(b"GET /yeah HTTP/0.9\r\n");
    assert_eq!(message.http_version(), (0, 9));
}

#[test]
fn test_09_resp() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let request = request(HttpMethod::Get, b"/", (0, 9));
    assert_eq!(
        generate(
            &mut codec,
            EgressFrame::Headers {
                stream,
                message: &request,
                end_stream: true,
                extra_headers: None,
            }
        ),
        b"GET /\r\n"
    );
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, b"old style body"), 14);
    assert_eq!(callback.bodies, b"old style body");
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let _ = codec.on_ingress_eof(&mut callback, &mut egress);
    assert!(callback.events.contains(&"complete"));
}

#[test]
fn test_response_split() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Get, b"/foo", (1, 1));
    message
        .headers_mut()
        .set(b"InjectHeader", b"Value\r\nx: new");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"InjectHeader").is_none());
    assert!(find_bytes(&wire, b"x: new").is_none());
}

#[test]
fn test_o9_no_version() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let message = request(HttpMethod::Get, b"/yeah", (0, 9));
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert_eq!(wire, b"GET /yeah\r\n");
}

#[test]
fn test_keepalive_09_10() {
    let mut http09 = Http1xCodec::with_options(TransportDirection::Downstream, true, false);
    let mut callback = Recorder::default();
    parse(&mut http09, &mut callback, b"GET /\r\n");
    let mut message = response(200, (0, 9));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    assert!(
        generate(
            &mut http09,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
        )
        .is_empty()
    );
    assert!(!http09.is_reusable());

    let mut http10_close = Http1xCodec::with_options(TransportDirection::Downstream, true, false);
    let mut callback = Recorder::default();
    parse(&mut http10_close, &mut callback, b"GET / HTTP/1.0\r\n\r\n");
    let mut message = response(200, (1, 0));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    let wire = generate(
        &mut http10_close,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Connection: close").is_some());
    assert!(!http10_close.is_reusable());

    let mut http10_keepalive =
        Http1xCodec::with_options(TransportDirection::Downstream, true, false);
    let mut callback = Recorder::default();
    parse(
        &mut http10_keepalive,
        &mut callback,
        b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
    );
    let wire = generate(
        &mut http10_keepalive,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Connection: keep-alive").is_some());
    assert!(http10_keepalive.is_reusable());

    let mut http10_dechunk = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut http10_dechunk,
        &mut callback,
        b"GET / HTTP/1.0\r\n\r\n",
    );
    let mut chunked = response(200, (1, 0));
    chunked.set_chunked(true);
    chunked
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    let wire = generate(
        &mut http10_dechunk,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &chunked,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Transfer-Encoding").is_none());
    assert!(find_bytes(&wire, b"Connection: close").is_some());
}

#[test]
fn http10_keepalive_request_cannot_reverse_disabled_latch() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, b"GET /first HTTP/1.1\r\n\r\n");
    let mut response_11 = response(200, (1, 1));
    response_11
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    let first_wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &response_11,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&first_wire, b"Connection: keep-alive").is_some());
    assert!(codec.is_reusable());

    parse(
        &mut codec,
        &mut callback,
        b"GET /second HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
    );
    let mut response_10 = response(200, (1, 0));
    response_10
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    let second_wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(2),
            message: &response_10,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&second_wire, b"Connection: close").is_some());
    assert!(!codec.is_reusable());
}

#[test]
fn parsed_request_exposes_connection_close_to_the_session() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(callback.messages.len(), 1);
    assert!(!callback.messages[0].wants_keepalive());
    assert!(!codec.is_reusable());
}

#[test]
fn test_bad_headers() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, b"GET / HTTP/1.1\r\nX: a\x10b");
    assert_eq!(callback.errors.len(), 1);
    assert_eq!(callback.errors[0].0, ProxygenError::PARSE_HEADER);
    assert_eq!(callback.errors[0].1, Some(400));
    let mut partial = callback.errors[0].2.clone().unwrap();
    assert_eq!(partial.method_string().unwrap(), "GET");
    assert_eq!(partial.url().unwrap(), b"/");
    let event_count = callback.events.len();
    assert_eq!(
        parse(
            &mut codec,
            &mut callback,
            b"\r\n\r\nGET /later HTTP/1.1\r\n\r\n",
        ),
        0
    );
    assert_eq!(callback.events.len(), event_count);
}

#[test]
fn test_high_ascii_ua() {
    assert_error(
        "GET / HTTP/1.1\r\nUser-Agent: ꪶ𝛸\r\n\r\n".as_bytes(),
        true,
        ProxygenError::HEADER_CONTENT_VALIDATION,
    );
}

#[test]
fn test_bad_url() {
    let callback = assert_header_error("GET /Ñoño HTTP/1.1\r\n\r\n".as_bytes(), true);
    let partial = callback.errors[0].2.as_ref().unwrap();
    assert!(!partial.is_request());
}

#[test]
fn test_underscore_allowed_in_host() {
    let mut message = assert_simple_request(b"CONNECT face_book.facebook.com:443 HTTP/1.1\r\n\r\n");
    assert_eq!(message.method().unwrap(), Some(HttpMethod::Connect));
}

#[test]
fn test_head_request_chunked_response() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, b"HEAD / HTTP/1.1\r\n\r\n");
    let mut message = response(200, (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"0\r\n").is_none());
}

#[test]
fn test_get_request_chunked_response() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, b"GET / HTTP/1.1\r\n\r\n");
    let mut message = response(200, (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    let body = BufQueue::from_bytes(Bytes::from_static(b"Hello"), 5).unwrap();
    assert_eq!(
        generate(
            &mut codec,
            EgressFrame::Body {
                stream: StreamId::new(1),
                body,
                padding: None,
                end_stream: true,
            },
        ),
        b"5\r\nHello\r\n0\r\n\r\n"
    );
}

#[test]
fn test_chunked_headers() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    let first = b"GET /aha HTTP/1.1\n";
    let second = b"Host: m.facebook.com\nAccept-Encoding: meflate\n\n";
    assert_eq!(parse(&mut codec, &mut callback, first), first.len());
    assert!(callback.messages.is_empty());
    assert_eq!(parse(&mut codec, &mut callback, second), second.len());
    assert_eq!(
        callback.messages[0].ingress_header_size().uncompressed as usize,
        first.len() + second.len()
    );
}

#[test]
fn test_chunked_upstream() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Post, b"/", (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    let body = BufQueue::from_bytes(Bytes::from_static(b"World"), 5).unwrap();
    assert_eq!(
        generate(
            &mut codec,
            EgressFrame::Body {
                stream,
                body,
                padding: None,
                end_stream: true,
            },
        ),
        b"5\r\nWorld\r\n0\r\n\r\n"
    );
}

struct BadPostCallback {
    events: Vec<&'static str>,
}

impl HttpCodecCallback for BadPostCallback {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        match event {
            CodecEvent::MessageBegin { stream, .. } if stream == StreamId::new(1) => {
                self.events.push("begin-1");
                CodecCallbackActions::new()
            }
            CodecEvent::HeadersComplete { stream, .. } if stream == StreamId::new(1) => {
                self.events.push("headers-1/generate-100");
                vec![CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                    stream,
                    message: Box::new(response(100, (1, 1))),
                    end_stream: false,
                    extra_headers: None,
                })]
                .into()
            }
            CodecEvent::Body { stream, .. } if stream == StreamId::new(1) => {
                self.events.push("body-1");
                CodecCallbackActions::new()
            }
            CodecEvent::MessageComplete { stream, .. } if stream == StreamId::new(1) => {
                self.events.push("complete-1");
                CodecCallbackActions::new()
            }
            CodecEvent::MessageBegin { stream, .. } if stream == StreamId::new(2) => {
                self.events.push("begin-2/generate-200");
                vec![
                    CodecCallbackAction::SetParserPaused(true),
                    CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                        stream: StreamId::new(1),
                        message: Box::new(response(200, (1, 1))),
                        end_stream: true,
                        extra_headers: None,
                    }),
                    CodecCallbackAction::SetParserPaused(false),
                ]
                .into()
            }
            CodecEvent::Error { stream, .. } if stream == StreamId::new(2) => {
                self.events.push("error-2/generate-400");
                vec![CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                    stream,
                    message: Box::new(response(400, (1, 1))),
                    end_stream: true,
                    extra_headers: None,
                })]
                .into()
            }
            _ => CodecCallbackActions::new(),
        }
    }
}

#[test]
fn test_bad_post_100() {
    let input = b"POST / HTTP/1.1\r\nExpect: 100-Continue\r\nContent-Length: 5\r\n\r\nabcdefghij";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = BadPostCallback { events: Vec::new() };
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let result = codec.on_ingress(&ingress_queue(input), &mut callback, &mut egress);
    assert_eq!(result.consumed, input.len() - 4);
    assert_eq!(result.egress_results.len(), 3);
    assert!(result.egress_results.iter().all(Result::is_ok));
    assert_eq!(
        callback.events,
        [
            "begin-1",
            "headers-1/generate-100",
            "body-1",
            "complete-1",
            "begin-2/generate-200",
            "error-2/generate-400",
        ]
    );
    assert!(codec.is_parser_paused());
    let wire = queue_bytes(&egress);
    let continue_at = find_bytes(&wire, b" 100 ").unwrap();
    let ok_at = find_bytes(&wire, b" 200 ").unwrap();
    let bad_at = find_bytes(&wire, b" 400 ").unwrap();
    assert!(continue_at < ok_at && ok_at < bad_at);
}

#[test]
fn test_multiple_identical_content_length_headers() {
    let input = b"POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, input), input.len());
    let message = callback.messages.pop().unwrap();
    assert_eq!(
        message
            .headers()
            .number_of_values(HttpHeaderCode::ContentLength),
        2
    );
}

#[test]
fn test_multiple_distinct_content_length_headers() {
    assert_header_error(
        b"POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
        false,
    );
}

#[test]
fn test_correct_transfer_encoding_header() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    let input = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    parse(&mut codec, &mut callback, input);
    assert!(callback.messages[0].is_chunked());
    assert!(callback.events.contains(&"complete"));
}

#[test]
fn test_folded_transfer_encoding_header() {
    assert_header_error(
        b"POST / HTTP/1.1\r\nTransfer-Encoding:\r\n chunked\r\nContent-Length: 8\r\n\r\n",
        false,
    );
}

#[test]
fn test_bad_transfer_encoding_header() {
    let mut callback = assert_header_error(
        b"POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\nTransfer-Encoding: chunked, zorg\r\n\r\n",
        false,
    );
    let mut partial = callback.errors[0].2.take().unwrap();
    assert_eq!(partial.method_string().unwrap(), "POST");
    assert_eq!(partial.url().unwrap(), b"/www.facebook.com");
}

#[test]
fn test_partial_msg_backfilled_for_pipelined_request() {
    let input =
        b"GET /first HTTP/1.1\r\n\r\nPOST /second HTTP/1.1\r\nTransfer-Encoding: zorg\r\n\r\n";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, input);
    let mut partial = callback.errors[0].2.take().unwrap();
    assert_eq!(partial.url().unwrap(), b"/second");
}

#[test]
fn test_partial_msg_backfilled_on_content_validation_failure() {
    let mut callback = assert_error(
        "GET /yeah?id=7 HTTP/1.1\r\nUser-Agent: ꪶ𝛸\r\n\r\n".as_bytes(),
        true,
        ProxygenError::HEADER_CONTENT_VALIDATION,
    );
    let mut partial = callback.errors[0].2.take().unwrap();
    assert_eq!(partial.path().unwrap(), b"/yeah");
    assert_eq!(partial.query_string().unwrap(), b"id=7");
}

#[test]
fn test_partial_msg_not_backfilled_when_request_line_unparsed() {
    let mut callback = assert_header_error("GET /Ñoño HTTP/1.1\r\n\r\n".as_bytes(), true);
    let mut partial = callback.errors[0].2.take().unwrap();
    assert_eq!(partial.method_string().unwrap(), "");
    assert_eq!(partial.url().unwrap(), b"");
}

#[test]
fn test_partial_msg_not_backfilled_upstream() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 9\r\n\r\n",
    );
    let partial = callback.errors[0].2.as_ref().unwrap();
    assert!(partial.is_response());
}

#[test]
fn test_malformed_chunk_delimiter() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\rX",
    );
    assert_eq!(callback.errors[0].0, ProxygenError::PARSE_BODY);
    let event_count = callback.events.len();
    assert_eq!(
        parse(&mut codec, &mut callback, b"GET /later HTTP/1.1\r\n\r\n"),
        0
    );
    assert_eq!(callback.errors.len(), 1);
    assert_eq!(callback.events.len(), event_count);
}

/// A codec that has parsed a chunked message's head and waits for its first
/// chunk-size line, with the callbacks recorded so far.
fn chunk_size_codec(upstream: bool, strict: bool) -> (Http1xCodec, Recorder) {
    let direction = if upstream {
        TransportDirection::Upstream
    } else {
        TransportDirection::Downstream
    };
    let mut codec = Http1xCodec::with_options(direction, false, strict);
    let head = if upstream {
        send_trace_request(&mut codec);
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice()
    } else {
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice()
    };
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, head), head.len());
    assert_eq!(callback.events, ["begin", "headers"]);
    (codec, callback)
}

#[test]
fn chunk_delimiter_boundaries_are_independent_of_delivery() {
    // The terminator bytes after a 1-byte chunk, the bytes the codec consumes,
    // the error, and whether the codec holds the terminator's CR afterwards.
    for (delimiter, expected_consumed, expected_error, expected_after_cr) in [
        (b"\n".as_slice(), 1, None, false),
        (b"\r\n".as_slice(), 2, None, false),
        (b"".as_slice(), 0, Some(ProxygenError::EOF), false),
        (b"\r".as_slice(), 1, Some(ProxygenError::EOF), true),
        (
            b"X0\r\n\r\n".as_slice(),
            1,
            Some(ProxygenError::PARSE_BODY),
            false,
        ),
        (
            b"\0\n".as_slice(),
            1,
            Some(ProxygenError::PARSE_BODY),
            false,
        ),
        (
            b"\rX0\r\n\r\n".as_slice(),
            2,
            Some(ProxygenError::PARSE_BODY),
            true,
        ),
        (
            b"\r\r\n".as_slice(),
            2,
            Some(ProxygenError::PARSE_BODY),
            true,
        ),
    ] {
        for upstream in [false, true] {
            for strict in [false, true] {
                for split in 0..=delimiter.len() {
                    for queued in [false, true] {
                        let context = format!(
                            "delimiter={} upstream={upstream} strict={strict} split={split} queued={queued}",
                            delimiter.escape_ascii()
                        );
                        let (mut codec, mut callback) = chunk_size_codec(upstream, strict);
                        assert_eq!(parse(&mut codec, &mut callback, b"1\r\na"), 4);
                        let mut egress = BufQueue::new(QUEUE_LIMIT);
                        let consumed = if queued {
                            let mut input = BufQueue::new(delimiter.len());
                            for bytes in [&delimiter[..split], &delimiter[split..]] {
                                input.push_bytes(Bytes::copy_from_slice(bytes)).unwrap();
                            }
                            codec
                                .on_ingress(&input, &mut callback, &mut egress)
                                .consumed
                        } else {
                            parse(&mut codec, &mut callback, &delimiter[..split])
                                + parse(&mut codec, &mut callback, &delimiter[split..])
                        };
                        assert_eq!(consumed, expected_consumed, "{context}");
                        if let Some(error) = expected_error {
                            assert!(
                                matches!(codec.state, ParseState::ChunkDelimiter(after_cr) if after_cr == expected_after_cr),
                                "{context}"
                            );
                            if error == ProxygenError::EOF {
                                assert!(callback.errors.is_empty(), "{context}");
                                assert_eq!(callback.events.last(), Some(&"body"), "{context}");
                                let _ = codec.on_ingress_eof(&mut callback, &mut egress);
                            }
                            assert_eq!(
                                callback.events,
                                ["begin", "headers", "chunk_header", "body", "error"],
                                "{context}"
                            );
                            assert_eq!(callback.errors.len(), 1, "{context}");
                            assert_eq!(callback.errors[0].0, error, "{context}");
                            assert_eq!(
                                callback.errors[0].1,
                                (!upstream).then_some(400),
                                "{context}"
                            );
                            let message = if error == ProxygenError::EOF {
                                b"unexpected EOF".as_slice()
                            } else {
                                b"invalid HTTP chunk terminator".as_slice()
                            };
                            assert_eq!(callback.error_messages[0], message, "{context}");
                            let callbacks = callback.callback_count;
                            assert_eq!(parse(&mut codec, &mut callback, b"0\r\n\r\n"), 0);
                            let _ = codec.on_ingress_eof(&mut callback, &mut egress);
                            assert_eq!(callback.callback_count, callbacks, "{context}");
                        } else {
                            assert!(
                                matches!(codec.state, ParseState::ChunkSize(line) if line.length == 0),
                                "{context}"
                            );
                            assert_eq!(
                                callback.events,
                                ["begin", "headers", "chunk_header", "body", "chunk_complete"],
                                "{context}"
                            );
                            assert_eq!(parse(&mut codec, &mut callback, b"0\r\n\r\n"), 5);
                            assert_eq!(callback.completions, [false], "{context}");
                            assert!(callback.errors.is_empty(), "{context}");
                        }
                        assert_eq!(callback.bodies, b"a", "{context}");
                        assert_eq!(callback.chunk_lengths, [1], "{context}");
                    }
                }
            }
        }
    }
}

#[test]
fn chunk_callbacks_pause_before_the_next_phase() {
    struct PauseOnChunk(Recorder, SharedBodyCapture);

    impl HttpCodecCallback for PauseOnChunk {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            if let CodecEvent::Body { body, .. } = &event {
                self.1.segments.extend(body.segments().cloned());
            }
            self.0.on_event(event);
        }

        fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
            let pause = matches!(
                event,
                CodecEvent::ChunkHeader { .. } | CodecEvent::ChunkComplete { .. }
            );
            self.on_event(event);
            if pause {
                vec![CodecCallbackAction::SetParserPaused(true)].into()
            } else {
                CodecCallbackActions::new()
            }
        }
    }

    // The chunked body, CRLF and bare-LF terminated, and the consumed offset
    // of each paused callback.
    for (bytes, offsets) in [
        (b"2\r\nab\r\n1\r\nc\r\n0\r\n\r\n".as_slice(), [3, 7, 10, 13]),
        (b"2\r\nab\n1\r\nc\n0\r\n\r\n".as_slice(), [3, 6, 9, 11]),
    ] {
        for split in 0..=bytes.len() {
            let wire = Bytes::copy_from_slice(bytes);
            let expected_body_pointers = [
                wire.as_ptr().wrapping_add(3),
                wire.as_ptr().wrapping_add(4),
                wire.as_ptr().wrapping_add(offsets[2]),
            ];
            let (mut codec, recorder) = chunk_size_codec(false, false);
            let mut callback = PauseOnChunk(recorder, SharedBodyCapture::default());
            let mut input = BufQueue::new(wire.len());
            input.push_bytes(wire.slice(..split)).unwrap();
            input.push_bytes(wire.slice(split..)).unwrap();
            drop(wire);
            let mut egress = BufQueue::new(QUEUE_LIMIT);
            let mut consumed = 0;
            for (offset, event) in offsets.into_iter().zip([
                "chunk_header",
                "chunk_complete",
                "chunk_header",
                "chunk_complete",
            ]) {
                codec.set_parser_paused(false);
                let result = codec.on_ingress(&input, &mut callback, &mut egress);
                consumed += result.consumed;
                input.trim_start(result.consumed).unwrap();
                assert_eq!(consumed, offset, "split={split}");
                assert!(codec.is_parser_paused());
                assert_eq!(callback.0.events.last(), Some(&event));
                if event == "chunk_complete" {
                    assert!(matches!(codec.state, ParseState::ChunkSize(line) if line.length == 0));
                } else {
                    assert!(matches!(codec.state, ParseState::ChunkBody(_)));
                }
                let callbacks = callback.0.callback_count;
                assert_eq!(
                    codec
                        .on_ingress(&input, &mut callback, &mut egress)
                        .consumed,
                    0
                );
                assert_eq!(callback.0.callback_count, callbacks);
            }
            codec.set_parser_paused(false);
            consumed += codec
                .on_ingress(&input, &mut callback, &mut egress)
                .consumed;
            assert_eq!(consumed, bytes.len());
            input.trim_start(input.chain_length()).unwrap();
            drop(input);
            drop(codec);
            assert_eq!(callback.0.chunk_lengths, [2, 1]);
            assert_eq!(callback.0.bodies, b"abc");
            assert_eq!(callback.0.completions, [false]);
            assert!(callback.0.errors.is_empty());
            let mut captured = Vec::new();
            let mut pointers = Vec::new();
            for body in &callback.1.segments {
                captured.extend_from_slice(body);
                pointers.extend((0..body.len()).map(|offset| body.as_ptr().wrapping_add(offset)));
            }
            assert_eq!(captured, b"abc");
            assert_eq!(pointers, expected_body_pointers);
        }
    }
}

#[test]
fn test_1xx_connection_header() {
    let mut codec = Http1xCodec::make_response_codec(true);
    let stream = codec.create_stream();
    let mut message = response(100, (1, 1));
    message
        .headers_mut()
        .add(HttpHeaderCode::Connection, b"Upgrade");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Connection: Upgrade\r\n").is_some());
    assert!(find_bytes(&wire, b"keep-alive").is_none());
}

#[test]
fn test_chained_body() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nabcde",
    );
    parse(&mut codec, &mut callback, b"fghij");
    assert_eq!(callback.bodies, b"abcdefghij");
}

#[test]
fn test_ignore_upstream_upgrade() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let mut callback = Recorder::default();
    let input = b"HTTP/1.1 200 OK\r\nConnection: close\r\nUpgrade: h2,h2c\r\n\r\n<!DOCTYPE html>";
    parse(&mut codec, &mut callback, input);
    assert_eq!(callback.bodies, b"<!DOCTYPE html>");
    assert!(callback.errors.is_empty());
}

#[test]
fn upgraded_ingress_forwards_raw_bytes_while_paused_and_completes_at_eof() {
    let head = b"CONNECT example.com:443 HTTP/1.1\r\n\r\n";
    let payload = b"opaque tunnel bytes";
    let mut input = head.to_vec();
    input.extend_from_slice(payload);
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);

    let first = codec.on_ingress(&ingress_queue(&input), &mut callback, &mut callback_egress);
    assert_eq!(first.consumed, head.len());
    assert!(codec.is_parser_paused());
    assert_eq!(callback.completions, [true]);
    assert!(callback.bodies.is_empty());

    let second = codec.on_ingress(
        &ingress_queue(&input[first.consumed..]),
        &mut callback,
        &mut callback_egress,
    );
    assert_eq!(second.consumed, payload.len());
    assert_eq!(callback.bodies, payload);

    let _ = codec.on_ingress_eof(&mut callback, &mut callback_egress);
    assert_eq!(callback.completions, [true, false]);
    assert_eq!(
        callback.events,
        ["begin", "headers", "complete", "body", "complete"]
    );
}

fn websocket_round_trip() -> (Vec<u8>, Vec<u8>) {
    let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
    let stream = upstream.create_stream();
    let mut request = request(HttpMethod::Get, b"/websocket", (1, 1));
    request.set_egress_websocket_upgrade();
    let request_wire = generate(
        &mut upstream,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    );
    let mut downstream = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut downstream, &mut callback, &request_wire);
    assert!(callback.messages[0].is_ingress_websocket_upgrade());
    let mut response = response(101, (1, 1));
    response.set_egress_websocket_upgrade();
    let response_wire = generate(
        &mut downstream,
        EgressFrame::Headers {
            stream,
            message: &response,
            end_stream: false,
            extra_headers: None,
        },
    );
    let mut callback = Recorder::default();
    parse(&mut upstream, &mut callback, &response_wire);
    assert!(callback.errors.is_empty());
    (request_wire, response_wire)
}

#[test]
fn websocket_upgrade() {
    let (request, response) = websocket_round_trip();
    assert!(find_bytes(&request, b"Sec-WebSocket-Key: ").is_some());
    assert!(find_bytes(&response, b"Sec-WebSocket-Accept: ").is_some());
}

#[test]
fn paused_downstream_websocket_commits_only_after_successful_response_generation() {
    let head = b"GET / HTTP/1.1\r\nHost: example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    let raw = b"websocket bytes";
    let mut input = head.to_vec();
    input.extend_from_slice(raw);
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = PauseOnHeaders::default();
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
    let first = codec.on_ingress(&ingress_queue(&input), &mut callback, &mut callback_egress);
    assert_eq!(first.consumed, head.len());
    assert!(codec.is_parser_paused());

    let mut accepted = response(101, (1, 1));
    accepted.set_egress_websocket_upgrade();
    let mut tiny = BufQueue::new(1);
    assert!(
        codec
            .generate(
                &mut tiny,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &accepted,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Recorder::default(),
            )
            .is_err()
    );
    let _ = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &accepted,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert_eq!(codec.resolve_downstream_upgrade(&accepted), Some(true));
    codec.set_parser_paused(false);
    let transition = codec.on_ingress(
        &ingress_queue(&input[first.consumed..]),
        &mut callback,
        &mut callback_egress,
    );
    assert_eq!(transition.consumed, 0);
    assert_eq!(callback.recorder.completions, [true]);
    assert!(callback.recorder.bodies.is_empty());
    let raw_result = codec.on_ingress(
        &ingress_queue(&input[first.consumed..]),
        &mut callback,
        &mut callback_egress,
    );
    assert_eq!(raw_result.consumed, raw.len());
    assert_eq!(callback.recorder.bodies, raw);
}

#[test]
fn paused_downstream_connect_accepts_raw_and_rejection_demotes_to_http() {
    for (status, accepted) in [(200, true), (403, false)] {
        let head = b"CONNECT example:443 HTTP/1.1\r\nHost: example\r\n\r\n";
        let following = if accepted {
            b"connect bytes".as_slice()
        } else {
            b"GET /later HTTP/1.1\r\nHost: example\r\n\r\n".as_slice()
        };
        let mut input = head.to_vec();
        input.extend_from_slice(following);
        let mut codec = Http1xCodec::new(TransportDirection::Downstream);
        let mut callback = PauseOnHeaders::default();
        let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
        let first = codec.on_ingress(&ingress_queue(&input), &mut callback, &mut callback_egress);
        assert_eq!(first.consumed, head.len());
        let response = response(status, (1, 1));
        let _ = generate(
            &mut codec,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &response,
                end_stream: !accepted,
                extra_headers: None,
            },
        );
        assert_eq!(codec.resolve_downstream_upgrade(&response), Some(accepted));
        codec.set_parser_paused(false);
        let second = codec.on_ingress(
            &ingress_queue(&input[first.consumed..]),
            &mut callback,
            &mut callback_egress,
        );
        assert_eq!(callback.recorder.completions, [accepted]);
        if accepted {
            assert_eq!(second.consumed, 0);
            assert!(callback.recorder.bodies.is_empty());
            let raw_result = codec.on_ingress(
                &ingress_queue(&input[first.consumed..]),
                &mut callback,
                &mut callback_egress,
            );
            assert_eq!(raw_result.consumed, following.len());
            assert_eq!(callback.recorder.bodies, following);
        } else {
            assert_eq!(second.consumed, following.len());
            assert_eq!(
                callback
                    .recorder
                    .events
                    .iter()
                    .filter(|event| **event == "begin")
                    .count(),
                2,
            );
            assert!(callback.recorder.bodies.is_empty());
        }
    }
}

#[test]
fn paused_downstream_websocket_rejection_demotes_to_http() {
    let head = b"GET / HTTP/1.1\r\nHost: example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    let following = b"GET /later HTTP/1.1\r\nHost: example\r\n\r\n";
    let mut input = head.to_vec();
    input.extend_from_slice(following);
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = PauseOnHeaders::default();
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
    let first = codec.on_ingress(&ingress_queue(&input), &mut callback, &mut callback_egress);
    assert_eq!(first.consumed, head.len());
    let rejected = response(400, (1, 1));
    let _ = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &rejected,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert_eq!(codec.resolve_downstream_upgrade(&rejected), Some(false));
    codec.set_parser_paused(false);
    let second = codec.on_ingress(
        &ingress_queue(&input[first.consumed..]),
        &mut callback,
        &mut callback_egress,
    );
    assert_eq!(second.consumed, following.len());
    assert_eq!(callback.recorder.completions, [false]);
    assert_eq!(
        callback
            .recorder
            .events
            .iter()
            .filter(|event| **event == "begin")
            .count(),
        2,
    );
    assert!(callback.recorder.bodies.is_empty());
}

#[test]
fn http11_upgrade_request_reject_has_connection_close() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"GET / HTTP/1.1\r\nUpgrade: blarf\r\nConnection: upgrade\r\n\r\n",
    );
    let mut message = response(200, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Connection: close").is_some());
    assert!(!codec.is_reusable());
}

#[test]
fn http11_websocket_upgrade_accept_keeps_connection_upgrade() {
    let (_, response) = websocket_round_trip();
    assert!(find_bytes(&response, b"Connection: Upgrade").is_some());
    assert!(find_bytes(&response, b"close").is_none());
}

#[test]
fn http11_non_upgrade_request_no_forced_close() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, b"GET / HTTP/1.1\r\n\r\n");
    let mut message = response(200, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Connection: keep-alive").is_some());
    assert!(codec.is_reusable());
}

#[test]
fn http11_empty_connection_header_suppresses_implicit_keepalive_header() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Get, b"/", (1, 1));
    message.headers_mut().add(HttpHeaderCode::Connection, b"");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Connection:").is_none());
    assert!(codec.is_reusable());
}

#[test]
fn websocket_upgrade_key_error() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let mut request = request(HttpMethod::Get, b"/", (1, 1));
    request.set_egress_websocket_upgrade();
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    );
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: wrong\r\n\r\n",
    );
    assert_eq!(callback.errors.len(), 1);
}

#[test]
fn websocket_upgrade_header_set() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Get, b"/", (1, 1));
    message.set_egress_websocket_upgrade();
    message
        .headers_mut()
        .set(HttpHeaderCode::Upgrade, b"Websocket");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Sec-WebSocket-Key").is_none());
}

#[test]
fn websocket_connection_header() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Get, b"/websocket", (1, 1));
    message.set_egress_websocket_upgrade();
    message
        .headers_mut()
        .add(HttpHeaderCode::Connection, b"upgrade, keep-alive");
    message
        .headers_mut()
        .add(HttpHeaderCode::SecWebSocketKey, b"key should change");
    message
        .headers_mut()
        .add(HttpHeaderCode::SecWebSocketAccept, b"must be stripped");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&wire, b"Connection: upgrade, keep-alive").is_some());
    assert!(find_bytes(&wire, b"key should change").is_none());
    assert!(find_bytes(&wire, b"must be stripped").is_none());

    let mut downstream = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut downstream, &mut callback, &wire);
    let key = callback.messages[0]
        .headers()
        .get_single_or_empty(HttpHeaderCode::SecWebSocketKey);
    assert_ne!(key, b"key should change");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(key)
            .unwrap()
            .len(),
        16
    );
    assert!(
        callback.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::SecWebSocketAccept)
            .is_empty()
    );
}

#[test]
fn websocket_upgrade_second_txn() {
    let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
    let mut downstream = Http1xCodec::new(TransportDirection::Downstream);
    let mut upstream_events = Recorder::default();
    let mut downstream_events = Recorder::default();

    let first = upstream.create_stream();
    let first_request = request(HttpMethod::Get, b"/", (1, 1));
    let request_wire = generate(
        &mut upstream,
        EgressFrame::Headers {
            stream: first,
            message: &first_request,
            end_stream: true,
            extra_headers: None,
        },
    );
    parse(&mut downstream, &mut downstream_events, &request_wire);
    let mut first_response = response(200, (1, 1));
    first_response
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    let response_wire = generate(
        &mut downstream,
        EgressFrame::Headers {
            stream: first,
            message: &first_response,
            end_stream: true,
            extra_headers: None,
        },
    );
    parse(&mut upstream, &mut upstream_events, &response_wire);
    assert!(upstream.is_reusable());
    assert!(downstream.is_reusable());

    let second = upstream.create_stream();
    let mut websocket = request(HttpMethod::Get, b"/ws", (1, 1));
    websocket.set_egress_websocket_upgrade();
    let request_wire = generate(
        &mut upstream,
        EgressFrame::Headers {
            stream: second,
            message: &websocket,
            end_stream: false,
            extra_headers: None,
        },
    );
    parse(&mut downstream, &mut downstream_events, &request_wire);
    assert_eq!(downstream_events.messages.len(), 2);
    assert!(downstream_events.messages[1].is_ingress_websocket_upgrade());
    let key = downstream_events.messages[1]
        .headers()
        .get_single_or_empty(HttpHeaderCode::SecWebSocketKey);
    assert!(!key.is_empty());

    let mut switching = response(101, (1, 1));
    switching.set_egress_websocket_upgrade();
    let response_wire = generate(
        &mut downstream,
        EgressFrame::Headers {
            stream: second,
            message: &switching,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&response_wire, b"Sec-WebSocket-Accept: ").is_some());
    parse(&mut upstream, &mut upstream_events, &response_wire);
    assert!(upstream_events.errors.is_empty());
    assert!(upstream_events.messages[1].is_ingress_websocket_upgrade());
    assert!(!upstream.is_reusable());
    assert!(!downstream.is_reusable());
}

#[test]
fn trailers_and_eom_are_not_generated_when_non_chunked() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let message = request(HttpMethod::Get, b"/", (1, 1));
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    let mut trailers = HttpHeaders::new();
    trailers.add(b"X-Trailer", b"value");
    assert!(
        generate(
            &mut codec,
            EgressFrame::Trailers {
                stream,
                trailers: &trailers,
            },
        )
        .is_empty()
    );
    assert!(generate(&mut codec, EgressFrame::EndOfMessage { stream }).is_empty());
}

#[test]
fn test_chunk_response_serialization() {
    let mut downstream = Http1xCodec::make_response_codec(true);
    let stream = downstream.create_stream();
    let mut message = response(200, (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    let mut wire = generate(
        &mut downstream,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    let body = BufQueue::from_bytes(Bytes::from_static(b"pizza"), 5).unwrap();
    wire.extend(generate(
        &mut downstream,
        EgressFrame::Body {
            stream,
            body,
            padding: None,
            end_stream: false,
        },
    ));
    let mut trailers = HttpHeaders::new();
    trailers.add(b"X-Test-Trailer", b"chicken kyiv");
    wire.extend(generate(
        &mut downstream,
        EgressFrame::Trailers {
            stream,
            trailers: &trailers,
        },
    ));
    let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
    let mut callback = Recorder::default();
    parse(&mut upstream, &mut callback, &wire);
    assert_eq!(callback.bodies, b"pizza");
    assert_eq!(callback.trailers.len(), 1);
    assert!(callback.events.contains(&"complete"));
}

#[test]
fn test_generate_empty_body_with_eom() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Post, b"/", (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert_eq!(
        generate(
            &mut codec,
            EgressFrame::Body {
                stream,
                body: BufQueue::new(0),
                padding: None,
                end_stream: true,
            },
        ),
        b"0\r\n\r\n"
    );
}

#[test]
fn test_header_value_white_spaces() {
    let message = assert_simple_request(
        b"GET / HTTP/1.1\r\nHost: www.facebook.com  \r\nX-FB-HEADER: yay \r\n\r\n",
    );
    assert_eq!(
        message.headers().get_single_or_empty(HttpHeaderCode::Host),
        b"www.facebook.com"
    );
    assert_eq!(
        message.headers().get_single_or_empty(b"X-FB-HEADER"),
        b"yay"
    );
}

#[test]
fn downstream_goaway() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    assert!(codec.is_reusable());
    generate(
        &mut codec,
        EgressFrame::Goaway {
            last_stream: StreamId::MAX,
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        },
    );
    assert!(codec.is_waiting_to_drain());
}

#[test]
fn downstream_immediate_goaway() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    assert_eq!(codec.generate_immediate_goaway().bytes_written, 0);
    assert!(!codec.is_reusable());
}

#[test]
fn downstream_error_goaway() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    generate(
        &mut codec,
        EgressFrame::Goaway {
            last_stream: StreamId::MAX,
            code: H2ErrorCode::PROTOCOL_ERROR,
            debug_data: None,
        },
    );
    assert!(!codec.is_reusable());
}

#[test]
fn generate_rst() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    generate(
        &mut codec,
        EgressFrame::Reset {
            stream: StreamId::new(1),
            code: H2ErrorCode::INTERNAL_ERROR,
        },
    );
    assert!(!codec.is_reusable());
}

#[test]
fn upstream_goaway() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    generate(
        &mut codec,
        EgressFrame::Goaway {
            last_stream: StreamId::UNSPECIFIED,
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        },
    );
    assert!(!codec.is_reusable());
}

#[test]
fn close_on_egress_complete_upstream_connect() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let message = request(HttpMethod::Connect, b"/", (1, 1));
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(!codec.close_on_egress_complete());
    generate(&mut codec, EgressFrame::EndOfMessage { stream });
    assert!(codec.close_on_egress_complete());
}

#[test]
fn close_on_egress_complete_downstream_response() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, b"GET / HTTP/1.0\r\n\r\n");
    let message = response(200, (1, 0));
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(!codec.close_on_egress_complete());
    generate(
        &mut codec,
        EgressFrame::EndOfMessage {
            stream: StreamId::new(1),
        },
    );
    assert!(codec.close_on_egress_complete());
}

#[test]
fn generate_extra_headers() {
    let mut codec = Http1xCodec::make_response_codec(true);
    let stream = codec.create_stream();
    let mut message = response(200, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"1000");
    let mut extra = HttpHeaders::new();
    extra.add(HttpHeaderCode::Priority, b"u=1");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: Some(&extra),
        },
    );
    assert!(find_bytes(&wire, b"Priority: u=1").is_some());
}

#[test]
fn websocket_upgrade_duplicate() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    let input = b"GET / HTTP/1.1\r\nUpgrade: websocket\r\nSec-WebSocket-Key: one\r\n\r\n";
    parse(&mut codec, &mut callback, input);
    codec.set_parser_paused(false);
    parse(&mut codec, &mut callback, input);
    assert_eq!(callback.errors.len(), 1);
}

#[test]
fn upgrade_accepted() {
    for (client, server, accepted) in [
        (
            b"  Websocket,,,".as_slice(),
            b",,,websocket  ".as_slice(),
            true,
        ),
        (
            b"h2c, ,websocket".as_slice(),
            b" ,not-advertised".as_slice(),
            false,
        ),
        (
            b",test1,test2,".as_slice(),
            b",,Test3,Test4".as_slice(),
            false,
        ),
        (b"websocket".as_slice(), b"".as_slice(), false),
        (b"".as_slice(), b"websocket".as_slice(), false),
        (b"test1,websocket".as_slice(), b"websocket".as_slice(), true),
    ] {
        assert_eq!(server_accepted_upgrade(client, server), accepted);
    }
}

#[test]
fn huge_url() {
    let mut request = b"GET /echo?q=".to_vec();
    request.extend(std::iter::repeat_n(b'a', 84 * 1024));
    request.extend_from_slice(b" HTTP/1.1\r\nHost: foo\r\n\r\n");
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    for fragment in request.chunks(4096) {
        parse(&mut codec, &mut callback, fragment);
    }
    assert_eq!(callback.errors.len(), 1);
}

#[test]
fn utf8_chars() {
    assert_simple_request(b"GET /echo?dl=%C3%91o%C3%B1o HTTP/1.1\r\n\r\n");
    assert_header_error("GET /echo?dl=Ñoño HTTP/1.1\r\n\r\n".as_bytes(), true);
}

#[test]
fn extra_crlf() {
    let input =
        b"\rGET /one HTTP/1.1\r\n\r\n\nGET /two HTTP/1.1\r\n\r\n\r\nGET /three HTTP/1.1\r\n\r\n";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, input), input.len());
    assert_eq!(callback.messages.len(), 3);
}

#[test]
fn content_length_last() {
    let mut codec = Http1xCodec::make_response_codec(true);
    let stream = codec.create_stream();
    let mut message = response(200, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"100");
    message.headers_mut().set(b"z", b"x");
    let wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert!(wire.ends_with(b"Content-Length: 100\r\n\r\n"));
}

#[test]
fn huge_chunk_length() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\n",
    );
    assert_eq!(callback.errors[0].0, ProxygenError::PARSE_BODY);
}

#[test]
fn huge_content_length() {
    assert_header_error(
        b"POST / HTTP/1.1\r\nContent-Length: ffffffffffffffff\r\n\r\n",
        false,
    );
}

fn chunkify_wire(request_headers: &[u8], response_version: (u8, u8)) -> Vec<u8> {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, request_headers);
    let message = response(200, response_version);
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    )
}

#[test]
fn dechunk() {
    let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
    let stream = upstream.create_stream();
    let request = request(HttpMethod::Get, b"/", (1, 0));
    let request_wire = generate(
        &mut upstream,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: true,
            extra_headers: None,
        },
    );
    let mut downstream = Http1xCodec::new(TransportDirection::Downstream);
    let mut downstream_events = Recorder::default();
    parse(&mut downstream, &mut downstream_events, &request_wire);
    let mut response = response(200, (1, 1));
    response.set_chunked(true);
    response
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    let mut wire = generate(
        &mut downstream,
        EgressFrame::Headers {
            stream,
            message: &response,
            end_stream: false,
            extra_headers: None,
        },
    );
    wire.extend(generate(
        &mut downstream,
        EgressFrame::ChunkHeader { stream, length: 10 },
    ));
    wire.extend(generate(
        &mut downstream,
        EgressFrame::Body {
            stream,
            body: BufQueue::from_bytes(Bytes::from_static(b"aaaaaaaaaa"), 10).unwrap(),
            padding: None,
            end_stream: false,
        },
    ));
    wire.extend(generate(
        &mut downstream,
        EgressFrame::ChunkTerminator { stream },
    ));
    wire.extend(generate(
        &mut downstream,
        EgressFrame::EndOfMessage { stream },
    ));
    assert!(find_bytes(&wire, b"Transfer-Encoding").is_none());
    assert!(find_bytes(&wire, b"Connection: close").is_some());

    let mut callback = Recorder::default();
    parse(&mut upstream, &mut callback, &wire);
    assert_eq!(callback.bodies, b"aaaaaaaaaa");
    assert!(callback.chunk_lengths.is_empty());
    assert!(!callback.events.contains(&"complete"));
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
    let _ = upstream.on_ingress_eof(&mut callback, &mut callback_egress);
    assert!(callback.events.contains(&"complete"));
}

#[test]
fn chunkify() {
    let wire = chunkify_wire(b"GET / HTTP/1.1\r\n\r\n", (1, 0));
    assert!(find_bytes(&wire, b"Transfer-Encoding: chunked").is_some());
}

#[test]
fn chunkify_conn_close() {
    let wire = chunkify_wire(b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n", (1, 1));
    assert!(find_bytes(&wire, b"Transfer-Encoding: chunked").is_some());
    assert!(find_bytes(&wire, b"Connection: close").is_some());
}

#[test]
fn chunkify_100() {
    let mut codec = Http1xCodec::with_options(TransportDirection::Downstream, true, false);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"GET / HTTP/1.1\r\nExpect: 100-continue\r\n\r\n",
    );
    let first = response(100, (1, 0));
    let interim = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &first,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&interim, b"Transfer-Encoding").is_none());
    let final_response = response(200, (1, 0));
    let final_wire = generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &final_response,
            end_stream: false,
            extra_headers: None,
        },
    );
    assert!(find_bytes(&final_wire, b"Transfer-Encoding: chunked").is_some());
}

#[test]
fn trailers_non_chunked() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let request = request(HttpMethod::Get, b"/", (1, 1));
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: true,
            extra_headers: None,
        },
    );
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na0\r\nTrailer: x\r\n\r\n",
    );
    assert_eq!(callback.errors.len(), 1);
}

#[test]
fn header_ctls() {
    let message = assert_simple_request(b"GET / HTTP/1.1\r\nFoo: \"\\\r\\\n\"\r\n\r\n");
    assert_eq!(message.headers().number_of_values(b"Foo"), 1);
}

#[test]
fn header_ctls_middle() {
    assert_error(
        b"GET / HTTP/1.1\r\nFoo: \"\\\r\\\n\"\r\nBar: b\r\n\r\n",
        true,
        ProxygenError::HEADER_CONTENT_VALIDATION,
    );
}

#[test]
fn trailer_ctls() {
    let mut codec = Http1xCodec::with_options(TransportDirection::Downstream, false, true);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"GET / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nFoo: \"\\\r\\\n\"\r\n\r\n",
    );
    assert_eq!(callback.errors.len(), 1);
}

#[test]
fn absolute_url_no_path() {
    let mut message = assert_simple_request(b"GET http://www.foo.com HTTP/1.0\r\n\r\n");
    assert_eq!(message.path().unwrap(), b"/");
    assert_eq!(
        message.headers().get_single_or_empty(HttpHeaderCode::Host),
        b"www.foo.com"
    );
}

#[test]
fn connection_headers_parameter_matrix() {
    let cases: &[(&[&[u8]], &[u8])] = &[
        (&[b"foo", b"bar", b"close", b"baz"], b"foo, bar, baz, close"),
        (&[b"foo", b"bar, close", b"baz"], b"foo, bar, baz, close"),
        (
            &[b" foo", b"bar, close ", b" baz "],
            b"foo, bar, baz, close",
        ),
        (
            &[b"foo", b"bar, boo", b"baz"],
            b"foo, bar, boo, baz, keep-alive",
        ),
        (&[b"foo", b"keep-alive, boo", b"close"], b"foo, boo, close"),
        (
            &[b"foo", b"upgrade, boo", b"baz"],
            b"foo, upgrade, boo, baz, keep-alive",
        ),
    ];
    for (values, expected) in cases {
        let mut codec = Http1xCodec::new(TransportDirection::Upstream);
        let stream = codec.create_stream();
        let mut message = request(HttpMethod::Get, b"/", (1, 1));
        for &value in *values {
            message.headers_mut().add(HttpHeaderCode::Connection, value);
        }
        let wire = generate(
            &mut codec,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
        );
        let mut line = b"Connection: ".to_vec();
        line.extend_from_slice(expected);
        line.extend_from_slice(b"\r\n");
        assert!(find_bytes(&wire, &line).is_some());
    }
}

#[test]
fn h1_upstream_session_test_codec_httperror() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, b"BLARF"), 1);
    assert_eq!(callback.errors.len(), 1);
    assert_eq!(callback.errors[0].1, None);
    assert!(!callback.errors[0].2.as_ref().unwrap().is_request());
}

#[test]
fn h1_upstream_session_test_trailing_garbage() {
    #[derive(Default)]
    struct RejectUnexpectedResponse {
        bodies: Vec<u8>,
        completes: usize,
    }
    impl HttpCodecCallback for RejectUnexpectedResponse {
        fn on_event(&mut self, _event: CodecEvent<'_>) {}
        fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
            match event {
                CodecEvent::Body { body, .. } => {
                    for segment in body.segments() {
                        self.bodies.extend_from_slice(segment);
                    }
                    CodecCallbackActions::new()
                }
                CodecEvent::MessageComplete { .. } => {
                    self.completes += 1;
                    CodecCallbackActions::new()
                }
                CodecEvent::MessageBegin { stream, .. } if stream == StreamId::new(2) => {
                    vec![CodecCallbackAction::Generate(OwnedEgressFrame::Reset {
                        stream,
                        code: H2ErrorCode::PROTOCOL_ERROR,
                    })]
                    .into()
                }
                _ => CodecCallbackActions::new(),
            }
        }
    }
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let request = request(HttpMethod::Post, b"/", (1, 1));
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &request,
            end_stream: false,
            extra_headers: None,
        },
    );
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789HTTP/1.1 200 OK\r\n\r\n";
    let mut callback = RejectUnexpectedResponse::default();
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
    let result = codec.on_ingress(&ingress_queue(input), &mut callback, &mut callback_egress);
    assert_eq!(result.consumed, input.len());
    assert_eq!(result.egress_results.len(), 1);
    assert!(result.egress_results[0].is_ok());
    assert_eq!(callback.bodies, b"0123456789");
    assert_eq!(callback.completes, 1);
    assert!(!codec.is_reusable());
}

#[test]
fn h1_downstream_session_test_codec_httperror() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, b"BLARF");
    assert_eq!(callback.errors.len(), 1);
}

#[test]
fn h1_downstream_session_test_codec_httperror_carries_partial_message() {
    let mut callback = assert_header_error(
        b"POST /upload?id=7 HTTP/1.1\r\nHost: www.facebook.com\r\nTransfer-Encoding: zorg\r\n\r\n",
        false,
    );
    let mut partial = callback.errors[0].2.take().unwrap();
    assert_eq!(partial.url().unwrap(), b"/upload?id=7");
    assert_eq!(
        partial.headers().get_single_or_empty(HttpHeaderCode::Host),
        b"www.facebook.com"
    );
}

#[test]
fn h1_downstream_session_test_codec_httperror_stream_egress_complete() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    let response = response(301, (1, 1));
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &response,
            end_stream: true,
            extra_headers: None,
        },
    );
    parse(&mut codec, &mut callback, b"z\r\n");
    assert_eq!(callback.errors[0].0, ProxygenError::PARSE_BODY);
    assert_eq!(callback.errors[0].1, None);
}

#[test]
fn h1_downstream_session_test_codec_httperror_stream_read_in_progress() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(
        &mut codec,
        &mut callback,
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nz\r\n",
    );
    assert_eq!(callback.events, ["begin", "headers", "error"]);
    assert_eq!(callback.errors[0].1, Some(400));
}

#[test]
fn bounded_fragmentation_consumed_and_callback_order() {
    let wire = b"POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\ndata";
    for split in 0..=wire.len() {
        let mut codec = Http1xCodec::new(TransportDirection::Downstream);
        let mut callback = Recorder::default();
        assert_eq!(parse(&mut codec, &mut callback, &wire[..split]), split);
        assert_eq!(
            parse(&mut codec, &mut callback, &wire[split..]),
            wire.len() - split
        );
        assert_eq!(callback.events.first(), Some(&"begin"));
        assert_eq!(callback.events.last(), Some(&"complete"));
        assert_eq!(callback.bodies, b"data");
    }
}

#[test]
fn partial_message_owns_parser_data() {
    let mut input = b"POST /owned HTTP/1.1\r\nTransfer-Encoding: bad\r\n\r\n".to_vec();
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    parse(&mut codec, &mut callback, &input);
    input.fill(b'x');
    let mut partial = callback.errors[0].2.take().unwrap();
    assert_eq!(partial.url().unwrap(), b"/owned");
}

#[test]
fn bounded_malformed_input_never_panics_or_double_errors() {
    let corpus: &[&[u8]] = &[
        b"B",
        b"GET",
        b"GET / HTTP/1.1\r\nBad\r\n\r\n",
        b"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nz\r\n",
    ];
    for input in corpus {
        let mut codec = Http1xCodec::new(TransportDirection::Downstream);
        let mut callback = Recorder::default();
        parse(&mut codec, &mut callback, input);
        if callback.errors.is_empty() {
            let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
            let _ = codec.on_ingress_eof(&mut callback, &mut callback_egress);
        }
        assert_eq!(callback.errors.len(), 1);
        let terminal_callback_count = callback.callback_count;
        let terminal_events = callback.events.clone();
        assert_eq!(
            parse(&mut codec, &mut callback, b"GET /later HTTP/1.1\r\n\r\n",),
            0
        );
        let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
        let _ = codec.on_ingress_eof(&mut callback, &mut callback_egress);
        assert_eq!(callback.errors.len(), 1);
        assert_eq!(callback.callback_count, terminal_callback_count);
        assert_eq!(callback.events, terminal_events);
    }
}

#[test]
fn bounded_egress_failure_is_atomic_and_body_segments_remain_segmented() {
    let mut codec = Http1xCodec::make_response_codec(true);
    let stream = codec.create_stream();
    let mut message = response(200, (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    let mut callback = Recorder::default();
    let mut rejected = BufQueue::new(1);
    assert!(
        codec
            .generate(
                &mut rejected,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .is_err()
    );
    assert!(rejected.is_empty());

    let mut accepted = BufQueue::new(QUEUE_LIMIT);
    codec
        .generate(
            &mut accepted,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
            &mut callback,
        )
        .expect("failed admission must not consume response sequencing state");

    let mut body = BufQueue::new(4);
    body.push_bytes(Bytes::from_static(b"ab")).unwrap();
    body.push_bytes(Bytes::from_static(b"cd")).unwrap();
    let mut framed_body = BufQueue::new(64);
    codec
        .generate(
            &mut framed_body,
            EgressFrame::Body {
                stream,
                body,
                padding: None,
                end_stream: true,
            },
            &mut callback,
        )
        .unwrap();
    assert_eq!(queue_bytes(&framed_body), b"4\r\nabcd\r\n0\r\n\r\n");
    assert_eq!(framed_body.segment_count(), 4);
}

#[test]
fn body_egress_preserves_segment_backing() {
    let mut codec = Http1xCodec::make_response_codec(true);
    let stream = codec.create_stream();
    let mut message = response(200, (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    let mut callback = Recorder::default();
    codec
        .generate(
            &mut BufQueue::new(QUEUE_LIMIT),
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
            &mut callback,
        )
        .expect("response headers establish chunked egress");

    let first_backing = Bytes::from_static(b"--ab--");
    let first = first_backing.slice(2..4);
    let second = Bytes::from_static(b"cd");
    let pointers = [first.as_ptr(), second.as_ptr()];
    let mut body = BufQueue::new(4);
    body.push_bytes(first).expect("first body segment fits");
    body.push_bytes(second).expect("second body segment fits");
    let mut output = BufQueue::new(64);

    let generated = codec
        .generate(
            &mut output,
            EgressFrame::Body {
                stream,
                body,
                padding: None,
                end_stream: true,
            },
            &mut callback,
        )
        .expect("segmented body generation succeeds");

    assert_eq!(generated.bytes_written, 14);
    assert_eq!(generated.payload_offset, Some(3));
    assert_eq!(queue_bytes(&output), b"4\r\nabcd\r\n0\r\n\r\n");
    let segments = output.segments().collect::<Vec<_>>();
    assert_eq!(segments.len(), 4);
    assert_eq!(segments[0].as_ref(), b"4\r\n");
    assert_eq!(segments[1].as_ref(), b"ab");
    assert_eq!(segments[1].as_ptr(), pointers[0]);
    assert_eq!(segments[2].as_ref(), b"cd");
    assert_eq!(segments[2].as_ptr(), pointers[1]);
    assert_eq!(segments[3].as_ref(), b"\r\n0\r\n\r\n");
}

// Bulk head-parse oracles (`repair.http1_bulk_head_parse`, group-153). The
// golden traces were recorded from the per-byte codec that
// `roxygen_http1_baseline` freezes.

const TRACE_DATE: &[u8] = b"Tue, 06 Oct 2026 00:00:00 GMT";
const TRACE_REQUESTS: usize = 4;
const TOO_LARGE: &[u8] = b"HTTP header block is too large";
const INVALID_START_LINE: &[u8] = b"invalid HTTP start line";
const INVALID_HEADER_BYTE: &[u8] = b"invalid HTTP header byte";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PauseAt {
    MessageBegin,
    LaterMessageBegin,
    HeadersComplete,
    Body,
    ChunkHeader,
    ChunkComplete,
    MessageComplete,
}

struct TraceCase {
    name: &'static str,
    upstream: bool,
    strict: bool,
    pause: &'static [PauseAt],
    generate: bool,
    wire: &'static [u8],
}

const fn down(name: &'static str, strict: bool, wire: &'static [u8]) -> TraceCase {
    TraceCase {
        name,
        upstream: false,
        strict,
        pause: &[],
        generate: false,
        wire,
    }
}

const fn up(name: &'static str, strict: bool, wire: &'static [u8]) -> TraceCase {
    TraceCase {
        name,
        upstream: true,
        strict,
        pause: &[],
        generate: false,
        wire,
    }
}

const fn paused(
    name: &'static str,
    upstream: bool,
    pause: &'static [PauseAt],
    generate: bool,
    wire: &'static [u8],
) -> TraceCase {
    TraceCase {
        name,
        upstream,
        strict: false,
        pause,
        generate,
        wire,
    }
}

const TRACE_CASES: &[TraceCase] = &[
    down(
        "request crlf",
        false,
        b"GET /path?q=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: trace\r\nAccept: */*\r\n\r\n",
    ),
    down(
        "request bare lf with duplicates",
        true,
        b"GET / HTTP/1.1\nHost: a\nX-Dup: 1\nX-Dup: 2\n\n",
    ),
    down(
        "leading empty lines",
        false,
        b"\r\n\r\nGET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
    ),
    down(
        "pipelined with body in the head segment",
        false,
        b"GET /a HTTP/1.1\r\nHost: a\r\n\r\nPOST /b HTTP/1.1\r\nHost: b\r\nContent-Length: 5\r\n\r\nhelloGET /c HTTP/1.1\r\n\r\n",
    ),
    down(
        "chunked request with trailers",
        true,
        b"POST /u HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nX-T: 1\r\nX-T: 2\r\n\r\n",
    ),
    down(
        "obs-fold",
        false,
        b"GET / HTTP/1.1\r\nX-Fold: a\r\n  b\r\n\tc \r\nHost: h\r\n\r\n",
    ),
    down(
        "obs-fold without a header",
        false,
        b"GET / HTTP/1.1\r\n folded\r\n\r\n",
    ),
    down(
        "escaped newline lenient",
        false,
        b"GET / HTTP/1.1\r\nX-Esc: a\\\r\\\nb\r\n\r\n",
    ),
    down(
        "escaped newline strict",
        true,
        b"GET / HTTP/1.1\r\nX-Esc: a\\\r\\\nb\r\n\r\n",
    ),
    down(
        "escaped newline before a bare lf",
        false,
        b"GET / HTTP/1.1\r\nX-Esc: a\\\r\\\n\n",
    ),
    down(
        "lf cr lf does not end a head",
        false,
        b"GET / HTTP/1.1\n\r\nHost: a\r\n\r\n",
    ),
    down("http/0.9 without version", false, b"GET /old\r\n"),
    down("http/0.9 with version", false, b"GET /old HTTP/0.9\n"),
    down("http/0.9 cr cr lf", false, b"GET /old\r\r\n"),
    down("request line of one word", false, b"GET\r\n\r\n"),
    down("request line with leading space", false, b" / HTTP/1.1\r\n\r\n"),
    down("tab delimited request line", false, b"GET\t/\tHTTP/1.1\r\n\r\n"),
    down(
        "high bytes lenient",
        false,
        b"GET /\x7f\x80\xff HTTP/1.1\r\nX-H: \t\x7f\x80\xfe v\r\n\r\n",
    ),
    down(
        "high bytes strict",
        true,
        b"GET /\x7f\x80\xff HTTP/1.1\r\nX-H: \t\x7f\x80\xfe v\r\n\r\n",
    ),
    down(
        "high bytes in a header value strict",
        true,
        b"GET / HTTP/1.1\r\nX-H: \x80\r\n\r\n",
    ),
    down(
        "invalid header name",
        false,
        b"GET / HTTP/1.1\r\nBad Name: x\r\n\r\n",
    ),
    down(
        "header without colon",
        false,
        b"GET / HTTP/1.1\r\nNoColon\r\n\r\n",
    ),
    down("invalid version", false, b"GET / HTTP/1.x\r\n\r\n"),
    down(
        "invalid content length",
        false,
        b"POST /form HTTP/1.0\r\nContent-Length: abc\r\n\r\n",
    ),
    down(
        "conflicting content length",
        false,
        b"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx",
    ),
    down(
        "control byte in a header",
        false,
        b"GET /path HTTP/1.1\r\nX: \x01\r\n\r\n",
    ),
    down("invalid method", false, b"GETS / HTTP/1.1\r\n\r\n"),
    down(
        "websocket upgrade",
        false,
        b"GET /ws HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\nframe",
    ),
    down(
        "connect",
        false,
        b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\n\r\n",
    ),
    down("connect with tunnel bytes", false, CONNECT_THEN_TUNNEL),
    down("strict invalid url", true, b"GET /a\"b HTTP/1.1\r\n\r\n"),
    down("eof downstream before lf", false, b"GET /p HTTP/1.1"),
    down("eof downstream after lf", false, b"GET /p HTTP/1.1\r\nHost: h"),
    up(
        "response content length",
        false,
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nServer: s\r\n\r\nhello",
    ),
    up(
        "response 100 then 200",
        false,
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
    ),
    up(
        "response chunked",
        true,
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
    ),
    up(
        "response until eof",
        false,
        b"HTTP/1.0 200 OK\r\nX: y\r\n\r\nbody-until-eof",
    ),
    up(
        "response bare lf",
        false,
        b"HTTP/1.1 404 Not Found\nX-A: 1\n\n",
    ),
    up("response invalid version", false, b"HTTP/2 200 OK\r\n\r\n"),
    up("response invalid status", false, b"HTTP/1.1 2x0 OK\r\n\r\n"),
    up("response without reason", false, b"HTTP/1.1 204\r\n\r\n"),
    up(
        "response high bytes lenient",
        false,
        b"HTTP/1.1 200 \t\x7f\xff\r\nX: \x80\x7f\r\nContent-Length: 0\r\n\r\n",
    ),
    up(
        "response high bytes strict",
        true,
        b"HTTP/1.1 200 \t\x7f\xff\r\nX: \x80\x7f\r\nContent-Length: 0\r\n\r\n",
    ),
    up(
        "response obs-fold",
        false,
        b"HTTP/1.1 200 OK\r\nX-Fold: a\r\n b\r\nContent-Length: 0\r\n\r\n",
    ),
    up("eof upstream before lf", false, b"HTTP/1.1 200 OK"),
    up("eof upstream after lf", false, b"HTTP/1.1 200 OK\r\nServer: s"),
];

const PIPELINED_GETS: &[u8] =
    b"GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /b HTTP/1.1\r\nHost: b\r\n\r\n";
const FIXED_POST_THEN_GET: &[u8] =
    b"POST /p HTTP/1.1\r\nContent-Length: 5\r\n\r\nhelloGET /g HTTP/1.1\r\n\r\n";
const CHUNKED_POST_THEN_GET: &[u8] = b"POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\nX-T: 1\r\n\r\nGET /g HTTP/1.1\r\n\r\n";
const WEBSOCKET_THEN_BYTES: &[u8] = b"GET /ws HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\nGET /after HTTP/1.1\r\n\r\n";
const CONNECT_THEN_TUNNEL: &[u8] =
    b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\n\r\n\x16\x03\x01\x00GET / HTTP/1.1\r\n\r\n";
const UPSTREAM_RESPONSES: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbodyHTTP/1.1 204 No Content\r\n\r\n";

const PAUSE_CASES: &[TraceCase] = &[
    paused(
        "pipelined gets paused at the second begin",
        false,
        &[PauseAt::LaterMessageBegin],
        false,
        PIPELINED_GETS,
    ),
    paused(
        "fixed post paused at headers",
        false,
        &[PauseAt::HeadersComplete],
        false,
        FIXED_POST_THEN_GET,
    ),
    paused(
        "fixed post paused at body",
        false,
        &[PauseAt::Body],
        false,
        FIXED_POST_THEN_GET,
    ),
    paused(
        "fixed post paused at complete",
        false,
        &[PauseAt::MessageComplete],
        false,
        FIXED_POST_THEN_GET,
    ),
    paused(
        "chunked post paused at body",
        false,
        &[PauseAt::Body],
        false,
        CHUNKED_POST_THEN_GET,
    ),
    paused(
        "chunked post paused at complete",
        false,
        &[PauseAt::MessageComplete],
        false,
        CHUNKED_POST_THEN_GET,
    ),
    paused(
        "websocket upgrade paused at headers",
        false,
        &[PauseAt::HeadersComplete],
        false,
        WEBSOCKET_THEN_BYTES,
    ),
    paused(
        "fixed post generates at headers",
        false,
        &[PauseAt::HeadersComplete],
        true,
        FIXED_POST_THEN_GET,
    ),
    paused(
        "malformed head generates at begin",
        false,
        &[PauseAt::MessageBegin],
        true,
        b"GET / HTTP/1.1\r\nNoColon\r\n\r\n",
    ),
    paused(
        "invalid first byte generates at begin",
        false,
        &[PauseAt::MessageBegin],
        true,
        b"XGET / HTTP/1.1\r\n\r\n",
    ),
    paused(
        "upstream paused at headers",
        true,
        &[PauseAt::HeadersComplete],
        false,
        UPSTREAM_RESPONSES,
    ),
    paused(
        "upstream paused at complete",
        true,
        &[PauseAt::MessageComplete],
        false,
        UPSTREAM_RESPONSES,
    ),
    paused(
        "connect with tunnel bytes paused at headers",
        false,
        &[PauseAt::HeadersComplete],
        false,
        CONNECT_THEN_TUNNEL,
    ),
];

const GOLDEN_TRACES: &[(&str, &str)] = &[
    (
        "request crlf",
        r##"begin 1
headers 1 request method=Some("GET") url="/path?q=1" version=1.1 headers=["Host": "example.com", "User-Agent": "trace", "Accept": "*/*"] size=77/77 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 77
eof
"##,
    ),
    (
        "request bare lf with duplicates",
        r##"begin 1
headers 1 request method=Some("GET") url="/" version=1.1 headers=["Host": "a", "X-Dup": "1", "X-Dup": "2"] size=42/42 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 42
eof
"##,
    ),
    (
        "leading empty lines",
        r##"begin 1
headers 1 request method=Some("GET") url="/" version=1.0 headers=["Connection": "keep-alive"] size=42/42 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 46
eof
"##,
    ),
    (
        "pipelined with body in the head segment",
        r##"begin 1
headers 1 request method=Some("GET") url="/a" version=1.1 headers=["Host": "a"] size=28/28 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
begin 2
headers 2 request method=Some("POST") url="/b" version=1.1 headers=["Host": "b", "Content-Length": "5"] size=48/48 upgraded=false websocket=false keepalive=true chunked=false
body 2 "hello"
complete 2 upgraded=false
begin 3
headers 3 request method=Some("GET") url="/c" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 3 upgraded=false
consumed 100
eof
"##,
    ),
    (
        "chunked request with trailers",
        r##"begin 1
headers 1 request method=Some("POST") url="/u" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 5
body 1 "hello"
chunk_complete 1
trailers 1 ["X-T": "1", "X-T": "2"]
complete 1 upgraded=false
consumed 79
eof
"##,
    ),
    (
        "obs-fold",
        r##"begin 1
headers 1 request method=Some("GET") url="/" version=1.1 headers=["X-Fold": "a b c", "Host": "h"] size=48/48 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 48
eof
"##,
    ),
    (
        "obs-fold without a header",
        r##"begin 1
error 1 proxygen=16 "invalid folded header" status=Some(400) new_stream=false partial=request method=Some("GET") url="/" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 27
eof
"##,
    ),
    (
        "escaped newline lenient",
        r##"begin 1
headers 1 request method=Some("GET") url="/" version=1.1 headers=["X-Esc": "a\\\r\\\nb"] size=33/33 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 33
eof
"##,
    ),
    (
        "escaped newline strict",
        r##"begin 1
error 1 proxygen=27 "invalid HTTP header content" status=Some(400) new_stream=false partial=request method=Some("GET") url="/" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 33
eof
"##,
    ),
    (
        "escaped newline before a bare lf",
        r##"begin 1
headers 1 request method=Some("GET") url="/" version=1.1 headers=["X-Esc": "a\\\r\\"] size=29/29 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 29
eof
"##,
    ),
    (
        "lf cr lf does not end a head",
        r##"begin 1
headers 1 request method=Some("GET") url="/" version=1.1 headers=[] size=28/28 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 28
eof
"##,
    ),
    (
        "http/0.9 without version",
        r##"begin 1
headers 1 request method=Some("GET") url="/old" version=0.9 headers=[] size=10/10 upgraded=false websocket=false keepalive=false chunked=false
complete 1 upgraded=false
consumed 10
eof
"##,
    ),
    (
        "http/0.9 with version",
        r##"begin 1
headers 1 request method=Some("GET") url="/old" version=0.9 headers=[] size=18/18 upgraded=false websocket=false keepalive=false chunked=false
complete 1 upgraded=false
consumed 18
eof
"##,
    ),
    (
        "http/0.9 cr cr lf",
        r##"begin 1
headers 1 request method=Some("GET") url="/old\r" version=0.9 headers=[] size=11/11 upgraded=false websocket=false keepalive=false chunked=false
complete 1 upgraded=false
consumed 11
eof
"##,
    ),
    (
        "request line of one word",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP request line" status=Some(400) new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 7
eof
"##,
    ),
    (
        "request line with leading space",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP request line" status=Some(400) new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 15
eof
"##,
    ),
    (
        "tab delimited request line",
        r##"begin 1
headers 1 request method=Some("GET") url="/" version=1.1 headers=[] size=18/18 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 18
eof
"##,
    ),
    (
        "high bytes lenient",
        r##"begin 1
error 1 proxygen=27 "invalid HTTP header content" status=Some(400) new_stream=false partial=request method=Some("GET") url="/\x7f\x80\xff" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 34
eof
"##,
    ),
    (
        "high bytes strict",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP request URL" status=Some(400) new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 34
eof
"##,
    ),
    (
        "high bytes in a header value strict",
        r##"begin 1
error 1 proxygen=27 "invalid HTTP header content" status=Some(400) new_stream=false partial=request method=Some("GET") url="/" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 26
eof
"##,
    ),
    (
        "invalid header name",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP header name" status=Some(400) new_stream=false partial=request method=Some("GET") url="/" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 31
eof
"##,
    ),
    (
        "header without colon",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP header" status=Some(400) new_stream=false partial=request method=Some("GET") url="/" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 27
eof
"##,
    ),
    (
        "invalid version",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP request version" status=Some(400) new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 18
eof
"##,
    ),
    (
        "invalid content length",
        r##"begin 1
error 1 proxygen=16 "invalid Content-Length" status=Some(400) new_stream=false partial=request method=Some("POST") url="/form" version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 44
eof
"##,
    ),
    (
        "conflicting content length",
        r##"begin 1
error 1 proxygen=16 "conflicting Content-Length headers" status=Some(400) new_stream=false partial=request method=Some("POST") url="/" version=1.1 headers=["Content-Length": "1", "Content-Length": "2"] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 57
eof
"##,
    ),
    (
        "control byte in a header",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP header byte" status=Some(400) new_stream=false partial=request method=Some("GET") url="/path" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 24
eof
"##,
    ),
    (
        "invalid method",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP start line" status=Some(400) new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 4
eof
"##,
    ),
    (
        "websocket upgrade",
        r##"begin 1
headers 1 request method=Some("GET") url="/ws" version=1.1 headers=["Host": "h", "Upgrade": "websocket", "Connection": "Upgrade", "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="] size=115/115 upgraded=false websocket=true keepalive=true chunked=false
complete 1 upgraded=false
begin 2
error 2 proxygen=16 "invalid HTTP start line" status=Some(400) new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 116
eof
"##,
    ),
    (
        "connect",
        r##"begin 1
headers 1 request method=Some("CONNECT") url="h:443" version=1.1 headers=["Host": "h:443"] size=39/39 upgraded=true websocket=false keepalive=true chunked=false
complete 1 upgraded=true
consumed 39
eof
complete 1 upgraded=false
"##,
    ),
    (
        "connect with tunnel bytes",
        r##"begin 1
headers 1 request method=Some("CONNECT") url="h:443" version=1.1 headers=["Host": "h:443"] size=39/39 upgraded=true websocket=false keepalive=true chunked=false
complete 1 upgraded=true
body 1 "\x16\x03\x01\x00GET / HTTP/1.1\r\n\r\n"
consumed 61
eof
complete 1 upgraded=false
"##,
    ),
    (
        "strict invalid url",
        r##"begin 1
headers 1 request method=Some("GET") url="/a\"b" version=1.1 headers=[] size=21/21 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 21
eof
"##,
    ),
    (
        "eof downstream before lf",
        r##"begin 1
consumed 15
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
"##,
    ),
    (
        "eof downstream after lf",
        r##"begin 1
consumed 24
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=request method=Some("GET") url="/p" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
"##,
    ),
    (
        "response content length",
        r##"begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["Content-Length": "5", "Server": "s"] size=49/49 upgraded=false websocket=false keepalive=true chunked=false
body 1 "hello"
complete 1 upgraded=false
consumed 54
eof
"##,
    ),
    (
        "response 100 then 200",
        r##"begin 1
headers 1 response status=100 reason="Continue" version=1.1 headers=[] size=25/25 upgraded=false websocket=false keepalive=true chunked=false
begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["Content-Length": "2"] size=38/38 upgraded=false websocket=false keepalive=true chunked=false
body 1 "ok"
complete 1 upgraded=false
consumed 65
eof
"##,
    ),
    (
        "response chunked",
        r##"begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["Transfer-Encoding": "chunked"] size=47/47 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 3
body 1 "abc"
chunk_complete 1
complete 1 upgraded=false
consumed 60
eof
"##,
    ),
    (
        "response until eof",
        r##"begin 1
headers 1 response status=200 reason="OK" version=1.0 headers=["X": "y"] size=25/25 upgraded=false websocket=false keepalive=false chunked=false
body 1 "body-until-eof"
consumed 39
eof
complete 1 upgraded=false
"##,
    ),
    (
        "response bare lf",
        r##"begin 1
headers 1 response status=404 reason="Not Found" version=1.1 headers=["X-A": "1"] size=31/31 upgraded=false websocket=false keepalive=true chunked=false
consumed 31
eof
complete 1 upgraded=false
"##,
    ),
    (
        "response invalid version",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP response version" status=None new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 17
eof
"##,
    ),
    (
        "response invalid status",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP response status" status=None new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 19
eof
"##,
    ),
    (
        "response without reason",
        r##"begin 1
headers 1 response status=204 reason="" version=1.1 headers=[] size=16/16 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 16
eof
"##,
    ),
    (
        "response high bytes lenient",
        r##"begin 1
error 1 proxygen=27 "invalid HTTP header content" status=None new_stream=false partial=response status=200 reason="\t\x7f\xff" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 46
eof
"##,
    ),
    (
        "response high bytes strict",
        r##"begin 1
error 1 proxygen=27 "invalid HTTP header content" status=None new_stream=false partial=response status=200 reason="\t\x7f\xff" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 46
eof
"##,
    ),
    (
        "response obs-fold",
        r##"begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["X-Fold": "a b", "Content-Length": "0"] size=53/53 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
consumed 53
eof
"##,
    ),
    (
        "eof upstream before lf",
        r##"begin 1
consumed 15
eof
error 1 proxygen=18 "unexpected EOF" status=None new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
"##,
    ),
    (
        "eof upstream after lf",
        r##"begin 1
consumed 26
eof
error 1 proxygen=18 "unexpected EOF" status=None new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
"##,
    ),
    (
        "pipelined gets paused at the second begin",
        r##"begin 1
headers 1 request method=Some("GET") url="/a" version=1.1 headers=["Host": "a"] size=28/28 upgraded=false websocket=false keepalive=true chunked=false
complete 1 upgraded=false
begin 2
pause MessageBegin at 29
headers 2 request method=Some("GET") url="/b" version=1.1 headers=["Host": "b"] size=28/28 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
consumed 56
eof
"##,
    ),
    (
        "fixed post paused at headers",
        r##"begin 1
headers 1 request method=Some("POST") url="/p" version=1.1 headers=["Content-Length": "5"] size=39/39 upgraded=false websocket=false keepalive=true chunked=false
pause HeadersComplete at 39
body 1 "hello"
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
pause HeadersComplete at 63
complete 2 upgraded=false
consumed 63
eof
"##,
    ),
    (
        "fixed post paused at body",
        r##"begin 1
headers 1 request method=Some("POST") url="/p" version=1.1 headers=["Content-Length": "5"] size=39/39 upgraded=false websocket=false keepalive=true chunked=false
body 1 "hello"
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
consumed 63
eof
"##,
    ),
    (
        "fixed post paused at complete",
        r##"begin 1
headers 1 request method=Some("POST") url="/p" version=1.1 headers=["Content-Length": "5"] size=39/39 upgraded=false websocket=false keepalive=true chunked=false
body 1 "hello"
complete 1 upgraded=false
pause MessageComplete at 44
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
pause MessageComplete at 63
consumed 63
eof
"##,
    ),
    (
        "chunked post paused at body",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 3
body 1 "abc"
chunk_complete 1
chunk_header 1 2
body 1 "de"
chunk_complete 1
trailers 1 ["X-T": "1"]
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
consumed 95
eof
"##,
    ),
    (
        "chunked post paused at complete",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 3
body 1 "abc"
chunk_complete 1
chunk_header 1 2
body 1 "de"
chunk_complete 1
trailers 1 ["X-T": "1"]
complete 1 upgraded=false
pause MessageComplete at 76
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
pause MessageComplete at 95
consumed 95
eof
"##,
    ),
    (
        "websocket upgrade paused at headers",
        r##"begin 1
headers 1 request method=Some("GET") url="/ws" version=1.1 headers=["Upgrade": "websocket", "Connection": "Upgrade", "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="] size=106/106 upgraded=false websocket=true keepalive=true chunked=false
pause HeadersComplete at 106
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/after" version=1.1 headers=[] size=23/23 upgraded=false websocket=false keepalive=true chunked=false
pause HeadersComplete at 129
complete 2 upgraded=false
consumed 129
eof
"##,
    ),
    (
        "fixed post generates at headers",
        r##"begin 1
headers 1 request method=Some("POST") url="/p" version=1.1 headers=["Content-Length": "5"] size=39/39 upgraded=false websocket=false keepalive=true chunked=false
generated bytes=99 header_size=Some((99, 99)) payload_offset=None
egress "HTTP/1.1 200 OK\r\nDate: Tue, 06 Oct 2026 00:00:00 GMT\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"
pause HeadersComplete at 39
body 1 "hello"
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
generated bytes=99 header_size=Some((99, 99)) payload_offset=None
egress "HTTP/1.1 200 OK\r\nDate: Tue, 06 Oct 2026 00:00:00 GMT\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"
pause HeadersComplete at 63
complete 2 upgraded=false
consumed 63
eof
"##,
    ),
    (
        "malformed head generates at begin",
        r##"begin 1
generated bytes=99 header_size=Some((99, 99)) payload_offset=None
egress "HTTP/1.1 200 OK\r\nDate: Tue, 06 Oct 2026 00:00:00 GMT\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"
pause MessageBegin at 1
error 1 proxygen=16 "invalid HTTP header" status=None new_stream=false partial=request method=Some("GET") url="/" version=1.1 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
consumed 27
eof
"##,
    ),
    (
        "invalid first byte generates at begin",
        r##"begin 1
error 1 proxygen=16 "invalid HTTP start line" status=None new_stream=false partial=empty version=1.0 headers=[] size=0/0 upgraded=false websocket=false keepalive=true chunked=false
generated bytes=99 header_size=Some((99, 99)) payload_offset=None
egress "HTTP/1.1 200 OK\r\nDate: Tue, 06 Oct 2026 00:00:00 GMT\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"
pause MessageBegin at 1
consumed 1
eof
"##,
    ),
    (
        "upstream paused at headers",
        r##"begin 1
headers 1 response status=100 reason="Continue" version=1.1 headers=[] size=25/25 upgraded=false websocket=false keepalive=true chunked=false
pause HeadersComplete at 25
begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["Content-Length": "4"] size=38/38 upgraded=false websocket=false keepalive=true chunked=false
pause HeadersComplete at 63
body 1 "body"
complete 1 upgraded=false
begin 2
headers 2 response status=204 reason="No Content" version=1.1 headers=[] size=27/27 upgraded=false websocket=false keepalive=true chunked=false
pause HeadersComplete at 94
complete 2 upgraded=false
consumed 94
eof
"##,
    ),
    (
        "upstream paused at complete",
        r##"begin 1
headers 1 response status=100 reason="Continue" version=1.1 headers=[] size=25/25 upgraded=false websocket=false keepalive=true chunked=false
begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["Content-Length": "4"] size=38/38 upgraded=false websocket=false keepalive=true chunked=false
body 1 "body"
complete 1 upgraded=false
pause MessageComplete at 67
begin 2
headers 2 response status=204 reason="No Content" version=1.1 headers=[] size=27/27 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
pause MessageComplete at 94
consumed 94
eof
"##,
    ),
    (
        "connect with tunnel bytes paused at headers",
        r##"begin 1
headers 1 request method=Some("CONNECT") url="h:443" version=1.1 headers=["Host": "h:443"] size=39/39 upgraded=true websocket=false keepalive=true chunked=false
pause HeadersComplete at 39
complete 1 upgraded=true
body 1 "\x16\x03\x01\x00GET / HTTP/1.1\r\n\r\n"
consumed 61
eof
complete 1 upgraded=false
"##,
    ),
];

enum TraceEntry {
    Line(String),
    Body(u64, Vec<u8>),
}

struct TraceError {
    proxygen_error: ProxygenError,
    message: Vec<u8>,
    status: Option<u32>,
    partial: Option<HttpMessage>,
}

/// Records a normalized trace: adjacent body callbacks of one stream merge,
/// because their number follows the segmentation.
struct TraceRecorder {
    wire: Bytes,
    upstream: bool,
    pause: &'static [PauseAt],
    generate: bool,
    acting: bool,
    entries: Vec<TraceEntry>,
    events: Vec<&'static str>,
    errors: Vec<TraceError>,
    messages: Vec<HttpMessage>,
    completed: usize,
    upgraded: bool,
    begins: usize,
    answered: Vec<StreamId>,
    paused_at: Option<PauseAt>,
    body_end: usize,
}

struct TraceOutcome {
    trace: String,
    consumed: usize,
    events: Vec<&'static str>,
    errors: Vec<TraceError>,
    messages: Vec<HttpMessage>,
}

impl TraceRecorder {
    fn new(case: &TraceCase, wire: Bytes) -> Self {
        Self {
            wire,
            upstream: case.upstream,
            pause: case.pause,
            generate: case.generate,
            acting: true,
            entries: Vec::new(),
            events: Vec::new(),
            errors: Vec::new(),
            messages: Vec::new(),
            completed: 0,
            upgraded: false,
            begins: 0,
            answered: Vec::new(),
            paused_at: None,
            body_end: 0,
        }
    }

    fn line(&mut self, line: String) {
        self.entries.push(TraceEntry::Line(line));
    }

    fn event(&mut self, kind: &'static str, line: String) {
        self.events.push(kind);
        self.line(line);
    }

    fn selection(&mut self, event: &CodecEvent<'_>) -> Option<(PauseAt, StreamId)> {
        let (at, stream) = match event {
            CodecEvent::MessageBegin { stream, .. } => {
                self.begins += 1;
                let selected = self.pause.contains(&PauseAt::MessageBegin)
                    || (self.begins > 1 && self.pause.contains(&PauseAt::LaterMessageBegin));
                return selected.then_some((PauseAt::MessageBegin, *stream));
            }
            CodecEvent::HeadersComplete { stream, .. } => (PauseAt::HeadersComplete, *stream),
            CodecEvent::Body { stream, .. } => (PauseAt::Body, *stream),
            CodecEvent::ChunkHeader { stream, .. } => (PauseAt::ChunkHeader, *stream),
            CodecEvent::ChunkComplete { stream } => (PauseAt::ChunkComplete, *stream),
            CodecEvent::MessageComplete { stream, .. } => (PauseAt::MessageComplete, *stream),
            _ => return None,
        };
        self.pause.contains(&at).then_some((at, stream))
    }

    fn record(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::MessageBegin { stream, .. } => {
                self.event("begin", format!("begin {}", stream.get()));
            }
            CodecEvent::HeadersComplete { stream, message } => {
                self.event(
                    "headers",
                    format!("headers {} {}", stream.get(), render_message(&message)),
                );
                self.messages.push(*message);
            }
            CodecEvent::Body { stream, body, .. } => {
                self.events.push("body");
                let mut bytes = Vec::new();
                for segment in body.segments() {
                    bytes.extend_from_slice(segment);
                    self.body_end = segment
                        .as_ptr()
                        .addr()
                        .wrapping_sub(self.wire.as_ptr().addr())
                        .wrapping_add(segment.len());
                }
                match self.entries.last_mut() {
                    Some(TraceEntry::Body(last, merged)) if *last == stream.get() => {
                        merged.extend_from_slice(&bytes);
                    }
                    _ => self.entries.push(TraceEntry::Body(stream.get(), bytes)),
                }
            }
            CodecEvent::ChunkHeader { stream, length } => {
                self.event(
                    "chunk_header",
                    format!("chunk_header {} {length}", stream.get()),
                );
            }
            CodecEvent::ChunkComplete { stream } => {
                self.event("chunk_complete", format!("chunk_complete {}", stream.get()));
            }
            CodecEvent::TrailersComplete { stream, trailers } => {
                self.event(
                    "trailers",
                    format!("trailers {} [{}]", stream.get(), render_headers(&trailers)),
                );
            }
            CodecEvent::MessageComplete { stream, upgraded } => {
                self.completed += 1;
                self.upgraded |= upgraded;
                self.event(
                    "complete",
                    format!("complete {} upgraded={upgraded}", stream.get()),
                );
            }
            CodecEvent::Error {
                stream,
                error,
                new_stream,
            } => {
                let partial = error.partial_message().cloned();
                self.event(
                    "error",
                    format!(
                        "error {} proxygen={} \"{}\" status={:?} new_stream={new_stream} partial={}",
                        stream.get(),
                        error.proxygen_error().as_u8(),
                        error.message().escape_ascii(),
                        error.http_status_code(),
                        partial
                            .as_ref()
                            .map_or_else(|| "none".to_owned(), render_message),
                    ),
                );
                self.errors.push(TraceError {
                    proxygen_error: error.proxygen_error(),
                    message: error.message().to_vec(),
                    status: error.http_status_code(),
                    partial,
                });
            }
            _ => self.event("other", "other".to_owned()),
        }
    }

    fn finish(self, consumed: usize) -> TraceOutcome {
        let mut trace = String::new();
        for entry in &self.entries {
            match entry {
                TraceEntry::Line(line) => trace.push_str(line),
                TraceEntry::Body(stream, bytes) => {
                    let _ = write!(trace, "body {stream} \"{}\"", bytes.escape_ascii());
                }
            }
            trace.push('\n');
        }
        TraceOutcome {
            trace,
            consumed,
            events: self.events,
            errors: self.errors,
            messages: self.messages,
        }
    }
}

impl HttpCodecCallback for TraceRecorder {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.record(event);
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let selected = self.selection(&event);
        self.record(event);
        let Some((at, stream)) = selected.filter(|_| self.acting) else {
            return CodecCallbackActions::new();
        };
        self.paused_at = Some(at);
        let mut actions = Vec::new();
        if self.generate && !self.upstream && !self.answered.contains(&stream) {
            self.answered.push(stream);
            actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                stream,
                message: Box::new(trace_response()),
                end_stream: true,
                extra_headers: None,
            }));
        }
        actions.push(CodecCallbackAction::SetParserPaused(true));
        actions.into()
    }
}

/// Quotes each name and value; `escape_ascii` escapes the quote, so the
/// rendering is unambiguous.
fn render_headers(headers: &HttpHeaders) -> String {
    let mut rendered = String::new();
    headers.for_each(|name, value| {
        if !rendered.is_empty() {
            rendered.push_str(", ");
        }
        let _ = write!(
            rendered,
            "\"{}\": \"{}\"",
            name.escape_ascii(),
            value.escape_ascii()
        );
    });
    rendered
}

fn render_message(message: &HttpMessage) -> String {
    let mut rendered = String::new();
    if message.is_request() {
        let _ = write!(
            rendered,
            "request method={:?} url=\"{}\"",
            message.request_method(),
            message.request_url().unwrap_or_default().escape_ascii()
        );
    } else if let Some(status) = message.response_status() {
        let mut copy = message.clone();
        let reason = copy
            .status_message()
            .map(<[u8]>::to_vec)
            .unwrap_or_default();
        let _ = write!(
            rendered,
            "response status={status} reason=\"{}\"",
            reason.escape_ascii()
        );
    } else {
        rendered.push_str("empty");
    }
    let (major, minor) = message.http_version();
    let size = message.ingress_header_size();
    let _ = write!(
        rendered,
        " version={major}.{minor} headers=[{}] size={}/{} upgraded={} websocket={} keepalive={} chunked={}",
        render_headers(message.headers()),
        size.compressed,
        size.uncompressed,
        message.is_upgraded(),
        message.is_ingress_websocket_upgrade(),
        message.wants_keepalive(),
        message.is_chunked(),
    );
    rendered
}

fn render_generated(result: &Result<GeneratedFrame, HttpError>) -> String {
    match result {
        Ok(frame) => format!(
            "generated bytes={} header_size={:?} payload_offset={:?}",
            frame.bytes_written,
            frame
                .header_size
                .map(|size| (size.compressed, size.uncompressed)),
            frame.payload_offset,
        ),
        Err(error) => format!(
            "generate_error {} \"{}\"",
            error.code().as_u16(),
            error.message().escape_ascii()
        ),
    }
}

fn trace_response() -> HttpMessage {
    let mut message = response(200, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    message.headers_mut().set(HttpHeaderCode::Date, TRACE_DATE);
    message
}

fn send_trace_request(codec: &mut Http1xCodec) {
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Get, b"/trace", (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::Host, b"trace.example");
    let mut output = BufQueue::new(QUEUE_LIMIT);
    let _ = codec.generate(
        &mut output,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
        &mut Recorder::default(),
    );
}

/// One `on_ingress` call over the queued reads. Returns whether a selected
/// callback paused the parser.
fn trace_ingress(
    codec: &mut Http1xCodec,
    recorder: &mut TraceRecorder,
    ingress: &mut BufQueue,
    consumed: &mut usize,
    requests: &mut usize,
) -> bool {
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let result = codec.on_ingress(ingress, recorder, &mut egress);
    ingress
        .trim_start(result.consumed)
        .expect("the codec consumes at most its input");
    *consumed += result.consumed;
    for generated in &result.egress_results {
        recorder.line(render_generated(generated));
    }
    if !egress.is_empty() {
        recorder.line(format!(
            "egress \"{}\"",
            queue_bytes(&egress).escape_ascii()
        ));
    }
    let paused_at = recorder.paused_at.take();
    match paused_at {
        Some(PauseAt::Body) => assert_eq!(
            *consumed, recorder.body_end,
            "a Body pause stops at the end of that callback's bytes"
        ),
        Some(at) => recorder.line(format!("pause {at:?} at {consumed}")),
        None => {}
    }
    for _ in 0..std::mem::take(&mut recorder.completed) {
        if recorder.upstream && *requests < TRACE_REQUESTS {
            send_trace_request(codec);
            *requests += 1;
        }
    }
    paused_at.is_some()
}

type Reads = Vec<Vec<Range<usize>>>;

fn one_segment_read(range: Range<usize>) -> Vec<Range<usize>> {
    std::iter::once(range).collect()
}

/// Drives `wire` as the session does: each read appends its segments to the
/// ingress queue before one `on_ingress` call. A callback pause holds until
/// the next call; after the last read, paused calls resume on what remains,
/// and after an upgrade one more call takes the bytes that follow it.
fn run_trace(case: &TraceCase, wire: &Bytes, reads: &[Vec<Range<usize>>]) -> TraceOutcome {
    let direction = if case.upstream {
        TransportDirection::Upstream
    } else {
        TransportDirection::Downstream
    };
    let mut codec = Http1xCodec::with_options(direction, false, case.strict);
    let mut recorder = TraceRecorder::new(case, wire.clone());
    let mut requests = 0;
    if case.upstream {
        send_trace_request(&mut codec);
        requests += 1;
    }
    let mut ingress = BufQueue::new(usize::MAX);
    let mut consumed = 0;
    let mut callback_paused = false;
    for read in reads {
        for range in read {
            ingress
                .push_bytes(wire.slice(range.clone()))
                .expect("an unbounded queue accepts every read");
        }
        if callback_paused {
            codec.set_parser_paused(false);
        }
        callback_paused = trace_ingress(
            &mut codec,
            &mut recorder,
            &mut ingress,
            &mut consumed,
            &mut requests,
        );
    }
    let mut resumed = 0;
    while callback_paused && resumed < 4 * wire.len() + 4 {
        codec.set_parser_paused(false);
        callback_paused = trace_ingress(
            &mut codec,
            &mut recorder,
            &mut ingress,
            &mut consumed,
            &mut requests,
        );
        resumed += 1;
    }
    if recorder.upgraded && !ingress.is_empty() {
        trace_ingress(
            &mut codec,
            &mut recorder,
            &mut ingress,
            &mut consumed,
            &mut requests,
        );
    }
    recorder.line(format!("consumed {consumed}"));
    recorder.acting = false;
    recorder.line("eof".to_owned());
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let eof = codec.on_ingress_eof(&mut recorder, &mut egress);
    assert!(eof.egress_results.is_empty() && egress.is_empty());
    recorder.finish(consumed)
}

fn split_deliveries(length: usize, split: usize) -> [(String, Reads); 2] {
    [
        (
            format!("two reads split at {split}"),
            vec![one_segment_read(0..split), one_segment_read(split..length)],
        ),
        (
            format!("one read of two segments split at {split}"),
            vec![vec![0..split, split..length]],
        ),
    ]
}

fn byte_by_byte(length: usize) -> (String, Reads) {
    (
        "byte by byte".to_owned(),
        (0..length)
            .map(|index| one_segment_read(index..index + 1))
            .collect(),
    )
}

fn deliveries(length: usize) -> Vec<(String, Reads)> {
    let mut deliveries = vec![("one segment".to_owned(), vec![one_segment_read(0..length)])];
    for split in 1..length {
        deliveries.extend(split_deliveries(length, split));
    }
    deliveries.push(byte_by_byte(length));
    deliveries
}

fn for_every_delivery(case: &TraceCase, mut check: impl FnMut(&str, &TraceOutcome)) {
    let wire = Bytes::from_static(case.wire);
    for (delivery, reads) in deliveries(wire.len()) {
        check(
            &format!("{}: {delivery}", case.name),
            &run_trace(case, &wire, &reads),
        );
    }
}

fn one_segment_trace(case: &TraceCase) -> TraceOutcome {
    let wire = Bytes::from_static(case.wire);
    run_trace(case, &wire, &[one_segment_read(0..wire.len())])
}

fn golden_trace(name: &str) -> &'static str {
    GOLDEN_TRACES
        .iter()
        .find(|(golden, _)| *golden == name)
        .map(|(_, trace)| *trace)
        .unwrap_or_else(|| panic!("no golden trace for {name}"))
}

fn trace_case(name: &str) -> &'static TraceCase {
    TRACE_CASES
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("no trace case {name}"))
}

fn header_pairs(message: &HttpMessage) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = Vec::new();
    message
        .headers()
        .for_each(|name, value| pairs.push((name.to_vec(), value.to_vec())));
    pairs
}

fn assert_one_error<'a>(
    outcome: &'a TraceOutcome,
    context: &str,
    proxygen_error: ProxygenError,
    message: &[u8],
) -> &'a TraceError {
    let [error] = outcome.errors.as_slice() else {
        panic!(
            "{context}: expected one error, got {}",
            outcome.errors.len()
        );
    };
    assert_eq!(error.proxygen_error, proxygen_error, "{context}");
    assert_eq!(
        error.message.escape_ascii().to_string(),
        message.escape_ascii().to_string(),
        "{context}"
    );
    error
}

fn assert_scan_error(outcome: &TraceOutcome, context: &str, message: &[u8], consumed: usize) {
    assert_eq!(outcome.consumed, consumed, "{context}");
    assert_eq!(outcome.events.last(), Some(&"error"), "{context}");
    assert_one_error(outcome, context, ProxygenError::PARSE_HEADER, message);
}

#[test]
fn head_parse_trace_is_independent_of_segmentation() {
    for case in TRACE_CASES {
        let expected = one_segment_trace(case).trace;
        for_every_delivery(case, |context, outcome| {
            assert_eq!(outcome.trace, expected, "{context}");
        });
    }
}

#[test]
fn head_parse_traces_match_the_recorded_baseline() {
    let mut two = HttpHeaders::new();
    two.add("X-A", b"one");
    two.add("X-B", b"two");
    for merged in [b"one, X-B: two".as_slice(), b"one\", \"X-B\": \"two"] {
        let mut one = HttpHeaders::new();
        one.add("X-A", merged);
        assert_ne!(
            render_headers(&one),
            render_headers(&two),
            "a rendered trace keeps header boundaries"
        );
    }
    for case in TRACE_CASES {
        assert_eq!(
            one_segment_trace(case).trace,
            golden_trace(case.name),
            "{}",
            case.name
        );
    }
}

fn padded_head(prefix: &[u8], length: usize, suffix: &[u8]) -> Vec<u8> {
    let mut head = prefix.to_vec();
    head.resize(length - suffix.len(), b'a');
    head.extend_from_slice(suffix);
    head
}

/// One segment, the head's last bytes split around the limit, and byte by
/// byte, for a head that starts at `start`.
fn limit_deliveries(start: usize, length: usize) -> Vec<(String, Reads)> {
    let mut deliveries = vec![("one segment".to_owned(), vec![one_segment_read(0..length)])];
    for split in [
        start + 1,
        start + MAX_HEADER_BYTES - 1,
        start + MAX_HEADER_BYTES,
        start + MAX_HEADER_BYTES + 1,
    ] {
        if split < length {
            deliveries.extend(split_deliveries(length, split));
        }
    }
    deliveries.push(byte_by_byte(length));
    deliveries
}

#[test]
fn head_size_limit_fails_at_the_first_byte_past_the_limit() {
    let directions: [(bool, &[u8], &[u8]); 2] = [
        (
            false,
            b"GET /first HTTP/1.1\r\n\r\n",
            b"GET / HTTP/1.1\r\nX-Pad: ",
        ),
        (
            true,
            b"HTTP/1.1 204 No Content\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Pad: ",
        ),
    ];
    for (upstream, first, prefix) in directions {
        let case = TraceCase {
            name: if upstream { "upstream" } else { "downstream" },
            upstream,
            strict: false,
            pause: &[],
            generate: false,
            wire: b"",
        };
        let complete = Bytes::from(padded_head(prefix, MAX_HEADER_BYTES, b"\r\n\r\n"));
        for (delivery, reads) in limit_deliveries(0, complete.len()) {
            let outcome = run_trace(&case, &complete, &reads);
            let context = format!("{} complete at the limit: {delivery}", case.name);
            assert_eq!(outcome.consumed, MAX_HEADER_BYTES, "{context}");
            assert_eq!(
                outcome.events,
                ["begin", "headers", "complete"],
                "{context}"
            );
            let size = outcome.messages[0].ingress_header_size();
            assert_eq!(size.compressed as usize, MAX_HEADER_BYTES, "{context}");
        }

        let past: [(&str, Vec<u8>); 4] = [
            ("token byte", padded_head(prefix, MAX_HEADER_BYTES + 1, b"")),
            (
                "control byte",
                padded_head(prefix, MAX_HEADER_BYTES + 1, b"\x01"),
            ),
            (
                "terminating lf",
                padded_head(prefix, MAX_HEADER_BYTES + 1, b"\r\n\r\n"),
            ),
            (
                "bare terminating lf",
                padded_head(prefix, MAX_HEADER_BYTES + 1, b"\n\n"),
            ),
        ];
        for (byte, head) in past {
            for start in [0, first.len()] {
                let wire = Bytes::from([&first[..start], head.as_slice()].concat());
                for (delivery, reads) in limit_deliveries(start, wire.len()) {
                    let outcome = run_trace(&case, &wire, &reads);
                    let context =
                        format!("{} {byte} past the limit at {start}: {delivery}", case.name);
                    assert_scan_error(&outcome, &context, TOO_LARGE, start + MAX_HEADER_BYTES + 1);
                    let expected: &[&str] = if start == 0 {
                        &["begin", "error"]
                    } else {
                        &["begin", "headers", "complete", "begin", "error"]
                    };
                    assert_eq!(outcome.events, expected, "{context}");
                }
            }
        }
    }
}

#[test]
fn invalid_start_line_fails_at_the_first_byte_that_breaks_the_prefix() {
    let invalid: [(bool, &[u8], usize); 15] = [
        (true, b"XTTP/1.1 200 OK\r\n\r\n", 1),
        (true, b"http/1.1 200 OK\r\n\r\n", 1),
        (true, b"HXTP/1.1 200 OK\r\n\r\n", 2),
        (true, b"HTXP/1.1 200 OK\r\n\r\n", 3),
        (true, b"HTTX/1.1 200 OK\r\n\r\n", 4),
        (true, b"HTTP1.1 200 OK\r\n\r\n", 5),
        (true, b"\r\n\r\nHTTPS/1.1 200 OK\r\n\r\n", 9),
        (false, b"XGET / HTTP/1.1\r\n\r\n", 1),
        (false, b"get / HTTP/1.1\r\n\r\n", 1),
        (false, b"GEX / HTTP/1.1\r\n\r\n", 3),
        (false, b"GETS / HTTP/1.1\r\n\r\n", 4),
        (false, b"\nPOSTX / HTTP/1.1\r\n\r\n", 6),
        (false, b"M-SEARCHX * HTTP/1.1\r\n\r\n", 9),
        (false, b"PROPPATCHED / HTTP/1.1\r\n\r\n", 10),
        (false, b"UNSUBSCRIBEX / HTTP/1.1\r\n\r\n", 12),
    ];
    for (upstream, wire, consumed) in invalid {
        let case = TraceCase {
            name: "invalid start line",
            upstream,
            strict: false,
            pause: &[],
            generate: false,
            wire,
        };
        for_every_delivery(&case, |context, outcome| {
            assert_scan_error(outcome, context, INVALID_START_LINE, consumed);
            assert_eq!(outcome.events, ["begin", "error"], "{context}");
            let expected_status = (!upstream).then_some(400);
            assert_eq!(outcome.errors[0].status, expected_status, "{context}");
        });
    }
    for wire in [
        b"UNSUBSCRIBE / HTTP/1.1\r\n\r\n".as_slice(),
        b"MKACTIVITY / HTTP/1.1\r\n\r\n",
    ] {
        for_every_delivery(&down("longest methods", false, wire), |context, outcome| {
            assert_eq!(
                outcome.events,
                ["begin", "headers", "complete"],
                "{context}"
            );
        });
    }
}

#[test]
fn head_control_byte_fails_at_its_offset() {
    for control in (0..b' ').filter(|byte| !matches!(*byte, b'\t' | b'\n' | b'\r')) {
        let byte = [control];
        let checks: [(bool, Vec<u8>, &[u8], usize); 12] = [
            (
                false,
                [b"GET / HTTP/1.1\r\nX: a".as_slice(), &byte, b"b\r\n\r\n"].concat(),
                INVALID_HEADER_BYTE,
                21,
            ),
            (
                false,
                [b"GET /".as_slice(), &byte, b" HTTP/1.1\r\n\r\n"].concat(),
                INVALID_HEADER_BYTE,
                6,
            ),
            (
                false,
                [b"GET ".as_slice(), &byte, b"/ HTTP/1.1\r\n\r\n"].concat(),
                INVALID_HEADER_BYTE,
                5,
            ),
            (
                false,
                [b" ".as_slice(), &byte, b" / HTTP/1.1\r\n\r\n"].concat(),
                INVALID_HEADER_BYTE,
                2,
            ),
            (
                true,
                [b"HTTP/1.1".as_slice(), &byte, b" 200 OK\r\n\r\n"].concat(),
                INVALID_HEADER_BYTE,
                9,
            ),
            (
                true,
                [b"HTTP/1.1 200 OK\r\nX: ".as_slice(), &byte, b"\r\n\r\n"].concat(),
                INVALID_HEADER_BYTE,
                21,
            ),
            (
                false,
                [byte.as_slice(), b"GET / HTTP/1.1\r\n\r\n"].concat(),
                INVALID_START_LINE,
                1,
            ),
            (
                true,
                [byte.as_slice(), b"HTTP/1.1 200 OK\r\n\r\n"].concat(),
                INVALID_START_LINE,
                1,
            ),
            (
                false,
                [b"G".as_slice(), &byte, b"ET / HTTP/1.1\r\n\r\n"].concat(),
                INVALID_START_LINE,
                2,
            ),
            (
                false,
                [b"GET".as_slice(), &byte, b" / HTTP/1.1\r\n\r\n"].concat(),
                INVALID_START_LINE,
                4,
            ),
            (
                true,
                [b"HT".as_slice(), &byte, b"TP/1.1 200 OK\r\n\r\n"].concat(),
                INVALID_START_LINE,
                3,
            ),
            (
                true,
                [b"HTTP".as_slice(), &byte, b"/1.1 200 OK\r\n\r\n"].concat(),
                INVALID_START_LINE,
                5,
            ),
        ];
        for (upstream, wire, message, consumed) in checks {
            let case = TraceCase {
                name: "control byte",
                upstream,
                strict: false,
                pause: &[],
                generate: false,
                wire: b"",
            };
            let wire = Bytes::from(wire);
            for (delivery, reads) in deliveries(wire.len()) {
                let outcome = run_trace(&case, &wire, &reads);
                let context = format!("{:?} {delivery}", wire.escape_ascii().to_string());
                assert_scan_error(&outcome, &context, message, consumed);
                assert_eq!(outcome.events, ["begin", "error"], "{context}");
            }
        }
    }
    for name in [
        "high bytes lenient",
        "high bytes strict",
        "high bytes in a header value strict",
        "tab delimited request line",
        "response high bytes lenient",
        "response high bytes strict",
    ] {
        let case = trace_case(name);
        for_every_delivery(case, |context, outcome| {
            assert_eq!(outcome.consumed, case.wire.len(), "{context}");
            assert!(
                outcome.errors.iter().all(|error| ![
                    TOO_LARGE,
                    INVALID_START_LINE,
                    INVALID_HEADER_BYTE
                ]
                .contains(&error.message.as_slice())),
                "{context}"
            );
            assert_eq!(outcome.trace, golden_trace(name), "{context}");
        });
    }
}

#[test]
fn head_terminators_escaped_newline_obs_fold_and_http09_are_unchanged() {
    let complete: [(bool, &[u8]); 5] = [
        (false, b"GET / HTTP/1.1\r\nHost: a\r\n\r\n"),
        (false, b"GET / HTTP/1.1\nHost: a\n\n"),
        (false, b"GET / HTTP/1.1\r\nHost: a\r\n\n"),
        (
            true,
            b"HTTP/1.1 200 OK\r\nHost: a\r\nContent-Length: 0\r\n\r\n",
        ),
        (true, b"HTTP/1.1 200 OK\nHost: a\nContent-Length: 0\n\n"),
    ];
    for (upstream, wire) in complete {
        let case = TraceCase {
            name: "terminated head",
            upstream,
            strict: true,
            pause: &[],
            generate: false,
            wire,
        };
        for_every_delivery(&case, |context, outcome| {
            assert_eq!(outcome.consumed, wire.len(), "{context}");
            assert_eq!(
                outcome.events,
                ["begin", "headers", "complete"],
                "{context}"
            );
            assert_eq!(
                header_pairs(&outcome.messages[0])[0],
                (b"Host".to_vec(), b"a".to_vec()),
                "{context}"
            );
        });
    }

    let lenient = trace_case("escaped newline lenient");
    for_every_delivery(lenient, |context, outcome| {
        assert_eq!(
            outcome.events,
            ["begin", "headers", "complete"],
            "{context}"
        );
        assert_eq!(
            header_pairs(&outcome.messages[0]),
            [(b"X-Esc".to_vec(), b"a\\\r\\\nb".to_vec())],
            "{context}"
        );
    });
    let strict = trace_case("escaped newline strict");
    for_every_delivery(strict, |context, outcome| {
        assert_eq!(outcome.consumed, strict.wire.len(), "{context}");
        assert_one_error(
            outcome,
            context,
            ProxygenError::HEADER_CONTENT_VALIDATION,
            b"invalid HTTP header content",
        );
    });

    for_every_delivery(trace_case("obs-fold"), |context, outcome| {
        assert_eq!(
            outcome.events,
            ["begin", "headers", "complete"],
            "{context}"
        );
        assert_eq!(
            header_pairs(&outcome.messages[0]),
            [
                (b"X-Fold".to_vec(), b"a b c".to_vec()),
                (b"Host".to_vec(), b"h".to_vec())
            ],
            "{context}"
        );
    });

    for (wire, consumed) in [
        (b"GET /old\r\n".as_slice(), 10),
        (b"GET /old HTTP/0.9\n", 18),
    ] {
        for_every_delivery(&down("http/0.9", false, wire), |context, outcome| {
            assert_eq!(outcome.consumed, consumed, "{context}");
            assert_eq!(
                outcome.events,
                ["begin", "headers", "complete"],
                "{context}"
            );
            assert_eq!(outcome.messages[0].http_version(), (0, 9), "{context}");
        });
    }
    let unfinished: [(bool, &[u8]); 3] = [
        (false, b"GET /new HTTP/1.1\r\n"),
        (false, b"GET\r\n"),
        (true, b"HTTP/1.1 200 OK\r\n"),
    ];
    for (upstream, wire) in unfinished {
        let case = TraceCase {
            name: "first line only",
            upstream,
            strict: false,
            pause: &[],
            generate: false,
            wire,
        };
        for_every_delivery(&case, |context, outcome| {
            assert_eq!(outcome.consumed, wire.len(), "{context}");
            assert_eq!(outcome.events, ["begin", "error"], "{context}");
            assert_one_error(outcome, context, ProxygenError::EOF, b"unexpected EOF");
        });
    }
}

#[test]
fn message_begin_pause_stops_after_the_first_head_byte() {
    let valid: [(bool, &[u8], &[u8]); 2] = [
        (false, b"\r\nGET / HTTP/1.1\r\n\r\n", b"G"),
        (true, b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", b"H"),
    ];
    for (upstream, wire, first) in valid {
        let case = TraceCase {
            name: "message begin pause",
            upstream,
            strict: false,
            pause: &[PauseAt::MessageBegin],
            generate: false,
            wire,
        };
        let pause_offset = wire.len() - wire.trim_ascii_start().len() + 1;
        let direction = if upstream {
            TransportDirection::Upstream
        } else {
            TransportDirection::Downstream
        };
        let mut codec = Http1xCodec::new(direction);
        if upstream {
            send_trace_request(&mut codec);
        }
        let mut recorder = TraceRecorder::new(&case, Bytes::from_static(wire));
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let first_call = codec.on_ingress(&ingress_queue(wire), &mut recorder, &mut egress);
        assert_eq!(first_call.consumed, pause_offset);
        assert!(codec.is_parser_paused());
        assert_eq!(recorder.events, ["begin"]);
        assert!(matches!(&codec.state, ParseState::Head(head) if head == first));
        codec.set_parser_paused(false);
        recorder.acting = false;
        let resumed = codec.on_ingress(
            &ingress_queue(&wire[pause_offset..]),
            &mut recorder,
            &mut egress,
        );
        assert_eq!(resumed.consumed, wire.len() - pause_offset);
        assert_eq!(recorder.events, ["begin", "headers", "complete"]);

        for_every_delivery(&case, |context, outcome| {
            assert!(
                outcome
                    .trace
                    .contains(&format!("pause MessageBegin at {pause_offset}\n")),
                "{context}"
            );
            assert_eq!(outcome.consumed, wire.len(), "{context}");
            assert_eq!(
                outcome.events,
                ["begin", "headers", "complete"],
                "{context}"
            );
        });
    }

    let invalid: [(bool, &[u8]); 2] = [
        (false, b"XGET / HTTP/1.1\r\n\r\n"),
        (true, b"XTTP/1.1 200 OK\r\n\r\n"),
    ];
    for (upstream, wire) in invalid {
        let case = TraceCase {
            name: "invalid first byte under a message begin pause",
            upstream,
            strict: false,
            pause: &[PauseAt::MessageBegin],
            generate: false,
            wire,
        };
        for_every_delivery(&case, |context, outcome| {
            assert_scan_error(outcome, context, INVALID_START_LINE, 1);
            assert_eq!(outcome.events, ["begin", "error"], "{context}");
        });
    }
}

#[test]
fn failed_single_segment_head_backfills_the_partial_request() {
    let checks: [(&[u8], ProxygenError, &[u8], &str, &[u8], (u8, u8)); 2] = [
        (
            b"GET /path HTTP/1.1\r\nX: \x01\r\n\r\n",
            ProxygenError::PARSE_HEADER,
            INVALID_HEADER_BYTE,
            "GET",
            b"/path",
            (1, 1),
        ),
        (
            b"POST /form HTTP/1.0\r\nContent-Length: abc\r\n\r\n",
            ProxygenError::PARSE_HEADER,
            b"invalid Content-Length",
            "POST",
            b"/form",
            (1, 0),
        ),
    ];
    for (wire, proxygen_error, message, method, url, version) in checks {
        for_every_delivery(&down("backfill", false, wire), |context, outcome| {
            let error = assert_one_error(outcome, context, proxygen_error, message);
            let partial = error.partial.as_ref().expect("a partial message");
            assert_eq!(partial.request_method(), Some(method), "{context}");
            assert_eq!(partial.request_url(), Some(url), "{context}");
            assert_eq!(partial.http_version(), version, "{context}");
        });
    }
}

#[test]
fn eof_inside_a_head_fails_with_the_recorded_partial_message() {
    for name in [
        "eof downstream before lf",
        "eof downstream after lf",
        "eof upstream before lf",
        "eof upstream after lf",
    ] {
        let case = trace_case(name);
        for_every_delivery(case, |context, outcome| {
            assert_eq!(outcome.consumed, case.wire.len(), "{context}");
            assert_eq!(outcome.events, ["begin", "error"], "{context}");
            let error = assert_one_error(outcome, context, ProxygenError::EOF, b"unexpected EOF");
            let partial = error.partial.as_ref().expect("a partial message");
            if name == "eof downstream after lf" {
                assert_eq!(partial.request_method(), Some("GET"), "{context}");
                assert_eq!(partial.request_url(), Some(b"/p".as_slice()), "{context}");
                assert_eq!(partial.http_version(), (1, 1), "{context}");
            } else {
                assert!(!partial.is_request() && !partial.is_response(), "{context}");
            }
            assert_eq!(outcome.trace, golden_trace(name), "{context}");
        });
    }
}

#[test]
fn callback_pauses_stop_and_resume_at_the_recorded_offsets() {
    for case in PAUSE_CASES {
        for_every_delivery(case, |context, outcome| {
            assert_eq!(outcome.trace, golden_trace(case.name), "{context}");
        });
    }
    let second_begin = &PAUSE_CASES[0];
    assert!(
        one_segment_trace(second_begin)
            .trace
            .contains("begin 2\npause MessageBegin at 29\n")
    );
    let malformed = one_segment_trace(&PAUSE_CASES[8]);
    assert_eq!(malformed.errors.len(), 1);
    assert_eq!(malformed.errors[0].status, None);
}

const INVALID_HEADER: &[u8] = b"invalid HTTP header";
const INVALID_HEADER_NAME: &[u8] = b"invalid HTTP header name";
const INVALID_TRAILERS: &[u8] = b"invalid HTTP trailers";
const CONFLICTING_CONTENT_LENGTH: &[u8] = b"conflicting Content-Length headers";
const INVALID_CONTENT_LENGTH: &[u8] = b"invalid Content-Length";

fn head_case(upstream: bool, strict: bool) -> TraceCase {
    TraceCase {
        name: if upstream { "upstream" } else { "downstream" },
        upstream,
        strict,
        pause: &[],
        generate: false,
        wire: b"",
    }
}

fn start_line(upstream: bool) -> &'static [u8] {
    if upstream {
        b"HTTP/1.1 200 OK\r\n"
    } else {
        b"POST /p HTTP/1.1\r\n"
    }
}

fn for_every_wire_delivery(
    case: &TraceCase,
    wire: &[u8],
    mut check: impl FnMut(&str, &TraceOutcome),
) {
    let wire = Bytes::copy_from_slice(wire);
    for (delivery, reads) in deliveries(wire.len()) {
        check(
            &format!(
                "{} strict={} {:?}: {delivery}",
                case.name,
                case.strict,
                wire.escape_ascii().to_string()
            ),
            &run_trace(case, &wire, &reads),
        );
    }
}

#[test]
fn header_line_without_a_colon_fails_as_an_invalid_header() {
    for line in [
        b"a b".as_slice(),
        b"a",
        b"a\tb",
        b"a\"b",
        b"(a)",
        b"a\x7fb",
        b"a\x80b",
        b"\xff",
        b"a b c",
        b"@",
        b"a\\b",
    ] {
        for upstream in [false, true] {
            for strict in [false, true] {
                let wire = [start_line(upstream), b"Host: h\r\n", line, b"\r\n\r\n"].concat();
                for_every_wire_delivery(&head_case(upstream, strict), &wire, |context, outcome| {
                    assert_eq!(outcome.consumed, wire.len(), "{context}");
                    assert_eq!(outcome.events, ["begin", "error"], "{context}");
                    let error = assert_one_error(
                        outcome,
                        context,
                        ProxygenError::PARSE_HEADER,
                        INVALID_HEADER,
                    );
                    assert_eq!(error.status, (!upstream).then_some(400), "{context}");
                    let partial = error.partial.as_ref().expect("a partial message");
                    assert_eq!(
                        header_pairs(partial),
                        [(b"Host".to_vec(), b"h".to_vec())],
                        "{context}"
                    );
                });
            }
        }
    }
}

#[test]
fn empty_or_non_token_header_names_fail_as_invalid_names() {
    for line in [
        b"a b: v".as_slice(),
        b": v",
        b":",
        b"a b:",
        b"a\tb: v",
        b"a\"b: v",
        b"(a): v",
        b"a\x7f: v",
        b"a\x80: v",
        b"\xff: v",
        b"a,b: v",
        b"a/b: v",
        b"a@: v",
        b"[a]: v",
        b"a=b: v",
        b"{a}: v",
        b"a?: v",
        b";: v",
        b"a\\b: v",
        b"a b: c: d",
    ] {
        for upstream in [false, true] {
            for strict in [false, true] {
                let wire = [
                    start_line(upstream),
                    b"Host: h\r\n",
                    line,
                    b"\r\nX: y\r\n\r\n",
                ]
                .concat();
                for_every_wire_delivery(&head_case(upstream, strict), &wire, |context, outcome| {
                    assert_eq!(outcome.consumed, wire.len(), "{context}");
                    assert_eq!(outcome.events, ["begin", "error"], "{context}");
                    let error = assert_one_error(
                        outcome,
                        context,
                        ProxygenError::PARSE_HEADER,
                        INVALID_HEADER_NAME,
                    );
                    assert_eq!(error.status, (!upstream).then_some(400), "{context}");
                    let partial = error.partial.as_ref().expect("a partial message");
                    assert_eq!(
                        header_pairs(partial),
                        [(b"Host".to_vec(), b"h".to_vec())],
                        "{context}"
                    );
                });
            }
        }
    }
    for upstream in [false, true] {
        let wire = [
            start_line(upstream),
            b"!#$%&'*+-.^_`|~09AZaz: v\r\nContent-Length: 0\r\n\r\n",
        ]
        .concat();
        for_every_wire_delivery(&head_case(upstream, true), &wire, |context, outcome| {
            assert_eq!(
                outcome.events,
                ["begin", "headers", "complete"],
                "{context}"
            );
            assert_eq!(
                header_pairs(&outcome.messages[0])[0],
                (b"!#$%&'*+-.^_`|~09AZaz".to_vec(), b"v".to_vec()),
                "{context}"
            );
        });
    }
}

#[derive(Clone, Copy)]
enum TrailerOutcome {
    Body,
    StrictDependent,
    StrictOnly,
    Accepted,
}

#[test]
fn trailer_name_errors_keep_their_kinds() {
    let checks: [(&[u8], TrailerOutcome); 14] = [
        (b"a b", TrailerOutcome::Body),
        (b"a", TrailerOutcome::Body),
        (b"a\x80b", TrailerOutcome::Body),
        (b"a b: v", TrailerOutcome::StrictDependent),
        (b": v", TrailerOutcome::StrictDependent),
        (b"a\x80: v", TrailerOutcome::StrictDependent),
        (b"a\x01: v", TrailerOutcome::StrictDependent),
        (b"(a): v", TrailerOutcome::StrictDependent),
        (b"X: a\x01b", TrailerOutcome::StrictDependent),
        (b"X: a\x7fb", TrailerOutcome::StrictDependent),
        (b"X: a\x0bb", TrailerOutcome::StrictDependent),
        (b"X: \x80", TrailerOutcome::StrictOnly),
        (b"X: a\\\r\\\nb", TrailerOutcome::StrictOnly),
        (b"X: a\tb", TrailerOutcome::Accepted),
    ];
    for (line, expected) in checks {
        for upstream in [false, true] {
            for strict in [false, true] {
                let head: &[u8] = if upstream {
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n"
                } else {
                    b"POST /p HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n"
                };
                let wire = [head, line, b"\r\n\r\n"].concat();
                let kind = match expected {
                    TrailerOutcome::Body => Some(ProxygenError::PARSE_BODY),
                    TrailerOutcome::StrictDependent if strict => {
                        Some(ProxygenError::HEADER_CONTENT_VALIDATION)
                    }
                    TrailerOutcome::StrictDependent => Some(ProxygenError::PARSE_BODY),
                    TrailerOutcome::StrictOnly if strict => {
                        Some(ProxygenError::HEADER_CONTENT_VALIDATION)
                    }
                    TrailerOutcome::StrictOnly | TrailerOutcome::Accepted => None,
                };
                for_every_wire_delivery(&head_case(upstream, strict), &wire, |context, outcome| {
                    assert_eq!(outcome.consumed, wire.len(), "{context}");
                    let Some(kind) = kind else {
                        assert_eq!(
                            outcome.events,
                            ["begin", "headers", "trailers", "complete"],
                            "{context}"
                        );
                        return;
                    };
                    assert_eq!(outcome.events, ["begin", "headers", "error"], "{context}");
                    let error = assert_one_error(outcome, context, kind, INVALID_TRAILERS);
                    assert_eq!(error.status, (!upstream).then_some(400), "{context}");
                });
            }
        }
    }
}

#[test]
fn content_length_errors_keep_their_order() {
    let failing: [(&[u8], &[u8], usize); 10] = [
        (
            b"Content-Length: abc\r\nContent-Length: 5\r\n",
            CONFLICTING_CONTENT_LENGTH,
            2,
        ),
        (
            b"Content-Length: 5\r\nContent-Length: abc\r\n",
            CONFLICTING_CONTENT_LENGTH,
            2,
        ),
        (
            b"Content-Length: 5\r\nContent-Length: 6\r\n",
            CONFLICTING_CONTENT_LENGTH,
            2,
        ),
        (
            b"Content-Length: 5\r\nContent-Length: 5\r\nContent-Length: 6\r\n",
            CONFLICTING_CONTENT_LENGTH,
            3,
        ),
        (
            b"Transfer-Encoding: chunked\r\nContent-Length: 5\r\ncontent-length: 6\r\n",
            CONFLICTING_CONTENT_LENGTH,
            2,
        ),
        (
            b"Content-Length: abc\r\nContent-Length: abc\r\n",
            INVALID_CONTENT_LENGTH,
            0,
        ),
        (b"Content-Length: abc\r\n", INVALID_CONTENT_LENGTH, 0),
        (b"Content-Length: \r\n", INVALID_CONTENT_LENGTH, 0),
        (b"Content-Length: 5, 5\r\n", INVALID_CONTENT_LENGTH, 0),
        (
            b"Content-Length: 18446744073709551616\r\n",
            INVALID_CONTENT_LENGTH,
            0,
        ),
    ];
    for (headers, message, partial_lengths) in failing {
        for upstream in [false, true] {
            let wire = [start_line(upstream), headers, b"\r\n"].concat();
            for_every_wire_delivery(&head_case(upstream, false), &wire, |context, outcome| {
                assert_eq!(outcome.consumed, wire.len(), "{context}");
                assert_eq!(outcome.events, ["begin", "error"], "{context}");
                let error =
                    assert_one_error(outcome, context, ProxygenError::PARSE_HEADER, message);
                assert_eq!(error.status, (!upstream).then_some(400), "{context}");
                let partial = error.partial.as_ref().expect("a partial message");
                assert_eq!(
                    partial
                        .headers()
                        .number_of_values(HttpHeaderCode::ContentLength),
                    partial_lengths,
                    "{context}"
                );
            });
        }
    }

    let accepted: [&[u8]; 3] = [
        b"Content-Length: 5\r\nContent-Length: 5\r\n",
        b"Content-Length: 5\r\nX: a\r\ncontent-length:  5 \r\n",
        b"CONTENT-LENGTH: 05\r\nContent-Length: 05\r\n",
    ];
    for headers in accepted {
        for upstream in [false, true] {
            let wire = [start_line(upstream), headers, b"\r\nhello"].concat();
            for_every_wire_delivery(&head_case(upstream, false), &wire, |context, outcome| {
                assert_eq!(outcome.consumed, wire.len(), "{context}");
                assert_eq!(
                    outcome
                        .events
                        .iter()
                        .filter(|event| **event != "body")
                        .collect::<Vec<_>>(),
                    [&"begin", &"headers", &"complete"],
                    "{context}"
                );
                assert!(outcome.trace.contains("body 1 \"hello\"\n"), "{context}");
            });
        }
    }
}

#[test]
fn parsed_head_reserves_one_entry_per_line_up_to_the_cap() {
    use super::HEAD_RESERVE_CAP;

    for line_end in [b"\r\n".as_slice(), b"\n"] {
        let mut head = [b"HTTP/1.1 200 OK", line_end].concat();
        for index in 0..24 {
            head.extend_from_slice(format!("X-Header-{index}: value").as_bytes());
            head.extend_from_slice(line_end);
        }
        head.extend_from_slice(&[b"Content-Length: 0", line_end, line_end].concat());
        let mut codec = Http1xCodec::new(TransportDirection::Upstream);
        send_trace_request(&mut codec);
        let mut callback = Recorder::default();
        assert_eq!(parse(&mut codec, &mut callback, &head), head.len());
        assert_eq!(callback.events, ["begin", "headers", "complete"]);
        assert_eq!(callback.messages[0].headers().len(), 25);
        assert_eq!(
            codec.head_reserve,
            Some((26, 0)),
            "25 header lines and room for one inferred header"
        );
    }

    let request = b"GET http://example.com/ HTTP/1.1\r\nX-A: 1\r\nX-B: 2\r\n\r\n";
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, request), request.len());
    assert_eq!(
        codec.head_reserve,
        Some((3, 0)),
        "the reservation precedes the inferred Host"
    );
    assert_eq!(
        header_pairs(&callback.messages[0]),
        [
            (b"X-A".to_vec(), b"1".to_vec()),
            (b"X-B".to_vec(), b"2".to_vec()),
            (b"Host".to_vec(), b"example.com".to_vec()),
        ]
    );

    let start = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX: ";
    let folds = (MAX_HEADER_BYTES - start.len() - b"a\r\n\r\n".len()) / b" \r\n".len();
    let mut folded = start.to_vec();
    folded.resize(
        MAX_HEADER_BYTES - folds * b" \r\n".len() - b"\r\n\r\n".len(),
        b'a',
    );
    folded.extend_from_slice(b"\r\n");
    for _ in 0..folds {
        folded.extend_from_slice(b" \r\n");
    }
    folded.extend_from_slice(b"\r\n");
    assert_eq!(folded.len(), MAX_HEADER_BYTES);
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    send_trace_request(&mut codec);
    let mut callback = Recorder::default();
    assert_eq!(parse(&mut codec, &mut callback, &folded), folded.len());
    assert_eq!(callback.events, ["begin", "headers", "complete"]);
    assert_eq!(callback.messages[0].headers().len(), 2);
    assert_eq!(codec.head_reserve, Some((HEAD_RESERVE_CAP, 0)));

    let mut leading_folds = b"HTTP/1.1 200 OK\r\n".to_vec();
    while leading_folds.len() + b" \r\n\r\n".len() <= MAX_HEADER_BYTES {
        leading_folds.extend_from_slice(b" \r\n");
    }
    leading_folds.extend_from_slice(b"\r\n");
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    send_trace_request(&mut codec);
    let mut callback = Recorder::default();
    assert_eq!(
        parse(&mut codec, &mut callback, &leading_folds),
        leading_folds.len()
    );
    assert_eq!(callback.errors.len(), 1);
    assert_eq!(callback.errors[0].0, ProxygenError::PARSE_HEADER);
    assert_eq!(codec.head_reserve, Some((HEAD_RESERVE_CAP, 0)));
}

#[test]
fn chunked_response_trailers_round_trip() {
    const GRPC_MESSAGE: &[u8] = b"upstream request timed out after 15000ms";
    const SECOND_CHUNK: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCD";
    assert_eq!((GRPC_MESSAGE.len(), SECOND_CHUNK.len()), (40, 40));
    let mut upstream_request = Http1xCodec::new(TransportDirection::Upstream);
    let request_stream = upstream_request.create_stream();
    let mut get = request(HttpMethod::Get, b"/", (1, 1));
    get.headers_mut().set(HttpHeaderCode::Host, b"example.com");
    let request_wire = generate(
        &mut upstream_request,
        EgressFrame::Headers {
            stream: request_stream,
            message: &get,
            end_stream: true,
            extra_headers: None,
        },
    );

    let mut downstream = Http1xCodec::new(TransportDirection::Downstream);
    let mut requests = Recorder::default();
    assert_eq!(
        parse(&mut downstream, &mut requests, &request_wire),
        request_wire.len()
    );
    assert_eq!(requests.events, ["begin", "headers", "complete"]);
    let stream = StreamId::new(1);
    let mut message = response(200, (1, 1));
    message.set_chunked(true);
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    message
        .headers_mut()
        .set(HttpHeaderCode::Date, b"Thu, 01 Jan 1970 00:00:00 GMT");
    let mut wire = generate(
        &mut downstream,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    for chunk in [&b"hello"[..], SECOND_CHUNK] {
        wire.extend(generate(
            &mut downstream,
            EgressFrame::Body {
                stream,
                body: BufQueue::copy_from_slice(chunk, chunk.len()).unwrap(),
                padding: None,
                end_stream: false,
            },
        ));
    }
    let mut trailers = HttpHeaders::new();
    trailers.add(b"grpc-status", b"9");
    trailers.add(b"grpc-message", GRPC_MESSAGE);
    wire.extend(generate(
        &mut downstream,
        EgressFrame::Trailers {
            stream,
            trailers: &trailers,
        },
    ));

    let mut body = b"hello".to_vec();
    body.extend_from_slice(SECOND_CHUNK);
    for split in 0..=wire.len() {
        let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
        let upstream_stream = upstream.create_stream();
        assert_eq!(
            generate(
                &mut upstream,
                EgressFrame::Headers {
                    stream: upstream_stream,
                    message: &get,
                    end_stream: true,
                    extra_headers: None,
                },
            ),
            request_wire
        );
        let mut callback = Recorder::default();
        let consumed = parse(&mut upstream, &mut callback, &wire[..split]);
        let mut rest = wire[consumed..split].to_vec();
        rest.extend_from_slice(&wire[split..]);
        assert_eq!(
            parse(&mut upstream, &mut callback, &rest),
            rest.len(),
            "split at {split}"
        );
        assert!(callback.errors.is_empty(), "split at {split}");
        assert_eq!(callback.messages.len(), 1, "split at {split}");
        assert_eq!(callback.messages[0].status_code().unwrap(), 200);
        assert_eq!(callback.bodies, body, "split at {split}");
        assert_eq!(callback.chunk_lengths, [5, 40], "split at {split}");
        let fields: Vec<(Vec<u8>, Vec<u8>)> = callback
            .trailers
            .iter()
            .flat_map(|trailers| trailers.iter())
            .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
            .collect();
        assert_eq!(
            fields,
            [
                (b"grpc-status".to_vec(), b"9".to_vec()),
                (b"grpc-message".to_vec(), GRPC_MESSAGE.to_vec()),
            ],
            "split at {split}"
        );
        assert_eq!(callback.trailers.len(), 1, "split at {split}");
        assert_eq!(
            callback.events[callback.events.len() - 2..],
            ["trailers", "complete"],
            "split at {split}"
        );
        assert_eq!(callback.completions, [false], "split at {split}");
    }
}

/// Parses one downstream request with strict validation off and on, in every
/// delivery the trace cases use, and checks that each run carries `headers`
/// in this order and otherwise parses as one complete request (`connect`: an
/// upgraded CONNECT that leaves the connection unusable).
fn assert_request_headers(
    wire: &'static [u8],
    url: &[u8],
    connect: bool,
    headers: &[(&str, &str)],
) {
    let mut expected = HttpHeaders::new();
    for (name, value) in headers {
        expected.add(*name, value.as_bytes());
    }
    let expected = render_headers(&expected);
    let events: &[&str] = if connect {
        &["begin", "headers", "complete", "complete"]
    } else {
        &["begin", "headers", "complete"]
    };
    for strict in [false, true] {
        let case = down("inferred host", strict, wire);
        let whole = one_segment_trace(&case).trace;
        for_every_delivery(&case, |context, outcome| {
            let context = format!("{context}, strict={strict}");
            assert_eq!(outcome.trace, whole, "{context}");
            assert_eq!(outcome.events, events, "{context}");
            assert_eq!(outcome.consumed, wire.len(), "{context}");
            let [message] = outcome.messages.as_slice() else {
                panic!("{context}: expected one message");
            };
            assert_eq!(render_headers(message.headers()), expected, "{context}");
            assert_eq!(message.request_url(), Some(url), "{context}");
            assert_eq!(message.http_version(), (1, 1), "{context}");
            assert_eq!(
                message.ingress_header_size().compressed as usize,
                wire.len(),
                "{context}"
            );
            assert!(message.wants_keepalive(), "{context}");
            assert_eq!(message.is_upgraded(), connect, "{context}");
            if connect {
                assert!(
                    outcome.trace.contains("\ncomplete 1 upgraded=true\n"),
                    "{context}"
                );
            }
        });
        let mut codec = Http1xCodec::with_options(TransportDirection::Downstream, false, strict);
        let mut callback = Recorder::default();
        assert_eq!(parse(&mut codec, &mut callback, wire), wire.len());
        assert_eq!(codec.is_reusable(), !connect, "strict={strict}");
    }
}

#[test]
fn absolute_form_target_without_a_wire_host_infers_the_last_host() {
    assert_request_headers(
        b"GET http://a/ HTTP/1.1\r\nX-One: 1\r\n\r\n",
        b"http://a/",
        false,
        &[("X-One", "1"), ("Host", "a")],
    );
}

#[test]
fn absolute_form_target_replaces_an_equal_wire_host() {
    assert_request_headers(
        b"GET http://a/ HTTP/1.1\r\nHost: a\r\nX-One: 1\r\n\r\n",
        b"http://a/",
        false,
        &[("X-One", "1"), ("Host", "a")],
    );
}

#[test]
fn absolute_form_target_replaces_different_wire_hosts() {
    assert_request_headers(
        b"GET http://a/ HTTP/1.1\r\nHost: b\r\n\r\n",
        b"http://a/",
        false,
        &[("Host", "a")],
    );
    assert_request_headers(
        b"GET http://a/ HTTP/1.1\r\nHost: b\r\nX-Dup: 1\r\nHost: c\r\nX-Dup: 2\r\n\r\n",
        b"http://a/",
        false,
        &[("X-Dup", "1"), ("X-Dup", "2"), ("Host", "a")],
    );
}

#[test]
fn absolute_form_target_with_a_port_infers_host_and_port() {
    assert_request_headers(
        b"GET http://a:8080/p HTTP/1.1\r\nHost: b\r\n\r\n",
        b"http://a:8080/p",
        false,
        &[("Host", "a:8080")],
    );
}

#[test]
fn connect_authority_without_a_wire_host_infers_the_last_host() {
    assert_request_headers(
        b"CONNECT h:443 HTTP/1.1\r\nX-One: 1\r\n\r\n",
        b"h:443",
        true,
        &[("X-One", "1"), ("Host", "h:443")],
    );
}

#[test]
fn connect_authority_replaces_an_equal_wire_host() {
    assert_request_headers(
        b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nX-One: 1\r\n\r\n",
        b"h:443",
        true,
        &[("X-One", "1"), ("Host", "h:443")],
    );
}

#[test]
fn connect_authority_replaces_a_different_wire_host() {
    assert_request_headers(
        b"CONNECT h:443 HTTP/1.1\r\nX-One: 1\r\nHost: x\r\n\r\n",
        b"h:443",
        true,
        &[("X-One", "1"), ("Host", "h:443")],
    );
}

#[test]
fn content_length_error_precedes_the_inferred_host() {
    let wire: &'static [u8] =
        b"GET http://a/ HTTP/1.1\r\nHost: b\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n";
    for strict in [false, true] {
        let case = down(
            "conflicting content length before the inferred host",
            strict,
            wire,
        );
        let whole = one_segment_trace(&case).trace;
        for_every_delivery(&case, |context, outcome| {
            let context = format!("{context}, strict={strict}");
            assert_eq!(outcome.trace, whole, "{context}");
            assert_eq!(outcome.events, ["begin", "error"], "{context}");
            assert_eq!(outcome.consumed, wire.len(), "{context}");
            let error = assert_one_error(
                outcome,
                &context,
                ProxygenError::PARSE_HEADER,
                CONFLICTING_CONTENT_LENGTH,
            );
            assert_eq!(error.status, Some(400), "{context}");
            let partial = error.partial.as_ref().expect("a partial message");
            assert_eq!(
                render_message(partial),
                "request method=Some(\"GET\") url=\"http://a/\" version=1.1 \
                 headers=[\"Host\": \"b\", \"Content-Length\": \"1\", \"Content-Length\": \"2\"] \
                 size=0/0 upgraded=false websocket=false keepalive=true chunked=false",
                "{context}"
            );
        });
    }
}

#[test]
fn origin_form_target_keeps_every_wire_host() {
    assert_request_headers(
        b"GET /p HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
        b"/p",
        false,
        &[("Host", "a"), ("Host", "b")],
    );
    assert_request_headers(
        b"GET /p HTTP/1.1\r\nHost: a\r\nX-A: 1\r\nHost: a\r\n\r\n",
        b"/p",
        false,
        &[("Host", "a"), ("X-A", "1"), ("Host", "a")],
    );
}

const ORDINARY_WEBSOCKET_HEAD: &[u8] = b"GET /chat HTTP/1.1\r\nHost: example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
const ORDINARY_FOLLOWING_GET: &[u8] = b"GET /later HTTP/1.1\r\nHost: example\r\n\r\n";

/// A downstream codec that parses upgrade and CONNECT requests as ordinary requests.
fn ordinary_upgrade_codec() -> Http1xCodec {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    codec.set_downstream_upgrade_as_ordinary(true);
    codec
}

/// Parses `input` once per delivery (one segment, every two-read split, byte at a time) on a
/// fresh codec from `make`, and hands each run's codec, recorder and consumed count to `check`.
fn for_each_ingress_delivery(
    input: &[u8],
    make: impl Fn() -> Http1xCodec,
    mut check: impl FnMut(&str, &mut Http1xCodec, &mut Recorder, usize),
) {
    let wire = Bytes::copy_from_slice(input);
    for (delivery, reads) in deliveries(wire.len()) {
        let mut codec = make();
        let mut recorder = Recorder::default();
        let mut ingress = BufQueue::new(usize::MAX);
        let mut consumed = 0;
        for read in reads {
            for range in read {
                ingress
                    .push_bytes(wire.slice(range))
                    .expect("an unbounded queue accepts every read");
            }
            let mut egress = BufQueue::new(QUEUE_LIMIT);
            let result = codec.on_ingress(&ingress, &mut recorder, &mut egress);
            ingress
                .trim_start(result.consumed)
                .expect("the codec consumes at most its input");
            consumed += result.consumed;
        }
        check(&delivery, &mut codec, &mut recorder, consumed);
    }
}

fn wire_header_pairs(head: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let head = std::str::from_utf8(head).expect("test heads are ASCII");
    head.split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line
                .split_once(':')
                .expect("test header lines have a colon");
            (name.as_bytes().to_vec(), value.trim().as_bytes().to_vec())
        })
        .collect()
}

/// Generates a 200 for `stream` with an empty body and returns its bytes.
fn generate_empty_ok(codec: &mut Http1xCodec, stream: u64) -> Vec<u8> {
    let mut message = response(200, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    generate(
        codec,
        EgressFrame::Headers {
            stream: StreamId::new(stream),
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    )
}

/// Asserts that a run parsed exactly an ordinary request with `head`'s headers and no body,
/// then the GET behind it, and that empty 200s to both keep the connection reusable.
fn assert_ordinary_request_then_get(
    context: &str,
    codec: &mut Http1xCodec,
    recorder: &Recorder,
    consumed: usize,
    head: &[u8],
    input_len: usize,
) {
    assert_eq!(consumed, input_len, "{context}: every byte is parsed");
    assert!(
        recorder.errors.is_empty(),
        "{context}: {:?}",
        recorder.errors
    );
    assert_eq!(
        recorder.events,
        [
            "begin", "headers", "complete", "begin", "headers", "complete"
        ],
        "{context}"
    );
    assert_eq!(recorder.completions, [false, false], "{context}");
    let mut first = recorder.messages[0].clone();
    assert!(!first.is_upgraded(), "{context}: not marked upgraded");
    assert!(
        !first.is_ingress_websocket_upgrade(),
        "{context}: not marked as an ingress WebSocket upgrade"
    );
    assert_eq!(header_pairs(&first), wire_header_pairs(head), "{context}");
    let start = std::str::from_utf8(head)
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap();
    if first.method().unwrap() != Some(HttpMethod::Connect) {
        assert_eq!(first.url().unwrap(), start.as_bytes(), "{context}");
    }
    let mut second = recorder.messages[1].clone();
    assert_eq!(second.url().unwrap(), b"/later", "{context}");
    let first_wire = generate_empty_ok(codec, 1);
    assert!(
        find_bytes(&first_wire, b"Connection: close").is_none(),
        "{context}: {}",
        first_wire.escape_ascii()
    );
    assert!(
        codec.is_reusable(),
        "{context}: reusable after the first response"
    );
    let second_wire = generate_empty_ok(codec, 2);
    assert!(
        find_bytes(&second_wire, b"Connection: close").is_none(),
        "{context}"
    );
    assert!(
        codec.is_reusable(),
        "{context}: reusable after the second response"
    );
}

#[test]
fn ordinary_upgrade_option_keeps_parsing_http() {
    let input = [ORDINARY_WEBSOCKET_HEAD, ORDINARY_FOLLOWING_GET].concat();
    for_each_ingress_delivery(
        &input,
        ordinary_upgrade_codec,
        |delivery, codec, recorder, consumed| {
            assert_ordinary_request_then_get(
                delivery,
                codec,
                recorder,
                consumed,
                ORDINARY_WEBSOCKET_HEAD,
                input.len(),
            );
        },
    );
}

const ORDINARY_CONNECT_HEAD: &[u8] =
    b"CONNECT h:443 HTTP/1.1\r\nUser-Agent: c1\r\nHost: h:443\r\n\r\n";

/// Generates a 200 with a five-byte body for `stream` and returns its bytes.
fn generate_hello_ok(codec: &mut Http1xCodec, stream: u64) -> Vec<u8> {
    let mut message = response(200, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"5");
    message.headers_mut().set(HttpHeaderCode::Date, b"fixed");
    let mut wire = generate(
        codec,
        EgressFrame::Headers {
            stream: StreamId::new(stream),
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
    );
    let body = BufQueue::from_bytes(Bytes::from_static(b"hello"), 5).unwrap();
    wire.extend(generate(
        codec,
        EgressFrame::Body {
            stream: StreamId::new(stream),
            body,
            padding: None,
            end_stream: true,
        },
    ));
    wire
}

#[test]
fn ordinary_connect_option_keeps_parsing_http() {
    // The authority-form target's inferred Host is the one a default codec gives.
    let mut default_codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut default_recorder = Recorder::default();
    parse(
        &mut default_codec,
        &mut default_recorder,
        ORDINARY_CONNECT_HEAD,
    );
    assert_eq!(
        header_pairs(&default_recorder.messages[0]),
        wire_header_pairs(ORDINARY_CONNECT_HEAD)
    );

    // No Content-Length: no body, and the next bytes are the next request.
    let input = [ORDINARY_CONNECT_HEAD, ORDINARY_FOLLOWING_GET].concat();
    for strict in [false, true] {
        let make = || {
            let mut codec =
                Http1xCodec::with_options(TransportDirection::Downstream, false, strict);
            codec.set_downstream_upgrade_as_ordinary(true);
            codec
        };
        for_each_ingress_delivery(&input, make, |delivery, codec, recorder, consumed| {
            let context = format!("strict {strict}, {delivery}");
            assert_ordinary_request_then_get(
                &context,
                codec,
                recorder,
                consumed,
                ORDINARY_CONNECT_HEAD,
                input.len(),
            );
            let mut first = recorder.messages[0].clone();
            assert_eq!(
                first.method().unwrap(),
                Some(HttpMethod::Connect),
                "{context}"
            );
        });
    }

    // Content-Length: 5: the five bytes are the CONNECT request's body, and a 200 with a body is
    // framed with that body, not as a tunnel.
    let mut reference = Http1xCodec::new(TransportDirection::Downstream);
    parse(
        &mut reference,
        &mut Recorder::default(),
        b"POST / HTTP/1.1\r\nHost: h:443\r\nContent-Length: 5\r\n\r\nabcde",
    );
    let ordinary_wire = generate_hello_ok(&mut reference, 1);
    assert!(ordinary_wire.ends_with(b"\r\n\r\nhello"));
    assert!(find_bytes(&ordinary_wire, b"Content-Length: 5").is_some());
    assert!(find_bytes(&ordinary_wire, b"Connection: close").is_none());
    let head = b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nContent-Length: 5\r\n\r\n";
    let input = [head.as_slice(), b"abcde", ORDINARY_FOLLOWING_GET].concat();
    for_each_ingress_delivery(
        &input,
        ordinary_upgrade_codec,
        |delivery, codec, recorder, consumed| {
            assert_eq!(consumed, input.len(), "{delivery}");
            assert!(recorder.errors.is_empty(), "{delivery}");
            let events: Vec<_> = recorder
                .events
                .iter()
                .filter(|event| **event != "body")
                .copied()
                .collect();
            assert_eq!(
                events,
                [
                    "begin", "headers", "complete", "begin", "headers", "complete"
                ],
                "{delivery}"
            );
            assert_eq!(recorder.bodies, b"abcde", "{delivery}");
            assert_eq!(recorder.completions, [false, false], "{delivery}");
            assert!(!recorder.messages[0].is_upgraded(), "{delivery}");
            let wire = generate_hello_ok(codec, 1);
            assert_eq!(wire, ordinary_wire, "{delivery}: {}", wire.escape_ascii());
            assert!(codec.is_reusable(), "{delivery}");
            let _ = generate_empty_ok(codec, 2);
            assert!(codec.is_reusable(), "{delivery}");
        },
    );
}

/// A default codec, and one that sets the option and clears it before the head.
fn default_upgrade_codecs() -> [(&'static str, Http1xCodec); 2] {
    let mut cleared = Http1xCodec::new(TransportDirection::Downstream);
    cleared.set_downstream_upgrade_as_ordinary(true);
    cleared.set_downstream_upgrade_as_ordinary(false);
    [
        (
            "never set",
            Http1xCodec::new(TransportDirection::Downstream),
        ),
        ("set then cleared", cleared),
    ]
}

/// Generates a refusing 403 for `stream` and returns its bytes.
fn generate_refusal(codec: &mut Http1xCodec, stream: u64) -> Vec<u8> {
    let mut message = response(403, (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    generate(
        codec,
        EgressFrame::Headers {
            stream: StreamId::new(stream),
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    )
}

#[test]
fn default_upgrade_handling_is_unchanged() {
    for (name, mut codec) in default_upgrade_codecs() {
        let mut recorder = Recorder::default();
        let input = [ORDINARY_CONNECT_HEAD, ORDINARY_FOLLOWING_GET].concat();
        let consumed = parse(&mut codec, &mut recorder, &input);
        assert_eq!(
            consumed,
            ORDINARY_CONNECT_HEAD.len(),
            "{name}: nothing behind CONNECT"
        );
        assert_eq!(
            recorder.completions,
            [true],
            "{name}: an upgraded completion"
        );
        assert!(recorder.messages[0].is_upgraded(), "{name}");
        let wire = generate_refusal(&mut codec, 1);
        assert!(find_bytes(&wire, b"Connection: close").is_some(), "{name}");
        assert!(!codec.is_reusable(), "{name}");
    }
    for (name, mut codec) in default_upgrade_codecs() {
        let mut recorder = Recorder::default();
        parse(&mut codec, &mut recorder, ORDINARY_WEBSOCKET_HEAD);
        assert!(recorder.errors.is_empty(), "{name}");
        assert!(
            recorder.messages[0].is_ingress_websocket_upgrade(),
            "{name}"
        );
        let wire = generate_refusal(&mut codec, 1);
        assert!(find_bytes(&wire, b"Connection: close").is_some(), "{name}");
        assert!(!codec.is_reusable(), "{name}");
    }
    for (name, mut codec) in default_upgrade_codecs() {
        let mut recorder = Recorder::default();
        let input = [ORDINARY_WEBSOCKET_HEAD, ORDINARY_WEBSOCKET_HEAD].concat();
        parse(&mut codec, &mut recorder, &input);
        assert_eq!(recorder.errors.len(), 1, "{name}");
        assert_eq!(recorder.errors[0].0, ProxygenError::PARSE_HEADER, "{name}");
        assert_eq!(
            recorder.error_messages,
            [b"duplicate websocket upgrade".to_vec()],
            "{name}"
        );
    }
}

#[test]
fn ordinary_upgrade_option_keeps_parsing_a_non_websocket_upgrade() {
    for head in [
        b"GET / HTTP/1.1\r\nHost: example\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n\r\n".as_slice(),
        b"GET / HTTP/1.1\r\nUpgrade: blarf\r\nConnection: upgrade\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: example\r\nConnection: Upgrade\r\nUpgrade: h2c, websocket\r\n\r\n",
    ] {
        let input = [head, ORDINARY_FOLLOWING_GET].concat();
        for_each_ingress_delivery(
            &input,
            ordinary_upgrade_codec,
            |delivery, codec, recorder, consumed| {
                let context = format!("{}: {delivery}", head.escape_ascii());
                assert_ordinary_request_then_get(
                    &context,
                    codec,
                    recorder,
                    consumed,
                    head,
                    input.len(),
                );
            },
        );
    }
}

#[test]
fn ordinary_upgrade_option_ends_a_chunked_request_at_its_last_chunk() {
    const GET_SHAPED: &[u8] = b"GET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n";
    let mut chunked_body = b"3\r\nabc\r\n".to_vec();
    let mut second_chunk_line = format!("{:x};ext=1\r\n", GET_SHAPED.len()).into_bytes();
    chunked_body.append(&mut second_chunk_line);
    chunked_body.extend_from_slice(GET_SHAPED);
    chunked_body.extend_from_slice(b"\r\n0\r\nX-Trailer: t\r\n\r\n");
    for head in [
        b"GET /chat HTTP/1.1\r\nHost: example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(),
        b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nTransfer-Encoding: chunked\r\n\r\n",
    ] {
        let first_request = [head, chunked_body.as_slice()].concat();
        let input = [first_request.as_slice(), ORDINARY_FOLLOWING_GET].concat();
        let expected_body = [b"abc".as_slice(), GET_SHAPED].concat();
        for_each_ingress_delivery(
            &input,
            ordinary_upgrade_codec,
            |delivery, codec, recorder, consumed| {
                let context = format!("{}: {delivery}", head.escape_ascii());
                assert_eq!(consumed, input.len(), "{context}");
                assert!(recorder.errors.is_empty(), "{context}");
                let events: Vec<_> = recorder
                    .events
                    .iter()
                    .filter(|event| !matches!(**event, "body" | "chunk_header" | "chunk_complete"))
                    .copied()
                    .collect();
                assert_eq!(
                    events,
                    ["begin", "headers", "trailers", "complete", "begin", "headers", "complete"],
                    "{context}"
                );
                assert_eq!(recorder.bodies, expected_body, "{context}");
                assert_eq!(recorder.completions, [false, false], "{context}");
                assert_eq!(
                    recorder.trailers[0].get_single_or_empty("X-Trailer"),
                    b"t",
                    "{context}"
                );
                let mut second = recorder.messages[1].clone();
                assert_eq!(second.url().unwrap(), b"/later", "{context}");
                let wire = generate_empty_ok(codec, 1);
                assert!(find_bytes(&wire, b"Connection: close").is_none(), "{context}");
                assert!(codec.is_reusable(), "{context}");
            },
        );
        // The request completes only once the trailer section's final CRLF is read.
        let mut codec = ordinary_upgrade_codec();
        let mut recorder = Recorder::default();
        let held = first_request.len() - 1;
        assert_eq!(parse(&mut codec, &mut recorder, &first_request[..held]), held);
        assert!(recorder.completions.is_empty(), "{}", head.escape_ascii());
        assert_eq!(
            parse(&mut codec, &mut recorder, &input[held..]),
            input.len() - held
        );
        assert_eq!(recorder.completions, [false, false], "{}", head.escape_ascii());
    }
}

#[test]
fn ordinary_upgrade_option_ends_a_content_length_request_at_its_length() {
    const UPGRADE_LINES: &str = "Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n";
    let head =
        format!("GET /up HTTP/1.1\r\nHost: example\r\n{UPGRADE_LINES}Content-Length: 40\r\n\r\n");
    let ordinary_head = "GET /up HTTP/1.1\r\nHost: example\r\nContent-Length: 40\r\n\r\n";
    let mut body = b"GET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
    body.resize(40, b'p');
    let input = [head.as_bytes(), &body, ORDINARY_FOLLOWING_GET].concat();
    for_each_ingress_delivery(
        &input,
        ordinary_upgrade_codec,
        |delivery, codec, recorder, consumed| {
            assert_eq!(consumed, input.len(), "{delivery}");
            assert!(recorder.errors.is_empty(), "{delivery}");
            let events: Vec<_> = recorder
                .events
                .iter()
                .filter(|event| **event != "body")
                .copied()
                .collect();
            assert_eq!(
                events,
                [
                    "begin", "headers", "complete", "begin", "headers", "complete"
                ],
                "{delivery}"
            );
            assert_eq!(recorder.bodies, body, "{delivery}");
            assert_eq!(recorder.completions, [false, false], "{delivery}");
            let first = &recorder.messages[0];
            assert!(!first.is_upgraded(), "{delivery}");
            assert_eq!(
                header_pairs(first),
                wire_header_pairs(head.as_bytes()),
                "{delivery}"
            );
            let mut second = recorder.messages[1].clone();
            assert_eq!(second.url().unwrap(), b"/later", "{delivery}");
            let wire = generate_empty_ok(codec, 1);
            assert!(
                find_bytes(&wire, b"Connection: close").is_none(),
                "{delivery}"
            );
            assert!(codec.is_reusable(), "{delivery}");
        },
    );
    // The completion comes at the 40th body byte, not before.
    let mut codec = ordinary_upgrade_codec();
    let mut recorder = Recorder::default();
    let held = head.len() + body.len() - 1;
    assert_eq!(parse(&mut codec, &mut recorder, &input[..held]), held);
    assert!(recorder.completions.is_empty());
    assert_eq!(
        parse(&mut codec, &mut recorder, &input[held..]),
        input.len() - held
    );
    assert_eq!(recorder.completions, [false, false]);

    // A premature EOF fails as an ordinary request truncated at the same offset does.
    for received in [0, 1, 20, 39] {
        let truncate = |head: &str| [head.as_bytes(), &body[..received]].concat();
        let mut reference = Http1xCodec::new(TransportDirection::Downstream);
        let mut expected = Recorder::default();
        parse(&mut reference, &mut expected, &truncate(ordinary_head));
        let _ = reference.on_ingress_eof(&mut expected, &mut BufQueue::new(QUEUE_LIMIT));
        assert_eq!(expected.errors.len(), 1, "received {received}");
        assert_eq!(
            expected.errors[0].0,
            ProxygenError::EOF,
            "received {received}"
        );
        let truncated = truncate(&head);
        for_each_ingress_delivery(
            &truncated,
            ordinary_upgrade_codec,
            |delivery, codec, recorder, consumed| {
                let context = format!("received {received}, {delivery}");
                assert_eq!(consumed, truncated.len(), "{context}");
                let _ = codec.on_ingress_eof(recorder, &mut BufQueue::new(QUEUE_LIMIT));
                assert_eq!(recorder.bodies, &body[..received], "{context}");
                assert!(recorder.completions.is_empty(), "{context}");
                assert_eq!(recorder.messages.len(), 1, "{context}");
                assert_eq!(
                    recorder.error_messages, expected.error_messages,
                    "{context}"
                );
                assert_eq!(recorder.errors.len(), 1, "{context}");
                assert_eq!(recorder.errors[0].0, expected.errors[0].0, "{context}");
                assert_eq!(recorder.errors[0].1, expected.errors[0].1, "{context}");
                assert_eq!(
                    recorder.errors[0].2.is_some(),
                    expected.errors[0].2.is_some(),
                    "{context}"
                );
                if let (Some(actual), Some(reference)) =
                    (&recorder.errors[0].2, &expected.errors[0].2)
                {
                    let mut actual = actual.clone();
                    let mut reference = reference.clone();
                    assert_eq!(actual.url().unwrap(), reference.url().unwrap(), "{context}");
                    assert_eq!(
                        header_pairs(&actual),
                        wire_header_pairs(head.as_bytes()),
                        "{context}"
                    );
                    assert_eq!(
                        header_pairs(&reference),
                        wire_header_pairs(ordinary_head.as_bytes()),
                        "{context}"
                    );
                }
            },
        );
        assert_eq!(
            expected.error_messages,
            [b"unexpected EOF".to_vec()],
            "received {received}"
        );
    }
}

#[test]
fn ordinary_upgrade_option_allows_a_second_websocket_request() {
    let input = [ORDINARY_WEBSOCKET_HEAD, ORDINARY_WEBSOCKET_HEAD].concat();
    for_each_ingress_delivery(
        &input,
        ordinary_upgrade_codec,
        |delivery, codec, recorder, consumed| {
            assert_eq!(consumed, input.len(), "{delivery}");
            assert!(
                recorder.errors.is_empty(),
                "{delivery}: {:?}",
                recorder.error_messages
            );
            assert_eq!(recorder.completions, [false, false], "{delivery}");
            for message in &recorder.messages {
                assert!(!message.is_ingress_websocket_upgrade(), "{delivery}");
            }
            let _ = generate_empty_ok(codec, 1);
            let _ = generate_empty_ok(codec, 2);
            assert!(codec.is_reusable(), "{delivery}");
        },
    );
}

#[test]
fn ordinary_upgrade_option_keeps_parse_errors() {
    let cases: [(&[u8], bool); 6] = [
        (
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
            false,
        ),
        (
            b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
            false,
        ),
        (
            b"GET / HTTP/1.1\r\nHost: a\r\nUpgrade: h2c\r\nX-Bad: a\x01b\r\n\r\n",
            false,
        ),
        (
            b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nX-Bad: a\x01b\r\n\r\n",
            false,
        ),
        (b"CONNECT h\xc3\x91:443 HTTP/1.1\r\nHost: h\r\n\r\n", true),
        (
            b"GET / HTTP/1.1\r\nUpgrade: websocket\r\nTransfer-Encoding: gzip\r\n\r\n",
            false,
        ),
    ];
    for (input, strict) in cases {
        let run = |ordinary: bool| {
            let mut codec =
                Http1xCodec::with_options(TransportDirection::Downstream, false, strict);
            codec.set_downstream_upgrade_as_ordinary(ordinary);
            let mut recorder = Recorder::default();
            let consumed = parse(&mut codec, &mut recorder, input);
            (consumed, recorder)
        };
        let context = input.escape_ascii().to_string();
        let (default_consumed, default) = run(false);
        let (consumed, ordinary) = run(true);
        assert_eq!(default.errors.len(), 1, "{context}: the default rejects it");
        assert_eq!(consumed, default_consumed, "{context}");
        assert_eq!(ordinary.events, default.events, "{context}");
        assert_eq!(ordinary.error_messages, default.error_messages, "{context}");
        assert_eq!(ordinary.errors.len(), 1, "{context}");
        assert_eq!(ordinary.errors[0].0, default.errors[0].0, "{context}");
        assert_eq!(ordinary.errors[0].1, default.errors[0].1, "{context}");
        assert_eq!(
            ordinary.errors[0].2.as_ref().map(render_message),
            default.errors[0].2.as_ref().map(render_message),
            "{context}"
        );
    }
}

/// `head` with the method `CONNECT` read as `OPTIONS` and the `Upgrade` header name read as
/// `Xpgrade`: an ordinary request of the same length.
fn same_length_ordinary_head(head: &[u8]) -> Vec<u8> {
    let head = std::str::from_utf8(head).expect("test heads are ASCII");
    head.replacen("CONNECT ", "OPTIONS ", 1)
        .replace("\r\nUpgrade:", "\r\nXpgrade:")
        .into_bytes()
}

/// Parses `input` with a pause at every request's headers, and returns, per pause, the offset
/// and the codec's upgrade resolution for an empty 200, then the final consumed count.
fn header_pause_offsets(
    mut codec: Http1xCodec,
    input: &[u8],
) -> (Vec<(usize, Option<bool>)>, usize, Vec<&'static str>) {
    let mut callback = PauseOnHeaders::default();
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
    let mut consumed = 0;
    let mut pauses = Vec::new();
    for _ in 0..=input.len() {
        let result = codec.on_ingress(
            &ingress_queue(&input[consumed..]),
            &mut callback,
            &mut callback_egress,
        );
        consumed += result.consumed;
        if !codec.is_parser_paused() {
            break;
        }
        pauses.push((
            consumed,
            codec.resolve_downstream_upgrade(&response(200, (1, 1))),
        ));
        codec.set_parser_paused(false);
    }
    (pauses, consumed, callback.recorder.events)
}

#[test]
fn ordinary_upgrade_option_pauses_at_the_recorded_offsets() {
    let mut body_head = b"GET /up HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: h2c\r\nContent-Length: 3\r\n\r\nabc".to_vec();
    body_head.extend_from_slice(ORDINARY_FOLLOWING_GET);
    for input in [
        [ORDINARY_WEBSOCKET_HEAD, ORDINARY_FOLLOWING_GET].concat(),
        [ORDINARY_CONNECT_HEAD, ORDINARY_FOLLOWING_GET].concat(),
        body_head,
    ] {
        let context = input.escape_ascii().to_string();
        let ordinary_input = same_length_ordinary_head(&input);
        assert_eq!(ordinary_input.len(), input.len());
        let expected = header_pause_offsets(
            Http1xCodec::new(TransportDirection::Downstream),
            &ordinary_input,
        );
        assert_eq!(expected.0.len(), 2, "{context}: one pause per request");
        assert_eq!(expected.1, input.len(), "{context}");
        let actual = header_pause_offsets(ordinary_upgrade_codec(), &input);
        assert_eq!(actual, expected, "{context}");

        // An external pause stops parsing before the first byte, and resuming parses it all.
        let mut codec = ordinary_upgrade_codec();
        let mut recorder = Recorder::default();
        codec.set_parser_paused(true);
        assert_eq!(parse(&mut codec, &mut recorder, &input), 0, "{context}");
        assert!(recorder.events.is_empty(), "{context}");
        codec.set_parser_paused(false);
        assert_eq!(
            parse(&mut codec, &mut recorder, &input),
            input.len(),
            "{context}"
        );
        assert_eq!(recorder.completions, [false, false], "{context}");
    }
}

#[test]
fn ordinary_upgrade_option_refuses_an_upgrading_response() {
    let mut switching = response(101, (1, 1));
    switching.headers_mut().set(HttpHeaderCode::Upgrade, b"h2c");
    let mut websocket = response(200, (1, 1));
    websocket.set_egress_websocket_upgrade();
    for head in [
        ORDINARY_WEBSOCKET_HEAD,
        b"GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n",
        ORDINARY_CONNECT_HEAD,
        b"GET / HTTP/1.1\r\nHost: a\r\n\r\n",
    ] {
        for (name, upgrading) in [("101", &switching), ("websocket", &websocket)] {
            let context = format!("{name} to {}", head.escape_ascii());
            let mut codec = ordinary_upgrade_codec();
            let mut recorder = Recorder::default();
            assert_eq!(
                parse(&mut codec, &mut recorder, head),
                head.len(),
                "{context}"
            );
            let mut output = BufQueue::new(QUEUE_LIMIT);
            let error = codec
                .generate(
                    &mut output,
                    EgressFrame::Headers {
                        stream: StreamId::new(1),
                        message: upgrading,
                        end_stream: false,
                        extra_headers: None,
                    },
                    &mut Recorder::default(),
                )
                .expect_err(&context);
            assert_eq!(
                error.code(),
                roxygen_core::error::HttpErrorCode::INVALID_STATE_TRANSITION,
                "{context}"
            );
            assert!(output.is_empty(), "{context}: writes nothing");
            // The refusal leaves the codec as it was: an ordinary response still follows.
            let wire = generate_empty_ok(&mut codec, 1);
            assert!(wire.starts_with(b"HTTP/1.1 200 OK\r\n"), "{context}");
            assert!(
                find_bytes(&wire, b"Connection: close").is_none(),
                "{context}"
            );
            assert!(codec.is_reusable(), "{context}");
        }
    }
}

#[test]
fn ordinary_upgrade_option_resolves_an_upgrading_response_as_refused() {
    let mut switching = response(101, (1, 1));
    switching.headers_mut().set(HttpHeaderCode::Upgrade, b"h2c");
    let mut switching_websocket = response(101, (1, 1));
    switching_websocket.set_egress_websocket_upgrade();
    let mut websocket = response(200, (1, 1));
    websocket.set_egress_websocket_upgrade();
    for head in [
        ORDINARY_WEBSOCKET_HEAD,
        b"GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n",
        ORDINARY_CONNECT_HEAD,
        b"GET / HTTP/1.1\r\nHost: a\r\n\r\n",
    ] {
        for (name, upgrading) in [
            ("101 h2c", &switching),
            ("101 websocket", &switching_websocket),
            ("200 websocket", &websocket),
        ] {
            let context = format!("{name} to {}", head.escape_ascii());
            let input = [head, ORDINARY_FOLLOWING_GET].concat();
            let mut codec = ordinary_upgrade_codec();
            let mut callback = PauseOnHeaders::default();
            let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
            let first =
                codec.on_ingress(&ingress_queue(&input), &mut callback, &mut callback_egress);
            assert_eq!(first.consumed, head.len(), "{context}");
            assert!(codec.is_parser_paused(), "{context}");
            // The bytes behind the request are HTTP/1.1, so the resolution is a refusal.
            assert_eq!(
                codec.resolve_downstream_upgrade(upgrading),
                Some(false),
                "{context}"
            );
            codec.set_parser_paused(false);
            let second = codec.on_ingress(
                &ingress_queue(&input[first.consumed..]),
                &mut callback,
                &mut callback_egress,
            );
            assert_eq!(second.consumed, ORDINARY_FOLLOWING_GET.len(), "{context}");
            let recorder = &callback.recorder;
            assert!(
                recorder.errors.is_empty(),
                "{context}: {:?}",
                recorder.errors
            );
            assert!(recorder.bodies.is_empty(), "{context}: no tunnel body");
            assert_eq!(
                recorder.events,
                ["begin", "headers", "complete", "begin", "headers"],
                "{context}"
            );
            assert_eq!(recorder.completions, [false], "{context}");
            let mut second_message = recorder.messages[1].clone();
            assert_eq!(second_message.url().unwrap(), b"/later", "{context}");
        }
    }
}

#[test]
fn clearing_the_ordinary_upgrade_option_restores_upgrades_for_later_requests() {
    let mut websocket = response(101, (1, 1));
    websocket.set_egress_websocket_upgrade();
    let connect_ok = response(200, (1, 1));
    for (head, accepting) in [
        (ORDINARY_WEBSOCKET_HEAD, &websocket),
        (ORDINARY_CONNECT_HEAD, &connect_ok),
    ] {
        let context = head.escape_ascii().to_string();
        let mut codec = ordinary_upgrade_codec();
        let ordinary = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
        assert_eq!(
            parse(&mut codec, &mut Recorder::default(), ordinary),
            ordinary.len(),
            "{context}"
        );
        generate_empty_ok(&mut codec, 1);
        codec.set_downstream_upgrade_as_ordinary(false);

        let raw = b"raw tunnel bytes";
        let input = [head, raw.as_slice()].concat();
        let mut callback = PauseOnHeaders::default();
        let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
        let first = codec.on_ingress(&ingress_queue(&input), &mut callback, &mut callback_egress);
        assert_eq!(first.consumed, head.len(), "{context}");
        let mut output = BufQueue::new(QUEUE_LIMIT);
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(2),
                    message: accepting,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Recorder::default(),
            )
            .unwrap_or_else(|error| panic!("{context}: the upgrade is generated: {error:?}"));
        assert_eq!(
            codec.resolve_downstream_upgrade(accepting),
            Some(true),
            "{context}"
        );
        codec.set_parser_paused(false);
        let transition = codec.on_ingress(
            &ingress_queue(&input[first.consumed..]),
            &mut callback,
            &mut callback_egress,
        );
        assert_eq!(transition.consumed, 0, "{context}");
        assert_eq!(callback.recorder.completions, [true], "{context}");
        let tunnel = codec.on_ingress(
            &ingress_queue(&input[first.consumed..]),
            &mut callback,
            &mut callback_egress,
        );
        assert_eq!(tunnel.consumed, raw.len(), "{context}");
        assert_eq!(callback.recorder.bodies, raw, "{context}");
    }
}

#[test]
fn clearing_the_ordinary_upgrade_option_still_refuses_an_upgrade_to_an_earlier_request() {
    let mut websocket = response(101, (1, 1));
    websocket.set_egress_websocket_upgrade();
    let mut codec = ordinary_upgrade_codec();
    let mut recorder = Recorder::default();
    assert_eq!(
        parse(&mut codec, &mut recorder, ORDINARY_WEBSOCKET_HEAD),
        ORDINARY_WEBSOCKET_HEAD.len()
    );
    codec.set_downstream_upgrade_as_ordinary(false);
    // A pipelined request parsed before the first response, with the option cleared.
    assert_eq!(
        parse(&mut codec, &mut recorder, ORDINARY_FOLLOWING_GET),
        ORDINARY_FOLLOWING_GET.len()
    );
    assert_eq!(recorder.completions, [false, false]);

    let mut output = BufQueue::new(QUEUE_LIMIT);
    let error = codec
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &websocket,
                end_stream: false,
                extra_headers: None,
            },
            &mut Recorder::default(),
        )
        .expect_err("the first request was parsed as ordinary");
    assert_eq!(
        error.code(),
        roxygen_core::error::HttpErrorCode::INVALID_STATE_TRANSITION
    );
    assert!(output.is_empty(), "writes nothing");
}

#[test]
fn ordinary_upgrade_option_does_not_affect_upstream() {
    // A 2xx to the upstream codec's CONNECT enters the tunnel.
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    codec.set_downstream_upgrade_as_ordinary(true);
    let stream = codec.create_stream();
    let mut connect = HttpMessage::new();
    connect.set_method(HttpMethod::Connect).unwrap();
    connect.set_url(b"h:443", false).unwrap();
    connect.set_http_version(1, 1);
    connect.headers_mut().set(HttpHeaderCode::Host, b"h:443");
    let _ = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &connect,
            end_stream: false,
            extra_headers: None,
        },
    );
    let mut recorder = Recorder::default();
    let accepted = b"HTTP/1.1 200 OK\r\n\r\n";
    assert_eq!(parse(&mut codec, &mut recorder, accepted), accepted.len());
    assert_eq!(recorder.completions, [true]);
    assert!(recorder.messages[0].is_upgraded());
    codec.set_parser_paused(false);
    assert_eq!(parse(&mut codec, &mut recorder, b"tunnel"), b"tunnel".len());
    assert_eq!(recorder.bodies, b"tunnel");

    // A 101 that accepts its WebSocket upgrade enters the tunnel, and a wrong accept key fails.
    for (accept_matches, name) in [(true, "matching key"), (false, "wrong key")] {
        let mut codec = Http1xCodec::new(TransportDirection::Upstream);
        codec.set_downstream_upgrade_as_ordinary(true);
        let stream = codec.create_stream();
        let mut upgrade = request(HttpMethod::Get, b"/websocket", (1, 1));
        upgrade.set_egress_websocket_upgrade();
        let request_wire = generate(
            &mut codec,
            EgressFrame::Headers {
                stream,
                message: &upgrade,
                end_stream: false,
                extra_headers: None,
            },
        );
        let key_start = find_bytes(&request_wire, b"Sec-WebSocket-Key: ").unwrap() + 19;
        let key_end = key_start + find_bytes(&request_wire[key_start..], b"\r\n").unwrap();
        let key = &request_wire[key_start..key_end];
        let accept = if accept_matches {
            String::from_utf8(super::websocket_accept(key)).unwrap()
        } else {
            "d3Jvbmc=".to_owned()
        };
        let response_wire = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        let mut recorder = Recorder::default();
        parse(&mut codec, &mut recorder, response_wire.as_bytes());
        if accept_matches {
            assert!(recorder.errors.is_empty(), "{name}");
            assert!(
                recorder.messages[0].is_ingress_websocket_upgrade(),
                "{name}"
            );
            assert_eq!(recorder.completions, [true], "{name}");
            codec.set_parser_paused(false);
            assert_eq!(parse(&mut codec, &mut recorder, b"frames"), 6, "{name}");
            assert_eq!(recorder.bodies, b"frames", "{name}");
        } else {
            assert_eq!(
                recorder.error_messages,
                [b"invalid websocket accept key".to_vec()],
                "{name}"
            );
        }
    }
}

#[test]
fn ordinary_upgrade_option_keeps_parsing_a_bare_upgrade_header() {
    for head in [
        b"GET / HTTP/1.1\r\nHost: a\r\nUpgrade: h2c\r\n\r\n".as_slice(),
        b"GET /chat HTTP/1.1\r\nHost: a\r\nConnection: keep-alive\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    ] {
        let input = [head, ORDINARY_FOLLOWING_GET].concat();
        for_each_ingress_delivery(
            &input,
            ordinary_upgrade_codec,
            |delivery, codec, recorder, consumed| {
                let context = format!("{}: {delivery}", head.escape_ascii());
                assert_ordinary_request_then_get(
                    &context,
                    codec,
                    recorder,
                    consumed,
                    head,
                    input.len(),
                );
            },
        );
    }
}

#[test]
fn ordinary_upgrade_option_keeps_connection_close() {
    for head in [
        b"GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade, close\r\nUpgrade: h2c\r\n\r\n"
            .as_slice(),
        b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nConnection: close\r\n\r\n",
    ] {
        let context = head.escape_ascii().to_string();
        let mut codec = ordinary_upgrade_codec();
        let mut recorder = Recorder::default();
        assert_eq!(
            parse(&mut codec, &mut recorder, head),
            head.len(),
            "{context}"
        );
        assert_eq!(recorder.completions, [false], "{context}");
        assert!(!recorder.messages[0].wants_keepalive(), "{context}");
        let wire = generate_empty_ok(&mut codec, 1);
        assert!(
            find_bytes(&wire, b"Connection: close").is_some(),
            "{context}"
        );
        assert!(!codec.is_reusable(), "{context}");
    }
}

/// The CONNECT request a downstream consumer's origin reads.
const CONSUMER_CONNECT_WIRE: &[u8] =
    b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nConnection: keep-alive\r\n\r\n";
const UPSTREAM_OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

/// An upstream codec that sends upgrade and CONNECT requests as ordinary requests.
fn upstream_ordinary_codec() -> Http1xCodec {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    codec.set_upstream_upgrade_as_ordinary(true);
    codec
}

/// `CONNECT h:443` with `Host: h:443`.
fn upstream_connect() -> HttpMessage {
    let mut message = request(HttpMethod::Connect, b"h:443", (1, 1));
    message.headers_mut().set(HttpHeaderCode::Host, b"h:443");
    message
}

/// Generates `message`'s head on the codec's next stream and returns its bytes.
fn generate_request(codec: &mut Http1xCodec, message: &HttpMessage, end_stream: bool) -> Vec<u8> {
    let stream = codec.create_stream();
    generate(
        codec,
        EgressFrame::Headers {
            stream,
            message,
            end_stream,
            extra_headers: None,
        },
    )
}

/// An upstream codec with the option set that has sent `message` as a whole request.
fn upstream_ordinary_codec_after(message: &HttpMessage) -> Http1xCodec {
    let mut codec = upstream_ordinary_codec();
    generate_request(&mut codec, message, true);
    codec
}

/// Events other than body and chunk framing events.
fn framing_events(recorder: &Recorder) -> Vec<&'static str> {
    recorder
        .events
        .iter()
        .filter(|event| !matches!(**event, "body" | "chunk_header" | "chunk_complete"))
        .copied()
        .collect()
}

/// Asserts that a run read one ordinary, complete response with `body`, and that the codec
/// stays reusable and reads the 200 to a GET generated next.
fn assert_ordinary_response_then_reuse(
    context: &str,
    codec: &mut Http1xCodec,
    recorder: &Recorder,
    consumed: usize,
    input_len: usize,
    status: u16,
    body: &[u8],
) {
    assert_eq!(consumed, input_len, "{context}: every byte is parsed");
    assert!(
        recorder.errors.is_empty(),
        "{context}: {:?}",
        recorder.error_messages
    );
    assert_eq!(
        framing_events(recorder),
        ["begin", "headers", "complete"],
        "{context}"
    );
    assert_eq!(recorder.completions, [false], "{context}");
    let mut message = recorder.messages[0].clone();
    assert_eq!(message.status_code().unwrap(), status, "{context}");
    assert!(!message.is_upgraded(), "{context}: not upgraded");
    assert_eq!(recorder.bodies, body, "{context}");
    assert!(
        codec.is_reusable(),
        "{context}: reusable after the response"
    );
    let get = request(HttpMethod::Get, b"/next", (1, 1));
    let get_wire = generate_request(codec, &get, true);
    assert!(get_wire.starts_with(b"GET /next HTTP/1.1\r\n"), "{context}");
    let mut next = Recorder::default();
    assert_eq!(
        parse(codec, &mut next, UPSTREAM_OK),
        UPSTREAM_OK.len(),
        "{context}"
    );
    assert!(next.errors.is_empty(), "{context}");
    assert_eq!(next.completions, [false], "{context}");
    assert_eq!(next.bodies, b"ok", "{context}");
    assert!(
        codec.is_reusable(),
        "{context}: reusable after the next response"
    );
}

#[test]
fn upstream_ordinary_connect_reads_a_framed_response() {
    // The CONNECT is written as the default writes it, the consumer's bytes, and leaves the codec
    // reusable after its end of message.
    let mut default_codec = Http1xCodec::new(TransportDirection::Upstream);
    assert_eq!(
        generate_request(&mut default_codec, &upstream_connect(), true),
        CONSUMER_CONNECT_WIRE
    );
    let mut codec = upstream_ordinary_codec();
    let wire = generate_request(&mut codec, &upstream_connect(), true);
    assert_eq!(wire, CONSUMER_CONNECT_WIRE, "{}", wire.escape_ascii());
    assert!(!codec.close_on_egress_complete());
    assert!(codec.is_reusable());
    // A CONNECT with an empty URL still takes its Host as the target.
    let mut empty_url = request(HttpMethod::Connect, b"", (1, 1));
    empty_url.headers_mut().set(HttpHeaderCode::Host, b"h:443");
    let mut default_codec = Http1xCodec::new(TransportDirection::Upstream);
    let default_wire = generate_request(&mut default_codec, &empty_url, true);
    assert!(default_wire.starts_with(b"CONNECT h:443 HTTP/1.1\r\n"));
    assert_eq!(
        generate_request(&mut upstream_ordinary_codec(), &empty_url, true),
        default_wire
    );

    let make = || upstream_ordinary_codec_after(&upstream_connect());
    for (response, status, body) in [
        (UPSTREAM_OK, 200, b"ok".as_slice()),
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n".as_slice(),
            200,
            b"ok",
        ),
        (
            b"HTTP/1.1 403 Forbidden\r\nContent-Length: 5\r\n\r\nnope!",
            403,
            b"nope!",
        ),
    ] {
        for_each_ingress_delivery(response, make, |delivery, codec, recorder, consumed| {
            let context = format!("{}: {delivery}", response.escape_ascii());
            assert_ordinary_response_then_reuse(
                &context,
                codec,
                recorder,
                consumed,
                response.len(),
                status,
                body,
            );
        });
    }

    // The 200 completes at its second body byte, not at its head.
    let mut codec = make();
    let mut recorder = Recorder::default();
    let held = UPSTREAM_OK.len() - 1;
    assert_eq!(parse(&mut codec, &mut recorder, &UPSTREAM_OK[..held]), held);
    assert!(recorder.completions.is_empty());
    assert_eq!(recorder.bodies, b"o");
    assert_eq!(parse(&mut codec, &mut recorder, &UPSTREAM_OK[held..]), 1);
    assert_eq!(recorder.completions, [false]);

    // A response without a length is read to EOF, as any such response is.
    let until_eof = b"HTTP/1.1 200 OK\r\n\r\ntunnel-free bytes";
    for_each_ingress_delivery(until_eof, make, |delivery, codec, recorder, consumed| {
        assert_eq!(consumed, until_eof.len(), "{delivery}");
        assert!(recorder.completions.is_empty(), "{delivery}");
        let _ = codec.on_ingress_eof(recorder, &mut BufQueue::new(QUEUE_LIMIT));
        assert!(recorder.errors.is_empty(), "{delivery}");
        assert_eq!(recorder.completions, [false], "{delivery}");
        assert_eq!(recorder.bodies, b"tunnel-free bytes", "{delivery}");
        assert!(!recorder.messages[0].is_upgraded(), "{delivery}");
    });

    // EOF inside the body fails as the same response to a GET truncated there does.
    let head_len = UPSTREAM_OK.len() - 2;
    for received in [0, 1] {
        let truncated = &UPSTREAM_OK[..head_len + received];
        let mut reference = upstream_ordinary_codec_after(&request(HttpMethod::Get, b"/", (1, 1)));
        let mut expected = Recorder::default();
        parse(&mut reference, &mut expected, truncated);
        let _ = reference.on_ingress_eof(&mut expected, &mut BufQueue::new(QUEUE_LIMIT));
        assert_eq!(
            expected.error_messages,
            [b"unexpected EOF".to_vec()],
            "received {received}"
        );
        for_each_ingress_delivery(truncated, make, |delivery, codec, recorder, consumed| {
            let context = format!("received {received}, {delivery}");
            assert_eq!(consumed, truncated.len(), "{context}");
            let _ = codec.on_ingress_eof(recorder, &mut BufQueue::new(QUEUE_LIMIT));
            assert_eq!(recorder.bodies, &b"ok"[..received], "{context}");
            assert!(recorder.completions.is_empty(), "{context}");
            assert_eq!(
                recorder.error_messages, expected.error_messages,
                "{context}"
            );
            assert_eq!(recorder.errors.len(), 1, "{context}");
            assert_eq!(recorder.errors[0].0, expected.errors[0].0, "{context}");
            assert_eq!(recorder.errors[0].1, expected.errors[0].1, "{context}");
            assert_eq!(
                recorder.errors[0].2.as_ref().map(render_message),
                expected.errors[0].2.as_ref().map(render_message),
                "{context}"
            );
        });
    }
}

/// Generates `message` with `body` and, when chunked, a trailer section, and returns the bytes.
fn generate_request_with_body(
    codec: &mut Http1xCodec,
    message: &HttpMessage,
    body: &'static [u8],
    trailers: bool,
) -> Vec<u8> {
    let stream = codec.create_stream();
    let mut wire = generate(
        codec,
        EgressFrame::Headers {
            stream,
            message,
            end_stream: false,
            extra_headers: None,
        },
    );
    let body = BufQueue::from_bytes(Bytes::from_static(body), body.len()).unwrap();
    wire.extend(generate(
        codec,
        EgressFrame::Body {
            stream,
            body,
            padding: None,
            end_stream: !trailers,
        },
    ));
    if trailers {
        let mut section = HttpHeaders::new();
        section.add(b"X-Trailer", b"t");
        wire.extend(generate(
            codec,
            EgressFrame::Trailers {
                stream,
                trailers: &section,
            },
        ));
    }
    wire
}

/// `message` with its request line's method replaced by GET.
fn as_get(message: &HttpMessage) -> HttpMessage {
    let mut get = message.clone();
    get.set_method(HttpMethod::Get).unwrap();
    get
}

/// The bytes behind the request line.
fn after_request_line(wire: &[u8]) -> &[u8] {
    &wire[find_bytes(wire, b"\r\n").unwrap() + 2..]
}

#[test]
fn upstream_ordinary_connect_frames_its_body() {
    // Content-Length: 5: the head, then exactly the five body bytes.
    let mut length = upstream_connect();
    length
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"5");
    let mut codec = upstream_ordinary_codec();
    let wire = generate_request_with_body(&mut codec, &length, b"hello", false);
    assert_eq!(
        wire,
        b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nhello",
        "{}",
        wire.escape_ascii()
    );
    assert!(!codec.close_on_egress_complete());

    // Chunked: chunk framing, the last chunk and the trailers, as the same message with GET.
    let mut chunked = upstream_connect();
    chunked
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    chunked.set_chunked(true);
    let mut codec = upstream_ordinary_codec();
    let wire = generate_request_with_body(&mut codec, &chunked, b"hello", true);
    assert!(wire.starts_with(b"CONNECT h:443 HTTP/1.1\r\n"));
    let get_wire = generate_request_with_body(
        &mut upstream_ordinary_codec(),
        &as_get(&chunked),
        b"hello",
        true,
    );
    assert_eq!(
        after_request_line(&wire),
        after_request_line(&get_wire),
        "{}",
        wire.escape_ascii()
    );
    assert!(
        wire.ends_with(b"\r\n\r\n5\r\nhello\r\n0\r\nX-Trailer: t\r\n\r\n"),
        "{}",
        wire.escape_ascii()
    );
    assert!(!codec.close_on_egress_complete());
    assert!(codec.is_reusable());

    // Without the option the same CONNECT's body is tunnel bytes, with no chunk framing.
    let default_wire = generate_request_with_body(
        &mut Http1xCodec::new(TransportDirection::Upstream),
        &chunked,
        b"hello",
        false,
    );
    assert!(
        default_wire.ends_with(b"\r\n\r\nhello"),
        "{}",
        default_wire.escape_ascii()
    );
}

/// The WebSocket GET a downstream consumer's origin reads, ending the message at its head.
fn consumer_websocket_get() -> HttpMessage {
    let mut message = request(HttpMethod::Get, b"/ws", (1, 1));
    let headers = message.headers_mut();
    headers.add(b"Host", b"c1");
    headers.add(b"Upgrade", b"websocket");
    headers.add(b"Sec-WebSocket-Key", b"dGhlIHNhbXBsZSBub25jZQ==");
    headers.add(b"sec-websocket-version", b"13");
    headers.add(b"Connection", b"upgrade");
    message
}

/// The bytes a downstream consumer's origin reads for `consumer_websocket_get`.
const CONSUMER_WEBSOCKET_WIRE: &[u8] = b"GET /ws HTTP/1.1\r\nHost: c1\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-version: 13\r\nConnection: upgrade, keep-alive\r\n\r\n";

#[test]
fn upstream_ordinary_response_upgrade_header_is_ordinary() {
    for request_message in [
        consumer_websocket_get(),
        request(HttpMethod::Get, b"/", (1, 1)),
    ] {
        for response in [
            b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\nConnection: upgrade\r\nSec-WebSocket-Accept: d3Jvbmc=\r\nContent-Length: 2\r\n\r\nok".as_slice(),
            b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\nContent-Length: 2\r\n\r\nok",
            b"HTTP/1.1 200 OK\r\nUpgrade: h2c\r\nContent-Length: 2\r\n\r\nok",
        ] {
            let make = || upstream_ordinary_codec_after(&request_message);
            for_each_ingress_delivery(response, make, |delivery, codec, recorder, consumed| {
                let context = format!("{}: {delivery}", response.escape_ascii());
                let message = &recorder.messages.first().cloned();
                assert_ordinary_response_then_reuse(
                    &context,
                    codec,
                    recorder,
                    consumed,
                    response.len(),
                    200,
                    b"ok",
                );
                let message = message.as_ref().expect("a response");
                assert!(!message.is_ingress_websocket_upgrade(), "{context}");
                assert_eq!(
                    header_pairs(message),
                    wire_header_pairs(response),
                    "{context}: every header, the Upgrade header included"
                );
            });
        }
        // The frozen TestIgnoreUpstreamUpgrade response is read to EOF and closes, as without
        // the option.
        let ignored =
            b"HTTP/1.1 200 OK\r\nConnection: close\r\nUpgrade: h2,h2c\r\n\r\n<!DOCTYPE html>";
        let mut default_codec = Http1xCodec::new(TransportDirection::Upstream);
        generate_request(&mut default_codec, &request_message, true);
        let mut expected = Recorder::default();
        parse(&mut default_codec, &mut expected, ignored);
        let _ = default_codec.on_ingress_eof(&mut expected, &mut BufQueue::new(QUEUE_LIMIT));
        let make = || upstream_ordinary_codec_after(&request_message);
        for_each_ingress_delivery(ignored, make, |delivery, codec, recorder, consumed| {
            assert_eq!(consumed, ignored.len(), "{delivery}");
            let _ = codec.on_ingress_eof(recorder, &mut BufQueue::new(QUEUE_LIMIT));
            assert!(recorder.errors.is_empty(), "{delivery}");
            assert_eq!(recorder.bodies, b"<!DOCTYPE html>", "{delivery}");
            assert_eq!(recorder.bodies, expected.bodies, "{delivery}");
            assert_eq!(recorder.completions, [false], "{delivery}");
            assert_eq!(recorder.completions, expected.completions, "{delivery}");
            assert!(!codec.is_reusable(), "{delivery}: Connection: close");
        });
    }
}

/// A GET with `Upgrade: h2c`, ending the message at its head.
fn upstream_h2c_get() -> HttpMessage {
    let mut message = request(HttpMethod::Get, b"/", (1, 1));
    message.headers_mut().add(b"Host", b"a");
    message.headers_mut().add(b"Connection", b"Upgrade");
    message.headers_mut().add(b"Upgrade", b"h2c");
    message
}

/// The message `upstream_ordinary_option_refuses_a_101` expects for a refused 101.
const ORDINARY_101_REFUSAL: &[u8] = b"switching protocols refused for an ordinary request";

#[test]
fn upstream_ordinary_option_refuses_a_101() {
    // The kind and status of the default's `upgrade protocol mismatch`.
    let mut mismatch = Http1xCodec::new(TransportDirection::Upstream);
    generate_request(&mut mismatch, &upstream_h2c_get(), true);
    let mut reference = Recorder::default();
    parse(
        &mut mismatch,
        &mut reference,
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n",
    );
    assert_eq!(
        reference.error_messages,
        [b"upgrade protocol mismatch".to_vec()]
    );
    for (request_message, upgrade) in [
        (consumer_websocket_get(), "websocket"),
        (upstream_h2c_get(), "h2c"),
        (upstream_connect(), "websocket"),
        (request(HttpMethod::Get, b"/", (1, 1)), "websocket"),
    ] {
        let switching = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: {upgrade}\r\nConnection: Upgrade\r\n\r\nraw bytes behind the 101"
        );
        let make = || upstream_ordinary_codec_after(&request_message);
        for_each_ingress_delivery(
            switching.as_bytes(),
            make,
            |delivery, codec, recorder, _consumed| {
                let context = format!("{}: {delivery}", switching.escape_default());
                assert_eq!(recorder.events, ["begin", "error"], "{context}");
                assert_eq!(
                    recorder.error_messages,
                    [ORDINARY_101_REFUSAL.to_vec()],
                    "{context}"
                );
                assert_eq!(recorder.errors[0].0, reference.errors[0].0, "{context}");
                assert_eq!(recorder.errors[0].1, reference.errors[0].1, "{context}");
                assert!(recorder.bodies.is_empty(), "{context}: no tunnel");
                assert!(!codec.is_reusable(), "{context}");
            },
        );
        // A 100 Continue and a 103 before the final 200 are informational, as without the option.
        let interim = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        let mut default_codec = Http1xCodec::new(TransportDirection::Upstream);
        generate_request(
            &mut default_codec,
            &request(HttpMethod::Get, b"/", (1, 1)),
            true,
        );
        let mut expected = Recorder::default();
        assert_eq!(
            parse(&mut default_codec, &mut expected, interim),
            interim.len()
        );
        for_each_ingress_delivery(interim, make, |delivery, codec, recorder, consumed| {
            assert_eq!(consumed, interim.len(), "{delivery}");
            assert!(recorder.errors.is_empty(), "{delivery}");
            assert_eq!(
                framing_events(recorder),
                framing_events(&expected),
                "{delivery}"
            );
            assert_eq!(recorder.completions, expected.completions, "{delivery}");
            let statuses: Vec<_> = recorder
                .messages
                .iter()
                .map(|message| message.clone().status_code().unwrap())
                .collect();
            assert_eq!(statuses, [100, 103, 200], "{delivery}");
            assert_eq!(recorder.bodies, b"ok", "{delivery}");
            assert!(codec.is_reusable(), "{delivery}");
        });
    }
}

/// Asserts that generating `message` on a fresh upstream codec with the option set fails with
/// `INVALID_STATE_TRANSITION` and writes nothing, and that a GET generated next for the same
/// stream is written as on a fresh codec with the option set.
fn assert_upstream_ordinary_refusal(
    context: &str,
    message: &HttpMessage,
    end_stream: bool,
    extra_headers: Option<&HttpHeaders>,
) {
    let mut codec = upstream_ordinary_codec();
    let stream = codec.create_stream();
    let mut output = BufQueue::new(QUEUE_LIMIT);
    let error = codec
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message,
                end_stream,
                extra_headers,
            },
            &mut Recorder::default(),
        )
        .expect_err(context);
    assert_eq!(
        error.code(),
        roxygen_core::error::HttpErrorCode::INVALID_STATE_TRANSITION,
        "{context}"
    );
    assert!(output.is_empty(), "{context}: writes nothing");
    let get = request(HttpMethod::Get, b"/after", (1, 1));
    let after = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &get,
            end_stream: true,
            extra_headers: None,
        },
    );
    let fresh = generate_request(&mut upstream_ordinary_codec(), &get, true);
    assert_eq!(after, fresh, "{context}: the codec's state rolled back");
    assert!(codec.is_reusable(), "{context}");
}

#[test]
fn upstream_ordinary_option_refuses_the_egress_websocket_flag() {
    let mut flagged = request(HttpMethod::Get, b"/websocket", (1, 1));
    flagged.set_egress_websocket_upgrade();
    let mut with_upgrade = flagged.clone();
    with_upgrade
        .headers_mut()
        .set(HttpHeaderCode::Upgrade, b"websocket");
    for (name, message) in [
        ("flag alone", &flagged),
        ("flag with Upgrade", &with_upgrade),
    ] {
        for end_stream in [true, false] {
            assert_upstream_ordinary_refusal(
                &format!("{name}, end_stream {end_stream}"),
                message,
                end_stream,
                None,
            );
        }
    }
}

/// A GET to `/` with `headers`, ending the message at its head.
fn upstream_get_with(headers: &[(&str, &str)]) -> HttpMessage {
    let mut message = request(HttpMethod::Get, b"/", (1, 1));
    for (name, value) in headers {
        message.headers_mut().add(name.as_bytes(), value.as_bytes());
    }
    message
}

#[test]
fn upstream_ordinary_upgrade_request_is_unchanged_and_reusable() {
    let mut consumer = upstream_ordinary_codec();
    assert_eq!(
        generate_request(&mut consumer, &consumer_websocket_get(), true),
        CONSUMER_WEBSOCKET_WIRE
    );
    for message in [
        consumer_websocket_get(),
        upstream_get_with(&[
            ("Host", "a"),
            ("Connection", "Upgrade, HTTP2-Settings"),
            ("Upgrade", "h2c"),
            ("HTTP2-Settings", "AAMAAABkAARAAAAAAAIAAAAA"),
        ]),
        upstream_get_with(&[("Host", "a"), ("Upgrade", "h2c")]),
        upstream_get_with(&[
            ("Host", "a"),
            ("Connection", "Upgrade"),
            ("Upgrade", "h2c, websocket"),
        ]),
    ] {
        let default_wire = generate_request(
            &mut Http1xCodec::new(TransportDirection::Upstream),
            &message,
            true,
        );
        let mut codec = upstream_ordinary_codec();
        let wire = generate_request(&mut codec, &message, true);
        assert_eq!(wire, default_wire, "{}", wire.escape_ascii());
        assert!(
            find_bytes(&wire, b"\r\nUpgrade: ").is_some(),
            "{}",
            wire.escape_ascii()
        );
        let context = wire.escape_ascii().to_string();
        let make = || upstream_ordinary_codec_after(&message);
        for_each_ingress_delivery(UPSTREAM_OK, make, |delivery, codec, recorder, consumed| {
            let context = format!("{context}: {delivery}");
            assert!(recorder.messages[0].wants_keepalive(), "{context}");
            assert_ordinary_response_then_reuse(
                &context,
                codec,
                recorder,
                consumed,
                UPSTREAM_OK.len(),
                200,
                b"ok",
            );
        });
    }
    // The option never forces keep-alive.
    let closing = upstream_get_with(&[
        ("Host", "a"),
        ("Connection", "Upgrade, close"),
        ("Upgrade", "h2c"),
    ]);
    let mut codec = upstream_ordinary_codec();
    let wire = generate_request(&mut codec, &closing, true);
    assert!(
        find_bytes(&wire, b"\r\nConnection: Upgrade, close\r\n").is_some(),
        "{}",
        wire.escape_ascii()
    );
    let mut recorder = Recorder::default();
    assert_eq!(
        parse(&mut codec, &mut recorder, UPSTREAM_OK),
        UPSTREAM_OK.len()
    );
    assert_eq!(recorder.completions, [false]);
    assert!(!codec.is_reusable());
}

/// Headers holding `lines`, for a frame's `extra_headers`.
fn extra(lines: &[(&str, &str)]) -> HttpHeaders {
    let mut headers = HttpHeaders::new();
    for (name, value) in lines {
        headers.add(name.as_bytes(), value.as_bytes());
    }
    headers
}

/// `message` with `lines` added and, with `chunked`, marked chunked.
fn with_lines(message: &HttpMessage, lines: &[(&str, &str)], chunked: bool) -> HttpMessage {
    let mut message = message.clone();
    for (name, value) in lines {
        message.headers_mut().add(name.as_bytes(), value.as_bytes());
    }
    if chunked {
        message.set_chunked(true);
    }
    message
}

/// Generates `message` with `extra_headers` on the codec's next stream, then, unless `body` is
/// `None` (a head that ends the message), one body frame with `body` that ends the message.
fn generate_upstream_exchange(
    codec: &mut Http1xCodec,
    message: &HttpMessage,
    extra_headers: Option<&HttpHeaders>,
    body: Option<&'static [u8]>,
) -> (Vec<u8>, Vec<u8>) {
    let stream = codec.create_stream();
    let head = generate(
        codec,
        EgressFrame::Headers {
            stream,
            message,
            end_stream: body.is_none(),
            extra_headers,
        },
    );
    let Some(body) = body else {
        return (head, Vec::new());
    };
    let queue = BufQueue::from_bytes(Bytes::from_static(body), body.len().max(1)).unwrap();
    let rest = generate(
        codec,
        EgressFrame::Body {
            stream,
            body: queue,
            padding: None,
            end_stream: true,
        },
    );
    (head, rest)
}

/// A case for the framing rule: a name, the message, its extra headers, and whether its head
/// ends the message.
type FramingCase<'a> = (&'a str, HttpMessage, Option<HttpHeaders>, bool);

/// Asserts each case is refused under the option, and generated as today without it.
fn assert_framing_refusals(cases: &[FramingCase<'_>]) {
    for (name, message, extra_headers, end_stream) in cases {
        assert_upstream_ordinary_refusal(name, message, *end_stream, extra_headers.as_ref());
        let mut default_codec = Http1xCodec::new(TransportDirection::Upstream);
        let stream = default_codec.create_stream();
        let mut output = BufQueue::new(QUEUE_LIMIT);
        default_codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message,
                    end_stream: *end_stream,
                    extra_headers: extra_headers.as_ref(),
                },
                &mut Recorder::default(),
            )
            .unwrap_or_else(|error| panic!("{name}: generated without the option: {error:?}"));
        assert!(!output.is_empty(), "{name}");
    }
}

#[test]
fn upstream_ordinary_option_refuses_an_unframed_connect_body() {
    let connect = upstream_connect();
    let mut connect_10 = connect.clone();
    connect_10.set_http_version(1, 0);
    let te = ("Transfer-Encoding", "chunked");
    let gzip = ("Transfer-Encoding", "gzip");
    let cl5 = ("Content-Length", "5");
    let cases: Vec<FramingCase<'_>> = vec![
        // (d): an unframed body.
        ("neither framing header", connect.clone(), None, false),
        (
            "HTTP/1.0, marked chunked",
            with_lines(&connect_10, &[te], true),
            None,
            false,
        ),
        (
            "Transfer-Encoding: chunked, not marked chunked",
            with_lines(&connect, &[te], false),
            None,
            false,
        ),
        (
            "Transfer-Encoding: chunked only in the extra headers, not marked chunked",
            connect.clone(),
            Some(extra(&[te])),
            false,
        ),
        // (a): the Content-Length.
        (
            "two equal Content-Length lines",
            with_lines(&connect, &[cl5, cl5], false),
            None,
            false,
        ),
        (
            "a second Content-Length only in the extra headers",
            with_lines(&connect, &[cl5], false),
            Some(extra(&[cl5])),
            false,
        ),
        (
            "Content-Length: 5, 5",
            with_lines(&connect, &[("Content-Length", "5, 5")], false),
            None,
            false,
        ),
        (
            "Content-Length: +5",
            with_lines(&connect, &[("Content-Length", "+5")], false),
            None,
            false,
        ),
        // (c): both headers.
        (
            "Transfer-Encoding: chunked and Content-Length, not marked chunked",
            with_lines(&connect, &[te, cl5], false),
            None,
            false,
        ),
        (
            "Transfer-Encoding: chunked and Content-Length, marked chunked",
            with_lines(&connect, &[te, cl5], true),
            None,
            false,
        ),
        (
            "marked chunked, Content-Length only in the extra headers",
            with_lines(&connect, &[te], true),
            Some(extra(&[cl5])),
            false,
        ),
        (
            "HTTP/1.0, a written gzip line beside Content-Length",
            with_lines(&connect_10, &[te, gzip, cl5], false),
            None,
            false,
        ),
        (
            "HTTP/1.0, the gzip line only in the extra headers",
            with_lines(&connect_10, &[te, cl5], false),
            Some(extra(&[gzip])),
            false,
        ),
        // A written Transfer-Encoding line needs the codec's chunk framing.
        (
            "HTTP/1.0, a written gzip line on a head that ends the message",
            with_lines(&connect_10, &[te, gzip], false),
            None,
            true,
        ),
        // A head that ends the message declares no Content-Length but 0.
        (
            "Content-Length: 5 on a head that ends the message",
            with_lines(&connect, &[cl5], false),
            None,
            true,
        ),
        (
            "Content-Length: 5 only in the extra headers on a head that ends the message",
            connect.clone(),
            Some(extra(&[cl5])),
            true,
        ),
    ];
    assert_framing_refusals(&cases);

    // Well-formed heads are written, byte for byte as without the option, and leave the
    // codec reusable.
    let accepted: [(&str, HttpMessage, Option<HttpHeaders>, Option<&'static [u8]>, &[u8]); 3] = [
        (
            "Content-Length: 0",
            with_lines(&connect, &[("Content-Length", "0")], false),
            None,
            Some(b""),
            b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n",
        ),
        (
            "marked chunked, Transfer-Encoding: chunked only in the extra headers",
            with_lines(&connect, &[], true),
            Some(extra(&[te])),
            Some(b"hello"),
            b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        ),
        (
            "HTTP/1.0, a dropped chunked line and Content-Length: 2",
            with_lines(&connect_10, &[te, ("Content-Length", "2")], false),
            None,
            Some(b"ok"),
            b"CONNECT h:443 HTTP/1.0\r\nHost: h:443\r\nConnection: keep-alive\r\nContent-Length: 2\r\n\r\nok",
        ),
    ];
    for (name, message, extra_headers, body, expected) in accepted {
        let mut codec = upstream_ordinary_codec();
        let (head, rest) =
            generate_upstream_exchange(&mut codec, &message, extra_headers.as_ref(), body);
        let wire = [head.as_slice(), &rest].concat();
        assert_eq!(wire, expected, "{name}: {}", wire.escape_ascii());
        if body == Some(b"") {
            assert!(rest.is_empty(), "{name}: the end of message adds no byte");
        }
        // As the same message with GET writes it after its request line.
        let (get_head, get_rest) = generate_upstream_exchange(
            &mut Http1xCodec::new(TransportDirection::Upstream),
            &as_get(&message),
            extra_headers.as_ref(),
            body,
        );
        assert_eq!(
            after_request_line(&wire),
            after_request_line(&[get_head.as_slice(), &get_rest].concat()),
            "{name}"
        );
        assert!(codec.is_reusable(), "{name}");
    }
}

#[test]
fn upstream_ordinary_option_refuses_an_unframed_upgrade_body() {
    let websocket = consumer_websocket_get();
    let mut websocket_10 = websocket.clone();
    websocket_10.set_http_version(1, 0);
    let h2c = upstream_get_with(&[("Host", "a"), ("Upgrade", "h2c")]);
    let mut post_h2c_10 = with_lines(
        &request(HttpMethod::Post, b"/", (1, 0)),
        &[("Host", "a"), ("Upgrade", "h2c")],
        false,
    );
    post_h2c_10.set_http_version(1, 0);
    let plain = upstream_get_with(&[("Host", "a")]);
    let te = ("Transfer-Encoding", "chunked");
    let gzip = ("Transfer-Encoding", "gzip");
    let upgrade_h2c = ("Upgrade", "h2c");
    let cl0 = ("Content-Length", "0");
    let cl2 = ("Content-Length", "2");
    let cases: Vec<FramingCase<'_>> = vec![
        // (d): an unframed body.
        (
            "WebSocket GET, neither framing header",
            websocket.clone(),
            None,
            false,
        ),
        ("h2c GET, neither framing header", h2c.clone(), None, false),
        (
            "HTTP/1.0 WebSocket GET, marked chunked",
            with_lines(&websocket_10, &[te], true),
            None,
            false,
        ),
        (
            "HTTP/1.0 POST with Upgrade: h2c",
            post_h2c_10.clone(),
            None,
            false,
        ),
        (
            "Upgrade: h2c only in the extra headers",
            plain.clone(),
            Some(extra(&[upgrade_h2c])),
            false,
        ),
        (
            "Transfer-Encoding: chunked, not marked chunked",
            with_lines(&websocket, &[te], false),
            None,
            false,
        ),
        (
            "Transfer-Encoding: chunked only in the extra headers, not marked chunked",
            websocket.clone(),
            Some(extra(&[te])),
            false,
        ),
        // A written chunked line on a head that ends the message: no last chunk follows.
        (
            "Transfer-Encoding: chunked, not marked chunked, on a head that ends the message",
            with_lines(&websocket, &[te], false),
            None,
            true,
        ),
        // (c).
        (
            "Transfer-Encoding: chunked and Content-Length, not marked chunked",
            with_lines(&websocket, &[te, cl2], false),
            None,
            false,
        ),
        // (b): a second written Transfer-Encoding line.
        (
            "chunked then gzip, marked chunked",
            with_lines(&websocket, &[te, gzip], true),
            None,
            false,
        ),
        // (a) on a head that ends the message.
        (
            "two Content-Length: 0 lines on a head that ends the message",
            with_lines(&websocket, &[cl0, cl0], false),
            None,
            true,
        ),
        // A non-zero Content-Length on a head that ends the message.
        (
            "Content-Length: 2 on a head that ends the message",
            with_lines(&websocket, &[cl2], false),
            None,
            true,
        ),
    ];
    assert_framing_refusals(&cases);

    // Well-formed heads are written byte for byte as without the option, and leave the codec
    // reusable after their end of message.
    let accepted: Vec<(
        &str,
        HttpMessage,
        Option<HttpHeaders>,
        Option<&'static [u8]>,
    )> = vec![
        (
            "WebSocket GET ending at its head",
            websocket.clone(),
            None,
            None,
        ),
        (
            "WebSocket GET, Content-Length: 0",
            with_lines(&websocket, &[cl0], false),
            None,
            Some(b""),
        ),
        (
            "WebSocket GET, Content-Length: 2",
            with_lines(&websocket, &[cl2], false),
            None,
            Some(b"ok"),
        ),
        (
            "WebSocket GET, marked chunked",
            with_lines(&websocket, &[te], true),
            None,
            Some(b"hello"),
        ),
        (
            "HTTP/1.1 POST with Upgrade: h2c",
            with_lines(
                &request(HttpMethod::Post, b"/", (1, 1)),
                &[("Host", "a"), upgrade_h2c],
                false,
            ),
            None,
            Some(b"hello"),
        ),
        (
            "Upgrade: h2c only in the extra headers, Content-Length: 2",
            with_lines(&plain, &[cl2], false),
            Some(extra(&[upgrade_h2c])),
            Some(b"ok"),
        ),
        (
            "Upgrade: h2c and Content-Length: 2 only in the extra headers",
            plain.clone(),
            Some(extra(&[upgrade_h2c, cl2])),
            Some(b"ok"),
        ),
        (
            "WebSocket GET, marked chunked, the chunked line only in the extra headers",
            with_lines(&websocket, &[], true),
            Some(extra(&[te])),
            Some(b"hello"),
        ),
        (
            "WebSocket GET, marked chunked, a dropped gzip line before the chunked one",
            with_lines(&websocket, &[gzip, te], true),
            None,
            Some(b"hello"),
        ),
        (
            "WebSocket GET, marked chunked, ending at its head",
            with_lines(&websocket, &[te], true),
            None,
            None,
        ),
    ];
    for (name, message, extra_headers, body) in accepted {
        let mut codec = upstream_ordinary_codec();
        let (head, rest) =
            generate_upstream_exchange(&mut codec, &message, extra_headers.as_ref(), body);
        let (default_head, default_rest) = generate_upstream_exchange(
            &mut Http1xCodec::new(TransportDirection::Upstream),
            &message,
            extra_headers.as_ref(),
            body,
        );
        let wire = [head.as_slice(), &rest].concat();
        assert_eq!(
            wire,
            [default_head.as_slice(), &default_rest].concat(),
            "{name}: {}",
            wire.escape_ascii()
        );
        assert_eq!(
            find_bytes(&wire, b"Transfer-Encoding").is_some(),
            find_bytes(&wire, b"\r\n0\r\n\r\n").is_some(),
            "{name}: chunked framing under its one Transfer-Encoding line"
        );
        if body == Some(b"") {
            assert!(rest.is_empty(), "{name}: the end of message adds no byte");
        }
        assert!(codec.is_reusable(), "{name}");
    }
}

#[test]
fn clearing_the_upstream_ordinary_option_keeps_an_earlier_request_ordinary() {
    for (request_message, upgrade) in [
        (upstream_connect(), "websocket"),
        (upstream_h2c_get(), "h2c"),
    ] {
        let sent_then_cleared = || {
            let mut codec = upstream_ordinary_codec_after(&request_message);
            codec.set_upstream_upgrade_as_ordinary(false);
            codec
        };
        let context = format!("{:?}", request_message.request_method());
        // A 2xx is still read as a framed ordinary response.
        let mut codec = sent_then_cleared();
        let mut recorder = Recorder::default();
        let consumed = parse(&mut codec, &mut recorder, UPSTREAM_OK);
        assert_eq!(consumed, UPSTREAM_OK.len(), "{context}");
        assert!(recorder.errors.is_empty(), "{context}");
        assert_eq!(recorder.completions, [false], "{context}");
        assert_eq!(recorder.bodies, b"ok", "{context}");
        assert!(codec.is_reusable(), "{context}");
        // A 101 is still refused.
        let mut codec = sent_then_cleared();
        let mut recorder = Recorder::default();
        let switching = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: {upgrade}\r\nConnection: Upgrade\r\n\r\nraw"
        );
        parse(&mut codec, &mut recorder, switching.as_bytes());
        assert_eq!(recorder.events, ["begin", "error"], "{context}");
        assert_eq!(
            recorder.error_messages,
            [ORDINARY_101_REFUSAL.to_vec()],
            "{context}"
        );
        assert!(recorder.bodies.is_empty(), "{context}");
        assert!(!codec.is_reusable(), "{context}");
    }
}

#[test]
fn clearing_the_upstream_ordinary_option_restores_upgrades_for_later_requests() {
    let get = request(HttpMethod::Get, b"/", (1, 1));
    let cleared_after_a_get = || {
        let mut codec = upstream_ordinary_codec_after(&get);
        let mut recorder = Recorder::default();
        assert_eq!(
            parse(&mut codec, &mut recorder, UPSTREAM_OK),
            UPSTREAM_OK.len()
        );
        assert_eq!(recorder.completions, [false]);
        codec.set_upstream_upgrade_as_ordinary(false);
        codec
    };
    // A CONNECT generated next is an egress upgrade whose 2xx starts the tunnel.
    let mut codec = cleared_after_a_get();
    generate_request(&mut codec, &upstream_connect(), true);
    assert!(codec.close_on_egress_complete(), "an egress upgrade");
    let mut recorder = Recorder::default();
    let accepted = b"HTTP/1.1 200 OK\r\n\r\n";
    assert_eq!(parse(&mut codec, &mut recorder, accepted), accepted.len());
    assert_eq!(recorder.completions, [true]);
    assert!(recorder.messages[0].is_upgraded());
    codec.set_parser_paused(false);
    assert_eq!(parse(&mut codec, &mut recorder, b"tunnel"), 6);
    assert_eq!(recorder.bodies, b"tunnel");

    // A request with Upgrade: h2c generated next is accepted by a matching 101.
    let mut codec = cleared_after_a_get();
    generate_request(&mut codec, &upstream_h2c_get(), true);
    let mut recorder = Recorder::default();
    let switching =
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n";
    assert_eq!(parse(&mut codec, &mut recorder, switching), switching.len());
    assert!(recorder.errors.is_empty(), "{:?}", recorder.error_messages);
    assert_eq!(recorder.completions, [true]);
    codec.set_parser_paused(false);
    assert_eq!(parse(&mut codec, &mut recorder, b"frames"), 6);
    assert_eq!(recorder.bodies, b"frames");
    assert!(!codec.is_reusable());
}

/// What a downstream codec did with `input` and a `response` to its first request.
#[derive(Debug, PartialEq)]
struct DownstreamUpgradeRun {
    consumed: usize,
    events: Vec<&'static str>,
    completions: Vec<bool>,
    bodies: Vec<u8>,
    errors: Vec<Vec<u8>>,
    generated: Result<Vec<u8>, roxygen_core::error::HttpErrorCode>,
    reusable: bool,
}

fn downstream_upgrade_run(
    upstream_option: bool,
    downstream_option: bool,
    input: &[u8],
    response: &HttpMessage,
) -> DownstreamUpgradeRun {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    codec.set_upstream_upgrade_as_ordinary(upstream_option);
    codec.set_downstream_upgrade_as_ordinary(downstream_option);
    let mut recorder = Recorder::default();
    let mut consumed = parse(&mut codec, &mut recorder, input);
    let mut output = BufQueue::new(QUEUE_LIMIT);
    let generated = codec
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: response,
                end_stream: false,
                extra_headers: None,
            },
            &mut Recorder::default(),
        )
        .map(|_| queue_bytes(&output))
        .map_err(|error| error.code());
    codec.set_parser_paused(false);
    consumed += parse(&mut codec, &mut recorder, &input[consumed..]);
    DownstreamUpgradeRun {
        consumed,
        events: recorder.events,
        completions: recorder.completions,
        bodies: recorder.bodies,
        errors: recorder.error_messages,
        generated,
        reusable: codec.is_reusable(),
    }
}

#[test]
fn upstream_ordinary_option_does_not_affect_downstream() {
    let mut connect_ok = response(200, (1, 1));
    connect_ok.headers_mut().set(HttpHeaderCode::Date, b"fixed");
    let mut switching = response(101, (1, 1));
    switching.set_egress_websocket_upgrade();
    switching.headers_mut().set(HttpHeaderCode::Date, b"fixed");
    let behind = b"bytes behind the request";
    for (head, accepting) in [
        (ORDINARY_CONNECT_HEAD, &connect_ok),
        (ORDINARY_WEBSOCKET_HEAD, &switching),
    ] {
        let input = [head, behind.as_slice()].concat();
        for downstream_option in [false, true] {
            let expected = downstream_upgrade_run(false, downstream_option, &input, accepting);
            let actual = downstream_upgrade_run(true, downstream_option, &input, accepting);
            assert_eq!(
                actual,
                expected,
                "{} with the downstream option {downstream_option}",
                head.escape_ascii()
            );
        }
    }
}

#[test]
fn upstream_ordinary_option_keeps_response_parse_errors() {
    let responses: [&[u8]; 6] = [
        b"HTTP/1.1 2x0 OK\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nok",
        b"HTTP/1.1 200 OK\r\nX-Bad: a\x01b\r\nContent-Length: 2\r\n\r\nok",
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nX-Bad: a\x01b\r\n\r\n",
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nok\r\n0\r\n\r\n",
    ];
    for response in responses {
        let run = |message: &HttpMessage| {
            let mut codec = upstream_ordinary_codec_after(message);
            let mut recorder = Recorder::default();
            let consumed = parse(&mut codec, &mut recorder, response);
            (consumed, recorder, codec.is_reusable())
        };
        let (expected_consumed, expected, _) = run(&request(HttpMethod::Get, b"/", (1, 1)));
        assert_eq!(expected.errors.len(), 1, "{}", response.escape_ascii());
        for message in [
            upstream_connect(),
            consumer_websocket_get(),
            upstream_h2c_get(),
        ] {
            let context = format!(
                "{} to {:?}",
                response.escape_ascii(),
                message.request_method()
            );
            let (consumed, actual, reusable) = run(&message);
            assert_eq!(consumed, expected_consumed, "{context}");
            assert_eq!(actual.events, expected.events, "{context}");
            assert_eq!(actual.error_messages, expected.error_messages, "{context}");
            assert_eq!(actual.errors.len(), 1, "{context}");
            assert_eq!(actual.errors[0].0, expected.errors[0].0, "{context}");
            assert_eq!(actual.errors[0].1, expected.errors[0].1, "{context}");
            assert_eq!(
                actual.errors[0].2.as_ref().map(render_message),
                expected.errors[0].2.as_ref().map(render_message),
                "{context}"
            );
            assert!(!reusable, "{context}");
        }
    }
}

/// Pauses the parser at each callback whose event name is in `at`.
struct PauseAtEvents {
    recorder: Recorder,
    at: &'static [&'static str],
}

impl HttpCodecCallback for PauseAtEvents {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.recorder.on_event(event);
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let recorded = self.recorder.events.len();
        self.recorder.on_event(event);
        if self.recorder.events[recorded..]
            .iter()
            .any(|name| self.at.contains(name))
        {
            vec![CodecCallbackAction::SetParserPaused(true)].into()
        } else {
            CodecCallbackActions::new()
        }
    }
}

/// Parses `input` with a pause at each event in `at`, resuming after each, and returns the
/// pause offsets, the final consumed count, the events and the body bytes.
fn response_pause_offsets(
    mut codec: Http1xCodec,
    input: &[u8],
    at: &'static [&'static str],
) -> (Vec<usize>, usize, Vec<&'static str>, Vec<u8>) {
    let mut callback = PauseAtEvents {
        recorder: Recorder::default(),
        at,
    };
    let mut callback_egress = BufQueue::new(QUEUE_LIMIT);
    let mut consumed = 0;
    let mut pauses = Vec::new();
    for _ in 0..=2 * input.len() {
        let result = codec.on_ingress(
            &ingress_queue(&input[consumed..]),
            &mut callback,
            &mut callback_egress,
        );
        consumed += result.consumed;
        if !codec.is_parser_paused() {
            break;
        }
        pauses.push(consumed);
        codec.set_parser_paused(false);
    }
    (
        pauses,
        consumed,
        callback.recorder.events,
        callback.recorder.bodies,
    )
}

#[test]
fn upstream_ordinary_option_pauses_at_the_ordinary_offsets() {
    let responses: [&[u8]; 4] = [
        UPSTREAM_OK,
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n3\r\nabc\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\nConnection: upgrade\r\nContent-Length: 2\r\n\r\nok",
        b"HTTP/1.1 403 Forbidden\r\nContent-Length: 3\r\n\r\nno!",
    ];
    let pause_sets: [&'static [&'static str]; 4] = [
        &["headers"],
        &["body"],
        &["complete"],
        &["headers", "body", "complete"],
    ];
    let get = request(HttpMethod::Get, b"/", (1, 1));
    for response in responses {
        for at in pause_sets {
            let mut plain = Http1xCodec::new(TransportDirection::Upstream);
            generate_request(&mut plain, &get, true);
            let expected = response_pause_offsets(plain, response, at);
            assert!(!expected.0.is_empty(), "{}", response.escape_ascii());
            assert_eq!(expected.1, response.len(), "{}", response.escape_ascii());
            for message in [
                upstream_connect(),
                consumer_websocket_get(),
                upstream_h2c_get(),
            ] {
                let context = format!(
                    "{} to {:?} pausing at {at:?}",
                    response.escape_ascii(),
                    message.request_method()
                );
                let actual =
                    response_pause_offsets(upstream_ordinary_codec_after(&message), response, at);
                assert_eq!(actual, expected, "{context}");
            }
        }
        // An external pause stops parsing before the first byte, and resuming parses it all.
        for message in [
            upstream_connect(),
            consumer_websocket_get(),
            upstream_h2c_get(),
        ] {
            let context = format!(
                "{} to {:?}",
                response.escape_ascii(),
                message.request_method()
            );
            let mut codec = upstream_ordinary_codec_after(&message);
            let mut recorder = Recorder::default();
            codec.set_parser_paused(true);
            assert_eq!(parse(&mut codec, &mut recorder, response), 0, "{context}");
            assert!(recorder.events.is_empty(), "{context}");
            codec.set_parser_paused(false);
            assert_eq!(
                parse(&mut codec, &mut recorder, response),
                response.len(),
                "{context}"
            );
            assert_eq!(recorder.completions, [false], "{context}");
            assert!(codec.is_reusable(), "{context}");
        }
    }
}

#[test]
fn default_upstream_upgrade_handling_is_unchanged() {
    let makers: [(&str, fn() -> Http1xCodec); 2] = [
        ("never set", || {
            Http1xCodec::new(TransportDirection::Upstream)
        }),
        ("set then cleared", || {
            let mut codec = Http1xCodec::new(TransportDirection::Upstream);
            codec.set_upstream_upgrade_as_ordinary(true);
            codec.set_upstream_upgrade_as_ordinary(false);
            codec
        }),
    ];
    let get = request(HttpMethod::Get, b"/", (1, 1));
    let mut chunked_connect = upstream_connect();
    chunked_connect
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    chunked_connect.set_chunked(true);
    let mut flagged = request(HttpMethod::Get, b"/websocket", (1, 1));
    flagged.set_egress_websocket_upgrade();
    for (name, make) in makers {
        // A chunked CONNECT's body is tunnel bytes, and the connection closes after it.
        let mut codec = make();
        let wire = generate_request_with_body(&mut codec, &chunked_connect, b"hello", false);
        assert!(
            wire.ends_with(b"\r\n\r\nhello"),
            "{name}: {}",
            wire.escape_ascii()
        );
        assert!(codec.close_on_egress_complete(), "{name}");

        // A 2xx to a CONNECT is upgraded, and the bytes behind it are tunnel body.
        let mut codec = make();
        generate_request(&mut codec, &upstream_connect(), true);
        let mut recorder = Recorder::default();
        let accepted = b"HTTP/1.1 200 OK\r\n\r\n";
        assert_eq!(
            parse(&mut codec, &mut recorder, accepted),
            accepted.len(),
            "{name}"
        );
        assert_eq!(recorder.completions, [true], "{name}");
        assert!(recorder.messages[0].is_upgraded(), "{name}");
        codec.set_parser_paused(false);
        assert_eq!(parse(&mut codec, &mut recorder, b"tunnel"), 6, "{name}");
        assert_eq!(recorder.bodies, b"tunnel", "{name}");

        // A matching 101 to an h2c request is accepted; a websocket 101 is a mismatch.
        let mut codec = make();
        generate_request(&mut codec, &upstream_h2c_get(), true);
        let mut recorder = Recorder::default();
        let switching =
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n";
        assert_eq!(
            parse(&mut codec, &mut recorder, switching),
            switching.len(),
            "{name}"
        );
        assert!(
            recorder.errors.is_empty(),
            "{name}: {:?}",
            recorder.error_messages
        );
        assert_eq!(recorder.completions, [true], "{name}");
        let mut codec = make();
        generate_request(&mut codec, &upstream_h2c_get(), true);
        let mut recorder = Recorder::default();
        parse(
            &mut codec,
            &mut recorder,
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
        );
        assert_eq!(
            recorder.error_messages,
            [b"upgrade protocol mismatch".to_vec()],
            "{name}"
        );

        // A 200 with an Upgrade header ends keep-alive.
        let mut codec = make();
        generate_request(&mut codec, &get, true);
        let mut recorder = Recorder::default();
        let upgrade_ok = b"HTTP/1.1 200 OK\r\nUpgrade: h2c\r\nContent-Length: 2\r\n\r\nok";
        assert_eq!(
            parse(&mut codec, &mut recorder, upgrade_ok),
            upgrade_ok.len(),
            "{name}"
        );
        assert_eq!(recorder.completions, [false], "{name}");
        assert_eq!(recorder.bodies, b"ok", "{name}");
        assert!(!codec.is_reusable(), "{name}");

        // A 200 with Upgrade: websocket is marked as a websocket upgrade.
        let mut codec = make();
        generate_request(&mut codec, &get, true);
        let mut recorder = Recorder::default();
        parse(
            &mut codec,
            &mut recorder,
            b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\nContent-Length: 2\r\n\r\nok",
        );
        assert!(
            recorder.errors.is_empty(),
            "{name}: {:?}",
            recorder.error_messages
        );
        assert!(
            recorder.messages[0].is_ingress_websocket_upgrade(),
            "{name}"
        );

        // The egress flag writes the handshake, and a wrong accept key fails its 101.
        let mut codec = make();
        let wire = generate_request(&mut codec, &flagged, false);
        assert!(
            find_bytes(&wire, b"\r\nUpgrade: websocket\r\n").is_some(),
            "{name}: {}",
            wire.escape_ascii()
        );
        assert!(
            find_bytes(&wire, b"\r\nSec-WebSocket-Key: ").is_some(),
            "{name}: {}",
            wire.escape_ascii()
        );
        let mut recorder = Recorder::default();
        parse(
            &mut codec,
            &mut recorder,
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: wrong\r\n\r\n",
        );
        assert_eq!(
            recorder.error_messages,
            [b"invalid websocket accept key".to_vec()],
            "{name}"
        );
    }
}

#[test]
fn generated_head_keeps_uncommon_spelling() {
    // Names outside the common-header table are written as the message spells them.
    let mut message = request(HttpMethod::Get, b"/", (1, 1));
    message.headers_mut().add(HttpHeaderCode::Host, b"a");
    message.headers_mut().add("expect", b"100-continue");
    message.headers_mut().add("x-forwarded-proto", b"https");
    message.headers_mut().add("link", b"<a>");
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    let wire = generate_request(&mut codec, &message, true);
    assert_eq!(
        wire,
        b"GET / HTTP/1.1\r\nHost: a\r\nexpect: 100-continue\r\nx-forwarded-proto: https\r\nlink: <a>\r\nConnection: keep-alive\r\n\r\n",
        "{}",
        wire.escape_ascii()
    );
}

// Chunked ingress oracles (`repair.http1_chunked_ingress`). The golden traces
// were recorded from the codec at `62d2959bb6d3`, whose chunk-size and
// chunk-terminator states pushed each byte into a vector.

const CHUNKED_REQUEST_HEAD: &[u8] = b"POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
const CHUNKED_RESPONSE_HEAD: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
const CHUNKED_POST_WITH_EXTENSION_THEN_GET: &[u8] =
    b"3\r\nabc\r\n2;x\r\nde\r\n0\r\n\r\nGET /g HTTP/1.1\r\n\r\n";
const CHUNK_ERRORS: &[&str] = &[
    "HTTP chunk header is too large",
    "invalid HTTP chunk delimiter",
    "invalid HTTP chunk length",
    "HTTP chunk length overflow",
    "invalid HTTP chunk terminator",
];

/// A chunked message's head followed by `body`. [`CHUNKED_TRACE_CASES`] keeps
/// every case, so each leaked wire stays reachable for the whole test run.
fn chunked_case(
    name: &'static str,
    upstream: bool,
    pause: &'static [PauseAt],
    body: &[u8],
) -> TraceCase {
    let head = if upstream {
        CHUNKED_RESPONSE_HEAD
    } else {
        CHUNKED_REQUEST_HEAD
    };
    TraceCase {
        name,
        upstream,
        strict: false,
        pause,
        generate: false,
        wire: [head, body].concat().leak(),
    }
}

/// A chunk-size line of `length` bytes: size 1, then an extension padded to
/// end with `terminator`.
fn padded_chunk_line(length: usize, terminator: &[u8]) -> Vec<u8> {
    let mut line = b"1;".to_vec();
    line.resize(length - terminator.len(), b'a');
    line.extend_from_slice(terminator);
    line
}

static CHUNKED_TRACE_CASES: LazyLock<Vec<TraceCase>> = LazyLock::new(chunked_trace_cases);

fn chunked_trace_cases() -> Vec<TraceCase> {
    let down = |name, body: &[u8]| chunked_case(name, false, &[], body);
    let up = |name, body: &[u8]| chunked_case(name, true, &[], body);
    vec![
        down("crlf chunks", b"5\r\nhello\r\n0\r\n\r\n"),
        down("bare lf chunks", b"5\nhello\n0\n\n"),
        down(
            "chunk extensions",
            b"5;name=value\r\nhello\r\n3 ; a=b;c\r\nabc\r\n0;last\r\n\r\n",
        ),
        down(
            "whitespace around chunk sizes",
            b" 5 \t\r\nhello\r\n\t3\t;x\r\nabc\r\n0 \r\n\r\n",
        ),
        down("trailing space before a bare lf", b"5 \nhello\n0\n\n"),
        down(
            "hex digits of either case after leading zeros",
            b"0000000000000000000000000001a\r\nabcdefghijklmnopqrstuvwxyz\r\nA\r\n0123456789\r\n00\r\n\r\n",
        ),
        down("largest chunk length", b"7fffffffffffffff\r\nab"),
        down(
            "largest chunk length after leading zeros",
            b"00000000000000000007FFFFFFFFFFFFFFF\r\nab",
        ),
        down("chunk length past i64", b"8000000000000000\r\nab"),
        down("chunk length of u64 max", b"ffffffffffffffff\r\n"),
        down("chunk length past u64", b"10000000000000000\r\n"),
        down(
            "chunk length past u64 before an extension",
            b"10000000000000000;x\r\n",
        ),
        down("invalid chunk length digit", b"5x\r\nhello\r\n"),
        down("space inside a chunk length", b"5 5\r\n"),
        down("empty chunk length", b"\r\n"),
        down("whitespace-only chunk length", b" \t \r\n"),
        down("extension without a chunk length", b";ext\r\n"),
        down("0x chunk length", b"0x5\r\n"),
        down("signed chunk length", b"+5\r\n"),
        down("vertical tab after a chunk length", b"5\x0b\r\n"),
        down("high byte after a chunk length", b"5\xff\r\n"),
        down("cr cr lf after a chunk length", b"5\r\r\nhello\r\n"),
        down("cr inside a chunk length", b"5\rx\n"),
        down("cr before a chunk length", b"\r5\r\n"),
        down("cr inside a chunk extension", b"5;a\rb\r\n"),
        down("cr before a chunk extension", b"5\r;x\r\n"),
        down("stray cr before an invalid length", b"zz\rq\r\n"),
        down(
            "stray cr before a length overflow",
            b"8000000000000000\r\r\n",
        ),
        down(
            "chunk header at the size limit",
            &[padded_chunk_line(128, b"\r\n").as_slice(), b"a\r\n0\r\n\r\n"].concat(),
        ),
        down(
            "bare lf chunk header at the size limit",
            &[padded_chunk_line(128, b"\n").as_slice(), b"a\r\n0\r\n\r\n"].concat(),
        ),
        down(
            "chunk header lf past the size limit",
            &[padded_chunk_line(129, b"\r\n").as_slice(), b"a\r\n"].concat(),
        ),
        down(
            "chunk header byte past the size limit",
            &padded_chunk_line(129, b"a"),
        ),
        down("invalid terminator byte", b"1\r\naX"),
        down("cr then an invalid terminator byte", b"1\r\na\rX"),
        down("cr cr terminator", b"1\r\na\r\r\n"),
        down("bare lf terminator", b"1\r\na\n0\r\n\r\n"),
        down("lf then cr lf after a chunk", b"1\r\na\n\r\n"),
        down("eof inside a chunk length", b"5"),
        down("eof after a chunk length cr", b"5\r"),
        down("eof inside a chunk extension", b"5;ext"),
        down("eof before a chunk terminator", b"1\r\na"),
        down("eof inside a chunk terminator", b"1\r\na\r"),
        down(
            "last chunk extension and trailers",
            b"0;ext=1\r\nX-T: 1\r\n\r\n",
        ),
        down(
            "chunked request then a get",
            CHUNKED_POST_WITH_EXTENSION_THEN_GET,
        ),
        up("chunked response with an extension", b"3;x\r\nabc\r\n0\r\n\r\n"),
        up("chunked response with an invalid length", b"g\r\n"),
        chunked_case(
            "paused at chunk headers",
            false,
            &[PauseAt::ChunkHeader],
            CHUNKED_POST_WITH_EXTENSION_THEN_GET,
        ),
        chunked_case(
            "paused at chunk completions",
            false,
            &[PauseAt::ChunkComplete],
            CHUNKED_POST_WITH_EXTENSION_THEN_GET,
        ),
    ]
}

const CHUNKED_GOLDEN_TRACES: &[(&str, &str)] = &[
    (
        "crlf chunks",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 5
body 1 "hello"
chunk_complete 1
complete 1 upgraded=false
consumed 63
eof
"##,
    ),
    (
        "bare lf chunks",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 5
body 1 "hello"
chunk_complete 1
complete 1 upgraded=false
consumed 59
eof
"##,
    ),
    (
        "chunk extensions",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 5
body 1 "hello"
chunk_complete 1
chunk_header 1 3
body 1 "abc"
chunk_complete 1
complete 1 upgraded=false
consumed 95
eof
"##,
    ),
    (
        "whitespace around chunk sizes",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 5
body 1 "hello"
chunk_complete 1
chunk_header 1 3
body 1 "abc"
chunk_complete 1
complete 1 upgraded=false
consumed 79
eof
"##,
    ),
    (
        "trailing space before a bare lf",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 5
body 1 "hello"
chunk_complete 1
complete 1 upgraded=false
consumed 60
eof
"##,
    ),
    (
        "hex digits of either case after leading zeros",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 26
body 1 "abcdefghijklmnopqrstuvwxyz"
chunk_complete 1
chunk_header 1 10
body 1 "0123456789"
chunk_complete 1
complete 1 upgraded=false
consumed 128
eof
"##,
    ),
    (
        "largest chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 9223372036854775807
body 1 "ab"
consumed 68
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=none
"##,
    ),
    (
        "largest chunk length after leading zeros",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 9223372036854775807
body 1 "ab"
consumed 87
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=none
"##,
    ),
    (
        "chunk length past i64",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "HTTP chunk length overflow" status=Some(400) new_stream=false partial=none
consumed 66
eof
"##,
    ),
    (
        "chunk length of u64 max",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "HTTP chunk length overflow" status=Some(400) new_stream=false partial=none
consumed 66
eof
"##,
    ),
    (
        "chunk length past u64",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 67
eof
"##,
    ),
    (
        "chunk length past u64 before an extension",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 69
eof
"##,
    ),
    (
        "invalid chunk length digit",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 52
eof
"##,
    ),
    (
        "space inside a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 53
eof
"##,
    ),
    (
        "empty chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 50
eof
"##,
    ),
    (
        "whitespace-only chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 53
eof
"##,
    ),
    (
        "extension without a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 54
eof
"##,
    ),
    (
        "0x chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 53
eof
"##,
    ),
    (
        "signed chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 52
eof
"##,
    ),
    (
        "vertical tab after a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 52
eof
"##,
    ),
    (
        "high byte after a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 52
eof
"##,
    ),
    (
        "cr cr lf after a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk delimiter" status=Some(400) new_stream=false partial=none
consumed 52
eof
"##,
    ),
    (
        "cr inside a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk delimiter" status=Some(400) new_stream=false partial=none
consumed 52
eof
"##,
    ),
    (
        "cr before a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk delimiter" status=Some(400) new_stream=false partial=none
consumed 52
eof
"##,
    ),
    (
        "cr inside a chunk extension",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk delimiter" status=Some(400) new_stream=false partial=none
consumed 55
eof
"##,
    ),
    (
        "cr before a chunk extension",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk delimiter" status=Some(400) new_stream=false partial=none
consumed 54
eof
"##,
    ),
    (
        "stray cr before an invalid length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk delimiter" status=Some(400) new_stream=false partial=none
consumed 54
eof
"##,
    ),
    (
        "stray cr before a length overflow",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk delimiter" status=Some(400) new_stream=false partial=none
consumed 67
eof
"##,
    ),
    (
        "chunk header at the size limit",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
chunk_complete 1
complete 1 upgraded=false
consumed 184
eof
"##,
    ),
    (
        "bare lf chunk header at the size limit",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
chunk_complete 1
complete 1 upgraded=false
consumed 184
eof
"##,
    ),
    (
        "chunk header lf past the size limit",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "HTTP chunk header is too large" status=Some(400) new_stream=false partial=none
consumed 177
eof
"##,
    ),
    (
        "chunk header byte past the size limit",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "HTTP chunk header is too large" status=Some(400) new_stream=false partial=none
consumed 177
eof
"##,
    ),
    (
        "invalid terminator byte",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
error 1 proxygen=17 "invalid HTTP chunk terminator" status=Some(400) new_stream=false partial=none
consumed 53
eof
"##,
    ),
    (
        "cr then an invalid terminator byte",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
error 1 proxygen=17 "invalid HTTP chunk terminator" status=Some(400) new_stream=false partial=none
consumed 54
eof
"##,
    ),
    (
        "cr cr terminator",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
error 1 proxygen=17 "invalid HTTP chunk terminator" status=Some(400) new_stream=false partial=none
consumed 54
eof
"##,
    ),
    (
        "bare lf terminator",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
chunk_complete 1
complete 1 upgraded=false
consumed 58
eof
"##,
    ),
    (
        "lf then cr lf after a chunk",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
chunk_complete 1
error 1 proxygen=17 "invalid HTTP chunk length" status=Some(400) new_stream=false partial=none
consumed 55
eof
"##,
    ),
    (
        "eof inside a chunk length",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
consumed 49
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=none
"##,
    ),
    (
        "eof after a chunk length cr",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
consumed 50
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=none
"##,
    ),
    (
        "eof inside a chunk extension",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
consumed 53
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=none
"##,
    ),
    (
        "eof before a chunk terminator",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
consumed 52
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=none
"##,
    ),
    (
        "eof inside a chunk terminator",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 1
body 1 "a"
consumed 53
eof
error 1 proxygen=18 "unexpected EOF" status=Some(400) new_stream=false partial=none
"##,
    ),
    (
        "last chunk extension and trailers",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
trailers 1 ["X-T": "1"]
complete 1 upgraded=false
consumed 67
eof
"##,
    ),
    (
        "chunked request then a get",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 3
body 1 "abc"
chunk_complete 1
chunk_header 1 2
body 1 "de"
chunk_complete 1
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
consumed 89
eof
"##,
    ),
    (
        "chunked response with an extension",
        r##"begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["Transfer-Encoding": "chunked"] size=47/47 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 3
body 1 "abc"
chunk_complete 1
complete 1 upgraded=false
consumed 62
eof
"##,
    ),
    (
        "chunked response with an invalid length",
        r##"begin 1
headers 1 response status=200 reason="OK" version=1.1 headers=["Transfer-Encoding": "chunked"] size=47/47 upgraded=false websocket=false keepalive=true chunked=true
error 1 proxygen=17 "invalid HTTP chunk length" status=None new_stream=false partial=none
consumed 50
eof
"##,
    ),
    (
        "paused at chunk headers",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 3
pause ChunkHeader at 51
body 1 "abc"
chunk_complete 1
chunk_header 1 2
pause ChunkHeader at 61
body 1 "de"
chunk_complete 1
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
consumed 89
eof
"##,
    ),
    (
        "paused at chunk completions",
        r##"begin 1
headers 1 request method=Some("POST") url="/c" version=1.1 headers=["Transfer-Encoding": "chunked"] size=48/48 upgraded=false websocket=false keepalive=true chunked=true
chunk_header 1 3
body 1 "abc"
chunk_complete 1
pause ChunkComplete at 56
chunk_header 1 2
body 1 "de"
chunk_complete 1
pause ChunkComplete at 65
complete 1 upgraded=false
begin 2
headers 2 request method=Some("GET") url="/g" version=1.1 headers=[] size=19/19 upgraded=false websocket=false keepalive=true chunked=false
complete 2 upgraded=false
consumed 89
eof
"##,
    ),
];

fn chunked_golden_trace(name: &str) -> &'static str {
    CHUNKED_GOLDEN_TRACES
        .iter()
        .find(|(golden, _)| *golden == name)
        .map(|(_, trace)| *trace)
        .unwrap_or_else(|| panic!("no chunked golden trace for {name}"))
}

#[test]
fn chunked_ingress_matches_the_recorded_traces_at_every_split() {
    assert_eq!(
        CHUNKED_TRACE_CASES.len(),
        CHUNKED_GOLDEN_TRACES.len(),
        "one golden trace per chunked case"
    );
    for error in CHUNK_ERRORS {
        assert!(
            CHUNKED_GOLDEN_TRACES
                .iter()
                .any(|(_, trace)| trace.contains(&format!("\"{error}\""))),
            "the chunked cases reach \"{error}\""
        );
    }
    for case in CHUNKED_TRACE_CASES.iter() {
        let golden = chunked_golden_trace(case.name);
        for_every_delivery(case, |context, outcome| {
            assert_eq!(outcome.trace, golden, "{context}");
        });
    }
}

/// Counts heap allocations made on a thread while its counter is enabled. It
/// serves every test in this crate and only counts inside
/// [`count_allocations`].
struct CountingAllocator;

std::thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation() {
    if COUNTING.with(Cell::get) {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
    }
}

// SAFETY: every method passes its arguments unchanged to the same `System`
// method, so `System` upholds the `GlobalAlloc` contract. Counting touches only
// const-initialized thread-local cells, which neither allocate nor register
// destructors.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc`'s layout precondition.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc_zeroed`'s layout precondition.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`, and this call's caller upheld `realloc`'s size precondition.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Runs `run` and returns its value and the allocations it made on this
/// thread, reallocations included.
fn count_allocations<T>(run: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    let value = run();
    COUNTING.with(|counting| counting.set(false));
    (value, ALLOCATIONS.with(Cell::get))
}

/// Whether this build's std allocates through [`ALLOCATOR`]. Dev builds link a
/// shared std whose own allocations, `Vec` growth among them, bypass it.
fn std_allocations_observable() -> bool {
    count_allocations(|| drop(black_box(Vec::<u8>::with_capacity(4_096)))).1 == 1
}

/// Counts chunk events and body bytes without allocating.
#[derive(Debug, Default, Eq, PartialEq)]
struct ChunkCounter {
    chunk_headers: usize,
    chunk_bytes: usize,
    chunk_completions: usize,
    body_bytes: usize,
    errors: usize,
}

impl HttpCodecCallback for ChunkCounter {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::ChunkHeader { length, .. } => {
                self.chunk_headers += 1;
                self.chunk_bytes += length;
            }
            CodecEvent::ChunkComplete { .. } => self.chunk_completions += 1,
            CodecEvent::Body { body, .. } => self.body_bytes += body.chain_length(),
            CodecEvent::Error { .. } => self.errors += 1,
            _ => {}
        }
    }
}

#[test]
fn chunk_headers_and_terminators_allocate_nothing_after_warm_up() {
    const CHUNKS: usize = 8;
    // Each framing ends one 5-byte chunk and begins the next.
    let framings: [&[u8]; 3] = [b"\r\n5\r\n", b"\n5\n", b"\r\n5;name=value\r\n"];
    for framing in framings {
        for byte_by_byte in [false, true] {
            let context = format!("{} byte_by_byte={byte_by_byte}", framing.escape_ascii());
            let mut codec = Http1xCodec::new(TransportDirection::Downstream);
            let mut counter = ChunkCounter::default();
            let mut egress = BufQueue::new(QUEUE_LIMIT);
            let mut ingress = |codec: &mut Http1xCodec, read: &BufQueue| {
                codec.on_ingress(read, &mut counter, &mut egress).consumed
            };
            let warm_up = [CHUNKED_REQUEST_HEAD, b"5\r\nhello"].concat();
            assert_eq!(ingress(&mut codec, &ingress_queue(&warm_up)), warm_up.len());
            let framing_reads = if byte_by_byte {
                framing.chunks(1).map(ingress_queue).collect::<Vec<_>>()
            } else {
                vec![ingress_queue(framing)]
            };
            let body = ingress_queue(b"hello");
            let mut allocations = 0;
            for _ in 0..CHUNKS {
                let (consumed, counted) = count_allocations(|| {
                    framing_reads
                        .iter()
                        .map(|read| ingress(&mut codec, read))
                        .sum::<usize>()
                });
                assert_eq!(consumed, framing.len(), "{context}");
                allocations += counted;
                assert_eq!(ingress(&mut codec, &body), body.chain_length(), "{context}");
            }
            assert_eq!(
                counter,
                ChunkCounter {
                    chunk_headers: CHUNKS + 1,
                    chunk_bytes: 5 * (CHUNKS + 1),
                    chunk_completions: CHUNKS,
                    body_bytes: 5 * (CHUNKS + 1),
                    errors: 0,
                },
                "{context}"
            );
            if std_allocations_observable() {
                assert_eq!(
                    allocations, 0,
                    "{context}: chunk terminators and chunk-size lines allocate nothing"
                );
            } else {
                eprintln!(
                    "{context}: allocation counts need a static std, as in @fbcode//mode/opt"
                );
            }
        }
    }
}

/// Counts `HeadersComplete` events and notes where the last one's message
/// lives, without allocating.
#[derive(Debug, Default)]
struct HeadAddresses {
    heads: usize,
    last: Option<*const HttpMessage>,
}

impl HttpCodecCallback for HeadAddresses {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if let CodecEvent::HeadersComplete { message, .. } = event {
            self.heads += 1;
            self.last = Some(ptr::from_ref(message.as_ref()));
        }
    }
}

/// A codec ready for each path through `install_parsed_head`, with its head:
/// a request with a body, a bodiless request that completes at once, and a
/// chunked response.
fn parsed_head_cases() -> [(&'static str, Http1xCodec, &'static [u8]); 3] {
    let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
    send_trace_request(&mut upstream);
    [
        (
            "request with a body",
            Http1xCodec::new(TransportDirection::Downstream),
            b"POST /c HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\n",
        ),
        (
            "bodiless request",
            Http1xCodec::new(TransportDirection::Downstream),
            b"GET /g HTTP/1.1\r\nHost: a\r\nUser-Agent: t\r\n\r\n",
        ),
        ("chunked response", upstream, CHUNKED_RESPONSE_HEAD),
    ]
}

#[test]
fn a_parsed_head_reaches_headers_complete_in_the_box_it_was_parsed_into() {
    for (name, mut codec, head) in parsed_head_cases() {
        let parsed = codec.parse_head(head, None, None).expect("the head parses");
        let parsed_message: &HttpMessage = parsed.message.borrow();
        let parsed_at = ptr::from_ref(parsed_message);
        let mut delivered = HeadAddresses::default();
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let mut result = CodecIngressResult::new(0);
        codec.install_parsed_head(parsed, &mut delivered, &mut egress, &mut result);
        assert_eq!(delivered.heads, 1, "{name}");
        assert_eq!(
            delivered.last,
            Some(parsed_at),
            "{name}: HeadersComplete carries the message where the parse left it"
        );
    }
}

#[test]
fn delivering_a_parsed_head_allocates_nothing() {
    for (name, mut codec, head) in parsed_head_cases() {
        let parsed = codec.parse_head(head, None, None).expect("the head parses");
        let mut delivered = HeadAddresses::default();
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let mut result = CodecIngressResult::new(0);
        let ((), allocations) = count_allocations(|| {
            codec.install_parsed_head(parsed, &mut delivered, &mut egress, &mut result);
        });
        assert_eq!(delivered.heads, 1, "{name}");
        if std_allocations_observable() {
            assert_eq!(allocations, 0, "{name}: delivering a parsed head allocates");
        } else {
            eprintln!("{name}: allocation counts need a static std, as in @fbcode//mode/opt");
        }
    }
}

#[test]
fn parsing_and_delivering_a_head_allocates_as_before() {
    let allocations = parsed_head_cases().map(|(name, mut codec, head)| {
        let read = ingress_queue(head);
        let mut delivered = HeadAddresses::default();
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let (consumed, allocations) = count_allocations(|| {
            codec
                .on_ingress(&read, &mut delivered, &mut egress)
                .consumed
        });
        assert_eq!(consumed, head.len(), "{name}");
        assert_eq!(delivered.heads, 1, "{name}");
        (name, allocations)
    });
    if !std_allocations_observable() {
        eprintln!("allocation counts need a static std, as in @fbcode//mode/opt");
        return;
    }
    // Each head: its header list's reservation and its message's box. The
    // codec also keeps a copy of the first User-Agent.
    assert_eq!(
        allocations,
        [
            ("request with a body", 2),
            ("bodiless request", 3),
            ("chunked response", 2),
        ],
        "allocations to parse and deliver each head"
    );
}

/// Whether a `T` can own heap memory: a type without drop glue cannot.
fn may_own_heap<T>(_: &T) -> bool {
    std::mem::needs_drop::<T>()
}

#[test]
fn chunk_size_and_terminator_progress_holds_no_heap_buffer() {
    let mut codec = Http1xCodec::new(TransportDirection::Downstream);
    let mut counter = ChunkCounter::default();
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let reads: [&[u8]; 3] = [CHUNKED_REQUEST_HEAD, b"5;na", b"me=value\r\nhello\r"];
    let mut ingress = |codec: &mut Http1xCodec, read: &[u8]| {
        codec
            .on_ingress(&ingress_queue(read), &mut counter, &mut egress)
            .consumed
    };

    assert_eq!(ingress(&mut codec, reads[0]), reads[0].len());
    assert_eq!(ingress(&mut codec, reads[1]), reads[1].len());
    let ParseState::ChunkSize(progress) = &codec.state else {
        panic!("a partial chunk-size line leaves the codec in ChunkSize");
    };
    assert!(
        !may_own_heap(progress),
        "chunk-size progress holds no heap buffer"
    );

    assert_eq!(ingress(&mut codec, reads[2]), reads[2].len());
    let ParseState::ChunkDelimiter(progress) = &codec.state else {
        panic!("a partial chunk terminator leaves the codec in ChunkDelimiter");
    };
    assert!(
        !may_own_heap(progress),
        "chunk-terminator progress holds no heap buffer"
    );
}

#[test]
fn a_chunk_terminator_inside_one_segment_takes_one_parse_turn() {
    // A chunked body as the segments of one read, the bytes the codec
    // consumes, and the parse-loop turns its chunk terminators take: one per
    // terminator, plus one for a CR that is not followed by an LF in its
    // segment. The last case fails at its second CR.
    let cases: [(&[&[u8]], usize, usize); 5] = [
        (&[b"1\r\na\r\n1\r\nb\r\n0\r\n\r\n"], 17, 2),
        (&[b"1\r\na\n1\r\nb\n0\r\n\r\n"], 15, 2),
        (&[b"1\r\na\r", b"\n1\r\nb\r\n0\r\n\r\n"], 17, 3),
        (&[b"1\r\na", b"\r\n1\r\nb\r\n0\r\n\r\n"], 17, 2),
        (&[b"1\r\na\r\r\n"], 6, 2),
    ];
    for (segments, expected_consumed, expected_turns) in cases {
        let context = segments
            .iter()
            .map(|segment| segment.escape_ascii().to_string())
            .collect::<Vec<_>>()
            .join(" | ");
        let (mut codec, mut recorder) = chunk_size_codec(false, false);
        let length = segments.iter().map(|segment| segment.len()).sum();
        let mut input = BufQueue::new(length);
        for segment in segments {
            input.push_bytes(Bytes::from_static(segment)).unwrap();
        }
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let before = chunk_delimiter_turns();
        let consumed = codec
            .on_ingress(&input, &mut recorder, &mut egress)
            .consumed;
        let turns = chunk_delimiter_turns() - before;

        assert_eq!(consumed, expected_consumed, "{context}");
        assert_eq!(
            turns, expected_turns,
            "{context}: chunk-terminator turns of the parse loop"
        );
    }
}

fn generate_post_head(unframed: bool, end_stream: bool, content_length: Option<&[u8]>) -> Vec<u8> {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    codec.set_unframed_bodiless_requests(unframed);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Post, b"/generate", (1, 1));
    if let Some(content_length) = content_length {
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, content_length);
    }
    generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream,
            extra_headers: None,
        },
    )
}

#[test]
fn a_bodiless_post_is_framed_as_an_empty_chunked_body_by_default() {
    assert_eq!(
        generate_post_head(false, true, None),
        b"POST /generate HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n0\r\n\r\n"
    );
}

#[test]
fn a_bodiless_post_has_no_body_framing_when_enabled() {
    assert_eq!(
        generate_post_head(true, true, None),
        b"POST /generate HTTP/1.1\r\nConnection: keep-alive\r\n\r\n"
    );
}

#[test]
fn a_post_with_a_body_still_auto_chunks_when_enabled() {
    assert_eq!(
        generate_post_head(true, false, None),
        b"POST /generate HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n"
    );
}

#[test]
fn an_explicit_zero_content_length_is_kept_when_enabled() {
    assert_eq!(
        generate_post_head(true, true, Some(b"0")),
        b"POST /generate HTTP/1.1\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"
    );
}

#[test]
fn a_bodiless_http10_post_keeps_the_connection_when_enabled() {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    codec.set_unframed_bodiless_requests(true);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Post, b"/", (1, 0));
    message
        .headers_mut()
        .set(HttpHeaderCode::Connection, b"keep-alive");
    let head = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    assert_eq!(head, b"POST / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
    assert!(codec.is_reusable());
}

/// The head of a bodiless POST that carries the caller's own chunked
/// `Transfer-Encoding`, and whether the connection stays reusable after it.
fn bodiless_post_with_chunked_transfer_encoding(unframed: bool) -> (Vec<u8>, bool) {
    let mut codec = Http1xCodec::new(TransportDirection::Upstream);
    codec.set_unframed_bodiless_requests(unframed);
    let stream = codec.create_stream();
    let mut message = request(HttpMethod::Post, b"/generate", (1, 1));
    message
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    let head = generate(
        &mut codec,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    );
    (head, codec.is_reusable())
}

/// Caller framing is the caller's: a chunked `Transfer-Encoding` on a head that
/// ends the message gets no last chunk, so the connection must close after it,
/// with the option as without it. Reusing it would let a server that answers
/// without reading the body parse the next request as chunk data.
#[test]
fn a_bodiless_post_with_caller_chunked_framing_closes_the_connection_when_enabled() {
    let expected = (
        b"POST /generate HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            .to_vec(),
        false,
    );
    assert_eq!(
        bodiless_post_with_chunked_transfer_encoding(false),
        expected
    );
    assert_eq!(bodiless_post_with_chunked_transfer_encoding(true), expected);
}
