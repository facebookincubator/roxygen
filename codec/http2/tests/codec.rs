use std::cell::Cell;
use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::rc::Rc;

use bytes::Bytes;
use roxygen_codec::CodecCallbackAction;
use roxygen_codec::CodecCallbackActions;
use roxygen_codec::CodecEvent;
use roxygen_codec::CodecSetting;
use roxygen_codec::EgressFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::HttpExceptionDirection;
use roxygen_codec::OwnedEgressFrame;
use roxygen_codec::ProxygenError;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::COMPACT_WRITE_LIMIT;
use roxygen_core::headers::HttpCommonHeaderTable;
use roxygen_core::headers::HttpCommonHeaders;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::priority::HttpPriority;

use super::codec_impl::Http2Codec;
use super::constants::CONNECTION_PREFACE;
use super::constants::FRAME_HEADER_SIZE;
use super::constants::MAX_FRAME_PAYLOAD_LENGTH_MIN;
use super::constants::MAX_WINDOW_UPDATE_SIZE;
use super::frame;
use super::frame::Flags;
use super::frame::Frame;
use super::frame::FrameType;
use super::frame::Setting;
use super::frame::parse_frame;
use super::header_decode::FieldDecodeOptions;
use super::hpack::HeaderCodecStats;
use super::hpack::HeaderIndexingStrategy;
use super::hpack::HeaderName;
use super::hpack::HpackCodec;
use super::hpack::HpackEncodeBuffer;
use super::hpack::HpackHeader;
use super::settings::SettingsId;
use super::util::ControlEscapeMode;
use super::util::validate_header_value;
use super::util::validate_method;
use crate::hpack_contract::fnv1a64;

#[derive(Debug, Eq, PartialEq)]
enum Seen {
    Frame(u32, u8, u32),
    Begin(u64),
    PushBegin(u64, u64),
    Headers(u64, bool, u16, Vec<u8>),
    Body(u64, usize, u16),
    Trailers(u64, usize),
    Complete(u64),
    Error(u64, Option<H2ErrorCode>, Option<u32>, bool),
    Abort(u64, H2ErrorCode),
    Settings(Vec<CodecSetting>),
    SettingsAck,
    Ping(bool, u64),
    Window(u64, u32),
    Goaway(u64, H2ErrorCode, Vec<u8>),
    Priority(u64, u8, bool),
    Generated(u64, u8, u64),
}

#[derive(Default)]
struct Trace {
    events: Vec<Seen>,
}

#[derive(Default)]
struct MessageCapture {
    events: Vec<&'static str>,
    messages: Vec<HttpMessage>,
    bodies: Vec<(usize, u16)>,
    push_ids: Vec<(u64, u64)>,
    push_messages: Vec<Option<HttpMessage>>,
    error_partial_message: Option<HttpMessage>,
    error: Option<(
        HttpExceptionDirection,
        Option<H2ErrorCode>,
        Option<u32>,
        bool,
    )>,
}

impl HttpCodecCallback for MessageCapture {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::MessageBegin { .. } => self.events.push("begin"),
            CodecEvent::PushMessageBegin {
                stream,
                associated_stream,
                message,
            } => {
                self.events.push("push-begin");
                self.push_ids.push((stream.get(), associated_stream.get()));
                self.push_messages
                    .push(message.map(|message| (*message).clone()));
            }
            CodecEvent::HeadersComplete { message, .. } => {
                self.events.push("headers");
                self.messages.push(*message);
            }
            CodecEvent::Body { body, padding, .. } => {
                self.events.push("body");
                self.bodies.push((body.chain_length(), padding));
            }
            CodecEvent::TrailersComplete { trailers, .. } => {
                self.events.push("trailers");
                let mut message = HttpMessage::new();
                *message.headers_mut() = *trailers;
                self.messages.push(message);
            }
            CodecEvent::MessageComplete { .. } => self.events.push("complete"),
            CodecEvent::Error {
                error, new_stream, ..
            } => {
                self.events.push("error");
                self.error = Some((
                    error.direction(),
                    error.codec_status_code(),
                    error.http_status_code(),
                    new_stream,
                ));
                self.error_partial_message = error.partial_message().cloned();
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct SharedDataCapture {
    segments: Vec<Bytes>,
}

impl HttpCodecCallback for SharedDataCapture {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if let CodecEvent::Body { body, .. } = event {
            self.segments.extend(body.into_bytes_segments());
        }
    }
}

impl HttpCodecCallback for Trace {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        let seen = match event {
            CodecEvent::FrameHeader {
                stream,
                frame_type,
                length,
                ..
            } => Seen::Frame(stream.get() as u32, frame_type as u8, length as u32),
            CodecEvent::MessageBegin { stream, .. } => Seen::Begin(stream.get()),
            CodecEvent::PushMessageBegin {
                stream,
                associated_stream,
                ..
            } => Seen::PushBegin(stream.get(), associated_stream.get()),
            CodecEvent::HeadersComplete {
                stream,
                mut message,
            } => {
                let request = message.is_request();
                let status = message.status_code().unwrap_or_default();
                let path = message.url().unwrap_or_default().to_vec();
                Seen::Headers(stream.get(), request, status, path)
            }
            CodecEvent::Body {
                stream,
                body,
                padding,
            } => Seen::Body(stream.get(), body.chain_length(), padding),
            CodecEvent::TrailersComplete { stream, trailers } => {
                Seen::Trailers(stream.get(), trailers.len())
            }
            CodecEvent::MessageComplete { stream, .. } => Seen::Complete(stream.get()),
            CodecEvent::Error {
                stream,
                error,
                new_stream,
            } => Seen::Error(
                stream.get(),
                error.codec_status_code(),
                error.http_status_code(),
                new_stream,
            ),
            CodecEvent::Abort { stream, code } => Seen::Abort(stream.get(), code),
            CodecEvent::Settings { settings } => Seen::Settings(settings.into_vec()),
            CodecEvent::SettingsAck => Seen::SettingsAck,
            CodecEvent::PingRequest { data } => Seen::Ping(false, data),
            CodecEvent::PingReply { data } => Seen::Ping(true, data),
            CodecEvent::WindowUpdate { stream, amount } => Seen::Window(stream.get(), amount),
            CodecEvent::Goaway {
                last_good_stream,
                code,
                debug_data,
            } => Seen::Goaway(
                last_good_stream.get(),
                code,
                debug_data.as_ref().map_or_else(Vec::new, bytes),
            ),
            CodecEvent::Priority {
                stream, priority, ..
            } => Seen::Priority(stream.get(), priority.urgency, priority.incremental),
            CodecEvent::PushPriority {
                stream, priority, ..
            } => Seen::Priority(stream.get(), priority.urgency, priority.incremental),
            CodecEvent::GeneratedFrameHeader {
                stream,
                frame_type,
                length,
                ..
            } => Seen::Generated(stream.get(), frame_type, length),
            CodecEvent::ChunkHeader { .. } | CodecEvent::ChunkComplete { .. } => return,
        };
        self.events.push(seen);
    }
}

fn bytes(queue: &BufQueue) -> Vec<u8> {
    queue
        .segments()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}

fn queue(value: &[u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(value), value.len()).unwrap()
}

fn empty_settings() -> Vec<u8> {
    let mut output = BufQueue::new(1024);
    frame::write_settings(&mut output, &[]).unwrap();
    bytes(&output)
}

fn open(codec: &mut Http2Codec, trace: &mut Trace) {
    let settings = empty_settings();
    let mut ingress = Vec::new();
    if codec.transport_direction().is_downstream() {
        ingress.extend_from_slice(CONNECTION_PREFACE);
    }
    ingress.extend_from_slice(&settings);
    let mut egress = BufQueue::new(64 * 1024);
    let result = codec.on_ingress(&queue(&ingress), trace, &mut egress);
    assert_eq!(result.consumed, ingress.len());
}

fn request(path: &[u8]) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Get).unwrap();
    message.set_url(path, false).unwrap();
    message.set_http_version(1, 1);
    message
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example.com");
    message
}

fn response(status: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    message
        .set_status_message(HttpMessage::default_reason(status))
        .unwrap();
    message.set_http_version(1, 1);
    message
}

#[test]
fn compact_data_preserves_wire_events_and_offsets_for_both_directions() {
    for direction in [TransportDirection::Upstream, TransportDirection::Downstream] {
        for length in [
            0,
            128,
            1024,
            COMPACT_WRITE_LIMIT - FRAME_HEADER_SIZE,
            COMPACT_WRITE_LIMIT,
            16_384 + 128,
        ] {
            for padding in [None, Some(7)] {
                for end_stream in [false, true] {
                    for flushed_headers in [false, true] {
                        let mut expected = None;
                        for compact in [false, true] {
                            let mut codec = Http2Codec::new(direction);
                            codec.set_compact_data(compact);
                            codec.set_add_date_header_to_response(false);
                            let message = if direction.is_upstream() {
                                request(b"/compact")
                            } else {
                                response(200)
                            };
                            let mut output = BufQueue::new(32 * 1024);
                            let mut trace = Trace::default();
                            codec
                                .generate(
                                    &mut output,
                                    EgressFrame::Headers {
                                        stream: StreamId::new(1),
                                        message: &message,
                                        end_stream: false,
                                        extra_headers: None,
                                    },
                                    &mut trace,
                                )
                                .unwrap();
                            if flushed_headers {
                                drop(output.take());
                            }
                            let header_length = output.chain_length();
                            let payload = vec![b'b'; length];
                            let mut body = BufQueue::new(length);
                            payload
                                .chunks(37)
                                .for_each(|chunk| body.push_copy(chunk).unwrap());
                            let generated = codec
                                .generate(
                                    &mut output,
                                    EgressFrame::Body {
                                        stream: StreamId::new(1),
                                        body,
                                        padding,
                                        end_stream,
                                    },
                                    &mut trace,
                                )
                                .unwrap();
                            let observed = (
                                bytes(&output),
                                trace.events,
                                generated.bytes_written,
                                generated.payload_offset,
                            );
                            if let Some(expected) = &expected {
                                assert_eq!(&observed, expected);
                            } else {
                                expected = Some(observed);
                            }
                            if compact
                                && padding.is_none()
                                && header_length + FRAME_HEADER_SIZE + length <= COMPACT_WRITE_LIMIT
                            {
                                assert_eq!(output.segment_count(), 1);
                                assert!(
                                    output.backing_capacity_upper_bound().unwrap()
                                        <= COMPACT_WRITE_LIMIT
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn compact_data_commits_wire_before_body_owner_destruction() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct Owner {
        drops: Arc<AtomicUsize>,
        panic: bool,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            b"body"
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic, "body owner drop panic");
        }
    }
    for panic in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let body = BufQueue::from_bytes(
            Bytes::from_owner(Owner {
                drops: drops.clone(),
                panic,
            }),
            4,
        )
        .unwrap();
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        codec.set_compact_data(true);
        let stream = codec.create_stream();
        let mut output = BufQueue::from_vec(b"prefix".to_vec(), 19).unwrap();
        let mut trace = Trace::default();
        let result = catch_unwind(AssertUnwindSafe(|| {
            codec.generate(
                &mut output,
                EgressFrame::Body {
                    stream,
                    body,
                    padding: None,
                    end_stream: true,
                },
                &mut trace,
            )
        }));
        assert_eq!(result.is_err(), panic);
        if let Ok(result) = result {
            assert_eq!(result.unwrap().bytes_written, 13);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(bytes(&output), b"prefix\0\0\x04\0\x01\0\0\0\x01body");
        assert_eq!(output.chain_length(), 19);
        assert_eq!(output.remaining_capacity(), 0);
        assert_eq!(output.segment_count(), 1);
        assert_eq!(trace.events.len(), usize::from(!panic));
    }
}

#[test]
fn compact_end_of_message_reuses_headers_and_preserves_callbacks() {
    let mut expected = None;
    for compact in [false, true] {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        codec.set_compact_data(compact);
        codec.set_add_date_header_to_response(false);
        let mut output = BufQueue::new(128);
        let mut trace = Trace::default();
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &response(200),
                    end_stream: false,
                    extra_headers: None,
                },
                &mut trace,
            )
            .unwrap();
        let generated = codec
            .generate(
                &mut output,
                EgressFrame::EndOfMessage {
                    stream: StreamId::new(1),
                },
                &mut trace,
            )
            .unwrap();
        let observed = (
            bytes(&output),
            trace.events,
            generated.bytes_written,
            generated.payload_offset,
        );
        if let Some(expected) = &expected {
            assert_eq!(&observed, expected);
        } else {
            expected = Some(observed);
        }
        if compact {
            assert_eq!(output.segment_count(), 1);
        }
    }
}

#[test]
fn compact_data_preserves_aliases_and_rejects_invalid_frames_before_mutation() {
    let prefix = Bytes::from(b"prefix".to_vec());
    let payload = Bytes::from(b"body".to_vec());
    for (stream, limit) in [(0, 64), (1, 18)] {
        let mut output = BufQueue::from_bytes(prefix.clone(), limit).unwrap();
        let body = BufQueue::from_bytes(payload.clone(), 4).unwrap();
        assert!(frame::write_compact_data(&mut output, body, stream, true).is_err());
        assert_eq!(bytes(&output), b"prefix");
        assert_eq!(output.front().unwrap().as_ptr(), prefix.as_ptr());
    }
    let mut output = BufQueue::from_bytes(prefix.clone(), 19).unwrap();
    let body = BufQueue::from_bytes(payload.clone(), 4).unwrap();
    assert_eq!(
        frame::write_compact_data(&mut output, body, 1, true).unwrap(),
        13
    );
    assert_eq!(output.segment_count(), 2);
    assert_eq!(bytes(&output), b"prefix\0\0\x04\0\x01\0\0\0\x01body");
    assert_eq!(prefix.as_ref(), b"prefix");
    assert_eq!(payload.as_ref(), b"body");

    let payload = Bytes::from(vec![b'b'; COMPACT_WRITE_LIMIT]);
    let mut output = BufQueue::new(COMPACT_WRITE_LIMIT + FRAME_HEADER_SIZE);
    let body = BufQueue::from_bytes(payload.clone(), COMPACT_WRITE_LIMIT).unwrap();
    frame::write_compact_data(&mut output, body, 1, true).unwrap();
    assert_eq!(output.segment_count(), 2);
    assert_eq!(output.segments().last().unwrap().as_ptr(), payload.as_ptr());
}

fn round_trip_headers(message: &HttpMessage, direction: TransportDirection, end: bool) -> Trace {
    let sender_direction = if direction.is_downstream() {
        TransportDirection::Upstream
    } else {
        TransportDirection::Downstream
    };
    let mut sender = Http2Codec::new(sender_direction);
    let stream = if sender_direction.is_upstream() {
        sender.create_stream()
    } else {
        StreamId::new(1)
    };
    let mut output = BufQueue::new(512 * 1024);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message,
                end_stream: end,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let payload = bytes(&output);
    let mut receiver = Http2Codec::new(direction);
    if direction.is_upstream() {
        assert_eq!(receiver.create_stream(), StreamId::new(1));
    }
    let mut trace = Trace::default();
    open(&mut receiver, &mut trace);
    trace.events.clear();
    let mut callback_egress = BufQueue::new(64 * 1024);
    assert_eq!(
        receiver
            .on_ingress(&queue(&payload), &mut trace, &mut callback_egress)
            .consumed,
        payload.len()
    );
    trace
}

fn round_trip_message(
    message: &HttpMessage,
    direction: TransportDirection,
    end: bool,
) -> MessageCapture {
    let sender_direction = if direction.is_downstream() {
        TransportDirection::Upstream
    } else {
        TransportDirection::Downstream
    };
    let mut sender = Http2Codec::new(sender_direction);
    let stream = if sender_direction.is_upstream() {
        sender.create_stream()
    } else {
        StreamId::new(1)
    };
    let mut output = BufQueue::new(512 * 1024);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message,
                end_stream: end,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let payload = bytes(&output);
    let mut receiver = Http2Codec::new(direction);
    if direction.is_upstream() {
        assert_eq!(receiver.create_stream(), StreamId::new(1));
    }
    open(&mut receiver, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        receiver
            .on_ingress(
                &queue(&payload),
                &mut callback,
                &mut BufQueue::new(64 * 1024),
            )
            .consumed,
        payload.len()
    );
    callback
}

fn ingress_one(
    codec: &mut Http2Codec,
    wire: &[u8],
    trace: &mut Trace,
) -> roxygen_codec::CodecIngressResult {
    codec.on_ingress(&queue(wire), trace, &mut BufQueue::new(64 * 1024))
}

#[test]
fn http_constants_are_common_headers() {
    assert_eq!(
        HttpCommonHeaders::name(
            HttpHeaderCode::ColonMethod,
            HttpCommonHeaderTable::Lowercase
        ),
        b":method"
    );
    assert_eq!(
        HttpCommonHeaders::name(
            HttpHeaderCode::ColonStatus,
            HttpCommonHeaderTable::Lowercase
        ),
        b":status"
    );
}

#[test]
fn ignore_unknown_settings() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    frame::write_settings(
        &mut wire,
        &[
            Setting {
                identifier: 0xfefe,
                value: 42,
            },
            Setting {
                identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                value: 10,
            },
        ],
    )
    .unwrap();
    let mut trace = Trace::default();
    let input = bytes(&wire);
    assert_eq!(
        ingress_one(&mut codec, &input, &mut trace).consumed,
        input.len()
    );
    assert!(
        trace
            .events
            .iter()
            .any(|event| matches!(event, Seen::Settings(settings) if settings.len() == 1))
    );
}

#[test]
fn omit_settings_after_connection_preface_error() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut bad = CONNECTION_PREFACE.to_vec();
    bad[0] = b'X';
    bad.extend_from_slice(&empty_settings());
    let mut trace = Trace::default();
    let result = ingress_one(&mut codec, &bad, &mut trace);
    assert_eq!(result.consumed, CONNECTION_PREFACE.len());
    assert_eq!(trace.events.len(), 1);
    assert!(matches!(
        trace.events[0],
        Seen::Error(0, Some(H2ErrorCode::PROTOCOL_ERROR), _, false)
    ));
}

#[test]
fn basic_setting() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    frame::write_settings(
        &mut wire,
        &[Setting {
            identifier: SettingsId::MaxConcurrentStreams.as_u16(),
            value: 100,
        }],
    )
    .unwrap();
    let input = bytes(&wire);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &input, &mut trace).consumed,
        input.len()
    );
    assert_eq!(
        codec
            .ingress_settings()
            .value_or(SettingsId::MaxConcurrentStreams, 0),
        100
    );
}

#[test]
fn settings_ack() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    frame::write_settings_ack(&mut wire).unwrap();
    let input = bytes(&wire);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &input, &mut trace).consumed,
        input.len()
    );
    assert!(trace.events.contains(&Seen::SettingsAck));
}

fn assert_bad_setting(id: SettingsId, value: u32) {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    frame::write_settings(
        &mut wire,
        &[Setting {
            identifier: id.as_u16(),
            value,
        }],
    )
    .unwrap();
    let input = bytes(&wire);
    let mut trace = Trace::default();
    let _ = ingress_one(&mut codec, &input, &mut trace);
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(0, Some(H2ErrorCode::PROTOCOL_ERROR), _, false)
    )));
}

#[test]
fn bad_settings() {
    assert_bad_setting(SettingsId::MaxFrameSize, 1);
}

#[test]
fn zero_and_maximum_initial_window_size_settings() {
    for direction in [TransportDirection::Upstream, TransportDirection::Downstream] {
        let mut codec = Http2Codec::new(direction);
        open(&mut codec, &mut Trace::default());
        for value in [0, MAX_WINDOW_UPDATE_SIZE] {
            let mut wire = BufQueue::new(1024);
            frame::write_settings(
                &mut wire,
                &[Setting {
                    identifier: SettingsId::InitialWindowSize.as_u16(),
                    value,
                }],
            )
            .unwrap();
            let input = bytes(&wire);
            let mut trace = Trace::default();
            assert_eq!(
                ingress_one(&mut codec, &input, &mut trace).consumed,
                input.len()
            );
            assert!(
                !trace
                    .events
                    .iter()
                    .any(|event| matches!(event, Seen::Error(..)))
            );
            assert!(trace.events.contains(&Seen::Settings(vec![CodecSetting {
                identifier: SettingsId::InitialWindowSize.as_u16(),
                value: u64::from(value),
            }])));
            assert_eq!(
                codec
                    .ingress_settings()
                    .value_or(SettingsId::InitialWindowSize, u64::MAX),
                u64::from(value)
            );
        }
    }
}

#[test]
fn oversized_initial_window_size_is_flow_control_error() {
    for direction in [TransportDirection::Upstream, TransportDirection::Downstream] {
        let mut codec = Http2Codec::new(direction);
        open(&mut codec, &mut Trace::default());
        let mut wire = BufQueue::new(1024);
        frame::write_settings(
            &mut wire,
            &[Setting {
                identifier: SettingsId::InitialWindowSize.as_u16(),
                value: MAX_WINDOW_UPDATE_SIZE + 1,
            }],
        )
        .unwrap();
        let mut trace = Trace::default();
        let _ = ingress_one(&mut codec, &bytes(&wire), &mut trace);
        assert!(trace.events.iter().any(|event| matches!(
            event,
            Seen::Error(0, Some(H2ErrorCode::FLOW_CONTROL_ERROR), _, false)
        )));
        assert!(
            !trace
                .events
                .iter()
                .any(|event| matches!(event, Seen::Settings(_)))
        );
        assert!(
            codec
                .ingress_settings()
                .get(SettingsId::InitialWindowSize)
                .is_none()
        );
    }
}

#[test]
fn headers_on_stream_zero_are_rejected_without_output() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let message = response(200);
    let mut output = BufQueue::new(512 * 1024);
    let rejected = codec.generate(
        &mut output,
        EgressFrame::Headers {
            stream: StreamId::new(0),
            message: &message,
            end_stream: false,
            extra_headers: None,
        },
        &mut Trace::default(),
    );
    assert!(rejected.is_err(), "HEADERS on stream 0 must be rejected");
    assert_eq!(output.chain_length(), 0, "no frame may be serialized");

    let mut valid_output = BufQueue::new(512 * 1024);
    codec
        .generate(
            &mut valid_output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .expect("a valid HEADERS frame still generates after the rejection");
    assert!(valid_output.chain_length() > 0);
}

#[test]
fn bad_push_settings() {
    assert_bad_setting(SettingsId::EnablePush, 2);
}

#[test]
fn settings_table_size() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    frame::write_settings(
        &mut wire,
        &[Setting {
            identifier: SettingsId::HeaderTableSize.as_u16(),
            value: 1024,
        }],
    )
    .unwrap();
    let input = bytes(&wire);
    assert_eq!(
        ingress_one(&mut codec, &input, &mut Trace::default()).consumed,
        input.len()
    );
    assert_eq!(
        codec
            .ingress_settings()
            .value_or(SettingsId::HeaderTableSize, 0),
        1024
    );
}

#[test]
fn bad_settings_table_size() {
    let mut upstream = Http2Codec::new(TransportDirection::Upstream);
    upstream
        .egress_settings_mut()
        .set(SettingsId::HeaderTableSize, 8192);
    let mut settings_wire = BufQueue::new(1024);
    upstream
        .generate_configured_settings(&mut settings_wire, &mut Trace::default())
        .unwrap();

    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    open(&mut downstream, &mut Trace::default());
    let settings_wire = bytes(&settings_wire);
    assert_eq!(
        ingress_one(&mut downstream, &settings_wire, &mut Trace::default()).consumed,
        settings_wire.len()
    );
    open(&mut upstream, &mut Trace::default());
    upstream
        .egress_settings_mut()
        .set(SettingsId::HeaderTableSize, 4096);
    assert_eq!(upstream.create_stream(), StreamId::new(1));

    let mut message = response(200);
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b"x-coolio");
    let mut response_wire = BufQueue::new(4096);
    downstream
        .generate(
            &mut response_wire,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let response_wire = bytes(&response_wire);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut upstream, &response_wire, &mut trace).consumed,
        response_wire.len()
    );
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(0, Some(H2ErrorCode::COMPRESSION_ERROR), _, false)
    )));
    assert!(!trace.events.iter().any(|event| matches!(
        event,
        Seen::Begin(_) | Seen::Headers(..) | Seen::Complete(_)
    )));
    assert!(!upstream.is_reusable());
}

#[test]
fn settings_table_size_early_shrink() {
    let mut upstream = Http2Codec::new(TransportDirection::Upstream);
    upstream
        .egress_settings_mut()
        .set(SettingsId::HeaderTableSize, 2048);
    let mut settings_wire = BufQueue::new(1024);
    upstream
        .generate_configured_settings(&mut settings_wire, &mut Trace::default())
        .unwrap();

    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    open(&mut downstream, &mut Trace::default());
    let mut settings_trace = Trace::default();
    let wire = bytes(&settings_wire);
    assert_eq!(
        ingress_one(&mut downstream, &wire, &mut settings_trace).consumed,
        wire.len()
    );
    assert_eq!(
        settings_trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Settings(_)))
            .count(),
        1
    );

    let mut ack = BufQueue::new(1024);
    downstream
        .generate(&mut ack, EgressFrame::SettingsAck, &mut Trace::default())
        .unwrap();
    open(&mut upstream, &mut Trace::default());
    let ack_wire = bytes(&ack);
    assert_eq!(
        ingress_one(&mut upstream, &ack_wire, &mut Trace::default()).consumed,
        ack_wire.len()
    );

    let mut response_wire = BufQueue::new(4096);
    let mut message = response(200);
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b"x-coolio");
    downstream
        .generate(
            &mut response_wire,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    upstream
        .egress_settings_mut()
        .set(SettingsId::HeaderTableSize, 512);
    upstream
        .generate_configured_settings(&mut BufQueue::new(1024), &mut Trace::default())
        .unwrap();
    assert_eq!(upstream.create_stream(), StreamId::new(1));
    let response_wire = bytes(&response_wire);
    let mut callback = MessageCapture::default();
    assert_eq!(
        upstream
            .on_ingress(
                &queue(&response_wire),
                &mut callback,
                &mut BufQueue::new(1024),
            )
            .consumed,
        response_wire.len()
    );
    assert_eq!(callback.events, ["begin", "headers", "complete"]);
    assert_eq!(
        callback.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentType),
        b"x-coolio"
    );
}

#[test]
fn concurrent_streams() {
    struct AtCapacity(Trace);
    impl HttpCodecCallback for AtCapacity {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            self.0.on_event(event);
        }

        fn num_incoming_streams(&self) -> u32 {
            1
        }
    }
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    codec
        .egress_settings_mut()
        .set(SettingsId::MaxConcurrentStreams, 1);
    let mut setup = Trace::default();
    open(&mut codec, &mut setup);
    let wire = encoded_headers(1, &request_fields(b"/"), true);
    let mut callback = AtCapacity(Trace::default());
    let mut egress = BufQueue::new(1024);
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut egress)
            .consumed,
        wire.len()
    );
    assert!(callback.0.events.iter().any(|event| matches!(
        event,
        Seen::Error(1, Some(H2ErrorCode::REFUSED_STREAM), _, true)
    )));
    assert!(
        !callback
            .0
            .events
            .iter()
            .any(|event| matches!(event, Seen::Headers(..)))
    );
}

#[test]
fn refused_header_block_preserves_later_dynamic_reference() {
    struct AtCapacity(MessageCapture);

    impl HttpCodecCallback for AtCapacity {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            self.0.on_event(event);
        }

        fn num_incoming_streams(&self) -> u32 {
            1
        }
    }

    for fragmented in [false, true] {
        let input = |wire: &[u8]| {
            if !fragmented {
                return queue(wire);
            }
            let split = FRAME_HEADER_SIZE + 1;
            assert!(split < wire.len());
            let mut input = BufQueue::new(wire.len());
            input
                .push_bytes(Bytes::copy_from_slice(&wire[..split]))
                .unwrap();
            input
                .push_bytes(Bytes::copy_from_slice(&wire[split..]))
                .unwrap();
            input
        };
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        codec
            .egress_settings_mut()
            .set(SettingsId::MaxConcurrentStreams, 1);
        open(&mut codec, &mut Trace::default());
        let mut encoder = HpackCodec::new();
        let inserted = HpackHeader::new(b"user-agent", b"from-refused-stream");
        let mut fields = request_fields(b"/");
        fields.push(inserted.clone());
        let encoded = encoder.encode(&fields);
        let mut wire = BufQueue::new(1024);
        frame::write_headers(&mut wire, queue(&encoded), 1, None, true, true).unwrap();
        let wire = bytes(&wire);
        let mut refused = AtCapacity(MessageCapture::default());
        assert_eq!(
            codec
                .on_ingress(&input(&wire), &mut refused, &mut BufQueue::new(1024))
                .consumed,
            wire.len(),
        );
        assert_eq!(refused.0.events, ["error"]);
        assert!(refused.0.messages.is_empty());
        assert_eq!(
            refused.0.error.as_ref().and_then(|error| error.1),
            Some(H2ErrorCode::REFUSED_STREAM),
        );
        assert!(codec.is_reusable());

        let mut encoded = encoder.encode(&request_fields(b"/"));
        let indexed = encoder.encode(&[inserted]);
        assert_eq!(
            indexed,
            [0xbe],
            "the final field must reference the entry inserted by the refused block"
        );
        encoded.extend_from_slice(&indexed);
        let mut wire = BufQueue::new(1024);
        frame::write_headers(&mut wire, queue(&encoded), 3, None, true, true).unwrap();
        let wire = bytes(&wire);
        let mut accepted = MessageCapture::default();
        assert_eq!(
            codec
                .on_ingress(&input(&wire), &mut accepted, &mut BufQueue::new(1024))
                .consumed,
            wire.len(),
        );
        assert_eq!(accepted.events, ["begin", "headers", "complete"]);
        assert!(accepted.error.is_none());
        assert_eq!(accepted.messages.len(), 1);
        let mut message = accepted.messages.remove(0);
        assert!(message.is_request());
        assert_eq!(message.url().unwrap(), b"/");
        assert_eq!(
            message
                .headers()
                .get_single_or_empty(HttpHeaderCode::UserAgent),
            b"from-refused-stream",
        );
        assert!(codec.is_reusable());
    }
}

#[test]
fn bad_server_preface() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    frame::write_ping(&mut wire, 1, false).unwrap();
    let input = bytes(&wire);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &input, &mut trace).consumed,
        FRAME_HEADER_SIZE
    );
    assert!(!codec.is_reusable());
}

#[test]
fn stream_id_overflow() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    codec.set_next_egress_stream_id(i32::MAX as u32 - 10);
    let mut last = StreamId::UNSPECIFIED;
    while codec.is_reusable() {
        last = codec.create_stream();
    }
    assert_eq!(last.get(), i32::MAX as u64 - 2);
}

#[test]
fn no_ex_headers() {
    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    let mut wire = BufQueue::new(1024);
    downstream
        .generate_configured_settings(&mut wire, &mut Trace::default())
        .unwrap();
    let mut upstream = Http2Codec::new(TransportDirection::Upstream);
    let mut trace = Trace::default();
    let wire = bytes(&wire);
    assert_eq!(
        ingress_one(&mut upstream, &wire, &mut trace).consumed,
        wire.len()
    );
    assert_eq!(
        trace.events,
        [
            Seen::Frame(0, FrameType::SETTINGS.as_u8(), 18),
            Seen::Settings(vec![
                CodecSetting {
                    identifier: SettingsId::HeaderTableSize.as_u16(),
                    value: 4096,
                },
                CodecSetting {
                    identifier: SettingsId::MaxFrameSize.as_u16(),
                    value: 16_384,
                },
                CodecSetting {
                    identifier: SettingsId::MaxHeaderListSize.as_u16(),
                    value: 131_072,
                },
            ]),
        ]
    );
}

#[test]
fn basic_header() {
    let mut message = request(b"/guacamole");
    message
        .headers_mut()
        .set(HttpHeaderCode::Host, b"www.foo.com");
    message
        .headers_mut()
        .add(HttpHeaderCode::UserAgent, b"coolio");
    message.headers_mut().add(b"tab-hdr", b"coolio\tv2");
    message
        .headers_mut()
        .add(HttpHeaderCode::Connection, b"Love");
    message.set_secure(true);
    let mut capture = round_trip_message(&message, TransportDirection::Downstream, true);
    assert_eq!(capture.events, ["begin", "headers", "complete"]);
    let mut decoded = capture.messages.remove(0);
    assert!(decoded.is_request());
    assert_eq!(decoded.url().unwrap(), b"/guacamole");
    assert!(decoded.is_secure());
    assert_eq!(decoded.headers().len(), 3);
    assert_eq!(
        decoded
            .headers()
            .get_single_or_empty(HttpHeaderCode::UserAgent),
        b"coolio"
    );
    assert_eq!(
        decoded.headers().get_single_or_empty(b"tab-hdr" as &[u8]),
        b"coolio\tv2"
    );
    assert_eq!(
        decoded.headers().get_single_or_empty(HttpHeaderCode::Host),
        b"www.foo.com"
    );
    assert!(!decoded.headers().exists(HttpHeaderCode::Connection));
}

#[test]
fn generate_extra_headers() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let stream = sender.create_stream();
    let mut message = request(b"/fish_taco");
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentLength, b"157");
    let mut extra = HttpHeaders::new();
    extra.add(HttpHeaderCode::Priority, b"u=0");
    let mut output = BufQueue::new(65536);
    let generated = sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: true,
                extra_headers: Some(&extra),
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(generated.header_size.unwrap().uncompressed > 0);
    let wire = bytes(&output);
    let mut receiver = Http2Codec::new(TransportDirection::Downstream);
    open(&mut receiver, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        receiver
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    assert_eq!(callback.events, ["begin", "headers", "complete"]);
    let mut decoded = callback.messages.remove(0);
    assert_eq!(decoded.url().unwrap(), b"/fish_taco");
    assert_eq!(decoded.headers().len(), 3);
    assert_eq!(
        decoded
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentLength),
        b"157"
    );
    assert_eq!(
        decoded
            .headers()
            .get_single_or_empty(HttpHeaderCode::Priority),
        b"u=0"
    );
}

#[test]
fn generated_fields_preserve_order_duplicates_filtering_and_trailer_values() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let stream = sender.create_stream();
    let mut message = request(b"/ordered");
    message.set_secure(true);
    message.headers_mut().add(b"X-Repeat", b" \tfirst \t");
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b" text/plain ");
    message.headers_mut().add(b"x-repeat", b"second");
    message
        .headers_mut()
        .add(HttpHeaderCode::Connection, b"close");
    message
        .headers_mut()
        .add(b":ignored", b"message pseudo-field");
    let mut extra = HttpHeaders::new();
    extra.add(HttpHeaderCode::Host, b"ignored.example");
    extra.add(b"x-repeat", b"third");
    extra.add(HttpHeaderCode::KeepAlive, b"ignored");
    extra.add(b":path", b"/ignored");
    extra.add(b"x-extra", b" \tlast\t ");
    let mut output = BufQueue::new(4096);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: false,
                extra_headers: Some(&extra),
            },
            &mut Trace::default(),
        )
        .unwrap();
    drop(message);
    drop(extra);
    let parsed = parse_frame(&mut output).unwrap().unwrap();
    assert!(output.is_empty());
    assert_eq!(parsed.header.flags, Flags::END_HEADERS);
    let Frame::Headers(block) = parsed.frame else {
        panic!("expected one HEADERS frame");
    };
    let mut decoder = HpackCodec::new();
    let expected: Vec<_> = [
        (":method", "GET"),
        (":scheme", "https"),
        (":authority", "example.com"),
        (":path", "/ordered"),
        ("x-repeat", "first"),
        ("content-type", "text/plain"),
        ("x-repeat", "second"),
        ("x-repeat", "third"),
        ("x-extra", "last"),
    ]
    .into_iter()
    .map(|(name, value)| HpackHeader::new(name, value))
    .collect();
    assert_eq!(
        decoder
            .decode_buf_queue(&block, block.chain_length())
            .unwrap()
            .headers,
        expected
    );

    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-repeat", b"first");
    trailers.add(HttpHeaderCode::Host, b"ignored.example");
    trailers.add(b"x-repeat", b"sec \tond");
    trailers.add(HttpHeaderCode::TransferEncoding, b"chunked");
    trailers.add(b":ignored", b"trailer pseudo-field");
    trailers.add(b"x-extra", b"last");
    sender
        .generate(
            &mut output,
            EgressFrame::Trailers {
                stream,
                trailers: &trailers,
            },
            &mut Trace::default(),
        )
        .unwrap();
    drop(trailers);
    let parsed = parse_frame(&mut output).unwrap().unwrap();
    assert!(output.is_empty());
    assert_eq!(parsed.header.flags, Flags::END_HEADERS | Flags::END_STREAM);
    let Frame::Headers(block) = parsed.frame else {
        panic!("trailers use one HEADERS frame");
    };
    assert_eq!(
        decoder
            .decode_buf_queue(&block, block.chain_length())
            .unwrap()
            .headers,
        vec![
            HpackHeader::new("x-repeat", "first"),
            HpackHeader::new("x-repeat", "sec \tond"),
            HpackHeader::new("x-extra", "last"),
        ]
    );
}

pub(crate) fn encoded_headers(stream: u32, headers: &[HpackHeader], end_stream: bool) -> Vec<u8> {
    let encoded = HpackCodec::new().encode(headers);
    let mut wire = BufQueue::new(512 * 1024);
    frame::write_headers(&mut wire, queue(&encoded), stream, None, end_stream, true).unwrap();
    bytes(&wire)
}

#[test]
fn fragmented_data_shares_backing() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let headers = encoded_headers(1, &request_fields(b"/shared"), false);
    assert_eq!(
        ingress_one(&mut codec, &headers, &mut Trace::default()).consumed,
        headers.len(),
    );

    let data_header = [
        0,
        0,
        4,
        FrameType::DATA.as_u8(),
        Flags::END_STREAM,
        0,
        0,
        0,
        1,
    ];
    let first = Bytes::copy_from_slice(&data_header[..4]);
    let mut middle_bytes = data_header[4..].to_vec();
    middle_bytes.extend_from_slice(b"ab");
    let middle = Bytes::from(middle_bytes);
    let tail = Bytes::from_static(b"cd");
    let middle_body_pointer = middle.as_ptr().wrapping_add(data_header.len() - 4);
    let tail_body_pointer = tail.as_ptr();
    let input_length = first.len() + middle.len() + tail.len();
    let mut input = BufQueue::new(input_length);
    input.push_bytes(first).expect("frame prefix fits");
    input
        .push_bytes(middle.clone())
        .expect("middle segment fits");
    input.push_bytes(tail.clone()).expect("tail segment fits");
    let mut callback = SharedDataCapture::default();

    let result = codec.on_ingress(&input, &mut callback, &mut BufQueue::new(1024));

    assert_eq!(result.consumed, input_length);
    assert_eq!(input.segment_count(), 3);
    drop(input);
    drop(middle);
    drop(tail);
    assert_eq!(callback.segments.len(), 2);
    assert_eq!(callback.segments[0].as_ref(), b"ab");
    assert_eq!(callback.segments[0].as_ptr(), middle_body_pointer);
    assert_eq!(callback.segments[1].as_ref(), b"cd");
    assert_eq!(callback.segments[1].as_ptr(), tail_body_pointer);
}

fn padded_data_prefix(payload_prefix: &[u8]) -> Vec<u8> {
    let mut wire = vec![
        0,
        0,
        6,
        FrameType::DATA.as_u8(),
        Flags::PADDED | Flags::END_STREAM,
        0,
        0,
        0,
        1,
    ];
    wire.extend_from_slice(payload_prefix);
    wire
}

fn codec_with_open_request() -> Http2Codec {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let headers = encoded_headers(1, &request_fields(b"/padded"), false);
    assert_eq!(
        ingress_one(&mut codec, &headers, &mut Trace::default()).consumed,
        headers.len(),
    );
    codec
}

#[test]
fn padded_data_split_across_calls_preserves_body_order() {
    let mut codec = codec_with_open_request();
    let first = padded_data_prefix(b"\x01da");
    let mut callback = MessageCapture::default();
    let first_result = codec.on_ingress(&queue(&first), &mut callback, &mut BufQueue::new(1024));

    assert_eq!(first_result.consumed, first.len());
    assert_eq!(callback.bodies, [(2, 2)]);

    let second = b"ta\0";
    let second_result = codec.on_ingress(&queue(second), &mut callback, &mut BufQueue::new(1024));

    assert_eq!(second_result.consumed, second.len());
    assert_eq!(callback.bodies, [(2, 2), (2, 0)]);
    assert_eq!(callback.events, ["body", "body", "complete"]);
}

#[test]
fn fragmented_malformed_padding_suppresses_current_body_callback() {
    let mut codec = codec_with_open_request();
    let first = padded_data_prefix(b"\x01da");
    let mut callback = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&first), &mut callback, &mut BufQueue::new(1024),)
            .consumed,
        first.len(),
    );
    assert_eq!(callback.bodies, [(2, 2)]);

    let mut suffix = queue(b"ta\x01");
    let mut trailing = BufQueue::new(64);
    frame::write_ping(&mut trailing, 42, false).unwrap();
    let mut input = BufQueue::new(64);
    input.append(&mut suffix).unwrap();
    input.append(&mut trailing).unwrap();
    let result = codec.on_ingress(&input, &mut callback, &mut BufQueue::new(1024));

    assert_eq!(result.consumed, 2);
    assert_eq!(callback.bodies, [(2, 2)]);
    assert_eq!(callback.events, ["body", "error"]);
    assert_eq!(
        callback.error.as_ref().and_then(|error| error.1),
        Some(H2ErrorCode::PROTOCOL_ERROR),
    );
}

fn request_fields(path: &[u8]) -> Vec<HpackHeader> {
    vec![
        HpackHeader::new(b":method", b"GET"),
        HpackHeader::new(b":scheme", b"https"),
        HpackHeader::new(b":path", path),
        HpackHeader::new(b":authority", b"example.com"),
    ]
}

fn decode_fields(headers: Vec<HpackHeader>) -> Trace {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut trace = Trace::default();
    open(&mut codec, &mut trace);
    trace.events.clear();
    let wire = encoded_headers(1, &headers, true);
    assert_eq!(
        ingress_one(&mut codec, &wire, &mut trace).consumed,
        wire.len()
    );
    trace
}

fn decode_fields_capture(headers: Vec<HpackHeader>) -> MessageCapture {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let wire = encoded_headers(1, &headers, true);
    let mut callback = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    callback
}

fn assert_request_field_error(headers: Vec<HpackHeader>) {
    let trace = decode_fields(headers);
    assert!(
        trace
            .events
            .iter()
            .any(|event| matches!(event, Seen::Error(1, _, Some(400), true)))
    );
}

#[test]
fn bad_headers() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"connection", b"close"));
    assert_request_field_error(fields);
}

/// Decodes the request `fields` on stream 1 and then again on stream 3 (the
/// second block may come from the decoder memo) with a codec `configure` set
/// up, and returns what the callback saw.
fn decode_twice_with(
    fields: &[HpackHeader],
    configure: impl FnOnce(&mut Http2Codec),
) -> MessageCapture {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    configure(&mut codec);
    open(&mut codec, &mut Trace::default());
    let mut callback = MessageCapture::default();
    let mut encoder = HpackCodec::new();
    for stream in [1, 3] {
        let mut wire = BufQueue::new(64 * 1024);
        frame::write_headers(
            &mut wire,
            queue(&encoder.encode(fields)),
            stream,
            None,
            true,
            true,
        )
        .unwrap();
        let wire = bytes(&wire);
        assert_eq!(
            codec
                .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
                .consumed,
            wire.len()
        );
    }
    callback
}

fn request_with_method(method: &[u8]) -> Vec<HpackHeader> {
    let mut fields = request_fields(b"/generate");
    fields[0] = HpackHeader::new(b":method", method);
    fields
}

#[test]
fn exact_methods_keep_the_method_pseudo_header_byte_for_byte_when_enabled() {
    for (method, recognized) in [
        (b"POST".as_slice(), Some(HttpMethod::Post)),
        (b"post", None),
        (b"Post", None),
        (b"PURGE", None),
        (b"purge", None),
        (b"M-SEARCH", None),
        (b"FOO_BAR.1~", None),
    ] {
        let capture = decode_twice_with(&request_with_method(method), |codec| {
            codec.set_exact_methods(true);
        });
        assert_eq!(
            capture.events,
            [
                "begin", "headers", "complete", "begin", "headers", "complete"
            ],
            "{method:?}"
        );
        for mut message in capture.messages {
            assert_eq!(message.method_string().unwrap().as_bytes(), method);
            assert_eq!(message.method().unwrap(), recognized, "{method:?}");
        }
    }
}

#[test]
fn exact_methods_reject_a_method_that_is_not_a_token_when_enabled() {
    for method in [b"".as_slice(), b"P@ST", b"POST ", b"(GET)", b"GE\x7fT"] {
        let capture = decode_twice_with(&request_with_method(method), |codec| {
            codec.set_exact_methods(true);
        });
        assert!(capture.messages.is_empty(), "{method:?}");
        assert_eq!(
            capture
                .error
                .map(|(_, _, status, new_stream)| (status, new_stream)),
            Some((Some(400), true)),
            "{method:?}"
        );
    }
}

/// Every header but `Host` (which `:authority` sets), in order.
fn regular_headers(message: &HttpMessage) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut headers = Vec::new();
    message.headers().for_each_with_code(|code, name, value| {
        if code != HttpHeaderCode::Host {
            headers.push((name.to_ascii_lowercase(), value.to_vec()));
        }
    });
    headers
}

fn request_with_fields(fields: &[(&[u8], &[u8])]) -> Vec<HpackHeader> {
    let mut request = request_fields(b"/generate");
    request.extend(
        fields
            .iter()
            .map(|(name, value)| HpackHeader::new(name, value)),
    );
    request
}

const PADDED_FIELDS: [(&[u8], &[u8]); 8] = [
    (b"user-agent", b" ua\t"),
    (b"x-lead", b"  leading"),
    (b"x-trail", b"trailing\t"),
    (b"x-both", b"\t both "),
    (b"x-empty", b""),
    (b"x-only-space", b" \t "),
    (b"x-dup", b" one"),
    (b"x-dup", b"two "),
];

fn owned_fields(fields: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Vec<u8>)> {
    fields
        .iter()
        .map(|(name, value)| (name.to_vec(), value.to_vec()))
        .collect()
}

#[test]
fn verbatim_field_values_keep_surrounding_whitespace_when_enabled() {
    let capture = decode_twice_with(&request_with_fields(&PADDED_FIELDS), |codec| {
        codec.set_verbatim_field_values(true);
    });
    assert_eq!(capture.messages.len(), 2);
    for message in &capture.messages {
        assert_eq!(regular_headers(message), owned_fields(&PADDED_FIELDS));
        assert_eq!(
            message.headers().combine_default(b"x-dup"),
            b" one, two ".to_vec().into()
        );
    }
}

#[test]
fn field_values_are_trimmed_by_default() {
    let capture = decode_twice_with(&request_with_fields(&PADDED_FIELDS), |_| {});
    assert_eq!(capture.messages.len(), 2);
    for message in &capture.messages {
        assert_eq!(
            regular_headers(message),
            owned_fields(&[
                (b"user-agent", b"ua"),
                (b"x-lead", b"leading"),
                (b"x-trail", b"trailing"),
                (b"x-both", b"both"),
                (b"x-empty", b""),
                (b"x-only-space", b""),
                (b"x-dup", b"one"),
                (b"x-dup", b"two"),
            ])
        );
    }
}

/// Whether the request with the extra `fields` decodes, with verbatim values
/// when `verbatim`; a refused request is a stream error answered with 400.
fn field_values_accepted(fields: &[(&[u8], &[u8])], verbatim: bool) -> bool {
    let capture = decode_twice_with(&request_with_fields(fields), |codec| {
        codec.set_verbatim_field_values(verbatim);
    });
    if capture.messages.len() == 2 {
        return true;
    }
    assert_eq!(
        capture
            .error
            .map(|(_, _, status, new_stream)| (status, new_stream)),
        Some((Some(400), true)),
        "{fields:?}"
    );
    false
}

#[test]
fn verbatim_field_values_refuse_what_h2_refuses_when_enabled() {
    // h2 0.4 (`HeaderValue::from_bytes`) refuses a value byte below 0x20 other
    // than HTAB, and DEL; RFC 9113 section 8.2.1 forbids NUL, CR and LF.
    for value in [
        b"a\r\n b".as_slice(),
        b"abc\r\n ",
        b"a\rb",
        b"a\nb",
        b"a\0b",
        b"a\x01b",
        b"a\x7fb",
    ] {
        assert!(
            !field_values_accepted(&[(b"x-value", value)], true),
            "{value:?}"
        );
    }
    // h2 resets a stream whose content-length is not decimal digits (at most 19).
    for value in [b" 0".as_slice(), b"0 ", b"+0", b"00000000000000000000"] {
        assert!(
            !field_values_accepted(&[(b"content-length", value)], true),
            "{value:?}"
        );
    }
    // h2's `parse_u64` reads an empty content-length as zero.
    for value in [b"0".as_slice(), b"0000000000000000000", b""] {
        assert!(
            field_values_accepted(&[(b"content-length", value)], true),
            "{value:?}"
        );
    }
    // h2 resets a stream whose te is anything but exactly `trailers`.
    assert!(field_values_accepted(&[(b"te", b"trailers")], true));
    for value in [b"trailers ".as_slice(), b"gzip"] {
        assert!(!field_values_accepted(&[(b"te", value)], true), "{value:?}");
    }
    // Obsolete text is a field value byte h2 accepts.
    assert!(field_values_accepted(&[(b"x-value", b"\x80\xff")], true));
}

#[test]
fn verbatim_field_values_keep_strict_validation_when_enabled() {
    for (value, accepted) in [(b"\x80\xff".as_slice(), false), (b" v\t", true)] {
        let capture = decode_twice_with(&request_with_fields(&[(b"x-value", value)]), |codec| {
            codec.set_verbatim_field_values(true);
            codec.set_strict_validation(true);
        });
        if accepted {
            assert_eq!(capture.messages.len(), 2, "{value:?}");
            for message in &capture.messages {
                assert_eq!(
                    regular_headers(message),
                    owned_fields(&[(b"x-value", b" v\t")]),
                    "the padded value is kept"
                );
            }
            continue;
        }
        assert!(
            capture.messages.is_empty(),
            "strict validation refuses a byte above 0x7E: {value:?}"
        );
        assert_eq!(
            capture
                .error
                .map(|(_, _, status, new_stream)| (status, new_stream)),
            Some((Some(400), true))
        );
    }
}

#[test]
fn verbatim_field_values_keep_an_empty_content_length_when_enabled() {
    let capture = decode_twice_with(&request_with_fields(&[(b"content-length", b"")]), |codec| {
        codec.set_verbatim_field_values(true);
    });
    assert_eq!(capture.messages.len(), 2);
    for message in &capture.messages {
        assert_eq!(
            regular_headers(message),
            owned_fields(&[(b"content-length", b"")]),
            "the empty field is kept, as h2 keeps it"
        );
    }
}

/// The regular fields of a response head the codec generates, its values
/// inserted with `add_verbatim`, as HPACK decodes them (no `date`).
fn generated_padded_response_fields(verbatim: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut sender = Http2Codec::new(TransportDirection::Downstream);
    sender.set_verbatim_field_values(verbatim);
    let mut message = response(200);
    for (name, value) in [
        (b"x-request-id".as_slice(), b" rid-7\t".as_slice()),
        (b"x-padded", b" v\t"),
        (b"x-empty", b""),
    ] {
        message.headers_mut().add_verbatim(name, value);
    }
    let mut output = BufQueue::new(4096);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let parsed = parse_frame(&mut output).unwrap().unwrap();
    let Frame::Headers(block) = parsed.frame else {
        panic!("expected one HEADERS frame");
    };
    HpackCodec::new()
        .decode_buf_queue(&block, block.chain_length())
        .unwrap()
        .headers
        .into_iter()
        .map(|header| (header.name.as_bytes().to_vec(), header.value.to_vec()))
        .filter(|(name, _)| !name.starts_with(b":") && name != b"date")
        .collect()
}

#[test]
fn verbatim_field_values_are_generated_as_stored_when_enabled() {
    assert_eq!(
        generated_padded_response_fields(true),
        owned_fields(&[
            (b"x-request-id", b" rid-7\t"),
            (b"x-padded", b" v\t"),
            (b"x-empty", b""),
        ]),
        "as h2 writes a `HeaderValue`, byte for byte"
    );
    assert_eq!(
        generated_padded_response_fields(false),
        owned_fields(&[
            (b"x-request-id", b"rid-7"),
            (b"x-padded", b"v"),
            (b"x-empty", b""),
        ]),
        "by default the encoder trims surrounding spaces and tabs"
    );
}

#[test]
fn field_value_checks_are_unchanged_by_default() {
    for (value, accepted) in [
        (b"a\r\n b".as_slice(), true),
        (b"abc\r\n ", true),
        (b"a\rb", false),
        (b"a\nb", false),
        (b"a\0b", false),
        (b"a\x01b", false),
        (b"a\x7fb", false),
    ] {
        assert_eq!(
            field_values_accepted(&[(b"x-value", value)], false),
            accepted,
            "{value:?}"
        );
    }
    assert!(field_values_accepted(&[(b"content-length", b" 0")], false));
    assert!(field_values_accepted(&[(b"te", b"gzip")], false));
    let capture = decode_twice_with(&request_with_fields(&[(b"x-value", b"abc\r\n ")]), |_| {});
    assert_eq!(
        capture.messages[0]
            .headers()
            .get_single_or_empty(b"x-value"),
        b"abc"
    );
}

#[test]
fn verbatim_field_values_combine_cookie_crumbs_without_trimming_when_enabled() {
    let crumbs: [(&[u8], &[u8]); 2] = [(b"cookie", b" a=1"), (b"cookie", b"b=2 ")];
    for (verbatim, combined) in [(true, b" a=1; b=2 ".as_slice()), (false, b"a=1; b=2")] {
        let capture = decode_twice_with(&request_with_fields(&crumbs), |codec| {
            codec.set_verbatim_field_values(verbatim);
        });
        for message in &capture.messages {
            assert_eq!(message.headers().number_of_values(b"cookie"), 1);
            assert_eq!(message.headers().get_single_or_empty(b"cookie"), combined);
        }
    }
}

/// A field section `decoded_outcome` decodes. A request's fields follow `:method POST`,
/// `:scheme https`, `:authority a` and `:path /`, a method request's follow its `:method` and
/// those three, and the other sections' fields are the whole section.
#[derive(Clone, Copy, Debug)]
enum Section {
    Request,
    Method(&'static [u8]),
    WholeRequest,
    RequestTrailers,
    Response,
}

/// What `HeaderDecodeInfo` makes of `fields` in `section` with `validate`, `strict` and
/// `options` (`None`: as `HeaderDecodeInfo::new` alone builds it): the request line or status
/// and every field in order, or `!`, the parsing error and any Proxygen error and offending
/// value.
fn decoded_outcome(
    section: Section,
    fields: &[(&[u8], &[u8])],
    (validate, strict): (bool, bool),
    options: Option<FieldDecodeOptions>,
) -> String {
    use super::header_decode::HeaderDecodeInfo;

    let method = match section {
        Section::Method(method) => method,
        _ => b"POST",
    };
    let pseudo: [(&[u8], &[u8]); 4] = [
        (b":method", method),
        (b":scheme", b"https"),
        (b":authority", b"a"),
        (b":path", b"/"),
    ];
    let fields: Vec<HpackHeader> = match section {
        Section::Request | Section::Method(_) => pseudo.iter().chain(fields).collect(),
        _ => fields.iter().collect::<Vec<_>>(),
    }
    .into_iter()
    .map(|(name, value)| HpackHeader::new(name, value))
    .collect();
    let decoded = HpackCodec::new()
        .decode(&HpackCodec::new().encode(&fields))
        .unwrap();
    let (request, trailers) = match section {
        Section::RequestTrailers => (true, true),
        Section::Response => (false, false),
        _ => (true, false),
    };
    let info = HeaderDecodeInfo::new(request, trailers, validate, strict, false);
    let info = match options {
        Some(options) => info.with_options(options),
        None => info,
    };
    let mut message = match info.decode(decoded) {
        Ok(message) => message,
        Err(info) => {
            let mut outcome = format!("! {}", info.parsing_error.unwrap_or_default());
            if let Some(error) = info.proxygen_error {
                let error = match error {
                    ProxygenError::PARSE_HEADER => "PARSE_HEADER".to_owned(),
                    ProxygenError::HEADER_CONTENT_VALIDATION => {
                        "HEADER_CONTENT_VALIDATION".to_owned()
                    }
                    other => format!("{other:?}"),
                };
                outcome += &format!(" [{error}] {}", info.header_error_value.escape_ascii());
            }
            return outcome;
        }
    };
    let url = message.request_url().map(<[u8]>::to_vec);
    let mut outcome = match (url, message.response_status()) {
        (Some(url), _) => format!(
            "{} {}",
            message.method_string().unwrap(),
            url.escape_ascii()
        ),
        (None, Some(status)) => status.to_string(),
        (None, None) => String::new(),
    };
    message.headers().for_each(|name, value| {
        outcome += &format!(" {}={}", name.escape_ascii(), value.escape_ascii());
    });
    outcome
}

/// Which option a pinned section exercises.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Exercised {
    ExactMethods,
    VerbatimValues,
}

/// `validate` and `strict` of a pinned outcome.
const CHECKED: (bool, bool) = (true, false);
const STRICT: (bool, bool) = (true, true);
const UNCHECKED: (bool, bool) = (false, false);

/// A field section and, for each `validate` and `strict`, what current Roxygen makes of it
/// with its option off and on, byte for byte.
struct PinnedSection {
    section: Section,
    fields: &'static [(&'static [u8], &'static [u8])],
    exercised: Exercised,
    outcomes: &'static [((bool, bool), &'static str, &'static str)],
}

const PINNED_SECTIONS: &[PinnedSection] = &[
    PinnedSection {
        section: Section::Request,
        fields: &[
            (b"user-agent", b" ua\t"),
            (b"x-both", b"\t b "),
            (b"x-empty", b""),
            (b"x-blank", b" \t "),
        ],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "POST / Host=a User-Agent=ua x-both=b x-empty= x-blank=",
                "POST / Host=a User-Agent= ua\\t x-both=\\t b  x-empty= x-blank= \\t ",
            ),
            (
                STRICT,
                "POST / Host=a User-Agent=ua x-both=b x-empty= x-blank=",
                "POST / Host=a User-Agent= ua\\t x-both=\\t b  x-empty= x-blank= \\t ",
            ),
            (
                UNCHECKED,
                "POST / Host=a User-Agent=ua x-both=b x-empty= x-blank=",
                "POST / Host=a User-Agent= ua\\t x-both=\\t b  x-empty= x-blank= \\t ",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"x-value", b"abc\r\n ")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "POST / Host=a x-value=abc",
                "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] abc\\r\\n ",
            ),
            (
                STRICT,
                "POST / Host=a x-value=abc",
                "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] abc\\r\\n ",
            ),
            (
                UNCHECKED,
                "POST / Host=a x-value=abc",
                "POST / Host=a x-value=abc\\r\\n ",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"x-value", b"a\x01b")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] a\\x01b",
                "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] a\\x01b",
            ),
            (
                UNCHECKED,
                "POST / Host=a x-value=a\\x01b",
                "POST / Host=a x-value=a\\x01b",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"x-value", b"a\x7fb")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] a\\x7fb",
            "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] a\\x7fb",
        )],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"x-value", b"\x80\xff")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "POST / Host=a x-value=\\x80\\xff",
                "POST / Host=a x-value=\\x80\\xff",
            ),
            (
                STRICT,
                "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] \\x80\\xff",
                "! Invalid header name=x-value [HEADER_CONTENT_VALIDATION] \\x80\\xff",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"content-length", b"")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "POST / Host=a Content-Length=",
            "POST / Host=a Content-Length=",
        )],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"content-length", b" 5 ")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "POST / Host=a Content-Length=5",
                "! Invalid content-length",
            ),
            (
                UNCHECKED,
                "POST / Host=a Content-Length=5",
                "! Invalid content-length",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"content-length", b"5"), (b"content-length", b"5")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "POST / Host=a Content-Length=5",
            "POST / Host=a Content-Length=5",
        )],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"content-length", b"5"), (b"content-length", b"x")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "! Multiple content-length headers",
            "! Invalid content-length",
        )],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"te", b"trailers")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "POST / Host=a TE=trailers",
            "POST / Host=a TE=trailers",
        )],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"te", b" gzip")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "POST / Host=a TE=gzip",
                "! HTTP/2 Message with a TE other than trailers",
            ),
            (
                UNCHECKED,
                "POST / Host=a TE=gzip",
                "! HTTP/2 Message with a TE other than trailers",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"te", b"gz\x01ip")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "! Invalid header name=te [HEADER_CONTENT_VALIDATION] gz\\x01ip",
                "! HTTP/2 Message with a TE other than trailers",
            ),
            (
                STRICT,
                "! Invalid header name=te [HEADER_CONTENT_VALIDATION] gz\\x01ip",
                "! HTTP/2 Message with a TE other than trailers",
            ),
            (
                UNCHECKED,
                "POST / Host=a TE=gz\\x01ip",
                "! HTTP/2 Message with a TE other than trailers",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"connection", b"close")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "! HTTP/2 Message with Connection header",
            "! HTTP/2 Message with Connection header",
        )],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"host", b"a"), (b"host", b"b")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "! :authority/Host header mismatch",
            "! :authority/Host header mismatch",
        )],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"x bad", b" v")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (CHECKED, "POST / Host=a x bad=v", "POST / Host=a x bad= v"),
            (
                STRICT,
                "! Invalid header name=x bad [HEADER_CONTENT_VALIDATION]  v",
                "! Invalid header name=x bad [HEADER_CONTENT_VALIDATION]  v",
            ),
            (UNCHECKED, "POST / Host=a x bad=v", "POST / Host=a x bad= v"),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"cookie", b" a=1"), (b"cookie", b"b=2 ")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "POST / Host=a Cookie=a=1; b=2",
                "POST / Host=a Cookie= a=1; b=2 ",
            ),
            (
                UNCHECKED,
                "POST / Host=a Cookie=a=1; b=2",
                "POST / Host=a Cookie= a=1; b=2 ",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"", b" v")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "! Invalid header name= [PARSE_HEADER]  v",
                "! Invalid header name= [PARSE_HEADER]  v",
            ),
            (
                STRICT,
                "! Invalid header name= [PARSE_HEADER]  v",
                "! Invalid header name= [PARSE_HEADER]  v",
            ),
        ],
    },
    PinnedSection {
        section: Section::WholeRequest,
        fields: &[(b":method", b"GET"), (b"x-a", b"1"), (b":path", b"/")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "! Illegal pseudo header name=:path",
            "! Illegal pseudo header name=:path",
        )],
    },
    PinnedSection {
        section: Section::Response,
        fields: &[
            (b":status", b"200"),
            (b"x-a", b" v "),
            (b"content-length", b"7"),
        ],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            "200 x-a=v Content-Length=7",
            "200 x-a= v  Content-Length=7",
        )],
    },
    PinnedSection {
        section: Section::Response,
        fields: &[(b":status", b"200"), (b"te", b"gzip"), (b"x-b", b"a\x01b")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "! Invalid header name=x-b [HEADER_CONTENT_VALIDATION] a\\x01b",
                "! HTTP/2 Message with a TE other than trailers",
            ),
            (
                UNCHECKED,
                "200 TE=gzip x-b=a\\x01b",
                "! HTTP/2 Message with a TE other than trailers",
            ),
        ],
    },
    PinnedSection {
        section: Section::Request,
        fields: &[(b"x-b", b"a\x01b"), (b"te", b"gzip")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[
            (
                CHECKED,
                "! Invalid header name=x-b [HEADER_CONTENT_VALIDATION] a\\x01b",
                "! Invalid header name=x-b [HEADER_CONTENT_VALIDATION] a\\x01b",
            ),
            (
                UNCHECKED,
                "POST / Host=a x-b=a\\x01b TE=gzip",
                "! HTTP/2 Message with a TE other than trailers",
            ),
        ],
    },
    PinnedSection {
        section: Section::RequestTrailers,
        fields: &[(b"x-t", b" t "), (b"te", b"gzip")],
        exercised: Exercised::VerbatimValues,
        outcomes: &[(
            CHECKED,
            " x-t=t TE=gzip",
            "! HTTP/2 Message with a TE other than trailers",
        )],
    },
    PinnedSection {
        section: Section::Method(b"post"),
        fields: &[],
        exercised: Exercised::ExactMethods,
        outcomes: &[
            (CHECKED, "POST / Host=a", "post / Host=a"),
            (UNCHECKED, "POST / Host=a", "post / Host=a"),
        ],
    },
    PinnedSection {
        section: Section::Method(b"PURGE"),
        fields: &[],
        exercised: Exercised::ExactMethods,
        outcomes: &[(CHECKED, "PURGE / Host=a", "PURGE / Host=a")],
    },
    PinnedSection {
        section: Section::Method(b"purge"),
        fields: &[],
        exercised: Exercised::ExactMethods,
        outcomes: &[(CHECKED, "PURGE / Host=a", "purge / Host=a")],
    },
    PinnedSection {
        section: Section::Method(b"FOO_BAR.1~"),
        fields: &[],
        exercised: Exercised::ExactMethods,
        outcomes: &[
            (
                CHECKED,
                "! Invalid method: FOO_BAR.1~ [HEADER_CONTENT_VALIDATION] ",
                "FOO_BAR.1~ / Host=a",
            ),
            (
                UNCHECKED,
                "! Invalid method: FOO_BAR.1~ [HEADER_CONTENT_VALIDATION] ",
                "FOO_BAR.1~ / Host=a",
            ),
        ],
    },
    PinnedSection {
        section: Section::Method(b""),
        fields: &[],
        exercised: Exercised::ExactMethods,
        outcomes: &[(
            CHECKED,
            " / Host=a",
            "! Invalid method:  [HEADER_CONTENT_VALIDATION] ",
        )],
    },
    PinnedSection {
        section: Section::Method(b"-GET"),
        fields: &[],
        exercised: Exercised::ExactMethods,
        outcomes: &[(
            CHECKED,
            "! Invalid method: -GET [HEADER_CONTENT_VALIDATION] ",
            "-GET / Host=a",
        )],
    },
    PinnedSection {
        section: Section::Method(b"P@ST"),
        fields: &[],
        exercised: Exercised::ExactMethods,
        outcomes: &[(
            CHECKED,
            "! Invalid method: P@ST [HEADER_CONTENT_VALIDATION] ",
            "! Invalid method: P@ST [HEADER_CONTENT_VALIDATION] ",
        )],
    },
    PinnedSection {
        section: Section::WholeRequest,
        fields: &[
            (b":method", b"GET"),
            (b":method", b"get"),
            (b":scheme", b"https"),
            (b":path", b"/"),
        ],
        exercised: Exercised::ExactMethods,
        outcomes: &[(CHECKED, "! Duplicate method", "! Duplicate method")],
    },
];

#[test]
fn field_section_outcomes_are_pinned_with_each_option_off_and_on() {
    let exact = FieldDecodeOptions {
        exact_methods: true,
        verbatim_values: false,
    };
    let verbatim = FieldDecodeOptions {
        exact_methods: false,
        verbatim_values: true,
    };
    let both = FieldDecodeOptions {
        exact_methods: true,
        verbatim_values: true,
    };
    let mut mismatches = Vec::new();
    for case in PINNED_SECTIONS {
        let (on, other) = match case.exercised {
            Exercised::ExactMethods => (exact, verbatim),
            Exercised::VerbatimValues => (verbatim, exact),
        };
        for (config, off_outcome, on_outcome) in case.outcomes {
            for (label, options, expected) in [
                ("no options set", None, off_outcome),
                ("off", Some(FieldDecodeOptions::default()), off_outcome),
                ("other option on", Some(other), off_outcome),
                ("on", Some(on), on_outcome),
                ("both on", Some(both), on_outcome),
            ] {
                let actual = decoded_outcome(case.section, case.fields, *config, options);
                if actual != *expected {
                    mismatches.push(format!(
                        "{:?} {:?} {config:?} {label}:\n  actual   {actual:?}\n  expected {expected:?}",
                        case.section, case.fields
                    ));
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// A request section that both options change, with `validate` and `strict`, and what current
/// Roxygen makes of it with both off, with exact methods alone, with verbatim values alone and
/// with both on.
const PINNED_COMBINED: &[(Section, &[(&[u8], &[u8])], (bool, bool), [&str; 4])] = &[
    (
        Section::Method(b"post"),
        &[(b"x-a", b" v ")],
        CHECKED,
        [
            "POST / Host=a x-a=v",
            "post / Host=a x-a=v",
            "POST / Host=a x-a= v ",
            "post / Host=a x-a= v ",
        ],
    ),
    (
        Section::Method(b"FOO_BAR.1~"),
        &[(b"te", b"gzip")],
        CHECKED,
        [
            "! Invalid method: FOO_BAR.1~ [HEADER_CONTENT_VALIDATION] ",
            "FOO_BAR.1~ / Host=a TE=gzip",
            "! Invalid method: FOO_BAR.1~ [HEADER_CONTENT_VALIDATION] ",
            "! HTTP/2 Message with a TE other than trailers",
        ],
    ),
    (
        Section::Method(b"FOO_BAR.1~"),
        &[(b"te", b"gzip")],
        UNCHECKED,
        [
            "! Invalid method: FOO_BAR.1~ [HEADER_CONTENT_VALIDATION] ",
            "FOO_BAR.1~ / Host=a TE=gzip",
            "! Invalid method: FOO_BAR.1~ [HEADER_CONTENT_VALIDATION] ",
            "! HTTP/2 Message with a TE other than trailers",
        ],
    ),
];

#[test]
fn field_section_outcomes_are_pinned_with_both_options_on() {
    let mut mismatches = Vec::new();
    for (section, fields, config, expected) in PINNED_COMBINED {
        for (options, expected) in [(false, false), (true, false), (false, true), (true, true)]
            .into_iter()
            .map(|(exact_methods, verbatim_values)| FieldDecodeOptions {
                exact_methods,
                verbatim_values,
            })
            .zip(expected)
        {
            let actual = decoded_outcome(*section, fields, *config, Some(options));
            if actual != *expected {
                mismatches.push(format!(
                    "{section:?} {fields:?} {config:?} {options:?}:\n  actual   {actual:?}\n  expected {expected:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn request_methods_are_canonicalized_by_default() {
    for (method, canonical, recognized) in [
        (b"POST".as_slice(), "POST", Some(HttpMethod::Post)),
        (b"post", "POST", Some(HttpMethod::Post)),
        (b"Post", "POST", Some(HttpMethod::Post)),
        (b"purge", "PURGE", None),
    ] {
        let capture = decode_twice_with(&request_with_method(method), |_| {});
        assert_eq!(capture.messages.len(), 2, "{method:?}");
        for mut message in capture.messages {
            assert_eq!(message.method_string().unwrap(), canonical);
            assert_eq!(message.method().unwrap(), recognized);
        }
    }
}

#[test]
fn header_decode_retains_owned_message_and_public_result_type() {
    use super::header_decode::HeaderDecodeInfo;

    let check = |message: &HttpMessage| {
        assert_eq!(message.request_url(), Some(b"/decode-owned".as_slice()));
        assert_eq!(
            message.headers().get_single_or_empty(b"x-retained"),
            b"header-owned-across-decoder-drop"
        );
    };
    for malformed in [false, true] {
        let result: Result<HttpMessage, Box<HeaderDecodeInfo>> = {
            let mut fields = request_fields(b"/decode-owned");
            fields.push(HpackHeader::new(
                b"x-retained",
                b"header-owned-across-decoder-drop",
            ));
            if malformed {
                fields.push(HpackHeader::new(b"connection", b"close"));
            }
            let encoded = HpackCodec::new().encode(&fields);
            let decoded = HpackCodec::new().decode(&encoded).unwrap();
            HeaderDecodeInfo::new(true, false, true, true, false).decode(decoded)
        };
        match result {
            Ok(message) => {
                assert!(!malformed);
                check(&message);
            }
            Err(info) => {
                assert!(malformed);
                assert!(info.parsing_error.is_some());
                check(&info.message);
            }
        }
    }
}

#[test]
fn begin_mutation_survives_owned_and_partial_message_publication() {
    #[derive(Default)]
    struct MutateBegin(MessageCapture);

    impl HttpCodecCallback for MutateBegin {
        fn on_event(&mut self, mut event: CodecEvent<'_>) {
            if let CodecEvent::MessageBegin { message, .. } = &mut event {
                assert_eq!(message.advanced_protocol_string(), Some(b"h2".as_slice()));
                message
                    .headers_mut()
                    .set(b"x-begin", b"changed-before-publication");
            }
            self.0.on_event(event);
        }
    }

    for direction in [TransportDirection::Downstream, TransportDirection::Upstream] {
        for malformed in [false, true] {
            let mut callback = MutateBegin::default();
            {
                let mut codec = Http2Codec::new(direction);
                if direction.is_upstream() {
                    assert_eq!(codec.create_stream(), StreamId::new(1));
                }
                open(&mut codec, &mut Trace::default());
                let mut fields = if direction.is_downstream() {
                    request_fields(b"/begin-mutation")
                } else {
                    vec![HpackHeader::new(b":status", b"200")]
                };
                fields.push(HpackHeader::new(
                    b"x-retained",
                    b"header-owned-across-codec-drop",
                ));
                if malformed {
                    fields.push(HpackHeader::new(b"connection", b"close"));
                }
                let wire = encoded_headers(1, &fields, true);
                assert_eq!(
                    codec
                        .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
                        .consumed,
                    wire.len()
                );
            }
            let message = if malformed {
                assert_eq!(callback.0.events, ["begin", "error"]);
                callback.0.error_partial_message.as_ref().unwrap()
            } else {
                assert_eq!(callback.0.events, ["begin", "headers", "complete"]);
                assert!(callback.0.error.is_none());
                &callback.0.messages[0]
            };
            assert_eq!(
                message.headers().get_single_or_empty(b"x-begin"),
                b"changed-before-publication"
            );
            assert_eq!(
                message.headers().get_single_or_empty(b"x-retained"),
                b"header-owned-across-codec-drop"
            );
        }
    }
}

#[test]
fn malformed_downstream_headers_begin_before_ingress_error() {
    let mut fields = request_fields(b"/bad");
    fields.push(HpackHeader::new(b"user-agent", b"accepted-agent"));
    fields.push(HpackHeader::new(b"connection", b"close"));
    let wire = encoded_headers(1, &fields, true);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    assert_eq!(callback.events, ["begin", "error"]);
    assert_eq!(
        callback.error,
        Some((HttpExceptionDirection::Ingress, None, Some(400), true))
    );
    let partial = callback.error_partial_message.as_ref().unwrap();
    assert_eq!(partial.advanced_protocol_string(), Some(b"h2".as_slice()));
    assert_eq!(
        partial
            .headers()
            .get_single_or_empty(HttpHeaderCode::UserAgent),
        b"accepted-agent"
    );
    assert_eq!(codec.user_agent(), b"accepted-agent");
}

#[test]
fn invalid_http_fields_preserve_later_hpack_insertions() {
    let mut encoder = HpackCodec::new();
    let inserted = HpackHeader::new(b"user-agent", b"after-error-agent");
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"connection", b"close"));
    fields.push(inserted.clone());
    let encoded = encoder.encode(&fields);
    let mut wire = BufQueue::new(1024);
    frame::write_headers(&mut wire, queue(&encoded), 1, None, true, true).unwrap();
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut callback = MessageCapture::default();
    let wire = bytes(&wire);
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len(),
    );
    assert_eq!(callback.events, ["begin", "error"]);
    assert_eq!(
        callback.error,
        Some((HttpExceptionDirection::Ingress, None, Some(400), true))
    );

    let mut encoded = encoder.encode(&request_fields(b"/"));
    let indexed = encoder.encode(&[inserted]);
    assert_eq!(
        indexed,
        [0xbe],
        "the final field must reference the newest dynamic entry"
    );
    encoded.extend_from_slice(&indexed);
    let mut wire = BufQueue::new(1024);
    frame::write_headers(&mut wire, queue(&encoded), 3, None, true, true).unwrap();
    let wire = bytes(&wire);
    let mut callback = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len(),
    );
    assert_eq!(callback.events, ["begin", "headers", "complete"]);
    assert_eq!(
        callback.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::UserAgent),
        b"after-error-agent",
    );
}

#[test]
fn hpack_failure_takes_precedence_over_invalid_http_fields() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"connection", b"close"));
    let mut encoded = HpackCodec::new().encode(&fields);
    encoded.push(0x80);
    let mut wire = BufQueue::new(1024);
    frame::write_headers(&mut wire, queue(&encoded), 1, None, true, true).unwrap();
    let wire = bytes(&wire);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len(),
    );
    assert_eq!(callback.events, ["error"]);
    assert_eq!(
        callback.error,
        Some((
            HttpExceptionDirection::IngressAndEgress,
            Some(H2ErrorCode::COMPRESSION_ERROR),
            None,
            false
        )),
    );
    assert!(callback.error_partial_message.is_none());
    assert!(!codec.is_reusable());
}

#[test]
fn bad_pseudo_headers() {
    let mut fields = vec![HpackHeader::new(b"x-first", b"1")];
    fields.extend(request_fields(b"/"));
    assert_request_field_error(fields);
}

#[test]
fn bad_header_values() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"x-bad", b"bad\rvalue"));
    assert_request_field_error(fields);
}

#[test]
fn host_authority() {
    let mut equal = request_fields(b"/");
    equal.push(HpackHeader::new(b"host", b"example.com"));
    assert!(
        !decode_fields(equal)
            .events
            .iter()
            .any(|event| matches!(event, Seen::Error(..)))
    );
    let mut unequal = request_fields(b"/");
    unequal.push(HpackHeader::new(b"host", b"other.example"));
    assert_request_field_error(unequal);
}

#[test]
fn high_ascii() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"x-high", [0x80]));
    assert!(
        !decode_fields(fields)
            .events
            .iter()
            .any(|event| matches!(event, Seen::Error(..)))
    );
}

#[test]
fn empty_path() {
    assert_request_field_error(request_fields(b""));
}

#[test]
fn empty_header_name() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"", b"value"));
    assert_request_field_error(fields);
}

#[test]
fn normal_size_header() {
    let mut message = request(b"/");
    message.headers_mut().add(b"x-large", vec![b'a'; 32 * 1024]);
    let trace = round_trip_headers(&message, TransportDirection::Downstream, true);
    assert!(trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn oversized_header() {
    let mut message = request(b"/");
    message
        .headers_mut()
        .add(b"x-large", vec![b'a'; 129 * 1024]);
    let trace = round_trip_headers(&message, TransportDirection::Downstream, true);
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(0, Some(H2ErrorCode::COMPRESSION_ERROR), _, false)
    )));
}

#[test]
fn normal_size_frame() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let mut wire = BufQueue::new(32768);
    frame::write_data(&mut wire, queue(&vec![b'x'; 16384]), 1, None, false, false).unwrap();
    let input = bytes(&wire);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &input, &mut trace).consumed,
        input.len()
    );
}

#[test]
fn oversized_frame() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let mut header = vec![0, 0x40, 1, FrameType::DATA.as_u8(), 0, 0, 0, 0, 1];
    header.extend_from_slice(&vec![0; 0x4001]);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &header, &mut trace).consumed,
        FRAME_HEADER_SIZE
    );
    assert!(!codec.is_reusable());
}

#[test]
fn big_header_compressed() {
    let value = (0..20_000)
        .map(|index| b'a' + (index % 26) as u8)
        .collect::<Vec<_>>();
    let mut message = request(b"/");
    message.headers_mut().add(b"x-random", value);
    let trace = round_trip_headers(&message, TransportDirection::Downstream, true);
    assert!(trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn basic_header_reply() {
    let mut message = response(200);
    message.set_status_message(b"nifty-nice").unwrap();
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b"x-coolio");
    let capture = round_trip_message(&message, TransportDirection::Upstream, true);
    assert_eq!(capture.events, ["begin", "headers", "complete"]);
    let mut message = capture.messages[0].clone();
    assert_eq!(message.status_code().unwrap(), 200);
    assert_eq!(message.status_message().unwrap(), b"OK");
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentType),
        b"x-coolio"
    );
    assert!(message.headers().exists(HttpHeaderCode::Date));
}

#[test]
fn dont_double_date() {
    let mut message = response(200);
    message.headers_mut().add(HttpHeaderCode::Date, b"Today!");
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b"x-coolio");
    let capture = round_trip_message(&message, TransportDirection::Upstream, true);
    assert_eq!(capture.events, ["begin", "headers", "complete"]);
    assert_eq!(
        capture.messages[0]
            .headers()
            .number_of_values(HttpHeaderCode::Date),
        1
    );
    assert_eq!(
        capture.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::Date),
        b"Today!"
    );

    let mut sender = Http2Codec::new(TransportDirection::Downstream);
    let mut message = response(200);
    message.headers_mut().add(b"X-Repeat", b"main");
    let mut extra = HttpHeaders::new();
    extra.add(b"x-repeat", b"extra");
    extra.add(HttpHeaderCode::Date, b"Yesterday!");
    extra.add(b"X-Last", b"last");
    let mut output = BufQueue::new(4096);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: true,
                extra_headers: Some(&extra),
            },
            &mut Trace::default(),
        )
        .unwrap();
    drop(message);
    drop(extra);
    let parsed = parse_frame(&mut output).unwrap().unwrap();
    assert!(output.is_empty());
    assert_eq!(parsed.header.flags, Flags::END_HEADERS | Flags::END_STREAM);
    let Frame::Headers(block) = parsed.frame else {
        panic!("expected one response HEADERS frame");
    };
    assert_eq!(
        HpackCodec::new()
            .decode_buf_queue(&block, block.chain_length())
            .unwrap()
            .headers,
        vec![
            HpackHeader::new(":status", "200"),
            HpackHeader::new("x-repeat", "main"),
            HpackHeader::new("x-repeat", "extra"),
            HpackHeader::new("date", "Yesterday!"),
            HpackHeader::new("x-last", "last"),
        ]
    );
}

#[test]
fn bad_headers_reply() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    codec.create_stream();
    let mut trace = Trace::default();
    open(&mut codec, &mut trace);
    trace.events.clear();
    let wire = encoded_headers(
        1,
        &[
            HpackHeader::new(b":status", b"20"),
            HpackHeader::new(b"x", b"y"),
        ],
        true,
    );
    assert_eq!(
        ingress_one(&mut codec, &wire, &mut trace).consumed,
        wire.len()
    );
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(1, Some(H2ErrorCode::PROTOCOL_ERROR), _, true)
    )));
}

#[test]
fn strip_lws_header_value() {
    let mut message = request(b"/");
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b" application/x-fb");
    message
        .headers_mut()
        .add(HttpHeaderCode::UserAgent, b"\r\n\t ");
    let capture = round_trip_message(&message, TransportDirection::Downstream, true);
    assert_eq!(
        capture.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentType),
        b"application/x-fb"
    );
    assert_eq!(
        capture.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::UserAgent),
        b""
    );

    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"content-type", b"\tapplication/x-fb"));
    fields.push(HpackHeader::new(b"user-agent", b"\r\n\t "));
    let wire = encoded_headers(1, &fields, true);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut decoded = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut decoded, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    assert_eq!(
        decoded.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentType),
        b"application/x-fb"
    );
    assert_eq!(
        decoded.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::UserAgent),
        b""
    );
}

#[test]
fn cookies() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"cookie", b"a=1"));
    fields.push(HpackHeader::new(b"cookie", b"b=2"));
    fields.push(HpackHeader::new(b"cookie", b"c=three"));
    fields.push(HpackHeader::new(b"cookie", b"d=four"));
    let wire = encoded_headers(1, &fields, true);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    let message = &callback.messages[0];
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Cookie),
        b"a=1; b=2; c=three; d=four"
    );
    assert_eq!(&*message.cookie(b"a"), b"1");
    assert_eq!(&*message.cookie(b"b"), b"2");
    assert_eq!(&*message.cookie(b"c"), b"three");
    assert_eq!(&*message.cookie(b"d"), b"four");
}

#[test]
fn test_multiple_different_content_length_headers() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"content-length", b"1"));
    fields.push(HpackHeader::new(b"content-length", b"2"));
    assert_request_field_error(fields);
}

#[test]
fn test_multiple_identical_content_length_headers() {
    let mut fields = request_fields(b"/");
    fields.push(HpackHeader::new(b"content-length", b"2"));
    fields.push(HpackHeader::new(b"content-length", b"2"));
    assert!(
        !decode_fields(fields)
            .events
            .iter()
            .any(|event| matches!(event, Seen::Error(..)))
    );
}

#[test]
fn response_without_request() {
    let mut upstream = Http2Codec::new(TransportDirection::Upstream);
    let mut trace = Trace::default();
    open(&mut upstream, &mut trace);
    trace.events.clear();
    let wire = encoded_headers(1, &[HpackHeader::new(b":status", b"200")], true);
    assert_eq!(
        ingress_one(&mut upstream, &wire, &mut trace).consumed,
        wire.len()
    );
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(0, Some(H2ErrorCode::PROTOCOL_ERROR), _, false)
    )));
}

#[test]
fn generate_headers_with_empty_request() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut message = HttpMessage::new();
    let _ = message.client_address().unwrap();
    let mut output = BufQueue::new(1024);
    codec
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = bytes(&output);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(trace.events.contains(&Seen::Complete(1)));
}

fn connect_fields(extended: bool) -> Vec<HpackHeader> {
    let mut fields = vec![
        HpackHeader::new(b":method", b"CONNECT"),
        HpackHeader::new(b":authority", b"example.com:443"),
    ];
    if extended {
        fields.push(HpackHeader::new(b":scheme", b"https"));
        fields.push(HpackHeader::new(b":path", b"/chat"));
        fields.push(HpackHeader::new(b":protocol", b"websocket"));
    }
    fields
}

#[test]
fn basic_connect() {
    assert!(
        decode_fields(connect_fields(false))
            .events
            .contains(&Seen::Complete(1))
    );
}

#[test]
fn generated_connect_headers_preserve_pseudo_fields_and_authority() {
    let cases: &[(bool, bool, &[(&str, &str)])] = &[
        (
            false,
            false,
            &[(":method", "CONNECT"), (":authority", "example.com:443")],
        ),
        (false, true, &[(":method", "CONNECT")]),
        (
            true,
            false,
            &[
                (":method", "CONNECT"),
                (":scheme", "https"),
                (":authority", "example.com:443"),
                (":path", "/chat"),
                (":protocol", "websocket"),
            ],
        ),
        (
            true,
            true,
            &[
                (":method", "CONNECT"),
                (":scheme", "https"),
                (":path", "/chat"),
                (":protocol", "websocket"),
            ],
        ),
    ];
    for &(extended, duplicate_host, expected) in cases {
        let mut message = request(b"/chat");
        message.set_method(HttpMethod::Connect).unwrap();
        message.set_secure(true);
        message
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com:443");
        if duplicate_host {
            message
                .headers_mut()
                .add(HttpHeaderCode::Host, b"second.example");
        }
        if extended {
            message.set_upgrade_protocol(b"websocket");
        }
        message.headers_mut().add(b"x-last", b"kept");
        let mut sender = Http2Codec::new(TransportDirection::Upstream);
        let stream = sender.create_stream();
        let mut output = BufQueue::new(4096);
        sender
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .unwrap();
        drop(message);
        let parsed = parse_frame(&mut output).unwrap().unwrap();
        assert!(output.is_empty());
        let Frame::Headers(block) = parsed.frame else {
            panic!("expected CONNECT HEADERS");
        };
        let mut expected: Vec<_> = expected
            .iter()
            .map(|(name, value)| HpackHeader::new(name, value))
            .collect();
        expected.push(HpackHeader::new("x-last", "kept"));
        assert_eq!(
            HpackCodec::new()
                .decode_buf_queue(&block, block.chain_length())
                .unwrap()
                .headers,
            expected,
        );
    }
}

#[test]
fn bad_connect() {
    let mut fields = connect_fields(false);
    fields.push(HpackHeader::new(b":scheme", b"https"));
    assert_request_field_error(fields);
}

#[test]
fn template_driven_connect() {
    assert!(
        decode_fields(connect_fields(true))
            .events
            .contains(&Seen::Complete(1))
    );
}

#[test]
fn http2_enable_connect() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    codec
        .egress_settings_mut()
        .set(SettingsId::EnableConnectProtocol, 1);
    assert_eq!(
        codec
            .egress_settings_mut()
            .value_or(SettingsId::EnableConnectProtocol, 0),
        1
    );
}

#[test]
fn websocket_upgrade() {
    let mut capture = decode_fields_capture(connect_fields(true));
    assert_eq!(capture.events, ["begin", "headers", "complete"]);
    let mut message = capture.messages.remove(0);
    assert!(message.is_ingress_websocket_upgrade());
    assert_eq!(message.method().unwrap(), Some(HttpMethod::Connect));
    assert_eq!(message.url().unwrap(), b"/chat");
    assert_eq!(message.upgrade_protocol(), Some(b"websocket".as_slice()));
}

#[test]
fn websocket_bad_header() {
    let mut fields = connect_fields(true);
    fields.push(HpackHeader::new(b":protocol", b"other"));
    let trace = decode_fields(fields);
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Begin(1)))
            .count(),
        1
    );
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Error(1, _, Some(400), true)))
            .count(),
        1
    );
    assert!(!trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn websocket_duplicate_protocol() {
    let mut fields = connect_fields(true);
    fields.push(HpackHeader::new(b":protocol", b"websocket"));
    let callback = decode_fields_capture(fields);
    assert_eq!(callback.events, ["begin", "error"]);
    assert!(callback.messages.is_empty());
    assert!(callback.bodies.is_empty());
    assert_eq!(
        callback.error,
        Some((HttpExceptionDirection::Ingress, None, Some(400), true))
    );
}

#[test]
fn websocket_incorrect_response() {
    let mut message = response(400);
    message.set_status_message(b"Bad Request").unwrap();
    let mut capture = round_trip_message(&message, TransportDirection::Upstream, true);
    assert_eq!(capture.events, ["begin", "headers", "complete"]);
    let mut message = capture.messages.remove(0);
    assert_eq!(message.status_code().unwrap(), 400);
    assert!(!message.is_ingress_websocket_upgrade());
    assert!(capture.error.is_none());
}

fn continuation_wire(
    stream: u32,
    splits: &[usize],
    end_stream: bool,
    wrong_stream: Option<u32>,
) -> Vec<u8> {
    let encoded = HpackCodec::new().encode(&request_fields(b"/continued"));
    let mut output = BufQueue::new(512 * 1024);
    let first = splits[0].min(encoded.len());
    frame::write_headers(
        &mut output,
        queue(&encoded[..first]),
        stream,
        None,
        end_stream,
        first == encoded.len(),
    )
    .unwrap();
    let mut offset = first;
    for (index, requested) in splits[1..].iter().enumerate() {
        if offset == encoded.len() {
            break;
        }
        let amount = (*requested).min(encoded.len() - offset);
        let final_frame = offset + amount == encoded.len();
        frame::write_continuation(
            &mut output,
            wrong_stream.filter(|_| index == 0).unwrap_or(stream),
            final_frame,
            queue(&encoded[offset..offset + amount]),
        )
        .unwrap();
        offset += amount;
    }
    if offset < encoded.len() {
        frame::write_continuation(&mut output, stream, true, queue(&encoded[offset..])).unwrap();
    }
    bytes(&output)
}

fn run_downstream_wire(wire: &[u8]) -> (Http2Codec, Trace, usize) {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut trace = Trace::default();
    open(&mut codec, &mut trace);
    trace.events.clear();
    let consumed = ingress_one(&mut codec, wire, &mut trace).consumed;
    (codec, trace, consumed)
}

#[test]
fn basic_continuation() {
    let wire = continuation_wire(1, &[1, usize::MAX], false, None);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(
        trace
            .events
            .contains(&Seen::Headers(1, true, 0, b"/continued".to_vec()))
    );
}

#[test]
fn continuation_chain() {
    let wire = continuation_wire(1, &[1, 1, 1, 1], false, None);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(trace.events.contains(&Seen::Begin(1)));
}

#[test]
fn basic_continuation_end_stream() {
    let wire = continuation_wire(1, &[1, usize::MAX], true, None);
    let (_, trace, _) = run_downstream_wire(&wire);
    assert!(trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn bad_continuation() {
    let mut output = BufQueue::new(1024);
    frame::write_continuation(&mut output, 1, true, queue(b"bad")).unwrap();
    let wire = bytes(&output);
    let (codec, _, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(!codec.is_reusable());
}

#[test]
fn missing_continuation() {
    let encoded = HpackCodec::new().encode(&request_fields(b"/"));
    let mut output = BufQueue::new(1024);
    frame::write_headers(&mut output, queue(&encoded[..1]), 1, None, false, false).unwrap();
    frame::write_ping(&mut output, 7, false).unwrap();
    let wire = bytes(&output);
    let (codec, _, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(!codec.is_reusable());
}

#[test]
fn missing_continuation_bad_frame() {
    let encoded = HpackCodec::new().encode(&request_fields(b"/big"));
    let mut output = BufQueue::new(1024);
    frame::write_headers(&mut output, queue(&encoded[..1]), 1, None, false, false).unwrap();
    let mut wire = bytes(&output);
    wire.extend_from_slice(&[0xfa, 0, 0, FrameType::DATA.as_u8(), 0, 0, 0, 0, 1]);
    let (codec, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(!codec.is_reusable());
    assert!(!trace.events.iter().any(|event| matches!(
        event,
        Seen::Begin(_) | Seen::Headers(..) | Seen::Complete(_)
    )));
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Error(0, _, _, false)))
            .count(),
        1
    );
}

#[test]
fn bad_continuation_stream() {
    let wire = continuation_wire(1, &[1, usize::MAX], false, Some(3));
    let (codec, _, _) = run_downstream_wire(&wire);
    assert!(!codec.is_reusable());
}

fn many_continuations(count: usize) -> Vec<u8> {
    let mut output = BufQueue::new(16 * 1024);
    frame::write_headers(&mut output, BufQueue::new(0), 1, None, false, false).unwrap();
    for index in 0..count {
        frame::write_continuation(
            &mut output,
            1,
            index + 1 == count,
            queue(if index + 1 == count {
                b"\x82\x87\x84"
            } else {
                b""
            }),
        )
        .unwrap();
    }
    bytes(&output)
}

#[test]
fn continuation_limit() {
    let wire = many_continuations(101);
    let (codec, trace, _) = run_downstream_wire(&wire);
    assert!(!codec.is_reusable());
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(0, Some(H2ErrorCode::ENHANCE_YOUR_CALM), _, false)
    )));
}

#[test]
fn continuation_under_limit() {
    let wire = many_continuations(100);
    let (codec, _, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(codec.is_reusable());
}

#[test]
fn normal_1024_continuation() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let stream = sender.create_stream();
    let mut message = request(b"/");
    let mut value = vec![b'!'; 8691];
    value.extend_from_slice(&vec![b'@'; 8691]);
    message.headers_mut().add(b"x-headr", &value);
    let mut output = BufQueue::new(64 * 1024);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();

    let wire = bytes(&output);
    let mut receiver = Http2Codec::new(TransportDirection::Downstream);
    open(&mut receiver, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        receiver
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    assert_eq!(callback.events, ["begin", "headers"]);
    assert!(callback.error.is_none());
    let mut decoded = callback.messages.remove(0);
    assert_eq!(decoded.url().unwrap(), b"/");
    assert_eq!(
        decoded.headers().get_single_or_empty(b"x-headr" as &[u8]),
        value
    );

    let mut frames = queue(&wire);
    let mut frame_types = Vec::new();
    while let Some(parsed) = parse_frame(&mut frames).unwrap() {
        frame_types.push(parsed.header.frame_type);
    }
    assert_eq!(frame_types, [FrameType::HEADERS, FrameType::CONTINUATION]);

    let mut ack = BufQueue::new(1024);
    sender
        .generate(&mut ack, EgressFrame::SettingsAck, &mut Trace::default())
        .unwrap();
    let mut trace = Trace::default();
    let ack = bytes(&ack);
    assert_eq!(
        ingress_one(&mut receiver, &ack, &mut trace).consumed,
        ack.len()
    );
    assert!(trace.events.contains(&Seen::SettingsAck));
}

#[test]
fn frame_too_large() {
    let wire = [0, 0x80, 0, FrameType::DATA.as_u8(), 0, 0, 0, 0, 1];
    let (codec, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, FRAME_HEADER_SIZE);
    assert!(!codec.is_reusable());
    assert!(!trace.events.iter().any(|event| matches!(
        event,
        Seen::Begin(_) | Seen::Headers(..) | Seen::Complete(_)
    )));
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(0, Some(H2ErrorCode::FRAME_SIZE_ERROR), _, false)
    )));
}

#[test]
fn data_frame_zero_length_with_eom() {
    let mut output = BufQueue::new(1024);
    frame::write_data(&mut output, BufQueue::new(0), 1, None, true, false).unwrap();
    let wire = bytes(&output);
    let (_, trace, _) = run_downstream_wire(&wire);
    assert!(trace.events.contains(&Seen::Complete(1)));
    assert!(
        !trace
            .events
            .iter()
            .any(|event| matches!(event, Seen::Body(..)))
    );
}

#[test]
fn unknown_frame_type() {
    let mut wire = CONNECTION_PREFACE.to_vec();
    wire.extend_from_slice(&empty_settings());
    wire.extend_from_slice(&[0, 0, 3, 0xfe, 0, 0, 0, 0, 0, 1, 2, 3]);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut trace = Trace::default();
    let consumed = ingress_one(&mut codec, &wire, &mut trace).consumed;
    assert_eq!(consumed, wire.len());
    assert!(codec.is_reusable());
    assert_eq!(
        trace.events,
        [
            Seen::Frame(0, FrameType::SETTINGS.as_u8(), 0),
            Seen::Settings(Vec::new()),
            Seen::Frame(0, 0xfe, 3),
        ]
    );
}

#[test]
fn junk_after_connection_error() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut trace = Trace::default();
    let mut bad = CONNECTION_PREFACE.to_vec();
    bad[2] ^= 1;
    assert_eq!(
        ingress_one(&mut codec, &bad, &mut trace).consumed,
        bad.len()
    );
    assert_eq!(ingress_one(&mut codec, b"junk", &mut trace).consumed, 0);
}

fn data_wire(data: &[u8], padding: Option<u8>, end: bool) -> Vec<u8> {
    let mut output = BufQueue::new(512 * 1024);
    frame::write_data(&mut output, queue(data), 1, padding, end, false).unwrap();
    bytes(&output)
}

#[test]
fn basic_data() {
    let wire = data_wire(b"hello", None, false);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert!(trace.events.contains(&Seen::Body(1, 5, 0)));
}

#[test]
fn long_data() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let mut output = BufQueue::new(128 * 1024);
    sender
        .generate(
            &mut output,
            EgressFrame::Body {
                stream: StreamId::new(1),
                body: queue(&vec![b'x'; 64 * 1024]),
                padding: None,
                end_stream: true,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = bytes(&output);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert_eq!(
        trace
            .events
            .iter()
            .filter_map(|event| match event {
                Seen::Body(_, length, _) => Some(*length),
                _ => None,
            })
            .sum::<usize>(),
        64 * 1024
    );
}

#[test]
fn malformed_padding_length() {
    let wire = [0, 0, 1, 0, Flags::PADDED, 0, 0, 0, 1, 2, 0];
    let (codec, _, _) = run_downstream_wire(&wire);
    assert!(!codec.is_reusable());
}

#[test]
fn malformed_padding() {
    let mut wire = data_wire(b"x", Some(2), false);
    *wire.last_mut().unwrap() = 1;
    let data_length = wire.len();
    let mut trailing = BufQueue::new(64);
    frame::write_ping(&mut trailing, 42, false).unwrap();
    wire.extend_from_slice(&bytes(&trailing));
    let (codec, trace, consumed) = run_downstream_wire(&wire);
    assert!(!codec.is_reusable());
    assert_eq!(consumed, data_length);
    assert!(!trace.events.contains(&Seen::Ping(false, 42)));
    assert!(
        !trace
            .events
            .iter()
            .any(|event| matches!(event, Seen::Body(..)))
    );
}

#[test]
fn no_application_byte() {
    let wire = [
        0,
        0,
        86,
        FrameType::DATA.as_u8(),
        Flags::PADDED,
        0,
        0,
        0,
        1,
        85,
        0,
    ];
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire, &mut trace).consumed,
        wire.len()
    );
    assert!(trace.events.contains(&Seen::Body(1, 0, 86)));
}

#[test]
fn data_frame_partial_data_on_frame_header_call() {
    let wire = data_wire(b"abcdef", None, true);
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire[..12], &mut trace).consumed,
        12
    );
    assert!(trace.events.contains(&Seen::Body(1, 3, 0)));
    assert_eq!(
        ingress_one(&mut codec, &wire[12..], &mut trace).consumed,
        wire.len() - 12
    );
    assert!(trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn data_frame_partial_data_with_no_application_byte() {
    let wire = data_wire(b"a", Some(2), true);
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire[..FRAME_HEADER_SIZE + 1], &mut trace).consumed,
        FRAME_HEADER_SIZE
    );
    assert_eq!(
        ingress_one(&mut codec, &wire[FRAME_HEADER_SIZE..], &mut trace).consumed,
        wire.len() - FRAME_HEADER_SIZE
    );
    assert!(trace.events.contains(&Seen::Body(1, 1, 3)));
}

#[test]
fn basic_rst() {
    let mut wire = BufQueue::new(1024);
    frame::write_rst_stream(&mut wire, 1, H2ErrorCode::CANCEL).unwrap();
    let input = bytes(&wire);
    let (_, trace, _) = run_downstream_wire(&input);
    assert!(trace.events.contains(&Seen::Abort(1, H2ErrorCode::CANCEL)));
}

#[test]
fn basic_rst_invalid_code() {
    let mut wire = BufQueue::new(1024);
    frame::write_rst_stream(&mut wire, 1, H2ErrorCode::from_u32(99)).unwrap();
    let input = bytes(&wire);
    let (codec, _, _) = run_downstream_wire(&input);
    assert!(!codec.is_reusable());
}

#[test]
fn basic_ping() {
    let mut wire = BufQueue::new(1024);
    frame::write_ping(&mut wire, 0x0102_0304_0506_0708, false).unwrap();
    frame::write_ping(&mut wire, 7, true).unwrap();
    let input = bytes(&wire);
    let (_, trace, _) = run_downstream_wire(&input);
    assert!(
        trace
            .events
            .contains(&Seen::Ping(false, 0x0102_0304_0506_0708))
    );
    assert!(trace.events.contains(&Seen::Ping(true, 7)));
}

#[test]
fn many_one_byte_segments_parse_multiple_frames() {
    let mut wire = BufQueue::new(64);
    frame::write_ping(&mut wire, 11, false).expect("first PING fits");
    frame::write_ping(&mut wire, 22, false).expect("second PING fits");
    let wire = bytes(&wire);
    let mut input = BufQueue::new(wire.len());
    for byte in &wire {
        input
            .push_bytes(Bytes::copy_from_slice(&[*byte]))
            .expect("one-byte frame segment fits");
    }
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut trace = Trace::default();

    let result = codec.on_ingress(&input, &mut trace, &mut BufQueue::new(1024));

    assert_eq!(result.consumed, wire.len());
    assert_eq!(input.segment_count(), wire.len());
    assert!(trace.events.contains(&Seen::Ping(false, 11)));
    assert!(trace.events.contains(&Seen::Ping(false, 22)));
}

#[test]
fn segmented_data_continues_at_the_following_frame() {
    let mut wire = data_wire(b"abcdef", Some(2), true);
    let mut ping = BufQueue::new(64);
    frame::write_ping(&mut ping, 42, false).unwrap();
    wire.extend_from_slice(&bytes(&ping));
    for first_length in [
        0,
        FRAME_HEADER_SIZE + 1,
        FRAME_HEADER_SIZE + 4,
        FRAME_HEADER_SIZE + 7,
    ] {
        let mut codec = codec_with_open_request();
        let mut trace = Trace::default();
        let first = ingress_one(&mut codec, &wire[..first_length], &mut trace).consumed;
        let mut remainder = BufQueue::new(wire.len() - first);
        for byte in &wire[first..] {
            remainder.push_copy(&[*byte]).unwrap();
        }

        let result = codec.on_ingress(&remainder, &mut trace, &mut BufQueue::new(1024));

        assert_eq!(first + result.consumed, wire.len());
        assert!(codec.is_reusable());
        let (length, padding) = trace.events.iter().fold((0, 0), |total, event| {
            if let Seen::Body(1, length, padding) = event {
                (total.0 + length, total.1 + padding)
            } else {
                total
            }
        });
        assert_eq!((length, padding), (6, 3));
        assert!(trace.events.contains(&Seen::Complete(1)));
        assert_eq!(trace.events.last(), Some(&Seen::Ping(false, 42)));
    }
}

#[test]
fn basic_window() {
    let mut wire = BufQueue::new(1024);
    frame::write_window_update(&mut wire, 0, 10).unwrap();
    frame::write_window_update(&mut wire, 0, 0x7fff_ffff).unwrap();
    frame::write_window_update(&mut wire, 1, 12).unwrap();
    frame::write_window_update(&mut wire, 1, 0x7fff_ffff).unwrap();
    let input = bytes(&wire);
    let (codec, trace, consumed) = run_downstream_wire(&input);
    assert_eq!(consumed, input.len());
    assert_eq!(
        trace
            .events
            .into_iter()
            .filter(|event| matches!(event, Seen::Window(..)))
            .collect::<Vec<_>>(),
        [
            Seen::Window(0, 10),
            Seen::Window(0, 0x7fff_ffff),
            Seen::Window(1, 12),
            Seen::Window(1, 0x7fff_ffff),
        ]
    );
    assert!(codec.is_reusable());
}

#[test]
fn zero_window() {
    let wire = [
        0,
        0,
        4,
        FrameType::WINDOW_UPDATE.as_u8(),
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
    ];
    let (codec, trace, _) = run_downstream_wire(&wire);
    assert!(!codec.is_reusable());
    assert!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Error(..)))
            .count()
            >= 2
    );
}

#[test]
fn basic_goaway() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    assert_eq!(
        sender
            .generate(
                &mut wire,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(17),
                    code: H2ErrorCode::ENHANCE_YOUR_CALM,
                    debug_data: Some(queue(b"debugData")),
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        26
    );
    let input = bytes(&wire);
    let (codec, trace, consumed) = run_downstream_wire(&input);
    assert_eq!(consumed, input.len());
    assert_eq!(
        trace.events,
        [
            Seen::Frame(0, FrameType::GOAWAY.as_u8(), 17),
            Seen::Goaway(17, H2ErrorCode::ENHANCE_YOUR_CALM, b"debugData".to_vec()),
        ]
    );
    assert!(!codec.is_reusable());
}

#[test]
fn bad_goaway() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut wire = BufQueue::new(1024);
    assert_eq!(
        codec
            .generate(
                &mut wire,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(17),
                    code: H2ErrorCode::ENHANCE_YOUR_CALM,
                    debug_data: Some(queue(b"debugData")),
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        26
    );
    let before = wire.chain_length();
    assert_eq!(
        codec
            .generate(
                &mut wire,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(27),
                    code: H2ErrorCode::ENHANCE_YOUR_CALM,
                    debug_data: None,
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        0
    );
    assert_eq!(wire.chain_length(), before);
}

#[test]
fn rst_stream_no_error() {
    let mut wire = BufQueue::new(1024);
    frame::write_rst_stream(&mut wire, 1, H2ErrorCode::NO_ERROR).unwrap();
    let input = bytes(&wire);
    let (_, trace, _) = run_downstream_wire(&input);
    assert!(
        trace
            .events
            .contains(&Seen::Abort(1, H2ErrorCode::NO_ERROR))
    );
}

#[test]
fn double_goaway() {
    let mut upstream = Http2Codec::new(TransportDirection::Upstream);
    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    open(&mut upstream, &mut Trace::default());
    open(&mut downstream, &mut Trace::default());
    downstream.enable_double_goaway_drain();

    let mut wire = BufQueue::new(1024);
    downstream
        .generate(
            &mut wire,
            EgressFrame::Goaway {
                last_stream: StreamId::MAX,
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(downstream.is_waiting_to_drain());
    assert!(downstream.is_reusable());
    for stream in [0, 1, 2] {
        assert!(downstream.is_stream_ingress_egress_allowed(StreamId::new(stream)));
    }
    let first = bytes(&wire);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut upstream, &first, &mut trace).consumed,
        first.len()
    );
    assert!(!upstream.is_reusable());
    assert!(trace.events.contains(&Seen::Goaway(
        i32::MAX as u64,
        H2ErrorCode::NO_ERROR,
        Vec::new()
    )));

    let mut wire = BufQueue::new(1024);
    downstream
        .generate(
            &mut wire,
            EgressFrame::Goaway {
                last_stream: StreamId::UNSPECIFIED,
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(!downstream.is_waiting_to_drain());
    assert!(!downstream.is_reusable());
    assert!(downstream.is_stream_ingress_egress_allowed(StreamId::UNSPECIFIED));
    assert!(!downstream.is_stream_ingress_egress_allowed(StreamId::new(1)));
    assert!(downstream.is_stream_ingress_egress_allowed(StreamId::new(2)));
    let second = bytes(&wire);
    assert_eq!(
        ingress_one(&mut upstream, &second, &mut trace).consumed,
        second.len()
    );
    assert!(!upstream.is_stream_ingress_egress_allowed(StreamId::new(1)));
    assert!(upstream.is_stream_ingress_egress_allowed(StreamId::new(2)));
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Goaway(..)))
            .count(),
        2
    );

    let mut reverse = BufQueue::new(1024);
    upstream
        .generate(
            &mut reverse,
            EgressFrame::Goaway {
                last_stream: StreamId::UNSPECIFIED,
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let reverse = bytes(&reverse);
    assert_eq!(
        ingress_one(&mut downstream, &reverse, &mut Trace::default()).consumed,
        reverse.len()
    );
    assert!(!downstream.is_stream_ingress_egress_allowed(StreamId::new(1)));
    assert!(!downstream.is_stream_ingress_egress_allowed(StreamId::new(2)));
}

#[test]
fn double_goaway_with_error() {
    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    downstream.enable_double_goaway_drain();
    let mut wire = BufQueue::new(1024);
    downstream
        .generate(
            &mut wire,
            EgressFrame::Goaway {
                last_stream: StreamId::MAX,
                code: H2ErrorCode::ENHANCE_YOUR_CALM,
                debug_data: Some(queue(b"debugData")),
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(!downstream.is_reusable());
    assert!(!downstream.is_waiting_to_drain());
    assert_eq!(
        downstream
            .generate(
                &mut wire,
                EgressFrame::Goaway {
                    last_stream: StreamId::UNSPECIFIED,
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        0
    );
    let input = bytes(&wire);
    let (_, trace, consumed) = run_downstream_wire(&input);
    assert_eq!(consumed, input.len());
    assert!(trace.events.contains(&Seen::Goaway(
        0,
        H2ErrorCode::ENHANCE_YOUR_CALM,
        b"debugData".to_vec()
    )));
}

#[test]
fn goaway_handling() {
    let mut upstream = Http2Codec::new(TransportDirection::Upstream);
    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    open(&mut upstream, &mut Trace::default());
    open(&mut downstream, &mut Trace::default());

    upstream
        .egress_settings_mut()
        .set(SettingsId::EnablePush, 1);
    let mut settings = BufQueue::new(1024);
    upstream
        .generate_configured_settings(&mut settings, &mut Trace::default())
        .unwrap();
    let settings = bytes(&settings);
    assert_eq!(
        ingress_one(&mut downstream, &settings, &mut Trace::default()).consumed,
        settings.len()
    );

    let request_stream = upstream.create_stream();
    let request_message = request(b"/");
    let mut request_wire = BufQueue::new(4096);
    let generated = upstream
        .generate(
            &mut request_wire,
            EgressFrame::Headers {
                stream: request_stream,
                message: &request_message,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(generated.header_size.unwrap().uncompressed > 0);
    let request_wire = bytes(&request_wire);
    let mut request_trace = Trace::default();
    assert_eq!(
        ingress_one(&mut downstream, &request_wire, &mut request_trace).consumed,
        request_wire.len()
    );
    assert!(
        request_trace
            .events
            .contains(&Seen::Headers(1, true, 0, b"/".to_vec()))
    );
    assert!(request_trace.events.contains(&Seen::Complete(1)));

    let mut goaway = BufQueue::new(1024);
    downstream
        .generate(
            &mut goaway,
            EgressFrame::Goaway {
                last_stream: request_stream,
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let goaway = bytes(&goaway);
    assert_eq!(
        ingress_one(&mut upstream, &goaway, &mut Trace::default()).consumed,
        goaway.len()
    );

    let mut rejected_egress = BufQueue::new(4096);
    for frame in [
        EgressFrame::Headers {
            stream: StreamId::new(3),
            message: &request_message,
            end_stream: false,
            extra_headers: None,
        },
        EgressFrame::WindowUpdate {
            stream: StreamId::new(3),
            amount: 100,
        },
        EgressFrame::Body {
            stream: StreamId::new(3),
            body: queue(&[0; 10]),
            padding: None,
            end_stream: false,
        },
        EgressFrame::EndOfMessage {
            stream: StreamId::new(3),
        },
        EgressFrame::Reset {
            stream: StreamId::new(3),
            code: H2ErrorCode::CANCEL,
        },
    ] {
        assert_eq!(
            upstream
                .generate(&mut rejected_egress, frame, &mut Trace::default())
                .unwrap()
                .bytes_written,
            0
        );
    }
    assert!(rejected_egress.is_empty());

    let mut pushed_request = request(b"/");
    pushed_request.headers_mut().add(b"foomonkey", b"george");
    let mut first_batch = BufQueue::new(16 * 1024);
    let push = downstream
        .generate(
            &mut first_batch,
            EgressFrame::PushPromise {
                stream: StreamId::new(2),
                associated_stream: request_stream,
                message: &pushed_request,
                end_stream: false,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(push.header_size.unwrap().uncompressed > 0);
    downstream
        .generate(
            &mut first_batch,
            EgressFrame::Headers {
                stream: StreamId::new(2),
                message: &response(200),
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    downstream
        .generate(
            &mut first_batch,
            EgressFrame::WindowUpdate {
                stream: StreamId::new(2),
                amount: 100,
            },
            &mut Trace::default(),
        )
        .unwrap();
    downstream
        .generate(
            &mut first_batch,
            EgressFrame::Body {
                stream: StreamId::new(2),
                body: queue(&[0; 10]),
                padding: None,
                end_stream: false,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let mut first_batch = bytes(&first_batch);
    first_batch.extend_from_slice(&[0, 0, 20, FrameType::DATA.as_u8(), 0, 0, 0, 0, 2]);
    first_batch.extend_from_slice(&[0; 10]);

    upstream
        .generate(
            &mut BufQueue::new(1024),
            EgressFrame::Goaway {
                last_stream: StreamId::UNSPECIFIED,
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let mut ignored_trace = Trace::default();
    assert_eq!(
        ingress_one(&mut upstream, &first_batch, &mut ignored_trace).consumed,
        first_batch.len()
    );
    assert!(!ignored_trace.events.iter().any(|event| matches!(
        event,
        Seen::PushBegin(2, _)
            | Seen::Headers(2, ..)
            | Seen::Body(2, ..)
            | Seen::Window(2, _)
            | Seen::Complete(2)
            | Seen::Abort(2, _)
    )));

    let mut second_batch = BufQueue::new(4096);
    downstream
        .generate(
            &mut second_batch,
            EgressFrame::EndOfMessage {
                stream: StreamId::new(2),
            },
            &mut Trace::default(),
        )
        .unwrap();
    downstream
        .generate(
            &mut second_batch,
            EgressFrame::Reset {
                stream: StreamId::new(2),
                code: H2ErrorCode::CANCEL,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let accepted = downstream
        .generate(
            &mut second_batch,
            EgressFrame::Headers {
                stream: request_stream,
                message: &response(200),
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(accepted.header_size.unwrap().uncompressed > 0);
    let mut second_wire = vec![0; 10];
    second_wire.extend_from_slice(&bytes(&second_batch));
    let mut accepted_trace = Trace::default();
    assert_eq!(
        ingress_one(&mut upstream, &second_wire, &mut accepted_trace).consumed,
        second_wire.len()
    );
    assert!(
        accepted_trace
            .events
            .contains(&Seen::Headers(1, false, 200, Vec::new()))
    );
    assert!(accepted_trace.events.contains(&Seen::Complete(1)));
    assert!(!accepted_trace.events.iter().any(|event| matches!(
        event,
        Seen::PushBegin(2, _)
            | Seen::Headers(2, ..)
            | Seen::Body(2, ..)
            | Seen::Window(2, _)
            | Seen::Complete(2)
            | Seen::Abort(2, _)
    )));
}

fn stream_frames_after_goaway(stream: u32, headers: &[HpackHeader]) -> Vec<u8> {
    let mut wire = BufQueue::new(4096);
    frame::write_data(&mut wire, queue(b"late"), stream, None, false, false).unwrap();
    frame::write_rst_stream(&mut wire, stream, H2ErrorCode::CANCEL).unwrap();
    frame::write_window_update(&mut wire, stream, 17).unwrap();
    let encoded = HpackCodec::new().encode(headers);
    frame::write_headers(&mut wire, queue(&encoded), stream, None, true, true).unwrap();
    bytes(&wire)
}

fn assert_only_late_frame_headers(trace: &Trace, stream: u32) {
    assert_eq!(trace.events.len(), 4);
    assert!(
        trace
            .events
            .iter()
            .all(|event| matches!(event, Seen::Frame(seen, _, _) if *seen == stream))
    );
}

#[test]
fn ingress_goaway_suppresses_later_local_stream_callbacks() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(codec.create_stream(), StreamId::new(1));
    assert_eq!(codec.create_stream(), StreamId::new(3));
    open(&mut codec, &mut Trace::default());
    let mut goaway = BufQueue::new(1024);
    frame::write_goaway(&mut goaway, 1, H2ErrorCode::NO_ERROR, None).unwrap();
    assert_eq!(
        ingress_one(&mut codec, &bytes(&goaway), &mut Trace::default()).consumed,
        goaway.chain_length()
    );

    let wire = stream_frames_after_goaway(3, &[HpackHeader::new(b":status", b"200")]);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire, &mut trace).consumed,
        wire.len()
    );
    assert_only_late_frame_headers(&trace, 3);
}

#[test]
fn egress_goaway_suppresses_later_peer_stream_callbacks_and_admission() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    codec
        .generate(
            &mut BufQueue::new(1024),
            EgressFrame::Goaway {
                last_stream: StreamId::new(1),
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = stream_frames_after_goaway(3, &request_fields(b"/late"));
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire, &mut trace).consumed,
        wire.len()
    );
    assert_only_late_frame_headers(&trace, 3);
    assert_eq!(codec.last_incoming_stream_id(), StreamId::UNSPECIFIED);
}

#[test]
fn goaway_reply() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut output = BufQueue::new(1024);
    let generated = codec
        .generate(
            &mut output,
            EgressFrame::Goaway {
                last_stream: StreamId::new(7),
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert_eq!(generated.bytes_written, 17);
}

#[test]
fn enable_double_goaway_after_close() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut output = BufQueue::new(1024);
    codec
        .generate(
            &mut output,
            EgressFrame::Goaway {
                last_stream: StreamId::new(0),
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    codec.enable_double_goaway_drain();
    assert_eq!(
        codec
            .generate(
                &mut output,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(0),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        0
    );
}

#[test]
fn basic_rfc9218_priority() {
    let mut wire = BufQueue::new(1024);
    frame::write_rfc9218_priority(&mut wire, 3, b"u=1,i").unwrap();
    let input = bytes(&wire);
    let (_, trace, _) = run_downstream_wire(&input);
    assert!(trace.events.contains(&Seen::Priority(3, 1, true)));
}

#[test]
fn bad_rfc9218_priority() {
    let mut wire = BufQueue::new(1024);
    frame::write_rfc9218_priority(&mut wire, 3, b"u=99").unwrap();
    let input = bytes(&wire);
    let (_, trace, _) = run_downstream_wire(&input);
    assert!(trace.events.contains(&Seen::Priority(3, 3, true)));
}

fn push_wire(split: bool) -> Vec<u8> {
    let encoded = HpackCodec::new().encode(&request_fields(b"/push"));
    let first_length = if split { 1 } else { encoded.len() };
    let mut output = BufQueue::new(4096);
    frame::write_push_promise(
        &mut output,
        1,
        2,
        queue(&encoded[..first_length]),
        None,
        !split,
    )
    .unwrap();
    if split {
        frame::write_continuation(&mut output, 1, true, queue(&encoded[1..])).unwrap();
    }
    bytes(&output)
}

fn run_push(wire: &[u8]) -> (Http2Codec, Trace) {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(codec.create_stream(), StreamId::new(1));
    codec.egress_settings_mut().set(SettingsId::EnablePush, 1);
    let mut trace = Trace::default();
    open(&mut codec, &mut trace);
    trace.events.clear();
    assert_eq!(
        ingress_one(&mut codec, wire, &mut trace).consumed,
        wire.len()
    );
    (codec, trace)
}

#[test]
fn push_promise_continuation() {
    let wire = push_wire(true);
    let first_length = FRAME_HEADER_SIZE
        + ((usize::from(wire[0]) << 16) | (usize::from(wire[1]) << 8) | usize::from(wire[2]));
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(codec.create_stream(), StreamId::new(1));
    codec.egress_settings_mut().set(SettingsId::EnablePush, 1);
    open(&mut codec, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        codec
            .on_ingress(
                &queue(&wire[..first_length]),
                &mut callback,
                &mut BufQueue::new(1024),
            )
            .consumed,
        first_length
    );
    assert_eq!(callback.events, ["push-begin"]);
    assert_eq!(callback.push_ids, [(2, 1)]);
    assert_eq!(callback.push_messages.len(), 1);
    assert!(callback.push_messages[0].is_none());

    assert_eq!(
        codec
            .on_ingress(
                &queue(&wire[first_length..]),
                &mut callback,
                &mut BufQueue::new(1024),
            )
            .consumed,
        wire.len() - first_length
    );
    assert_eq!(callback.events, ["push-begin", "headers"]);
    assert!(callback.messages[0].is_request());
    assert_eq!(callback.messages[0].url().unwrap(), b"/push");
    assert!(callback.messages[0].is_chunked());
}

#[test]
fn basic_push_promise() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    open(&mut downstream, &mut Trace::default());
    let mut settings = BufQueue::new(1024);
    codec
        .generate_configured_settings(&mut settings, &mut Trace::default())
        .unwrap();
    let settings = bytes(&settings);
    assert_eq!(
        ingress_one(&mut downstream, &settings, &mut Trace::default()).consumed,
        settings.len()
    );
    assert!(!codec.supports_push_transactions());
    assert!(!downstream.supports_push_transactions());
    codec.egress_settings_mut().set(SettingsId::EnablePush, 1);
    let mut settings = BufQueue::new(1024);
    codec
        .generate_configured_settings(&mut settings, &mut Trace::default())
        .unwrap();
    let settings = bytes(&settings);
    assert_eq!(
        ingress_one(&mut downstream, &settings, &mut Trace::default()).consumed,
        settings.len()
    );
    assert!(codec.supports_push_transactions());
    assert!(downstream.supports_push_transactions());
    for expected in [StreamId::new(1), StreamId::new(3), StreamId::new(5)] {
        assert_eq!(codec.create_stream(), expected);
    }
    let associated = codec.create_stream();
    assert_eq!(associated, StreamId::new(7));

    for promised in [2, 4] {
        let mut wire = BufQueue::new(4096);
        let promised_stream = downstream.create_stream();
        assert_eq!(promised_stream, StreamId::new(promised));
        let mut request = request(b"/");
        request
            .headers_mut()
            .add(HttpHeaderCode::UserAgent, b"coolio");
        downstream
            .generate(
                &mut wire,
                EgressFrame::PushPromise {
                    stream: promised_stream,
                    associated_stream: associated,
                    message: &request,
                    end_stream: false,
                },
                &mut Trace::default(),
            )
            .unwrap();
        let wire = bytes(&wire);
        let mut callback = MessageCapture::default();
        assert_eq!(
            codec
                .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
                .consumed,
            wire.len()
        );
        assert_eq!(callback.events, ["push-begin", "headers"]);
        assert_eq!(callback.push_ids, [(promised, associated.get())]);
        let mut begin = callback.push_messages.remove(0).unwrap();
        assert!(begin.is_request());
        assert_eq!(begin.url().unwrap(), b"/");
        assert_eq!(begin.advanced_protocol_string(), Some(b"h2".as_slice()));
        assert_eq!(
            begin
                .headers()
                .get_single_or_empty(HttpHeaderCode::UserAgent),
            b"coolio"
        );
        let mut complete = callback.messages.remove(0);
        assert_eq!(complete.url().unwrap(), b"/");
        assert!(complete.is_chunked());
        assert_eq!(
            complete
                .headers()
                .get_single_or_empty(HttpHeaderCode::UserAgent),
            b"coolio"
        );

        let mut response = response(200);
        response
            .headers_mut()
            .add(HttpHeaderCode::ContentType, b"text/plain");
        let mut response_wire = BufQueue::new(4096);
        downstream
            .generate(
                &mut response_wire,
                EgressFrame::Headers {
                    stream: promised_stream,
                    message: &response,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .unwrap();
        let response_wire = bytes(&response_wire);
        let mut response_callback = MessageCapture::default();
        assert_eq!(
            codec
                .on_ingress(
                    &queue(&response_wire),
                    &mut response_callback,
                    &mut BufQueue::new(1024),
                )
                .consumed,
            response_wire.len()
        );
        assert_eq!(response_callback.events, ["begin", "headers"]);
        let mut decoded = response_callback.messages.remove(0);
        assert_eq!(decoded.status_code().unwrap(), 200);
        assert!(decoded.headers().exists(HttpHeaderCode::Date));
        assert_eq!(
            decoded
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentType),
            b"text/plain"
        );
    }
}

#[test]
fn rejected_push_promise_ids_preserve_hpack_for_retry() {
    for (promised, associated) in [(2, 2), (3, 1)] {
        let mut sender = Http2Codec::new(TransportDirection::Downstream);
        let mut receiver = Http2Codec::new(TransportDirection::Upstream);
        assert_eq!(receiver.create_stream(), StreamId::new(1));
        receiver
            .egress_settings_mut()
            .set(SettingsId::EnablePush, 1);
        open(&mut receiver, &mut Trace::default());
        let mut message = request(b"/push-retry");
        message.headers_mut().add(b"x-push-retry", b"indexed-value");
        let mut wire = BufQueue::new(4096);
        frame::write_ping(&mut wire, 17, false).unwrap();
        let before = bytes(&wire);
        let mut failed_trace = Trace::default();
        assert!(
            sender
                .generate(
                    &mut wire,
                    EgressFrame::PushPromise {
                        stream: StreamId::new(promised),
                        associated_stream: StreamId::new(associated),
                        message: &message,
                        end_stream: false,
                    },
                    &mut failed_trace,
                )
                .is_err()
        );
        assert_eq!(bytes(&wire), before);
        assert!(failed_trace.events.is_empty());
        sender
            .generate(
                &mut wire,
                EgressFrame::PushPromise {
                    stream: StreamId::new(2),
                    associated_stream: StreamId::new(1),
                    message: &message,
                    end_stream: false,
                },
                &mut Trace::default(),
            )
            .unwrap();
        let mut callback = MessageCapture::default();
        assert_eq!(
            receiver
                .on_ingress(&wire, &mut callback, &mut BufQueue::new(1024))
                .consumed,
            wire.chain_length()
        );
        assert_eq!(callback.events, ["push-begin", "headers"]);
        assert_eq!(callback.push_ids, [(2, 1)]);
        let decoded = &callback.messages[0];
        assert_eq!(decoded.request_url(), Some(b"/push-retry".as_slice()));
        assert_eq!(
            decoded.headers().get_single_or_empty(b"x-push-retry"),
            b"indexed-value"
        );
        assert!(receiver.is_reusable());
    }
}

#[test]
fn unsplit_push_hpack_failure_has_no_push_begin() {
    let mut wire = BufQueue::new(1024);
    frame::write_push_promise(&mut wire, 1, 2, queue(&[0xff]), None, true).unwrap();
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(codec.create_stream(), StreamId::new(1));
    codec.egress_settings_mut().set(SettingsId::EnablePush, 1);
    open(&mut codec, &mut Trace::default());
    let wire = bytes(&wire);
    let mut trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire, &mut trace).consumed,
        wire.len()
    );
    assert!(
        !trace
            .events
            .iter()
            .any(|event| matches!(event, Seen::PushBegin(..) | Seen::Headers(..)))
    );
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(0, Some(H2ErrorCode::COMPRESSION_ERROR), _, false)
    )));
    assert!(!codec.is_reusable());
}

#[test]
fn duplicate_push_promise() {
    let mut wire = push_wire(false);
    wire.extend_from_slice(&push_wire(false));
    let (codec, _) = run_push(&wire);
    assert!(!codec.is_reusable());
}

#[test]
fn bad_push_promise() {
    let (codec, _, _) = run_downstream_wire(&push_wire(false));
    assert!(!codec.is_reusable());
}

#[test]
fn bad_push_promise_resets() {
    let mut fields = request_fields(b"/push");
    fields.push(HpackHeader::new(b"content-length", b"100"));
    fields.push(HpackHeader::new(b"content-length", b"200"));
    let encoded = HpackCodec::new().encode(&fields);
    let mut output = BufQueue::new(4096);
    frame::write_push_promise(&mut output, 1, 2, queue(&encoded), None, true).unwrap();

    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(codec.create_stream(), StreamId::new(1));
    codec.egress_settings_mut().set(SettingsId::EnablePush, 1);
    open(&mut codec, &mut Trace::default());
    let mut trace = Trace::default();
    let wire = bytes(&output);
    assert_eq!(
        ingress_one(&mut codec, &wire, &mut trace).consumed,
        wire.len()
    );
    assert!(trace.events.contains(&Seen::PushBegin(2, 1)));
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Error(..)))
            .count(),
        2
    );
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(1, Some(H2ErrorCode::PROTOCOL_ERROR), _, false)
    )));
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(2, Some(H2ErrorCode::REFUSED_STREAM), _, false)
    )));
    assert!(
        !trace
            .events
            .iter()
            .any(|event| matches!(event, Seen::Headers(..) | Seen::Complete(_)))
    );
    assert!(codec.is_reusable());
}

fn request_with_trailers(trailers: &HttpHeaders) -> Trace {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let stream = sender.create_stream();
    let mut output = BufQueue::new(512 * 1024);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &request(b"/trailers"),
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    sender
        .generate(
            &mut output,
            EgressFrame::Trailers { stream, trailers },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = bytes(&output);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    trace
}

fn response_with_trailers(trailers: &HttpHeaders) -> (Http2Codec, Trace, Vec<u8>) {
    let mut sender = Http2Codec::new(TransportDirection::Downstream);
    let mut output = BufQueue::new(512 * 1024);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &response(200),
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    sender
        .generate(
            &mut output,
            EgressFrame::Trailers {
                stream: StreamId::new(1),
                trailers,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = bytes(&output);
    let mut receiver = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(receiver.create_stream(), StreamId::new(1));
    let mut trace = Trace::default();
    open(&mut receiver, &mut trace);
    trace.events.clear();
    assert_eq!(
        ingress_one(&mut receiver, &wire, &mut trace).consumed,
        wire.len()
    );
    (receiver, trace, wire)
}

fn sample_trailers() -> HttpHeaders {
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer", b"done");
    trailers
}

#[test]
fn trailers() {
    let trace = request_with_trailers(&sample_trailers());
    assert!(trace.events.contains(&Seen::Trailers(1, 1)));
    assert!(trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn trailers_with_pseudo_headers() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let stream = sender.create_stream();
    let mut output = BufQueue::new(4096);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &request(b"/"),
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let pseudo = encoded_headers(1, &request_fields(b"/again"), true);
    let mut wire = bytes(&output);
    wire.extend_from_slice(&pseudo);
    let (_, trace, _) = run_downstream_wire(&wire);
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(1, Some(H2ErrorCode::PROTOCOL_ERROR), _, true)
    )));
}

#[test]
fn trailers_no_body() {
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer-1", b"pico-de-gallo");
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let stream = sender.create_stream();
    let mut output = BufQueue::new(4096);
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &request(b"/guacamole"),
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    sender
        .generate(
            &mut output,
            EgressFrame::Trailers {
                stream,
                trailers: &trailers,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = bytes(&output);
    let mut receiver = Http2Codec::new(TransportDirection::Downstream);
    open(&mut receiver, &mut Trace::default());
    let mut callback = MessageCapture::default();
    assert_eq!(
        receiver
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    assert_eq!(
        callback.events,
        ["begin", "headers", "trailers", "complete"]
    );
    assert!(callback.bodies.is_empty());
    assert_eq!(
        callback.messages[1]
            .headers()
            .get_single_or_empty(b"x-trailer-1" as &[u8]),
        b"pico-de-gallo"
    );
}

#[test]
fn trailers_continuation() {
    let mut trailers = sample_trailers();
    trailers.add(b"x-huge", vec![b'x'; 20 * 1024]);
    let trace = request_with_trailers(&trailers);
    assert!(trace.events.contains(&Seen::Trailers(1, 2)));
}

#[test]
fn trailers_reply() {
    let (_, trace, _) = response_with_trailers(&sample_trailers());
    assert!(trace.events.contains(&Seen::Trailers(1, 1)));
    assert!(trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn trailers_reply_empty() {
    let (_, trace, _) = response_with_trailers(&HttpHeaders::new());
    assert!(
        !trace
            .events
            .iter()
            .any(|event| matches!(event, Seen::Trailers(..)))
    );
    assert!(trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn trailers_reply_with_no_data() {
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer-1", b"pico-de-gallo");
    let mut sender = Http2Codec::new(TransportDirection::Downstream);
    let mut output = BufQueue::new(4096);
    let mut message = response(200);
    message
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b"x-coolio");
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(1),
                message: &message,
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    sender
        .generate(
            &mut output,
            EgressFrame::Trailers {
                stream: StreamId::new(1),
                trailers: &trailers,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let mut receiver = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(receiver.create_stream(), StreamId::new(1));
    open(&mut receiver, &mut Trace::default());
    let mut callback = MessageCapture::default();
    let wire = bytes(&output);
    assert_eq!(
        receiver
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    assert_eq!(
        callback.events,
        ["begin", "headers", "trailers", "complete"]
    );
    assert!(callback.bodies.is_empty());
    assert_eq!(
        callback.messages[0]
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentType),
        b"x-coolio"
    );
    assert!(callback.messages[0].headers().exists(HttpHeaderCode::Date));
    assert_eq!(
        callback.messages[1]
            .headers()
            .get_single_or_empty(b"x-trailer-1" as &[u8]),
        b"pico-de-gallo"
    );
}

#[test]
fn trailers_reply_with_pseudo_headers() {
    let mut receiver = Http2Codec::new(TransportDirection::Upstream);
    receiver.create_stream();
    let mut trace = Trace::default();
    open(&mut receiver, &mut trace);
    let mut wire = encoded_headers(1, &[HpackHeader::new(b":status", b"200")], false);
    wire.extend_from_slice(&encoded_headers(
        1,
        &[HpackHeader::new(b":method", b"POST")],
        true,
    ));
    trace.events.clear();
    assert_eq!(
        ingress_one(&mut receiver, &wire, &mut trace).consumed,
        wire.len()
    );
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(1, Some(H2ErrorCode::PROTOCOL_ERROR), _, true)
    )));
}

#[test]
fn trailers_reply_continuation() {
    let mut trailers = sample_trailers();
    trailers.add(b"x-huge", vec![b'x'; 20 * 1024]);
    let (_, trace, _) = response_with_trailers(&trailers);
    assert!(trace.events.contains(&Seen::Trailers(1, 2)));
}

#[test]
fn trailers_reply_missing_continuation() {
    let mut trailers = sample_trailers();
    trailers.add(b"x-huge", vec![b'x'; 20 * 1024]);
    let (_, _, mut wire) = response_with_trailers(&trailers);
    wire.truncate(wire.len() - 100);
    let mut receiver = Http2Codec::new(TransportDirection::Upstream);
    receiver.create_stream();
    let mut trace = Trace::default();
    open(&mut receiver, &mut trace);
    trace.events.clear();
    let consumed = ingress_one(&mut receiver, &wire, &mut trace).consumed;
    assert!(consumed < wire.len());
    assert!(!trace.events.contains(&Seen::Complete(1)));
}

#[test]
fn trailers_not_latest() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let first = sender.create_stream();
    let second = sender.create_stream();
    let mut output = BufQueue::new(65536);
    for stream in [first, second] {
        sender
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request(b"/"),
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .unwrap();
    }
    sender
        .generate(
            &mut output,
            EgressFrame::Trailers {
                stream: first,
                trailers: &sample_trailers(),
            },
            &mut Trace::default(),
        )
        .unwrap();
    sender
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: second,
                message: &request(b"/again"),
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let (_, trace, _) = run_downstream_wire(&bytes(&output));
    assert!(trace.events.contains(&Seen::Trailers(1, 1)));
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(3, Some(H2ErrorCode::PROTOCOL_ERROR), _, true)
    )));
}

#[test]
fn send_empty_trailers() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut output = BufQueue::new(1024);
    let generated = codec
        .generate(
            &mut output,
            EgressFrame::Trailers {
                stream: StreamId::new(1),
                trailers: &HttpHeaders::new(),
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert_eq!(generated.bytes_written, FRAME_HEADER_SIZE);
    let mut parsed = output;
    let parsed = parse_frame(&mut parsed).unwrap().unwrap();
    assert_eq!(parsed.header.frame_type, FrameType::DATA);
    assert_ne!(parsed.header.flags & Flags::END_STREAM, 0);
}

#[test]
fn all_egress_frame_type_callbacks() {
    let mut upstream = Http2Codec::new(TransportDirection::Upstream);
    let mut downstream = Http2Codec::new(TransportDirection::Downstream);
    let mut trace = Trace::default();
    let mut output = BufQueue::new(256 * 1024);
    upstream
        .generate(
            &mut output,
            EgressFrame::Body {
                stream: StreamId::new(2),
                body: queue(b"abcde"),
                padding: None,
                end_stream: true,
            },
            &mut trace,
        )
        .unwrap();
    let stream = upstream.create_stream();
    upstream
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &request(b"/"),
                end_stream: true,
                extra_headers: None,
            },
            &mut trace,
        )
        .unwrap();
    upstream
        .generate(
            &mut output,
            EgressFrame::Priority {
                stream: StreamId::new(3),
                priority: HttpPriority::new(1, true, 0, false, 0),
            },
            &mut trace,
        )
        .unwrap();
    upstream
        .generate(
            &mut output,
            EgressFrame::Reset {
                stream: StreamId::new(2),
                code: H2ErrorCode::ENHANCE_YOUR_CALM,
            },
            &mut trace,
        )
        .unwrap();
    upstream
        .generate_configured_settings(&mut output, &mut trace)
        .unwrap();
    downstream
        .generate(
            &mut output,
            EgressFrame::PushPromise {
                stream: StreamId::new(2),
                associated_stream: StreamId::new(1),
                message: &request(b"/push"),
                end_stream: false,
            },
            &mut trace,
        )
        .unwrap();
    upstream
        .generate(
            &mut output,
            EgressFrame::PingRequest { data: None },
            &mut trace,
        )
        .unwrap();
    upstream
        .generate(
            &mut output,
            EgressFrame::Goaway {
                last_stream: StreamId::new(17),
                code: H2ErrorCode::ENHANCE_YOUR_CALM,
                debug_data: Some(queue(b"debugData")),
            },
            &mut trace,
        )
        .unwrap();
    upstream
        .generate(
            &mut output,
            EgressFrame::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 10,
            },
            &mut trace,
        )
        .unwrap();
    let mut big = request(b"/");
    big.headers_mut().add(b"x-big", vec![b'!'; 18_000]);
    upstream
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &big,
                end_stream: true,
                extra_headers: None,
            },
            &mut trace,
        )
        .unwrap();
    let generated = trace
        .events
        .iter()
        .filter_map(|event| match event {
            Seen::Generated(_, frame_type, _) => Some(*frame_type),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        generated,
        [
            FrameType::DATA.as_u8(),
            FrameType::HEADERS.as_u8(),
            FrameType::RFC9218_PRIORITY.as_u8(),
            FrameType::RST_STREAM.as_u8(),
            FrameType::SETTINGS.as_u8(),
            FrameType::PUSH_PROMISE.as_u8(),
            FrameType::PING.as_u8(),
            FrameType::GOAWAY.as_u8(),
            FrameType::WINDOW_UPDATE.as_u8(),
            FrameType::HEADERS.as_u8(),
            FrameType::CONTINUATION.as_u8(),
        ]
    );

    let mut continued_output = BufQueue::new(128 * 1024);
    let mut continued_trace = Trace::default();
    downstream
        .generate(
            &mut continued_output,
            EgressFrame::PushPromise {
                stream: StreamId::new(4),
                associated_stream: StreamId::new(1),
                message: &big,
                end_stream: false,
            },
            &mut continued_trace,
        )
        .unwrap();
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer", vec![b'!'; 18_000]);
    downstream
        .generate(
            &mut continued_output,
            EgressFrame::Trailers {
                stream: StreamId::new(4),
                trailers: &trailers,
            },
            &mut continued_trace,
        )
        .unwrap();
    let mut expected = Vec::new();
    for (callback_stream, wire_stream, frame_type) in [
        (4, 1, FrameType::PUSH_PROMISE),
        (1, 1, FrameType::CONTINUATION),
        (4, 4, FrameType::HEADERS),
        (4, 4, FrameType::CONTINUATION),
    ] {
        let parsed = parse_frame(&mut continued_output).unwrap().unwrap();
        assert_eq!(parsed.header.frame_type, frame_type);
        assert_eq!(parsed.header.stream, wire_stream);
        expected.push(Seen::Generated(
            callback_stream,
            frame_type.as_u8(),
            u64::from(parsed.header.length) + FRAME_HEADER_SIZE as u64,
        ));
    }
    assert!(continued_output.is_empty());
    assert_eq!(continued_trace.events, expected);
}

fn split_after_first_frame(wire: &[u8]) -> (&[u8], &[u8]) {
    let length = usize::from(wire[0]) << 16 | usize::from(wire[1]) << 8 | usize::from(wire[2]);
    wire.split_at(FRAME_HEADER_SIZE + length)
}

/// A request head that needs a CONTINUATION frame reaches the callback only once its block
/// ends; until then the downstream codec reports the partial head (`DIV-055`), and trailers
/// on an open stream are not a request head.
#[test]
fn partial_request_head_is_reported_until_its_block_ends() {
    let mut client = Http2Codec::new(TransportDirection::Upstream);
    let mut big = request(b"/big");
    big.headers_mut().add(b"x-big", vec![b'!'; 18_000]);
    let stream = client.create_stream();
    let mut head = BufQueue::new(128 * 1024);
    client
        .generate(
            &mut head,
            EgressFrame::Headers {
                stream,
                message: &big,
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer", vec![b'!'; 18_000]);
    let mut trailer_block = BufQueue::new(128 * 1024);
    client
        .generate(
            &mut trailer_block,
            EgressFrame::Trailers {
                stream,
                trailers: &trailers,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let (head, trailer_block) = (bytes(&head), bytes(&trailer_block));

    let mut server = Http2Codec::new(TransportDirection::Downstream);
    let mut trace = Trace::default();
    open(&mut server, &mut trace);
    let mut egress = BufQueue::new(64 * 1024);
    assert!(!server.has_partial_request_head());
    let (first, rest) = split_after_first_frame(&head);
    assert_eq!(
        server
            .on_ingress(&queue(first), &mut trace, &mut egress)
            .consumed,
        first.len()
    );
    assert!(
        server.has_partial_request_head(),
        "HEADERS without END_HEADERS"
    );
    assert!(!trace.events.contains(&Seen::Begin(1)));
    assert_eq!(
        server
            .on_ingress(&queue(rest), &mut trace, &mut egress)
            .consumed,
        rest.len()
    );
    assert!(
        !server.has_partial_request_head(),
        "the CONTINUATION ends the block"
    );
    assert!(trace.events.contains(&Seen::Begin(1)));
    let (first, _) = split_after_first_frame(&trailer_block);
    assert_eq!(
        server
            .on_ingress(&queue(first), &mut trace, &mut egress)
            .consumed,
        first.len()
    );
    assert!(
        !server.has_partial_request_head(),
        "partial trailers open no stream"
    );
}

/// A request block on a stream above the final GOAWAY's last stream opens no stream: the
/// codec drops its events when the block ends, so it is no partial request head (`DIV-055`).
#[test]
fn partial_request_head_above_the_final_goaway_is_not_reported() {
    let mut client = Http2Codec::new(TransportDirection::Upstream);
    let mut first = BufQueue::new(1024);
    let stream = client.create_stream();
    client
        .generate(
            &mut first,
            EgressFrame::Headers {
                stream,
                message: &request(b"/first"),
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let mut big = request(b"/big");
    big.headers_mut().add(b"x-big", vec![b'!'; 18_000]);
    let mut late = BufQueue::new(128 * 1024);
    let stream = client.create_stream();
    client
        .generate(
            &mut late,
            EgressFrame::Headers {
                stream,
                message: &big,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let (first, late) = (bytes(&first), bytes(&late));

    let mut server = Http2Codec::new(TransportDirection::Downstream);
    let mut trace = Trace::default();
    open(&mut server, &mut trace);
    let mut egress = BufQueue::new(64 * 1024);
    assert_eq!(
        server
            .on_ingress(&queue(&first), &mut trace, &mut egress)
            .consumed,
        first.len()
    );
    assert!(trace.events.contains(&Seen::Begin(1)));
    server
        .generate(
            &mut egress,
            EgressFrame::Goaway {
                last_stream: StreamId::new(1),
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(!server.is_waiting_to_drain(), "the GOAWAY is the final one");
    let (head, rest) = split_after_first_frame(&late);
    assert_eq!(
        server
            .on_ingress(&queue(head), &mut trace, &mut egress)
            .consumed,
        head.len()
    );
    assert!(
        !server.has_partial_request_head(),
        "stream 3 is above the final GOAWAY's last stream"
    );
    assert_eq!(
        server
            .on_ingress(&queue(rest), &mut trace, &mut egress)
            .consumed,
        rest.len()
    );
    assert!(
        !trace.events.contains(&Seen::Begin(3)),
        "the codec drops the block's events"
    );
}

#[test]
fn padding_frame() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut output = BufQueue::new(4096);
    assert_eq!(
        codec
            .generate(
                &mut output,
                EgressFrame::Padding {
                    stream: StreamId::new(2),
                    length: 80,
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        80
    );
    codec
        .generate(
            &mut output,
            EgressFrame::Body {
                stream: StreamId::new(2),
                body: queue(b"abcde"),
                padding: None,
                end_stream: false,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert_eq!(
        codec
            .generate(
                &mut output,
                EgressFrame::Padding {
                    stream: StreamId::new(2),
                    length: 40,
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        40
    );
    codec
        .generate(
            &mut output,
            EgressFrame::EndOfMessage {
                stream: StreamId::new(2),
            },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = bytes(&output);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Frame(..)))
            .count(),
        4
    );
    assert_eq!(
        trace
            .events
            .iter()
            .filter_map(|event| match event {
                Seen::Body(_, length, _) => Some(*length),
                _ => None,
            })
            .sum::<usize>(),
        5
    );
    assert!(trace.events.contains(&Seen::Complete(2)));
}

#[test]
fn max_padding() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut output = BufQueue::new(128 * 1024);
    codec
        .generate(
            &mut output,
            EgressFrame::Body {
                stream: StreamId::new(2),
                body: queue(b"abcde"),
                padding: None,
                end_stream: false,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let generated = codec
        .generate(
            &mut output,
            EgressFrame::Padding {
                stream: StreamId::new(2),
                length: u16::MAX,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert_eq!(generated.bytes_written, 65_536);
    codec
        .generate(
            &mut output,
            EgressFrame::EndOfMessage {
                stream: StreamId::new(2),
            },
            &mut Trace::default(),
        )
        .unwrap();
    let wire = bytes(&output);
    let (_, trace, consumed) = run_downstream_wire(&wire);
    assert_eq!(consumed, wire.len());
    assert_eq!(
        trace
            .events
            .iter()
            .filter(|event| matches!(event, Seen::Frame(..)))
            .count(),
        6
    );
    assert_eq!(
        trace
            .events
            .iter()
            .filter_map(|event| match event {
                Seen::Body(_, length, _) => Some(*length),
                _ => None,
            })
            .sum::<usize>(),
        5
    );
    assert!(trace.events.contains(&Seen::Complete(2)));
}

#[derive(Default)]
struct ResetOnHeaders {
    complete: usize,
}

impl HttpCodecCallback for ResetOnHeaders {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if matches!(event, CodecEvent::MessageComplete { .. }) {
            self.complete += 1;
        }
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let reset = match event {
            CodecEvent::HeadersComplete { stream, .. } => Some(stream),
            CodecEvent::MessageComplete { .. } => {
                self.complete += 1;
                None
            }
            _ => None,
        };
        reset.map_or_else(CodecCallbackActions::new, |stream| {
            vec![CodecCallbackAction::Generate(OwnedEgressFrame::Reset {
                stream,
                code: H2ErrorCode::CANCEL,
            })]
            .into()
        })
    }
}

#[test]
fn reentrant_reset_suppresses_deferred_end_stream() {
    let wire = encoded_headers(1, &request_fields(b"/reset"), true);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut callback = ResetOnHeaders::default();
    let mut egress = BufQueue::new(1024);
    let result = codec.on_ingress(&queue(&wire), &mut callback, &mut egress);
    assert_eq!(result.consumed, wire.len());
    assert_eq!(callback.complete, 0);
    assert_eq!(result.egress_results.len(), 1);
    assert_eq!(
        parse_frame(&mut egress).unwrap().unwrap().header.frame_type,
        FrameType::RST_STREAM
    );
}

#[test]
fn failed_reentrant_reset_preserves_deferred_end_stream() {
    let wire = encoded_headers(1, &request_fields(b"/reset-retry"), true);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let mut callback = ResetOnHeaders::default();
    let result = codec.on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(8));
    assert_eq!(result.consumed, wire.len());
    assert_eq!(callback.complete, 1);
    assert_eq!(result.egress_results.len(), 1);
    assert!(result.egress_results[0].is_err());
}

#[derive(Default)]
struct HeaderEncodeStats(RefCell<Vec<HttpHeaderSize>>);

impl HeaderCodecStats for HeaderEncodeStats {
    fn record_encode(&self, size: HttpHeaderSize) {
        self.0.borrow_mut().push(size);
    }

    fn record_decode(&self, _size: HttpHeaderSize) {}
    fn record_decode_error(&self) {}
    fn record_decode_too_large(&self) {}
}

#[test]
fn header_generation_failure_rolls_back_hpack_and_retries_exactly() {
    let mut message = request(b"/transactional");
    message.headers_mut().add(b"x-indexed", b"value-to-index");

    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let stream = codec.create_stream();
    let before = codec.compression_info();
    let mut failed_trace = Trace::default();
    let mut too_small = BufQueue::new(20);
    assert!(
        codec
            .generate(
                &mut too_small,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut failed_trace,
            )
            .is_err()
    );
    assert!(too_small.is_empty());
    assert_eq!(codec.compression_info(), before);
    assert!(failed_trace.events.is_empty());

    let mut retried = BufQueue::new(4096);
    codec
        .generate(
            &mut retried,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let mut fresh = Http2Codec::new(TransportDirection::Upstream);
    let fresh_stream = fresh.create_stream();
    let mut expected = BufQueue::new(4096);
    fresh
        .generate(
            &mut expected,
            EgressFrame::Headers {
                stream: fresh_stream,
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert_eq!(bytes(&retried), bytes(&expected));
    assert_eq!(codec.compression_info(), fresh.compression_info());

    for push in [false, true] {
        for continued in [false, true] {
            let direction = if push {
                TransportDirection::Downstream
            } else {
                TransportDirection::Upstream
            };
            let mut codec = Http2Codec::new(direction);
            let mut reference = Http2Codec::new(direction);
            let stream = StreamId::new(if push { 2 } else { 1 });
            let generate = |codec: &mut Http2Codec,
                            output: &mut BufQueue,
                            message: &HttpMessage,
                            callback: &mut Trace| {
                let event = if push {
                    EgressFrame::PushPromise {
                        stream,
                        associated_stream: StreamId::new(1),
                        message,
                        end_stream: false,
                    }
                } else {
                    EgressFrame::Headers {
                        stream,
                        message,
                        end_stream: true,
                        extra_headers: None,
                    }
                };
                codec.generate(output, event, callback)
            };
            let mut message = request(b"/transactional");
            message
                .headers_mut()
                .add(b"x-size", vec![b'!'; if continued { 18_000 } else { 12 }]);
            message.headers_mut().add(b"x-indexed", b"value-to-index");
            let stats = Rc::new(HeaderEncodeStats::default());
            codec.set_header_codec_stats(Some(stats.clone()));
            let mut expected = BufQueue::new(usize::MAX);
            expected.push_static(b"kept").unwrap();
            let reference_result = generate(
                &mut reference,
                &mut expected,
                &message,
                &mut Trace::default(),
            )
            .unwrap();
            let before = codec.compression_info().egress;
            let mut rejected = BufQueue::new(expected.chain_length() - 1);
            rejected.push_static(b"kept").unwrap();
            let mut callback = Trace::default();
            assert!(generate(&mut codec, &mut rejected, &message, &mut callback).is_err());
            assert_eq!(bytes(&rejected), b"kept");
            assert_eq!(codec.compression_info().egress, before);
            assert!(stats.0.borrow().is_empty());
            assert!(callback.events.is_empty());

            let mut exact = BufQueue::new(expected.chain_length());
            exact.push_static(b"kept").unwrap();
            let result = generate(&mut codec, &mut exact, &message, &mut callback).unwrap();
            assert_eq!(bytes(&exact), bytes(&expected));
            assert_eq!(result.bytes_written, expected.chain_length() - 4);
            assert_eq!(result.header_size, reference_result.header_size);
            assert_eq!(
                codec.compression_info().egress,
                reference.compression_info().egress
            );
            assert_eq!(bytes(&exact.split_at_most(4)), b"kept");
            let mut compressed = BufQueue::new(128 * 1024);
            let mut expected_callbacks = Vec::new();
            let frame_count = if continued { 2 } else { 1 };
            for ordinal in 0..frame_count {
                let parsed = parse_frame(&mut exact).unwrap().unwrap();
                let first = ordinal == 0;
                let frame_type = if !first {
                    FrameType::CONTINUATION
                } else if push {
                    FrameType::PUSH_PROMISE
                } else {
                    FrameType::HEADERS
                };
                assert_eq!(parsed.header.frame_type, frame_type);
                assert_eq!(parsed.header.stream, 1);
                assert_eq!(
                    parsed.header.flags,
                    if first && !push { Flags::END_STREAM } else { 0 }
                        | if ordinal + 1 == frame_count {
                            Flags::END_HEADERS
                        } else {
                            0
                        }
                );
                expected_callbacks.push(Seen::Generated(
                    if first { stream.get() } else { 1 },
                    frame_type.as_u8(),
                    u64::from(parsed.header.length) + FRAME_HEADER_SIZE as u64,
                ));
                let mut block = match parsed.frame {
                    Frame::PushPromise {
                        promised_stream,
                        header_block,
                    } => {
                        assert_eq!(promised_stream, 2);
                        header_block
                    }
                    Frame::Headers(block) | Frame::Continuation(block) => block,
                    _ => panic!("expected a header block frame"),
                };
                compressed.append(&mut block).unwrap();
            }
            assert!(exact.is_empty());
            assert_eq!(callback.events, expected_callbacks);
            let size = result.header_size.unwrap();
            assert_eq!(size.compressed as usize, compressed.chain_length());
            assert_eq!(size.compressed_block, size.compressed);
            assert_eq!(stats.0.borrow().as_slice(), &[size]);
            let mut decoder = HpackCodec::new();
            let decoded = decoder
                .decode_buf_queue(&compressed, compressed.chain_length())
                .unwrap();
            assert!(
                decoded
                    .headers
                    .contains(&HpackHeader::new("x-indexed", "value-to-index"))
            );

            let mut followup = request(b"/transactional");
            followup.headers_mut().add(b"x-indexed", b"value-to-index");
            let mut actual_next = BufQueue::new(4096);
            let next = generate(
                &mut codec,
                &mut actual_next,
                &followup,
                &mut Trace::default(),
            )
            .unwrap();
            let mut expected_next = BufQueue::new(4096);
            generate(
                &mut reference,
                &mut expected_next,
                &followup,
                &mut Trace::default(),
            )
            .unwrap();
            assert_eq!(bytes(&actual_next), bytes(&expected_next));
            let parsed = parse_frame(&mut actual_next).unwrap().unwrap();
            let block = match parsed.frame {
                Frame::Headers(block)
                | Frame::PushPromise {
                    header_block: block,
                    ..
                } => block,
                _ => panic!("expected the dependent header block"),
            };
            assert!(actual_next.is_empty());
            assert!(
                HpackCodec::new()
                    .decode_buf_queue(&block, block.chain_length())
                    .is_err()
            );
            assert!(
                decoder
                    .decode_buf_queue(&block, block.chain_length())
                    .unwrap()
                    .headers
                    .contains(&HpackHeader::new("x-indexed", "value-to-index"))
            );
            let next_size = next.header_size.unwrap();
            assert_eq!(next_size.compressed as usize, block.chain_length());
            assert_eq!(
                next_size.compressed_block,
                size.compressed + next_size.compressed
            );
            assert_eq!(stats.0.borrow().as_slice(), &[size, next_size]);
        }
    }
}

#[test]
fn header_indexing_panic_preserves_output_and_retry_state() {
    struct PanicStrategy {
        calls: Cell<u32>,
        clones: Rc<Cell<u32>>,
        fail: Rc<Cell<bool>>,
    }

    impl HeaderIndexingStrategy for PanicStrategy {
        fn index_header(&self, name: &HeaderName, _value: &[u8], _exists: bool) -> bool {
            self.calls.set(self.calls.get() + 1);
            if self.fail.get() && name.as_bytes() == b"x-panic" {
                panic!("codec indexing panic");
            }
            self.calls.get() % 2 == 1
        }

        fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
            self.clones.set(self.clones.get() + 1);
            Box::new(Self {
                calls: Cell::new(self.calls.get()),
                clones: self.clones.clone(),
                fail: self.fail.clone(),
            })
        }
    }

    let fail = Rc::new(Cell::new(true));
    let strategy = Rc::new(PanicStrategy {
        calls: Cell::new(0),
        clones: Rc::new(Cell::new(0)),
        fail: fail.clone(),
    });
    let stats = Rc::new(HeaderEncodeStats::default());
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    codec.set_header_indexing_strategy(strategy.clone());
    codec.set_header_codec_stats(Some(stats.clone()));
    let before = codec.compression_info().egress;
    let mut message = request(b"/panic-retry");
    message
        .headers_mut()
        .add(b"x-first", b"indexed before panic");
    message.headers_mut().add(b"x-panic", b"last field");
    let event = || EgressFrame::Headers {
        stream: StreamId::new(1),
        message: &message,
        end_stream: true,
        extra_headers: None,
    };
    let mut output = BufQueue::new(4096);
    output.push_static(b"kept").unwrap();
    let mut callback = Trace::default();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        codec.generate(&mut output, event(), &mut callback)
    }))
    .unwrap_err();
    fail.set(false);
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"codec indexing panic"));
    assert_eq!(bytes(&output), b"kept");
    assert_eq!(codec.compression_info().egress, before);
    assert_eq!(strategy.calls.get(), 0);
    assert_eq!(strategy.clones.get(), 1);
    assert!(stats.0.borrow().is_empty());
    assert!(callback.events.is_empty());

    let retried = codec.generate(&mut output, event(), &mut callback).unwrap();
    let mut reference = Http2Codec::new(TransportDirection::Upstream);
    reference.set_header_indexing_strategy(strategy.clone());
    let mut expected = BufQueue::new(4096);
    expected.push_static(b"kept").unwrap();
    let mut expected_callback = Trace::default();
    let generated = reference
        .generate(&mut expected, event(), &mut expected_callback)
        .unwrap();
    assert_eq!(bytes(&output), bytes(&expected));
    assert_eq!(callback.events, expected_callback.events);
    assert_eq!(retried.header_size, generated.header_size);
    assert_eq!(
        codec.compression_info().egress,
        reference.compression_info().egress
    );
    assert_eq!(stats.0.borrow().as_slice(), &[retried.header_size.unwrap()]);
}

#[test]
fn settings_generation_failure_does_not_commit_pending_state() {
    let settings = [
        CodecSetting {
            identifier: SettingsId::HeaderTableSize.as_u16(),
            value: 2048,
        },
        CodecSetting {
            identifier: SettingsId::MaxHeaderListSize.as_u16(),
            value: 1,
        },
    ];
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    assert!(
        codec
            .generate(
                &mut BufQueue::new(8),
                EgressFrame::Settings {
                    settings: &settings,
                },
                &mut Trace::default(),
            )
            .is_err()
    );
    open(&mut codec, &mut Trace::default());
    assert_eq!(codec.create_stream(), StreamId::new(1));
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut response_wire = BufQueue::new(1024);
    peer.generate(
        &mut response_wire,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &response(200),
            end_stream: true,
            extra_headers: None,
        },
        &mut Trace::default(),
    )
    .unwrap();
    let response_wire = bytes(&response_wire);
    let mut response_trace = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &response_wire, &mut response_trace).consumed,
        response_wire.len()
    );
    assert!(response_trace.events.contains(&Seen::Complete(1)));

    let mut retried = BufQueue::new(1024);
    codec
        .generate(
            &mut retried,
            EgressFrame::Settings {
                settings: &settings,
            },
            &mut Trace::default(),
        )
        .unwrap();
    let parsed = parse_frame(&mut retried).unwrap().unwrap();
    let Frame::Settings(settings) = parsed.frame else {
        panic!("expected SETTINGS frame");
    };
    assert_eq!(
        settings,
        [
            Setting {
                identifier: SettingsId::HeaderTableSize.as_u16(),
                value: 2048,
            },
            Setting {
                identifier: SettingsId::MaxHeaderListSize.as_u16(),
                value: 1,
            },
        ]
    );
}

#[test]
fn goaway_generation_failure_does_not_commit_watermark() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    assert!(codec.is_reusable());
    assert!(
        codec
            .generate(
                &mut BufQueue::new(16),
                EgressFrame::Goaway {
                    last_stream: StreamId::new(1),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
                &mut Trace::default(),
            )
            .is_err()
    );
    assert!(codec.is_reusable());
    let mut retry = BufQueue::new(64);
    assert_eq!(
        codec
            .generate(
                &mut retry,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(1),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
                &mut Trace::default(),
            )
            .unwrap()
            .bytes_written,
        17
    );
    assert!(!codec.is_reusable());
}

#[test]
fn failed_eom_keeps_websocket_response_upgrade_state() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let stream = codec.create_stream();
    let mut request = request(b"/chat");
    request.set_method(HttpMethod::Connect).unwrap();
    request.set_secure(true);
    request.set_upgrade_protocol(b"websocket");
    request.set_egress_websocket_upgrade();
    codec
        .generate(
            &mut BufQueue::new(4096),
            EgressFrame::Headers {
                stream,
                message: &request,
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    assert!(
        codec
            .generate(
                &mut BufQueue::new(8),
                EgressFrame::EndOfMessage { stream },
                &mut Trace::default(),
            )
            .is_err()
    );

    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut response_wire = BufQueue::new(4096);
    peer.generate(
        &mut response_wire,
        EgressFrame::Headers {
            stream,
            message: &response(200),
            end_stream: true,
            extra_headers: None,
        },
        &mut Trace::default(),
    )
    .unwrap();
    let mut callback = MessageCapture::default();
    let wire = bytes(&response_wire);
    assert_eq!(
        codec
            .on_ingress(&queue(&wire), &mut callback, &mut BufQueue::new(1024))
            .consumed,
        wire.len()
    );
    assert!(callback.messages[0].is_ingress_websocket_upgrade());
}

struct PanicOnGenerated;

impl HttpCodecCallback for PanicOnGenerated {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if matches!(event, CodecEvent::GeneratedFrameHeader { .. }) {
            panic!("generated callback panic");
        }
    }
}

#[test]
fn websocket_upgrade_state_rolls_back_on_output_failure() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let stream = codec.create_stream();
    let mut request = request(b"/chat");
    request.set_method(HttpMethod::Connect).unwrap();
    request.set_secure(true);
    request.set_upgrade_protocol(b"websocket");
    request.set_egress_websocket_upgrade();
    assert!(
        codec
            .generate(
                &mut BufQueue::new(8),
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .is_err()
    );

    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut response_wire = BufQueue::new(4096);
    peer.generate(
        &mut response_wire,
        EgressFrame::Headers {
            stream,
            message: &response(200),
            end_stream: true,
            extra_headers: None,
        },
        &mut Trace::default(),
    )
    .unwrap();
    let mut callback = MessageCapture::default();
    let response_wire = bytes(&response_wire);
    assert_eq!(
        codec
            .on_ingress(
                &queue(&response_wire),
                &mut callback,
                &mut BufQueue::new(1024),
            )
            .consumed,
        response_wire.len()
    );
    assert!(!callback.messages[0].is_ingress_websocket_upgrade());
}

#[test]
fn websocket_upgrade_state_commits_before_generated_callback() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let stream = codec.create_stream();
    let mut request = request(b"/chat");
    request.set_method(HttpMethod::Connect).unwrap();
    request.set_secure(true);
    request.set_upgrade_protocol(b"websocket");
    request.set_egress_websocket_upgrade();
    let mut request_wire = BufQueue::new(4096);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _ = codec.generate(
                &mut request_wire,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut PanicOnGenerated,
            );
        }))
        .is_err()
    );
    assert!(!request_wire.is_empty());

    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut response_wire = BufQueue::new(4096);
    peer.generate(
        &mut response_wire,
        EgressFrame::Headers {
            stream,
            message: &response(200),
            end_stream: true,
            extra_headers: None,
        },
        &mut Trace::default(),
    )
    .unwrap();
    let mut callback = MessageCapture::default();
    let response_wire = bytes(&response_wire);
    assert_eq!(
        codec
            .on_ingress(
                &queue(&response_wire),
                &mut callback,
                &mut BufQueue::new(1024),
            )
            .consumed,
        response_wire.len()
    );
    assert!(callback.messages[0].is_ingress_websocket_upgrade());
}

/// With no WebSocket upgrade pending, no codec path looks a stream up in the upgrade set: an end
/// of message, an upstream response and its trailers, an egress reset and an ingress reset.
#[test]
fn streams_without_a_pending_upgrade_never_probe_the_upgrade_set() {
    let generate = |codec: &mut Http2Codec, frame: EgressFrame<'_>| {
        let mut output = BufQueue::new(4096);
        codec
            .generate(&mut output, frame, &mut Trace::default())
            .unwrap();
        bytes(&output)
    };
    let request = request(b"/a");
    let request_headers = |stream| EgressFrame::Headers {
        stream,
        message: &request,
        end_stream: false,
        extra_headers: None,
    };
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    open(&mut codec, &mut Trace::default());
    let first = codec.create_stream();
    generate(&mut codec, request_headers(first));
    Http2Codec::take_upgraded_stream_probes();
    let mut probes = Vec::new();

    generate(&mut codec, EgressFrame::EndOfMessage { stream: first });
    probes.push(("end of message", Http2Codec::take_upgraded_stream_probes()));

    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let response_wire = generate(
        &mut peer,
        EgressFrame::Headers {
            stream: first,
            message: &response(200),
            end_stream: false,
            extra_headers: None,
        },
    );
    let trailers_wire = generate(
        &mut peer,
        EgressFrame::Trailers {
            stream: first,
            trailers: &sample_trailers(),
        },
    );
    for (step, wire, event) in [
        ("response", response_wire, "headers"),
        ("trailers", trailers_wire, "trailers"),
    ] {
        let mut capture = MessageCapture::default();
        assert_eq!(
            codec
                .on_ingress(&queue(&wire), &mut capture, &mut BufQueue::new(1024))
                .consumed,
            wire.len()
        );
        assert!(capture.events.contains(&event), "{step} is delivered");
        probes.push((step, Http2Codec::take_upgraded_stream_probes()));
    }

    let second = codec.create_stream();
    generate(&mut codec, request_headers(second));
    generate(
        &mut codec,
        EgressFrame::Reset {
            stream: second,
            code: H2ErrorCode::CANCEL,
        },
    );
    probes.push(("egress reset", Http2Codec::take_upgraded_stream_probes()));

    let third = codec.create_stream();
    generate(&mut codec, request_headers(third));
    let mut reset_wire = BufQueue::new(64);
    frame::write_rst_stream(
        &mut reset_wire,
        u32::try_from(third.get()).unwrap(),
        H2ErrorCode::CANCEL,
    )
    .unwrap();
    let reset_wire = bytes(&reset_wire);
    let mut trace = Trace::default();
    assert_eq!(
        codec
            .on_ingress(&queue(&reset_wire), &mut trace, &mut BufQueue::new(1024))
            .consumed,
        reset_wire.len()
    );
    assert!(
        trace
            .events
            .contains(&Seen::Abort(third.get(), H2ErrorCode::CANCEL))
    );
    probes.push(("ingress reset", Http2Codec::take_upgraded_stream_probes()));

    assert_eq!(
        probes,
        [
            ("end of message", 0),
            ("response", 0),
            ("trailers", 0),
            ("egress reset", 0),
            ("ingress reset", 0),
        ],
        "an empty upgrade set is never probed"
    );
}

#[test]
fn generated_ping_tokens_are_nonzero_and_fresh() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut output = BufQueue::new(128);
    for _ in 0..2 {
        codec
            .generate(
                &mut output,
                EgressFrame::PingRequest { data: None },
                &mut Trace::default(),
            )
            .unwrap();
    }
    let first = parse_frame(&mut output).unwrap().unwrap();
    let second = parse_frame(&mut output).unwrap().unwrap();
    let Frame::Ping(first) = first.frame else {
        panic!("expected PING frame");
    };
    let Frame::Ping(second) = second.frame else {
        panic!("expected PING frame");
    };
    assert_ne!(first, 0);
    assert_ne!(second, 0);
    assert_ne!(first, second);
}

#[test]
fn method_validation_matches_trailing_hyphen_compatibility() {
    assert!(validate_method(b"GET-"));
    assert!(validate_method(b"M-SEARCH"));
    assert!(!validate_method(b"-GET"));
    assert!(!validate_method(b"G3T"));
}

#[test]
fn header_value_validation_preserves_escape_and_folding_modes() {
    let cases: &[(&[u8], [bool; 3])] = &[
        (b"", [true, true, true]),
        (b"a", [true, true, true]),
        (b"128", [true, true, true]),
        (b"Wed, 16 Sep 2026 12:34:56 GMT", [true, true, true]),
        (b"\\", [true, true, true]),
        (b"\t", [true, true, true]),
        (b"\"", [true, true, true]),
        (b"\"abc\"", [true, true, true]),
        (b"\"abc", [true, true, true]),
        (b"\"\\", [false, false, false]),
        (b"\"\\\x01\"", [true, false, false]),
        (b"\"\\\r\\\n\"", [true, false, false]),
        (b"\"\\\\", [true, false, false]),
        (b"\"\\\"\\", [false, true, true]),
        (b"\x80", [true, true, false]),
        (b"\xff", [true, true, false]),
        (b"\x7f", [false, false, false]),
        (b"\0", [false, false, false]),
        (b"\n", [false, false, false]),
        (b"abc\r\n\tdef", [true, true, true]),
        (b"abc\r\n \t \t def", [true, true, true]),
        (b"\r", [false, false, false]),
        (b"\r\n", [false, false, false]),
        (b"\r\n ", [true, true, true]),
        (b"\r\n \r\n\t", [true, true, true]),
        (b"\r \tdef", [false, false, false]),
        (b"\r\ndef", [false, false, false]),
        (b"\r\n \x01", [false, false, false]),
        (b"\r\n \"\\", [false, false, false]),
        (b"\"\r\n \\", [false, false, false]),
        (b"\r\n \\", [true, true, true]),
        (b"\r\n \xff", [true, true, false]),
    ];
    for &(value, expected) in cases {
        for prefix in [b"".as_slice(), b"\t\\ !#~".as_slice()] {
            let mut input = prefix.to_vec();
            input.extend_from_slice(value);
            for (mode, expected) in [
                ControlEscapeMode::Compliant,
                ControlEscapeMode::StrictCompat,
                ControlEscapeMode::Strict,
            ]
            .into_iter()
            .zip(expected)
            {
                assert_eq!(
                    validate_header_value(&input, mode),
                    expected,
                    "value={input:?}, mode={mode:?}",
                );
            }
        }
    }
}

const VALUE_MODES: [ControlEscapeMode; 3] = [
    ControlEscapeMode::Compliant,
    ControlEscapeMode::StrictCompat,
    ControlEscapeMode::Strict,
];

/// A frozen copy of `validate_header_value` before the word-at-a-time
/// prefix scan.
fn reference_validate_header_value(value: &[u8], mode: ControlEscapeMode) -> bool {
    #[derive(Clone, Copy)]
    enum Fold {
        None,
        Newline,
        Whitespace,
        MoreWhitespace,
    }

    let mut escaped = false;
    let mut quoted = false;
    let mut fold = Fold::None;
    let mut index = value
        .iter()
        .position(|byte| !matches!(*byte, b'\t' | 0x20..=0x21 | 0x23..=0x7e))
        .unwrap_or(value.len());
    while index < value.len() {
        let byte = value[index];
        if escaped {
            escaped = false;
            if mode == ControlEscapeMode::Compliant {
                index += 1;
                continue;
            }
        }
        match fold {
            Fold::None => match byte {
                b'\\' if quoted => escaped = true,
                b'"' => quoted = !quoted,
                b'\r' => fold = Fold::Newline,
                byte if (byte < 0x20 && byte != b'\t')
                    || byte == 0x7f
                    || (byte > 0x7f && mode == ControlEscapeMode::Strict) =>
                {
                    return false;
                }
                _ => {}
            },
            Fold::Newline => {
                if byte != b'\n' {
                    return false;
                }
                fold = Fold::Whitespace;
            }
            Fold::Whitespace => {
                if !matches!(byte, b' ' | b'\t') {
                    return false;
                }
                fold = Fold::MoreWhitespace;
            }
            Fold::MoreWhitespace => {
                if !matches!(byte, b' ' | b'\t') {
                    fold = Fold::None;
                    continue;
                }
            }
        }
        index += 1;
    }
    !escaped && matches!(fold, Fold::None | Fold::MoreWhitespace)
}

#[test]
fn value_prescan_matches_the_byte_loop_exhaustively() {
    for length in 1..=24 {
        for position in 0..length {
            for byte in 0..=u8::MAX {
                let mut value = vec![b'a'; length];
                value[position] = byte;
                for mode in VALUE_MODES {
                    assert_eq!(
                        validate_header_value(&value, mode),
                        reference_validate_header_value(&value, mode),
                        "value={value:?}, mode={mode:?}",
                    );
                }
            }
        }
    }
}

#[test]
fn long_values_match_with_a_stride() {
    let accepted: Vec<u8> = (0..=u8::MAX)
        .filter(|byte| matches!(*byte, b'\t' | 0x20..=0x21 | 0x23..=0x7e))
        .collect();
    let rejected: Vec<u8> = (0..=u8::MAX)
        .filter(|byte| !accepted.contains(byte))
        .collect();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for length in 25..=4096 {
        let clean: Vec<u8> = (0..length)
            .map(|_| accepted[next() as usize % accepted.len()])
            .collect();
        let stride = length / 7 + 1;
        let positions =
            (0..length)
                .step_by(stride)
                .chain([7, 8, length - 9, length - 8, length - 1]);
        let mut values = vec![clean.clone()];
        for position in positions {
            let mut value = clean.clone();
            value[position] = rejected[next() as usize % rejected.len()];
            values.push(value);
        }
        for value in values {
            for mode in VALUE_MODES {
                assert_eq!(
                    validate_header_value(&value, mode),
                    reference_validate_header_value(&value, mode),
                    "length={length}, value={value:?}, mode={mode:?}",
                );
            }
        }
    }
}

#[test]
fn parser_pause_does_not_capture_or_replace_the_per_call_callback() {
    let wire = encoded_headers(1, &request_fields(b"/borrowed-callback"), true);
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    codec.set_parser_paused(true);
    assert!(!codec.is_parser_paused());
    let mut first_callback = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire[..FRAME_HEADER_SIZE], &mut first_callback).consumed,
        FRAME_HEADER_SIZE
    );
    let mut second_callback = Trace::default();
    assert_eq!(
        ingress_one(&mut codec, &wire[FRAME_HEADER_SIZE..], &mut second_callback).consumed,
        wire.len() - FRAME_HEADER_SIZE
    );
    assert!(first_callback.events.iter().any(|event| matches!(
        event,
        Seen::Frame(1, frame_type, _) if *frame_type == FrameType::HEADERS.as_u8()
    )));
    assert!(second_callback.events.contains(&Seen::Complete(1)));
}

#[test]
fn single_frame_data_preserves_prefilled_output_and_commit_boundaries() {
    let prefixes = [Bytes::from_static(b"pre"), Bytes::from_static(b"fix")];
    for prefix_count in [1, 2] {
        let prefix_length = prefix_count * 3;
        let prefix_pointers = prefixes[..prefix_count]
            .iter()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>();
        let prefixed_output = |limit| {
            let mut output = BufQueue::new(limit);
            for prefix in prefixes.iter().take(prefix_count) {
                output.push_bytes(prefix.clone()).unwrap();
            }
            output
        };
        for payload in [b"".as_slice(), b"body".as_slice()] {
            let mut body = BufQueue::new(payload.len());
            for part in payload.chunks(2) {
                body.push_bytes(Bytes::copy_from_slice(part)).unwrap();
            }
            let body_pointers = body
                .segments()
                .map(|segment| segment.as_ptr())
                .collect::<Vec<_>>();
            for (padding, padding_bytes, payload_offset, flags) in [
                (None, 0, 9, Flags::END_STREAM),
                (Some(0), 1, 10, Flags::END_STREAM | Flags::PADDED),
                (Some(2), 3, 10, Flags::END_STREAM | Flags::PADDED),
            ] {
                let frame_length = FRAME_HEADER_SIZE + payload.len() + padding_bytes;
                let mut codec = Http2Codec::new(TransportDirection::Upstream);
                let stream = codec.create_stream();
                let event = || EgressFrame::Body {
                    stream,
                    body: body.clone_shared(),
                    padding,
                    end_stream: true,
                };
                let mut trace = Trace::default();
                let mut short = prefixed_output(prefix_length + frame_length - 1);
                let error = codec.generate(&mut short, event(), &mut trace).unwrap_err();
                assert_eq!(
                    error.message(),
                    format!(
                        "buffer limit {} exceeded: 0 existing bytes plus {} incoming bytes",
                        frame_length - 1,
                        frame_length,
                    )
                    .as_bytes(),
                );
                assert_eq!(bytes(&short), &b"prefix"[..prefix_length]);
                assert_eq!(
                    short
                        .segments()
                        .map(|segment| segment.as_ptr())
                        .collect::<Vec<_>>(),
                    prefix_pointers,
                );
                assert!(trace.events.is_empty());

                let mut output = prefixed_output(prefix_length + frame_length);
                let generated = codec.generate(&mut output, event(), &mut trace).unwrap();
                assert_eq!(generated.bytes_written, frame_length);
                assert_eq!(generated.payload_offset, Some(payload_offset));
                assert_eq!(
                    trace.events,
                    [Seen::Generated(
                        stream.get(),
                        FrameType::DATA.as_u8(),
                        frame_length as u64,
                    )],
                );
                let expected_wire = bytes(&output);
                assert_eq!(&expected_wire[..prefix_length], &b"prefix"[..prefix_length]);
                assert_eq!(
                    output
                        .segments()
                        .take(prefix_count)
                        .map(|segment| segment.as_ptr())
                        .collect::<Vec<_>>(),
                    prefix_pointers,
                );
                output.trim_start(prefix_length).unwrap();
                let parsed = parse_frame(&mut output).unwrap().unwrap();
                assert_eq!(parsed.header.stream, stream.get() as u32);
                assert_eq!(parsed.header.flags, flags);
                assert_eq!(parsed.header.length as usize, payload.len() + padding_bytes);
                assert_eq!(parsed.consumed, frame_length);
                let Frame::Data(data) = parsed.frame else {
                    panic!("expected DATA");
                };
                assert_eq!(bytes(&data.data), payload);
                assert_eq!(data.padding as usize, padding_bytes);
                assert_eq!(
                    data.data
                        .segments()
                        .map(|segment| segment.as_ptr())
                        .collect::<Vec<_>>(),
                    body_pointers,
                );
                assert!(output.is_empty());

                let mut output = prefixed_output(prefix_length + frame_length);
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    codec
                        .generate(&mut output, event(), &mut PanicOnGenerated)
                        .unwrap();
                }))
                .unwrap_err();
                assert_eq!(
                    panic.downcast_ref::<&str>(),
                    Some(&"generated callback panic"),
                );
                assert_eq!(bytes(&output), expected_wire);
            }
        }
    }
}

#[test]
fn data_append_preserves_prefix_and_payload_ownership() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct Owner {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    for prefix in [
        "unique", "many", "trimmed", "split", "aliased", "static", "opaque", "short",
    ] {
        for outcome in ["complete", "limited", "panic"] {
            let frame_length = FRAME_HEADER_SIZE + 4;
            let limit = 6 + frame_length - usize::from(outcome == "limited");
            let mut output = BufQueue::new(limit);
            let prefix_drops = Arc::new(AtomicUsize::new(0));
            let mut alias = None;
            match prefix {
                "static" => output.push_static(b"prefix").unwrap(),
                "opaque" => output
                    .push_bytes(Bytes::from_owner(Owner {
                        bytes: b"prefix".to_vec(),
                        drops: prefix_drops.clone(),
                    }))
                    .unwrap(),
                _ => {
                    let mut tail = Vec::with_capacity(if prefix == "short" { 8 } else { 32 });
                    if prefix == "many" {
                        output.push_static(b"pre").unwrap();
                        tail.extend_from_slice(b"fix");
                    } else {
                        if matches!(prefix, "trimmed" | "split") {
                            tail.extend_from_slice(b"xx");
                        }
                        tail.extend_from_slice(b"prefix");
                    }
                    if prefix == "aliased" {
                        let tail = Bytes::from(tail);
                        alias = Some(tail.clone());
                        output.push_bytes(tail).unwrap();
                    } else {
                        output.push_vec(tail).unwrap();
                    }
                    if prefix == "trimmed" {
                        output.trim_start(2).unwrap();
                    } else if prefix == "split" {
                        drop(output.split_at_most(2));
                    }
                }
            }
            let prefix_pointers = output
                .segments()
                .map(|segment| segment.as_ptr())
                .collect::<Vec<_>>();
            let prefix_bound = output.backing_capacity_upper_bound();
            let body_drops = Arc::new(AtomicUsize::new(0));
            let body = Bytes::from_owner(Owner {
                bytes: b"body".to_vec(),
                drops: body_drops.clone(),
            });
            let body_pointer = body.as_ptr();
            let mut codec = Http2Codec::new(TransportDirection::Upstream);
            let stream = codec.create_stream();
            let event = EgressFrame::Body {
                stream,
                body: BufQueue::from_bytes(body, 4).unwrap(),
                padding: None,
                end_stream: true,
            };
            let mut trace = Trace::default();
            let result = catch_unwind(AssertUnwindSafe(|| {
                if outcome == "panic" {
                    codec.generate(&mut output, event, &mut PanicOnGenerated)
                } else {
                    codec.generate(&mut output, event, &mut trace)
                }
            }));
            if outcome == "limited" {
                let error = result.unwrap().unwrap_err();
                assert_eq!(
                    error.message(),
                    format!(
                        "buffer limit {} exceeded: 0 existing bytes plus {} incoming bytes",
                        frame_length - 1,
                        frame_length,
                    )
                    .as_bytes(),
                );
                assert_eq!(bytes(&output), b"prefix");
                assert_eq!(output.backing_capacity_upper_bound(), prefix_bound);
                assert!(trace.events.is_empty());
                assert_eq!(body_drops.load(Ordering::SeqCst), 1);
            } else {
                if outcome == "panic" {
                    assert_eq!(
                        result.unwrap_err().downcast_ref::<&str>(),
                        Some(&"generated callback panic"),
                    );
                } else {
                    let generated = result.unwrap().unwrap();
                    assert_eq!(generated.bytes_written, frame_length);
                    assert_eq!(generated.payload_offset, Some(FRAME_HEADER_SIZE as u64));
                    assert_eq!(
                        trace.events,
                        [Seen::Generated(stream.get(), FrameType::DATA.as_u8(), 13)],
                    );
                }
                assert_eq!(bytes(&output), b"prefix\0\0\x04\0\x01\0\0\0\x01body");
                assert_eq!(body_drops.load(Ordering::SeqCst), 0);
            }
            assert_eq!(
                output
                    .segments()
                    .take(prefix_pointers.len())
                    .map(|segment| segment.as_ptr())
                    .collect::<Vec<_>>(),
                prefix_pointers,
            );
            assert_eq!(output.limit(), limit);
            assert_eq!(
                output.remaining_capacity(),
                usize::from(outcome == "limited") * (frame_length - 1),
            );
            assert_eq!(prefix_drops.load(Ordering::SeqCst), 0);
            if let Some(alias) = alias {
                assert_eq!(alias.as_ref(), b"prefix");
            }
            if outcome != "limited" {
                output.trim_start(6).unwrap();
                let parsed = parse_frame(&mut output).unwrap().unwrap();
                assert_eq!(parsed.header.stream, stream.get() as u32);
                assert_eq!(parsed.header.flags, Flags::END_STREAM);
                assert_eq!(parsed.consumed, frame_length);
                let Frame::Data(data) = parsed.frame else {
                    panic!("expected DATA");
                };
                assert_eq!(bytes(&data.data), b"body");
                assert_eq!(data.data.front().unwrap().as_ptr(), body_pointer);
                assert!(output.is_empty());
                drop(output);
                assert_eq!(body_drops.load(Ordering::SeqCst), 0);
                drop(data);
                assert_eq!(body_drops.load(Ordering::SeqCst), 1);
            } else {
                drop(output);
            }
            assert_eq!(
                prefix_drops.load(Ordering::SeqCst),
                usize::from(prefix == "opaque"),
            );
        }
    }
}

#[test]
fn data_egress_preserves_segment_backing() {
    let max_frame = MAX_FRAME_PAYLOAD_LENGTH_MIN as usize;
    for reuse_payload_headroom in [false, true] {
        let first = Bytes::from(vec![b'a'; max_frame - 2]);
        let second = Bytes::from_static(b"middle");
        let third = Bytes::from_static(b"tail");
        let pointers = [first.as_ptr(), second.as_ptr(), third.as_ptr()];
        let body_length = first.len() + second.len() + third.len();
        let mut body = BufQueue::new(body_length);
        body.push_bytes(first).expect("first body segment fits");
        body.push_bytes(second).expect("second body segment fits");
        body.push_bytes(third).expect("third body segment fits");
        let expected_written = body_length + 2 * (FRAME_HEADER_SIZE + 3);
        let mut output = BufQueue::new(expected_written);
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        codec.set_reuse_payload_headroom(reuse_payload_headroom);
        let stream = codec.create_stream();
        let mut callback = Trace::default();

        let mut too_small = BufQueue::new(expected_written - 1);
        assert!(
            codec
                .generate(
                    &mut too_small,
                    EgressFrame::Body {
                        stream,
                        body: body.clone_shared(),
                        padding: Some(2),
                        end_stream: true,
                    },
                    &mut callback,
                )
                .is_err()
        );
        assert!(too_small.is_empty());
        assert!(callback.events.is_empty());

        let generated = codec
            .generate(
                &mut output,
                EgressFrame::Body {
                    stream,
                    body,
                    padding: Some(2),
                    end_stream: true,
                },
                &mut callback,
            )
            .expect("segmented DATA generation succeeds");

        assert_eq!(generated.bytes_written, expected_written);
        assert_eq!(generated.payload_offset, Some(10));
        assert_eq!(
            callback.events,
            [
                Seen::Generated(stream.get(), FrameType::DATA.as_u8(), 16_396),
                Seen::Generated(stream.get(), FrameType::DATA.as_u8(), 20),
            ]
        );
        let segments = output.segments().collect::<Vec<_>>();
        assert_eq!(
            segments
                .iter()
                .map(|segment| segment.len())
                .collect::<Vec<_>>(),
            [10, max_frame - 2, 2, 2, 10, 4, 4, 2]
        );
        assert_eq!(segments[0][4], Flags::PADDED);
        assert_eq!(segments[1].as_ptr(), pointers[0]);
        assert_eq!(segments[2].as_ptr(), pointers[1]);
        assert_eq!(segments[4][4], Flags::PADDED | Flags::END_STREAM);
        assert_eq!(segments[5].as_ptr(), pointers[1].wrapping_add(2));
        assert_eq!(segments[6].as_ptr(), pointers[2]);
        assert_eq!(segments[3].as_ref(), &[0, 0]);
        assert_eq!(segments[7].as_ref(), &[0, 0]);
    }
}

#[test]
fn response_status_encoding_preserves_u16_decimal_values() {
    for (status, expected) in [
        (None, "0"),
        (Some(0), "0"),
        (Some(9), "9"),
        (Some(10), "10"),
        (Some(99), "99"),
        (Some(100), "100"),
        (Some(200), "200"),
        (Some(204), "204"),
        (Some(206), "206"),
        (Some(304), "304"),
        (Some(400), "400"),
        (Some(404), "404"),
        (Some(500), "500"),
        (Some(999), "999"),
        (Some(1_000), "1000"),
        (Some(9_999), "9999"),
        (Some(10_000), "10000"),
        (Some(65_535), "65535"),
    ] {
        let mut message = HttpMessage::new();
        if let Some(status) = status {
            message.set_status_code(status).unwrap();
        } else {
            assert_eq!(message.status_code().unwrap(), 0);
        }
        let date = b"Thu, 01 Jan 1970 00:00:00 GMT";
        message.headers_mut().set(HttpHeaderCode::Date, date);
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut output = BufQueue::new(4096);
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .unwrap();
        let parsed = parse_frame(&mut output).unwrap().unwrap();
        assert_eq!(parsed.header.stream, 1);
        assert_eq!(parsed.header.flags, Flags::END_HEADERS);
        let Frame::Headers(block) = parsed.frame else {
            panic!("expected the response HEADERS frame");
        };
        assert!(output.is_empty());
        let decoded = HpackCodec::new()
            .decode_buf_queue(&block, block.chain_length())
            .unwrap();
        assert_eq!(
            decoded.headers,
            [
                HpackHeader::new(":status", expected),
                HpackHeader::new("date", date)
            ],
            "status={status:?}"
        );
    }
}

fn memo_ingress(codec: &mut Http2Codec, wire: &BufQueue, callback: &mut dyn HttpCodecCallback) {
    let result = codec.on_ingress(wire, callback, &mut BufQueue::new(4096));
    assert_eq!(result.consumed, wire.chain_length());
}

fn memo_wire(
    codec: &mut Http2Codec,
    stream: StreamId,
    message: &HttpMessage,
    end: bool,
) -> BufQueue {
    let mut output = BufQueue::new(16 * 1024);
    codec
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message,
                end_stream: end,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    output
}

#[test]
fn whole_header_memos_bypass_both_loops_on_requests_and_responses() {
    for downstream in [true, false] {
        let send_direction = if downstream {
            TransportDirection::Upstream
        } else {
            TransportDirection::Downstream
        };
        let receive_direction = if downstream {
            TransportDirection::Downstream
        } else {
            TransportDirection::Upstream
        };
        let mut sender = Http2Codec::new(send_direction);
        let mut receiver = Http2Codec::new(receive_direction);
        let mut reference_sender = Http2Codec::new(send_direction);
        let mut reference_receiver = Http2Codec::new(receive_direction);
        reference_sender.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
        reference_receiver.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
        open(&mut receiver, &mut Trace::default());
        open(&mut reference_receiver, &mut Trace::default());
        let mut message = if downstream {
            request(b"/memo")
        } else {
            response(200)
        };
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"0");
        if !downstream {
            message
                .headers_mut()
                .set(HttpHeaderCode::Date, b"Mon, 21 Sep 2026 12:00:00 GMT");
        }
        let mut previous = None;
        for ordinal in 0..6 {
            let stream = if downstream {
                let id = sender.create_stream();
                assert_eq!(reference_sender.create_stream(), id);
                id
            } else {
                let id = receiver.create_stream();
                assert_eq!(reference_receiver.create_stream(), id);
                id
            };
            let before_encode = sender.header_loop_counts().0;
            let before_decode = receiver.header_loop_counts().1;
            let end = ordinal % 2 == 0;
            let wire = memo_wire(&mut sender, stream, &message, end);
            let expected = memo_wire(&mut reference_sender, stream, &message, end);
            assert_eq!(bytes(&wire), bytes(&expected));
            let mut inspected = queue(&bytes(&wire));
            assert_eq!(
                parse_frame(&mut inspected).unwrap().unwrap().header.stream,
                stream.get() as u32
            );
            let started = std::time::Instant::now();
            let mut actual = MessageCapture::default();
            let mut reference = MessageCapture::default();
            memo_ingress(&mut receiver, &wire, &mut actual);
            memo_ingress(&mut reference_receiver, &expected, &mut reference);
            assert_eq!(actual.events, reference.events);
            assert_eq!(actual.messages.len(), 1);
            let mut received = actual.messages.pop().unwrap();
            let expected = reference.messages.pop().unwrap();
            assert_eq!(received.headers(), expected.headers());
            assert_eq!(received.request_url(), expected.request_url());
            assert_eq!(received.response_status(), expected.response_status());
            assert_eq!(
                received.ingress_header_size(),
                expected.ingress_header_size()
            );
            assert_eq!(received.is_chunked(), expected.is_chunked());
            assert!(received.start_time() >= started);
            assert!(!received.headers().exists(b"x-mutated-after-delivery"));
            if ordinal >= 3 {
                assert_eq!(sender.header_loop_counts().0, before_encode);
                assert_eq!(receiver.header_loop_counts().1, before_decode);
            }
            assert_eq!(
                sender.compression_info().egress,
                reference_sender.compression_info().egress
            );
            assert_eq!(
                receiver.compression_info().ingress,
                reference_receiver.compression_info().ingress
            );
            received
                .headers_mut()
                .set(b"x-mutated-after-delivery", b"old-message");
            previous = Some(received);
        }
        assert!(
            previous
                .unwrap()
                .headers()
                .exists(b"x-mutated-after-delivery")
        );
        assert_eq!(receiver.header_template_copies(), 1);
    }
}

#[test]
fn memo_replay_message_equals_fresh_decode() {
    for downstream in [true, false] {
        let (send_direction, receive_direction) = if downstream {
            (TransportDirection::Upstream, TransportDirection::Downstream)
        } else {
            (TransportDirection::Downstream, TransportDirection::Upstream)
        };
        let mut sender = Http2Codec::new(send_direction);
        let mut receiver = Http2Codec::new(receive_direction);
        let mut reference = Http2Codec::new(receive_direction);
        reference.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
        open(&mut receiver, &mut Trace::default());
        open(&mut reference, &mut Trace::default());
        let mut message = if downstream {
            let mut message = request(b"/memo/path?query=1&other=two");
            message.set_secure(true);
            message.headers_mut().add(b"x-request-id", b"memo-request");
            message
                .headers_mut()
                .add(HttpHeaderCode::UserAgent, b"memo-agent");
            message
        } else {
            let mut message = response(200);
            message
                .headers_mut()
                .set(HttpHeaderCode::ContentType, b"text/plain");
            message
        };
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"0");
        for ordinal in 0..6 {
            let stream = if downstream {
                sender.create_stream()
            } else {
                let id = receiver.create_stream();
                assert_eq!(reference.create_stream(), id);
                id
            };
            let before_decode = receiver.header_loop_counts().1;
            let wire = memo_wire(&mut sender, stream, &message, ordinal % 2 == 0);
            let mut actual = MessageCapture::default();
            let mut expected = MessageCapture::default();
            memo_ingress(&mut receiver, &wire, &mut actual);
            memo_ingress(&mut reference, &wire, &mut expected);
            assert_eq!(actual.events, expected.events);
            let mut replayed = actual.messages.pop().unwrap();
            let mut decoded = expected.messages.pop().unwrap();
            if ordinal >= 3 {
                assert_eq!(receiver.header_loop_counts().1, before_decode);
            }
            let common = std::time::Instant::now();
            replayed.set_start_time(common);
            decoded.set_start_time(common);
            assert_eq!(
                format!("{replayed:?}"),
                format!("{decoded:?}"),
                "downstream {downstream} ordinal {ordinal}"
            );
        }
        assert_eq!(receiver.header_template_copies(), 1);
        assert_eq!(reference.header_template_copies(), 0);
    }
}

/// The consumer-shaped client's request HEADERS block for `id`, as a load generator writes it:
/// static-table indices and literals without indexing only, so decoding it never changes the
/// dynamic table.
fn consumer_request_block(id: u64) -> Vec<u8> {
    let mut block = vec![0x80 | 3, 0x80 | 6, 4, 5];
    block.extend_from_slice(b"/echo");
    block.extend_from_slice(&[1, 2]);
    block.extend_from_slice(b"c1");
    block.extend_from_slice(&[0, 12]);
    block.extend_from_slice(b"x-request-id");
    block.push(16);
    block.extend_from_slice(format!("{id:016x}").as_bytes());
    block
}

/// A downstream codec past the preface, and a reference codec whose header stats disable the
/// decoder memo, so it decodes every block in full.
fn consumer_receivers() -> (Http2Codec, Http2Codec) {
    let mut receiver = Http2Codec::new(TransportDirection::Downstream);
    let mut reference = Http2Codec::new(TransportDirection::Downstream);
    reference.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
    open(&mut receiver, &mut Trace::default());
    open(&mut reference, &mut Trace::default());
    (receiver, reference)
}

/// Feeds the consumer-shaped request block for `id` on `stream` to `codec` and returns the
/// decoded message.
fn consumer_ingress(codec: &mut Http2Codec, stream: u32, id: u64) -> HttpMessage {
    let mut wire = BufQueue::new(1024);
    frame::write_headers(
        &mut wire,
        queue(&consumer_request_block(id)),
        stream,
        None,
        true,
        true,
    )
    .unwrap();
    let mut capture = MessageCapture::default();
    memo_ingress(codec, &wire, &mut capture);
    assert_eq!(capture.events, ["begin", "headers", "complete"]);
    capture.messages.pop().unwrap()
}

#[test]
fn unique_blocks_do_not_copy_into_the_memo() {
    let (mut receiver, _) = consumer_receivers();
    Http2Codec::take_memo_copies();
    for id in 0..100 {
        let message = consumer_ingress(&mut receiver, 2 * id as u32 + 1, id);
        assert_eq!(
            message.headers().get_single_or_empty(b"x-request-id"),
            format!("{id:016x}").as_bytes()
        );
    }
    assert_eq!(
        Http2Codec::take_memo_copies(),
        0,
        "header blocks copied into the decoder memo for 100 unique consumer-shaped blocks"
    );
}

#[test]
fn third_identical_block_still_hits() {
    let (mut receiver, mut reference) = consumer_receivers();
    Http2Codec::take_memo_copies();
    let decodes: Vec<_> = (0..5u32)
        .map(|ordinal| {
            let before = receiver.header_loop_counts().1;
            let mut message = consumer_ingress(&mut receiver, 2 * ordinal + 1, 7);
            let mut expected = consumer_ingress(&mut reference, 2 * ordinal + 1, 7);
            let common = std::time::Instant::now();
            message.set_start_time(common);
            expected.set_start_time(common);
            assert_eq!(format!("{message:?}"), format!("{expected:?}"));
            receiver.header_loop_counts().1 - before
        })
        .collect();
    assert_eq!(
        decodes,
        [1, 1, 0, 0, 0],
        "HPACK decodes per block: the 1st and 2nd identical blocks decode, the 3rd and later hit"
    );
    assert_eq!(
        (
            Http2Codec::take_memo_copies(),
            receiver.header_template_copies()
        ),
        (1, 1),
        "(wire copies, template copies): both are made once, at the 2nd identical block"
    );
}

#[test]
fn forced_fingerprint_collision_never_false_hits() {
    let (mut receiver, mut reference) = consumer_receivers();
    receiver.force_equal_memo_fingerprints();
    Http2Codec::take_memo_copies();
    for ordinal in 0..6u32 {
        let id = u64::from(ordinal % 2);
        let before = receiver.header_loop_counts().1;
        let mut message = consumer_ingress(&mut receiver, 2 * ordinal + 1, id);
        let mut expected = consumer_ingress(&mut reference, 2 * ordinal + 1, id);
        assert_eq!(
            receiver.header_loop_counts().1,
            before + 1,
            "block {ordinal} is decoded, not replayed from the other block's template"
        );
        assert_eq!(
            message.headers().get_single_or_empty(b"x-request-id"),
            format!("{id:016x}").as_bytes()
        );
        let common = std::time::Instant::now();
        message.set_start_time(common);
        expected.set_start_time(common);
        assert_eq!(format!("{message:?}"), format!("{expected:?}"));
    }
    assert_eq!(
        Http2Codec::take_memo_copies(),
        5,
        "every block after the first matches the forced fingerprint and is copied into the memo"
    );
}

#[test]
fn unmemoizable_encode_visits_each_header_once() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut decoder = HpackCodec::new();
    let mut message = request(b"/echo");
    message.set_method(HttpMethod::Post).unwrap();
    for block in 0..4_u32 {
        // A new id per block is inserted into the table, so the epoch changes every block and no
        // memo can arm.
        message
            .headers_mut()
            .set(b"x-request-id", format!("{block:016x}").as_bytes());
        let stream = codec.create_stream();
        super::codec_impl::take_header_visits();
        let wire = bytes(&memo_wire(&mut codec, stream, &message, true));
        let visits = super::codec_impl::take_header_visits();
        let fields = decoder
            .decode(&wire[FRAME_HEADER_SIZE..])
            .unwrap()
            .headers
            .len();
        assert_eq!(
            visits, fields,
            "block {block}: each of the {fields} encoded fields is visited once"
        );
    }
}

#[test]
fn length_bound_covers_every_encoded_field() {
    let codec = Http2Codec::new(TransportDirection::Downstream);
    let mut message = response(200);
    message
        .headers_mut()
        .set(HttpHeaderCode::Connection, b"keep-alive");
    message
        .headers_mut()
        .set(b"x-padded", b" \t padded value \t ");
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentType, b"application/json");
    let mut extra = HttpHeaders::new();
    extra.set(b"x-extra", [b'e'; 100].as_slice());
    let (length_bound, bound) = codec.header_length_bounds(&message, Some(&extra));
    let (length_bound, bound) = (length_bound.unwrap(), bound.unwrap());
    assert!(
        length_bound >= bound,
        "the length-only bound {length_bound} covers the encoded fields' bound {bound}"
    );
}

#[test]
fn warmed_header_memo_capacity_failure_preserves_prefix_and_retries_without_encoding() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut reference = Http2Codec::new(TransportDirection::Upstream);
    reference.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
    let mut message = request(b"/memo");
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    for _ in 0..4 {
        let stream = codec.create_stream();
        assert_eq!(reference.create_stream(), stream);
        assert_eq!(
            bytes(&memo_wire(&mut codec, stream, &message, true)),
            bytes(&memo_wire(&mut reference, stream, &message, true))
        );
    }
    let stream = codec.create_stream();
    assert_eq!(reference.create_stream(), stream);
    let expected = memo_wire(&mut reference, stream, &message, true);
    let before = codec.compression_info().egress;
    let loops = codec.header_loop_counts();
    let mut failed = BufQueue::new(4 + expected.chain_length() - 1);
    failed.push_static(b"kept").unwrap();
    let mut events = Trace::default();
    assert!(
        codec
            .generate(
                &mut failed,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut events
            )
            .is_err()
    );
    assert_eq!(bytes(&failed), b"kept");
    assert!(events.events.is_empty());
    assert_eq!(codec.compression_info().egress, before);
    assert_eq!(codec.header_loop_counts(), loops);
    let mut retried = BufQueue::new(4 + expected.chain_length());
    retried.push_static(b"kept").unwrap();
    codec
        .generate(
            &mut retried,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
            &mut events,
        )
        .unwrap();
    assert_eq!(bytes(&retried)[4..], bytes(&expected));
    assert_eq!(
        codec.compression_info().egress,
        reference.compression_info().egress
    );
    assert_eq!(codec.header_loop_counts(), loops);
}

#[test]
fn whole_header_memo_churn_copies_templates_without_claiming_hits() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let mut receiver = Http2Codec::new(TransportDirection::Downstream);
    let mut reference = Http2Codec::new(TransportDirection::Downstream);
    reference.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
    open(&mut receiver, &mut Trace::default());
    open(&mut reference, &mut Trace::default());
    let mut message = request(b"/item?id=A");
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    for path in [
        b"/item?id=A",
        b"/item?id=A",
        b"/item?id=A",
        b"/item?id=A",
        b"/item?id=B",
        b"/item?id=B",
        b"/item?id=C",
        b"/item?id=C",
        b"/item?id=A",
        b"/item?id=A",
        b"/item?id=A",
    ] {
        message.set_url(path, false).unwrap();
        let stream = sender.create_stream();
        let wire = memo_wire(&mut sender, stream, &message, true);
        let mut actual = MessageCapture::default();
        let mut expected = MessageCapture::default();
        memo_ingress(&mut receiver, &wire, &mut actual);
        memo_ingress(&mut reference, &wire, &mut expected);
        assert_eq!(actual.events, expected.events);
        assert_eq!(actual.messages[0].headers(), expected.messages[0].headers());
        assert_eq!(actual.messages[0].request_url(), Some(path.as_slice()));
        assert_eq!(
            actual.messages[0].request_url(),
            expected.messages[0].request_url()
        );
        assert_eq!(
            actual.messages[0]
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"0"
        );
        assert_eq!(
            receiver.compression_info().ingress,
            reference.compression_info().ingress
        );
    }
    assert_eq!(sender.header_loop_counts().0, 9);
    assert_eq!(receiver.header_loop_counts().1, 9);
    assert_eq!(reference.header_loop_counts().1, 11);
    assert_eq!(receiver.header_template_copies(), 4);
}

fn memo_raw_headers(stream: u32, block: &[u8]) -> BufQueue {
    let mut wire = BufQueue::new(4096);
    frame::write_headers(&mut wire, queue(block), stream, None, true, true).unwrap();
    wire
}

fn memo_request_block(path: &[u8]) -> Vec<u8> {
    let mut block = vec![0x82, 0x87, 0x01, 11];
    block.extend_from_slice(b"example.com");
    block.extend_from_slice(path);
    block
}

#[test]
fn repeated_header_wire_is_revalidated_after_dynamic_table_mutation() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let first = memo_request_block(&[0x44, 4, b'/', b'o', b'n', b'e']);
    let stable = memo_request_block(&[0xbe]);
    for (stream, block) in [
        (1, first.as_slice()),
        (3, stable.as_slice()),
        (5, stable.as_slice()),
        (7, stable.as_slice()),
    ] {
        let mut capture = MessageCapture::default();
        memo_ingress(&mut codec, &memo_raw_headers(stream, block), &mut capture);
        assert_eq!(capture.messages[0].request_url(), Some(b"/one".as_slice()));
    }
    assert_eq!(codec.header_loop_counts().1, 3);
    let changed = memo_request_block(&[0x44, 4, b'/', b't', b'w', b'o']);
    memo_ingress(
        &mut codec,
        &memo_raw_headers(9, &changed),
        &mut MessageCapture::default(),
    );
    let mut capture = MessageCapture::default();
    memo_ingress(&mut codec, &memo_raw_headers(11, &stable), &mut capture);
    assert_eq!(capture.messages[0].request_url(), Some(b"/two".as_slice()));
    assert_eq!(codec.header_loop_counts().1, 5);
}

#[test]
fn never_indexed_and_invalid_blocks_cannot_use_validated_templates() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let sensitive = memo_request_block(&[0x14, 5, b'/', b'm', b'e', b'm', b'o']);
    for stream in [1, 3, 5, 7] {
        let mut capture = MessageCapture::default();
        memo_ingress(
            &mut codec,
            &memo_raw_headers(stream, &sensitive),
            &mut capture,
        );
        assert_eq!(capture.messages[0].request_url(), Some(b"/memo".as_slice()));
    }
    assert_eq!(codec.header_loop_counts().1, 4);
    assert_eq!(codec.header_template_copies(), 0);
    let invalid = memo_request_block(&[0x04, 0x81, 0xff]);
    let mut trace = Trace::default();
    let _ = codec.on_ingress(
        &memo_raw_headers(9, &invalid),
        &mut trace,
        &mut BufQueue::new(4096),
    );
    assert!(trace.events.iter().any(|event| matches!(
        event,
        Seen::Error(_, Some(H2ErrorCode::COMPRESSION_ERROR), _, _)
    )));
}

#[test]
fn completed_continuation_reuses_template_and_stats_installation_restores_decode() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let block = memo_request_block(&[0x04, 5, b'/', b'm', b'e', b'm', b'o']);
    for stream in [1, 3] {
        memo_ingress(
            &mut codec,
            &memo_raw_headers(stream, &block),
            &mut MessageCapture::default(),
        );
    }
    let before = codec.header_loop_counts();
    let mut prefix = BufQueue::new(4096);
    frame::write_headers(&mut prefix, queue(&block[..2]), 5, None, true, false).unwrap();
    let mut suffix = BufQueue::new(4096);
    frame::write_continuation(&mut suffix, 5, true, queue(&block[2..])).unwrap();
    let mut capture = MessageCapture::default();
    memo_ingress(&mut codec, &prefix, &mut capture);
    assert!(capture.messages.is_empty());
    memo_ingress(&mut codec, &suffix, &mut capture);
    assert_eq!(capture.messages[0].request_url(), Some(b"/memo".as_slice()));
    assert_eq!(codec.header_loop_counts(), before);
    codec.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
    memo_ingress(
        &mut codec,
        &memo_raw_headers(7, &block),
        &mut MessageCapture::default(),
    );
    assert_eq!(codec.header_loop_counts().1, before.1 + 1);
}

#[test]
fn warm_template_precedes_message_begin_mutation() {
    struct MutateBegin {
        begins: usize,
        completes: usize,
        originals: Vec<HttpMessage>,
    }
    impl HttpCodecCallback for MutateBegin {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            match event {
                CodecEvent::MessageBegin { message, .. } => {
                    assert!(!message.headers().exists(b"x-callback"));
                    let original = message.clone();
                    let before = message
                        .headers()
                        .get_single_or_empty(HttpHeaderCode::Host)
                        .as_ptr();
                    message.headers_mut().set(b"x-callback", b"this-message");
                    if self.begins >= 1 {
                        assert_ne!(
                            message
                                .headers()
                                .get_single_or_empty(HttpHeaderCode::Host)
                                .as_ptr(),
                            before
                        );
                    }
                    assert!(!original.headers().exists(b"x-callback"));
                    self.originals.push(original);
                    self.begins += 1;
                }
                CodecEvent::HeadersComplete { message, .. } => {
                    assert_eq!(
                        message.headers().get_single_or_empty(b"x-callback"),
                        b"this-message"
                    );
                    self.completes += 1;
                }
                CodecEvent::Error { .. } => panic!("valid repeated request failed"),
                _ => {}
            }
        }
    }
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    open(&mut codec, &mut Trace::default());
    let block = memo_request_block(&[0x04, 5, b'/', b'm', b'e', b'm', b'o']);
    let mut callback = MutateBegin {
        begins: 0,
        completes: 0,
        originals: Vec::new(),
    };
    for stream in [1, 3, 5, 7] {
        memo_ingress(&mut codec, &memo_raw_headers(stream, &block), &mut callback);
    }
    assert_eq!(codec.header_loop_counts().1, 2);
    assert_eq!(codec.header_template_copies(), 1);
    assert_eq!((callback.begins, callback.completes), (4, 4));
    let template = callback.originals[1]
        .headers()
        .get_single_or_empty(HttpHeaderCode::Host)
        .as_ptr();
    for original in &callback.originals[2..] {
        assert_eq!(
            original
                .headers()
                .get_single_or_empty(HttpHeaderCode::Host)
                .as_ptr(),
            template
        );
        assert!(!original.headers().exists(b"x-callback"));
    }
}

#[test]
fn informational_responses_and_trailers_use_normal_decode_after_a_warm_response() {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    let mut reference = Http2Codec::new(TransportDirection::Upstream);
    reference.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
    open(&mut codec, &mut Trace::default());
    open(&mut reference, &mut Trace::default());
    for _ in 0..3 {
        let stream = codec.create_stream();
        assert_eq!(reference.create_stream(), stream);
        let wire = memo_raw_headers(stream.get() as u32, &[0x88]);
        let mut actual = Trace::default();
        let mut expected = Trace::default();
        memo_ingress(&mut codec, &wire, &mut actual);
        memo_ingress(&mut reference, &wire, &mut expected);
        assert_eq!(actual.events, expected.events);
    }
    assert_eq!(codec.header_loop_counts().1, 2);
    let stream = codec.create_stream();
    assert_eq!(reference.create_stream(), stream);
    let informational = [0x08, 3, b'1', b'0', b'3'];
    let trailers = [0x00, 5, b'x', b'-', b'e', b'n', b'd', 1, b'1'];
    for (block, end) in [
        (informational.as_slice(), false),
        (informational.as_slice(), false),
        (informational.as_slice(), false),
        ([0x88].as_slice(), false),
        (trailers.as_slice(), true),
    ] {
        let mut wire = BufQueue::new(4096);
        frame::write_headers(
            &mut wire,
            queue(block),
            stream.get() as u32,
            None,
            end,
            true,
        )
        .unwrap();
        let mut actual = Trace::default();
        let mut expected = Trace::default();
        let before = codec.header_loop_counts().1;
        memo_ingress(&mut codec, &wire, &mut actual);
        memo_ingress(&mut reference, &wire, &mut expected);
        assert_eq!(actual.events, expected.events);
        assert_eq!(codec.header_loop_counts().1, before + 1);
        assert!(
            !actual
                .events
                .iter()
                .any(|event| matches!(event, Seen::Error(..)))
        );
    }
    assert_eq!(codec.header_template_copies(), 1);
    assert_eq!(
        codec.compression_info().ingress,
        reference.compression_info().ingress
    );
}

fn memo_backing(message: &HttpMessage) -> (*const u8, *const u8) {
    (
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentLength)
            .as_ptr(),
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Date)
            .as_ptr(),
    )
}

#[test]
fn memo_hits_share_inline_entries_and_long_values_through_both_egress_directions() {
    for downstream in [true, false] {
        let (send, receive) = if downstream {
            (TransportDirection::Upstream, TransportDirection::Downstream)
        } else {
            (TransportDirection::Downstream, TransportDirection::Upstream)
        };
        let mut sender = Http2Codec::new(send);
        let mut receiver = Http2Codec::new(receive);
        let mut forwarded = Http2Codec::new(send);
        let mut reference = Http2Codec::new(send);
        reference.set_header_codec_stats(Some(Rc::new(HeaderEncodeStats::default())));
        open(&mut receiver, &mut Trace::default());
        let mut source = if downstream {
            request(b"/shared")
        } else {
            response(200)
        };
        source
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"0");
        source
            .headers_mut()
            .set(HttpHeaderCode::Date, b"Mon, 21 Sep 2026 12:00:00 GMT");
        source.headers_mut().add(b"x-duplicate", b"first");
        source.headers_mut().add(b"x-duplicate", b"second");
        let mut retained = Vec::new();
        for ordinal in 0..6 {
            let stream = if downstream {
                sender.create_stream()
            } else {
                receiver.create_stream()
            };
            let end = ordinal % 2 == 0;
            let input = memo_wire(&mut sender, stream, &source, end);
            let before = receiver.header_loop_counts().1;
            let started = std::time::Instant::now();
            let mut capture = MessageCapture::default();
            memo_ingress(&mut receiver, &input, &mut capture);
            let mut message = capture.messages.pop().unwrap();
            assert_eq!(message.is_chunked(), !end);
            assert!(message.start_time() >= started);
            assert_eq!(
                message.headers().combine_default(b"x-duplicate").as_ref(),
                b"first, second"
            );
            let pointers = memo_backing(&message);
            if ordinal >= 3 {
                assert_eq!(receiver.header_loop_counts().1, before);
                assert_eq!(pointers, memo_backing(&retained[2]));
            }
            let original_version = message.http_version();
            message.set_http_version(2, 0);
            let _ = message.headers_mut();
            let outgoing = memo_wire(&mut forwarded, stream, &message, end);
            let expected = memo_wire(&mut reference, stream, &message, end);
            assert_eq!(bytes(&outgoing), bytes(&expected));
            assert_eq!(memo_backing(&message), pointers);
            message.set_http_version(original_version.0, original_version.1);
            retained.push(message);
        }
        assert_eq!(receiver.header_template_copies(), 1);
        let mut mutated = retained[3].clone();
        mutated
            .headers_mut()
            .set(HttpHeaderCode::Date, b"Tue, 22 Sep 2026 12:00:00 GMT");
        assert_ne!(memo_backing(&mutated).0, memo_backing(&retained[3]).0);
        assert_ne!(memo_backing(&mutated).1, memo_backing(&retained[3]).1);
        assert_eq!(
            retained[3]
                .headers()
                .get_single_or_empty(HttpHeaderCode::Date),
            b"Mon, 21 Sep 2026 12:00:00 GMT"
        );
        let stream = StreamId::new(13);
        assert_eq!(
            bytes(&memo_wire(&mut forwarded, stream, &mutated, true)),
            bytes(&memo_wire(&mut reference, stream, &mutated, true)),
        );
    }
}

#[test]
fn shared_headers_remain_valid_after_memo_invalidation_and_replacement() {
    let mut sender = Http2Codec::new(TransportDirection::Upstream);
    let mut receiver = Http2Codec::new(TransportDirection::Downstream);
    open(&mut receiver, &mut Trace::default());
    let mut source = request(b"/shared");
    source
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"0");
    source
        .headers_mut()
        .set(HttpHeaderCode::Date, b"Mon, 21 Sep 2026 12:00:00 GMT");
    let mut retained = Vec::new();
    for _ in 0..4 {
        let stream = sender.create_stream();
        let wire = memo_wire(&mut sender, stream, &source, true);
        let mut capture = MessageCapture::default();
        memo_ingress(&mut receiver, &wire, &mut capture);
        retained.push(capture.messages.pop().unwrap());
    }
    assert_eq!(memo_backing(&retained[2]), memo_backing(&retained[3]));
    let original = memo_backing(&retained[3]);
    source
        .headers_mut()
        .set(HttpHeaderCode::Date, b"Tue, 22 Sep 2026 12:00:00 GMT");
    let before = receiver.header_loop_counts().1;
    let mut replacements = Vec::new();
    for _ in 0..4 {
        let stream = sender.create_stream();
        let wire = memo_wire(&mut sender, stream, &source, true);
        let mut capture = MessageCapture::default();
        memo_ingress(&mut receiver, &wire, &mut capture);
        replacements.push(capture.messages.pop().unwrap());
    }
    assert_eq!(receiver.header_loop_counts().1, before + 3);
    assert_eq!(receiver.header_template_copies(), 2);
    assert_eq!(
        memo_backing(&replacements[2]),
        memo_backing(&replacements[3])
    );
    assert_ne!(memo_backing(&replacements[3]).0, original.0);
    assert_ne!(memo_backing(&replacements[3]).1, original.1);
    assert_eq!(memo_backing(&retained[3]), original);
    assert_eq!(
        retained[3]
            .headers()
            .get_single_or_empty(HttpHeaderCode::Date),
        b"Mon, 21 Sep 2026 12:00:00 GMT"
    );
    assert_eq!(
        replacements[3]
            .headers()
            .get_single_or_empty(HttpHeaderCode::Date),
        b"Tue, 22 Sep 2026 12:00:00 GMT"
    );
}

/// The DATA generation `Http2Codec::generate` performed before payload
/// prefixes moved straight into the staging queue: one split-off chunk queue
/// per non-final frame and a vector of frame lengths.
fn reference_generate_data(
    output: &mut BufQueue,
    mut body: BufQueue,
    stream: u32,
    padding: Option<u8>,
    end_stream: bool,
    max_frame: usize,
    compact_data: bool,
) -> Result<(roxygen_codec::GeneratedFrame, Vec<usize>), frame::FrameError> {
    let direct = body.chain_length() <= max_frame
        && body
            .chain_length()
            .checked_add(FRAME_HEADER_SIZE + padding.map_or(0, |amount| usize::from(amount) + 1))
            .is_some_and(|length| length <= output.remaining_capacity());
    let mut staged = BufQueue::new(output.remaining_capacity());
    let destination = if direct { &mut *output } else { &mut staged };
    let mut bytes_written = 0;
    let mut first_payload_offset = None;
    let mut generated_lengths = Vec::new();
    while body.chain_length() > max_frame {
        let chunk = body.split_at_most(max_frame);
        let written = frame::write_data(destination, chunk, stream, padding, false, false)?;
        first_payload_offset
            .get_or_insert((FRAME_HEADER_SIZE + usize::from(padding.is_some())) as u64);
        generated_lengths.push(written);
        bytes_written += written;
    }
    let written = if compact_data && padding.is_none() {
        frame::write_compact_data(destination, body, stream, end_stream)
    } else {
        frame::write_data(destination, body, stream, padding, end_stream, false)
    }?;
    first_payload_offset.get_or_insert((FRAME_HEADER_SIZE + usize::from(padding.is_some())) as u64);
    bytes_written += written;
    if !direct {
        output.append(&mut staged)?;
    }
    generated_lengths.push(written);
    Ok((
        roxygen_codec::GeneratedFrame {
            bytes_written,
            header_size: None,
            payload_offset: first_payload_offset,
        },
        generated_lengths,
    ))
}

#[test]
fn multi_frame_data_matches_the_reference_frame_loop() {
    use std::sync::Arc;
    use std::sync::Mutex;

    struct LoggedOwner {
        bytes: Vec<u8>,
        id: usize,
        log: Arc<Mutex<Vec<usize>>>,
    }

    impl AsRef<[u8]> for LoggedOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for LoggedOwner {
        fn drop(&mut self) {
            self.log.lock().unwrap().push(self.id);
        }
    }

    /// An output segment as a view of an original prefix or body segment
    /// (index, offset, length), or as its bytes when it is new metadata.
    #[derive(Debug, Eq, PartialEq)]
    enum Piece {
        Original(usize, usize, usize),
        Other(Vec<u8>),
    }

    struct Run {
        output: BufQueue,
        body: BufQueue,
        /// (start address, length, index), sorted by address.
        originals: Vec<(usize, usize, usize)>,
        log: Arc<Mutex<Vec<usize>>>,
    }

    static PATTERN: std::sync::LazyLock<Vec<u8>> =
        std::sync::LazyLock::new(|| (0..400_000).map(|i| (i % 251) as u8).collect());

    // (kind, length) pieces: owner-backed misaligned segments, unique vectors
    // of exactly one frame with spare capacity (the next header may be copied
    // into their tail), many small static and copied segments, one inline
    // segment, and one segment spanning several frames.
    let layout = |name: &str, length: usize, max_frame: usize| -> Vec<(u8, usize)> {
        let sizes: Vec<(u8, usize)> = match name {
            "owners" => vec![(b'o', 7_000), (b'o', 13_000), (b'o', 1), (b'o', 40_000)],
            "frames" => vec![(b'v', max_frame); length / max_frame],
            "small" => (0..length / 997)
                .map(|index| (if index % 2 == 0 { b's' } else { b'c' }, 997))
                .collect(),
            "inline" => Vec::new(),
            _ => vec![(b'o', 3 * max_frame + 5)],
        };
        let mut pieces = Vec::new();
        let mut rest = length;
        for (kind, size) in sizes {
            let size = size.min(rest);
            if size != 0 {
                pieces.push((kind, size));
                rest -= size;
            }
        }
        if rest != 0 {
            pieces.push((if name == "owners" { b'o' } else { b'v' }, rest));
        }
        pieces
    };

    let build = |pieces: &[(u8, usize)], prefix: &str, limit: usize| -> Run {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut originals = Vec::new();
        let mut output = BufQueue::new(limit);
        match prefix {
            "unique" => {
                let mut tail = Vec::with_capacity(64);
                tail.extend_from_slice(b"prefix");
                originals.push((tail.as_ptr() as usize, 6, usize::MAX));
                output.push_vec(tail).unwrap();
            }
            "owner" => {
                let owned = Bytes::from_owner(LoggedOwner {
                    bytes: b"prefix".to_vec(),
                    id: usize::MAX,
                    log: log.clone(),
                });
                originals.push((owned.as_ptr() as usize, 6, usize::MAX));
                output.push_bytes(owned).unwrap();
            }
            _ => {}
        }
        let mut body = BufQueue::new(usize::MAX / 2);
        let mut offset = 0;
        for (index, &(kind, size)) in pieces.iter().enumerate() {
            let content: &'static [u8] = &PATTERN[offset..offset + size];
            offset += size;
            let segment = match kind {
                b'o' => Bytes::from_owner(LoggedOwner {
                    bytes: content.to_vec(),
                    id: index,
                    log: log.clone(),
                }),
                b'v' => {
                    let mut vector = Vec::with_capacity(size + 64);
                    vector.extend_from_slice(content);
                    Bytes::from(vector)
                }
                b's' => Bytes::from_static(content),
                _ => Bytes::copy_from_slice(content),
            };
            originals.push((segment.as_ptr() as usize, size, index));
            body.push_bytes(segment).unwrap();
        }
        originals.sort_unstable();
        Run {
            output,
            body,
            originals,
            log,
        }
    };

    let canonical = |run: &Run| -> Vec<Piece> {
        run.output
            .segments()
            .map(|segment| {
                let pointer = segment.as_ptr() as usize;
                let at = run
                    .originals
                    .partition_point(|(start, _, _)| *start <= pointer);
                match at.checked_sub(1).map(|at| run.originals[at]) {
                    Some((start, length, index)) if pointer < start + length => {
                        Piece::Original(index, pointer - start, segment.len())
                    }
                    _ => Piece::Other(segment.to_vec()),
                }
            })
            .collect()
    };

    for max_frame in [MAX_FRAME_PAYLOAD_LENGTH_MIN, 20_000] {
        let frame_size = max_frame as usize;
        let lengths = [
            frame_size - 1,
            frame_size,
            2 * frame_size - 1,
            2 * frame_size,
            2 * frame_size + 1,
            3 * frame_size,
            3 * frame_size + 1,
            17 * frame_size + 1,
        ];
        let names: &[&str] = if max_frame == MAX_FRAME_PAYLOAD_LENGTH_MIN {
            &["owners", "frames", "small", "inline", "spanning"]
        } else {
            &["owners", "frames"]
        };
        for &name in names {
            for length in lengths {
                let pieces = layout(name, length, frame_size);
                for (padding, compact) in [
                    (None, false),
                    (None, true),
                    (Some(0), false),
                    (Some(3), false),
                ] {
                    let padding_length = padding.map_or(0, |amount| usize::from(amount) + 1);
                    let frames = length.div_ceil(frame_size);
                    let total = length + frames * (FRAME_HEADER_SIZE + padding_length);
                    let full_frame = FRAME_HEADER_SIZE + frame_size + padding_length;
                    for prefix in ["empty", "unique", "owner"] {
                        let prefix_length = if prefix == "empty" { 0 } else { 6 };
                        for limit in [
                            prefix_length + total,
                            prefix_length + total - 1,
                            prefix_length + full_frame + 5,
                            prefix_length + 3,
                        ] {
                            let context = format!(
                                "max_frame {max_frame} {name} length {length} padding {padding:?} \
                                 compact {compact} prefix {prefix} limit {limit}"
                            );
                            let mut codec = Http2Codec::new(TransportDirection::Upstream);
                            codec.set_compact_data(compact);
                            if max_frame != MAX_FRAME_PAYLOAD_LENGTH_MIN {
                                let mut settings = BufQueue::new(1024);
                                frame::write_settings(
                                    &mut settings,
                                    &[Setting {
                                        identifier: SettingsId::MaxFrameSize.as_u16(),
                                        value: max_frame,
                                    }],
                                )
                                .unwrap();
                                let wire = bytes(&settings);
                                assert_eq!(
                                    ingress_one(&mut codec, &wire, &mut Trace::default()).consumed,
                                    wire.len()
                                );
                            }
                            let stream = codec.create_stream();
                            let succeeds = limit >= prefix_length + total;

                            let mut expected = build(&pieces, prefix, limit);
                            let expected_result = reference_generate_data(
                                &mut expected.output,
                                std::mem::replace(&mut expected.body, BufQueue::new(0)),
                                stream.get() as u32,
                                padding,
                                true,
                                frame_size,
                                compact,
                            );
                            let expected_drops = expected.log.lock().unwrap().clone();

                            let mut actual = build(&pieces, prefix, limit);
                            let bound = frame::data_frames_segment_bound(
                                &actual.body,
                                frame_size,
                                padding,
                                actual.output.remaining_capacity(),
                            );
                            assert_eq!(bound != 0, succeeds, "{context}");
                            let mut trace = Trace::default();
                            let actual_result = codec.generate(
                                &mut actual.output,
                                EgressFrame::Body {
                                    stream,
                                    body: std::mem::replace(&mut actual.body, BufQueue::new(0)),
                                    padding,
                                    end_stream: true,
                                },
                                &mut trace,
                            );
                            assert_eq!(
                                *actual.log.lock().unwrap(),
                                expected_drops,
                                "{context}: owner drops during generate"
                            );
                            assert_eq!(canonical(&actual), canonical(&expected), "{context}");
                            assert_eq!(
                                actual.output.backing_capacity_upper_bound(),
                                expected.output.backing_capacity_upper_bound(),
                                "{context}"
                            );
                            assert_eq!(
                                actual.output.chain_length(),
                                expected.output.chain_length(),
                                "{context}"
                            );
                            match (actual_result, expected_result) {
                                (Ok(actual_frame), Ok((expected_frame, lengths))) => {
                                    assert!(succeeds, "{context}");
                                    assert!(
                                        actual.output.segment_count()
                                            - usize::from(prefix_length != 0)
                                            <= bound,
                                        "{context}: staged segments exceed the bound"
                                    );
                                    assert_eq!(actual_frame, expected_frame, "{context}");
                                    assert_eq!(expected_frame.bytes_written, total, "{context}");
                                    let callbacks = lengths
                                        .iter()
                                        .map(|length| {
                                            Seen::Generated(
                                                stream.get(),
                                                FrameType::DATA.as_u8(),
                                                *length as u64,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    assert_eq!(trace.events, callbacks, "{context}");
                                }
                                (Err(actual_error), Err(expected_error)) => {
                                    assert!(!succeeds, "{context}");
                                    assert_eq!(
                                        actual_error.message(),
                                        expected_error.to_string().as_bytes(),
                                        "{context}"
                                    );
                                    assert!(trace.events.is_empty(), "{context}");
                                }
                                (actual_result, expected_result) => {
                                    panic!("{context}: {actual_result:?} vs {expected_result:?}")
                                }
                            }
                            drop(expected.output);
                            drop(actual.output);
                            assert_eq!(
                                *actual.log.lock().unwrap(),
                                *expected.log.lock().unwrap(),
                                "{context}: owner drops"
                            );

                            if !succeeds || prefix != "owner" {
                                continue;
                            }
                            // Every frame is committed before any callback runs.
                            let mut panicked = build(&pieces, prefix, limit);
                            let panic = catch_unwind(AssertUnwindSafe(|| {
                                let _ = codec.generate(
                                    &mut panicked.output,
                                    EgressFrame::Body {
                                        stream,
                                        body: std::mem::replace(
                                            &mut panicked.body,
                                            BufQueue::new(0),
                                        ),
                                        padding,
                                        end_stream: true,
                                    },
                                    &mut PanicOnGenerated,
                                );
                            }))
                            .unwrap_err();
                            assert_eq!(
                                panic.downcast_ref::<&str>(),
                                Some(&"generated callback panic"),
                            );
                            let mut reference = build(&pieces, prefix, limit);
                            reference_generate_data(
                                &mut reference.output,
                                std::mem::replace(&mut reference.body, BufQueue::new(0)),
                                stream.get() as u32,
                                padding,
                                true,
                                frame_size,
                                compact,
                            )
                            .unwrap();
                            assert_eq!(
                                canonical(&panicked),
                                canonical(&reference),
                                "{context}: panic"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Records events as [`Trace`] does, answers PINGs and connection errors as a
/// session would, and may opt out of frame-header events.
struct FrameHeaderRecorder {
    observes_frame_headers: bool,
    trace: Trace,
}

impl HttpCodecCallback for FrameHeaderRecorder {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.trace.on_event(event);
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let reply = match &event {
            CodecEvent::PingRequest { data } => Some(OwnedEgressFrame::PingReply { data: *data }),
            CodecEvent::Error { stream, error, .. } if stream.is_unspecified() => {
                Some(OwnedEgressFrame::Goaway {
                    last_stream: StreamId::new(0),
                    code: error
                        .codec_status_code()
                        .expect("a connection error carries its code"),
                    debug_data: None,
                })
            }
            _ => None,
        };
        self.trace.on_event(event);
        reply.map_or_else(CodecCallbackActions::new, |frame| {
            vec![CodecCallbackAction::Generate(frame)].into()
        })
    }

    fn observes_frame_headers(&self) -> bool {
        self.observes_frame_headers
    }
}

/// Feeds `input` to a fresh downstream codec in one call and returns the
/// events seen, the consumed byte count and the callback egress bytes.
fn frame_header_run(input: &[u8], observes_frame_headers: bool) -> (Vec<Seen>, usize, Vec<u8>) {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut recorder = FrameHeaderRecorder {
        observes_frame_headers,
        trace: Trace::default(),
    };
    let mut egress = BufQueue::new(64 * 1024);
    let result = codec.on_ingress(&queue(input), &mut recorder, &mut egress);
    assert!(result.egress_results.iter().all(Result::is_ok));
    (recorder.trace.events, result.consumed, bytes(&egress))
}

fn without_frame_headers(events: &[Seen]) -> Vec<&Seen> {
    events
        .iter()
        .filter(|seen| !matches!(seen, Seen::Frame(..)))
        .collect()
}

#[test]
fn opted_out_callback_sees_no_frame_headers() {
    const PING: u64 = 0x0102_0304_0506_0708;
    let mut client = Http2Codec::new(TransportDirection::Upstream);
    let mut trace = Trace::default();
    let mut output = BufQueue::new(64 * 1024);
    output.push_copy(CONNECTION_PREFACE).unwrap();
    output.push_copy(&empty_settings()).unwrap();
    let stream = client.create_stream();
    let frames = [
        EgressFrame::Headers {
            stream,
            message: &request(b"/opt-out"),
            end_stream: false,
            extra_headers: None,
        },
        EgressFrame::Body {
            stream,
            body: queue(b"body"),
            padding: None,
            end_stream: true,
        },
        EgressFrame::WindowUpdate {
            stream,
            amount: 1000,
        },
        EgressFrame::PingRequest { data: Some(PING) },
    ];
    for frame in frames {
        client.generate(&mut output, frame, &mut trace).unwrap();
    }
    let input = bytes(&output);

    let (observed, observed_consumed, observed_egress) = frame_header_run(&input, true);
    let (opted_out, opted_out_consumed, opted_out_egress) = frame_header_run(&input, false);

    assert_eq!(
        observed.len() - without_frame_headers(&observed).len(),
        5,
        "the default callback sees the SETTINGS, HEADERS, DATA, WINDOW_UPDATE and PING headers: {observed:?}"
    );
    assert_eq!(
        opted_out.iter().collect::<Vec<_>>(),
        without_frame_headers(&observed),
        "the opted-out callback sees every other event, in order"
    );
    assert!(
        opted_out.contains(&Seen::Complete(1)),
        "END_STREAM on DATA still completes the message: {opted_out:?}"
    );
    assert_eq!(observed_consumed, input.len());
    assert_eq!(opted_out_consumed, observed_consumed);
    let ping_ack = [&[0, 0, 8, 6, 1, 0, 0, 0, 0][..], &PING.to_le_bytes()].concat();
    assert_eq!(observed_egress, ping_ack);
    assert_eq!(opted_out_egress, observed_egress);
}

#[test]
fn first_frame_check_survives_the_opt_out() {
    let mut client = Http2Codec::new(TransportDirection::Upstream);
    let mut trace = Trace::default();
    let mut output = BufQueue::new(64 * 1024);
    output.push_copy(CONNECTION_PREFACE).unwrap();
    let stream = client.create_stream();
    client
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &request(b"/first"),
                end_stream: true,
                extra_headers: None,
            },
            &mut trace,
        )
        .unwrap();
    let input = bytes(&output);

    let (observed, observed_consumed, observed_egress) = frame_header_run(&input, true);
    let (opted_out, opted_out_consumed, opted_out_egress) = frame_header_run(&input, false);

    assert!(
        matches!(observed.first(), Some(Seen::Frame(1, frame_type, _)) if *frame_type == FrameType::HEADERS.as_u8()),
        "the default callback sees the HEADERS frame header first: {observed:?}"
    );
    let expected = without_frame_headers(&observed);
    assert!(
        matches!(
            expected.as_slice(),
            [
                Seen::Error(0, Some(H2ErrorCode::PROTOCOL_ERROR), None, false),
                Seen::Generated(0, 7, 17),
            ]
        ),
        "HEADERS first is a PROTOCOL_ERROR connection error answered by a GOAWAY: {observed:?}"
    );
    assert_eq!(opted_out.iter().collect::<Vec<_>>(), expected);
    assert_eq!(opted_out_consumed, observed_consumed);
    let goaway = [
        &[0, 0, 8, 7, 0, 0, 0, 0, 0][..],
        &0_u32.to_be_bytes(),
        &H2ErrorCode::PROTOCOL_ERROR.as_u32().to_be_bytes(),
    ]
    .concat();
    assert_eq!(observed_egress, goaway);
    assert_eq!(opted_out_egress, observed_egress);
}

/// Value length of `x-golden-pad` for the HEADERS framing golden's `ordinal`th block. Every value is made of bytes
/// whose Huffman code is 8 bits long, so each block grows by exactly one byte per value byte, and the sweeps cross
/// the compact tail limit, the maximum frame size and the CONTINUATION split.
fn golden_pad_length(ordinal: usize) -> usize {
    let step = ordinal / 10;
    match ordinal % 10 {
        7 => 1_990 + step,
        8 => 16_330 + step,
        9 if step % 10 == 9 => 40_000,
        9 => 16_384 + 37 * step,
        _ => ordinal % 61,
    }
}

/// Generates 500 deterministic HEADERS blocks (and 10 push promises from the downstream codec), each followed
/// by 128 B of DATA, alternating a downstream and an upstream codec, and returns the wire bytes, the codec events
/// and the summed `bytes_written`. Each codec's output is flushed after every second block of that codec, so blocks land both
/// in empty outputs and behind earlier frames.
fn header_frames_transcript() -> (Vec<u8>, Vec<Seen>, usize) {
    let mut codecs = [
        Http2Codec::new(TransportDirection::Downstream),
        Http2Codec::new(TransportDirection::Upstream),
    ];
    for codec in &mut codecs {
        codec.set_compact_data(true);
        codec.set_add_date_header_to_response(false);
    }
    let mut outputs = [BufQueue::new(256 * 1024), BufQueue::new(256 * 1024)];
    let mut transcript = Vec::new();
    let mut trace = Trace::default();
    let mut written = 0;
    for ordinal in 0..500 {
        let side = ordinal % 2;
        let stream = StreamId::new(2 * ordinal as u64 + 1);
        let mut message = if side == 0 {
            response(200)
        } else {
            request(format!("/golden/{ordinal}").as_bytes())
        };
        message
            .headers_mut()
            .add(b"x-golden-ordinal", ordinal.to_string().as_bytes());
        let pad: Vec<u8> = format!("{ordinal};")
            .bytes()
            .chain(b"X&*,;Z".iter().copied().cycle())
            .take(golden_pad_length(ordinal))
            .collect();
        message.headers_mut().add(b"x-golden-pad", &pad);
        let codec = &mut codecs[side];
        let output = &mut outputs[side];
        if side == 0 && ordinal % 25 == 4 {
            let promised = request(format!("/pushed/{ordinal}").as_bytes());
            written += codec
                .generate(
                    output,
                    EgressFrame::PushPromise {
                        stream: StreamId::new(2 * ordinal as u64 + 2),
                        associated_stream: stream,
                        message: &promised,
                        end_stream: false,
                    },
                    &mut trace,
                )
                .unwrap()
                .bytes_written;
        }
        written += codec
            .generate(
                output,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut trace,
            )
            .unwrap()
            .bytes_written;
        written += codec
            .generate(
                output,
                EgressFrame::Body {
                    stream,
                    body: queue(&[b'd'; 128]),
                    padding: None,
                    end_stream: true,
                },
                &mut trace,
            )
            .unwrap()
            .bytes_written;
        if ordinal % 4 >= 2 {
            transcript.extend(bytes(&output.take()));
        }
    }
    for output in &mut outputs {
        transcript.extend(bytes(&output.take()));
    }
    (transcript, trace.events, written)
}

/// `(frame type, flags, payload length)` of every frame in `wire`.
fn frame_shapes(wire: &[u8]) -> Vec<(u8, u8, usize)> {
    let mut shapes = Vec::new();
    let mut rest = wire;
    while !rest.is_empty() {
        let length = usize::from(rest[0]) << 16 | usize::from(rest[1]) << 8 | usize::from(rest[2]);
        shapes.push((rest[3], rest[4], length));
        rest = &rest[FRAME_HEADER_SIZE + length..];
    }
    shapes
}

#[test]
fn header_frames_transcript_is_unchanged() {
    let (wire, events, written) = header_frames_transcript();
    assert_eq!(written, wire.len());
    let events = format!("{events:?}");
    assert_eq!(
        (fnv1a64(&wire), wire.len(), fnv1a64(events.as_bytes())),
        (0x1f55_d3cc_2269_4bbf, 1_988_138, 0x365c_9c94_c3d0_0445),
        "the HEADERS framing golden, recorded at 0e711498a9a4"
    );
    let shapes = frame_shapes(&wire);
    let headers = FrameType::HEADERS.as_u8();
    let continuation = FrameType::CONTINUATION.as_u8();
    assert!(
        shapes.contains(&(
            headers,
            Flags::END_HEADERS,
            MAX_FRAME_PAYLOAD_LENGTH_MIN as usize
        )),
        "a single HEADERS frame fills the maximum frame size"
    );
    assert!(
        shapes.contains(&(
            headers,
            Flags::END_HEADERS,
            COMPACT_WRITE_LIMIT - FRAME_HEADER_SIZE
        )),
        "a single HEADERS frame fills the compact tail limit"
    );
    assert!(
        shapes
            .iter()
            .any(|&(kind, flags, _)| kind == continuation && flags == Flags::END_HEADERS),
        "some blocks need CONTINUATION frames"
    );
    assert!(
        shapes
            .iter()
            .any(|&(kind, _, _)| kind == FrameType::PUSH_PROMISE.as_u8()),
        "the transcript includes push promises"
    );
}

/// A response HEADERS block followed by 128 B of DATA, as a consumer's downstream session writes
/// them.
fn bench_response() -> HttpMessage {
    let mut message = response(200);
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"128");
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentType, b"application/octet-stream");
    message
        .headers_mut()
        .add(b"x-request-id", b"0123456789abcdef");
    message
}

/// An output whose only segment is a unique small tail with 1 KiB of spare capacity.
fn output_with_room() -> BufQueue {
    let mut tail = Vec::with_capacity(1024);
    tail.extend_from_slice(b"earlier");
    let mut output = BufQueue::new(64 * 1024);
    output.push_vec(tail).unwrap();
    output
}

#[test]
fn single_frame_headers_allocate_once() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    codec.set_compact_data(true);
    codec.set_add_date_header_to_response(false);
    let message = bench_response();
    let generate = |codec: &mut Http2Codec, output: &mut BufQueue, stream: u64| {
        codec
            .generate(
                output,
                EgressFrame::Headers {
                    stream: StreamId::new(stream),
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .unwrap();
    };
    generate(&mut codec, &mut BufQueue::new(64 * 1024), 1);
    HpackEncodeBuffer::take_block_allocations();
    assert_eq!(
        Http2Codec::take_header_frame_allocations(),
        (0, 1),
        "an empty output has no tail, so the first block takes the exactly sized fallback box"
    );
    // The second block is a plain encode that arms the memo; the third and fourth hit it.
    let segments: Vec<_> = [3, 5, 7]
        .into_iter()
        .map(|stream| {
            let mut output = output_with_room();
            generate(&mut codec, &mut output, stream);
            output.segment_count()
        })
        .collect();
    assert_eq!(
        (
            HpackEncodeBuffer::take_block_allocations(),
            Http2Codec::take_header_frame_allocations(),
            segments,
        ),
        (0, (0, 0), vec![1, 1, 1]),
        "(block buffer allocations, (temporary queue pushes, fallback boxes), output segments) for three HEADERS"
    );
}

#[test]
fn strategy_headers_reuse_block_storage() {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    codec.set_compact_data(true);
    codec.set_add_date_header_to_response(false);
    codec.set_header_indexing_strategy(Rc::new(super::hpack::NoPathIndexingStrategy));
    let message = bench_response();
    let mut generate = |output: &mut BufQueue, stream: u64| {
        codec
            .generate(
                output,
                EgressFrame::Headers {
                    stream: StreamId::new(stream),
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .unwrap();
    };
    generate(&mut BufQueue::new(64 * 1024), 1);
    HpackEncodeBuffer::take_block_allocations();
    for stream in [3, 5, 7] {
        generate(&mut output_with_room(), stream);
    }
    assert_eq!(
        HpackEncodeBuffer::take_block_allocations(),
        0,
        "block buffer allocations for three HEADERS under a custom indexing strategy"
    );
}

/// A 200 response whose `x-golden-pad` value is `length` bytes, each with an 8-bit Huffman code, so the HEADERS
/// payload grows by exactly one byte per value byte.
fn padded_response(length: usize) -> HttpMessage {
    let mut message = response(200);
    let pad: Vec<u8> = b"X&*,;Z".iter().copied().cycle().take(length).collect();
    message.headers_mut().add(b"x-golden-pad", &pad);
    message
}

fn padded_headers_codec() -> Http2Codec {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    codec.set_compact_data(true);
    codec.set_add_date_header_to_response(false);
    codec
}

/// Writes HEADERS for `message` into an empty output and returns the frame shapes.
fn padded_headers_shapes(
    codec: &mut Http2Codec,
    message: &HttpMessage,
    stream: u64,
) -> Vec<(u8, u8, usize)> {
    let mut output = BufQueue::new(256 * 1024);
    codec
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: StreamId::new(stream),
                message,
                end_stream: false,
                extra_headers: None,
            },
            &mut Trace::default(),
        )
        .unwrap();
    frame_shapes(&bytes(&output))
}

#[test]
fn single_frame_headers_above_the_compact_limit_reuse_block_storage() {
    let max_frame = MAX_FRAME_PAYLOAD_LENGTH_MIN as usize;
    let compact_payload = COMPACT_WRITE_LIMIT - FRAME_HEADER_SIZE;
    let probe = 16_000;
    let probed = padded_headers_shapes(&mut padded_headers_codec(), &padded_response(probe), 1);
    let overhead = probed[0].2 - probe;
    let mut codec = padded_headers_codec();
    let headers = FrameType::HEADERS.as_u8();

    // The block that fills the maximum frame size comes first and sizes the storage.
    assert_eq!(
        padded_headers_shapes(&mut codec, &padded_response(max_frame - overhead), 1),
        vec![(headers, Flags::END_HEADERS, max_frame)]
    );
    HpackEncodeBuffer::take_block_allocations();
    Http2Codec::take_header_frame_allocations();
    let payloads: Vec<usize> = [compact_payload + 1, 4_000, 8_000, 12_000, 16_000, 2_100]
        .into_iter()
        .zip((3..).step_by(2))
        .map(|(payload, stream)| {
            let shapes =
                padded_headers_shapes(&mut codec, &padded_response(payload - overhead), stream);
            assert_eq!(shapes.len(), 1, "one frame for a {payload} B target");
            assert_eq!((shapes[0].0, shapes[0].1), (headers, Flags::END_HEADERS));
            shapes[0].2
        })
        .collect();
    assert_eq!(payloads[0], compact_payload + 1);
    assert!(
        payloads
            .iter()
            .all(|&payload| payload > compact_payload && payload <= max_frame),
        "every block is a single frame above the compact tail limit: {payloads:?}"
    );
    assert_eq!(
        (
            HpackEncodeBuffer::take_block_allocations(),
            Http2Codec::take_header_frame_allocations(),
        ),
        (0, (0, 6)),
        "(block buffer allocations, (temporary queue pushes, fallback boxes)) for six single-frame HEADERS above \
         the compact tail limit"
    );

    // A block that needs CONTINUATION gives its storage to the output, so the codec keeps nothing larger than a
    // single frame and the next block allocates again.
    let shapes = padded_headers_shapes(&mut codec, &padded_response(40_000), 15);
    assert_eq!(
        shapes.last().map(|&(kind, flags, _)| (kind, flags)),
        Some((FrameType::CONTINUATION.as_u8(), Flags::END_HEADERS))
    );
    HpackEncodeBuffer::take_block_allocations();
    padded_headers_shapes(&mut codec, &padded_response(8_000), 17);
    assert!(
        HpackEncodeBuffer::take_block_allocations() > 0,
        "the block after a CONTINUATION block starts from new storage"
    );
}

#[test]
fn headers_then_small_data_share_one_tail() {
    let message = bench_response();
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    codec.set_compact_data(true);
    codec.set_add_date_header_to_response(false);
    let mut write = |output: &mut BufQueue, stream: u64| {
        let stream = StreamId::new(stream);
        let mut trace = Trace::default();
        let headers = codec
            .generate(
                output,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut trace,
            )
            .unwrap()
            .bytes_written;
        let after_headers = (
            output.segment_count(),
            output.backing_capacity_upper_bound(),
        );
        codec
            .generate(
                output,
                EgressFrame::Body {
                    stream,
                    body: queue(&[b'd'; 128]),
                    padding: None,
                    end_stream: true,
                },
                &mut trace,
            )
            .unwrap();
        (headers, after_headers)
    };

    // A block written at the start of a flush is an exactly sized tail that the DATA then extends.
    let mut output = BufQueue::new(64 * 1024);
    let (headers, after_headers) = write(&mut output, 1);
    assert_eq!(
        (
            after_headers,
            output.segment_count(),
            output.backing_capacity_upper_bound(),
        ),
        (
            (1, Some(headers)),
            1,
            Some(headers + FRAME_HEADER_SIZE + 128),
        ),
        "(after HEADERS (segments, backing), segments, backing) from an empty output"
    );

    // A tail with room takes the block and the DATA without growing.
    let mut output = output_with_room();
    let before = output.backing_capacity_upper_bound();
    let (headers, after_headers) = write(&mut output, 3);
    assert_eq!(
        (
            after_headers,
            output.segment_count(),
            output.backing_capacity_upper_bound(),
            output.chain_length(),
        ),
        (
            (1, before),
            1,
            before,
            b"earlier".len() + headers + FRAME_HEADER_SIZE + 128,
        ),
        "(after HEADERS (segments, backing), segments, backing, length) behind a tail with room"
    );
}

/// A deterministic 64-bit mix for consumer-shaped fixture values.
fn consumer_mix(seed: u64) -> u64 {
    let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// A consumer-shaped request: four pseudo-headers and 17 regular headers, with ids, routing keys
/// and counters that vary with `sequence`.
pub(crate) fn consumer_request_fields(sequence: u64) -> Vec<HpackHeader> {
    let mix = |salt: u64| consumer_mix(sequence.wrapping_mul(16).wrapping_add(salt));
    let request_id = format!(
        "{:016x}{:016x}-{:016x}{:016x}",
        mix(1),
        mix(2),
        mix(3),
        mix(4)
    );
    let routing_key = format!(
        "tenant-{:04}/shard-{:016x}/partition-{:016x}{:016x}",
        mix(5) % 10_000,
        mix(6),
        mix(7),
        mix(8)
    );
    vec![
        HpackHeader::new(b":method", b"POST"),
        HpackHeader::new(b":scheme", b"https"),
        HpackHeader::new(b":path", b"/service.Inference/Generate"),
        HpackHeader::new(b":authority", b"api.example.com"),
        HpackHeader::new(b"user-agent", b"consumer-client/4.2"),
        HpackHeader::new(b"accept", b"application/octet-stream"),
        HpackHeader::new(b"accept-encoding", b"gzip, deflate"),
        HpackHeader::new(b"content-type", b"application/octet-stream"),
        HpackHeader::new(b"x-request-id", &request_id),
        HpackHeader::new(b"x-routing-key", &routing_key),
        HpackHeader::new(b"x-routing-key-copy", &routing_key),
        HpackHeader::new(b"x-token-count", (mix(9) % 8192).to_string()),
        HpackHeader::new(b"x-prefix-hash", format!("{:016x}", mix(10))),
        HpackHeader::new(b"x-timeout-ms", b"2500"),
        HpackHeader::new(b"x-pad-req", [b'r'; 48]),
        HpackHeader::new(b"cache-control", b"no-cache"),
        HpackHeader::new(b"accept-language", b"en-US"),
        HpackHeader::new(b"pragma", b"no-cache"),
        HpackHeader::new(b"origin", b"https://client.example.com"),
        HpackHeader::new(b"x-forwarded-for", format!("192.0.2.{}", mix(11) % 256)),
        HpackHeader::new(b"referer", b"https://client.example.com/session"),
    ]
}

/// A consumer-shaped response: `:status` and 17 regular headers, with a service time and echoed
/// ids that vary with `sequence`.
pub(crate) fn consumer_response_fields(sequence: u64) -> Vec<HpackHeader> {
    let mix = |salt: u64| consumer_mix(sequence.wrapping_mul(16).wrapping_add(salt) ^ 0x5a5a);
    vec![
        HpackHeader::new(b":status", b"200"),
        HpackHeader::new(b"content-type", b"application/octet-stream"),
        HpackHeader::new(b"date", b"Mon, 02 Nov 2026 12:00:00 GMT"),
        HpackHeader::new(b"x-pad-resp", [b'p'; 107]),
        HpackHeader::new(b"x-upstream-time", (100 + mix(1) % 900).to_string()),
        HpackHeader::new(b"server", b"consumer-service"),
        HpackHeader::new(b"cache-control", b"private, max-age=0"),
        HpackHeader::new(b"vary", b"accept-encoding"),
        HpackHeader::new(b"x-request-id", format!("{:016x}{:016x}", mix(2), mix(3))),
        HpackHeader::new(b"x-routing-key", format!("tenant-{:04}", mix(4) % 10_000)),
        HpackHeader::new(b"x-token-count", (mix(5) % 8192).to_string()),
        HpackHeader::new(b"x-prefix-hash", format!("{:016x}", mix(6))),
        HpackHeader::new(b"content-language", b"en"),
        HpackHeader::new(b"expires", b"0"),
        HpackHeader::new(b"last-modified", b"Mon, 02 Nov 2026 11:59:59 GMT"),
        HpackHeader::new(b"etag", format!("\"{:08x}\"", mix(7) as u32)),
        HpackHeader::new(b"x-content-type-options", b"nosniff"),
        HpackHeader::new(b"access-control-allow-origin", b"*"),
    ]
}

/// A delivered name as it was spelled when the digests were recorded. `origin` was then in the
/// common-header table, which spelled it `Origin`; it is now delivered as received.
fn recorded_spelling(name: &[u8]) -> &[u8] {
    if name == b"origin" { b"Origin" } else { name }
}

/// The delivered heads' `(name, value)` lists, in order, and the bytes of re-encoding each head
/// through a fresh egress codec.
fn delivered_and_reencoded(messages: &[HttpMessage], direction: TransportDirection) -> [u64; 4] {
    let mut heads = Vec::new();
    let mut reencoded = Vec::new();
    for message in messages {
        for (_, name, value) in message.headers().iter() {
            heads.extend_from_slice(recorded_spelling(name));
            heads.push(0);
            heads.extend_from_slice(value);
            heads.push(0);
        }
        heads.push(0xff);
        let mut egress = Http2Codec::new(direction);
        egress.set_add_date_header_to_response(false);
        let stream = if direction.is_upstream() {
            egress.create_stream()
        } else {
            StreamId::new(1)
        };
        let mut output = BufQueue::new(64 * 1024);
        egress
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut Trace::default(),
            )
            .unwrap();
        reencoded.extend_from_slice(&bytes(&output));
    }
    [
        fnv1a64(&heads),
        heads.len() as u64,
        fnv1a64(&reencoded),
        reencoded.len() as u64,
    ]
}

/// Decodes 32 stateful consumer-shaped blocks through one codec of `direction`. Responses keep
/// their streams open, as responses with a body do.
fn consumer_shaped_heads(direction: TransportDirection) -> Vec<HttpMessage> {
    let mut codec = Http2Codec::new(direction);
    if direction.is_upstream() {
        for _ in 0..32 {
            codec.create_stream();
        }
    }
    open(&mut codec, &mut Trace::default());
    let mut encoder = HpackCodec::new();
    let mut capture = MessageCapture::default();
    for sequence in 0..32_u32 {
        let fields = if direction.is_downstream() {
            consumer_request_fields(u64::from(sequence))
        } else {
            consumer_response_fields(u64::from(sequence))
        };
        let encoded = encoder.encode(&fields);
        let mut wire = BufQueue::new(64 * 1024);
        frame::write_headers(
            &mut wire,
            queue(&encoded),
            2 * sequence + 1,
            None,
            direction.is_downstream(),
            true,
        )
        .unwrap();
        let wire = bytes(&wire);
        assert_eq!(
            codec
                .on_ingress(&queue(&wire), &mut capture, &mut BufQueue::new(64 * 1024))
                .consumed,
            wire.len()
        );
    }
    assert!(capture.error.is_none());
    assert_eq!(capture.messages.len(), 32);
    capture.messages
}

#[test]
fn consumer_shaped_heads_decode_and_reencode_identically() {
    let requests = consumer_shaped_heads(TransportDirection::Downstream);
    let responses = consumer_shaped_heads(TransportDirection::Upstream);
    let digests = (
        delivered_and_reencoded(&requests, TransportDirection::Upstream),
        delivered_and_reencoded(&responses, TransportDirection::Downstream),
    );
    assert_eq!(
        digests,
        (
            [0xd1b2_28b5_9e60_f70e, 22_834, 0xcf15_344b_8f99_2d7a, 15_974],
            [0x5cda_4c4c_cb54_073c, 18_048, 0x9e92_dae2_96f8_4ff5, 11_789],
        ),
        "recorded before any header-map pre-size"
    );
    let origin = (
        HttpHeaderCode::Other,
        &b"origin"[..],
        &b"https://client.example.com"[..],
    );
    assert!(
        requests
            .iter()
            .all(|message| message.headers().iter().any(|header| header == origin)),
        "`origin` is delivered as received"
    );
    assert!(requests.iter().all(|message| message.headers().len() > 16));
    assert!(responses.iter().all(|message| message.headers().len() > 16));
}
