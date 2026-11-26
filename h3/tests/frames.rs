/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::Bytes;
use bytes::BytesMut;

use crate::codec::DecodeError;
use crate::codec::EncodeError;
use crate::codec::Frame;
use crate::codec::FrameDecoder;
use crate::codec::FrameLimits;
use crate::codec::FrameType;
use crate::codec::H3ErrorCode;
use crate::codec::Role;
use crate::codec::SettingId;
use crate::codec::Settings;
use crate::codec::StreamKind;
use crate::codec::VARINT_MAX;
use crate::codec::encode_frame_header;
use crate::codec::frame_header_len;

fn encode(frames: &[Frame]) -> Bytes {
    let mut buf = BytesMut::new();
    for frame in frames {
        frame.encode(&mut buf).unwrap();
    }
    buf.freeze()
}

fn decoder(kind: StreamKind, role: Role) -> FrameDecoder {
    FrameDecoder::new(kind, role, FrameLimits::default())
}

/// Feeds `input` in one call and collects every frame.
fn decode_all(decoder: &mut FrameDecoder, input: &[u8]) -> Result<Vec<Frame>, DecodeError> {
    let mut input = Bytes::copy_from_slice(input);
    let mut frames = Vec::new();
    while let Some(frame) = decoder.decode(&mut input)? {
        frames.push(frame);
    }
    assert!(
        input.is_empty(),
        "the decoder stopped with {} bytes left",
        input.len()
    );
    Ok(frames)
}

#[test]
fn data_frames_round_trip() {
    let frames = [
        Frame::Data(Bytes::from_static(b"hello")),
        Frame::Data(Bytes::new()),
    ];
    let wire = encode(&frames);
    assert_eq!(&wire[..], b"\x00\x05hello\x00\x00");
    let mut decoder = decoder(StreamKind::Request, Role::Server);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(frames.to_vec()));
}

#[test]
fn headers_frames_round_trip() {
    let frames = [
        Frame::Headers(Bytes::from_static(b"\x00\x00\xd1\xd7")),
        Frame::Headers(Bytes::new()),
    ];
    let wire = encode(&frames);
    assert_eq!(&wire[..], b"\x01\x04\x00\x00\xd1\xd7\x01\x00");
    let mut decoder = decoder(StreamKind::Request, Role::Server);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(frames.to_vec()));
}

#[test]
fn settings_frames_round_trip() {
    let mut settings = Settings::new();
    settings.set(SettingId::QPACK_BLOCKED_STREAMS, 100).unwrap();
    settings
        .set(SettingId::MAX_FIELD_SECTION_SIZE, 16_384)
        .unwrap();
    settings
        .set(SettingId::QPACK_MAX_TABLE_CAPACITY, 0)
        .unwrap();
    settings.set(SettingId::grease(5), VARINT_MAX).unwrap();
    let frame = Frame::Settings(settings);
    let wire = encode(std::slice::from_ref(&frame));
    let expected: &[u8] = &[
        0x04, 0x14, // type, length
        0x01, 0x00, // QPACK_MAX_TABLE_CAPACITY = 0
        0x06, 0x80, 0x00, 0x40, 0x00, // MAX_FIELD_SECTION_SIZE = 16384
        0x07, 0x40, 0x64, // QPACK_BLOCKED_STREAMS = 100
        0x40, 0xbc, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // grease = 2^62 - 1
    ];
    assert_eq!(&wire[..], expected);
    let mut decoder = decoder(StreamKind::Control, Role::Server);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(vec![frame]));

    let empty = Frame::Settings(Settings::new());
    let wire = encode(std::slice::from_ref(&empty));
    assert_eq!(&wire[..], b"\x04\x00");
    let mut decoder = self::decoder(StreamKind::Control, Role::Client);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(vec![empty]));
}

/// Prefixes the SETTINGS frame that every control stream starts with.
fn control_stream(frames: &[Frame]) -> Vec<Frame> {
    let mut all = vec![Frame::Settings(Settings::new())];
    all.extend_from_slice(frames);
    all
}

#[test]
fn cancel_push_frames_round_trip() {
    let frames = control_stream(&[Frame::CancelPush(0), Frame::CancelPush(VARINT_MAX)]);
    let wire = encode(&frames);
    assert_eq!(
        &wire[..],
        b"\x04\x00\x03\x01\x00\x03\x08\xff\xff\xff\xff\xff\xff\xff\xff"
    );
    let mut decoder = decoder(StreamKind::Control, Role::Server);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(frames));
}

#[test]
fn goaway_frames_round_trip() {
    // A server's GOAWAY carries a client-initiated bidirectional stream ID, a client's a push ID.
    for (role, ids) in [(Role::Client, [16_384, 4]), (Role::Server, [20_000, 2])] {
        let frames = control_stream(&[Frame::Goaway(ids[0]), Frame::Goaway(ids[1])]);
        let wire = encode(&frames);
        let mut expected = b"\x04\x00\x07\x04".to_vec();
        expected.extend_from_slice(&(ids[0] as u32 | 0x8000_0000).to_be_bytes());
        expected.extend_from_slice(&[0x07, 0x01, ids[1] as u8]);
        assert_eq!(&wire[..], expected, "{role:?}");
        let mut decoder = decoder(StreamKind::Control, role);
        assert_eq!(decode_all(&mut decoder, &wire), Ok(frames), "{role:?}");
    }
}

#[test]
fn max_push_id_frames_round_trip() {
    let frames = control_stream(&[Frame::MaxPushId(0), Frame::MaxPushId(100)]);
    let wire = encode(&frames);
    assert_eq!(&wire[..], b"\x04\x00\x0d\x01\x00\x0d\x02\x40\x64");
    let mut decoder = decoder(StreamKind::Control, Role::Server);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(frames));
}

#[test]
fn push_promise_frames_round_trip() {
    let frames = [
        Frame::PushPromise {
            push_id: 3,
            field_section: Bytes::from_static(b"\x00\x00\xd1"),
        },
        Frame::PushPromise {
            push_id: 16_383,
            field_section: Bytes::new(),
        },
    ];
    let wire = encode(&frames);
    assert_eq!(&wire[..], b"\x05\x04\x03\x00\x00\xd1\x05\x02\x7f\xff");
    let mut decoder = decoder(StreamKind::Request, Role::Client);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(frames.to_vec()));
}

#[test]
fn grease_and_unknown_frames_are_skipped() {
    let grease = Frame::Grease {
        frame_type: FrameType::grease(0),
        payload: Bytes::from_static(b"pad"),
    };
    assert!(FrameType::grease(0).is_grease());
    assert_eq!(&encode(std::slice::from_ref(&grease))[..], b"\x21\x03pad");

    // A PRIORITY_UPDATE frame (RFC 9218), unknown here, whose payload exceeds every limit.
    let mut unknown = b"\x80\x0f\x07\x00\x80\x10\x00\x00".to_vec();
    unknown.resize(unknown.len() + (1 << 20), 0xaa);

    let data = Frame::Data(Bytes::from_static(b"a"));
    let headers = Frame::Headers(Bytes::from_static(b"\x00\x00"));
    let mut wire = encode(&[grease.clone(), data.clone()]).to_vec();
    wire.extend_from_slice(&unknown);
    wire.extend_from_slice(&encode(&[headers.clone(), grease.clone()]));
    let mut decoder = decoder(StreamKind::Request, Role::Server);
    assert_eq!(decode_all(&mut decoder, &wire), Ok(vec![data, headers]));

    let goaway = Frame::Goaway(0);
    let mut wire = encode(&control_stream(std::slice::from_ref(&grease))).to_vec();
    wire.extend_from_slice(&unknown);
    wire.extend_from_slice(&encode(&[goaway.clone(), grease]));
    let mut decoder = self::decoder(StreamKind::Control, Role::Client);
    assert_eq!(
        decode_all(&mut decoder, &wire),
        Ok(control_stream(&[goaway]))
    );
}

/// Feeds `pieces` one call each and collects every frame, joining the parts of each DATA run so
/// that results compare equal however the input was cut.
fn decode_pieces<'a>(
    decoder: &mut FrameDecoder,
    pieces: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Vec<Frame>, DecodeError> {
    let mut frames = Vec::new();
    for piece in pieces {
        let mut input = Bytes::copy_from_slice(piece);
        while let Some(frame) = decoder.decode(&mut input)? {
            frames.push(frame);
        }
        assert!(input.is_empty());
    }
    Ok(join_data(frames))
}

fn join_data(frames: Vec<Frame>) -> Vec<Frame> {
    let mut joined: Vec<Frame> = Vec::new();
    for frame in frames {
        match (joined.last_mut(), frame) {
            (Some(Frame::Data(run)), Frame::Data(more)) => {
                *run = [&run[..], &more[..]].concat().into();
            }
            (_, frame) => joined.push(frame),
        }
    }
    joined
}

/// Checks that `wire` decodes the same when cut at any one point and when fed a byte at a time.
fn check_every_split(kind: StreamKind, role: Role, wire: &[u8]) {
    let whole = join_data(decode_all(&mut decoder(kind, role), wire).unwrap());
    for cut in 0..=wire.len() {
        let (head, tail) = wire.split_at(cut);
        let frames = decode_pieces(&mut decoder(kind, role), [head, tail]);
        assert_eq!(frames.as_ref(), Ok(&whole), "cut at {cut}");
    }
    let bytes = decode_pieces(&mut decoder(kind, role), wire.chunks(1));
    assert_eq!(bytes, Ok(whole), "byte at a time");
}

#[test]
fn frames_decode_when_split_at_every_byte() {
    let mut settings = Settings::new();
    settings
        .set(SettingId::MAX_FIELD_SECTION_SIZE, 1 << 40)
        .unwrap();
    settings
        .set(SettingId::QPACK_BLOCKED_STREAMS, 16_384)
        .unwrap();
    settings.set(SettingId::grease(1 << 40), 7).unwrap();
    let grease = Frame::Grease {
        frame_type: FrameType::grease(1 << 33),
        payload: Bytes::from_static(b"padding"),
    };
    let control = encode(&[
        Frame::Settings(settings),
        grease.clone(),
        Frame::CancelPush(VARINT_MAX),
        Frame::MaxPushId(5),
        Frame::Goaway(1 << 20),
    ]);
    check_every_split(StreamKind::Control, Role::Server, &control);

    let request = encode(&[
        Frame::Headers(Bytes::from_static(b"\x00\x00\xd1")),
        grease,
        Frame::Data(Bytes::from(vec![0x5a; 300])),
        Frame::Data(Bytes::new()),
        Frame::PushPromise {
            push_id: 1 << 30,
            field_section: Bytes::from_static(b"\x00\x00\xd1\xd7"),
        },
        Frame::Data(Bytes::from_static(b"tail")),
        Frame::Headers(Bytes::from_static(b"\x00\x00")),
    ]);
    check_every_split(StreamKind::Request, Role::Client, &request);
}

#[test]
fn payloads_are_delivered_without_copying() {
    let mut decoder = decoder(StreamKind::Request, Role::Server);
    let mut first = Bytes::from_static(b"\x00\x0aabcd");
    let Ok(Some(Frame::Data(chunk))) = decoder.decode(&mut first) else {
        panic!("expected the first part of the DATA payload");
    };
    assert_eq!(&chunk[..], b"abcd");
    assert_eq!(chunk.as_ptr(), first.as_ptr().wrapping_sub(4));
    assert_eq!(decoder.decode(&mut first), Ok(None));

    let mut second = Bytes::from_static(b"efghij\x01\x03\x00\x00\xd1");
    let start = second.as_ptr();
    let Ok(Some(Frame::Data(chunk))) = decoder.decode(&mut second) else {
        panic!("expected the rest of the DATA payload");
    };
    assert_eq!((&chunk[..], chunk.as_ptr()), (&b"efghij"[..], start));
    let Ok(Some(Frame::Headers(field_section))) = decoder.decode(&mut second) else {
        panic!("expected HEADERS");
    };
    assert_eq!(&field_section[..], b"\x00\x00\xd1");
    assert_eq!(field_section.as_ptr(), start.wrapping_add(8));
}

/// Returns a valid frame of type `frame_type`, which must be a defined or grease type.
fn sample(frame_type: FrameType) -> Frame {
    match frame_type {
        FrameType::DATA => Frame::Data(Bytes::from_static(b"x")),
        FrameType::HEADERS => Frame::Headers(Bytes::from_static(b"\x00\x00")),
        FrameType::CANCEL_PUSH => Frame::CancelPush(1),
        FrameType::SETTINGS => Frame::Settings(Settings::new()),
        FrameType::PUSH_PROMISE => Frame::PushPromise {
            push_id: 1,
            field_section: Bytes::from_static(b"\x00\x00"),
        },
        FrameType::GOAWAY => Frame::Goaway(0),
        FrameType::MAX_PUSH_ID => Frame::MaxPushId(1),
        frame_type => Frame::Grease {
            frame_type,
            payload: Bytes::new(),
        },
    }
}

/// Returns a decoder for `kind`, which for a control stream has read the opening SETTINGS frame.
fn started(kind: StreamKind, role: Role) -> FrameDecoder {
    let mut decoder = decoder(kind, role);
    if kind == StreamKind::Control {
        assert_eq!(decode_all(&mut decoder, b"\x04\x00").unwrap().len(), 1);
    }
    decoder
}

#[test]
fn frame_types_are_checked_against_the_stream_kind() {
    use StreamKind::*;
    // The frame types that each stream kind carries, as received by a client and by a server.
    // SETTINGS, permitted only as the first frame of a control stream, is checked separately.
    let table: [(FrameType, &[StreamKind], &[StreamKind]); 8] = [
        (FrameType::DATA, &[Request, Push], &[Request, Push]),
        (FrameType::HEADERS, &[Request, Push], &[Request, Push]),
        (FrameType::CANCEL_PUSH, &[Control], &[Control]),
        (FrameType::SETTINGS, &[], &[]),
        (FrameType::PUSH_PROMISE, &[Request], &[]),
        (FrameType::GOAWAY, &[Control], &[Control]),
        (FrameType::MAX_PUSH_ID, &[], &[Control]),
        (
            FrameType::grease(3),
            &[Control, Request, Push],
            &[Control, Request, Push],
        ),
    ];
    for (frame_type, at_client, at_server) in table {
        for (role, permitted) in [(Role::Client, at_client), (Role::Server, at_server)] {
            for kind in [Control, Request, Push] {
                if (frame_type, kind) == (FrameType::SETTINGS, Control) {
                    continue;
                }
                let mut decoder = started(kind, role);
                let wire = encode(&[sample(frame_type)]);
                let context = format!("{frame_type:?} on {kind:?} at the {role:?}");
                if permitted.contains(&kind) {
                    let decoded = decode_all(&mut decoder, &wire);
                    assert!(decoded.is_ok(), "{context}: {decoded:?}");
                } else {
                    // The type alone is enough to reject the frame.
                    let type_len = wire.len() - 1 - (wire[1] as usize);
                    let mut input = wire.slice(..type_len);
                    let error = DecodeError::UnexpectedFrame(frame_type);
                    assert_eq!(decoder.decode(&mut input), Err(error), "{context}");
                    assert_eq!(error.code(), H3ErrorCode::FRAME_UNEXPECTED, "{context}");
                }
            }
        }
    }
}

#[test]
fn http2_frame_types_are_unexpected_on_every_stream() {
    // PRIORITY, PING, WINDOW_UPDATE and CONTINUATION.
    for value in [0x02, 0x06, 0x08, 0x09] {
        let frame_type = FrameType::new(value).unwrap();
        for role in [Role::Client, Role::Server] {
            for kind in [StreamKind::Control, StreamKind::Request, StreamKind::Push] {
                let mut decoder = started(kind, role);
                assert_eq!(
                    decoder.decode(&mut &[value as u8][..]),
                    Err(DecodeError::UnexpectedFrame(frame_type)),
                    "{value:#x} on {kind:?} at the {role:?}"
                );
            }
        }
    }
}

#[test]
fn control_streams_must_start_with_settings() {
    let first_frames = [
        FrameType::DATA,
        FrameType::CANCEL_PUSH,
        FrameType::GOAWAY,
        FrameType::MAX_PUSH_ID,
        FrameType::grease(0),
        FrameType::new(0x02).unwrap(),
        FrameType::new(0xf0700).unwrap(),
    ];
    for frame_type in first_frames {
        let mut decoder = decoder(StreamKind::Control, Role::Server);
        let mut wire = Vec::new();
        crate::codec::varint::put_varint(&mut wire, frame_type.as_u64());
        let error = decoder.decode(&mut &wire[..]).unwrap_err();
        assert_eq!(error, DecodeError::MissingSettings, "{frame_type:?}");
        assert_eq!(error.code(), H3ErrorCode::MISSING_SETTINGS);
    }
}

#[test]
fn a_second_settings_frame_is_unexpected() {
    for role in [Role::Client, Role::Server] {
        let mut decoder = decoder(StreamKind::Control, role);
        let wire = encode(&control_stream(&[
            Frame::Goaway(0),
            Frame::Settings(Settings::new()),
        ]));
        assert_eq!(
            decode_all(&mut decoder, &wire),
            Err(DecodeError::UnexpectedFrame(FrameType::SETTINGS)),
            "{role:?}"
        );
    }
}

#[test]
fn single_integer_frames_must_match_their_length() {
    // The length and payload, after the type, of frames whose length does not match the one
    // integer they carry.
    let bad_payloads: [&[u8]; 5] = [
        b"\x00",                                     // empty
        b"\x01\x40",                                 // ends inside a two-byte integer
        b"\x02\x05\x00",                             // a byte after the integer
        b"\x03\x80\x00\x00",                         // ends inside a four-byte integer
        b"\x09\xc0\x00\x00\x00\x00\x00\x00\x00\x00", // longer than any integer
    ];
    for frame_type in [
        FrameType::CANCEL_PUSH,
        FrameType::GOAWAY,
        FrameType::MAX_PUSH_ID,
    ] {
        for payload in bad_payloads {
            let mut decoder = started(StreamKind::Control, Role::Server);
            let mut wire = vec![frame_type.as_u64() as u8];
            wire.extend_from_slice(payload);
            let error = decode_all(&mut decoder, &wire).unwrap_err();
            assert_eq!(error, DecodeError::MalformedFrame(frame_type), "{wire:x?}");
            assert_eq!(error.code(), H3ErrorCode::FRAME_ERROR);
        }
        // A length longer than any integer is rejected before the payload arrives.
        let mut decoder = started(StreamKind::Control, Role::Server);
        let wire = [frame_type.as_u64() as u8, 0x09];
        assert_eq!(
            decoder.decode(&mut &wire[..]),
            Err(DecodeError::MalformedFrame(frame_type))
        );
    }
}

#[test]
fn single_integer_frames_longer_than_a_control_stream_buffers_are_excessive_load() {
    let bound = FrameLimits::default().max_settings_payload as u64;
    for frame_type in [
        FrameType::CANCEL_PUSH,
        FrameType::GOAWAY,
        FrameType::MAX_PUSH_ID,
    ] {
        let header = |len: u64| {
            let mut wire = Vec::new();
            crate::codec::varint::put_varint(&mut wire, frame_type.as_u64());
            crate::codec::varint::put_varint(&mut wire, len);
            wire
        };
        // h3spec's GOAWAY declares 2^30 bytes.
        for len in [1 << 30, bound + 1] {
            let mut decoder = started(StreamKind::Control, Role::Server);
            let error = decoder.decode(&mut &header(len)[..]).unwrap_err();
            assert_eq!(
                error,
                DecodeError::FrameTooLarge(frame_type),
                "{frame_type:?} of {len} bytes"
            );
            assert_eq!(error.code(), H3ErrorCode::EXCESSIVE_LOAD);
        }
        // A length the stream may buffer, but too long for one integer, is malformed.
        let mut decoder = started(StreamKind::Control, Role::Server);
        let error = decoder.decode(&mut &header(bound)[..]).unwrap_err();
        assert_eq!(error, DecodeError::MalformedFrame(frame_type));
        assert_eq!(error.code(), H3ErrorCode::FRAME_ERROR);
    }
}

#[test]
fn eight_byte_integer_frames_are_accepted_under_a_smaller_settings_limit() {
    let limits = FrameLimits {
        max_settings_payload: 4,
        ..FrameLimits::default()
    };
    let frames = control_stream(&[
        Frame::CancelPush(VARINT_MAX),
        Frame::Goaway(VARINT_MAX),
        Frame::MaxPushId(VARINT_MAX),
    ]);
    let mut decoder = FrameDecoder::new(StreamKind::Control, Role::Server, limits);
    assert_eq!(decode_all(&mut decoder, &encode(&frames)), Ok(frames));
}

#[test]
fn push_promise_must_hold_a_whole_push_id() {
    for wire in [&b"\x05\x00"[..], b"\x05\x01\x40", b"\x05\x03\x80\x00\x00"] {
        let mut decoder = decoder(StreamKind::Request, Role::Client);
        assert_eq!(
            decode_all(&mut decoder, wire),
            Err(DecodeError::MalformedFrame(FrameType::PUSH_PROMISE)),
            "{wire:x?}"
        );
    }
}

#[test]
fn a_decoder_stays_failed_after_an_error() {
    let cases = [
        // A SETTINGS frame with a duplicate identifier, then a valid GOAWAY.
        (
            StreamKind::Control,
            &b"\x04\x04\x06\x01\x06\x02\x07\x01\x00"[..],
            DecodeError::DuplicateSetting(SettingId::MAX_FIELD_SECTION_SIZE),
        ),
        // A SETTINGS frame on a request stream, then a valid DATA frame.
        (
            StreamKind::Request,
            b"\x04\x00\x00\x01x",
            DecodeError::UnexpectedFrame(FrameType::SETTINGS),
        ),
    ];
    for (kind, wire, error) in cases {
        let mut decoder = decoder(kind, Role::Client);
        let mut input = Bytes::from_static(wire);
        assert_eq!(decoder.decode(&mut input), Err(error), "{kind:?}");
        let left = input.len();
        assert_eq!(decoder.decode(&mut input), Err(error), "{kind:?}");
        assert_eq!(
            input.len(),
            left,
            "{kind:?}: a failed decoder consumes nothing"
        );
        assert_eq!(
            decoder.finish(),
            Err(error),
            "{kind:?}: the stream then ends"
        );
    }
}

#[test]
fn buffered_frames_above_the_limits_are_rejected_before_their_payload() {
    let limits = FrameLimits {
        max_field_section_payload: 100,
        max_settings_payload: 10,
    };
    let cases = [
        (StreamKind::Request, Role::Server, FrameType::HEADERS, 100),
        (
            StreamKind::Request,
            Role::Client,
            FrameType::PUSH_PROMISE,
            100,
        ),
        (StreamKind::Push, Role::Client, FrameType::HEADERS, 100),
        (StreamKind::Control, Role::Server, FrameType::SETTINGS, 10),
    ];
    for (kind, role, frame_type, limit) in cases {
        let header = |len: u64| {
            let mut wire = Vec::new();
            crate::codec::varint::put_varint(&mut wire, frame_type.as_u64());
            crate::codec::varint::put_varint(&mut wire, len);
            wire
        };
        let mut decoder = FrameDecoder::new(kind, role, limits);
        let error = decoder.decode(&mut &header(limit + 1)[..]).unwrap_err();
        assert_eq!(
            error,
            DecodeError::FrameTooLarge(frame_type),
            "{frame_type:?}"
        );
        assert_eq!(error.code(), H3ErrorCode::EXCESSIVE_LOAD);

        // A payload of exactly the limit is accepted. It starts with a push ID and is a series of
        // distinct settings.
        let mut wire = header(limit);
        wire.extend((0..limit as u8).map(|i| if i % 2 == 0 { 0x10 + i / 2 } else { 0 }));
        let mut decoder = FrameDecoder::new(kind, role, limits);
        let decoded = decode_all(&mut decoder, &wire);
        assert!(
            matches!(decoded.as_deref(), Ok([_])),
            "{frame_type:?}: {decoded:?}"
        );
    }
}

#[test]
fn a_stream_that_ends_inside_a_frame_is_malformed() {
    let frames = [
        Frame::Headers(Bytes::from_static(b"\x00\x00\xd1")),
        Frame::Data(Bytes::from(vec![0x5a; 70])),
        Frame::Grease {
            frame_type: FrameType::grease(2),
            payload: Bytes::from_static(b"pad"),
        },
        Frame::Data(Bytes::new()),
    ];
    let mut boundaries = vec![0];
    for frame in &frames {
        boundaries.push(boundaries.last().unwrap() + encode(std::slice::from_ref(frame)).len());
    }
    let wire = encode(&frames);
    for cut in 0..=wire.len() {
        let mut decoder = decoder(StreamKind::Request, Role::Server);
        decode_pieces(&mut decoder, [&wire[..cut]]).unwrap();
        let finished = decoder.finish();
        if boundaries.contains(&cut) {
            assert_eq!(finished, Ok(()), "end at {cut}");
        } else {
            assert_eq!(finished, Err(DecodeError::TruncatedFrame), "end at {cut}");
            assert_eq!(finished.unwrap_err().code(), H3ErrorCode::FRAME_ERROR);
        }
    }
}

#[test]
fn ending_a_control_stream_is_an_error() {
    for wire in [&b""[..], b"\x04", b"\x04\x00", b"\x04\x00\x07\x01"] {
        let mut decoder = decoder(StreamKind::Control, Role::Client);
        decode_pieces(&mut decoder, [wire]).unwrap();
        let error = decoder.finish().unwrap_err();
        assert_eq!(error, DecodeError::ClosedCriticalStream, "{wire:x?}");
        assert_eq!(error.code(), H3ErrorCode::CLOSED_CRITICAL_STREAM);
    }
}

#[test]
fn a_client_rejects_goaway_without_a_request_stream_id() {
    for id in [1, 2, 3, 7, 1 << 20 | 2] {
        let mut decoder = started(StreamKind::Control, Role::Client);
        let error = decode_all(&mut decoder, &encode(&[Frame::Goaway(id)])).unwrap_err();
        assert_eq!(error, DecodeError::InvalidId(FrameType::GOAWAY), "{id}");
        assert_eq!(error.code(), H3ErrorCode::ID_ERROR);
    }
    // A client's GOAWAY carries a push ID, which may take any value.
    let mut decoder = started(StreamKind::Control, Role::Server);
    assert_eq!(
        decode_all(&mut decoder, &encode(&[Frame::Goaway(3)])),
        Ok(vec![Frame::Goaway(3)])
    );
}

#[test]
fn goaway_ids_must_not_increase() {
    for role in [Role::Client, Role::Server] {
        let mut decoder = started(StreamKind::Control, role);
        let frames = [Frame::Goaway(8), Frame::Goaway(8), Frame::Goaway(4)];
        assert_eq!(
            decode_all(&mut decoder, &encode(&frames)),
            Ok(frames.to_vec()),
            "{role:?}"
        );
        assert_eq!(
            decode_all(&mut decoder, &encode(&[Frame::Goaway(8)])),
            Err(DecodeError::InvalidId(FrameType::GOAWAY)),
            "{role:?}"
        );
    }
}

#[test]
fn max_push_id_must_not_decrease() {
    let mut decoder = started(StreamKind::Control, Role::Server);
    let frames = [
        Frame::MaxPushId(4),
        Frame::MaxPushId(4),
        Frame::MaxPushId(9),
    ];
    assert_eq!(
        decode_all(&mut decoder, &encode(&frames)),
        Ok(frames.to_vec())
    );
    let error = decode_all(&mut decoder, &encode(&[Frame::MaxPushId(8)])).unwrap_err();
    assert_eq!(error, DecodeError::InvalidId(FrameType::MAX_PUSH_ID));
    assert_eq!(error.code(), H3ErrorCode::ID_ERROR);
}

#[test]
fn values_above_62_bits_are_not_encoded() {
    let too_large = VARINT_MAX + 1;
    let frames = [
        Frame::CancelPush(too_large),
        Frame::Goaway(u64::MAX),
        Frame::MaxPushId(too_large),
        Frame::PushPromise {
            push_id: too_large,
            field_section: Bytes::from_static(b"\x00\x00"),
        },
    ];
    for frame in frames {
        let mut buf = BytesMut::from(&b"kept"[..]);
        assert_eq!(
            frame.encode(&mut buf),
            Err(EncodeError::ValueTooLarge),
            "{frame:?}"
        );
        assert_eq!(&buf[..], b"kept", "{frame:?}");
    }
}

#[test]
fn grease_frames_need_a_reserved_type() {
    for value in [0x00, 0x02, 0x04, 0x20, 0x22, 0xf0700] {
        let frame_type = FrameType::new(value).unwrap();
        let frame = Frame::Grease {
            frame_type,
            payload: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        assert_eq!(
            frame.encode(&mut buf),
            Err(EncodeError::NotGrease(frame_type))
        );
        assert!(buf.is_empty());
    }
}

#[test]
fn frame_headers_encode_apart_from_their_payload() {
    let payload = Bytes::from(vec![0x5a; 300]);
    let mut buf = BytesMut::new();
    encode_frame_header(&mut buf, FrameType::DATA, payload.len() as u64).unwrap();
    assert_eq!(&buf[..], b"\x00\x41\x2c");
    assert_eq!(frame_header_len(FrameType::DATA, 300), 3);
    buf.extend_from_slice(&payload);
    assert_eq!(buf.freeze(), encode(&[Frame::Data(payload)]));

    let mut buf = BytesMut::new();
    encode_frame_header(&mut buf, FrameType::grease(1 << 30), 1 << 30).unwrap();
    assert_eq!(buf.len(), 16);
    assert_eq!(frame_header_len(FrameType::grease(1 << 30), 1 << 30), 16);

    let mut buf = BytesMut::new();
    let result = encode_frame_header(&mut buf, FrameType::HEADERS, VARINT_MAX + 1);
    assert_eq!(result, Err(EncodeError::ValueTooLarge));
    assert!(buf.is_empty());
}

#[test]
fn http2_frame_types_are_not_encoded() {
    // PRIORITY, PING, WINDOW_UPDATE and CONTINUATION.
    for value in [0x02, 0x06, 0x08, 0x09] {
        let frame_type = FrameType::new(value).unwrap();
        let mut buf = BytesMut::from(&b"kept"[..]);
        assert_eq!(
            encode_frame_header(&mut buf, frame_type, 0),
            Err(EncodeError::ReservedFrameType(frame_type)),
            "{value:#x}"
        );
        assert_eq!(&buf[..], b"kept", "{value:#x}");
    }
}
