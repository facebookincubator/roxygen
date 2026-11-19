/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::panic::catch_unwind;

use bytes::Bytes;
use roxygen_codec::H2ErrorCode;
use roxygen_core::buf_queue::BufQueue;

use crate::constants::FRAME_CERTIFICATE_REQUEST_SIZE_BASE;
use crate::constants::FRAME_CERTIFICATE_SIZE_BASE;
use crate::constants::FRAME_GOAWAY_SIZE;
use crate::constants::FRAME_HEADER_SIZE;
use crate::constants::MAX_FRAME_PAYLOAD_LENGTH;
use crate::constants::MAX_STREAM_ID;
use crate::frame::AltSvcFrame;
use crate::frame::CertificateFrame;
use crate::frame::CertificateRequestFrame;
use crate::frame::Flags;
use crate::frame::Frame;
use crate::frame::FrameDecoder;
use crate::frame::FrameError;
use crate::frame::FrameHeader;
use crate::frame::FrameType;
use crate::frame::ParsedFrame;
use crate::frame::SegmentCursor;
use crate::frame::Setting;
use crate::frame::calculate_pre_header_block_size;
use crate::frame::frame_affects_compression;
use crate::frame::get_frame_type_string;
use crate::frame::is_valid_frame_type;
use crate::frame::parse_certificate;
use crate::frame::parse_certificate_request;
use crate::frame::parse_data;
use crate::frame::parse_data_begin;
use crate::frame::parse_data_end;
use crate::frame::parse_frame;
use crate::frame::parse_frame_header;
use crate::frame::parse_frame_header_cursor;
use crate::frame::parse_headers;
use crate::frame::parse_settings;
use crate::frame::write_alt_svc;
use crate::frame::write_certificate;
use crate::frame::write_certificate_request;
use crate::frame::write_continuation;
use crate::frame::write_data;
use crate::frame::write_goaway;
use crate::frame::write_headers;
use crate::frame::write_padding;
use crate::frame::write_ping;
use crate::frame::write_push_promise;
use crate::frame::write_rfc9218_priority;
use crate::frame::write_rst_stream;
use crate::frame::write_settings;
use crate::frame::write_settings_ack;
use crate::frame::write_window_update;

fn queue(parts: &[&[u8]]) -> BufQueue {
    let length = parts.iter().map(|part| part.len()).sum();
    let mut queue = BufQueue::new(length);
    for part in parts {
        queue
            .push_bytes(Bytes::copy_from_slice(part))
            .expect("fixture fits its exact bound");
    }
    queue
}

fn zeroes(length: usize) -> BufQueue {
    if length == 0 {
        BufQueue::new(0)
    } else {
        let data = vec![0; length];
        queue(&[&data])
    }
}

fn bytes(queue: &BufQueue) -> Vec<u8> {
    queue
        .segments()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}

fn manual_frame(
    length: u32,
    frame_type: FrameType,
    flags: u8,
    stream: u32,
    payload: &[&[u8]],
) -> BufQueue {
    let mut header = [0; FRAME_HEADER_SIZE];
    header[0] = (length >> 16) as u8;
    header[1] = (length >> 8) as u8;
    header[2] = length as u8;
    header[3] = frame_type.as_u8();
    header[4] = flags;
    header[5..].copy_from_slice(&stream.to_be_bytes());
    let mut parts = Vec::with_capacity(payload.len() + 1);
    parts.push(header.as_slice());
    parts.extend_from_slice(payload);
    queue(&parts)
}

fn payload(wire: &BufQueue, header: &FrameHeader) -> BufQueue {
    let mut payload = wire.clone_shared();
    payload
        .trim_start(FRAME_HEADER_SIZE)
        .expect("wire fixture contains a frame header");
    assert!(payload.chain_length() >= header.length as usize);
    payload.split_at_most(header.length as usize)
}

fn parsed(mut wire: BufQueue) -> ParsedFrame {
    let parsed = parse_frame(&mut wire)
        .expect("generated frame parses")
        .expect("generated frame is complete");
    assert!(wire.is_empty());
    parsed
}

fn assert_header(header: FrameHeader, frame_type: FrameType, length: u32, stream: u32, flags: u8) {
    assert_eq!(
        header,
        FrameHeader {
            length,
            stream,
            frame_type,
            flags,
        }
    );
}

fn data_round_trip(data_length: usize, padding: Option<u8>) {
    let data_bytes = vec![0x5a; data_length];
    let data = queue(&[&data_bytes]);
    let total = FRAME_HEADER_SIZE + data_length + padding.map_or(0, |pad| usize::from(pad) + 1);
    let mut wire = BufQueue::new(total);
    assert_eq!(
        write_data(&mut wire, data, 1, padding, false, true),
        Ok(total)
    );
    let parsed = parsed(wire);
    assert_header(
        parsed.header,
        FrameType::DATA,
        (data_length + padding.map_or(0, |pad| usize::from(pad) + 1)) as u32,
        1,
        if padding.is_some() { Flags::PADDED } else { 0 },
    );
    let Frame::Data(data) = parsed.frame else {
        panic!("expected DATA frame");
    };
    assert_eq!(bytes(&data.data), data_bytes);
    assert_eq!(
        data.padding,
        padding.map_or(0, |amount| u16::from(amount) + 1)
    );
}

#[test]
fn write_and_parse_small_data_no_pad() {
    data_round_trip(20, None);
}

#[test]
fn write_and_parse_small_data_zero_pad() {
    data_round_trip(20, Some(0));
}

#[test]
fn write_and_parse_small_data_small_pad() {
    data_round_trip(20, Some(20));
}

#[test]
fn many_data_frame_sizes() {
    data_round_trip(0, None);
    let mut data_length = 1usize;
    while data_length < MAX_FRAME_PAYLOAD_LENGTH as usize {
        data_round_trip(data_length, Some(0));
        data_round_trip(0, Some(data_length as u8));
        let mut padding = 1u8;
        loop {
            data_round_trip(data_length, Some(padding));
            let Some(next) = padding.checked_mul(2) else {
                break;
            };
            padding = next;
        }
        data_length <<= 1;
    }
}

#[test]
fn bad_stream_data() {
    let wire = manual_frame(0, FrameType::DATA, 0, 0, &[]);
    assert!(matches!(
        parse_frame(&mut wire.clone_shared()),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::PROTOCOL_ERROR
    ));
}

#[test]
fn bad_stream_settings() {
    let wire = manual_frame(0, FrameType::SETTINGS, Flags::ACK, 1, &[]);
    assert!(matches!(
        parse_frame(&mut wire.clone_shared()),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::PROTOCOL_ERROR
    ));
}

#[test]
fn bad_pad() {
    let wire = manual_frame(
        11,
        FrameType::DATA,
        Flags::PADDED,
        1,
        &[&[5], &[0; 5], b"abcde"],
    );
    assert!(matches!(
        parse_frame(&mut wire.clone_shared()),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::PROTOCOL_ERROR
    ));
}

#[test]
fn data_frame_preserves_payload_segments() {
    for (reuse_payload_headroom, padding) in [
        (false, None),
        (true, None),
        (false, Some(0)),
        (true, Some(0)),
        (false, Some(2)),
        (true, Some(2)),
    ] {
        let first_backing = Bytes::from_static(b"--first--");
        let first = first_backing.slice(2..7);
        let second = Bytes::from_static(b"second");
        let third = Bytes::from_static(b"third");
        let pointers = [first.as_ptr(), second.as_ptr(), third.as_ptr()];
        let mut data = BufQueue::new(first.len() + second.len() + third.len());
        data.push_bytes(first).expect("first payload segment fits");
        data.push_bytes(second)
            .expect("second payload segment fits");
        data.push_bytes(third).expect("third payload segment fits");
        let padding_length = padding.map_or(0, |amount| usize::from(amount) + 1);
        let total = FRAME_HEADER_SIZE + 16 + padding_length;
        let mut wire = BufQueue::new(total);

        assert_eq!(
            write_data(&mut wire, data, 1, padding, true, reuse_payload_headroom,),
            Ok(total)
        );

        assert_header(
            parse_frame_header(&wire).expect("generated DATA has a complete frame header"),
            FrameType::DATA,
            (16 + padding_length) as u32,
            1,
            Flags::END_STREAM | if padding.is_some() { Flags::PADDED } else { 0 },
        );
        let segments = wire.segments().collect::<Vec<_>>();
        assert_eq!(
            segments.len(),
            if padding == Some(2) { 5 } else { 4 },
            "frame metadata and payload must remain separate"
        );
        assert_eq!(
            segments[0].len(),
            FRAME_HEADER_SIZE + usize::from(padding.is_some())
        );
        assert_eq!(segments[1].as_ref(), b"first");
        assert_eq!(segments[1].as_ptr(), pointers[0]);
        assert_eq!(segments[2].as_ref(), b"second");
        assert_eq!(segments[2].as_ptr(), pointers[1]);
        assert_eq!(segments[3].as_ref(), b"third");
        assert_eq!(segments[3].as_ptr(), pointers[2]);
        if padding == Some(2) {
            assert_eq!(segments[4].as_ref(), &[0, 0]);
        }
    }
}

#[test]
fn bad_stream_id() {
    let mut wire = BufQueue::new(13);
    assert!(matches!(
        write_rst_stream(&mut wire, u32::MAX, H2ErrorCode::PROTOCOL_ERROR),
        Err(FrameError::InvalidInput(_))
    ));
    assert!(wire.is_empty());
}

#[test]
fn rst_stream() {
    let mut wire = BufQueue::new(13);
    assert_eq!(write_rst_stream(&mut wire, 1, H2ErrorCode::CANCEL), Ok(13));
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::RST_STREAM, 4, 1, 0);
    assert!(matches!(parsed.frame, Frame::RstStream(code) if code == H2ErrorCode::CANCEL));
}

#[test]
fn goaway() {
    let mut wire = BufQueue::new(17);
    assert_eq!(
        write_goaway(&mut wire, 0, H2ErrorCode::FLOW_CONTROL_ERROR, None),
        Ok(17)
    );
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::GOAWAY, 8, 0, 0);
    let Frame::Goaway {
        last_stream,
        error_code,
        debug_data,
    } = parsed.frame
    else {
        panic!("expected GOAWAY frame");
    };
    assert_eq!(last_stream, 0);
    assert_eq!(error_code, H2ErrorCode::FLOW_CONTROL_ERROR);
    assert!(debug_data.is_empty());
}

#[test]
fn goaway_debug_data() {
    let debug = queue(&[b"abc", b"de"]);
    let mut wire = BufQueue::new(22);
    assert_eq!(
        write_goaway(&mut wire, 0, H2ErrorCode::FLOW_CONTROL_ERROR, Some(debug)),
        Ok(22)
    );
    let parsed = parsed(wire);
    assert_header(
        parsed.header,
        FrameType::GOAWAY,
        FRAME_GOAWAY_SIZE as u32 + 5,
        0,
        0,
    );
    let Frame::Goaway { debug_data, .. } = parsed.frame else {
        panic!("expected GOAWAY frame");
    };
    assert_eq!(bytes(&debug_data), b"abcde");
}

#[test]
fn goaway_double_read() {
    let wire = manual_frame(
        FRAME_GOAWAY_SIZE as u32,
        FrameType::GOAWAY,
        0,
        0,
        &[&0u32.to_be_bytes(), &u32::MAX.to_be_bytes()],
    );
    assert!(matches!(
        parse_frame(&mut wire.clone_shared()),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::PROTOCOL_ERROR
    ));
}

#[test]
fn basic_rfc9218_priority() {
    let mut wire = BufQueue::new(64);
    write_rfc9218_priority(&mut wire, 1, b"u=7,i").unwrap();
    let parsed = parsed(wire);
    let Frame::Rfc9218Priority {
        prioritized_stream,
        priority,
    } = parsed.frame
    else {
        panic!("expected RFC 9218 PRIORITY_UPDATE frame");
    };
    assert_header(parsed.header, FrameType::RFC9218_PRIORITY, 9, 0, 0);
    assert_eq!(prioritized_stream, 1);
    assert_eq!(priority, b"u=7,i");
}

#[test]
fn bad_rfc9218_priority() {
    let urgency = 7_u8;
    let priority = format!("u={urgency},i");
    let mut wire = BufQueue::new(64);
    write_rfc9218_priority(&mut wire, 1, priority.as_bytes()).unwrap();
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::RFC9218_PRIORITY, 9, 0, 0);
    assert!(matches!(
        parsed.frame,
        Frame::Rfc9218Priority {
            prioritized_stream: 1,
            priority
        } if priority == b"u=7,i"
    ));
}

#[test]
fn ping() {
    let opaque = 0x0123_4567_89ab_cdef;
    let mut wire = BufQueue::new(17);
    write_ping(&mut wire, opaque, false).unwrap();
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::PING, 8, 0, 0);
    assert!(matches!(parsed.frame, Frame::Ping(value) if value == opaque));
}

#[test]
fn window_update() {
    let mut wire = BufQueue::new(13);
    write_window_update(&mut wire, 33, 120).unwrap();
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::WINDOW_UPDATE, 4, 33, 0);
    assert!(matches!(parsed.frame, Frame::WindowUpdate(120)));
}

fn parsed_certificate_request(data: Option<BufQueue>) -> (FrameHeader, CertificateRequestFrame) {
    let size = FRAME_HEADER_SIZE
        + FRAME_CERTIFICATE_REQUEST_SIZE_BASE
        + data.as_ref().map_or(0, BufQueue::chain_length);
    let mut wire = BufQueue::new(size);
    write_certificate_request(&mut wire, 120, data).unwrap();
    let parsed = parsed(wire);
    let Frame::CertificateRequest(request) = parsed.frame else {
        panic!("expected CERTIFICATE_REQUEST frame");
    };
    (parsed.header, request)
}

#[test]
fn certificate_request() {
    let data = queue(&[b"abc", b"def"]);
    let (header, request) = parsed_certificate_request(Some(data));
    assert_eq!(header.frame_type, FrameType::CERTIFICATE_REQUEST);
    assert_eq!(header.stream, 0);
    assert_eq!(header.length, 8);
    assert_eq!(header.flags, 0);
    assert_eq!(request.request_id, 120);
    assert_eq!(bytes(&request.authenticator_request), b"abcdef");
}

#[test]
fn empty_certificate_request() {
    let (header, request) = parsed_certificate_request(None);
    assert_header(
        header,
        FrameType::CERTIFICATE_REQUEST,
        FRAME_CERTIFICATE_REQUEST_SIZE_BASE as u32,
        0,
        0,
    );
    assert_eq!(request.request_id, 120);
    assert!(request.authenticator_request.is_empty());
}

#[test]
fn short_certificate_request() {
    let header = FrameHeader {
        length: 1,
        stream: 0,
        frame_type: FrameType::CERTIFICATE_REQUEST,
        flags: 0,
    };
    let input = queue(&[&[1]]);
    assert!(matches!(
        parse_certificate_request(&input, &header),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::FRAME_SIZE_ERROR
    ));
}

#[test]
fn certificate_request_on_nonzero_stream() {
    let header = FrameHeader {
        length: 2,
        stream: 1,
        frame_type: FrameType::CERTIFICATE_REQUEST,
        flags: 0,
    };
    let input = queue(&[&1u16.to_be_bytes()]);
    assert!(matches!(
        parse_certificate_request(&input, &header),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::PROTOCOL_ERROR
    ));
}

fn parsed_certificate(data: Option<BufQueue>, continued: bool) -> (FrameHeader, CertificateFrame) {
    let size = FRAME_HEADER_SIZE
        + FRAME_CERTIFICATE_SIZE_BASE
        + data.as_ref().map_or(0, BufQueue::chain_length);
    let mut wire = BufQueue::new(size);
    write_certificate(&mut wire, 120, data, continued).unwrap();
    let parsed = parsed(wire);
    let Frame::Certificate(certificate) = parsed.frame else {
        panic!("expected CERTIFICATE frame");
    };
    (parsed.header, certificate)
}

#[test]
fn ending_certificate() {
    let data = queue(&[b"abcdef"]);
    let (header, certificate) = parsed_certificate(Some(data), false);
    assert_header(header, FrameType::CERTIFICATE, 8, 0, 0);
    assert_eq!(certificate.certificate_id, 120);
    assert_eq!(bytes(&certificate.authenticator), b"abcdef");
}

#[test]
fn certificate() {
    let data = queue(&[b"abcdef"]);
    let (header, certificate) = parsed_certificate(Some(data), true);
    assert_header(header, FrameType::CERTIFICATE, 8, 0, Flags::TO_BE_CONTINUED);
    assert_eq!(certificate.certificate_id, 120);
    assert_eq!(bytes(&certificate.authenticator), b"abcdef");
}

#[test]
fn empty_certificate() {
    let (header, certificate) = parsed_certificate(None, true);
    assert_header(
        header,
        FrameType::CERTIFICATE,
        FRAME_CERTIFICATE_SIZE_BASE as u32,
        0,
        Flags::TO_BE_CONTINUED,
    );
    assert!(certificate.authenticator.is_empty());
}

#[test]
fn short_certificate() {
    let header = FrameHeader {
        length: 1,
        stream: 0,
        frame_type: FrameType::CERTIFICATE,
        flags: 0,
    };
    let input = queue(&[&[1]]);
    assert!(matches!(
        parse_certificate(&input, &header),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::FRAME_SIZE_ERROR
    ));
}

#[test]
fn certificate_on_nonzero_stream() {
    let header = FrameHeader {
        length: 2,
        stream: 1,
        frame_type: FrameType::CERTIFICATE,
        flags: 0,
    };
    let input = queue(&[&1u16.to_be_bytes()]);
    assert!(matches!(
        parse_certificate(&input, &header),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::PROTOCOL_ERROR
    ));
}

#[test]
fn short_window_update() {
    let wire = manual_frame(0, FrameType::WINDOW_UPDATE, 0, 0, &[]);
    assert!(matches!(
        parse_frame(&mut wire.clone_shared()),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::FRAME_SIZE_ERROR
    ));
}

#[test]
fn uint32_max_window_update() {
    let wire = manual_frame(
        4,
        FrameType::WINDOW_UPDATE,
        0,
        33,
        &[&u32::MAX.to_be_bytes()],
    );
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::WINDOW_UPDATE, 4, 33, 0);
    assert!(matches!(
        parsed.frame,
        Frame::WindowUpdate(amount) if amount == MAX_STREAM_ID
    ));
}

#[test]
fn alt_svc() {
    let mut wire = BufQueue::new(128);
    write_alt_svc(
        &mut wire,
        2,
        150,
        8080,
        b"special-proto",
        b"special-host",
        b"special-origin",
    )
    .unwrap();
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::ALTSVC, 47, 2, 0);
    assert!(matches!(
        parsed.frame,
        Frame::AltSvc(AltSvcFrame {
            max_age: 150,
            port: 8080,
            protocol,
            host,
            origin,
        }) if protocol == b"special-proto"
            && host == b"special-host"
            && origin == b"special-origin"
    ));
}

#[test]
fn alt_svc_length_octets_truncate_but_full_strings_are_written() {
    let protocol = vec![b'p'; 256];
    let host = vec![b'h'; 257];
    let origin = b"origin";
    let frame_length = 8 + protocol.len() + host.len() + origin.len();
    let mut wire = BufQueue::new(FRAME_HEADER_SIZE + frame_length);

    assert_eq!(
        write_alt_svc(&mut wire, 0, 1, 443, &protocol, &host, origin,),
        Ok(FRAME_HEADER_SIZE + frame_length)
    );
    let wire = bytes(&wire);
    assert_eq!(wire[FRAME_HEADER_SIZE + 6], 0);
    assert_eq!(
        &wire[FRAME_HEADER_SIZE + 7..FRAME_HEADER_SIZE + 7 + protocol.len()],
        protocol
    );
    assert_eq!(wire[FRAME_HEADER_SIZE + 7 + protocol.len()], 1);
    assert_eq!(
        &wire[FRAME_HEADER_SIZE + 8 + protocol.len()
            ..FRAME_HEADER_SIZE + 8 + protocol.len() + host.len()],
        host
    );
    assert_eq!(&wire[wire.len() - origin.len()..], origin);
}

#[test]
fn settings() {
    let settings = [
        Setting {
            identifier: 1,
            value: 3,
        },
        Setting {
            identifier: 3,
            value: 4,
        },
    ];
    let mut wire = BufQueue::new(21);
    write_settings(&mut wire, &settings).unwrap();
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::SETTINGS, 12, 0, 0);
    assert!(matches!(parsed.frame, Frame::Settings(actual) if actual == settings));
}

#[test]
fn settings_ack() {
    let mut wire = BufQueue::new(9);
    write_settings_ack(&mut wire).unwrap();
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::SETTINGS, 0, 0, Flags::ACK);
    assert!(matches!(parsed.frame, Frame::Settings(settings) if settings.is_empty()));
}

#[test]
fn bad_settings_ack() {
    let wire = manual_frame(1, FrameType::SETTINGS, Flags::ACK, 0, &[b"o"]);
    assert!(matches!(
        parse_frame(&mut wire.clone_shared()),
        Err(FrameError::Peer(code)) if code == H2ErrorCode::FRAME_SIZE_ERROR
    ));
}

#[test]
fn unknown_setting() {
    let settings = [
        Setting {
            identifier: 31_337,
            value: 3,
        },
        Setting {
            identifier: 0,
            value: 4,
        },
    ];
    let mut wire = BufQueue::new(21);
    write_settings(&mut wire, &settings).unwrap();
    let parsed = parsed(wire);
    assert_header(parsed.header, FrameType::SETTINGS, 12, 0, 0);
    assert!(matches!(parsed.frame, Frame::Settings(actual) if actual == settings));
}

#[test]
fn push_promise() {
    let header_block = zeroes(500);
    let mut wire = BufQueue::new(800);
    write_push_promise(&mut wire, 21, 22, header_block, Some(255), true).unwrap();
    let parsed = parsed(wire);
    assert_header(
        parsed.header,
        FrameType::PUSH_PROMISE,
        760,
        21,
        Flags::END_HEADERS | Flags::PADDED,
    );
    let Frame::PushPromise {
        promised_stream,
        header_block,
    } = parsed.frame
    else {
        panic!("expected PUSH_PROMISE frame");
    };
    assert_eq!(promised_stream, 22);
    assert_eq!(header_block.chain_length(), 500);
}

#[test]
fn continuation() {
    let header_block = zeroes(800);
    let mut wire = BufQueue::new(809);
    write_continuation(&mut wire, 1, true, header_block).unwrap();
    let parsed = parsed(wire);
    assert_header(
        parsed.header,
        FrameType::CONTINUATION,
        800,
        1,
        Flags::END_HEADERS,
    );
    assert_eq!(
        match parsed.frame {
            Frame::Continuation(block) => block.chain_length(),
            _ => panic!("expected CONTINUATION frame"),
        },
        800
    );
}

#[test]
fn headers_and_incremental_data_preserve_exact_boundaries() {
    let header_block = queue(&[b"abc", b"def"]);
    let mut wire = BufQueue::new(64);
    write_headers(&mut wire, header_block, 1, Some(2), true, true).unwrap();
    let header = parse_frame_header(&wire).unwrap();
    assert_eq!(header.length, 9);
    assert_eq!(
        header.flags,
        Flags::END_STREAM | Flags::END_HEADERS | Flags::PADDED
    );
    assert_eq!(
        bytes(&parse_headers(&payload(&wire, &header), &header).unwrap()),
        b"abcdef"
    );

    let data_header = FrameHeader {
        length: 11,
        stream: 1,
        frame_type: FrameType::DATA,
        flags: Flags::PADDED,
    };
    let begin = parse_data_begin(&queue(&[&[3]]), &data_header).unwrap();
    assert_eq!(begin.data_length, 7);
    assert_eq!(begin.padding, 4);
    assert_eq!(parse_data_end(&queue(&[&[0], &[0, 0]]), 3, 3), Ok(3));
}

#[test]
fn incremental_segmented_decoder_waits_without_consuming() {
    let mut encoded = BufQueue::new(64);
    write_ping(&mut encoded, 42, true).unwrap();
    let wire = bytes(&encoded);
    let mut decoder = FrameDecoder::new(64);
    for byte in &wire[..wire.len() - 1] {
        decoder
            .push_bytes(Bytes::copy_from_slice(&[*byte]))
            .unwrap();
        assert!(decoder.next_frame().unwrap().is_none());
    }
    assert_eq!(decoder.pending_bytes(), wire.len() - 1);
    decoder
        .push_bytes(Bytes::copy_from_slice(&wire[wire.len() - 1..]))
        .unwrap();
    let parsed = decoder.next_frame().unwrap().unwrap();
    assert_eq!(parsed.consumed, 17);
    assert_eq!(decoder.pending_bytes(), 0);
    assert_eq!(parsed.header.flags, Flags::ACK);
    assert!(matches!(parsed.frame, Frame::Ping(42)));
}

#[test]
fn frame_header_masks_reserved_bits_and_keeps_unknown_types() {
    let wire = manual_frame(0x01_0203, FrameType::from_u8(0x7f), 0xa5, 0x8000_0021, &[]);
    let header = parse_frame_header(&wire).unwrap();
    assert_eq!(header.length, 0x01_0203);
    assert_eq!(header.frame_type, FrameType::from_u8(0x7f));
    assert_eq!(header.flags, 0xa5);
    assert_eq!(header.stream, 0x21);
}

#[test]
fn padding_frame_and_frame_metadata_match_proxygen() {
    let mut short = BufQueue::new(9);
    assert_eq!(write_padding(&mut short, 7, 4), Ok(9));
    assert_eq!(bytes(&short), [0, 0, 0, 0xbb, 0, 0, 0, 0, 7]);

    let mut padded = BufQueue::new(20);
    assert_eq!(write_padding(&mut padded, 0, 20), Ok(20));
    let parsed = parsed(padded);
    assert_eq!(parsed.header.length, 11);
    assert!(matches!(parsed.frame, Frame::Padding(body) if bytes(&body) == vec![0; 11]));

    assert!(is_valid_frame_type(FrameType::ALTSVC));
    assert!(is_valid_frame_type(FrameType::CERTIFICATE));
    assert!(!is_valid_frame_type(FrameType::CERTIFICATE_NEEDED));
    assert_eq!(get_frame_type_string(FrameType::from_u8(0xfe)), "Unknown");
    assert!(frame_affects_compression(FrameType::HEADERS));
    assert_eq!(calculate_pre_header_block_size(true, true, true), 14);
    assert_eq!(calculate_pre_header_block_size(false, true, true), 15);
}

#[test]
fn malformed_peer_input_never_panics() {
    for length in 0..FRAME_HEADER_SIZE {
        let data = vec![0xff; length];
        let input = queue(&[&data]);
        assert!(catch_unwind(|| parse_frame_header(&input)).is_ok());
    }

    let malformed = [
        manual_frame(0, FrameType::DATA, Flags::PADDED, 1, &[]),
        manual_frame(1, FrameType::DATA, Flags::PADDED, 1, &[&[255]]),
        manual_frame(3, FrameType::PING, 0, 0, &[&[0; 3]]),
        manual_frame(2, FrameType::ALTSVC, 0, 0, &[&[0; 2]]),
        manual_frame(1, FrameType::CERTIFICATE, 0, 0, &[&[0]]),
    ];
    for mut input in malformed {
        assert!(catch_unwind(move || parse_frame(&mut input)).is_ok());
    }
}

#[test]
fn parse_helpers_reject_truncated_segmented_payloads_without_panicking() {
    let header = FrameHeader {
        length: 12,
        stream: 0,
        frame_type: FrameType::SETTINGS,
        flags: 0,
    };
    let input = queue(&[&[0, 1], &[0, 0, 0]]);
    assert_eq!(
        parse_settings(&input, &header),
        Err(FrameError::Incomplete {
            needed: 12,
            available: 5,
        })
    );

    let header = FrameHeader {
        length: 1,
        stream: 0,
        frame_type: FrameType::DATA,
        flags: 0,
    };
    let empty = BufQueue::new(0);
    assert_eq!(
        parse_data(&empty, &header).unwrap_err(),
        FrameError::Peer(H2ErrorCode::PROTOCOL_ERROR),
    );
    let mut wire = manual_frame(1, FrameType::DATA, 0, 0, &[]);
    let before = bytes(&wire);
    assert!(parse_frame(&mut wire).unwrap().is_none());
    assert_eq!(bytes(&wire), before);

    let padded = FrameHeader {
        length: 0,
        stream: 1,
        flags: Flags::PADDED,
        ..header
    };
    assert_eq!(
        parse_data_begin(&empty, &padded).unwrap_err(),
        FrameError::Incomplete {
            needed: 1,
            available: 0,
        },
    );
    assert_eq!(
        parse_data_begin(&queue(&[&[0]]), &padded).unwrap_err(),
        FrameError::Peer(H2ErrorCode::FRAME_SIZE_ERROR),
    );
}

#[test]
fn borrowed_frame_cursor_preserves_boundaries_and_padding() {
    let backing = Bytes::from_static(b"\x02\x80\0\0\x03\x10abcdef\0\0\xff");
    let mut input = BufQueue::new(backing.len());
    for range in [0..2, 2..7, 7..11, 11..13, 13..15] {
        input.push_bytes(backing.slice(range)).unwrap();
    }
    let header = FrameHeader {
        length: 14,
        stream: 1,
        frame_type: FrameType::HEADERS,
        flags: Flags::PADDED | Flags::PRIORITY | Flags::END_HEADERS,
    };
    let expected_pointers = [
        backing.slice(6..7).as_ptr(),
        backing.slice(7..11).as_ptr(),
        backing.slice(11..12).as_ptr(),
    ];
    let block = parse_headers(&input, &header).unwrap();
    assert_eq!(bytes(&block), b"abcdef");
    assert_eq!(
        block
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        expected_pointers,
    );
    assert_eq!(bytes(&input), backing.as_ref());

    let mut truncated = input.clone_shared();
    let prefix = truncated.split_at_most(13);
    assert_eq!(
        parse_headers(&prefix, &header).unwrap_err(),
        FrameError::Incomplete {
            needed: 14,
            available: 13,
        },
    );
    let mut bad_padding = backing.to_vec();
    bad_padding[13] = 1;
    assert_eq!(
        parse_headers(&queue(&[&bad_padding[..13], &bad_padding[13..]]), &header).unwrap_err(),
        FrameError::Peer(H2ErrorCode::PROTOCOL_ERROR),
    );

    let header_bytes = [
        0,
        0,
        14,
        FrameType::HEADERS.as_u8(),
        header.flags,
        0,
        0,
        0,
        1,
    ];
    let mut wire = BufQueue::new(FRAME_HEADER_SIZE + backing.len());
    for part in [
        &header_bytes[..1],
        &header_bytes[1..4],
        &header_bytes[4..8],
        &header_bytes[8..],
    ] {
        wire.push_copy(part).unwrap();
    }
    let mut payload = input.clone_shared();
    wire.append(&mut payload.split_at_most(13)).unwrap();
    let before = bytes(&wire);
    assert!(parse_frame(&mut wire).unwrap().is_none());
    assert_eq!(bytes(&wire), before);
    wire.append(&mut payload).unwrap();
    let parsed = parse_frame(&mut wire).unwrap().unwrap();
    assert_eq!(parsed.header, header);
    assert_eq!(parsed.consumed, FRAME_HEADER_SIZE + 14);
    let Frame::Headers(block) = parsed.frame else {
        panic!("expected HEADERS frame");
    };
    assert_eq!(bytes(&block), b"abcdef");
    assert_eq!(
        block
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        expected_pointers,
    );
    assert_eq!(bytes(&wire), &[0xff]);
    assert_eq!(wire.front().unwrap().as_ptr(), backing.slice(14..).as_ptr());
}

/// Splits `data` after byte `boundary` for every set bit `boundary - 1` of `mask`.
fn segmented(data: &[u8], mask: u64) -> Vec<&[u8]> {
    let mut parts = Vec::new();
    let mut start = 0;
    for boundary in 1..data.len() {
        if mask & (1 << (boundary - 1)) != 0 {
            parts.push(&data[start..boundary]);
            start = boundary;
        }
    }
    parts.push(&data[start..]);
    parts
}

fn frame_summary(parsed: &ParsedFrame) -> String {
    let frame = match &parsed.frame {
        Frame::Data(data) => format!("Data({:?}, {})", bytes(&data.data), data.padding),
        Frame::Headers(block) => format!("Headers({:?})", bytes(block)),
        Frame::Priority(body) => format!("Priority({:?})", bytes(body)),
        Frame::RstStream(code) => format!("RstStream({code:?})"),
        Frame::Settings(settings) => format!("Settings({settings:?})"),
        Frame::PushPromise {
            promised_stream,
            header_block,
        } => format!("PushPromise({promised_stream}, {:?})", bytes(header_block)),
        Frame::Ping(opaque) => format!("Ping({opaque:#x})"),
        Frame::Goaway {
            last_stream,
            error_code,
            debug_data,
        } => format!(
            "Goaway({last_stream}, {error_code:?}, {:?})",
            bytes(debug_data)
        ),
        Frame::WindowUpdate(amount) => format!("WindowUpdate({amount})"),
        Frame::Continuation(block) => format!("Continuation({:?})", bytes(block)),
        Frame::AltSvc(alt_svc) => format!("{alt_svc:?}"),
        Frame::Rfc9218Priority {
            prioritized_stream,
            priority,
        } => format!("Rfc9218Priority({prioritized_stream}, {priority:?})"),
        Frame::Padding(body) => format!("Padding({:?})", bytes(body)),
        Frame::CertificateRequest(request) => format!(
            "CertificateRequest({}, {:?})",
            request.request_id,
            bytes(&request.authenticator_request)
        ),
        Frame::Certificate(certificate) => format!(
            "Certificate({}, {:?})",
            certificate.certificate_id,
            bytes(&certificate.authenticator)
        ),
        Frame::Unknown(body) => format!("Unknown({:?})", bytes(body)),
    };
    format!("{:?} consumed={} {frame}", parsed.header, parsed.consumed)
}

fn cursor_rest(cursor: &mut SegmentCursor<'_>) -> Vec<u8> {
    let mut rest = Vec::new();
    while let Some(chunk) = cursor.next_chunk() {
        rest.extend_from_slice(chunk);
    }
    rest
}

#[test]
fn frame_header_parses_identically_across_every_segmentation() {
    let cases: [(u32, FrameType, u8, u32, &[u8]); 5] = [
        (
            MAX_FRAME_PAYLOAD_LENGTH,
            FrameType::DATA,
            Flags::END_STREAM,
            3,
            b"",
        ),
        (0x01_0203, FrameType::from_u8(0x7f), 0xa5, 0x8000_0021, b""),
        (
            8,
            FrameType::PING,
            Flags::ACK,
            0,
            b"\x01\x02\x03\x04\x05\x06\x07\x08",
        ),
        (
            4,
            FrameType::WINDOW_UPDATE,
            0,
            0xffff_ffff,
            b"\x80\x00\x00\x07",
        ),
        (3, FrameType::from_u8(0xfe), 0xff, 0x8000_0000, b"xyz"),
    ];
    for (length, frame_type, flags, stream, payload) in cases {
        let contiguous = manual_frame(length, frame_type, flags, stream, &[payload]);
        let wire_bytes = bytes(&contiguous);
        let expected_header = FrameHeader {
            length: length & MAX_FRAME_PAYLOAD_LENGTH,
            stream: stream & MAX_STREAM_ID,
            frame_type,
            flags,
        };
        assert_eq!(parse_frame_header(&contiguous), Ok(expected_header));
        let expected_frame = parse_frame(&mut contiguous.clone_shared())
            .unwrap()
            .map(|parsed| frame_summary(&parsed));
        assert_eq!(expected_frame.is_some(), payload.len() == length as usize);
        for mask in 0..(1 << (FRAME_HEADER_SIZE - 1)) {
            let mut glued = segmented(&wire_bytes[..FRAME_HEADER_SIZE], mask);
            let mut separate = glued.clone();
            separate.push(&wire_bytes[FRAME_HEADER_SIZE..]);
            let last = glued.pop().expect("a split header has a last segment");
            glued.push(&wire_bytes[FRAME_HEADER_SIZE - last.len()..]);
            for parts in [separate, glued] {
                let wire = queue(&parts);
                assert_eq!(bytes(&wire), wire_bytes);
                assert_eq!(
                    parse_frame_header(&wire),
                    Ok(expected_header),
                    "split mask {mask:#x}"
                );

                let mut cursor = SegmentCursor::from_queue(&wire);
                assert_eq!(parse_frame_header_cursor(&mut cursor), Ok(expected_header));
                assert_eq!(cursor.remaining(), wire_bytes.len() - FRAME_HEADER_SIZE);
                assert_eq!(cursor_rest(&mut cursor), &wire_bytes[FRAME_HEADER_SIZE..]);

                let mut consumed = wire.clone_shared();
                let actual = parse_frame(&mut consumed)
                    .unwrap()
                    .map(|parsed| frame_summary(&parsed));
                assert_eq!(actual, expected_frame, "split mask {mask:#x}");
                let left = if expected_frame.is_some() {
                    Vec::new()
                } else {
                    wire_bytes.clone()
                };
                assert_eq!(bytes(&consumed), left);
            }
        }
    }

    let first = FrameHeader {
        length: 0,
        stream: 0,
        frame_type: FrameType::SETTINGS,
        flags: Flags::ACK,
    };
    let second = FrameHeader {
        length: 0x00_4000,
        stream: 1,
        frame_type: FrameType::HEADERS,
        flags: Flags::END_HEADERS | Flags::END_STREAM,
    };
    let mut back_to_back = bytes(&manual_frame(0, FrameType::SETTINGS, Flags::ACK, 0, &[]));
    back_to_back.extend_from_slice(&bytes(&manual_frame(
        0x00_4000,
        FrameType::HEADERS,
        Flags::END_HEADERS | Flags::END_STREAM,
        0x8000_0001,
        &[b"xyz"],
    )));
    let total = back_to_back.len();
    let mut splits = vec![Vec::new(), (1..total).collect::<Vec<_>>()];
    for left in 1..total {
        splits.push(vec![left]);
        for right in left + 1..total {
            splits.push(vec![left, right]);
        }
    }
    for boundaries in splits {
        let mut parts = Vec::new();
        let mut start = 0;
        for boundary in &boundaries {
            parts.push(&back_to_back[start..*boundary]);
            start = *boundary;
        }
        parts.push(&back_to_back[start..]);
        let wire = queue(&parts);

        let mut cursor = SegmentCursor::from_queue(&wire);
        assert_eq!(parse_frame_header_cursor(&mut cursor), Ok(first));
        assert_eq!(cursor.remaining(), total - FRAME_HEADER_SIZE);
        assert_eq!(parse_frame_header_cursor(&mut cursor), Ok(second));
        assert_eq!(cursor.remaining(), 3);
        assert_eq!(cursor_rest(&mut cursor), b"xyz");

        let mut parent = SegmentCursor::from_queue(&wire);
        let mut bounded = parent.take(2 * FRAME_HEADER_SIZE).unwrap();
        assert_eq!(parse_frame_header_cursor(&mut bounded), Ok(first));
        assert_eq!(parse_frame_header_cursor(&mut bounded), Ok(second));
        assert_eq!(bounded.remaining(), 0);
        assert_eq!(
            parse_frame_header_cursor(&mut bounded),
            Err(FrameError::Incomplete {
                needed: FRAME_HEADER_SIZE,
                available: 0,
            })
        );
        assert_eq!(cursor_rest(&mut parent), b"xyz");
    }
}

#[test]
fn truncated_frame_header_reports_exact_incomplete() {
    let wire_bytes = bytes(&manual_frame(
        5,
        FrameType::HEADERS,
        Flags::END_HEADERS,
        1,
        &[b"abcde"],
    ));
    for available in 0..FRAME_HEADER_SIZE {
        let expected = Err(FrameError::Incomplete {
            needed: FRAME_HEADER_SIZE,
            available,
        });
        let prefix = &wire_bytes[..available];
        for mask in 0..(1 << available.saturating_sub(1)) {
            let wire = queue(&segmented(prefix, mask));
            assert_eq!(parse_frame_header(&wire), expected, "split mask {mask:#x}");
            let mut cursor = SegmentCursor::from_queue(&wire);
            assert_eq!(parse_frame_header_cursor(&mut cursor), expected);
            assert_eq!(cursor.remaining(), available);
            assert_eq!(cursor_rest(&mut cursor), prefix);
            let mut unconsumed = wire.clone_shared();
            assert!(parse_frame(&mut unconsumed).unwrap().is_none());
            assert_eq!(bytes(&unconsumed), prefix);
        }

        for split in 0..=wire_bytes.len() {
            let wire = queue(&[&wire_bytes[..split], &wire_bytes[split..]]);
            let mut parent = SegmentCursor::from_queue(&wire);
            let mut bounded = parent.take(available).unwrap();
            assert_eq!(parse_frame_header_cursor(&mut bounded), expected);
            assert_eq!(bounded.remaining(), available);
            assert_eq!(cursor_rest(&mut bounded), prefix);
            assert_eq!(parent.remaining(), wire_bytes.len() - available);
            assert_eq!(cursor_rest(&mut parent), &wire_bytes[available..]);
        }
    }

    for split in 0..=wire_bytes.len() {
        let wire = queue(&[&wire_bytes[..split], &wire_bytes[split..]]);
        let mut cursor = SegmentCursor::from_queue(&wire);
        assert!(parse_frame_header_cursor(&mut cursor).is_ok());
        assert_eq!(
            parse_frame_header_cursor(&mut cursor),
            Err(FrameError::Incomplete {
                needed: FRAME_HEADER_SIZE,
                available: 5,
            })
        );
        assert_eq!(cursor_rest(&mut cursor), b"abcde");
    }
}

#[test]
fn fixed_width_fields_parse_across_segment_boundaries() {
    fn written(write: impl FnOnce(&mut BufQueue) -> Result<usize, FrameError>) -> Vec<u8> {
        let mut wire = BufQueue::new(256);
        write(&mut wire).expect("fixture frame is valid");
        bytes(&wire)
    }
    let wires = [
        written(|wire| write_window_update(wire, 33, 0x7fff_ffff)),
        bytes(&manual_frame(
            4,
            FrameType::WINDOW_UPDATE,
            0,
            0,
            &[&[0x80, 0, 1, 0]],
        )),
        written(|wire| write_rst_stream(wire, 0x7fff_ffff, H2ErrorCode::CANCEL)),
        written(|wire| write_ping(wire, 0x0102_0304_0506_0708, true)),
        written(|wire| {
            write_goaway(
                wire,
                0x7fff_fffe,
                H2ErrorCode::FLOW_CONTROL_ERROR,
                Some(queue(&[b"debug"])),
            )
        }),
        bytes(&manual_frame(
            9,
            FrameType::GOAWAY,
            0,
            0,
            &[&[0x80, 0, 0, 5, 0, 0, 0, 1, b'x']],
        )),
        bytes(&manual_frame(
            5,
            FrameType::PRIORITY,
            0,
            3,
            &[&[0x80, 0, 0, 1, 15]],
        )),
        written(|wire| {
            write_settings(
                wire,
                &[
                    Setting {
                        identifier: 1,
                        value: 0x0102_0304,
                    },
                    Setting {
                        identifier: 31_337,
                        value: 0xffff_ffff,
                    },
                ],
            )
        }),
        written(|wire| write_rfc9218_priority(wire, 5, b"u=3,i")),
        written(|wire| write_alt_svc(wire, 2, 150, 8080, b"h2", b"host", b"origin")),
        written(|wire| write_push_promise(wire, 21, 22, queue(&[b"block"]), Some(3), true)),
        written(|wire| write_headers(wire, queue(&[b"block"]), 1, Some(2), true, true)),
        written(|wire| write_data(wire, queue(&[b"payload"]), 1, Some(4), true, true)),
        written(|wire| write_certificate_request(wire, 120, Some(queue(&[b"req"])))),
    ];
    for wire_bytes in wires {
        let expected = frame_summary(&parsed(queue(&[&wire_bytes])));
        let mut layouts = vec![wire_bytes.chunks(1).collect::<Vec<_>>()];
        for split in 1..wire_bytes.len() {
            layouts.push(vec![&wire_bytes[..split], &wire_bytes[split..]]);
        }
        for parts in layouts {
            assert_eq!(frame_summary(&parsed(queue(&parts))), expected);
        }
    }
}
