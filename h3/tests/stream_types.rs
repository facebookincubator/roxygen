/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::BytesMut;

use crate::codec::DecodeError;
use crate::codec::EncodeError;
use crate::codec::H3ErrorCode;
use crate::codec::Role;
use crate::codec::StreamType;
use crate::codec::UniStreamHeader;
use crate::codec::UniStreamHeaderDecoder;
use crate::codec::VARINT_MAX;

#[test]
fn unidirectional_stream_headers_round_trip() {
    let cases: [(UniStreamHeader, &[u8]); 7] = [
        (UniStreamHeader::Control, b"\x00"),
        (UniStreamHeader::Push { push_id: 5 }, b"\x01\x05"),
        (
            UniStreamHeader::Push {
                push_id: VARINT_MAX,
            },
            b"\x01\xff\xff\xff\xff\xff\xff\xff\xff",
        ),
        (UniStreamHeader::QpackEncoder, b"\x02"),
        (UniStreamHeader::QpackDecoder, b"\x03"),
        (UniStreamHeader::Unknown(StreamType::grease(0)), b"\x21"),
        // WebTransport's stream type, unknown here.
        (
            UniStreamHeader::Unknown(StreamType::new(0x54).unwrap()),
            b"\x40\x54",
        ),
    ];
    assert!(StreamType::grease(0).is_grease());
    for (header, wire) in cases {
        let mut buf = BytesMut::new();
        header.encode(&mut buf).unwrap();
        assert_eq!(&buf[..], wire, "{header:?}");

        // The stream's first frame follows the header and stays in the input.
        buf.extend_from_slice(b"\x04\x00");
        let mut input = buf.freeze();
        let mut decoder = UniStreamHeaderDecoder::new(Role::Client);
        assert_eq!(decoder.decode(&mut input), Ok(Some(header)));
        assert_eq!(&input[..], b"\x04\x00", "{header:?}");
        assert_eq!(decoder.decode(&mut input), Ok(None));
        assert_eq!(input.len(), 2, "a finished decoder consumes nothing");
    }
}

#[test]
fn stream_headers_decode_when_split_at_every_byte() {
    let mut wire = BytesMut::new();
    let header = UniStreamHeader::Push { push_id: 1 << 40 };
    header.encode(&mut wire).unwrap();
    let header_len = wire.len();
    wire.extend_from_slice(b"\x01\x00");
    let wire = wire.freeze();
    for cut in 0..=wire.len() {
        let mut decoder = UniStreamHeaderDecoder::new(Role::Client);
        let mut first = wire.slice(..cut);
        let mut second = wire.slice(cut..);
        let mut decoded = decoder.decode(&mut first).unwrap();
        if decoded.is_none() {
            assert!(first.is_empty(), "cut at {cut}");
            decoded = decoder.decode(&mut second).unwrap();
        }
        assert_eq!(decoded, Some(header), "cut at {cut}");
        assert_eq!(
            first.len() + second.len(),
            wire.len() - header_len,
            "cut at {cut}"
        );
    }
    let mut decoder = UniStreamHeaderDecoder::new(Role::Client);
    let mut decoded = None;
    for byte in &wire[..header_len] {
        assert_eq!(decoded, None);
        decoded = decoder.decode(&mut &[*byte][..]).unwrap();
    }
    assert_eq!(decoded, Some(header));
}

#[test]
fn a_server_rejects_push_streams() {
    // The type alone is enough; the push ID never arrives.
    let mut decoder = UniStreamHeaderDecoder::new(Role::Server);
    let error = decoder.decode(&mut &b"\x01"[..]).unwrap_err();
    assert_eq!(error, DecodeError::UnexpectedStream(StreamType::PUSH));
    assert_eq!(error.code(), H3ErrorCode::STREAM_CREATION_ERROR);
    assert_eq!(
        decoder.decode(&mut &b"\x05"[..]),
        Err(error),
        "the error is kept"
    );

    for (wire, header) in [
        (&b"\x00"[..], UniStreamHeader::Control),
        (b"\x02", UniStreamHeader::QpackEncoder),
        (b"\x03", UniStreamHeader::QpackDecoder),
        (b"\x21", UniStreamHeader::Unknown(StreamType::grease(0))),
    ] {
        let mut decoder = UniStreamHeaderDecoder::new(Role::Server);
        assert_eq!(decoder.decode(&mut &wire[..]), Ok(Some(header)));
    }
}

#[test]
fn stream_headers_that_would_not_decode_as_given_are_not_encoded() {
    let cases = [
        (
            UniStreamHeader::Push {
                push_id: VARINT_MAX + 1,
            },
            EncodeError::ValueTooLarge,
        ),
        (
            UniStreamHeader::Unknown(StreamType::CONTROL),
            EncodeError::KnownStreamType(StreamType::CONTROL),
        ),
        (
            UniStreamHeader::Unknown(StreamType::PUSH),
            EncodeError::KnownStreamType(StreamType::PUSH),
        ),
        (
            UniStreamHeader::Unknown(StreamType::QPACK_DECODER),
            EncodeError::KnownStreamType(StreamType::QPACK_DECODER),
        ),
    ];
    for (header, error) in cases {
        let mut buf = BytesMut::new();
        assert_eq!(header.encode(&mut buf), Err(error), "{header:?}");
        assert!(buf.is_empty(), "{header:?}");
    }
}
