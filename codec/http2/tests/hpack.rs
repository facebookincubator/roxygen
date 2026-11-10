use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaderName;
use roxygen_core::http::HttpHeaderSize;

use crate::hpack::*;

/// 64-bit FNV-1a, the digest that wire-identity golden tests pin.
pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn headers(values: &[(&str, &str)]) -> Vec<HpackHeader> {
    values
        .iter()
        .map(|(name, value)| HpackHeader::new(name, value))
        .collect()
}

fn borrowed_headers(values: &[HpackHeader]) -> Vec<Header<'_>> {
    values
        .iter()
        .map(|header| Header::new(HttpHeaderName::Name(header.name.as_bytes()), &header.value))
        .collect()
}

fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |byte| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => panic!("invalid test hex"),
            };
            (digit(pair[0]) << 4) | digit(pair[1])
        })
        .collect()
}

mod hpack_buffer {
    use super::*;

    #[test]
    fn borrowed_decode_input_constructor_and_clone_semantics() {
        struct IteratorDrop<'a>(&'a Cell<bool>);

        impl Drop for IteratorDrop<'_> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let wire = [67, 31, 154, 10, 0xff];
        let parts = [&[][..], &wire[..2], &[], &wire[2..3], &wire[3..], &[]];
        let visited = Cell::new(0);
        let dropped = Cell::new(false);
        let drop_guard = IteratorDrop(&dropped);
        let visits = &visited;
        let collected = HpackDecodeBuffer::from_segments(
            parts.into_iter().inspect(move |_| {
                let _ = &drop_guard;
                visits.set(visits.get() + 1);
            }),
            4,
            64,
        );
        assert_eq!(visited.get(), parts.len());
        assert!(dropped.get());

        let mut queue = BufQueue::new(wire.len());
        for part in parts {
            queue.push_bytes(Bytes::copy_from_slice(part)).unwrap();
        }
        for mut input in [
            HpackDecodeBuffer::new(&wire[..4], 64),
            HpackDecodeBuffer::from_buf_queue(&queue, 4, 64),
            collected,
        ] {
            assert_eq!(input.decode_integer(8), Ok(67));
            let mut cloned = input.clone();
            assert_eq!(input.decode_integer(5), Ok(1337));
            assert_eq!(input.consumed_bytes(), 4);
            assert!(input.is_empty());
            assert_eq!(cloned.consumed_bytes(), 1);
            assert_eq!(cloned.remaining_bytes(), 3);
            assert_eq!(cloned.peek(), Ok(31));
            assert_eq!(cloned.decode_integer(5), Ok(1337));
            assert_eq!(cloned.peek(), Err(HpackError::BufferUnderflow));
            assert_eq!(input.peek(), Err(HpackError::BufferUnderflow));
        }

        let empty = BufQueue::new(0);
        for mut input in [
            HpackDecodeBuffer::new(&[], 64),
            HpackDecodeBuffer::from_buf_queue(&empty, 0, 64),
            HpackDecodeBuffer::from_segments([&[][..], &[]], 0, 64),
        ] {
            assert!(input.is_empty());
            assert_eq!(input.peek(), Err(HpackError::BufferUnderflow));
            assert_eq!(
                input.clone().decode_literal(),
                Err(HpackError::BufferUnderflow),
            );
            assert_eq!(input.consumed_bytes(), 0);
        }
    }

    #[test]
    fn borrowed_literal_input_preserves_consumption_and_errors() {
        for (literal, total, maximum, expected, consumed) in [
            (b"\x04gzip".as_slice(), 5, 4, Ok(b"gzip".as_slice()), 5),
            (
                b"\x83\x9b\xd9\xab".as_slice(),
                4,
                4,
                Ok(b"gzip".as_slice()),
                4,
            ),
            (b"\x00\x2a".as_slice(), 1, 0, Ok(b"".as_slice()), 1),
            (b"\x80\x2a".as_slice(), 1, 0, Ok(b"".as_slice()), 1),
            (
                b"\x82\xf8\xff\x2a".as_slice(),
                4,
                4,
                Err(HpackError::InvalidHuffmanCode),
                3,
            ),
            (
                b"\x04gzip".as_slice(),
                3,
                2,
                Err(HpackError::BufferUnderflow),
                1,
            ),
            (
                b"\x04gz".as_slice(),
                5,
                4,
                Err(HpackError::BufferUnderflow),
                3,
            ),
            (
                b"\x04gzip".as_slice(),
                5,
                3,
                Err(HpackError::LiteralTooLarge),
                1,
            ),
        ] {
            let mut wire = vec![67];
            wire.extend_from_slice(literal);
            let contiguous = BufQueue::copy_from_slice(&wire, wire.len()).unwrap();
            let mut fragmented = BufQueue::new(wire.len());
            for byte in &wire {
                fragmented.push_copy(&[*byte]).unwrap();
            }
            for queue in [&contiguous, &fragmented] {
                let mut input = HpackDecodeBuffer::from_buf_queue(queue, total + 1, maximum);
                assert_eq!(input.decode_integer(8), Ok(67));
                assert_eq!(input.decode_literal(), expected.map(<[u8]>::to_vec));
                assert_eq!(input.consumed_bytes(), consumed + 1);
                assert_eq!(input.remaining_bytes(), total - consumed);
                let mut cloned = input.clone();
                let next = input.decode_integer(8);
                assert_eq!(cloned.consumed_bytes(), consumed + 1);
                assert_eq!(cloned.decode_integer(8), next);
                assert_eq!(cloned.consumed_bytes(), input.consumed_bytes());
            }
        }
    }

    #[test]
    fn encode_integer() {
        let mut buffer = HpackEncodeBuffer::new(false);
        assert_eq!(buffer.encode_integer_parts(7, 192, 6), 1);
        assert_eq!(buffer.encode_integer_parts(7, 192, 3), 2);
        assert_eq!(buffer.encode_integer_parts(255, 0, 8), 2);
        assert_eq!(buffer.as_slice(), &[199, 199, 0, 255, 0]);

        let mut buffer = HpackEncodeBuffer::new(false);
        assert_eq!(buffer.encode_integer_parts(7, 192, 2), 2);
        assert_eq!(buffer.as_slice(), &[195, 4]);

        let mut buffer = HpackEncodeBuffer::new(false);
        assert_eq!(buffer.encode_integer_parts(1337, 0, 5), 3);
        assert_eq!(buffer.as_slice(), &[31, 154, 10]);
    }

    #[test]
    fn encode_plain_literal() {
        let mut buffer = HpackEncodeBuffer::new(false);
        assert_eq!(buffer.encode_literal(b"accept-encoding"), 16);
        assert_eq!(buffer.as_slice(), b"\x0faccept-encoding");
    }

    #[test]
    fn encode_plain_literal_n() {
        let mut buffer = HpackEncodeBuffer::new(false);
        assert_eq!(buffer.encode_literal_parts(0xe0, 4, b"accept-encodin"), 15);
        assert_eq!(buffer.as_slice()[0], 0xee);
        assert_eq!(&buffer.as_slice()[1..], b"accept-encodin");
    }

    #[test]
    fn encode_huffman_literal() {
        let mut buffer = HpackEncodeBuffer::new(true);
        assert_eq!(buffer.encode_literal(b"accept-encoding"), 12);
        assert_eq!(buffer.as_slice()[0], 0x8b);
        assert_eq!(buffer.as_slice()[11], 0x7f);
    }

    #[test]
    fn encode_huffman_literal_n() {
        let mut buffer = HpackEncodeBuffer::new(true);
        assert_eq!(buffer.encode_literal_parts(0xe0, 4, b"accept-encoding"), 12);
        assert_eq!(buffer.as_slice()[0], 0xfb);
        assert_eq!(buffer.as_slice()[11], 0x7f);

        let mut buffer = HpackEncodeBuffer::with_headroom(true, 3);
        assert_eq!(buffer.encode_integer_parts(1337, 0, 5), 3);
        assert_eq!(buffer.encode_literal_parts(0xe0, 4, b""), 1);
        assert_eq!(buffer.encode_literal_parts(0xe0, 4, b"gzip"), 4);
        assert_eq!(buffer.as_slice(), b"\x1f\x9a\x0a\xf0\xf3\x9b\xd9\xab");
        let mut encoded = buffer.release();
        assert_eq!(encoded.headroom(), 3);
        assert!(encoded.prepend(b"pre"));
        assert_eq!(encoded.into_bytes(), b"pre\x1f\x9a\x0a\xf0\xf3\x9b\xd9\xab");
    }

    #[test]
    fn huffman_literals_grow_the_block_like_one_reserve() {
        // Bytes 0-7 carry 13- to 28-bit codes, so even a short literal can need two doublings.
        let mut state = 0x5EED_B10C_u64;
        for _ in 0..2_000 {
            let mut buffer = HpackEncodeBuffer::new(true);
            let mut model = Vec::new();
            let count = 1 + state % 12;
            for _ in 0..count {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let literal: Vec<u8> = (0..1 + state % 30)
                    .map(|i| ((state >> (2 * i)) as u8) % 8)
                    .collect();
                let size = huffman_encoded_size(&literal);
                buffer.encode_literal(&literal);
                model.push(0x80 | u8::try_from(size).expect("one-byte literal length"));
                model.reserve(size);
                model.extend_from_slice(&encode_huffman(&literal));
            }
            let bytes = buffer.into_bytes();
            assert_eq!(bytes, model);
            assert_eq!(bytes.capacity(), model.capacity(), "block {model:02x?}");
        }
    }

    #[test]
    fn decode_single_byte() {
        let mut decoder = HpackDecodeBuffer::new(&[67], 1 << 17);
        assert_eq!(decoder.decode_integer(7), Ok(67));
        let mut decoder = HpackDecodeBuffer::new(&[67], 1 << 17);
        assert_eq!(decoder.decode_integer(6), Ok(3));
        let mut decoder = HpackDecodeBuffer::new(&[195], 1 << 17);
        assert_eq!(decoder.decode_integer(7), Ok(67));
        let mut decoder = HpackDecodeBuffer::new(&[195], 1 << 17);
        assert_eq!(decoder.decode_integer(8), Ok(195));
    }

    #[test]
    fn decode_multi_byte() {
        for (bytes, prefix, expected) in [
            (&[67, 0][..], 2, 3),
            (&[3, 127][..], 2, 130),
            (&[3, 128, 1][..], 2, 131),
            (&[31, 154, 10][..], 5, 1337),
        ] {
            let mut decoder = HpackDecodeBuffer::new(bytes, 1 << 17);
            assert_eq!(decoder.decode_integer(prefix), Ok(expected));
            assert!(decoder.is_empty());
        }
    }

    #[test]
    fn decode_integer_error() {
        let mut decoder = HpackDecodeBuffer::new(&[], 1 << 17);
        assert_eq!(decoder.decode_integer(5), Err(HpackError::BufferUnderflow));
        let mut decoder = HpackDecodeBuffer::new(&[31, 154], 1 << 17);
        assert_eq!(decoder.decode_integer(5), Err(HpackError::BufferUnderflow));
    }

    #[test]
    fn decode_literal_error() {
        let mut decoder = HpackDecodeBuffer::new(&[255, b'a', b'b'], 1 << 17);
        assert_eq!(decoder.decode_literal(), Err(HpackError::BufferUnderflow));
        let mut decoder = HpackDecodeBuffer::new(&[0xff, 0x80, 0x80], 1 << 17);
        assert_eq!(decoder.decode_literal(), Err(HpackError::BufferUnderflow));
    }

    #[test]
    fn decode_literal_multi_buffer() {
        let mut literal = vec![b'x'; 126];
        literal.extend([b'y'; 120]);
        let mut encoded = HpackEncodeBuffer::new(false);
        encoded.encode_literal(&literal);
        let split = 128;
        let segments = [&encoded.as_slice()[..split], &encoded.as_slice()[split..]];
        let mut decoder =
            HpackDecodeBuffer::from_segments(segments, encoded.as_slice().len(), 1 << 17);
        assert_eq!(decoder.decode_literal(), Ok(literal));
    }

    #[test]
    fn decode_huffman_literal_multi_buffer() {
        let literal = b"gzip".repeat(80);
        let mut encoded = HpackEncodeBuffer::new(true);
        encoded.encode_literal(&literal);
        let segments = [&encoded.as_slice()[..128], &encoded.as_slice()[128..]];
        let mut decoder =
            HpackDecodeBuffer::from_segments(segments, encoded.as_slice().len(), 1 << 17);
        assert_eq!(decoder.decode_literal(), Ok(literal));
    }

    #[test]
    fn decode_huffman_literal_n() {
        let mut encoded = HpackEncodeBuffer::new(true);
        encoded.encode_literal_parts(0x80, 4, b"gzip");
        assert_eq!(encoded.as_slice(), &[0x93, 0x9b, 0xd9, 0xab]);
        let mut decoder = HpackDecodeBuffer::new(encoded.as_slice(), 1 << 17);
        assert_eq!(decoder.decode_literal_with_prefix(4), Ok(b"gzip".to_vec()));
    }

    #[test]
    fn decode_plain_literal() {
        let mut decoder = HpackDecodeBuffer::new(b"\x04gzip", 1 << 17);
        assert_eq!(decoder.decode_literal(), Ok(b"gzip".to_vec()));
    }

    #[test]
    fn decode_plain_literal_n() {
        let mut decoder = HpackDecodeBuffer::new(b"\xe4gzip", 1 << 17);
        assert_eq!(decoder.decode_literal_with_prefix(4), Ok(b"gzip".to_vec()));
    }

    #[test]
    fn integer_encode_decode() {
        for (value, prefix, instruction) in [(145333, 5, 128), (63, 6, 64)] {
            let mut encoded = HpackEncodeBuffer::new(false);
            encoded.encode_integer_parts(value, instruction, prefix);
            let mut decoded = HpackDecodeBuffer::new(encoded.as_slice(), 1 << 17);
            assert_eq!(decoded.decode_integer(prefix), Ok(value));
            assert!(decoded.is_empty());
        }
    }

    #[test]
    fn integer_overflow() {
        let mut accumulated = vec![0xff; 10];
        accumulated.push(0x0f);
        let mut decoder = HpackDecodeBuffer::new(&accumulated, 1 << 17);
        assert_eq!(decoder.decode_integer(8), Err(HpackError::IntegerOverflow));

        let mut factor = vec![0xff];
        factor.extend([0x80; 10]);
        factor.push(1);
        let mut decoder = HpackDecodeBuffer::new(&factor, 1 << 17);
        assert_eq!(decoder.decode_integer(8), Err(HpackError::IntegerOverflow));
    }

    #[test]
    fn integer_max() {
        let maximum = u64::MAX - 1;
        for prefix in 1..=8 {
            let mut encoded = HpackEncodeBuffer::new(false);
            encoded.encode_integer_parts(maximum, 0, prefix);
            let mut decoded = HpackDecodeBuffer::new(encoded.as_slice(), 1 << 17);
            assert_eq!(decoded.decode_integer(prefix), Ok(maximum));
        }
    }

    #[test]
    fn empty_buffer_literal() {
        let mut queue = BufQueue::new(128);
        queue.push_bytes(Bytes::new()).expect("empty segment fits");
        queue
            .push_bytes(Bytes::from_static(b"\x04gzip"))
            .expect("literal fits");
        let mut decoder = HpackDecodeBuffer::from_buf_queue(&queue, 5, 1 << 17);
        assert_eq!(decoder.decode_literal(), Ok(b"gzip".to_vec()));
    }

    #[test]
    fn large_literal_error() {
        let literal = vec![b'x'; (1 << 17) + 10];
        let mut encoded = HpackEncodeBuffer::new(false);
        encoded.encode_literal(&literal);
        let mut decoder = HpackDecodeBuffer::new(encoded.as_slice(), 1 << 17);
        assert_eq!(decoder.decode_literal(), Err(HpackError::LiteralTooLarge));
    }
}

mod hpack_header {
    use super::*;

    #[test]
    fn size() {
        assert_eq!(HpackHeader::new(":path", "/").bytes(), 38);
    }

    #[test]
    fn operators() {
        let path = HpackHeader::new(":path", "/");
        assert_eq!(path, path.clone());
        let index = HpackHeader::new(":path", "/index.php");
        let debug = HpackHeader::new("x-fb-debug", "test");
        assert_ne!(path, index);
        assert_eq!(path.cmp(&path), std::cmp::Ordering::Equal);
        assert!(path < index);
        assert!(path < debug);
        assert_eq!(index.cmp(&index), std::cmp::Ordering::Equal);
        assert!(debug > index);
        assert!(index > path);
        assert_eq!(path.to_string(), ":path: /");
    }

    #[test]
    fn has_value() {
        assert!(!HpackHeader::new(":path", "").has_value());
        assert!(HpackHeader::new(":path", "/").has_value());
    }

    #[test]
    fn header_indexing_strategy_basic() {
        let strategy = DefaultIndexingStrategy;
        assert!(!strategy.index_header(&HeaderName::new(":path"), b"index.php?q=42", false));
        assert!(!strategy.index_header(&HeaderName::new(":path"), b"/a.jpg", false));
        assert!(!strategy.index_header(&HeaderName::new("content-length"), b"512", false));
        assert!(strategy.index_header(&HeaderName::new("data"), b"value", false));

        let no_path = NoPathIndexingStrategy;
        assert!(!no_path.index_header(&HeaderName::new(":path"), b"/", false));
        assert!(no_path.index_header(&HeaderName::new("data"), b"value", false));
    }
}

mod hpack_header_name {
    use super::*;

    #[test]
    fn constructor() {
        assert_eq!(
            HeaderName::new("Accept-Encoding").as_bytes(),
            b"accept-encoding"
        );
        assert_eq!(
            HeaderName::new("UnCommon-Name").as_bytes(),
            b"uncommon-name"
        );
    }

    #[test]
    fn copy_constructor() {
        let original = HeaderName::new("uncommon-name");
        assert_eq!(original.clone(), original);
    }

    #[test]
    fn move_constructor() {
        let original = HeaderName::new("accept-encoding");
        let moved = original;
        assert_eq!(moved.as_bytes(), b"accept-encoding");
    }

    #[test]
    fn assignment_operators() {
        let mut name = HeaderName::new("accept-encoding");
        assert_eq!(name.as_bytes(), b"accept-encoding");
        name = HeaderName::new("uncommon-name");
        assert_eq!(name.as_bytes(), b"uncommon-name");
        name = name.clone();
        assert_eq!(name.as_bytes(), b"uncommon-name");
    }

    #[test]
    fn get_size() {
        assert_eq!(HeaderName::new("accept-encoding").len(), 15);
        assert_eq!(HeaderName::new("uncommon-header_now").len(), 19);
    }

    #[test]
    fn operators() {
        let first = HeaderName::new("aaa");
        let second = HeaderName::new("bbb");
        assert!(first < second);
        assert!(second > first);
        assert!(first <= HeaderName::new("AAA"));
        assert!(second >= first);
        assert_eq!(first, HeaderName::new("AAA"));
    }

    #[test]
    fn is_common_header() {
        for code in HttpHeaderCode::ALL {
            let name = if code.as_u8() >= HttpHeaderCode::COMMON_OFFSET {
                HeaderName::from_code(*code)
            } else {
                HeaderName::new("")
            };
            assert_eq!(
                name.is_common_header(),
                code.as_u8() >= HttpHeaderCode::COMMON_OFFSET
            );
        }
        assert!(!HeaderName::new("externalHeader").is_common_header());
    }
}

mod header_piece {
    use super::*;

    #[test]
    fn basic_owned_borrowed() {
        let borrowed = HeaderPiece::borrowed(&[], true);
        assert!(!borrowed.is_owned());
        assert!(borrowed.is_multi_valued());
        let owned = HeaderPiece::owned(vec![0; 16], true);
        assert!(owned.is_owned());
        assert_eq!(owned.as_bytes().len(), 16);
    }
}

mod header_table {
    use super::*;

    #[test]
    fn lookup_preserves_newest_name_and_exact_value_after_changes() {
        use std::panic::catch_unwind;

        for name in ["accept-encoding", "x-lookup-ä"] {
            let query = HeaderName::new(name.to_ascii_uppercase());
            let empty = HpackHeader::new(name, "");
            let older = HpackHeader::new(name, "old");
            let newest = HpackHeader::new(name, "new");
            let unrelated = HpackHeader::new("x-other", "gap");
            let missing = HeaderName::new("x-missing");
            let mut table = HeaderTable::new(512);
            for header in [&empty, &unrelated, &older, &newest] {
                assert!(table.add(header.clone()));
            }
            assert_eq!(table.name_index(&query), 1);
            assert_eq!(table.get_index_parts(&query, b"new"), (1, 0));
            assert_eq!(table.get_index_parts(&query, b"old"), (2, 0));
            assert_eq!(table.get_index_parts(&query, b""), (4, 0));
            assert_eq!(table.get_index_parts(&query, b"missing"), (0, 1));
            assert_eq!(table.get_index(&unrelated), (3, 0));
            assert!(table.has_name(&query));
            assert!(!table.has_name(&missing));
            assert_eq!(table.name_index(&missing), 0);
            assert_eq!(table.get_index_parts(&missing, b"new"), (0, 0));

            let snapshot = table.clone();
            assert!(table.set_capacity(newest.bytes() + older.bytes()));
            assert_eq!(table.size(), 2);
            assert_eq!(table.get_index(&empty), (0, 1));
            assert_eq!(table.get_index(&unrelated), (0, 0));
            assert_eq!(table.name_index(&query), 1);
            assert!(table.add(newest.clone()));
            assert_eq!(table.get_index(&newest), (1, 0));
            assert_eq!(table.get_index(&older), (0, 1));
            assert_eq!(snapshot.get_index(&empty), (4, 0));
            assert_eq!(snapshot.get_index(&unrelated), (3, 0));
            assert_eq!(snapshot.name_index(&query), 1);

            assert!(table.set_capacity(0));
            assert_eq!(table.name_index(&query), 0);
            assert_eq!(table.get_index(&newest), (0, 0));
            assert!(!table.has_name(&query));
            assert!(table.set_capacity(512));
            assert!(table.add(older.clone()));
            assert_eq!(table.get_index_parts(&query, b"old"), (1, 0));
            assert_eq!(table.name_index(&query), 1);
            table.disable_names_index();
            assert!(catch_unwind(|| table.get_index(&older)).is_err());
            assert!(catch_unwind(|| table.name_index(&query)).is_err());
            assert!(catch_unwind(|| table.has_name(&query)).is_err());
            table.clear();
            assert!(catch_unwind(|| table.name_index(&query)).is_err());
        }
    }

    #[test]
    fn probe_counts_every_entry_a_lookup_reads() {
        let mut table = HeaderTable::new(4096);
        for ordinal in 0..10 {
            assert!(table.add(HpackHeader::new(format!("x-probe-{ordinal}"), "v")));
        }
        HeaderTable::take_examined();

        assert_eq!(table.get_index(&HpackHeader::new("x-missing", "v")), (0, 0));
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "a missing-name lookup reads no entry"
        );
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "taking the count resets it"
        );

        assert_eq!(table.name_index(&HeaderName::new("x-missing")), 0);
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "a missing name_index reads no entry"
        );

        assert!(!table.has_name(&HeaderName::new("x-missing")));
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "a missing has_name reads no entry"
        );

        assert_eq!(table.get_index(&HpackHeader::new("x-probe-7", "v")), (3, 0));
        assert_eq!(
            HeaderTable::take_examined(),
            1,
            "an exact match at index 3 reads only the one x-probe-7 entry"
        );
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "taking the count resets it"
        );
        assert_eq!(table.name_index(&HeaderName::new("x-probe-7")), 3);
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "a name_index hit reads no entry"
        );
        assert!(table.has_name(&HeaderName::new("x-probe-7")));
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "a has_name hit reads no entry"
        );
    }

    #[test]
    fn indexed_lookup_examines_only_same_name_entries() {
        let accept_encoding = HeaderName::new("accept-encoding");
        let mut table = HeaderTable::new(4096);
        assert!(table.add(HpackHeader::new("accept-encoding", "gzip")));
        for filler in 0..60 {
            assert!(table.add(HpackHeader::new(format!("x-filler-{filler:02}"), "v")));
        }
        assert_eq!(table.size(), 61, "the fixture fits without evicting");
        let same_name = table
            .entries()
            .filter(|header| header.name == accept_encoding)
            .count();
        HeaderTable::take_examined();

        assert_eq!(
            table.get_index(&HpackHeader::new("accept-encoding", "br")),
            (0, 61)
        );
        assert!(
            HeaderTable::take_examined() <= same_name,
            "a value miss reads only the accept-encoding entries"
        );
        assert_eq!(table.name_index(&accept_encoding), 61);
        assert_eq!(HeaderTable::take_examined(), 0, "name_index reads no entry");
        assert!(table.has_name(&accept_encoding));
        assert_eq!(HeaderTable::take_examined(), 0, "has_name reads no entry");

        let missing = HeaderName::new("x-missing");
        assert_eq!(table.get_index_parts(&missing, b"v"), (0, 0));
        assert_eq!(table.name_index(&missing), 0);
        assert!(!table.has_name(&missing));
        assert_eq!(
            HeaderTable::take_examined(),
            0,
            "lookups of a missing name read no entry"
        );
    }

    #[test]
    fn full_64k_table_lookup_examines_only_same_name_entries() {
        let target = HeaderName::new("accept-encoding");
        let mut table = HeaderTable::new(65536);
        assert!(table.add(HpackHeader::new("accept-encoding", "gzip")));
        for filler in 0..1000 {
            assert!(table.add(HpackHeader::new(format!("x-filler-{filler:04}"), "v")));
        }
        assert_eq!(table.size(), 1001, "the fixture fits without evicting");
        let same_name = table
            .entries()
            .filter(|header| header.name == target)
            .count();
        HeaderTable::take_examined();

        assert_eq!(table.get_index_parts(&target, b"br"), (0, 1001));
        assert!(
            HeaderTable::take_examined() <= same_name,
            "a target miss in a 64 KiB table reads only the target's entries"
        );
    }

    #[test]
    fn same_name_miss_examines_at_most_the_same_name_entries() {
        let request_id = HeaderName::new("x-request-id");
        let mut table = HeaderTable::new(4096);
        assert!(table.add(HpackHeader::new(":authority", "c1")));
        assert!(table.add(HpackHeader::new(":path", "/echo")));
        for id in 0..66_u64 {
            assert!(table.add(HpackHeader::new("x-request-id", format!("{id:016x}"))));
        }
        assert_eq!(table.size(), 68, "66 * 60 + 44 + 42 = 4046 bytes fit");
        HeaderTable::take_examined();

        assert_eq!(
            table.get_index_parts(&request_id, b"00000000000000ff"),
            (0, 1)
        );
        let examined = HeaderTable::take_examined();
        assert!(
            examined <= 66,
            "a new x-request-id value reads at most the 66 x-request-id entries, read {examined}"
        );
    }

    #[test]
    fn unique_value_miss_examines_at_most_one_entry() {
        let request_id = HeaderName::new("x-request-id");
        let mut table = HeaderTable::new(4096);
        assert!(table.add(HpackHeader::new(":authority", "c1")));
        assert!(table.add(HpackHeader::new(":path", "/echo")));
        for id in 0..66_u64 {
            assert!(table.add(HpackHeader::new("x-request-id", format!("{id:016x}"))));
        }
        assert_eq!(table.size(), 68, "66 * 60 + 44 + 42 = 4046 bytes fit");
        HeaderTable::take_examined();

        assert_eq!(
            table.get_index_parts(&request_id, b"00000000000000ff"),
            (0, 1),
            "a new x-request-id value names the newest x-request-id entry"
        );
        let examined = HeaderTable::take_examined();
        assert!(
            examined <= 1,
            "a new x-request-id value reads at most one entry, read {examined}"
        );

        for (id, index) in [(0_u64, 66_u32), (40, 26), (65, 1)] {
            let value = format!("{id:016x}");
            assert_eq!(
                table.get_index_parts(&request_id, value.as_bytes()),
                (index, 0),
                "x-request-id {value} is the {index}th newest entry (id 65 is the newest)"
            );
            let examined = HeaderTable::take_examined();
            assert!(
                examined <= 1,
                "an existing x-request-id value reads at most one entry, read {examined}"
            );
        }
    }

    #[test]
    fn many_values_under_one_name_track_newest_across_eviction_and_resize() {
        fn header(value: u32) -> HpackHeader {
            HpackHeader::new("x-many", format!("value-{value:02}"))
        }
        fn assert_indices_match_positions(table: &HeaderTable) {
            for position in 1..=table.size() {
                let stored = table.get_header(position).expect("position is live");
                assert_eq!(
                    table.get_index(stored),
                    (position, 0),
                    "lookup of {stored} returns its position"
                );
            }
        }

        let name = HeaderName::new("x-many");
        let entry_bytes = header(0).bytes();
        assert_eq!(entry_bytes, 46, "6 + 8 + 32 bytes per entry");
        let mut table = HeaderTable::new(entry_bytes * 8);
        for value in 0..20 {
            assert!(table.add(header(value)));
        }
        assert_eq!(table.size(), 8, "values 12..=19 are live");
        assert_eq!(table.get_index(&header(19)), (1, 0));
        assert_eq!(table.get_index(&header(12)), (8, 0));
        assert_eq!(table.name_index(&name), 1);
        assert_eq!(table.name_entry_count(&name), 8);
        assert_eq!(
            table.get_index(&header(11)),
            (0, 1),
            "an evicted value misses with the newest name index"
        );
        assert_eq!(table.get_index(&header(0)), (0, 1));

        assert!(table.add(header(15)), "re-adding 15 evicts 12");
        assert_eq!(table.get_header(6), Some(&header(15)));
        assert_eq!(
            table.get_index(&header(15)),
            (1, 0),
            "the newest exact match wins"
        );
        assert_eq!(table.get_index(&header(12)), (0, 1));
        assert_eq!(table.get_index(&header(13)), (8, 0));

        assert!(table.set_capacity(entry_bytes * 3));
        assert_eq!(table.size(), 3, "15, 19 and 18 stay");
        assert_eq!(table.get_index(&header(18)), (3, 0));
        assert_eq!(table.get_index(&header(17)), (0, 1));
        assert_eq!(table.name_entry_count(&name), 3);
        assert_indices_match_positions(&table);

        assert!(table.set_capacity(entry_bytes * 8));
        assert_indices_match_positions(&table);
        for value in 20..25 {
            assert!(table.add(header(value)));
        }
        assert_eq!(table.size(), 8);
        assert_eq!(table.name_index(&name), 1);
        assert_eq!(table.get_index(&header(18)), (8, 0));
        assert_indices_match_positions(&table);
    }

    #[test]
    fn names_index_is_not_maintained_after_disable() {
        use std::panic::catch_unwind;

        let header = accept();
        let mut table = HeaderTable::new(header.bytes() * 2);
        table.disable_names_index();
        for _ in 0..3 {
            assert!(table.add(header.clone()));
        }
        assert_eq!(table.size(), 2, "the third add evicts the oldest entry");
        assert_eq!(table.names_len(), 0);
        assert!(table.set_capacity(header.bytes()));
        assert_eq!(table.size(), 1, "shrinking evicts one more entry");
        assert_eq!(table.names_len(), 0);
        assert!(table.add(HpackHeader::new("x-other", "v")));
        assert_eq!(table.size(), 1, "an add that evicts the last entry");
        assert_eq!(table.names_len(), 0);

        let name = HeaderName::new("accept-encoding");
        let payloads = [
            catch_unwind(|| table.get_index(&header)).map(|_| ()),
            catch_unwind(|| table.get_index_parts(&name, b"gzip")).map(|_| ()),
            catch_unwind(|| table.name_index(&name)).map(|_| ()),
            catch_unwind(|| table.has_name(&name)).map(|_| ()),
        ];
        for payload in payloads {
            let payload = payload.expect_err("a lookup without a names index panics");
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"name lookup requires a names index")
            );
        }
    }

    fn accept() -> HpackHeader {
        HpackHeader::new("accept-encoding", "gzip")
    }

    #[test]
    fn index_translation() {
        assert_eq!(HeaderTable::to_external(5, 10, 0), 6);
        assert_eq!(HeaderTable::to_external(5, 10, 3), 3);
        assert_eq!(HeaderTable::to_external(5, 10, 5), 1);
        assert_eq!(HeaderTable::to_internal(5, 10, 1), 5);
        assert_eq!(HeaderTable::to_internal(5, 10, 6), 0);
        assert_eq!(HeaderTable::to_external(1, 10, 0), 2);
        assert_eq!(HeaderTable::to_internal(1, 10, 2), 0);
        assert_eq!(HeaderTable::to_external(1, 10, 8), 4);
        assert_eq!(HeaderTable::to_internal(1, 10, 4), 8);
        assert_eq!(HeaderTable::to_external(1, 10, 5), 7);
        assert_eq!(HeaderTable::to_internal(1, 10, 7), 5);
    }

    #[test]
    fn add() {
        let mut table = HeaderTable::new(4096);
        for _ in 0..3 {
            assert!(table.add(accept()));
        }
        assert_eq!(table.names_len(), 1);
        assert!(table.has_name(&HeaderName::new("accept-encoding")));
        assert_eq!(
            table.name_entry_count(&HeaderName::new("accept-encoding")),
            3
        );
        assert_eq!(table.name_index(&HeaderName::new("accept-encoding")), 1);
        assert_eq!(table.size(), 3);
    }

    #[test]
    fn evict() {
        let header = accept();
        let capacity = header.bytes() * 10;
        let mut table = HeaderTable::new(capacity);
        for _ in 0..10 {
            assert!(table.add(header.clone()));
        }
        assert_eq!(table.size(), 10);
        let replacement = HpackHeader::new("accept-encoding", "----");
        assert!(table.add(replacement.clone()));
        assert_eq!(table.get_header(1), Some(&replacement));
        assert_eq!(table.name_entry_count(&replacement.name), 10);
        for _ in 0..9 {
            assert!(table.add(replacement.clone()));
        }
        assert_eq!(table.size(), 10);
        assert_eq!(table.get_header(10), Some(&replacement));
        assert_eq!(table.names_len(), 1);
        let larger = HpackHeader::new("accept-encoding", "third");
        assert!(table.add(larger.clone()));
        assert_eq!(table.get_header(1), Some(&larger));
        assert_eq!(table.size(), 9, "one byte larger evicts two entries");
        assert!(!table.add(HpackHeader::new(
            "user-agent",
            vec![b'x'; capacity as usize]
        )));
        assert_eq!(table.size(), 0);
        assert_eq!(table.names_len(), 0);
    }

    #[test]
    fn reduce_capacity() {
        let header = accept();
        let mut table = HeaderTable::new(header.bytes() * 10);
        for _ in 0..10 {
            assert!(table.add(header.clone()));
        }
        assert!(table.set_capacity(header.bytes() * 5));
        assert_eq!(table.size(), 5);
        assert_eq!(table.bytes(), header.bytes() * 5);
    }

    #[test]
    fn comparison() {
        let mut first = HeaderTable::new(128);
        let mut second = HeaderTable::new(128);
        first.add(HpackHeader::new("Content-Encoding", "gzip"));
        assert_ne!(first, second);
        second.add(HpackHeader::new("Content-Encoding", "deflate"));
        assert_ne!(first, second);
        first.add(HpackHeader::new("Content-Encoding", "deflate"));
        second.add(HpackHeader::new("Content-Encoding", "gzip"));
        assert_eq!(first, second);
    }

    #[test]
    fn print() {
        let mut table = HeaderTable::new(128);
        table.add(accept());
        assert_eq!(
            table.to_string(),
            "\n[1] (s=51) accept-encoding: gzip\ntotal size: 51\n"
        );
    }

    #[test]
    fn increase_capacity() {
        let header = accept();
        let mut table = HeaderTable::new(header.bytes() * 4);
        let initial_slots = table.allocated_slots();
        for _ in 0..=initial_slots {
            assert!(table.add(header.clone()));
        }
        assert_eq!(table.size(), 4);
        assert!(table.set_capacity(header.bytes() * 8));
        assert!(table.allocated_slots() <= table.max_table_length() as usize);
        assert_eq!(table.get_index(&header).0, 1);
    }

    fn resize_and_fill(table: &mut HeaderTable, header: &HpackHeader, maximum: u32, count: u32) {
        assert!(table.set_capacity(header.bytes() * maximum));
        for _ in 0..=count {
            assert!(table.add(header.clone()));
        }
        assert_eq!(table.size(), maximum);
        assert_eq!(table.bytes(), header.bytes() * maximum);
    }

    #[test]
    fn vary_capacity() {
        let header = accept();
        let mut table = HeaderTable::new(header.bytes() * 6);
        for _ in 0..=table.allocated_slots() {
            assert!(table.add(header.clone()));
        }
        resize_and_fill(&mut table, &header, 4, 5);
        resize_and_fill(&mut table, &header, 5, 6);
        resize_and_fill(&mut table, &header, 8, 9);
    }

    #[test]
    fn vary_capacity_malign_head_index() {
        let header = accept();
        let mut table = HeaderTable::new(header.bytes() * 6);
        for _ in 0..table.max_table_length() {
            assert!(table.add(header.clone()));
        }
        assert!(!table.add(HpackHeader::new(
            "flush",
            vec![b'a'; table.capacity() as usize + 1]
        )));
        assert!(table.set_capacity(header.bytes() * 3));
        assert!(table.set_capacity(header.bytes() * 4));
        for _ in 0..=table.allocated_slots() {
            assert!(table.add(header.clone()));
        }
        assert_eq!(table.size(), 4);
    }

    #[test]
    fn add_larger_than_table() {
        let mut table = HeaderTable::new(256);
        table.add(accept());
        table.add(accept());
        table.add(HpackHeader::new("test-encoding", "gzip"));
        assert_eq!(table.names_len(), 2);
        assert!(!table.add(HpackHeader::new("a".repeat(256), "gzip")));
        assert_eq!(table.size(), 0);
        assert_eq!(table.names_len(), 0);
        table.add(accept());
        table.add(accept());
        table.add(HpackHeader::new("test-encoding", "gzip"));
        assert_eq!(table.names_len(), 2);
        assert!(table.has_name(&HeaderName::new("accept-encoding")));
        assert_eq!(
            table.name_entry_count(&HeaderName::new("accept-encoding")),
            2
        );
        assert_eq!(table.name_index(&HeaderName::new("accept-encoding")), 2);
    }

    #[test]
    fn increase_length_of_full_table() {
        let large = HpackHeader::new("Access-Control-Allow-Credentials", "true");
        let small = HpackHeader::new("Accept", "All-Content");
        let mut table = HeaderTable::new(448);
        assert_eq!(table.allocated_slots(), 7);
        for _ in 0..3 {
            table.add(large.clone());
            table.add(small.clone());
        }
        assert_eq!(table.allocated_slots(), 7);
        table.add(small.clone());
        table.add(small.clone());
        table.add(small.clone());
        assert_eq!(table.allocated_slots(), 11);
        assert_eq!(table.size(), 8);
        assert_eq!(table.get_header(1), Some(&small));
        assert_eq!(table.get_header(2), Some(&small));
        assert_eq!(table.get_header(3), Some(&small));
        assert_eq!(table.get_header(4), Some(&small));
        assert_eq!(table.get_header(5), Some(&large));
        assert_eq!(table.get_header(6), Some(&small));
        assert_eq!(table.get_header(7), Some(&large));
        assert_eq!(table.get_header(8), Some(&small));
        assert_eq!(table.name_index(&small.name), 1);
        assert_eq!(table.get_index(&large), (5, 0));
        assert_eq!(table.get_index(&small), (1, 0));
        assert_eq!(table.name_entry_count(&small.name), 6);
    }

    #[test]
    fn small_table() {
        let mut table = HeaderTable::new(80);
        assert!(table.add(HpackHeader::new("Foo", "bar")));
        assert!(table.add(HpackHeader::new("Foo", "bar")));
        assert_eq!(table.size(), 2);
        assert_eq!(table.allocated_slots(), 2);
    }

    #[test]
    fn tiny_table() {
        let mut table = HeaderTable::new(63);
        assert!(table.add(HpackHeader::new("Foo", "barbarbarbarbarbarbar1")));
        assert_eq!(table.size(), 1);
        assert_eq!(table.allocated_slots(), 1);
    }
}

mod huffman {
    use std::sync::OnceLock;

    use super::*;

    #[test]
    fn codes() {
        assert_eq!(HUFFMAN_TABLE_SIZE, 256);
        assert_eq!(EOS_HPACK, 0x3fff_ffff);
        assert_eq!(huffman_code(b'e'), (0x05, 5));
        assert_eq!(huffman_code(0), (0x1ff8, 13));
        assert_eq!(huffman_code(255), (0x3ffffee, 26));
        assert_eq!(huff_tree().codes_table().len(), HUFFMAN_TABLE_SIZE);
        assert_eq!(huff_tree().bits_table().len(), HUFFMAN_TABLE_SIZE);
    }

    #[test]
    fn size() {
        assert_eq!(huffman_encoded_size(b"x"), 1);
        assert_eq!(huffman_encoded_size(b"accept-encoding"), 11);
    }

    #[test]
    fn encode() {
        assert_eq!(encode_huffman(b"gzip"), [0x9b, 0xd9, 0xab]);
        assert_eq!(encode_huffman(b"accept-encoding").len(), 11);
    }

    #[test]
    fn decode() {
        assert_eq!(decode_huffman(&[0x60, 0xbf]), Ok(b"/e".to_vec()));
        assert_eq!(decode_huffman(&[0x9b, 0xd9, 0xab]), Ok(b"gzip".to_vec()));
        assert_eq!(decode_huffman(&[0x98, 0xbf]), Ok(b"ge".to_vec()));
    }

    #[test]
    fn non_printable_decode() {
        assert_eq!(decode_huffman(&[0xff, 0xff, 0xea, 0xf8]), Ok(vec![9, 38]));
        assert_eq!(
            decode_huffman(&[0xff, 0xff, 0xb1, 0xff, 0xff, 0xf5, 0xff]),
            Ok(vec![1, 240])
        );
    }

    #[test]
    fn example_com() {
        let value = b"www.example.com";
        let encoded = encode_huffman(value);
        assert_eq!(encoded.len(), 12);
        assert_eq!(decode_huffman(&encoded), Ok(value.to_vec()));
    }

    #[test]
    fn user_agent() {
        let value = b"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/11B554a [FBAN/FBIOS;FBAV/6.7;FBBV/566055;FBDV/iPhone5,1;FBMD/iPhone;FBSN/iPhone OS;FBSV/7.0.4;FBSS/2; FBCR/AT&T;FBID/phone;FBLC/en_US;FBOP/5]";
        let encoded = encode_huffman(value);
        assert_eq!(decode_huffman(&encoded), Ok(value.to_vec()));
    }

    #[test]
    fn fit_in_buffer() {
        assert!(encode_huffman(b"").is_empty());
        assert_eq!(encode_huffman(b"g").len(), 1);
    }

    #[test]
    fn sanity_checks() {
        for byte in 0..=u8::MAX {
            let encoded = encode_huffman(&[byte]);
            assert_eq!(decode_huffman(&encoded), Ok(vec![byte]));
        }
        assert_eq!(
            decode_huffman(&[0xff, 0xff, 0xff, 0xff]),
            Err(HpackError::InvalidHuffmanCode)
        );
        assert_eq!(decode_huffman(&[0x00]), Err(HpackError::InvalidHuffmanCode));
    }

    #[test]
    fn all_byte_pairs_round_trip() {
        for first in 0..=u8::MAX {
            for second in 0..=u8::MAX {
                let literal = [first, second];
                let encoded = encode_huffman(&literal);
                assert_eq!(
                    decode_huffman(&encoded),
                    Ok(literal.to_vec()),
                    "literal {literal:?}"
                );
            }
        }
    }

    #[test]
    fn mixed_code_lengths_at_all_bit_offsets() {
        for prefix_len in 0..8 {
            let mut literal = vec![b'e'; prefix_len];
            literal.extend_from_slice(b"0\x00a\xff&\x09/echo");
            let encoded = encode_huffman(&literal);
            assert_eq!(
                decode_huffman(&encoded),
                Ok(literal),
                "prefix length {prefix_len}"
            );
        }
    }

    #[test]
    fn short_terminal_codes_and_padding() {
        let cases: &[(&[u8], &[u8])] = &[
            (&[], b""),
            (&[0x00, 0x01], b"000"),
            (&[0x00, 0xe1], b"0aA"),
            (&[0x03, 0x0c, 0x5d], b"0//B"),
        ];
        for &(encoded, literal) in cases {
            assert_eq!(decode_huffman(encoded), Ok(literal.to_vec()));
        }
    }

    #[test]
    fn malformed_after_short_symbols() {
        let cases: &[(&str, &[u8])] = &[
            ("overlong padding", &[0xf8, 0xff]),
            ("truncated long code", &[0xf8, 0xff, 0xff, 0xff]),
            ("EOS after aligned symbol", &[0xf8, 0xff, 0xff, 0xff, 0xff]),
            (
                "EOS after unaligned symbol",
                &[0x2f, 0xff, 0xff, 0xff, 0xff],
            ),
            ("non-one padding after unaligned symbol", &[0x2e]),
            ("non-one padding after partial terminal code", &[0x00, 0x00]),
        ];
        for &(case, encoded) in cases {
            assert_eq!(
                decode_huffman(encoded),
                Err(HpackError::InvalidHuffmanCode),
                "{case}"
            );
        }
    }

    const CORPUS_SEED: u64 = 0x5eed_0141;
    const DIFFERENTIAL_SEED: u64 = 0xd1ff_0141;
    const EOS_SYMBOL: u16 = 256;
    const EOS_BITS: u8 = 30;
    const LOWER_HEX: &[u8] = b"0123456789abcdef";
    const DECIMAL: &[u8] = b"0123456789";
    const LOWER_ALPHANUMERIC: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    const MIXED_ALPHANUMERIC: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:,/ ";
    const FIXED_DATE: &[u8] = b"Sun, 06 Nov 1994 08:49:37 GMT";
    const CONSUMER_NAMES: [&[u8]; 9] = [
        b"x-request-id",
        b"x-routing-key",
        b"x-routing-key-copy",
        b"x-token-count",
        b"x-prefix-hash",
        b"x-timeout-ms",
        b"x-pad-req",
        b"x-pad-resp",
        b"x-upstream-time",
    ];
    /// RFC 7541 C.4 and C.6 Huffman-coded strings and their decoded text.
    const RFC_HUFFMAN: [(&str, &[u8]); 11] = [
        ("f1e3c2e5f23a6ba0ab90f4ff", b"www.example.com"),
        ("a8eb10649cbf", b"no-cache"),
        ("25a849e95ba97d7f", b"custom-key"),
        ("25a849e95bb8e8b4bf", b"custom-value"),
        ("6402", b"302"),
        ("aec3771a4b", b"private"),
        (
            "d07abe941054d444a8200595040b8166e082a62d1bff",
            b"Mon, 21 Oct 2013 20:13:21 GMT",
        ),
        (
            "9d29ad171863c78f0b97c8e9ae82ae43d3",
            b"https://www.example.com",
        ),
        ("640eff", b"307"),
        (
            "d07abe941054d444a8200595040b8166e084a62d1bff",
            b"Mon, 21 Oct 2013 20:13:22 GMT",
        ),
        (
            "94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007",
            b"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
        ),
    ];

    fn splitmix64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = *state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    fn below(state: &mut u64, bound: usize) -> usize {
        (splitmix64(state) % bound as u64) as usize
    }

    fn random_from(state: &mut u64, alphabet: &[u8], length: usize) -> Vec<u8> {
        (0..length)
            .map(|_| alphabet[below(state, alphabet.len())])
            .collect()
    }

    fn random_bytes(state: &mut u64, length: usize) -> Vec<u8> {
        (0..length).map(|_| splitmix64(state) as u8).collect()
    }

    fn symbol_code(symbol: u16) -> (u32, u8) {
        match u8::try_from(symbol) {
            Ok(byte) => huffman_code(byte),
            Err(_) => (EOS_HPACK, EOS_BITS),
        }
    }

    /// Huffman-codes symbols (byte values, or `EOS_SYMBOL`) from the public
    /// code table, padding the last byte with ones, without the product encoder.
    fn encode_symbols(symbols: impl IntoIterator<Item = u16>) -> Vec<u8> {
        let mut output = Vec::new();
        let mut accumulator = 0_u64;
        let mut bits = 0_u32;
        for symbol in symbols {
            let (code, length) = symbol_code(symbol);
            accumulator = (accumulator << length) | u64::from(code);
            bits += u32::from(length);
            while bits >= 8 {
                bits -= 8;
                output.push((accumulator >> bits) as u8);
            }
            accumulator &= (1_u64 << bits) - 1;
        }
        if bits != 0 {
            output.push(((accumulator << (8 - bits)) | ((1_u64 << (8 - bits)) - 1)) as u8);
        }
        output
    }

    fn reference_encode(literal: &[u8]) -> Vec<u8> {
        encode_symbols(literal.iter().map(|&byte| u16::from(byte)))
    }

    #[derive(Clone, Copy, Default)]
    struct ReferenceNode {
        children: [Option<usize>; 2],
        symbol: Option<u16>,
    }

    /// A binary tree of the 257 HPACK codes, built from the public code table.
    fn reference_tree() -> Vec<ReferenceNode> {
        let mut nodes = vec![ReferenceNode::default()];
        for symbol in 0..=EOS_SYMBOL {
            let (code, length) = symbol_code(symbol);
            let mut node = 0;
            for shift in (0..length).rev() {
                let bit = usize::from((code >> shift) & 1 == 1);
                node = match nodes[node].children[bit] {
                    Some(child) => child,
                    None => {
                        nodes.push(ReferenceNode::default());
                        let child = nodes.len() - 1;
                        nodes[node].children[bit] = Some(child);
                        child
                    }
                };
            }
            nodes[node].symbol = Some(symbol);
        }
        nodes
    }

    /// Decodes HPACK Huffman one bit at a time. Accepts exactly when no EOS is
    /// decoded and the bits after the last symbol are fewer than 8, all ones.
    fn reference_decode(encoded: &[u8]) -> Result<Vec<u8>, HpackError> {
        static TREE: OnceLock<Vec<ReferenceNode>> = OnceLock::new();
        let tree = TREE.get_or_init(reference_tree);
        let mut output = Vec::new();
        let mut node = 0;
        let mut pending_bits = 0_usize;
        let mut pending_all_ones = true;
        for byte in encoded {
            for shift in (0..8).rev() {
                let bit = usize::from((byte >> shift) & 1);
                pending_bits += 1;
                pending_all_ones &= bit == 1;
                node = tree[node].children[bit].ok_or(HpackError::InvalidHuffmanCode)?;
                let Some(symbol) = tree[node].symbol else {
                    continue;
                };
                let byte = u8::try_from(symbol).map_err(|_| HpackError::InvalidHuffmanCode)?;
                output.push(byte);
                node = 0;
                pending_bits = 0;
                pending_all_ones = true;
            }
        }
        if pending_bits > 7 || !pending_all_ones {
            return Err(HpackError::InvalidHuffmanCode);
        }
        Ok(output)
    }

    fn consumer_literals(state: &mut u64) -> Vec<Vec<u8>> {
        let mut literals = Vec::new();
        for _ in 0..64 {
            let mut id = b"api.proxy.".to_vec();
            id.extend(random_from(state, LOWER_HEX, 8));
            id.extend(random_from(state, LOWER_HEX, 16));
            id.push(b'-');
            id.extend(random_from(state, LOWER_HEX, 32));
            assert_eq!(id.len(), 67);
            let mut key = b"session-".to_vec();
            key.extend(random_from(state, LOWER_HEX, 64));
            key.push(b'-');
            key.extend(random_from(state, DECIMAL, 4));
            assert_eq!(key.len(), 77);
            literals.push(id);
            literals.push(key.clone());
            literals.push(key);
            literals.push(random_from(state, DECIMAL, 4));
            literals.push(random_from(state, LOWER_HEX, 16));
            literals.push(FIXED_DATE.to_vec());
            literals.push(random_from(state, DECIMAL, 3));
            literals.extend(CONSUMER_NAMES.iter().map(|name| name.to_vec()));
        }
        literals
    }

    fn literals_from(state: &mut u64) -> Vec<Vec<u8>> {
        let mut literals: Vec<Vec<u8>> = RFC_HUFFMAN
            .iter()
            .map(|(_, literal)| literal.to_vec())
            .collect();
        literals.extend((0..=u8::MAX).map(|byte| vec![byte]));
        for length in 0..=160 {
            for alphabet in [LOWER_HEX, DECIMAL, LOWER_ALPHANUMERIC, MIXED_ALPHANUMERIC] {
                literals.push(random_from(state, alphabet, length));
            }
            literals.push((0..length).map(|i| (length * 7 + i) as u8).collect());
        }
        literals.extend(consumer_literals(state));
        literals
    }

    /// Every valid literal of the Huffman corpus, in corpus order.
    pub(super) fn huffman_literals() -> Vec<Vec<u8>> {
        let mut state = CORPUS_SEED;
        literals_from(&mut state)
    }

    /// The deterministic Huffman decode corpus: every valid literal's
    /// encoding, EOS alone, malformed variants of each valid encoding, and
    /// short random byte strings.
    fn huffman_corpus() -> Vec<Vec<u8>> {
        let mut state = CORPUS_SEED;
        let literals = literals_from(&mut state);
        let mut corpus: Vec<Vec<u8>> = literals
            .iter()
            .map(|literal| reference_encode(literal))
            .collect();
        corpus.push(encode_symbols([EOS_SYMBOL]));
        for literal in &literals {
            let encoded = reference_encode(literal);
            let mut overlong = encoded.clone();
            overlong.push(0xff);
            corpus.push(overlong);
            if let Some((&last, head)) = encoded.split_last() {
                let mut flipped = encoded.clone();
                *flipped.last_mut().expect("non-empty encoding") = last ^ 1;
                corpus.push(flipped);
                corpus.push(head.to_vec());
            }
            for symbols in 1..=3 {
                if literal.len() >= symbols {
                    let (before, after) = literal.split_at(symbols);
                    corpus.push(encode_symbols(
                        before
                            .iter()
                            .map(|&byte| u16::from(byte))
                            .chain([EOS_SYMBOL])
                            .chain(after.iter().map(|&byte| u16::from(byte))),
                    ));
                }
            }
        }
        for _ in 0..4096 {
            let length = 1 + below(&mut state, 12);
            corpus.push(random_bytes(&mut state, length));
        }
        corpus
    }

    #[test]
    fn huffman_decode_golden_digest_is_unchanged() {
        let mut stream = Vec::new();
        for input in huffman_corpus() {
            let decoded = decode_huffman(&input);
            stream.push(u8::from(decoded.is_ok()));
            let output = decoded.unwrap_or_default();
            let length = u32::try_from(output.len()).expect("decoded length fits u32");
            stream.extend_from_slice(&length.to_le_bytes());
            stream.extend_from_slice(&output);
        }
        assert_eq!(
            (fnv1a64(&stream), stream.len()),
            (0x7a64_e577_303d_2b80, 216_726),
            "recorded before any Huffman decoder change"
        );
    }

    #[test]
    fn huffman_encode_golden_digest_is_unchanged() {
        let stream: Vec<u8> = huffman_literals()
            .iter()
            .flat_map(|literal| encode_huffman(literal))
            .collect();
        assert_eq!(
            (fnv1a64(&stream), stream.len()),
            (0xdb17_3cbf_ef1a_576e, 86_374),
            "recorded before any Huffman encoder change"
        );
    }

    #[test]
    fn huffman_decode_matches_the_reference_decoder() {
        let assert_agrees = |input: &[u8]| {
            let expected = reference_decode(input);
            assert_eq!(decode_huffman(input), expected, "input {input:02x?}");
            assert_eq!(huff_tree().decode(input), expected, "input {input:02x?}");
        };
        huffman_corpus()
            .iter()
            .for_each(|input| assert_agrees(input));

        let alphabets = [LOWER_HEX, DECIMAL, LOWER_ALPHANUMERIC, MIXED_ALPHANUMERIC];
        let mut state = DIFFERENTIAL_SEED;
        for case in 0..200_000_usize {
            let input = if case % 2 == 0 {
                let length = below(&mut state, 201);
                let alphabet = below(&mut state, alphabets.len() + 1);
                let literal = match alphabets.get(alphabet) {
                    Some(alphabet) => random_from(&mut state, alphabet, length),
                    None => random_bytes(&mut state, length),
                };
                let mut encoded = reference_encode(&literal);
                let valid_case = case / 2;
                if valid_case % 3 == 0 && !encoded.is_empty() {
                    let bit = below(&mut state, encoded.len() * 8);
                    encoded[bit / 8] ^= 0x80 >> (bit % 8);
                }
                if valid_case % 10 == 0 {
                    encoded.push(0xff);
                }
                encoded
            } else {
                let length = below(&mut state, 25);
                random_bytes(&mut state, length)
            };
            assert_agrees(&input);
        }
    }

    #[test]
    fn huffman_round_trips_every_corpus_literal() {
        for literal in huffman_literals() {
            let encoded = encode_huffman(&literal);
            assert_eq!(
                decode_huffman(&encoded),
                Ok(literal.clone()),
                "literal {literal:02x?}"
            );
            assert_eq!(
                encoded.len(),
                huffman_encoded_size(&literal),
                "literal {literal:02x?}"
            );
        }
    }

    #[test]
    fn reference_decoder_agrees_with_rfc_vectors() {
        // C.4.1-C.4.3 are the first four entries, C.6.1 the next four.
        for (encoded, literal) in &RFC_HUFFMAN[..8] {
            assert_eq!(
                reference_decode(&hex(encoded)),
                Ok(literal.to_vec()),
                "{encoded}"
            );
        }
        for (encoded, literal) in &RFC_HUFFMAN {
            assert_eq!(reference_encode(literal), hex(encoded), "{encoded}");
        }

        let mut a_then_overlong_padding = encode_huffman(b"a");
        a_then_overlong_padding.push(0xff);
        for malformed in [
            vec![0xff],
            vec![0xff, 0xff, 0xff, 0xff],
            a_then_overlong_padding,
        ] {
            assert_eq!(
                reference_decode(&malformed),
                Err(HpackError::InvalidHuffmanCode),
                "{malformed:02x?}"
            );
        }
    }
}

mod hpack_logging {
    use super::*;

    #[test]
    fn refset() {
        assert_eq!(dump_refset(&[3, 5]), "\n[3 5 ]\n");
    }

    #[test]
    fn dump_header_vector() {
        assert_eq!(
            crate::hpack::dump_header_vector(&headers(&[
                (":path", "index.html"),
                ("content-type", "gzip"),
            ])),
            ":path: index.html\ncontent-type: gzip\n\n"
        );
    }

    #[test]
    fn print_delta() {
        let first = headers(&[(":path", "/"), (":host", "www.facebook.com")]);
        assert_eq!(
            crate::hpack::print_delta(&first, &[]),
            "\n + :path: /\n + :host: www.facebook.com\n"
        );
        assert_eq!(
            crate::hpack::print_delta(&[], &first),
            "\n - :path: /\n - :host: www.facebook.com\n"
        );
        assert_eq!(
            crate::hpack::print_delta(&first, &headers(&[(":path", "/")])),
            "\n + :host: www.facebook.com\n"
        );
        assert_eq!(
            crate::hpack::print_delta(&headers(&[(":path", "/"), (":path", "/")]), &first,),
            "\n - :host: www.facebook.com\n + :path: / (duplicate)\n"
        );
        let duplicate = headers(&[(":a", "b"), (":a", "b"), (":path", "/")]);
        assert_eq!(
            crate::hpack::print_delta(&duplicate, &headers(&[(":path", "/")])),
            "\n + :a: b\n duplicate :a: b\n"
        );
    }

    #[test]
    fn dump_binary() {
        let mut buffer = HpackEncodeBuffer::new(false);
        buffer.encode_literal(b"test");
        assert_eq!(
            crate::hpack::dump_binary(buffer.as_slice()),
            "00000100   01110100 t 01100101 e 01110011 s 01110100 t \n"
        );
    }
}

mod hpack_context {
    use super::*;

    #[test]
    fn static_lookup_preserves_all_names_values_and_reference_counts() {
        let table = StaticHeaderTable::get();
        assert_eq!(table.len(), 61);
        let mut uncommon = Vec::new();
        for (offset, header) in table.iter().enumerate() {
            let index = u32::try_from(offset + 1).unwrap();
            let first = match header.name.as_bytes() {
                b":method" => 2,
                b":path" => 4,
                b":scheme" => 6,
                b":status" => 8,
                _ => index,
            };
            let mut names = vec![
                header.name.clone(),
                HeaderName::new(header.name.as_bytes().to_ascii_uppercase()),
            ];
            if header.name.is_common_header() {
                names.push(HeaderName::from_code(header.name.code()));
            } else {
                assert_eq!(header.name.code(), HttpHeaderCode::Other);
                uncommon.push(index);
            }
            for name in names {
                assert_eq!(name, header.name);
                assert_eq!(StaticHeaderTable::name_index(&name), first);
                assert_eq!(
                    StaticHeaderTable::get_index(&name, &header.value),
                    (index, 0)
                );
                assert_eq!(
                    StaticHeaderTable::get_index(&name, b"not-static"),
                    (0, first)
                );
                assert_eq!(
                    StaticHeaderTable::get_index(&name, b""),
                    if header.value.is_empty() {
                        (index, 0)
                    } else {
                        (0, first)
                    }
                );
            }
        }
        assert_eq!(
            uncommon,
            [15, 22, 29, 35, 37, 39, 42, 43, 45, 47, 52, 53, 61]
        );
        for name in [
            HeaderName::from_code(HttpHeaderCode::ColonProtocol),
            HeaderName::new("x-no-static-match"),
            HeaderName::new(b"ALLOW\x80"),
            HeaderName::new(""),
        ] {
            assert_eq!(StaticHeaderTable::name_index(&name), 0);
            assert_eq!(StaticHeaderTable::get_index(&name, b""), (0, 0));
            assert_eq!(StaticHeaderTable::get_index(&name, b"not-static"), (0, 0));
        }

        let mut context = HpackContext::new(DEFAULT_TABLE_SIZE);
        context.seed_header_table(headers(&[
            (":method", "GET"),
            (":method", "PATCH"),
            ("date", "yesterday"),
            ("server", "origin"),
            ("allow", "GET"),
            ("x-custom", "one"),
        ]));
        for (name, value, dynamic, expected, refs) in [
            (":method", "GET", true, (2, 0), 1),
            (":method", "PATCH", true, (66, 0), 1),
            (":method", "DELETE", true, (0, 2), 2),
            ("server", "origin", true, (64, 0), 2),
            ("server", "other", true, (0, 64), 2),
            ("date", "yesterday", true, (65, 0), 2),
            ("date", "today", true, (0, 33), 3),
            ("allow", "GET", true, (63, 0), 3),
            ("allow", "", true, (0, 63), 3),
            ("server", "origin", false, (0, 54), 4),
            (":method", "PATCH", false, (0, 2), 5),
            ("x-custom", "one", false, (0, 0), 5),
        ] {
            assert_eq!(
                context.get_index_parts(&HeaderName::new(name), value.as_bytes(), dynamic),
                expected,
                "{name}: {value}, dynamic={dynamic}"
            );
            assert_eq!(context.static_refs(), refs);
        }
        for (name, index, refs) in [
            ("server", 54, 6),
            ("allow", 22, 7),
            ("x-custom", 62, 7),
            ("x-missing", 0, 7),
        ] {
            assert_eq!(context.name_index(&HeaderName::new(name)), index);
            assert_eq!(context.static_refs(), refs);
        }
        assert_eq!(
            context.get_header(2),
            Some(&HpackHeader::new(":method", "GET"))
        );
        assert_eq!(context.static_refs(), 8);
        assert_eq!(
            context.get_header(62),
            Some(&HpackHeader::new("x-custom", "one"))
        );
        assert_eq!(context.static_refs(), 8);
        context.table_mut().clear();
        assert_eq!(context.get_index(&HpackHeader::new("allow", "")), (0, 0));
        assert_eq!(context.static_refs(), 8);
        assert_eq!(context.name_index(&HeaderName::new("allow")), 22);
        assert_eq!(context.static_refs(), 9);
    }

    #[test]
    fn get_index() {
        let mut context = HpackContext::new(DEFAULT_TABLE_SIZE);
        assert_eq!(context.get_index(&HpackHeader::new(":method", "POST")).0, 3);
    }

    #[test]
    fn is_static() {
        assert!(HpackContext::is_static(1));
        assert!(HpackContext::is_static(60));
        assert!(!HpackContext::is_static(69));
    }

    #[test]
    fn static_table() {
        let table = StaticHeaderTable::get();
        assert_eq!(table.len(), 61);
        assert_eq!(
            StaticHeaderTable::get_header(1),
            Some(&HpackHeader::new(":authority", ""))
        );
        assert_eq!(
            StaticHeaderTable::get_header(3),
            Some(&HpackHeader::new(":method", "POST"))
        );
        assert_eq!(
            table
                .last()
                .expect("static table is nonempty")
                .name
                .as_bytes(),
            b"www-authenticate"
        );
    }

    #[test]
    fn static_table_header_names_are_common() {
        // Static-table names outside the common-header table. They are still found, by name
        // rather than by code.
        let uncommon = [
            b"allow".as_slice(),
            b"content-location",
            b"from",
            b"if-match",
            b"if-unmodified-since",
            b"max-forwards",
            b"if-range",
            b"refresh",
            b"accept-charset",
            b"expect",
            b"link",
            b"retry-after",
            b"www-authenticate",
        ];
        assert!(StaticHeaderTable::get().iter().all(|header| {
            header.name.is_common_header() || uncommon.contains(&header.name.as_bytes())
        }));
    }

    #[test]
    fn static_table_header_code_with_nonempty_value() {
        assert!(StaticHeaderTable::get().iter().all(|header| {
            header.value.is_empty()
                != StaticHeaderTable::is_header_code_in_table_with_non_empty_value(
                    header.name.code(),
                )
        }));
    }

    #[test]
    fn static_index() {
        let mut context = HpackContext::new(DEFAULT_TABLE_SIZE);
        assert_eq!(
            context.get_header(1),
            Some(&HpackHeader::new(":authority", ""))
        );
        assert_eq!(
            context.get_header(3),
            Some(&HpackHeader::new(":method", "POST"))
        );
        assert_eq!(
            context.get_header(28),
            Some(&HpackHeader::new("content-length", ""))
        );
    }

    #[test]
    fn encoder_multiple_values() {
        let values = headers(&[
            ("accept-encoding", "gzip"),
            ("accept-encoding", "sdch,gzip"),
        ]);
        let mut encoder = HpackEncoder::new(true);
        let first = encoder.encode(&values);
        assert_eq!(encoder.context().table().size(), 2);
        let second = encoder.encode(&values);
        assert!(second.len() < first.len());
        assert!(!second.is_empty());
    }

    #[test]
    fn decoder_large_header() {
        let values = headers(&[(":path", "verylargeheader"), (":method", "GET")]);
        let mut encoder = HpackEncoder::with_table_size(true, 32);
        let mut decoder = HpackDecoder::with_limits(32, DEFAULT_MAX_UNCOMPRESSED);
        let encoded = encoder.encode(&values);
        assert!(decoder.decode(&encoded).is_ok());
        assert_eq!(encoder.context().table().size(), 0);
        assert_eq!(decoder.context().table().size(), 0);
    }

    #[test]
    fn decoder_large_header_clear() {
        let mut encoder = HpackEncoder::with_table_size(true, 4096);
        let mut decoder = HpackDecoder::with_limits(40, DEFAULT_MAX_UNCOMPRESSED);
        let first = encoder.encode(&headers(&[("foo", "bar")]));
        assert!(decoder.decode(&first).is_ok());
        assert_eq!(decoder.context().table().size(), 1);
        let second = encoder.encode(&headers(&[("bar", "verylargeheader")]));
        assert!(decoder.decode(&second).is_ok());
        assert_eq!(decoder.context().table().size(), 0);
    }

    #[test]
    fn decoder_invalid_peek() {
        let mut encoder = HpackEncoder::new(true);
        let expected = headers(&[("x-fb-debug", "test")]);
        let encoded = encoder.encode(&expected);
        let decoded = HpackDecoder::new()
            .decode_segments([&[][..], encoded.as_slice()], encoded.len())
            .expect("segmented header block decodes");
        assert_eq!(decoded.headers, expected);
        assert_eq!(decoded.bytes_consumed, encoded.len());
    }

    #[test]
    fn decoder_invalid_literal_peek() {
        let mut encoder = HpackEncoder::new(true);
        let expected = headers(&[("x-fb-random", "bla")]);
        let encoded = encoder.encode(&expected);
        let decoded = HpackDecoder::new()
            .decode_segments([&[][..], encoded.as_slice()], encoded.len())
            .expect("segmented literal header block decodes");
        assert_eq!(decoded.headers, expected);
        assert_eq!(decoded.bytes_consumed, encoded.len());
    }

    #[test]
    fn decode_errors() {
        for (encoded, expected) in [
            (vec![0x3f], HpackError::BufferUnderflow),
            (vec![0x7f, 0xff, 0x7f], HpackError::InvalidIndex),
            (vec![0x00], HpackError::BufferUnderflow),
            (vec![0x00, 0x01, b'h'], HpackError::BufferUnderflow),
            (vec![0x00, 0x81, 0xff], HpackError::InvalidHuffmanCode),
            (vec![0xff, 0x80, 0x80], HpackError::BufferUnderflow),
            (vec![0x3f, 0xff, 0x7f], HpackError::InvalidTableSize),
            (
                vec![
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
                ],
                HpackError::IntegerOverflow,
            ),
        ] {
            assert_eq!(HpackDecoder::new().decode(&encoded), Err(expected));
        }
    }

    #[test]
    fn exclude_headers_larger_than_table() {
        let mut encoder = HpackEncoder::with_table_size(true, 128);
        let short = HpackHeader::new("Short", "header");
        let long = HpackHeader::new(".".repeat(150), "header");
        encoder.encode(&[short.clone(), long.clone()]);
        assert_eq!(encoder.context().table().get_index(&long).0, 0);
        assert_eq!(encoder.context().table().get_index(&short).0, 1);
    }

    #[test]
    fn encode_to_write_buffer() {
        let expected = headers(&[("x-fb-debug", "test")]);
        let mut encoder = HpackEncoder::new(true);
        let mut queue = BufQueue::new(1024);
        let encoded_size = encoder
            .encode_to_buf_queue(&expected, &mut queue)
            .expect("header block fits");
        assert_eq!(encoded_size, queue.chain_length());
        assert_eq!(
            HpackDecoder::new()
                .decode_buf_queue(&queue, queue.chain_length())
                .expect("queued header block decodes")
                .headers,
            expected
        );

        let mut rejected = HpackEncoder::new(true);
        let mut full_queue = BufQueue::new(0);
        assert!(
            rejected
                .encode_to_buf_queue(&headers(&[("x-fb-debug", "test")]), &mut full_queue)
                .is_err()
        );
        assert_eq!(rejected.context().table().size(), 0);
    }

    struct StatefulIndexingStrategy {
        calls: Cell<u32>,
    }

    impl HeaderIndexingStrategy for StatefulIndexingStrategy {
        fn index_header(&self, _name: &HeaderName, _value: &[u8], _name_exists: bool) -> bool {
            self.calls.set(self.calls.get() + 1);
            self.calls.get() == 1
        }

        fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
            Box::new(Self {
                calls: Cell::new(self.calls.get()),
            })
        }
    }

    #[test]
    fn bounded_queue_failure_does_not_mutate_indexing_strategy() {
        let mut encoder = HpackEncoder::new(false);
        encoder.set_header_indexing_strategy(Rc::new(StatefulIndexingStrategy {
            calls: Cell::new(0),
        }));

        let mut rejected = BufQueue::new(0);
        assert!(
            encoder
                .encode_to_buf_queue(&headers(&[("x-first", "value")]), &mut rejected)
                .is_err()
        );
        assert_eq!(encoder.context().table().size(), 0);

        let first = encoder.encode(&headers(&[("x-first", "value")]));
        assert!(!first.is_empty());
        assert_eq!(encoder.context().table().size(), 1);

        let mut encoder = HpackEncoder::new(false);
        encoder.set_header_indexing_strategy(Rc::new(StatefulIndexingStrategy {
            calls: Cell::new(0),
        }));
        let mut accepted = BufQueue::new(1024);
        assert!(
            encoder
                .encode_to_buf_queue(&headers(&[("x-first", "value")]), &mut accepted)
                .is_ok()
        );
        assert_eq!(encoder.context().table().size(), 1);
        encoder.encode(&headers(&[("x-second", "value")]));
        assert_eq!(encoder.context().table().size(), 1);
    }

    #[test]
    fn ample_and_exact_fit_queues_preserve_hpack_wire_and_context_across_blocks() {
        for huffman in [false, true] {
            let mut ample = HpackEncoder::with_table_size(huffman, 256);
            let mut exact = HpackEncoder::with_table_size(huffman, 256);
            let mut reference = HpackEncoder::with_table_size(huffman, 256);
            let mut borrowed = [HpackCodec::new(), HpackCodec::new()];
            for codec in &mut borrowed {
                *codec.encoder_mut() = HpackEncoder::with_table_size(huffman, 256);
            }
            let mut decoder = HpackDecoder::with_limits(256, DEFAULT_MAX_UNCOMPRESSED);
            let first = vec![
                HpackHeader::new(":method", "GET"),
                HpackHeader::new("x-hot", "same"),
                HpackHeader::new("x-expand", vec![0xff; 48]),
            ];
            let eviction = vec![
                HpackHeader::new("x-first", vec![b'a'; 80]),
                HpackHeader::new("x-second", vec![b'b'; 80]),
                HpackHeader::new("x-third", vec![b'c'; 80]),
            ];
            let blocks = [
                (vec![], first.clone()),
                (vec![], first.clone()),
                (vec![], eviction),
                (vec![], headers(&[("x-hot", "same")])),
                (vec![0, 256], vec![]),
                (vec![], first.clone()),
                (vec![], first),
            ];
            for (ordinal, (updates, mut values)) in blocks.into_iter().enumerate() {
                for encoder in [&mut ample, &mut exact, &mut reference] {
                    for size in &updates {
                        encoder.set_header_table_size(*size);
                    }
                }
                for codec in &mut borrowed {
                    for size in &updates {
                        codec.set_encoder_header_table_size(*size);
                    }
                }
                let expected = reference.encode(&values);
                if huffman && ordinal == 0 {
                    let plain = HpackEncoder::with_table_size(false, 256).encode(&values);
                    assert!(
                        expected.len() > plain.len(),
                        "the opaque value must expand under Huffman encoding"
                    );
                }
                if ordinal == 1 {
                    assert_eq!(expected, hex("82bfbe"));
                } else if ordinal == 2 {
                    assert!(
                        !reference
                            .context()
                            .table()
                            .has_name(&HeaderName::new("x-hot"))
                    );
                } else if ordinal == 4 {
                    assert_eq!(expected, hex("203fe101"));
                }
                for (encoder, capacity) in [(&mut ample, 16 << 10), (&mut exact, expected.len())] {
                    let mut queue = BufQueue::new(capacity);
                    assert_eq!(
                        encoder.encode_to_buf_queue(&values, &mut queue),
                        Ok(expected.len())
                    );
                    assert_eq!(queue.chain_length(), expected.len());
                    assert_eq!(
                        queue.coalesce(queue.chain_length()).unwrap().as_ref(),
                        expected
                    );
                    let table = encoder.context().table();
                    let reference_table = reference.context().table();
                    assert!(table.entries().eq(reference_table.entries()));
                    assert_eq!(
                        (
                            table.capacity(),
                            table.bytes(),
                            table.insert_count(),
                            encoder.context().static_refs()
                        ),
                        (
                            reference_table.capacity(),
                            reference_table.bytes(),
                            reference_table.insert_count(),
                            reference.context().static_refs()
                        )
                    );
                }
                let inputs = borrowed_headers(&values);
                for (codec, capacity) in borrowed.iter_mut().zip([16 << 10, expected.len()]) {
                    let before_info = codec.compression_info().egress;
                    let before_size = codec.encoded_size();
                    let before = codec.encoder().clone();
                    let mut rejected = BufQueue::new(expected.len() - 1);
                    assert!(
                        codec
                            .encode_borrowed_to_buf_queue(&inputs, &mut rejected)
                            .is_err()
                    );
                    assert!(rejected.is_empty());
                    assert_eq!(codec.compression_info().egress, before_info);
                    assert_eq!(codec.encoded_size(), before_size);
                    assert!(
                        codec
                            .encoder()
                            .context()
                            .table()
                            .entries()
                            .eq(before.context().table().entries())
                    );
                    let mut queue = BufQueue::new(capacity);
                    assert_eq!(
                        codec.encode_borrowed_to_buf_queue(&inputs, &mut queue),
                        Ok(expected.len())
                    );
                    assert_eq!(
                        queue.coalesce(queue.chain_length()).unwrap().as_ref(),
                        expected
                    );
                }
                assert_eq!(decoder.decode(&expected).unwrap().headers, values);
                drop(inputs);
                for header in &mut values {
                    header.value.fill(b'?');
                }
                drop(values);
                for codec in &borrowed {
                    assert!(
                        codec
                            .encoder()
                            .context()
                            .table()
                            .entries()
                            .eq(reference.context().table().entries())
                    );
                    assert_eq!(
                        codec.encoder().context().table().insert_count(),
                        reference.context().table().insert_count()
                    );
                    assert_eq!(
                        codec.encoder().context().static_refs(),
                        reference.context().static_refs()
                    );
                }
            }
        }
    }

    #[test]
    fn input_blocks_preserve_unknown_lookup_wire_context_and_retry() {
        struct Inputs<'a>(&'a [Header<'a>]);

        impl crate::hpack::HeaderInputs for Inputs<'_> {
            fn for_each<'a>(&'a self, visitor: impl FnMut(Header<'a>)) {
                self.0.iter().copied().for_each(visitor);
            }
        }

        let input = |code, name: &'static [u8], value: &'static [u8]| Header { code, name, value };
        let assert_context = |actual: &HpackContext, expected: &HpackContext| {
            let actual_table = actual.table();
            let expected_table = expected.table();
            assert!(actual_table.entries().eq(expected_table.entries()));
            assert_eq!(
                (
                    actual_table.capacity(),
                    actual_table.bytes(),
                    actual_table.insert_count(),
                    actual_table.allocated_slots(),
                    actual.static_refs(),
                ),
                (
                    expected_table.capacity(),
                    expected_table.bytes(),
                    expected_table.insert_count(),
                    expected_table.allocated_slots(),
                    expected.static_refs(),
                )
            );
        };
        for huffman in [false, true] {
            let mut reference = HpackCodec::new();
            *reference.encoder_mut() = HpackEncoder::with_table_size(huffman, 512);
            reference.set_encode_headroom(9);
            let mut codecs = [HpackCodec::new(), HpackCodec::new()];
            for codec in &mut codecs {
                *codec.encoder_mut() = HpackEncoder::with_table_size(huffman, 512);
            }
            let blocks = [
                (
                    vec![],
                    vec![
                        input(HttpHeaderCode::Other, b"X-Lookup", b"old"),
                        input(HttpHeaderCode::None, b"x-lookup", b"new"),
                        input(HttpHeaderCode::Other, b"Allow", b""),
                        input(HttpHeaderCode::None, b"X-\x80", b"\0\xff"),
                    ],
                ),
                (
                    vec![],
                    vec![
                        input(HttpHeaderCode::None, b"x-LoOkUp", b"old"),
                        input(HttpHeaderCode::Other, b"X-LOOKUP", b"new"),
                        input(HttpHeaderCode::Other, b"X-Lookup", b"changed"),
                        input(HttpHeaderCode::None, b"ALLOW", b""),
                        input(HttpHeaderCode::Other, b"x-\x80", b"\0\xff"),
                    ],
                ),
                (
                    vec![],
                    vec![
                        input(HttpHeaderCode::None, b"Accept-Encoding", b"gzip, deflate"),
                        input(HttpHeaderCode::Other, b"Content-Length", b"1024"),
                        input(HttpHeaderCode::ColonMethod, b"x-conflicting-name", b"GET"),
                        input(HttpHeaderCode::Other, b"X-\x81", b"\0\xff"),
                        input(HttpHeaderCode::None, b"x-\x80", b"\0\xff"),
                    ],
                ),
                (
                    vec![96],
                    vec![
                        input(HttpHeaderCode::Other, b"x-evict", &[b'q'; 40]),
                        input(HttpHeaderCode::None, b"X-LOOKUP", b"old"),
                        input(HttpHeaderCode::Other, b"x-lookup", b"new"),
                        input(HttpHeaderCode::None, b"X-Lookup", b"old"),
                    ],
                ),
                (
                    vec![0],
                    vec![
                        input(HttpHeaderCode::Other, b"x-lookup", b"old"),
                        input(HttpHeaderCode::None, b"allow", b""),
                        input(HttpHeaderCode::Other, b"Date", b"date"),
                    ],
                ),
                (
                    vec![256],
                    vec![
                        input(HttpHeaderCode::Other, b"X-Lookup", b"old"),
                        input(HttpHeaderCode::None, b"x-lookup", b"old"),
                        input(HttpHeaderCode::Other, b"allow", b""),
                        input(HttpHeaderCode::None, b"ALLOW", b""),
                    ],
                ),
            ];
            for (ordinal, (updates, inputs)) in blocks.into_iter().enumerate() {
                for size in updates {
                    reference.set_encoder_header_table_size(size);
                    for codec in &mut codecs {
                        codec.set_encoder_header_table_size(size);
                    }
                }
                let mut owned: Vec<_> = inputs
                    .iter()
                    .map(|header| HpackHeader::new(header.name, header.value))
                    .collect();
                if ordinal == 2 {
                    owned[2] = HpackHeader::new(b":method", b"GET");
                }
                let expected = reference.encode_block(&owned).into_bytes();
                for (codec, maximum) in codecs.iter_mut().zip([16 << 10, expected.len()]) {
                    let before = codec.encoder().clone();
                    let before_size = codec.encoded_size();
                    assert!(
                        codec
                            .encode_inputs_block(&Inputs(&inputs), expected.len() - 1, 9)
                            .is_err()
                    );
                    assert_context(codec.encoder().context(), before.context());
                    assert_eq!(codec.encoded_size(), before_size);
                    let encoded = codec
                        .encode_inputs_block(&Inputs(&inputs), maximum, 9)
                        .unwrap();
                    assert_eq!(encoded.headroom(), 9);
                    assert_eq!(encoded.as_bytes(), expected);
                    assert_context(codec.encoder().context(), reference.encoder().context());
                }
            }
        }
    }

    #[test]
    fn custom_strategy_panic_preserves_queue_context_and_retry_state() {
        struct PanicAfterInsertion {
            calls: Cell<u32>,
            clones: Rc<Cell<u32>>,
        }

        impl HeaderIndexingStrategy for PanicAfterInsertion {
            fn index_header(&self, name: &HeaderName, _value: &[u8], _name_exists: bool) -> bool {
                self.calls.set(self.calls.get() + 1);
                assert_ne!(
                    name.as_bytes(),
                    b"x-panic",
                    "scripted indexing strategy panic"
                );
                self.calls.get() == 1
            }

            fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
                self.clones.set(self.clones.get() + 1);
                Box::new(Self {
                    calls: Cell::new(self.calls.get()),
                    clones: Rc::clone(&self.clones),
                })
            }
        }

        for (codec_setter, borrowed) in [(false, false), (true, false), (false, true), (true, true)]
        {
            let mut codec = HpackCodec::new();
            codec.set_encoder_header_table_size(256);
            codec.encode(&headers(&[("x-seed", "kept")]));
            let strategy = Rc::new(PanicAfterInsertion {
                calls: Cell::new(0),
                clones: Rc::new(Cell::new(0)),
            });
            if codec_setter {
                codec.set_header_indexing_strategy(strategy.clone());
            } else {
                codec
                    .encoder_mut()
                    .set_header_indexing_strategy(strategy.clone());
            }
            codec.set_encoder_header_table_size(128);
            codec.set_encoder_header_table_size(256);
            let before_info = codec.compression_info().egress;
            let before_size = codec.encoded_size();
            let mut reference = codec.encoder().clone();
            let failing = headers(&[
                (":method", "GET"),
                ("x-first", "inserted only into the snapshot"),
                ("x-panic", "fail after insertion"),
            ]);
            let mut rejected = BufQueue::new(0);
            let capacity_probe = headers(&[("x-capacity", "not committed")]);
            let result = if borrowed {
                codec
                    .encode_borrowed_to_buf_queue(&borrowed_headers(&capacity_probe), &mut rejected)
            } else {
                codec.encode_to_buf_queue(&capacity_probe, &mut rejected)
            };
            assert!(result.is_err());
            assert!(rejected.is_empty());
            assert_eq!(strategy.calls.get(), 0);
            assert_eq!(codec.compression_info().egress, before_info);
            assert_eq!(codec.encoded_size(), before_size);
            let mut queue = BufQueue::new(16 << 10);
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if borrowed {
                    codec.encode_borrowed_to_buf_queue(&borrowed_headers(&failing), &mut queue)
                } else {
                    codec.encode_to_buf_queue(&failing, &mut queue)
                }
            }));
            assert!(panicked.is_err());
            assert!(queue.is_empty());
            assert_eq!(strategy.calls.get(), 0);
            assert_eq!(codec.compression_info().egress, before_info);
            assert_eq!(codec.encoded_size(), before_size);
            assert!(
                codec
                    .encoder()
                    .context()
                    .table()
                    .entries()
                    .eq(reference.context().table().entries())
            );
            for values in [
                headers(&[
                    (":method", "GET"),
                    ("x-retry", "indexed"),
                    ("x-next", "not indexed"),
                ]),
                headers(&[("x-followup", "still not indexed")]),
            ] {
                let expected = reference.encode(&values);
                let mut queue = BufQueue::new(16 << 10);
                let result = if borrowed {
                    codec.encode_borrowed_to_buf_queue(&borrowed_headers(&values), &mut queue)
                } else {
                    codec.encode_to_buf_queue(&values, &mut queue)
                };
                assert_eq!(result, Ok(expected.len()));
                assert_eq!(
                    queue.coalesce(queue.chain_length()).unwrap().as_ref(),
                    expected
                );
                assert!(
                    codec
                        .encoder()
                        .context()
                        .table()
                        .entries()
                        .eq(reference.context().table().entries())
                );
                assert_eq!(
                    codec.encoder().context().static_refs(),
                    reference.context().static_refs()
                );
                assert_eq!(
                    codec.encoder().context().table().insert_count(),
                    reference.context().table().insert_count()
                );
            }
        }
        for codec_entry in [false, true] {
            let strategy = Rc::new(PanicAfterInsertion {
                calls: Cell::new(0),
                clones: Rc::new(Cell::new(0)),
            });
            let mut codec = HpackCodec::new();
            codec.set_header_indexing_strategy(strategy.clone());
            let failing = headers(&[("x-first", "retained"), ("x-panic", "original panic")]);
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if codec_entry {
                    codec.encode_borrowed(&borrowed_headers(&failing));
                } else {
                    codec
                        .encoder_mut()
                        .encode_borrowed(&borrowed_headers(&failing));
                }
            }));
            assert!(panicked.is_err());
            assert_eq!(strategy.calls.get(), 2);
            assert_eq!(strategy.clones.get(), 0);
            assert_eq!(codec.encoder().context().table().size(), 1);
            assert_eq!(
                codec.encoder().context().table().entries().next(),
                Some(&HpackHeader::new("x-first", "retained"))
            );
            let retry = headers(&[("x-retry", "not indexed after direct panic")]);
            let encoded = if codec_entry {
                codec.encode_borrowed(&borrowed_headers(&retry))
            } else {
                codec
                    .encoder_mut()
                    .encode_borrowed(&borrowed_headers(&retry))
                    .0
            };
            assert_eq!(HpackDecoder::new().decode(&encoded).unwrap().headers, retry);
            assert_eq!(strategy.calls.get(), 3);
            assert_eq!(strategy.clones.get(), 0);
            assert_eq!(codec.encoder().context().table().size(), 1);
        }
    }

    #[test]
    fn exact_fit_table_update_and_indexed_header_succeed() {
        let mut update_encoder = HpackEncoder::new(false);
        update_encoder.set_header_table_size(0);
        let mut update_queue = BufQueue::new(1);
        assert_eq!(
            update_encoder.encode_to_buf_queue(&[], &mut update_queue),
            Ok(1)
        );
        assert_eq!(
            update_queue.front().map(|bytes| bytes.as_ref()),
            Some(&[0x20][..])
        );

        let mut indexed_encoder = HpackEncoder::new(false);
        let mut indexed_queue = BufQueue::new(1);
        assert_eq!(
            indexed_encoder
                .encode_to_buf_queue(&headers(&[(":method", "GET")]), &mut indexed_queue,),
            Ok(1)
        );
        assert_eq!(
            indexed_queue.front().map(|bytes| bytes.as_ref()),
            Some(&[0x82][..])
        );
    }

    #[test]
    fn table_size_updates_preserve_smallest_then_final_value() {
        let mut encoder = HpackEncoder::new(false);
        encoder.set_header_table_size(0);
        encoder.set_header_table_size(DEFAULT_TABLE_SIZE);

        let encoded = encoder.encode(&[]);
        assert_eq!(encoded, hex("203fe11f"));
        let mut decoder = HpackDecoder::new();
        assert!(decoder.decode(&encoded).is_ok());
        assert_eq!(decoder.context().table().capacity(), DEFAULT_TABLE_SIZE);
        assert!(encoder.encode(&[]).is_empty());
    }

    #[test]
    fn decoder_rejects_late_table_size_update_and_sticks() {
        let mut decoder = HpackDecoder::new();
        assert_eq!(
            decoder.decode(&[0x82, 0x20]),
            Err(HpackError::InvalidTableSizeUpdate)
        );
        assert_eq!(decoder.error(), Some(HpackError::InvalidTableSizeUpdate));
        assert_eq!(
            decoder.decode(&[0x82]),
            Err(HpackError::InvalidTableSizeUpdate)
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    enum DecodeEvent {
        Header(HpackHeader),
        Complete(HttpHeaderSize),
        Error(HpackError),
    }

    #[derive(Default)]
    struct DecodeTrace(Vec<DecodeEvent>);

    impl StreamingCallback for DecodeTrace {
        fn on_header(&mut self, name: &HeaderName, value: &[u8]) {
            self.0.push(DecodeEvent::Header(HpackHeader::from_name(
                name.clone(),
                value,
            )));
        }

        fn on_headers_complete(&mut self, size: HttpHeaderSize, acknowledge: bool) {
            assert!(!acknowledge);
            self.0.push(DecodeEvent::Complete(size));
        }

        fn on_decode_error(&mut self, error: HpackError) {
            self.0.push(DecodeEvent::Error(error));
        }
    }

    fn decode_streaming_input(
        decoder: &mut HpackDecoder,
        wire: &[u8],
        fragmented: bool,
        callback: &mut DecodeTrace,
    ) -> Result<HttpHeaderSize, HpackError> {
        if fragmented {
            let mut queue = BufQueue::new(wire.len());
            for byte in wire {
                queue.push_copy(&[*byte]).unwrap();
            }
            decoder.decode_streaming_buf_queue(&queue, wire.len(), callback)
        } else {
            decoder.decode_streaming(wire, callback)
        }
    }

    /// A consumer-shaped request: 21 fields whose names and values are all
    /// Huffman-coded literals. The long id, routing key, routing-key copy and
    /// `date` values are not indexed; the other values are.
    const CONSUMER_REQUEST: [(&[u8], &[u8], bool); 21] = [
        (b":method", b"POST", true),
        (b":scheme", b"https", true),
        (b":authority", b"api.service.example.com", true),
        (b":path", b"/rpc/v2/inference.Service/Generate", true),
        (b"content-type", b"application/x-thrift", true),
        (b"te", b"trailers", true),
        (
            b"user-agent",
            b"consumer-client/4.2.1 (linux; x86_64)",
            true,
        ),
        (b"accept", b"*/*", true),
        (b"accept-encoding", b"gzip, deflate", true),
        (b"date", b"Sun, 06 Nov 1994 08:49:37 GMT", false),
        (b"content-length", b"1024", true),
        (b"cache-control", b"no-cache", true),
        (
            b"x-request-id",
            b"api.proxy.3f9a0c7e9b1d4f2a6c8e0b3d-0123456789abcdef0123456789abcdef",
            false,
        ),
        (
            b"x-routing-key",
            b"session-00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210-4096",
            false,
        ),
        (
            b"x-routing-key-copy",
            b"session-00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210-4096",
            false,
        ),
        (b"x-token-count", b"2048", true),
        (b"x-prefix-hash", b"9e3779b97f4a7c15", true),
        (b"x-timeout-ms", b"30000", true),
        (b"x-pad-req", b"pppppppppppppppppppppppppppppppp", true),
        (b"x-pad-resp", b"0", true),
        (b"x-upstream-time", b"137", true),
    ];

    #[test]
    fn consumer_shaped_literals_decode_in_one_pass() {
        assert_eq!(CONSUMER_REQUEST[12].1.len(), 67);
        assert_eq!(CONSUMER_REQUEST[13].1.len(), 77);
        let mut encoded = HpackEncodeBuffer::new(true);
        for (name, value, indexed) in CONSUMER_REQUEST {
            let instruction = if indexed { LITERAL_INC_INDEX } else { LITERAL };
            encoded.encode_integer(0, instruction);
            encoded.encode_literal(name);
            encoded.encode_literal(value);
        }
        let wire = encoded.into_bytes();
        let expected: Vec<DecodeEvent> = CONSUMER_REQUEST
            .iter()
            .map(|(name, value, _)| DecodeEvent::Header(HpackHeader::new(name, value)))
            .collect();

        for fragmented in [false, true] {
            let mut decoder = HpackDecoder::new();
            let mut callback = DecodeTrace::default();
            let size = decode_streaming_input(&mut decoder, &wire, fragmented, &mut callback)
                .expect("the consumer-shaped block decodes");
            assert_eq!(
                callback.0.split_last().map(|(last, _)| last),
                Some(&DecodeEvent::Complete(size))
            );
            assert_eq!(callback.0[..callback.0.len() - 1], expected);
        }

        let mut decoder = HpackDecoder::new();
        let mut callback = DecodeTrace::default();
        decoder
            .decode_streaming(&wire, &mut callback)
            .expect("the consumer-shaped block decodes");
        let mut stream = Vec::new();
        for event in &callback.0 {
            if let DecodeEvent::Header(header) = event {
                stream.extend_from_slice(header.name.as_bytes());
                stream.push(0);
                stream.extend_from_slice(&header.value);
                stream.push(0);
            }
        }
        assert_eq!(
            (fnv1a64(&wire), wire.len(), fnv1a64(&stream), stream.len()),
            (0xce16_dfde_03cd_71f5, 572, 0xf681_d188_c263_467d, 732),
            "recorded before any Huffman decoder change"
        );
    }

    /// Names whose values change on every message; a consumer sends them without indexing.
    const UNINDEXED_NAMES: [&[u8]; 5] = [
        b"x-request-id",
        b"x-routing-key",
        b"x-routing-key-copy",
        b"date",
        b"x-upstream-time",
    ];

    struct ConsumerIndexing;

    impl HeaderIndexingStrategy for ConsumerIndexing {
        fn index_header(&self, name: &HeaderName, value: &[u8], name_exists: bool) -> bool {
            !UNINDEXED_NAMES.contains(&name.as_bytes())
                && DefaultIndexingStrategy.index_header(name, value, name_exists)
        }

        fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
            Box::new(Self)
        }
    }

    fn scramble(seed: u64) -> u64 {
        let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn hex_digits(seed: u64, count: usize) -> String {
        (0..count)
            .map(|digit| {
                let word = scramble(seed.wrapping_mul(64).wrapping_add((digit / 16) as u64));
                char::from(b"0123456789abcdef"[((word >> (4 * (digit % 16))) & 0xf) as usize])
            })
            .collect()
    }

    /// A consumer-shaped request: a 67-byte id and a 77-byte routing key, sent twice, all
    /// without indexing, next to fields that are indexed once and referenced afterwards.
    fn consumer_request(sequence: u64) -> Vec<HpackHeader> {
        let request_id = format!(
            "api.proxy.{}-{}",
            hex_digits(4 * sequence, 24),
            hex_digits(4 * sequence + 1, 32)
        );
        let routing_key = format!("session-{}-4096", hex_digits(4 * sequence + 2, 64));
        let token_count = (1 + scramble(4 * sequence + 3) % 4096).to_string();
        let prefix_hash = hex_digits(4 * sequence + 3, 16);
        let fields: [(&str, &str); 18] = [
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "api.service.example.com"),
            (":path", "/rpc/v2/inference.Service/Generate"),
            ("content-type", "application/x-thrift"),
            ("te", "trailers"),
            ("user-agent", "consumer-client/4.2.1 (linux; x86_64)"),
            ("accept", "*/*"),
            ("accept-encoding", "gzip, deflate"),
            ("content-length", "1024"),
            ("x-request-id", &request_id),
            ("x-routing-key", &routing_key),
            ("x-routing-key-copy", &routing_key),
            ("x-token-count", &token_count),
            ("x-prefix-hash", &prefix_hash),
            ("x-timeout-ms", "30000"),
            ("x-pad-req", "pppppppppppppppppppppppppppppppp"),
            ("cache-control", "no-cache"),
        ];
        headers(&fields)
    }

    /// A consumer-shaped response: a fixed `date` and a short timing value without indexing, and a
    /// 107-byte padding value that the first block indexes.
    fn consumer_response(sequence: u64) -> Vec<HpackHeader> {
        let padding: String = b"0123456789abcdefghijklmnopqrstuvwxyz"
            .iter()
            .cycle()
            .take(107)
            .map(|byte| char::from(*byte))
            .collect();
        let upstream_time = (100 + scramble(sequence) % 900).to_string();
        headers(&[
            (":status", "200"),
            ("content-type", "application/x-thrift"),
            ("date", "Sun, 06 Nov 1994 08:49:37 GMT"),
            ("x-pad-resp", &padding),
            ("x-upstream-time", &upstream_time),
        ])
    }

    fn consumer_encoder() -> HpackEncoder {
        let mut encoder = HpackEncoder::new(true);
        encoder.set_header_indexing_strategy(Rc::new(ConsumerIndexing));
        encoder
    }

    fn decoded_headers(trace: &DecodeTrace) -> Vec<&HpackHeader> {
        trace
            .0
            .iter()
            .filter_map(|event| match event {
                DecodeEvent::Header(header) => Some(header),
                _ => None,
            })
            .collect()
    }

    fn append_field(stream: &mut Vec<u8>, header: &HpackHeader) {
        stream.extend_from_slice(header.name.as_bytes());
        stream.push(0);
        stream.extend_from_slice(&header.value);
        stream.push(0);
    }

    #[test]
    fn consumer_shaped_blocks_decode_identically() {
        assert_eq!(consumer_request(0)[10].value.len(), 67);
        assert_eq!(consumer_request(0)[11].value.len(), 77);
        assert_eq!(consumer_response(0)[3].value.len(), 107);
        let builders: [fn(u64) -> Vec<HpackHeader>; 2] = [consumer_request, consumer_response];
        let digests: Vec<_> = builders
            .iter()
            .map(|build| {
                let mut encoder = consumer_encoder();
                let mut decoder = HpackDecoder::new();
                let mut wire = Vec::new();
                let mut stream = Vec::new();
                for sequence in 0..64 {
                    let fields = build(sequence);
                    let block = encoder.encode(&fields);
                    let mut trace = DecodeTrace::default();
                    decoder
                        .decode_streaming(&block, &mut trace)
                        .expect("the consumer-shaped block decodes");
                    let decoded = decoded_headers(&trace);
                    assert_eq!(decoded, fields.iter().collect::<Vec<_>>());
                    wire.extend_from_slice(&block);
                    decoded
                        .into_iter()
                        .for_each(|header| append_field(&mut stream, header));
                }
                let table = decoder.context().table();
                let mut entries = Vec::new();
                (1..=table.size())
                    .filter_map(|index| table.get_header(index))
                    .for_each(|header| append_field(&mut entries, header));
                (
                    fnv1a64(&wire),
                    wire.len(),
                    fnv1a64(&stream),
                    stream.len(),
                    table.size(),
                    fnv1a64(&entries),
                    entries.len(),
                )
            })
            .collect();
        assert_eq!(
            digests,
            [
                (
                    0x1360_a26f_7257_e4fb,
                    15547,
                    0x46ed_55da_9554_f046,
                    42476,
                    73,
                    0x6b77_479f_7fdd_f242,
                    1860
                ),
                (
                    0x7c37_1f52_ddee_582e,
                    3040,
                    0x9cad_e472_6245_a1e2,
                    14080,
                    2,
                    0x253f_777c_c9f7_89bf,
                    153
                ),
            ],
            "recorded before any literal-scratch change"
        );
    }

    #[test]
    fn long_huffman_values_decode_without_a_heap_buffer() {
        let fields = consumer_request(0);
        let block = consumer_encoder().encode(&fields);
        let mut decoder = HpackDecoder::new();
        let mut trace = DecodeTrace::default();
        HpackDecodeBuffer::take_huffman_heap_decodes();
        decoder
            .decode_streaming(&block, &mut trace)
            .expect("the consumer-shaped block decodes");
        assert_eq!(
            HpackDecodeBuffer::take_huffman_heap_decodes(),
            0,
            "the id and routing-key values decode into the decoder's scratch"
        );
        assert_eq!(decoded_headers(&trace), fields.iter().collect::<Vec<_>>());
    }

    #[test]
    fn oversized_huffman_literal_still_decodes() {
        // Every symbol of the value has a five-bit code: 400 bytes encode to 250.
        let value: Vec<u8> = b"012aceiost".iter().copied().cycle().take(400).collect();
        for (instruction, indexed) in [(LITERAL, false), (LITERAL_INC_INDEX, true)] {
            let mut encoded = HpackEncodeBuffer::new(true);
            encoded.encode_integer(0, instruction);
            encoded.encode_literal(b"x-pad-resp");
            let value_bytes = encoded.encode_literal(&value);
            assert_eq!(
                value_bytes,
                2 + 250,
                "a two-byte length and 250 encoded bytes"
            );
            let block = encoded.into_bytes();

            let mut decoder = HpackDecoder::new();
            let mut trace = DecodeTrace::default();
            HpackDecodeBuffer::take_huffman_heap_decodes();
            decoder
                .decode_streaming(&block, &mut trace)
                .expect("the oversized literal decodes");
            assert_eq!(
                HpackDecodeBuffer::take_huffman_heap_decodes(),
                1,
                "only the oversized value takes the heap path"
            );
            let expected = HpackHeader::new(b"x-pad-resp", &value);
            assert_eq!(decoded_headers(&trace), [&expected]);
            let table = decoder.context().table();
            if indexed {
                assert_eq!(table.size(), 1);
                assert_eq!(table.get_header(1), Some(&expected));
            } else {
                assert_eq!(table.size(), 0);
            }
        }
    }

    #[test]
    fn indexed_huffman_value_lands_in_the_table() {
        let routing_key = consumer_request(0)[11].clone();
        assert_eq!(routing_key.value.len(), 77);
        let mut encoded = HpackEncodeBuffer::new(true);
        encoded.encode_integer(0, LITERAL_INC_INDEX);
        encoded.encode_literal(routing_key.name.as_bytes());
        encoded.encode_literal(&routing_key.value);
        let block = encoded.into_bytes();

        let mut decoder = HpackDecoder::new();
        let mut trace = DecodeTrace::default();
        decoder
            .decode_streaming(&block, &mut trace)
            .expect("the indexed literal decodes");
        assert_eq!(decoded_headers(&trace), [&routing_key]);
        let entry = decoder
            .context()
            .table()
            .get_header(1)
            .expect("the field was indexed");
        assert_eq!(entry, &routing_key);
        assert_eq!(entry.value.len(), 77, "the table holds the exact value");

        let mut trace = DecodeTrace::default();
        decoder
            .decode_streaming(&[0x80 | 62], &mut trace)
            .expect("the first dynamic entry is referenced");
        assert_eq!(decoded_headers(&trace), [&routing_key]);
    }

    #[test]
    fn split_segment_literals_are_unchanged() {
        let fields = consumer_request(0);
        for huffman in [false, true] {
            let mut encoder = HpackEncoder::new(huffman);
            encoder.set_header_indexing_strategy(Rc::new(ConsumerIndexing));
            let block = encoder.encode(&fields);
            let mut contiguous = DecodeTrace::default();
            HpackDecoder::new()
                .decode_streaming(&block, &mut contiguous)
                .expect("the contiguous block decodes");
            assert_eq!(
                decoded_headers(&contiguous),
                fields.iter().collect::<Vec<_>>()
            );

            // Every offset, so every literal, the longest included, is split at each of its bytes.
            for split in 1..block.len() {
                let mut queue = BufQueue::new(block.len());
                queue.push_copy(&block[..split]).unwrap();
                queue.push_copy(&block[split..]).unwrap();
                let mut trace = DecodeTrace::default();
                HpackDecoder::new()
                    .decode_streaming_buf_queue(&queue, block.len(), &mut trace)
                    .expect("the split block decodes");
                assert_eq!(trace.0, contiguous.0, "huffman {huffman}, split at {split}");
            }
        }
    }

    #[test]
    fn nonindexed_huffman_literals_preserve_boundary_values_and_completion() {
        for instruction in [LITERAL, LITERAL_NEV_INDEX] {
            for (value, encoded_length) in [
                (Vec::new(), 0),
                (b"1024".to_vec(), 3),
                (vec![b'0'; 32], 20),
                (vec![b'0'; 33], 21),
            ] {
                assert_eq!(huffman_encoded_size(&value), encoded_length);
                let mut encoded = HpackEncodeBuffer::new(true);
                encoded.encode_integer(4, instruction);
                encoded.encode_literal(&value);
                let wire = encoded.into_bytes();
                let size = HttpHeaderSize {
                    compressed: wire.len() as u32,
                    compressed_block: wire.len() as u32,
                    uncompressed: (5 + value.len() + 2) as u32,
                };
                for fragmented in [false, true] {
                    let mut decoder =
                        HpackDecoder::with_limits(DEFAULT_TABLE_SIZE, (5 + value.len()) as u64);
                    let mut callback = DecodeTrace::default();
                    assert_eq!(
                        decode_streaming_input(&mut decoder, &wire, fragmented, &mut callback),
                        Ok(size)
                    );
                    assert_eq!(
                        callback.0,
                        [
                            DecodeEvent::Header(HpackHeader::new(b":path", &value)),
                            DecodeEvent::Complete(size),
                        ]
                    );
                    assert_eq!(decoder.context().table().size(), 0);
                    assert_eq!(decoder.context().table().insert_count(), 0);
                }
            }
        }
    }

    #[test]
    fn nonindexed_huffman_errors_do_not_emit_partial_headers() {
        for instruction in [LITERAL, LITERAL_NEV_INDEX] {
            for literal in [&[0xf8, 0xff][..], &[0xf8, 0xff, 0xff, 0xff, 0xff], &[0x2e]] {
                let mut wire = vec![instruction.code | 4, 0x80 | literal.len() as u8];
                wire.extend_from_slice(literal);
                for fragmented in [false, true] {
                    let mut decoder = HpackDecoder::new();
                    let mut callback = DecodeTrace::default();
                    assert_eq!(
                        decode_streaming_input(&mut decoder, &wire, fragmented, &mut callback),
                        Err(HpackError::InvalidHuffmanCode)
                    );
                    assert_eq!(
                        callback.0,
                        [DecodeEvent::Error(HpackError::InvalidHuffmanCode)]
                    );
                    assert_eq!(decoder.error(), Some(HpackError::InvalidHuffmanCode));
                    callback.0.clear();
                    assert_eq!(
                        decode_streaming_input(&mut decoder, &[0x84], fragmented, &mut callback),
                        Err(HpackError::InvalidHuffmanCode)
                    );
                    assert_eq!(
                        callback.0,
                        [DecodeEvent::Error(HpackError::InvalidHuffmanCode)]
                    );
                    assert_eq!(decoder.context().table().size(), 0);
                }
            }
        }
    }

    #[test]
    fn nonindexed_literal_errors_preserve_validation_and_callback_order() {
        for instruction in [LITERAL, LITERAL_NEV_INDEX] {
            for fragmented in [false, true] {
                for (literal, error) in [
                    (&[0x82, 0xf8][..], HpackError::BufferUnderflow),
                    (&[0x82, 0xf8, 0xff], HpackError::LiteralTooLarge),
                ] {
                    let mut wire = vec![instruction.code | 4];
                    wire.extend_from_slice(literal);
                    let mut decoder = HpackDecoder::with_limits(DEFAULT_TABLE_SIZE, 1);
                    let mut callback = DecodeTrace::default();
                    assert_eq!(
                        decode_streaming_input(&mut decoder, &wire, fragmented, &mut callback),
                        Err(error)
                    );
                    assert_eq!(callback.0, [DecodeEvent::Error(error)]);
                }
                let value = [b'0'; 32];
                let mut encoded = HpackEncodeBuffer::new(true);
                encoded.encode_integer(4, instruction);
                encoded.encode_literal(&value);
                let mut decoder = HpackDecoder::with_limits(DEFAULT_TABLE_SIZE, 36);
                let mut callback = DecodeTrace::default();
                assert_eq!(
                    decode_streaming_input(
                        &mut decoder,
                        encoded.as_slice(),
                        fragmented,
                        &mut callback,
                    ),
                    Err(HpackError::HeadersTooLarge)
                );
                assert_eq!(
                    callback.0,
                    [
                        DecodeEvent::Header(HpackHeader::new(b":path", value)),
                        DecodeEvent::Error(HpackError::HeadersTooLarge),
                    ]
                );
                assert_eq!(decoder.error(), Some(HpackError::HeadersTooLarge));
                assert_eq!(decoder.context().table().size(), 0);
            }
        }
    }

    #[test]
    fn uncompressed_limit_excludes_per_header_accounting_overhead() {
        let encoded = HpackEncoder::new(false).encode(&headers(&[("x", "y")]));
        let decoded = HpackDecoder::with_limits(DEFAULT_TABLE_SIZE, 2)
            .decode(&encoded)
            .expect("exact real-byte limit succeeds");
        assert_eq!(decoded.size.uncompressed, 4);
        assert_eq!(
            HpackDecoder::with_limits(DEFAULT_TABLE_SIZE, 1).decode(&encoded),
            Err(HpackError::HeadersTooLarge)
        );
    }

    #[test]
    fn context_update_parameter_matrix() {
        for set_decoder_size in [true, false] {
            let mut encoder = HpackEncoder::new(true);
            encoder.set_header_table_size(8192);
            let mut decoder = HpackDecoder::new();
            if set_decoder_size {
                decoder.set_header_table_max_size(8192);
            }
            let encoded = encoder.encode(&headers(&[("x-fb-random", "bla")]));
            let result = decoder.decode(&encoded);
            if set_decoder_size {
                assert!(result.is_ok());
            } else {
                assert_eq!(result, Err(HpackError::InvalidTableSize));
            }
        }
    }
}

mod hpack_codec {
    use super::*;

    fn basic_headers() -> Vec<HpackHeader> {
        headers(&[
            (":path", "/index.php"),
            (":authority", "www.facebook.com"),
            (":method", "GET"),
            (":scheme", "https"),
            ("Host", "www.facebook.com"),
            ("accept-encoding", "gzip"),
        ])
    }

    #[derive(Default)]
    struct Stats {
        encodes: Cell<u32>,
        decodes: Cell<u32>,
        errors: Cell<u32>,
        too_large: Cell<u32>,
        encoded: Cell<HttpHeaderSize>,
        decoded: Cell<HttpHeaderSize>,
    }

    impl HeaderCodecStats for Stats {
        fn record_encode(&self, size: HttpHeaderSize) {
            self.encodes.set(self.encodes.get() + 1);
            self.encoded.set(size);
        }

        fn record_decode(&self, size: HttpHeaderSize) {
            self.decodes.set(self.decodes.get() + 1);
            self.decoded.set(size);
        }

        fn record_decode_error(&self) {
            self.errors.set(self.errors.get() + 1);
        }

        fn record_decode_too_large(&self) {
            self.too_large.set(self.too_large.get() + 1);
        }
    }

    #[derive(Clone, Copy)]
    enum ExpectedTerminal {
        Complete,
        Error,
        TooLarge,
    }

    struct OrderObservingCallback {
        stats: Rc<Stats>,
        expected: ExpectedTerminal,
        terminal_seen: bool,
    }

    impl StreamingCallback for OrderObservingCallback {
        fn on_header(&mut self, _name: &HeaderName, _value: &[u8]) {}

        fn on_headers_complete(&mut self, _decoded_size: HttpHeaderSize, _acknowledge: bool) {
            assert!(matches!(self.expected, ExpectedTerminal::Complete));
            assert_eq!(self.stats.decodes.get(), 1);
            assert_eq!(self.stats.errors.get(), 0);
            assert_eq!(self.stats.too_large.get(), 0);
            self.terminal_seen = true;
        }

        fn on_decode_error(&mut self, _error: HpackError) {
            match self.expected {
                ExpectedTerminal::Complete => panic!("expected successful completion"),
                ExpectedTerminal::Error => {
                    assert_eq!(self.stats.errors.get(), 1);
                    assert_eq!(self.stats.too_large.get(), 0);
                }
                ExpectedTerminal::TooLarge => {
                    assert_eq!(self.stats.errors.get(), 0);
                    assert_eq!(self.stats.too_large.get(), 1);
                }
            }
            self.terminal_seen = true;
        }
    }

    fn round_trip(encoder: &mut HpackCodec, decoder: &mut HpackCodec, values: &[HpackHeader]) {
        let encoded = encoder.encode(values);
        let decoded = decoder.decode(&encoded).expect("header block decodes");
        assert_eq!(decoded.headers, values);
    }

    #[test]
    fn request() {
        let mut client = HpackCodec::new();
        let mut server = HpackCodec::new();
        for _ in 0..3 {
            round_trip(&mut client, &mut server, &basic_headers());
        }
    }

    #[test]
    fn response() {
        let mut server = HpackCodec::new();
        let mut client = HpackCodec::new();
        for _ in 0..3 {
            round_trip(&mut server, &mut client, &basic_headers());
        }
    }

    #[test]
    fn headroom() {
        let mut client = HpackCodec::new();
        client.set_encode_headroom(20);
        let mut encoded = client.encode_block(&basic_headers());
        assert_eq!(encoded.headroom(), 20);
        let bytes = encoded.as_bytes().to_vec();
        assert!(encoded.prepend(&[0; 9]));
        assert_eq!(encoded.headroom(), 11);
        assert_eq!(&encoded.as_bytes()[9..], bytes);
    }

    #[test]
    fn lowercasing_header_names() {
        let input = [
            ("Content-Length", "80"),
            ("Content-Encoding", "gzip"),
            ("X-FB-Debug", "bleah"),
        ];
        let mut values: Vec<_> = input
            .iter()
            .map(|(name, value)| Header::new(HttpHeaderName::from(*name), value.as_bytes()))
            .collect();
        values[2].code = HttpHeaderCode::ContentLength;
        let encoded = HpackCodec::new().encode_borrowed(&values);
        let mut queued = BufQueue::new(4096);
        HpackCodec::new()
            .encode_borrowed_to_buf_queue(&values, &mut queued)
            .unwrap();
        assert_eq!(
            queued.coalesce(queued.chain_length()).unwrap().as_ref(),
            encoded
        );
        let (direct, uncompressed) = HpackEncoder::new(true).encode_borrowed(&values);
        assert_eq!(direct, encoded);
        let decoded = HpackCodec::new().decode(&encoded).expect("decode");
        assert_eq!(uncompressed, decoded.size.uncompressed);
        assert_eq!(decoded.headers, headers(&input));
        assert!(decoded.headers.iter().all(|header| {
            header
                .name
                .as_bytes()
                .iter()
                .all(|byte| !byte.is_ascii_alphabetic() || byte.is_ascii_lowercase())
        }));
    }

    #[test]
    fn multivalue_headers() {
        let values = headers(&[
            ("Content-Length", "80"),
            ("Content-Encoding", "gzip"),
            ("X-FB-Dup", "bleah"),
            ("X-FB-Dup", "hahaha"),
        ]);
        let encoded = HpackCodec::new().encode(&values);
        let decoded = HpackCodec::new().decode(&encoded).expect("decode");
        assert_eq!(
            decoded
                .headers
                .iter()
                .filter(|header| header.name.as_bytes() == b"x-fb-dup")
                .count(),
            2
        );
    }

    #[test]
    fn decode_error() {
        let stats = Rc::new(Stats::default());
        let stats_object: Rc<dyn HeaderCodecStats> = stats.clone();
        let mut encoded = HpackCodec::new().encode(&headers(&[("content-length", "80")]));
        encoded[0] = 0xff;
        let mut decoder = HpackCodec::new();
        decoder.set_stats(Some(stats_object));
        assert_eq!(decoder.decode(&encoded), Err(HpackError::InvalidIndex));
        assert_eq!(stats.errors.get(), 1);
    }

    #[test]
    fn header_codec_stats() {
        for borrowed in [false, true] {
            let stats = Rc::new(Stats::default());
            let stats_object: Rc<dyn HeaderCodecStats> = stats.clone();
            let values = headers(&[("Content-Length", "80"), ("X-FB-Debug", "value")]);
            let mut encoder = HpackCodec::new();
            encoder.set_stats(Some(stats_object.clone()));
            let encoded = if borrowed {
                let inputs = borrowed_headers(&values);
                assert!(
                    encoder
                        .encode_borrowed_to_buf_queue(&inputs, &mut BufQueue::new(0))
                        .is_err()
                );
                assert_eq!(stats.encodes.get(), 0);
                let mut queue = BufQueue::new(4096);
                encoder
                    .encode_borrowed_to_buf_queue(&inputs, &mut queue)
                    .unwrap();
                queue.coalesce(queue.chain_length()).unwrap().to_vec()
            } else {
                encoder.encode(&values)
            };
            assert_eq!(stats.encodes.get(), 1);
            assert!(stats.encoded.get().compressed > 0);
            assert!(stats.encoded.get().compressed_block > 0);
            assert!(stats.encoded.get().uncompressed > 0);
            assert_eq!(stats.encoded.get(), encoder.encoded_size());
            let mut decoder = HpackCodec::new();
            decoder.set_stats(Some(stats_object));
            assert!(decoder.decode(&encoded).is_ok());
            assert_eq!(stats.decodes.get(), 1);
            assert!(stats.decoded.get().compressed > 0);
        }
    }

    #[test]
    fn streaming_stats_precede_terminal_callbacks() {
        let encoded = HpackEncoder::new(false).encode(&headers(&[("x", "y")]));
        let stats = Rc::new(Stats::default());
        let mut decoder = HpackCodec::new();
        decoder.set_stats(Some(stats.clone()));
        let mut callback = OrderObservingCallback {
            stats,
            expected: ExpectedTerminal::Complete,
            terminal_seen: false,
        };
        assert!(decoder.decode_streaming(&encoded, &mut callback).is_ok());
        assert!(callback.terminal_seen);

        let stats = Rc::new(Stats::default());
        let mut decoder = HpackCodec::new();
        decoder.set_stats(Some(stats.clone()));
        let mut callback = OrderObservingCallback {
            stats,
            expected: ExpectedTerminal::Error,
            terminal_seen: false,
        };
        assert_eq!(
            decoder.decode_streaming(&[0x80], &mut callback),
            Err(HpackError::InvalidIndex)
        );
        assert!(callback.terminal_seen);

        let stats = Rc::new(Stats::default());
        let mut decoder = HpackCodec::new();
        decoder.set_max_uncompressed(0);
        decoder.set_stats(Some(stats.clone()));
        let mut callback = OrderObservingCallback {
            stats,
            expected: ExpectedTerminal::TooLarge,
            terminal_seen: false,
        };
        assert_eq!(
            decoder.decode_streaming(&[0x82], &mut callback),
            Err(HpackError::HeadersTooLarge)
        );
        assert!(callback.terminal_seen);
    }

    #[test]
    fn uncompressed_size_limit() {
        let values: Vec<_> = (0..10_000)
            .map(|value| HpackHeader::new("content-length", value.to_string()))
            .collect();
        let encoded = HpackCodec::new().encode(&values);
        assert_eq!(
            HpackCodec::new().decode(&encoded),
            Err(HpackError::HeadersTooLarge)
        );
    }

    #[test]
    fn size_limit_stats() {
        let values: Vec<_> = (0..10_000)
            .map(|value| HpackHeader::new("content-length", value.to_string()))
            .collect();
        let encoded = HpackCodec::new().encode(&values);
        let stats = Rc::new(Stats::default());
        let stats_object: Rc<dyn HeaderCodecStats> = stats.clone();
        let mut decoder = HpackCodec::new();
        decoder.set_stats(Some(stats_object));
        assert_eq!(decoder.decode(&encoded), Err(HpackError::HeadersTooLarge));
        assert_eq!(stats.too_large.get(), 1);
    }

    #[test]
    fn default_header_indexing_strategy() {
        let mut encoder = HpackCodec::new();
        encoder.encode(&basic_headers());
        assert_eq!(encoder.compression_info().egress.headers_stored, 4);
        encoder.encode(&headers(&[
            ("content-length", "80"),
            (":path", "/some/random/file.jpg"),
            (":path", "checks_for_="),
            ("if-modified-since", "some_value"),
            ("last-modified", "some_value"),
        ]));
        assert_eq!(encoder.compression_info().egress.headers_stored, 4);
    }

    struct NeverIndex;

    impl HeaderIndexingStrategy for NeverIndex {
        fn index_header(&self, _name: &HeaderName, _value: &[u8], _name_exists: bool) -> bool {
            false
        }

        fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
            Box::new(Self)
        }

        fn huffman_limits(&self) -> (usize, usize) {
            (5, 10)
        }
    }

    struct ObserveDynamicName {
        saw_existing_name: Rc<Cell<bool>>,
    }

    impl HeaderIndexingStrategy for ObserveDynamicName {
        fn index_header(&self, _name: &HeaderName, _value: &[u8], name_exists: bool) -> bool {
            self.saw_existing_name
                .set(self.saw_existing_name.get() || name_exists);
            true
        }

        fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
            Box::new(Self {
                saw_existing_name: Rc::new(Cell::new(self.saw_existing_name.get())),
            })
        }
    }

    #[test]
    fn custom_header_indexing_strategy() {
        let mut codec = HpackCodec::new();
        codec.set_header_indexing_strategy(Rc::new(NeverIndex));
        let encoded = codec.encode(&basic_headers());
        assert_eq!(codec.compression_info().egress.headers_stored, 0);
        assert!(encoded.windows(4).any(|window| window == b"gzip"));
        assert!(
            encoded
                .windows(16)
                .any(|window| window == b"www.facebook.com")
        );
        assert!(!encoded.windows(10).any(|window| window == b"/index.php"));

        let saw_existing_name = Rc::new(Cell::new(false));
        let mut encoder = HpackEncoder::new(false);
        encoder.set_header_indexing_strategy(Rc::new(ObserveDynamicName {
            saw_existing_name: saw_existing_name.clone(),
        }));
        encoder.encode(&headers(&[("x-dynamic", "one"), ("x-dynamic", "two")]));
        assert!(saw_existing_name.get());
    }

    struct InputBlock<'a>(Vec<Header<'a>>);

    impl crate::hpack::HeaderInputs for InputBlock<'_> {
        fn for_each<'a>(&'a self, visitor: impl FnMut(Header<'a>)) {
            self.0.iter().copied().for_each(visitor);
        }
    }

    #[derive(Debug, PartialEq)]
    enum Field {
        Indexed(u64),
        IncrementallyIndexedLiteral { name_index: u64 },
    }

    fn read_integer(block: &[u8], cursor: &mut usize, prefix_bits: u32) -> u64 {
        let mask = (1_u8 << prefix_bits) - 1;
        let mut value = u64::from(block[*cursor] & mask);
        *cursor += 1;
        if value < u64::from(mask) {
            return value;
        }
        let mut shift = 0;
        loop {
            let byte = block[*cursor];
            *cursor += 1;
            value += u64::from(byte & 0x7f) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                return value;
            }
        }
    }

    fn skip_string(block: &[u8], cursor: &mut usize) {
        let length = read_integer(block, cursor, 7);
        *cursor += usize::try_from(length).expect("test literal length fits in usize");
    }

    /// Splits a header block into RFC 7541 section 6 representations, failing on
    /// any representation other than an indexed field or a literal with incremental indexing.
    fn parse_fields(block: &[u8]) -> Vec<Field> {
        let mut fields = Vec::new();
        let mut cursor = 0;
        while cursor < block.len() {
            let first = block[cursor];
            if first & 0x80 != 0 {
                fields.push(Field::Indexed(read_integer(block, &mut cursor, 7)));
            } else if first & 0x40 != 0 {
                let name_index = read_integer(block, &mut cursor, 6);
                if name_index == 0 {
                    skip_string(block, &mut cursor);
                }
                skip_string(block, &mut cursor);
                fields.push(Field::IncrementallyIndexedLiteral { name_index });
            } else {
                panic!("unexpected representation {first:#04x} at byte {cursor}");
            }
        }
        fields
    }

    /// An RFC 7541 section 4 dynamic table of 4096 bytes, newest first, so the
    /// newest entry has HPACK index 62.
    #[derive(Default)]
    struct TableModel {
        entries: VecDeque<(&'static [u8], Vec<u8>)>,
        literals: u32,
    }

    impl TableModel {
        fn newest(&self, name: &[u8], value: Option<&[u8]>) -> Option<u64> {
            self.entries
                .iter()
                .position(|(stored, stored_value)| {
                    *stored == name && value.is_none_or(|value| stored_value == value)
                })
                .map(|offset| 62 + u64::try_from(offset).expect("test index fits in u64"))
        }

        fn insert(&mut self, name: &'static [u8], value: &[u8]) {
            let entry_size = |name: &[u8], value: &[u8]| name.len() + value.len() + 32;
            let incoming = entry_size(name, value);
            while self
                .entries
                .iter()
                .map(|(name, value)| entry_size(name, value))
                .sum::<usize>()
                + incoming
                > 4096
            {
                self.entries.pop_back();
            }
            self.entries.push_front((name, value.to_vec()));
        }

        /// A repeated field: an indexed reference while the table holds it,
        /// otherwise a literal on its static name that the encoder then inserts.
        fn repeated(&mut self, name: &'static [u8], value: &[u8], static_name: u64) -> Field {
            if let Some(index) = self.newest(name, Some(value)) {
                return Field::Indexed(index);
            }
            self.literals += 1;
            self.insert(name, value);
            Field::IncrementallyIndexedLiteral {
                name_index: static_name,
            }
        }

        /// A never-seen value: a literal naming the newest same-name entry, then inserted.
        fn unique(&mut self, name: &'static [u8], value: &[u8]) -> Field {
            let name_index = self.newest(name, None).unwrap_or(0);
            self.insert(name, value);
            Field::IncrementallyIndexedLiteral { name_index }
        }

        fn matches(&self, table: &HeaderTable) -> bool {
            table
                .entries()
                .map(|header| (header.name.as_bytes(), header.value.as_ref()))
                .eq(self
                    .entries
                    .iter()
                    .map(|(name, value)| (*name, value.as_slice())))
        }
    }

    #[test]
    fn consumer_shape_unique_blocks_have_the_predicted_structure() {
        const DATE: &[u8] = b"Sun, 06 Nov 1994 08:49:37 GMT";
        let mut request_codec = HpackCodec::new();
        let mut response_codec = HpackCodec::new();
        let mut request_decoder = HpackDecoder::new();
        let mut response_decoder = HpackDecoder::new();
        let mut request_model = TableModel::default();
        let mut response_model = TableModel::default();
        for counter in 0..300_u64 {
            let id = format!("{counter:016x}");

            let request = InputBlock(vec![
                Header::new(HttpHeaderCode::ColonMethod.into(), b"POST"),
                Header::new(HttpHeaderCode::ColonScheme.into(), b"http"),
                Header::new(HttpHeaderCode::ColonAuthority.into(), b"c1"),
                Header::new(HttpHeaderCode::ColonPath.into(), b"/echo"),
                Header {
                    code: HttpHeaderCode::Other,
                    name: b"x-request-id",
                    value: id.as_bytes(),
                },
            ]);
            let encoded = request_codec
                .encode_inputs_block(&request, 16 << 10, 9)
                .expect("request block fits");
            assert_eq!(
                request_decoder
                    .decode(encoded.as_bytes())
                    .expect("request block decodes")
                    .headers,
                headers(&[
                    (":method", "POST"),
                    (":scheme", "http"),
                    (":authority", "c1"),
                    (":path", "/echo"),
                    ("x-request-id", &id),
                ])
            );
            // Static indices (RFC 7541 appendix A): 1 :authority, 3 :method POST,
            // 4 :path, 6 :scheme http.
            let expected = vec![
                Field::Indexed(3),
                Field::Indexed(6),
                request_model.repeated(b":authority", b"c1", 1),
                request_model.repeated(b":path", b"/echo", 4),
                request_model.unique(b"x-request-id", id.as_bytes()),
            ];
            assert_eq!(
                parse_fields(encoded.as_bytes()),
                expected,
                "request block {counter}"
            );

            let response = InputBlock(vec![
                Header::new(HttpHeaderCode::ColonStatus.into(), b"200"),
                Header::new(HttpHeaderCode::Date.into(), DATE),
                Header {
                    code: HttpHeaderCode::Other,
                    name: b"x-response-id",
                    value: id.as_bytes(),
                },
            ]);
            let encoded = response_codec
                .encode_inputs_block(&response, 16 << 10, 9)
                .expect("response block fits");
            assert_eq!(
                response_decoder
                    .decode(encoded.as_bytes())
                    .expect("response block decodes")
                    .headers,
                vec![
                    HpackHeader::new(":status", "200"),
                    HpackHeader::new("date", DATE),
                    HpackHeader::new("x-response-id", &id),
                ]
            );
            // Static indices: 8 :status 200, 33 date.
            let expected = vec![
                Field::Indexed(8),
                response_model.repeated(b"date", DATE, 33),
                response_model.unique(b"x-response-id", id.as_bytes()),
            ];
            assert_eq!(
                parse_fields(encoded.as_bytes()),
                expected,
                "response block {counter}"
            );
        }

        assert!(
            request_model.literals > 2 && response_model.literals > 1,
            "the tables filled and evicted then re-inserted the repeated fields"
        );
        assert!(request_model.matches(request_codec.encoder().context().table()));
        assert!(response_model.matches(response_codec.encoder().context().table()));
    }

    #[test]
    fn consumer_shape_block_examines_only_same_name_entries() {
        fn encode_request(codec: &mut HpackCodec, counter: u64) {
            let id = format!("{counter:016x}");
            let request = InputBlock(vec![
                Header::new(HttpHeaderCode::ColonMethod.into(), b"POST"),
                Header::new(HttpHeaderCode::ColonScheme.into(), b"http"),
                Header::new(HttpHeaderCode::ColonAuthority.into(), b"c1"),
                Header::new(HttpHeaderCode::ColonPath.into(), b"/echo"),
                Header {
                    code: HttpHeaderCode::Other,
                    name: b"x-request-id",
                    value: id.as_bytes(),
                },
            ]);
            codec
                .encode_inputs_block(&request, 16 << 10, 9)
                .expect("request block fits");
        }

        let mut codec = HpackCodec::new();
        for counter in 0..200 {
            encode_request(&mut codec, counter);
        }
        let table = codec.encoder().context().table();
        let same_name: Vec<usize> = [":authority", ":path", "x-request-id"]
            .into_iter()
            .map(|name| {
                let name = HeaderName::new(name);
                table.entries().filter(|header| header.name == name).count()
            })
            .collect();
        assert_eq!(
            (table.size(), same_name.as_slice()),
            (68, &[1, 1, 66][..]),
            "after 200 blocks the table holds :authority, :path and 66 x-request-id entries"
        );
        let bound: usize = same_name.iter().sum();
        HeaderTable::take_examined();

        encode_request(&mut codec, 200);
        let examined = HeaderTable::take_examined();
        assert!(
            examined <= bound,
            "a consumer-shaped request block reads at most the {bound} entries named :authority, \
             :path or x-request-id, read {examined}"
        );
    }

    /// The HPACK sequence that wire-identity tests pin: 2,000 deterministic blocks from a request
    /// encoder and a response encoder, both with 4096-byte tables. Block `i` is, by `i % 4`, a
    /// consumer-shaped request (0), a consumer-shaped response (1), a realistic 14-header request
    /// with a rotating `traceparent` and a mixed-case `X-Request-Id` (2), or a realistic 8-header
    /// response (3); requests use the request encoder. Both encoders resize to 256 bytes before
    /// block 500 and back to 4096 before block 501. Responses of kind 3 take the owned-header path,
    /// the rest the borrowed-input path.
    fn golden_blocks() -> Vec<Vec<u8>> {
        golden_blocks_observed(|_, _| {})
    }

    /// `golden_blocks`, calling `observe(i, codec)` after block `i` with the codec that encoded it.
    fn golden_blocks_observed(mut observe: impl FnMut(u64, &HpackCodec)) -> Vec<Vec<u8>> {
        const DATE: &[u8] = b"Sun, 06 Nov 1994 08:49:37 GMT";
        fn encode_inputs(codec: &mut HpackCodec, block: &InputBlock<'_>) -> Vec<u8> {
            codec
                .encode_inputs_block(block, 16 << 10, 9)
                .expect("golden block fits")
                .as_bytes()
                .to_vec()
        }

        let mut request_codec = HpackCodec::new();
        let mut response_codec = HpackCodec::new();
        (0..2_000_u64)
            .map(|block| {
                let resize = match block {
                    500 => Some(256),
                    501 => Some(4096),
                    _ => None,
                };
                if let Some(size) = resize {
                    request_codec.set_encoder_header_table_size(size);
                    response_codec.set_encoder_header_table_size(size);
                }
                let id = format!("{block:016x}");
                let wire = match block % 4 {
                    0 => encode_inputs(
                        &mut request_codec,
                        &InputBlock(vec![
                            Header::new(HttpHeaderCode::ColonMethod.into(), b"POST"),
                            Header::new(HttpHeaderCode::ColonScheme.into(), b"http"),
                            Header::new(HttpHeaderCode::ColonAuthority.into(), b"c1"),
                            Header::new(HttpHeaderCode::ColonPath.into(), b"/echo"),
                            Header::new(HttpHeaderName::Name(b"x-request-id"), id.as_bytes()),
                        ]),
                    ),
                    1 => encode_inputs(
                        &mut response_codec,
                        &InputBlock(vec![
                            Header::new(HttpHeaderCode::ColonStatus.into(), b"200"),
                            Header::new(HttpHeaderCode::Date.into(), DATE),
                            Header::new(HttpHeaderName::Name(b"x-response-id"), id.as_bytes()),
                        ]),
                    ),
                    2 => {
                        let path = format!("/v1/items/{}", block % 37);
                        let cookie = format!("session={:08x}; theme=dark", block / 64);
                        let traceparent = format!(
                            "00-{:016x}{:016x}-{:016x}-01",
                            block.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                            block.wrapping_mul(0xC2B2_AE3D_27D4_EB4F),
                            block.wrapping_mul(0x1656_67B1_9E37_79F9),
                        );
                        let forwarded = format!("203.0.113.{}", block % 251);
                        let tenant = format!("tenant-{}", block % 5);
                        encode_inputs(
                            &mut request_codec,
                            &InputBlock(vec![
                                Header::new(HttpHeaderCode::ColonMethod.into(), b"GET"),
                                Header::new(HttpHeaderCode::ColonScheme.into(), b"https"),
                                Header::new(
                                    HttpHeaderCode::ColonAuthority.into(),
                                    b"api.example.com",
                                ),
                                Header::new(HttpHeaderCode::ColonPath.into(), path.as_bytes()),
                                Header::new(
                                    HttpHeaderName::Name(b"user-agent"),
                                    b"roxygen-golden/1.0",
                                ),
                                Header::new(HttpHeaderName::Name(b"accept"), b"application/json"),
                                Header::new(
                                    HttpHeaderName::Name(b"accept-encoding"),
                                    b"gzip, deflate",
                                ),
                                Header::new(
                                    HttpHeaderName::Name(b"accept-language"),
                                    b"en-US,en;q=0.9",
                                ),
                                Header::new(HttpHeaderName::Name(b"cookie"), cookie.as_bytes()),
                                Header::new(
                                    HttpHeaderName::Name(b"traceparent"),
                                    traceparent.as_bytes(),
                                ),
                                Header::new(HttpHeaderName::Name(b"X-Request-Id"), id.as_bytes()),
                                Header::new(
                                    HttpHeaderName::Name(b"x-forwarded-for"),
                                    forwarded.as_bytes(),
                                ),
                                Header::new(HttpHeaderName::Name(b"cache-control"), b"no-cache"),
                                Header::new(HttpHeaderName::Name(b"x-tenant"), tenant.as_bytes()),
                            ]),
                        )
                    }
                    _ => {
                        let status = if block % 7 == 3 { "404" } else { "200" };
                        let length = (100 + block % 900).to_string();
                        response_codec.encode(&[
                            HpackHeader::new(":status", status),
                            HpackHeader::new("date", DATE),
                            HpackHeader::new("content-type", "application/json"),
                            HpackHeader::new("content-length", &length),
                            HpackHeader::new("server", "roxygen-golden"),
                            HpackHeader::new("cache-control", "private, max-age=0"),
                            HpackHeader::new("X-Response-Id", &id),
                            HpackHeader::new("vary", "accept-encoding"),
                        ])
                    }
                };
                observe(
                    block,
                    if block % 2 == 0 {
                        &request_codec
                    } else {
                        &response_codec
                    },
                );
                wire
            })
            .collect()
    }

    #[test]
    fn hpack_golden_digest_is_unchanged() {
        let wire = golden_blocks().concat();
        assert_eq!(
            (fnv1a64(&wire), wire.len()),
            (0x134a_2162_91b5_f740, 80_249),
            "the golden HPACK sequence's FNV-1a digest and length, recorded at 4194b6f17524"
        );
    }

    #[test]
    fn hpack_golden_decode_digest_is_unchanged() {
        let mut request_decoder = HpackDecoder::new();
        let mut response_decoder = HpackDecoder::new();
        let mut stream = Vec::new();
        for (block, wire) in golden_blocks().iter().enumerate() {
            let decoder = if block % 2 == 0 {
                &mut request_decoder
            } else {
                &mut response_decoder
            };
            let decoded = decoder.decode(wire).expect("golden block decodes");
            for header in &decoded.headers {
                for part in [header.name.as_bytes(), &header.value[..]] {
                    let length = u32::try_from(part.len()).expect("golden field length");
                    stream.extend_from_slice(&length.to_le_bytes());
                    stream.extend_from_slice(part);
                }
            }
        }
        assert_eq!(
            (fnv1a64(&stream), stream.len()),
            (0x08b7_4c32_9f1c_0fef, 467_144),
            "the decoded golden blocks' length-prefixed name/value stream, recorded at 1ab7e556f82e"
        );
    }

    /// An FNV-1a digest fed incrementally, so a long stream need not be held in memory.
    struct StreamDigest {
        hash: u64,
        length: usize,
    }

    impl StreamDigest {
        fn new() -> Self {
            Self {
                hash: 0xcbf2_9ce4_8422_2325,
                length: 0,
            }
        }

        fn bytes(&mut self, bytes: &[u8]) {
            for &byte in bytes {
                self.hash = (self.hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
            }
            self.length += bytes.len();
        }

        fn part(&mut self, bytes: &[u8]) {
            let length = u32::try_from(bytes.len()).expect("digest part length fits in u32");
            self.bytes(&length.to_le_bytes());
            self.bytes(bytes);
        }
    }

    /// Every decode event, length-prefixed, in the order the decoder reports it.
    #[derive(Default)]
    struct DecodeEvents(Vec<u8>);

    impl DecodeEvents {
        fn part(&mut self, bytes: &[u8]) {
            let length = u32::try_from(bytes.len()).expect("event part length fits in u32");
            self.0.extend_from_slice(&length.to_le_bytes());
            self.0.extend_from_slice(bytes);
        }

        fn header(&mut self, name: &[u8], value: &[u8]) {
            self.0.push(b'h');
            self.part(name);
            self.part(value);
        }

        fn size(&mut self, tag: u8, size: HttpHeaderSize) {
            self.0.push(tag);
            for value in [size.compressed, size.compressed_block, size.uncompressed] {
                self.0.extend_from_slice(&value.to_le_bytes());
            }
        }

        fn error(&mut self, tag: u8, error: HpackError) {
            self.0.push(tag);
            self.part(format!("{error:?}").as_bytes());
        }

        fn streaming_result(&mut self, result: Result<HttpHeaderSize, HpackError>) {
            match result {
                Ok(size) => self.size(b'r', size),
                Err(error) => self.error(b'x', error),
            }
        }

        fn block_result(&mut self, result: Result<DecodedHeaderBlock, HpackError>) {
            match result {
                Ok(decoded) => {
                    for header in &decoded.headers {
                        self.header(header.name.as_bytes(), &header.value);
                    }
                    self.size(b'r', decoded.size);
                    let consumed =
                        u32::try_from(decoded.bytes_consumed).expect("consumed fits in u32");
                    self.0.push(b'n');
                    self.0.extend_from_slice(&consumed.to_le_bytes());
                }
                Err(error) => self.error(b'x', error),
            }
        }

        /// The decoder's sticky error and its dynamic table's capacity, size, bytes and inserts.
        fn decoder_state(&mut self, decoder: &HpackDecoder) {
            match decoder.error() {
                Some(error) => self.error(b'E', error),
                None => self.0.push(b'O'),
            }
            let table = decoder.context().table();
            for value in [
                table.capacity(),
                table.size(),
                table.bytes(),
                table.insert_count(),
            ] {
                self.0.extend_from_slice(&value.to_le_bytes());
            }
        }
    }

    impl StreamingCallback for DecodeEvents {
        fn on_header(&mut self, name: &HeaderName, value: &[u8]) {
            self.header(name.as_bytes(), value);
        }

        fn on_headers_complete(&mut self, decoded_size: HttpHeaderSize, acknowledge: bool) {
            self.size(b'c', decoded_size);
            self.0.push(u8::from(acknowledge));
        }

        fn on_decode_error(&mut self, error: HpackError) {
            self.error(b'e', error);
        }
    }

    /// A decoder's dynamic table capacity, bytes and entries, newest first. `HeaderTable`'s
    /// `PartialEq` compares only the entry count and bytes.
    fn table_contents(decoder: &HpackDecoder) -> (u32, u32, Vec<HpackHeader>) {
        let table = decoder.context().table();
        (
            table.capacity(),
            table.bytes(),
            table.entries().cloned().collect(),
        )
    }

    /// A block that gives a new decoder `reference`'s dynamic table: a size update to its
    /// capacity, then each entry, oldest first, as a raw literal with incremental indexing.
    fn table_priming_block(reference: &HpackDecoder) -> Vec<u8> {
        let table = reference.context().table();
        let mut entries: Vec<_> = table.entries().collect();
        entries.reverse();
        let mut block = HpackEncodeBuffer::new(false);
        block.encode_integer_parts(
            u64::from(table.capacity()),
            TABLE_SIZE_UPDATE.code,
            TABLE_SIZE_UPDATE.prefix_length,
        );
        for entry in entries {
            block.encode_integer_parts(0, LITERAL_INC_INDEX.code, LITERAL_INC_INDEX.prefix_length);
            block.encode_literal(entry.name.as_bytes());
            block.encode_literal(&entry.value);
        }
        block.into_bytes()
    }

    /// Decodes `wire` four ways, each on a new decoder holding `reference`'s dynamic table, and
    /// feeds every decode's events and final decoder state to `digest`: (a) contiguous; (b) split
    /// into two segments at every byte boundary, then into 1-byte segments; (c) from a one-segment
    /// queue holding all of `wire`, with `total_bytes` cut at every length; (d) truncated at every
    /// length. Then `reference` decodes `wire` and the contiguous result is checked against it.
    fn digest_block_decodes(
        digest: &mut StreamDigest,
        reference: &mut HpackDecoder,
        table_size: u32,
        wire: &[u8],
    ) {
        let priming = table_priming_block(reference);
        let primed = || {
            let mut decoder = HpackDecoder::with_limits(table_size, DEFAULT_MAX_UNCOMPRESSED);
            decoder
                .decode(&priming)
                .expect("the table priming block decodes");
            assert_eq!(
                table_contents(&decoder),
                table_contents(reference),
                "the primed table matches the reference table"
            );
            decoder
        };
        type Decode<'f> = dyn FnMut(&mut HpackDecoder, &mut DecodeEvents) + 'f;
        let mut record = |way: u8, point: usize, decode: &mut Decode<'_>| {
            let mut decoder = primed();
            let mut events = DecodeEvents::default();
            decode(&mut decoder, &mut events);
            events.decoder_state(&decoder);
            digest.bytes(&[way]);
            digest.bytes(
                &u32::try_from(point)
                    .expect("point fits in u32")
                    .to_le_bytes(),
            );
            digest.part(&events.0);
            events.0
        };

        let contiguous = record(b'a', 0, &mut |decoder, events| {
            let result = decoder.decode_streaming(wire, events);
            events.streaming_result(result);
        });
        for split in 0..=wire.len() {
            record(b'b', split, &mut |decoder, events| {
                let result = decoder.decode_segments([&wire[..split], &wire[split..]], wire.len());
                events.block_result(result);
            });
        }
        record(b'B', 0, &mut |decoder, events| {
            let result = decoder.decode_segments(wire.chunks(1), wire.len());
            events.block_result(result);
        });
        let queue = BufQueue::copy_from_slice(wire, wire.len()).expect("the block fits a queue");
        for cut in 0..=wire.len() {
            record(b'c', cut, &mut |decoder, events| {
                let result = decoder.decode_streaming_buf_queue(&queue, cut, events);
                events.streaming_result(result);
            });
        }
        for cut in 0..=wire.len() {
            record(b'd', cut, &mut |decoder, events| {
                let result = decoder.decode_streaming(&wire[..cut], events);
                events.streaming_result(result);
            });
        }

        let decoded = reference.decode(wire).expect("the reference block decodes");
        let mut expected = DecodeEvents::default();
        for header in &decoded.headers {
            expected.header(header.name.as_bytes(), &header.value);
        }
        assert!(
            contiguous.starts_with(&expected.0),
            "the primed contiguous decode emits the reference decode's headers"
        );
    }

    /// RFC 7541 Appendix C header blocks, as (decoder table size, one connection's blocks):
    /// C.2.1-C.2.4 each on a new decoder, then C.3, C.4, C.5 and C.6.
    const RFC_7541_BLOCKS: [(u32, &[&str]); 8] = [
        (
            DEFAULT_TABLE_SIZE,
            &["400a637573746f6d2d6b65790d637573746f6d2d686561646572"],
        ),
        (DEFAULT_TABLE_SIZE, &["040c2f73616d706c652f70617468"]),
        (DEFAULT_TABLE_SIZE, &["100870617373776f726406736563726574"]),
        (DEFAULT_TABLE_SIZE, &["82"]),
        (
            DEFAULT_TABLE_SIZE,
            &[
                "828684410f7777772e6578616d706c652e636f6d",
                "828684be58086e6f2d6361636865",
                "828785bf400a637573746f6d2d6b65790c637573746f6d2d76616c7565",
            ],
        ),
        (
            DEFAULT_TABLE_SIZE,
            &[
                "828684418cf1e3c2e5f23a6ba0ab90f4ff",
                "828684be5886a8eb10649cbf",
                "828785bf408825a849e95ba97d7f8925a849e95bb8e8b4bf",
            ],
        ),
        (
            256,
            &[
                "4803333032580770726976617465611d4d6f6e2c203231204f637420323031332032303a31333a323120474d546e1768747470733a2f2f7777772e6578616d706c652e636f6d",
                "4803333037c1c0bf",
                "88c1611d4d6f6e2c203231204f637420323031332032303a31333a323220474d54c05a04677a69707738666f6f3d4153444a4b48514b425a584f5157454f50495541585157454f49553b206d61782d6167653d333630303b2076657273696f6e3d31",
            ],
        ),
        (
            256,
            &[
                "488264025885aec3771a4b6196d07abe941054d444a8200595040b8166e082a62d1bff6e919d29ad171863c78f0b97c8e9ae82ae43d3",
                "4883640effc1c0bf",
                "88c16196d07abe941054d444a8200595040b8166e084a62d1bffc05a839bd9ab77ad94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007",
            ],
        ),
    ];

    #[test]
    fn segmented_and_truncated_decodes_are_unchanged() {
        let mut digest = StreamDigest::new();
        for (table_size, blocks) in RFC_7541_BLOCKS {
            let mut reference = HpackDecoder::with_limits(table_size, DEFAULT_MAX_UNCOMPRESSED);
            for block in blocks {
                digest_block_decodes(&mut digest, &mut reference, table_size, &hex(block));
            }
        }
        let mut references = [HpackDecoder::new(), HpackDecoder::new()];
        for (block, wire) in golden_blocks().iter().enumerate() {
            digest_block_decodes(
                &mut digest,
                &mut references[block % 2],
                DEFAULT_TABLE_SIZE,
                wire,
            );
        }
        assert_eq!(
            (digest.hash, digest.length),
            (0x62ba_44e6_5f2e_f974, 82_210_029),
            "the RFC 7541 Appendix C and golden blocks decoded contiguously, split, from a cut \
             queue and truncated: every decode's events and final decoder state, recorded at \
             e56cb47a8dee"
        );
    }

    /// Each RFC 7541 Appendix C block, and the consumer-shaped forwarded request encoded twice
    /// (literals, then references), decodes from a two-segment queue split at every byte boundary
    /// and from a queue of 1-byte segments, with `total_bytes` cut at every length, exactly as that
    /// prefix decodes contiguously: the result, the sticky error and the dynamic table. Every
    /// decode runs on a new decoder holding the connection's table so far.
    #[test]
    fn split_queue_decodes_match_contiguous_prefix_decodes() {
        let fields = forwarded_request_fields();
        let mut encoder = HpackEncoder::new(true);
        let forwarded: Vec<Vec<u8>> = (0..2).map(|_| encoder.encode(&fields)).collect();
        let connections = RFC_7541_BLOCKS
            .iter()
            .map(|&(table_size, blocks)| {
                (table_size, blocks.iter().map(|block| hex(block)).collect())
            })
            .chain([(DEFAULT_TABLE_SIZE, forwarded)]);
        for (table_size, blocks) in connections {
            let mut reference = HpackDecoder::with_limits(table_size, DEFAULT_MAX_UNCOMPRESSED);
            for wire in blocks {
                let priming = table_priming_block(&reference);
                let primed = || {
                    let mut decoder =
                        HpackDecoder::with_limits(table_size, DEFAULT_MAX_UNCOMPRESSED);
                    decoder
                        .decode(&priming)
                        .expect("the table priming block decodes");
                    decoder
                };
                let mut queues = Vec::new();
                for split in 0..=wire.len() {
                    let mut queue = BufQueue::new(wire.len());
                    for segment in [&wire[..split], &wire[split..]] {
                        queue.push_copy(segment).expect("the block fits its queue");
                    }
                    queues.push((format!("split {split}"), queue));
                }
                let mut bytes = BufQueue::new(wire.len());
                for byte in wire.chunks(1) {
                    bytes.push_copy(byte).expect("the block fits its queue");
                }
                queues.push(("1-byte segments".to_owned(), bytes));
                for (shape, queue) in &queues {
                    for total in 0..=wire.len() {
                        let mut contiguous = primed();
                        let mut segmented = primed();
                        let expected = contiguous.decode(&wire[..total]);
                        assert_eq!(
                            segmented.decode_buf_queue(queue, total),
                            expected,
                            "{shape}, total {total}"
                        );
                        let state = |decoder: &HpackDecoder| {
                            (
                                decoder.error(),
                                decoder.context().table().insert_count(),
                                table_contents(decoder),
                            )
                        };
                        assert_eq!(
                            state(&segmented),
                            state(&contiguous),
                            "{shape}, total {total}: sticky error, inserts and table"
                        );
                    }
                }
                reference
                    .decode(&wire)
                    .expect("the reference block decodes");
            }
        }
    }

    /// A 21-field consumer-shaped forwarded request.
    fn forwarded_request_fields() -> Vec<HpackHeader> {
        headers(&[
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "c1"),
            (":path", "/v1/route/items"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
            ("user-agent", "grpc-client/1.0"),
            ("x-request-id", "5f0c2a9e41b7d3c8"),
            ("x-routing-key", "shard-0042/region-7"),
            ("x-routing-key-copy", "shard-0042/region-7"),
            ("x-timeout-ms", "1500"),
            ("x-pad-req", "pppppppppppppppppppppppp"),
            ("x-forwarded-for", "203.0.113.7"),
            ("x-forwarded-proto", "https"),
            ("accept-encoding", "gzip"),
            ("grpc-accept-encoding", "identity,deflate,gzip"),
            ("grpc-timeout", "1499m"),
            (
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ),
            ("x-client-id", "client-17"),
            ("content-length", "128"),
            ("x-tenant", "tenant-3"),
        ])
    }

    #[test]
    fn contiguous_decode_reads_each_field_start_once_off_the_slow_path() {
        let fields = forwarded_request_fields();
        assert_eq!(fields.len(), 21);
        let mut encoder = HpackEncoder::new(true);
        let mut decoder = HpackDecoder::new();
        // The first block inserts literals, the second refers to them.
        for block in 0..2 {
            let wire = encoder.encode(&fields);
            HpackDecodeBuffer::take_slow_path_entries();
            decoder.take_field_start_reads();
            let decoded = decoder.decode(&wire).expect("the request decodes");
            assert_eq!(decoded.headers, fields, "block {block}");
            assert_eq!(
                (
                    HpackDecodeBuffer::take_slow_path_entries(),
                    decoder.take_field_start_reads(),
                ),
                (0, fields.len()),
                "block {block}: (slow-path entries, field-start reads) of a contiguous decode"
            );
        }
    }

    #[test]
    fn encode_stats_are_unchanged() {
        let mut stats = Vec::new();
        golden_blocks_observed(|block, codec| {
            if block % 4 == 3 {
                return;
            }
            let size = codec.encoded_size();
            let fields = u32::try_from(codec.last_encode_fields()).expect("golden field count");
            for value in [size.compressed, size.uncompressed, fields] {
                stats.extend_from_slice(&value.to_le_bytes());
            }
        });
        assert_eq!(
            (fnv1a64(&stats), stats.len()),
            (0x8b42_4ea3_0089_f126, 18_000),
            "the borrowed-input golden blocks' (encoded length, uncompressed, fields) triples, \
             recorded at 29a715fcd667"
        );
    }

    /// Appends `wire`, then the encoder table's size, bytes and insert count and every entry newest
    /// first, each part length-prefixed.
    fn append_encoder_state(stream: &mut Vec<u8>, wire: &[u8], table: &HeaderTable) {
        let mut part = |bytes: &[u8]| {
            let length = u32::try_from(bytes.len()).expect("test part length fits in u32");
            stream.extend_from_slice(&length.to_le_bytes());
            stream.extend_from_slice(bytes);
        };
        part(wire);
        for value in [table.size(), table.bytes(), table.insert_count()] {
            part(&value.to_le_bytes());
        }
        for header in table.entries() {
            part(header.name.as_bytes());
            part(&header.value);
        }
    }

    fn mix(seed: u64) -> u64 {
        let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn filler(length: usize) -> String {
        b"abcdefghijklmnopqrstuvwxyz0123456789"
            .iter()
            .cycle()
            .take(length)
            .map(|&byte| char::from(byte))
            .collect()
    }

    type Fields = Vec<(String, String)>;

    fn fields(values: &[(&str, &str)]) -> Fields {
        values
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    /// A consumer-shaped request of 21 fields: a 67-byte id, a 77-byte routing key sent twice, a
    /// token count and a prefix hash that vary per request, next to constant fields.
    fn r1_shaped_request(sequence: u64) -> Fields {
        let request_id = format!(
            "api.proxy.{:016x}{:08x}-{:016x}{:016x}",
            mix(4 * sequence),
            mix(4 * sequence + 1) >> 32,
            mix(4 * sequence + 2),
            mix(4 * sequence + 3)
        );
        let routing_key = format!(
            "session-{:016x}{:016x}{:016x}{:016x}-{:04}",
            mix(sequence ^ 0x10),
            mix(sequence ^ 0x11),
            mix(sequence ^ 0x12),
            mix(sequence ^ 0x13),
            sequence % 10_000
        );
        assert_eq!((request_id.len(), routing_key.len()), (67, 77));
        let length = (100 + mix(sequence ^ 0x20) % 900).to_string();
        let tokens = (1 + mix(sequence ^ 0x21) % 4096).to_string();
        let prefix = format!("{:016x}", mix(sequence ^ 0x22));
        let pad = filler(33);
        let cluster = filler(56);
        fields(&[
            (":method", "POST"),
            (":scheme", "http"),
            (":authority", "api-proxy.service.test"),
            (":path", "/generate"),
            ("content-type", "application/json"),
            ("accept", "text/event-stream"),
            ("content-length", &length),
            ("x-scope", "inference.api_proxy"),
            ("x-request-id", &request_id),
            ("x-routing-key", &routing_key),
            ("x-token-count", &tokens),
            ("x-prefix-hash", &prefix),
            ("x-stream", "true"),
            ("x-batch", "false"),
            ("x-return-logprob", "false"),
            ("x-return-probe-results", "true"),
            ("x-pad-req", &pad),
            ("x-forwarded-proto", "http"),
            ("x-cluster", &cluster),
            ("x-routing-key-copy", &routing_key),
            ("x-timeout-ms", "3600000"),
        ])
    }

    fn r1_shaped_response(sequence: u64) -> Fields {
        let upstream_time = (1 + mix(sequence ^ 0x30) % 999).to_string();
        fields(&[
            (":status", "200"),
            ("content-type", "text/event-stream"),
            ("date", "Sun, 06 Nov 1994 08:49:37 GMT"),
            ("server", "proxy"),
            ("x-upstream-time", &upstream_time),
            ("x-pad-resp", &filler(107)),
        ])
    }

    /// A consumer-shaped gRPC request of 13 fields with a UUID-shaped id; `heavy` sends
    /// `:scheme https` and adds a 4,400-byte client certificate from one of 16 identities, too large
    /// to index in a 4,096-byte table.
    fn r2_shaped_request(sequence: u64, heavy: bool) -> Fields {
        let id = mix(sequence ^ 0x40);
        let request_id = format!(
            "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
            id >> 32,
            (id >> 16) & 0xffff,
            id & 0xfff,
            mix(id) & 0xfff,
            mix(id ^ 1) & 0xffff_ffff_ffff
        );
        let mut request = fields(&[
            (":method", "POST"),
            (":scheme", if heavy { "https" } else { "http" }),
            (":authority", &filler(62)),
            (":path", "/service.Api/UpdateResource"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
            ("user-agent", "grpc-client/0.14.6"),
            ("x-scope", "service"),
            ("x-pad-req", &filler(56)),
            ("x-request-id", &request_id),
            ("x-forwarded-proto", "http"),
            ("x-cluster", &filler(48)),
            ("x-pad-proxy", &filler(19)),
        ]);
        if heavy {
            let identity = sequence % 16;
            let mut certificate = format!(
                "Hash={:016x};Subject=\"CN=client-{identity:02}\";",
                mix(identity)
            );
            while certificate.len() < 4_400 {
                certificate.push(char::from(
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
                        [(mix((identity << 32) | certificate.len() as u64) % 64) as usize],
                ));
            }
            request.push(("x-client-cert".to_owned(), certificate));
        }
        request
    }

    fn r2_shaped_response(sequence: u64) -> Fields {
        fields(&[
            (":status", "200"),
            ("content-type", "application/grpc"),
            ("date", "Sun, 06 Nov 1994 08:49:37 GMT"),
            ("server", "proxy"),
            ("x-upstream-time", &(sequence % 10).to_string()),
        ])
    }

    /// Eight literals of the Huffman corpus from `cursor` on, as values of eight uncommon names,
    /// every third name in mixed case as an HTTP/1.1 peer may send it.
    fn corpus_block(literals: &[Vec<u8>], cursor: &mut usize) -> Vec<(String, Vec<u8>)> {
        (0..8)
            .map(|field| {
                let literal = literals[*cursor % literals.len()].clone();
                *cursor += 1;
                let name = if cursor.is_multiple_of(3) {
                    format!("X-Corpus-{field}")
                } else {
                    format!("x-corpus-{field}")
                };
                (name, literal)
            })
            .collect()
    }

    /// The encoder-equivalence sequence: 1,800 blocks over a request encoder, a response encoder
    /// and a request encoder with `NoPathIndexingStrategy`, all with 4,096-byte tables. Block `i`
    /// is, by `i % 6`, a consumer-shaped request (0), its response (1), a gRPC request on the owned
    /// path (2), its response (3), eight Huffman-corpus values (4), and every fifth time a heavy
    /// gRPC request, otherwise eight more corpus values on the response encoder's owned path (5).
    /// Every request also goes through the strategy encoder. Both plain encoders resize to 256
    /// bytes before block 600, to 0 before block 601 and back to 4,096 before block 602.
    fn encoder_state_stream() -> Vec<u8> {
        fn encode_inputs<N: AsRef<[u8]>, V: AsRef<[u8]>>(
            codec: &mut HpackCodec,
            block: &[(N, V)],
        ) -> Vec<u8> {
            let inputs = InputBlock(
                block
                    .iter()
                    .map(|(name, value)| {
                        Header::new(HttpHeaderName::Name(name.as_ref()), value.as_ref())
                    })
                    .collect(),
            );
            codec
                .encode_inputs_block(&inputs, 64 << 10, 9)
                .expect("equivalence block fits")
                .as_bytes()
                .to_vec()
        }
        fn encode_owned<N: AsRef<[u8]>, V: AsRef<[u8]>>(
            codec: &mut HpackCodec,
            block: &[(N, V)],
        ) -> Vec<u8> {
            let owned: Vec<HpackHeader> = block
                .iter()
                .map(|(name, value)| HpackHeader::new(name, value))
                .collect();
            codec.encode(&owned)
        }

        let literals = super::huffman::huffman_literals();
        let mut cursor = 0;
        let mut request_codec = HpackCodec::new();
        let mut response_codec = HpackCodec::new();
        let mut strategy_codec = HpackCodec::new();
        strategy_codec.set_header_indexing_strategy(Rc::new(NoPathIndexingStrategy));
        let mut stream = Vec::new();
        for block in 0..1_800_u64 {
            if let Some(size) = match block {
                600 => Some(256),
                601 => Some(0),
                602 => Some(4096),
                _ => None,
            } {
                request_codec.set_encoder_header_table_size(size);
                response_codec.set_encoder_header_table_size(size);
            }
            let sequence = block / 6;
            let (wire, codec, request) = match block % 6 {
                0 => {
                    let request = r1_shaped_request(sequence);
                    (
                        encode_inputs(&mut request_codec, &request),
                        &request_codec,
                        Some(request),
                    )
                }
                1 => (
                    encode_inputs(&mut response_codec, &r1_shaped_response(sequence)),
                    &response_codec,
                    None,
                ),
                2 => {
                    let request = r2_shaped_request(sequence, false);
                    (
                        encode_owned(&mut request_codec, &request),
                        &request_codec,
                        Some(request),
                    )
                }
                3 => (
                    encode_inputs(&mut response_codec, &r2_shaped_response(sequence)),
                    &response_codec,
                    None,
                ),
                4 => (
                    encode_inputs(&mut request_codec, &corpus_block(&literals, &mut cursor)),
                    &request_codec,
                    None,
                ),
                _ if sequence % 5 == 0 => {
                    let request = r2_shaped_request(sequence, true);
                    (
                        encode_inputs(&mut request_codec, &request),
                        &request_codec,
                        Some(request),
                    )
                }
                _ => (
                    encode_owned(&mut response_codec, &corpus_block(&literals, &mut cursor)),
                    &response_codec,
                    None,
                ),
            };
            append_encoder_state(&mut stream, &wire, codec.encoder().context().table());
            if let Some(request) = request {
                let wire = encode_inputs(&mut strategy_codec, &request);
                append_encoder_state(
                    &mut stream,
                    &wire,
                    strategy_codec.encoder().context().table(),
                );
            }
        }
        assert!(
            cursor > literals.len(),
            "the sequence covers every corpus literal"
        );
        stream
    }

    #[test]
    fn encoder_wire_and_table_state_are_unchanged() {
        let stream = encoder_state_stream();
        assert_eq!(
            (fnv1a64(&stream), stream.len()),
            (0x6a50_2f26_3134_ebfc, 7_601_938),
            "the encoder-equivalence sequence's wire bytes and table states, recorded at 3e8f525cff27"
        );
    }

    /// One step of the encoder-memo sequence.
    enum MemoStep {
        /// A block through `encode_inputs_block` with this encoded-size maximum.
        Inputs(Fields, usize),
        /// A block through the owned-header `encode`, which leaves the encoder memo in place.
        Owned(Fields),
        /// An encoder table size change before the next block, as a peer's SETTINGS applies it.
        TableSize(u32),
        /// A new encoder installed through `encoder_mut`.
        Replace,
    }

    /// The encoder-memo sequence for one codec with a 4,096-byte table, mostly the consumer-shaped
    /// gRPC response whose `x-upstream-time` cycles through ten digits: warm-up blocks that insert
    /// every digit, identical runs, churn runs long enough for the longest memo back-off, isolated
    /// misses between identical runs, A/B alternation, table-mutating blocks, owned-path blocks,
    /// blocks over the maximum (on a ready memo and while backing off), ineligible blocks, a table
    /// shrink then grow (as two SETTINGS, then within one), and encoder replacement (while backing
    /// off and while a memo is ready).
    fn memo_steps() -> Vec<MemoStep> {
        const MAXIMUM: usize = 16 << 10;
        fn response(digit: u64) -> MemoStep {
            MemoStep::Inputs(r2_shaped_response(digit), MAXIMUM)
        }
        fn run(steps: &mut Vec<MemoStep>, digit: u64, blocks: usize) {
            steps.extend((0..blocks).map(|_| response(digit)));
        }
        fn churn(steps: &mut Vec<MemoStep>, blocks: u64) {
            steps.extend((0..blocks).map(response));
        }

        let mut steps = Vec::new();
        churn(&mut steps, 10);
        run(&mut steps, 3, 6);
        churn(&mut steps, 80);
        run(&mut steps, 5, 40);
        for digit in [6, 7, 6, 7] {
            run(&mut steps, digit, 3);
        }
        steps.extend((0..40).map(|block| response(1 + block % 2)));
        // Table-mutating blocks inside a churn: a new request id every fourth block and a new
        // two-digit value every ninth.
        for block in 0..36_u64 {
            let mut fields = r2_shaped_response(block);
            if block % 4 == 0 {
                fields.push(("x-request-id".to_owned(), format!("{:016x}", mix(block))));
            }
            if block % 9 == 4 {
                fields[4].1 = (10 + block).to_string();
            }
            steps.push(MemoStep::Inputs(fields, MAXIMUM));
        }
        for block in 0..30_u64 {
            steps.push(if block % 7 == 3 {
                MemoStep::Owned(r2_shaped_response(block))
            } else {
                response(block)
            });
        }
        for block in 0..10 {
            steps.push(if block == 4 {
                MemoStep::Owned(r2_shaped_response(8))
            } else {
                response(8)
            });
        }
        run(&mut steps, 4, 3);
        steps.push(MemoStep::Inputs(r2_shaped_response(4), 1));
        run(&mut steps, 4, 1);
        churn(&mut steps, 6);
        steps.push(MemoStep::Inputs(r2_shaped_response(9), 1));
        churn(&mut steps, 10);
        // Ineligible blocks: 33 fields, then more than 4,096 uncompressed bytes.
        let mut wide = r2_shaped_response(2);
        wide.extend((0..28).map(|field| (format!("x-pad-{field}"), "1".to_owned())));
        let mut large = r2_shaped_response(2);
        large.push(("x-pad-req".to_owned(), filler(5_000)));
        for fields in [wide.clone(), wide, large.clone(), large] {
            steps.push(MemoStep::Inputs(fields, MAXIMUM));
        }
        run(&mut steps, 2, 4);
        run(&mut steps, 3, 4);
        steps.push(MemoStep::TableSize(256));
        run(&mut steps, 3, 1);
        steps.push(MemoStep::TableSize(4096));
        run(&mut steps, 3, 6);
        churn(&mut steps, 20);
        steps.push(MemoStep::TableSize(256));
        steps.push(MemoStep::TableSize(4096));
        run(&mut steps, 3, 6);
        churn(&mut steps, 12);
        steps.push(MemoStep::Replace);
        churn(&mut steps, 40);
        run(&mut steps, 7, 4);
        steps.push(MemoStep::Replace);
        run(&mut steps, 7, 4);
        churn(&mut steps, 40);
        steps
    }

    /// Runs `memo_steps` on one codec. Every block must equal the block a fresh copy of the
    /// encoder (in a codec without a memo) makes: wire bytes or error, table state, egress
    /// compression info, and compressed and uncompressed size. Returns, per block, the wire bytes
    /// (or the error) with the table state, then the codec's `encoded_size` and every field of its
    /// `compression_info`.
    fn memo_stream() -> Vec<u8> {
        fn encode(
            codec: &mut HpackCodec,
            step: &MemoStep,
        ) -> Result<Vec<u8>, roxygen_core::buf_queue::BufQueueError> {
            match step {
                MemoStep::Inputs(fields, maximum) => {
                    let inputs = InputBlock(
                        fields
                            .iter()
                            .map(|(name, value)| {
                                Header::new(HttpHeaderName::Name(name.as_bytes()), value.as_bytes())
                            })
                            .collect(),
                    );
                    codec
                        .encode_inputs_block(&inputs, *maximum, 9)
                        .map(|block| block.as_bytes().to_vec())
                }
                MemoStep::Owned(fields) => {
                    let owned: Vec<HpackHeader> = fields
                        .iter()
                        .map(|(name, value)| HpackHeader::new(name, value))
                        .collect();
                    Ok(codec.encode(&owned))
                }
                MemoStep::TableSize(_) | MemoStep::Replace => unreachable!("not a block"),
            }
        }
        fn table_state(codec: &HpackCodec) -> Vec<u8> {
            let mut state = Vec::new();
            append_encoder_state(&mut state, &[], codec.encoder().context().table());
            state
        }

        let mut codec = HpackCodec::new();
        let mut stream = Vec::new();
        for (index, step) in memo_steps().iter().enumerate() {
            match step {
                MemoStep::TableSize(size) => {
                    codec.set_encoder_header_table_size(*size);
                    continue;
                }
                MemoStep::Replace => {
                    *codec.encoder_mut() = HpackEncoder::new(true);
                    continue;
                }
                MemoStep::Inputs(..) | MemoStep::Owned(_) => {}
            }
            let mut fresh = HpackCodec::new();
            *fresh.encoder_mut() = codec.encoder().clone();
            let actual = encode(&mut codec, step);
            assert_eq!(
                actual,
                encode(&mut fresh, step),
                "step {index}: the block a fresh copy of the encoder makes"
            );
            assert_eq!(table_state(&codec), table_state(&fresh), "step {index}");
            assert_eq!(
                codec.compression_info().egress,
                fresh.compression_info().egress,
                "step {index}"
            );
            if actual.is_ok() {
                let (size, expected) = (codec.encoded_size(), fresh.encoded_size());
                assert_eq!(
                    (size.compressed, size.uncompressed),
                    (expected.compressed, expected.uncompressed),
                    "step {index}"
                );
            }
            let wire = actual.unwrap_or_else(|error| error.to_string().into_bytes());
            append_encoder_state(&mut stream, &wire, codec.encoder().context().table());
            let size = codec.encoded_size();
            let info = codec.compression_info();
            for value in [size.compressed, size.compressed_block, size.uncompressed] {
                stream.extend_from_slice(&value.to_le_bytes());
            }
            for part in [info.egress, info.ingress] {
                for value in [
                    part.header_table_size,
                    part.bytes_stored,
                    part.headers_stored,
                    part.inserts,
                    part.blocked_inserts,
                    part.duplications,
                    part.static_refs,
                ] {
                    stream.extend_from_slice(&value.to_le_bytes());
                }
            }
        }
        stream
    }

    #[test]
    fn encoder_memo_sequence_is_unchanged() {
        let stream = memo_stream();
        assert_eq!(
            (fnv1a64(&stream), stream.len()),
            (0x8387_ef6b_0f53_633d, 234_833),
            "the encoder-memo sequence's wire bytes, table states, encoded sizes and compression \
             info, recorded at 167176432c6d"
        );
    }

    #[test]
    fn http1_sourced_mixed_case_names_encode_lowercase() {
        let mut headers = roxygen_core::headers::HttpHeaders::new();
        headers.add("X-Request-Id", "0123456789abcdef");
        headers.add("X-Tenant", "tenant-3");
        headers.add("Content-Type", "application/json");
        let block = InputBlock(
            headers
                .iter()
                .map(|(code, name, value)| Header { code, name, value })
                .collect(),
        );
        let mut codec = HpackCodec::new();
        let blocks: Vec<Vec<u8>> = (0..2)
            .map(|_| {
                codec
                    .encode_inputs_block(&block, 16 << 10, 9)
                    .expect("block fits")
                    .as_bytes()
                    .to_vec()
            })
            .collect();

        let decoded = HpackCodec::new().decode(&blocks[0]).unwrap();
        let names: Vec<&[u8]> = decoded
            .headers
            .iter()
            .map(|header| header.name.as_bytes())
            .collect();
        assert_eq!(
            names,
            [&b"x-request-id"[..], b"x-tenant", b"content-type"],
            "HTTP/1-sourced names go on the wire lowercase"
        );
        let wire = blocks.concat();
        assert_eq!(
            (fnv1a64(&wire), wire.len()),
            (0xb9c0_cbae_6936_703c, 55),
            "the HTTP/1-sourced blocks' FNV-1a digest and length, recorded at 692435845855"
        );
    }

    /// For each consumer-shaped name, the lowercase name and every variant with one uppercase
    /// letter or all letters uppercase: a fresh encoder first encodes the lowercase name with
    /// value `v1`, then the query with `v1` (a full hit on index 62) and with `v2` (a name hit on
    /// index 62). Every query encodes exactly like the lowercase name.
    #[test]
    fn mixed_case_names_index_like_lowercase() {
        const NAMES: [&str; 9] = [
            "x-request-id",
            "x-routing-key",
            "x-routing-key-copy",
            "x-token-count",
            "x-prefix-hash",
            "x-timeout-ms",
            "x-pad-req",
            "x-pad-resp",
            "x-upstream-time",
        ];
        fn encode(codec: &mut HpackCodec, name: &[u8], value: &[u8]) -> Vec<u8> {
            codec
                .encode_inputs_block(
                    &InputBlock(vec![Header::new(HttpHeaderName::Name(name), value)]),
                    16 << 10,
                    9,
                )
                .expect("block fits")
                .as_bytes()
                .to_vec()
        }

        let mut wire = Vec::new();
        for name in NAMES {
            let lowercase = name.as_bytes();
            let one_uppercase = (0..lowercase.len())
                .filter(|&position| lowercase[position].is_ascii_lowercase())
                .map(|position| {
                    let mut query = lowercase.to_vec();
                    query[position].make_ascii_uppercase();
                    query
                });
            let queries = std::iter::once(lowercase.to_vec())
                .chain(one_uppercase)
                .chain(std::iter::once(lowercase.to_ascii_uppercase()));
            let mut reference = None;
            for query in queries {
                let shown = String::from_utf8_lossy(&query).into_owned();
                let mut codec = HpackCodec::new();
                let first = encode(&mut codec, lowercase, b"v1");
                let hit = encode(&mut codec, &query, b"v1");
                let name_hit = encode(&mut codec, &query, b"v2");
                assert_eq!(
                    parse_fields(&hit),
                    [Field::Indexed(62)],
                    "{shown}: v1 is the lowercase entry"
                );
                assert_eq!(
                    parse_fields(&name_hit),
                    [Field::IncrementallyIndexedLiteral { name_index: 62 }],
                    "{shown}: v2 reuses the lowercase entry's name"
                );
                let blocks = [first, hit, name_hit];
                let reference = reference.get_or_insert_with(|| blocks.clone());
                assert_eq!(&blocks, reference, "{shown} encodes like {name}");
                wire.extend(blocks.concat());
            }
        }
        assert_eq!(
            (fnv1a64(&wire), wire.len()),
            (0x847d_c5cd_416f_3e71, 2_292),
            "the mixed-case query blocks' FNV-1a digest and length, recorded at e9d82bf3157a"
        );
    }

    /// A response block: `:status: 200` from the static table, then two literal fields without
    /// indexing whose new names are sent raw (not Huffman): `x-request-id` and the mixed-case
    /// `x-Mixed-Case`.
    fn literal_new_name_block() -> Vec<u8> {
        let fields: [(&[u8], &[u8]); 2] = [
            (b"x-request-id", b"0123456789abcdef"),
            (b"x-Mixed-Case", b"Value-Kept"),
        ];
        fields.iter().fold(vec![0x88], |mut block, (name, value)| {
            block.push(0x00);
            block.push(u8::try_from(name.len()).unwrap());
            block.extend_from_slice(name);
            block.push(u8::try_from(value.len()).unwrap());
            block.extend_from_slice(value);
            block
        })
    }

    /// Decodes `literal_new_name_block` through the HTTP/2 header path into a response message.
    /// The name counters are reset after the decoder and the process-wide static table are built,
    /// so they cover only the decode.
    fn decode_literal_new_names() -> roxygen_core::message::HttpMessage {
        let block = literal_new_name_block();
        let mut decoder = HpackDecoder::new();
        let mut info = crate::header_decode::HeaderDecodeInfo::new(false, false, true, true, false);
        assert!(StaticHeaderTable::get_header(8).is_some());
        HeaderName::take_name_buffers();
        HeaderName::take_common_name_hashes();
        decoder
            .decode_streaming(&block, &mut info)
            .expect("the block decodes");
        info.into_message().expect("the block is a valid response")
    }

    #[test]
    fn new_literal_name_allocates_once() {
        let message = decode_literal_new_names();
        assert_eq!(
            HeaderName::take_name_buffers(),
            0,
            "each literal new name is decoded borrowed and lowercased into the decoder's name \
             buffer, which the decoder allocated when it was built, before the counters reset"
        );
        let headers: Vec<(&[u8], &[u8])> = message
            .headers()
            .iter()
            .map(|(_, name, value)| (name, value))
            .collect();
        assert_eq!(
            headers,
            [
                (&b"x-request-id"[..], &b"0123456789abcdef"[..]),
                (b"x-mixed-case", b"Value-Kept"),
            ],
            "decoded names are lowercase and values exact"
        );
    }

    #[test]
    fn decoded_uncommon_names_are_not_rehashed() {
        decode_literal_new_names();
        assert_eq!(
            HeaderName::take_common_name_hashes(),
            2,
            "each literal new name is classified once, when its HeaderName is built"
        );
    }

    /// A mixed-case token of `length` bytes: letters alternate case and every seventh byte is `-`.
    fn mixed_case_name(length: usize, seed: usize) -> Vec<u8> {
        (0..length)
            .map(|at| {
                if at % 7 == 6 {
                    return b'-';
                }
                let letter = b'a' + u8::try_from((at * 5 + seed) % 26).unwrap();
                if (at + seed).is_multiple_of(2) {
                    letter.to_ascii_uppercase()
                } else {
                    letter
                }
            })
            .collect()
    }

    /// One literal field with a new name, `first` being the representation's first byte: `0x00`
    /// without indexing, `0x10` never indexed or `0x40` with incremental indexing. The name is
    /// Huffman-coded when `huffman_name`; the value is raw.
    fn literal_field(first: u8, name: &[u8], huffman_name: bool, value: &[u8]) -> Vec<u8> {
        let mut field = vec![first];
        let mut name_literal = HpackEncodeBuffer::new(huffman_name);
        name_literal.encode_literal(name);
        field.extend(name_literal.into_bytes());
        let mut value_literal = HpackEncodeBuffer::new(false);
        value_literal.encode_literal(value);
        field.extend(value_literal.into_bytes());
        field
    }

    /// Header blocks of literal fields with new names: names of 63, 64 and 65 bytes, mixed case,
    /// raw and Huffman-coded, without indexing and never indexed; shorter names after longer ones;
    /// common names, an empty name and non-ASCII bytes sent as literal names; and literals with
    /// incremental indexing, references to the entries they add and a literal without indexing
    /// that takes its name from the table, among literals with new names.
    fn literal_name_blocks() -> Vec<Vec<u8>> {
        let [n63, n64, n65] =
            [(63, 1), (64, 2), (65, 3)].map(|(length, seed)| mixed_case_name(length, seed));
        let mut name_reference = HpackEncodeBuffer::new(false);
        name_reference.encode_integer_parts(62, LITERAL.code, LITERAL.prefix_length);
        name_reference.encode_literal(b"by-index");
        let name_reference = name_reference.into_bytes();
        vec![
            [
                literal_field(0x00, &n64, false, b"v64"),
                literal_field(0x00, b"X-A", false, b"short"),
                literal_field(0x10, &n63, false, b"v63"),
                literal_field(0x00, b"x-b", true, b""),
            ]
            .concat(),
            [
                literal_field(0x00, &n65, false, b"v65"),
                literal_field(0x00, &n63, true, b"h63"),
                literal_field(0x10, &n65, true, b"h65"),
                literal_field(0x00, &n64, true, b"h64"),
                literal_field(0x00, b"X-Tail", false, b"t"),
            ]
            .concat(),
            [
                literal_field(0x00, b"Content-Type", false, b"text/plain"),
                literal_field(0x10, b"USER-AGENT", true, b"agent"),
                literal_field(0x00, b"", false, b"empty"),
                literal_field(0x00, b"X-\xc3\x89t\xc9", false, b"non-ascii"),
                literal_field(0x00, b"x-request-id", false, b"rid"),
            ]
            .concat(),
            [
                literal_field(0x40, &n63, false, b"i63"),
                literal_field(0x00, b"X-After-Index", false, b"a"),
                vec![0xbe],
                name_reference,
                literal_field(0x40, b"X-Indexed", true, b"i"),
                literal_field(0x00, &n64, false, b"after"),
                vec![0xbe, 0xbf],
            ]
            .concat(),
        ]
    }

    /// A response block: `:status: 200`, then four literal fields with new uncommon names, raw
    /// and Huffman-coded, mixed case, without indexing and never indexed.
    fn uncommon_literal_name_block() -> Vec<u8> {
        [
            vec![0x88],
            literal_field(0x00, b"x-request-id", false, b"0123456789abcdef"),
            literal_field(0x00, b"X-Mixed-Case", true, b"Value-Kept"),
            literal_field(0x10, b"x-routing-key", false, b"shard-7"),
            literal_field(0x00, b"X-Pad-Req", true, b"pad"),
        ]
        .concat()
    }

    /// Decodes `block` through the HTTP/2 header path, which does not clone names, and returns
    /// the response's headers.
    fn decode_response_headers(
        decoder: &mut HpackDecoder,
        block: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut info = crate::header_decode::HeaderDecodeInfo::new(false, false, true, true, false);
        decoder
            .decode_streaming(block, &mut info)
            .expect("the block decodes");
        let message = info.into_message().expect("the block is a valid response");
        message
            .headers()
            .iter()
            .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
            .collect()
    }

    const UNCOMMON_LITERAL_NAME_HEADERS: [(&[u8], &[u8]); 4] = [
        (b"x-request-id", b"0123456789abcdef"),
        (b"x-mixed-case", b"Value-Kept"),
        (b"x-routing-key", b"shard-7"),
        (b"x-pad-req", b"pad"),
    ];

    fn owned_headers(headers: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Vec<u8>)> {
        headers
            .iter()
            .map(|(name, value)| (name.to_vec(), value.to_vec()))
            .collect()
    }

    #[test]
    fn repeated_literal_names_reuse_the_decoder_name_buffer() {
        let block = uncommon_literal_name_block();
        let mut decoder = HpackDecoder::new();
        assert!(StaticHeaderTable::get_header(8).is_some());
        HeaderName::take_name_buffers();
        let buffers: Vec<usize> = (0..2)
            .map(|_| {
                assert_eq!(
                    decode_response_headers(&mut decoder, &block),
                    owned_headers(&UNCOMMON_LITERAL_NAME_HEADERS),
                    "decoded names are lowercase and values exact"
                );
                HeaderName::take_name_buffers()
            })
            .collect();
        assert_eq!(
            buffers,
            [0, 0],
            "literal names without indexing are lowercased into the decoder's one name buffer, \
             so neither decode allocates a name"
        );
    }

    /// The decoder allocates one 64-byte name buffer when it is built and keeps that allocation:
    /// literal names without indexing borrow it and give it back, while literals with incremental
    /// indexing (the table keeps their names), names over 64 bytes, common names and names taken
    /// from the table never take it.
    #[test]
    fn name_buffer_keeps_its_allocation_across_decodes() {
        HeaderName::take_name_buffers();
        let mut decoder = HpackDecoder::new();
        assert_eq!(
            HeaderName::take_name_buffers(),
            1,
            "the decoder allocates its one name buffer when it is built"
        );
        let (pointer, capacity) = decoder.name_buffer_allocation();
        assert_eq!(capacity, 64, "the name buffer holds 64 bytes");
        let blocks = literal_name_blocks();
        for (block, wire) in blocks
            .iter()
            .chain(&blocks)
            .chain([&uncommon_literal_name_block()])
            .enumerate()
        {
            decoder
                .decode_streaming(wire, &mut DecodeEvents::default())
                .expect("the block decodes");
            assert_eq!(
                decoder.name_buffer_allocation(),
                (pointer, 64),
                "block {block}: the decoder still holds its first name buffer"
            );
        }
    }

    /// Panics on every header.
    struct PanicOnHeader;

    impl StreamingCallback for PanicOnHeader {
        fn on_header(&mut self, _name: &HeaderName, _value: &[u8]) {
            panic!("header callback panic");
        }

        fn on_headers_complete(&mut self, _decoded_size: HttpHeaderSize, _acknowledge: bool) {}

        fn on_decode_error(&mut self, _error: HpackError) {}
    }

    /// A callback that panics while it is given a literal name drops the decoder's name buffer
    /// with that name. Later literal names each get their own allocation, as they would without
    /// the buffer; the decoder does not grow a replacement buffer.
    #[test]
    fn name_buffer_lost_to_a_callback_panic_falls_back_to_allocating() {
        let mut decoder = HpackDecoder::new();
        let first = literal_field(0x00, b"x-request-id", false, b"lost");
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decoder.decode_streaming(&first, &mut PanicOnHeader)
            }))
            .is_err()
        );
        assert!(StaticHeaderTable::get_header(8).is_some());
        HeaderName::take_name_buffers();
        assert_eq!(
            decode_response_headers(&mut decoder, &uncommon_literal_name_block()),
            owned_headers(&UNCOMMON_LITERAL_NAME_HEADERS),
            "decoded names are lowercase and values exact"
        );
        assert_eq!(
            HeaderName::take_name_buffers(),
            4,
            "each literal name is lowercased into its own allocation"
        );
        assert_eq!(
            decoder.name_buffer_allocation().1,
            0,
            "the lost buffer is not replaced"
        );
    }

    /// Streaming decode events with each header's name code ahead of its name and value.
    #[derive(Default)]
    struct CodedDecodeEvents(DecodeEvents);

    impl StreamingCallback for CodedDecodeEvents {
        fn on_header(&mut self, name: &HeaderName, value: &[u8]) {
            self.0.0.push(name.code().as_u8());
            self.0.header(name.as_bytes(), value);
        }

        fn on_headers_complete(&mut self, decoded_size: HttpHeaderSize, acknowledge: bool) {
            self.0.on_headers_complete(decoded_size, acknowledge);
        }

        fn on_decode_error(&mut self, error: HpackError) {
            self.0.on_decode_error(error);
        }
    }

    /// The golden blocks, then the literal-name blocks twice, decoded in order by one streaming
    /// and one collecting decoder per direction (the literal-name blocks by both directions):
    /// each header's code, name and value, each block's result, and each decoder's final state.
    /// Then every literal-name block decoded contiguously, split, from a cut queue and truncated
    /// on one new connection.
    #[test]
    fn decoded_literal_names_digest_is_unchanged() {
        let golden = golden_blocks();
        let literal_names = literal_name_blocks();
        let mut digest = StreamDigest::new();
        let mut streaming = [HpackDecoder::new(), HpackDecoder::new()];
        let mut collecting = [HpackDecoder::new(), HpackDecoder::new()];
        let sequence = golden
            .iter()
            .enumerate()
            .map(|(block, wire)| (block % 2, wire))
            .chain(
                literal_names
                    .iter()
                    .chain(&literal_names)
                    .flat_map(|wire| [(0, wire), (1, wire)]),
            );
        for (direction, wire) in sequence {
            let mut events = CodedDecodeEvents::default();
            let result = streaming[direction].decode_streaming(wire, &mut events);
            assert!(result.is_ok(), "every block decodes while streaming");
            events.0.streaming_result(result);
            digest.part(&events.0.0);
            let decoded = collecting[direction]
                .decode(wire)
                .expect("every block decodes while collecting");
            let mut events = DecodeEvents::default();
            for header in &decoded.headers {
                events.0.push(header.name.code().as_u8());
                events.header(header.name.as_bytes(), &header.value);
            }
            events.size(b'r', decoded.size);
            let consumed = u32::try_from(decoded.bytes_consumed).expect("consumed fits in u32");
            events.0.extend_from_slice(&consumed.to_le_bytes());
            digest.part(&events.0);
        }
        let mut states = DecodeEvents::default();
        for decoder in streaming.iter().chain(&collecting) {
            states.decoder_state(decoder);
        }
        digest.part(&states.0);
        let mut reference = HpackDecoder::new();
        for wire in &literal_names {
            digest_block_decodes(&mut digest, &mut reference, DEFAULT_TABLE_SIZE, wire);
        }
        let name_lengths: Vec<usize> = HpackDecoder::new()
            .decode(&literal_names[1])
            .expect("the block decodes")
            .headers
            .iter()
            .map(|header| header.name.len())
            .collect();
        assert_eq!(
            name_lengths,
            [65, 63, 65, 64, 6],
            "the corpus sends names over, under and at 64 bytes"
        );
        assert_eq!(
            (digest.hash, digest.length),
            (0x3b59_51bf_3754_2cb7, 1_595_913),
            "the golden and literal-name blocks' decoded codes, names, values, results and \
             decoder states, recorded at df5729995574"
        );
    }

    #[test]
    fn encoder_moves_uncommon_names_into_the_table() {
        let block = InputBlock(vec![Header::new(
            HttpHeaderName::Name(b"x-request-id"),
            b"unique-0001",
        )]);
        let mut codec = HpackCodec::new();
        assert!(StaticHeaderTable::get_header(1).is_some());
        HeaderName::take_name_buffers();
        codec
            .encode_inputs_block(&block, 16 << 10, 9)
            .expect("block fits");
        assert_eq!(
            HeaderName::take_name_buffers(),
            1,
            "the encoder builds the uncommon name once and moves it into the table"
        );
        let newest = codec
            .encoder()
            .context()
            .table()
            .get_header(1)
            .expect("the field was indexed");
        assert_eq!(
            (newest.name.as_bytes(), newest.name.code(), &*newest.value),
            (
                &b"x-request-id"[..],
                HttpHeaderCode::Other,
                &b"unique-0001"[..]
            ),
            "the table's newest entry is the encoded field"
        );
    }

    const RAW_LITERAL_FIELDS: [(&[u8], &[u8]); 3] = [
        (b"x-request-id", b"0123456789abcdef"),
        (b"x-tenant", b"tenant-3"),
        (b"x-trace", b"00-4bf92f3577b34da6-01"),
    ];

    /// Three literal fields without indexing (`0x00`), each with a raw new name and a raw value.
    fn raw_literal_block() -> Vec<u8> {
        RAW_LITERAL_FIELDS
            .iter()
            .fold(Vec::new(), |mut block, (name, value)| {
                block.push(0x00);
                block.push(u8::try_from(name.len()).unwrap());
                block.extend_from_slice(name);
                block.push(u8::try_from(value.len()).unwrap());
                block.extend_from_slice(value);
                block
            })
    }

    fn decoded_fields(decoded: &DecodedHeaderBlock) -> Vec<(&[u8], &[u8])> {
        decoded
            .headers
            .iter()
            .map(|header| (header.name.as_bytes(), &header.value[..]))
            .collect()
    }

    #[test]
    fn raw_literal_values_are_borrowed() {
        let block = raw_literal_block();
        let mut decoder = HpackDecoder::new();
        HpackDecodeBuffer::take_owned_literal_copies();
        let decoded = decoder.decode(&block).expect("the block decodes");
        assert_eq!(
            HpackDecodeBuffer::take_owned_literal_copies(),
            0,
            "raw values inside one contiguous block are borrowed, not copied"
        );
        assert_eq!(decoded_fields(&decoded), RAW_LITERAL_FIELDS);
    }

    #[test]
    fn split_raw_literal_is_still_owned() {
        let block = raw_literal_block();
        let (name, value) = RAW_LITERAL_FIELDS[1];
        let first_field = 1 + 1 + RAW_LITERAL_FIELDS[0].0.len() + 1 + RAW_LITERAL_FIELDS[0].1.len();
        let split = first_field + 1 + 1 + name.len() + 1 + value.len() / 2;
        let mut decoder = HpackDecoder::new();
        HpackDecodeBuffer::take_owned_literal_copies();
        let decoded = decoder
            .decode_segments([&block[..split], &block[split..]], block.len())
            .expect("the split block decodes");
        assert_eq!(
            HpackDecodeBuffer::take_owned_literal_copies(),
            1,
            "only the value split across the two segments is copied"
        );
        assert_eq!(decoded_fields(&decoded), RAW_LITERAL_FIELDS);
    }

    #[test]
    fn indexed_literals_stay_owned() {
        let (name, value) = RAW_LITERAL_FIELDS[0];
        let mut block = vec![0x40, u8::try_from(name.len()).unwrap()];
        block.extend_from_slice(name);
        block.push(u8::try_from(value.len()).unwrap());
        block.extend_from_slice(value);
        let mut decoder = HpackDecoder::new();
        let decoded = decoder.decode(&block).expect("the block decodes");
        assert_eq!(decoded_fields(&decoded), [(name, value)]);
        let newest = decoder
            .context()
            .table()
            .get_header(1)
            .expect("the field was indexed");
        assert_eq!(
            (newest.name.as_bytes(), &*newest.value),
            (name, value),
            "a literal with incremental indexing lands in the table with its exact value"
        );
    }

    /// One RFC 7541 section 6 field representation with the index it carries: the entry for an
    /// indexed field, otherwise the name index (0 for a literal name).
    #[derive(Debug, PartialEq)]
    enum Representation {
        Indexed(u64),
        IncrementalIndexing(u64),
        WithoutIndexing(u64),
        NeverIndexed(u64),
    }

    /// Splits a header block into its field representations, failing on a table size update.
    fn representations(block: &[u8]) -> Vec<Representation> {
        let mut fields = Vec::new();
        let mut cursor = 0;
        while cursor < block.len() {
            let first = block[cursor];
            if first & 0x80 != 0 {
                fields.push(Representation::Indexed(read_integer(block, &mut cursor, 7)));
                continue;
            }
            let (prefix_bits, representation): (u32, fn(u64) -> Representation) = match first & 0xf0
            {
                0x40..=0x70 => (6, Representation::IncrementalIndexing),
                0x10 => (4, Representation::NeverIndexed),
                0x00 => (4, Representation::WithoutIndexing),
                _ => panic!("unexpected representation {first:#04x} at byte {cursor}"),
            };
            let name_index = read_integer(block, &mut cursor, prefix_bits);
            if name_index == 0 {
                skip_string(block, &mut cursor);
            }
            skip_string(block, &mut cursor);
            fields.push(representation(name_index));
        }
        fields
    }

    /// A deterministic certificate-sized value; `seed` varies its bytes.
    fn client_certificate(seed: usize) -> Vec<u8> {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        (0..4400)
            .map(|at| ALPHABET[(at * 37 + seed) % 64])
            .collect()
    }

    #[test]
    fn oversized_literal_policy_is_pinned() {
        const XFCC: &[u8] = b"x-forwarded-client-cert";
        let mut codec = HpackCodec::new();
        let mut decoder = HpackDecoder::new();
        assert_eq!(codec.encoder().context().table().capacity(), 4096);
        let certificates = [client_certificate(0), client_certificate(1)];
        assert_ne!(certificates[0], certificates[1]);
        assert!(
            HpackHeader::bytes_for(XFCC.len(), certificates[0].len()) > 4096,
            "the certificate entry is larger than the whole table"
        );
        let observed: Vec<_> = certificates
            .iter()
            .map(|certificate| {
                let block = InputBlock(vec![
                    Header::new(HttpHeaderCode::ColonMethod.into(), b"POST"),
                    Header::new(HttpHeaderCode::ColonScheme.into(), b"https"),
                    Header::new(HttpHeaderCode::ColonPath.into(), b"/svc/Method"),
                    Header::new(HttpHeaderCode::ContentType.into(), b"application/grpc"),
                    Header {
                        code: HttpHeaderCode::Other,
                        name: XFCC,
                        value: certificate,
                    },
                ]);
                let encoded = codec
                    .encode_inputs_block(&block, 16 << 10, 9)
                    .expect("the block fits");
                let certificate = String::from_utf8(certificate.clone()).unwrap();
                assert_eq!(
                    decoder
                        .decode(encoded.as_bytes())
                        .expect("the block decodes")
                        .headers,
                    headers(&[
                        (":method", "POST"),
                        (":scheme", "https"),
                        (":path", "/svc/Method"),
                        ("content-type", "application/grpc"),
                        ("x-forwarded-client-cert", &certificate),
                    ])
                );
                let table = codec.encoder().context().table();
                assert_eq!(
                    table
                        .entries()
                        .map(|entry| (entry.name.as_bytes(), entry.value.as_ref()))
                        .collect::<Vec<_>>(),
                    [
                        (&b"content-type"[..], &b"application/grpc"[..]),
                        (b":path", b"/svc/Method"),
                    ],
                    "newest first; the certificate is never inserted"
                );
                assert_eq!(decoder.context().table().size(), table.size());
                (
                    representations(encoded.as_bytes()),
                    table.size(),
                    table.bytes(),
                )
            })
            .collect();
        // Static indices (RFC 7541 appendix A): 3 :method POST, 4 :path, 7 :scheme https,
        // 31 content-type. The certificate is a literal without indexing (section 6.2.2), so
        // the section 4.4 rule (inserting an entry larger than the table empties it) never
        // applies and the second block still references the first block's entries.
        assert_eq!(
            observed,
            [
                (
                    vec![
                        Representation::Indexed(3),
                        Representation::Indexed(7),
                        Representation::IncrementalIndexing(4),
                        Representation::IncrementalIndexing(31),
                        Representation::WithoutIndexing(0),
                    ],
                    2,
                    108,
                ),
                (
                    vec![
                        Representation::Indexed(3),
                        Representation::Indexed(7),
                        Representation::Indexed(63),
                        Representation::Indexed(62),
                        Representation::WithoutIndexing(0),
                    ],
                    2,
                    108,
                ),
            ]
        );
    }
}

mod hpack_rfc {
    use super::*;

    fn requests() -> [Vec<HpackHeader>; 3] {
        [
            headers(&[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":authority", "www.example.com"),
            ]),
            headers(&[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":authority", "www.example.com"),
                ("cache-control", "no-cache"),
            ]),
            headers(&[
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/index.html"),
                (":authority", "www.example.com"),
                ("custom-key", "custom-value"),
            ]),
        ]
    }

    fn responses() -> [Vec<HpackHeader>; 3] {
        [
            headers(&[
                (":status", "302"),
                ("cache-control", "private"),
                ("date", "Mon, 21 Oct 2013 20:13:21 GMT"),
                ("location", "https://www.example.com"),
            ]),
            headers(&[
                (":status", "307"),
                ("cache-control", "private"),
                ("date", "Mon, 21 Oct 2013 20:13:21 GMT"),
                ("location", "https://www.example.com"),
            ]),
            headers(&[
                (":status", "200"),
                ("cache-control", "private"),
                ("date", "Mon, 21 Oct 2013 20:13:22 GMT"),
                ("location", "https://www.example.com"),
                ("content-encoding", "gzip"),
                (
                    "set-cookie",
                    "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
                ),
            ]),
        ]
    }

    fn assert_matrix(
        huffman: bool,
        blocks: &[&str],
        expected: [Vec<HpackHeader>; 3],
        table_size: u32,
        table_states: [(u32, u32); 3],
    ) {
        let mut encoder = HpackEncoder::with_table_size(huffman, table_size);
        let mut decoder = HpackDecoder::with_limits(table_size, DEFAULT_MAX_UNCOMPRESSED);
        for ((headers, expected_hex), (bytes, entries)) in
            expected.iter().zip(blocks).zip(table_states)
        {
            let encoded = encoder.encode(headers);
            assert_eq!(encoded, hex(expected_hex));
            assert_eq!(
                decoder.decode(&encoded).expect("RFC block decodes").headers,
                *headers
            );
            assert_eq!(encoder.context().table().bytes(), bytes);
            assert_eq!(encoder.context().table().size(), entries);
        }
    }

    #[test]
    fn request_parameter_matrix() {
        assert_matrix(
            false,
            &[
                "828684410f7777772e6578616d706c652e636f6d",
                "828684be58086e6f2d6361636865",
                "828785bf400a637573746f6d2d6b65790c637573746f6d2d76616c7565",
            ],
            requests(),
            DEFAULT_TABLE_SIZE,
            [(57, 1), (110, 2), (164, 3)],
        );
        assert_matrix(
            true,
            &[
                "828684418cf1e3c2e5f23a6ba0ab90f4ff",
                "828684be5886a8eb10649cbf",
                "828785bf408825a849e95ba97d7f8925a849e95bb8e8b4bf",
            ],
            requests(),
            DEFAULT_TABLE_SIZE,
            [(57, 1), (110, 2), (164, 3)],
        );
    }

    #[test]
    fn response_parameter_matrix() {
        assert_matrix(
            false,
            &[
                "4803333032580770726976617465611d4d6f6e2c203231204f637420323031332032303a31333a323120474d546e1768747470733a2f2f7777772e6578616d706c652e636f6d",
                "4803333037c1c0bf",
                "88c1611d4d6f6e2c203231204f637420323031332032303a31333a323220474d54c05a04677a69707738666f6f3d4153444a4b48514b425a584f5157454f50495541585157454f49553b206d61782d6167653d333630303b2076657273696f6e3d31",
            ],
            responses(),
            256,
            [(222, 4), (222, 4), (215, 3)],
        );
        assert_matrix(
            true,
            &[
                "488264025885aec3771a4b6196d07abe941054d444a8200595040b8166e082a62d1bff6e919d29ad171863c78f0b97c8e9ae82ae43d3",
                "4883640effc1c0bf",
                "88c16196d07abe941054d444a8200595040b8166e084a62d1bffc05a839bd9ab77ad94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007",
            ],
            responses(),
            256,
            [(222, 4), (222, 4), (215, 3)],
        );
    }
}
