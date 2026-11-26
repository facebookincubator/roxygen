/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::DecodeError;
use crate::EncoderStreamError;
use crate::FieldLine;
use crate::StaticRef;
use crate::WireError;
use crate::integer;
use crate::static_table::STATIC_TABLE;
use crate::string::StringLiteral;

/// A field section decoder for an endpoint that advertises a dynamic table capacity of 0, so
/// every dynamic table reference is an error. It reuses its Huffman scratch buffers.
#[derive(Debug, Default)]
pub struct Decoder {
    name: Vec<u8>,
    value: Vec<u8>,
}

impl Decoder {
    /// Creates a decoder with empty scratch buffers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes the complete encoded field section `block`, passing each field line to `on_field`
    /// in order. On error, the lines already passed belong to an invalid section.
    pub fn decode_field_section(
        &mut self,
        block: &[u8],
        mut on_field: impl FnMut(FieldLine<'_>),
    ) -> Result<(), DecodeError> {
        self.decode_resolved_section(block, |line, _| on_field(line))
    }

    /// Decodes like [`Self::decode_field_section`], and also passes the static table entry that
    /// each line's name, or name and value, came from.
    pub fn decode_resolved_section(
        &mut self,
        block: &[u8],
        mut on_field: impl FnMut(FieldLine<'_>, StaticRef),
    ) -> Result<(), DecodeError> {
        let mut input = Input(block);
        let required_insert_count = input.integer(8)?;
        if required_insert_count != 0 {
            return Err(DecodeError::RequiredInsertCount(required_insert_count));
        }
        // With a Required Insert Count of 0, a set sign bit means Base = -Delta Base - 1.
        let sign = input.first().is_some_and(|b| b & 0b1000_0000 != 0);
        input.integer(7)?;
        if sign {
            return Err(DecodeError::NegativeBase);
        }
        while let Some(first) = input.first() {
            let (line, static_ref) = if first & 0b1000_0000 != 0 {
                // Indexed Field Line: 1, T, then a 6-bit prefix index.
                if first & 0b0100_0000 == 0 {
                    return Err(DecodeError::DynamicTableReference);
                }
                let index = static_index(input.integer(6)?)?;
                let (name, value) = STATIC_TABLE[usize::from(index)];
                let line = FieldLine {
                    name,
                    value,
                    never_indexed: false,
                };
                (line, StaticRef::Entry(index))
            } else if first & 0b0100_0000 != 0 {
                // Literal Field Line with Name Reference: 01, N, T, then a 4-bit prefix index.
                if first & 0b0001_0000 == 0 {
                    return Err(DecodeError::DynamicTableReference);
                }
                let index = static_index(input.integer(4)?)?;
                let (name, _) = STATIC_TABLE[usize::from(index)];
                let value = text(input.string(8)?, &mut self.value)?;
                let line = FieldLine {
                    name,
                    value,
                    never_indexed: first & 0b0010_0000 != 0,
                };
                (line, StaticRef::Name(index))
            } else if first & 0b0010_0000 != 0 {
                // Literal Field Line with Literal Name: 001, N, then a 4-bit prefix name string.
                let name = input.string(4)?;
                let value = input.string(8)?;
                let line = FieldLine {
                    name: text(name, &mut self.name)?,
                    value: text(value, &mut self.value)?,
                    never_indexed: first & 0b0001_0000 != 0,
                };
                (line, StaticRef::Unresolved)
            } else {
                // 0001 and 0000 start the post-Base forms, which reference the dynamic table.
                return Err(DecodeError::DynamicTableReference);
            };
            on_field(line, static_ref);
        }
        Ok(())
    }

    /// Processes bytes from the peer's encoder stream and returns how many were consumed. The
    /// caller keeps the rest, a partial instruction, and passes it again with the next bytes.
    pub fn feed_encoder_stream(&mut self, buf: &[u8]) -> Result<usize, EncoderStreamError> {
        let mut used = 0;
        while let Some(&first) = buf.get(used) {
            // Inserts (1 or 01) and Duplicate (000) are invalid under a maximum capacity of 0, so
            // they fail on their first byte, and Set Dynamic Table Capacity (001) must set 0.
            if first & 0b1100_0000 != 0 {
                return Err(EncoderStreamError::Insert);
            }
            if first & 0b0010_0000 == 0 {
                return Err(EncoderStreamError::Duplicate);
            }
            match integer::decode(&buf[used..], 5) {
                Ok((0, len)) => used += len,
                Ok((capacity, _)) => return Err(EncoderStreamError::CapacityExceeded(capacity)),
                Err(WireError::Truncated) => break,
                Err(_) => return Err(EncoderStreamError::IntegerTooLarge),
            }
        }
        Ok(used)
    }
}

/// The unread part of a field section.
struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn first(&self) -> Option<u8> {
        self.0.first().copied()
    }

    fn integer(&mut self, prefix_bits: u8) -> Result<u64, WireError> {
        let (value, used) = integer::decode(self.0, prefix_bits)?;
        self.0 = &self.0[used..];
        Ok(value)
    }

    fn string(&mut self, prefix_bits: u8) -> Result<StringLiteral<'a>, WireError> {
        let (literal, used) = StringLiteral::parse(self.0, prefix_bits)?;
        self.0 = &self.0[used..];
        Ok(literal)
    }
}

/// Returns `index` if it names a static table entry.
fn static_index(index: u64) -> Result<u8, DecodeError> {
    u8::try_from(index)
        .ok()
        .filter(|&i| usize::from(i) < STATIC_TABLE.len())
        .ok_or(DecodeError::InvalidStaticIndex(index))
}

/// Returns the bytes of `literal`: its own data when raw, else its decoding in `scratch`, which
/// keeps its length between calls so that it is zeroed only when it grows.
fn text<'a>(literal: StringLiteral<'a>, scratch: &'a mut Vec<u8>) -> Result<&'a [u8], DecodeError> {
    if !literal.huffman {
        return Ok(literal.data);
    }
    let bound = roxygen_huffman::max_decoded_len(literal.data.len());
    if scratch.len() < bound {
        scratch.resize(bound, 0);
    }
    let len = roxygen_huffman::decode_to_slice(literal.data, scratch).map_err(WireError::from)?;
    Ok(&scratch[..len])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Encoder;
    use crate::test_util::hex;

    type Lines = Vec<(Vec<u8>, Vec<u8>, bool)>;

    fn decode(block: &[u8]) -> Result<Lines, DecodeError> {
        let mut lines = Vec::new();
        Decoder::new().decode_field_section(block, |line| {
            lines.push((line.name.to_vec(), line.value.to_vec(), line.never_indexed));
        })?;
        Ok(lines)
    }

    fn line(name: &str, value: &str, never_indexed: bool) -> (Vec<u8>, Vec<u8>, bool) {
        (name.into(), value.into(), never_indexed)
    }

    #[test]
    fn decodes_rfc9204_b1() {
        let block = hex("0000 510b 2f69 6e64 6578 2e68 746d 6c");
        assert_eq!(
            decode(&block),
            Ok(vec![line(":path", "/index.html", false)])
        );
    }

    #[test]
    fn decodes_indexed_static_lines() {
        let expected = vec![
            line(":method", "GET", false),
            line(":status", "200", false),
            line("x-frame-options", "sameorigin", false),
            line(":authority", "", false),
        ];
        assert_eq!(decode(&hex("0000 d1d9 ff23 c0")), Ok(expected));
    }

    #[test]
    fn decodes_literal_lines_in_both_codings() {
        let huffman = hex(
            "0000 5188 60d5 485f 2bce 9a68 2f01 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf 2261 6200",
        );
        let expected = vec![
            line(":path", "/index.html", false),
            line("custom-key", "custom-value", false),
            line("ab", "", false),
        ];
        assert_eq!(decode(&huffman), Ok(expected));
        let never_indexed = [
            &hex("0000 7f45 06")[..],
            b"secret",
            &hex("7f02 03"),
            b"GET",
            &hex("3701"),
            b"x-secret",
            &[0x01],
            b"v",
        ]
        .concat();
        let expected = vec![
            line("authorization", "secret", true),
            line(":method", "GET", true),
            line("x-secret", "v", true),
        ];
        assert_eq!(decode(&never_indexed), Ok(expected));
    }

    #[test]
    fn passes_the_static_entry_of_each_line() {
        for index in 0..99u8 {
            let (name, value) = STATIC_TABLE[usize::from(index)];
            // An Indexed Field Line; Literal Field Lines with a Name Reference, a raw value and a
            // Huffman-coded "a", the second with N; then a Literal Field Line with Literal Name.
            let mut block = hex("0000");
            integer::encode(&mut block, 0b1100_0000, 6, index.into());
            integer::encode(&mut block, 0b0101_0000, 4, index.into());
            block.extend([0x01, b'v']);
            integer::encode(&mut block, 0b0111_0000, 4, index.into());
            block.extend([0x81, 0x1f]);
            block.extend(b"\x25:path\x01/");
            let mut lines = Vec::new();
            Decoder::new()
                .decode_resolved_section(&block, |line, static_ref| {
                    lines.push((line.name.to_vec(), line.value.to_vec(), static_ref));
                })
                .unwrap();
            let expected = vec![
                (name.to_vec(), value.to_vec(), StaticRef::Entry(index)),
                (name.to_vec(), b"v".to_vec(), StaticRef::Name(index)),
                (name.to_vec(), b"a".to_vec(), StaticRef::Name(index)),
                (b":path".to_vec(), b"/".to_vec(), StaticRef::Unresolved),
            ];
            assert_eq!(lines, expected, "{index}");
        }
    }

    #[test]
    fn rejects_dynamic_table_references() {
        for block in [
            // Indexed Field Line with T=0, then with a post-Base index.
            "0000 80",
            "0000 10",
            // Literal Field Line with a dynamic Name Reference, without and with N.
            "0000 4000",
            "0000 6000",
            // Literal Field Line with a post-Base Name Reference, without and with N.
            "0000 0000",
            "0000 0800",
            // A dynamic reference after a valid line.
            "0000 d1 81",
        ] {
            assert_eq!(
                decode(&hex(block)),
                Err(DecodeError::DynamicTableReference),
                "{block}"
            );
        }
    }

    #[test]
    fn rejects_a_nonzero_required_insert_count() {
        // The field sections of RFC 9204 B.2 and B.4, then static lines under a non-zero count.
        assert_eq!(
            decode(&hex("0381 1011")),
            Err(DecodeError::RequiredInsertCount(3))
        );
        assert_eq!(
            decode(&hex("0500 80c1 81")),
            Err(DecodeError::RequiredInsertCount(5))
        );
        assert_eq!(
            decode(&hex("0100 d1")),
            Err(DecodeError::RequiredInsertCount(1))
        );
    }

    #[test]
    fn rejects_static_indices_past_the_table() {
        // Index 99 in an Indexed Field Line and in a Name Reference, then a 40-bit index.
        assert_eq!(
            decode(&hex("0000 ff24")),
            Err(DecodeError::InvalidStaticIndex(99))
        );
        assert_eq!(
            decode(&hex("0000 5f54 00")),
            Err(DecodeError::InvalidStaticIndex(99))
        );
        let mut block = hex("0000");
        integer::encode(&mut block, 0b1100_0000, 6, 1 << 40);
        assert_eq!(
            decode(&block),
            Err(DecodeError::InvalidStaticIndex(1 << 40))
        );
        assert_eq!(
            decode(&hex("0000 5f53 00")),
            Ok(vec![line("x-frame-options", "", false)])
        );
    }

    #[test]
    fn rejects_malformed_primitives() {
        let wire = |e| Err(DecodeError::Wire(e));
        for block in [
            "",
            "00",
            "0000 51",
            "0000 510b 2f69",
            "0000 2f",
            "0000 2f01 25a8",
            "0000 ff",
            "0000 5f",
        ] {
            assert_eq!(decode(&hex(block)), wire(WireError::Truncated), "{block}");
        }
        // Ten continuation bytes after a full prefix, in an index, a Required Insert Count and a
        // Delta Base.
        for block in [
            "0000 ffff ffff ffff ffff ffff 01",
            "ffff ffff ffff ffff ffff 01",
            "00ff ffff ffff ffff ffff ff01",
        ] {
            assert_eq!(
                decode(&hex(block)),
                wire(WireError::IntegerTooLarge),
                "{block}"
            );
        }
        assert_eq!(
            decode(&hex("0000 5181 ff")),
            wire(WireError::HuffmanPadding)
        );
        assert_eq!(
            decode(&hex("0000 5184 ffff ffff")),
            wire(WireError::HuffmanEos)
        );
        assert_eq!(
            decode(&hex("0000 29ff 00")),
            wire(WireError::HuffmanPadding)
        );
    }

    /// Returns field sections that cover every representation the encoder emits.
    fn sample_sections() -> Vec<Vec<(Vec<u8>, Vec<u8>, bool)>> {
        let request = vec![
            line(":method", "GET", false),
            line(":scheme", "https", false),
            line(":authority", "www.example.com", false),
            line(":path", "/index.html?q=1", false),
            line("user-agent", "h2load nghttp2/1.64.0", false),
            line("accept", "*/*", false),
            line("cookie", "a=b", true),
        ];
        let response = vec![
            line(":status", "200", false),
            line("content-type", "text/plain", false),
            line("content-length", "1234", false),
            line("server", "roxygen", false),
            line("x-trace", "", false),
            line("set-cookie", "id=42; Secure", true),
        ];
        let full_matches = STATIC_TABLE
            .iter()
            .map(|&(n, v)| (n.to_vec(), v.to_vec(), false))
            .collect();
        let name_matches = STATIC_TABLE
            .iter()
            .map(|&(n, _)| (n.to_vec(), b"\x7f\x80".to_vec(), true))
            .collect();
        let all_bytes: Vec<u8> = (0..=255).collect();
        let unusual = vec![
            (all_bytes.clone(), all_bytes.clone(), false),
            (Vec::new(), Vec::new(), false),
            (b"x-long".to_vec(), vec![b'a'; 20_000], false),
            (vec![b'n'; 300], vec![0xff; 300], true),
        ];
        vec![
            request,
            response,
            full_matches,
            name_matches,
            unusual,
            Vec::new(),
        ]
    }

    #[test]
    fn round_trips_static_field_sections() {
        let mut decoder = Decoder::new();
        for huffman in [true, false] {
            let mut encoder = Encoder::new();
            encoder.set_huffman(huffman);
            for section in sample_sections() {
                let mut block = Vec::new();
                let lines = section.iter().map(|(n, v, never)| FieldLine {
                    name: n,
                    value: v,
                    never_indexed: *never,
                });
                encoder.encode_field_section(lines, &mut block);
                let mut decoded = Vec::new();
                decoder
                    .decode_field_section(&block, |l| {
                        decoded.push((l.name.to_vec(), l.value.to_vec(), l.never_indexed))
                    })
                    .unwrap();
                assert_eq!(decoded, section, "huffman {huffman}");
            }
        }
    }

    #[test]
    fn truncated_sections_fail_or_decode_a_prefix() {
        for section in sample_sections().into_iter().take(2) {
            let mut block = Vec::new();
            let lines = section.iter().map(|(n, v, never)| FieldLine {
                name: n,
                value: v,
                never_indexed: *never,
            });
            Encoder::new().encode_field_section(lines, &mut block);
            for cut in 0..block.len() {
                match decode(&block[..cut]) {
                    Ok(lines) => assert_eq!(lines[..], section[..lines.len()], "cut at {cut}"),
                    Err(e) => {
                        assert_eq!(e, DecodeError::Wire(WireError::Truncated), "cut at {cut}")
                    }
                }
            }
        }
    }

    #[test]
    fn checks_the_base() {
        // With a Required Insert Count of 0, a set sign bit means a negative Base.
        assert_eq!(decode(&hex("0080 d1")), Err(DecodeError::NegativeBase));
        assert_eq!(decode(&hex("00ff 05 d1")), Err(DecodeError::NegativeBase));
        // Any positive Delta Base is valid when the section does not use the dynamic table.
        assert_eq!(
            decode(&hex("007f 05 d1")),
            Ok(vec![line(":method", "GET", false)])
        );
    }

    #[test]
    fn accepts_only_a_zero_capacity_on_the_encoder_stream() {
        let feed = |buf: &[u8]| Decoder::new().feed_encoder_stream(buf);
        assert_eq!(feed(&[]), Ok(0));
        assert_eq!(feed(&[0x20]), Ok(1));
        assert_eq!(feed(&[0x20, 0x20]), Ok(2));
        // A capacity that needs continuation bytes stays unconsumed until they arrive.
        assert_eq!(feed(&[0x20, 0x3f]), Ok(1));
        assert_eq!(
            feed(&hex("3fbd01")),
            Err(EncoderStreamError::CapacityExceeded(220))
        );
        assert_eq!(
            feed(&hex("3fff ffff ffff ffff ffff 01")),
            Err(EncoderStreamError::IntegerTooLarge)
        );
        // Inserts and duplicates fail on their first byte.
        for first in [0xc0, 0x80, 0x4a, 0x60] {
            assert_eq!(
                feed(&[0x20, first]),
                Err(EncoderStreamError::Insert),
                "{first:x}"
            );
        }
        assert_eq!(feed(&[0x20, 0x02]), Err(EncoderStreamError::Duplicate));
        assert_eq!(feed(&[0x1f]), Err(EncoderStreamError::Duplicate));
    }
}
