/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::BufMut;

use crate::DecoderStreamError;
use crate::FieldLine;
use crate::StaticRef;
use crate::instructions::DecoderInstruction;
use crate::integer;
use crate::static_table::STATIC_TABLE;
use crate::static_table::StaticMatch;
use crate::static_table::find;
use crate::static_table::find_by_name_index;
use crate::string;

/// A field section encoder that uses only the static table and literals, so its field sections
/// never block the peer's decoder and it does not send encoder stream instructions.
#[derive(Clone, Debug)]
pub struct Encoder {
    huffman: bool,
}

impl Default for Encoder {
    fn default() -> Self {
        Self { huffman: true }
    }
}

impl Encoder {
    /// Creates an encoder with Huffman coding on.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether string literals are Huffman-coded when that makes them shorter. On by default.
    pub fn set_huffman(&mut self, huffman: bool) {
        self.huffman = huffman;
    }

    /// Appends the encoded field section for `fields` to `dst`, in order.
    pub fn encode_field_section<'a>(
        &self,
        fields: impl IntoIterator<Item = FieldLine<'a>>,
        dst: &mut impl BufMut,
    ) {
        let fields = fields
            .into_iter()
            .map(|field| (field, StaticRef::Unresolved));
        self.encode_resolved_section(fields, dst);
    }

    /// Encodes like [`Self::encode_field_section`], given each line's [`StaticRef`], which spares
    /// looking up its name. The encoder checks that an entry given by index has the line's name,
    /// and value for [`StaticRef::Entry`], and otherwise looks the line up. It writes a line given
    /// as [`StaticRef::Unlisted`] with a literal name.
    pub fn encode_resolved_section<'a>(
        &self,
        fields: impl IntoIterator<Item = (FieldLine<'a>, StaticRef)>,
        dst: &mut impl BufMut,
    ) {
        // Required Insert Count 0 and Delta Base 0.
        dst.put_u8(0);
        dst.put_u8(0);
        for (field, static_ref) in fields {
            let n = u8::from(field.never_indexed);
            let entry = |index: u8| STATIC_TABLE.get(usize::from(index));
            let found = match static_ref {
                StaticRef::Entry(index) if entry(index) == Some(&(field.name, field.value)) => {
                    Some(StaticMatch::Full(index))
                }
                StaticRef::Name(index) if entry(index).is_some_and(|e| e.0 == field.name) => {
                    Some(find_by_name_index(index, field.value))
                }
                StaticRef::Unlisted => None,
                _ => find(field.name, field.value),
            };
            match found {
                // A never-indexed line stays a literal even when the static table has it.
                Some(StaticMatch::Full(index)) if !field.never_indexed => {
                    // Indexed Field Line: 1, T=1, then a 6-bit prefix index.
                    integer::encode(dst, 0b1100_0000, 6, u64::from(index));
                }
                Some(StaticMatch::Full(index) | StaticMatch::Name(index)) => {
                    // Literal Field Line with Name Reference: 01, N, T=1, a 4-bit prefix index.
                    integer::encode(dst, 0b0101_0000 | n << 5, 4, u64::from(index));
                    string::encode(dst, 0, 8, field.value, self.huffman);
                }
                None => {
                    // Literal Field Line with Literal Name: 001, N, a 4-bit prefix name string.
                    string::encode(dst, 0b0010_0000 | n << 4, 4, field.name, self.huffman);
                    string::encode(dst, 0, 8, field.value, self.huffman);
                }
            }
        }
    }

    /// Processes bytes from the peer's decoder stream and returns how many were consumed. The
    /// caller keeps the rest, a partial instruction, and passes it again with the next bytes.
    pub fn feed_decoder_stream(&mut self, buf: &[u8]) -> Result<usize, DecoderStreamError> {
        let mut used = 0;
        loop {
            let parsed = DecoderInstruction::parse(&buf[used..]);
            match parsed.map_err(|_| DecoderStreamError::IntegerTooLarge)? {
                None => return Ok(used),
                Some((DecoderInstruction::StreamCancellation(_), len)) => used += len,
                Some((DecoderInstruction::SectionAcknowledgment(stream_id), _)) => {
                    return Err(DecoderStreamError::SectionAcknowledgment(stream_id));
                }
                // Even an increment of 0 is an error.
                Some((DecoderInstruction::InsertCountIncrement(increment), _)) => {
                    return Err(DecoderStreamError::InsertCountIncrement(increment));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::static_table::NAME_LOOKUPS;
    use crate::test_util::hex;

    fn encode(encoder: &Encoder, fields: &[FieldLine<'_>]) -> Vec<u8> {
        let mut out = Vec::new();
        encoder.encode_field_section(fields.iter().copied(), &mut out);
        out
    }

    #[test]
    fn encodes_rfc9204_b1() {
        let fields = [FieldLine::new(b":path", b"/index.html")];
        let mut raw = Encoder::new();
        raw.set_huffman(false);
        assert_eq!(
            encode(&raw, &fields),
            hex("0000 510b 2f69 6e64 6578 2e68 746d 6c")
        );
        // The default encoder Huffman-codes the value, which takes 8 bytes rather than 11.
        assert_eq!(
            encode(&Encoder::new(), &fields),
            hex("0000 5188 60d5 485f 2bce 9a68")
        );
    }

    #[test]
    fn encodes_full_static_matches_as_indexed_lines() {
        let fields = [
            FieldLine::new(b":method", b"GET"),
            FieldLine::new(b":status", b"200"),
            // Index 98 overflows the 6-bit prefix.
            FieldLine::new(b"x-frame-options", b"sameorigin"),
            FieldLine::new(b":authority", b""),
        ];
        assert_eq!(encode(&Encoder::new(), &fields), hex("0000 d1d9 ff23 c0"));
    }

    #[test]
    fn encodes_unlisted_names_as_literals() {
        let fields = [
            FieldLine::new(b"custom-key", b"custom-value"),
            FieldLine::new(b"ab", b""),
        ];
        let expected = "0000 2f01 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf 2261 6200";
        assert_eq!(encode(&Encoder::new(), &fields), hex(expected));
        let mut raw = Encoder::new();
        raw.set_huffman(false);
        let expected = [
            &hex("0000 2703")[..],
            b"custom-key",
            &[0x0c],
            b"custom-value",
            &hex("2261 6200"),
        ];
        assert_eq!(encode(&raw, &fields), expected.concat());
    }

    #[test]
    fn never_indexed_lines_are_literals_with_the_n_bit() {
        let fields = [
            FieldLine::never_indexed(b"authorization", b"secret"),
            // A full static match still becomes a literal.
            FieldLine::never_indexed(b":method", b"GET"),
            FieldLine::never_indexed(b"x-secret", b"v"),
        ];
        let mut raw = Encoder::new();
        raw.set_huffman(false);
        let expected = [
            &hex("0000 7f45 06")[..],
            b"secret",
            &hex("7f02 03"),
            b"GET",
            &hex("3701"),
            b"x-secret",
            &[0x01],
            b"v",
        ];
        assert_eq!(encode(&raw, &fields), expected.concat());
    }

    #[test]
    fn resolved_lines_skip_the_name_lookup_and_encode_as_before() {
        let mut lines = Vec::new();
        for (index, &(name, value)) in STATIC_TABLE.iter().enumerate() {
            let index = index as u8;
            lines.push((FieldLine::new(name, value), StaticRef::Entry(index)));
            lines.push((
                FieldLine::never_indexed(name, value),
                StaticRef::Entry(index),
            ));
            lines.push((FieldLine::new(name, value), StaticRef::Name(index)));
            lines.push((FieldLine::new(name, b"other"), StaticRef::Name(index)));
            lines.push((FieldLine::never_indexed(name, b""), StaticRef::Name(index)));
        }
        lines.push((FieldLine::new(b"x-unlisted", b"value"), StaticRef::Unlisted));
        lines.push((
            FieldLine::never_indexed(b"x-secret", b"v"),
            StaticRef::Unlisted,
        ));
        for huffman in [true, false] {
            let mut encoder = Encoder::new();
            encoder.set_huffman(huffman);
            let expected = encode(
                &encoder,
                &lines.iter().map(|(line, _)| *line).collect::<Vec<_>>(),
            );
            NAME_LOOKUPS.set(0);
            let mut out = Vec::new();
            encoder.encode_resolved_section(lines.iter().copied(), &mut out);
            assert_eq!(NAME_LOOKUPS.get(), 0, "huffman {huffman}");
            assert_eq!(out, expected, "huffman {huffman}");
        }
        // Entries that do not have the line's name and value are not used.
        let mismatched = [
            (FieldLine::new(b":path", b"/"), StaticRef::Entry(17)),
            (FieldLine::new(b":method", b"get"), StaticRef::Entry(17)),
            (
                FieldLine::new(b"content-type", b"text/css"),
                StaticRef::Name(0),
            ),
            (FieldLine::new(b"x-a", b"b"), StaticRef::Entry(99)),
            (FieldLine::new(b"x-a", b"b"), StaticRef::Name(255)),
        ];
        let mut out = Vec::new();
        Encoder::new().encode_resolved_section(mismatched, &mut out);
        let lines: Vec<_> = mismatched.iter().map(|(line, _)| *line).collect();
        assert_eq!(out, encode(&Encoder::new(), &lines));
    }

    #[test]
    fn ignores_only_stream_cancellations_on_the_decoder_stream() {
        let feed = |buf: &[u8]| Encoder::new().feed_decoder_stream(buf);
        assert_eq!(feed(&[]), Ok(0));
        assert_eq!(feed(&hex("48 7f80 01")), Ok(4));
        // A stream ID that needs continuation bytes stays unconsumed until they arrive.
        assert_eq!(feed(&hex("48 7f80")), Ok(1));
        assert_eq!(
            feed(&hex("48 84")),
            Err(DecoderStreamError::SectionAcknowledgment(4))
        );
        assert_eq!(
            feed(&hex("01")),
            Err(DecoderStreamError::InsertCountIncrement(1))
        );
        assert_eq!(
            feed(&hex("00")),
            Err(DecoderStreamError::InsertCountIncrement(0))
        );
        assert_eq!(
            feed(&hex("7fff ffff ffff ffff ffff 01")),
            Err(DecoderStreamError::IntegerTooLarge)
        );
    }
}
