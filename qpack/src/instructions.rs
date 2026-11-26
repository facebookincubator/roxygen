/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The instructions of the QPACK encoder and decoder streams. Both streams are unframed, so the
//! parsers report an instruction that is not yet complete rather than an error.

use bytes::BufMut;

use crate::WireError;
use crate::integer;
use crate::string::StringLiteral;

/// An instruction on the encoder stream, sent by the encoder to the decoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncoderInstruction<'a> {
    /// Sets the dynamic table capacity, in bytes.
    SetDynamicTableCapacity(u64),
    /// Inserts an entry whose name is that of a static table entry (`static_table`) or of the
    /// dynamic table entry at a relative index.
    InsertWithNameReference {
        static_table: bool,
        name_index: u64,
        value: StringLiteral<'a>,
    },
    /// Inserts an entry with a literal name.
    InsertWithLiteralName {
        name: StringLiteral<'a>,
        value: StringLiteral<'a>,
    },
    /// Inserts a copy of the dynamic table entry at a relative index.
    Duplicate(u64),
}

impl<'a> EncoderInstruction<'a> {
    /// Parses the instruction at the start of `buf`. Returns the instruction and its length, or
    /// `None` when `buf` holds only part of one.
    pub fn parse(buf: &'a [u8]) -> Result<Option<(Self, usize)>, WireError> {
        incomplete_as_none(Self::parse_complete(buf))
    }

    /// Appends the instruction. String literals keep their coding.
    pub fn encode(&self, dst: &mut impl BufMut) {
        match *self {
            Self::SetDynamicTableCapacity(capacity) => {
                integer::encode(dst, 0b0010_0000, 5, capacity)
            }
            Self::InsertWithNameReference {
                static_table,
                name_index,
                value,
            } => {
                let t_bit = if static_table { 0b0100_0000 } else { 0 };
                integer::encode(dst, 0b1000_0000 | t_bit, 6, name_index);
                value.encode(dst, 0, 8);
            }
            Self::InsertWithLiteralName { name, value } => {
                name.encode(dst, 0b0100_0000, 6);
                value.encode(dst, 0, 8);
            }
            Self::Duplicate(index) => integer::encode(dst, 0, 5, index),
        }
    }

    fn parse_complete(buf: &'a [u8]) -> Result<(Self, usize), WireError> {
        let first = *buf.first().ok_or(WireError::Truncated)?;
        let parsed = if first & 0b1000_0000 != 0 {
            // 1T, then a 6-bit prefix name index and the value.
            let (name_index, used) = integer::decode(buf, 6)?;
            let (value, len) = StringLiteral::parse(&buf[used..], 8)?;
            let static_table = first & 0b0100_0000 != 0;
            (
                Self::InsertWithNameReference {
                    static_table,
                    name_index,
                    value,
                },
                used + len,
            )
        } else if first & 0b0100_0000 != 0 {
            // 01, then a 6-bit prefix name string and the value.
            let (name, used) = StringLiteral::parse(buf, 6)?;
            let (value, len) = StringLiteral::parse(&buf[used..], 8)?;
            (Self::InsertWithLiteralName { name, value }, used + len)
        } else if first & 0b0010_0000 != 0 {
            let (capacity, used) = integer::decode(buf, 5)?;
            (Self::SetDynamicTableCapacity(capacity), used)
        } else {
            let (index, used) = integer::decode(buf, 5)?;
            (Self::Duplicate(index), used)
        };
        Ok(parsed)
    }
}

/// An instruction on the decoder stream, sent by the decoder to the encoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecoderInstruction {
    /// The decoder has processed a field section with dynamic table references on this stream.
    SectionAcknowledgment(u64),
    /// The decoder has abandoned this stream.
    StreamCancellation(u64),
    /// The decoder has processed this many more dynamic table inserts.
    InsertCountIncrement(u64),
}

impl DecoderInstruction {
    /// Parses the instruction at the start of `buf`. Returns the instruction and its length, or
    /// `None` when `buf` holds only part of one.
    pub fn parse(buf: &[u8]) -> Result<Option<(Self, usize)>, WireError> {
        incomplete_as_none(Self::parse_complete(buf))
    }

    /// Appends the instruction.
    pub fn encode(&self, dst: &mut impl BufMut) {
        match *self {
            Self::SectionAcknowledgment(stream_id) => {
                integer::encode(dst, 0b1000_0000, 7, stream_id)
            }
            Self::StreamCancellation(stream_id) => integer::encode(dst, 0b0100_0000, 6, stream_id),
            Self::InsertCountIncrement(increment) => integer::encode(dst, 0, 6, increment),
        }
    }

    fn parse_complete(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let first = *buf.first().ok_or(WireError::Truncated)?;
        Ok(if first & 0b1000_0000 != 0 {
            let (stream_id, used) = integer::decode(buf, 7)?;
            (Self::SectionAcknowledgment(stream_id), used)
        } else if first & 0b0100_0000 != 0 {
            let (stream_id, used) = integer::decode(buf, 6)?;
            (Self::StreamCancellation(stream_id), used)
        } else {
            let (increment, used) = integer::decode(buf, 6)?;
            (Self::InsertCountIncrement(increment), used)
        })
    }
}

fn incomplete_as_none<T>(parsed: Result<T, WireError>) -> Result<Option<T>, WireError> {
    match parsed {
        Ok(parsed) => Ok(Some(parsed)),
        Err(WireError::Truncated) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::hex;

    fn raw(data: &[u8]) -> StringLiteral<'_> {
        StringLiteral {
            huffman: false,
            data,
        }
    }

    /// Returns the encoder stream of RFC 9204 B.2 to B.5, one instruction per entry.
    fn rfc9204_encoder_stream() -> Vec<(Vec<u8>, EncoderInstruction<'static>)> {
        use EncoderInstruction::*;
        vec![
            (hex("3fbd01"), SetDynamicTableCapacity(220)),
            (
                hex("c00f 7777 772e 6578 616d 706c 652e 636f 6d"),
                InsertWithNameReference {
                    static_table: true,
                    name_index: 0,
                    value: raw(b"www.example.com"),
                },
            ),
            (
                hex("c10c 2f73 616d 706c 652f 7061 7468"),
                InsertWithNameReference {
                    static_table: true,
                    name_index: 1,
                    value: raw(b"/sample/path"),
                },
            ),
            (
                hex("4a63 7573 746f 6d2d 6b65 790c 6375 7374 6f6d 2d76 616c 7565"),
                InsertWithLiteralName {
                    name: raw(b"custom-key"),
                    value: raw(b"custom-value"),
                },
            ),
            (hex("02"), Duplicate(2)),
            (
                hex("810d 6375 7374 6f6d 2d76 616c 7565 32"),
                InsertWithNameReference {
                    static_table: false,
                    name_index: 1,
                    value: raw(b"custom-value2"),
                },
            ),
        ]
    }

    #[test]
    fn parses_the_rfc9204_encoder_stream() {
        let examples = rfc9204_encoder_stream();
        let stream: Vec<u8> = examples
            .iter()
            .flat_map(|(bytes, _)| bytes.clone())
            .collect();
        let mut rest = &stream[..];
        for (bytes, expected) in &examples {
            assert_eq!(
                EncoderInstruction::parse(rest),
                Ok(Some((*expected, bytes.len())))
            );
            rest = &rest[bytes.len()..];
        }
        assert!(rest.is_empty());
    }

    #[test]
    fn encodes_the_rfc9204_encoder_stream() {
        for (bytes, instruction) in rfc9204_encoder_stream() {
            let mut out = Vec::new();
            instruction.encode(&mut out);
            assert_eq!(out, bytes, "{instruction:?}");
        }
        let huffman = StringLiteral {
            huffman: true,
            data: &[0x25, 0xa8],
        };
        let mut out = Vec::new();
        EncoderInstruction::InsertWithLiteralName {
            name: huffman,
            value: huffman,
        }
        .encode(&mut out);
        assert_eq!(out, hex("6225 a882 25a8"));
    }

    #[test]
    fn partial_encoder_instructions_are_incomplete() {
        assert_eq!(EncoderInstruction::parse(&[]), Ok(None));
        for (bytes, _) in rfc9204_encoder_stream() {
            for cut in 1..bytes.len() {
                assert_eq!(
                    EncoderInstruction::parse(&bytes[..cut]),
                    Ok(None),
                    "{bytes:x?} cut at {cut}"
                );
            }
        }
    }

    /// Returns the decoder stream of RFC 9204 B.2 to B.4, then values that need continuation bytes.
    fn decoder_stream() -> Vec<(Vec<u8>, DecoderInstruction)> {
        use DecoderInstruction::*;
        vec![
            (hex("84"), SectionAcknowledgment(4)),
            (hex("01"), InsertCountIncrement(1)),
            (hex("48"), StreamCancellation(8)),
            (hex("ffe9 06"), SectionAcknowledgment(1000)),
            (hex("7f80 01"), StreamCancellation(191)),
            (hex("3f00"), InsertCountIncrement(63)),
        ]
    }

    #[test]
    fn parses_decoder_instructions() {
        for (bytes, expected) in decoder_stream() {
            let mut with_tail = bytes.clone();
            with_tail.push(0x84);
            assert_eq!(
                DecoderInstruction::parse(&with_tail),
                Ok(Some((expected, bytes.len())))
            );
        }
    }

    #[test]
    fn encodes_decoder_instructions() {
        for (bytes, instruction) in decoder_stream() {
            let mut out = Vec::new();
            instruction.encode(&mut out);
            assert_eq!(out, bytes, "{instruction:?}");
        }
    }

    #[test]
    fn partial_decoder_instructions_are_incomplete() {
        assert_eq!(DecoderInstruction::parse(&[]), Ok(None));
        for (bytes, _) in decoder_stream() {
            for cut in 1..bytes.len() {
                assert_eq!(
                    DecoderInstruction::parse(&bytes[..cut]),
                    Ok(None),
                    "{bytes:x?} cut at {cut}"
                );
            }
        }
    }

    #[test]
    fn rejects_integers_beyond_62_bits() {
        let too_long = |first: u8| [&[first][..], &[0xff; 9], &[0x01]].concat();
        // Set Dynamic Table Capacity, Duplicate, both Insert forms, then each decoder instruction.
        for first in [0x3f, 0x1f, 0xff, 0x7f] {
            assert_eq!(
                EncoderInstruction::parse(&too_long(first)),
                Err(WireError::IntegerTooLarge),
                "{first:x}"
            );
        }
        for first in [0xff, 0x7f, 0x3f] {
            assert_eq!(
                DecoderInstruction::parse(&too_long(first)),
                Err(WireError::IntegerTooLarge),
                "{first:x}"
            );
        }
    }
}
