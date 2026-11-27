/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use thiserror::Error;

/// The HTTP/3 error code for an encoded field section that the decoder cannot interpret.
pub const QPACK_DECOMPRESSION_FAILED: u64 = 0x0200;
/// The HTTP/3 error code for an encoder stream instruction that the decoder cannot interpret.
pub const QPACK_ENCODER_STREAM_ERROR: u64 = 0x0201;
/// The HTTP/3 error code for a decoder stream instruction that the encoder cannot interpret.
pub const QPACK_DECODER_STREAM_ERROR: u64 = 0x0202;

/// A malformed primitive: a prefixed integer or a string literal.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WireError {
    /// The input ends inside the primitive.
    #[error("input ends inside an integer or string literal")]
    Truncated,
    /// An integer is larger than 2^62 - 1, or uses more continuation bytes than such a value needs.
    #[error("integer exceeds 62 bits")]
    IntegerTooLarge,
    /// A Huffman-coded string contains the EOS symbol.
    #[error("Huffman-coded string contains EOS")]
    HuffmanEos,
    /// A Huffman-coded string ends with more than 7 bits of padding, or with padding that is not
    /// the start of EOS.
    #[error("invalid Huffman padding")]
    HuffmanPadding,
}

/// An encoded field section that cannot be decoded. Each is a QPACK_DECOMPRESSION_FAILED error.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DecodeError {
    /// A malformed integer or string literal.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// A field line references a static table index past the end of the table.
    #[error("static table index {0} is out of range")]
    InvalidStaticIndex(u64),
    /// A field line references a dynamic table entry that the section cannot use: one at or past
    /// its Required Insert Count, or one evicted. Under a maximum capacity of 0 that is every
    /// reference.
    #[error("reference to a dynamic table entry that the section cannot use")]
    DynamicTableReference,
    /// The prefix encodes a Required Insert Count that no count encodes to, or a non-zero one
    /// where the dynamic table cannot be used: under a maximum capacity of 0, or in a method that
    /// does not acknowledge sections. The value is as encoded.
    #[error("invalid encoded Required Insert Count {0}")]
    RequiredInsertCount(u64),
    /// The prefix's sign bit makes the Base negative.
    #[error("negative Base")]
    NegativeBase,
    /// The Required Insert Count is larger than the number of entries received, so the section
    /// would block, and the decoder allows no blocked streams.
    #[error("Required Insert Count {required} exceeds the {inserted} entries received")]
    Blocked {
        /// The section's Required Insert Count.
        required: u64,
        /// The number of entries the decoder has received.
        inserted: u64,
    },
}

/// An encoder stream instruction that the decoder rejects. Each is a QPACK_ENCODER_STREAM_ERROR
/// connection error.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EncoderStreamError {
    /// An integer larger than 2^62 - 1, or with more continuation bytes than such a value needs.
    #[error("integer exceeds 62 bits")]
    IntegerTooLarge,
    /// Set Dynamic Table Capacity above the advertised maximum.
    #[error("dynamic table capacity {0} exceeds the advertised maximum")]
    CapacityExceeded(u64),
    /// An Insert instruction while the capacity is below 32 bytes, the size of the smallest entry.
    #[error("insert into a dynamic table smaller than any entry")]
    Insert,
    /// A Duplicate instruction, which references an entry of the empty dynamic table.
    #[error("duplicate of an entry of the empty dynamic table")]
    Duplicate,
    /// An insert whose entry is larger than the dynamic table capacity.
    #[error("entry of at least {size} bytes exceeds the dynamic table capacity of {capacity}")]
    EntryTooLarge {
        /// The entry's size, or the least size its length prefixes allow.
        size: u64,
        /// The dynamic table capacity.
        capacity: u64,
    },
    /// An insert with a name reference past the end of the static table.
    #[error("static table index {0} is out of range")]
    InvalidStaticIndex(u64),
    /// An insert with a name reference, or a Duplicate, of a relative index that names no entry
    /// of the dynamic table.
    #[error("relative index {0} names no dynamic table entry")]
    InvalidRelativeIndex(u64),
    /// An insert with an invalid Huffman-coded string.
    #[error("invalid Huffman-coded string")]
    InvalidHuffman,
}

/// A decoder stream instruction that the encoder rejects. Each is a QPACK_DECODER_STREAM_ERROR
/// connection error.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DecoderStreamError {
    /// An integer larger than 2^62 - 1, or with more continuation bytes than such a value needs.
    #[error("integer exceeds 62 bits")]
    IntegerTooLarge,
    /// A Section Acknowledgment, though the encoder's field sections do not use the dynamic table.
    #[error("section acknowledgment for stream {0}, which has no dynamic table references")]
    SectionAcknowledgment(u64),
    /// An Insert Count Increment, though the encoder has not inserted any entries.
    #[error("insert count increment of {0} with no inserts")]
    InsertCountIncrement(u64),
}

impl DecodeError {
    /// Returns the HTTP/3 error code, which is always [`QPACK_DECOMPRESSION_FAILED`].
    pub const fn code(&self) -> u64 {
        QPACK_DECOMPRESSION_FAILED
    }

    /// Returns whether the error closes the connection. An integer too large to decode resets only
    /// the request stream, and every other failure is a connection error.
    pub const fn is_connection_error(&self) -> bool {
        !matches!(self, Self::Wire(WireError::IntegerTooLarge))
    }
}

impl EncoderStreamError {
    /// Returns the HTTP/3 error code, which is always [`QPACK_ENCODER_STREAM_ERROR`].
    pub const fn code(&self) -> u64 {
        QPACK_ENCODER_STREAM_ERROR
    }
}

impl DecoderStreamError {
    /// Returns the HTTP/3 error code, which is always [`QPACK_DECODER_STREAM_ERROR`].
    pub const fn code(&self) -> u64 {
        QPACK_DECODER_STREAM_ERROR
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_errors_to_rfc9204_codes() {
        assert_eq!(QPACK_DECOMPRESSION_FAILED, 0x0200);
        assert_eq!(QPACK_ENCODER_STREAM_ERROR, 0x0201);
        assert_eq!(QPACK_DECODER_STREAM_ERROR, 0x0202);
        assert_eq!(DecodeError::NegativeBase.code(), QPACK_DECOMPRESSION_FAILED);
        assert_eq!(
            DecodeError::Wire(WireError::Truncated).code(),
            QPACK_DECOMPRESSION_FAILED
        );
        assert_eq!(
            EncoderStreamError::Insert.code(),
            QPACK_ENCODER_STREAM_ERROR
        );
        assert_eq!(
            DecoderStreamError::InsertCountIncrement(1).code(),
            QPACK_DECODER_STREAM_ERROR
        );
        assert!(!DecodeError::Wire(WireError::IntegerTooLarge).is_connection_error());
        for connection_error in [
            DecodeError::Wire(WireError::Truncated),
            DecodeError::Wire(WireError::HuffmanEos),
            DecodeError::Wire(WireError::HuffmanPadding),
            DecodeError::InvalidStaticIndex(99),
            DecodeError::DynamicTableReference,
            DecodeError::RequiredInsertCount(1),
            DecodeError::NegativeBase,
        ] {
            assert!(
                connection_error.is_connection_error(),
                "{connection_error:?}"
            );
        }
    }
}
