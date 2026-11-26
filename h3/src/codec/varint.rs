/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! QUIC variable-length integers (RFC 9000).

use bytes::Buf;
use bytes::BufMut;

/// The largest value a variable-length integer holds.
pub const VARINT_MAX: u64 = (1 << 62) - 1;

/// Returns the length of the shortest encoding of `value`, or the longest length for a value
/// above [`VARINT_MAX`], which cannot be encoded.
pub(crate) const fn varint_len(value: u64) -> usize {
    if value < 1 << 6 {
        1
    } else if value < 1 << 14 {
        2
    } else if value < 1 << 30 {
        4
    } else {
        8
    }
}

/// Returns the length of the integer whose first byte is `first`.
pub(crate) const fn varint_len_from_first(first: u8) -> usize {
    1 << (first >> 6)
}

/// Writes the shortest encoding of `value`, which is at most [`VARINT_MAX`].
pub(crate) fn put_varint<B: BufMut>(buf: &mut B, value: u64) {
    debug_assert!(value <= VARINT_MAX);
    match varint_len(value) {
        1 => buf.put_u8(value as u8),
        2 => buf.put_u16(value as u16 | 0x4000),
        4 => buf.put_u32(value as u32 | 0x8000_0000),
        _ => buf.put_u64(value | 0xc000_0000_0000_0000),
    }
}

/// Decodes the integer at the start of `buf` and returns it with its length, or `None` if `buf`
/// ends inside it.
pub(crate) fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let len = varint_len_from_first(*buf.first()?);
    let bytes = buf.get(..len)?;
    let value = bytes[1..]
        .iter()
        .fold(u64::from(bytes[0] & 0x3f), |value, &byte| {
            (value << 8) | u64::from(byte)
        });
    Some((value, len))
}

/// A reader for one integer that may arrive split across several inputs.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct VarIntReader {
    bytes: [u8; 8],
    len: u8,
}

impl VarIntReader {
    /// Consumes the integer's bytes from `input` and returns it once complete.
    pub(crate) fn read<B: Buf>(&mut self, input: &mut B) -> Option<u64> {
        if self.len == 0
            && let Some((value, len)) = decode_varint(input.chunk())
        {
            input.advance(len);
            return Some(value);
        }
        while input.has_remaining() {
            self.bytes[usize::from(self.len)] = input.get_u8();
            self.len += 1;
            let len = usize::from(self.len);
            if len == varint_len_from_first(self.bytes[0]) {
                self.len = 0;
                return decode_varint(&self.bytes[..len]).map(|(value, _)| value);
            }
        }
        None
    }

    /// Returns whether some but not all of an integer's bytes have been read.
    pub(crate) fn is_partial(&self) -> bool {
        self.len > 0
    }
}

const GREASE_BASE: u64 = 0x21;
const GREASE_STEP: u64 = 0x1f;
const GREASE_COUNT: u64 = (VARINT_MAX - GREASE_BASE) / GREASE_STEP + 1;

/// Returns the reserved value `0x1f * N + 0x21` that `random` selects, for frame types, setting
/// identifiers, stream types and error codes that exercise a peer's handling of unknown values.
pub(crate) const fn grease(random: u64) -> u64 {
    GREASE_BASE + GREASE_STEP * (random % GREASE_COUNT)
}

pub(crate) const fn is_grease(value: u64) -> bool {
    value >= GREASE_BASE && (value - GREASE_BASE).is_multiple_of(GREASE_STEP)
}
