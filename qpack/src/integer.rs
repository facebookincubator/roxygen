/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Prefixed integers (RFC 7541, used unmodified by RFC 9204).

use bytes::BufMut;

use crate::WireError;

/// The largest integer the decoder accepts. RFC 9204 requires decoding integers of up to 62 bits.
pub const MAX_VALUE: u64 = (1 << 62) - 1;

/// Appends `value` as an integer with a `prefix_bits`-bit prefix. `first` holds the bits of the
/// first byte above the prefix; its prefix bits must be zero.
pub fn encode(dst: &mut impl BufMut, first: u8, prefix_bits: u8, value: u64) {
    let max_prefix = prefix_max(prefix_bits);
    if value < u64::from(max_prefix) {
        dst.put_u8(first | value as u8);
        return;
    }
    dst.put_u8(first | max_prefix);
    let mut rest = value - u64::from(max_prefix);
    while rest >= 0x80 {
        dst.put_u8(0x80 | (rest & 0x7f) as u8);
        rest >>= 7;
    }
    dst.put_u8(rest as u8);
}

/// Returns the length of `value` encoded with a `prefix_bits`-bit prefix.
pub fn encoded_len(prefix_bits: u8, value: u64) -> usize {
    let max_prefix = u64::from(prefix_max(prefix_bits));
    if value < max_prefix {
        return 1;
    }
    let rest = value - max_prefix;
    2 + (u64::BITS - rest.leading_zeros()).saturating_sub(1) as usize / 7
}

/// Reads an integer with a `prefix_bits`-bit prefix from the start of `buf`, ignoring the bits of
/// the first byte above the prefix. Returns the value and the number of bytes read.
pub fn decode(buf: &[u8], prefix_bits: u8) -> Result<(u64, usize), WireError> {
    let max_prefix = prefix_max(prefix_bits);
    let first = *buf.first().ok_or(WireError::Truncated)?;
    let mut value = u64::from(first & max_prefix);
    if value < u64::from(max_prefix) {
        return Ok((value, 1));
    }
    let mut shift = 0;
    let mut pos = 1;
    loop {
        // Nine 7-bit groups hold any value up to MAX_VALUE; at shift 56 the sum still fits a u64.
        if shift > 56 {
            return Err(WireError::IntegerTooLarge);
        }
        let byte = *buf.get(pos).ok_or(WireError::Truncated)?;
        pos += 1;
        value += u64::from(byte & 0x7f) << shift;
        if value > MAX_VALUE {
            return Err(WireError::IntegerTooLarge);
        }
        if byte & 0x80 == 0 {
            return Ok((value, pos));
        }
        shift += 7;
    }
}

fn prefix_max(prefix_bits: u8) -> u8 {
    debug_assert!((1..=8).contains(&prefix_bits));
    ((1u16 << prefix_bits) - 1) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(first: u8, prefix_bits: u8, value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        encode(&mut out, first, prefix_bits, value);
        out
    }

    #[test]
    fn encodes_rfc7541_examples() {
        // RFC 7541 C.1.1, C.1.2 and C.1.3, then the same values under flag bits above the prefix.
        assert_eq!(encoded(0, 5, 10), [0b0000_1010]);
        assert_eq!(encoded(0, 5, 1337), [0b0001_1111, 0b1001_1010, 0b0000_1010]);
        assert_eq!(encoded(0, 8, 42), [0b0010_1010]);
        assert_eq!(encoded(0b1110_0000, 5, 10), [0b1110_1010]);
        assert_eq!(
            encoded(0b1010_0000, 5, 1337),
            [0b1011_1111, 0b1001_1010, 0b0000_1010]
        );
    }

    #[test]
    fn decodes_rfc7541_examples() {
        assert_eq!(decode(&[0b0000_1010], 5), Ok((10, 1)));
        assert_eq!(decode(&[0b1110_1010], 5), Ok((10, 1)));
        assert_eq!(
            decode(&[0b0001_1111, 0b1001_1010, 0b0000_1010], 5),
            Ok((1337, 3))
        );
        assert_eq!(
            decode(&[0b1011_1111, 0b1001_1010, 0b0000_1010, 0xff], 5),
            Ok((1337, 3))
        );
        assert_eq!(decode(&[0b0010_1010], 8), Ok((42, 1)));
    }

    #[test]
    fn reports_truncated_input() {
        assert_eq!(decode(&[], 5), Err(WireError::Truncated));
        assert_eq!(decode(&[0b0001_1111], 5), Err(WireError::Truncated));
        assert_eq!(
            decode(&[0b0001_1111, 0b1001_1010], 5),
            Err(WireError::Truncated)
        );
        assert_eq!(decode(&[0xff, 0x80, 0x80], 8), Err(WireError::Truncated));
    }

    #[test]
    fn round_trips_values_around_every_prefix_boundary() {
        for prefix_bits in 1..=8u8 {
            let max_prefix = (1u64 << prefix_bits) - 1;
            for value in [
                0,
                max_prefix - 1,
                max_prefix,
                max_prefix + 127,
                max_prefix + 128,
                1 << 32,
            ] {
                let flags = !(max_prefix as u8);
                let bytes = encoded(flags, prefix_bits, value);
                assert_eq!(bytes[0] & flags, flags, "flag bits survive");
                assert_eq!(decode(&bytes, prefix_bits), Ok((value, bytes.len())));
                assert_eq!(
                    bytes.len() == 1,
                    value < max_prefix,
                    "{value} with {prefix_bits} bits"
                );
            }
        }
    }

    #[test]
    fn enforces_the_62_bit_limit() {
        for prefix_bits in 1..=8 {
            let max = encoded(0, prefix_bits, MAX_VALUE);
            assert_eq!(decode(&max, prefix_bits), Ok((MAX_VALUE, max.len())));
            for value in [MAX_VALUE + 1, u64::MAX] {
                let big = encoded(0, prefix_bits, value);
                assert_eq!(decode(&big, prefix_bits), Err(WireError::IntegerTooLarge));
            }
        }
        // Nine continuation bytes cover 62 bits; a tenth is too long even when its bits are zero.
        let mut padded = vec![0x1f];
        padded.extend([0x80; 8]);
        padded.push(0);
        assert_eq!(decode(&padded, 5), Ok((31, 10)));
        padded.insert(1, 0x80);
        assert_eq!(decode(&padded, 5), Err(WireError::IntegerTooLarge));
    }
}
