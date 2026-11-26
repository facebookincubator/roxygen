/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! String literals: an H flag, a prefixed length, then the raw or Huffman-coded bytes. RFC 9204
//! extends the RFC 7541 form so that a literal may start mid-byte: an N-bit prefix string literal
//! puts H at bit N - 1 of the first byte and the length in the N - 1 bits below it.

use bytes::BufMut;

use crate::WireError;
use crate::huffman;
use crate::integer;

/// A string literal in wire form, as in a field line or an encoder stream instruction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StringLiteral<'a> {
    /// Whether `data` is Huffman-coded.
    pub huffman: bool,
    /// The bytes after the length, still Huffman-coded when `huffman` is set.
    pub data: &'a [u8],
}

impl<'a> StringLiteral<'a> {
    /// Reads a `prefix_bits`-bit prefix string literal from the start of `buf`, ignoring the bits
    /// of the first byte above the H flag. Returns the literal and the number of bytes read.
    pub(crate) fn parse(buf: &'a [u8], prefix_bits: u8) -> Result<(Self, usize), WireError> {
        let huffman = buf
            .first()
            .is_some_and(|&b| b & (1 << (prefix_bits - 1)) != 0);
        let (len, used) = integer::decode(buf, prefix_bits - 1)?;
        let rest = &buf[used..];
        if len > rest.len() as u64 {
            return Err(WireError::Truncated);
        }
        let len = len as usize;
        Ok((
            Self {
                huffman,
                data: &rest[..len],
            },
            used + len,
        ))
    }

    /// Appends the literal as a `prefix_bits`-bit prefix string literal, keeping its coding.
    /// `first` is as for the `encode` function.
    pub(crate) fn encode(&self, dst: &mut impl BufMut, first: u8, prefix_bits: u8) {
        let h_flag = if self.huffman {
            1 << (prefix_bits - 1)
        } else {
            0
        };
        integer::encode(dst, first | h_flag, prefix_bits - 1, self.data.len() as u64);
        dst.put_slice(self.data);
    }

    /// Appends the string's bytes to `dst`, Huffman-decoding them if needed.
    pub fn decode_into(&self, dst: &mut Vec<u8>) -> Result<(), WireError> {
        if self.huffman {
            huffman::decode(self.data, dst)
        } else {
            dst.extend_from_slice(self.data);
            Ok(())
        }
    }
}

/// Appends `value` as a `prefix_bits`-bit prefix string literal. With `huffman` set, the bytes are
/// Huffman-coded when that is strictly shorter. `first` holds the bits of the first byte above
/// the H flag; the bits from H down must be zero.
pub fn encode(dst: &mut impl BufMut, first: u8, prefix_bits: u8, value: &[u8], huffman: bool) {
    let h_flag = 1 << (prefix_bits - 1);
    let coded_len = if huffman {
        huffman::encoded_len(value)
    } else {
        usize::MAX
    };
    if coded_len < value.len() {
        integer::encode(dst, first | h_flag, prefix_bits - 1, coded_len as u64);
        huffman::encode(value, dst);
    } else {
        integer::encode(dst, first, prefix_bits - 1, value.len() as u64);
        dst.put_slice(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::hex;

    fn encoded(first: u8, prefix_bits: u8, value: &[u8], huffman: bool) -> Vec<u8> {
        let mut out = Vec::new();
        encode(&mut out, first, prefix_bits, value, huffman);
        out
    }

    #[test]
    fn encodes_huffman_only_when_strictly_shorter() {
        // RFC 7541 C.4.1, and the same value without Huffman coding.
        let www = b"www.example.com";
        assert_eq!(
            encoded(0, 8, www, true),
            hex("8c f1e3 c2e5 f23a 6ba0 ab90 f4ff")
        );
        assert_eq!(encoded(0, 8, www, false), [&[0x0f][..], www].concat());
        // Two zero bytes take 26 bits when Huffman-coded, and "&" takes exactly 8.
        assert_eq!(encoded(0, 8, b"\0\0", true), [0x02, 0, 0]);
        assert_eq!(encoded(0, 8, b"&", true), [0x01, b'&']);
        assert_eq!(encoded(0, 8, b"", true), [0x00]);
        // A 6-bit prefix after the 01 pattern: raw as in RFC 9204 B.3, then Huffman-coded.
        let key = b"custom-key";
        assert_eq!(encoded(0x40, 6, key, false), [&[0x4a][..], key].concat());
        assert_eq!(encoded(0x40, 6, key, true), hex("68 25a8 49e9 5ba9 7d7f"));
        // A 4-bit prefix after the 0010 pattern, where eight bytes overflow the 3-bit length.
        assert_eq!(
            encoded(0x20, 4, key, true),
            hex("2f 01 25a8 49e9 5ba9 7d7f")
        );
        assert_eq!(encoded(0x30, 4, b"ab", true), [0x32, b'a', b'b']);
    }

    fn parsed(buf: &[u8], prefix_bits: u8) -> (bool, Vec<u8>, usize) {
        let (literal, used) = StringLiteral::parse(buf, prefix_bits).unwrap();
        let mut value = b"old".to_vec();
        literal.decode_into(&mut value).unwrap();
        assert_eq!(&value[..3], b"old");
        (literal.huffman, value[3..].to_vec(), used)
    }

    #[test]
    fn parses_raw_and_huffman_literals() {
        let www = b"www.example.com".to_vec();
        let coded = hex("8c f1e3 c2e5 f23a 6ba0 ab90 f4ff 99");
        assert_eq!(parsed(&coded, 8), (true, www.clone(), 13));
        let raw = [&[0x0f][..], &www, b"tail"].concat();
        assert_eq!(parsed(&raw, 8), (false, www, 16));
        assert_eq!(
            parsed(&hex("4a 6375 7374 6f6d 2d6b 6579"), 6),
            (false, b"custom-key".to_vec(), 11)
        );
        assert_eq!(
            parsed(&hex("2f 01 25a8 49e9 5ba9 7d7f"), 4),
            (true, b"custom-key".to_vec(), 10)
        );
        assert_eq!(parsed(&[0xc0], 6), (false, Vec::new(), 1));
    }

    #[test]
    fn rejects_malformed_literals() {
        let parse =
            |buf: &[u8], prefix_bits| StringLiteral::parse(buf, prefix_bits).map(|(_, used)| used);
        assert_eq!(parse(&[], 8), Err(WireError::Truncated));
        assert_eq!(parse(&[0x03, b'a', b'b'], 8), Err(WireError::Truncated));
        assert_eq!(parse(&[0x7f], 8), Err(WireError::Truncated));
        // A length of 2^62 - 1, then the same length plus one.
        let mut max = Vec::new();
        integer::encode(&mut max, 0, 7, integer::MAX_VALUE);
        assert_eq!(parse(&max, 8), Err(WireError::Truncated));
        let mut too_long = Vec::new();
        integer::encode(&mut too_long, 0, 7, integer::MAX_VALUE + 1);
        assert_eq!(parse(&too_long, 8), Err(WireError::IntegerTooLarge));
        // Huffman errors surface when the bytes are decoded.
        let (eos, _) = StringLiteral::parse(&[0x84, 0xff, 0xff, 0xff, 0xff], 8).unwrap();
        assert_eq!(eos.decode_into(&mut Vec::new()), Err(WireError::HuffmanEos));
        let (padding, _) = StringLiteral::parse(&[0x81, 0xff], 8).unwrap();
        assert_eq!(
            padding.decode_into(&mut Vec::new()),
            Err(WireError::HuffmanPadding)
        );
    }
}
