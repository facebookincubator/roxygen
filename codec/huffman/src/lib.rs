/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The static Huffman code of HPACK, which QPACK uses unmodified for string literals.
//!
//! RFC 7541 defines the code. Roxygen's HTTP/2 and HTTP/3 codecs both use this crate.
//! [`decode_to_slice`] reads 12 input bits per table lookup and writes up to two symbols per
//! lookup. [`encode_to_slice`] fills an output of [`encoded_size`] bytes 32 bits at a time, and
//! [`encode_with`] passes the same pieces to a caller's writer instead. [`append_encoded_sized`]
//! encodes a literal under 8 bytes one byte at a time and a longer one with [`encode_to_slice`].

/// The end-of-string symbol, EOS, which a decoded string must not contain.
pub const EOS_SYMBOL: u16 = 256;

/// The symbols other than EOS: one per byte value.
const HUFFMAN_TABLE_SIZE: usize = 256;

/// The decoding of up to two symbols for each 12-bit key. Entry layout: bits 0-7 the first symbol,
/// bits 8-15 the second, bits 16-20 the first code's length, bits 21-25 the total length and
/// bits 26-27 the symbol count. Zero means the key starts a code longer than 12 bits. Public only
/// for the HTTP/2 codec's table test.
#[doc(hidden)]
pub static DECODE_PAIRS: [u32; 4096] = {
    let mut table = [0; 4096];
    let mut first = 0;
    while first < HUFFMAN_TABLE_SIZE {
        let first_length = CODE_LENGTHS[first] as u32;
        if first_length <= 12 {
            let first_key = CODES[first] << (12 - first_length);
            let single = first as u32 | (first_length << 16) | (first_length << 21) | (1 << 26);
            let mut key = first_key;
            while key < first_key + (1 << (12 - first_length)) {
                table[key as usize] = single;
                key += 1;
            }

            let mut second = 0;
            while second < HUFFMAN_TABLE_SIZE {
                let total = first_length + CODE_LENGTHS[second] as u32;
                if total <= 12 {
                    let pair_key = first_key | (CODES[second] << (12 - total));
                    let pair = first as u32
                        | ((second as u32) << 8)
                        | (first_length << 16)
                        | (total << 21)
                        | (2 << 26);
                    let mut key = pair_key;
                    while key < pair_key + (1 << (12 - total)) {
                        table[key as usize] = pair;
                        key += 1;
                    }
                }
                second += 1;
            }
        }
        first += 1;
    }
    table
};

/// The canonical code by length (1-30): each length's first code, its code count, the offset of
/// its first symbol in `LONG_SYMBOLS`, and every symbol sorted by `(length, code)`. Decodes the
/// codes longer than `DECODE_PAIRS` covers.
const CANONICAL_CODE: ([u32; 31], [u32; 31], [u16; 31], [u16; 257]) = canonical_code();
static LONG_FIRST: [u32; 31] = CANONICAL_CODE.0;
static LONG_COUNT: [u32; 31] = CANONICAL_CODE.1;
static LONG_OFFSET: [u16; 31] = CANONICAL_CODE.2;
static LONG_SYMBOLS: [u16; 257] = CANONICAL_CODE.3;

const fn canonical_code() -> ([u32; 31], [u32; 31], [u16; 31], [u16; 257]) {
    let mut first = [0; 31];
    let mut count = [0; 31];
    let mut symbol = 0;
    while symbol < CODES.len() {
        let length = CODE_LENGTHS[symbol] as usize;
        if count[length] == 0 || CODES[symbol] < first[length] {
            first[length] = CODES[symbol];
        }
        count[length] += 1;
        symbol += 1;
    }

    let mut offset = [0; 31];
    let mut next_offset = 0;
    let mut length = 1;
    while length < 31 {
        offset[length] = next_offset;
        next_offset += count[length] as u16;
        length += 1;
    }

    let mut symbols = [u16::MAX; 257];
    symbol = 0;
    while symbol < CODES.len() {
        let length = CODE_LENGTHS[symbol] as usize;
        let rank = CODES[symbol] - first[length];
        assert!(rank < count[length], "Huffman code is not canonical");
        let slot = (offset[length] as u32 + rank) as usize;
        assert!(symbols[slot] == u16::MAX, "Huffman code is not canonical");
        symbols[slot] = symbol as u16;
        symbol += 1;
    }
    (first, count, offset, symbols)
}

/// Decodes one code longer than 12 bits from the top `bits` bits of `window`.
fn long_code(window: u64, bits: u32) -> Option<(u16, u32)> {
    let mut length = 13;
    while length <= bits.min(30) {
        let code = (window >> (64 - length)) as u32;
        let rank = code.wrapping_sub(LONG_FIRST[length as usize]);
        if rank < LONG_COUNT[length as usize] {
            let slot = u32::from(LONG_OFFSET[length as usize]) + rank;
            return Some((LONG_SYMBOLS[slot as usize], length));
        }
        length += 1;
    }
    None
}

/// Why a Huffman-coded string does not decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HuffmanError {
    /// The string contains the EOS symbol.
    Eos,
    /// The string ends with more than 7 bits of padding, or with padding that is not the most
    /// significant bits of EOS.
    Padding,
}

impl std::fmt::Display for HuffmanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Eos => "Huffman-coded string contains EOS",
            Self::Padding => "invalid Huffman padding",
        })
    }
}

impl std::error::Error for HuffmanError {}

/// Returns the length in bytes of the Huffman coding of `literal`.
#[inline]
pub fn encoded_size(literal: &[u8]) -> usize {
    literal
        .iter()
        .map(|byte| usize::from(CODE_LENGTHS[usize::from(*byte)]))
        .sum::<usize>()
        .div_ceil(8)
}

/// Appends the Huffman coding of `literal`, padded to a byte boundary with the most significant
/// bits of EOS, one byte at a time.
#[inline]
pub fn append_encoded(literal: &[u8], output: &mut Vec<u8>) {
    let mut accumulator = 0_u64;
    let mut bits = 0_u8;

    for byte in literal {
        let code = u64::from(CODES[usize::from(*byte)]);
        let code_bits = CODE_LENGTHS[usize::from(*byte)];
        accumulator = (accumulator << code_bits) | code;
        bits += code_bits;
        while bits >= 8 {
            bits -= 8;
            output.push((accumulator >> bits) as u8);
        }
    }

    if bits != 0 {
        output.push(((accumulator << (8 - bits)) | ((1_u64 << (8 - bits)) - 1)) as u8);
    }
}

/// Appends the Huffman encoding of `literal`, whose [`encoded_size`] is `size`, to `output`.
/// Panics if `literal` has 8 or more bytes and `size` is smaller than its encoded size, and also
/// if `size` is larger when debug assertions are enabled.
#[inline]
pub fn append_encoded_sized(literal: &[u8], size: usize, output: &mut Vec<u8>) {
    // Under 8 bytes the bytewise loop is cheaper than sizing and flushing words.
    if literal.len() < 8 {
        output.reserve(size);
        append_encoded(literal, output);
        return;
    }

    let start = output.len();
    output.resize(start + size, 0);
    encode_to_slice(literal, &mut output[start..]);
}

/// Passes the Huffman coding of `literal` to `put` in pieces of at most four bytes, padded to a
/// byte boundary with the most significant bits of EOS.
#[inline]
pub fn encode_with(literal: &[u8], mut put: impl FnMut(&[u8])) {
    let mut accumulator = 0_u64;
    let mut bits = 0_u32;

    // At most 31 pending bits plus one 30-bit code fit in the accumulator.
    for byte in literal {
        let code_bits = u32::from(CODE_LENGTHS[usize::from(*byte)]);
        accumulator = (accumulator << code_bits) | u64::from(CODES[usize::from(*byte)]);
        bits += code_bits;
        if bits >= 32 {
            bits -= 32;
            put(&((accumulator >> bits) as u32).to_be_bytes());
        }
    }

    // The last 0 to 31 bits and their padding, moved to the top of a word.
    let padding = (8 - bits % 8) % 8;
    let tail = ((accumulator << padding) | ((1_u64 << padding) - 1)) << (32 - bits - padding);
    let tail_len = ((bits + padding) / 8) as usize;
    if tail_len != 0 {
        put(&(tail as u32).to_be_bytes()[..tail_len]);
    }
}

/// Writes the Huffman coding of `literal` to `out`, whose length must be its [`encoded_size`].
/// Panics if `out` is shorter, and also if it is longer when debug assertions are enabled.
#[inline]
pub fn encode_to_slice(literal: &[u8], out: &mut [u8]) {
    let mut o = 0;
    encode_with(literal, |bytes| {
        out[o..o + bytes.len()].copy_from_slice(bytes);
        o += bytes.len();
    });
    debug_assert_eq!(o, out.len());
}

/// Returns the most bytes [`decode_to_slice`] writes for `encoded_len` input bytes: the shortest
/// code is five bits, and a two-symbol store may write one byte past the last symbol.
#[inline]
pub const fn max_decoded_len(encoded_len: usize) -> usize {
    encoded_len.saturating_mul(8) / 5 + 1
}

/// Decodes `encoded` into the front of `out` and returns the decoded length. Fails with
/// [`HuffmanError`] when `encoded` does not decode, and panics if `out` is shorter than
/// [`max_decoded_len`]`(encoded.len())`.
#[inline(never)]
pub fn decode_to_slice(encoded: &[u8], out: &mut [u8]) -> Result<usize, HuffmanError> {
    assert!(
        out.len() >= max_decoded_len(encoded.len()),
        "Huffman output is smaller than max_decoded_len"
    );
    // The top `bits` bits of `window` are the next input bits. Below them is more input or, once
    // the input is exhausted, ones: the EOS prefix that padding must match.
    let mut window = 0_u64;
    let mut bits = 0_u32;
    let mut next = 0;
    let mut written = 0;
    let mut tail = false;
    loop {
        if !tail {
            let remaining = encoded.len() - next;
            let word = match encoded[next..].first_chunk::<8>() {
                Some(word) => u64::from_be_bytes(*word),
                None => {
                    let mut word = [0; 8];
                    word[..remaining].copy_from_slice(&encoded[next..]);
                    u64::from_be_bytes(word)
                }
            };
            let take = ((63 - bits) as usize / 8).min(remaining);
            window |= word >> bits;
            next += take;
            bits += take as u32 * 8;
            if next == encoded.len() {
                tail = true;
                window = (window & !(u64::MAX >> bits)) | (u64::MAX >> bits);
                if bits == 0 {
                    return Ok(written);
                }
            }
        }

        loop {
            let entry = DECODE_PAIRS[(window >> 52) as usize];
            let total = (entry >> 21) & 31;
            if total.wrapping_sub(1) < bits {
                out[written..written + 2].copy_from_slice(&(entry as u16).to_le_bytes());
                written += (entry >> 26) as usize;
                window <<= total;
                bits -= total;
                if bits < 30 && !tail {
                    break;
                }
                continue;
            }

            // Only in the tail: the second symbol would read padding.
            let first_length = (entry >> 16) & 31;
            if first_length != 0 && first_length <= bits {
                out[written] = entry as u8;
                written += 1;
                window <<= first_length;
                bits -= first_length;
                continue;
            }

            if first_length == 0
                && let Some((symbol, length)) = long_code(window, bits)
            {
                if symbol == EOS_SYMBOL {
                    return Err(HuffmanError::Eos);
                }
                out[written] = symbol as u8;
                written += 1;
                window <<= length;
                bits -= length;
                if bits < 30 && !tail {
                    break;
                }
                continue;
            }

            // The remaining bits do not decode to a symbol, so they must be padding of fewer
            // than 8 one bits.
            if tail && bits < 8 && (window | (u64::MAX >> bits)) == u64::MAX {
                return Ok(written);
            }
            return Err(HuffmanError::Padding);
        }
    }
}

/// The code of each symbol, aligned to the least significant bit: the 256 byte values, then EOS.
pub const CODES: [u32; 257] = [
    0x1ff8, 0x7fffd8, 0xfffffe2, 0xfffffe3, 0xfffffe4, 0xfffffe5, 0xfffffe6, 0xfffffe7, 0xfffffe8,
    0xffffea, 0x3ffffffc, 0xfffffe9, 0xfffffea, 0x3ffffffd, 0xfffffeb, 0xfffffec, 0xfffffed,
    0xfffffee, 0xfffffef, 0xffffff0, 0xffffff1, 0xffffff2, 0x3ffffffe, 0xffffff3, 0xffffff4,
    0xffffff5, 0xffffff6, 0xffffff7, 0xffffff8, 0xffffff9, 0xffffffa, 0xffffffb, 0x14, 0x3f8,
    0x3f9, 0xffa, 0x1ff9, 0x15, 0xf8, 0x7fa, 0x3fa, 0x3fb, 0xf9, 0x7fb, 0xfa, 0x16, 0x17, 0x18,
    0x0, 0x1, 0x2, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x5c, 0xfb, 0x7ffc, 0x20, 0xffb,
    0x3fc, 0x1ffa, 0x21, 0x5d, 0x5e, 0x5f, 0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
    0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0xfc, 0x73, 0xfd, 0x1ffb, 0x7fff0,
    0x1ffc, 0x3ffc, 0x22, 0x7ffd, 0x3, 0x23, 0x4, 0x24, 0x5, 0x25, 0x26, 0x27, 0x6, 0x74, 0x75,
    0x28, 0x29, 0x2a, 0x7, 0x2b, 0x76, 0x2c, 0x8, 0x9, 0x2d, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7ffe,
    0x7fc, 0x3ffd, 0x1ffd, 0xffffffc, 0xfffe6, 0x3fffd2, 0xfffe7, 0xfffe8, 0x3fffd3, 0x3fffd4,
    0x3fffd5, 0x7fffd9, 0x3fffd6, 0x7fffda, 0x7fffdb, 0x7fffdc, 0x7fffdd, 0x7fffde, 0xffffeb,
    0x7fffdf, 0xffffec, 0xffffed, 0x3fffd7, 0x7fffe0, 0xffffee, 0x7fffe1, 0x7fffe2, 0x7fffe3,
    0x7fffe4, 0x1fffdc, 0x3fffd8, 0x7fffe5, 0x3fffd9, 0x7fffe6, 0x7fffe7, 0xffffef, 0x3fffda,
    0x1fffdd, 0xfffe9, 0x3fffdb, 0x3fffdc, 0x7fffe8, 0x7fffe9, 0x1fffde, 0x7fffea, 0x3fffdd,
    0x3fffde, 0xfffff0, 0x1fffdf, 0x3fffdf, 0x7fffeb, 0x7fffec, 0x1fffe0, 0x1fffe1, 0x3fffe0,
    0x1fffe2, 0x7fffed, 0x3fffe1, 0x7fffee, 0x7fffef, 0xfffea, 0x3fffe2, 0x3fffe3, 0x3fffe4,
    0x7ffff0, 0x3fffe5, 0x3fffe6, 0x7ffff1, 0x3ffffe0, 0x3ffffe1, 0xfffeb, 0x7fff1, 0x3fffe7,
    0x7ffff2, 0x3fffe8, 0x1ffffec, 0x3ffffe2, 0x3ffffe3, 0x3ffffe4, 0x7ffffde, 0x7ffffdf,
    0x3ffffe5, 0xfffff1, 0x1ffffed, 0x7fff2, 0x1fffe3, 0x3ffffe6, 0x7ffffe0, 0x7ffffe1, 0x3ffffe7,
    0x7ffffe2, 0xfffff2, 0x1fffe4, 0x1fffe5, 0x3ffffe8, 0x3ffffe9, 0xffffffd, 0x7ffffe3, 0x7ffffe4,
    0x7ffffe5, 0xfffec, 0xfffff3, 0xfffed, 0x1fffe6, 0x3fffe9, 0x1fffe7, 0x1fffe8, 0x7ffff3,
    0x3fffea, 0x3fffeb, 0x1ffffee, 0x1ffffef, 0xfffff4, 0xfffff5, 0x3ffffea, 0x7ffff4, 0x3ffffeb,
    0x7ffffe6, 0x3ffffec, 0x3ffffed, 0x7ffffe7, 0x7ffffe8, 0x7ffffe9, 0x7ffffea, 0x7ffffeb,
    0xffffffe, 0x7ffffec, 0x7ffffed, 0x7ffffee, 0x7ffffef, 0x7fffff0, 0x3ffffee, 0x3fffffff,
];

/// The length in bits of each symbol's code.
pub const CODE_LENGTHS: [u8; 257] = [
    13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 30, 28,
    28, 28, 28, 28, 28, 28, 28, 28, 6, 10, 10, 12, 13, 6, 8, 11, 10, 10, 8, 11, 8, 6, 6, 6, 5, 5,
    5, 6, 6, 6, 6, 6, 6, 6, 7, 8, 15, 6, 12, 10, 13, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 7, 8, 13, 19, 13, 14, 6, 15, 5, 6, 5, 6, 5, 6, 6, 6, 5, 7, 7, 6, 6,
    6, 5, 6, 7, 6, 5, 5, 6, 7, 7, 7, 7, 7, 15, 11, 14, 13, 28, 20, 22, 20, 20, 22, 22, 22, 23, 22,
    23, 23, 23, 23, 23, 24, 23, 24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24, 22,
    21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23, 21, 21, 22, 21, 23, 22, 23, 23, 20,
    22, 22, 22, 23, 22, 22, 23, 26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25, 19,
    21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27, 20, 24, 20, 21, 22, 21, 21, 23, 22,
    22, 25, 25, 24, 24, 26, 23, 26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26, 30,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_word_encoders_match_the_bytewise_encoder() {
        let mut state = 0x7541_c0de_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for length in 0..=300 {
            let random: Vec<u8> = (0..length).map(|_| next() as u8).collect();
            let text: Vec<u8> = (0..length).map(|_| b' ' + (next() % 95) as u8).collect();
            for literal in [random, text] {
                let mut expected = Vec::new();
                append_encoded(&literal, &mut expected);
                let mut out = vec![0xa5; encoded_size(&literal)];
                encode_to_slice(&literal, &mut out);
                assert_eq!(out, expected, "{literal:02x?}");
                let mut streamed = Vec::new();
                encode_with(&literal, |bytes| streamed.extend_from_slice(bytes));
                assert_eq!(streamed, expected, "{literal:02x?}");
            }
        }
    }

    fn decode(encoded: &[u8]) -> Result<Vec<u8>, HuffmanError> {
        let mut out = vec![0; max_decoded_len(encoded.len())];
        let length = decode_to_slice(encoded, &mut out)?;
        out.truncate(length);
        Ok(out)
    }

    #[test]
    fn a_string_with_eos_is_an_eos_error() {
        for encoded in [
            // EOS, then 00 or two bits of padding.
            &[0xff, 0xff, 0xff, 0xfc][..],
            &[0xff, 0xff, 0xff, 0xff],
            // "a" (00011), then EOS, then five bits of padding.
            &[0x1f, 0xff, 0xff, 0xff, 0xff],
            // "0" (00000), then EOS, then "0" and a bit of padding.
            &[0x07, 0xff, 0xff, 0xff, 0xf8, 0x01],
        ] {
            assert_eq!(decode(encoded), Err(HuffmanError::Eos), "{encoded:02x?}");
        }
    }

    #[test]
    fn a_string_with_bad_padding_is_a_padding_error() {
        for encoded in [
            // Eight, then 24 bits of padding, which do not complete EOS.
            &[0xff][..],
            &[0xff, 0xff, 0xff],
            // "0" (00000), then eleven bits of padding.
            &[0x07, 0xff],
            // "0", then 000.
            &[0x00],
            // "a" (00011), then 110.
            &[0x1e],
        ] {
            assert_eq!(
                decode(encoded),
                Err(HuffmanError::Padding),
                "{encoded:02x?}"
            );
        }
        assert_eq!(decode(&[0x07]), Ok(b"0".to_vec()));
    }
}
