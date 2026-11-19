/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::OnceLock;

use super::HpackError;

const EOS_SYMBOL: u16 = 256;
pub const HUFFMAN_TABLE_SIZE: usize = 256;
pub const EOS_HPACK: u32 = 0x3fff_ffff;

/// Decodes up to two symbols from the next 12 input bits. Entry layout: bits 0-7 the first symbol,
/// bits 8-15 the second, bits 16-20 the first code's length, bits 21-25 the total length and
/// bits 26-27 the symbol count. Zero means the key starts a code longer than 12 bits.
static DECODE_PAIRS: [u32; 4096] = {
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HuffNode {
    children: [Option<usize>; 2],
    symbol: Option<u16>,
}

impl HuffNode {
    pub const fn is_leaf(&self) -> bool {
        self.symbol.is_some()
    }

    pub const fn symbol(&self) -> Option<u16> {
        self.symbol
    }
}

pub type SuperHuffNode = [HuffNode; 256];

#[derive(Clone, Debug)]
pub struct HuffTree {
    nodes: Vec<HuffNode>,
}

impl HuffTree {
    fn new() -> Self {
        let mut nodes = vec![HuffNode::default()];
        for (symbol, (&code, &length)) in CODES.iter().zip(CODE_LENGTHS.iter()).enumerate() {
            let mut node = 0;
            for shift in (0..length).rev() {
                let bit = usize::from(((code >> shift) & 1) != 0);
                let next = match nodes[node].children[bit] {
                    Some(next) => next,
                    None => {
                        let next = nodes.len();
                        nodes.push(HuffNode::default());
                        nodes[node].children[bit] = Some(next);
                        next
                    }
                };
                node = next;
            }
            nodes[node].symbol = Some(symbol as u16);
        }
        Self { nodes }
    }

    pub fn get_encode_size(&self, literal: &[u8]) -> usize {
        literal
            .iter()
            .map(|byte| usize::from(CODE_LENGTHS[usize::from(*byte)]))
            .sum::<usize>()
            .div_ceil(8)
    }

    pub fn encode(&self, literal: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.get_encode_size(literal));
        append_encoded(literal, &mut output);
        output
    }

    pub fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, HpackError> {
        let mut output = vec![0; max_decoded_len(encoded.len())];
        let length = decode_to_slice(encoded, &mut output)?;
        output.truncate(length);
        Ok(output)
    }

    pub fn get_code(&self, byte: u8) -> (u32, u8) {
        let index = usize::from(byte);
        (CODES[index], CODE_LENGTHS[index])
    }

    pub fn codes_table(&self) -> &'static [u32] {
        &CODES[..HUFFMAN_TABLE_SIZE]
    }

    pub fn bits_table(&self) -> &'static [u8] {
        &CODE_LENGTHS[..HUFFMAN_TABLE_SIZE]
    }

    pub fn nodes(&self) -> &[HuffNode] {
        &self.nodes
    }
}

pub fn huff_tree() -> &'static HuffTree {
    static TREE: OnceLock<HuffTree> = OnceLock::new();
    TREE.get_or_init(HuffTree::new)
}

pub fn encoded_size(literal: &[u8]) -> usize {
    literal
        .iter()
        .map(|byte| usize::from(CODE_LENGTHS[usize::from(*byte)]))
        .sum::<usize>()
        .div_ceil(8)
}

pub fn encode(literal: &[u8]) -> Vec<u8> {
    huff_tree().encode(literal)
}

pub(super) fn append_encoded(literal: &[u8], output: &mut Vec<u8>) {
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
pub(super) fn append_encoded_sized(literal: &[u8], size: usize, output: &mut Vec<u8>) {
    // Under 8 bytes the bytewise loop is cheaper than sizing and flushing words.
    if literal.len() < 8 {
        output.reserve(size);
        append_encoded(literal, output);
        return;
    }

    let start = output.len();
    output.resize(start + size, 0);
    let out = &mut output[start..];
    let mut accumulator = 0_u64;
    let mut bits = 0_u32;
    let mut o = 0;

    // At most 31 pending bits plus one 30-bit code fit in the accumulator.
    for byte in literal {
        let code_bits = u32::from(CODE_LENGTHS[usize::from(*byte)]);
        accumulator = (accumulator << code_bits) | u64::from(CODES[usize::from(*byte)]);
        bits += code_bits;
        if bits >= 32 {
            bits -= 32;
            out[o..o + 4].copy_from_slice(&((accumulator >> bits) as u32).to_be_bytes());
            o += 4;
        }
    }

    let padding = (8 - bits % 8) % 8;
    accumulator = (accumulator << padding) | ((1_u64 << padding) - 1);
    bits += padding;
    while bits >= 8 {
        bits -= 8;
        out[o] = (accumulator >> bits) as u8;
        o += 1;
    }
    debug_assert_eq!(o, size);
}

pub fn decode(encoded: &[u8]) -> Result<Vec<u8>, HpackError> {
    huff_tree().decode(encoded)
}

/// The most bytes [`decode_to_slice`] writes for `encoded_len` input bytes: the shortest code is
/// five bits, and a two-symbol store may write one byte past the last symbol.
pub(super) const fn max_decoded_len(encoded_len: usize) -> usize {
    encoded_len.saturating_mul(8) / 5 + 1
}

/// Decodes `encoded` into the front of `out`, which must hold
/// [`max_decoded_len`]`(encoded.len())` bytes, and returns the decoded length.
#[inline(never)]
pub(super) fn decode_to_slice(encoded: &[u8], out: &mut [u8]) -> Result<usize, HpackError> {
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
                    return Err(HpackError::InvalidHuffmanCode);
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
            return Err(HpackError::InvalidHuffmanCode);
        }
    }
}

pub fn code(byte: u8) -> (u32, u8) {
    huff_tree().get_code(byte)
}

const CODES: [u32; 257] = [
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

const CODE_LENGTHS: [u8; 257] = [
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
    use std::sync::OnceLock;

    use super::*;

    const KEY_BITS: u32 = 12;

    /// The symbol whose code is a prefix of the top of an `nbits`-bit value.
    fn prefix_symbol(bits: u32, nbits: u32) -> Option<(u32, u32)> {
        (0..=EOS_SYMBOL).find_map(|symbol| {
            let length = u32::from(CODE_LENGTHS[usize::from(symbol)]);
            (length <= nbits && bits >> (nbits - length) == CODES[usize::from(symbol)])
                .then_some((u32::from(symbol), length))
        })
    }

    #[derive(Clone, Copy, Default)]
    struct ReferenceNode {
        children: [Option<usize>; 2],
        symbol: Option<u16>,
    }

    /// A binary tree of the 257 HPACK codes, built from the code table.
    fn reference_tree() -> Vec<ReferenceNode> {
        let mut nodes = vec![ReferenceNode::default()];
        for symbol in 0..=EOS_SYMBOL {
            let code = CODES[usize::from(symbol)];
            let length = CODE_LENGTHS[usize::from(symbol)];
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
    fn reference(encoded: &[u8]) -> Result<Vec<u8>, HpackError> {
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

    fn decode_with_slice(encoded: &[u8]) -> Result<Vec<u8>, HpackError> {
        let mut out = vec![0; max_decoded_len(encoded.len())];
        let length = decode_to_slice(encoded, &mut out)?;
        out.truncate(length);
        Ok(out)
    }

    #[test]
    fn long_codes_decode_through_the_fallback() {
        let long_symbols = (0..=u8::MAX).filter(|&byte| CODE_LENGTHS[usize::from(byte)] > 12);
        for symbol in long_symbols {
            // Up to seven 5-bit symbols in front put the long code at every bit phase.
            for phase in 0..8 {
                let prefix = vec![b'0'; phase];
                let alone = [&prefix[..], &[symbol]].concat();
                let runs =
                    (1..=9).map(|run| [&prefix[..], b"a", &vec![symbol; run], b"e"].concat());
                for literal in std::iter::once(alone).chain(runs) {
                    let encoded = encode(&literal);
                    assert_eq!(reference(&encoded), Ok(literal.clone()), "{literal:02x?}");
                    assert_eq!(
                        decode_with_slice(&encoded),
                        Ok(literal.clone()),
                        "{literal:02x?}"
                    );
                }
            }
        }
    }

    /// The code bits of `literal`, most significant first, without padding.
    fn code_bits(literal: &[u8]) -> Vec<bool> {
        literal
            .iter()
            .flat_map(|&byte| {
                let code = CODES[usize::from(byte)];
                let length = CODE_LENGTHS[usize::from(byte)];
                (0..length).rev().map(move |shift| (code >> shift) & 1 == 1)
            })
            .collect()
    }

    /// Packs bits into bytes, filling the last byte with `fill`.
    fn pack(bits: &[bool], fill: bool) -> Vec<u8> {
        bits.chunks(8)
            .map(|chunk| {
                (0..8).fold(0, |byte, index| {
                    (byte << 1) | u8::from(chunk.get(index).copied().unwrap_or(fill))
                })
            })
            .collect()
    }

    #[test]
    fn tail_padding_rules_hold_at_every_length() {
        const MIXED: &[u8] = b"0aA:-~/zQ%e\x00\xff\x80Tt";
        for length in 0..=40 {
            let mixed: Vec<u8> = (0..length)
                .map(|index| MIXED[(index * 7 + length) % MIXED.len()])
                .collect();
            let bytes: Vec<u8> = (0..length)
                .map(|index| (length * 31 + index * 13) as u8)
                .collect();
            for literal in [mixed, bytes] {
                let bits = code_bits(&literal);
                let padding = (8 - bits.len() % 8) % 8;
                let mut inputs = Vec::new();
                // 0-32 one bits after the last code, then ones to the byte: padding of 0-7 bits
                // is valid, 8 or more is not, and 30 completes EOS.
                for extra in 0..=32 {
                    let mut padded = bits.clone();
                    padded.extend(std::iter::repeat_n(true, extra));
                    inputs.push(pack(&padded, true));
                }
                let mut overlong = pack(&bits, true);
                overlong.push(0xff);
                inputs.push(overlong);
                inputs.push(pack(&bits, false));
                for cleared in 0..padding {
                    let mut input = pack(&bits, true);
                    *input.last_mut().expect("padding implies a last byte") &= !(1 << cleared);
                    inputs.push(input);
                }
                for input in inputs {
                    assert_eq!(decode_with_slice(&input), reference(&input), "{input:02x?}");
                }
            }
        }
    }

    #[test]
    fn huffman_code_is_canonical() {
        let mut order: Vec<usize> = (0..CODES.len()).collect();
        order.sort_by_key(|&symbol| (CODE_LENGTHS[symbol], CODES[symbol]));
        assert_eq!(CODES[order[0]], 0, "the first canonical code is zero");
        for pair in order.windows(2) {
            let (previous, next) = (pair[0], pair[1]);
            let gap = CODE_LENGTHS[next] - CODE_LENGTHS[previous];
            assert_eq!(
                CODES[next],
                (CODES[previous] + 1) << gap,
                "symbol {next} follows symbol {previous}"
            );
        }
    }

    #[test]
    fn decode_to_slice_fits_in_max_decoded_len() {
        for count in 0..=400 {
            // `0` has a 5-bit code, the shortest, so it decodes the most symbols per byte.
            let literal = vec![b'0'; count];
            let encoded = encode(&literal);
            let mut out = vec![0; max_decoded_len(encoded.len())];
            let decoded = decode_to_slice(&encoded, &mut out).map(|length| &out[..length]);
            assert_eq!(decoded, Ok(&literal[..]), "{count} symbols");
        }
    }

    #[test]
    fn decode_pairs_match_a_bit_walk() {
        for key in 0..1_u32 << KEY_BITS {
            let expected = match prefix_symbol(key, KEY_BITS) {
                Some((first, first_length)) if first != u32::from(EOS_SYMBOL) => {
                    let rest_bits = KEY_BITS - first_length;
                    let rest = key & ((1 << rest_bits) - 1);
                    match prefix_symbol(rest, rest_bits) {
                        Some((second, second_length)) if second != u32::from(EOS_SYMBOL) => {
                            first
                                | (second << 8)
                                | (first_length << 16)
                                | ((first_length + second_length) << 21)
                                | (2 << 26)
                        }
                        _ => first | (first_length << 16) | (first_length << 21) | (1 << 26),
                    }
                }
                _ => 0,
            };
            assert_eq!(DECODE_PAIRS[key as usize], expected, "key {key:012b}");
        }
    }

    /// One literal of each encoder corpus class at `length`: four header-value alphabets, a
    /// sweep over every byte value (every code length up to 30 bits) and fixed-seed random bytes.
    fn corpus_classes(length: usize, state: &mut u64) -> [Vec<u8>; 6] {
        const ALPHABETS: [&[u8]; 4] = [
            b"0123456789abcdef",
            b"0123456789",
            b"0123456789abcdefghijklmnopqrstuvwxyz",
            b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
        ];
        let mut next = || {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        };
        let mut from = |alphabet: &[u8]| -> Vec<u8> {
            (0..length)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect()
        };
        let [hex, decimal, lower, mixed] = ALPHABETS.map(&mut from);
        let sweep = (0..length).map(|i| (length * 7 + i) as u8).collect();
        let random = (0..length).map(|_| next() as u8).collect();
        [hex, decimal, lower, mixed, sweep, random]
    }

    #[test]
    fn sized_append_matches_the_bytewise_encoder() {
        let mut state = 0x5EED_E4C0_DE32_u64;
        for length in 0..=300 {
            for literal in corpus_classes(length, &mut state) {
                let size = encoded_size(&literal);
                for prefix_len in 0..=7_u8 {
                    let prefix: Vec<u8> = (0..prefix_len).map(|i| 0xa0 | i).collect();
                    let mut expected = prefix.clone();
                    append_encoded(&literal, &mut expected);
                    let mut output = prefix.clone();
                    append_encoded_sized(&literal, size, &mut output);
                    assert_eq!(
                        output, expected,
                        "literal {literal:02x?}, prefix {prefix_len}"
                    );
                    assert_eq!(output.len(), prefix.len() + size, "literal {literal:02x?}");
                }
            }
        }
    }

    #[test]
    fn encoded_size_needs_no_tree() {
        let mut state = 0x5EED_512E_u64;
        for length in 0..=300 {
            for literal in corpus_classes(length, &mut state) {
                let expected = huff_tree().get_encode_size(&literal);
                assert_eq!(encoded_size(&literal), expected, "literal {literal:02x?}");
            }
        }
    }
}
