/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::OnceLock;

use roxygen_huffman::CODE_LENGTHS;
use roxygen_huffman::CODES;
#[cfg(test)]
use roxygen_huffman::DECODE_PAIRS;
#[cfg(test)]
use roxygen_huffman::EOS_SYMBOL;
pub(super) use roxygen_huffman::append_encoded;
pub(super) use roxygen_huffman::append_encoded_sized;
pub use roxygen_huffman::encoded_size;
pub(super) use roxygen_huffman::max_decoded_len;

use super::HpackError;

pub const HUFFMAN_TABLE_SIZE: usize = 256;
pub const EOS_HPACK: u32 = 0x3fff_ffff;

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

pub fn encode(literal: &[u8]) -> Vec<u8> {
    huff_tree().encode(literal)
}

pub fn decode(encoded: &[u8]) -> Result<Vec<u8>, HpackError> {
    huff_tree().decode(encoded)
}

/// Decodes `encoded` into the front of `out`, which must hold
/// [`max_decoded_len`]`(encoded.len())` bytes, and returns the decoded length.
pub(super) fn decode_to_slice(encoded: &[u8], out: &mut [u8]) -> Result<usize, HpackError> {
    roxygen_huffman::decode_to_slice(encoded, out).map_err(|_| HpackError::InvalidHuffmanCode)
}

pub fn code(byte: u8) -> (u32, u8) {
    huff_tree().get_code(byte)
}

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
