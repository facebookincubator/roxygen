/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The static Huffman code of RFC 7541, which QPACK uses unmodified for string literals. The
//! coder is `roxygen_huffman`, which Roxygen's HTTP/2 codec also uses.

use bytes::BufMut;
use roxygen_huffman::HuffmanError;

use crate::WireError;

impl From<HuffmanError> for WireError {
    fn from(error: HuffmanError) -> Self {
        match error {
            HuffmanError::Eos => Self::HuffmanEos,
            HuffmanError::Padding => Self::HuffmanPadding,
        }
    }
}

/// Returns the length in bytes of the Huffman coding of `src`.
pub fn encoded_len(src: &[u8]) -> usize {
    roxygen_huffman::encoded_size(src)
}

/// Appends the Huffman coding of `src`, padded to a byte boundary with the high bits of EOS.
pub fn encode(src: &[u8], dst: &mut impl BufMut) {
    roxygen_huffman::encode_with(src, |bytes| dst.put_slice(bytes));
}

/// Decodes the Huffman-coded `src`, appending the bytes to `dst`.
pub fn decode(src: &[u8], dst: &mut Vec<u8>) -> Result<(), WireError> {
    let start = dst.len();
    dst.resize(start + roxygen_huffman::max_decoded_len(src.len()), 0);
    match roxygen_huffman::decode_to_slice(src, &mut dst[start..]) {
        Ok(len) => {
            dst.truncate(start + len);
            Ok(())
        }
        Err(error) => {
            dst.truncate(start);
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::count_allocations;
    use crate::test_util::hex;

    /// The Huffman-coded strings of RFC 7541 C.4 and C.6.
    const RFC7541_EXAMPLES: &[(&str, &str)] = &[
        ("www.example.com", "f1e3 c2e5 f23a 6ba0 ab90 f4ff"),
        ("no-cache", "a8eb 1064 9cbf"),
        ("custom-key", "25a8 49e9 5ba9 7d7f"),
        ("custom-value", "25a8 49e9 5bb8 e8b4 bf"),
        ("302", "6402"),
        ("private", "aec3 771a 4b"),
        (
            "Mon, 21 Oct 2013 20:13:21 GMT",
            "d07a be94 1054 d444 a820 0595 040b 8166 e082 a62d 1bff",
        ),
        (
            "https://www.example.com",
            "9d29 ad17 1863 c78f 0b97 c8e9 ae82 ae43 d3",
        ),
        ("307", "640e ff"),
        (
            "Mon, 21 Oct 2013 20:13:22 GMT",
            "d07a be94 1054 d444 a820 0595 040b 8166 e084 a62d 1bff",
        ),
        ("gzip", "9bd9 ab"),
        (
            "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
            "94e7 821d d7f2 e6c7 b335 dfdf cd5b 3960 d5af 2708 7f36 72c1 ab27 0fb5 291f 9587 \
             3160 65c0 03ed 4ee5 b106 3d50 07",
        ),
    ];

    #[test]
    fn encodes_rfc7541_examples() {
        for &(plain, coded) in RFC7541_EXAMPLES {
            let mut out = Vec::new();
            encode(plain.as_bytes(), &mut out);
            assert_eq!(out, hex(coded), "{plain}");
        }
    }

    #[test]
    fn encoded_len_matches_encode() {
        assert_eq!(encoded_len(b""), 0);
        for &(plain, coded) in RFC7541_EXAMPLES {
            assert_eq!(encoded_len(plain.as_bytes()), hex(coded).len(), "{plain}");
        }
        for byte in 0..=255u8 {
            let mut out = Vec::new();
            encode(&[byte, byte, byte], &mut out);
            assert_eq!(encoded_len(&[byte, byte, byte]), out.len(), "{byte}");
        }
    }

    #[test]
    fn decodes_rfc7541_examples() {
        for &(plain, coded) in RFC7541_EXAMPLES {
            let mut out = b"prefix".to_vec();
            assert_eq!(decode(&hex(coded), &mut out), Ok(()), "{plain}");
            assert_eq!(out, [b"prefix", plain.as_bytes()].concat(), "{plain}");
        }
    }

    fn round_trip(plain: &[u8]) {
        let mut coded = Vec::new();
        encode(plain, &mut coded);
        let mut decoded = Vec::new();
        assert_eq!(decode(&coded, &mut decoded), Ok(()), "{plain:?}");
        assert_eq!(decoded, plain);
    }

    #[test]
    fn rejects_eos() {
        for coded in [
            &[0xff, 0xff, 0xff, 0xfc][..],
            &[0xff, 0xff, 0xff, 0xff],
            // "a" (00011), then EOS, then five bits of padding.
            &[0x1f, 0xff, 0xff, 0xff, 0xff],
        ] {
            assert_eq!(
                decode(coded, &mut Vec::new()),
                Err(WireError::HuffmanEos),
                "{coded:x?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_padding() {
        for coded in [
            // Eight bits of padding.
            &[0xff][..],
            // "0" (00000), then eleven bits of padding.
            &[0x07, 0xff],
            // "0", then 000.
            &[0x00],
            // "a" (00011), then 110.
            &[0x1e],
        ] {
            assert_eq!(
                decode(coded, &mut Vec::new()),
                Err(WireError::HuffmanPadding),
                "{coded:x?}"
            );
        }
        let mut out = Vec::new();
        assert_eq!(decode(&[0x07], &mut out), Ok(()));
        assert_eq!(out, b"0");
    }

    #[test]
    fn round_trips_every_byte_value() {
        round_trip(b"");
        let all: Vec<u8> = (0..=255).collect();
        round_trip(&all);
        for byte in 0..=255u8 {
            for len in 1..=8 {
                round_trip(&vec![byte; len]);
            }
        }
        // A seeded linear congruential generator gives mixed strings of every length up to 300.
        let mut seed = 0x2545_f491_u32;
        for len in 0..300 {
            let plain: Vec<u8> = (0..len)
                .map(|_| {
                    seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (seed >> 24) as u8
                })
                .collect();
            round_trip(&plain);
        }
    }

    /// The reference decoder, which walks the code tree four bits at a time.
    mod nibble {
        use roxygen_huffman::CODE_LENGTHS;
        use roxygen_huffman::CODES;

        use crate::WireError;

        const LEAF: u16 = 0x8000;
        const EOS: u16 = 256;
        const NODES: usize = 256;
        const EMIT: u8 = 1;
        const FAIL: u8 = 2;

        #[derive(Clone, Copy)]
        struct Step {
            next: u8,
            flags: u8,
            symbol: u8,
        }

        const TREE: [[u16; 2]; NODES] = build_tree();
        static PADDING_STATES: [bool; NODES] = build_padding_states();
        static STEPS: [[Step; 16]; NODES] = build_steps();

        pub(super) fn decode(src: &[u8], dst: &mut Vec<u8>) -> Result<(), WireError> {
            dst.reserve(src.len().saturating_mul(8) / 5);
            let mut state = 0;
            for &byte in src {
                for nibble in [byte >> 4, byte & 0x0f] {
                    let step = STEPS[state][usize::from(nibble)];
                    if step.flags & FAIL != 0 {
                        return Err(WireError::HuffmanEos);
                    }
                    if step.flags & EMIT != 0 {
                        dst.push(step.symbol);
                    }
                    state = usize::from(step.next);
                }
            }
            if !PADDING_STATES[state] {
                return Err(WireError::HuffmanPadding);
            }
            Ok(())
        }

        const fn build_tree() -> [[u16; 2]; NODES] {
            let mut children = [[0u16; 2]; NODES];
            let mut allocated = 1;
            let mut symbol = 0;
            while symbol < CODES.len() {
                let (code, len) = (CODES[symbol], CODE_LENGTHS[symbol]);
                let mut node = 0;
                let mut bit = len;
                while bit > 1 {
                    bit -= 1;
                    let side = ((code >> bit) & 1) as usize;
                    if children[node][side] == 0 {
                        children[node][side] = allocated as u16;
                        allocated += 1;
                    }
                    assert!(
                        children[node][side] & LEAF == 0,
                        "the code is not prefix-free"
                    );
                    node = children[node][side] as usize;
                }
                children[node][(code & 1) as usize] = LEAF | symbol as u16;
                symbol += 1;
            }
            assert!(allocated == NODES, "the code is not complete");
            children
        }

        const fn build_padding_states() -> [bool; NODES] {
            let mut states = [false; NODES];
            let mut node = 0;
            states[node] = true;
            let mut depth = 0;
            while depth < 7 {
                assert!(TREE[node][1] & LEAF == 0, "a code of seven or fewer 1 bits");
                node = TREE[node][1] as usize;
                states[node] = true;
                depth += 1;
            }
            states
        }

        const fn build_steps() -> [[Step; 16]; NODES] {
            let mut steps = [[Step {
                next: 0,
                flags: 0,
                symbol: 0,
            }; 16]; NODES];
            let mut state = 0;
            while state < NODES {
                let mut nibble = 0;
                while nibble < 16 {
                    let mut step = Step {
                        next: 0,
                        flags: 0,
                        symbol: 0,
                    };
                    let mut node = state;
                    let mut bit = 4;
                    while bit > 0 {
                        bit -= 1;
                        let child = TREE[node][(nibble >> bit) & 1];
                        if child & LEAF == 0 {
                            node = child as usize;
                        } else if child == LEAF | EOS {
                            step.flags |= FAIL;
                            node = 0;
                        } else {
                            step.flags |= EMIT;
                            step.symbol = (child & !LEAF) as u8;
                            node = 0;
                        }
                    }
                    step.next = node as u8;
                    steps[state][nibble] = step;
                    nibble += 1;
                }
                state += 1;
            }
            steps
        }
    }

    /// The reference encoder, which writes one byte at a time.
    mod bytewise {
        use bytes::BufMut;
        use roxygen_huffman::CODE_LENGTHS;
        use roxygen_huffman::CODES;

        pub(super) fn encode(src: &[u8], dst: &mut impl BufMut) {
            let mut acc: u64 = 0;
            let mut pending: u32 = 0;
            for &byte in src {
                let (code, len) = (CODES[usize::from(byte)], CODE_LENGTHS[usize::from(byte)]);
                acc = (acc << len) | u64::from(code);
                pending += u32::from(len);
                while pending >= 8 {
                    pending -= 8;
                    dst.put_u8((acc >> pending) as u8);
                }
            }
            if pending > 0 {
                dst.put_u8(((acc << (8 - pending)) as u8) | (0xff >> pending));
            }
        }
    }

    #[test]
    fn encodes_as_the_bytewise_encoder() {
        let mut state = 0x7541_0b17_u64;
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
                bytewise::encode(&literal, &mut expected);
                // After a prefix, into each buffer type the encoder writes to.
                let prefix = [0xaa; 3];
                let mut vec = prefix.to_vec();
                encode(&literal, &mut vec);
                assert_eq!(vec[3..], expected, "{literal:02x?}");
                let mut bytes = bytes::BytesMut::from(&prefix[..]);
                encode(&literal, &mut bytes);
                assert_eq!(bytes[3..], expected, "{literal:02x?}");
            }
        }
    }

    #[test]
    fn a_long_literal_is_encoded_without_allocating() {
        // 300 bytes of "a" (00011) take 188 bytes.
        let literal = [b'a'; 300];
        let mut vec = Vec::with_capacity(256);
        let mut bytes = bytes::BytesMut::with_capacity(256);
        let allocations = count_allocations(|| {
            encode(&literal, &mut vec);
            encode(&literal, &mut bytes);
        });
        let mut expected = Vec::new();
        bytewise::encode(&literal, &mut expected);
        assert_eq!(expected.len(), 188);
        assert_eq!((&vec[..], &bytes[..]), (&expected[..], &expected[..]));
        match allocations {
            Some(allocations) => assert_eq!(allocations, 0),
            None => eprintln!("allocation count needs a statically linked std"),
        }
    }

    /// The strings of the RFC 9204 Appendix B examples.
    const RFC9204_STRINGS: &[&str] = &[
        "/index.html",
        "www.example.com",
        "/sample/path",
        "custom-key",
        "custom-value",
        "custom-value2",
    ];

    /// The bytes of the RFC 9204 Appendix B examples.
    const RFC9204_BYTES: &[&str] = &[
        "0000 510b 2f69 6e64 6578 2e68 746d 6c",
        "3fbd01 c00f 7777 772e 6578 616d 706c 652e 636f 6d c10c 2f73 616d 706c 652f 7061 7468",
        "0381 1011",
        "84",
        "4a63 7573 746f 6d2d 6b65 790c 6375 7374 6f6d 2d76 616c 7565",
        "01",
        "02",
        "0500 80c1 81",
        "48",
        "810d 6375 7374 6f6d 2d76 616c 7565 32",
    ];

    /// Returns the inputs for the differential test: the RFC examples, then seeded random strings,
    /// valid codings of random text, and those codings with bad padding, an EOS run, a flipped bit
    /// or a missing last byte.
    fn differential_inputs() -> Vec<Vec<u8>> {
        let mut inputs = Vec::new();
        for &(plain, coded) in RFC7541_EXAMPLES {
            inputs.push(hex(coded));
            inputs.push(plain.as_bytes().to_vec());
        }
        for &plain in RFC9204_STRINGS {
            let mut coded = Vec::new();
            encode(plain.as_bytes(), &mut coded);
            inputs.push(coded);
            inputs.push(plain.as_bytes().to_vec());
        }
        inputs.extend(RFC9204_BYTES.iter().map(|bytes| hex(bytes)));
        let mut state = 0x9204_7541_d1ff_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..4000 {
            let len = (next() % 48) as usize;
            let random: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            inputs.push(random.clone());
            // Mostly header-like text, sometimes any byte.
            let plain: Vec<u8> = if round % 4 == 0 {
                random
            } else {
                (0..len).map(|_| b' ' + (next() % 95) as u8).collect()
            };
            let mut coded = Vec::new();
            encode(&plain, &mut coded);
            let mut padded = coded.clone();
            padded.push(0xff);
            inputs.push(padded);
            if let Some(last) = coded.last() {
                let mut cleared = coded.clone();
                *cleared.last_mut().unwrap() = last & !(1 << (next() % 8));
                inputs.push(cleared);
                let mut short = coded.clone();
                short.pop();
                inputs.push(short);
                let mut flipped = coded.clone();
                let at = (next() as usize) % flipped.len();
                flipped[at] ^= 1 << (next() % 8);
                inputs.push(flipped);
            }
            let mut eos = coded.clone();
            let at = (next() as usize) % (eos.len() + 1);
            eos.splice(at..at, [0xff; 4]);
            inputs.push(eos);
            inputs.push(coded);
        }
        inputs
    }

    #[test]
    fn agrees_with_the_nibble_decoder() {
        let run = |decode: fn(&[u8], &mut Vec<u8>) -> Result<(), WireError>, input: &[u8]| {
            let mut out = Vec::new();
            decode(input, &mut out).map(|()| out)
        };
        let (mut valid, mut eos, mut padding) = (0, 0, 0);
        for input in differential_inputs() {
            let expected = run(nibble::decode, &input);
            match expected {
                Ok(_) => valid += 1,
                Err(WireError::HuffmanEos) => eos += 1,
                Err(_) => padding += 1,
            }
            assert_eq!(run(decode, &input), expected, "{input:02x?}");
        }
        // Each outcome is well represented.
        assert!(
            valid > 4000 && eos > 2000 && padding > 4000,
            "{valid} {eos} {padding}"
        );
    }
}
