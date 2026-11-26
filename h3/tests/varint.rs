/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::codec::varint::decode_varint;
use crate::codec::varint::put_varint;
use crate::codec::varint::varint_len;

#[test]
fn varints_match_the_rfc_9000_examples() {
    let examples: [(&[u8], u64); 5] = [
        (
            &[0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c],
            151_288_809_941_952_652,
        ),
        (&[0x9d, 0x7f, 0x3e, 0x7d], 494_878_333),
        (&[0x7b, 0xbd], 15_293),
        (&[0x25], 37),
        (&[0x40, 0x25], 37),
    ];
    for (wire, value) in examples {
        assert_eq!(decode_varint(wire), Some((value, wire.len())), "{wire:x?}");
        for short in 0..wire.len() {
            assert_eq!(
                decode_varint(&wire[..short]),
                None,
                "{wire:x?} cut at {short}"
            );
        }
    }
    for (wire, value) in &examples[..4] {
        let mut out = Vec::new();
        put_varint(&mut out, *value);
        assert_eq!(out, *wire);
        assert_eq!(varint_len(*value), wire.len());
    }
    for (value, len) in [
        (63, 1),
        (64, 2),
        (16_383, 2),
        (16_384, 4),
        ((1 << 30) - 1, 4),
    ] {
        assert_eq!(varint_len(value), len, "{value}");
    }
    assert_eq!(varint_len(1 << 30), 8);
    assert_eq!(varint_len(crate::codec::VARINT_MAX), 8);
}
