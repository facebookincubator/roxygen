/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::codec::H3ErrorCode;

#[test]
fn error_codes_match_the_rfc_9114_registry() {
    let table = [
        (H3ErrorCode::NO_ERROR, 0x0100),
        (H3ErrorCode::GENERAL_PROTOCOL_ERROR, 0x0101),
        (H3ErrorCode::INTERNAL_ERROR, 0x0102),
        (H3ErrorCode::STREAM_CREATION_ERROR, 0x0103),
        (H3ErrorCode::CLOSED_CRITICAL_STREAM, 0x0104),
        (H3ErrorCode::FRAME_UNEXPECTED, 0x0105),
        (H3ErrorCode::FRAME_ERROR, 0x0106),
        (H3ErrorCode::EXCESSIVE_LOAD, 0x0107),
        (H3ErrorCode::ID_ERROR, 0x0108),
        (H3ErrorCode::SETTINGS_ERROR, 0x0109),
        (H3ErrorCode::MISSING_SETTINGS, 0x010a),
        (H3ErrorCode::REQUEST_REJECTED, 0x010b),
        (H3ErrorCode::REQUEST_CANCELLED, 0x010c),
        (H3ErrorCode::REQUEST_INCOMPLETE, 0x010d),
        (H3ErrorCode::MESSAGE_ERROR, 0x010e),
        (H3ErrorCode::CONNECT_ERROR, 0x010f),
        (H3ErrorCode::VERSION_FALLBACK, 0x0110),
    ];
    for (code, value) in table {
        assert_eq!(code.as_u64(), value, "{code:?}");
        assert_eq!(H3ErrorCode::new(value), Some(code));
    }
}

#[test]
fn reserved_error_codes_follow_0x1f_n_plus_0x21() {
    // N runs from 0 to (2^62 - 1 - 0x21) / 0x1f, so there are that many plus one values.
    let count = (crate::codec::VARINT_MAX - 0x21) / 0x1f + 1;
    assert_eq!(H3ErrorCode::grease(0).as_u64(), 0x21);
    assert_eq!(H3ErrorCode::grease(1).as_u64(), 0x40);
    assert_eq!(
        H3ErrorCode::grease(count - 1).as_u64(),
        0x3fff_ffff_ffff_fffe
    );
    assert_eq!(H3ErrorCode::grease(count).as_u64(), 0x21);
    assert_eq!(
        H3ErrorCode::grease(u64::MAX),
        H3ErrorCode::grease(u64::MAX % count)
    );
    for random in [0, 1, 7, 1 << 40, u64::MAX] {
        assert!(H3ErrorCode::grease(random).is_grease());
    }
    for value in [0x20, 0x22, 0x3f, 0x41, 0x0100, 0x3fff_ffff_ffff_ffff] {
        assert!(!H3ErrorCode::new(value).unwrap().is_grease(), "{value:#x}");
    }
}
