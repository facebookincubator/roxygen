/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::codec::DecodeError;
use crate::codec::EncodeError;
use crate::codec::FrameType;
use crate::codec::H3ErrorCode;
use crate::codec::SettingId;
use crate::codec::Settings;
use crate::codec::VARINT_MAX;

#[test]
fn settings_ending_inside_a_parameter_are_malformed() {
    for payload in [
        &b"\x06"[..],
        b"\x06\x40",
        b"\x40",
        b"\x01\x00\x07\x80\x00\x00",
    ] {
        let error = Settings::decode(payload).unwrap_err();
        assert_eq!(
            error,
            DecodeError::MalformedFrame(FrameType::SETTINGS),
            "{payload:x?}"
        );
        assert_eq!(error.code(), H3ErrorCode::FRAME_ERROR);
    }
}

#[test]
fn duplicate_setting_identifiers_are_rejected() {
    let cases: [(&[u8], u64); 4] = [
        (b"\x06\x01\x06\x01", 0x06),
        (b"\x06\x01\x07\x00\x06\x02", 0x06),
        (b"\x80\x00\x12\x34\x00\x07\x00\x80\x00\x12\x34\x05", 0x1234),
        (b"\x21\x00\x40\x21\x00", 0x21),
    ];
    for (payload, id) in cases {
        let error = Settings::decode(payload).unwrap_err();
        let id = SettingId::new(id).unwrap();
        assert_eq!(error, DecodeError::DuplicateSetting(id), "{payload:x?}");
        assert_eq!(error.code(), H3ErrorCode::SETTINGS_ERROR);
    }
}

#[test]
fn http2_setting_identifiers_are_rejected() {
    // ENABLE_PUSH, MAX_CONCURRENT_STREAMS, INITIAL_WINDOW_SIZE and MAX_FRAME_SIZE.
    for id in 0x02..=0x05 {
        for payload in [vec![id, 0x00], vec![0x06, 0x01, 0x40, id, 0x01]] {
            let error = Settings::decode(&payload).unwrap_err();
            let id = SettingId::new(u64::from(id)).unwrap();
            assert_eq!(error, DecodeError::ReservedSetting(id), "{payload:x?}");
            assert_eq!(error.code(), H3ErrorCode::SETTINGS_ERROR);
        }
    }
    // 0x00 is reserved too, but HTTP/2 never defined it and receiving it is not an error.
    let settings = Settings::decode(b"\x00\x07").unwrap();
    assert_eq!(settings.get(SettingId::new(0).unwrap()), Some(7));
}

#[test]
fn settings_report_defaults_values_and_unknown_identifiers() {
    let defaults = Settings::new();
    assert_eq!(defaults.max_field_section_size(), None);
    assert_eq!(defaults.qpack_max_table_capacity(), 0);
    assert_eq!(defaults.qpack_blocked_streams(), 0);
    assert!(defaults.is_empty());

    // Unknown and reserved identifiers are kept but do not change the defined values.
    let settings =
        Settings::decode(b"\x40\x21\x05\x07\x10\x06\x44\x00\x01\x80\x00\x10\x00\x33\x01").unwrap();
    assert_eq!(settings.max_field_section_size(), Some(0x400));
    assert_eq!(settings.qpack_max_table_capacity(), 4096);
    assert_eq!(settings.qpack_blocked_streams(), 16);
    let id = |value| SettingId::new(value).unwrap();
    assert_eq!(settings.get(id(0x33)), Some(1));
    assert_eq!(settings.get(SettingId::grease(0)), Some(5));
    assert_eq!(settings.get(id(0x08)), None);
    let all: Vec<_> = settings.iter().collect();
    assert_eq!(
        all,
        [
            (id(0x01), 4096),
            (id(0x06), 0x400),
            (id(0x07), 16),
            (id(0x21), 5),
            (id(0x33), 1)
        ]
    );
    assert_eq!(settings.len(), 5);
}

#[test]
fn set_refuses_reserved_identifiers_and_oversized_values() {
    let mut settings = Settings::new();
    settings
        .set(SettingId::MAX_FIELD_SECTION_SIZE, 100)
        .unwrap();
    let before = settings.clone();
    for id in [0x00, 0x02, 0x03, 0x04, 0x05] {
        let id = SettingId::new(id).unwrap();
        assert_eq!(settings.set(id, 1), Err(EncodeError::ReservedSetting(id)));
    }
    assert_eq!(
        settings.set(SettingId::MAX_FIELD_SECTION_SIZE, VARINT_MAX + 1),
        Err(EncodeError::ValueTooLarge)
    );
    assert_eq!(settings, before);

    settings
        .set(SettingId::MAX_FIELD_SECTION_SIZE, VARINT_MAX)
        .unwrap();
    assert_eq!(
        settings.iter().collect::<Vec<_>>(),
        [(SettingId::MAX_FIELD_SECTION_SIZE, VARINT_MAX)]
    );
}
