/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::BufMut;

use crate::codec::DecodeError;
use crate::codec::EncodeError;
use crate::codec::FrameType;
use crate::codec::varint;
use crate::codec::varint::VARINT_MAX;
use crate::codec::varint::decode_varint;
use crate::codec::varint::put_varint;
use crate::codec::varint::varint_len;

/// An HTTP/3 setting identifier. Unknown peer-provided values stay representable.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct SettingId(u64);

impl SettingId {
    /// The QPACK decoder's dynamic table capacity limit (RFC 9204). Defaults to 0.
    pub const QPACK_MAX_TABLE_CAPACITY: Self = Self(0x01);
    /// The largest field section the sender accepts. Defaults to unlimited.
    pub const MAX_FIELD_SECTION_SIZE: Self = Self(0x06);
    /// How many streams the QPACK decoder lets block (RFC 9204). Defaults to 0.
    pub const QPACK_BLOCKED_STREAMS: Self = Self(0x07);

    /// Returns whether this is one of the HTTP/2 settings that HTTP/3 reserves: ENABLE_PUSH,
    /// MAX_CONCURRENT_STREAMS, INITIAL_WINDOW_SIZE or MAX_FRAME_SIZE.
    const fn is_http2_reserved(self) -> bool {
        matches!(self.0, 0x02..=0x05)
    }

    /// Returns the identifier `value`, or `None` if it does not fit a QUIC variable-length integer.
    pub const fn new(value: u64) -> Option<Self> {
        if value <= VARINT_MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns a reserved identifier chosen by `random`, for settings that exercise a peer's
    /// handling of unknown settings. Small values of `random` give short encodings.
    pub const fn grease(random: u64) -> Self {
        Self(varint::grease(random))
    }

    /// Returns whether this is a reserved identifier of the form `0x1f * N + 0x21`.
    pub const fn is_grease(self) -> bool {
        varint::is_grease(self.0)
    }
}

/// The parameters of a SETTINGS frame, each identifier at most once, in identifier order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Settings {
    entries: Vec<(SettingId, u64)>,
}

impl Settings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the value of `id`, if present.
    pub fn get(&self, id: SettingId) -> Option<u64> {
        self.entries
            .binary_search_by_key(&id, |&(id, _)| id)
            .ok()
            .map(|index| self.entries[index].1)
    }

    /// Returns the value of [`SettingId::MAX_FIELD_SECTION_SIZE`], or `None` for the default,
    /// unlimited.
    pub fn max_field_section_size(&self) -> Option<u64> {
        self.get(SettingId::MAX_FIELD_SECTION_SIZE)
    }

    /// Returns the value of [`SettingId::QPACK_MAX_TABLE_CAPACITY`], or its default.
    pub fn qpack_max_table_capacity(&self) -> u64 {
        self.get(SettingId::QPACK_MAX_TABLE_CAPACITY).unwrap_or(0)
    }

    /// Returns the value of [`SettingId::QPACK_BLOCKED_STREAMS`], or its default.
    pub fn qpack_blocked_streams(&self) -> u64 {
        self.get(SettingId::QPACK_BLOCKED_STREAMS).unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns an iterator over every parameter, in identifier order.
    pub fn iter(&self) -> impl Iterator<Item = (SettingId, u64)> {
        self.entries.iter().copied()
    }

    /// Sets `id` to `value`, replacing any earlier value. Fails with
    /// [`EncodeError::ReservedSetting`] for an identifier that HTTP/3 reserves, 0x00 or an HTTP/2
    /// setting, and with [`EncodeError::ValueTooLarge`] for a value above [`VARINT_MAX`].
    pub fn set(&mut self, id: SettingId, value: u64) -> Result<(), EncodeError> {
        if id.0 == 0x00 || id.is_http2_reserved() {
            return Err(EncodeError::ReservedSetting(id));
        }
        if value > VARINT_MAX {
            return Err(EncodeError::ValueTooLarge);
        }
        match self.entries.binary_search_by_key(&id, |&(id, _)| id) {
            Ok(index) => self.entries[index].1 = value,
            Err(index) => self.entries.insert(index, (id, value)),
        }
        Ok(())
    }

    /// Returns the length of the SETTINGS payload.
    pub(crate) fn payload_len(&self) -> usize {
        self.entries
            .iter()
            .map(|&(id, value)| varint_len(id.0) + varint_len(value))
            .sum()
    }

    pub(crate) fn put_payload<B: BufMut>(&self, buf: &mut B) {
        for &(id, value) in &self.entries {
            put_varint(buf, id.0);
            put_varint(buf, value);
        }
    }

    /// Parses a SETTINGS payload. Rejects a payload that ends inside a parameter, a repeated
    /// identifier and the HTTP/2 settings; keeps unknown and reserved identifiers.
    pub fn decode(mut payload: &[u8]) -> Result<Self, DecodeError> {
        let malformed = DecodeError::MalformedFrame(FrameType::SETTINGS);
        let mut settings = Self::new();
        while !payload.is_empty() {
            let (id, id_len) = decode_varint(payload).ok_or(malformed)?;
            let (value, value_len) = decode_varint(&payload[id_len..]).ok_or(malformed)?;
            let id = SettingId(id);
            if id.is_http2_reserved() {
                return Err(DecodeError::ReservedSetting(id));
            }
            settings.entries.push((id, value));
            payload = &payload[id_len + value_len..];
        }
        settings.entries.sort_unstable_by_key(|&(id, _)| id);
        if let Some(pair) = settings
            .entries
            .windows(2)
            .find(|pair| pair[0].0 == pair[1].0)
        {
            return Err(DecodeError::DuplicateSetting(pair[0].0));
        }
        Ok(settings)
    }
}
