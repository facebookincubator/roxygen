/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/2 settings storage and wire identifiers.

use roxygen_codec::CodecSetting;

use crate::constants::MAX_FRAME_PAYLOAD_LENGTH_MIN;

/// An HTTP/2 setting identifier defined by RFC 7540 or RFC 8441.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u16)]
pub enum SettingsId {
    HeaderTableSize = 1,
    EnablePush = 2,
    MaxConcurrentStreams = 3,
    InitialWindowSize = 4,
    MaxFrameSize = 5,
    MaxHeaderListSize = 6,
    EnableConnectProtocol = 8,
}

impl SettingsId {
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    pub const fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::HeaderTableSize),
            2 => Some(Self::EnablePush),
            3 => Some(Self::MaxConcurrentStreams),
            4 => Some(Self::InitialWindowSize),
            5 => Some(Self::MaxFrameSize),
            6 => Some(Self::MaxHeaderListSize),
            8 => Some(Self::EnableConnectProtocol),
            _ => None,
        }
    }
}

/// One enabled setting and its protocol-width value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpSetting {
    pub id: SettingsId,
    pub value: u64,
}

impl HttpSetting {
    pub const fn new(id: SettingsId, value: u64) -> Self {
        Self { id, value }
    }
}

/// Compact insertion-ordered settings storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpSettings {
    settings: Vec<HttpSetting>,
}

impl HttpSettings {
    /// Constructs the default HTTP/2 settings.
    pub fn new() -> Self {
        Self {
            settings: vec![
                HttpSetting::new(SettingsId::HeaderTableSize, 4096),
                HttpSetting::new(SettingsId::EnablePush, 1),
                HttpSetting::new(
                    SettingsId::MaxFrameSize,
                    u64::from(MAX_FRAME_PAYLOAD_LENGTH_MIN),
                ),
            ],
        }
    }

    /// Constructs a de-duplicated list, retaining each identifier's first
    /// position and most recently supplied value.
    pub fn from_pairs(settings: impl IntoIterator<Item = (SettingsId, u64)>) -> Self {
        let mut result = Self {
            settings: Vec::new(),
        };
        for (id, value) in settings {
            result.set(id, value);
        }
        result
    }

    pub fn set(&mut self, id: SettingsId, value: u64) {
        if let Some(setting) = self.settings.iter_mut().find(|setting| setting.id == id) {
            setting.value = value;
        } else {
            self.settings.push(HttpSetting::new(id, value));
        }
    }

    /// Inserts only when `id` is absent and reports whether insertion occurred.
    pub fn set_if_not_present(&mut self, id: SettingsId, value: u64) -> bool {
        if self.get(id).is_some() {
            false
        } else {
            self.settings.push(HttpSetting::new(id, value));
            true
        }
    }

    pub fn unset(&mut self, id: SettingsId) {
        if let Some(index) = self.settings.iter().position(|setting| setting.id == id) {
            self.settings.swap_remove(index);
        }
    }

    pub fn get(&self, id: SettingsId) -> Option<&HttpSetting> {
        self.settings.iter().find(|setting| setting.id == id)
    }

    pub fn value_or(&self, id: SettingsId, default: u64) -> u64 {
        self.get(id).map_or(default, |setting| setting.value)
    }

    pub fn len(&self) -> usize {
        self.settings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.settings.is_empty()
    }

    pub fn all(&self) -> &[HttpSetting] {
        &self.settings
    }

    pub fn clear(&mut self) {
        self.settings.clear();
    }

    pub fn codec_settings(&self) -> Vec<CodecSetting> {
        self.settings
            .iter()
            .map(|setting| CodecSetting {
                identifier: setting.id.as_u16(),
                value: setting.value,
            })
            .collect()
    }
}

impl Default for HttpSettings {
    fn default() -> Self {
        Self::new()
    }
}
