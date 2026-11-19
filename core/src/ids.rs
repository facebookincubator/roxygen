/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// A stream identifier. Its meaning depends on the codec.
///
/// Zero means an unknown or unspecified stream. The absence of a stream is
/// represented separately by [`NO_STREAM`].
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamId(u64);

impl StreamId {
    pub const UNSPECIFIED: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn is_unspecified(self) -> bool {
        self.0 == 0
    }
}

impl From<u64> for StreamId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<StreamId> for u64 {
    fn from(stream_id: StreamId) -> Self {
        stream_id.get()
    }
}

/// The absence of a stream.
pub const NO_STREAM: Option<StreamId> = None;

/// The largest stream identifier.
pub const MAX_STREAM_ID: StreamId = StreamId::MAX;
