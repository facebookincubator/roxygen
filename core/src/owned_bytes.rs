/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum OwnedBytes {
    Inline {
        len: u8,
        bytes: [u8; OwnedBytes::INLINE],
    },
    Heap(Box<[u8]>),
}

const _: () = assert!(size_of::<OwnedBytes>() == 24);

impl OwnedBytes {
    /// The 24-byte size of `Heap`, less the tag and the length byte.
    const INLINE: usize = 22;

    pub(crate) fn new(value: &[u8]) -> Self {
        if value.len() <= Self::INLINE {
            let mut bytes = [0; Self::INLINE];
            bytes[..value.len()].copy_from_slice(value);
            Self::Inline {
                len: value.len() as u8,
                bytes,
            }
        } else {
            Self::Heap(value.to_vec().into_boxed_slice())
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Inline { len, bytes } => &bytes[..usize::from(*len)],
            Self::Heap(value) => value,
        }
    }
}

impl Default for OwnedBytes {
    fn default() -> Self {
        Self::new(b"")
    }
}

impl fmt::Debug for OwnedBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.bytes(), formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::OwnedBytes;

    fn value(len: usize) -> Vec<u8> {
        (0..len).map(|index| b'a' + (index % 26) as u8).collect()
    }

    #[test]
    fn values_round_trip_inline_up_to_22_bytes() {
        for (len, inline) in [(0, true), (7, true), (8, true), (22, true), (23, false)] {
            let value = value(len);
            let owned = OwnedBytes::new(&value);
            assert_eq!(owned.bytes(), value.as_slice(), "{len} bytes round-trip");
            assert_eq!(
                matches!(owned, OwnedBytes::Inline { .. }),
                inline,
                "{len} bytes should be inline: {inline}",
            );
            assert_eq!(format!("{owned:?}"), format!("{:?}", value.as_slice()));
            assert_eq!(owned.clone(), owned);
        }
    }

    #[test]
    fn equality_compares_bytes() {
        assert_eq!(OwnedBytes::default(), OwnedBytes::new(b""));
        assert_eq!(OwnedBytes::default().bytes(), b"");
        assert_ne!(OwnedBytes::new(b"/a"), OwnedBytes::new(b"/a\0"));
        assert_ne!(OwnedBytes::new(&value(22)), OwnedBytes::new(&value(23)));
        assert_ne!(OwnedBytes::new(&value(22)), OwnedBytes::new(b"/"));
    }
}
