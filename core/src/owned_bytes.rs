use std::fmt;

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum OwnedBytes {
    Inline { len: u8, bytes: [u8; 7] },
    Heap(Box<[u8]>),
}

impl OwnedBytes {
    pub(crate) fn new(value: &[u8]) -> Self {
        if value.len() <= 7 {
            let mut bytes = [0; 7];
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
