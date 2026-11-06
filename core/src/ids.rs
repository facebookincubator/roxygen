/// A stream identifier whose interpretation belongs to its codec.
///
/// Zero is the initialized representation of an unknown or unspecified stream.
/// Absence of any stream is represented separately by [`NO_STREAM`].
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

/// The absence of a stream, corresponding to `HTTPCodec::NoStream`.
pub const NO_STREAM: Option<StreamId> = None;

/// The greatest value representable by `HTTPCodec::StreamID`.
pub const MAX_STREAM_ID: StreamId = StreamId::MAX;
