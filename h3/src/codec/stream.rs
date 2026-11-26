/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::Buf;
use bytes::BufMut;

use crate::codec::DecodeError;
use crate::codec::EncodeError;
use crate::codec::Role;
use crate::codec::varint;
use crate::codec::varint::VARINT_MAX;
use crate::codec::varint::VarIntReader;
use crate::codec::varint::put_varint;

/// A unidirectional stream type. Unknown peer-provided values stay representable.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct StreamType(u64);

impl StreamType {
    pub const CONTROL: Self = Self(0x00);
    pub const PUSH: Self = Self(0x01);
    /// The QPACK encoder stream (RFC 9204).
    pub const QPACK_ENCODER: Self = Self(0x02);
    /// The QPACK decoder stream (RFC 9204).
    pub const QPACK_DECODER: Self = Self(0x03);

    /// Returns the type `value`, or `None` if it does not fit a QUIC variable-length integer.
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

    /// Returns a type read from the wire, which always fits.
    const fn from_wire(value: u64) -> Self {
        Self(value)
    }

    /// Returns a reserved type chosen by `random`, for streams that exercise a peer's handling
    /// of unknown stream types. Small values of `random` give short encodings.
    pub const fn grease(random: u64) -> Self {
        Self(varint::grease(random))
    }

    /// Returns whether this is a reserved type of the form `0x1f * N + 0x21`.
    pub const fn is_grease(self) -> bool {
        varint::is_grease(self.0)
    }
}

/// The header that starts a unidirectional stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UniStreamHeader {
    Control,
    /// A push stream, which fulfils the promise of `push_id`.
    Push {
        push_id: u64,
    },
    QpackEncoder,
    QpackDecoder,
    /// A reserved or unknown type. The receiver discards the rest of the stream or stops reading
    /// it, and never treats it as a connection error.
    Unknown(StreamType),
}

impl UniStreamHeader {
    /// Appends the encoded header to `buf`. Fails with [`EncodeError::ValueTooLarge`] for a push
    /// ID above [`VARINT_MAX`], and with [`EncodeError::KnownStreamType`] for an
    /// [`Unknown`](Self::Unknown) header that holds a type this crate defines. On error `buf` is
    /// unchanged. Panics if `buf` is too small and cannot grow.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        match *self {
            Self::Push { push_id } if push_id > VARINT_MAX => {
                return Err(EncodeError::ValueTooLarge);
            }
            Self::Unknown(
                stream_type @ (StreamType::CONTROL
                | StreamType::PUSH
                | StreamType::QPACK_ENCODER
                | StreamType::QPACK_DECODER),
            ) => return Err(EncodeError::KnownStreamType(stream_type)),
            _ => {}
        }
        put_varint(buf, self.stream_type().0);
        if let Self::Push { push_id } = self {
            put_varint(buf, *push_id);
        }
        Ok(())
    }

    pub fn stream_type(&self) -> StreamType {
        match *self {
            Self::Control => StreamType::CONTROL,
            Self::Push { .. } => StreamType::PUSH,
            Self::QpackEncoder => StreamType::QPACK_ENCODER,
            Self::QpackDecoder => StreamType::QPACK_DECODER,
            Self::Unknown(stream_type) => stream_type,
        }
    }
}

/// An incremental parser for the header of a unidirectional stream that the peer opened.
#[derive(Debug)]
pub struct UniStreamHeaderDecoder {
    role: Role,
    state: HeaderState,
    varint: VarIntReader,
}

#[derive(Clone, Copy, Debug)]
enum HeaderState {
    Type,
    PushId,
    Done,
    Failed(DecodeError),
}

impl UniStreamHeaderDecoder {
    pub fn new(role: Role) -> Self {
        Self {
            role,
            state: HeaderState::Type,
            varint: VarIntReader::default(),
        }
    }

    /// Consumes the header's bytes from `input` and returns the header once complete, leaving
    /// the stream's later bytes in `input`. Returns `Ok(None)` if `input` ends first; later calls
    /// continue the header. Fails with [`DecodeError::UnexpectedStream`] when a server reads the
    /// type of a push stream, since only servers push. After the header or an error, it stops
    /// consuming input and returns `Ok(None)` or the same error.
    pub fn decode<B: Buf>(
        &mut self,
        input: &mut B,
    ) -> Result<Option<UniStreamHeader>, DecodeError> {
        loop {
            let header = match self.state {
                HeaderState::Done => return Ok(None),
                HeaderState::Failed(error) => return Err(error),
                HeaderState::Type => {
                    let Some(value) = self.varint.read(input) else {
                        return Ok(None);
                    };
                    match StreamType::from_wire(value) {
                        StreamType::CONTROL => UniStreamHeader::Control,
                        // Only servers push.
                        StreamType::PUSH if self.role == Role::Server => {
                            let error = DecodeError::UnexpectedStream(StreamType::PUSH);
                            self.state = HeaderState::Failed(error);
                            return Err(error);
                        }
                        StreamType::PUSH => {
                            self.state = HeaderState::PushId;
                            continue;
                        }
                        StreamType::QPACK_ENCODER => UniStreamHeader::QpackEncoder,
                        StreamType::QPACK_DECODER => UniStreamHeader::QpackDecoder,
                        stream_type => UniStreamHeader::Unknown(stream_type),
                    }
                }
                HeaderState::PushId => {
                    let Some(push_id) = self.varint.read(input) else {
                        return Ok(None);
                    };
                    UniStreamHeader::Push { push_id }
                }
            };
            self.state = HeaderState::Done;
            return Ok(Some(header));
        }
    }
}
