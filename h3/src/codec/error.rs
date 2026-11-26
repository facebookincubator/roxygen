/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;

use crate::codec::FrameType;
use crate::codec::SettingId;
use crate::codec::StreamType;
use crate::codec::varint;
use crate::codec::varint::VARINT_MAX;

/// An HTTP/3 error code, as carried by QUIC's RESET_STREAM, STOP_SENDING and
/// CONNECTION_CLOSE frames.
///
/// Unknown and peer-provided values stay representable. Receivers treat an unknown code as
/// [`NO_ERROR`](Self::NO_ERROR).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct H3ErrorCode(u64);

impl H3ErrorCode {
    pub const NO_ERROR: Self = Self(0x0100);
    pub const GENERAL_PROTOCOL_ERROR: Self = Self(0x0101);
    pub const INTERNAL_ERROR: Self = Self(0x0102);
    pub const STREAM_CREATION_ERROR: Self = Self(0x0103);
    pub const CLOSED_CRITICAL_STREAM: Self = Self(0x0104);
    pub const FRAME_UNEXPECTED: Self = Self(0x0105);
    pub const FRAME_ERROR: Self = Self(0x0106);
    pub const EXCESSIVE_LOAD: Self = Self(0x0107);
    pub const ID_ERROR: Self = Self(0x0108);
    pub const SETTINGS_ERROR: Self = Self(0x0109);
    pub const MISSING_SETTINGS: Self = Self(0x010a);
    pub const REQUEST_REJECTED: Self = Self(0x010b);
    pub const REQUEST_CANCELLED: Self = Self(0x010c);
    pub const REQUEST_INCOMPLETE: Self = Self(0x010d);
    pub const MESSAGE_ERROR: Self = Self(0x010e);
    pub const CONNECT_ERROR: Self = Self(0x010f);
    pub const VERSION_FALLBACK: Self = Self(0x0110);

    /// Returns the code `value`, or `None` if it does not fit a QUIC variable-length integer.
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

    /// Returns a reserved code chosen by `random`, which a sender can use where it would send
    /// [`NO_ERROR`](Self::NO_ERROR). Small values of `random` give short encodings.
    pub const fn grease(random: u64) -> Self {
        Self(varint::grease(random))
    }

    /// Returns whether this is a reserved code of the form `0x1f * N + 0x21`.
    pub const fn is_grease(self) -> bool {
        varint::is_grease(self.0)
    }
}

/// Why received bytes are not valid HTTP/3. [`code`](Self::code) is the error code to close the
/// connection or stream with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    /// A frame of a type that this stream does not permit, as received by this role.
    UnexpectedFrame(FrameType),
    /// A control stream whose first frame is not SETTINGS.
    MissingSettings,
    /// A frame whose payload does not hold exactly the fields its type defines.
    MalformedFrame(FrameType),
    /// A stream that ends inside a frame.
    TruncatedFrame,
    /// A control stream that ends.
    ClosedCriticalStream,
    /// A frame whose payload is larger than the decoder's [`FrameLimits`] allow.
    ///
    /// [`FrameLimits`]: crate::codec::FrameLimits
    FrameTooLarge(FrameType),
    /// A unidirectional stream of a type that the peer may not open, as received by this role.
    UnexpectedStream(StreamType),
    /// A GOAWAY or MAX_PUSH_ID frame whose stream or push ID breaks the rules for its use.
    InvalidId(FrameType),
    /// A SETTINGS frame that holds an identifier more than once.
    DuplicateSetting(SettingId),
    /// A SETTINGS frame that holds an HTTP/2 setting that HTTP/3 does not have.
    ReservedSetting(SettingId),
}

impl DecodeError {
    pub fn code(self) -> H3ErrorCode {
        match self {
            Self::UnexpectedFrame(_) => H3ErrorCode::FRAME_UNEXPECTED,
            Self::MissingSettings => H3ErrorCode::MISSING_SETTINGS,
            Self::MalformedFrame(_) => H3ErrorCode::FRAME_ERROR,
            Self::TruncatedFrame => H3ErrorCode::FRAME_ERROR,
            Self::ClosedCriticalStream => H3ErrorCode::CLOSED_CRITICAL_STREAM,
            Self::FrameTooLarge(_) => H3ErrorCode::EXCESSIVE_LOAD,
            Self::UnexpectedStream(_) => H3ErrorCode::STREAM_CREATION_ERROR,
            Self::InvalidId(_) => H3ErrorCode::ID_ERROR,
            Self::DuplicateSetting(_) | Self::ReservedSetting(_) => H3ErrorCode::SETTINGS_ERROR,
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::UnexpectedFrame(frame_type) => {
                write!(
                    f,
                    "frame type {:#x} is not permitted on this stream",
                    frame_type.as_u64()
                )
            }
            Self::MissingSettings => f.write_str("the control stream does not start with SETTINGS"),
            Self::MalformedFrame(frame_type) => {
                write!(f, "malformed frame of type {:#x}", frame_type.as_u64())
            }
            Self::TruncatedFrame => f.write_str("the stream ends inside a frame"),
            Self::ClosedCriticalStream => f.write_str("the control stream ends"),
            Self::FrameTooLarge(frame_type) => {
                write!(f, "frame of type {:#x} is too large", frame_type.as_u64())
            }
            Self::UnexpectedStream(stream_type) => {
                write!(
                    f,
                    "the peer may not open a stream of type {:#x}",
                    stream_type.as_u64()
                )
            }
            Self::InvalidId(frame_type) => {
                write!(
                    f,
                    "frame of type {:#x} carries an invalid ID",
                    frame_type.as_u64()
                )
            }
            Self::DuplicateSetting(id) => write!(f, "setting {:#x} appears twice", id.as_u64()),
            Self::ReservedSetting(id) => write!(f, "setting {:#x} is reserved", id.as_u64()),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Why a value cannot be encoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodeError {
    /// A value above [`VARINT_MAX`].
    ValueTooLarge,
    /// An HTTP/2 frame type that HTTP/3 reserves and that is never sent.
    ReservedFrameType(FrameType),
    /// A setting identifier that HTTP/3 reserves and that is never sent.
    ReservedSetting(SettingId),
    /// A [`Frame::Grease`](crate::codec::Frame::Grease) whose type is not of the form
    /// `0x1f * N + 0x21`.
    NotGrease(FrameType),
    /// A [`UniStreamHeader::Unknown`](crate::codec::UniStreamHeader::Unknown) that holds one of the
    /// types this crate defines.
    KnownStreamType(StreamType),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::ValueTooLarge => f.write_str("the value does not fit a variable-length integer"),
            Self::ReservedFrameType(frame_type) => {
                write!(f, "frame type {:#x} is reserved", frame_type.as_u64())
            }
            Self::ReservedSetting(id) => write!(f, "setting {:#x} is reserved", id.as_u64()),
            Self::NotGrease(frame_type) => {
                write!(
                    f,
                    "frame type {:#x} is not a reserved type",
                    frame_type.as_u64()
                )
            }
            Self::KnownStreamType(stream_type) => {
                write!(f, "stream type {:#x} is not unknown", stream_type.as_u64())
            }
        }
    }
}

impl std::error::Error for EncodeError {}
