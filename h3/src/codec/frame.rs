/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::BufMut;
use bytes::Bytes;

use crate::codec::EncodeError;
use crate::codec::Settings;
use crate::codec::varint;
use crate::codec::varint::VARINT_MAX;
use crate::codec::varint::put_varint;
use crate::codec::varint::varint_len;

/// An HTTP/3 frame type. Unknown peer-provided values stay representable.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct FrameType(u64);

impl FrameType {
    pub const DATA: Self = Self(0x00);
    pub const HEADERS: Self = Self(0x01);
    pub const CANCEL_PUSH: Self = Self(0x03);
    pub const SETTINGS: Self = Self(0x04);
    pub const PUSH_PROMISE: Self = Self(0x05);
    pub const GOAWAY: Self = Self(0x07);
    pub const MAX_PUSH_ID: Self = Self(0x0d);

    /// HTTP/2's PRIORITY, PING, WINDOW_UPDATE and CONTINUATION, reserved and never sent.
    const HTTP2_RESERVED: [Self; 4] = [Self(0x02), Self(0x06), Self(0x08), Self(0x09)];

    /// Returns the type `value`, or `None` if it does not fit a QUIC variable-length integer.
    pub const fn new(value: u64) -> Option<Self> {
        if value <= VARINT_MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Returns a type read from the wire, which always fits.
    pub(crate) const fn from_wire(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns a reserved type chosen by `random`, for frames that exercise a peer's handling of
    /// unknown frame types. Small values of `random` give short encodings.
    pub const fn grease(random: u64) -> Self {
        Self(varint::grease(random))
    }

    /// Returns whether this is a reserved type of the form `0x1f * N + 0x21`.
    pub const fn is_grease(self) -> bool {
        varint::is_grease(self.0)
    }

    pub(crate) fn is_http2_reserved(self) -> bool {
        Self::HTTP2_RESERVED.contains(&self)
    }
}

/// An HTTP/3 frame.
///
/// [`FrameDecoder`](crate::codec::FrameDecoder) yields a DATA frame's payload as it arrives, as
/// one or more [`Frame::Data`] values whose payloads concatenate to the frame's payload (one
/// empty value for an empty frame).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
    Data(Bytes),
    /// An encoded field section.
    Headers(Bytes),
    /// A push ID.
    CancelPush(u64),
    Settings(Settings),
    PushPromise {
        push_id: u64,
        /// The promised request's encoded field section.
        field_section: Bytes,
    },
    /// A stream ID from a server, a push ID from a client.
    Goaway(u64),
    /// The largest push ID the client permits.
    MaxPushId(u64),
    /// A frame of a reserved type, which the peer ignores, sent to exercise its handling of
    /// unknown frame types or as padding.
    Grease {
        frame_type: FrameType,
        payload: Bytes,
    },
}

impl Frame {
    /// Appends the encoded frame to `buf`. Fails with [`EncodeError::ValueTooLarge`] for an ID
    /// above [`VARINT_MAX`], and with [`EncodeError::NotGrease`] for a [`Frame::Grease`] of a type
    /// not of the form `0x1f * N + 0x21`. On error `buf` is unchanged. Panics if `buf` is too
    /// small and cannot grow.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        let values_fit = match self {
            Self::CancelPush(value) | Self::Goaway(value) | Self::MaxPushId(value) => {
                *value <= VARINT_MAX
            }
            Self::PushPromise { push_id, .. } => *push_id <= VARINT_MAX,
            _ => true,
        };
        if !values_fit {
            return Err(EncodeError::ValueTooLarge);
        }
        if let Self::Grease { frame_type, .. } = self
            && !frame_type.is_grease()
        {
            return Err(EncodeError::NotGrease(*frame_type));
        }
        match self {
            Self::Data(payload) => put_payload_frame(buf, FrameType::DATA, payload),
            Self::Headers(field_section) => {
                put_payload_frame(buf, FrameType::HEADERS, field_section)
            }
            Self::CancelPush(push_id) => put_varint_frame(buf, FrameType::CANCEL_PUSH, *push_id),
            Self::Settings(settings) => {
                put_varint(buf, FrameType::SETTINGS.0);
                put_varint(buf, settings.payload_len() as u64);
                settings.put_payload(buf);
            }
            Self::PushPromise {
                push_id,
                field_section,
            } => {
                put_varint(buf, FrameType::PUSH_PROMISE.0);
                put_varint(buf, (varint_len(*push_id) + field_section.len()) as u64);
                put_varint(buf, *push_id);
                buf.put_slice(field_section);
            }
            Self::Goaway(id) => put_varint_frame(buf, FrameType::GOAWAY, *id),
            Self::MaxPushId(push_id) => put_varint_frame(buf, FrameType::MAX_PUSH_ID, *push_id),
            Self::Grease {
                frame_type,
                payload,
            } => put_payload_frame(buf, *frame_type, payload),
        }
        Ok(())
    }
}

/// Appends the type and length of a frame whose `payload_len` bytes of payload the caller
/// appends next, so that a shared payload is never copied. Fails with
/// [`EncodeError::ValueTooLarge`] if `payload_len` is above [`VARINT_MAX`], and with
/// [`EncodeError::ReservedFrameType`] for an HTTP/2 frame type that HTTP/3 reserves. On error
/// `buf` is unchanged. Panics if `buf` is too small and cannot grow.
pub fn encode_frame_header<B: BufMut>(
    buf: &mut B,
    frame_type: FrameType,
    payload_len: u64,
) -> Result<(), EncodeError> {
    if payload_len > VARINT_MAX {
        return Err(EncodeError::ValueTooLarge);
    }
    if frame_type.is_http2_reserved() {
        return Err(EncodeError::ReservedFrameType(frame_type));
    }
    put_varint(buf, frame_type.0);
    put_varint(buf, payload_len);
    Ok(())
}

/// Returns the length of the type and length of a frame with `payload_len` bytes of payload.
pub fn frame_header_len(frame_type: FrameType, payload_len: u64) -> usize {
    varint_len(frame_type.0) + varint_len(payload_len)
}

fn put_varint_frame<B: BufMut>(buf: &mut B, frame_type: FrameType, value: u64) {
    put_varint(buf, frame_type.0);
    put_varint(buf, varint_len(value) as u64);
    put_varint(buf, value);
}

fn put_payload_frame<B: BufMut>(buf: &mut B, frame_type: FrameType, payload: &[u8]) {
    put_varint(buf, frame_type.0);
    put_varint(buf, payload.len() as u64);
    buf.put_slice(payload);
}
