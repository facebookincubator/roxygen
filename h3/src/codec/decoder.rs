/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;

use crate::codec::DecodeError;
use crate::codec::Frame;
use crate::codec::FrameType;
use crate::codec::Settings;
use crate::codec::varint::VarIntReader;
use crate::codec::varint::decode_varint;

/// The kinds of HTTP/3 stream that carry frames. Each permits a different set of frame types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamKind {
    Control,
    Request,
    Push,
}

/// The local endpoint's role in the connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Client,
    Server,
}

/// Payload sizes, in bytes, above which [`FrameDecoder`] rejects a frame that it would otherwise
/// buffer. The defaults are 128 KiB for HEADERS and PUSH_PROMISE and 4 KiB for SETTINGS. DATA
/// payloads and the payloads of skipped frames are neither buffered nor limited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    /// The largest HEADERS or PUSH_PROMISE payload.
    pub max_field_section_payload: usize,
    /// The largest SETTINGS payload. A CANCEL_PUSH, GOAWAY or MAX_PUSH_ID frame that declares more
    /// than the larger of this and 8 bytes is rejected too.
    pub max_settings_payload: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_field_section_payload: 1 << 17,
            max_settings_payload: 1 << 12,
        }
    }
}

/// An incremental parser for the frames received on one stream, after the stream's header.
///
/// It checks each frame's layout and size, which frame types the stream carries for this role,
/// the contents of SETTINGS, and that GOAWAY IDs never increase and MAX_PUSH_ID never decreases.
/// The order of HEADERS and DATA within a message and push ID limits are left to the caller.
/// Unknown frame types, grease included, are skipped, and the HTTP/2 frame types that HTTP/3
/// reserves are rejected.
#[derive(Debug)]
pub struct FrameDecoder {
    kind: StreamKind,
    role: Role,
    limits: FrameLimits,
    state: State,
    varint: VarIntReader,
    /// Whether a control stream's SETTINGS frame has arrived.
    settings_seen: bool,
    /// The ID in the last GOAWAY frame.
    last_goaway: Option<u64>,
    /// The push ID in the last MAX_PUSH_ID frame.
    max_push_id: Option<u64>,
    /// The part of a buffered frame's payload that has arrived.
    payload: BytesMut,
}

#[derive(Clone, Copy, Debug)]
enum State {
    Type,
    Length(FrameType),
    Data { remaining: u64 },
    Buffered { frame_type: FrameType, len: usize },
    Skip { remaining: u64 },
    Failed(DecodeError),
}

impl FrameDecoder {
    pub fn new(kind: StreamKind, role: Role, limits: FrameLimits) -> Self {
        Self {
            kind,
            role,
            limits,
            state: State::Type,
            varint: VarIntReader::default(),
            settings_seen: false,
            last_goaway: None,
            max_push_id: None,
            payload: BytesMut::new(),
        }
    }

    /// Consumes bytes from `input` up to the end of the next frame, or of the next part of a
    /// DATA payload, and returns it. Returns `Ok(None)` once `input` is empty; a frame that
    /// `input` ends inside is kept and completed by later calls.
    ///
    /// After an error the decoder stops consuming input and returns the same error.
    pub fn decode<B: Buf>(&mut self, input: &mut B) -> Result<Option<Frame>, DecodeError> {
        let result = self.decode_next(input);
        if let Err(error) = result {
            self.state = State::Failed(error);
        }
        result
    }

    /// Checks that the stream may end here, when the peer has ended it cleanly. A control stream
    /// never may. After an error it returns that error.
    pub fn finish(&self) -> Result<(), DecodeError> {
        if let State::Failed(error) = self.state {
            return Err(error);
        }
        if self.kind == StreamKind::Control {
            return Err(DecodeError::ClosedCriticalStream);
        }
        match self.state {
            State::Type if !self.varint.is_partial() => Ok(()),
            _ => Err(DecodeError::TruncatedFrame),
        }
    }

    fn decode_next<B: Buf>(&mut self, input: &mut B) -> Result<Option<Frame>, DecodeError> {
        loop {
            match self.state {
                State::Failed(error) => return Err(error),
                State::Type => {
                    let Some(value) = self.varint.read(input) else {
                        return Ok(None);
                    };
                    let frame_type = FrameType::from_wire(value);
                    self.check_type(frame_type)?;
                    self.state = State::Length(frame_type);
                }
                State::Length(frame_type) => {
                    let Some(len) = self.varint.read(input) else {
                        return Ok(None);
                    };
                    if let Some(frame) = self.start_payload(frame_type, len)? {
                        return Ok(Some(frame));
                    }
                }
                State::Data { remaining } => {
                    let n = remaining.min(input.chunk().len() as u64);
                    if n == 0 {
                        return Ok(None);
                    }
                    self.state = match remaining - n {
                        0 => State::Type,
                        remaining => State::Data { remaining },
                    };
                    return Ok(Some(Frame::Data(input.copy_to_bytes(n as usize))));
                }
                State::Buffered { frame_type, len } => {
                    let Some(payload) = self.take_payload(input, len) else {
                        return Ok(None);
                    };
                    self.state = State::Type;
                    return self.parse_buffered(frame_type, payload).map(Some);
                }
                State::Skip { remaining } => {
                    let n = remaining.min(input.remaining() as u64);
                    input.advance(n as usize);
                    if n < remaining {
                        self.state = State::Skip {
                            remaining: remaining - n,
                        };
                        return Ok(None);
                    }
                    self.state = State::Type;
                }
            }
        }
    }

    fn check_type(&mut self, frame_type: FrameType) -> Result<(), DecodeError> {
        let control = self.kind == StreamKind::Control;
        if control && !self.settings_seen {
            if frame_type != FrameType::SETTINGS {
                return Err(DecodeError::MissingSettings);
            }
            self.settings_seen = true;
            return Ok(());
        }
        let permitted = match frame_type {
            FrameType::DATA | FrameType::HEADERS => !control,
            // SETTINGS is permitted only as the first frame of a control stream.
            FrameType::SETTINGS => false,
            FrameType::CANCEL_PUSH | FrameType::GOAWAY => control,
            FrameType::PUSH_PROMISE => {
                self.kind == StreamKind::Request && self.role == Role::Client
            }
            FrameType::MAX_PUSH_ID => control && self.role == Role::Server,
            _ => !frame_type.is_http2_reserved(),
        };
        if permitted {
            Ok(())
        } else {
            Err(DecodeError::UnexpectedFrame(frame_type))
        }
    }

    /// Picks the state for a payload of `len` bytes, or returns an empty DATA frame at once.
    fn start_payload(
        &mut self,
        frame_type: FrameType,
        len: u64,
    ) -> Result<Option<Frame>, DecodeError> {
        let limit = match frame_type {
            FrameType::DATA if len == 0 => {
                self.state = State::Type;
                return Ok(Some(Frame::Data(Bytes::new())));
            }
            FrameType::DATA => {
                self.state = State::Data { remaining: len };
                return Ok(None);
            }
            FrameType::HEADERS | FrameType::PUSH_PROMISE => self.limits.max_field_section_payload,
            FrameType::SETTINGS => self.limits.max_settings_payload,
            // The payload is one integer, of 1, 2, 4 or 8 bytes. A length that cannot hold exactly
            // one is malformed, unless it is more than a control stream buffers for any frame,
            // which is excessive load.
            FrameType::CANCEL_PUSH | FrameType::GOAWAY | FrameType::MAX_PUSH_ID => {
                let limit = self.limits.max_settings_payload.max(8);
                if (len == 0 || len > 8) && usize::try_from(len).is_ok_and(|len| len <= limit) {
                    return Err(DecodeError::MalformedFrame(frame_type));
                }
                limit
            }
            _ => {
                self.state = State::Skip { remaining: len };
                return Ok(None);
            }
        };
        match usize::try_from(len) {
            Ok(len) if len <= limit => {
                self.state = State::Buffered { frame_type, len };
                Ok(None)
            }
            _ => Err(DecodeError::FrameTooLarge(frame_type)),
        }
    }

    fn parse_buffered(
        &mut self,
        frame_type: FrameType,
        payload: Bytes,
    ) -> Result<Frame, DecodeError> {
        Ok(match frame_type {
            FrameType::SETTINGS => Frame::Settings(Settings::decode(&payload)?),
            FrameType::CANCEL_PUSH => Frame::CancelPush(single_varint(frame_type, &payload)?),
            FrameType::PUSH_PROMISE => {
                let (push_id, len) =
                    decode_varint(&payload).ok_or(DecodeError::MalformedFrame(frame_type))?;
                Frame::PushPromise {
                    push_id,
                    field_section: payload.slice(len..),
                }
            }
            FrameType::GOAWAY => {
                let id = single_varint(frame_type, &payload)?;
                // A server's GOAWAY names a client-initiated bidirectional stream.
                let wrong_kind = self.role == Role::Client && id % 4 != 0;
                if wrong_kind || self.last_goaway.is_some_and(|last| id > last) {
                    return Err(DecodeError::InvalidId(frame_type));
                }
                self.last_goaway = Some(id);
                Frame::Goaway(id)
            }
            FrameType::MAX_PUSH_ID => {
                let push_id = single_varint(frame_type, &payload)?;
                if self.max_push_id.is_some_and(|max| push_id < max) {
                    return Err(DecodeError::InvalidId(frame_type));
                }
                self.max_push_id = Some(push_id);
                Frame::MaxPushId(push_id)
            }
            // HEADERS, the only other type that is buffered.
            _ => Frame::Headers(payload),
        })
    }

    /// Collects a payload of `len` bytes, without copying from `Bytes` input that holds all of it.
    fn take_payload<B: Buf>(&mut self, input: &mut B, len: usize) -> Option<Bytes> {
        if self.payload.is_empty() && input.chunk().len() >= len {
            return Some(input.copy_to_bytes(len));
        }
        while self.payload.len() < len && input.has_remaining() {
            let chunk = input.chunk();
            let n = chunk.len().min(len - self.payload.len());
            self.payload.extend_from_slice(&chunk[..n]);
            input.advance(n);
        }
        (self.payload.len() == len).then(|| self.payload.split().freeze())
    }
}

/// Returns the one integer that makes up a payload.
fn single_varint(frame_type: FrameType, payload: &[u8]) -> Result<u64, DecodeError> {
    match decode_varint(payload) {
        Some((value, len)) if len == payload.len() => Ok(value),
        _ => Err(DecodeError::MalformedFrame(frame_type)),
    }
}
