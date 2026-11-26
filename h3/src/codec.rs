/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/3 framing (RFC 9114), with the QPACK stream types of RFC 9204.
//!
//! - [`UniStreamHeaderDecoder`] reads the header that starts each unidirectional stream the peer
//!   opens: its type, and a push stream's push ID. [`UniStreamHeader::encode`] writes one.
//! - [`FrameDecoder`] parses the frames of one stream from input split at any point, and checks
//!   them against the stream's kind and the local role. DATA payloads come out as they arrive,
//!   as views of the input when it is `Bytes`.
//! - [`Frame::encode`] writes a frame. [`encode_frame_header`] writes only a type and length, for a
//!   payload that is sent as its own buffer.
//! - [`Settings`] holds the parameters of a SETTINGS frame, and [`H3ErrorCode`] the error codes.
//! - The `grease` constructors pick reserved values of the form `0x1f * N + 0x21`, which a peer
//!   must ignore, for exercising its handling of unknown values.

mod decoder;
mod error;
mod frame;
mod settings;
mod stream;
pub(crate) mod varint;

pub use decoder::FrameDecoder;
pub use decoder::FrameLimits;
pub use decoder::Role;
pub use decoder::StreamKind;
pub use error::DecodeError;
pub use error::EncodeError;
pub use error::H3ErrorCode;
pub use frame::Frame;
pub use frame::FrameType;
pub use frame::encode_frame_header;
pub use frame::frame_header_len;
pub use settings::SettingId;
pub use settings::Settings;
pub use stream::StreamType;
pub use stream::UniStreamHeader;
pub use stream::UniStreamHeaderDecoder;
pub use varint::VARINT_MAX;
