/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/2 wire constants used by the frame encoder and decoder.

/// Bytes in the common HTTP/2 frame header.
pub const FRAME_HEADER_SIZE: usize = 9;
/// Maximum bytes preceding an encoded header block in a HEADERS frame.
pub const FRAME_HEADERS_BASE_MAX_SIZE: usize = FRAME_PRIORITY_SIZE + 1;
/// Bytes in the RFC 7540 priority fields carried by a HEADERS frame.
pub const FRAME_PRIORITY_SIZE: usize = 5;
/// Bytes in an encoded stream identifier.
pub const FRAME_STREAM_ID_SIZE: usize = 4;
/// Required RST_STREAM payload size.
pub const FRAME_RST_STREAM_SIZE: usize = 4;
/// Required PUSH_PROMISE stream-id payload size.
pub const FRAME_PUSH_PROMISE_SIZE: usize = 4;
/// Required PING payload size.
pub const FRAME_PING_SIZE: usize = 8;
/// Required GOAWAY payload prefix size.
pub const FRAME_GOAWAY_SIZE: usize = 8;
/// Required WINDOW_UPDATE payload size.
pub const FRAME_WINDOW_UPDATE_SIZE: usize = 4;
/// Required CERTIFICATE_REQUEST payload prefix size.
pub const FRAME_CERTIFICATE_REQUEST_SIZE_BASE: usize = 2;
/// Required CERTIFICATE payload prefix size.
pub const FRAME_CERTIFICATE_SIZE_BASE: usize = 2;
/// Required Alt-Svc payload prefix size.
pub const FRAME_ALT_SVC_SIZE_BASE: usize = 8;

/// Minimum value peers may advertise for SETTINGS_MAX_FRAME_SIZE.
pub const MAX_FRAME_PAYLOAD_LENGTH_MIN: u32 = 1 << 14;
/// Largest payload representable by the frame header's 24-bit length.
pub const MAX_FRAME_PAYLOAD_LENGTH: u32 = (1 << 24) - 1;
/// Largest HTTP/2 stream identifier after removal of the reserved bit.
pub const MAX_STREAM_ID: u32 = (1 << 31) - 1;
/// Initial HTTP/2 stream and connection flow-control window.
pub const INITIAL_WINDOW: u32 = (1 << 16) - 1;
/// Largest legal WINDOW_UPDATE increment.
pub const MAX_WINDOW_UPDATE_SIZE: u32 = (1 << 31) - 1;
/// Default upper bound for an HPACK header table.
pub const MAX_HEADER_TABLE_SIZE: u32 = 1 << 16;
/// Largest cached secondary-authentication authenticator.
pub const MAX_AUTHENTICATOR_BUFFER_SIZE: u32 = 0x20_000;

/// HTTP/2 client connection preface.
pub const CONNECTION_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
/// Standard TLS ALPN identifier.
pub const PROTOCOL: &str = "h2";
/// Historical draft ALPN identifier.
pub const PROTOCOL_DRAFT: &str = "h2-14";
/// Proxygen's experimental ALPN identifier.
pub const PROTOCOL_EXPERIMENTAL: &str = "h2-fb";
/// Cleartext HTTP/2 upgrade identifier.
pub const PROTOCOL_CLEARTEXT: &str = "h2c";
/// HTTP/1 upgrade header carrying encoded HTTP/2 settings.
pub const PROTOCOL_SETTINGS_HEADER: &str = "HTTP2-Settings";
