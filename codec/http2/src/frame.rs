/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Source-faithful HTTP/2 frame parsing and serialization.

use bytes::Bytes;
use bytes::BytesMut;
use roxygen_codec::H2ErrorCode;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::BufQueueError;
use roxygen_core::buf_queue::BufQueueSegments;
use roxygen_core::buf_queue::COMPACT_WRITE_LIMIT;
use thiserror::Error;

use crate::constants::FRAME_ALT_SVC_SIZE_BASE;
use crate::constants::FRAME_CERTIFICATE_REQUEST_SIZE_BASE;
use crate::constants::FRAME_CERTIFICATE_SIZE_BASE;
use crate::constants::FRAME_GOAWAY_SIZE;
use crate::constants::FRAME_HEADER_SIZE;
use crate::constants::FRAME_PING_SIZE;
use crate::constants::FRAME_PRIORITY_SIZE;
use crate::constants::FRAME_PUSH_PROMISE_SIZE;
use crate::constants::FRAME_RST_STREAM_SIZE;
use crate::constants::FRAME_WINDOW_UPDATE_SIZE;
use crate::constants::MAX_FRAME_PAYLOAD_LENGTH;
use crate::constants::MAX_STREAM_ID;
use crate::constants::MAX_WINDOW_UPDATE_SIZE;

/// Lowest frame type reserved by Proxygen for experimental extensions.
pub const MIN_EXPERIMENTAL_FRAME_TYPE: u8 = 0xf0;

/// A wire frame type. Unknown peer-provided values remain representable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct FrameType(u8);

impl FrameType {
    pub const DATA: Self = Self(0);
    pub const HEADERS: Self = Self(1);
    pub const PRIORITY: Self = Self(2);
    pub const RST_STREAM: Self = Self(3);
    pub const SETTINGS: Self = Self(4);
    pub const PUSH_PROMISE: Self = Self(5);
    pub const PING: Self = Self(6);
    pub const GOAWAY: Self = Self(7);
    pub const WINDOW_UPDATE: Self = Self(8);
    pub const CONTINUATION: Self = Self(9);
    pub const ALTSVC: Self = Self(10);
    pub const RFC9218_PRIORITY: Self = Self(16);
    pub const PADDING: Self = Self(0xbb);
    pub const CERTIFICATE_REQUEST: Self = Self(0xf0);
    pub const CERTIFICATE: Self = Self(0xf1);
    pub const CERTIFICATE_NEEDED: Self = Self(0xf2);
    pub const USE_CERTIFICATE: Self = Self(0xf3);

    pub const fn from_u8(value: u8) -> Self {
        Self(value)
    }

    pub const fn as_u8(self) -> u8 {
        self.0
    }
}

impl From<u8> for FrameType {
    fn from(value: u8) -> Self {
        Self::from_u8(value)
    }
}

impl From<FrameType> for u8 {
    fn from(value: FrameType) -> Self {
        value.as_u8()
    }
}

/// HTTP/2 frame flags. Some bit values have frame-specific meanings.
pub struct Flags;

impl Flags {
    pub const ACK: u8 = 0x1;
    pub const END_STREAM: u8 = 0x1;
    pub const END_HEADERS: u8 = 0x4;
    pub const PADDED: u8 = 0x8;
    pub const PRIORITY: u8 = 0x20;
    pub const UNIDIRECTIONAL: u8 = 0x40;
    pub const UNSOLICITED: u8 = 0x1;
    pub const TO_BE_CONTINUED: u8 = 0x1;
}

/// Parsed common frame header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub length: u32,
    pub stream: u32,
    pub frame_type: FrameType,
    pub flags: u8,
}

/// Legacy dependency-based priority fields retained by the framing surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriorityUpdate {
    pub stream_dependency: u64,
    pub exclusive: bool,
    pub weight: u8,
}

/// Proxygen's default legacy priority tuple.
pub const DEFAULT_PRIORITY: PriorityUpdate = PriorityUpdate {
    stream_dependency: 0,
    exclusive: false,
    weight: 15,
};

/// One HTTP/2 setting in wire-width form.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Setting {
    pub identifier: u16,
    pub value: u32,
}

/// A safe framing failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FrameError {
    #[error("incomplete frame input: need {needed} bytes but only {available} are available")]
    Incomplete { needed: usize, available: usize },
    #[error("HTTP/2 frame error: {0:?}")]
    Peer(H2ErrorCode),
    #[error("invalid local frame input: {0}")]
    InvalidInput(&'static str),
    #[error(transparent)]
    Buffer(#[from] BufQueueError),
}

impl FrameError {
    /// Returns the HTTP/2 connection error carried by a peer-input failure.
    pub const fn peer_code(self) -> Option<H2ErrorCode> {
        match self {
            Self::Peer(code) => Some(code),
            Self::Incomplete { .. } | Self::InvalidInput(_) | Self::Buffer(_) => None,
        }
    }
}

/// Parsed DATA payload and its flow-controlled padding byte count.
#[derive(Debug)]
pub struct DataFrame {
    pub data: BufQueue,
    pub padding: u16,
}

/// Information needed to stream a DATA payload before its trailing padding arrives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataFrameBegin {
    pub data_length: usize,
    pub padding: u16,
    pub prefix_length: usize,
}

/// Parsed Alt-Svc extension payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AltSvcFrame {
    pub max_age: u32,
    pub port: u16,
    pub protocol: Vec<u8>,
    pub host: Vec<u8>,
    pub origin: Vec<u8>,
}

/// Parsed secondary-certificate request payload.
#[derive(Debug)]
pub struct CertificateRequestFrame {
    pub request_id: u16,
    pub authenticator_request: BufQueue,
}

/// Parsed secondary-certificate payload.
#[derive(Debug)]
pub struct CertificateFrame {
    pub certificate_id: u16,
    pub authenticator: BufQueue,
}

/// Typed payload returned by [`parse_frame`].
#[derive(Debug)]
pub enum Frame {
    Data(DataFrame),
    Headers(BufQueue),
    Priority(BufQueue),
    RstStream(H2ErrorCode),
    Settings(Vec<Setting>),
    PushPromise {
        promised_stream: u32,
        header_block: BufQueue,
    },
    Ping(u64),
    Goaway {
        last_stream: u32,
        error_code: H2ErrorCode,
        debug_data: BufQueue,
    },
    WindowUpdate(u32),
    Continuation(BufQueue),
    AltSvc(AltSvcFrame),
    Rfc9218Priority {
        prioritized_stream: u32,
        priority: Vec<u8>,
    },
    Padding(BufQueue),
    CertificateRequest(CertificateRequestFrame),
    Certificate(CertificateFrame),
    Unknown(BufQueue),
}

/// One complete frame together with its exact consumed byte count.
#[derive(Debug)]
pub struct ParsedFrame {
    pub header: FrameHeader,
    pub frame: Frame,
    pub consumed: usize,
}

/// Whether Proxygen recognizes a frame type for local generation.
pub const fn is_valid_frame_type(frame_type: FrameType) -> bool {
    let value = frame_type.as_u8();
    if value < MIN_EXPERIMENTAL_FRAME_TYPE {
        value <= FrameType::ALTSVC.as_u8()
            || value == FrameType::RFC9218_PRIORITY.as_u8()
            || value == FrameType::PADDING.as_u8()
    } else {
        value == FrameType::CERTIFICATE_REQUEST.as_u8() || value == FrameType::CERTIFICATE.as_u8()
    }
}

/// Whether a frame participates in an HPACK header block.
pub const fn frame_affects_compression(frame_type: FrameType) -> bool {
    frame_type.as_u8() == FrameType::HEADERS.as_u8()
        || frame_type.as_u8() == FrameType::PUSH_PROMISE.as_u8()
        || frame_type.as_u8() == FrameType::CONTINUATION.as_u8()
}

/// Whether the frame's PADDED bit is set.
pub const fn frame_has_padding(header: &FrameHeader) -> bool {
    header.flags & Flags::PADDED != 0
}

/// Returns Proxygen's diagnostic spelling for a frame type.
pub const fn get_frame_type_string(frame_type: FrameType) -> &'static str {
    match frame_type.as_u8() {
        0 => "DATA",
        1 => "HEADERS",
        2 => "PRIORITY",
        3 => "RST_STREAM",
        4 => "SETTINGS",
        5 => "PUSH_PROMISE",
        6 => "PING",
        7 => "GOAWAY",
        8 => "WINDOW_UPDATE",
        9 => "CONTINUATION",
        10 => "ALTSVC",
        16 => "RFC9218_PRIORITY",
        0xbb => "PADDING",
        0xf0 => "CERTIFICATE_REQUEST",
        0xf1 => "CERTIFICATE",
        0xf2 => "CERTIFICATE_NEEDED",
        0xf3 => "USE_CERTIFICATE",
        _ => "Unknown",
    }
}

/// Calculates the frame-header and prefix bytes before a header block.
pub const fn calculate_pre_header_block_size(
    has_associated_stream: bool,
    has_priority: bool,
    has_padding: bool,
) -> u8 {
    let mut size = FRAME_HEADER_SIZE as u8;
    if has_associated_stream {
        size += 4;
    } else if has_priority {
        size += FRAME_PRIORITY_SIZE as u8;
    }
    if has_padding {
        size += 1;
    }
    size
}

#[derive(Clone)]
pub(crate) struct SegmentCursor<'a> {
    segments: BufQueueSegments<'a>,
    segment: Option<&'a Bytes>,
    offset: usize,
    remaining: usize,
}

impl<'a> SegmentCursor<'a> {
    fn new(queue: &'a BufQueue, limit: usize) -> Result<Self, FrameError> {
        Self::from_queue(queue).bounded(limit)
    }

    pub(crate) fn from_queue(queue: &'a BufQueue) -> Self {
        let mut segments = queue.segments();
        let segment = segments.next();
        Self {
            segments,
            segment,
            offset: 0,
            remaining: queue.chain_length(),
        }
    }

    fn require(&self, length: usize) -> Result<(), FrameError> {
        if length > self.remaining {
            return Err(FrameError::Incomplete {
                needed: length,
                available: self.remaining,
            });
        }
        Ok(())
    }

    fn bounded(mut self, length: usize) -> Result<Self, FrameError> {
        self.require(length)?;
        self.remaining = length;
        Ok(self)
    }

    pub(crate) fn take(&mut self, length: usize) -> Result<Self, FrameError> {
        let child = self.clone().bounded(length)?;
        self.advance(length)?;
        Ok(child)
    }

    pub(crate) const fn remaining(&self) -> usize {
        self.remaining
    }

    pub(crate) fn contiguous(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        self.skip_empty_segments();
        let bytes = &self.segment?[self.offset..];
        if bytes.len() < self.remaining {
            return None;
        }
        Some(&bytes[..self.remaining])
    }

    pub(crate) fn next_chunk(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        self.skip_empty_segments();
        let segment = self.segment?;
        let count = (segment.len() - self.offset).min(self.remaining);
        let bytes = &segment[self.offset..self.offset + count];
        self.offset += count;
        self.remaining -= count;
        Some(bytes)
    }

    #[inline]
    fn read_into(&mut self, output: &mut [u8]) -> Result<(), FrameError> {
        let length = output.len();
        if length <= self.remaining {
            self.skip_empty_segments();
            if let Some(segment) = self.segment
                && segment.len() - self.offset >= length
            {
                output.copy_from_slice(&segment[self.offset..self.offset + length]);
                self.offset += length;
                self.remaining -= length;
                return Ok(());
            }
        }
        self.read_into_segmented(output)
    }

    #[inline(never)]
    fn read_into_segmented(&mut self, output: &mut [u8]) -> Result<(), FrameError> {
        if output.len() > self.remaining {
            return Err(FrameError::Incomplete {
                needed: output.len(),
                available: self.remaining,
            });
        }
        let mut written = 0;
        while written < output.len() {
            self.skip_empty_segments();
            let Some(segment) = self.segment else {
                return Err(FrameError::Incomplete {
                    needed: output.len() - written,
                    available: 0,
                });
            };
            let count = (segment.len() - self.offset).min(output.len() - written);
            output[written..written + count]
                .copy_from_slice(&segment[self.offset..self.offset + count]);
            written += count;
            self.offset += count;
            self.remaining -= count;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn read_u8(&mut self) -> Result<u8, FrameError> {
        let mut bytes = [0; 1];
        self.read_into(&mut bytes)?;
        Ok(bytes[0])
    }

    #[inline]
    fn read_u16(&mut self) -> Result<u16, FrameError> {
        let mut bytes = [0; 2];
        self.read_into(&mut bytes)?;
        Ok(u16::from_be_bytes(bytes))
    }

    #[inline]
    fn read_u32(&mut self) -> Result<u32, FrameError> {
        let mut bytes = [0; 4];
        self.read_into(&mut bytes)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64_native(&mut self) -> Result<u64, FrameError> {
        let mut bytes = [0; 8];
        self.read_into(&mut bytes)?;
        Ok(u64::from_ne_bytes(bytes))
    }

    fn read_vec(&mut self, length: usize) -> Result<Vec<u8>, FrameError> {
        let mut bytes = vec![0; length];
        self.read_into(&mut bytes)?;
        Ok(bytes)
    }

    pub(crate) fn read_queue(&mut self, length: usize) -> Result<BufQueue, FrameError> {
        if length > self.remaining {
            return Err(FrameError::Incomplete {
                needed: length,
                available: self.remaining,
            });
        }
        let mut output = BufQueue::new(length);
        let mut left = length;
        while left != 0 {
            self.skip_empty_segments();
            let Some(segment) = self.segment else {
                return Err(FrameError::Incomplete {
                    needed: left,
                    available: 0,
                });
            };
            let count = (segment.len() - self.offset).min(left);
            output.push_bytes(segment.slice(self.offset..self.offset + count))?;
            self.offset += count;
            self.remaining -= count;
            left -= count;
        }
        Ok(output)
    }

    fn skip_empty_segments(&mut self) {
        while self
            .segment
            .is_some_and(|segment| self.offset == segment.len())
        {
            self.segment = self.segments.next();
            self.offset = 0;
        }
    }

    pub(crate) fn advance(&mut self, length: usize) -> Result<(), FrameError> {
        self.require(length)?;
        let mut left = length;
        while left != 0 {
            self.skip_empty_segments();
            let Some(segment) = self.segment else {
                return Err(FrameError::Incomplete {
                    needed: left,
                    available: 0,
                });
            };
            let count = (segment.len() - self.offset).min(left);
            self.offset += count;
            self.remaining -= count;
            left -= count;
        }
        Ok(())
    }

    fn skip_zeroes(&mut self, length: usize) -> Result<(), FrameError> {
        let mut scratch = [0; 32];
        let mut left = length;
        while left != 0 {
            let count = left.min(scratch.len());
            self.read_into(&mut scratch[..count])?;
            if scratch[..count].iter().any(|byte| *byte != 0) {
                return Err(FrameError::Peer(H2ErrorCode::PROTOCOL_ERROR));
            }
            left -= count;
        }
        Ok(())
    }
}

fn peer(code: H2ErrorCode) -> FrameError {
    FrameError::Peer(code)
}

fn payload_cursor<'a>(
    input: &'a BufQueue,
    header: &FrameHeader,
) -> Result<SegmentCursor<'a>, FrameError> {
    SegmentCursor::new(input, header.length as usize)
}

fn parse_uint31(cursor: &mut SegmentCursor<'_>) -> Result<u32, FrameError> {
    Ok(cursor.read_u32()? & MAX_STREAM_ID)
}

fn parse_error_code(cursor: &mut SegmentCursor<'_>) -> Result<H2ErrorCode, FrameError> {
    let code = cursor.read_u32()?;
    if code >= 14 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    Ok(H2ErrorCode::from_u32(code))
}

fn parse_padding(
    cursor: &mut SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<(u8, usize), FrameError> {
    let mut left = header.length as usize;
    let padding = if frame_has_padding(header) {
        if left < 1 {
            return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
        }
        left -= 1;
        cursor.read_u8()?
    } else {
        0
    };
    if left < usize::from(padding) {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    Ok((padding, left - usize::from(padding)))
}

/// Parses a common nine-byte HTTP/2 frame header without consuming `input`.
pub fn parse_frame_header(input: &BufQueue) -> Result<FrameHeader, FrameError> {
    parse_frame_header_cursor(&mut SegmentCursor::from_queue(input))
}

#[inline]
pub(crate) fn parse_frame_header_cursor(
    cursor: &mut SegmentCursor<'_>,
) -> Result<FrameHeader, FrameError> {
    let mut bytes = [0; FRAME_HEADER_SIZE];
    cursor.read_into(&mut bytes)?;
    let [l0, l1, l2, frame_type, flags, s0, s1, s2, s3] = bytes;
    let length = u32::from_be_bytes([0, l0, l1, l2]) & MAX_FRAME_PAYLOAD_LENGTH;
    let frame_type = FrameType::from_u8(frame_type);
    let stream = u32::from_be_bytes([s0, s1, s2, s3]) & MAX_STREAM_ID;
    Ok(FrameHeader {
        length,
        stream,
        frame_type,
        flags,
    })
}

/// Parses a DATA payload, verifies zero padding, and preserves payload segments.
pub fn parse_data(input: &BufQueue, header: &FrameHeader) -> Result<DataFrame, FrameError> {
    parse_data_cursor(&mut SegmentCursor::from_queue(input), header)
}

pub(crate) fn parse_data_cursor(
    cursor: &mut SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<DataFrame, FrameError> {
    if header.stream == 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    cursor.require(header.length as usize)?;
    let (padding, data_length) = parse_padding(cursor, header)?;
    let data = cursor.read_queue(data_length)?;
    cursor.skip_zeroes(usize::from(padding))?;
    Ok(DataFrame {
        data,
        padding: u16::from(padding) + if frame_has_padding(header) { 1 } else { 0 },
    })
}

/// Parses the optional DATA padding prefix for incremental body delivery.
pub fn parse_data_begin(
    input: &BufQueue,
    header: &FrameHeader,
) -> Result<DataFrameBegin, FrameError> {
    let prefix_length = usize::from(frame_has_padding(header));
    let mut cursor = SegmentCursor::new(input, prefix_length)?;
    let (padding, data_length) = parse_padding(&mut cursor, header)?;
    Ok(DataFrameBegin {
        data_length,
        padding: u16::from(padding) + if frame_has_padding(header) { 1 } else { 0 },
        prefix_length,
    })
}

/// Verifies the currently available suffix of an incrementally parsed DATA frame.
pub fn parse_data_end(
    input: &BufQueue,
    buffer_length: usize,
    pending_padding_bytes: usize,
) -> Result<usize, FrameError> {
    parse_data_end_cursor(
        &mut SegmentCursor::from_queue(input),
        buffer_length,
        pending_padding_bytes,
    )
}

pub(crate) fn parse_data_end_cursor(
    cursor: &mut SegmentCursor<'_>,
    buffer_length: usize,
    pending_padding_bytes: usize,
) -> Result<usize, FrameError> {
    let to_skip = pending_padding_bytes.min(buffer_length);
    cursor.require(to_skip)?;
    cursor.skip_zeroes(to_skip)?;
    Ok(to_skip)
}

/// Parses a HEADERS payload and returns only its encoded header block.
pub fn parse_headers(input: &BufQueue, header: &FrameHeader) -> Result<BufQueue, FrameError> {
    let mut block = parse_headers_cursor(SegmentCursor::from_queue(input), header)?;
    block.read_queue(block.remaining())
}

pub(crate) fn parse_headers_cursor<'a>(
    input: SegmentCursor<'a>,
    header: &FrameHeader,
) -> Result<SegmentCursor<'a>, FrameError> {
    if header.stream == 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    let mut cursor = input.bounded(header.length as usize)?;
    let (padding, mut header_length) = parse_padding(&mut cursor, header)?;
    if header.flags & Flags::PRIORITY != 0 {
        if header_length < FRAME_PRIORITY_SIZE {
            return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
        }
        cursor.advance(FRAME_PRIORITY_SIZE)?;
        header_length -= FRAME_PRIORITY_SIZE;
    }
    let header_block = cursor.take(header_length)?;
    cursor.skip_zeroes(usize::from(padding))?;
    Ok(header_block)
}

/// Parses an RFC 9218 PRIORITY_UPDATE payload.
pub fn parse_rfc9218_priority(
    input: &BufQueue,
    header: &FrameHeader,
) -> Result<(u32, Vec<u8>), FrameError> {
    parse_rfc9218_priority_cursor(SegmentCursor::from_queue(input), header)
}

pub(crate) fn parse_rfc9218_priority_cursor(
    input: SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<(u32, Vec<u8>), FrameError> {
    if header.stream != 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    if header.length <= 4 {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    let mut cursor = input.bounded(header.length as usize)?;
    let stream = parse_uint31(&mut cursor)?;
    let priority = cursor.read_vec(header.length as usize - 4)?;
    Ok((stream, priority))
}

/// Parses an RST_STREAM payload.
pub fn parse_rst_stream(input: &BufQueue, header: &FrameHeader) -> Result<H2ErrorCode, FrameError> {
    parse_rst_stream_cursor(SegmentCursor::from_queue(input), header)
}

pub(crate) fn parse_rst_stream_cursor(
    input: SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<H2ErrorCode, FrameError> {
    if header.length as usize != FRAME_RST_STREAM_SIZE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    if header.stream == 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    parse_error_code(&mut input.bounded(header.length as usize)?)
}

/// Parses a SETTINGS payload without filtering unknown identifiers.
pub fn parse_settings(input: &BufQueue, header: &FrameHeader) -> Result<Vec<Setting>, FrameError> {
    parse_settings_cursor(SegmentCursor::from_queue(input), header)
}

pub(crate) fn parse_settings_cursor(
    input: SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<Vec<Setting>, FrameError> {
    if header.stream != 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    if header.flags & Flags::ACK != 0 {
        if header.length != 0 {
            return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
        }
        return Ok(Vec::new());
    }
    if !header.length.is_multiple_of(6) {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    let mut cursor = input.bounded(header.length as usize)?;
    let mut settings = Vec::with_capacity(header.length as usize / 6);
    while cursor.remaining() != 0 {
        settings.push(Setting {
            identifier: cursor.read_u16()?,
            value: cursor.read_u32()?,
        });
    }
    Ok(settings)
}

/// Parses a PUSH_PROMISE payload.
pub fn parse_push_promise(
    input: &BufQueue,
    header: &FrameHeader,
) -> Result<(u32, BufQueue), FrameError> {
    let (promised_stream, mut block) =
        parse_push_promise_cursor(SegmentCursor::from_queue(input), header)?;
    Ok((promised_stream, block.read_queue(block.remaining())?))
}

pub(crate) fn parse_push_promise_cursor<'a>(
    input: SegmentCursor<'a>,
    header: &FrameHeader,
) -> Result<(u32, SegmentCursor<'a>), FrameError> {
    if header.stream == 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    let mut cursor = input.bounded(header.length as usize)?;
    let (padding, mut header_length) = parse_padding(&mut cursor, header)?;
    if header_length < FRAME_PUSH_PROMISE_SIZE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    header_length -= FRAME_PUSH_PROMISE_SIZE;
    let promised_stream = parse_uint31(&mut cursor)?;
    if promised_stream == 0 || promised_stream & 1 != 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    if header_length < usize::from(padding) {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    let header_block = cursor.take(header_length)?;
    cursor.skip_zeroes(usize::from(padding))?;
    Ok((promised_stream, header_block))
}

/// Parses a PING payload, preserving Proxygen's opaque host-order `u64` API.
pub fn parse_ping(input: &BufQueue, header: &FrameHeader) -> Result<u64, FrameError> {
    parse_ping_cursor(SegmentCursor::from_queue(input), header)
}

pub(crate) fn parse_ping_cursor(
    input: SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<u64, FrameError> {
    if header.length as usize != FRAME_PING_SIZE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    if header.stream != 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    input.bounded(header.length as usize)?.read_u64_native()
}

/// Parses a GOAWAY payload.
pub fn parse_goaway(
    input: &BufQueue,
    header: &FrameHeader,
) -> Result<(u32, H2ErrorCode, BufQueue), FrameError> {
    parse_goaway_cursor(SegmentCursor::from_queue(input), header)
}

pub(crate) fn parse_goaway_cursor(
    input: SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<(u32, H2ErrorCode, BufQueue), FrameError> {
    if (header.length as usize) < FRAME_GOAWAY_SIZE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    if header.stream != 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    let mut cursor = input.bounded(header.length as usize)?;
    let last_stream = parse_uint31(&mut cursor)?;
    let error_code = parse_error_code(&mut cursor)?;
    let debug_data = cursor.read_queue(header.length as usize - FRAME_GOAWAY_SIZE)?;
    Ok((last_stream, error_code, debug_data))
}

/// Parses a WINDOW_UPDATE payload, ignoring its reserved high bit.
pub fn parse_window_update(input: &BufQueue, header: &FrameHeader) -> Result<u32, FrameError> {
    parse_window_update_cursor(SegmentCursor::from_queue(input), header)
}

pub(crate) fn parse_window_update_cursor(
    input: SegmentCursor<'_>,
    header: &FrameHeader,
) -> Result<u32, FrameError> {
    if header.length as usize != FRAME_WINDOW_UPDATE_SIZE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    parse_uint31(&mut input.bounded(header.length as usize)?)
}

/// Parses a CONTINUATION payload.
pub fn parse_continuation(input: &BufQueue, header: &FrameHeader) -> Result<BufQueue, FrameError> {
    let mut block = parse_continuation_cursor(SegmentCursor::from_queue(input), header)?;
    block.read_queue(block.remaining())
}

pub(crate) fn parse_continuation_cursor<'a>(
    input: SegmentCursor<'a>,
    header: &FrameHeader,
) -> Result<SegmentCursor<'a>, FrameError> {
    if header.stream == 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    let length = header.length as usize;
    input.bounded(length)
}

/// Parses Proxygen's Alt-Svc extension payload.
pub fn parse_alt_svc(input: &BufQueue, header: &FrameHeader) -> Result<AltSvcFrame, FrameError> {
    if (header.length as usize) < FRAME_ALT_SVC_SIZE_BASE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    let mut cursor = payload_cursor(input, header)?;
    let max_age = cursor.read_u32()?;
    let port = cursor.read_u16()?;
    let protocol_length = usize::from(cursor.read_u8()?);
    if (header.length as usize) < FRAME_ALT_SVC_SIZE_BASE + protocol_length {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    let protocol = cursor.read_vec(protocol_length)?;
    let host_length = usize::from(cursor.read_u8()?);
    if (header.length as usize) < FRAME_ALT_SVC_SIZE_BASE + protocol_length + host_length {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    let host = cursor.read_vec(host_length)?;
    let origin = cursor.read_vec(
        header.length as usize - FRAME_ALT_SVC_SIZE_BASE - protocol_length - host_length,
    )?;
    Ok(AltSvcFrame {
        max_age,
        port,
        protocol,
        host,
        origin,
    })
}

/// Parses a secondary-authentication CERTIFICATE_REQUEST payload.
pub fn parse_certificate_request(
    input: &BufQueue,
    header: &FrameHeader,
) -> Result<CertificateRequestFrame, FrameError> {
    if (header.length as usize) < FRAME_CERTIFICATE_REQUEST_SIZE_BASE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    if header.stream != 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    let mut cursor = payload_cursor(input, header)?;
    let request_id = cursor.read_u16()?;
    let authenticator_request =
        cursor.read_queue(header.length as usize - FRAME_CERTIFICATE_REQUEST_SIZE_BASE)?;
    Ok(CertificateRequestFrame {
        request_id,
        authenticator_request,
    })
}

/// Parses a secondary-authentication CERTIFICATE payload.
pub fn parse_certificate(
    input: &BufQueue,
    header: &FrameHeader,
) -> Result<CertificateFrame, FrameError> {
    if (header.length as usize) < FRAME_CERTIFICATE_SIZE_BASE {
        return Err(peer(H2ErrorCode::FRAME_SIZE_ERROR));
    }
    if header.stream != 0 {
        return Err(peer(H2ErrorCode::PROTOCOL_ERROR));
    }
    let mut cursor = payload_cursor(input, header)?;
    let certificate_id = cursor.read_u16()?;
    let authenticator = cursor.read_queue(header.length as usize - FRAME_CERTIFICATE_SIZE_BASE)?;
    Ok(CertificateFrame {
        certificate_id,
        authenticator,
    })
}

fn payload_length(length: usize, padding: Option<u8>) -> Result<u32, FrameError> {
    let length = length
        .checked_add(padding.map_or(0, |amount| usize::from(amount) + 1))
        .ok_or(FrameError::InvalidInput("frame payload length overflow"))?;
    if length > MAX_FRAME_PAYLOAD_LENGTH as usize {
        return Err(FrameError::InvalidInput(
            "frame payload exceeds the 24-bit length field",
        ));
    }
    Ok(length as u32)
}

pub(crate) fn validate_stream(stream: u32, allow_zero: bool) -> Result<(), FrameError> {
    if stream > MAX_STREAM_ID {
        return Err(FrameError::InvalidInput(
            "stream identifier uses the reserved high bit",
        ));
    }
    if !allow_zero && stream == 0 {
        return Err(FrameError::InvalidInput(
            "frame requires a nonzero stream identifier",
        ));
    }
    Ok(())
}

fn frame_header(
    length: usize,
    frame_type: FrameType,
    mut flags: u8,
    stream: u32,
    padding: Option<u8>,
) -> Result<FrameHeader, FrameError> {
    if !is_valid_frame_type(frame_type) {
        return Err(FrameError::InvalidInput("unrecognized local frame type"));
    }
    validate_stream(stream, true)?;
    let wire_length = payload_length(length, padding)?;
    if padding.is_some() {
        flags |= Flags::PADDED;
    } else {
        flags &= !Flags::PADDED;
    }
    Ok(FrameHeader {
        length: wire_length,
        stream,
        frame_type,
        flags,
    })
}

fn serialize_frame_header(header: FrameHeader) -> [u8; FRAME_HEADER_SIZE] {
    let stream = header.stream.to_be_bytes();
    [
        (header.length >> 16) as u8,
        (header.length >> 8) as u8,
        header.length as u8,
        header.frame_type.as_u8(),
        header.flags,
        stream[0],
        stream[1],
        stream[2],
        stream[3],
    ]
}

pub(crate) fn frame_header_prefix(
    length: usize,
    frame_type: FrameType,
    flags: u8,
    stream: u32,
) -> Result<[u8; FRAME_HEADER_SIZE], FrameError> {
    Ok(serialize_frame_header(frame_header(
        length, frame_type, flags, stream, None,
    )?))
}

fn append_frame_header_bytes(output: &mut Vec<u8>, header: FrameHeader, padding: Option<u8>) {
    output.extend_from_slice(&serialize_frame_header(header));
    if let Some(padding) = padding {
        output.push(padding);
    }
}

fn frame_header_bytes(
    length: usize,
    frame_type: FrameType,
    flags: u8,
    stream: u32,
    padding: Option<u8>,
) -> Result<(Vec<u8>, u32), FrameError> {
    let header = frame_header(length, frame_type, flags, stream, padding)?;
    let mut bytes = Vec::with_capacity(FRAME_HEADER_SIZE + usize::from(padding.is_some() as u8));
    append_frame_header_bytes(&mut bytes, header, padding);
    Ok((bytes, header.length))
}

fn ensure_output_capacity(output: &BufQueue, length: usize) -> Result<(), FrameError> {
    if output.remaining_capacity() < length {
        return Err(FrameError::Buffer(BufQueueError::LimitExceeded {
            limit: output.limit(),
            current: output.chain_length(),
            incoming: length,
        }));
    }
    Ok(())
}

fn push_bytes(output: &mut BufQueue, bytes: impl AsRef<[u8]>) -> Result<(), FrameError> {
    output.push_copy(bytes.as_ref()).map_err(FrameError::from)
}

fn append_zero_padding(output: &mut BufQueue, padding: Option<u8>) -> Result<(), FrameError> {
    if let Some(amount) = padding.filter(|amount| *amount != 0) {
        output.push_static(&[0; 255][..usize::from(amount)])?;
    }
    Ok(())
}

fn write_small_frame(
    output: &mut BufQueue,
    frame_type: FrameType,
    flags: u8,
    stream: u32,
    payload: &[u8],
) -> Result<usize, FrameError> {
    let header = frame_header(payload.len(), frame_type, flags, stream, None)?;
    let total = FRAME_HEADER_SIZE + header.length as usize;
    ensure_output_capacity(output, total)?;
    let mut frame = Vec::with_capacity(total);
    append_frame_header_bytes(&mut frame, header, None);
    frame.extend_from_slice(payload);
    output.push_vec(frame)?;
    Ok(total)
}

/// Writes a DATA frame while preserving the backing of every payload segment.
///
/// `reuse_payload_headroom` is retained for API compatibility with Proxygen.
/// [`Bytes`] cannot safely expose mutable headroom. Both values append unpadded
/// metadata to a unique output tail with room, or use a separate allocation.
pub fn write_data(
    output: &mut BufQueue,
    data: BufQueue,
    stream: u32,
    padding: Option<u8>,
    end_stream: bool,
    _reuse_payload_headroom: bool,
) -> Result<usize, FrameError> {
    write_data_with(output, data, stream, padding, end_stream, None)
}

/// Bytes of one [`DataHeaderArena`] allocation.
pub(crate) const DATA_HEADER_CHUNK: usize = 32 * FRAME_HEADER_SIZE;

/// Backing for DATA frame headers that precede payloads whose own backing is
/// already shared. Each header is still its own 9-byte segment, but up to
/// [`DATA_HEADER_CHUNK`] bytes of headers share one allocation instead of one
/// vector each. A chunk is released after its last header.
#[derive(Default)]
pub(crate) struct DataHeaderArena(BytesMut);

impl DataHeaderArena {
    fn header(&mut self, header: &[u8; FRAME_HEADER_SIZE]) -> Bytes {
        if self.0.capacity() < FRAME_HEADER_SIZE {
            self.0 = BytesMut::with_capacity(DATA_HEADER_CHUNK);
        }
        self.0.extend_from_slice(header);
        self.0.split().freeze()
    }
}

/// The arena, only when `payload` starts with a segment that is not unique.
///
/// The header is never unique then either, so the payload alone already decides
/// whether exports and tail appends can reuse the segments around the header.
fn header_arena<'a>(
    arena: Option<&'a mut DataHeaderArena>,
    payload: &BufQueue,
) -> Option<&'a mut DataHeaderArena> {
    arena.filter(|_| payload.front().is_some_and(|front| !front.is_unique()))
}

pub(crate) fn write_data_with(
    output: &mut BufQueue,
    mut data: BufQueue,
    stream: u32,
    padding: Option<u8>,
    end_stream: bool,
    headers: Option<&mut DataHeaderArena>,
) -> Result<usize, FrameError> {
    let headers = header_arena(headers, &data);
    let total = write_data_header(
        output,
        data.chain_length(),
        stream,
        padding,
        end_stream,
        headers,
    )?;
    output.append(&mut data)?;
    append_zero_padding(output, padding)?;
    Ok(total)
}

/// Writes a DATA frame without END_STREAM around the first `length` bytes of
/// `data`, moving those segments straight into `output`.
///
/// On failure the frame's payload is dropped before returning, as
/// [`write_data`] drops its payload, and the rest of `data` stays queued.
pub(crate) fn write_data_from_with(
    output: &mut BufQueue,
    data: &mut BufQueue,
    length: usize,
    stream: u32,
    padding: Option<u8>,
    headers: Option<&mut DataHeaderArena>,
) -> Result<usize, FrameError> {
    let length = length.min(data.chain_length());
    let headers = header_arena(headers, data).filter(|_| length != 0);
    let moved =
        write_data_header(output, length, stream, padding, false, headers).and_then(|total| {
            output.append_prefix_from(data, length)?;
            Ok(total)
        });
    let total = match moved {
        Ok(total) => total,
        Err(error) => {
            drop(data.split_at_most(length));
            return Err(error);
        }
    };
    append_zero_padding(output, padding)?;
    Ok(total)
}

/// Segment capacity that stages `body` as DATA frames of at most `max_frame`
/// payload bytes: each frame adds a header, at most one split boundary and at
/// most one padding segment. Zero when the frames exceed `capacity`, so a write
/// that is going to fail reserves nothing.
pub(crate) fn data_frames_segment_bound(
    body: &BufQueue,
    max_frame: usize,
    padding: Option<u8>,
    capacity: usize,
) -> usize {
    if max_frame == 0 {
        return 0;
    }
    let length = body.chain_length();
    let frames = length.div_ceil(max_frame);
    let overhead = FRAME_HEADER_SIZE + padding.map_or(0, |amount| usize::from(amount) + 1);
    let fits = frames
        .checked_mul(overhead)
        .and_then(|metadata| metadata.checked_add(length))
        .is_some_and(|total| total <= capacity);
    if !fits {
        return 0;
    }
    body.segment_count() + frames * (2 + usize::from(padding.is_some_and(|amount| amount != 0)))
}

fn write_data_header(
    output: &mut BufQueue,
    data_length: usize,
    stream: u32,
    padding: Option<u8>,
    end_stream: bool,
    headers: Option<&mut DataHeaderArena>,
) -> Result<usize, FrameError> {
    validate_stream(stream, false)?;
    let flags = if end_stream { Flags::END_STREAM } else { 0 };
    if padding.is_none() {
        let header = frame_header_prefix(data_length, FrameType::DATA, flags, stream)?;
        let total = FRAME_HEADER_SIZE + data_length;
        ensure_output_capacity(output, total)?;
        if !output.try_append_copy_to_tail(&header)? {
            match headers {
                // Recorded as the vector it replaces, so compact-tail and
                // backing decisions downstream stay the same.
                Some(arena) => {
                    output.push_bytes_with_backing(arena.header(&header), FRAME_HEADER_SIZE)?
                }
                None => output.push_vec(header.to_vec())?,
            }
        }
        Ok(total)
    } else {
        let (header, wire_length) =
            frame_header_bytes(data_length, FrameType::DATA, flags, stream, padding)?;
        let total = FRAME_HEADER_SIZE + wire_length as usize;
        ensure_output_capacity(output, total)?;
        output.push_vec(header)?;
        Ok(total)
    }
}

pub(crate) fn write_compact_data(
    output: &mut BufQueue,
    data: BufQueue,
    stream: u32,
    end_stream: bool,
) -> Result<usize, FrameError> {
    write_compact_data_with(output, data, stream, end_stream, None)
}

pub(crate) fn write_compact_data_with(
    output: &mut BufQueue,
    data: BufQueue,
    stream: u32,
    end_stream: bool,
    headers: Option<&mut DataHeaderArena>,
) -> Result<usize, FrameError> {
    if data.chain_length() > COMPACT_WRITE_LIMIT - FRAME_HEADER_SIZE {
        return write_data_with(output, data, stream, None, end_stream, headers);
    }
    validate_stream(stream, false)?;
    let flags = if end_stream { Flags::END_STREAM } else { 0 };
    let header = frame_header_prefix(data.chain_length(), FrameType::DATA, flags, stream)?;
    let total = FRAME_HEADER_SIZE + data.chain_length();
    ensure_output_capacity(output, total)?;
    if !output.try_append_compact_to_tail(&header, &data)? {
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(&header);
        data.segments()
            .for_each(|segment| bytes.extend_from_slice(segment));
        output.push_vec(bytes)?;
    }
    Ok(total)
}

/// Writes a HEADERS frame around an already encoded header block.
pub fn write_headers(
    output: &mut BufQueue,
    mut header_block: BufQueue,
    stream: u32,
    padding: Option<u8>,
    end_stream: bool,
    end_headers: bool,
) -> Result<usize, FrameError> {
    validate_stream(stream, false)?;
    let mut flags = 0;
    if end_stream {
        flags |= Flags::END_STREAM;
    }
    if end_headers {
        flags |= Flags::END_HEADERS;
    }
    let (header, wire_length) = frame_header_bytes(
        header_block.chain_length(),
        FrameType::HEADERS,
        flags,
        stream,
        padding,
    )?;
    let total = FRAME_HEADER_SIZE + wire_length as usize;
    ensure_output_capacity(output, total)?;
    output.push_vec(header)?;
    output.append(&mut header_block)?;
    append_zero_padding(output, padding)?;
    Ok(total)
}

/// Writes an RFC 9218 PRIORITY_UPDATE frame.
pub fn write_rfc9218_priority(
    output: &mut BufQueue,
    stream: u32,
    priority: impl AsRef<[u8]>,
) -> Result<usize, FrameError> {
    validate_stream(stream, false)?;
    let priority = priority.as_ref();
    let mut payload = Vec::with_capacity(4 + priority.len());
    payload.extend_from_slice(&stream.to_be_bytes());
    payload.extend_from_slice(priority);
    write_small_frame(output, FrameType::RFC9218_PRIORITY, 0, 0, &payload)
}

/// Writes an RST_STREAM frame.
pub fn write_rst_stream(
    output: &mut BufQueue,
    stream: u32,
    error_code: H2ErrorCode,
) -> Result<usize, FrameError> {
    validate_stream(stream, false)?;
    write_small_frame(
        output,
        FrameType::RST_STREAM,
        0,
        stream,
        &error_code.as_u32().to_be_bytes(),
    )
}

/// Writes a SETTINGS frame in caller-provided order.
pub fn write_settings(output: &mut BufQueue, settings: &[Setting]) -> Result<usize, FrameError> {
    let payload_size = settings
        .len()
        .checked_mul(6)
        .ok_or(FrameError::InvalidInput("settings payload length overflow"))?;
    if payload_size > MAX_FRAME_PAYLOAD_LENGTH as usize {
        return Err(FrameError::InvalidInput("settings frame is too large"));
    }
    let mut payload = Vec::with_capacity(payload_size);
    for setting in settings {
        payload.extend_from_slice(&setting.identifier.to_be_bytes());
        payload.extend_from_slice(&setting.value.to_be_bytes());
    }
    write_small_frame(output, FrameType::SETTINGS, 0, 0, &payload)
}

/// Writes an empty SETTINGS acknowledgement.
pub fn write_settings_ack(output: &mut BufQueue) -> Result<usize, FrameError> {
    write_small_frame(output, FrameType::SETTINGS, Flags::ACK, 0, &[])
}

pub(crate) fn validate_push_promise_streams(
    associated_stream: u32,
    promised_stream: u32,
) -> Result<(), FrameError> {
    validate_stream(associated_stream, false)?;
    validate_stream(promised_stream, false)?;
    if associated_stream & 1 == 0 {
        return Err(FrameError::InvalidInput(
            "associated PUSH_PROMISE stream must be odd",
        ));
    }
    if promised_stream & 1 != 0 {
        return Err(FrameError::InvalidInput(
            "promised PUSH_PROMISE stream must be even",
        ));
    }
    Ok(())
}

/// Writes a PUSH_PROMISE frame around an encoded header block.
pub fn write_push_promise(
    output: &mut BufQueue,
    associated_stream: u32,
    promised_stream: u32,
    mut header_block: BufQueue,
    padding: Option<u8>,
    end_headers: bool,
) -> Result<usize, FrameError> {
    validate_push_promise_streams(associated_stream, promised_stream)?;
    let block_length = header_block.chain_length();
    let unpadded_length = FRAME_PUSH_PROMISE_SIZE
        .checked_add(block_length)
        .ok_or(FrameError::InvalidInput("push-promise length overflow"))?;
    let flags = if end_headers { Flags::END_HEADERS } else { 0 };
    let (header, wire_length) = frame_header_bytes(
        unpadded_length,
        FrameType::PUSH_PROMISE,
        flags,
        associated_stream,
        padding,
    )?;
    let total = FRAME_HEADER_SIZE + wire_length as usize;
    ensure_output_capacity(output, total)?;
    output.push_vec(header)?;
    push_bytes(output, promised_stream.to_be_bytes())?;
    output.append(&mut header_block)?;
    append_zero_padding(output, padding)?;
    Ok(total)
}

/// Writes a PING request or acknowledgement.
pub fn write_ping(output: &mut BufQueue, opaque_data: u64, ack: bool) -> Result<usize, FrameError> {
    write_small_frame(
        output,
        FrameType::PING,
        if ack { Flags::ACK } else { 0 },
        0,
        &opaque_data.to_ne_bytes(),
    )
}

/// Writes a GOAWAY frame, moving optional debug-data segments into `output`.
pub fn write_goaway(
    output: &mut BufQueue,
    last_stream: u32,
    error_code: H2ErrorCode,
    debug_data: Option<BufQueue>,
) -> Result<usize, FrameError> {
    validate_stream(last_stream, true)?;
    let debug_length = debug_data.as_ref().map_or(0, BufQueue::chain_length);
    let payload_length = FRAME_GOAWAY_SIZE
        .checked_add(debug_length)
        .ok_or(FrameError::InvalidInput("GOAWAY payload length overflow"))?;
    if payload_length > MAX_FRAME_PAYLOAD_LENGTH as usize {
        return Err(FrameError::InvalidInput("GOAWAY frame is too large"));
    }
    let total = FRAME_HEADER_SIZE + payload_length;
    ensure_output_capacity(output, total)?;
    let (header, _) = frame_header_bytes(payload_length, FrameType::GOAWAY, 0, 0, None)?;
    output.push_vec(header)?;
    push_bytes(output, last_stream.to_be_bytes())?;
    push_bytes(output, error_code.as_u32().to_be_bytes())?;
    if let Some(mut debug_data) = debug_data {
        output.append(&mut debug_data)?;
    }
    Ok(total)
}

/// Writes a WINDOW_UPDATE frame.
pub fn write_window_update(
    output: &mut BufQueue,
    stream: u32,
    amount: u32,
) -> Result<usize, FrameError> {
    validate_stream(stream, true)?;
    if !(1..=MAX_WINDOW_UPDATE_SIZE).contains(&amount) {
        return Err(FrameError::InvalidInput(
            "WINDOW_UPDATE amount must be in 1..=2^31-1",
        ));
    }
    write_small_frame(
        output,
        FrameType::WINDOW_UPDATE,
        0,
        stream,
        &amount.to_be_bytes(),
    )
}

/// Writes a CONTINUATION frame.
pub fn write_continuation(
    output: &mut BufQueue,
    stream: u32,
    end_headers: bool,
    mut header_block: BufQueue,
) -> Result<usize, FrameError> {
    validate_stream(stream, false)?;
    let length = header_block.chain_length();
    let (header, wire_length) = frame_header_bytes(
        length,
        FrameType::CONTINUATION,
        if end_headers { Flags::END_HEADERS } else { 0 },
        stream,
        None,
    )?;
    let total = FRAME_HEADER_SIZE + wire_length as usize;
    ensure_output_capacity(output, total)?;
    output.push_vec(header)?;
    output.append(&mut header_block)?;
    Ok(total)
}

/// Writes Proxygen's Alt-Svc extension frame.
pub fn write_alt_svc(
    output: &mut BufQueue,
    stream: u32,
    max_age: u32,
    port: u16,
    protocol: impl AsRef<[u8]>,
    host: impl AsRef<[u8]>,
    origin: impl AsRef<[u8]>,
) -> Result<usize, FrameError> {
    validate_stream(stream, true)?;
    let protocol = protocol.as_ref();
    let host = host.as_ref();
    let origin = origin.as_ref();
    let protocol_length = protocol.len() as u8;
    let host_length = host.len() as u8;
    let payload_length = FRAME_ALT_SVC_SIZE_BASE
        .checked_add(protocol.len())
        .and_then(|length| length.checked_add(host.len()))
        .and_then(|length| length.checked_add(origin.len()))
        .ok_or(FrameError::InvalidInput("Alt-Svc payload length overflow"))?;
    if payload_length > MAX_FRAME_PAYLOAD_LENGTH as usize {
        return Err(FrameError::InvalidInput("Alt-Svc frame is too large"));
    }
    let mut payload = Vec::with_capacity(payload_length);
    payload.extend_from_slice(&max_age.to_be_bytes());
    payload.extend_from_slice(&port.to_be_bytes());
    payload.push(protocol_length);
    payload.extend_from_slice(protocol);
    payload.push(host_length);
    payload.extend_from_slice(host);
    payload.extend_from_slice(origin);
    write_small_frame(output, FrameType::ALTSVC, 0, stream, &payload)
}

/// Writes a secondary-authentication CERTIFICATE_REQUEST frame, moving the
/// optional authenticator-request segments into `output`.
pub fn write_certificate_request(
    output: &mut BufQueue,
    request_id: u16,
    authenticator_request: Option<BufQueue>,
) -> Result<usize, FrameError> {
    let authenticator_length = authenticator_request
        .as_ref()
        .map_or(0, BufQueue::chain_length);
    let payload_length = FRAME_CERTIFICATE_REQUEST_SIZE_BASE
        .checked_add(authenticator_length)
        .ok_or(FrameError::InvalidInput(
            "certificate request length overflow",
        ))?;
    if payload_length > MAX_FRAME_PAYLOAD_LENGTH as usize {
        return Err(FrameError::InvalidInput(
            "certificate request frame is too large",
        ));
    }
    let total = FRAME_HEADER_SIZE + payload_length;
    ensure_output_capacity(output, total)?;
    let (header, _) =
        frame_header_bytes(payload_length, FrameType::CERTIFICATE_REQUEST, 0, 0, None)?;
    output.push_vec(header)?;
    push_bytes(output, request_id.to_be_bytes())?;
    if let Some(mut authenticator_request) = authenticator_request {
        output.append(&mut authenticator_request)?;
    }
    Ok(total)
}

/// Writes a secondary-authentication CERTIFICATE frame, moving the optional
/// authenticator segments into `output`.
pub fn write_certificate(
    output: &mut BufQueue,
    certificate_id: u16,
    authenticator: Option<BufQueue>,
    to_be_continued: bool,
) -> Result<usize, FrameError> {
    let authenticator_length = authenticator.as_ref().map_or(0, BufQueue::chain_length);
    let payload_length = FRAME_CERTIFICATE_SIZE_BASE
        .checked_add(authenticator_length)
        .ok_or(FrameError::InvalidInput("certificate length overflow"))?;
    if payload_length > MAX_FRAME_PAYLOAD_LENGTH as usize {
        return Err(FrameError::InvalidInput("certificate frame is too large"));
    }
    let total = FRAME_HEADER_SIZE + payload_length;
    ensure_output_capacity(output, total)?;
    let (header, _) = frame_header_bytes(
        payload_length,
        FrameType::CERTIFICATE,
        if to_be_continued {
            Flags::TO_BE_CONTINUED
        } else {
            0
        },
        0,
        None,
    )?;
    output.push_vec(header)?;
    push_bytes(output, certificate_id.to_be_bytes())?;
    if let Some(mut authenticator) = authenticator {
        output.append(&mut authenticator)?;
    }
    Ok(total)
}

/// Writes a reserved PADDING frame whose total size is at least one frame header.
pub fn write_padding(
    output: &mut BufQueue,
    stream: u32,
    requested_size: u16,
) -> Result<usize, FrameError> {
    validate_stream(stream, true)?;
    let body_length = usize::from(requested_size).saturating_sub(FRAME_HEADER_SIZE);
    let total = FRAME_HEADER_SIZE + body_length;
    ensure_output_capacity(output, total)?;
    let (header, _) = frame_header_bytes(body_length, FrameType::PADDING, 0, stream, None)?;
    output.push_vec(header)?;
    if body_length != 0 {
        push_bytes(output, vec![0; body_length])?;
    }
    Ok(total)
}

/// Parses and removes one complete frame, leaving incomplete input untouched.
pub fn parse_frame(input: &mut BufQueue) -> Result<Option<ParsedFrame>, FrameError> {
    if input.chain_length() < FRAME_HEADER_SIZE {
        return Ok(None);
    }
    let header = parse_frame_header(input)?;
    let consumed = FRAME_HEADER_SIZE
        .checked_add(header.length as usize)
        .ok_or(FrameError::InvalidInput("frame length overflow"))?;
    if input.chain_length() < consumed {
        return Ok(None);
    }
    let mut complete = input.split_at_most(consumed);
    complete.trim_start(FRAME_HEADER_SIZE)?;
    let frame = match header.frame_type {
        FrameType::DATA => Frame::Data(parse_data(&complete, &header)?),
        FrameType::HEADERS => Frame::Headers(parse_headers(&complete, &header)?),
        FrameType::PRIORITY => Frame::Priority(complete),
        FrameType::RST_STREAM => Frame::RstStream(parse_rst_stream(&complete, &header)?),
        FrameType::SETTINGS => Frame::Settings(parse_settings(&complete, &header)?),
        FrameType::PUSH_PROMISE => {
            let (promised_stream, header_block) = parse_push_promise(&complete, &header)?;
            Frame::PushPromise {
                promised_stream,
                header_block,
            }
        }
        FrameType::PING => Frame::Ping(parse_ping(&complete, &header)?),
        FrameType::GOAWAY => {
            let (last_stream, error_code, debug_data) = parse_goaway(&complete, &header)?;
            Frame::Goaway {
                last_stream,
                error_code,
                debug_data,
            }
        }
        FrameType::WINDOW_UPDATE => Frame::WindowUpdate(parse_window_update(&complete, &header)?),
        FrameType::CONTINUATION => Frame::Continuation(parse_continuation(&complete, &header)?),
        FrameType::ALTSVC => Frame::AltSvc(parse_alt_svc(&complete, &header)?),
        FrameType::RFC9218_PRIORITY => {
            let (prioritized_stream, priority) = parse_rfc9218_priority(&complete, &header)?;
            Frame::Rfc9218Priority {
                prioritized_stream,
                priority,
            }
        }
        FrameType::PADDING => Frame::Padding(complete),
        FrameType::CERTIFICATE_REQUEST => {
            Frame::CertificateRequest(parse_certificate_request(&complete, &header)?)
        }
        FrameType::CERTIFICATE => Frame::Certificate(parse_certificate(&complete, &header)?),
        _ => Frame::Unknown(complete),
    };
    Ok(Some(ParsedFrame {
        header,
        frame,
        consumed,
    }))
}

/// Bounded incremental decoder for arbitrarily segmented input.
pub struct FrameDecoder {
    pending: BufQueue,
}

impl FrameDecoder {
    pub fn new(limit: usize) -> Self {
        Self {
            pending: BufQueue::new(limit),
        }
    }

    pub fn push_bytes(&mut self, bytes: Bytes) -> Result<(), FrameError> {
        self.pending.push_bytes(bytes)?;
        Ok(())
    }

    pub fn push_queue(&mut self, input: &mut BufQueue) -> Result<(), FrameError> {
        self.pending.append(input)?;
        Ok(())
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending.chain_length()
    }

    pub fn next_frame(&mut self) -> Result<Option<ParsedFrame>, FrameError> {
        parse_frame(&mut self.pending)
    }
}

#[cfg(test)]
mod data_header_tests {
    use super::*;

    struct Owner(Vec<u8>);

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    fn owned_payload(length: usize, fill: u8) -> BufQueue {
        BufQueue::from_bytes(Bytes::from_owner(Owner(vec![fill; length])), usize::MAX).unwrap()
    }

    fn shape(output: &BufQueue) -> (Vec<u8>, Vec<usize>, Option<usize>) {
        let bytes = output
            .segments()
            .flat_map(|segment| segment.to_vec())
            .collect();
        let lengths = output.segments().map(|segment| segment.len()).collect();
        (bytes, lengths, output.backing_capacity_upper_bound())
    }

    fn frames(
        headers: Option<&mut DataHeaderArena>,
        payload: impl Fn(usize) -> BufQueue,
        count: usize,
    ) -> BufQueue {
        let mut headers = headers;
        let mut output = BufQueue::new(usize::MAX);
        for index in 0..count {
            let stream = 1 + 2 * (index as u32 % 3);
            let mut body = payload(index);
            let length = body.chain_length() / 2;
            write_data_from_with(
                &mut output,
                &mut body,
                length,
                stream,
                None,
                headers.as_deref_mut(),
            )
            .unwrap();
            write_data_with(
                &mut output,
                body,
                stream,
                None,
                index % 2 == 0,
                headers.as_deref_mut(),
            )
            .unwrap();
        }
        output
    }

    #[test]
    fn arena_headers_match_vector_headers_byte_for_byte() {
        let payload = |index: usize| owned_payload(4096 + 37 * index, index as u8);
        let count = 2 * DATA_HEADER_CHUNK / FRAME_HEADER_SIZE + 3;
        let expected = frames(None, payload, count);
        let mut headers = DataHeaderArena::default();
        let actual = frames(Some(&mut headers), payload, count);
        assert_eq!(shape(&actual), shape(&expected));
    }

    #[test]
    fn headers_before_shared_payloads_share_one_chunk() {
        let mut headers = DataHeaderArena::default();
        let output = frames(Some(&mut headers), |_| owned_payload(4096, 1), 3);
        let header_segments: Vec<&Bytes> = output.segments().step_by(2).collect();
        assert_eq!(header_segments.len(), 6);
        for pair in header_segments.windows(2) {
            assert_eq!(pair[0].len(), FRAME_HEADER_SIZE);
            assert!(!pair[0].is_unique());
            assert_eq!(
                pair[0].as_ptr().wrapping_add(FRAME_HEADER_SIZE),
                pair[1].as_ptr()
            );
        }
    }

    #[test]
    fn headers_before_unique_or_empty_payloads_keep_their_own_vectors() {
        let mut headers = DataHeaderArena::default();
        let mut unique = BufQueue::new(usize::MAX);
        for stream in [1, 3] {
            let body = BufQueue::from_vec(vec![3; 4096], usize::MAX).unwrap();
            write_data_with(&mut unique, body, stream, None, false, Some(&mut headers)).unwrap();
        }
        assert!(unique.segments().step_by(2).all(Bytes::is_unique));
        let mut empty = BufQueue::new(usize::MAX);
        write_data_with(
            &mut empty,
            BufQueue::new(0),
            1,
            None,
            true,
            Some(&mut headers),
        )
        .unwrap();
        assert!(empty.front().unwrap().is_unique());
        assert_eq!(
            empty.backing_capacity_upper_bound(),
            Some(FRAME_HEADER_SIZE)
        );
    }

    #[test]
    fn arena_headers_outlive_their_arena_in_any_drop_order() {
        let mut headers = DataHeaderArena::default();
        let count = DATA_HEADER_CHUNK / FRAME_HEADER_SIZE + 2;
        let output = frames(Some(&mut headers), |_| owned_payload(64, 9), count);
        let expected = shape(&frames(None, |_| owned_payload(64, 9), count));
        drop(headers);
        let mut segments: Vec<Bytes> = output.into_segment_iter().collect();
        let bytes: Vec<u8> = segments
            .iter()
            .flat_map(|segment| segment.to_vec())
            .collect();
        assert_eq!(bytes, expected.0);
        while !segments.is_empty() {
            let index = (segments.len() * 7 + 3) % segments.len();
            drop(segments.swap_remove(index));
        }
    }
}
