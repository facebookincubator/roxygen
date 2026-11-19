/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::message::HttpMessage;

/// An HTTP/2 wire error code.
///
/// HTTP/2 carries error codes in a 32-bit field. The transparent representation
/// keeps extension and peer-provided values inspectable without constructing an
/// invalid Rust enum discriminant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct H2ErrorCode(u32);

impl H2ErrorCode {
    pub const NO_ERROR: Self = Self(0);
    pub const PROTOCOL_ERROR: Self = Self(1);
    pub const INTERNAL_ERROR: Self = Self(2);
    pub const FLOW_CONTROL_ERROR: Self = Self(3);
    pub const SETTINGS_TIMEOUT: Self = Self(4);
    pub const STREAM_CLOSED: Self = Self(5);
    pub const FRAME_SIZE_ERROR: Self = Self(6);
    pub const REFUSED_STREAM: Self = Self(7);
    pub const CANCEL: Self = Self(8);
    pub const COMPRESSION_ERROR: Self = Self(9);
    pub const CONNECT_ERROR: Self = Self(10);
    pub const ENHANCE_YOUR_CALM: Self = Self(11);
    pub const INADEQUATE_SECURITY: Self = Self(12);
    pub const HTTP_1_1_REQUIRED: Self = Self(13);

    pub const fn from_u32(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for H2ErrorCode {
    fn from(value: u32) -> Self {
        Self::from_u32(value)
    }
}

impl From<H2ErrorCode> for u32 {
    fn from(code: H2ErrorCode) -> Self {
        code.as_u32()
    }
}

/// An error category carried by [`HttpException`].
///
/// The numeric values match Proxygen's `ProxygenError` enum. Only some values
/// have names. Other values remain representable, and
/// [`http_exception_to_http_error_code`] maps them to `PROTOCOL_ERROR`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ProxygenError(u8);

impl ProxygenError {
    pub const NONE: Self = Self(0);
    pub const MESSAGE: Self = Self(1);
    pub const WRITE: Self = Self(5);
    pub const TIMEOUT: Self = Self(6);
    pub const DROPPED: Self = Self(13);
    pub const CONNECTION_RESET: Self = Self(15);
    pub const PARSE_HEADER: Self = Self(16);
    pub const PARSE_BODY: Self = Self(17);
    pub const EOF: Self = Self(18);
    pub const UNKNOWN: Self = Self(20);
    pub const STREAM_ABORT: Self = Self(23);
    pub const STREAM_UNACKNOWLEDGED: Self = Self(24);
    pub const HEADER_CONTENT_VALIDATION: Self = Self(27);
    pub const INGRESS_STATE_TRANSITION: Self = Self(35);
    pub const CANCELED: Self = Self(37);

    pub const fn from_u8(value: u8) -> Self {
        Self(value)
    }

    pub const fn as_u8(self) -> u8 {
        self.0
    }
}

/// The data-flow direction affected by an [`HttpException`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpExceptionDirection {
    Ingress,
    Egress,
    IngressAndEgress,
}

/// An HTTP/1.1 or HTTP/2 codec error, as delivered by
/// [`CodecEvent::Error`](crate::CodecEvent::Error).
#[derive(Clone, Debug)]
pub struct HttpException {
    direction: HttpExceptionDirection,
    message: Box<[u8]>,
    proxygen_error: ProxygenError,
    http_status_code: Option<u32>,
    codec_status_code: Option<H2ErrorCode>,
    partial_message: Option<Box<HttpMessage>>,
}

impl HttpException {
    pub fn new(direction: HttpExceptionDirection, message: impl AsRef<[u8]>) -> Self {
        Self {
            direction,
            message: message.as_ref().to_vec().into_boxed_slice(),
            proxygen_error: ProxygenError::NONE,
            http_status_code: None,
            codec_status_code: None,
            partial_message: None,
        }
    }

    pub const fn direction(&self) -> HttpExceptionDirection {
        self.direction
    }

    pub const fn is_ingress_exception(&self) -> bool {
        matches!(
            self.direction,
            HttpExceptionDirection::Ingress | HttpExceptionDirection::IngressAndEgress
        )
    }

    pub const fn is_egress_exception(&self) -> bool {
        matches!(
            self.direction,
            HttpExceptionDirection::Egress | HttpExceptionDirection::IngressAndEgress
        )
    }

    pub fn message(&self) -> &[u8] {
        &self.message
    }

    pub const fn has_proxygen_error(&self) -> bool {
        self.proxygen_error.as_u8() != ProxygenError::NONE.as_u8()
    }

    pub const fn proxygen_error(&self) -> ProxygenError {
        self.proxygen_error
    }

    pub fn set_proxygen_error(&mut self, error: ProxygenError) {
        self.proxygen_error = error;
    }

    /// Optional HTTP response status associated with an HTTP-layer failure.
    pub const fn http_status_code(&self) -> Option<u32> {
        self.http_status_code
    }

    /// Whether an HTTP response status was attached to this failure.
    pub const fn has_http_status_code(&self) -> bool {
        self.http_status_code.is_some()
    }

    /// Sets the HTTP response status. Zero clears it.
    pub fn set_http_status_code(&mut self, status: u32) {
        self.http_status_code = (status != 0).then_some(status);
    }

    pub const fn codec_status_code(&self) -> Option<H2ErrorCode> {
        self.codec_status_code
    }

    pub fn set_codec_status_code(&mut self, code: H2ErrorCode) {
        self.codec_status_code = Some(code);
    }

    pub fn partial_message(&self) -> Option<&HttpMessage> {
        self.partial_message.as_deref()
    }

    pub fn set_partial_message(&mut self, message: Option<HttpMessage>) {
        self.partial_message = message.map(Box::new);
    }
}

/// Converts a source or library error into the H2 code sent on the wire.
pub const fn http_error_code_to_h2_error_code(
    code: HttpErrorCode,
    from_source: bool,
) -> H2ErrorCode {
    let raw = code.as_u16();
    if from_source && is_invalid_source_error_code(raw) {
        return H2ErrorCode::INTERNAL_ERROR;
    }

    match raw {
        0 => H2ErrorCode::NO_ERROR,
        1 | 0x10e if !from_source => H2ErrorCode::PROTOCOL_ERROR,
        2 | 0x1003 => H2ErrorCode::INTERNAL_ERROR,
        3 if !from_source => H2ErrorCode::FLOW_CONTROL_ERROR,
        7 => H2ErrorCode::REFUSED_STREAM,
        8 => H2ErrorCode::CANCEL,
        10 => H2ErrorCode::CONNECT_ERROR,
        11 => H2ErrorCode::ENHANCE_YOUR_CALM,
        12 => H2ErrorCode::INADEQUATE_SECURITY,
        13 => H2ErrorCode::HTTP_1_1_REQUIRED,
        _ => H2ErrorCode::INTERNAL_ERROR,
    }
}

const fn is_invalid_source_error_code(raw: u16) -> bool {
    matches!(
        raw,
        1 | 3
            | 4
            | 5
            | 6
            | 9
            | 0x10e
            | 0x1000
            | 0x1001
            | 0x1002
            | 0x1004
            | 0x1005
            | 0x1006
            | 0x1007
            | 0x1008
            | 0x1009
    )
}

/// Converts an H2 wire code into the protocol-neutral [`HttpErrorCode`].
pub const fn h2_error_code_to_http_error_code(code: H2ErrorCode) -> HttpErrorCode {
    let raw = code.as_u32();
    if raw <= H2ErrorCode::HTTP_1_1_REQUIRED.as_u32() {
        HttpErrorCode::from_u16(raw as u16)
    } else {
        HttpErrorCode::PROTOCOL_ERROR
    }
}

/// Converts a codec exception into an [`HttpErrorCode`].
pub const fn http_exception_to_http_error_code(exception: &HttpException) -> HttpErrorCode {
    if let Some(code) = exception.codec_status_code() {
        return h2_error_code_to_http_error_code(code);
    }

    match exception.proxygen_error().as_u8() {
        16 => HttpErrorCode::HEADER_PARSE_ERROR,
        17 => HttpErrorCode::BODY_PARSE_ERROR,
        18 => HttpErrorCode::TRANSPORT_EOF,
        _ => HttpErrorCode::PROTOCOL_ERROR,
    }
}

/// Converts an [`HttpErrorCode`] into its [`ProxygenError`] category.
pub const fn http_error_code_to_proxygen_error(code: HttpErrorCode) -> ProxygenError {
    match code.as_u16() {
        0 => ProxygenError::NONE,
        0x10e => ProxygenError::MESSAGE,
        1 | 5 => ProxygenError::STREAM_ABORT,
        8 | 0x1005 => ProxygenError::CANCELED,
        7 => ProxygenError::STREAM_UNACKNOWLEDGED,
        0x1000 => ProxygenError::INGRESS_STATE_TRANSITION,
        0x1001 => ProxygenError::PARSE_HEADER,
        0x1002 | 0x100a => ProxygenError::PARSE_BODY,
        0x1007 => ProxygenError::CONNECTION_RESET,
        0x1008 => ProxygenError::WRITE,
        0x1006 => ProxygenError::EOF,
        0x1003 => ProxygenError::TIMEOUT,
        0x1009 => ProxygenError::DROPPED,
        _ => ProxygenError::STREAM_ABORT,
    }
}

/// Converts an [`HttpError`] into an [`HttpException`] that affects both
/// directions.
pub fn http_error_to_http_exception(error: &HttpError) -> HttpException {
    let mut exception =
        HttpException::new(HttpExceptionDirection::IngressAndEgress, error.message());
    exception.set_proxygen_error(http_error_code_to_proxygen_error(error.code()));

    let raw = error.code().as_u16();
    if (HttpErrorCode::PROTOCOL_ERROR.as_u16()..=HttpErrorCode::HTTP_1_1_REQUIRED.as_u16())
        .contains(&raw)
    {
        exception.set_codec_status_code(H2ErrorCode::from_u32(u32::from(raw)));
    }
    exception.set_partial_message(error.http_message().cloned());
    exception
}
