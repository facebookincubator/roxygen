/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::sync::Arc;

use crate::message::HttpMessage;

/// An HTTP/2 wire or protocol-independent library error code.
///
/// The transparent representation keeps unknown peer-provided values safe to
/// inspect. HTTP/3 and QPACK codes intentionally have no named constants.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct HttpErrorCode(u16);

impl HttpErrorCode {
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

    // This protocol-independent library error keeps its established value, even
    // though it lies in the HTTP/3 numeric range that the other codes avoid.
    pub const MESSAGE_ERROR: Self = Self(0x10e);

    pub const INVALID_STATE_TRANSITION: Self = Self(0x1000);
    pub const HEADER_PARSE_ERROR: Self = Self(0x1001);
    pub const BODY_PARSE_ERROR: Self = Self(0x1002);
    pub const READ_TIMEOUT: Self = Self(0x1003);
    pub const WRITE_TIMEOUT: Self = Self(0x1004);
    pub const CORO_CANCELLED: Self = Self(0x1005);
    pub const TRANSPORT_EOF: Self = Self(0x1006);
    pub const TRANSPORT_READ_ERROR: Self = Self(0x1007);
    pub const TRANSPORT_WRITE_ERROR: Self = Self(0x1008);
    pub const DROPPED: Self = Self(0x1009);
    pub const CONTENT_LENGTH_MISMATCH: Self = Self(0x100a);

    pub const fn from_u16(value: u16) -> Self {
        Self(value)
    }

    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

impl From<u16> for HttpErrorCode {
    fn from(value: u16) -> Self {
        Self::from_u16(value)
    }
}

impl From<HttpErrorCode> for u16 {
    fn from(code: HttpErrorCode) -> Self {
        code.as_u16()
    }
}

pub const fn get_error_string(code: HttpErrorCode) -> &'static str {
    match code.as_u16() {
        0 => "NO_ERROR",
        1 => "PROTOCOL_ERROR",
        2 => "INTERNAL_ERROR",
        3 => "FLOW_CONTROL_ERROR",
        4 => "SETTINGS_TIMEOUT",
        5 => "STREAM_CLOSED",
        6 => "FRAME_SIZE_ERROR",
        7 => "REFUSED_STREAM",
        8 => "CANCEL",
        9 => "COMPRESSION_ERROR",
        10 => "CONNECT_ERROR",
        11 => "ENHANCE_YOUR_CALM",
        12 => "INADEQUATE_SECURITY",
        13 => "HTTP_1_1_REQUIRED",
        0x10e => "MESSAGE_ERROR",
        0x1000 => "INVALID_STATE_TRANSITION",
        0x1001 => "HEADER_PARSE_ERROR",
        0x1002 => "BODY_PARSE_ERROR",
        0x1003 => "READ_TIMEOUT",
        0x1004 => "WRITE_TIMEOUT",
        0x1005 => "CORO_CANCELLED",
        0x1006 => "TRANSPORT_EOF",
        0x1007 => "TRANSPORT_READ_ERROR",
        0x1008 => "TRANSPORT_WRITE_ERROR",
        0x1009 => "DROPPED",
        0x100a => "CONTENT_LENGTH_MISMATCH",
        _ => "UNKNOWN_ERROR",
    }
}

pub const fn is_internal_error(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::INTERNAL_ERROR.as_u16()
}

pub const fn is_request_rejected(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::REFUSED_STREAM.as_u16()
}

pub const fn is_cancelled(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::CANCEL.as_u16()
}

pub const fn is_connect_error(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::CONNECT_ERROR.as_u16()
}

pub const fn is_excessive_load(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::ENHANCE_YOUR_CALM.as_u16()
}

pub const fn is_version_fallback(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::HTTP_1_1_REQUIRED.as_u16()
}

pub const fn is_protocol_error(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::PROTOCOL_ERROR.as_u16()
}

pub const fn is_no_error(code: HttpErrorCode) -> bool {
    code.as_u16() == HttpErrorCode::NO_ERROR.as_u16()
}

#[derive(Debug)]
struct ErrorMessage<'a>(&'a [u8]);

impl fmt::Display for ErrorMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&String::from_utf8_lossy(self.0))
    }
}

#[derive(Clone, Default)]
struct ErrorDetails {
    message: Option<Box<[u8]>>,
    http_message: Option<Box<HttpMessage>>,
    cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

pub struct HttpError {
    code: HttpErrorCode,
    // Changing the code preserves the diagnostic chosen at construction.
    message_code: HttpErrorCode,
    details: Option<Box<ErrorDetails>>,
}

impl HttpError {
    pub fn new(code: HttpErrorCode) -> Self {
        Self {
            code,
            message_code: code,
            details: None,
        }
    }

    #[cold]
    #[inline(never)]
    pub fn with_message(code: HttpErrorCode, message: impl AsRef<[u8]>) -> Self {
        Self {
            code,
            message_code: code,
            details: Some(Box::new(ErrorDetails {
                message: Some(message.as_ref().to_vec().into_boxed_slice()),
                http_message: None,
                cause: None,
            })),
        }
    }

    fn details_mut(&mut self) -> &mut ErrorDetails {
        self.details.get_or_insert_with(Default::default)
    }

    /// Retains this error's original typed cause without including it in diagnostics.
    ///
    /// Clones share the cause. Changing HTTP context preserves it, while
    /// `clone_from` replaces or clears it.
    #[cold]
    #[inline(never)]
    pub fn with_cause(mut self, cause: Arc<dyn std::error::Error + Send + Sync>) -> Self {
        self.details_mut().cause = Some(cause);
        self
    }

    pub const fn code(&self) -> HttpErrorCode {
        self.code
    }

    pub fn set_code(&mut self, code: HttpErrorCode) {
        self.code = code;
    }

    pub fn message(&self) -> &[u8] {
        self.details
            .as_ref()
            .and_then(|details| details.message.as_deref())
            .unwrap_or_else(|| get_error_string(self.message_code).as_bytes())
    }

    #[cold]
    #[inline(never)]
    pub fn set_message(&mut self, message: impl AsRef<[u8]>) {
        let message = message.as_ref().to_vec().into_boxed_slice();
        self.details_mut().message = Some(message);
    }

    pub fn http_message(&self) -> Option<&HttpMessage> {
        self.details
            .as_ref()
            .and_then(|details| details.http_message.as_deref())
    }

    pub fn http_message_mut(&mut self) -> Option<&mut HttpMessage> {
        self.details
            .as_mut()
            .and_then(|details| details.http_message.as_deref_mut())
    }

    pub fn set_http_message(&mut self, message: Option<HttpMessage>) {
        if let Some(message) = message {
            self.details_mut().http_message = Some(Box::new(message));
        } else if let Some(details) = &mut self.details {
            details.http_message = None;
        }
    }

    pub fn with_http_message(mut self, message: HttpMessage) -> Self {
        self.set_http_message(Some(message));
        self
    }

    pub fn take_http_message(&mut self) -> Option<HttpMessage> {
        self.details
            .as_mut()
            .and_then(|details| details.http_message.take())
            .map(|message| *message)
    }

    #[cold]
    #[inline(never)]
    pub fn describe(&self) -> Vec<u8> {
        let mut description = format!(
            "error={}({}) msg=",
            get_error_string(self.code),
            self.code.as_u16(),
        )
        .into_bytes();
        description.extend_from_slice(self.message());
        description
    }
}

impl Clone for HttpError {
    #[cold]
    #[inline(never)]
    fn clone(&self) -> Self {
        Self {
            code: self.code,
            message_code: self.message_code,
            details: self.details.clone(),
        }
    }

    #[cold]
    #[inline(never)]
    fn clone_from(&mut self, source: &Self) {
        self.code = source.code;
        self.message_code = source.message_code;
        if let Some(source) = &source.details {
            let target = self.details_mut();
            target.message.clone_from(&source.message);
            target.cause.clone_from(&source.cause);
            // Unlike `clone`, this keeps the destination's partial message when the
            // source has none.
            if let Some(message) = &source.http_message {
                target.http_message = Some(message.clone());
            }
        } else if let Some(target) = &mut self.details {
            target.message = None;
            target.cause = None;
        }
    }
}

impl fmt::Debug for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpError")
            .field("code", &self.code)
            .field("message", &ErrorMessage(self.message()))
            .field("http_message", &self.http_message())
            .field(
                "has_cause",
                &self
                    .details
                    .as_ref()
                    .is_some_and(|details| details.cause.is_some()),
            )
            .finish()
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&ErrorMessage(self.message()), formatter)
    }
}

impl std::error::Error for HttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Expose the underlying error so source downcasts retain its identity.
        // Deriving Error with an Arc source exposes the Arc wrapper instead.
        self.details
            .as_ref()
            .and_then(|details| details.cause.as_deref())
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

impl From<HttpErrorCode> for HttpError {
    fn from(code: HttpErrorCode) -> Self {
        Self::new(code)
    }
}
