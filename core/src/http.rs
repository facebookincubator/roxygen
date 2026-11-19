/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::ops::Not;

/// Byte counts associated with an HTTP header block.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct HttpHeaderSize {
    /// Bytes after compression or before decompression, including control data.
    pub compressed: u32,
    /// Bytes before compression or after decompression.
    pub uncompressed: u32,
    /// Bytes in the compressed header block, excluding control data.
    pub compressed_block: u32,
}

/// The HTTP methods that have named variants, including the non-standard `SUB`,
/// `PUB` and `UNSUB`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HttpMethod {
    Get,
    Post,
    Options,
    Delete,
    Head,
    Connect,
    Put,
    Trace,
    Patch,
    Sub,
    Pub,
    Unsub,
}

impl HttpMethod {
    pub const ALL: [Self; 12] = [
        Self::Get,
        Self::Post,
        Self::Options,
        Self::Delete,
        Self::Head,
        Self::Connect,
        Self::Put,
        Self::Trace,
        Self::Patch,
        Self::Sub,
        Self::Pub,
        Self::Unsub,
    ];

    /// Matches a known method case-insensitively.
    pub fn from_bytes(method: &[u8]) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|candidate| method.eq_ignore_ascii_case(candidate.as_bytes()))
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Options => "OPTIONS",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Connect => "CONNECT",
            Self::Put => "PUT",
            Self::Trace => "TRACE",
            Self::Patch => "PATCH",
            Self::Sub => "SUB",
            Self::Pub => "PUB",
            Self::Unsub => "UNSUB",
        }
    }

    pub const fn as_bytes(&self) -> &'static [u8] {
        self.as_str().as_bytes()
    }
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// An owned method token and its [`HttpMethod`], if it has one.
///
/// [`Self::from_name`] and the `From` conversions match known methods
/// case-insensitively and uppercase extension methods. [`Self::from_exact_token`]
/// matches case-sensitively and keeps the token as received.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Method {
    name: MethodName,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum MethodName {
    Known(HttpMethod),
    Extension(Box<str>),
}

impl Method {
    pub fn from_name(name: impl Into<Box<str>>) -> Self {
        let name = name.into();
        if let Some(method) = HttpMethod::from_bytes(name.as_bytes()) {
            return Self::from(method);
        }
        Self::from_extension(name)
    }

    pub fn as_str(&self) -> &str {
        match &self.name {
            MethodName::Known(method) => method.as_str(),
            MethodName::Extension(name) => name,
        }
    }

    /// Parses a request-line or `:method` token case-sensitively, as RFC 9110 requires and
    /// `http::Method::from_bytes` does.
    ///
    /// Only a known method's exact canonical spelling maps to that method. Any other token is
    /// an extension method kept byte for byte, so `post` and `Post` are extension methods
    /// distinct from `POST`. Returns `None` unless `token` is an RFC 9110 `token` (one or
    /// more `tchar`).
    pub fn from_exact_token(token: &[u8]) -> Option<Self> {
        if token.is_empty() || !token.iter().copied().all(is_tchar) {
            return None;
        }
        if let Some(method) = HttpMethod::ALL
            .iter()
            .copied()
            .find(|method| method.as_bytes() == token)
        {
            return Some(Self::from(method));
        }
        let name = std::str::from_utf8(token).ok()?;
        Some(Self {
            name: MethodName::Extension(name.into()),
        })
    }

    pub const fn recognized(&self) -> Option<HttpMethod> {
        match &self.name {
            MethodName::Known(method) => Some(*method),
            MethodName::Extension(_) => None,
        }
    }

    fn from_extension(mut name: Box<str>) -> Self {
        name.make_ascii_uppercase();
        Self {
            name: MethodName::Extension(name),
        }
    }
}

impl From<HttpMethod> for Method {
    fn from(method: HttpMethod) -> Self {
        Self {
            name: MethodName::Known(method),
        }
    }
}

impl From<&str> for Method {
    fn from(method: &str) -> Self {
        if let Some(method) = HttpMethod::from_bytes(method.as_bytes()) {
            return Self::from(method);
        }
        Self::from_extension(method.into())
    }
}

impl From<String> for Method {
    fn from(method: String) -> Self {
        if let Some(method) = HttpMethod::from_bytes(method.as_bytes()) {
            return Self::from(method);
        }
        Self::from_extension(method.into_boxed_str())
    }
}

impl AsRef<str> for Method {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Method {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether `byte` is an RFC 9110 `tchar`: a digit, a letter, or one of ``!#$%&'*+-.^_`|~``.
pub const fn is_tchar(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
            | b'0'..=b'9'
            | b'a'..=b'z'
            | b'A'..=b'Z'
    )
}

/// Wire protocol selected for an HTTP codec.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CodecProtocol {
    Http11 = 0,
    Http2 = 1,
    Hq = 2,
    Http3 = 3,
    HttpBinary = 4,
    TunnelLite = 5,
    TunnelConnect = 6,
}

/// The direction in which a codec transports HTTP messages.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TransportDirection {
    /// The codec receives requests from clients.
    Downstream = 0,
    /// The codec sends requests to origin servers.
    Upstream = 1,
}

impl TransportDirection {
    pub const fn is_upstream(self) -> bool {
        matches!(self, Self::Upstream)
    }

    pub const fn is_downstream(self) -> bool {
        matches!(self, Self::Downstream)
    }
}

impl Not for TransportDirection {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Downstream => Self::Upstream,
            Self::Upstream => Self::Downstream,
        }
    }
}
