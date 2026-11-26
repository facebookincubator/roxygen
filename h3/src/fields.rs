/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/3 message rules for field sections (RFC 9114) and the `priority` header (RFC 9218).
//!
//! Request heads and trailers are decoded through QPACK into Roxygen's message types, and the
//! field lines of responses and trailers are encoded through QPACK.

use std::net::Ipv6Addr;

use bytes::BytesMut;
use mvfstr_proto::SendOrder;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::headers::HttpCommonHeaderTable;
use roxygen_core::headers::HttpCommonHeaders;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::Method as MethodToken;
use roxygen_core::message::HttpMessage;
use roxygen_core::priority::DEFAULT_HTTP_PRIORITY_URGENCY;
use roxygen_qpack::DecodeError;
use roxygen_qpack::Decoder;
use roxygen_qpack::Encoder;
use roxygen_qpack::FieldLine;
use roxygen_qpack::StaticRef;

use crate::codec::H3ErrorCode;

/// The per-line overhead in the size of a field section, as RFC 9114 computes it.
const FIELD_LINE_OVERHEAD: u64 = 32;

/// The scheduling class of request streams; control and QPACK streams use class 0.
const REQUEST_CLASS: u8 = 1;

/// The methods of the QPACK static table's `:method` entries, at indices 15 to 21. Its `:scheme`
/// entries are `http` at 22 and `https` at 23.
const STATIC_METHODS: [HttpMethod; 7] = [
    HttpMethod::Connect,
    HttpMethod::Delete,
    HttpMethod::Get,
    HttpMethod::Head,
    HttpMethod::Options,
    HttpMethod::Post,
    HttpMethod::Put,
];

/// The QPACK static table entry for each header code's name: the first entry with that name, else
/// unlisted. A name without a common code is looked up by QPACK.
const QPACK_NAMES: [StaticRef; HttpCommonHeaders::NUM_CODES] = {
    let mut names = [StaticRef::Unlisted; HttpCommonHeaders::NUM_CODES];
    names[HttpHeaderCode::None as usize] = StaticRef::Unresolved;
    names[HttpHeaderCode::Other as usize] = StaticRef::Unresolved;
    // Downwards, so that the first entry with a name is the one kept.
    let mut index = STATIC_CODES.len();
    while index > 0 {
        index -= 1;
        let code = STATIC_CODES[index] as usize;
        if code != HttpHeaderCode::Other as usize {
            names[code] = StaticRef::Name(index as u8);
        }
    }
    names
};

/// The header code of the name of each entry of the QPACK static table.
const STATIC_CODES: [HttpHeaderCode; 99] = {
    use HttpHeaderCode as H;
    [
        H::ColonAuthority,
        H::ColonPath,
        H::Age,
        H::ContentDisposition,
        H::ContentLength,
        H::Cookie,
        H::Date,
        H::Etag,
        H::IfModifiedSince,
        H::IfNoneMatch,
        H::LastModified,
        H::Other,
        H::Location,
        H::Referer,
        H::SetCookie,
        H::ColonMethod,
        H::ColonMethod,
        H::ColonMethod,
        H::ColonMethod,
        H::ColonMethod,
        H::ColonMethod,
        H::ColonMethod,
        H::ColonScheme,
        H::ColonScheme,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::Accept,
        H::Accept,
        H::AcceptEncoding,
        H::AcceptRanges,
        H::Other,
        H::Other,
        H::AccessControlAllowOrigin,
        H::CacheControl,
        H::CacheControl,
        H::CacheControl,
        H::CacheControl,
        H::CacheControl,
        H::CacheControl,
        H::ContentEncoding,
        H::ContentEncoding,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::ContentType,
        H::Range,
        H::StrictTransportSecurity,
        H::StrictTransportSecurity,
        H::StrictTransportSecurity,
        H::Vary,
        H::Vary,
        H::XContentTypeOptions,
        H::XXssProtection,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::ColonStatus,
        H::AcceptLanguage,
        H::AccessControlAllowCredentials,
        H::AccessControlAllowCredentials,
        H::Other,
        H::AccessControlAllowMethods,
        H::AccessControlAllowMethods,
        H::AccessControlAllowMethods,
        H::AccessControlExposeHeaders,
        H::Other,
        H::Other,
        H::Other,
        H::AltSvc,
        H::Authorization,
        H::ContentSecurityPolicy,
        H::Other,
        H::Other,
        H::Other,
        H::Other,
        H::Other,
        H::Other,
        H::Server,
        H::TimingAllowOrigin,
        H::Other,
        H::UserAgent,
        H::XForwardedFor,
        H::XFrameOptions,
        H::XFrameOptions,
    ]
};

/// Why a field section was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldError {
    /// QPACK cannot decode it.
    Qpack(DecodeError),
    /// It decodes but breaks the HTTP/3 message rules, or exceeds the advertised size limit,
    /// which RFC 9114 lets the receiver treat as malformed: H3_MESSAGE_ERROR.
    Malformed,
}

/// A validated request head.
pub(crate) struct RequestHead {
    pub(crate) message: Box<HttpMessage>,
    pub(crate) content_length: Option<u64>,
    /// A CONNECT request, whose stream then carries only DATA (RFC 9114).
    pub(crate) connect: bool,
    /// A HEAD request, whose response does not carry content (RFC 9110).
    pub(crate) head_request: bool,
}

/// Decodes and validates the request head in `block`.
pub(crate) fn decode_request(
    decoder: &mut Decoder,
    block: &[u8],
    max_size: u64,
) -> Result<RequestHead, FieldError> {
    let mut builder = RequestBuilder::new(max_size);
    decoder
        .decode_resolved_section(block, |line, static_ref| builder.field(line, static_ref))
        .map_err(FieldError::Qpack)?;
    builder.finish(block.len())
}

/// Decodes and validates a trailer section, which must not hold pseudo-header fields.
pub(crate) fn decode_trailers(
    decoder: &mut Decoder,
    block: &[u8],
    max_size: u64,
) -> Result<HttpHeaders, FieldError> {
    let mut trailers = HttpHeaders::new();
    let mut size = 0u64;
    let mut malformed = false;
    decoder
        .decode_field_section(block, |line| {
            size = size.saturating_add(line_size(&line));
            malformed |= size > max_size
                || line.name.starts_with(b":")
                || !valid_name(line.name)
                || !valid_value(line.value)
                || connection_specific(HttpCommonHeaders::hash(line.name));
            if !malformed {
                add_field(&mut trailers, line.name, line.value);
            }
        })
        .map_err(FieldError::Qpack)?;
    if malformed {
        return Err(FieldError::Malformed);
    }
    Ok(trailers)
}

/// Returns the wire order that a request's `priority` header sets, with the defaults of RFC 9218
/// (urgency 3, not incremental) for each parameter that is missing or invalid, and for all of them
/// when the header does not parse.
pub(crate) fn request_order(message: &HttpMessage) -> SendOrder {
    let mut members = PriorityMembers::default();
    let parsed = !message
        .headers()
        .for_each_value(HttpHeaderCode::Priority, |line| !members.parse(line));
    let priority = if parsed {
        members.priority()
    } else {
        Priority::default()
    };
    SendOrder {
        class: REQUEST_CLASS,
        urgency: priority.urgency,
        incremental: priority.incremental,
        order: u128::from(priority.order),
    }
}

/// The parameters of a `priority` field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Priority {
    urgency: u8,
    incremental: bool,
    /// Roxygen's `o` extension, as `roxygen_core::priority` reads it.
    order: u32,
}

impl Default for Priority {
    fn default() -> Self {
        Self {
            urgency: DEFAULT_HTTP_PRIORITY_URGENCY,
            incremental: false,
            order: 0,
        }
    }
}

/// The last value of each priority member a `priority` field's lines hold. The field is a
/// Structured Field Dictionary (RFC 8941), whose later members replace earlier ones with the same
/// key, also across field lines.
#[derive(Default)]
struct PriorityMembers {
    urgency: Option<Item>,
    incremental: Option<Item>,
    order: Option<Item>,
}

impl PriorityMembers {
    /// Adds the members of one field line. Returns false if it does not parse.
    fn parse(&mut self, line: &[u8]) -> bool {
        let mut input = Sf { input: line, at: 0 };
        input.skip(b" ");
        if input.done() {
            return true;
        }
        loop {
            let Some(key) = input.key() else {
                return false;
            };
            let item = if input.eat(b'=') {
                input.member_value()
            } else {
                input.parameters().map(|()| Item::Boolean(true))
            };
            let Some(item) = item else {
                return false;
            };
            match key {
                b"u" => self.urgency = Some(item),
                b"i" => self.incremental = Some(item),
                b"o" => self.order = Some(item),
                _ => {}
            }
            input.skip(b" \t");
            if input.done() {
                return true;
            }
            if !input.eat(b',') {
                return false;
            }
            input.skip(b" \t");
            if input.done() {
                return false;
            }
        }
    }

    /// Returns the members' priority, ignoring a member whose value is of another type or out of
    /// range, as RFC 9218 requires.
    fn priority(&self) -> Priority {
        let default = Priority::default();
        Priority {
            urgency: match self.urgency {
                Some(Item::Integer(urgency @ 0..=7)) => urgency as u8,
                _ => default.urgency,
            },
            incremental: match self.incremental {
                Some(Item::Boolean(incremental)) => incremental,
                _ => default.incremental,
            },
            order: match self.order {
                Some(Item::Integer(order)) => u32::try_from(order).unwrap_or(default.order),
                _ => default.order,
            },
        }
    }
}

/// A Structured Field item, as far as the priority members need it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Item {
    Integer(i64),
    Boolean(bool),
    /// A decimal, string, token, byte sequence or inner list.
    Other,
}

/// A Structured Field parser over one field line (RFC 8941).
struct Sf<'a> {
    input: &'a [u8],
    at: usize,
}

impl<'a> Sf<'a> {
    fn peek(&self) -> Option<u8> {
        self.input.get(self.at).copied()
    }

    fn done(&self) -> bool {
        self.at == self.input.len()
    }

    fn eat(&mut self, byte: u8) -> bool {
        let found = self.peek() == Some(byte);
        self.at += usize::from(found);
        found
    }

    fn skip(&mut self, bytes: &[u8]) {
        while self.peek().is_some_and(|byte| bytes.contains(&byte)) {
            self.at += 1;
        }
    }

    /// Consumes bytes while `accept` holds and returns how many.
    fn take_while(&mut self, accept: impl Fn(u8) -> bool) -> usize {
        let start = self.at;
        while self.peek().is_some_and(&accept) {
            self.at += 1;
        }
        self.at - start
    }

    fn key(&mut self) -> Option<&'a [u8]> {
        let start = self.at;
        if !self
            .peek()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'*')
        {
            return None;
        }
        self.take_while(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.*".contains(&byte)
        });
        Some(&self.input[start..self.at])
    }

    /// Parses an item or an inner list, with its parameters.
    fn member_value(&mut self) -> Option<Item> {
        let item = if self.eat(b'(') {
            loop {
                self.skip(b" ");
                if self.eat(b')') {
                    break Item::Other;
                }
                self.bare_item()?;
                self.parameters()?;
                if !matches!(self.peek(), Some(b' ' | b')')) {
                    return None;
                }
            }
        } else {
            self.bare_item()?
        };
        self.parameters()?;
        Some(item)
    }

    fn parameters(&mut self) -> Option<()> {
        while self.eat(b';') {
            self.skip(b" ");
            self.key()?;
            if self.eat(b'=') {
                self.bare_item()?;
            }
        }
        Some(())
    }

    fn bare_item(&mut self) -> Option<Item> {
        match self.peek()? {
            b'-' | b'0'..=b'9' => self.number(),
            b'"' => {
                self.at += 1;
                loop {
                    match self.peek()? {
                        b'"' => break,
                        b'\\' => {
                            self.at += 1;
                            if !matches!(self.peek()?, b'"' | b'\\') {
                                return None;
                            }
                        }
                        0x20..=0x7e => {}
                        _ => return None,
                    }
                    self.at += 1;
                }
                self.at += 1;
                Some(Item::Other)
            }
            b':' => {
                self.at += 1;
                self.take_while(|byte| byte.is_ascii_alphanumeric() || b"+/=".contains(&byte));
                self.eat(b':').then_some(Item::Other)
            }
            b'?' => {
                self.at += 1;
                let value = match self.peek()? {
                    b'0' => false,
                    b'1' => true,
                    _ => return None,
                };
                self.at += 1;
                Some(Item::Boolean(value))
            }
            byte if byte.is_ascii_alphabetic() || byte == b'*' => {
                self.take_while(|byte| {
                    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~:/".contains(&byte)
                });
                Some(Item::Other)
            }
            _ => None,
        }
    }

    /// Parses an integer of at most 15 digits, or a decimal with at most 12 digits before the
    /// point and 3 after, as RFC 8941 limits them.
    fn number(&mut self) -> Option<Item> {
        let negative = self.eat(b'-');
        let start = self.at;
        let whole = self.take_while(|byte| byte.is_ascii_digit());
        if whole == 0 {
            return None;
        }
        if self.eat(b'.') {
            let fraction = self.take_while(|byte| byte.is_ascii_digit());
            return (whole <= 12 && (1..=3).contains(&fraction)).then_some(Item::Other);
        }
        if whole > 15 {
            return None;
        }
        let magnitude = self.input[start..self.at]
            .iter()
            .fold(0i64, |value, &digit| value * 10 + i64::from(digit - b'0'));
        Some(Item::Integer(if negative { -magnitude } else { magnitude }))
    }
}

/// Maps a peer's RESET_STREAM or STOP_SENDING code to a Roxygen error code. An unknown code is
/// read as H3_NO_ERROR, as RFC 9114 requires, and both mean that the peer cancelled the request.
pub(crate) fn peer_error_code(code: u64) -> HttpErrorCode {
    match H3ErrorCode::new(code) {
        Some(H3ErrorCode::REQUEST_REJECTED) => HttpErrorCode::REFUSED_STREAM,
        Some(H3ErrorCode::INTERNAL_ERROR) => HttpErrorCode::INTERNAL_ERROR,
        Some(H3ErrorCode::EXCESSIVE_LOAD) => HttpErrorCode::ENHANCE_YOUR_CALM,
        Some(H3ErrorCode::MESSAGE_ERROR) => HttpErrorCode::MESSAGE_ERROR,
        Some(H3ErrorCode::CONNECT_ERROR) => HttpErrorCode::CONNECT_ERROR,
        Some(H3ErrorCode::VERSION_FALLBACK) => HttpErrorCode::HTTP_1_1_REQUIRED,
        Some(
            H3ErrorCode::GENERAL_PROTOCOL_ERROR
            | H3ErrorCode::FRAME_UNEXPECTED
            | H3ErrorCode::FRAME_ERROR
            | H3ErrorCode::REQUEST_INCOMPLETE,
        ) => HttpErrorCode::PROTOCOL_ERROR,
        _ => HttpErrorCode::CANCEL,
    }
}

/// Encodes the field section of a response head into `dst` and returns its size. `None` if the
/// message is not a final or interim response whose status HTTP/3 can carry.
pub(crate) fn encode_response(
    encoder: &Encoder,
    message: &HttpMessage,
    names: &mut Vec<u8>,
    dst: &mut BytesMut,
) -> Option<HttpHeaderSize> {
    let status = message.response_status().filter(|status| {
        // HTTP/3 does not support 101 (Switching Protocols).
        (100..=999).contains(status) && *status != 101
    })?;
    let digits = [
        b'0' + (status / 100) as u8,
        b'0' + (status / 10 % 10) as u8,
        b'0' + (status % 10) as u8,
    ];
    let status = (
        FieldLine::new(b":status", &digits),
        StaticRef::status(status),
    );
    Some(encode_fields(
        encoder,
        Some(status),
        message.headers(),
        names,
        dst,
    ))
}

/// Encodes a trailer section into `dst` and returns its size.
pub(crate) fn encode_trailers(
    encoder: &Encoder,
    trailers: &HttpHeaders,
    names: &mut Vec<u8>,
    dst: &mut BytesMut,
) -> HttpHeaderSize {
    encode_fields(encoder, None, trailers, names, dst)
}

/// Encodes the `:status` line when given, then each header that HTTP/3 carries, with lowercase
/// names. Names outside the common table that hold uppercase letters are lowercased into `names`
/// first, so the second pass can borrow them.
fn encode_fields(
    encoder: &Encoder,
    status: Option<(FieldLine<'_>, StaticRef)>,
    headers: &HttpHeaders,
    names: &mut Vec<u8>,
    dst: &mut BytesMut,
) -> HttpHeaderSize {
    names.clear();
    for (code, name, _) in headers.iter() {
        if carried(code, name) && code == HttpHeaderCode::Other && has_uppercase(name) {
            names.extend(name.iter().map(u8::to_ascii_lowercase));
        }
    }
    let names: &[u8] = names;
    let mut lowered = 0;
    let lines = headers.iter().filter_map(|(code, name, value)| {
        if !carried(code, name) {
            return None;
        }
        let (name, static_ref) = if code != HttpHeaderCode::Other {
            let name = HttpCommonHeaders::name(code, HttpCommonHeaderTable::Lowercase);
            (name, QPACK_NAMES[usize::from(code.as_u8())])
        } else if has_uppercase(name) {
            let start = lowered;
            lowered += name.len();
            (&names[start..lowered], StaticRef::Unresolved)
        } else {
            (name, StaticRef::Unresolved)
        };
        Some((FieldLine::new(name, value), static_ref))
    });
    let lines = status.into_iter().chain(lines);
    #[cfg(test)]
    let lines = lines.inspect(|(line, static_ref)| tests::record_encoded_line(line, *static_ref));
    let mut uncompressed = 0u64;
    let start = dst.len();
    encoder.encode_resolved_section(
        lines.inspect(|(line, _)| {
            uncompressed = uncompressed.saturating_add(line_size(line));
        }),
        dst,
    );
    let block = u32::try_from(dst.len() - start).unwrap_or(u32::MAX);
    HttpHeaderSize {
        compressed: block,
        uncompressed: u32::try_from(uncompressed).unwrap_or(u32::MAX),
        compressed_block: block,
    }
}

/// Returns whether HTTP/3 carries a header. It does not carry a pseudo-header that the message
/// holds as a header, `host`, which a response does not have, or a connection-specific field.
fn carried(code: HttpHeaderCode, name: &[u8]) -> bool {
    !name.is_empty()
        && !name.starts_with(b":")
        && code != HttpHeaderCode::Host
        && !connection_specific(code)
}

/// Returns whether `code` is a connection-specific field, which HTTP/3 forbids, or `te`, which
/// only the header section of a request may carry, and only as "trailers".
fn connection_specific(code: HttpHeaderCode) -> bool {
    matches!(
        code,
        HttpHeaderCode::Connection
            | HttpHeaderCode::KeepAlive
            | HttpHeaderCode::ProxyConnection
            | HttpHeaderCode::TransferEncoding
            | HttpHeaderCode::Upgrade
            | HttpHeaderCode::Te
    )
}

fn has_uppercase(name: &[u8]) -> bool {
    name.iter().any(u8::is_ascii_uppercase)
}

fn line_size(line: &FieldLine<'_>) -> u64 {
    (line.name.len() + line.value.len()) as u64 + FIELD_LINE_OVERHEAD
}

/// Returns the header code of a literal field name in a request head, or `None` if a request may
/// not carry it: a pseudo-header field that requests do not define, or a name that is not a
/// lowercase token.
fn literal_code(name: &[u8]) -> Option<HttpHeaderCode> {
    #[cfg(test)]
    tests::count(&tests::NAME_CHECKS);
    Some(match name {
        b":method" => HttpHeaderCode::ColonMethod,
        b":scheme" => HttpHeaderCode::ColonScheme,
        b":authority" => HttpHeaderCode::ColonAuthority,
        b":path" => HttpHeaderCode::ColonPath,
        [b':', ..] => return None,
        _ if valid_name(name) => HttpCommonHeaders::hash(name),
        _ => return None,
    })
}

/// Returns whether `name` is a lowercase token, as RFC 9114 requires of field names.
fn valid_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.iter().all(|&byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&byte)
        })
}

/// Returns whether `value` follows the field-content rule of RFC 9110: it does not contain a
/// control character other than HTAB, or start or end with SP or HTAB.
fn valid_value(value: &[u8]) -> bool {
    #[cfg(test)]
    tests::count(&tests::VALUE_CHECKS);
    let inner = |byte: Option<&u8>| !matches!(byte, Some(b' ' | b'\t'));
    inner(value.first())
        && inner(value.last())
        && value
            .iter()
            .all(|&byte| (byte >= 0x20 || byte == b'\t') && byte != 0x7f)
}

fn add_field(headers: &mut HttpHeaders, name: &[u8], value: &[u8]) {
    match HttpCommonHeaders::hash(name) {
        HttpHeaderCode::Other => headers.add_uncommon(name, value),
        code => headers.add(code, value),
    }
}

/// The kind of scheme a request names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scheme {
    /// `http` or `https`, whose URIs have a mandatory authority.
    Http,
    Other,
}

/// The methods with their own request-target rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Method {
    /// Names only an authority (RFC 9114).
    Connect,
    /// May use the asterisk form (RFC 9110).
    Options,
    Head,
    Other,
}

/// The form of a request's `:path`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Path {
    Empty,
    Asterisk,
    /// A path-absolute with an optional query.
    Origin,
}

/// The state of a request head while its field lines arrive.
struct RequestBuilder {
    message: Box<HttpMessage>,
    max_size: u64,
    size: u64,
    method: Option<Method>,
    scheme: Option<Scheme>,
    path: Option<Path>,
    authority: Option<bool>,
    /// Whether a `host` header arrived.
    host: bool,
    regular: bool,
    content_length: Option<u64>,
    malformed: bool,
}

impl RequestBuilder {
    fn new(max_size: u64) -> Self {
        Self {
            message: Box::new(HttpMessage::new()),
            max_size,
            size: 0,
            method: None,
            scheme: None,
            path: None,
            authority: None,
            host: false,
            regular: false,
            content_length: None,
            malformed: false,
        }
    }

    /// Applies one field line, given the static table entry of its name or of its name and value.
    /// After the first error the rest are only decoded, so that QPACK errors still surface.
    fn field(&mut self, line: FieldLine<'_>, static_ref: StaticRef) {
        if self.malformed {
            return;
        }
        self.size = self.size.saturating_add(line_size(&line));
        let (name, value) = (line.name, line.value);
        let ok = self.size <= self.max_size
            && match static_ref {
                // The static table's names are lowercase tokens and its values are valid.
                StaticRef::Entry(index) => self.entry(index, name, value),
                StaticRef::Name(index) => {
                    valid_value(value) && self.coded(STATIC_CODES[usize::from(index)], name, value)
                }
                StaticRef::Unresolved | StaticRef::Unlisted => {
                    valid_value(value)
                        && literal_code(name).is_some_and(|code| self.coded(code, name, value))
                }
            };
        self.malformed = !ok;
    }

    /// Applies the static table entry at `index`. A `:method` or `:scheme` entry sets the method or
    /// scheme by its index, without parsing the value.
    fn entry(&mut self, index: u8, name: &[u8], value: &[u8]) -> bool {
        match index {
            15..=21 if !self.regular && self.method.is_none() => {
                self.set_method(MethodToken::from(STATIC_METHODS[usize::from(index - 15)]))
            }
            22 | 23 if !self.regular && self.scheme.is_none() => self.set_scheme(index == 23, true),
            _ => self.coded(STATIC_CODES[usize::from(index)], name, value),
        }
    }

    /// Applies a line whose name has the header code `code`.
    fn coded(&mut self, code: HttpHeaderCode, name: &[u8], value: &[u8]) -> bool {
        match code {
            HttpHeaderCode::ColonAuthority
            | HttpHeaderCode::ColonMethod
            | HttpHeaderCode::ColonPath
            | HttpHeaderCode::ColonScheme
            | HttpHeaderCode::ColonStatus
            | HttpHeaderCode::ColonProtocol => self.pseudo(code, value),
            _ => self.regular(code, name, value),
        }
    }

    fn pseudo(&mut self, code: HttpHeaderCode, value: &[u8]) -> bool {
        if self.regular {
            return false;
        }
        match code {
            HttpHeaderCode::ColonMethod if self.method.is_none() => {
                // Methods are case-sensitive (RFC 9110): the token is kept as sent, and only the
                // exact spellings are CONNECT and OPTIONS.
                MethodToken::from_exact_token(value).is_some_and(|method| self.set_method(method))
            }
            HttpHeaderCode::ColonScheme if self.scheme.is_none() => {
                if !valid_scheme(value) {
                    return false;
                }
                // Schemes are case-insensitive (RFC 3986).
                let secure = value.eq_ignore_ascii_case(b"https");
                self.set_scheme(secure, secure || value.eq_ignore_ascii_case(b"http"))
            }
            HttpHeaderCode::ColonAuthority if self.authority.is_none() => {
                self.authority = Some(value.is_empty());
                if !value.is_empty() {
                    self.message.headers_mut().add(HttpHeaderCode::Host, value);
                }
                true
            }
            HttpHeaderCode::ColonPath if self.path.is_none() => {
                let path = match value {
                    b"" => Path::Empty,
                    b"*" => Path::Asterisk,
                    // RFC 9114 requires the path and query of the target URI, so another form,
                    // a fragment or whitespace is malformed.
                    [b'/', ..]
                        if value
                            .iter()
                            .all(|&byte| byte > 0x20 && byte != 0x7f && byte != b'#') =>
                    {
                        Path::Origin
                    }
                    _ => return false,
                };
                self.path = Some(path);
                path == Path::Empty || self.message.set_url(value, false).is_ok()
            }
            // Duplicates, `:status`, `:protocol` (extended CONNECT is not offered) and
            // undefined pseudo-header fields.
            _ => false,
        }
    }

    fn set_method(&mut self, method: MethodToken) -> bool {
        self.method = Some(match method.recognized() {
            Some(HttpMethod::Connect) => Method::Connect,
            Some(HttpMethod::Options) => Method::Options,
            Some(HttpMethod::Head) => Method::Head,
            _ => Method::Other,
        });
        self.message.set_method(method).is_ok()
    }

    /// Records the scheme: `https` when `secure`, and `http` or `https` when `http`.
    fn set_scheme(&mut self, secure: bool, http: bool) -> bool {
        if secure {
            self.message.set_secure(true);
        }
        self.scheme = Some(if http { Scheme::Http } else { Scheme::Other });
        true
    }

    fn regular(&mut self, code: HttpHeaderCode, name: &[u8], value: &[u8]) -> bool {
        self.regular = true;
        match code {
            HttpHeaderCode::Te => {
                if !value.eq_ignore_ascii_case(b"trailers") {
                    return false;
                }
            }
            code if connection_specific(code) => return false,
            HttpHeaderCode::ContentLength => {
                let Some(length) = parse_content_length(value) else {
                    return false;
                };
                match self.content_length {
                    Some(previous) if previous != length => return false,
                    Some(_) => return true,
                    None => self.content_length = Some(length),
                }
            }
            HttpHeaderCode::Host => {
                if let Some(authority_empty) = self.authority {
                    // RFC 9114 requires `host` to equal `:authority` when both are present.
                    let authority = self.message.headers().get_single_or_empty(code);
                    return !authority_empty && authority == value;
                }
                if self.host {
                    return false;
                }
                self.host = true;
            }
            _ => {}
        }
        if code == HttpHeaderCode::Other {
            self.message.headers_mut().add_uncommon(name, value);
        } else {
            self.message.headers_mut().add(code, value);
        }
        true
    }

    fn finish(mut self, encoded_len: usize) -> Result<RequestHead, FieldError> {
        if self.malformed {
            return Err(FieldError::Malformed);
        }
        // `:authority`, or else `host`: the two have been checked to agree when both arrived.
        let host = self
            .message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Host);
        let Some(authority) = parse_authority(host) else {
            return Err(FieldError::Malformed);
        };
        // RFC 9110 and RFC 9114 require the authority of an http or https URI, or of CONNECT, to
        // have a host and to omit userinfo.
        let named = authority.host && !authority.userinfo;
        let complete = match (self.method, self.scheme, self.path) {
            // CONNECT names only an authority, with the host and port (RFC 9114).
            (Some(Method::Connect), None, None) => {
                self.authority == Some(false) && named && authority.port
            }
            (Some(method), Some(scheme), Some(path)) if method != Method::Connect => {
                let target = match path {
                    Path::Origin => true,
                    Path::Asterisk => method == Method::Options,
                    Path::Empty => false,
                };
                target && (scheme != Scheme::Http || named)
            }
            _ => false,
        };
        if !complete {
            return Err(FieldError::Malformed);
        }
        let headers = self.message.headers_mut();
        if headers.number_of_values(HttpHeaderCode::Cookie) > 1 {
            // Cookie crumbs reach the application as one value, as RFC 9114 requires.
            let cookie = headers.combine(HttpHeaderCode::Cookie, b"; ");
            headers.set(HttpHeaderCode::Cookie, cookie);
        }
        self.message.set_http_version(1, 1);
        self.message.set_advanced_protocol_string(b"h3");
        let encoded = u32::try_from(encoded_len).unwrap_or(u32::MAX);
        self.message.set_ingress_header_size(HttpHeaderSize {
            compressed: encoded,
            uncompressed: u32::try_from(self.size).unwrap_or(u32::MAX),
            compressed_block: encoded,
        });
        Ok(RequestHead {
            message: self.message,
            content_length: self.content_length,
            connect: self.method == Some(Method::Connect),
            head_request: self.method == Some(Method::Head),
        })
    }
}

/// The parts that a valid URI authority has.
#[derive(Clone, Copy, Debug)]
struct Authority {
    userinfo: bool,
    /// Whether the host is not empty.
    host: bool,
    /// Whether a port that is not empty follows the host.
    port: bool,
}

/// Parses a URI authority (RFC 3986), which may be empty: an optional userinfo, a host that is an
/// IP literal in brackets or a registered name, and an optional port. `None` if `value` is not one.
fn parse_authority(value: &[u8]) -> Option<Authority> {
    let (userinfo, host_port) = match value.iter().rposition(|&byte| byte == b'@') {
        Some(at) => (Some(&value[..at]), &value[at + 1..]),
        None => (None, value),
    };
    let (host, port) = match host_port.strip_prefix(b"[") {
        // The colons of an IPv6 literal do not start the port.
        Some(rest) => {
            let close = rest.iter().position(|&byte| byte == b']')?;
            let literal = &rest[..close];
            let port = match &rest[close + 1..] {
                [] => None,
                [b':', port @ ..] => Some(port),
                _ => return None,
            };
            if !ip_literal(literal) {
                return None;
            }
            (literal, port)
        }
        None => {
            let (host, port) = match host_port.iter().position(|&byte| byte == b':') {
                Some(colon) => (&host_port[..colon], Some(&host_port[colon + 1..])),
                None => (host_port, None),
            };
            if !uri_chars(host, b"") {
                return None;
            }
            (host, port)
        }
    };
    let valid = userinfo.is_none_or(|userinfo| uri_chars(userinfo, b":"))
        && port.is_none_or(|port| port.iter().all(u8::is_ascii_digit));
    valid.then_some(Authority {
        userinfo: userinfo.is_some(),
        host: !host.is_empty(),
        port: port.is_some_and(|port| !port.is_empty()),
    })
}

/// Returns whether `literal`, the text inside an IP literal's brackets, is an IPv6 address or an
/// IPvFuture.
fn ip_literal(literal: &[u8]) -> bool {
    let [b'v' | b'V', future @ ..] = literal else {
        return str::from_utf8(literal).is_ok_and(|text| text.parse::<Ipv6Addr>().is_ok());
    };
    let Some(dot) = future.iter().position(|&byte| byte == b'.') else {
        return false;
    };
    let (version, address) = (&future[..dot], &future[dot + 1..]);
    !version.is_empty()
        && version.iter().all(u8::is_ascii_hexdigit)
        && !address.is_empty()
        && !address.contains(&b'%')
        && uri_chars(address, b":")
}

/// Returns whether `value` holds only unreserved characters, sub-delims, percent-encoded octets
/// and the `extra` bytes (RFC 3986).
fn uri_chars(value: &[u8], extra: &[u8]) -> bool {
    let mut rest = value;
    while let [byte, tail @ ..] = rest {
        rest = tail;
        if *byte == b'%' {
            let [high, low, tail @ ..] = rest else {
                return false;
            };
            if !high.is_ascii_hexdigit() || !low.is_ascii_hexdigit() {
                return false;
            }
            rest = tail;
        } else if !(byte.is_ascii_alphanumeric()
            || b"-._~!$&'()*+,;=".contains(byte)
            || extra.contains(byte))
        {
            return false;
        }
    }
    true
}

/// Returns whether `value` is a URI scheme (RFC 3986): a letter, then letters, digits, `+`, `-`
/// and `.`.
fn valid_scheme(value: &[u8]) -> bool {
    value.first().is_some_and(u8::is_ascii_alphabetic)
        && value
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || b"+-.".contains(&byte))
}

/// Parses a `content-length` value: decimal digits that fit a `u64`.
fn parse_content_length(value: &[u8]) -> Option<u64> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    value.iter().try_fold(0u64, |length, &digit| {
        length.checked_mul(10)?.checked_add(u64::from(digit - b'0'))
    })
}

#[cfg(test)]
#[path = "../tests/fields.rs"]
mod tests;
