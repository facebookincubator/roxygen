/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::thread::LocalKey;
use std::time::Duration;
use std::time::Instant;

use super::*;

/// A field line's name and value, and the static table reference passed with it to the encoder.
type EncodedLine = (Vec<u8>, Vec<u8>, StaticRef);

thread_local! {
    /// The number of literal field names that the request builder classified on this thread.
    pub(super) static NAME_CHECKS: Cell<usize> = const { Cell::new(0) };
    /// The number of field values that the request builder checked on this thread.
    pub(super) static VALUE_CHECKS: Cell<usize> = const { Cell::new(0) };
    /// The lines that response and trailer encoding passed to QPACK on this thread.
    static ENCODED_LINES: RefCell<Vec<EncodedLine>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn count(counter: &'static LocalKey<Cell<usize>>) {
    counter.set(counter.get() + 1);
}

pub(super) fn record_encoded_line(line: &FieldLine<'_>, static_ref: StaticRef) {
    ENCODED_LINES.with_borrow_mut(|lines| {
        lines.push((line.name.to_vec(), line.value.to_vec(), static_ref));
    });
}

/// The reference request builder, which ignores the static table entries that QPACK resolves and
/// checks every name and value.
mod reference {
    use roxygen_core::headers::HttpCommonHeaders;
    use roxygen_core::headers::HttpHeaderCode;
    use roxygen_core::http::HttpHeaderSize;
    use roxygen_core::http::HttpMethod;
    use roxygen_core::http::Method as MethodToken;
    use roxygen_core::message::HttpMessage;
    use roxygen_qpack::Decoder;
    use roxygen_qpack::FieldLine;

    use super::super::FieldError;
    use super::super::Method;
    use super::super::Path;
    use super::super::RequestHead;
    use super::super::Scheme;
    use super::super::connection_specific;
    use super::super::line_size;
    use super::super::parse_authority;
    use super::super::parse_content_length;
    use super::super::valid_name;
    use super::super::valid_scheme;
    use super::super::valid_value;

    pub(super) fn decode_request(
        decoder: &mut Decoder,
        block: &[u8],
        max_size: u64,
    ) -> Result<RequestHead, FieldError> {
        let mut builder = RequestBuilder::new(max_size);
        decoder
            .decode_field_section(block, |line| builder.field(line))
            .map_err(FieldError::Qpack)?;
        builder.finish(block.len())
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

        /// Applies one field line. After the first error the rest are only decoded, so that QPACK
        /// errors still surface.
        fn field(&mut self, line: FieldLine<'_>) {
            if self.malformed {
                return;
            }
            self.size = self.size.saturating_add(line_size(&line));
            let ok = self.size <= self.max_size
                && valid_value(line.value)
                && if line.name.starts_with(b":") {
                    self.pseudo(line.name, line.value)
                } else {
                    self.regular(line.name, line.value)
                };
            self.malformed = !ok;
        }

        fn pseudo(&mut self, name: &[u8], value: &[u8]) -> bool {
            if self.regular {
                return false;
            }
            match name {
                b":method" if self.method.is_none() => {
                    // Methods are case-sensitive (RFC 9110): the token is kept as sent, and only
                    // the exact spellings are CONNECT and OPTIONS.
                    let Some(method) = MethodToken::from_exact_token(value) else {
                        return false;
                    };
                    self.method = Some(match method.recognized() {
                        Some(HttpMethod::Connect) => Method::Connect,
                        Some(HttpMethod::Options) => Method::Options,
                        Some(HttpMethod::Head) => Method::Head,
                        _ => Method::Other,
                    });
                    self.message.set_method(method).is_ok()
                }
                b":scheme" if self.scheme.is_none() => {
                    if !valid_scheme(value) {
                        return false;
                    }
                    // Schemes are case-insensitive (RFC 3986).
                    let secure = value.eq_ignore_ascii_case(b"https");
                    if secure {
                        self.message.set_secure(true);
                    }
                    self.scheme = Some(if secure || value.eq_ignore_ascii_case(b"http") {
                        Scheme::Http
                    } else {
                        Scheme::Other
                    });
                    true
                }
                b":authority" if self.authority.is_none() => {
                    self.authority = Some(value.is_empty());
                    if !value.is_empty() {
                        self.message.headers_mut().add(HttpHeaderCode::Host, value);
                    }
                    true
                }
                b":path" if self.path.is_none() => {
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

        fn regular(&mut self, name: &[u8], value: &[u8]) -> bool {
            self.regular = true;
            if !valid_name(name) {
                return false;
            }
            let code = HttpCommonHeaders::hash(name);
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
            // RFC 9110 and RFC 9114 require the authority of an http or https URI, or of CONNECT,
            // to have a host and to omit userinfo.
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
}

/// An encoded field section that a test builds line by line, choosing each line's representation.
struct Block(Vec<u8>);

impl Block {
    fn new() -> Self {
        // Required Insert Count 0 and Delta Base 0.
        Self(vec![0, 0])
    }

    fn integer(&mut self, first: u8, prefix_bits: u8, value: usize) {
        let max = (1usize << prefix_bits) - 1;
        if value < max {
            self.0.push(first | value as u8);
            return;
        }
        self.0.push(first | max as u8);
        let mut rest = value - max;
        while rest >= 0x80 {
            self.0.push(0x80 | (rest & 0x7f) as u8);
            rest >>= 7;
        }
        self.0.push(rest as u8);
    }

    /// Appends an Indexed Field Line for a static table entry.
    fn indexed(&mut self, index: u8) {
        self.integer(0b1100_0000, 6, usize::from(index));
    }

    /// Appends a Literal Field Line with a static Name Reference and a raw value.
    fn name_ref(&mut self, index: u8, value: &[u8]) {
        self.integer(0b0101_0000, 4, usize::from(index));
        self.integer(0, 7, value.len());
        self.0.extend_from_slice(value);
    }

    /// Appends a Literal Field Line with Literal Name, both raw.
    fn literal(&mut self, name: &[u8], value: &[u8]) {
        self.integer(0b0010_0000, 3, name.len());
        self.0.extend_from_slice(name);
        self.integer(0, 7, value.len());
        self.0.extend_from_slice(value);
    }
}

/// The outcome of decoding a request head, comparable across builders. The message is compared
/// by its debug form without its creation time.
#[derive(Debug, Eq, PartialEq)]
enum Outcome {
    Accepted {
        message: String,
        content_length: Option<u64>,
        connect: bool,
        head_request: bool,
    },
    Refused(FieldError),
}

fn outcome(result: Result<RequestHead, FieldError>) -> Outcome {
    match result {
        Ok(head) => {
            let message = format!("{:?}", head.message);
            let start = message
                .find("start_time: ")
                .expect("the message has a start time");
            let end = message
                .find("destination_address: ")
                .expect("and a destination");
            Outcome::Accepted {
                message: format!("{}{}", &message[..start], &message[end..]),
                content_length: head.content_length,
                connect: head.connect,
                head_request: head.head_request,
            }
        }
        Err(error) => Outcome::Refused(error),
    }
}

const VALUES: &[&[u8]] = &[
    b"",
    b"GET",
    b"POST",
    b"CONNECT",
    b"OPTIONS",
    b"HEAD",
    b"get",
    b"G T",
    b"https",
    b"http",
    b"HTTPS",
    b"ftp",
    b"1http",
    b"/",
    b"*",
    b"/index.html?q=1",
    b"/a#b",
    b"/a b",
    b"x",
    b"example.com",
    b"example.com:443",
    b"[::1]:8443",
    b"[v1.x]",
    b"[::1",
    b"u@example.com",
    b"example.com:",
    b"0",
    b"42",
    b"-1",
    b"18446744073709551616",
    b"trailers",
    b"Trailers",
    b" lead",
    b"trail ",
    b"a\tb",
    b"a\x00b",
    b"a\x7fb",
    b"\x80\xff",
    b"u=1, i",
    b"text/plain",
];

const NAMES: &[&[u8]] = &[
    b":method",
    b":scheme",
    b":authority",
    b":path",
    b":status",
    b":protocol",
    b":Method",
    b":",
    b":foo",
    b"host",
    b"Host",
    b"content-length",
    b"te",
    b"connection",
    b"keep-alive",
    b"transfer-encoding",
    b"upgrade",
    b"proxy-connection",
    b"cookie",
    b"x-custom",
    b"X-Custom",
    b"",
    b"bad name",
    b"user-agent",
    b"accept",
    b"priority",
    b"link",
];

/// Decodes `block` with both builders and checks that they agree. Returns whether it was
/// accepted.
fn compare(block: &[u8], max_size: u64) -> bool {
    let expected = outcome(reference::decode_request(
        &mut Decoder::new(),
        block,
        max_size,
    ));
    let actual = outcome(decode_request(
        &mut Decoder::new(),
        block,
        max_size,
        Instant::now(),
    ));
    assert_eq!(actual, expected, "{block:02x?} under {max_size}");
    matches!(actual, Outcome::Accepted { .. })
}

/// Appends a random line in a random representation.
fn random_line(block: &mut Block, next: &mut impl FnMut() -> u64) {
    let value = VALUES[(next() as usize) % VALUES.len()];
    match next() % 3 {
        0 => block.indexed((next() % 99) as u8),
        1 => block.name_ref((next() % 99) as u8, value),
        _ => block.literal(NAMES[(next() as usize) % NAMES.len()], value),
    }
}

/// Appends a pseudo-header line for `name` and `value` in a random representation that encodes
/// them: the static entry when one matches, a reference to the static name, or literals.
fn random_pseudo(block: &mut Block, name: &[u8], value: &[u8], next: &mut impl FnMut() -> u64) {
    let entries = roxygen_qpack::STATIC_TABLE;
    let full = entries.iter().position(|&(n, v)| n == name && v == value);
    let named = entries.iter().position(|&(n, _)| n == name);
    match (next() % 3, full, named) {
        (0, Some(index), _) => block.indexed(index as u8),
        (0 | 1, _, Some(index)) => block.name_ref(index as u8, value),
        _ => block.literal(name, value),
    }
}

#[test]
fn the_request_builder_accepts_and_rejects_what_it_did_before() {
    let mut state = 0x9114_0412_5eed_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let sizes = [u64::MAX, 4096, 160, 100];
    let mut accepted = 0;
    // Each static entry, and each static name with each value, after a valid request's
    // pseudo-header fields and before them.
    for index in 0..99u8 {
        let mut lines: Vec<Block> = vec![Block::new(), Block::new()];
        lines[0].indexed(index);
        lines[1].name_ref(index, b"v");
        for value in VALUES {
            let mut line = Block::new();
            line.name_ref(index, value);
            lines.push(line);
        }
        for line in lines {
            for before in [false, true] {
                let mut block = Block::new();
                if before {
                    block.0.extend_from_slice(&line.0[2..]);
                }
                block.indexed(17);
                block.indexed(23);
                block.name_ref(0, b"example.com");
                block.name_ref(1, b"/index.html");
                if !before {
                    block.0.extend_from_slice(&line.0[2..]);
                }
                accepted += usize::from(compare(&block.0, u64::MAX));
            }
        }
    }
    // Each static `:method` and `:scheme` entry in place of the request's own line, and CONNECT
    // with only an authority. Only CONNECT with a scheme and a path is refused.
    for index in 15..=23u8 {
        let (method, scheme) = if index <= 21 {
            (index, 23)
        } else {
            (17, index)
        };
        let mut block = Block::new();
        block.indexed(method);
        block.indexed(scheme);
        block.name_ref(0, b"example.com");
        block.name_ref(1, b"/index.html");
        assert_eq!(compare(&block.0, u64::MAX), index != 15, "{index}");
    }
    let mut connect = Block::new();
    connect.indexed(15);
    connect.name_ref(0, b"example.com:443");
    assert!(compare(&connect.0, u64::MAX));
    // Random requests: mostly the four pseudo-header fields of a request, in random
    // representations and sometimes another order, then random lines.
    for round in 0..30_000 {
        let mut block = Block::new();
        if round % 4 != 0 {
            let method = [
                b"GET".as_slice(),
                b"POST",
                b"CONNECT",
                b"OPTIONS",
                b"HEAD",
                b"DELETE",
                b"PUT",
                b"PATCH",
            ][(next() % 8) as usize];
            let scheme = [b"https".as_slice(), b"http", b"ftp"][(next() % 3) as usize];
            let authority =
                [b"example.com".as_slice(), b"example.com:443", b"", b"u@x"][(next() % 4) as usize];
            let path = [b"/".as_slice(), b"/index.html", b"*", b""][(next() % 4) as usize];
            let mut pseudo = vec![
                (b":method".as_slice(), method),
                (b":scheme", scheme),
                (b":authority", authority),
                (b":path", path),
            ];
            if next() % 4 == 0 {
                let (a, b) = ((next() % 4) as usize, (next() % 4) as usize);
                pseudo.swap(a, b);
            }
            if method == b"CONNECT" && next() % 2 == 0 {
                pseudo.retain(|(name, _)| *name == b":method" || *name == b":authority");
            }
            for (name, value) in pseudo {
                random_pseudo(&mut block, name, value, &mut next);
            }
        }
        for _ in 0..next() % 5 {
            random_line(&mut block, &mut next);
        }
        let max_size = sizes[(next() as usize) % sizes.len()];
        accepted += usize::from(compare(&block.0, max_size));
    }
    // Both acceptance and refusal are well represented.
    assert!(accepted > 3000, "{accepted} accepted");
}

#[test]
fn every_static_entry_decodes_to_the_code_of_its_name() {
    for index in 0..99u8 {
        let mut block = Block::new();
        block.indexed(index);
        block.name_ref(index, b"v");
        let mut lines = 0;
        Decoder::new()
            .decode_resolved_section(&block.0, |line, static_ref| {
                let (StaticRef::Entry(resolved) | StaticRef::Name(resolved)) = static_ref else {
                    panic!("{index}: {static_ref:?}");
                };
                assert_eq!(resolved, index);
                assert_eq!(
                    STATIC_CODES[usize::from(resolved)],
                    HttpCommonHeaders::hash(line.name),
                    "{index}: {:?}",
                    String::from_utf8_lossy(line.name)
                );
                lines += 1;
            })
            .unwrap();
        assert_eq!(lines, 2);
    }
}

#[test]
fn a_static_name_is_not_checked_again_nor_a_static_value() {
    for index in 0..99u8 {
        let (name, _) = roxygen_qpack::STATIC_TABLE[usize::from(index)];
        let mut entry = Block::new();
        entry.indexed(index);
        let mut static_name = Block::new();
        static_name.name_ref(index, b"v");
        let mut literal = Block::new();
        literal.literal(name, b"v");
        // Name checks and value checks for each representation of a line with this name.
        for (block, checks) in [(entry, (0, 0)), (static_name, (0, 1)), (literal, (1, 1))] {
            NAME_CHECKS.set(0);
            VALUE_CHECKS.set(0);
            let _ = decode_request(&mut Decoder::new(), &block.0, u64::MAX, Instant::now());
            assert_eq!(
                (NAME_CHECKS.get(), VALUE_CHECKS.get()),
                checks,
                "{index}: {block:02x?}",
                block = block.0
            );
        }
    }
}

#[test]
fn decode_request_stamps_the_reading_it_is_given() {
    // Ahead of the clock, so that a clock read cannot produce it.
    let reading = Instant::now() + Duration::from_secs(3600);
    let mut block = Block::new();
    block.indexed(17);
    block.indexed(23);
    block.name_ref(0, b"example.com");
    block.name_ref(1, b"/index.html");
    let Ok(head) = decode_request(&mut Decoder::new(), &block.0, u64::MAX, reading) else {
        panic!("a GET with its four pseudo-header fields is valid");
    };
    assert_eq!(head.message.start_time(), reading);
}

/// The reference response and trailer encoding, which leaves every static table lookup to QPACK.
mod reference_encoding {
    use bytes::BytesMut;
    use roxygen_core::headers::HttpCommonHeaderTable;
    use roxygen_core::headers::HttpCommonHeaders;
    use roxygen_core::headers::HttpHeaderCode;
    use roxygen_core::headers::HttpHeaders;
    use roxygen_core::http::HttpHeaderSize;
    use roxygen_core::message::HttpMessage;
    use roxygen_qpack::Encoder;
    use roxygen_qpack::FieldLine;

    use super::super::carried;
    use super::super::has_uppercase;
    use super::super::line_size;

    /// Encodes the field section of a response head into `dst` and returns its size. `None` if the
    /// message is not a final or interim response whose status HTTP/3 can carry.
    pub(super) fn encode_response(
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
        Some(encode_fields(
            encoder,
            Some(&digits),
            message.headers(),
            names,
            dst,
        ))
    }

    /// Encodes a trailer section into `dst` and returns its size.
    pub(super) fn encode_trailers(
        encoder: &Encoder,
        trailers: &HttpHeaders,
        names: &mut Vec<u8>,
        dst: &mut BytesMut,
    ) -> HttpHeaderSize {
        encode_fields(encoder, None, trailers, names, dst)
    }

    /// Encodes `:status` when given, then each header that HTTP/3 carries, with lowercase names.
    /// Names outside the common table that hold uppercase letters are lowercased into `names`
    /// first, so the second pass can borrow them.
    fn encode_fields(
        encoder: &Encoder,
        status: Option<&[u8; 3]>,
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
            let name = if code != HttpHeaderCode::Other {
                HttpCommonHeaders::name(code, HttpCommonHeaderTable::Lowercase)
            } else if has_uppercase(name) {
                let start = lowered;
                lowered += name.len();
                &names[start..lowered]
            } else {
                name
            };
            Some(FieldLine::new(name, value))
        });
        let status = status.map(|digits| FieldLine::new(b":status", digits));
        let mut uncompressed = 0u64;
        let start = dst.len();
        encoder.encode_field_section(
            status.into_iter().chain(lines).inspect(|line| {
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
}

/// Returns response header sets: every value of each static name with values, the other names a
/// response commonly carries, names that are not in either table, uppercase names, and fields
/// HTTP/3 does not carry.
fn response_header_sets() -> Vec<Vec<(String, Vec<u8>)>> {
    let field = |name: &str, value: &[u8]| (name.to_string(), value.to_vec());
    let mut sets = Vec::new();
    for &(name, value) in &roxygen_qpack::STATIC_TABLE {
        if !name.starts_with(b":") {
            let name = String::from_utf8(name.to_vec()).unwrap();
            sets.push(vec![field(&name, value), field(&name, b"other")]);
        }
    }
    sets.push(vec![
        field("content-type", b"text/plain"),
        field("content-length", b"13"),
    ]);
    sets.push(vec![
        field("Content-Type", b"application/javascript"),
        field("Content-Encoding", b"br"),
        field("Vary", b"Accept-Encoding"),
        field("Vary", b"accept-encoding"),
        field("Content-Length", b"7"),
    ]);
    sets.push(vec![
        field("server", b"roxygen"),
        field("date", b"Sat, 21 Nov 2026 10:00:00 GMT"),
        field("cache-control", b"max-age=604800"),
        field("etag", b"\"abc\""),
        field("last-modified", b"Fri, 20 Nov 2026 10:00:00 GMT"),
        field("set-cookie", b"id=42; Secure"),
        field("alt-svc", b"h3=\":443\""),
        field("strict-transport-security", b"max-age=31536000"),
    ]);
    sets.push(vec![
        field("x-trace", b""),
        field("X-Custom", b"Value"),
        field("expires", b"0"),
        field("content-language", b"en"),
        field("Link", b"</style.css>; rel=preload"),
        field("Early-Data", b"1"),
        field("x-forwarded-for", b"192.0.2.1"),
    ]);
    sets.push(vec![
        field("connection", b"close"),
        field("transfer-encoding", b"chunked"),
        field("host", b"example.com"),
        field("keep-alive", b"timeout=5"),
        field("te", b"trailers"),
        field("upgrade", b"h2c"),
        field("proxy-connection", b"close"),
        field("content-type", b"text/css"),
    ]);
    sets.push(Vec::new());
    sets
}

#[test]
fn response_and_trailer_sections_are_as_before() {
    let encoder = Encoder::new();
    let (mut names, mut expected_names) = (Vec::new(), Vec::new());
    let mut statuses: Vec<u16> = (0..=1000).collect();
    statuses.push(u16::MAX);
    for headers in response_header_sets() {
        let mut fields = HttpHeaders::new();
        for (name, value) in &headers {
            fields.add(name.as_str(), value);
        }
        for &status in &statuses {
            let mut message = HttpMessage::new();
            if message.set_status_code(status).is_err() {
                continue;
            }
            *message.headers_mut() = fields.clone();
            let mut expected = BytesMut::new();
            let expected_size = reference_encoding::encode_response(
                &encoder,
                &message,
                &mut expected_names,
                &mut expected,
            );
            let mut actual = BytesMut::new();
            let size = encode_response(&encoder, &message, &mut names, &mut actual);
            assert_eq!(size, expected_size, "{status} {headers:?}");
            assert_eq!(actual, expected, "{status} {headers:?}");
        }
        let mut expected = BytesMut::new();
        let expected_size = reference_encoding::encode_trailers(
            &encoder,
            &fields,
            &mut expected_names,
            &mut expected,
        );
        let mut actual = BytesMut::new();
        let size = encode_trailers(&encoder, &fields, &mut names, &mut actual);
        assert_eq!((size, actual), (expected_size, expected), "{headers:?}");
    }
}

/// Returns the static table reference that response encoding should pass to QPACK for a line: the
/// `:status` entry with its value, else the name of entry 24; the first entry with a common
/// header's name, else `Unlisted`; and `Unresolved` for other names.
fn expected_static_ref(name: &[u8], value: &[u8]) -> StaticRef {
    let table = &roxygen_qpack::STATIC_TABLE;
    if name == b":status" {
        return match table.iter().position(|&entry| entry == (name, value)) {
            Some(index) => StaticRef::Entry(index as u8),
            None => StaticRef::Name(24),
        };
    }
    if HttpCommonHeaders::hash(name) == HttpHeaderCode::Other {
        return StaticRef::Unresolved;
    }
    match table.iter().position(|&(entry_name, _)| entry_name == name) {
        Some(index) => StaticRef::Name(index as u8),
        None => StaticRef::Unlisted,
    }
}

#[test]
fn response_and_trailer_lines_reach_qpack_with_their_static_entries() {
    let encoder = Encoder::new();
    let mut names = Vec::new();
    let check = |section: &str| {
        let lines = ENCODED_LINES.take();
        assert!(!lines.is_empty() || section == "trailers", "{section}");
        for (name, value, static_ref) in lines {
            let line = (
                String::from_utf8_lossy(&name),
                String::from_utf8_lossy(&value),
            );
            assert_eq!(
                static_ref,
                expected_static_ref(&name, &value),
                "{section} {line:?}"
            );
        }
    };
    for headers in response_header_sets() {
        let mut fields = HttpHeaders::new();
        for (name, value) in &headers {
            fields.add(name.as_str(), value);
        }
        // HTTP/3 does not support 101.
        for status in (100..=999).filter(|&status| status != 101) {
            let mut message = HttpMessage::new();
            message.set_status_code(status).unwrap();
            *message.headers_mut() = fields.clone();
            ENCODED_LINES.take();
            let encoded = encode_response(&encoder, &message, &mut names, &mut BytesMut::new());
            assert!(encoded.is_some(), "{status}");
            check(&format!("{status}"));
        }
        ENCODED_LINES.take();
        encode_trailers(&encoder, &fields, &mut names, &mut BytesMut::new());
        check("trailers");
    }
}

#[test]
fn each_header_code_has_the_first_static_entry_with_its_name() {
    for &code in HttpHeaderCode::ALL {
        let name = HttpCommonHeaders::name(code, HttpCommonHeaderTable::Lowercase);
        let expected = match code {
            HttpHeaderCode::None | HttpHeaderCode::Other => StaticRef::Unresolved,
            _ => match roxygen_qpack::STATIC_TABLE.iter().position(|e| e.0 == name) {
                Some(index) => StaticRef::Name(index as u8),
                None => StaticRef::Unlisted,
            },
        };
        assert_eq!(QPACK_NAMES[usize::from(code.as_u8())], expected, "{code:?}");
    }
}
