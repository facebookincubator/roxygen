//! The HTTP/1.1 head path against a frozen copy of the code it replaced
//! (PARITY.md R6, a performance-only repair).
//!
//! The `old_*` items are verbatim copies of `ingress_head`,
//! `scan_head_counting_line_feeds`, `PrefixMatch`, `parse_head`,
//! `add_validated_header`, `HeaderLines`, `request_line_is_http_09`,
//! `valid_header_value` and `parse_version` as they stood before R6, with the
//! test-only work counters removed. `drive` runs `on_ingress`'s `Idle` and
//! `Head` arms over a sequence of reads with either head path, so the two runs
//! differ only below `ingress_head`. The tests compare every callback, every
//! ingress result, and every codec field that the head path reads or writes,
//! over generated heads, valid and malformed, in both directions, read whole
//! and split.

use std::borrow::Cow;

use roxygen_codec::CodecEvent;
use roxygen_codec::CodecIngressResult;
use roxygen_codec::EgressFrame;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::Method;
use roxygen_core::http::TransportDirection;
use roxygen_core::message::HttpMessage;
use roxygen_core::rfc2616;

use super::ALL_FROZEN_HTTP_METHODS;
use super::FROZEN_HTTP_METHODS;
use super::HEAD_RESERVE_CAP;
use super::HeadScan;
use super::HeadScanStop;
use super::HeaderLines;
use super::Http1xCodec;
use super::INITIAL_PREFIX_LIMIT;
use super::IngressBody;
use super::KeepaliveRequested;
use super::KnownLineFeeds;
use super::LineFeedPositions;
use super::MAX_HEADER_BYTES;
use super::ParseErrorKind;
use super::ParseState;
use super::ParsedHead;
use super::UPSTREAM_PREFIX_LIMIT;
use super::head_scan::Generator;
use super::head_scan::generated_heads;
use super::header_name_end;
use super::helper_work;
use super::is_frozen_http_method;
use super::parse_decimal_u64;
use super::server_accepted_upgrade;
use super::trim_http_whitespace;
use super::trim_trailing_cr;
use super::websocket_accept;
use super::word_scan;

const QUEUE_LIMIT: usize = 1 << 20;

// ---------------------------------------------------------------------------
// The head path before R6, frozen.
// ---------------------------------------------------------------------------

/// `valid_initial_prefix` of a head that grows one byte at a time, with each
/// byte read once. Downstream it keeps the frozen methods that the method token
/// still prefixes and whether a delimiter has ended the token; upstream, which
/// byte of `HTTP/` comes next. A prefix that fails stays failed: every longer
/// head holds the same mismatch.
struct OldPrefixMatch {
    upstream: bool,
    length: usize,
    candidates: u32,
    delimited: bool,
    valid: bool,
}

impl OldPrefixMatch {
    fn new(upstream: bool) -> Self {
        Self {
            upstream,
            length: 0,
            candidates: ALL_FROZEN_HTTP_METHODS,
            delimited: false,
            valid: true,
        }
    }

    /// Appends `byte` to the head, and returns whether the head passes
    /// `valid_initial_prefix`.
    #[inline]
    fn push(&mut self, byte: u8) -> bool {
        let index = self.length;
        self.length += 1;
        if !self.valid {
            return false;
        }
        if self.upstream {
            self.valid = b"HTTP/".get(index).is_none_or(|expected| *expected == byte);
        } else if !self.delimited {
            if matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
                self.delimited = true;
            } else {
                let mut remaining = self.candidates;
                while remaining != 0 {
                    let method = remaining.trailing_zeros() as usize;
                    remaining &= remaining - 1;
                    if FROZEN_HTTP_METHODS[method].get(index) != Some(&byte) {
                        self.candidates &= !(1 << method);
                    }
                }
                self.valid = self.candidates != 0;
            }
        }
        self.valid
    }
}

/// Scans `segment[from..]` as the continuation of a head whose first
/// bytes, at least one, are `buffered` and then `segment[..from]`. Each
/// byte gets the `ParseState::Head` checks in order: the size limit, the
/// start-line prefix, the terminator at an LF, and then control bytes. The
/// terminator is CRLFCRLF or LFLF, or downstream an HTTP/0.9 request line,
/// which can first hold at the head's first LF.
///
/// The prefix check reads each head byte inside its window once per call,
/// the bytes before `from` included. The scan also returns the number of
/// LFs in `segment[from..end]`, each of which it visits.
fn old_scan_head_counting_line_feeds(
    codec: &Http1xCodec,
    buffered: &[u8],
    segment: &[u8],
    from: usize,
) -> (HeadScan, usize) {
    let base = buffered.len();
    let head_byte = |index: usize| {
        if index < base {
            buffered[index]
        } else {
            segment[index - base]
        }
    };
    let limit = segment.len().min(MAX_HEADER_BYTES.saturating_sub(base));
    let prefix_limit = if codec.direction.is_upstream() {
        UPSTREAM_PREFIX_LIMIT
    } else {
        INITIAL_PREFIX_LIMIT
    };
    let mut prefix = OldPrefixMatch::new(codec.direction.is_upstream());
    let mut push_prefix = |byte: &u8| prefix.push(*byte);
    if base + from < prefix_limit {
        for byte in buffered.iter().chain(&segment[..from]) {
            push_prefix(byte);
        }
    }
    let mut line_feeds = 0;
    let mut earlier_newline = None;
    let mut position = from;
    while position < limit {
        let index = if base + position < prefix_limit {
            if !push_prefix(&segment[position]) {
                let scan = HeadScan {
                    end: position + 1,
                    stop: Some(HeadScanStop::InvalidStartLine),
                };
                return (scan, line_feeds);
            }
            position
        } else {
            let Some(offset) = word_scan::find_control(&segment[position..limit]) else {
                position = limit;
                break;
            };
            position + offset
        };
        position = index + 1;
        let stop = match segment[index] {
            b'\n' => {
                line_feeds += 1;
                let length = base + position;
                let terminated = (length >= 2 && head_byte(length - 2) == b'\n')
                    || (length >= 4
                        && head_byte(length - 4) == b'\r'
                        && head_byte(length - 3) == b'\n'
                        && head_byte(length - 2) == b'\r');
                let http_09 = codec.direction.is_downstream()
                    && !earlier_newline.replace(true).unwrap_or_else(|| {
                        buffered.contains(&b'\n') || segment[..from].contains(&b'\n')
                    })
                    && old_request_line_is_http_09(buffered, &segment[..index]);
                (terminated || http_09).then_some(HeadScanStop::Complete)
            }
            b'\r' | b'\t' => None,
            byte if byte < b' ' => Some(HeadScanStop::InvalidHeaderByte),
            _ => None,
        };
        if stop.is_some() {
            let scan = HeadScan {
                end: position,
                stop,
            };
            return (scan, line_feeds);
        }
    }
    if position < segment.len() {
        let scan = HeadScan {
            end: position + 1,
            stop: Some(HeadScanStop::TooLarge),
        };
        return (scan, line_feeds);
    }
    let scan = HeadScan {
        end: segment.len(),
        stop: None,
    };
    (scan, line_feeds)
}

/// Continues the head that `buffered` holds, or that begins at
/// `input[head_start]` when `buffered` is empty, through the rest of
/// `input`. The head's bytes before `result.consumed` are already taken. A
/// head that lies wholly in `input` is parsed in place.
fn old_ingress_head(
    codec: &mut Http1xCodec,
    mut buffered: Vec<u8>,
    input: &[u8],
    head_start: usize,
    callback: &mut dyn HttpCodecCallback,
    callback_egress: &mut BufQueue,
    result: &mut CodecIngressResult,
) {
    let segment = &input[head_start..];
    let from = result.consumed - head_start;
    let (scan, scanned_line_feeds) =
        old_scan_head_counting_line_feeds(codec, &buffered, segment, from);
    result.consumed = head_start + scan.end;
    let accepted = &segment[..scan.end];
    let message: &'static [u8] = match scan.stop {
        None => {
            buffered.extend_from_slice(accepted);
            codec.state = ParseState::Head(buffered);
            return;
        }
        Some(HeadScanStop::Complete) => {
            let parsed = if buffered.is_empty() {
                let line_feeds = scanned_line_feeds
                    + segment[..from]
                        .iter()
                        .filter(|byte| **byte == b'\n')
                        .count();
                old_parse_head(codec, accepted, Some(line_feeds))
            } else {
                buffered.extend_from_slice(accepted);
                old_parse_head(codec, &buffered, None)
            };
            match parsed {
                Ok(parsed) => {
                    codec.install_parsed_head(parsed, callback, callback_egress, result);
                }
                Err((kind, message)) => {
                    if buffered.is_empty() {
                        buffered = accepted.to_vec();
                    }
                    codec.state = ParseState::Head(buffered);
                    if codec.current_message.is_none() {
                        codec.current_message = Some(HttpMessage::new());
                    }
                    codec.fail(kind, message, callback, callback_egress, result);
                }
            }
            return;
        }
        Some(HeadScanStop::TooLarge) => b"HTTP header block is too large",
        Some(HeadScanStop::InvalidStartLine) => b"invalid HTTP start line",
        Some(HeadScanStop::InvalidHeaderByte) => b"invalid HTTP header byte",
    };
    buffered.extend_from_slice(accepted);
    codec.state = ParseState::Head(buffered);
    codec.fail(
        ParseErrorKind::Header,
        message,
        callback,
        callback_egress,
        result,
    );
}

/// Parses the complete head `raw`, which holds `line_feeds` LFs when the
/// scan counted them.
fn old_parse_head(
    codec: &mut Http1xCodec,
    raw: &[u8],
    line_feeds: Option<usize>,
) -> Result<ParsedHead, (ParseErrorKind, &'static [u8])> {
    let mut message = codec.current_message.take().unwrap_or_default();
    let line_feeds = line_feeds.unwrap_or_else(|| word_scan::count_lf(raw));
    // A head with N header lines has N + 2 LFs, so this leaves room for its
    // headers and an inferred Host. Folds and escaped newlines only
    // over-count, up to the cap.
    let reserve = line_feeds.saturating_sub(1).min(HEAD_RESERVE_CAP);
    codec.head_reserve = Some((reserve, message.headers().len()));
    message.headers_mut().reserve(reserve);
    let mut lines = old_header_lines(raw);
    let start_line = trim_trailing_cr(lines.next().unwrap_or_default());
    let mut inferred_host = None;
    let (version, informational) = if codec.direction.is_downstream() {
        let mut words = start_line.split(|byte| matches!(*byte, b' ' | b'\t'));
        let method = words.next().filter(|word| !word.is_empty()).ok_or((
            ParseErrorKind::Header,
            b"invalid HTTP request line".as_slice(),
        ))?;
        let url = words.next().filter(|word| !word.is_empty()).ok_or((
            ParseErrorKind::Header,
            b"invalid HTTP request line".as_slice(),
        ))?;
        let version = match words.next() {
            None => (0, 9),
            Some(version) => old_parse_version(version).ok_or((
                ParseErrorKind::Header,
                b"invalid HTTP request version".as_slice(),
            ))?,
        };
        if words.next().is_some() || !is_frozen_http_method(method) {
            codec.current_message = Some(message);
            return Err((ParseErrorKind::Header, b"invalid HTTP request line"));
        }
        let method = match HttpMethod::from_bytes(method) {
            Some(method) => Method::from(method),
            None => Method::from_name(String::from_utf8_lossy(method).into_owned()),
        };
        message.set_method(method).map_err(|_| {
            (
                ParseErrorKind::Header,
                b"invalid HTTP request method".as_slice(),
            )
        })?;
        let parsed = message.set_url(url, codec.strict_validation).map_err(|_| {
            (
                ParseErrorKind::Header,
                b"invalid HTTP request URL".as_slice(),
            )
        })?;
        if codec.strict_validation && !parsed.valid() {
            codec.current_message = Some(HttpMessage::new());
            return Err((ParseErrorKind::Header, b"invalid HTTP request URL"));
        }
        if parsed.has_host() {
            inferred_host = Some(parsed.host_and_port());
        }
        (version, false)
    } else {
        let mut words = start_line.splitn(3, |byte| *byte == b' ');
        let version = old_parse_version(words.next().unwrap_or_default()).ok_or((
            ParseErrorKind::Header,
            b"invalid HTTP response version".as_slice(),
        ))?;
        let status = parse_decimal_u64(words.next().unwrap_or_default())
            .filter(|status| *status <= 999)
            .ok_or((
                ParseErrorKind::Header,
                b"invalid HTTP response status".as_slice(),
            ))? as u16;
        message
            .set_status_code(status)
            .map_err(|_| (ParseErrorKind::Header, b"invalid HTTP response".as_slice()))?;
        message
            .set_status_message(words.next().unwrap_or_default())
            .map_err(|_| (ParseErrorKind::Header, b"invalid HTTP response".as_slice()))?;
        (version, status / 100 == 1)
    };
    message.set_http_version(version.0, version.1);

    let mut previous: Option<(&[u8], Cow<'_, [u8]>)> = None;
    for raw_line in lines {
        let line = trim_trailing_cr(raw_line);
        if line.is_empty() {
            break;
        }
        if matches!(line.first(), Some(b' ' | b'\t')) {
            let Some((_, value)) = previous.as_mut() else {
                codec.current_message = Some(message);
                return Err((ParseErrorKind::Header, b"invalid folded header"));
            };
            let value = value.to_mut();
            value.push(b' ');
            value.extend_from_slice(trim_http_whitespace(line));
            continue;
        }
        if let Some((name, value)) = previous.take() {
            old_add_validated_header(codec, &mut message, name, &value)?;
        }
        let Some((colon, valid_name)) = header_name_end(line) else {
            codec.current_message = Some(message);
            return Err((ParseErrorKind::Header, b"invalid HTTP header"));
        };
        if !valid_name {
            codec.current_message = Some(message);
            return Err((ParseErrorKind::Header, b"invalid HTTP header name"));
        }
        previous = Some((
            &line[..colon],
            Cow::Borrowed(trim_http_whitespace(&line[colon + 1..])),
        ));
    }
    if let Some((name, value)) = previous {
        old_add_validated_header(codec, &mut message, name, &value)?;
    }

    let mut first: Option<&[u8]> = None;
    let mut valid = true;
    if message.headers().exists(HttpHeaderCode::ContentLength) {
        for (code, _, value) in message.headers().iter() {
            if code != HttpHeaderCode::ContentLength {
                continue;
            }
            if first.is_some_and(|existing| existing != value) {
                valid = false;
                break;
            }
            first = Some(value);
        }
    }
    if !valid {
        codec.current_message = Some(message);
        return Err((
            ParseErrorKind::Header,
            b"conflicting Content-Length headers",
        ));
    }
    let content_length = first
        .map(|value| {
            parse_decimal_u64(value)
                .ok_or((ParseErrorKind::Header, b"invalid Content-Length".as_slice()))
        })
        .transpose()?;
    let mut chunked = false;
    let mut valid_transfer_encoding = true;
    message
        .headers()
        .for_each_value(HttpHeaderCode::TransferEncoding, |value| {
            if value.eq_ignore_ascii_case(b"chunked") {
                chunked = true;
                false
            } else {
                valid_transfer_encoding = false;
                true
            }
        });
    if !valid_transfer_encoding || (chunked && content_length.is_some()) {
        codec.current_message = Some(message);
        return Err((ParseErrorKind::Header, b"invalid Transfer-Encoding"));
    }
    message.set_chunked(chunked);
    // As frozen `onHeadersComplete`: after the last wire header and the
    // Content-Length and Transfer-Encoding checks, replacing every wire Host.
    if let Some(host) = inferred_host {
        message.headers_mut().set(HttpHeaderCode::Host, host);
    }

    let status = if codec.direction.is_upstream() {
        message.status_code().unwrap_or_default()
    } else {
        0
    };
    // `DIV-030`: under the option a downstream CONNECT is an ordinary request, with no
    // tunnel and no keep-alive change.
    let ordinary = codec.downstream_upgrade_as_ordinary && codec.direction.is_downstream();
    if ordinary {
        codec.last_ordinary_request = codec.ingress_stream;
    }
    codec.connect_request = if codec.direction.is_downstream() {
        !ordinary && message.method().ok().flatten() == Some(HttpMethod::Connect)
    } else {
        codec.connect_request
    };
    codec.head_request = if codec.direction.is_downstream() {
        message.method().ok().flatten() == Some(HttpMethod::Head)
    } else {
        codec.head_request
    };
    if codec.direction.is_downstream() {
        codec.may_chunk_egress = version.0 == 1 && version.1 >= 1;
    }

    let upgrade = message
        .headers()
        .get_single_or_empty(HttpHeaderCode::Upgrade);
    // `DIV-030`: so is its `Upgrade` header an ordinary header.
    let websocket = !ordinary && upgrade.eq_ignore_ascii_case(b"websocket");
    codec.keepalive &= ordinary || upgrade.is_empty();
    codec.keepalive &= !codec.connect_request;
    if websocket {
        message.set_ingress_websocket_upgrade();
        if codec.direction.is_upstream() {
            let accept = message
                .headers()
                .get_single_or_empty(HttpHeaderCode::SecWebSocketAccept);
            if accept != codec.websocket_accept_key.as_slice() {
                codec.current_message = Some(message);
                return Err((ParseErrorKind::Header, b"invalid websocket accept key"));
            }
        } else {
            if !codec.websocket_accept_key.is_empty() {
                codec.current_message = Some(message);
                return Err((ParseErrorKind::Header, b"duplicate websocket upgrade"));
            }
            let key = message
                .headers()
                .get_single_or_empty(HttpHeaderCode::SecWebSocketKey);
            codec.websocket_accept_key = websocket_accept(key);
        }
    }

    if codec.direction.is_upstream() {
        if codec.connect_request && (200..300).contains(&status) {
            codec.ingress_upgrade = true;
        } else if status == 101 {
            let server_upgrade = message
                .headers()
                .get_single_or_empty(HttpHeaderCode::Upgrade);
            if !server_accepted_upgrade(&codec.upgrade_header, server_upgrade) {
                codec.current_message = Some(message);
                return Err((ParseErrorKind::Header, b"upgrade protocol mismatch"));
            }
            codec.ingress_upgrade = true;
            codec.egress_upgrade = true;
        }
        if status != 100 {
            codec.upgrade_header.clear();
        }
    } else if codec.connect_request {
        codec.ingress_upgrade = true;
    }
    message.set_upgraded(codec.ingress_upgrade);

    let message_keepalive = message.compute_keepalive();
    message.set_wants_keepalive(message_keepalive);
    codec.keepalive &= message_keepalive;
    if codec.direction.is_downstream() {
        codec.keepalive_requested = if message_keepalive
            && version == (1, 0)
            && matches!(
                codec.keepalive_requested,
                KeepaliveRequested::Unset | KeepaliveRequested::Enabled
            ) {
            KeepaliveRequested::Enabled
        } else {
            KeepaliveRequested::Disabled
        };
    }
    if codec.user_agent.is_empty() {
        codec.user_agent = message
            .headers()
            .get_single_or_empty(HttpHeaderCode::UserAgent)
            .to_vec();
    }
    message.set_ingress_header_size(HttpHeaderSize {
        compressed: u32::try_from(raw.len()).unwrap_or(u32::MAX),
        uncompressed: u32::try_from(raw.len()).unwrap_or(u32::MAX),
        compressed_block: 0,
    });

    codec.informational_response = informational;
    let ignore_body = codec.direction.is_upstream()
        && (codec.expect_no_response_body
            || rfc2616::response_body_must_be_empty(u32::from(status)));
    let body = if codec.ingress_upgrade || ignore_body {
        IngressBody::None
    } else if chunked {
        IngressBody::Chunked
    } else if let Some(length) = content_length {
        if length == 0 {
            IngressBody::None
        } else {
            IngressBody::Fixed(length)
        }
    } else if codec.direction.is_upstream() {
        IngressBody::UntilEof
    } else {
        IngressBody::None
    };
    Ok(ParsedHead {
        message,
        body,
        informational,
        upgrade: codec.ingress_upgrade,
    })
}

fn old_add_validated_header(
    codec: &mut Http1xCodec,
    message: &mut HttpMessage,
    name: &[u8],
    value: &[u8],
) -> Result<(), (ParseErrorKind, &'static [u8])> {
    if !old_valid_header_value(value, codec.strict_validation) {
        codec.current_message = Some(message.clone());
        return Err((
            ParseErrorKind::HeaderContent,
            b"invalid HTTP header content",
        ));
    }
    message.headers_mut().add(name, value);
    Ok(())
}

fn old_parse_version(value: &[u8]) -> Option<(u8, u8)> {
    let value = value.strip_prefix(b"HTTP/")?;
    let separator = value.iter().position(|byte| *byte == b'.')?;
    let (major, minor_with_separator) = value.split_at(separator);
    let minor = minor_with_separator.get(1..)?;
    let major = parse_decimal_u64(major)?;
    let minor = parse_decimal_u64(minor)?;
    Some((u8::try_from(major).ok()?, u8::try_from(minor).ok()?))
}

/// Splits a header block at each LF except an escaped newline, an LF after
/// backslash, CR, backslash, which stays in its line. It yields at least one
/// line, and an empty line after a final LF.
fn old_header_lines(value: &[u8]) -> OldHeaderLines<'_> {
    OldHeaderLines {
        value,
        start: 0,
        finished: false,
    }
}

struct OldHeaderLines<'a> {
    value: &'a [u8],
    start: usize,
    finished: bool,
}

impl<'a> Iterator for OldHeaderLines<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.finished {
            return None;
        }
        let value = self.value;
        let mut search = self.start;
        while let Some(offset) = word_scan::find_lf(&value[search..]) {
            let index = search + offset;
            search = index + 1;
            if index < 3 || value[index - 3..index] != *b"\\\r\\" {
                let line = &value[self.start..index];
                self.start = search;
                return Some(line);
            }
        }
        self.finished = true;
        Some(&value[self.start..])
    }
}

/// Whether the request line that ends a head at its first LF, `first` followed
/// by `second`, is HTTP/0.9. Split at each SP and TAB after one trailing CR is
/// trimmed, it holds two words, the first nonempty, or three, the last
/// `HTTP/0.9`.
fn old_request_line_is_http_09(first: &[u8], second: &[u8]) -> bool {
    let byte = |index: usize| {
        if index < first.len() {
            first[index]
        } else {
            second[index - first.len()]
        }
    };
    let mut length = first.len() + second.len();
    if length > 0 && byte(length - 1) == b'\r' {
        length -= 1;
    }
    let mut delimiters = (0..length).filter(|index| matches!(byte(*index), b' ' | b'\t'));
    let Some(method_end) = delimiters.next() else {
        return false;
    };
    let Some(url_end) = delimiters.next() else {
        return method_end > 0;
    };
    if delimiters.next().is_some() {
        return false;
    }
    let version = url_end + 1..length;
    version.len() == b"HTTP/0.9".len()
        && version
            .zip(b"HTTP/0.9")
            .all(|(index, expected)| byte(index) == *expected)
}

/// Whether `value` passes `old_valid_header_value_exact`. A value of only visible
/// ASCII and spaces passes in both modes without the per-byte check.
#[inline]
fn old_valid_header_value(value: &[u8], strict: bool) -> bool {
    let (words, tail) = value.as_chunks::<8>();
    let visible = words.iter().all(|word| {
        let word = u64::from_le_bytes(*word);
        (word_scan::has_less(word, b' ')
            | (word & word_scan::HIGH)
            | word_scan::has_zero(word ^ word_scan::repeat(0x7f)))
            == 0
    }) && tail.iter().all(|byte| matches!(*byte, b' '..=b'~'));
    visible || old_valid_header_value_exact(value, strict)
}

fn old_valid_header_value_exact(value: &[u8], strict: bool) -> bool {
    value.iter().enumerate().all(|(index, byte)| {
        if *byte >= 0x80 {
            return !strict;
        }
        match *byte {
            b'\t' | b' '..=b'~' => true,
            b'\r' if !strict => {
                index != 0 && value[index - 1] == b'\\' && value.get(index + 1) == Some(&b'\\')
            }
            b'\n' if !strict => index != 0 && value[index - 1] == b'\\',
            _ => false,
        }
    })
}

// ---------------------------------------------------------------------------
// The driver, the observations, and the corpus.
// ---------------------------------------------------------------------------

/// `rendered` without the creation times of the messages in it.
fn without_start_times(rendered: String) -> String {
    const START: &str = "start_time: Instant {";
    let mut output = String::with_capacity(rendered.len());
    let mut rest = rendered.as_str();
    while let Some(at) = rest.find(START) {
        output.push_str(&rest[..at]);
        let after = &rest[at..];
        let end = after
            .find('}')
            .expect("an Instant renders its fields in braces");
        output.push_str("start_time: _");
        rest = &after[end + 1..];
    }
    output.push_str(rest);
    output
}

/// Renders every callback the head path dispatches.
#[derive(Default)]
struct Recorder {
    events: Vec<String>,
}

impl HttpCodecCallback for Recorder {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        let rendered = match event {
            CodecEvent::MessageBegin { stream, message } => {
                format!("begin {stream:?} {message:?}")
            }
            CodecEvent::HeadersComplete { stream, message } => {
                format!("headers {stream:?} {message:?}")
            }
            CodecEvent::MessageComplete { stream, upgraded } => {
                format!("complete {stream:?} {upgraded}")
            }
            CodecEvent::Error {
                stream,
                error,
                new_stream,
            } => format!("error {stream:?} {error:?} {new_stream}"),
            _ => "unexpected event".to_owned(),
        };
        self.events.push(without_start_times(rendered));
    }
}

/// Every codec field that the head path reads or writes.
fn snapshot(codec: &Http1xCodec) -> String {
    let head_capacity = match &codec.state {
        ParseState::Head(head) => Some(head.capacity()),
        _ => None,
    };
    without_start_times(format!(
        "state={:?} head_capacity={head_capacity:?} current={:?} ingress={:?} egress={:?} \
         paused={} error={} request_pending={} response_pending={} keepalive={} \
         requested={:?} connect={} head={} no_body={} may_chunk={} informational={} \
         ingress_upgrade={} upgrade_complete={} egress_upgrade={} upgrade={:?} accept={:?} \
         user_agent={:?} ordinary={:?} reserve={:?}",
        codec.state,
        codec.current_message,
        codec.ingress_stream,
        codec.egress_stream,
        codec.parser_paused,
        codec.parser_error,
        codec.request_pending,
        codec.response_pending,
        codec.keepalive,
        codec.keepalive_requested,
        codec.connect_request,
        codec.head_request,
        codec.expect_no_response_body,
        codec.may_chunk_egress,
        codec.informational_response,
        codec.ingress_upgrade,
        codec.ingress_upgrade_complete,
        codec.egress_upgrade,
        codec.upgrade_header.escape_ascii().to_string(),
        codec.websocket_accept_key.escape_ascii().to_string(),
        codec.user_agent.escape_ascii().to_string(),
        codec.last_ordinary_request,
        codec.head_reserve,
    ))
}

/// `on_ingress`'s `Idle` and `Head` arms over one read, with the head path
/// before R6 (`old`) or the current one. Parsing stops at any other state.
fn drive(
    codec: &mut Http1xCodec,
    input: &[u8],
    old: bool,
    callback: &mut Recorder,
    callback_egress: &mut BufQueue,
) -> CodecIngressResult {
    let mut result = CodecIngressResult::new(0);
    if codec.parser_error || codec.parser_paused {
        return result;
    }
    while result.consumed < input.len() && !codec.parser_paused && !codec.parser_error {
        let state = std::mem::take(&mut codec.state);
        match state {
            ParseState::Idle => {
                let Some(skipped) = input[result.consumed..]
                    .iter()
                    .position(|byte| !matches!(*byte, b'\r' | b'\n'))
                else {
                    result.consumed = input.len();
                    continue;
                };
                let head_start = result.consumed + skipped;
                let byte = input[head_start];
                result.consumed = head_start + 1;
                codec.begin_message(callback, callback_egress, &mut result);
                if !codec.valid_initial_prefix(&[byte]) {
                    codec.state = ParseState::Head(vec![byte]);
                    codec.fail(
                        ParseErrorKind::Header,
                        b"invalid HTTP start line",
                        callback,
                        callback_egress,
                        &mut result,
                    );
                } else if codec.parser_paused {
                    codec.state = ParseState::Head(vec![byte]);
                } else if old {
                    old_ingress_head(
                        codec,
                        Vec::new(),
                        input,
                        head_start,
                        callback,
                        callback_egress,
                        &mut result,
                    );
                } else {
                    codec.ingress_head(
                        Vec::new(),
                        input,
                        head_start,
                        callback,
                        callback_egress,
                        &mut result,
                    );
                }
            }
            ParseState::Head(head) => {
                let head_start = result.consumed;
                if old {
                    old_ingress_head(
                        codec,
                        head,
                        input,
                        head_start,
                        callback,
                        callback_egress,
                        &mut result,
                    );
                } else {
                    codec.ingress_head(
                        head,
                        input,
                        head_start,
                        callback,
                        callback_egress,
                        &mut result,
                    );
                }
            }
            state => {
                codec.state = state;
                break;
            }
        }
    }
    result
}

/// The request an upstream codec sent before the head it parses.
#[derive(Clone, Copy, Debug)]
enum Sent {
    Get,
    Head,
    Connect,
    Websocket,
    H2cUpgrade,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    direction: TransportDirection,
    strict: bool,
    ordinary: bool,
    sent: Sent,
}

fn configs() -> Vec<Config> {
    let mut configs = Vec::new();
    for strict in [false, true] {
        for ordinary in [false, true] {
            configs.push(Config {
                direction: TransportDirection::Downstream,
                strict,
                ordinary,
                sent: Sent::Get,
            });
        }
        for sent in [
            Sent::Get,
            Sent::Head,
            Sent::Connect,
            Sent::Websocket,
            Sent::H2cUpgrade,
        ] {
            configs.push(Config {
                direction: TransportDirection::Upstream,
                strict,
                ordinary: false,
                sent,
            });
        }
    }
    configs
}

fn codec_for(config: Config) -> Http1xCodec {
    let mut codec = Http1xCodec::new(config.direction);
    codec.set_strict_validation(config.strict);
    codec.set_downstream_upgrade_as_ordinary(config.ordinary);
    if config.direction.is_upstream() {
        let stream = codec.create_stream();
        let mut request = HttpMessage::new();
        let method = match config.sent {
            Sent::Head => HttpMethod::Head,
            Sent::Connect => HttpMethod::Connect,
            _ => HttpMethod::Get,
        };
        request.set_method(method).expect("a method");
        let url: &[u8] = match config.sent {
            Sent::Connect => b"example.test:443",
            _ => b"/",
        };
        request.set_url(url, false).expect("a URL");
        request.set_http_version(1, 1);
        match config.sent {
            Sent::Websocket => request.set_egress_websocket_upgrade(),
            Sent::H2cUpgrade => request.headers_mut().add(HttpHeaderCode::Upgrade, b"h2c"),
            _ => {}
        }
        let mut output = BufQueue::new(QUEUE_LIMIT);
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message: &request,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut Recorder::default(),
            )
            .expect("the request is generated");
        if matches!(config.sent, Sent::Websocket) {
            // The generated key is random; both runs expect the accept value
            // of the sample key instead.
            codec.websocket_accept_key = websocket_accept(b"dGhlIHNhbXBsZSBub25jZQ==");
        }
    }
    codec
}

/// Runs `reads` through a fresh codec of `config` with each head path and
/// asserts that every observation matches.
fn assert_paths_match(config: Config, reads: &[&[u8]]) {
    let mut runs = [false, true].map(|old| {
        let mut codec = codec_for(config);
        let mut recorder = Recorder::default();
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let mut observations = Vec::new();
        for read in reads {
            let result = drive(&mut codec, read, old, &mut recorder, &mut egress);
            observations.push(format!(
                "{result:?} {:?} {}",
                std::mem::take(&mut recorder.events),
                snapshot(&codec)
            ));
        }
        observations
    });
    let current = runs[1].split_off(0);
    let frozen = runs[0].split_off(0);
    if current != frozen {
        let head = reads.concat();
        for (index, (current, frozen)) in current.iter().zip(&frozen).enumerate() {
            assert_eq!(
                current,
                frozen,
                "{config:?} read {index} of {} ({:?}), head {:?}",
                reads.len(),
                reads.iter().map(|read| read.len()).collect::<Vec<_>>(),
                head[..head.len().min(400)].escape_ascii().to_string()
            );
        }
    }
}

const START_LINES: &[&[u8]] = &[
    b"GET / HTTP/1.1",
    b"POST /generate HTTP/1.1",
    b"HEAD /x HTTP/1.0",
    b"CONNECT example.test:443 HTTP/1.1",
    b"OPTIONS * HTTP/1.1",
    b"M-SEARCH * HTTP/1.1",
    b"UNSUBSCRIBE /u HTTP/1.1",
    b"GET http://absolute.test/path?q=1 HTTP/1.1",
    b"GET /old",
    b"GET /old HTTP/0.9",
    b"GET\t/tab\tHTTP/1.1",
    b"GET /two  spaces HTTP/1.1",
    b"GET / HTTP/1.1 extra",
    b"GET / HTTP/2.0",
    b"GET / HTTP/1.10",
    b"GET / HTTP/01.1",
    b"GET / HTTP/256.1",
    b"GET / HTTP/1.",
    b"GET / HTTP/.1",
    b"GET / HTTP/1",
    b"GET / http/1.1",
    b"GET / HTTP/1.1\r",
    b"GETX / HTTP/1.1",
    b"PATCH",
    b"GET ",
    b" / HTTP/1.1",
    b"GET /bad\x7furl HTTP/1.1",
    b"GET /\xffurl HTTP/1.1",
    b"HTTP/1.1 200 OK",
    b"HTTP/1.1 101 Switching Protocols",
    b"HTTP/1.1 100 Continue",
    b"HTTP/1.0 204 No Content",
    b"HTTP/1.1 304 Not Modified",
    b"HTTP/1.1 200",
    b"HTTP/1.1 200 ",
    b"HTTP/1.1 999 Max",
    b"HTTP/1.1 1000 Over",
    b"HTTP/1.1 2x0 Bad",
    b"HTTP/1.1  200 Double",
    b"HTTP/1.1\t200 Tab",
    b"HTTP/2 200 OK",
    b"HTTP/1.1 200 Reason with \x80 high",
    b"HTTP/",
    b"HTTX/1.1 200 OK",
];

const HEADER_LINES: &[&[u8]] = &[
    b"Host: example.test",
    b"host: forwarder",
    b"Host: other.test:8080",
    b"content-type: application/json",
    b"Content-Type: text/event-stream; charset=utf-8",
    b"x-stream: true",
    b"x-rid: rxbare-7",
    b"x-routing-key: key-7",
    b"x-input-token-count: 512",
    b"Content-Length: 0",
    b"Content-Length: 10",
    b"content-length: 10",
    b"Content-Length: 11",
    b"Content-Length: x",
    b"Content-Length: 18446744073709551616",
    b"Transfer-Encoding: chunked",
    b"Transfer-Encoding: gzip",
    b"Transfer-Encoding: CHUNKED",
    b"Connection: keep-alive",
    b"Connection: close",
    b"Connection: Upgrade",
    b"Upgrade: websocket",
    b"Upgrade: h2c",
    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
    b"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
    b"User-Agent: client/1.0",
    b"cache-control: no-cache",
    b"date: Fri, 06 Nov 2026 00:00:00 GMT",
    b"X-Empty:",
    b"X-Spaces:    padded value   ",
    b"X-Tab:\tvalue\t",
    b"X-Inner-Tab: a\tb",
    b"X-Long: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    b"X-Seven: 1234567",
    b"X-Eight: 12345678",
    b"X-Nine: 123456789",
    b"X-Del: a\x7fb",
    b"X-High: caf\xc3\xa9",
    b"X-High-Word: 0123456\x80",
    b"X-Escaped: a\\\r\\\nb",
    b"X-Escaped-Tail: value\\\r\\",
    b"X-Backslash-Lf: a\\\nb",
    b"X-Stray-Cr: a\rb",
    b"  folded continuation",
    b"\tfolded with tab",
    b"No colon here",
    b": empty name",
    b"Bad Name: value",
    b"Bad\"Name: value",
    b"X-Tilde~!#$%&'*+-.^_`|: token",
    b"x:y",
    b"X-Colon: a:b:c",
];

/// Heads built from start lines and header lines, ended by CRLF or LF in
/// varying mixes, with and without a terminator, and with single-byte
/// mutations.
fn structured_heads() -> Vec<Vec<u8>> {
    let mut generator = Generator(0x6a6b);
    let mut heads = Vec::new();
    let end = |generator: &mut Generator| -> &'static [u8] {
        match generator.next(8) {
            0 => b"\n",
            _ => b"\r\n",
        }
    };
    for _ in 0..2_400 {
        let mut head = START_LINES[generator.next(START_LINES.len())].to_vec();
        head.extend_from_slice(end(&mut generator));
        let lines = match generator.next(10) {
            0 => 33 + generator.next(40),
            _ => generator.next(14),
        };
        for _ in 0..lines {
            head.extend_from_slice(HEADER_LINES[generator.next(HEADER_LINES.len())]);
            head.extend_from_slice(end(&mut generator));
        }
        match generator.next(10) {
            0 => {}
            1 => head.extend_from_slice(b"\n"),
            _ => head.extend_from_slice(end(&mut generator)),
        }
        if generator.next(4) == 0 {
            let at = generator.next(head.len());
            match generator.next(3) {
                0 => {
                    head[at] = [b'\r', b'\n', b'\\', b'\t', b' ', b':', 0x01, 0x7f, 0xff]
                        [generator.next(9)]
                }
                1 => {
                    head.remove(at);
                }
                _ => head.insert(at, b"\r\n\\ "[generator.next(4)]),
            }
        }
        heads.push(head);
    }
    for count in [31, 32, 33, 34, 63, 64, 65, 66, 100] {
        let mut head = b"GET /many HTTP/1.1\r\n".to_vec();
        for index in 0..count {
            head.extend_from_slice(format!("X-Header-{index}: value-{index}\r\n").as_bytes());
        }
        head.extend_from_slice(b"\r\n");
        heads.push(head.clone());
        let mut escaped = b"GET /many HTTP/1.1\r\n".to_vec();
        for index in 0..count {
            escaped
                .extend_from_slice(format!("X-Header-{index}: a\\\r\\\nb{index}\r\n").as_bytes());
        }
        escaped.extend_from_slice(b"\r\n");
        heads.push(escaped);
    }
    heads.push(b"POST /generate HTTP/1.1\r\nhost: forwarder\r\ncontent-type: application/json\r\nx-stream: true\r\nx-rid: rxbare-7\r\nx-routing-key: key-7\r\nx-prefix-hash: prefix-7\r\nx-input-token-count: 512\r\ncontent-length: 1081\r\n\r\n".to_vec());
    heads.push(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\ntransfer-encoding: chunked\r\ndate: Fri, 06 Nov 2026 00:00:00 GMT\r\n\r\n".to_vec());
    heads
}

/// The splits of `head` to run: whole, and for short heads every two-read
/// split; longer heads take a few splits, and the shortest heads also run one
/// byte per read.
fn splits(head: &[u8], generator: &mut Generator) -> Vec<Vec<usize>> {
    let mut splits = vec![Vec::new()];
    let points: Vec<usize> = if head.len() <= 160 {
        (1..head.len()).collect()
    } else {
        (0..4).map(|_| 1 + generator.next(head.len() - 1)).collect()
    };
    splits.extend(points.into_iter().map(|point| vec![point]));
    if head.len() <= 48 {
        splits.push((1..head.len()).collect());
    }
    splits
}

fn reads_at<'a>(head: &'a [u8], points: &[usize]) -> Vec<&'a [u8]> {
    let mut reads = Vec::new();
    let mut start = 0;
    for point in points {
        reads.push(&head[start..*point]);
        start = *point;
    }
    reads.push(&head[start..]);
    reads
}

#[test]
fn structured_heads_parse_as_the_frozen_head_path_does() {
    let mut generator = Generator(0x5157);
    let configs = configs();
    for head in structured_heads() {
        // Every configuration reads each head whole; one configuration, drawn
        // per head, also reads it split.
        let split_config = generator.next(configs.len());
        for (index, config) in configs.iter().enumerate() {
            assert_paths_match(*config, &[&head]);
            if index == split_config {
                for points in splits(&head, &mut generator) {
                    assert_paths_match(*config, &reads_at(&head, &points));
                }
            }
        }
    }
}

#[test]
fn generated_heads_parse_as_the_frozen_head_path_does() {
    let mut generator = Generator(0x9e37);
    let configs = configs();
    for head in generated_heads() {
        let config = configs[generator.next(configs.len())];
        let downstream = configs[generator.next(4)];
        for config in [config, downstream] {
            assert_paths_match(config, &[&head]);
            if head.len() <= 4_096 {
                for points in splits(&head, &mut generator).into_iter().take(12) {
                    assert_paths_match(config, &reads_at(&head, &points));
                }
            }
        }
    }
}

#[test]
fn pipelined_heads_parse_as_the_frozen_head_path_does() {
    let mut generator = Generator(0x31d);
    let heads = structured_heads();
    let configs = configs();
    for _ in 0..600 {
        let first = &heads[generator.next(heads.len())];
        let second = &heads[generator.next(heads.len())];
        let input = [first.as_slice(), b"\r\n", second.as_slice()].concat();
        let config = configs[generator.next(configs.len())];
        assert_paths_match(config, &[&input]);
        let point = 1 + generator.next(input.len() - 1);
        assert_paths_match(config, &reads_at(&input, &[point]));
    }
}

// ---------------------------------------------------------------------------
// Work counts of the current head path.
// ---------------------------------------------------------------------------

/// Reads `head` whole into a fresh codec of `direction`, which must parse it
/// as a complete head.
fn parse_whole(direction: TransportDirection, head: &[u8]) -> Http1xCodec {
    let mut codec = codec_for(Config {
        direction,
        strict: false,
        ordinary: false,
        sent: Sent::Get,
    });
    let input = BufQueue::copy_from_slice(head, head.len()).expect("a test head fits its queue");
    let mut recorder = Recorder::default();
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let result = codec.on_ingress(&input, &mut recorder, &mut egress);
    assert_eq!(result.consumed, head.len(), "{:?}", recorder.events);
    assert!(
        recorder
            .events
            .iter()
            .any(|event| event.starts_with("headers")),
        "{:?}",
        recorder.events
    );
    codec
}

#[test]
fn start_line_prefix_takes_one_method_step_per_token_byte() {
    for method in FROZEN_HTTP_METHODS {
        let head = [method, b" /x HTTP/1.1\r\nHost: a\r\n\r\n".as_slice()].concat();
        let before = helper_work(|work| &work.prefix_method_steps);
        parse_whole(TransportDirection::Downstream, &head);
        let steps = helper_work(|work| &work.prefix_method_steps) - before;
        assert!(
            steps <= method.len(),
            "{}: {steps} method steps for a {}-byte token",
            method.escape_ascii(),
            method.len()
        );
    }
}

/// Heads with no TAB, so that every control byte in them is a CR or an LF.
fn heads_without_tabs() -> Vec<(TransportDirection, Vec<u8>)> {
    let mut heads = vec![
        (
            TransportDirection::Downstream,
            b"POST /generate HTTP/1.1\r\nhost: forwarder\r\ncontent-type: application/json\r\nx-stream: true\r\nx-rid: rxbare-7\r\nx-routing-key: key-7\r\nx-prefix-hash: prefix-7\r\nx-input-token-count: 512\r\ncontent-length: 1081\r\n\r\n".to_vec(),
        ),
        (
            TransportDirection::Upstream,
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-cache\r\ntransfer-encoding: chunked\r\ndate: Fri, 06 Nov 2026 00:00:00 GMT\r\n\r\n".to_vec(),
        ),
        (TransportDirection::Downstream, b"GET / HTTP/1.1\nHost: a\n\n".to_vec()),
        (TransportDirection::Downstream, b"GET /\r\n".to_vec()),
        (TransportDirection::Upstream, b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()),
    ];
    for method in FROZEN_HTTP_METHODS {
        let head = [method, b" /x HTTP/1.1\r\nHost: a\r\n\r\n".as_slice()].concat();
        heads.push((TransportDirection::Downstream, head));
    }
    heads
}

#[test]
fn in_place_head_scan_takes_one_step_per_line_feed() {
    for (direction, head) in heads_without_tabs() {
        let codec = parse_whole(direction, &head);
        let line_feeds = head.iter().filter(|byte| **byte == b'\n').count();
        assert!(
            codec.head_work.scan_steps.get() <= line_feeds,
            "{direction:?} {:?}: {} scan steps for {line_feeds} LFs",
            head.escape_ascii().to_string(),
            codec.head_work.scan_steps.get()
        );
    }
}

#[test]
fn http_09_check_stops_at_the_second_delimiter() {
    for (head, delimiter_reads) in [
        (b"POST /generate HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(), 15),
        (b"GET / HTTP/1.1\r\nHost: a\r\n\r\n", 6),
        (b"M-SEARCH * HTTP/1.1\r\nHost: a\r\n\r\n", 11),
        (b"GET /old HTTP/0.9\r\n", 9),
    ] {
        let before = helper_work(|work| &work.http_09_delimiter_reads);
        parse_whole(TransportDirection::Downstream, head);
        let reads = helper_work(|work| &work.http_09_delimiter_reads) - before;
        assert!(
            reads <= delimiter_reads,
            "{:?}: {reads} delimiter reads, the second delimiter is byte {delimiter_reads}",
            head.escape_ascii().to_string()
        );
    }
}

#[test]
fn http_09_check_matches_the_frozen_check_at_every_split() {
    let mut lines: Vec<Vec<u8>> = START_LINES.iter().map(|line| line.to_vec()).collect();
    let mut words = vec![Vec::new()];
    for _ in 0..5 {
        words = words
            .iter()
            .flat_map(|word| {
                b"A /\tH0.9\r"
                    .iter()
                    .map(move |byte| [word.as_slice(), &[*byte]].concat())
            })
            .collect();
        lines.extend(words.iter().cloned());
    }
    for (method, url) in [
        (b"GET".as_slice(), b"/".as_slice()),
        (b"", b"/"),
        (b"G", b""),
    ] {
        for version in [
            b"HTTP/0.9".as_slice(),
            b"HTTP/0.9\r",
            b"HTTP/0.9 ",
            b"HTTP/0.9\t",
            b"HTTP/0.",
            b"HTTP/0.99",
            b"HTTP/1.1",
            b"",
        ] {
            for delimiter in [b" ".as_slice(), b"\t", b"  "] {
                lines.push([method, delimiter, url, delimiter, version].concat());
                lines.push([method, delimiter, url].concat());
            }
        }
    }
    for line in lines {
        for split in 0..=line.len() {
            let (first, second) = line.split_at(split);
            assert_eq!(
                super::request_line_is_http_09(first, second),
                old_request_line_is_http_09(first, second),
                "{:?} split at {split}",
                line.escape_ascii().to_string()
            );
        }
    }
}

#[test]
fn heads_parsed_in_place_are_split_into_lines_without_another_search() {
    for (direction, head) in heads_without_tabs() {
        let before = helper_work(|work| &work.line_search_bytes);
        parse_whole(direction, &head);
        let searched = helper_work(|work| &work.line_search_bytes) - before;
        assert_eq!(
            searched,
            0,
            "{direction:?} {:?}: {searched} bytes searched for LFs again",
            head.escape_ascii().to_string()
        );
    }
    // Control: a head read in two parts is parsed from its buffer, which is
    // searched as before.
    let (direction, head) = heads_without_tabs().swap_remove(0);
    let mut codec = codec_for(Config {
        direction,
        strict: false,
        ordinary: false,
        sent: Sent::Get,
    });
    let mut recorder = Recorder::default();
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    let before = helper_work(|work| &work.line_search_bytes);
    for read in [&head[..40], &head[40..]] {
        let input =
            BufQueue::copy_from_slice(read, read.len()).expect("a test read fits its queue");
        assert_eq!(
            codec
                .on_ingress(&input, &mut recorder, &mut egress)
                .consumed,
            read.len()
        );
    }
    assert!(
        recorder
            .events
            .iter()
            .any(|event| event.starts_with("headers"))
    );
    assert!(helper_work(|work| &work.line_search_bytes) > before);
}

#[test]
fn known_line_feeds_split_as_the_frozen_splitter_does() {
    let mut values: Vec<Vec<u8>> = structured_heads();
    let mut words = vec![Vec::new()];
    for _ in 0..7 {
        words = words
            .iter()
            .flat_map(|word| {
                b"a\\\r\n"
                    .iter()
                    .map(move |byte| [word.as_slice(), &[*byte]].concat())
            })
            .collect();
        values.extend(words.iter().cloned());
    }
    for value in values {
        let frozen = old_header_lines(&value).collect::<Vec<_>>();
        let positions = value
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'\n')
            .map(|(index, _)| u32::try_from(index).expect("a test value is short"))
            .collect::<Vec<_>>();
        // Every prefix of the LF positions, with the search resuming after the
        // last of them, and all of them with nothing left to search.
        let mut cases = (0..=positions.len())
            .map(|recorded| {
                let searched_to = match recorded {
                    0 => 0,
                    _ => positions[recorded - 1] as usize + 1,
                };
                (recorded, searched_to)
            })
            .collect::<Vec<_>>();
        cases.push((positions.len(), value.len()));
        for (recorded, searched_to) in cases {
            let known = KnownLineFeeds {
                positions: &positions[..recorded],
                searched_to,
            };
            assert_eq!(
                HeaderLines::with_known(&value, known).collect::<Vec<_>>(),
                frozen,
                "{recorded} recorded, searched to {searched_to}: {:?}",
                value.escape_ascii().to_string()
            );
        }
        // As a scan records them, past `RECORDED_LINE_FEEDS` included.
        let mut recorder = LineFeedPositions::new();
        for position in &positions {
            recorder.record(*position as usize);
        }
        assert_eq!(
            HeaderLines::with_known(&value, recorder.known(value.len())).collect::<Vec<_>>(),
            frozen,
            "recorded by a scan: {:?}",
            value.escape_ascii().to_string()
        );
    }
}

#[test]
fn visible_header_values_are_checked_a_word_at_a_time() {
    let alphabet = b" !09:;AZaz~\"\\";
    for length in 0..=40 {
        let value = (0..length)
            .map(|index| alphabet[index % alphabet.len()])
            .collect::<Vec<_>>();
        for strict in [false, true] {
            let before = helper_work(|work| &work.value_byte_checks);
            assert!(super::valid_header_value(&value, strict));
            let checks = helper_work(|work| &work.value_byte_checks) - before;
            assert_eq!(
                checks, 0,
                "a {length}-byte visible value took {checks} single-byte checks"
            );
        }
    }
}

#[test]
fn http_1_versions_are_recognized_without_parsing_their_numbers() {
    for (version, expected) in [(b"HTTP/1.1".as_slice(), (1, 1)), (b"HTTP/1.0", (1, 0))] {
        let before = helper_work(|work| &work.version_number_parses);
        assert_eq!(super::parse_version(version), Some(expected));
        let parses = helper_work(|work| &work.version_number_parses) - before;
        assert_eq!(
            parses,
            0,
            "{}: {parses} decimal parses",
            version.escape_ascii()
        );
    }
}

/// The rxbare forwarder's engine request head: five forwarded headers,
/// `content-length`, `accept`, `host`, and a token-less `Connection`.
fn engine_request() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Post).expect("a method");
    message
        .set_url(b"/generate".as_slice(), false)
        .expect("a URL");
    message.set_http_version(1, 1);
    let headers = message.headers_mut();
    headers.add("content-type", b"application/json");
    headers.add("x-rid", b"rxbare-7");
    headers.add("x-routing-key", b"key-7");
    headers.add("x-prefix-hash", b"prefix-7");
    headers.add("x-input-token-count", b"512");
    headers.add("content-length", b"1081");
    headers.add("accept", b"*/*");
    headers.add("host", b"127.0.0.1:40123");
    headers.add(HttpHeaderCode::Connection, b"");
    message
}

/// The rxbare forwarder's client response head.
fn client_response() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_http_version(1, 1);
    message.set_status_code(200).expect("a status");
    message
        .set_status_message(b"OK".as_slice())
        .expect("a reason");
    let headers = message.headers_mut();
    headers.add("content-type", b"text/event-stream; charset=utf-8");
    headers.add(HttpHeaderCode::Date, b"Fri, 06 Nov 2026 12:00:00 GMT");
    message
}

#[test]
fn generated_header_values_are_checked_for_cr_and_lf_a_word_at_a_time() {
    // The values the two heads check: the forwarded headers, `accept` and
    // `host` (Content-Length is written last and Connection is rebuilt), and
    // the response's `content-type` and `date`. A value takes at most one
    // load per whole word and one for its end.
    let checked: [&[u8]; 9] = [
        b"application/json",
        b"rxbare-7",
        b"key-7",
        b"prefix-7",
        b"512",
        b"*/*",
        b"127.0.0.1:40123",
        b"text/event-stream; charset=utf-8",
        b"Fri, 06 Nov 2026 12:00:00 GMT",
    ];
    let word_loads: usize = checked.iter().map(|value| value.len() / 8 + 1).sum();
    let request = engine_request();
    let response = client_response();
    let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
    let mut downstream = codec_for(Config {
        direction: TransportDirection::Downstream,
        strict: false,
        ordinary: false,
        sent: Sent::Get,
    });
    let input = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
    let input = BufQueue::copy_from_slice(input, input.len()).expect("a test head fits its queue");
    let _ = downstream.on_ingress(
        &input,
        &mut Recorder::default(),
        &mut BufQueue::new(QUEUE_LIMIT),
    );
    let before = helper_work(|work| &work.egress_cr_lf_loads);
    for (codec, message, stream) in [
        (&mut upstream, &request, None),
        (&mut downstream, &response, Some(super::StreamId::new(1))),
    ] {
        let stream = stream.unwrap_or_else(|| codec.create_stream());
        let mut output = BufQueue::new(QUEUE_LIMIT);
        codec
            .generate(
                &mut output,
                EgressFrame::Headers {
                    stream,
                    message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut Recorder::default(),
            )
            .expect("the head is generated");
    }
    let loads = helper_work(|work| &work.egress_cr_lf_loads) - before;
    assert!(
        loads <= word_loads,
        "{loads} CR/LF loads, at most {word_loads} words"
    );
}

/// Byte strings of every length up to 40 over `alphabet`, and each of them
/// with every byte of `edges` at every position.
fn edge_values(alphabet: &[u8], edges: &[u8]) -> Vec<Vec<u8>> {
    let mut values = Vec::new();
    for length in 0..=40 {
        let base = (0..length)
            .map(|index| alphabet[index % alphabet.len()])
            .collect::<Vec<_>>();
        values.push(base.clone());
        for position in 0..length {
            for edge in edges {
                let mut value = base.clone();
                value[position] = *edge;
                values.push(value);
            }
        }
    }
    values
}

#[test]
fn header_value_checks_match_the_frozen_checks() {
    let mut edges = (0_u8..=0x20).collect::<Vec<_>>();
    edges.extend([b'\\', 0x7e, 0x7f, 0x80, 0xc3, 0xff]);
    let mut values = edge_values(b"a\\ ~!", &edges);
    for value in edge_values(b"x\\", b"\r\n\\") {
        // Escaped newlines in every position, which the lenient check accepts.
        values.push(value);
    }
    let mut generator = Generator(0x7a1);
    for _ in 0..20_000 {
        let length = generator.next(48);
        values.push(
            (0..length)
                .map(|_| {
                    [b'a', b' ', b'\\', b'\r', b'\n', b'\t', 0x7f, 0x80, 0x1f][generator.next(9)]
                })
                .collect(),
        );
    }
    for value in values {
        for strict in [false, true] {
            assert_eq!(
                super::valid_header_value(&value, strict),
                old_valid_header_value(&value, strict),
                "strict={strict} {:?}",
                value.escape_ascii().to_string()
            );
        }
        assert_eq!(
            super::holds_cr_or_lf(&value),
            value.iter().any(|byte| matches!(*byte, b'\r' | b'\n')),
            "{:?}",
            value.escape_ascii().to_string()
        );
    }
}

#[test]
fn version_parse_matches_the_frozen_parse() {
    let mut versions: Vec<Vec<u8>> = Vec::new();
    for major in [
        "",
        "0",
        "1",
        "01",
        "2",
        "10",
        "255",
        "256",
        "x",
        "18446744073709551616",
    ] {
        for separator in ["", ".", ".."] {
            for minor in ["", "0", "1", "9", "00", "10", "255", "256", " ", "1 "] {
                for prefix in ["HTTP/", "http/", "HTTP", "HTTP//", ""] {
                    versions.push(format!("{prefix}{major}{separator}{minor}").into_bytes());
                }
            }
        }
    }
    let mut words = vec![Vec::new()];
    for _ in 0..4 {
        words = words
            .iter()
            .flat_map(|word| {
                b"HTP/01.9"
                    .iter()
                    .map(move |byte| [word.as_slice(), &[*byte]].concat())
            })
            .collect();
        versions.extend(
            words
                .iter()
                .map(|word| [b"HTTP/".as_slice(), word].concat()),
        );
    }
    for version in versions {
        assert_eq!(
            super::parse_version(&version),
            old_parse_version(&version),
            "{:?}",
            version.escape_ascii().to_string()
        );
    }
}
