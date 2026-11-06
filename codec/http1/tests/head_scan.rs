//! Differentials of the bulk head scan, the header-line splitter, and the
//! header byte classes against transliterations of the per-byte code that
//! `roxygen_http1_baseline` freezes (`repair.http1_bulk_head_parse`,
//! group-154; `repair.h1_head_scan_and_header_trims`, group-187).

use std::ops::Range;

use roxygen_codec::CodecCallbackAction;
use roxygen_codec::CodecCallbackActions;
use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::message::HttpMessage;

use super::FROZEN_HTTP_METHODS;
use super::HEAD_RESERVE_CAP;
use super::HeadScan;
use super::HeadScanStop;
use super::Http1xCodec;
use super::INITIAL_PREFIX_LIMIT;
use super::MAX_HEADER_BYTES;
use super::ParseErrorKind;
use super::ParseState;
use super::UPSTREAM_PREFIX_LIMIT;
use super::header_lines;
use super::valid_header_value;

const QUEUE_LIMIT: usize = 1024 * 1024;

fn reference_trim_trailing_cr(value: &[u8]) -> &[u8] {
    value.strip_suffix(b"\r").unwrap_or(value)
}

fn reference_valid_initial_prefix(upstream: bool, head: &[u8]) -> bool {
    if upstream {
        return b"HTTP/".starts_with(head) || head.starts_with(b"HTTP/");
    }
    let token_end = head
        .iter()
        .position(|byte| matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
        .unwrap_or(head.len());
    let token = &head[..token_end];
    FROZEN_HTTP_METHODS
        .iter()
        .any(|method| method.starts_with(token))
}

fn reference_headers_terminated(bytes: &[u8]) -> bool {
    bytes.ends_with(b"\r\n\r\n") || bytes.ends_with(b"\n\n")
}

fn reference_request_is_http_09(bytes: &[u8]) -> bool {
    let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') else {
        return false;
    };
    let line = reference_trim_trailing_cr(&bytes[..newline]);
    let mut words = line.split(|byte| matches!(*byte, b' ' | b'\t'));
    let Some(method) = words.next() else {
        return false;
    };
    let Some(_url) = words.next() else {
        return false;
    };
    match words.next() {
        None => !method.is_empty(),
        Some(version) => version == b"HTTP/0.9" && words.next().is_none(),
    }
}

fn reference_trim_http_whitespace(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
        .map_or(start, |index| index + 1);
    &value[start..end]
}

fn reference_is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
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
        )
}

fn reference_valid_header_value(value: &[u8], strict: bool) -> bool {
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

fn reference_parse_trailers(raw: &[u8], strict: bool) -> Result<HttpHeaders, ParseErrorKind> {
    let mut trailers = HttpHeaders::new();
    for raw_line in reference_header_lines(raw) {
        let line = reference_trim_trailing_cr(&raw_line);
        if line.is_empty() {
            continue;
        }
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            return Err(ParseErrorKind::Body);
        };
        let name = &line[..colon];
        let value = reference_trim_http_whitespace(&line[colon + 1..]);
        if name.is_empty()
            || !name.iter().all(|byte| reference_is_header_name_byte(*byte))
            || !reference_valid_header_value(value, strict)
        {
            return Err(if strict {
                ParseErrorKind::HeaderContent
            } else {
                ParseErrorKind::Body
            });
        }
        trailers.add(name, value);
    }
    Ok(trailers)
}

/// The ranges `reference_header_lines` yields, into a reused vector.
fn reference_line_ranges(value: &[u8], ranges: &mut Vec<Range<usize>>) {
    ranges.clear();
    let mut start = 0;
    for (index, byte) in value.iter().copied().enumerate() {
        let escaped_newline = byte == b'\n'
            && index >= 3
            && value[index - 1] == b'\\'
            && value[index - 2] == b'\r'
            && value[index - 3] == b'\\';
        if byte == b'\n' && !escaped_newline {
            ranges.push(start..index);
            start = index + 1;
        }
    }
    ranges.push(start..value.len());
}

fn reference_header_lines(value: &[u8]) -> Vec<Vec<u8>> {
    let mut lines = vec![Vec::new()];
    for (index, byte) in value.iter().copied().enumerate() {
        let escaped_newline = byte == b'\n'
            && index >= 3
            && value[index - 1] == b'\\'
            && value[index - 2] == b'\r'
            && value[index - 3] == b'\\';
        if byte == b'\n' && !escaped_newline {
            lines.push(Vec::new());
        } else {
            lines.last_mut().expect("header line exists").push(byte);
        }
    }
    lines
}

/// The per-byte machine over a head from its first byte: `ParseState::Idle`
/// takes the first byte and checks only the start-line prefix, then each
/// `ParseState::Head` step pushes a byte and checks, in order, the size limit,
/// the prefix, the terminator, and control bytes. Returns the bytes accepted,
/// the stopping byte included, and why the machine stopped.
fn reference_scan(upstream: bool, head: &[u8]) -> HeadScan {
    let Some(&first) = head.first() else {
        return HeadScan { end: 0, stop: None };
    };
    if !reference_valid_initial_prefix(upstream, &[first]) {
        return HeadScan {
            end: 1,
            stop: Some(HeadScanStop::InvalidStartLine),
        };
    }
    let mut buffered = vec![first];
    for (index, byte) in head.iter().copied().enumerate().skip(1) {
        buffered.push(byte);
        let stop = if buffered.len() > MAX_HEADER_BYTES {
            Some(HeadScanStop::TooLarge)
        } else if !reference_valid_initial_prefix(upstream, &buffered) {
            Some(HeadScanStop::InvalidStartLine)
        } else if reference_headers_terminated(&buffered)
            || (!upstream && byte == b'\n' && reference_request_is_http_09(&buffered))
        {
            Some(HeadScanStop::Complete)
        } else if byte < b' ' && !matches!(byte, b'\r' | b'\n' | b'\t') {
            Some(HeadScanStop::InvalidHeaderByte)
        } else {
            None
        };
        if stop.is_some() {
            return HeadScan {
                end: index + 1,
                stop,
            };
        }
    }
    HeadScan {
        end: head.len(),
        stop: None,
    }
}

/// A deterministic linear congruential generator.
struct Generator(u64);

impl Generator {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as usize) % bound
    }
}

const PIECES: &[&[u8]] = &[
    b"GET",
    b"POST",
    b"UNSUBSCRIBE",
    b"M-SEARCH",
    b"MKACTIVITY",
    b"HTTP/1.1",
    b"HTTP/0.9",
    b"HTTP/",
    b"HTT",
    b" 200 OK",
    b" ",
    b" ",
    b"\t",
    b"\r",
    b"\n",
    b"\r\n",
    b"\r\n",
    b"\r\n\r\n",
    b"\n\n",
    b"\r\n\r",
    b":",
    b"\\",
    b"\\\r\\\n",
    b"\\\r\\",
    b"Host: h",
    b"X-A: v",
    b"/path",
    b"\x00",
    b"\x01",
    b"\x0b",
    b"\x1f",
    b"\x7f",
    b"\x80",
    b"\xff",
    b"  folded",
    b"\tfolded",
    b"a",
    b"0",
];

const STARTS: &[&[u8]] = &[
    b"GET / HTTP/1.1\r\n",
    b"GET /",
    b"POST /x HTTP/1.0\n",
    b"UNSUBSCRIBE * HTTP/1.1\r\n",
    b"GET /old\r\n",
    b"GET /old HTTP/0.9\n",
    b" / HTTP/1.1\r\n",
    b"\t/",
    b"G",
    b"GETX",
    b"HTTP/1.1 200 OK\r\n",
    b"HTTP/1.1 100 Continue\n",
    b"HTTP/",
    b"HTTX",
    b"H",
    b"\x01",
];

/// Heads built from start lines and pieces, with no leading CR or LF, which
/// `ParseState::Idle` skips before a head begins.
fn generated_heads() -> Vec<Vec<u8>> {
    let mut generator = Generator(0x5eed);
    let mut heads = Vec::new();
    for _ in 0..1_500 {
        let mut head = STARTS[generator.next(STARTS.len())].to_vec();
        for _ in 0..generator.next(40) {
            head.extend_from_slice(PIECES[generator.next(PIECES.len())]);
        }
        heads.push(head);
    }
    for start in STARTS {
        for piece in PIECES {
            heads.push([*start, *piece, b"\r\n\r\n"].concat());
        }
    }
    let downstream_alphabet = b"GT \t\r\n\x01a";
    let upstream_alphabet = b"HTP/ \r\n\x01";
    for alphabet in [downstream_alphabet, upstream_alphabet] {
        let mut words = vec![Vec::new()];
        for _ in 0..5 {
            words = words
                .iter()
                .flat_map(|word| {
                    alphabet
                        .iter()
                        .map(move |byte| [word.as_slice(), &[*byte]].concat())
                })
                .collect();
            heads.extend(words.iter().cloned());
        }
    }
    for (prefix, tails) in [
        (
            b"GET / HTTP/1.1\r\nX-Pad: ".as_slice(),
            [b"\r\n\r\n".as_slice(), b"\n\n", b"\x01", b"a", b"\r\n\r\na"],
        ),
        (
            b"HTTP/1.1 200 OK\r\nX-Pad: ".as_slice(),
            [b"\r\n\r\n".as_slice(), b"\n\n", b"\x01", b"a", b"\r\n\r\na"],
        ),
    ] {
        for tail in tails {
            for length in MAX_HEADER_BYTES - 1..=MAX_HEADER_BYTES + 1 {
                let mut head = prefix.to_vec();
                head.resize(length - tail.len(), b'a');
                head.extend_from_slice(tail);
                heads.push(head);
            }
        }
    }
    heads
        .into_iter()
        .map(|head| {
            let start = head
                .iter()
                .position(|byte| !matches!(*byte, b'\r' | b'\n'))
                .unwrap_or(head.len());
            head[start..].to_vec()
        })
        .filter(|head| !head.is_empty())
        .collect()
}

/// Buffered lengths at which the codec can hold a partial head: at least the
/// first byte, and short of the byte where the per-byte machine stops. Heads
/// near `MAX_HEADER_BYTES` use the first and last lengths, those just under the
/// limit, and a stride, instead of every length.
fn buffered_lengths(head: &[u8], reference: HeadScan) -> Vec<usize> {
    let last = if reference.stop.is_some() {
        reference.end - 1
    } else {
        head.len()
    };
    if head.len() <= 4_096 {
        return (1..=last).collect();
    }
    (1..=last)
        .filter(|length| {
            *length <= 4
                || *length + 4 >= last
                || *length + 4 >= MAX_HEADER_BYTES
                || length % 20_011 == 0
        })
        .collect()
}

#[test]
fn bulk_scan_matches_the_per_byte_reference() {
    let heads = generated_heads();
    let mut stops = [0_usize; 5];
    for upstream in [false, true] {
        let direction = if upstream {
            TransportDirection::Upstream
        } else {
            TransportDirection::Downstream
        };
        let codec = Http1xCodec::new(direction);
        for head in &heads {
            let reference = reference_scan(upstream, head);
            stops[match reference.stop {
                None => 0,
                Some(HeadScanStop::Complete) => 1,
                Some(HeadScanStop::TooLarge) => 2,
                Some(HeadScanStop::InvalidStartLine) => 3,
                Some(HeadScanStop::InvalidHeaderByte) => 4,
            }] += 1;
            for buffered in buffered_lengths(head, reference) {
                let context = format!(
                    "upstream={upstream} buffered={buffered} head={:?}",
                    head[..head.len().min(160)].escape_ascii().to_string()
                );
                assert_eq!(
                    codec.scan_head(&head[..buffered], &head[buffered..], 0),
                    HeadScan {
                        end: reference.end - buffered,
                        stop: reference.stop,
                    },
                    "{context}"
                );
                assert_eq!(codec.scan_head(&[], head, buffered), reference, "{context}");
            }
        }
    }
    assert!(
        stops.iter().all(|count| *count > 0),
        "the corpus reaches every outcome: {stops:?}"
    );
}

#[test]
fn header_line_ranges_match_the_reference_splitter() {
    let mut values = vec![Vec::new()];
    let mut words = vec![Vec::new()];
    for _ in 0..8 {
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
    values.extend(
        generated_heads()
            .into_iter()
            .filter(|head| head.len() < 4_096),
    );
    for value in values {
        let lines = header_lines(&value).map(<[u8]>::to_vec).collect::<Vec<_>>();
        assert_eq!(
            lines,
            reference_header_lines(&value),
            "{:?}",
            value.escape_ascii().to_string()
        );
    }
}

#[derive(Default)]
struct PauseAtMessageBegin {
    pause: bool,
}

impl HttpCodecCallback for PauseAtMessageBegin {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        if self.pause && matches!(event, CodecEvent::MessageBegin { .. }) {
            vec![CodecCallbackAction::SetParserPaused(true)].into()
        } else {
            CodecCallbackActions::new()
        }
    }
}

fn assert_partial_head_capacity(codec: &Http1xCodec, expected: &[u8], context: &str) {
    let ParseState::Head(head) = &codec.state else {
        panic!("{context}: the codec holds a partial head");
    };
    assert_eq!(head.as_slice(), expected, "{context}");
    assert!(
        head.capacity() <= (2 * head.len()).max(8),
        "{context}: capacity {} for {} bytes",
        head.capacity(),
        head.len()
    );
}

#[test]
fn partial_head_buffer_capacity_follows_its_length() {
    let mut unterminated = b"GET /capacity HTTP/1.1\r\nHost: example.com\r\nX-Long: ".to_vec();
    unterminated.resize(6_000, b'v');
    for (upstream, head) in [
        (false, unterminated.clone()),
        (true, {
            let mut head = b"HTTP/1.1 200 OK\r\nX-Long: ".to_vec();
            head.resize(6_000, b'v');
            head
        }),
    ] {
        let direction = if upstream {
            TransportDirection::Upstream
        } else {
            TransportDirection::Downstream
        };
        for chunk in [1, 3, 7, 64, 1_000, 6_000] {
            for prefix in [1, 2, 9, 31, 500, 5_999] {
                let mut codec = Http1xCodec::new(direction);
                let mut callback = PauseAtMessageBegin::default();
                let mut egress = BufQueue::new(QUEUE_LIMIT);
                let mut consumed = 0;
                for part in head[..prefix].chunks(chunk) {
                    let input = BufQueue::copy_from_slice(part, part.len())
                        .expect("a test read fits its queue");
                    consumed += codec
                        .on_ingress(&input, &mut callback, &mut egress)
                        .consumed;
                    assert_partial_head_capacity(
                        &codec,
                        &head[..consumed],
                        &format!("upstream={upstream} chunk={chunk} prefix={prefix}"),
                    );
                }
                assert_eq!(consumed, prefix);
            }
        }

        let mut codec = Http1xCodec::new(direction);
        let mut callback = PauseAtMessageBegin { pause: true };
        let mut egress = BufQueue::new(QUEUE_LIMIT);
        let input = BufQueue::copy_from_slice(&head, head.len()).expect("the head fits its queue");
        assert_eq!(
            codec
                .on_ingress(&input, &mut callback, &mut egress)
                .consumed,
            1
        );
        assert_partial_head_capacity(
            &codec,
            &head[..1],
            &format!("upstream={upstream} paused at MessageBegin"),
        );
    }
}

/// Bytes around every class boundary of the header-value check.
const VALUE_EDGE_BYTES: [u8; 48] = [
    0x00, 0x01, 0x08, b'\t', b'\n', 0x0b, 0x0c, b'\r', 0x0e, 0x1f, b' ', b'!', b'"', b',', b'-',
    b'/', b'0', b'9', b':', b';', b'@', b'A', b'Z', b'[', b'\\', b']', b'_', b'`', b'a', b'z',
    b'{', b'|', b'}', b'~', 0x7f, 0x80, 0x81, 0x9f, 0xa0, 0xbf, 0xc0, 0xdf, 0xe0, 0xef, 0xf0, 0xf7,
    0xfe, 0xff,
];

fn printable_value(generator: &mut Generator, length: usize) -> Vec<u8> {
    (0..length)
        .map(|_| b' ' + generator.next(usize::from(b'~' - b' ') + 1) as u8)
        .collect()
}

fn assert_value_check(value: &[u8]) {
    for strict in [false, true] {
        assert_eq!(
            valid_header_value(value, strict),
            reference_valid_header_value(value, strict),
            "strict={strict} {:?}",
            value.escape_ascii().to_string()
        );
    }
}

#[test]
fn header_value_check_matches_the_frozen_per_byte_check() {
    assert_value_check(b"");
    for first in 0..=u8::MAX {
        assert_value_check(&[first]);
        for second in 0..=u8::MAX {
            assert_value_check(&[first, second]);
        }
    }
    for first in VALUE_EDGE_BYTES {
        for second in VALUE_EDGE_BYTES {
            for third in VALUE_EDGE_BYTES {
                assert_value_check(&[first, second, third]);
            }
        }
    }

    let mut generator = Generator(0x7a1e);
    for length in 1..=64 {
        for offset in 0..length.min(24) {
            for byte in 0..=u8::MAX {
                let mut value = printable_value(&mut generator, length);
                value[offset] = byte;
                assert_value_check(&value);
            }
        }
    }

    let escapes: [&[u8]; 8] = [
        b"\\\r\\",
        b"\\\n",
        b"\\\r\\\n",
        b"\\\r\\\r\\",
        b"\\\r",
        b"\r\\",
        b"\r",
        b"\n",
    ];
    for escape in escapes {
        for start in 3..=17 {
            for length in [start + escape.len(), 20, 26] {
                let mut value = printable_value(&mut generator, length.max(start + escape.len()));
                value[start..start + escape.len()].copy_from_slice(escape);
                assert_value_check(&value);
                value[start - 1] = b'\\';
                assert_value_check(&value);
            }
        }
    }
}

fn line_range(value: &[u8], line: &[u8]) -> Range<usize> {
    let start = line.as_ptr().addr() - value.as_ptr().addr();
    start..start + line.len()
}

fn for_each_word(alphabet: &[u8], longest: usize, mut visit: impl FnMut(&[u8])) {
    let mut word = Vec::with_capacity(longest);
    visit(&word);
    let mut digits = Vec::with_capacity(longest);
    for length in 1..=longest {
        digits.clear();
        digits.resize(length, 0_usize);
        loop {
            word.clear();
            word.extend(digits.iter().map(|digit| alphabet[*digit]));
            visit(&word);
            let Some(position) = digits.iter().rposition(|digit| digit + 1 < alphabet.len()) else {
                break;
            };
            digits[position] += 1;
            digits[position + 1..].fill(0);
        }
    }
}

#[test]
fn header_line_ranges_match_the_reference_at_every_word_offset() {
    const FILLER: &[u8; 8] = b"aaaaaaaa";
    const EXTENDED: &[u8] = b"a\\\r\n\x0b\x00\xff";
    let mut ranges = Vec::new();
    let mut shifted = Vec::new();
    let mut words = 0_usize;
    let mut check_lines = |value: &[u8]| {
        words += 1;
        for shift in 0..=FILLER.len() {
            shifted.clear();
            shifted.extend_from_slice(&FILLER[..shift]);
            shifted.extend_from_slice(value);
            reference_line_ranges(&shifted, &mut ranges);
            assert!(
                header_lines(&shifted)
                    .map(|line| line_range(&shifted, line))
                    .eq(ranges.iter().cloned()),
                "{:?}",
                shifted.escape_ascii().to_string()
            );
        }
    };
    for_each_word(b"a\\\r\n", 8, &mut check_lines);
    for_each_word(EXTENDED, 7, &mut check_lines);
    assert_eq!(
        words,
        (0..=8).map(|length| 4_usize.pow(length)).sum::<usize>()
            + (0..=7).map(|length| 7_usize.pow(length)).sum::<usize>(),
        "every word of up to 8 symbols over 4 bytes and up to 7 over 7 bytes"
    );

    let mut trailers = Vec::new();
    for_each_word(EXTENDED, 5, |value| {
        for shift in 0..=FILLER.len() {
            trailers.clear();
            trailers.extend_from_slice(b"X:");
            trailers.extend_from_slice(&FILLER[..shift]);
            trailers.extend_from_slice(value);
            for strict in [false, true] {
                assert_eq!(
                    Http1xCodec::parse_trailers(&trailers, strict),
                    reference_parse_trailers(&trailers, strict),
                    "strict={strict} {:?}",
                    trailers.escape_ascii().to_string()
                );
            }
        }
    });
}

fn assert_scan_matches_the_reference(codec: &Http1xCodec, upstream: bool, head: &[u8]) {
    let reference = reference_scan(upstream, head);
    for buffered in buffered_lengths(head, reference) {
        let context = format!(
            "upstream={upstream} buffered={buffered} head={:?}",
            head.escape_ascii().to_string()
        );
        assert_eq!(
            codec.scan_head(&head[..buffered], &head[buffered..], 0),
            HeadScan {
                end: reference.end - buffered,
                stop: reference.stop,
            },
            "{context}"
        );
        assert_eq!(codec.scan_head(&[], head, buffered), reference, "{context}");
    }
}

#[test]
fn control_byte_scan_stops_at_word_boundary_offsets() {
    let mut stops: Vec<u8> = (0..b' ').collect();
    stops.extend([0x7f, 0x80, 0xff, b' ', b'a']);
    let tails: [&[u8]; 4] = [b" \r\n\r\n", b" \x01\r\n\r\n", b" a\t \r\n\r\n", b" "];
    for upstream in [false, true] {
        let (direction, start) = if upstream {
            (
                TransportDirection::Upstream,
                b"HTTP/1.1 200 OK\r\nX: ".as_slice(),
            )
        } else {
            (
                TransportDirection::Downstream,
                b"GET / HTTP/1.1\r\nX: ".as_slice(),
            )
        };
        let codec = Http1xCodec::new(direction);
        for offset in 0..=24 {
            for stop in &stops {
                for tail in tails {
                    let head = [start, &b"v".repeat(offset), &[*stop], tail].concat();
                    assert_scan_matches_the_reference(&codec, upstream, &head);
                }
            }
        }
    }
}

#[test]
fn upstream_prefix_window_matches_the_reference_for_every_mutation() {
    let heads: [(bool, &[u8]); 3] = [
        (true, b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
        (true, b"HTTP/1.0 101 S\nX: a\n\n"),
        (false, b"GET / HTTP/1.1\r\nHost: a\r\n\r\n"),
    ];
    for (upstream, head) in heads {
        let direction = if upstream {
            TransportDirection::Upstream
        } else {
            TransportDirection::Downstream
        };
        let codec = Http1xCodec::new(direction);
        for offset in 0..12 {
            for byte in 0..=u8::MAX {
                if offset == 0 && matches!(byte, b'\r' | b'\n') {
                    continue;
                }
                let mut mutated = head.to_vec();
                mutated[offset] = byte;
                assert_scan_matches_the_reference(&codec, upstream, &mutated);
            }
        }
    }
}

#[test]
fn header_name_table_matches_the_frozen_tchar_predicate() {
    use super::HEADER_NAME_BYTE;
    use super::header_name_byte;
    use super::header_name_end;

    for byte in 0..=u8::MAX {
        let expected = reference_is_header_name_byte(byte);
        assert_eq!(HEADER_NAME_BYTE[usize::from(byte)], expected, "{byte:#04x}");
        assert_eq!(header_name_byte(byte), expected, "{byte:#04x}");
    }
    assert_eq!(
        HEADER_NAME_BYTE.iter().filter(|token| **token).count(),
        77,
        "62 alphanumerics and 15 symbols"
    );

    for_each_word(b"a:Z-\" \t\x00\x7f\x80\xff(", 4, |line| {
        let expected = line.iter().position(|byte| *byte == b':').map(|colon| {
            (
                colon,
                colon != 0
                    && line[..colon]
                        .iter()
                        .all(|byte| reference_is_header_name_byte(*byte)),
            )
        });
        assert_eq!(
            header_name_end(line),
            expected,
            "{:?}",
            line.escape_ascii().to_string()
        );
    });
}

#[test]
fn line_feed_count_matches_a_byte_count() {
    use super::word_scan::count_lf;

    let check = |value: &[u8]| {
        for start in 0..value.len().min(8) {
            let bytes = &value[start..];
            assert_eq!(
                count_lf(bytes),
                bytes.iter().filter(|byte| **byte == b'\n').count(),
                "{:?}",
                bytes[..bytes.len().min(160)].escape_ascii().to_string()
            );
        }
    };
    generated_heads().iter().for_each(|value| check(value));
    for_each_word(b"a\n\x0b\x00\xff\x8a", 5, check);
    (0..=24).for_each(|length| check(&vec![b'\n'; length]));
}

/// The heads of the downstream prefix-window differential: every prefix of
/// every frozen method, extended by one byte or with one byte substituted at
/// each position, bare and followed by the rest of a request head.
fn downstream_prefix_window_heads() -> Vec<Vec<u8>> {
    let mut alphabet = FROZEN_HTTP_METHODS
        .iter()
        .flat_map(|method| {
            method
                .iter()
                .flat_map(|byte| [*byte, byte.to_ascii_lowercase()])
        })
        .collect::<Vec<_>>();
    alphabet.extend([b'-', b' ', b'\t', b'\r', b'\n', b'/', 0x00, 0x7f, 0x80]);
    alphabet.sort_unstable();
    alphabet.dedup();
    let mut heads = std::collections::BTreeSet::new();
    for method in FROZEN_HTTP_METHODS {
        for length in 0..=method.len() {
            let prefix = &method[..length];
            for byte in &alphabet {
                heads.insert([prefix, &[*byte]].concat());
                for position in 0..length {
                    let mut substituted = prefix.to_vec();
                    substituted[position] = *byte;
                    heads.insert(substituted);
                }
            }
        }
    }
    heads
        .into_iter()
        .filter(|head| !matches!(head.first(), None | Some(b'\r' | b'\n')))
        .flat_map(|head| {
            let completed = [head.as_slice(), b" /x HTTP/1.1\r\nHost: a\r\n\r\n"].concat();
            [head, completed]
        })
        .collect()
}

#[test]
fn downstream_prefix_window_matches_the_reference_for_every_mutation() {
    let codec = Http1xCodec::new(TransportDirection::Downstream);
    let heads = downstream_prefix_window_heads();
    let mut stops = [0_usize; 5];
    for head in &heads {
        stops[match reference_scan(false, head).stop {
            None => 0,
            Some(HeadScanStop::Complete) => 1,
            Some(HeadScanStop::TooLarge) => 2,
            Some(HeadScanStop::InvalidStartLine) => 3,
            Some(HeadScanStop::InvalidHeaderByte) => 4,
        }] += 1;
        assert_scan_matches_the_reference(&codec, false, head);
    }
    assert!(
        stops[0] > 0 && stops[1] > 0 && stops[3] > 0,
        "the heads end, complete, and fail the start line: {stops:?}"
    );
}

/// Records the outcome of the heads a codec parses.
#[derive(Default)]
struct HeadOutcome {
    messages: Vec<HttpMessage>,
    errors: usize,
}

impl HttpCodecCallback for HeadOutcome {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::HeadersComplete { message, .. } => self.messages.push(*message),
            CodecEvent::Error { .. } => self.errors += 1,
            _ => {}
        }
    }
}

/// A codec of `direction`, ready for a head: an upstream codec has sent a GET.
fn codec_for_a_head(direction: TransportDirection) -> Http1xCodec {
    let mut codec = Http1xCodec::new(direction);
    if direction.is_upstream() {
        let stream = codec.create_stream();
        let mut request = HttpMessage::new();
        request
            .set_method(HttpMethod::Get)
            .expect("GET is a method");
        request.set_url(b"/", false).expect("a static URL is valid");
        request.set_http_version(1, 1);
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
                &mut HeadOutcome::default(),
            )
            .expect("the request is generated");
    }
    codec
}

/// Parses `reads` in order, and returns the bytes consumed.
fn parse_reads(codec: &mut Http1xCodec, reads: &[&[u8]], outcome: &mut HeadOutcome) -> usize {
    let mut egress = BufQueue::new(QUEUE_LIMIT);
    reads
        .iter()
        .map(|read| {
            let input =
                BufQueue::copy_from_slice(read, read.len()).expect("a test read fits its queue");
            codec.on_ingress(&input, outcome, &mut egress).consumed
        })
        .sum()
}

/// A request head with each frozen method, and an upstream status line.
fn prefix_window_heads() -> Vec<(TransportDirection, usize, Vec<u8>)> {
    let mut heads = FROZEN_HTTP_METHODS
        .iter()
        .map(|method| {
            (
                TransportDirection::Downstream,
                INITIAL_PREFIX_LIMIT,
                [*method, b" /x HTTP/1.1\r\nHost: a\r\n\r\n"].concat(),
            )
        })
        .collect::<Vec<_>>();
    heads.push((
        TransportDirection::Upstream,
        UPSTREAM_PREFIX_LIMIT,
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
    ));
    heads
}

#[test]
fn prefix_check_reads_each_window_byte_once() {
    for (direction, window, head) in prefix_window_heads() {
        let context = format!("{direction:?} {:?}", head.escape_ascii().to_string());

        // One segment, parsed in place: the Idle first-byte check, then one
        // scan_head call from offset 1.
        let mut codec = codec_for_a_head(direction);
        let mut outcome = HeadOutcome::default();
        assert_eq!(parse_reads(&mut codec, &[&head], &mut outcome), head.len());
        assert_eq!(
            (outcome.messages.len(), outcome.errors),
            (1, 0),
            "{context}"
        );
        assert!(
            codec.head_work.prefix_reads.get() <= window + 1,
            "{context}: {} prefix reads in one segment, at most {} allowed",
            codec.head_work.prefix_reads.get(),
            window + 1
        );
        assert_eq!(codec.head_work.prefix_copies.get(), 0, "{context}");

        // Split after each window byte: each call rebuilds the token's state
        // from the earlier bytes once, reads its one visited position, and
        // copies nothing.
        let codec = Http1xCodec::new(direction);
        for buffered in 1..window {
            codec.head_work.prefix_reads.set(0);
            codec.head_work.prefix_copies.set(0);
            let scan = codec.scan_head(&head[..buffered], &head[buffered..=buffered], 0);
            assert_eq!(
                scan,
                HeadScan { end: 1, stop: None },
                "{context} buffered={buffered}"
            );
            assert!(
                codec.head_work.prefix_reads.get() <= buffered + 1,
                "{context} buffered={buffered}: {} prefix reads, at most {} allowed",
                codec.head_work.prefix_reads.get(),
                buffered + 1
            );
            assert_eq!(
                codec.head_work.prefix_copies.get(),
                0,
                "{context} buffered={buffered}"
            );
        }
    }
}

/// An 18-header, 412-byte request head with consumer-shaped field lengths and
/// generic names.
fn consumer_shaped_request_head() -> Vec<u8> {
    let mut head = b"GET /items/42 HTTP/1.1\r\n".to_vec();
    for (name, value) in [
        ("Host", "backend.example.com"),
        ("User-Agent", "client/1.0"),
        ("Accept", "*/*"),
        ("Accept-Encoding", "gzip, deflate"),
        ("Content-Length", "0"),
        ("x-routing-key", "c1-0123456789abcdef"),
        ("x-routing-key-copy", "c1-0123456789abcdef"),
        ("x-timeout-ms", "1500"),
        ("x-request-id", "1"),
        ("x-trace-id", "2"),
        ("x-client-version", "1.2.3"),
        ("x-region", "c1"),
        ("x-priority", "3"),
        ("x-attempt", "1"),
        ("x-flags", "a,b,c"),
        ("x-span", "01234567"),
        ("Connection", "keep-alive"),
    ] {
        head.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    let pad = 412 - head.len() - b"x-pad-req: \r\n\r\n".len();
    head.extend_from_slice(format!("x-pad-req: {}\r\n\r\n", "p".repeat(pad)).as_bytes());
    assert_eq!(head.len(), 412);
    assert_eq!(head.iter().filter(|byte| **byte == b'\n').count(), 18 + 2);
    head
}

#[test]
fn reserve_hint_matches_the_line_feed_count_for_every_split() {
    let consumer_shaped = consumer_shaped_request_head();
    let mut checked = [0_usize; 2];
    for upstream in [false, true] {
        let direction = if upstream {
            TransportDirection::Upstream
        } else {
            TransportDirection::Downstream
        };
        let mut heads = generated_heads();
        if !upstream {
            heads.push(consumer_shaped.clone());
        }
        for head in heads {
            let reference = reference_scan(upstream, &head);
            if reference.stop != Some(HeadScanStop::Complete) {
                continue;
            }
            let head = &head[..reference.end];
            let line_feeds = head.iter().filter(|byte| **byte == b'\n').count();
            let expected = Some((line_feeds.saturating_sub(1).min(HEAD_RESERVE_CAP), 0));
            let mut splits = vec![head.len()];
            splits.extend(
                buffered_lengths(head, reference)
                    .into_iter()
                    .filter(|length| *length < head.len()),
            );
            for split in splits {
                let context = format!(
                    "upstream={upstream} split={split} head={:?}",
                    head[..head.len().min(160)].escape_ascii().to_string()
                );
                let mut codec = codec_for_a_head(direction);
                let mut outcome = HeadOutcome::default();
                let reads: &[&[u8]] = if split == head.len() {
                    &[head]
                } else {
                    &[&head[..split], &head[split..]]
                };
                assert_eq!(
                    parse_reads(&mut codec, reads, &mut outcome),
                    head.len(),
                    "{context}"
                );
                assert_eq!(codec.head_reserve, expected, "{context}");
                if !outcome.messages.is_empty() {
                    checked[usize::from(upstream)] += 1;
                }
            }
        }
    }
    assert!(
        checked.iter().all(|count| *count > 0),
        "valid heads in both directions: {checked:?}"
    );
}

#[test]
fn in_place_head_takes_its_line_feed_count_from_the_scan() {
    let heads = [
        (
            TransportDirection::Downstream,
            consumer_shaped_request_head(),
        ),
        (
            TransportDirection::Downstream,
            b"POST /x HTTP/1.0\nHost: a\nContent-Length: 0\n\n".to_vec(),
        ),
        (
            TransportDirection::Upstream,
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-A: 1\r\n\r\n".to_vec(),
        ),
    ];
    for (direction, head) in heads {
        let line_feeds = head.iter().filter(|byte| **byte == b'\n').count();
        for split in [head.len(), 1, head.len() / 2, head.len() - 1] {
            let context = format!(
                "{direction:?} split={split} {:?}",
                head.escape_ascii().to_string()
            );
            let mut codec = codec_for_a_head(direction);
            let mut outcome = HeadOutcome::default();
            let reads: &[&[u8]] = if split == head.len() {
                &[&head]
            } else {
                &[&head[..split], &head[split..]]
            };
            assert_eq!(parse_reads(&mut codec, reads, &mut outcome), head.len());
            assert_eq!(
                (outcome.messages.len(), outcome.errors),
                (1, 0),
                "{context}"
            );
            assert_eq!(codec.head_reserve, Some((line_feeds - 1, 0)), "{context}");
            assert_eq!(
                codec.head_work.line_feed_counts.get(),
                usize::from(split < head.len()),
                "{context}: count_lf passes, none in place and one over buffered bytes"
            );
        }
    }
}

#[test]
fn recognized_method_and_upgrade_are_not_copied() {
    for method in FROZEN_HTTP_METHODS {
        let target: &[u8] = if method == b"CONNECT" {
            b"a:443"
        } else {
            b"/x"
        };
        let head = [method, b" ", target, b" HTTP/1.1\r\nHost: a:443\r\n\r\n"].concat();
        let context = format!("{:?}", head.escape_ascii().to_string());
        let mut codec = codec_for_a_head(TransportDirection::Downstream);
        let mut outcome = HeadOutcome::default();
        assert_eq!(parse_reads(&mut codec, &[&head], &mut outcome), head.len());
        assert_eq!(
            (outcome.messages.len(), outcome.errors),
            (1, 0),
            "{context}"
        );
        let recognized = HttpMethod::from_bytes(method);
        assert_eq!(
            codec.head_work.method_copies.get(),
            usize::from(recognized.is_none()),
            "{context}: owned copies of the method name"
        );
        let message = &mut outcome.messages[0];
        assert_eq!(message.method(), Ok(recognized), "{context}");
        assert_eq!(
            message.method_string().map(str::as_bytes),
            Ok(method),
            "{context}"
        );
    }

    let requests: [&[u8]; 3] = [
        b"GET / HTTP/1.1\r\nHost: a\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: k\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: a\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n",
    ];
    let responses: [&[u8]; 3] = [
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nUpgrade: h2c\r\nContent-Length: 0\r\n\r\n",
    ];
    for (direction, heads) in [
        (TransportDirection::Downstream, requests),
        (TransportDirection::Upstream, responses),
    ] {
        for head in heads {
            let context = format!("{direction:?} {:?}", head.escape_ascii().to_string());
            let mut codec = codec_for_a_head(direction);
            let mut outcome = HeadOutcome::default();
            assert_eq!(parse_reads(&mut codec, &[head], &mut outcome), head.len());
            assert_eq!(
                (outcome.messages.len(), outcome.errors),
                (1, 0),
                "{context}"
            );
            assert_eq!(
                codec.head_work.upgrade_copies.get(),
                0,
                "{context}: owned copies of the Upgrade value"
            );
        }
    }

    // user_agent keeps one copy per codec, at the first head with a
    // User-Agent.
    let mut codec = codec_for_a_head(TransportDirection::Downstream);
    let mut outcome = HeadOutcome::default();
    let mut copies = Vec::new();
    for head in [
        b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(),
        b"GET /2 HTTP/1.1\r\nHost: a\r\nUser-Agent: first\r\n\r\n",
        b"GET /3 HTTP/1.1\r\nHost: a\r\nUser-Agent: second\r\n\r\n",
    ] {
        assert_eq!(parse_reads(&mut codec, &[head], &mut outcome), head.len());
        copies.push(codec.head_work.user_agent_copies.get());
    }
    assert_eq!((outcome.messages.len(), outcome.errors), (3, 0));
    assert_eq!(copies, [0, 1, 1]);
    assert_eq!(codec.user_agent(), b"first");
}
