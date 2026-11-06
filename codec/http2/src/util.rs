//! Validation and header-conversion helpers shared by the HTTP/2 codec.

use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::message::HttpMessage;
use roxygen_core::url::ParseUrl;

use crate::hpack::HpackHeader;

/// Header-name validation policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderNameValidationMode {
    StrictCompat,
    Strict,
}

/// Header-value control-character policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlEscapeMode {
    Compliant,
    StrictCompat,
    Strict,
}

pub fn validate_url(url: &[u8], strict: bool) -> bool {
    if url.is_empty() || url.iter().any(|byte| matches!(*byte, 0..=0x20 | 0x7f)) {
        return false;
    }
    !strict || ParseUrl::parse_url(url, true).is_some()
}

pub fn validate_method(method: &[u8]) -> bool {
    method
        .iter()
        .enumerate()
        .all(|(index, byte)| byte.is_ascii_alphabetic() || (*byte == b'-' && index != 0))
}

pub fn validate_scheme(scheme: &[u8]) -> bool {
    scheme.iter().all(u8::is_ascii_alphabetic)
}

/// `NAME_BYTES` flag: the byte is a lowercase HTTP token character.
const STRICT_NAME_BYTE: u8 = 1;
/// `NAME_BYTES` flag: the byte is a token character in either case, a
/// separator `http_token` maps, or a byte at or above 0x80.
const COMPAT_NAME_BYTE: u8 = 2;

/// `http_token`'s answers for both name modes, by byte.
const NAME_BYTES: [u8; 256] = {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let byte = index as u8;
        if let Some(token) = http_token(byte) {
            table[index] = COMPAT_NAME_BYTE;
            if token == byte {
                table[index] |= STRICT_NAME_BYTE;
            }
        } else if byte >= 0x80 {
            table[index] = COMPAT_NAME_BYTE;
        }
        index += 1;
    }
    table
};

pub fn validate_header_name(name: &[u8], mode: HeaderNameValidationMode) -> bool {
    let flag = match mode {
        HeaderNameValidationMode::Strict => STRICT_NAME_BYTE,
        HeaderNameValidationMode::StrictCompat => COMPAT_NAME_BYTE,
    };
    !name.is_empty()
        && name
            .iter()
            .all(|byte| NAME_BYTES[usize::from(*byte)] & flag != 0)
}

pub fn validate_header_value(value: &[u8], mode: ControlEscapeMode) -> bool {
    #[derive(Clone, Copy)]
    enum Fold {
        None,
        Newline,
        Whitespace,
        MoreWhitespace,
    }

    let mut escaped = false;
    let mut quoted = false;
    let mut fold = Fold::None;
    let mut index = plain_value_prefix(value);
    while index < value.len() {
        let byte = value[index];
        if escaped {
            escaped = false;
            if mode == ControlEscapeMode::Compliant {
                index += 1;
                continue;
            }
        }
        match fold {
            Fold::None => match byte {
                b'\\' if quoted => escaped = true,
                b'"' => quoted = !quoted,
                b'\r' => fold = Fold::Newline,
                byte if (byte < 0x20 && byte != b'\t')
                    || byte == 0x7f
                    || (byte > 0x7f && mode == ControlEscapeMode::Strict) =>
                {
                    return false;
                }
                _ => {}
            },
            Fold::Newline => {
                if byte != b'\n' {
                    return false;
                }
                fold = Fold::Whitespace;
            }
            Fold::Whitespace => {
                if !matches!(byte, b' ' | b'\t') {
                    return false;
                }
                fold = Fold::MoreWhitespace;
            }
            Fold::MoreWhitespace => {
                if !matches!(byte, b' ' | b'\t') {
                    fold = Fold::None;
                    continue;
                }
            }
        }
        index += 1;
    }
    !escaped && matches!(fold, Fold::None | Fold::MoreWhitespace)
}

const fn is_plain_value_byte(byte: u8) -> bool {
    matches!(byte, b'\t' | 0x20..=0x21 | 0x23..=0x7e)
}

const LOW_BITS: u64 = 0x0101_0101_0101_0101;
const HIGH_BITS: u64 = LOW_BITS * 0x80;

/// Sets bit 7 of each byte of `word` that `is_plain_value_byte` rejects, and
/// clears every other bit. The additions cannot carry between bytes.
const fn non_plain_value_bytes(word: u64) -> u64 {
    let low = word & !HIGH_BITS;
    let below_space = !(low + LOW_BITS * (0x80 - 0x20)) & HIGH_BITS;
    let tab = !((low ^ (LOW_BITS * 0x09)) + LOW_BITS * 0x7f) & HIGH_BITS;
    let quote = !((low ^ (LOW_BITS * 0x22)) + LOW_BITS * 0x7f) & HIGH_BITS;
    let delete = (low + LOW_BITS) & HIGH_BITS;
    (word & HIGH_BITS) | (below_space & !tab) | quote | delete
}

fn word_at(bytes: &[u8], start: usize) -> u64 {
    let mut word = [0; 8];
    word.copy_from_slice(&bytes[start..start + 8]);
    u64::from_le_bytes(word)
}

fn first_marked_byte(marks: u64) -> usize {
    (marks.trailing_zeros() / 8) as usize
}

/// Returns the index of the first byte `is_plain_value_byte` rejects, or the
/// length. The bytes before it cannot change `validate_header_value`'s state, so
/// its loop may start there.
fn plain_value_prefix(value: &[u8]) -> usize {
    let length = value.len();
    if length < 8 {
        return value
            .iter()
            .position(|byte| !is_plain_value_byte(*byte))
            .unwrap_or(length);
    }
    let mut start = 0;
    while start + 8 <= length {
        let marks = non_plain_value_bytes(word_at(value, start));
        if marks != 0 {
            return start + first_marked_byte(marks);
        }
        start += 8;
    }
    if start < length {
        // The last word overlaps bytes already accepted, so its first mark
        // is past them.
        let marks = non_plain_value_bytes(word_at(value, length - 8));
        if marks != 0 {
            return length - 8 + first_marked_byte(marks);
        }
    }
    length
}

/// Whether `word` has a byte in `b'A'..=b'Z'`. Setting bit 7 of every byte first keeps both
/// subtractions from borrowing across bytes; `!word` then drops bytes at or above 0x80.
const fn has_uppercase_byte(word: u64) -> bool {
    let set = word | HIGH_BITS;
    let at_least_a = set.wrapping_sub(LOW_BITS * 0x41);
    let at_least_after_z = set.wrapping_sub(LOW_BITS * 0x5b);
    at_least_a & !at_least_after_z & !word & HIGH_BITS != 0
}

/// Equal to `bytes.iter().any(u8::is_ascii_uppercase)`, a word at a time for 8 bytes or more.
pub(crate) fn has_ascii_uppercase(bytes: &[u8]) -> bool {
    let length = bytes.len();
    if length < 8 {
        return bytes.iter().any(u8::is_ascii_uppercase);
    }
    let mut start = 0;
    while start + 8 <= length {
        if has_uppercase_byte(word_at(bytes, start)) {
            return true;
        }
        start += 8;
    }
    // The last word may re-read bytes already checked, which cannot change an "any" answer.
    start < length && has_uppercase_byte(word_at(bytes, length - 8))
}

/// Appends modern-HTTP fields in insertion order and reports whether the
/// requested common field was present, including fields skipped on the wire.
pub fn append_headers(
    input: &HttpHeaders,
    output: &mut Vec<HpackHeader>,
    header_to_check: HttpHeaderCode,
) -> bool {
    for_each_modern_header(input, header_to_check, |_, name, value| {
        output.push(HpackHeader::new(name, value));
    })
}

pub(crate) fn for_each_modern_header<'a>(
    input: &'a HttpHeaders,
    header_to_check: HttpHeaderCode,
    mut visitor: impl FnMut(HttpHeaderCode, &'a [u8], &'a [u8]),
) -> bool {
    let mut found = false;
    for (code, name, value) in input.iter() {
        if code == header_to_check {
            found = true;
        }
        if !is_modern_header(code, name) {
            continue;
        }
        visitor(code, name, value);
    }
    found
}

pub(crate) fn is_modern_header(code: HttpHeaderCode, name: &[u8]) -> bool {
    !name.is_empty()
        && !name.starts_with(b":")
        && code != HttpHeaderCode::Host
        && !disallowed_modern_http_field(code)
}

pub const fn disallowed_modern_http_field(code: HttpHeaderCode) -> bool {
    matches!(
        code,
        HttpHeaderCode::Connection
            | HttpHeaderCode::Host
            | HttpHeaderCode::KeepAlive
            | HttpHeaderCode::ProxyConnection
            | HttpHeaderCode::TransferEncoding
            | HttpHeaderCode::Upgrade
            | HttpHeaderCode::SecWebSocketKey
            | HttpHeaderCode::SecWebSocketAccept
    )
}

/// Produces the source diagnostic shape without exposing field values below
/// verbosity level two.
pub fn debug_message(message: &mut HttpMessage, level: u8) -> Vec<u8> {
    if !message.is_request() {
        return Vec::new();
    }
    let url = message.url().unwrap_or_default();
    let mut result = format!(": URL({})", url.len()).into_bytes();
    if level > 1 {
        result.extend_from_slice(url);
    }
    result.extend_from_slice(b", ");
    result
}

pub fn debug_headers(headers: &HttpHeaders, level: u8) -> Vec<u8> {
    let mut result = Vec::new();
    if level == 0 {
        return result;
    }
    headers.for_each(|name, value| {
        result.extend_from_slice(name);
        result.extend_from_slice(format!("({})", value.len()).as_bytes());
        if level > 1 {
            result.extend_from_slice(value);
        }
        result.extend_from_slice(b", ");
    });
    result
}

pub fn zeroed_buffer(size: u16) -> Box<[u8]> {
    vec![0; usize::from(size)].into_boxed_slice()
}

const fn http_token(byte: u8) -> Option<u8> {
    match byte {
        b'!'
        | b'#'..=b'\''
        | b'*'
        | b'+'
        | b'-'
        | b'.'
        | b'0'..=b'9'
        | b'^'
        | b'_'
        | b'`'
        | b'a'..=b'z'
        | b'|'
        | b'~' => Some(byte),
        b'A'..=b'Z' => Some(byte + (b'a' - b'A')),
        b' ' | b'"' | b'/' | b'}' => Some(1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_table_matches_http_token() {
        for byte in 0..=u8::MAX {
            let entry = NAME_BYTES[usize::from(byte)];
            assert_eq!(
                entry & STRICT_NAME_BYTE != 0,
                http_token(byte) == Some(byte),
                "byte={byte:#04x}",
            );
            assert_eq!(
                entry & COMPAT_NAME_BYTE != 0,
                byte >= 0x80 || http_token(byte).is_some(),
                "byte={byte:#04x}",
            );
        }
    }

    #[test]
    fn has_ascii_uppercase_matches_the_byte_scan() {
        fn byte_scan(bytes: &[u8]) -> bool {
            bytes.iter().any(u8::is_ascii_uppercase)
        }

        for length in 1..=24 {
            for position in 0..length {
                for byte in 0..=u8::MAX {
                    let mut bytes = vec![b'a'; length];
                    bytes[position] = byte;
                    assert_eq!(
                        has_ascii_uppercase(&bytes),
                        byte_scan(&bytes),
                        "byte={byte:#04x} at {position} of {length}",
                    );
                }
            }
        }

        let mut state = 0x5EED_CA5E_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..10_000 {
            let length = usize::try_from(next() % 65).expect("length fits in usize");
            let bytes: Vec<u8> = (0..length).map(|_| next().to_le_bytes()[0]).collect();
            assert_eq!(
                has_ascii_uppercase(&bytes),
                byte_scan(&bytes),
                "bytes={bytes:02x?}",
            );
        }
    }
}
