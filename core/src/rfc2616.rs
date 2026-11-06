use thiserror::Error;

use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::http::HttpMethod;

pub type EncodingParams = Vec<(Box<[u8]>, Box<[u8]>)>;
pub type EncodingList = Vec<(Box<[u8]>, EncodingParams)>;
pub type TokenQPair = (Box<[u8]>, f64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyAllowed {
    Defined,
    NotDefined,
    NotAllowed,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ParseEncodingError {
    #[error("header value must not be empty")]
    EmptyHeader,
    #[error("encoding must have a name")]
    MissingEncodingName,
    #[error("encoding must not be empty")]
    EmptyEncoding,
    #[error("parameter key must not be empty")]
    EmptyParameterKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRangeSpec {
    pub first_byte: u64,
    pub last_byte: u64,
    pub instance_length: u64,
}

pub fn is_request_body_allowed(method: Option<HttpMethod>) -> BodyAllowed {
    match method {
        Some(HttpMethod::Trace) => BodyAllowed::NotAllowed,
        Some(HttpMethod::Options | HttpMethod::Post | HttpMethod::Put) => BodyAllowed::Defined,
        _ => BodyAllowed::NotDefined,
    }
}

pub fn response_body_must_be_empty(status: u32) -> bool {
    status == 304 || status == 204 || (100..200).contains(&status)
}

pub fn body_implied(headers: &HttpHeaders) -> bool {
    headers.exists(HttpHeaderCode::TransferEncoding)
        || headers.exists(HttpHeaderCode::ContentLength)
}

pub fn parse_qvalues(value: impl AsRef<[u8]>, output: &mut Vec<TokenQPair>) -> bool {
    let Ok(encodings) = parse_encoding(value) else {
        return false;
    };
    let mut success = true;
    for (encoding, params) in encodings {
        let qvalue = match parse_qvalue(&params) {
            Some(qvalue) => qvalue,
            None => {
                success = false;
                1.0
            }
        };
        output.push((encoding, qvalue));
    }
    success
}

pub fn parse_encoding(header: impl AsRef<[u8]>) -> Result<EncodingList, ParseEncodingError> {
    let top_level = split_ignoring_empty(header.as_ref(), b',');
    if top_level.is_empty() {
        return Err(ParseEncodingError::EmptyHeader);
    }

    let mut result = Vec::with_capacity(top_level.len());
    for entry in top_level {
        let components = split_ignoring_empty(entry, b';');
        if components.is_empty() {
            return Err(ParseEncodingError::MissingEncodingName);
        }
        let encoding = trim_whitespace(components[0]);
        if encoding.is_empty() {
            return Err(ParseEncodingError::EmptyEncoding);
        }
        let mut params = Vec::with_capacity(components.len().saturating_sub(1));
        for component in &components[1..] {
            let separator = component
                .iter()
                .position(|byte| *byte == b'=')
                .unwrap_or(component.len());
            let key = trim_whitespace(&component[..separator]);
            let value = if separator == component.len() {
                b"".as_slice()
            } else {
                trim_whitespace(&component[separator + 1..])
            };
            if key.is_empty() {
                return Err(ParseEncodingError::EmptyParameterKey);
            }
            params.push((
                key.to_vec().into_boxed_slice(),
                value.to_vec().into_boxed_slice(),
            ));
        }
        result.push((encoding.to_vec().into_boxed_slice(), params));
    }
    Ok(result)
}

pub fn accepts_encoding(header: impl AsRef<[u8]>, encoding: &[u8]) -> bool {
    let Ok(encodings) = parse_encoding(header) else {
        return false;
    };
    accepts_encoding_list(&encodings, encoding)
}

pub fn accepts_encoding_list(encodings: &EncodingList, encoding: &[u8]) -> bool {
    for (candidate, params) in encodings {
        if candidate.eq_ignore_ascii_case(encoding) {
            return match parse_qvalue(params) {
                Some(qvalue) => qvalue > 0.0,
                None => true,
            };
        }
    }
    false
}

pub fn parse_byte_range_spec(value: impl AsRef<[u8]>) -> Option<ByteRangeSpec> {
    let value = value.as_ref();
    let mut cursor = 6;
    if !value.starts_with(b"bytes ") {
        return None;
    }

    let (first_byte, last_byte) = if let Some(first) = parse_unsigned_long(value, &mut cursor) {
        if value.get(cursor) != Some(&b'-') {
            return None;
        }
        cursor += 1;
        let last = parse_unsigned_long(value, &mut cursor)?;
        (first, last)
    } else {
        if value.get(cursor) != Some(&b'*') {
            return None;
        }
        cursor += 1;
        (0, u64::MAX)
    };

    if value.get(cursor) != Some(&b'/') {
        return None;
    }
    cursor += 1;
    let instance_length = if value.get(cursor) == Some(&b'*') {
        cursor += 1;
        u64::MAX
    } else {
        parse_unsigned_long(value, &mut cursor)?
    };

    if cursor < value.len() && value[cursor] != 0 {
        return None;
    }
    if last_byte < first_byte
        || last_byte.wrapping_sub(first_byte).wrapping_add(1) > instance_length
    {
        return None;
    }
    Some(ByteRangeSpec {
        first_byte,
        last_byte,
        instance_length,
    })
}

fn parse_qvalue(params: &EncodingParams) -> Option<f64> {
    let mut qvalue = 1.0;
    for (key, value) in params {
        if key.as_ref() == b"q" {
            qvalue = std::str::from_utf8(trim_numeric_whitespace(value))
                .ok()?
                .parse::<f64>()
                .ok()?;
        }
    }
    Some(qvalue)
}

fn split_ignoring_empty(value: &[u8], delimiter: u8) -> Vec<&[u8]> {
    value
        .split(|byte| *byte == delimiter)
        .filter(|value| !value.is_empty())
        .collect()
}

fn trim_whitespace(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\n' | b'\t' | b'\r'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\n' | b'\t' | b'\r'))
        .map_or(start, |index| index + 1);
    &value[start..end]
}

fn trim_numeric_whitespace(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t'..=b'\r'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\t'..=b'\r'))
        .map_or(start, |index| index + 1);
    &value[start..end]
}

fn parse_unsigned_long(value: &[u8], cursor: &mut usize) -> Option<u64> {
    while value
        .get(*cursor)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'..=b'\r'))
    {
        *cursor += 1;
    }
    let negative = match value.get(*cursor) {
        Some(b'-') => {
            *cursor += 1;
            true
        }
        Some(b'+') => {
            *cursor += 1;
            false
        }
        _ => false,
    };
    let start = *cursor;
    let mut parsed = 0_u64;
    let mut overflow = false;
    while let Some(digit) = value
        .get(*cursor)
        .and_then(|byte| byte.checked_sub(b'0'))
        .filter(|digit| *digit <= 9)
    {
        if let Some(next) = parsed
            .checked_mul(10)
            .and_then(|parsed| parsed.checked_add(u64::from(digit)))
        {
            parsed = next;
        } else {
            overflow = true;
        }
        *cursor += 1;
    }
    if *cursor == start {
        return None;
    }
    if overflow {
        Some(u64::MAX)
    } else if negative {
        Some(parsed.wrapping_neg())
    } else {
        Some(parsed)
    }
}
