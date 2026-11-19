/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashSet;

/// Default urgency used when no `u` dictionary member is present.
pub const DEFAULT_HTTP_PRIORITY_URGENCY: u8 = 3;
/// Constructor default for incremental delivery.
pub const DEFAULT_HTTP_PRIORITY_INCREMENTAL: bool = true;
/// Default ordering identifier.
pub const DEFAULT_HTTP_PRIORITY_ORDER_ID: u32 = 0;
/// Default paused state.
pub const DEFAULT_HTTP_PRIORITY_PAUSED: bool = false;
/// Default required transfer rate in bits per second.
pub const DEFAULT_HTTP_PRIORITY_REQUIRED_BPS: u64 = 0;
/// Lowest valid parsed urgency.
pub const MIN_HTTP_PRIORITY_URGENCY: i64 = 0;
/// Highest valid urgency.
pub const MAX_HTTP_PRIORITY_URGENCY: u8 = 7;

/// Parsed and rendered contents of an HTTP `Priority` header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpPriority {
    /// Relative urgency in the inclusive range `0..=7`.
    pub urgency: u8,
    /// Whether the response may be delivered incrementally.
    pub incremental: bool,
    /// Application-defined ordering identifier.
    pub order_id: u32,
    /// Whether delivery is paused.
    pub paused: bool,
    /// Required transfer rate in bits per second, or zero when unspecified.
    pub required_bps: u64,
}

impl HttpPriority {
    /// Constructs a priority, clamping urgency to `0..=7`.
    pub const fn new(
        urgency: u8,
        incremental: bool,
        order_id: u32,
        paused: bool,
        required_bps: u64,
    ) -> Self {
        Self {
            urgency: if urgency > MAX_HTTP_PRIORITY_URGENCY {
                MAX_HTTP_PRIORITY_URGENCY
            } else {
                urgency
            },
            incremental,
            order_id,
            paused,
            required_bps,
        }
    }

    /// Constructs a priority with default values for the optional extensions.
    pub const fn from_urgency_and_incremental(urgency: u8, incremental: bool) -> Self {
        Self::new(
            urgency,
            incremental,
            DEFAULT_HTTP_PRIORITY_ORDER_ID,
            DEFAULT_HTTP_PRIORITY_PAUSED,
            DEFAULT_HTTP_PRIORITY_REQUIRED_BPS,
        )
    }
}

impl Default for HttpPriority {
    fn default() -> Self {
        Self::new(
            DEFAULT_HTTP_PRIORITY_URGENCY,
            DEFAULT_HTTP_PRIORITY_INCREMENTAL,
            DEFAULT_HTTP_PRIORITY_ORDER_ID,
            DEFAULT_HTTP_PRIORITY_PAUSED,
            DEFAULT_HTTP_PRIORITY_REQUIRED_BPS,
        )
    }
}

/// Access to the `Priority` header, implemented by the concrete HTTP message type.
///
/// The priority helpers read only the unique `Priority` header and replace it when
/// setting a priority, so this trait keeps them independent of message state.
pub trait HttpPriorityHeaderAccess: Sized {
    /// Returns the unique `Priority` value or an empty byte string.
    fn priority_header_value(&self) -> &[u8];

    /// Replaces every `Priority` value with one owned copy of `value`.
    fn set_priority_header_value(&mut self, value: &[u8]);

    /// Parses this message's unique `Priority` header.
    fn get_http_priority(&self) -> Option<HttpPriority> {
        http_priority_from_http_message(self)
    }

    /// Serializes `priority` into this message's `Priority` header.
    fn set_http_priority(&mut self, priority: HttpPriority) {
        let value = http_priority_to_string(priority);
        self.set_priority_header_value(value.as_bytes());
    }

    /// Sets urgency and incremental delivery with all extension defaults.
    fn set_http_priority_parts(&mut self, urgency: u8, incremental: bool) {
        self.set_http_priority(HttpPriority::from_urgency_and_incremental(
            urgency,
            incremental,
        ));
    }
}

/// Renders a priority dictionary with its members in the fixed order `u`, `i`, `o`,
/// `p`, `r`.
pub fn http_priority_to_string(priority: HttpPriority) -> String {
    let urgency = priority.urgency.min(MAX_HTTP_PRIORITY_URGENCY);
    let mut rendered = format!("u={urgency}");
    if priority.incremental {
        rendered.push_str(",i");
    }
    if priority.order_id > 0 {
        rendered.push_str(",o=");
        rendered.push_str(&priority.order_id.to_string());
    }
    if priority.paused {
        rendered.push_str(",p");
    }
    if priority.required_bps > 0 {
        rendered.push_str(",r=");
        rendered.push_str(&priority.required_bps.to_string());
    }
    rendered
}

/// Parses the unique `Priority` header exposed by a message.
pub fn http_priority_from_http_message(
    message: &impl HttpPriorityHeaderAccess,
) -> Option<HttpPriority> {
    http_priority_from_string(message.priority_header_value())
}

/// Parses the structured-field dictionary form of an HTTP priority.
pub fn http_priority_from_string(priority: impl AsRef<[u8]>) -> Option<HttpPriority> {
    let priority = priority.as_ref();
    if priority.is_empty() {
        return None;
    }

    let mut parser = DictionaryParser::new(priority);
    let fields = parser.parse_priority_fields()?;
    if !fields.known_member_seen || fields.malformed {
        return None;
    }
    if !(MIN_HTTP_PRIORITY_URGENCY..=i64::from(MAX_HTTP_PRIORITY_URGENCY)).contains(&fields.urgency)
        || fields.order_id < 0
        || fields.required_bps < 0
    {
        return None;
    }

    Some(HttpPriority::new(
        fields.urgency as u8,
        fields.incremental,
        fields.order_id as u32,
        fields.paused,
        fields.required_bps as u64,
    ))
}

#[derive(Clone, Copy)]
enum StructuredItem {
    Integer(i64),
    Boolean(bool),
    Other,
}

struct PriorityFields {
    urgency: i64,
    incremental: bool,
    order_id: i64,
    paused: bool,
    required_bps: i64,
    known_member_seen: bool,
    malformed: bool,
}

impl Default for PriorityFields {
    fn default() -> Self {
        Self {
            urgency: i64::from(DEFAULT_HTTP_PRIORITY_URGENCY),
            incremental: false,
            order_id: 0,
            paused: false,
            required_bps: 0,
            known_member_seen: false,
            malformed: false,
        }
    }
}

struct DictionaryParser<'a> {
    input: &'a [u8],
    cursor: usize,
}

impl<'a> DictionaryParser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, cursor: 0 }
    }

    fn parse_priority_fields(&mut self) -> Option<PriorityFields> {
        self.skip_optional_whitespace();
        if self.is_empty() {
            return None;
        }

        let mut fields = PriorityFields::default();
        let mut keys = HashSet::new();

        loop {
            self.skip_optional_whitespace();
            let key = self.parse_identifier()?;
            if !keys.insert(key) {
                return None;
            }

            let item = if self.consume(b'=') {
                self.parse_item()?
            } else {
                StructuredItem::Boolean(true)
            };
            Self::record_field(&mut fields, key, item);

            if self.is_empty() {
                return Some(fields);
            }
            self.skip_optional_whitespace();
            if !self.consume(b',') || self.is_empty() {
                return None;
            }
        }
    }

    fn record_field(fields: &mut PriorityFields, key: &[u8], item: StructuredItem) {
        match key {
            b"u" => {
                fields.known_member_seen = true;
                if let StructuredItem::Integer(value) = item {
                    fields.urgency = value;
                } else {
                    fields.malformed = true;
                }
            }
            b"i" => {
                fields.known_member_seen = true;
                if let StructuredItem::Boolean(value) = item {
                    fields.incremental = value;
                } else {
                    fields.malformed = true;
                }
            }
            b"o" => {
                fields.known_member_seen = true;
                if let StructuredItem::Integer(value) = item {
                    fields.order_id = value;
                } else {
                    fields.malformed = true;
                }
            }
            b"p" => {
                fields.known_member_seen = true;
                if let StructuredItem::Boolean(value) = item {
                    fields.paused = value;
                } else {
                    fields.malformed = true;
                }
            }
            b"r" => {
                fields.known_member_seen = true;
                if let StructuredItem::Integer(value) = item {
                    fields.required_bps = value;
                } else {
                    fields.malformed = true;
                }
            }
            _ => {}
        }
    }

    fn parse_identifier(&mut self) -> Option<&'a [u8]> {
        let start = self.cursor;
        if !self.peek()?.is_ascii_lowercase() {
            return None;
        }
        self.cursor += 1;
        while self.peek().is_some_and(is_identifier_byte) {
            self.cursor += 1;
        }
        Some(&self.input[start..self.cursor])
    }

    fn parse_item(&mut self) -> Option<StructuredItem> {
        self.skip_optional_whitespace();
        match self.peek()? {
            b'"' => self.parse_string(),
            b'*' => self.parse_binary_content(),
            b'-' | b'0'..=b'9' => self.parse_number(),
            b'?' => self.parse_boolean(),
            _ => None,
        }
    }

    fn parse_number(&mut self) -> Option<StructuredItem> {
        const MAX_INTEGER_LENGTH: usize = 19;
        const MAX_FLOAT_LENGTH: usize = 16;

        let start = self.cursor;
        let negative = self.consume(b'-');
        if !self.peek()?.is_ascii_digit() {
            return None;
        }

        let mut is_float = false;
        while let Some(byte) = self.peek() {
            if byte.is_ascii_digit() {
                self.cursor += 1;
            } else if !is_float && byte == b'.' {
                is_float = true;
                self.cursor += 1;
            } else {
                break;
            }

            let sign_length = if negative { 1 } else { 0 };
            let content_length = self.cursor - start - sign_length;
            let maximum = if is_float {
                MAX_FLOAT_LENGTH
            } else {
                MAX_INTEGER_LENGTH
            };
            if content_length > maximum {
                return None;
            }
        }

        let bytes = &self.input[start..self.cursor];
        let number = std::str::from_utf8(bytes).ok()?;
        if is_float {
            if bytes.last() == Some(&b'.') {
                return None;
            }
            number.parse::<f64>().ok()?;
            Some(StructuredItem::Other)
        } else {
            Some(StructuredItem::Integer(number.parse::<i64>().ok()?))
        }
    }

    fn parse_boolean(&mut self) -> Option<StructuredItem> {
        self.cursor += 1;
        let value = match self.peek()? {
            b'0' => false,
            b'1' => true,
            _ => return None,
        };
        self.cursor += 1;
        self.is_empty().then_some(StructuredItem::Boolean(value))
    }

    fn parse_string(&mut self) -> Option<StructuredItem> {
        self.cursor += 1;
        while let Some(byte) = self.peek() {
            self.cursor += 1;
            match byte {
                b'\\' => {
                    let escaped = self.peek()?;
                    self.cursor += 1;
                    if !matches!(escaped, b'"' | b'\\') {
                        return None;
                    }
                }
                b'"' => return Some(StructuredItem::Other),
                0x20..=0x7e => {}
                _ => return None,
            }
        }
        None
    }

    fn parse_binary_content(&mut self) -> Option<StructuredItem> {
        self.cursor += 1;
        let start = self.cursor;
        while let Some(byte) = self.peek() {
            self.cursor += 1;
            if byte == b'*' {
                return valid_base64(&self.input[start..self.cursor - 1])
                    .then_some(StructuredItem::Other);
            }
            if !(byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=')) {
                return None;
            }
        }
        None
    }

    fn skip_optional_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t')) {
            self.cursor += 1;
        }
    }

    fn consume(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.cursor).copied()
    }

    fn is_empty(&self) -> bool {
        self.cursor == self.input.len()
    }
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'*' | b'/')
}

fn valid_base64(value: &[u8]) -> bool {
    if !value.len().is_multiple_of(4) {
        return false;
    }
    let padding = value.iter().rev().take_while(|byte| **byte == b'=').count();
    if padding > 2 {
        return false;
    }
    let content_len = value.len() - padding;
    if !value[..content_len]
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
        || !value[content_len..].iter().all(|byte| *byte == b'=')
    {
        return false;
    }

    match padding {
        2 => {
            content_len >= 2
                && base64_value(value[content_len - 1]).is_some_and(|bits| bits & 0x0f == 0)
        }
        1 => {
            content_len >= 3
                && base64_value(value[content_len - 1]).is_some_and(|bits| bits & 0x03 == 0)
        }
        _ => true,
    }
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}
