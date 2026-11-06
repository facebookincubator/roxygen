use std::net::IpAddr;
use std::ops::Range;

use crate::owned_bytes::OwnedBytes;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UrlMode {
    #[default]
    StrictCompat,
    Strict,
}

#[derive(Debug, Default)]
struct Components {
    scheme: Box<[u8]>,
    authority: Box<[u8]>,
    host: Box<[u8]>,
    path: Range<usize>,
    query: Range<usize>,
    fragment: Range<usize>,
    port: u16,
    valid: bool,
}

/// An owned parse of an absolute URL or URI reference.
#[derive(Debug)]
pub struct ParseUrl {
    url: OwnedBytes,
    components: Components,
}

impl ParseUrl {
    #[expect(
        clippy::self_named_constructors,
        reason = "preserves the assigned ParseURL::parseURL API name"
    )]
    pub fn parse_url(url: impl AsRef<[u8]>, strict: bool) -> Option<Self> {
        let parsed = Self::parse_url_maybe_invalid(url, strict);
        parsed.valid().then_some(parsed)
    }

    pub fn parse_url_maybe_invalid(url: impl AsRef<[u8]>, strict: bool) -> Self {
        let url = OwnedBytes::new(url.as_ref());
        let components = parse_components(url.bytes(), strict);
        Self { url, components }
    }

    pub fn is_supported_scheme(location: impl AsRef<[u8]>) -> bool {
        let location = location.as_ref();
        let Some(scheme_end) = find_bytes(location, b"://") else {
            return true;
        };
        let scheme = &location[..scheme_end];
        scheme == b"http" || scheme == b"https"
    }

    pub fn get_query_param<'a>(query: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
        let mut remaining = query;
        while !remaining.is_empty() {
            let separator = find_byte(remaining, b'&').unwrap_or(remaining.len());
            let parameter = &remaining[..separator];
            remaining = if separator == remaining.len() {
                &[]
            } else {
                &remaining[separator + 1..]
            };

            let Some(value) = parameter.strip_prefix(name) else {
                continue;
            };
            if value.is_empty() {
                return Some(value);
            }
            if value[0] == b'=' {
                return Some(&value[1..]);
            }
        }
        None
    }

    /// Resolves a redirect location using the original URL or `Host` fallback.
    pub fn get_redirect_destination(
        url: impl AsRef<[u8]>,
        request_scheme: impl AsRef<[u8]>,
        location: impl AsRef<[u8]>,
        header_host: impl AsRef<[u8]>,
    ) -> Option<Box<[u8]>> {
        let location = location.as_ref();
        let new_url = Self::parse_url(location, false)?;
        if new_url.has_host() {
            return Some(new_url.url().to_vec().into_boxed_slice());
        }

        let original_url = Self::parse_url(url, false)
            .filter(Self::has_host)
            .or_else(|| Self::parse_url(header_host, false).filter(Self::has_host))?;
        let host_and_port = original_url.host_and_port();
        let request_scheme = request_scheme.as_ref();
        let mut destination =
            Vec::with_capacity(request_scheme.len() + 3 + host_and_port.len() + location.len());
        destination.extend_from_slice(request_scheme);
        destination.extend_from_slice(b"://");
        destination.extend_from_slice(&host_and_port);
        destination.extend_from_slice(location);
        Some(destination.into_boxed_slice())
    }

    pub fn url(&self) -> &[u8] {
        self.url.bytes()
    }

    pub fn scheme(&self) -> &[u8] {
        &self.components.scheme
    }

    pub fn authority(&self) -> &[u8] {
        &self.components.authority
    }

    pub fn has_host(&self) -> bool {
        self.valid() && !self.components.host.is_empty()
    }

    pub fn host(&self) -> &[u8] {
        &self.components.host
    }

    pub fn host_no_brackets(&self) -> &[u8] {
        strip_brackets(&self.components.host)
    }

    pub fn host_is_ip_address(&self) -> bool {
        if !self.valid() {
            return false;
        }
        std::str::from_utf8(self.host_no_brackets())
            .ok()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .is_some()
    }

    pub fn port(&self) -> u16 {
        self.components.port
    }

    pub fn host_and_port(&self) -> Box<[u8]> {
        join_host_and_port(self.host(), self.port())
    }

    pub fn path(&self) -> &[u8] {
        &self.url.bytes()[self.components.path.clone()]
    }

    pub(crate) fn path_range(&self) -> Range<usize> {
        self.components.path.clone()
    }

    pub fn query(&self) -> &[u8] {
        &self.url.bytes()[self.components.query.clone()]
    }

    pub(crate) fn query_range(&self) -> Range<usize> {
        self.components.query.clone()
    }

    pub fn fragment(&self) -> &[u8] {
        &self.url.bytes()[self.components.fragment.clone()]
    }

    pub fn valid(&self) -> bool {
        self.components.valid
    }

    pub fn request_path(&self) -> &[u8] {
        if !self.valid() {
            return b"";
        }
        if self.path().is_empty() {
            b"/"
        } else {
            self.path()
        }
    }

    pub fn request_query(&self) -> &[u8] {
        if self.valid() { self.query() } else { b"" }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UrlParts<'a> {
    pub scheme: &'a [u8],
    pub host: &'a [u8],
    pub port: u16,
    pub path: &'a [u8],
    pub query: &'a [u8],
    pub fragment: &'a [u8],
}

/// An owned URL value with Proxygen-compatible scheme validity and defaults.
#[derive(Clone, Debug, Default)]
pub struct Url {
    scheme: Box<[u8]>,
    host: Box<[u8]>,
    port: u16,
    path: Box<[u8]>,
    query: Box<[u8]>,
    fragment: Box<[u8]>,
    url: Box<[u8]>,
    valid: bool,
}

impl Url {
    pub fn parse(url: impl AsRef<[u8]>, secure: bool, mode: UrlMode) -> Self {
        let parsed = ParseUrl::parse_url_maybe_invalid(url, mode == UrlMode::Strict);
        let scheme = normalize_scheme(parsed.scheme(), secure);
        let valid_scheme = is_http_scheme(&scheme);
        let port = match parsed.port() {
            0 if scheme.as_ref() == b"https" => 443,
            0 => 80,
            port => port,
        };
        Self {
            scheme,
            host: parsed.host_no_brackets().to_vec().into_boxed_slice(),
            port,
            path: parsed.path().to_vec().into_boxed_slice(),
            query: parsed.query().to_vec().into_boxed_slice(),
            fragment: parsed.fragment().to_vec().into_boxed_slice(),
            url: parsed.url().to_vec().into_boxed_slice(),
            valid: valid_scheme && (mode == UrlMode::StrictCompat || parsed.valid()),
        }
    }

    pub fn from_parts(parts: UrlParts<'_>) -> Self {
        let scheme = normalize_scheme(parts.scheme, false);
        let valid = is_http_scheme(&scheme);
        let host = parts.host.to_vec().into_boxed_slice();
        let serialized_port = parts.port;
        let url = create_url(
            &scheme,
            &join_host_and_port(&host, serialized_port),
            parts.path,
            parts.query,
            parts.fragment,
        );
        let port = match parts.port {
            0 if scheme.as_ref() == b"https" => 443,
            0 => 80,
            port => port,
        };
        Self {
            scheme,
            host,
            port,
            path: parts.path.to_vec().into_boxed_slice(),
            query: parts.query.to_vec().into_boxed_slice(),
            fragment: parts.fragment.to_vec().into_boxed_slice(),
            url,
            valid,
        }
    }

    pub fn create_url(
        scheme: &[u8],
        host_and_port: &[u8],
        path: &[u8],
        query: &[u8],
        fragment: &[u8],
    ) -> Box<[u8]> {
        create_url(scheme, host_and_port, path, query, fragment)
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn url(&self) -> &[u8] {
        &self.url
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn scheme(&self) -> &[u8] {
        &self.scheme
    }

    pub fn is_secure(&self) -> bool {
        self.scheme.as_ref() == b"https"
    }

    pub fn has_host(&self) -> bool {
        self.valid && !self.host.is_empty()
    }

    pub fn host(&self) -> &[u8] {
        &self.host
    }

    pub fn host_and_port(&self) -> Box<[u8]> {
        join_host_and_port(&self.host, self.port)
    }

    pub fn host_and_port_omit_default(&self) -> Box<[u8]> {
        if self.port == 0 || self.port == if self.is_secure() { 443 } else { 80 } {
            return self.host.clone();
        }
        self.host_and_port()
    }

    pub fn path(&self) -> &[u8] {
        &self.path
    }

    pub fn query(&self) -> &[u8] {
        &self.query
    }

    pub fn fragment(&self) -> &[u8] {
        &self.fragment
    }

    pub fn make_relative_url(&self) -> Box<[u8]> {
        let mut relative = Vec::new();
        if self.path.is_empty() {
            relative.push(b'/');
        } else {
            relative.extend_from_slice(&self.path);
        }
        if !self.query.is_empty() {
            relative.push(b'?');
            relative.extend_from_slice(&self.query);
        }
        if !self.fragment.is_empty() {
            relative.push(b'#');
            relative.extend_from_slice(&self.fragment);
        }
        relative.into_boxed_slice()
    }
}

impl PartialEq for Url {
    fn eq(&self, other: &Self) -> bool {
        self.scheme == other.scheme
            && self.host == other.host
            && self.port == other.port
            && self.path == other.path
            && self.query == other.query
            && self.fragment == other.fragment
            && self.url == other.url
    }
}

impl Eq for Url {}

fn parse_components(url: &[u8], strict: bool) -> Components {
    if url == b"/" {
        return Components {
            path: 0..1,
            valid: true,
            ..Components::default()
        };
    }

    match find_bytes(url, b"://") {
        Some(scheme_end)
            if scheme_end != 0
                && url[..scheme_end]
                    .iter()
                    .all(|byte| byte.is_ascii_alphabetic()) =>
        {
            parse_fully_formed(url, scheme_end, strict)
        }
        _ => parse_non_fully_formed(url, strict),
    }
}

fn parse_fully_formed(url: &[u8], scheme_end: usize, strict: bool) -> Components {
    if !validate_url(url, strict) {
        return Components::default();
    }

    let authority_start = scheme_end + 3;
    let authority_end = find_any_from(url, authority_start, b"/?#").unwrap_or(url.len());
    if authority_start == authority_end {
        return Components::default();
    }

    let raw_authority = &url[authority_start..authority_end];
    if url.get(authority_end) == Some(&b'#') {
        return Components::default();
    }
    let Some((host, port)) = parse_full_authority(raw_authority) else {
        return Components::default();
    };
    let mut components = Components {
        scheme: url[..scheme_end].to_vec().into_boxed_slice(),
        host: host.to_vec().into_boxed_slice(),
        port,
        ..Components::default()
    };

    let query_start = find_byte_from(url, b'?', authority_end);
    let fragment_start = find_byte_from(url, b'#', authority_end);
    let path_end = earliest(query_start, fragment_start).unwrap_or(url.len());
    if authority_end < path_end && url[authority_end] == b'/' {
        components.path = authority_end..path_end;
        if !url[components.path.clone()]
            .iter()
            .all(|byte| is_full_url_char(*byte))
        {
            return Components::default();
        }
    }
    if let Some(query_start) = query_start {
        if fragment_start.is_none_or(|fragment_start| query_start < fragment_start) {
            let query_end = fragment_start.unwrap_or(url.len());
            components.query = query_start + 1..query_end;
            if !url[components.query.clone()]
                .iter()
                .all(|byte| *byte == b'?' || is_full_url_char(*byte))
            {
                return Components::default();
            }
        }
    }
    if let Some(fragment_start) = fragment_start {
        let mut fragment_content_start = fragment_start + 1;
        while url.get(fragment_content_start) == Some(&b'#') {
            fragment_content_start += 1;
        }
        components.fragment = fragment_content_start..url.len();
        if !url[components.fragment.clone()]
            .iter()
            .all(|byte| matches!(*byte, b'?' | b'#') || is_full_url_char(*byte))
        {
            return Components::default();
        }
    }
    components.authority = join_host_and_port(&components.host, components.port);
    components.valid = true;
    components
}

fn parse_non_fully_formed(url: &[u8], strict: bool) -> Components {
    if url.is_empty() || !validate_url(url, strict) {
        return Components::default();
    }

    let path_start = find_byte(url, b'/');
    let query_start = find_byte(url, b'?');
    let fragment_start = find_byte(url, b'#');
    let path_end = earliest(query_start, fragment_start).unwrap_or(url.len());
    let authority_end = path_start.map_or(path_end, |path_start| path_start.min(path_end));
    let mut components = Components {
        authority: url[..authority_end].to_vec().into_boxed_slice(),
        ..Components::default()
    };

    if let Some(path_start) = path_start
        && path_start < path_end
    {
        components.path = path_start..path_end;
    }

    let query_end = fragment_start.unwrap_or(url.len());
    if let Some(query_start) = query_start {
        if query_start < query_end {
            components.query = query_start + 1..query_end;
        } else if fragment_start.is_some_and(|fragment_start| fragment_start < query_start) {
            return components;
        }
    }

    if let Some(fragment_start) = fragment_start {
        components.fragment = fragment_start + 1..url.len();
    }
    if !parse_authority(&mut components) {
        return components;
    }
    components.valid = true;
    components
}

fn parse_authority(components: &mut Components) -> bool {
    let authority = components.authority.as_ref();
    let left_bracket = find_byte(authority, b'[');
    let right_bracket = find_byte(authority, b']');
    let colon_start = right_bracket.map_or(0, |right| right + 1);
    let port_separator = find_byte_from(authority, b':', colon_start);

    if let Some(port_separator) = port_separator {
        let (port, valid) = parse_port_prefix(&authority[port_separator + 1..]);
        components.port = port;
        if !valid {
            return false;
        }
    }

    let host = match (left_bracket, right_bracket) {
        (None, None) => &authority[..port_separator.unwrap_or(authority.len())],
        (Some(left), Some(right)) if left < right => &authority[left..=right],
        _ => return false,
    };
    components.host = host.to_vec().into_boxed_slice();
    true
}

fn parse_full_authority(authority: &[u8]) -> Option<(&[u8], u16)> {
    let host_and_port = if let Some(user_info_end) = find_byte(authority, b'@') {
        let user_info = &authority[..user_info_end];
        if !user_info.iter().all(|byte| is_user_info_char(*byte)) {
            return None;
        }
        let host_and_port = &authority[user_info_end + 1..];
        if host_and_port.contains(&b'@') {
            return None;
        }
        host_and_port
    } else {
        authority
    };

    if host_and_port.first() == Some(&b'[') {
        let right_bracket = find_byte(host_and_port, b']')?;
        let host = &host_and_port[..=right_bracket];
        let address = &host[1..host.len() - 1];
        if address.is_empty()
            || !address
                .iter()
                .all(|byte| byte.is_ascii_hexdigit() || matches!(*byte, b':' | b'.'))
        {
            return None;
        }
        let remainder = &host_and_port[right_bracket + 1..];
        let port = if remainder.is_empty() {
            0
        } else {
            parse_port(remainder.strip_prefix(b":")?)?
        };
        return Some((host, port));
    }

    let port_separator = find_byte(host_and_port, b':');
    let host = &host_and_port[..port_separator.unwrap_or(host_and_port.len())];
    if host.is_empty() || !host.iter().all(|byte| is_host_char(*byte)) {
        return None;
    }
    let port = match port_separator {
        Some(port_separator) => parse_port(&host_and_port[port_separator + 1..])?,
        None => 0,
    };
    Some((host, port))
}

fn parse_port(port: &[u8]) -> Option<u16> {
    let (value, valid) = parse_port_prefix(port);
    valid.then_some(value)
}

fn parse_port_prefix(port: &[u8]) -> (u16, bool) {
    let mut value = 0_u16;
    let mut parsed_digit = false;
    for byte in port {
        let Some(digit) = byte.checked_sub(b'0').filter(|digit| *digit <= 9) else {
            return (value, false);
        };
        parsed_digit = true;
        let Some(next) = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u16::from(digit)))
        else {
            return (0, false);
        };
        value = next;
    }
    (value, parsed_digit)
}

fn validate_url(url: &[u8], strict: bool) -> bool {
    url.iter()
        .all(|byte| *byte > 0x20 && *byte != 0x7f && (!strict || *byte <= 0x7f))
}

fn is_user_info_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'_'
                | b'.'
                | b'!'
                | b'~'
                | b'*'
                | b'\''
                | b'('
                | b')'
                | b'%'
                | b';'
                | b':'
                | b'&'
                | b'='
                | b'+'
                | b'$'
                | b','
        )
}

fn is_host_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')
}

fn is_full_url_char(byte: u8) -> bool {
    if !byte.is_ascii() {
        return true;
    }
    (b'!'..=b'~').contains(&byte) && !matches!(byte, b'?' | b'#')
}

fn normalize_scheme(scheme: &[u8], secure: bool) -> Box<[u8]> {
    if scheme.is_empty() && secure {
        return b"https".to_vec().into_boxed_slice();
    }
    let mut scheme = scheme.to_vec();
    scheme.make_ascii_lowercase();
    scheme.into_boxed_slice()
}

fn is_http_scheme(scheme: &[u8]) -> bool {
    scheme == b"http" || scheme == b"https"
}

fn strip_brackets(host: &[u8]) -> &[u8] {
    if host.len() >= 2 && host.first() == Some(&b'[') && host.last() == Some(&b']') {
        &host[1..host.len() - 1]
    } else {
        host
    }
}

fn join_host_and_port(host: &[u8], port: u16) -> Box<[u8]> {
    if port == 0 {
        return host.to_vec().into_boxed_slice();
    }
    let port = port.to_string();
    let mut result = Vec::with_capacity(host.len() + 1 + port.len());
    result.extend_from_slice(host);
    result.push(b':');
    result.extend_from_slice(port.as_bytes());
    result.into_boxed_slice()
}

fn create_url(
    scheme: &[u8],
    host_and_port: &[u8],
    path: &[u8],
    query: &[u8],
    fragment: &[u8],
) -> Box<[u8]> {
    let mut result = Vec::with_capacity(
        scheme.len()
            + 3
            + host_and_port.len()
            + 1
            + path.len()
            + usize::from(!query.is_empty())
            + query.len()
            + usize::from(!fragment.is_empty())
            + fragment.len(),
    );
    result.extend_from_slice(scheme);
    result.extend_from_slice(b"://");
    result.extend_from_slice(host_and_port);
    result.push(b'/');
    result.extend_from_slice(path);
    if !query.is_empty() {
        result.push(b'?');
        result.extend_from_slice(query);
    }
    if !fragment.is_empty() {
        result.push(b'#');
        result.extend_from_slice(fragment);
    }
    result.into_boxed_slice()
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn find_byte(bytes: &[u8], needle: u8) -> Option<usize> {
    find_byte_from(bytes, needle, 0)
}

fn find_byte_from(bytes: &[u8], needle: u8, start: usize) -> Option<usize> {
    bytes[start..]
        .iter()
        .position(|byte| *byte == needle)
        .map(|position| start + position)
}

fn find_any_from(bytes: &[u8], start: usize, needles: &[u8]) -> Option<usize> {
    bytes[start..]
        .iter()
        .position(|byte| needles.contains(byte))
        .map(|position| start + position)
}

fn earliest(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(position), None) | (None, Some(position)) => Some(position),
        (None, None) => None,
    }
}
