/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::ops::Range;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use thiserror::Error;

use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::http::HttpHeaderSize;
use crate::http::HttpMethod;
use crate::http::Method;
use crate::owned_bytes::OwnedBytes;
use crate::priority::HttpPriority;
use crate::priority::HttpPriorityHeaderAccess;
use crate::url::ParseUrl;

pub type QueryParamMap = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SocketAddress {
    host: Box<[u8]>,
    port: u16,
    initialized: bool,
}

impl SocketAddress {
    pub fn new(host: impl AsRef<[u8]>, port: u16) -> Self {
        Self {
            host: host.as_ref().to_vec().into_boxed_slice(),
            port,
            initialized: true,
        }
    }

    pub fn host(&self) -> &[u8] {
        &self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }

    pub const fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn is_ipv6(&self) -> bool {
        std::str::from_utf8(&self.host)
            .ok()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .is_some_and(|address| address.is_ipv6())
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MessageTypeError {
    #[error("invoked request API on HTTP response")]
    RequestApiOnResponse,
    #[error("invoked response API on HTTP request")]
    ResponseApiOnRequest,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WebSocketUpgrade {
    #[default]
    None,
    Ingress,
    Egress,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Scheme {
    #[default]
    Http,
    Https,
    Masque,
}

#[derive(Debug, Default)]
struct RequestFields {
    client_address: SocketAddress,
    client_ip: Option<Box<[u8]>>,
    client_port: Option<Box<[u8]>>,
    method: Option<Method>,
    path: Option<Range<usize>>,
    query: Range<usize>,
    url: OwnedBytes,
}

// Header-memo replay clones a decoded template for every header block; its rare
// fields are empty there, so their copy paths stay out of the clone's hot code.
impl Clone for RequestFields {
    fn clone(&self) -> Self {
        let Self {
            client_address,
            client_ip,
            client_port,
            method,
            path,
            query,
            url,
        } = self;
        Self {
            client_address: clone_socket_address(client_address),
            client_ip: clone_rare(client_ip),
            client_port: clone_rare(client_port),
            method: method.clone(),
            path: path.clone(),
            query: query.clone(),
            url: url.clone(),
        }
    }
}

#[inline(always)]
fn clone_rare<T: Clone>(value: &Option<T>) -> Option<T> {
    match value {
        None => None,
        Some(value) => clone_rare_some(value),
    }
}

#[cold]
#[inline(never)]
fn clone_rare_some<T: Clone>(value: &T) -> Option<T> {
    Some(value.clone())
}

#[inline(always)]
fn clone_boxed_bytes(bytes: &[u8]) -> Box<[u8]> {
    if bytes.is_empty() {
        Box::default()
    } else {
        copy_boxed_bytes(bytes)
    }
}

#[cold]
#[inline(never)]
fn copy_boxed_bytes(bytes: &[u8]) -> Box<[u8]> {
    bytes.into()
}

#[inline(always)]
fn clone_socket_address(address: &SocketAddress) -> SocketAddress {
    SocketAddress {
        host: clone_boxed_bytes(&address.host),
        port: address.port,
        initialized: address.initialized,
    }
}

impl RequestFields {
    fn path(&self) -> &[u8] {
        match &self.path {
            None => b"",
            Some(range) if range.is_empty() => b"/",
            Some(range) => &self.url.bytes()[range.clone()],
        }
    }

    fn query(&self) -> &[u8] {
        &self.url.bytes()[self.query.clone()]
    }
}

#[derive(Clone, Debug, Default)]
struct ResponseFields {
    status: u16,
    status_set: bool,
    status_message: Cow<'static, [u8]>,
}

#[derive(Clone, Debug, Default)]
enum MessageFields {
    #[default]
    None,
    Request(RequestFields),
    Response(ResponseFields),
}

#[derive(Clone, Debug)]
struct HttpVersionText {
    bytes: [u8; 7],
    length: u8,
}

impl HttpVersionText {
    fn new(major: u8, minor: u8) -> Self {
        let mut bytes = [0; 7];
        let mut length = 0;
        for component in [major, minor] {
            if length != 0 {
                bytes[length] = b'.';
                length += 1;
            }
            if component >= 100 {
                bytes[length] = b'0' + component / 100;
                length += 1;
            }
            if component >= 10 {
                bytes[length] = b'0' + component / 10 % 10;
                length += 1;
            }
            bytes[length] = b'0' + component % 10;
            length += 1;
        }
        Self {
            bytes,
            length: length as u8,
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.length)]
    }
}

#[derive(Debug)]
pub struct HttpMessage {
    start_time: Instant,
    destination_address: SocketAddress,
    destination_ip: Box<[u8]>,
    destination_port: Box<[u8]>,
    local_ip: Box<[u8]>,
    version_string: HttpVersionText,
    fields: MessageFields,
    query_params: Option<QueryParamMap>,
    headers: HttpHeaders,
    stripped_per_hop_headers: Option<HttpHeaders>,
    header_size: HttpHeaderSize,
    websocket_upgrade: WebSocketUpgrade,
    trailers: Option<Box<HttpHeaders>>,
    sequence_number: i32,
    secure_version: i32,
    secure_cipher: Option<Box<[u8]>>,
    advanced_protocol: Option<Cow<'static, [u8]>>,
    upgrade_protocol: Option<Box<[u8]>>,
    legacy_priority: u8,
    version: (u8, u8),
    chunked: bool,
    upgraded: bool,
    wants_keepalive: bool,
    trailers_allowed: bool,
    scheme: Scheme,
}

impl Clone for HttpMessage {
    fn clone(&self) -> Self {
        let Self {
            start_time,
            destination_address,
            destination_ip,
            destination_port,
            local_ip,
            version_string,
            fields,
            query_params,
            headers,
            stripped_per_hop_headers,
            header_size,
            websocket_upgrade,
            trailers,
            sequence_number,
            secure_version,
            secure_cipher,
            advanced_protocol,
            upgrade_protocol,
            legacy_priority,
            version,
            chunked,
            upgraded,
            wants_keepalive,
            trailers_allowed,
            scheme,
        } = self;
        Self {
            start_time: *start_time,
            destination_address: clone_socket_address(destination_address),
            destination_ip: clone_boxed_bytes(destination_ip),
            destination_port: clone_boxed_bytes(destination_port),
            local_ip: clone_boxed_bytes(local_ip),
            version_string: version_string.clone(),
            fields: fields.clone(),
            query_params: clone_rare(query_params),
            headers: headers.clone(),
            stripped_per_hop_headers: clone_rare(stripped_per_hop_headers),
            header_size: *header_size,
            websocket_upgrade: *websocket_upgrade,
            trailers: clone_rare(trailers),
            sequence_number: *sequence_number,
            secure_version: *secure_version,
            secure_cipher: clone_rare(secure_cipher),
            advanced_protocol: clone_rare(advanced_protocol),
            upgrade_protocol: clone_rare(upgrade_protocol),
            legacy_priority: *legacy_priority,
            version: *version,
            chunked: *chunked,
            upgraded: *upgraded,
            wants_keepalive: *wants_keepalive,
            trailers_allowed: *trailers_allowed,
            scheme: *scheme,
        }
    }
}

impl Default for HttpMessage {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
            destination_address: SocketAddress::default(),
            destination_ip: Box::default(),
            destination_port: Box::default(),
            local_ip: Box::default(),
            version_string: HttpVersionText::new(1, 0),
            fields: MessageFields::None,
            query_params: None,
            headers: HttpHeaders::unallocated(),
            stripped_per_hop_headers: None,
            header_size: HttpHeaderSize::default(),
            websocket_upgrade: WebSocketUpgrade::None,
            trailers: None,
            sequence_number: -1,
            secure_version: 0,
            secure_cipher: None,
            advanced_protocol: None,
            upgrade_protocol: None,
            legacy_priority: crate::priority::DEFAULT_HTTP_PRIORITY_URGENCY,
            version: (1, 0),
            chunked: false,
            upgraded: false,
            wants_keepalive: true,
            trailers_allowed: false,
            scheme: Scheme::Http,
        }
    }
}

impl HttpMessage {
    pub const HTTP_VERSION_09: (u8, u8) = (0, 9);
    pub const HTTP_VERSION_10: (u8, u8) = (1, 0);
    pub const HTTP_VERSION_11: (u8, u8) = (1, 1);

    pub fn new() -> Self {
        Self::default()
    }

    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    pub fn set_ingress_websocket_upgrade(&mut self) {
        self.websocket_upgrade = WebSocketUpgrade::Ingress;
    }

    pub fn set_egress_websocket_upgrade(&mut self) {
        self.websocket_upgrade = WebSocketUpgrade::Egress;
    }

    pub fn is_ingress_websocket_upgrade(&self) -> bool {
        self.websocket_upgrade == WebSocketUpgrade::Ingress
    }

    pub fn is_egress_websocket_upgrade(&self) -> bool {
        self.websocket_upgrade == WebSocketUpgrade::Egress
    }

    pub fn set_chunked(&mut self, chunked: bool) {
        self.chunked = chunked;
    }

    pub const fn is_chunked(&self) -> bool {
        self.chunked
    }

    pub fn set_upgraded(&mut self, upgraded: bool) {
        self.upgraded = upgraded;
    }

    pub const fn is_upgraded(&self) -> bool {
        self.upgraded
    }

    pub fn set_client_address(
        &mut self,
        address: SocketAddress,
        ip: impl AsRef<[u8]>,
        port: impl AsRef<[u8]>,
    ) -> Result<(), MessageTypeError> {
        let ip = ip.as_ref();
        let port = port.as_ref();
        let request = self.request_mut()?;
        request.client_address = address;
        if !ip.is_empty() && !port.is_empty() {
            request.client_ip = Some(ip.to_vec().into_boxed_slice());
            request.client_port = Some(port.to_vec().into_boxed_slice());
        } else {
            request.client_ip = None;
            request.client_port = None;
        }
        Ok(())
    }

    pub fn set_client_socket_address(
        &mut self,
        address: SocketAddress,
    ) -> Result<(), MessageTypeError> {
        self.set_client_address(address, b"", b"")
    }

    pub fn client_address(&mut self) -> Result<&SocketAddress, MessageTypeError> {
        Ok(&self.request_mut()?.client_address)
    }

    pub fn client_ip(&mut self) -> Result<Box<[u8]>, MessageTypeError> {
        let request = self.request_mut()?;
        Ok(request
            .client_ip
            .as_deref()
            .unwrap_or_else(|| request.client_address.host())
            .to_vec()
            .into_boxed_slice())
    }

    pub fn client_port(&mut self) -> Result<Box<[u8]>, MessageTypeError> {
        let request = self.request_mut()?;
        if let Some(port) = &request.client_port {
            return Ok(port.clone());
        }
        if request.client_address.is_initialized() {
            return Ok(request
                .client_address
                .port()
                .to_string()
                .into_bytes()
                .into_boxed_slice());
        }
        Ok(Box::default())
    }

    pub fn set_destination_address(
        &mut self,
        address: SocketAddress,
        ip: impl AsRef<[u8]>,
        port: impl AsRef<[u8]>,
    ) {
        self.destination_address = address;
        let ip = ip.as_ref();
        let port = port.as_ref();
        if !ip.is_empty() && !port.is_empty() {
            self.destination_ip = ip.to_vec().into_boxed_slice();
            self.destination_port = port.to_vec().into_boxed_slice();
        } else {
            self.destination_ip = Box::default();
            self.destination_port = Box::default();
        }
    }

    pub fn set_destination_socket_address(&mut self, address: SocketAddress) {
        self.set_destination_address(address, b"", b"");
    }

    pub const fn destination_address(&self) -> &SocketAddress {
        &self.destination_address
    }

    pub fn destination_ip(&self) -> &[u8] {
        if self.destination_ip.is_empty() {
            self.destination_address.host()
        } else {
            &self.destination_ip
        }
    }

    pub fn destination_port(&self) -> Box<[u8]> {
        if !self.destination_port.is_empty() {
            return self.destination_port.clone();
        }
        if self.destination_address.is_initialized() {
            return self
                .destination_address
                .port()
                .to_string()
                .into_bytes()
                .into_boxed_slice();
        }
        Box::default()
    }

    pub fn set_local_ip(&mut self, ip: impl AsRef<[u8]>) {
        self.local_ip = ip.as_ref().to_vec().into_boxed_slice();
    }

    pub fn local_ip(&self) -> &[u8] {
        &self.local_ip
    }

    pub fn set_method(&mut self, method: impl Into<Method>) -> Result<(), MessageTypeError> {
        self.request_mut()?.method = Some(method.into());
        Ok(())
    }

    #[inline]
    pub fn method(&mut self) -> Result<Option<HttpMethod>, MessageTypeError> {
        Ok(self
            .request_mut()?
            .method
            .as_ref()
            .and_then(Method::recognized))
    }

    pub fn method_string(&mut self) -> Result<&str, MessageTypeError> {
        Ok(self
            .request_mut()?
            .method
            .as_ref()
            .map_or("", Method::as_str))
    }

    /// Borrows the request method without selecting or changing the message kind.
    pub fn request_method(&self) -> Option<&str> {
        match &self.fields {
            MessageFields::Request(request) => request.method.as_ref().map(Method::as_str),
            MessageFields::None | MessageFields::Response(_) => None,
        }
    }

    /// Borrows the full request target, including its query, when this is a request.
    pub fn request_url(&self) -> Option<&[u8]> {
        match &self.fields {
            MessageFields::Request(request) => Some(request.url.bytes()),
            MessageFields::None | MessageFields::Response(_) => None,
        }
    }

    pub fn set_url(
        &mut self,
        url: impl AsRef<[u8]>,
        strict: bool,
    ) -> Result<ParseUrl, MessageTypeError> {
        self.set_url_internal(url.as_ref(), true, strict)
    }

    pub fn url(&mut self) -> Result<&[u8], MessageTypeError> {
        Ok(self.request_mut()?.url.bytes())
    }

    pub fn path(&mut self) -> Result<&[u8], MessageTypeError> {
        Ok(self.request_mut()?.path())
    }

    pub fn query_string(&mut self) -> Result<&[u8], MessageTypeError> {
        Ok(self.request_mut()?.query())
    }

    pub fn set_http_version(&mut self, major: u8, minor: u8) {
        if self.version == (major, minor) {
            return;
        }
        self.version = (major, minor);
        self.version_string = HttpVersionText::new(major, minor);
    }

    pub const fn http_version(&self) -> (u8, u8) {
        self.version
    }

    pub fn version_string(&self) -> &[u8] {
        self.version_string.as_slice()
    }

    pub fn set_version_string(&mut self, version: &[u8]) {
        if version.len() == 3
            && version[1] == b'.'
            && version[0].is_ascii_digit()
            && version[2].is_ascii_digit()
        {
            self.set_http_version(version[0] - b'0', version[2] - b'0');
        }
    }

    pub fn set_status_message(
        &mut self,
        message: impl AsRef<[u8]>,
    ) -> Result<(), MessageTypeError> {
        let bytes = message.as_ref();
        let response = self.response_mut()?;
        let default = Self::default_reason(response.status);
        response.status_message = if bytes == default {
            Cow::Borrowed(default)
        } else {
            Cow::Owned(bytes.to_vec())
        };
        Ok(())
    }

    pub fn status_message(&mut self) -> Result<&[u8], MessageTypeError> {
        Ok(&self.response_mut()?.status_message)
    }

    /// Borrows the response reason phrase without selecting or changing the message kind.
    pub fn response_status_message(&self) -> Option<&[u8]> {
        match &self.fields {
            MessageFields::Response(response) => Some(&response.status_message),
            MessageFields::None | MessageFields::Request(_) => None,
        }
    }

    pub fn headers(&self) -> &HttpHeaders {
        &self.headers
    }

    pub fn headers_mut(&mut self) -> &mut HttpHeaders {
        &mut self.headers
    }

    pub fn extract_headers(&mut self) -> HttpHeaders {
        self.headers.take()
    }

    pub fn trailers(&self) -> Option<&HttpHeaders> {
        self.trailers.as_deref()
    }

    pub fn set_trailers(&mut self, trailers: Box<HttpHeaders>) {
        self.trailers = Some(trailers);
    }

    pub fn extract_trailers(&mut self) -> Option<Box<HttpHeaders>> {
        self.trailers.take()
    }

    pub fn process_max_forwards(&mut self) -> Result<u16, MessageTypeError> {
        let method = self.method()?;
        if !matches!(method, Some(HttpMethod::Trace | HttpMethod::Options)) {
            return Ok(0);
        }
        let value = self.headers.get_single_or_empty(b"Max-Forwards");
        if value.is_empty() {
            return Ok(0);
        }
        let Some(value) = std::str::from_utf8(trim_numeric_whitespace(value))
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
        else {
            return Ok(400);
        };
        if value < 0 {
            Ok(400)
        } else if value == 0 {
            Ok(501)
        } else {
            self.headers
                .set(b"Max-Forwards", (value - 1).to_string().as_bytes());
            Ok(0)
        }
    }

    pub fn is_http_1_0(&self) -> bool {
        self.version == Self::HTTP_VERSION_10
    }

    pub fn is_http_1_1(&self) -> bool {
        self.version == Self::HTTP_VERSION_11
    }

    pub fn is_final(&mut self) -> Result<bool, MessageTypeError> {
        if self.is_request() {
            return Ok(true);
        }
        let status = self.status_code()?;
        Ok(status == 101 || status / 100 != 1)
    }

    pub fn is_1xx_response(&mut self) -> Result<bool, MessageTypeError> {
        Ok(self.status_code()? / 100 == 1)
    }

    pub fn is_2xx_response(&mut self) -> Result<bool, MessageTypeError> {
        Ok(self.status_code()? / 100 == 2)
    }

    pub fn is_4xx_response(&mut self) -> Result<bool, MessageTypeError> {
        Ok(self.status_code()? / 100 == 4)
    }

    pub fn is_5xx_response(&mut self) -> Result<bool, MessageTypeError> {
        Ok(self.status_code()? / 100 == 5)
    }

    pub fn format_date_header() -> String {
        format_http_date(SystemTime::now())
    }

    pub fn ensure_host_header(&mut self) {
        if self.headers.exists(HttpHeaderCode::Host) {
            return;
        }
        let host = self.destination_ip().to_vec();
        if self.destination_address.is_ipv6() {
            let mut bracketed = Vec::with_capacity(host.len() + 2);
            bracketed.push(b'[');
            bracketed.extend_from_slice(&host);
            bracketed.push(b']');
            self.headers.add(HttpHeaderCode::Host, bracketed);
        } else {
            self.headers.add(HttpHeaderCode::Host, host);
        }
    }

    pub fn set_wants_keepalive(&mut self, wants_keepalive: bool) {
        self.wants_keepalive = wants_keepalive;
    }

    pub const fn wants_keepalive(&self) -> bool {
        self.wants_keepalive
    }

    pub const fn trailers_allowed(&self) -> bool {
        self.trailers_allowed
    }

    pub fn set_trailers_allowed(&mut self, allowed: bool) {
        self.trailers_allowed = allowed;
    }

    pub fn has_trailers(&self) -> bool {
        self.trailers_allowed
            && self
                .trailers
                .as_ref()
                .is_some_and(|trailers| !trailers.is_empty())
    }

    pub fn set_status_code(&mut self, status: u16) -> Result<(), MessageTypeError> {
        let response = self.response_mut()?;
        response.status = status;
        response.status_set = true;
        Ok(())
    }

    pub fn status_code(&mut self) -> Result<u16, MessageTypeError> {
        Ok(self.response_mut()?.status)
    }

    /// Reads the response status without selecting or changing the message kind.
    pub fn response_status(&self) -> Option<u16> {
        match &self.fields {
            MessageFields::Response(response) => Some(response.status),
            MessageFields::None | MessageFields::Request(_) => None,
        }
    }

    pub fn set_upgrade_protocol(&mut self, protocol: impl AsRef<[u8]>) {
        self.upgrade_protocol = Some(protocol.as_ref().to_vec().into_boxed_slice());
    }

    pub fn upgrade_protocol(&self) -> Option<&[u8]> {
        self.upgrade_protocol.as_deref()
    }

    pub fn construct_direct_response(
        &mut self,
        version: (u8, u8),
        status: u16,
        status_message: impl AsRef<[u8]>,
        content_length: i64,
    ) -> Result<(), MessageTypeError> {
        self.set_status_code(status)?;
        self.set_status_message(status_message)?;
        self.finish_direct_response(version, content_length)
    }

    pub fn finish_direct_response(
        &mut self,
        version: (u8, u8),
        content_length: i64,
    ) -> Result<(), MessageTypeError> {
        self.set_http_version(version.0, version.1);
        self.headers.set(
            HttpHeaderCode::ContentLength,
            content_length.to_string().as_bytes(),
        );
        if !self.headers.exists(HttpHeaderCode::ContentType) {
            self.headers.add(HttpHeaderCode::ContentType, b"text/plain");
        }
        self.chunked = false;
        self.upgraded = false;
        Ok(())
    }

    pub fn has_query_param(&mut self, name: &[u8]) -> Result<bool, MessageTypeError> {
        Ok(self.query_param(name)?.is_some())
    }

    pub fn query_param(&mut self, name: &[u8]) -> Result<Option<Box<[u8]>>, MessageTypeError> {
        self.ensure_query_params()?;
        Ok(self
            .query_params
            .as_ref()
            .and_then(|params| params.get(name))
            .map(|value| value.clone().into_boxed_slice()))
    }

    pub fn integer_query_param(&mut self, name: &[u8]) -> Result<i32, QueryParameterError> {
        let value = self
            .query_param(name)
            .map_err(QueryParameterError::Message)?
            .ok_or(QueryParameterError::InvalidInteger)?;
        std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .ok_or(QueryParameterError::InvalidInteger)
    }

    pub fn integer_query_param_or(&mut self, name: &[u8], default: i32) -> i32 {
        self.integer_query_param(name).unwrap_or(default)
    }

    pub fn decoded_query_param(&mut self, name: &[u8]) -> Result<Box<[u8]>, MessageTypeError> {
        let Some(value) = self.query_param(name)? else {
            return Ok(Box::default());
        };
        Ok(percent_decode_query(&value).into_boxed_slice())
    }

    pub fn query_params(&mut self) -> Result<QueryParamMap, MessageTypeError> {
        self.ensure_query_params()?;
        Ok(self.query_params.clone().unwrap_or_default())
    }

    pub fn set_query_string(
        &mut self,
        query: impl AsRef<[u8]>,
        strict: bool,
    ) -> Result<bool, MessageTypeError> {
        self.set_query_string_internal(query.as_ref(), true, strict)
    }

    pub fn remove_query_param(&mut self, name: &[u8]) -> Result<bool, MessageTypeError> {
        self.ensure_query_params()?;
        let Some(params) = self.query_params.as_mut() else {
            return Ok(false);
        };
        if params.remove(name).is_none() {
            return Ok(false);
        }
        let query = create_query_string(params);
        self.set_query_string_internal(&query, false, false)
    }

    pub fn set_query_param(
        &mut self,
        name: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        strict: bool,
    ) -> Result<bool, MessageTypeError> {
        self.ensure_query_params()?;
        let params = self
            .query_params
            .as_mut()
            .expect("query parameters initialized");
        params.insert(name.as_ref().to_vec(), value.as_ref().to_vec());
        let query = create_query_string(params);
        self.set_query_string_internal(&query, false, strict)
    }

    pub fn cookie(&self, name: &[u8]) -> Box<[u8]> {
        parse_cookies(&self.headers)
            .get(name)
            .cloned()
            .unwrap_or_default()
            .into_boxed_slice()
    }

    pub fn remove_cookie(&mut self, name: &[u8]) {
        let mut cookies = parse_cookies(&self.headers);
        if cookies.remove(name).is_none() {
            return;
        }
        if cookies.is_empty() {
            self.headers.remove(HttpHeaderCode::Cookie);
            return;
        }
        let mut value = Vec::new();
        for (index, (name, cookie)) in cookies.iter().enumerate() {
            if index != 0 {
                value.extend_from_slice(b"; ");
            }
            value.extend_from_slice(name);
            value.push(b'=');
            value.extend_from_slice(cookie);
        }
        self.headers.set(HttpHeaderCode::Cookie, value);
    }

    pub fn strip_per_hop_headers(
        &mut self,
        strip_priority: bool,
        custom_headers: Option<&HttpHeaders>,
    ) {
        let mut stripped = self.stripped_per_hop_headers.take().unwrap_or_default();
        stripped.remove_all();
        if !self.trailers_allowed {
            self.trailers_allowed =
                self.check_for_header_token(HttpHeaderCode::Te, b"trailers", false);
        }
        self.headers
            .strip_per_hop_headers(&mut stripped, strip_priority, custom_headers);
        self.stripped_per_hop_headers = Some(stripped);
    }

    pub fn stripped_per_hop_headers(&self) -> Option<&HttpHeaders> {
        self.stripped_per_hop_headers.as_ref()
    }

    pub fn set_secure(&mut self, secure: bool) {
        if secure && self.scheme != Scheme::Masque {
            self.scheme = Scheme::Https;
        } else if !secure {
            self.scheme = Scheme::Http;
        }
    }

    pub fn is_secure(&self) -> bool {
        matches!(self.scheme, Scheme::Https | Scheme::Masque)
    }

    pub fn set_masque(&mut self) {
        self.scheme = Scheme::Masque;
    }

    pub fn is_masque(&self) -> bool {
        self.scheme == Scheme::Masque
    }

    pub fn scheme(&self) -> &'static [u8] {
        match self.scheme {
            Scheme::Http => b"http",
            Scheme::Https => b"https",
            Scheme::Masque => b"masque",
        }
    }

    pub const fn secure_version(&self) -> i32 {
        self.secure_version
    }

    pub fn secure_cipher(&self) -> Option<&[u8]> {
        self.secure_cipher.as_deref()
    }

    pub fn set_secure_info(&mut self, version: i32, cipher: impl AsRef<[u8]>) {
        self.secure_version = version;
        self.secure_cipher = Some(cipher.as_ref().to_vec().into_boxed_slice());
    }

    pub fn set_advanced_protocol_string(&mut self, protocol: impl AsRef<[u8]>) {
        let protocol = protocol.as_ref();
        self.advanced_protocol = Some(if protocol == b"h2" {
            Cow::Borrowed(b"h2".as_slice())
        } else {
            Cow::Owned(protocol.to_vec())
        });
    }

    pub fn advanced_protocol_string(&self) -> Option<&[u8]> {
        self.advanced_protocol.as_deref()
    }

    pub fn is_advanced_protocol(&self) -> bool {
        self.advanced_protocol.is_some()
    }

    pub fn protocol_string(&self) -> &[u8] {
        self.advanced_protocol
            .as_deref()
            .unwrap_or(self.version_string())
    }

    pub const fn normalize_priority(priority: i8) -> u8 {
        if priority > 7 || priority < -7 {
            7
        } else if priority < 0 {
            (priority + 8) as u8
        } else {
            priority as u8
        }
    }

    pub fn set_legacy_priority(&mut self, priority: i8) {
        self.legacy_priority = Self::normalize_priority(priority);
    }

    pub const fn legacy_priority(&self) -> u8 {
        self.legacy_priority
    }

    pub fn set_http_priority(&mut self, priority: HttpPriority) {
        HttpPriorityHeaderAccess::set_http_priority(self, priority);
    }

    pub fn set_http_priority_parts(&mut self, urgency: u8, incremental: bool) {
        HttpPriorityHeaderAccess::set_http_priority_parts(self, urgency, incremental);
    }

    pub fn http_priority(&self) -> Option<HttpPriority> {
        HttpPriorityHeaderAccess::get_http_priority(self)
    }

    pub fn set_sequence_number(&mut self, sequence_number: i32) {
        self.sequence_number = sequence_number;
    }

    pub const fn sequence_number(&self) -> i32 {
        self.sequence_number
    }

    pub fn set_ingress_header_size(&mut self, size: HttpHeaderSize) {
        self.header_size = size;
    }

    pub const fn ingress_header_size(&self) -> &HttpHeaderSize {
        &self.header_size
    }

    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    pub fn set_start_time(&mut self, start_time: Instant) {
        self.start_time = start_time;
    }

    pub fn check_for_header_token(
        &self,
        header: HttpHeaderCode,
        token: &[u8],
        case_sensitive: bool,
    ) -> bool {
        header_contains_token(&self.headers, header, token, case_sensitive)
    }

    pub fn default_reason(status: u16) -> &'static [u8] {
        match status {
            100 => b"Continue",
            101 => b"Switching Protocols",
            200 => b"OK",
            201 => b"Created",
            202 => b"Accepted",
            203 => b"Non-Authoritative Information",
            204 => b"No Content",
            205 => b"Reset Content",
            206 => b"Partial Content",
            300 => b"Multiple Choices",
            301 => b"Moved Permanently",
            302 => b"Found",
            303 => b"See Other",
            304 => b"Not Modified",
            305 => b"Use Proxy",
            307 => b"Temporary Redirect",
            400 => b"Bad Request",
            401 => b"Unauthorized",
            402 => b"Payment Required",
            403 => b"Forbidden",
            404 => b"Not Found",
            405 => b"Method Not Allowed",
            406 => b"Not Acceptable",
            407 => b"Proxy Authentication Required",
            408 => b"Request Timeout",
            409 => b"Conflict",
            410 => b"Gone",
            411 => b"Length Required",
            412 => b"Precondition Failed",
            413 => b"Request Entity Too Large",
            414 => b"Request-URI Too Long",
            415 => b"Unsupported Media Type",
            416 => b"Requested Range Not Satisfiable",
            417 => b"Expectation Failed",
            418 => b"I'm a teapot",
            426 => b"Upgrade Required",
            428 => b"Precondition Required",
            429 => b"Too Many Requests",
            431 => b"Request Header Fields Too Large",
            451 => b"Unavailable For Legal Reasons",
            500 => b"Internal Server Error",
            501 => b"Not Implemented",
            502 => b"Bad Gateway",
            503 => b"Service Unavailable",
            504 => b"Gateway Timeout",
            505 => b"HTTP Version Not Supported",
            _ => b"-",
        }
    }

    pub fn is_connect_udp_request(&self) -> bool {
        matches!(&self.fields, MessageFields::Request(request) if request.method.as_ref().and_then(Method::recognized) == Some(HttpMethod::Connect))
            && self.upgrade_protocol.as_deref() == Some(b"connect-udp")
    }

    pub fn is_request(&self) -> bool {
        matches!(&self.fields, MessageFields::Request(_))
    }

    pub fn is_response(&self) -> bool {
        matches!(&self.fields, MessageFields::Response(_))
    }

    pub fn compute_keepalive(&self) -> bool {
        if self.version.0 < 1 {
            return false;
        }
        if header_contains_token(&self.headers, HttpHeaderCode::Connection, b"close", false) {
            return false;
        }
        if self.version == Self::HTTP_VERSION_10 {
            return header_contains_token(
                &self.headers,
                HttpHeaderCode::Connection,
                b"keep-alive",
                false,
            ) || self
                .stripped_per_hop_headers
                .as_ref()
                .is_some_and(|headers| {
                    header_contains_token(headers, HttpHeaderCode::Connection, b"keep-alive", false)
                });
        }
        true
    }

    pub fn split_name_value(
        input: &[u8],
        pair_delimiter: u8,
        value_delimiter: u8,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        split_name_value(input, pair_delimiter, value_delimiter, trim_http_whitespace)
    }

    pub fn create_url(
        scheme: &[u8],
        authority: &[u8],
        path: &[u8],
        query: &[u8],
        fragment: &[u8],
    ) -> Box<[u8]> {
        create_url(scheme, authority, path, query, fragment).into_boxed_slice()
    }

    pub fn create_query_string(params: &QueryParamMap) -> Box<[u8]> {
        create_query_string(params).into_boxed_slice()
    }

    #[inline]
    fn request_mut(&mut self) -> Result<&mut RequestFields, MessageTypeError> {
        if matches!(self.fields, MessageFields::None) {
            self.init_request_fields();
        }
        match &mut self.fields {
            MessageFields::Request(request) => Ok(request),
            MessageFields::Response(_) => Err(MessageTypeError::RequestApiOnResponse),
            MessageFields::None => unreachable!(),
        }
    }

    #[inline(never)]
    fn init_request_fields(&mut self) {
        self.fields = MessageFields::Request(RequestFields::default());
    }

    fn response_mut(&mut self) -> Result<&mut ResponseFields, MessageTypeError> {
        if matches!(self.fields, MessageFields::None) {
            self.fields = MessageFields::Response(ResponseFields::default());
        }
        match &mut self.fields {
            MessageFields::Response(response) => Ok(response),
            MessageFields::Request(_) => Err(MessageTypeError::ResponseApiOnRequest),
            MessageFields::None => unreachable!(),
        }
    }

    fn set_url_internal(
        &mut self,
        url: &[u8],
        unparse: bool,
        strict: bool,
    ) -> Result<ParseUrl, MessageTypeError> {
        let parsed = ParseUrl::parse_url_maybe_invalid(url, strict);
        let request = self.request_mut()?;
        request.url = OwnedBytes::new(url);
        if parsed.valid() {
            request.path = Some(parsed.path_range());
            request.query = parsed.query_range();
        } else {
            request.path = None;
            request.query = 0..0;
        }
        if unparse {
            self.query_params = None;
        }
        Ok(parsed)
    }

    fn ensure_query_params(&mut self) -> Result<(), MessageTypeError> {
        if self.query_params.is_none() {
            let query = self.request_mut()?.query().to_vec().into_boxed_slice();
            let mut params = QueryParamMap::new();
            for (name, value) in split_name_value(&query, b'&', b'=', trim_http_whitespace) {
                params.insert(name, value);
            }
            self.query_params = Some(params);
        } else {
            self.request_mut()?;
        }
        Ok(())
    }

    fn set_query_string_internal(
        &mut self,
        query: &[u8],
        unparse: bool,
        strict: bool,
    ) -> Result<bool, MessageTypeError> {
        let current_url = self.request_mut()?.url.clone();
        let Some(parsed) = ParseUrl::parse_url(current_url.bytes(), false) else {
            return Ok(false);
        };
        let url = create_url(
            parsed.scheme(),
            parsed.authority(),
            parsed.path(),
            query,
            parsed.fragment(),
        );
        let result = self.set_url_internal(&url, unparse, strict)?;
        Ok(!strict || result.valid())
    }
}

impl HttpPriorityHeaderAccess for HttpMessage {
    fn priority_header_value(&self) -> &[u8] {
        self.headers.get_single_or_empty(HttpHeaderCode::Priority)
    }

    fn set_priority_header_value(&mut self, value: &[u8]) {
        self.headers.set(HttpHeaderCode::Priority, value);
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum QueryParameterError {
    #[error(transparent)]
    Message(#[from] MessageTypeError),
    #[error("query parameter is missing or is not a valid integer")]
    InvalidInteger,
}

impl fmt::Display for HttpMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            ", chunked: {}, upgraded: {}, scheme: {}, Fields for message:\n",
            u8::from(self.chunked),
            u8::from(self.upgraded),
            String::from_utf8_lossy(self.scheme()),
        )?;

        write_message_field(formatter, b"local_ip", &self.local_ip)?;
        write_message_field(formatter, b"version", self.version_string())?;
        write_message_field(formatter, b"dst_ip", &self.destination_ip)?;
        write_message_field(formatter, b"dst_port", &self.destination_port)?;
        match &self.fields {
            MessageFields::Request(request) => {
                write_message_field(
                    formatter,
                    b"client_ip",
                    request.client_ip.as_deref().unwrap_or(b""),
                )?;
                write_message_field(
                    formatter,
                    b"client_port",
                    request.client_port.as_deref().unwrap_or(b""),
                )?;
                write_message_field(
                    formatter,
                    b"method",
                    request
                        .method
                        .as_ref()
                        .map_or(b"".as_slice(), |method| method.as_str().as_bytes()),
                )?;
                write_message_field(formatter, b"path", request.path())?;
                write_message_field(formatter, b"query", request.query())?;
                write_message_field(formatter, b"url", request.url.bytes())?;
            }
            MessageFields::Response(response) => {
                if response.status_set {
                    writeln!(formatter, " status:{}", response.status)?;
                }
                write_message_field(formatter, b"status_msg", &response.status_message)?;
            }
            MessageFields::None => {}
        }
        write_headers(formatter, &self.headers)?;
        if let Some(headers) = &self.stripped_per_hop_headers {
            if !headers.is_empty() {
                formatter.write_str("Per-Hop Headers\n")?;
                write_headers(formatter, headers)?;
            }
        }
        Ok(())
    }
}

fn write_message_field(
    formatter: &mut fmt::Formatter<'_>,
    name: &[u8],
    value: &[u8],
) -> fmt::Result {
    if !value.is_empty() {
        writeln!(
            formatter,
            " {}:{}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(&strip_control_chars(value)),
        )?;
    }
    Ok(())
}

fn write_headers(formatter: &mut fmt::Formatter<'_>, headers: &HttpHeaders) -> fmt::Result {
    for (_, name, value) in headers.iter() {
        writeln!(
            formatter,
            " {}: {}",
            String::from_utf8_lossy(&strip_control_chars(name)),
            String::from_utf8_lossy(&strip_control_chars(value)),
        )?;
    }
    Ok(())
}

fn parse_cookies(headers: &HttpHeaders) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut cookies = BTreeMap::new();
    headers.for_each_value(HttpHeaderCode::Cookie, |value| {
        for (name, value) in split_name_value(value, b';', b'=', trim_spaces) {
            cookies.entry(name).or_insert(value);
        }
        false
    });
    cookies
}

fn split_name_value(
    input: &[u8],
    pair_delimiter: u8,
    value_delimiter: u8,
    trim: fn(&[u8]) -> &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result = Vec::new();
    for pair in input.split(|byte| *byte == pair_delimiter) {
        if pair.is_empty() {
            continue;
        }
        if let Some(position) = pair.iter().position(|byte| *byte == value_delimiter) {
            result.push((
                trim(&pair[..position]).to_vec(),
                trim(&pair[position + 1..]).to_vec(),
            ));
        } else {
            result.push((trim(pair).to_vec(), Vec::new()));
        }
    }
    result
}

fn trim_spaces(mut value: &[u8]) -> &[u8] {
    while value.first() == Some(&b' ') {
        value = &value[1..];
    }
    while value.last() == Some(&b' ') {
        value = &value[..value.len() - 1];
    }
    value
}

fn trim_http_whitespace(value: &[u8]) -> &[u8] {
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

fn create_query_string(params: &QueryParamMap) -> Vec<u8> {
    let mut query = Vec::new();
    for (index, (name, value)) in params.iter().enumerate() {
        if index != 0 {
            query.push(b'&');
        }
        query.extend_from_slice(name);
        query.push(b'=');
        query.extend_from_slice(value);
    }
    query
}

fn create_url(
    scheme: &[u8],
    authority: &[u8],
    path: &[u8],
    query: &[u8],
    fragment: &[u8],
) -> Vec<u8> {
    let mut url = Vec::with_capacity(
        scheme.len() + authority.len() + path.len() + query.len() + fragment.len() + 5,
    );
    if !scheme.is_empty() {
        url.extend_from_slice(scheme);
        url.extend_from_slice(b"://");
    }
    url.extend_from_slice(authority);
    url.extend_from_slice(path);
    if !query.is_empty() {
        url.push(b'?');
        url.extend_from_slice(query);
    }
    if !fragment.is_empty() {
        url.push(b'#');
        url.extend_from_slice(fragment);
    }
    url
}

fn header_contains_token(
    headers: &HttpHeaders,
    header: HttpHeaderCode,
    token: &[u8],
    case_sensitive: bool,
) -> bool {
    headers.for_each_value(header, |value| {
        value.split(|byte| *byte == b',').any(|candidate| {
            let candidate = trim_spaces(candidate);
            if case_sensitive {
                candidate == token
            } else {
                candidate.eq_ignore_ascii_case(token)
            }
        })
    })
}

fn percent_decode_query(value: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut cursor = 0;
    let mut uncommitted_start = 0;
    while cursor < value.len() {
        match value[cursor] {
            b'+' => {
                decoded.extend_from_slice(&value[uncommitted_start..cursor]);
                decoded.push(b' ');
                cursor += 1;
                uncommitted_start = cursor;
            }
            b'%' => {
                let Some(high) = value.get(cursor + 1).and_then(|byte| hex_value(*byte)) else {
                    return decoded;
                };
                let Some(low) = value.get(cursor + 2).and_then(|byte| hex_value(*byte)) else {
                    return decoded;
                };
                decoded.extend_from_slice(&value[uncommitted_start..cursor]);
                decoded.push(high << 4 | low);
                cursor += 3;
                uncommitted_start = cursor;
            }
            _ => {
                cursor += 1;
            }
        }
    }
    decoded.extend_from_slice(&value[uncommitted_start..]);
    decoded
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn strip_control_chars(value: &[u8]) -> Vec<u8> {
    value
        .iter()
        .copied()
        .filter(|byte| *byte > 0x1f && *byte != 0x7f)
        .collect()
}

struct CachedHttpDate {
    seconds: i64,
    formatted: String,
}

thread_local! {
    static HTTP_DATE_CACHE: RefCell<Option<CachedHttpDate>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static DATE_FORMATS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn format_http_date(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let cached = HTTP_DATE_CACHE.try_with(|cache| {
        cache
            .borrow()
            .as_ref()
            .filter(|cached| cached.seconds == seconds)
            .map(|cached| cached.formatted.clone())
    });
    if let Ok(Some(formatted)) = cached {
        return formatted;
    }
    let formatted = format_http_date_seconds(seconds);
    let _ = HTTP_DATE_CACHE.try_with(|cache| {
        *cache.borrow_mut() = Some(CachedHttpDate {
            seconds,
            formatted: formatted.clone(),
        });
    });
    formatted
}

fn format_http_date_seconds(seconds: i64) -> String {
    #[cfg(test)]
    DATE_FORMATS.with(|formats| formats.set(formats.get() + 1));
    let days = seconds / 86_400;
    let seconds_of_day = seconds % 86_400;
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let second = seconds_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    let weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let weekday = (days + 4).rem_euclid(7) as usize;
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        weekdays[weekday],
        day,
        months[month as usize - 1],
        year,
        hour,
        minute,
        second,
    )
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

#[cfg(test)]
mod date_cache_tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use super::DATE_FORMATS;
    use super::HTTP_DATE_CACHE;
    use super::format_http_date;

    #[test]
    fn equal_second_calls_format_the_date_once() {
        HTTP_DATE_CACHE.with(RefCell::take);
        DATE_FORMATS.with(|formats| formats.set(0));
        let formats = || DATE_FORMATS.with(Cell::get);
        let time = UNIX_EPOCH + Duration::from_secs(784_111_777);
        for time in [time, time + Duration::new(0, 999_999_999), time] {
            assert_eq!(format_http_date(time), "Sun, 06 Nov 1994 08:49:37 GMT");
        }
        assert_eq!(formats(), 1);
        format_http_date(time + Duration::from_secs(1));
        assert_eq!(formats(), 2);
        format_http_date(time);
        assert_eq!(formats(), 3);
        let wide = UNIX_EPOCH + Duration::from_secs(253_402_300_800);
        format_http_date(wide);
        format_http_date(wide);
        assert_eq!(formats(), 4);
    }

    #[test]
    fn date_cache_tracks_wall_clock_seconds_in_both_directions() {
        HTTP_DATE_CACHE.with(RefCell::take);
        let cases = [
            (0, 0, "Thu, 01 Jan 1970 00:00:00 GMT"),
            (0, 999_999_999, "Thu, 01 Jan 1970 00:00:00 GMT"),
            (1, 0, "Thu, 01 Jan 1970 00:00:01 GMT"),
            (86_399, 999_999_999, "Thu, 01 Jan 1970 23:59:59 GMT"),
            (86_400, 0, "Fri, 02 Jan 1970 00:00:00 GMT"),
            (784_111_777, 0, "Sun, 06 Nov 1994 08:49:37 GMT"),
            (951_782_400, 0, "Tue, 29 Feb 2000 00:00:00 GMT"),
            (951_868_799, 999_999_999, "Tue, 29 Feb 2000 23:59:59 GMT"),
            (951_868_800, 0, "Wed, 01 Mar 2000 00:00:00 GMT"),
            (86_400, 0, "Fri, 02 Jan 1970 00:00:00 GMT"),
            (0, 0, "Thu, 01 Jan 1970 00:00:00 GMT"),
        ];
        for (seconds, nanos, expected) in cases {
            assert_eq!(
                format_http_date(UNIX_EPOCH + Duration::new(seconds, nanos)),
                expected,
            );
        }
    }

    #[test]
    fn date_cache_preserves_pre_epoch_and_extended_year_behavior() {
        HTTP_DATE_CACHE.with(RefCell::take);
        let epoch = "Thu, 01 Jan 1970 00:00:00 GMT";
        assert_eq!(
            format_http_date(UNIX_EPOCH - Duration::from_nanos(1)),
            epoch
        );
        assert_eq!(format_http_date(UNIX_EPOCH), epoch);
        for (seconds, expected) in [
            (253_402_300_799, "Fri, 31 Dec 9999 23:59:59 GMT"),
            (253_402_300_800, "Sat, 01 Jan 10000 00:00:00 GMT"),
            (253_402_300_801, "Sat, 01 Jan 10000 00:00:01 GMT"),
            (253_402_300_799, "Fri, 31 Dec 9999 23:59:59 GMT"),
        ] {
            let time = UNIX_EPOCH + Duration::from_secs(seconds);
            assert_eq!(format_http_date(time), expected);
            assert_eq!(format_http_date(time + Duration::from_nanos(1)), expected);
        }
        assert_eq!(format_http_date(UNIX_EPOCH - Duration::from_secs(1)), epoch);
    }

    #[test]
    fn date_cache_returns_independent_owned_strings() {
        HTTP_DATE_CACHE.with(RefCell::take);
        let time = UNIX_EPOCH + Duration::from_secs(784_111_777);
        let expected = "Sun, 06 Nov 1994 08:49:37 GMT";
        let mut first = format_http_date(time);
        let retained = format_http_date(time);
        first.replace_range(..3, "BAD");
        assert_eq!(retained, expected);
        assert_eq!(format_http_date(time), expected);
        assert_eq!(
            format_http_date(time + Duration::from_secs(1)),
            "Sun, 06 Nov 1994 08:49:38 GMT",
        );
        assert_eq!(retained, expected);
        assert_eq!(format_http_date(time), expected);
    }

    #[test]
    fn date_cache_is_thread_local_without_a_runtime() {
        HTTP_DATE_CACHE.with(RefCell::take);
        let expected = "Thu, 01 Jan 1970 00:00:01 GMT";
        assert_eq!(
            format_http_date(UNIX_EPOCH + Duration::from_secs(1)),
            expected
        );
        std::thread::spawn(|| {
            assert!(HTTP_DATE_CACHE.with(|cache| cache.borrow().is_none()));
            assert_eq!(
                format_http_date(UNIX_EPOCH + Duration::from_secs(2)),
                "Thu, 01 Jan 1970 00:00:02 GMT",
            );
        })
        .join()
        .expect("date formatting thread completes");
        assert_eq!(
            HTTP_DATE_CACHE.with(|cache| cache.borrow().as_ref().map(|cached| cached.seconds)),
            Some(1),
        );
        assert_eq!(
            format_http_date(UNIX_EPOCH + Duration::from_secs(1)),
            expected
        );
    }
}
