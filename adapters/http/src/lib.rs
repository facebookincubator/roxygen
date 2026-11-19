/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Owned metadata conversion between `http` 1.x and Roxygen.
//!
//! `http::HeaderMap` preserves the order of values for one name, but does not
//! represent a global insertion order across different names. Conversion into
//! Roxygen therefore uses `HeaderMap::iter()` order, and conversion back
//! preserves duplicate-value order. Header names use `HeaderMap`'s normalized
//! spelling. Raw header values, including obs-text, are otherwise preserved.

use std::collections::HashSet;

use http::Extensions;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Method as HttpMethod;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use http::Version;
use http::header::CONTENT_LENGTH;
use http::header::TRANSFER_ENCODING;
use http::uri::Authority;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::Method as RoxygenMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::MessageTypeError;
use roxygen_core::priority::DEFAULT_HTTP_PRIORITY_URGENCY;
use thiserror::Error;

/// Roxygen message metadata that an `http` request or response head cannot own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedMessageMetadata {
    Chunked,
    Upgraded,
    IngressWebSocketUpgrade,
    EgressWebSocketUpgrade,
    KeepaliveDisabled,
    TrailersAllowed,
    StrippedPerHopHeaders,
    MasqueScheme,
    UriSchemeMismatch,
    ResponseScheme,
    UpgradeProtocol,
    ClientAddress,
    DestinationAddress,
    LocalIp,
    SecureInfo,
    AdvancedProtocol,
    LegacyPriority,
    SequenceNumber,
    IngressHeaderSize,
}

/// A conversion failure for metadata not shared exactly by both models.
#[derive(Debug, Error)]
pub enum HttpAdapterError {
    #[error("expected a Roxygen request message")]
    ExpectedRequest,
    #[error("expected a Roxygen response message")]
    ExpectedResponse,
    #[error("Roxygen request has no method")]
    MissingMethod,
    #[error("basic H2 CONNECT authority requires a nonempty host without userinfo")]
    InvalidConnectAuthority,
    #[error("http method `{0}` is not in Roxygen's canonical uppercase form")]
    NonCanonicalMethod(String),
    #[error("http URI `{0}` is not accepted by Roxygen's strict URL parser")]
    UriNotRepresentable(Uri),
    #[error("http version {0:?} is outside the H1/H2 adapter scope")]
    UnsupportedHttpVersion(Version),
    #[error("Roxygen HTTP version {major}.{minor} is outside the H1/H2 adapter scope")]
    UnsupportedRoxygenVersion { major: u8, minor: u8 },
    #[error("transfer encoding is unsupported for this network head")]
    UnsupportedTransferEncoding,
    #[error("content-length and transfer-encoding cannot frame the same network message")]
    ConflictingBodyFraming,
    #[error("http extensions have no Roxygen message representation")]
    ExtensionsUnsupported,
    #[error("sensitivity metadata for header `{0}` has no Roxygen representation")]
    SensitiveHeader(HeaderName),
    #[error("header `{0}` has boundary whitespace that Roxygen would trim")]
    HeaderBoundaryWhitespace(HeaderName),
    #[error("Roxygen header name {original:?} would be normalized to `{normalized}`")]
    HeaderNameNormalization {
        original: Box<[u8]>,
        normalized: HeaderName,
    },
    #[error("repeated header `{0}` is interleaved with another name")]
    InterleavedHeaderName(HeaderName),
    #[error("URI scheme `{0}` is outside the HTTP/HTTPS adapter scope")]
    UnsupportedUriScheme(Box<str>),
    #[error("Roxygen metadata {0:?} has no http request/response-head representation")]
    UnsupportedMessageMetadata(UnsupportedMessageMetadata),
    #[error("trailers are body metadata and cannot be stored in an http request or response head")]
    TrailersUnsupported,
    #[error("custom response reason phrases have no http::Response representation")]
    StatusMessageUnsupported,
    #[error(transparent)]
    MessageType(#[from] MessageTypeError),
    #[error(transparent)]
    InvalidMethod(#[from] http::method::InvalidMethod),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    InvalidHeaderName(#[from] http::header::InvalidHeaderName),
    #[error(transparent)]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
    #[error(transparent)]
    InvalidStatusCode(#[from] http::status::InvalidStatusCode),
    #[error("http header map capacity exceeded")]
    MaxSizeReached(#[from] http::header::MaxSizeReached),
}

#[derive(Debug)]
struct HttpHeaderOrigin {
    roxygen_headers: HttpHeaders,
    http_headers: HeaderMap,
}

/// An owned Roxygen request head paired with its unchanged body value.
#[derive(Debug)]
pub struct RoxygenRequest<B> {
    message: HttpMessage,
    body: B,
    header_origin: Option<HttpHeaderOrigin>,
}

impl<B> RoxygenRequest<B> {
    pub fn try_from_parts(message: HttpMessage, body: B) -> Result<Self, HttpAdapterError> {
        if !message.is_request() {
            return Err(HttpAdapterError::ExpectedRequest);
        }
        Ok(Self {
            message,
            body,
            header_origin: None,
        })
    }

    pub const fn message(&self) -> &HttpMessage {
        &self.message
    }

    pub fn message_mut(&mut self) -> &mut HttpMessage {
        &mut self.message
    }

    pub const fn body(&self) -> &B {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut B {
        &mut self.body
    }

    pub fn into_parts(self) -> (HttpMessage, B) {
        (self.message, self.body)
    }
}

/// An owned Roxygen response head paired with its unchanged body value.
#[derive(Debug)]
pub struct RoxygenResponse<B> {
    message: HttpMessage,
    body: B,
    header_origin: Option<HttpHeaderOrigin>,
}

impl<B> RoxygenResponse<B> {
    pub fn try_from_parts(message: HttpMessage, body: B) -> Result<Self, HttpAdapterError> {
        if !message.is_response() {
            return Err(HttpAdapterError::ExpectedResponse);
        }
        Ok(Self {
            message,
            body,
            header_origin: None,
        })
    }

    pub const fn message(&self) -> &HttpMessage {
        &self.message
    }

    pub fn message_mut(&mut self) -> &mut HttpMessage {
        &mut self.message
    }

    pub const fn body(&self) -> &B {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut B {
        &mut self.body
    }

    pub fn into_parts(self) -> (HttpMessage, B) {
        (self.message, self.body)
    }
}

impl<B> TryFrom<Request<B>> for RoxygenRequest<B> {
    type Error = HttpAdapterError;

    fn try_from(request: Request<B>) -> Result<Self, Self::Error> {
        let (parts, body) = request.into_parts();
        if !parts.extensions.is_empty() {
            return Err(HttpAdapterError::ExtensionsUnsupported);
        }

        let version = version_from_http(parts.version)?;
        let method_name = parts.method.as_str();
        let method = RoxygenMethod::from_name(method_name);
        if method.as_str() != method_name {
            return Err(HttpAdapterError::NonCanonicalMethod(method_name.to_owned()));
        }
        let headers = headers_from_http(&parts.headers)?;
        let uri_bytes = parts.uri.to_string().into_bytes();

        let mut message = HttpMessage::new();
        message.set_method(method)?;
        let parsed = message.set_url(&uri_bytes, true)?;
        if !parsed.valid() {
            return Err(HttpAdapterError::UriNotRepresentable(parts.uri));
        }
        set_message_scheme_from_uri(&mut message, &parts.uri)?;
        message.set_http_version(version.0, version.1);
        let header_origin = HttpHeaderOrigin {
            roxygen_headers: headers.clone(),
            http_headers: parts.headers,
        };
        *message.headers_mut() = headers;
        Ok(Self {
            message,
            body,
            header_origin: Some(header_origin),
        })
    }
}

impl<B> TryFrom<RoxygenRequest<B>> for Request<B> {
    type Error = HttpAdapterError;

    fn try_from(request: RoxygenRequest<B>) -> Result<Self, Self::Error> {
        let RoxygenRequest {
            mut message,
            body,
            header_origin,
        } = request;
        if !message.is_request() {
            return Err(HttpAdapterError::ExpectedRequest);
        }
        reject_common_metadata(&message)?;
        reject_request_metadata(&mut message)?;

        let version = version_to_http(message.http_version())?;
        let method_name = message.method_string()?.to_owned();
        if method_name.is_empty() {
            return Err(HttpAdapterError::MissingMethod);
        }
        let method = HttpMethod::from_bytes(method_name.as_bytes())?;
        let uri = Uri::try_from(message.url()?)?;
        validate_request_scheme(&message, &uri)?;
        let headers = headers_to_http(message.headers(), header_origin)?;

        let mut converted = Request::new(body);
        *converted.method_mut() = method;
        *converted.uri_mut() = uri;
        *converted.version_mut() = version;
        *converted.headers_mut() = headers;
        Ok(converted)
    }
}

impl<B> TryFrom<Response<B>> for RoxygenResponse<B> {
    type Error = HttpAdapterError;

    fn try_from(response: Response<B>) -> Result<Self, Self::Error> {
        let (parts, body) = response.into_parts();
        if !parts.extensions.is_empty() {
            return Err(HttpAdapterError::ExtensionsUnsupported);
        }

        let version = version_from_http(parts.version)?;
        let headers = headers_from_http(&parts.headers)?;
        let mut message = HttpMessage::new();
        message.set_status_code(parts.status.as_u16())?;
        message.set_http_version(version.0, version.1);
        let header_origin = HttpHeaderOrigin {
            roxygen_headers: headers.clone(),
            http_headers: parts.headers,
        };
        *message.headers_mut() = headers;
        Ok(Self {
            message,
            body,
            header_origin: Some(header_origin),
        })
    }
}

impl<B> TryFrom<RoxygenResponse<B>> for Response<B> {
    type Error = HttpAdapterError;

    fn try_from(response: RoxygenResponse<B>) -> Result<Self, Self::Error> {
        let RoxygenResponse {
            mut message,
            body,
            header_origin,
        } = response;
        if !message.is_response() {
            return Err(HttpAdapterError::ExpectedResponse);
        }
        reject_common_metadata(&message)?;
        if message.scheme() != b"http" {
            return Err(HttpAdapterError::UnsupportedMessageMetadata(
                UnsupportedMessageMetadata::ResponseScheme,
            ));
        }
        if !message.status_message()?.is_empty() {
            return Err(HttpAdapterError::StatusMessageUnsupported);
        }

        let version = version_to_http(message.http_version())?;
        let status = StatusCode::from_u16(message.status_code()?)?;
        let headers = headers_to_http(message.headers(), header_origin)?;

        let mut converted = Response::new(body);
        *converted.status_mut() = status;
        *converted.version_mut() = version;
        *converted.headers_mut() = headers;
        Ok(converted)
    }
}

fn version_from_http(version: Version) -> Result<(u8, u8), HttpAdapterError> {
    if version == Version::HTTP_09 {
        Ok((0, 9))
    } else if version == Version::HTTP_10 {
        Ok((1, 0))
    } else if version == Version::HTTP_11 {
        Ok((1, 1))
    } else if version == Version::HTTP_2 {
        Ok((2, 0))
    } else {
        Err(HttpAdapterError::UnsupportedHttpVersion(version))
    }
}

fn version_to_http(version: (u8, u8)) -> Result<Version, HttpAdapterError> {
    match version {
        (0, 9) => Ok(Version::HTTP_09),
        (1, 0) => Ok(Version::HTTP_10),
        (1, 1) => Ok(Version::HTTP_11),
        (2, 0) => Ok(Version::HTTP_2),
        (major, minor) => Err(HttpAdapterError::UnsupportedRoxygenVersion { major, minor }),
    }
}

fn headers_from_http(headers: &HeaderMap) -> Result<HttpHeaders, HttpAdapterError> {
    let mut converted = HttpHeaders::new();
    for (name, value) in headers {
        if value.is_sensitive() {
            return Err(HttpAdapterError::SensitiveHeader(name.clone()));
        }
        let value_bytes = value.as_bytes();
        if value_bytes.first().is_some_and(|byte| is_lws(*byte))
            || value_bytes.last().is_some_and(|byte| is_lws(*byte))
        {
            return Err(HttpAdapterError::HeaderBoundaryWhitespace(name.clone()));
        }
        converted.add(name.as_str().as_bytes(), value_bytes);
    }
    Ok(converted)
}

fn headers_to_http(
    headers: &HttpHeaders,
    origin: Option<HttpHeaderOrigin>,
) -> Result<HeaderMap, HttpAdapterError> {
    if let Some(origin) = origin
        && origin.roxygen_headers == *headers
    {
        return Ok(origin.http_headers);
    }

    let mut converted = HeaderMap::new();
    let mut seen_names = HashSet::new();
    let mut current_name = None;
    for (_, name, value) in headers.iter() {
        let parsed_name = HeaderName::from_bytes(name)?;
        if parsed_name.as_str().as_bytes() != name {
            return Err(HttpAdapterError::HeaderNameNormalization {
                original: name.to_vec().into_boxed_slice(),
                normalized: parsed_name,
            });
        }
        if current_name.as_ref() != Some(&parsed_name) {
            if !seen_names.insert(parsed_name.clone()) {
                return Err(HttpAdapterError::InterleavedHeaderName(parsed_name));
            }
            current_name = Some(parsed_name.clone());
        }
        let value = HeaderValue::from_bytes(value)?;
        converted.try_append(parsed_name, value)?;
    }
    Ok(converted)
}

fn reject_common_metadata(message: &HttpMessage) -> Result<(), HttpAdapterError> {
    let reject = |metadata| Err(HttpAdapterError::UnsupportedMessageMetadata(metadata));
    if message.is_chunked() {
        return reject(UnsupportedMessageMetadata::Chunked);
    }
    if message.is_upgraded() {
        return reject(UnsupportedMessageMetadata::Upgraded);
    }
    if message.is_ingress_websocket_upgrade() {
        return reject(UnsupportedMessageMetadata::IngressWebSocketUpgrade);
    }
    if message.is_egress_websocket_upgrade() {
        return reject(UnsupportedMessageMetadata::EgressWebSocketUpgrade);
    }
    if !message.wants_keepalive() {
        return reject(UnsupportedMessageMetadata::KeepaliveDisabled);
    }
    if message.trailers_allowed() {
        return reject(UnsupportedMessageMetadata::TrailersAllowed);
    }
    if message.stripped_per_hop_headers().is_some() {
        return reject(UnsupportedMessageMetadata::StrippedPerHopHeaders);
    }
    if message.trailers().is_some() {
        return Err(HttpAdapterError::TrailersUnsupported);
    }
    if message.upgrade_protocol().is_some() {
        return reject(UnsupportedMessageMetadata::UpgradeProtocol);
    }
    if message.destination_address().is_initialized()
        || !message.destination_ip().is_empty()
        || !message.destination_port().is_empty()
    {
        return reject(UnsupportedMessageMetadata::DestinationAddress);
    }
    if !message.local_ip().is_empty() {
        return reject(UnsupportedMessageMetadata::LocalIp);
    }
    if message.secure_version() != 0 || message.secure_cipher().is_some() {
        return reject(UnsupportedMessageMetadata::SecureInfo);
    }
    if message.advanced_protocol_string().is_some() {
        return reject(UnsupportedMessageMetadata::AdvancedProtocol);
    }
    if message.legacy_priority() != DEFAULT_HTTP_PRIORITY_URGENCY {
        return reject(UnsupportedMessageMetadata::LegacyPriority);
    }
    if message.sequence_number() != -1 {
        return reject(UnsupportedMessageMetadata::SequenceNumber);
    }
    if message.ingress_header_size() != &HttpHeaderSize::default() {
        return reject(UnsupportedMessageMetadata::IngressHeaderSize);
    }
    if message.is_masque() {
        return reject(UnsupportedMessageMetadata::MasqueScheme);
    }
    Ok(())
}

fn reject_request_metadata(message: &mut HttpMessage) -> Result<(), HttpAdapterError> {
    let has_client_address = message.client_address()?.is_initialized()
        || !message.client_ip()?.is_empty()
        || !message.client_port()?.is_empty();
    if has_client_address {
        Err(HttpAdapterError::UnsupportedMessageMetadata(
            UnsupportedMessageMetadata::ClientAddress,
        ))
    } else {
        Ok(())
    }
}

fn set_message_scheme_from_uri(
    message: &mut HttpMessage,
    uri: &Uri,
) -> Result<(), HttpAdapterError> {
    match uri.scheme_str() {
        None | Some("http") => Ok(()),
        Some("https") => {
            message.set_secure(true);
            Ok(())
        }
        Some(scheme) => Err(HttpAdapterError::UnsupportedUriScheme(scheme.into())),
    }
}

fn validate_request_scheme(message: &HttpMessage, uri: &Uri) -> Result<(), HttpAdapterError> {
    if message.is_masque() {
        return Err(HttpAdapterError::UnsupportedMessageMetadata(
            UnsupportedMessageMetadata::MasqueScheme,
        ));
    }
    let agrees = match (message.scheme(), uri.scheme_str()) {
        (b"http", None | Some("http")) | (b"https", Some("https")) => true,
        (_, Some(scheme)) if !matches!(scheme, "http" | "https") => {
            return Err(HttpAdapterError::UnsupportedUriScheme(scheme.into()));
        }
        _ => false,
    };
    if agrees {
        Ok(())
    } else {
        Err(HttpAdapterError::UnsupportedMessageMetadata(
            UnsupportedMessageMetadata::UriSchemeMismatch,
        ))
    }
}

fn is_lws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\n' | b'\t' | b'\r')
}

/// Application-local metadata kept separately from a projected network head.
///
/// Keep this sidecar on the application side when sending the `HttpMessage` to
/// a worker. Extensions are moved, never serialized or reconstructed from
/// headers. Connector/acceptor identity and security context must be supplied
/// explicitly by the caller. Unchanged headers retain sensitivity flags.
#[derive(Debug, Default)]
pub struct NetworkHeadContext {
    extensions: Extensions,
    header_origin: Option<HttpHeaderOrigin>,
}

impl NetworkHeadContext {
    /// Supplies application context for a head received from the network.
    pub fn from_extensions(extensions: Extensions) -> Self {
        Self {
            extensions,
            header_origin: None,
        }
    }

    /// Borrows the unchanged application extension map.
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Lets the application add context without changing the wire message.
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }

    /// Recovers application context without performing a reverse projection.
    pub fn into_extensions(self) -> Extensions {
        self.extensions
    }
}

/// Separates a request's network head from its application-only context.
///
/// Unlike strict `TryFrom`, extensions and header sensitivity are retained in a
/// sidecar rather than rejected. Boundary whitespace is still rejected rather
/// than silently trimmed. Split the body off on the application runtime before
/// calling this function; no body ownership crosses this head-only boundary.
/// Only a single HTTP/1.1 chunked transfer encoding is supported; a simultaneous
/// content length is rejected. The native chunked flag follows that framing.
pub fn project_request_head(
    mut parts: http::request::Parts,
) -> Result<(HttpMessage, NetworkHeadContext), HttpAdapterError> {
    let chunked = network_is_chunked(parts.version, &parts.headers)?;
    let extensions = std::mem::take(&mut parts.extensions);
    let http_headers = parts.headers.clone();
    parts
        .headers
        .values_mut()
        .for_each(|value| value.set_sensitive(false));
    let (mut message, ()) = RoxygenRequest::try_from(Request::from_parts(parts, ()))?.into_parts();
    message.set_chunked(chunked);
    let context = NetworkHeadContext {
        extensions,
        header_origin: Some(HttpHeaderOrigin {
            roxygen_headers: message.headers().clone(),
            http_headers,
        }),
    };
    Ok((message, context))
}

/// Separates a response's network head from its unchanged application context.
/// This is a network projection, not the strict lossless `TryFrom` conversion.
/// Transfer encoding follows the request projection policy, is rejected for
/// 1xx/204 responses, and remains metadata without chunk framing for 304.
pub fn project_response_head(
    mut parts: http::response::Parts,
) -> Result<(HttpMessage, NetworkHeadContext), HttpAdapterError> {
    let chunked = network_is_chunked(parts.version, &parts.headers)?;
    if chunked && (parts.status.is_informational() || parts.status == StatusCode::NO_CONTENT) {
        return Err(HttpAdapterError::UnsupportedTransferEncoding);
    }
    let chunked = chunked && parts.status != StatusCode::NOT_MODIFIED;
    let extensions = std::mem::take(&mut parts.extensions);
    let http_headers = parts.headers.clone();
    parts
        .headers
        .values_mut()
        .for_each(|value| value.set_sensitive(false));
    let (mut message, ()) =
        RoxygenResponse::try_from(Response::from_parts(parts, ()))?.into_parts();
    message.set_chunked(chunked);
    let context = NetworkHeadContext {
        extensions,
        header_origin: Some(HttpHeaderOrigin {
            roxygen_headers: message.headers().clone(),
            http_headers,
        }),
    };
    Ok((message, context))
}

fn network_is_chunked(version: Version, headers: &HeaderMap) -> Result<bool, HttpAdapterError> {
    let mut encodings = headers.get_all(TRANSFER_ENCODING).iter();
    let Some(encoding) = encodings.next() else {
        return Ok(false);
    };
    if version != Version::HTTP_11
        || !encoding.as_bytes().eq_ignore_ascii_case(b"chunked")
        || encodings.next().is_some()
    {
        return Err(HttpAdapterError::UnsupportedTransferEncoding);
    }
    if headers.contains_key(CONTENT_LENGTH) {
        return Err(HttpAdapterError::ConflictingBodyFraming);
    }
    Ok(true)
}

/// Projects a worker message into an application request head and reattaches
/// its sidecar. The original message retains native connection/framing metadata.
///
/// Header spelling and cross-name ordering are normalized to `HeaderMap`;
/// duplicate-value order is retained. Native framing, address, security, and
/// accounting fields are not synthesized as headers or trusted extensions.
/// Origin-form targets are valid on secure connections. Upgrades and embedded
/// trailers are rejected, not silently dropped.
/// Exact native `h2` metadata projects validated numeric versions 1.1 and 2.0
/// as HTTP/2. Contradictory or unknown advanced-protocol metadata is rejected.
/// A basic H2 CONNECT with an empty native URL projects its single Host authority
/// into an authority-only URI. This does not provide a CONNECT tunnel transport.
/// This projects message metadata; it does not authenticate a connection or
/// infer its protocol or security from HTTP headers or application extensions.
pub fn request_head_to_http(
    message: &HttpMessage,
    context: NetworkHeadContext,
) -> Result<http::request::Parts, HttpAdapterError> {
    if !message.is_request() {
        return Err(HttpAdapterError::ExpectedRequest);
    }
    validate_network_projection(message)?;
    let mut message = message.clone();
    let method = HttpMethod::from_bytes(message.method_string()?.as_bytes())?;
    let uri = if method == HttpMethod::CONNECT
        && message.url()?.is_empty()
        && network_version_to_http(&message)? == Version::HTTP_2
    {
        let authority = Authority::try_from(message.headers().get_single_or_empty(b"host"))?;
        if authority.host().is_empty() || authority.as_str().contains('@') {
            return Err(HttpAdapterError::InvalidConnectAuthority);
        }
        Uri::from(authority)
    } else {
        Uri::try_from(message.url()?)?
    };
    if uri.scheme().is_some() {
        validate_request_scheme(&message, &uri)?;
    }
    let mut request = Request::new(());
    *request.method_mut() = method;
    *request.uri_mut() = uri;
    *request.version_mut() = network_version_to_http(&message)?;
    *request.headers_mut() = network_headers_to_http(message.headers(), context.header_origin)?;
    *request.extensions_mut() = context.extensions;
    Ok(request.into_parts().0)
}

/// Projects a response head, retaining duplicate values and local extensions.
/// Native framing/security metadata and reason phrases remain in the original
/// message, not in the application head. Strict lossless conversion is unchanged.
/// Exact native `h2` metadata projects validated numeric versions 1.1 and 2.0
/// as HTTP/2; contradictory or unknown advanced-protocol metadata is rejected.
/// HTTP headers and application extensions do not select protocol or security.
pub fn response_head_to_http(
    message: &HttpMessage,
    context: NetworkHeadContext,
) -> Result<http::response::Parts, HttpAdapterError> {
    if !message.is_response() {
        return Err(HttpAdapterError::ExpectedResponse);
    }
    validate_network_projection(message)?;
    let mut message = message.clone();
    let mut response = Response::new(());
    *response.status_mut() = StatusCode::from_u16(message.status_code()?)?;
    *response.version_mut() = network_version_to_http(&message)?;
    *response.headers_mut() = network_headers_to_http(message.headers(), context.header_origin)?;
    *response.extensions_mut() = context.extensions;
    Ok(response.into_parts().0)
}

fn network_version_to_http(message: &HttpMessage) -> Result<Version, HttpAdapterError> {
    let version = version_to_http(message.http_version())?;
    match message.advanced_protocol_string() {
        None => Ok(version),
        Some(b"h2") if matches!(message.http_version(), (1, 1) | (2, 0)) => Ok(Version::HTTP_2),
        Some(_) => Err(HttpAdapterError::UnsupportedMessageMetadata(
            UnsupportedMessageMetadata::AdvancedProtocol,
        )),
    }
}

fn network_headers_to_http(
    headers: &HttpHeaders,
    origin: Option<HttpHeaderOrigin>,
) -> Result<HeaderMap, HttpAdapterError> {
    let origin = match origin {
        Some(origin) if origin.roxygen_headers == *headers => return Ok(origin.http_headers),
        origin => origin,
    };
    let mut converted = HeaderMap::new();
    for (_, name, value) in headers.iter() {
        converted.try_append(
            HeaderName::from_bytes(name)?,
            HeaderValue::from_bytes(value)?,
        )?;
    }
    if let Some(origin) = origin {
        for name in origin.http_headers.keys() {
            let original = origin.http_headers.get_all(name);
            let unchanged = converted
                .get_all(name)
                .iter()
                .map(HeaderValue::as_bytes)
                .eq(original.iter().map(HeaderValue::as_bytes));
            if unchanged {
                converted.remove(name);
                for value in original.iter() {
                    converted.try_append(name.clone(), value.clone())?;
                }
            }
        }
    }
    Ok(converted)
}

fn validate_network_projection(message: &HttpMessage) -> Result<(), HttpAdapterError> {
    let unsupported = if message.is_upgraded() {
        Some(UnsupportedMessageMetadata::Upgraded)
    } else if message.is_ingress_websocket_upgrade() {
        Some(UnsupportedMessageMetadata::IngressWebSocketUpgrade)
    } else if message.is_egress_websocket_upgrade() {
        Some(UnsupportedMessageMetadata::EgressWebSocketUpgrade)
    } else if message.upgrade_protocol().is_some() {
        Some(UnsupportedMessageMetadata::UpgradeProtocol)
    } else if message.is_masque() {
        Some(UnsupportedMessageMetadata::MasqueScheme)
    } else {
        None
    };
    if let Some(unsupported) = unsupported {
        return Err(HttpAdapterError::UnsupportedMessageMetadata(unsupported));
    }
    if message.trailers().is_some() {
        return Err(HttpAdapterError::TrailersUnsupported);
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/round_trip.rs"]
mod round_trip_tests;

#[cfg(test)]
#[path = "../tests/network.rs"]
mod network_tests;
