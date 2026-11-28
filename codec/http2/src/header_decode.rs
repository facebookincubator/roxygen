/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HPACK-to-HTTP message validation.

use roxygen_codec::ProxygenError;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::Method;
use roxygen_core::message::HttpMessage;

use crate::hpack::DecodedHeaderBlock;
use crate::hpack::HeaderName;
use crate::hpack::HpackError;
use crate::hpack::StreamingCallback;
use crate::util::ControlEscapeMode;
use crate::util::HeaderNameValidationMode;
use crate::util::validate_header_name;
use crate::util::validate_header_value;
use crate::util::validate_method;
use crate::util::validate_scheme;
use crate::util::validate_url;

/// Options for decoding a field section, all off by default.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FieldDecodeOptions {
    /// Accepts any RFC 9110 token as a request's `:method`, case-sensitive and kept byte for
    /// byte (`Method::from_exact_token`), as `http::Method::from_bytes` parses it. When off, a
    /// method is a run of letters and `-`, and recognized methods match regardless of case.
    pub exact_methods: bool,
    /// Stores regular field values exactly as decoded, as h2 does, instead of trimming
    /// surrounding whitespace. Values are then validated as h2 validates them. They must not
    /// contain DEL or a byte below 0x20 other than HTAB (so obsolete line folding is
    /// rejected), and in strict mode no byte above 0x7E. A `content-length` has at most 19
    /// digits (an empty one is kept, and a session reads it as zero), and a `te` must be
    /// exactly `trailers`. `cookie` crumbs are still combined into one value, without
    /// trimming it.
    pub verbatim_values: bool,
}

/// Whether h2 0.4 accepts `value` as a field value (`HeaderValue::from_bytes`). It must not
/// contain DEL or a byte below 0x20 other than HTAB. With `strict`, bytes above 0x7E are
/// rejected too.
fn is_h2_field_value(value: &[u8], strict: bool) -> bool {
    value
        .iter()
        .all(|byte| (*byte >= 0x20 || *byte == b'\t') && *byte != 0x7f && (!strict || *byte < 0x80))
}

/// Whether h2 0.4 accepts `value` as a `content-length` (`frame::headers::parse_u64`). It must
/// have at most 19 decimal digits. An empty value is accepted and read as zero.
fn is_h2_content_length(value: &[u8]) -> bool {
    value.len() <= 19 && value.iter().all(u8::is_ascii_digit)
}

/// Request pseudo-header state and validation.
#[derive(Debug, Default)]
pub struct HttpRequestVerifier {
    has_method: bool,
    has_path: bool,
    has_scheme: bool,
    has_authority: bool,
    has_upgrade_protocol: bool,
    validation_error: bool,
    error: Option<String>,
}

impl HttpRequestVerifier {
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Calls `set_method`, or the exact-token parse if `options.exact_methods` is set.
    pub fn set_method_with(
        &mut self,
        message: &mut HttpMessage,
        method: &[u8],
        options: FieldDecodeOptions,
    ) -> bool {
        if options.exact_methods {
            return self.set_exact_method(message, method);
        }
        self.set_method(message, method)
    }

    /// `set_method` with `FieldDecodeOptions::exact_methods`, out of line so that the
    /// default path keeps its own frame.
    #[inline(never)]
    fn set_exact_method(&mut self, message: &mut HttpMessage, method: &[u8]) -> bool {
        if self.has_method {
            return self.fail("Duplicate method", false);
        }
        let Some(exact) = Method::from_exact_token(method) else {
            return self.fail(
                format!("Invalid method: {}", String::from_utf8_lossy(method)),
                true,
            );
        };
        self.has_method = true;
        message.set_method(exact).is_ok()
    }

    pub fn set_method(&mut self, message: &mut HttpMessage, method: &[u8]) -> bool {
        if self.has_method {
            return self.fail("Duplicate method", false);
        }
        if !validate_method(method) {
            return self.fail(
                format!("Invalid method: {}", String::from_utf8_lossy(method)),
                true,
            );
        }
        let Ok(method) = std::str::from_utf8(method) else {
            return self.fail("Invalid method", true);
        };
        self.has_method = true;
        message.set_method(method).is_ok()
    }

    pub fn set_path(
        &mut self,
        message: &mut HttpMessage,
        path: &[u8],
        strict: bool,
        allow_empty: bool,
    ) -> bool {
        if self.has_path {
            return self.fail("Duplicate path", false);
        }
        if !validate_url(path, strict) {
            return self.fail(
                format!("Invalid url: {}", String::from_utf8_lossy(path)),
                true,
            );
        }
        self.has_path = true;
        let strict = strict && !(allow_empty && path.is_empty());
        message
            .set_url_components(path, strict)
            .is_ok_and(|parsed| !strict || parsed.valid())
    }

    pub fn set_scheme(&mut self, message: &mut HttpMessage, scheme: &[u8]) -> bool {
        if self.has_scheme {
            return self.fail("Duplicate scheme", false);
        }
        if !validate_scheme(scheme) {
            return self.fail(
                format!("Invalid scheme: {}", String::from_utf8_lossy(scheme)),
                true,
            );
        }
        self.has_scheme = true;
        if scheme == b"https" {
            message.set_secure(true);
        } else if scheme == b"masque" {
            message.set_masque();
        }
        true
    }

    pub fn set_authority(
        &mut self,
        message: &mut HttpMessage,
        authority: &[u8],
        validate: bool,
        strict: bool,
    ) -> bool {
        if self.has_authority {
            return self.fail("Duplicate authority", false);
        }
        let mode = if strict {
            ControlEscapeMode::Strict
        } else {
            ControlEscapeMode::StrictCompat
        };
        if validate && !validate_header_value(authority, mode) {
            return self.fail(
                format!("Invalid authority: {}", String::from_utf8_lossy(authority)),
                true,
            );
        }
        self.has_authority = true;
        message.headers_mut().add(HttpHeaderCode::Host, authority);
        true
    }

    pub fn set_upgrade_protocol(
        &mut self,
        message: &mut HttpMessage,
        protocol: &[u8],
        strict: bool,
    ) -> bool {
        if self.has_upgrade_protocol {
            return self.fail("Duplicate protocol", false);
        }
        if strict && !validate_header_value(protocol, ControlEscapeMode::Strict) {
            return self.fail(
                format!("Invalid protocol: {}", String::from_utf8_lossy(protocol)),
                true,
            );
        }
        self.has_upgrade_protocol = true;
        message.set_upgrade_protocol(protocol);
        true
    }

    pub fn validate(&mut self, message: &mut HttpMessage) -> bool {
        if self.error.is_some() {
            return false;
        }
        let connect = message.method().ok().flatten() == Some(HttpMethod::Connect);
        let malformed = if connect {
            (!self.has_upgrade_protocol
                && (!self.has_method || !self.has_authority || self.has_scheme || self.has_path))
                || (self.has_upgrade_protocol && (!self.has_scheme || !self.has_path))
        } else {
            self.has_upgrade_protocol || !self.has_method || !self.has_scheme || !self.has_path
        };
        if malformed {
            self.error = Some(format!(
                "Malformed request m/a/s/pa/pr={}/{}/{}/{}/{}",
                self.has_method,
                self.has_authority,
                self.has_scheme,
                self.has_path,
                self.has_upgrade_protocol
            ));
        }
        !malformed
    }

    pub const fn has_authority(&self) -> bool {
        self.has_authority
    }

    pub const fn has_validation_error(&self) -> bool {
        self.validation_error
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    fn fail(&mut self, error: impl Into<String>, validation: bool) -> bool {
        self.error = Some(error.into());
        self.validation_error |= validation;
        false
    }
}

/// Stateful validation for one decoded HPACK field section.
#[derive(Debug)]
pub struct HeaderDecodeInfo {
    pub message: Box<HttpMessage>,
    pub parsing_error: Option<String>,
    pub header_error_value: Box<[u8]>,
    pub proxygen_error: Option<ProxygenError>,
    pub decode_error: Option<HpackError>,
    verifier: HttpRequestVerifier,
    options: FieldDecodeOptions,
    /// Set when `validate` is on and `FieldDecodeOptions::verbatim_values` is off. Regular
    /// fields are then validated and stored trimmed, and `on_header` tests only this flag.
    reference_fields: bool,
    is_request: bool,
    request_trailers: bool,
    validate: bool,
    strict: bool,
    allow_empty_path: bool,
    has_status: bool,
    regular_header_seen: bool,
    pseudo_header_seen: bool,
    missing_end_stream: bool,
}

impl HeaderDecodeInfo {
    pub fn new(
        is_request: bool,
        request_trailers: bool,
        validate: bool,
        strict: bool,
        allow_empty_path: bool,
    ) -> Self {
        let options = FieldDecodeOptions::default();
        Self {
            message: Box::new(HttpMessage::new()),
            parsing_error: None,
            header_error_value: Box::default(),
            proxygen_error: None,
            decode_error: None,
            verifier: HttpRequestVerifier::default(),
            options,
            reference_fields: Self::uses_reference_fields(validate, options),
            is_request,
            request_trailers,
            validate,
            strict,
            allow_empty_path,
            has_status: false,
            regular_header_seen: false,
            pseudo_header_seen: false,
            missing_end_stream: false,
        }
    }

    /// Decodes with `options` instead of the defaults.
    #[must_use]
    pub const fn with_options(mut self, options: FieldDecodeOptions) -> Self {
        self.set_options(options);
        self
    }

    /// Sets `options` in place, as `with_options` does by value.
    /// Marks a header block whose HEADERS frame did not end the stream, which trailers
    /// must do.
    pub(crate) const fn set_missing_end_stream(&mut self, missing: bool) {
        self.missing_end_stream = missing;
    }

    pub(crate) const fn set_options(&mut self, options: FieldDecodeOptions) {
        self.options = options;
        self.reference_fields = Self::uses_reference_fields(self.validate, options);
    }

    const fn uses_reference_fields(validate: bool, options: FieldDecodeOptions) -> bool {
        validate && !options.verbatim_values
    }

    pub fn decode(mut self, decoded: DecodedHeaderBlock) -> Result<HttpMessage, Box<Self>> {
        for header in decoded.headers {
            if !self.on_header(&header.name, &header.value) {
                break;
            }
        }
        self.on_headers_complete(decoded.size);
        self.into_message()
    }

    /// Consumes completed decoding, retaining partial message details on failure.
    pub(crate) fn into_message(self) -> Result<HttpMessage, Box<Self>> {
        if self.parsing_error.is_some() || self.decode_error.is_some() {
            Err(Box::new(self))
        } else {
            Ok(*self.message)
        }
    }

    pub const fn has_status(&self) -> bool {
        self.has_status
    }

    fn on_header(&mut self, name: &HeaderName, value: &[u8]) -> bool {
        debug_assert_eq!(
            self.reference_fields,
            Self::uses_reference_fields(self.validate, self.options)
        );
        if self.decode_error.is_some() || self.parsing_error.is_some() {
            return true;
        }
        let bytes = name.as_bytes();
        let code = name.code();
        if bytes.starts_with(b":") {
            self.pseudo_header_seen = true;
            if self.regular_header_seen {
                return self.fail(format!(
                    "Illegal pseudo header name={}",
                    String::from_utf8_lossy(bytes)
                ));
            }
            return if self.is_request {
                let ok = match code {
                    HttpHeaderCode::ColonMethod => {
                        self.verifier
                            .set_method_with(&mut self.message, value, self.options)
                    }
                    HttpHeaderCode::ColonScheme => {
                        self.verifier.set_scheme(&mut self.message, value)
                    }
                    HttpHeaderCode::ColonAuthority => self.verifier.set_authority(
                        &mut self.message,
                        value,
                        self.validate,
                        self.strict,
                    ),
                    HttpHeaderCode::ColonPath => self.verifier.set_path(
                        &mut self.message,
                        value,
                        self.strict,
                        self.allow_empty_path,
                    ),
                    HttpHeaderCode::ColonProtocol => {
                        self.verifier
                            .set_upgrade_protocol(&mut self.message, value, self.strict)
                    }
                    _ => {
                        return self.fail(format!(
                            "Invalid req header name={}",
                            String::from_utf8_lossy(bytes)
                        ));
                    }
                };
                if self.verifier.has_validation_error() {
                    self.proxygen_error = Some(ProxygenError::HEADER_CONTENT_VALIDATION);
                }
                if !ok {
                    self.parsing_error = self.verifier.error().map(str::to_owned);
                }
                ok
            } else if code == HttpHeaderCode::ColonStatus {
                if self.has_status {
                    return self.fail("Duplicate status");
                }
                self.has_status = true;
                let status = std::str::from_utf8(value)
                    .ok()
                    .and_then(|status| status.parse::<u16>().ok());
                if let Some(status) = status.filter(|status| (100..=999).contains(status)) {
                    let status_set = self.message.set_status_code(status).is_ok();
                    let reason_set = self
                        .message
                        .set_status_message(HttpMessage::default_reason(status))
                        .is_ok();
                    status_set && reason_set
                } else {
                    self.fail(format!(
                        "Malformed status code={}",
                        String::from_utf8_lossy(value)
                    ))
                }
            } else {
                self.fail(format!(
                    "Invalid resp header name={}",
                    String::from_utf8_lossy(bytes)
                ))
            };
        }

        self.regular_header_seen = true;
        match code {
            HttpHeaderCode::Connection => {
                return self.fail("HTTP/2 Message with Connection header");
            }
            HttpHeaderCode::ContentLength
                if self.options.verbatim_values && !is_h2_content_length(value) =>
            {
                return self.fail("Invalid content-length");
            }
            HttpHeaderCode::ContentLength => {
                if self
                    .message
                    .headers()
                    .number_of_values(HttpHeaderCode::ContentLength)
                    != 0
                {
                    let existing = self
                        .message
                        .headers()
                        .get_single_or_empty(HttpHeaderCode::ContentLength);
                    if existing != value {
                        return self.fail("Multiple content-length headers");
                    }
                    return true;
                }
            }
            HttpHeaderCode::Host if self.verifier.has_authority() => {
                if self
                    .message
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::Host)
                    != value
                {
                    return self.fail(":authority/Host header mismatch");
                }
                return true;
            }
            _ => {}
        }
        if self.reference_fields {
            if (!name.is_common_header() && !validate_header_name(bytes, self.name_mode()))
                || !validate_header_value(value, self.value_mode())
            {
                return self.reject_field(bytes, value);
            }
        } else if self.options.verbatim_values {
            return self.add_verbatim_field(name, value);
        }
        if name.is_common_header() {
            self.message.headers_mut().add(code, value);
        } else {
            self.message.headers_mut().add_uncommon(bytes, value);
        }
        true
    }

    /// The rest of `on_header` for a regular field when values are kept verbatim. The field is
    /// validated as h2 validates it.
    #[inline(never)]
    fn add_verbatim_field(&mut self, name: &HeaderName, value: &[u8]) -> bool {
        let bytes = name.as_bytes();
        if name.code() == HttpHeaderCode::Te && value != b"trailers" {
            return self.fail("HTTP/2 Message with a TE other than trailers");
        }
        if self.validate
            && ((!name.is_common_header() && !validate_header_name(bytes, self.name_mode()))
                || !is_h2_field_value(value, self.strict))
        {
            return self.reject_field(bytes, value);
        }
        if name.is_common_header() {
            self.message.headers_mut().add_verbatim(name.code(), value);
        } else {
            self.message
                .headers_mut()
                .add_uncommon_verbatim(bytes, value);
        }
        true
    }

    const fn name_mode(&self) -> HeaderNameValidationMode {
        if self.strict {
            HeaderNameValidationMode::Strict
        } else {
            HeaderNameValidationMode::StrictCompat
        }
    }

    const fn value_mode(&self) -> ControlEscapeMode {
        if self.strict {
            ControlEscapeMode::Strict
        } else {
            ControlEscapeMode::StrictCompat
        }
    }

    /// Fails on a regular field that validation rejects, keeping its value.
    #[cold]
    fn reject_field(&mut self, bytes: &[u8], value: &[u8]) -> bool {
        self.proxygen_error = Some(if bytes.is_empty() {
            ProxygenError::PARSE_HEADER
        } else {
            ProxygenError::HEADER_CONTENT_VALIDATION
        });
        self.header_error_value = value.to_vec().into_boxed_slice();
        self.fail(format!(
            "Invalid header name={}",
            String::from_utf8_lossy(bytes)
        ))
    }

    fn on_headers_complete(&mut self, decoded_size: HttpHeaderSize) {
        if self.parsing_error.is_some() {
            return;
        }
        if self.is_request && !self.request_trailers {
            let cookie = self
                .message
                .headers()
                .combine(HttpHeaderCode::Cookie, b"; ");
            if !cookie.is_empty() {
                self.set_combined_cookie(cookie);
            }
            if !self.verifier.validate(&mut self.message) {
                self.parsing_error = self.verifier.error().map(str::to_owned);
                return;
            }
        }
        let response_trailers = !self.is_request && !self.has_status;
        if self.request_trailers || response_trailers {
            if self.pseudo_header_seen {
                self.parsing_error = Some("Pseudo headers forbidden in trailers.".to_owned());
                return;
            }
            if self.missing_end_stream {
                self.parsing_error = Some("Trailers without END_STREAM.".to_owned());
                return;
            }
        }
        self.message.set_http_version(1, 1);
        self.message.set_ingress_header_size(decoded_size);
    }

    /// Replaces the `cookie` crumbs with their combined value, which is kept verbatim with
    /// `FieldDecodeOptions::verbatim_values`.
    #[inline(never)]
    fn set_combined_cookie(&mut self, cookie: Box<[u8]>) {
        let headers = self.message.headers_mut();
        if self.options.verbatim_values {
            headers.remove(HttpHeaderCode::Cookie);
            headers.add_verbatim(HttpHeaderCode::Cookie, cookie);
        } else {
            headers.set(HttpHeaderCode::Cookie, cookie);
        }
    }

    fn fail(&mut self, error: impl Into<String>) -> bool {
        self.parsing_error = Some(error.into());
        false
    }
}

impl StreamingCallback for HeaderDecodeInfo {
    fn on_header(&mut self, name: &HeaderName, value: &[u8]) {
        // HTTP validation errors must not stop HPACK's dynamic-table updates.
        Self::on_header(self, name, value);
    }

    fn on_headers_complete(&mut self, decoded_size: HttpHeaderSize, _acknowledge: bool) {
        Self::on_headers_complete(self, decoded_size);
    }

    fn on_decode_error(&mut self, error: HpackError) {
        self.decode_error = Some(error);
    }
}
