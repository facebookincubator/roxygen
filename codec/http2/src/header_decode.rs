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

/// Roxygen-only options for decoding a field section, all off by default.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FieldDecodeOptions {
    /// `DIV-040`: a request's `:method` is any RFC 9110 token, case-sensitive and kept byte
    /// for byte (`Method::from_exact_token`), as `http::Method::from_bytes` reads it, instead
    /// of a run of letters and `-` whose recognized methods match without regard to case.
    pub exact_methods: bool,
    /// `DIV-041`: regular field values are stored exactly as decoded, as h2 stores them,
    /// instead of trimmed of surrounding whitespace, and are validated as h2 validates
    /// them: no byte below 0x20 other than HTAB and no DEL (so no obsolete line folding),
    /// in strict mode also no byte above 0x7E, a `content-length` of at most 19 digits (an
    /// empty one is kept, and a session reads it as zero), and a `te` of exactly
    /// `trailers`. Crumbs of `cookie` are still combined into one value, without trimming
    /// it.
    pub verbatim_values: bool,
}

/// Whether `value` is a field value h2 0.4 accepts (`HeaderValue::from_bytes`): no byte
/// below 0x20 other than HTAB, and no DEL; with `strict`, Roxygen's strict validation, no
/// byte above 0x7E either.
fn is_h2_field_value(value: &[u8], strict: bool) -> bool {
    value
        .iter()
        .all(|byte| (*byte >= 0x20 || *byte == b'\t') && *byte != 0x7f && (!strict || *byte < 0x80))
}

/// Whether `value` is a `content-length` h2 0.4 accepts (`frame::headers::parse_u64`): at most
/// 19 decimal digits, an empty value (which it reads as zero) included.
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

    pub fn set_method(&mut self, message: &mut HttpMessage, method: &[u8]) -> bool {
        self.set_method_with(message, method, FieldDecodeOptions::default())
    }

    /// `set_method`, reading the method as `options` say.
    pub fn set_method_with(
        &mut self,
        message: &mut HttpMessage,
        method: &[u8],
        options: FieldDecodeOptions,
    ) -> bool {
        if self.has_method {
            return self.fail("Duplicate method", false);
        }
        if options.exact_methods {
            let Some(exact) = Method::from_exact_token(method) else {
                return self.fail(
                    format!("Invalid method: {}", String::from_utf8_lossy(method)),
                    true,
                );
            };
            self.has_method = true;
            return message.set_method(exact).is_ok();
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
            .set_url(path, strict)
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
    is_request: bool,
    request_trailers: bool,
    validate: bool,
    strict: bool,
    allow_empty_path: bool,
    has_status: bool,
    regular_header_seen: bool,
    pseudo_header_seen: bool,
}

impl HeaderDecodeInfo {
    pub fn new(
        is_request: bool,
        request_trailers: bool,
        validate: bool,
        strict: bool,
        allow_empty_path: bool,
    ) -> Self {
        Self {
            message: Box::new(HttpMessage::new()),
            parsing_error: None,
            header_error_value: Box::default(),
            proxygen_error: None,
            decode_error: None,
            verifier: HttpRequestVerifier::default(),
            options: FieldDecodeOptions::default(),
            is_request,
            request_trailers,
            validate,
            strict,
            allow_empty_path,
            has_status: false,
            regular_header_seen: false,
            pseudo_header_seen: false,
        }
    }

    /// Decodes with `options` instead of the defaults.
    #[must_use]
    pub const fn with_options(mut self, options: FieldDecodeOptions) -> Self {
        self.options = options;
        self
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
        let verbatim = self.options.verbatim_values;
        match code {
            HttpHeaderCode::Connection => {
                return self.fail("HTTP/2 Message with Connection header");
            }
            HttpHeaderCode::ContentLength if verbatim && !is_h2_content_length(value) => {
                return self.fail("Invalid content-length");
            }
            HttpHeaderCode::Te if verbatim && value != b"trailers" => {
                return self.fail("HTTP/2 Message with a TE other than trailers");
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
        let name_mode = if self.strict {
            HeaderNameValidationMode::Strict
        } else {
            HeaderNameValidationMode::StrictCompat
        };
        let value_mode = if self.strict {
            ControlEscapeMode::Strict
        } else {
            ControlEscapeMode::StrictCompat
        };
        let value_valid = if verbatim {
            is_h2_field_value(value, self.strict)
        } else {
            validate_header_value(value, value_mode)
        };
        if self.validate
            && ((!name.is_common_header() && !validate_header_name(bytes, name_mode))
                || !value_valid)
        {
            self.proxygen_error = Some(if bytes.is_empty() {
                ProxygenError::PARSE_HEADER
            } else {
                ProxygenError::HEADER_CONTENT_VALIDATION
            });
            self.header_error_value = value.to_vec().into_boxed_slice();
            return self.fail(format!(
                "Invalid header name={}",
                String::from_utf8_lossy(bytes)
            ));
        }
        match (name.is_common_header(), verbatim) {
            (true, false) => self.message.headers_mut().add(code, value),
            (false, false) => self.message.headers_mut().add_uncommon(bytes, value),
            (true, true) => self.message.headers_mut().add_verbatim(code, value),
            (false, true) => self
                .message
                .headers_mut()
                .add_uncommon_verbatim(bytes, value),
        }
        true
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
                let headers = self.message.headers_mut();
                if self.options.verbatim_values {
                    headers.remove(HttpHeaderCode::Cookie);
                    headers.add_verbatim(HttpHeaderCode::Cookie, cookie);
                } else {
                    headers.set(HttpHeaderCode::Cookie, cookie);
                }
            }
            if !self.verifier.validate(&mut self.message) {
                self.parsing_error = self.verifier.error().map(str::to_owned);
                return;
            }
        }
        let response_trailers = !self.is_request && !self.has_status;
        if (self.request_trailers || response_trailers) && self.pseudo_header_seen {
            self.parsing_error = Some("Pseudo headers forbidden in trailers.".to_owned());
            return;
        }
        self.message.set_http_version(1, 1);
        self.message.set_ingress_header_size(decoded_size);
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
