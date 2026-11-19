/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Version;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;

use crate::HttpAdapterError;
use crate::RoxygenRequest;
use crate::RoxygenResponse;
use crate::UnsupportedMessageMetadata;

#[test]
fn owned_request_response_round_trip() {
    let request_body = Box::new(vec![1_u8, 2, 3]);
    let request_body_ptr = request_body.as_ptr();
    let mut request = Request::new(request_body);
    *request.method_mut() = Method::from_bytes(b"PURGE").expect("valid extension method");
    *request.uri_mut() = "https://example.com/a/b?x=1"
        .parse()
        .expect("valid absolute URI");
    *request.version_mut() = Version::HTTP_2;
    request
        .headers_mut()
        .append("x-first", HeaderValue::from_static("one"));
    request.headers_mut().append(
        "x-raw",
        HeaderValue::from_bytes(b"inside\tand-obs-text\x80").expect("valid raw value"),
    );
    request
        .headers_mut()
        .append("x-first", HeaderValue::from_static("two"));
    let expected_headers = request.headers().clone();
    let expected_order: Vec<_> = request
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect();

    let mut roxygen = RoxygenRequest::try_from(request).expect("request converts to Roxygen");
    assert_eq!(roxygen.body().as_ptr(), request_body_ptr);
    assert_eq!(roxygen.message_mut().method_string().unwrap(), "PURGE");
    assert_eq!(
        roxygen.message_mut().url().unwrap(),
        b"https://example.com/a/b?x=1"
    );
    assert_eq!(roxygen.message().http_version(), (2, 0));
    assert!(roxygen.message().is_secure());
    let actual_order: Vec<_> = roxygen
        .message()
        .headers()
        .iter()
        .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
        .collect();
    assert_eq!(actual_order, expected_order);

    let request = Request::try_from(roxygen).expect("request converts back to http");
    assert_eq!(request.body().as_ptr(), request_body_ptr);
    assert_eq!(request.method(), "PURGE");
    assert_eq!(request.uri(), "https://example.com/a/b?x=1");
    assert_eq!(request.version(), Version::HTTP_2);
    assert_eq!(request.headers(), &expected_headers);
    assert_eq!(
        request
            .headers()
            .get_all("x-first")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect::<Vec<_>>(),
        vec![b"one".as_slice(), b"two".as_slice()]
    );
    assert_eq!(
        request.headers()["x-raw"].as_bytes(),
        b"inside\tand-obs-text\x80"
    );

    let response_body = Box::new(String::from("owned response body"));
    let response_body_ptr = response_body.as_ptr();
    let mut response = Response::new(response_body);
    *response.status_mut() = StatusCode::IM_A_TEAPOT;
    *response.version_mut() = Version::HTTP_10;
    response
        .headers_mut()
        .append("set-cookie", HeaderValue::from_static("a=1"));
    response
        .headers_mut()
        .append("set-cookie", HeaderValue::from_static("b=2"));
    response.headers_mut().append(
        "x-response-raw",
        HeaderValue::from_bytes(b"\xfe").expect("valid obs-text"),
    );
    let expected_headers = response.headers().clone();

    let mut roxygen = RoxygenResponse::try_from(response).expect("response converts to Roxygen");
    assert_eq!(roxygen.body().as_ptr(), response_body_ptr);
    assert_eq!(roxygen.message_mut().status_code().unwrap(), 418);
    assert_eq!(roxygen.message().http_version(), (1, 0));

    let response = Response::try_from(roxygen).expect("response converts back to http");
    assert_eq!(response.body().as_ptr(), response_body_ptr);
    assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
    assert_eq!(response.version(), Version::HTTP_10);
    assert_eq!(response.headers(), &expected_headers);
    assert_eq!(
        response
            .headers()
            .get_all("set-cookie")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect::<Vec<_>>(),
        vec![b"a=1".as_slice(), b"b=2".as_slice()]
    );
}

#[test]
fn roxygen_metadata_converts_without_defaults() {
    let mut request_message = HttpMessage::new();
    request_message.set_method("SUB").unwrap();
    request_message
        .set_url(b"/topic?partition=2", true)
        .unwrap();
    request_message.set_http_version(1, 1);
    request_message.headers_mut().add(b"x-sequence", b"first");
    request_message
        .headers_mut()
        .add(b"x-sequence", b"second\x80");

    let request = Request::try_from(
        RoxygenRequest::try_from_parts(request_message, vec![9_u8]).expect("request message kind"),
    )
    .expect("Roxygen request converts");
    assert_eq!(request.method(), "SUB");
    assert_eq!(request.uri(), "/topic?partition=2");
    assert_eq!(request.version(), Version::HTTP_11);
    assert_eq!(
        request
            .headers()
            .get_all("x-sequence")
            .iter()
            .map(HeaderValue::as_bytes)
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second\x80".as_slice()]
    );
    assert_eq!(request.body(), &[9]);

    let mut response_message = HttpMessage::new();
    response_message.set_status_code(599).unwrap();
    response_message.set_http_version(2, 0);
    response_message.headers_mut().add(b"x-result", b"complete");
    let response = Response::try_from(
        RoxygenResponse::try_from_parts(response_message, String::from("body"))
            .expect("response message kind"),
    )
    .expect("Roxygen response converts");
    assert_eq!(response.status().as_u16(), 599);
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.headers()["x-result"], "complete");
    assert_eq!(response.body(), "body");
}

#[test]
fn http_only_metadata_is_rejected_instead_of_dropped() {
    let mut with_extension = Request::new(());
    with_extension.extensions_mut().insert(7_u32);
    assert!(matches!(
        RoxygenRequest::try_from(with_extension),
        Err(HttpAdapterError::ExtensionsUnsupported)
    ));

    let mut lower_method = Request::new(());
    *lower_method.method_mut() = Method::from_bytes(b"custom").expect("valid method token");
    assert!(matches!(
        RoxygenRequest::try_from(lower_method),
        Err(HttpAdapterError::NonCanonicalMethod(method)) if method == "custom"
    ));

    let mut boundary_whitespace = Request::new(());
    boundary_whitespace
        .headers_mut()
        .insert("x-value", HeaderValue::from_static(" value\t"));
    assert!(matches!(
        RoxygenRequest::try_from(boundary_whitespace),
        Err(HttpAdapterError::HeaderBoundaryWhitespace(name)) if name == "x-value"
    ));

    let mut sensitive = Request::new(());
    let mut sensitive_value = HeaderValue::from_static("secret");
    sensitive_value.set_sensitive(true);
    sensitive
        .headers_mut()
        .insert("authorization", sensitive_value);
    assert!(matches!(
        RoxygenRequest::try_from(sensitive),
        Err(HttpAdapterError::SensitiveHeader(name)) if name == "authorization"
    ));

    let mut h3 = Request::new(());
    *h3.version_mut() = Version::HTTP_3;
    assert!(matches!(
        RoxygenRequest::try_from(h3),
        Err(HttpAdapterError::UnsupportedHttpVersion(Version::HTTP_3))
    ));

    let mut unsupported_uri = Request::new(());
    *unsupported_uri.uri_mut() = "git+ssh://example.com/repository"
        .parse()
        .expect("valid http::Uri with a non-Proxygen scheme grammar");
    assert!(matches!(
        RoxygenRequest::try_from(unsupported_uri),
        Err(HttpAdapterError::UriNotRepresentable(_))
    ));

    let mut unsupported_scheme = Request::new(());
    *unsupported_scheme.uri_mut() = "ftp://example.com/resource"
        .parse()
        .expect("valid http::Uri with a scheme outside the adapter scope");
    assert!(matches!(
        RoxygenRequest::try_from(unsupported_scheme),
        Err(HttpAdapterError::UnsupportedUriScheme(scheme)) if scheme.as_ref() == "ftp"
    ));
}

#[test]
fn roxygen_only_or_invalid_metadata_is_rejected() {
    let mut missing_method = HttpMessage::new();
    missing_method.set_url(b"/", true).unwrap();
    let request = RoxygenRequest::try_from_parts(missing_method, ()).unwrap();
    assert!(matches!(
        Request::try_from(request),
        Err(HttpAdapterError::MissingMethod)
    ));

    let mut unsupported_version = HttpMessage::new();
    unsupported_version.set_method("GET").unwrap();
    unsupported_version.set_url(b"/", true).unwrap();
    unsupported_version.set_http_version(9, 9);
    let request = RoxygenRequest::try_from_parts(unsupported_version, ()).unwrap();
    assert!(matches!(
        Request::try_from(request),
        Err(HttpAdapterError::UnsupportedRoxygenVersion { major: 9, minor: 9 })
    ));

    let mut invalid_header_name = HttpMessage::new();
    invalid_header_name.set_method("GET").unwrap();
    invalid_header_name.set_url(b"/", true).unwrap();
    invalid_header_name.headers_mut().add(b"bad name", b"value");
    let request = RoxygenRequest::try_from_parts(invalid_header_name, ()).unwrap();
    assert!(matches!(
        Request::try_from(request),
        Err(HttpAdapterError::InvalidHeaderName(_))
    ));

    let mut invalid_header_value = HttpMessage::new();
    invalid_header_value.set_method("GET").unwrap();
    invalid_header_value.set_url(b"/", true).unwrap();
    invalid_header_value
        .headers_mut()
        .add(b"x-value", b"contains\0nul");
    let request = RoxygenRequest::try_from_parts(invalid_header_value, ()).unwrap();
    assert!(matches!(
        Request::try_from(request),
        Err(HttpAdapterError::InvalidHeaderValue(_))
    ));

    let mut with_trailers = HttpMessage::new();
    with_trailers.set_method("GET").unwrap();
    with_trailers.set_url(b"/", true).unwrap();
    with_trailers.set_trailers(Box::new(HttpHeaders::new()));
    let request = RoxygenRequest::try_from_parts(with_trailers, ()).unwrap();
    assert!(matches!(
        Request::try_from(request),
        Err(HttpAdapterError::TrailersUnsupported)
    ));

    let mut reason_phrase = HttpMessage::new();
    reason_phrase.set_status_code(200).unwrap();
    reason_phrase
        .set_status_message(b"Everything Fine")
        .unwrap();
    let response = RoxygenResponse::try_from_parts(reason_phrase, ()).unwrap();
    assert!(matches!(
        Response::try_from(response),
        Err(HttpAdapterError::StatusMessageUnsupported)
    ));

    let mut invalid_status = HttpMessage::new();
    invalid_status.set_status_code(99).unwrap();
    let response = RoxygenResponse::try_from_parts(invalid_status, ()).unwrap();
    assert!(matches!(
        Response::try_from(response),
        Err(HttpAdapterError::InvalidStatusCode(_))
    ));
}

#[test]
fn nonrepresentable_roxygen_request_metadata_is_rejected() {
    assert_request_metadata_rejected(UnsupportedMessageMetadata::Chunked, |message| {
        message.set_chunked(true);
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::Upgraded, |message| {
        message.set_upgraded(true);
    });
    assert_request_metadata_rejected(
        UnsupportedMessageMetadata::IngressWebSocketUpgrade,
        HttpMessage::set_ingress_websocket_upgrade,
    );
    assert_request_metadata_rejected(
        UnsupportedMessageMetadata::EgressWebSocketUpgrade,
        HttpMessage::set_egress_websocket_upgrade,
    );
    assert_request_metadata_rejected(UnsupportedMessageMetadata::KeepaliveDisabled, |message| {
        message.set_wants_keepalive(false)
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::TrailersAllowed, |message| {
        message.set_trailers_allowed(true)
    });
    assert_request_metadata_rejected(
        UnsupportedMessageMetadata::StrippedPerHopHeaders,
        |message| message.strip_per_hop_headers(false, None),
    );
    assert_request_metadata_rejected(UnsupportedMessageMetadata::UpgradeProtocol, |message| {
        message.set_upgrade_protocol(b"websocket");
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::ClientAddress, |message| {
        message
            .set_client_socket_address(SocketAddress::new(b"127.0.0.1", 1234))
            .unwrap();
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::DestinationAddress, |message| {
        message.set_destination_socket_address(SocketAddress::new(b"127.0.0.2", 443));
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::LocalIp, |message| {
        message.set_local_ip(b"127.0.0.3");
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::SecureInfo, |message| {
        message.set_secure_info(13, b"cipher");
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::AdvancedProtocol, |message| {
        message.set_advanced_protocol_string(b"h2");
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::LegacyPriority, |message| {
        message.set_legacy_priority(0);
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::SequenceNumber, |message| {
        message.set_sequence_number(7);
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::IngressHeaderSize, |message| {
        message.set_ingress_header_size(HttpHeaderSize {
            compressed: 1,
            uncompressed: 2,
            compressed_block: 1,
        });
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::MasqueScheme, |message| {
        message.set_masque();
    });
    assert_request_metadata_rejected(UnsupportedMessageMetadata::UriSchemeMismatch, |message| {
        message.set_secure(true)
    });
}

#[test]
fn nonrepresentable_roxygen_response_metadata_is_rejected() {
    assert_response_metadata_rejected(UnsupportedMessageMetadata::Chunked, |message| {
        message.set_chunked(true);
    });
    assert_response_metadata_rejected(UnsupportedMessageMetadata::DestinationAddress, |message| {
        message.set_destination_socket_address(SocketAddress::new(b"::1", 8443));
    });
    assert_response_metadata_rejected(UnsupportedMessageMetadata::ResponseScheme, |message| {
        message.set_secure(true)
    });
}

#[test]
fn request_scheme_must_match_the_uri() {
    let mut secure = request_message();
    secure.set_url(b"https://example.com/secure", true).unwrap();
    secure.set_secure(true);
    let converted = Request::try_from(RoxygenRequest::try_from_parts(secure, ()).unwrap())
        .expect("coherent HTTPS metadata converts");
    assert_eq!(converted.uri(), "https://example.com/secure");

    let mut insecure_https = request_message();
    insecure_https
        .set_url(b"https://example.com/mismatch", true)
        .unwrap();
    let error = Request::try_from(
        RoxygenRequest::try_from_parts(insecure_https, ()).expect("request kind"),
    )
    .expect_err("HTTPS URI without secure message state must fail");
    assert!(matches!(
        error,
        HttpAdapterError::UnsupportedMessageMetadata(UnsupportedMessageMetadata::UriSchemeMismatch)
    ));

    let mut unsupported = request_message();
    unsupported
        .set_url(b"ftp://example.com/resource", true)
        .unwrap();
    let error =
        Request::try_from(RoxygenRequest::try_from_parts(unsupported, ()).expect("request kind"))
            .expect_err("non-HTTP URI scheme must fail");
    assert!(matches!(
        error,
        HttpAdapterError::UnsupportedUriScheme(scheme) if scheme.as_ref() == "ftp"
    ));
}

#[test]
fn core_header_layout_must_be_byte_faithful() {
    let mut normalized = request_message();
    normalized.headers_mut().add(b"X-Mixed-Case", b"value");
    let error =
        Request::try_from(RoxygenRequest::try_from_parts(normalized, ()).expect("request kind"))
            .expect_err("normalizing a header name must fail");
    assert!(matches!(
        error,
        HttpAdapterError::HeaderNameNormalization {
            original,
            normalized,
        } if original.as_ref() == b"X-Mixed-Case" && normalized == "x-mixed-case"
    ));

    let mut interleaved = request_message();
    interleaved.headers_mut().add(b"a", b"1");
    interleaved.headers_mut().add(b"b", b"x");
    interleaved.headers_mut().add(b"a", b"2");
    let error =
        Request::try_from(RoxygenRequest::try_from_parts(interleaved, ()).expect("request kind"))
            .expect_err("HeaderMap would regroup interleaved values");
    assert!(matches!(
        error,
        HttpAdapterError::InterleavedHeaderName(name) if name == "a"
    ));
}

#[test]
fn max_size_error_retains_its_source_type() {
    let source = HeaderMap::<HeaderValue>::try_with_capacity(usize::MAX)
        .expect_err("impossible HeaderMap capacity must fail");
    let error = HttpAdapterError::from(source);
    assert!(matches!(&error, HttpAdapterError::MaxSizeReached(_)));
    assert_eq!(
        std::error::Error::source(&error)
            .expect("retained capacity source")
            .to_string(),
        "max size reached"
    );
}

#[test]
fn request_and_response_wrappers_reject_the_wrong_message_kind() {
    let mut response = HttpMessage::new();
    response.set_status_code(200).unwrap();
    assert!(matches!(
        RoxygenRequest::try_from_parts(response, ()),
        Err(HttpAdapterError::ExpectedRequest)
    ));

    let mut request = HttpMessage::new();
    request.set_method("GET").unwrap();
    assert!(matches!(
        RoxygenResponse::try_from_parts(request, ()),
        Err(HttpAdapterError::ExpectedResponse)
    ));
}

fn request_message() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method("GET").unwrap();
    message.set_url(b"/", true).unwrap();
    message
}

fn response_message() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(200).unwrap();
    message
}

fn assert_request_metadata_rejected(
    expected: UnsupportedMessageMetadata,
    mutate: impl FnOnce(&mut HttpMessage),
) {
    let mut message = request_message();
    mutate(&mut message);
    let error = Request::try_from(
        RoxygenRequest::try_from_parts(message, ()).expect("request message kind"),
    )
    .expect_err("metadata must not be dropped");
    assert!(matches!(
        error,
        HttpAdapterError::UnsupportedMessageMetadata(actual) if actual == expected
    ));
}

fn assert_response_metadata_rejected(
    expected: UnsupportedMessageMetadata,
    mutate: impl FnOnce(&mut HttpMessage),
) {
    let mut message = response_message();
    mutate(&mut message);
    let error = Response::try_from(
        RoxygenResponse::try_from_parts(message, ()).expect("response message kind"),
    )
    .expect_err("metadata must not be dropped");
    assert!(matches!(
        error,
        HttpAdapterError::UnsupportedMessageMetadata(actual) if actual == expected
    ));
}
