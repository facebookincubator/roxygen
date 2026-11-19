/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use http::Extensions;
use http::HeaderMap;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Version;
use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_http1::Http1xCodec;
use roxygen_http2::Http2Codec;

use crate::HttpAdapterError;
use crate::NetworkHeadContext;
use crate::RoxygenRequest;
use crate::RoxygenResponse;
use crate::UnsupportedMessageMetadata;
use crate::project_request_head;
use crate::project_response_head;
use crate::request_head_to_http;
use crate::response_head_to_http;

#[test]
fn sensitivity_survives_unrelated_request_and_response_header_changes() {
    for response in [false, true] {
        let context = Arc::new(String::from("local context"));
        let mut headers = HeaderMap::new();
        for (name, value, sensitive) in [
            ("authorization", "secret", true),
            ("x-duplicate", "same", true),
            ("x-duplicate", "same", false),
            ("x-duplicate", "last", true),
            ("x-changed", "old", true),
            ("x-reordered", "first", true),
            ("x-reordered", "second", false),
            ("x-removed", "gone", true),
        ] {
            let mut value = HeaderValue::from_static(value);
            value.set_sensitive(sensitive);
            headers.append(name, value);
        }
        let (mut message, sidecar) = if response {
            let mut response = Response::new(());
            *response.headers_mut() = headers;
            response.extensions_mut().insert(Arc::clone(&context));
            project_response_head(response.into_parts().0).unwrap()
        } else {
            let mut request = Request::new(());
            *request.headers_mut() = headers;
            request.extensions_mut().insert(Arc::clone(&context));
            project_request_head(request.into_parts().0).unwrap()
        };
        message.headers_mut().add(b"X-Unrelated", b"new");
        message.headers_mut().set(b"x-changed", b"new");
        message.headers_mut().remove(b"x-reordered");
        message.headers_mut().add(b"X-Reordered", b"second");
        message.headers_mut().add(b"X-Reordered", b"first");
        message.headers_mut().remove(b"x-removed");
        let (headers, extensions) = if response {
            let parts = response_head_to_http(&message, sidecar).unwrap();
            (parts.headers, parts.extensions)
        } else {
            let parts = request_head_to_http(&message, sidecar).unwrap();
            (parts.headers, parts.extensions)
        };
        assert!(Arc::ptr_eq(
            extensions.get::<Arc<String>>().unwrap(),
            &context
        ));
        assert!(headers["authorization"].is_sensitive());
        assert_eq!(
            headers
                .get_all("x-duplicate")
                .iter()
                .map(|value| (value.as_bytes(), value.is_sensitive()))
                .collect::<Vec<_>>(),
            vec![
                (b"same".as_slice(), true),
                (b"same".as_slice(), false),
                (b"last".as_slice(), true),
            ]
        );
        assert!(!headers["x-unrelated"].is_sensitive());
        assert_eq!(headers["x-changed"], "new");
        assert!(!headers["x-changed"].is_sensitive());
        assert_eq!(
            headers
                .get_all("x-reordered")
                .iter()
                .map(|value| (value.as_bytes(), value.is_sensitive()))
                .collect::<Vec<_>>(),
            vec![(b"second".as_slice(), false), (b"first".as_slice(), false)]
        );
        assert!(!headers.contains_key("x-removed"));
    }
}

#[test]
fn network_projection() {
    let context = Arc::new(String::from("middleware context, not wire identity"));
    let body = Rc::new(17);
    let mut request = Request::builder()
        .method("POST")
        .uri("https://example.test/path?q=1")
        .version(Version::HTTP_2)
        .body(Rc::clone(&body))
        .unwrap();
    request.extensions_mut().insert(Arc::clone(&context));
    let mut sensitive = HeaderValue::from_static("secret");
    sensitive.set_sensitive(true);
    request.headers_mut().append("authorization", sensitive);
    request
        .headers_mut()
        .append("x-duplicate", HeaderValue::from_static("first"));
    request
        .headers_mut()
        .append("x-duplicate", HeaderValue::from_static("second"));
    let (parts, returned_body) = request.into_parts();
    let (message, sidecar) =
        project_request_head(parts).expect("network projection retains local metadata");
    assert!(Arc::ptr_eq(
        sidecar.extensions().get::<Arc<String>>().unwrap(),
        &context
    ));
    assert!(Rc::ptr_eq(&body, &returned_body));
    let parts = request_head_to_http(&message, sidecar).expect("head restored");
    assert_eq!(parts.uri, "https://example.test/path?q=1");
    assert_eq!(parts.version, Version::HTTP_2);
    assert!(parts.headers["authorization"].is_sensitive());
    assert_eq!(
        parts
            .headers
            .get_all("x-duplicate")
            .iter()
            .map(|value| value.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second".as_slice()]
    );
    assert!(Arc::ptr_eq(
        parts.extensions.get::<Arc<String>>().unwrap(),
        &context
    ));

    let mut response = Response::new(());
    *response.status_mut() = StatusCode::CREATED;
    response.extensions_mut().insert(Arc::clone(&context));
    let (message, sidecar) = project_response_head(response.into_parts().0).unwrap();
    let response = response_head_to_http(&message, sidecar).unwrap();
    assert_eq!(response.status, StatusCode::CREATED);
    assert!(Arc::ptr_eq(
        response.extensions.get::<Arc<String>>().unwrap(),
        &context
    ));
}

#[test]
fn received_network_metadata_does_not_weaken_lossless_conversion() {
    let mut message = HttpMessage::new();
    message.set_status_code(200).unwrap();
    message.set_status_message(b"a native reason").unwrap();
    message.set_chunked(true);
    message.set_trailers_allowed(true);
    message.set_secure(true);
    message.set_secure_info(3, b"test-cipher");
    message.headers_mut().add(b"X-Mixed", b"first");
    message.headers_mut().add(b"X-Other", b"middle");
    message.headers_mut().add(b"X-Mixed", b"last");
    let mut extensions = Extensions::new();
    extensions.insert(42_u64);
    let response =
        response_head_to_http(&message, NetworkHeadContext::from_extensions(extensions)).unwrap();
    assert_eq!(
        response
            .headers
            .get_all("x-mixed")
            .iter()
            .map(|value| value.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"last".as_slice()]
    );
    assert_eq!(response.extensions.get::<u64>(), Some(&42));
    assert!(message.is_chunked());
    assert!(matches!(
        Response::<()>::try_from(RoxygenResponse::try_from_parts(message, ()).unwrap()),
        Err(HttpAdapterError::UnsupportedMessageMetadata(_))
    ));

    let mut request = Request::new(());
    request.extensions_mut().insert(42_u64);
    assert!(matches!(
        RoxygenRequest::try_from(request),
        Err(HttpAdapterError::ExtensionsUnsupported)
    ));
    let mut response = Response::new(());
    let mut value = HeaderValue::from_static("secret");
    value.set_sensitive(true);
    response.headers_mut().insert("authorization", value);
    assert!(matches!(
        RoxygenResponse::try_from(response),
        Err(HttpAdapterError::SensitiveHeader(_))
    ));
}

#[test]
fn secure_origin_form_and_trailer_rejection_are_explicit() {
    let (mut message, sidecar) = project_request_head(
        Request::builder()
            .uri("/path")
            .body(())
            .unwrap()
            .into_parts()
            .0,
    )
    .unwrap();
    message.set_secure(true);
    let request = request_head_to_http(&message, sidecar).unwrap();
    assert_eq!(request.uri, "/path");
    assert!(
        request.extensions.is_empty(),
        "no identity is inferred from wire data"
    );
    message.set_trailers(Box::new(HttpHeaders::new()));
    assert!(matches!(
        request_head_to_http(&message, NetworkHeadContext::default()),
        Err(HttpAdapterError::TrailersUnsupported)
    ));
    let mut request = Request::new(());
    request
        .headers_mut()
        .insert("x-space", HeaderValue::from_static(" trimmed "));
    assert!(matches!(
        project_request_head(request.into_parts().0),
        Err(HttpAdapterError::HeaderBoundaryWhitespace(_))
    ));
}

#[test]
fn projected_chunked_heads_generate_chunked_h1_wire() {
    for response in [false, true] {
        let mut upstream = Http1xCodec::new(TransportDirection::Upstream);
        let mut downstream = Http1xCodec::new(TransportDirection::Downstream);
        let stream = upstream.create_stream();
        let mut callback = VersionHeads::default();
        let mut callback_egress = BufQueue::new(4096);
        if response {
            let request = BufQueue::from_bytes(
                Bytes::from_static(b"GET / HTTP/1.1\r\nHost: framing.test\r\n\r\n"),
                4096,
            )
            .unwrap();
            assert_eq!(
                downstream
                    .on_ingress(&request, &mut callback, &mut callback_egress)
                    .consumed,
                request.chain_length()
            );
        }
        let (message, _) = if response {
            project_response_head(
                Response::builder()
                    .header("transfer-encoding", "ChUnKeD")
                    .body(())
                    .unwrap()
                    .into_parts()
                    .0,
            )
        } else {
            project_request_head(
                Request::builder()
                    .method("PUT")
                    .uri("/upload")
                    .header("host", "framing.test")
                    .header("transfer-encoding", "ChUnKeD")
                    .body(())
                    .unwrap()
                    .into_parts()
                    .0,
            )
        }
        .unwrap();
        assert!(message.is_chunked());
        let sender = if response {
            &mut downstream
        } else {
            &mut upstream
        };
        let mut wire = BufQueue::new(4096);
        sender
            .generate(
                &mut wire,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .unwrap();
        sender
            .generate(
                &mut wire,
                EgressFrame::Body {
                    stream,
                    body: BufQueue::from_bytes(Bytes::from_static(b"hello"), 4096).unwrap(),
                    padding: None,
                    end_stream: true,
                },
                &mut callback,
            )
            .unwrap();
        let bytes = wire.segments().flatten().copied().collect::<Vec<_>>();
        let head_end = bytes
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        assert_eq!(&bytes[head_end + 4..], b"5\r\nhello\r\n0\r\n\r\n");
        assert!(
            bytes[..head_end]
                .windows(b"transfer-encoding: ChUnKeD".len())
                .any(|bytes| bytes.eq_ignore_ascii_case(b"transfer-encoding: chunked"))
        );
    }
}

#[test]
fn network_projection_rejects_ambiguous_or_unsupported_transfer_encoding() {
    for response in [false, true] {
        for (version, encodings, content_length) in [
            (Version::HTTP_11, vec!["gzip"], false),
            (Version::HTTP_11, vec!["gzip, chunked"], false),
            (Version::HTTP_11, vec!["chunked", "chunked"], false),
            (Version::HTTP_10, vec!["chunked"], false),
            (Version::HTTP_2, vec!["chunked"], false),
            (Version::HTTP_11, vec!["chunked"], true),
        ] {
            let mut headers = HeaderMap::new();
            for encoding in encodings {
                headers.append("transfer-encoding", HeaderValue::from_static(encoding));
            }
            if content_length {
                headers.insert("content-length", HeaderValue::from_static("5"));
            }
            let result = if response {
                let mut head = Response::new(());
                *head.version_mut() = version;
                *head.headers_mut() = headers;
                project_response_head(head.into_parts().0)
            } else {
                let mut head = Request::new(());
                *head.version_mut() = version;
                *head.headers_mut() = headers;
                project_request_head(head.into_parts().0)
            };
            if content_length {
                assert!(matches!(
                    result,
                    Err(HttpAdapterError::ConflictingBodyFraming)
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(HttpAdapterError::UnsupportedTransferEncoding)
                ));
            }
        }
    }
}

#[test]
fn bodyless_response_transfer_encoding_never_emits_a_chunk_terminator() {
    for status in [100, 101, 103, 204, 304] {
        let projected = project_response_head(
            Response::builder()
                .status(status)
                .header("transfer-encoding", "chunked")
                .body(())
                .unwrap()
                .into_parts()
                .0,
        );
        if status != 304 {
            assert!(matches!(
                projected,
                Err(HttpAdapterError::UnsupportedTransferEncoding)
            ));
            continue;
        }
        let (message, _) = projected.unwrap();
        assert!(!message.is_chunked());
        let mut codec = Http1xCodec::new(TransportDirection::Downstream);
        let mut callback = VersionHeads::default();
        let mut wire = BufQueue::new(4096);
        let request = BufQueue::from_bytes(
            Bytes::from_static(b"GET / HTTP/1.1\r\nHost: framing.test\r\n\r\n"),
            4096,
        )
        .unwrap();
        assert_eq!(
            codec
                .on_ingress(&request, &mut callback, &mut wire)
                .consumed,
            request.chain_length(),
        );
        codec
            .generate(
                &mut wire,
                EgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: &message,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut callback,
            )
            .unwrap();
        let bytes = wire.segments().flatten().copied().collect::<Vec<_>>();
        let head_end = bytes
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        assert_eq!(bytes.len(), head_end + 4, "304 must contain only its head");
    }
}

fn version_message(response: bool) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_http_version(1, 1);
    if response {
        message.set_status_code(200).unwrap();
    } else {
        message.set_method(HttpMethod::Get).unwrap();
        message.set_url(b"/version?source=codec", false).unwrap();
        message.headers_mut().add(b"host", b"version.test");
    }
    message.headers_mut().add(b"content-length", b"0");
    message.headers_mut().add(b"x-repeat", b"first");
    message.headers_mut().add(b"x-repeat", b"second");
    message
}

#[derive(Default)]
struct VersionHeads {
    messages: Vec<HttpMessage>,
    completions: usize,
}

impl HttpCodecCallback for VersionHeads {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::HeadersComplete { message, .. } => self.messages.push(*message),
            CodecEvent::MessageComplete { upgraded, .. } => {
                assert!(!upgraded);
                self.completions += 1;
            }
            CodecEvent::Error { error, .. } => panic!("real codec error: {error:?}"),
            CodecEvent::Body { body, .. } => assert!(body.is_empty()),
            _ => {}
        }
    }
}

fn version_codec(http2: bool, direction: TransportDirection) -> Box<dyn HttpCodec> {
    if http2 {
        Box::new(Http2Codec::new(direction))
    } else {
        Box::new(Http1xCodec::new(direction))
    }
}

fn codec_version_heads(http2: bool) -> [HttpMessage; 2] {
    let mut upstream = version_codec(http2, TransportDirection::Upstream);
    let mut downstream = version_codec(http2, TransportDirection::Downstream);
    let stream = upstream.create_stream();
    let mut decoded = Vec::new();
    for response in [false, true] {
        let (sender, receiver) = if response {
            (&mut downstream, &mut upstream)
        } else {
            (&mut upstream, &mut downstream)
        };
        let message = version_message(response);
        let mut wire = BufQueue::new(64 * 1024);
        let mut generated = VersionHeads::default();
        sender
            .generate(&mut wire, EgressFrame::ConnectionPreface, &mut generated)
            .unwrap();
        if http2 {
            sender
                .generate(
                    &mut wire,
                    EgressFrame::Settings { settings: &[] },
                    &mut generated,
                )
                .unwrap();
        }
        sender
            .generate(
                &mut wire,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: true,
                    extra_headers: None,
                },
                &mut generated,
            )
            .unwrap();
        let bytes = Bytes::from(wire.segments().flatten().copied().collect::<Vec<_>>());
        if !response {
            let prefix = if http2 {
                b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".as_slice()
            } else {
                b"GET /version?source=codec HTTP/1.1\r\n".as_slice()
            };
            assert!(bytes.starts_with(prefix));
        }
        let mut captured = VersionHeads::default();
        let mut callback_egress = BufQueue::new(64 * 1024);
        assert_eq!(
            receiver
                .on_ingress(&wire, &mut captured, &mut callback_egress)
                .consumed,
            bytes.len()
        );
        assert_eq!(captured.messages.len(), 1);
        assert_eq!(captured.completions, 1);
        decoded.push(captured.messages.pop().unwrap());
    }
    decoded.try_into().expect("one request and one response")
}

#[test]
fn real_h2_codec_heads_project_http2_without_changing_native_metadata() {
    for http2 in [false, true] {
        for mut message in codec_version_heads(http2) {
            let expected = if http2 {
                Version::HTTP_2
            } else {
                Version::HTTP_11
            };
            let protocol = http2.then_some(b"h2".as_slice());
            assert_eq!(message.http_version(), (1, 1));
            assert_eq!(message.advanced_protocol_string(), protocol);
            let identity = Arc::new(String::from("application-local version context"));
            let mut extensions = Extensions::new();
            extensions.insert(Arc::clone(&identity));
            let context = NetworkHeadContext::from_extensions(extensions);
            let (version, headers, extensions) = if message.is_request() {
                let head = request_head_to_http(&message, context).unwrap();
                assert_eq!(head.method, http::Method::GET);
                assert_eq!(head.uri, "/version?source=codec");
                (head.version, head.headers, head.extensions)
            } else {
                let head = response_head_to_http(&message, context).unwrap();
                assert_eq!(head.status, StatusCode::OK);
                (head.version, head.headers, head.extensions)
            };
            assert_eq!(version, expected);
            assert_eq!(headers["content-length"], "0");
            assert_eq!(
                headers
                    .get_all("x-repeat")
                    .iter()
                    .map(HeaderValue::as_bytes)
                    .collect::<Vec<_>>(),
                [b"first".as_slice(), b"second".as_slice()]
            );
            assert!(Arc::ptr_eq(
                extensions.get::<Arc<String>>().unwrap(),
                &identity
            ));
            assert_eq!(message.http_version(), (1, 1));
            assert_eq!(message.advanced_protocol_string(), protocol);
            assert_eq!(message.scheme(), b"http");
            if message.is_request() {
                assert_eq!(message.url().unwrap(), b"/version?source=codec");
            }
        }
    }
}

fn connect_message(authority: &[u8]) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_http_version(1, 1);
    message
        .set_method(HttpMethod::Connect)
        .expect("CONNECT method");
    message.headers_mut().add(b"host", authority);
    message.headers_mut().add(b"content-length", b"0");
    message.headers_mut().add(b"x-repeat", b"first");
    message.headers_mut().add(b"x-repeat", b"second");
    message
}

fn decoded_h2_connect(authority: &[u8]) -> HttpMessage {
    let mut upstream = version_codec(true, TransportDirection::Upstream);
    let mut downstream = version_codec(true, TransportDirection::Downstream);
    let stream = upstream.create_stream();
    let message = connect_message(authority);
    let mut wire = BufQueue::new(64 * 1024);
    let mut generated = VersionHeads::default();
    for frame in [
        EgressFrame::ConnectionPreface,
        EgressFrame::Settings { settings: &[] },
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: true,
            extra_headers: None,
        },
    ] {
        upstream
            .generate(&mut wire, frame, &mut generated)
            .expect("H2 request encoding");
    }
    let mut captured = VersionHeads::default();
    let mut callback_egress = BufQueue::new(64 * 1024);
    assert_eq!(
        downstream
            .on_ingress(&wire, &mut captured, &mut callback_egress)
            .consumed,
        wire.chain_length()
    );
    assert_eq!(captured.messages.len(), 1);
    assert_eq!(captured.completions, 1);
    captured.messages.pop().expect("one decoded CONNECT head")
}

#[test]
fn decoded_h2_connect_head_preserves_authority() {
    for authority in [
        "MiXeD.example.test:443",
        "[2001:db8::1]:443",
        "authority.test",
    ] {
        for secure in [false, true] {
            let mut message = decoded_h2_connect(authority.as_bytes());
            assert_eq!(
                message.method().expect("decoded method"),
                Some(HttpMethod::Connect)
            );
            assert_eq!(message.url().expect("decoded URL"), b"");
            assert_eq!(message.http_version(), (1, 1));
            assert_eq!(message.advanced_protocol_string(), Some(b"h2".as_slice()));
            assert_eq!(
                message.headers().get_single_or_empty(b"host"),
                authority.as_bytes()
            );
            assert!(!message.is_upgraded());
            assert!(message.upgrade_protocol().is_none());
            message.set_secure(secure);
            let original = format!("{message:?}");
            let identity = Arc::new(String::from("local CONNECT sidecar"));
            let mut extensions = Extensions::new();
            extensions.insert(Arc::clone(&identity));
            extensions.insert(Version::HTTP_11);
            let head =
                request_head_to_http(&message, NetworkHeadContext::from_extensions(extensions))
                    .expect("decoded basic CONNECT projection");
            assert_eq!(head.method, http::Method::CONNECT);
            assert_eq!(head.version, Version::HTTP_2);
            assert_eq!(head.uri.authority().expect("authority").as_str(), authority);
            assert_eq!(head.uri.to_string(), authority);
            assert!(head.uri.scheme().is_none());
            assert!(head.uri.path_and_query().is_none());
            assert_eq!(head.headers["host"].as_bytes(), authority.as_bytes());
            assert_eq!(
                head.headers
                    .get_all("x-repeat")
                    .iter()
                    .map(HeaderValue::as_bytes)
                    .collect::<Vec<_>>(),
                [b"first".as_slice(), b"second".as_slice()]
            );
            assert_eq!(head.extensions.len(), 2);
            assert!(Arc::ptr_eq(
                head.extensions.get::<Arc<String>>().expect("sidecar"),
                &identity
            ));
            assert_eq!(head.extensions.get::<Version>(), Some(&Version::HTTP_11));
            assert_eq!(format!("{message:?}"), original);
        }
    }
}

#[test]
fn connect_authority_projection_rejects_invalid_or_unrelated_heads() {
    let message = decoded_h2_connect(b"fixture.invalid:443");
    let project =
        |message: &HttpMessage| request_head_to_http(message, NetworkHeadContext::default());
    let mut missing = message.clone();
    missing.headers_mut().remove(b"host");
    assert!(matches!(
        project(&missing),
        Err(HttpAdapterError::InvalidUri(_))
    ));
    let mut duplicate = message.clone();
    duplicate.headers_mut().add(b"Host", b"fixture.invalid:443");
    assert!(matches!(
        project(&duplicate),
        Err(HttpAdapterError::InvalidUri(_))
    ));
    for authority in [
        b"".as_slice(),
        b"/path",
        b"https://fixture.invalid:443",
        b"fixture.invalid:443/path",
        b"fixture.invalid:443?query",
        b"fixture.invalid:443#fragment",
        b"[::1",
        b"fixture.invalid:443:80",
        b"fixture invalid:443",
        b"fixture\0.invalid:443",
        b"\xff.invalid:443",
    ] {
        let mut invalid = message.clone();
        invalid.headers_mut().set(b"host", authority);
        assert!(
            matches!(project(&invalid), Err(HttpAdapterError::InvalidUri(_))),
            "{authority:?}"
        );
    }
    for authority in [b":443".as_slice(), b"user@fixture.invalid:443"] {
        let mut invalid = message.clone();
        invalid.headers_mut().set(b"host", authority);
        assert!(matches!(
            project(&invalid),
            Err(HttpAdapterError::InvalidConnectAuthority)
        ));
    }
    let mut ordinary = message.clone();
    ordinary
        .set_method(HttpMethod::Get)
        .expect("ordinary method");
    assert!(matches!(
        project(&ordinary),
        Err(HttpAdapterError::InvalidUri(_))
    ));
    let mut h1 = connect_message(b"fixture.invalid:443");
    h1.headers_mut().add(b"alpn", b"h2");
    let mut extensions = Extensions::new();
    extensions.insert(Version::HTTP_2);
    assert!(matches!(
        request_head_to_http(&h1, NetworkHeadContext::from_extensions(extensions)),
        Err(HttpAdapterError::InvalidUri(_))
    ));
    for protocol in [b"".as_slice(), b"H2", b"h2c", b"h3", b"h2\0"] {
        let mut invalid = message.clone();
        invalid.set_advanced_protocol_string(protocol);
        assert!(matches!(
            project(&invalid),
            Err(HttpAdapterError::UnsupportedMessageMetadata(
                UnsupportedMessageMetadata::AdvancedProtocol
            ))
        ));
    }
    let mut invalid = message.clone();
    invalid.set_http_version(1, 0);
    assert!(matches!(
        project(&invalid),
        Err(HttpAdapterError::UnsupportedMessageMetadata(
            UnsupportedMessageMetadata::AdvancedProtocol
        ))
    ));
    invalid.set_http_version(3, 0);
    assert!(matches!(
        project(&invalid),
        Err(HttpAdapterError::UnsupportedRoxygenVersion { major: 3, minor: 0 })
    ));
    for metadata in [
        UnsupportedMessageMetadata::Upgraded,
        UnsupportedMessageMetadata::IngressWebSocketUpgrade,
        UnsupportedMessageMetadata::EgressWebSocketUpgrade,
        UnsupportedMessageMetadata::UpgradeProtocol,
        UnsupportedMessageMetadata::MasqueScheme,
    ] {
        let mut invalid = message.clone();
        match metadata {
            UnsupportedMessageMetadata::Upgraded => invalid.set_upgraded(true),
            UnsupportedMessageMetadata::IngressWebSocketUpgrade => {
                invalid.set_ingress_websocket_upgrade()
            }
            UnsupportedMessageMetadata::EgressWebSocketUpgrade => {
                invalid.set_egress_websocket_upgrade()
            }
            UnsupportedMessageMetadata::UpgradeProtocol => {
                invalid.set_upgrade_protocol(b"websocket")
            }
            UnsupportedMessageMetadata::MasqueScheme => invalid.set_masque(),
            _ => unreachable!(),
        }
        assert!(
            matches!(project(&invalid), Err(HttpAdapterError::UnsupportedMessageMetadata(actual)) if actual == metadata)
        );
    }
    let mut invalid = message.clone();
    invalid.set_trailers(Box::new(HttpHeaders::new()));
    assert!(matches!(
        project(&invalid),
        Err(HttpAdapterError::TrailersUnsupported)
    ));
    let mut invalid = message.clone();
    invalid.headers_mut().add(b"invalid header", b"value");
    assert!(matches!(
        project(&invalid),
        Err(HttpAdapterError::InvalidHeaderName(_))
    ));
    let mut invalid = message.clone();
    invalid.headers_mut().add(b"x-invalid", b"value\0suffix");
    assert!(matches!(
        project(&invalid),
        Err(HttpAdapterError::InvalidHeaderValue(_))
    ));
    for mut nonempty in [message, h1] {
        nonempty
            .set_url(b"existing.invalid:8443", false)
            .expect("existing target");
        assert_eq!(
            project(&nonempty).expect("nonempty target unchanged").uri,
            "existing.invalid:8443"
        );
    }
}

fn projected_version(message: &HttpMessage) -> Result<Version, HttpAdapterError> {
    if message.is_response() {
        response_head_to_http(message, NetworkHeadContext::default()).map(|head| head.version)
    } else {
        request_head_to_http(message, NetworkHeadContext::default()).map(|head| head.version)
    }
}

#[test]
fn network_protocol_metadata_rejects_contradictions() {
    for response in [false, true] {
        for numeric in [
            (0, 9),
            (1, 0),
            (1, 1),
            (2, 0),
            (0, 0),
            (1, 2),
            (2, 1),
            (3, 0),
            (255, 255),
        ] {
            for protocol in [
                None,
                Some(b"h2".as_slice()),
                Some(b""),
                Some(b"H2"),
                Some(b"h2c"),
                Some(b"h3"),
                Some(b"http/1.1"),
                Some(b"h2\0"),
            ] {
                let mut message = version_message(response);
                message.set_http_version(numeric.0, numeric.1);
                if let Some(protocol) = protocol {
                    message.set_advanced_protocol_string(protocol);
                }
                let result = projected_version(&message);
                let ordinary = match numeric {
                    (0, 9) => Some(Version::HTTP_09),
                    (1, 0) => Some(Version::HTTP_10),
                    (1, 1) => Some(Version::HTTP_11),
                    (2, 0) => Some(Version::HTTP_2),
                    _ => None,
                };
                match ordinary {
                    None => assert!(
                        matches!(result, Err(HttpAdapterError::UnsupportedRoxygenVersion { major, minor }) if (major, minor) == numeric)
                    ),
                    Some(ordinary) if protocol.is_none() => assert_eq!(result.unwrap(), ordinary),
                    Some(_)
                        if protocol == Some(b"h2".as_slice())
                            && matches!(numeric, (1, 1) | (2, 0)) =>
                    {
                        assert_eq!(result.unwrap(), Version::HTTP_2)
                    }
                    Some(_) => assert!(matches!(
                        result,
                        Err(HttpAdapterError::UnsupportedMessageMetadata(
                            UnsupportedMessageMetadata::AdvancedProtocol
                        ))
                    )),
                }
                assert_eq!(message.http_version(), numeric);
                assert_eq!(message.advanced_protocol_string(), protocol);
            }
        }
        let mut message = version_message(response);
        for name in [b"alpn".as_slice(), b"upgrade", b"advanced-protocol"] {
            message.headers_mut().add(name, b"h2");
        }
        let mut extensions = Extensions::new();
        extensions.insert(Version::HTTP_2);
        let context = NetworkHeadContext::from_extensions(extensions);
        let version = if response {
            response_head_to_http(&message, context).unwrap().version
        } else {
            request_head_to_http(&message, context).unwrap().version
        };
        assert_eq!(version, Version::HTTP_11);
        assert_eq!(message.advanced_protocol_string(), None);
        assert_eq!(message.scheme(), b"http");
    }
}

#[test]
fn h2_network_projection_preserves_validation_and_lossless_rejection() {
    for response in [false, true] {
        let mut message = version_message(response);
        message.set_advanced_protocol_string(b"h2");
        let wrong_kind = if response {
            request_head_to_http(&message, NetworkHeadContext::default()).unwrap_err()
        } else {
            response_head_to_http(&message, NetworkHeadContext::default()).unwrap_err()
        };
        assert!(matches!(
            (response, wrong_kind),
            (true, HttpAdapterError::ExpectedRequest) | (false, HttpAdapterError::ExpectedResponse)
        ));
        for metadata in [
            UnsupportedMessageMetadata::Upgraded,
            UnsupportedMessageMetadata::IngressWebSocketUpgrade,
            UnsupportedMessageMetadata::EgressWebSocketUpgrade,
            UnsupportedMessageMetadata::UpgradeProtocol,
            UnsupportedMessageMetadata::MasqueScheme,
        ] {
            let mut invalid = message.clone();
            match metadata {
                UnsupportedMessageMetadata::Upgraded => invalid.set_upgraded(true),
                UnsupportedMessageMetadata::IngressWebSocketUpgrade => {
                    invalid.set_ingress_websocket_upgrade()
                }
                UnsupportedMessageMetadata::EgressWebSocketUpgrade => {
                    invalid.set_egress_websocket_upgrade()
                }
                UnsupportedMessageMetadata::UpgradeProtocol => {
                    invalid.set_upgrade_protocol(b"websocket")
                }
                UnsupportedMessageMetadata::MasqueScheme => invalid.set_masque(),
                _ => unreachable!(),
            }
            assert!(
                matches!(projected_version(&invalid), Err(HttpAdapterError::UnsupportedMessageMetadata(actual)) if actual == metadata)
            );
        }
        let mut invalid = message.clone();
        invalid.set_trailers(Box::new(HttpHeaders::new()));
        assert!(matches!(
            projected_version(&invalid),
            Err(HttpAdapterError::TrailersUnsupported)
        ));
        let mut invalid = message.clone();
        invalid.headers_mut().add(b"invalid header", b"value");
        assert!(matches!(
            projected_version(&invalid),
            Err(HttpAdapterError::InvalidHeaderName(_))
        ));
        let mut invalid = message.clone();
        invalid.headers_mut().add(b"x-invalid", b"value\0suffix");
        assert!(matches!(
            projected_version(&invalid),
            Err(HttpAdapterError::InvalidHeaderValue(_))
        ));
        let lossless = if response {
            Response::<()>::try_from(RoxygenResponse::try_from_parts(message, ()).unwrap())
                .unwrap_err()
        } else {
            Request::<()>::try_from(RoxygenRequest::try_from_parts(message, ()).unwrap())
                .unwrap_err()
        };
        assert!(matches!(
            lossless,
            HttpAdapterError::UnsupportedMessageMetadata(
                UnsupportedMessageMetadata::AdvancedProtocol
            )
        ));
    }
    let mut invalid = version_message(true);
    invalid.set_advanced_protocol_string(b"h2");
    invalid.set_status_code(99).unwrap();
    assert!(matches!(
        projected_version(&invalid),
        Err(HttpAdapterError::InvalidStatusCode(_))
    ));
    let mut invalid = version_message(false);
    invalid.set_advanced_protocol_string(b"h2");
    invalid.set_method(String::from("invalid method")).unwrap();
    assert!(matches!(
        projected_version(&invalid),
        Err(HttpAdapterError::InvalidMethod(_))
    ));
    let mut invalid = version_message(false);
    invalid.set_advanced_protocol_string(b"h2");
    invalid.set_url(b"/invalid\0uri", false).unwrap();
    assert!(matches!(
        projected_version(&invalid),
        Err(HttpAdapterError::InvalidUri(_))
    ));
    let mut invalid = version_message(false);
    invalid.set_advanced_protocol_string(b"h2");
    invalid
        .set_url(b"https://version.test/path", false)
        .unwrap();
    invalid.set_secure(false);
    assert!(matches!(
        projected_version(&invalid),
        Err(HttpAdapterError::UnsupportedMessageMetadata(
            UnsupportedMessageMetadata::UriSchemeMismatch
        ))
    ));
    invalid.set_url(b"ftp://version.test/path", false).unwrap();
    assert!(
        matches!(projected_version(&invalid), Err(HttpAdapterError::UnsupportedUriScheme(scheme)) if scheme.as_ref() == "ftp")
    );
    for response in [false, true] {
        let identity = Arc::new(7_u64);
        let mut header = HeaderValue::from_static("sensitive");
        header.set_sensitive(true);
        let (mut message, context) = if response {
            let mut head = Response::new(());
            head.headers_mut().append("x-sensitive", header);
            head.extensions_mut().insert(Arc::clone(&identity));
            project_response_head(head.into_parts().0).unwrap()
        } else {
            let mut head = Request::new(());
            head.headers_mut().append("x-sensitive", header);
            head.extensions_mut().insert(Arc::clone(&identity));
            project_request_head(head.into_parts().0).unwrap()
        };
        message.set_advanced_protocol_string(b"h2");
        let (version, headers, extensions) = if response {
            let head = response_head_to_http(&message, context).unwrap();
            (head.version, head.headers, head.extensions)
        } else {
            let head = request_head_to_http(&message, context).unwrap();
            (head.version, head.headers, head.extensions)
        };
        assert_eq!(version, Version::HTTP_2);
        assert!(headers["x-sensitive"].is_sensitive());
        assert!(Arc::ptr_eq(
            extensions.get::<Arc<u64>>().unwrap(),
            &identity
        ));
    }
}
