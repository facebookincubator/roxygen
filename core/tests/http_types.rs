use crate::http::CodecProtocol;
use crate::http::HttpHeaderSize;
use crate::http::HttpMethod;
use crate::http::Method;
use crate::http::TransportDirection;
use crate::ids::MAX_STREAM_ID;
use crate::ids::NO_STREAM;
use crate::ids::StreamId;

#[test]
fn method_round_trip() {
    let mut method = Method::from(HttpMethod::Get);
    assert_eq!(method.as_str(), "GET");
    assert_eq!(method.recognized(), Some(HttpMethod::Get));

    method = Method::from("FOO");
    assert_eq!(method.as_str(), "FOO");
    assert_eq!(method.recognized(), None);

    method = Method::from(HttpMethod::Connect);
    assert_eq!(method.as_str(), "CONNECT");
    assert_eq!(method.recognized(), Some(HttpMethod::Connect));
}

#[test]
fn recognized_methods_are_ascii_case_insensitive_and_render_canonically() {
    for method in HttpMethod::ALL {
        assert_eq!(HttpMethod::from_bytes(method.as_bytes()), Some(method));
        assert_eq!(method.to_string(), method.as_str());
    }

    let lower = Method::from("patch");
    assert_eq!(lower.as_str(), "PATCH");
    assert_eq!(lower.recognized(), Some(HttpMethod::Patch));
    let extension = Method::from("fOo");
    assert_eq!(extension.as_str(), "FOO");
    assert_eq!(extension.recognized(), None);
    assert_eq!(HttpMethod::from_bytes(b"not-a-method"), None);
}

#[test]
fn method_constructors_preserve_value_and_hash_consistency() {
    use std::cell::Cell;
    use std::collections::HashSet;

    struct CountedName<'a> {
        value: &'a str,
        conversions: &'a Cell<usize>,
    }
    impl From<CountedName<'_>> for Box<str> {
        fn from(name: CountedName<'_>) -> Self {
            name.conversions.set(name.conversions.get() + 1);
            name.value.into()
        }
    }

    let mut distinct = HashSet::new();
    for (input, expected, recognized) in [
        ("gEt", "GET", Some(HttpMethod::Get)),
        ("pOsT", "POST", Some(HttpMethod::Post)),
        ("oPtIoNs", "OPTIONS", Some(HttpMethod::Options)),
        ("dElEtE", "DELETE", Some(HttpMethod::Delete)),
        ("hEaD", "HEAD", Some(HttpMethod::Head)),
        ("cOnNeCt", "CONNECT", Some(HttpMethod::Connect)),
        ("pUt", "PUT", Some(HttpMethod::Put)),
        ("tRaCe", "TRACE", Some(HttpMethod::Trace)),
        ("pAtCh", "PATCH", Some(HttpMethod::Patch)),
        ("sUb", "SUB", Some(HttpMethod::Sub)),
        ("pUb", "PUB", Some(HttpMethod::Pub)),
        ("uNsUb", "UNSUB", Some(HttpMethod::Unsub)),
        ("fOo", "FOO", None),
        ("", "", None),
        ("méthod-ß", "MéTHOD-ß", None),
    ] {
        let conversions = Cell::new(0);
        let values = [
            Method::from(input),
            Method::from(input.to_owned()),
            Method::from_name(input.to_owned().into_boxed_str()),
            Method::from_name(CountedName {
                value: input,
                conversions: &conversions,
            }),
        ];
        assert_eq!(conversions.get(), 1);
        let canonical = Method::from(expected);
        assert!(distinct.insert(canonical.clone()));
        let mut equivalent = HashSet::from([canonical.clone()]);
        for method in values.into_iter().chain(recognized.map(Method::from)) {
            assert_eq!(method.as_str(), expected);
            assert_eq!(method.as_ref(), expected);
            assert_eq!(method.to_string(), expected);
            assert_eq!(method.recognized(), recognized);
            assert_eq!(method, canonical);
            let cloned = method.clone();
            drop(method);
            assert_eq!(cloned.as_str(), expected);
            assert_eq!(cloned, canonical);
            assert!(!equivalent.insert(cloned));
        }
        assert_eq!(equivalent.len(), 1);
    }
    assert_eq!(distinct.len(), 15);
}

#[test]
fn exact_method_tokens_are_case_sensitive_and_kept_byte_for_byte() {
    for (token, expected, recognized) in [
        ("POST", "POST", Some(HttpMethod::Post)),
        ("GET", "GET", Some(HttpMethod::Get)),
        ("UNSUB", "UNSUB", Some(HttpMethod::Unsub)),
        ("post", "post", None),
        ("Post", "Post", None),
        ("pOsT", "pOsT", None),
        ("PURGE", "PURGE", None),
        ("purge", "purge", None),
        ("M-SEARCH", "M-SEARCH", None),
        ("!#$%&'*+-.^_`|~09azAZ", "!#$%&'*+-.^_`|~09azAZ", None),
    ] {
        let method = Method::from_exact_token(token.as_bytes()).expect("a token");
        assert_eq!(method.as_str(), expected);
        assert_eq!(method.recognized(), recognized);
    }
    assert_ne!(
        Method::from_exact_token(b"post"),
        Some(Method::from(HttpMethod::Post))
    );
    assert_eq!(
        Method::from_exact_token(b"POST"),
        Some(Method::from(HttpMethod::Post))
    );
    for invalid in [
        b"".as_slice(),
        b"PO ST",
        b"POST\t",
        b"GET\0",
        b"(GET)",
        b"GE/T",
        b"GET\"",
        b"GET,",
        b"M\xc3\xa9THOD",
        b"\x7f",
    ] {
        assert_eq!(Method::from_exact_token(invalid), None, "{invalid:?}");
    }
}

#[test]
fn header_size_fields_default_to_zero_and_remain_independent() {
    assert_eq!(
        HttpHeaderSize::default(),
        HttpHeaderSize {
            compressed: 0,
            uncompressed: 0,
            compressed_block: 0,
        }
    );

    let size = HttpHeaderSize {
        compressed: 31,
        uncompressed: 71,
        compressed_block: 29,
    };
    assert_eq!(size.compressed, 31);
    assert_eq!(size.uncompressed, 71);
    assert_eq!(size.compressed_block, 29);
}

#[test]
fn codec_protocol_discriminants_match_the_reference_enum() {
    assert_eq!(CodecProtocol::Http11 as u8, 0);
    assert_eq!(CodecProtocol::Http2 as u8, 1);
    assert_eq!(CodecProtocol::Hq as u8, 2);
    assert_eq!(CodecProtocol::Http3 as u8, 3);
    assert_eq!(CodecProtocol::HttpBinary as u8, 4);
    assert_eq!(CodecProtocol::TunnelLite as u8, 5);
    assert_eq!(CodecProtocol::TunnelConnect as u8, 6);
}

#[test]
fn stream_sentinels_preserve_no_stream_zero_and_max_distinctions() {
    assert_eq!(NO_STREAM, None);
    assert_eq!(StreamId::UNSPECIFIED.get(), 0);
    assert!(StreamId::UNSPECIFIED.is_unspecified());
    assert_eq!(MAX_STREAM_ID.get(), u64::MAX);
    assert!(!MAX_STREAM_ID.is_unspecified());
}

#[test]
fn transport_direction_predicates_and_negation_are_exact() {
    assert_eq!(TransportDirection::Downstream as u8, 0);
    assert_eq!(TransportDirection::Upstream as u8, 1);
    assert!(TransportDirection::Downstream.is_downstream());
    assert!(!TransportDirection::Downstream.is_upstream());
    assert!(TransportDirection::Upstream.is_upstream());
    assert!(!TransportDirection::Upstream.is_downstream());
    assert_eq!(
        !TransportDirection::Downstream,
        TransportDirection::Upstream
    );
    assert_eq!(
        !TransportDirection::Upstream,
        TransportDirection::Downstream
    );
}

#[test]
fn public_api_uses_roxygen_newtypes_only() {
    fn assert_owned_value<T: Clone + std::fmt::Debug + Eq + Send + Sync + 'static>() {}

    assert_owned_value::<HttpHeaderSize>();
    assert_owned_value::<HttpMethod>();
    assert_owned_value::<Method>();
    assert_owned_value::<CodecProtocol>();
    assert_owned_value::<TransportDirection>();
    assert_owned_value::<StreamId>();

    let method = Method::from(HttpMethod::Get);
    let _: &str = method.as_str();
    let _: Option<StreamId> = NO_STREAM;
    let _: u64 = MAX_STREAM_ID.get();
}
