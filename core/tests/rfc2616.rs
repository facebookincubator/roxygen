use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::http::HttpMethod;
use crate::rfc2616::BodyAllowed;
use crate::rfc2616::ByteRangeSpec;
use crate::rfc2616::accepts_encoding;
use crate::rfc2616::body_implied;
use crate::rfc2616::is_request_body_allowed;
use crate::rfc2616::parse_byte_range_spec;
use crate::rfc2616::parse_encoding;
use crate::rfc2616::parse_qvalues;
use crate::rfc2616::response_body_must_be_empty;

fn assert_qvalues(input: &[u8], expected: &[(&[u8], f64)]) {
    let mut output = Vec::new();
    assert!(parse_qvalues(input, &mut output));
    assert_eq!(output.len(), expected.len());
    for ((actual_token, actual_qvalue), (expected_token, expected_qvalue)) in
        output.iter().zip(expected)
    {
        assert_eq!(actual_token.as_ref(), *expected_token);
        assert_eq!(*actual_qvalue, *expected_qvalue);
    }
}

#[test]
fn qvalue_test_basic() {
    assert_qvalues(
        b"iso-8859-5, unicode-1-1;q=0.8",
        &[(b"iso-8859-5", 1.0), (b"unicode-1-1", 0.8)],
    );
    assert_qvalues(b"compress, gzip", &[(b"compress", 1.0), (b"gzip", 1.0)]);

    let mut output = Vec::new();
    assert!(!parse_qvalues(b"", &mut output));
    assert!(output.is_empty());
    assert!(!parse_qvalues(b" ", &mut output));
    assert!(output.is_empty());

    assert_qvalues(
        b"compress;q=0.5, gzip;q=1.0",
        &[(b"compress", 0.5), (b"gzip", 1.0)],
    );
    assert_qvalues(
        b"gzip;q=1.0, identity; q=0.5, *;q=0",
        &[(b"gzip", 1.0), (b"identity", 0.5), (b"*", 0.0)],
    );
    assert_qvalues(
        b"da, en-gb;q=0.8, en;q=0.7",
        &[(b"da", 1.0), (b"en-gb", 0.8), (b"en", 0.7)],
    );
}

#[test]
fn qvalue_test_extras() {
    assert_qvalues(b"gzip", &[(b"gzip", 1.0)]);
    assert_qvalues(b"gzip;q=\x0b0.5\x0c", &[(b"gzip", 0.5)]);
}

#[test]
fn qvalue_test_invalids() {
    let mut output = Vec::new();
    assert!(!parse_qvalues(b",,,", &mut output));
    assert!(output.is_empty());

    assert!(!parse_qvalues(b"  ; q=0.1", &mut output));
    assert!(output.is_empty());

    assert!(!parse_qvalues(b"gzip; q=uietplease", &mut output));
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].0.as_ref(), b"gzip");
    assert_eq!(output[0].1, 1.0);
}

#[test]
fn parse_encoding_test_simple() {
    let encodings = parse_encoding(b"zstd;q=1.0;wl=20,gzip;q=0.0").unwrap();
    assert_eq!(
        encodings,
        vec![
            (
                Box::from(&b"zstd"[..]),
                vec![
                    (Box::from(&b"q"[..]), Box::from(&b"1.0"[..])),
                    (Box::from(&b"wl"[..]), Box::from(&b"20"[..])),
                ],
            ),
            (
                Box::from(&b"gzip"[..]),
                vec![(Box::from(&b"q"[..]), Box::from(&b"0.0"[..]))],
            ),
        ],
    );
}

#[test]
fn parse_encoding_test_whitespace() {
    let encodings = parse_encoding(b"zstd ; q =\t1.0 ; wl = 20 , gzip ;\t q = 0.0").unwrap();
    assert_eq!(encodings[0].0.as_ref(), b"zstd");
    assert_eq!(encodings[0].1[0].0.as_ref(), b"q");
    assert_eq!(encodings[0].1[0].1.as_ref(), b"1.0");
    assert_eq!(encodings[0].1[1].0.as_ref(), b"wl");
    assert_eq!(encodings[0].1[1].1.as_ref(), b"20");
    assert_eq!(encodings[1].0.as_ref(), b"gzip");
    assert_eq!(encodings[1].1[0].0.as_ref(), b"q");
    assert_eq!(encodings[1].1[0].1.as_ref(), b"0.0");
}

#[test]
fn parse_encoding_test_invalid() {
    for invalid in [
        b"".as_slice(),
        b" ",
        b",",
        b" ,",
        b", ",
        b" , ",
        b" , , ",
        b";",
        b" ;",
        b"; ",
        b" ; ",
        b" ; ; ",
        b" ; ; , ; ;",
        b"zstd;=,gzip",
        b"zstd;=wat,gzip",
    ] {
        assert!(parse_encoding(invalid).is_err(), "accepted {invalid:?}");
    }
}

#[test]
fn accept_encoding_test_simple() {
    assert!(accepts_encoding(b"zstd", b"zstd"));
    assert!(accepts_encoding(b"zstd, gzip", b"zstd"));
    assert!(!accepts_encoding(b"gzip", b"zstd"));
    assert!(!accepts_encoding(b"*", b"zstd"));
}

#[test]
fn accept_encoding_test_qvalues() {
    assert!(accepts_encoding(b"zstd", b"zstd"));
    assert!(accepts_encoding(b"zstd;q=1.0", b"zstd"));
    assert!(accepts_encoding(b"zstd;q=wat", b"zstd"));
    assert!(!accepts_encoding(b"zstd;q=0.0", b"zstd"));
}

#[test]
fn accept_encoding_test_whitespace() {
    assert!(accepts_encoding(b" zstd ", b"zstd"));
    assert!(accepts_encoding(b" zstd ,", b"zstd"));
    assert!(accepts_encoding(b"gzip, br \t, \tzstd ", b"zstd"));
    assert!(!accepts_encoding(b"gzip, \tdeflate, br \t ", b"zstd"));
    assert!(!accepts_encoding(b"zstd;q=0.0", b"zstd"));
    assert!(!accepts_encoding(b"zstd; q = 0.0 ", b"zstd"));
    assert!(accepts_encoding(b"zstd;q=\x0b0.5\x0c", b"zstd"));
    assert!(!accepts_encoding(b"zstd;q=\x0b0.0\x0c", b"zstd"));
}

#[test]
fn byte_range_spec_test_valids() {
    assert_eq!(
        parse_byte_range_spec(b"bytes 0-10/100"),
        Some(ByteRangeSpec {
            first_byte: 0,
            last_byte: 10,
            instance_length: 100,
        }),
    );
    assert_eq!(
        parse_byte_range_spec(b"bytes */100"),
        Some(ByteRangeSpec {
            first_byte: 0,
            last_byte: u64::MAX,
            instance_length: 100,
        }),
    );
    assert_eq!(
        parse_byte_range_spec(b"bytes 0-10/*"),
        Some(ByteRangeSpec {
            first_byte: 0,
            last_byte: 10,
            instance_length: u64::MAX,
        }),
    );
}

#[test]
fn byte_range_spec_test_invalids() {
    for invalid in [
        b"0-10/100".as_slice(),
        b"bytes 10/100",
        b"bytes 10-/100",
        b"bytes 0-10 100",
        b"bytes 0-10/100Q",
        b"bytes 10-1/100",
        b"bytes 10-90/50",
        b"bytes x/100",
        b"bytes 0-x/100",
        b"bytes *-10/100",
        b"bytes 0-*/100",
    ] {
        assert!(
            parse_byte_range_spec(invalid).is_none(),
            "accepted {invalid:?}"
        );
    }

    let spec = b"bytes 0-10/100";
    for end in [
        spec.len() - 3,
        spec.len() - 4,
        spec.len() - 6,
        spec.len() - 7,
        spec.len() - 9,
    ] {
        assert!(parse_byte_range_spec(&spec[..end]).is_none());
    }
}

#[test]
fn body_policy_and_framing_boundaries() {
    assert_eq!(
        is_request_body_allowed(Some(HttpMethod::Trace)),
        BodyAllowed::NotAllowed
    );
    for method in [HttpMethod::Options, HttpMethod::Post, HttpMethod::Put] {
        assert_eq!(is_request_body_allowed(Some(method)), BodyAllowed::Defined);
    }
    assert_eq!(
        is_request_body_allowed(Some(HttpMethod::Get)),
        BodyAllowed::NotDefined
    );
    assert_eq!(is_request_body_allowed(None), BodyAllowed::NotDefined);

    for status in [100, 101, 199, 204, 304] {
        assert!(response_body_must_be_empty(status));
    }
    for status in [99, 200, 203, 205, 303, 305] {
        assert!(!response_body_must_be_empty(status));
    }

    let mut headers = HttpHeaders::new();
    assert!(!body_implied(&headers));
    headers.add(HttpHeaderCode::ContentLength, b"0");
    assert!(body_implied(&headers));
    headers.remove(HttpHeaderCode::ContentLength);
    headers.add(HttpHeaderCode::TransferEncoding, b"chunked");
    assert!(body_implied(&headers));
}

#[test]
fn byte_range_preserves_strtoul_edges() {
    assert_eq!(
        parse_byte_range_spec(b"bytes \x0b0-10/100"),
        Some(ByteRangeSpec {
            first_byte: 0,
            last_byte: 10,
            instance_length: 100,
        }),
    );
    assert_eq!(
        parse_byte_range_spec(b"bytes +0-+10/+100\0ignored"),
        Some(ByteRangeSpec {
            first_byte: 0,
            last_byte: 10,
            instance_length: 100,
        }),
    );
    assert!(parse_byte_range_spec(b"bytes 18446744073709551616-18446744073709551616/*").is_some());
}
