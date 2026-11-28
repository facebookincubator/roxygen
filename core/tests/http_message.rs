/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;
use std::time::Instant;

use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::http::HttpHeaderSize;
use crate::http::HttpMethod;
use crate::message::HttpMessage;
use crate::message::MessageTypeError;
use crate::message::SocketAddress;
use crate::priority::HttpPriority;

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[test]
fn immutable_projections_preserve_message_kind_and_borrow_owned_fields() {
    let mut message = HttpMessage::new();
    assert_eq!(message.request_method(), None);
    assert_eq!(message.request_url(), None);
    assert_eq!(message.response_status(), None);
    message.set_method(HttpMethod::Post).unwrap();
    message.set_url(b"/upload?part=2", true).unwrap();
    assert_eq!(message.request_method(), Some("POST"));
    assert_eq!(message.request_url(), Some(b"/upload?part=2".as_slice()));
    assert_eq!(message.response_status(), None);
    assert_eq!(
        message.set_status_code(201),
        Err(MessageTypeError::ResponseApiOnRequest)
    );

    let mut response = HttpMessage::new();
    assert_eq!(response.request_method(), None);
    response.set_status_code(201).unwrap();
    assert_eq!(response.response_status(), Some(201));
    assert_eq!(response.request_method(), None);
    assert_eq!(response.request_url(), None);
}

#[test]
fn response_status_message_reads_without_selecting_a_kind() {
    let kindless = HttpMessage::new();
    assert_eq!(kindless.response_status_message(), None);
    assert!(!kindless.is_request());
    assert!(!kindless.is_response());

    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Get).unwrap();
    assert_eq!(request.response_status_message(), None);
    assert!(request.is_request());

    let mut response = HttpMessage::new();
    response.set_status_code(404).unwrap();
    assert_eq!(response.response_status_message(), Some(b"".as_slice()));
    response.set_status_message(b"Gone Fishing").unwrap();
    assert_eq!(
        response.response_status_message(),
        Some(b"Gone Fishing".as_slice())
    );
    response.set_status_message(b"Not Found").unwrap();
    assert_eq!(
        response.response_status_message(),
        Some(b"Not Found".as_slice())
    );
    assert!(response.is_response());
}

#[test]
fn response_status_display_preserves_unset_and_full_u16_codes() {
    for select_by_reason in [false, true] {
        let mut response = HttpMessage::new();
        if select_by_reason {
            response.set_status_message(b"reason").unwrap();
        } else {
            assert_eq!(response.status_code().unwrap(), 0);
        }
        assert_eq!(response.response_status(), Some(0));
        let display = response.to_string();
        assert!(!display.lines().any(|line| line.starts_with(" status:")));
        assert_eq!(response.clone().to_string(), display);

        response.set_status_code(0).unwrap();
        let display = response.to_string();
        assert!(
            display
                .lines()
                .filter(|line| line.starts_with(" status:"))
                .eq([" status:0"])
        );
        assert_eq!(response.clone().to_string(), display);
    }

    let mut response = HttpMessage::new();
    for status in [0, 9, 10, 99, 100, 999, 1000, 9999, 10000, u16::MAX, 1, 0] {
        response.set_status_code(status).unwrap();
        assert_eq!(response.response_status(), Some(status));
        assert_eq!(response.status_code().unwrap(), status);
        assert_eq!(response.status_message().unwrap(), b"");
        let display = response.to_string();
        let expected = format!(" status:{status}");
        assert!(
            display
                .lines()
                .filter(|line| line.starts_with(" status:"))
                .eq([expected.as_str()])
        );
    }
}

#[test]
fn response_reason_storage_preserves_bytes_and_setter_order() {
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;

    struct ReasonInput<'a> {
        calls: &'a Cell<usize>,
        panics: bool,
    }

    impl AsRef<[u8]> for ReasonInput<'_> {
        fn as_ref(&self) -> &[u8] {
            self.calls.set(self.calls.get() + 1);
            if self.panics {
                panic_any("reason input panic");
            }
            b"probe"
        }
    }

    for reason in [
        b"OK".as_slice(),
        b"Not Found",
        b"",
        b"custom\0\r\n\t\x7f\xff",
    ] {
        let mut input = reason.to_vec();
        let mut response = HttpMessage::new();
        response.set_status_code(200).unwrap();
        response.set_status_message(&input).unwrap();
        assert_eq!(response.response_status(), Some(200));
        input.fill(b'x');
        drop(input);
        let mut cloned = response.clone();
        response.set_status_code(404).unwrap();
        assert_eq!(response.status_message().unwrap(), reason);
        response.set_status_message(b"Not Found").unwrap();
        assert_eq!(cloned.response_status(), Some(200));
        assert_eq!(cloned.status_message().unwrap(), reason);
        cloned.set_status_code(0).unwrap();
        assert_eq!(cloned.status_message().unwrap(), reason);
        assert_eq!(response.status_message().unwrap(), b"Not Found");
        assert_eq!(response.response_status(), Some(404));
    }

    let calls = Cell::new(0);
    let mut response = HttpMessage::new();
    response
        .set_status_message(ReasonInput {
            calls: &calls,
            panics: false,
        })
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(response.response_status(), Some(0));
    assert_eq!(response.status_message().unwrap(), b"probe");

    calls.set(0);
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Post).unwrap();
    request.set_url(b"/retained", false).unwrap();
    assert_eq!(
        request.set_status_message(ReasonInput {
            calls: &calls,
            panics: false
        }),
        Err(MessageTypeError::ResponseApiOnRequest)
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(request.request_method(), Some("POST"));
    assert_eq!(request.request_url(), Some(b"/retained".as_slice()));
    assert_eq!(request.response_status(), None);

    for already_response in [false, true] {
        let mut message = HttpMessage::new();
        if already_response {
            message.set_status_code(201).unwrap();
            message.set_status_message(b"prior reason").unwrap();
        }
        calls.set(0);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            message.set_status_message(ReasonInput {
                calls: &calls,
                panics: true,
            })
        }))
        .expect_err("input coercion must panic before changing the message");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"reason input panic"));
        assert_eq!(calls.get(), 1);
        if already_response {
            assert_eq!(message.response_status(), Some(201));
            assert_eq!(message.status_message().unwrap(), b"prior reason");
        } else {
            assert_eq!(message.response_status(), None);
            assert_eq!(message.request_method(), None);
            message.set_method(HttpMethod::Get).unwrap();
            assert_eq!(message.request_method(), Some("GET"));
        }
    }
}

#[test]
fn httpmessage_test_parse_cookies_simple() {
    let mut message = HttpMessage::new();
    message
        .headers_mut()
        .add(HttpHeaderCode::Cookie, b"id=1256679245; data=0:1234567");
    assert_eq!(message.cookie(b"id").as_ref(), b"1256679245");
    assert_eq!(message.cookie(b"data").as_ref(), b"0:1234567");
    assert_eq!(message.cookie(b"mising").as_ref(), b"");
}

#[test]
fn httpmessage_test_parse_cookies_spaces() {
    let mut message = HttpMessage::new();
    message.headers_mut().add(
        HttpHeaderCode::Cookie,
        b" id=1256679245 ;   data=0:1234567  ;",
    );
    assert_eq!(message.cookie(b"id").as_ref(), b"1256679245");
    assert_eq!(message.cookie(b"data").as_ref(), b"0:1234567");
}

#[test]
fn httpmessage_test_parse_cookies_single_cookie() {
    let mut message = HttpMessage::new();
    message
        .headers_mut()
        .add(HttpHeaderCode::Cookie, b"   user_id=1256679245  ");
    assert_eq!(message.cookie(b"user_id").as_ref(), b"1256679245");
}

#[test]
fn httpmessage_test_parse_cookies_multiple_cookies() {
    let mut message = HttpMessage::new();
    message.headers_mut().add(
        HttpHeaderCode::Cookie,
        b"id=1256679245; data=0:1234567; same=Always; Name",
    );
    message.headers_mut().add(
        HttpHeaderCode::Cookie,
        b"id2=1256679245; data2=0:1234567; same=Always; ",
    );
    assert_eq!(message.cookie(b"id").as_ref(), b"1256679245");
    assert_eq!(message.cookie(b"id2").as_ref(), b"1256679245");
    assert_eq!(message.cookie(b"data").as_ref(), b"0:1234567");
    assert_eq!(message.cookie(b"data2").as_ref(), b"0:1234567");
    assert_eq!(message.cookie(b"same").as_ref(), b"Always");
    assert_eq!(message.cookie(b"Name").as_ref(), b"");
}

#[test]
fn httpmessage_test_remove_cookie() {
    let mut message = HttpMessage::new();
    message.headers_mut().set(
        HttpHeaderCode::Cookie,
        b"id=1256679245; data=0:1234567; session=abc123",
    );
    assert_eq!(message.cookie(b"id").as_ref(), b"1256679245");
    assert_eq!(message.cookie(b"data").as_ref(), b"0:1234567");
    assert_eq!(message.cookie(b"session").as_ref(), b"abc123");

    message.remove_cookie(b"data");
    assert_eq!(message.cookie(b"id").as_ref(), b"1256679245");
    assert_eq!(message.cookie(b"data").as_ref(), b"");
    assert_eq!(message.cookie(b"session").as_ref(), b"abc123");
    let cookie = message
        .headers()
        .get_single_or_empty(HttpHeaderCode::Cookie);
    assert!(contains(cookie, b"id=1256679245"));
    assert!(contains(cookie, b"session=abc123"));
    assert!(!contains(cookie, b"data=0:1234567"));

    message.remove_cookie(b"id");
    assert_eq!(message.cookie(b"id").as_ref(), b"");
    assert_eq!(message.cookie(b"session").as_ref(), b"abc123");

    message.remove_cookie(b"session");
    assert_eq!(message.cookie(b"session").as_ref(), b"");
    assert!(!message.headers().exists(HttpHeaderCode::Cookie));
}

#[test]
fn httpmessage_test_remove_cookie_non_existent() {
    let mut message = HttpMessage::new();
    message
        .headers_mut()
        .set(HttpHeaderCode::Cookie, b"id=1256679245; data=0:1234567");
    message.remove_cookie(b"nonexistent");
    assert_eq!(message.cookie(b"id").as_ref(), b"1256679245");
    assert_eq!(message.cookie(b"data").as_ref(), b"0:1234567");
    let cookie = message
        .headers()
        .get_single_or_empty(HttpHeaderCode::Cookie);
    assert!(contains(cookie, b"id=1256679245"));
    assert!(contains(cookie, b"data=0:1234567"));
}

#[test]
fn httpmessage_test_remove_cookie_empty_header() {
    let mut message = HttpMessage::new();
    message.remove_cookie(b"nonexistent");
    assert!(!message.headers().exists(HttpHeaderCode::Cookie));
    assert_eq!(message.cookie(b"nonexistent").as_ref(), b"");
}

#[test]
fn httpmessage_test_parse_query_params_simple() {
    let mut message = HttpMessage::new();
    message
        .set_url(
            b"/test?seq=123456&userid=1256679245&dup=1&dup=2&helloWorld&second=was+it+clear+%28already%29%3F",
            false,
        )
        .unwrap();
    assert_eq!(
        message.query_param(b"seq").unwrap().as_deref(),
        Some(b"123456".as_slice())
    );
    assert_eq!(
        message.query_param(b"userid").unwrap().as_deref(),
        Some(b"1256679245".as_slice())
    );
    assert_eq!(
        message.query_param(b"dup").unwrap().as_deref(),
        Some(b"2".as_slice())
    );
    assert_eq!(
        message.query_param(b"helloWorld").unwrap().as_deref(),
        Some(b"".as_slice())
    );
    assert_eq!(message.integer_query_param_or(b"dup", 5), 2);
    assert_eq!(message.integer_query_param_or(b"abc", 5), 5);
    assert_eq!(
        message.decoded_query_param(b"second").unwrap().as_ref(),
        b"was it clear (already)?"
    );
    assert_eq!(
        message.decoded_query_param(b"seq").unwrap().as_ref(),
        b"123456"
    );
    assert!(message.has_query_param(b"seq").unwrap());
    assert!(!message.has_query_param(b"seq1").unwrap());
    assert_eq!(message.integer_query_param(b"dup").unwrap(), 2);
    assert!(message.integer_query_param(b"abc").is_err());
    assert!(message.integer_query_param(b"second").is_err());

    let parameter = message.query_param(b"seq").unwrap().unwrap();
    message.set_query_param(b"foo", b"bar", false).unwrap();
    assert_eq!(parameter.as_ref(), b"123456");
    message.remove_query_param(b"foo").unwrap();
    assert_eq!(parameter.as_ref(), b"123456");
}

#[test]
fn httpmessage_test_parse_query_params_complex() {
    let mut message = HttpMessage::new();
    let input: [[&[u8]; 3]; 6] = [
        [b"", b"", b""],
        [b"key_and_equal_but_no_value", b"=", b""],
        [b"only_key", b"", b""],
        [b"key_and_value", b"=", b"value"],
        [b"key_and_value_2", b"=", b"value2"],
        [b"key_and_value_3", b"=", b"value3"],
    ];

    for mask in 0..(1 << input.len()) {
        let mut subset: Vec<_> = input
            .iter()
            .enumerate()
            .filter_map(|(index, value)| ((mask & (1 << index)) != 0).then_some(*value))
            .collect();
        subset.sort();
        loop {
            let mut url = b"/test?".to_vec();
            for (index, value) in subset.iter().enumerate() {
                if index != 0 {
                    url.push(b'&');
                }
                url.extend_from_slice(value[0]);
                url.extend_from_slice(value[1]);
                url.extend_from_slice(value[2]);
            }
            message.set_url(&url, false).unwrap();
            for value in &subset {
                if !value[0].is_empty() {
                    assert_eq!(
                        message.query_param(value[0]).unwrap().as_deref(),
                        Some(value[2]),
                    );
                }
            }
            if !next_permutation(&mut subset) {
                break;
            }
        }
    }
}

#[test]
fn httpmessage_set_invalid_url() {
    let mut message = HttpMessage::new();
    message
        .set_url(b"http://www.foooooooooooooooooooo.com/bar", false)
        .unwrap();
    assert_eq!(message.path().unwrap(), b"/bar");
    message
        .set_url(b"/\t/?tbtkkukgrenncdlvlgbigerblcgjbkgb=1", false)
        .unwrap();
    assert_eq!(message.path().unwrap(), b"");
}

#[test]
fn httpmessage_set_urlempty() {
    let mut message = HttpMessage::new();
    assert!(!message.set_url(b"", false).unwrap().valid());
    assert_eq!(message.path().unwrap(), b"");
}

#[test]
fn httpmessage_set_absolute_urlno_path() {
    let mut message = HttpMessage::new();
    assert!(
        message
            .set_url(b"http://www.foo.com", false)
            .unwrap()
            .valid()
    );
    assert_eq!(message.path().unwrap(), b"/");
}

#[test]
fn httpmessage_test_header_preservation() {
    let mut message = HttpMessage::new();
    let headers = message.headers_mut();
    headers.add(b"Jojo", b"1");
    headers.add(b"Binky", b"2");
    headers.add(b"jOJo", b"3");
    headers.add(b"bINKy", b"4");
    assert_eq!(headers.size(), 4);
    assert_eq!(headers.number_of_values(b"jojo"), 2);
    assert_eq!(headers.number_of_values(b"binky"), 2);
}

#[test]
fn httpmessage_test_header_remove() {
    let mut message = HttpMessage::new();
    let headers = message.headers_mut();
    headers.add(b"Jojo", b"1");
    headers.add(b"Binky", b"2");
    headers.add(b"jOJo", b"3");
    headers.add(b"bINKy", b"4");
    headers.remove(b"jojo");
    assert_eq!(headers.size(), 2);
    assert_eq!(headers.number_of_values(b"binky"), 2);
}

#[test]
fn httpmessage_test_all_versions_header_remove() {
    let mut message = HttpMessage::new();
    let headers = message.headers_mut();
    headers.add(b"Jojo_1_2", b"1");
    headers.add(b"Binky", b"2");
    headers.add(b"Jojo_1-2", b"3");
    headers.add(b"Jojo-1_2", b"4");
    headers.add(b"Jojo-1-2", b"4");
    headers.remove_all_versions(HttpHeaderCode::None, b"Jojo_1_2");
    assert_eq!(headers.size(), 1);

    headers.add(HttpHeaderCode::ContentLength, b"1");
    headers.add(b"Content_Length", b"2");
    headers.remove_all_versions(HttpHeaderCode::ContentLength, b"Content-Length");
    assert_eq!(headers.size(), 1);
}

#[test]
fn httpmessage_test_set_header() {
    let mut message = HttpMessage::new();
    let headers = message.headers_mut();
    headers.set(b"Jojo", b"1");
    assert_eq!(headers.size(), 1);
    assert_eq!(headers.number_of_values(b"Jojo"), 1);
    headers.add(b"jojo", b"2");
    headers.add(b"jojo", b"3");
    headers.add(b"bar", b"4");
    assert_eq!(headers.size(), 4);
    assert_eq!(headers.number_of_values(b"Jojo"), 3);
    headers.set(b"joJO", b"5");
    assert_eq!(headers.size(), 2);
    assert_eq!(headers.number_of_values(b"Jojo"), 1);
}

#[test]
fn httpmessage_test_combine() {
    let mut message = HttpMessage::new();
    let headers = message.headers_mut();
    assert_eq!(headers.combine_default(b"Combine").as_ref(), b"");
    headers.add(b"Combine", b"first value");
    assert_eq!(headers.combine_default(b"Combine").as_ref(), b"first value");
    headers.add(b"Combine", b"second value");
    assert_eq!(
        headers.combine_default(b"Combine").as_ref(),
        b"first value, second value"
    );
    headers.add(b"Combine", b"third value");
    assert_eq!(
        headers.combine_default(b"Combine").as_ref(),
        b"first value, second value, third value"
    );
}

#[test]
fn httpmessage_test_proxification() {
    let mut message = HttpMessage::new();
    message.set_destination_socket_address(SocketAddress::new(b"192.168.1.1", 1887));
    message.set_local_ip(b"10.0.0.1");
    message.ensure_host_header();
    message.set_wants_keepalive(false);
    assert_eq!(
        message.headers().get_single_or_empty(HttpHeaderCode::Host),
        b"192.168.1.1"
    );
    assert!(!message.wants_keepalive());
}

#[test]
fn httpmessage_test_set_client_address() {
    let mut message = HttpMessage::new();
    message
        .set_client_socket_address(SocketAddress::new(b"74.125.127.9", 1987))
        .unwrap();
    assert_eq!(message.client_ip().unwrap().as_ref(), b"74.125.127.9");
    assert_eq!(message.client_port().unwrap().as_ref(), b"1987");
    assert_eq!(message.client_ip().unwrap().as_ref(), b"74.125.127.9");
    assert_eq!(message.client_port().unwrap().as_ref(), b"1987");

    message
        .set_client_socket_address(SocketAddress::new(b"74.125.127.8", 1988))
        .unwrap();
    assert_eq!(message.client_ip().unwrap().as_ref(), b"74.125.127.8");
    assert_eq!(message.client_port().unwrap().as_ref(), b"1988");
}

#[test]
fn httpmessage_bizarre_versions() {
    let mut message = HttpMessage::new();
    message.set_http_version(0, 22);
    assert_eq!(message.version_string(), b"0.22");
    message.set_http_version(10, 1);
    assert_eq!(message.version_string(), b"10.1");
}

#[test]
fn httpmessage_version_values_survive_clone_and_take() {
    let mut message = HttpMessage::new();
    for (major, minor, expected) in [
        (0, 0, b"0.0".as_slice()),
        (9, 10, b"9.10"),
        (10, 9, b"10.9"),
        (99, 99, b"99.99"),
        (99, 100, b"99.100"),
        (100, 99, b"100.99"),
        (255, 255, b"255.255"),
    ] {
        message.set_http_version(major, minor);
        assert_eq!(message.http_version(), (major, minor));
        assert_eq!(message.version_string(), expected);
        assert_eq!(message.protocol_string(), expected);
    }
    let mut cloned = message.clone();
    for invalid in [
        b"".as_slice(),
        b"1",
        b"1.",
        b"10.1",
        b"1.10",
        b"1-1",
        b"x.1",
        b"1.x",
        b"\xff.1",
    ] {
        message.set_version_string(invalid);
        assert_eq!(message.http_version(), (255, 255));
        assert_eq!(message.version_string(), b"255.255");
    }
    message.set_version_string(b"2.0");
    assert_eq!(message.http_version(), (2, 0));
    assert_eq!(message.version_string(), b"2.0");
    assert_eq!(message.protocol_string(), b"2.0");
    assert_eq!(cloned.http_version(), (255, 255));
    assert_eq!(cloned.version_string(), b"255.255");
    cloned.set_advanced_protocol_string(b"h2");
    assert_eq!(cloned.protocol_string(), b"h2");
    assert!(
        cloned
            .to_string()
            .lines()
            .any(|line| line == " version:255.255")
    );

    let taken = cloned.take();
    assert_eq!(taken.http_version(), (255, 255));
    assert_eq!(taken.version_string(), b"255.255");
    assert_eq!(taken.protocol_string(), b"h2");
    assert_eq!(cloned.http_version(), (1, 0));
    assert_eq!(cloned.version_string(), b"1.0");
    assert_eq!(cloned.protocol_string(), b"1.0");
}

#[test]
fn httpmessage_test_keepalive_check() {
    fn keepalive(version: (u8, u8), values: &[&[u8]], strip: bool) -> bool {
        let mut message = HttpMessage::new();
        message.set_http_version(version.0, version.1);
        for value in values {
            message
                .headers_mut()
                .add(HttpHeaderCode::Connection, *value);
        }
        if strip {
            message.strip_per_hop_headers(false, None);
        }
        message.compute_keepalive()
    }

    assert!(!keepalive((1, 0), &[], false));
    assert!(keepalive((1, 1), &[], false));
    assert!(!keepalive((1, 1), &[b"close"], false));
    assert!(!keepalive((1, 1), &[b"ClOsE"], false));
    assert!(keepalive((1, 1), &[b"foo,bar"], false));
    assert!(!keepalive(
        (1, 1),
        &[b"foo,bar", b"abc,CLOSE,def", b"xyz"],
        false
    ));
    assert!(!keepalive(
        (1, 1),
        &[b"foo,bar", b"abc ,  CLOSE , def", b"xyz"],
        false
    ));
    assert!(!keepalive((1, 1), &[b"  close "], false));
    assert!(!keepalive((1, 1), &[b",  close "], false));
    assert!(!keepalive((1, 1), &[b"  close , "], false));
    assert!(keepalive((1, 0), &[b"Keep-Alive"], false));
    assert!(keepalive((1, 0), &[b"keep-alive"], false));
    assert!(!keepalive((1, 0), &[b"keep-alive", b"close"], false));
    assert!(keepalive((1, 0), &[b"keep-alive"], true));
}

#[test]
fn httpmessage_test_header_strip_per_hop() {
    let mut message = HttpMessage::new();
    for value in [
        b"a, b, c".as_slice(),
        b"d",
        b",,,,",
        b" , , , ,",
        b", e",
        b" f ,\tg\t, \r\n\th ",
    ] {
        message.headers_mut().add(HttpHeaderCode::Connection, value);
    }
    message
        .headers_mut()
        .add(HttpHeaderCode::KeepAlive, b"true");
    message
        .headers_mut()
        .add(HttpHeaderCode::Priority, b"u=5, i");
    for (name, value) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
        (b"f", b"6"),
        (b"g", b"7"),
        (b"h", b"8"),
    ] {
        message.headers_mut().add(name, value);
    }
    assert_eq!(message.headers().size(), 16);
    message.strip_per_hop_headers(false, None);
    assert_eq!(message.headers().size(), 1);
    assert_eq!(message.stripped_per_hop_headers().unwrap().size(), 15);
    message.strip_per_hop_headers(true, None);
    assert_eq!(message.headers().size(), 0);
    assert_eq!(message.stripped_per_hop_headers().unwrap().size(), 1);
}

#[test]
#[should_panic(expected = "HTTP header names must not be empty")]
fn httpmessage_test_empty_name() {
    let mut message = HttpMessage::new();
    message.headers_mut().set(b"", b"empty name?!");
}

#[test]
fn httpmessage_check_header_for_token() {
    let cases = [
        (b"close".as_slice(), true),
        (b"xclose", false),
        (b"closex", false),
        (b"close, foo", true),
        (b"close, ", true),
        (b"foo, close", true),
        (b", close", true),
        (b"close, close, ", true),
    ];
    let mut message = HttpMessage::new();
    for (value, expected) in cases {
        message.headers_mut().set(HttpHeaderCode::Connection, value);
        assert_eq!(
            message.check_for_header_token(HttpHeaderCode::Connection, b"close", false),
            expected,
        );
    }
}

#[test]
fn httpmessage_remove_query_param_tests() {
    fn check(url: &[u8], parameter: &[u8], expected_url: &[u8], expected_query: &[u8]) {
        let mut message = HttpMessage::new();
        message.set_url(url, false).unwrap();
        assert_eq!(
            message.remove_query_param(parameter).unwrap(),
            url != expected_url
        );
        assert_eq!(message.url().unwrap(), expected_url);
        assert_eq!(message.query_string().unwrap(), expected_query);
    }
    check(
        b"http://localhost:80/foo?param1=a&param2=b#qqq",
        b"param2",
        b"http://localhost:80/foo?param1=a#qqq",
        b"param1=a",
    );
    check(
        b"http://localhost/foo?param1=a&param2=b#qqq",
        b"param3",
        b"http://localhost/foo?param1=a&param2=b#qqq",
        b"param1=a&param2=b",
    );
    check(
        b"localhost:80/foo?param1=a&param2=b#qqq",
        b"param2",
        b"localhost:80/foo?param1=a#qqq",
        b"param1=a",
    );
    check(b"localhost", b"param2", b"localhost", b"");
    check(b"localhost", b"", b"localhost", b"");
    check(b"/f/o/o?bar#qqq", b"bar", b"/f/o/o#qqq", b"");
}

#[test]
fn httpmessage_set_query_param_tests() {
    fn check(
        url: &[u8],
        parameter: &[u8],
        value: &[u8],
        expected_url: &[u8],
        expected_query: &[u8],
    ) {
        let mut message = HttpMessage::new();
        message.set_url(url, false).unwrap();
        assert_eq!(
            message.set_query_param(parameter, value, false).unwrap(),
            url != expected_url
        );
        assert_eq!(message.url().unwrap(), expected_url);
        assert_eq!(message.query_string().unwrap(), expected_query);
    }
    check(
        b"http://localhost:80/foo?param1=a&param2=b#qqq",
        b"param2",
        b"true",
        b"http://localhost:80/foo?param1=a&param2=true#qqq",
        b"param1=a&param2=true",
    );
    check(
        b"http://localhost/foo?param1=a&param2=b#qqq",
        b"param3",
        b"true",
        b"http://localhost/foo?param1=a&param2=b&param3=true#qqq",
        b"param1=a&param2=b&param3=true",
    );
    check(
        b"localhost:80/foo?param1=a&param3=c#qqq",
        b"param2",
        b"b",
        b"localhost:80/foo?param1=a&param2=b&param3=c#qqq",
        b"param1=a&param2=b&param3=c",
    );
    check(
        b"localhost:80/foo#qqq",
        b"param2",
        b"b",
        b"localhost:80/foo?param2=b#qqq",
        b"param2=b",
    );
}

#[test]
fn httpmessage_test_check_for_header_token() {
    let mut message = HttpMessage::new();
    message
        .headers_mut()
        .add(HttpHeaderCode::Connection, b"HTTP2-Settings");
    assert!(message.check_for_header_token(HttpHeaderCode::Connection, b"HTTP2-Settings", false,));
    assert!(!message.check_for_header_token(HttpHeaderCode::Connection, b"http2-settings", true,));
}

#[test]
fn httpmessage_test_protocol_string_httpversion() {
    let mut message = HttpMessage::new();
    message.set_http_version(1, 1);
    assert_eq!(message.protocol_string(), b"1.1");
}

#[test]
fn httpmessage_test_protocol_string_advanced_protocol() {
    let mut message = HttpMessage::new();
    message.set_advanced_protocol_string(b"h2");
    assert_eq!(message.protocol_string(), b"h2");
}

#[test]
fn advanced_protocol_storage_preserves_input_and_setter_order() {
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;

    struct ProtocolInput<'a> {
        bytes: &'a [u8],
        calls: &'a Cell<usize>,
        panics: bool,
    }

    impl AsRef<[u8]> for ProtocolInput<'_> {
        fn as_ref(&self) -> &[u8] {
            self.calls.set(self.calls.get() + 1);
            if self.panics {
                panic_any("protocol input panic");
            }
            self.bytes
        }
    }

    let calls = Cell::new(0);
    let mut message = HttpMessage::new();
    assert!(!message.is_advanced_protocol());
    assert_eq!(message.advanced_protocol_string(), None);
    assert_eq!(message.protocol_string(), b"1.0");
    for value in [b"h2".as_slice(), b"custom\0\xff", b"", b"h2"] {
        let previous = message.advanced_protocol_string().map(<[u8]>::to_vec);
        let previous_protocol = message.protocol_string().to_vec();
        calls.set(0);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            message.set_advanced_protocol_string(ProtocolInput {
                bytes: value,
                calls: &calls,
                panics: true,
            });
        }))
        .expect_err("input coercion must panic before changing the protocol");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"protocol input panic"));
        assert_eq!(calls.get(), 1);
        assert_eq!(message.advanced_protocol_string(), previous.as_deref());
        assert_eq!(message.protocol_string(), previous_protocol);
        assert_eq!(message.is_advanced_protocol(), previous.is_some());

        let mut input = value.to_vec();
        calls.set(0);
        message.set_advanced_protocol_string(ProtocolInput {
            bytes: &input,
            calls: &calls,
            panics: false,
        });
        assert_eq!(calls.get(), 1);
        input.fill(b'x');
        drop(input);
        message.set_http_version(2, 0);
        let cloned = message.clone();
        message.set_advanced_protocol_string(b"replacement");
        assert_eq!(message.protocol_string(), b"replacement");
        assert!(cloned.is_advanced_protocol());
        assert_eq!(cloned.advanced_protocol_string(), Some(value));
        assert_eq!(cloned.protocol_string(), value);
        message = cloned;
    }
}

#[test]
fn httpmessage_test_extract_trailers() {
    let mut message = HttpMessage::new();
    let mut trailers = Box::new(HttpHeaders::new());
    trailers.add(b"The-trailer", b"something");
    let original = std::ptr::from_ref::<HttpHeaders>(&*trailers);
    message.set_trailers(trailers);
    let extracted = message.extract_trailers().unwrap();
    assert_eq!(original, std::ptr::from_ref::<HttpHeaders>(&*extracted));
    assert!(message.trailers().is_none());
}

#[test]
fn httpmessage_test_move_copy() {
    let mut first = HttpMessage::new();
    first.set_url(vec![b'a'; 32], false).unwrap();
    let mut second = HttpMessage::new();
    second.set_url(vec![b'b'; 32], false).unwrap();
    assert_eq!(second.url().unwrap(), vec![b'b'; 32]);
    second = first.clone();
    assert_eq!(second.url().unwrap(), vec![b'a'; 32]);
    let mut moved = first;
    assert_eq!(moved.url().unwrap(), vec![b'a'; 32]);
}

#[test]
fn httpmessage_pretty_print() {
    let mut request = HttpMessage::new();
    request.set_url(b"/foo/bar.php?n=v", false).unwrap();
    request.set_http_version(1, 1);
    request.set_method("SPECIAL").unwrap();
    request.set_chunked(true);
    request
        .set_client_socket_address(SocketAddress::new(b"localhost", 9999))
        .unwrap();
    request.client_ip().unwrap();
    request.client_port().unwrap();
    request
        .headers_mut()
        .add(HttpHeaderCode::Host, b"www.intstagram.com");
    request.headers_mut().add(b"Bar", b"Foo");
    assert_eq!(request.method_string().unwrap(), "SPECIAL");

    let mut response = HttpMessage::new();
    response.set_status_code(418).unwrap();
    response.set_status_message(b"I am a teapot").unwrap();
    response.set_http_version(3, 0);
    response
        .headers_mut()
        .add(HttpHeaderCode::ContentLength, b"0");
    response
        .headers_mut()
        .add(HttpHeaderCode::ContentType, b"text/plain");
    assert_eq!(response.headers().size(), 2);
}

#[test]
fn httpmessage_default_scheme_http() {
    let message = HttpMessage::new();
    assert_eq!(message.scheme(), b"http");
    assert!(!message.is_secure());
}

#[test]
fn httpmessage_scheme_https() {
    let mut message = HttpMessage::new();
    message.set_secure(true);
    assert_eq!(message.scheme(), b"https");
    assert!(message.is_secure());
    message.set_secure(false);
    assert_eq!(message.scheme(), b"http");
    assert!(!message.is_secure());
}

#[test]
fn httpmessage_scheme_masque() {
    let mut message = HttpMessage::new();
    message.set_masque();
    assert_eq!(message.scheme(), b"masque");
    assert!(message.is_secure());
    message.set_secure(true);
    assert_eq!(message.scheme(), b"masque");
    assert!(message.is_secure());
    message.set_secure(false);
    assert_eq!(message.scheme(), b"http");
    assert!(!message.is_secure());
}

#[test]
fn httpmessage_strict_mode() {
    let mut message = HttpMessage::new();
    assert!(!message.set_url(b"/foo\xff", true).unwrap().valid());
    assert_eq!(message.url().unwrap(), b"/foo\xff");
    assert_eq!(message.path().unwrap(), b"");

    message.set_url(b"/", false).unwrap();
    assert!(message.set_query_string(b"a=b\xff", false).unwrap());
    assert_eq!(message.url().unwrap(), b"/?a=b\xff");
    assert_eq!(message.path().unwrap(), b"/");
    assert_eq!(message.query_string().unwrap(), b"a=b\xff");

    assert!(message.set_query_string(b"a=b", false).unwrap());
    assert_eq!(message.url().unwrap(), b"/?a=b");
    assert!(!message.set_query_string(b"a=b\xff", true).unwrap());
    assert_eq!(message.url().unwrap(), b"/?a=b\xff");
    assert_eq!(message.query_string().unwrap(), b"");

    assert!(message.set_query_string(b"a=b", false).unwrap());
    assert!(!message.set_query_param(b"c", b"d\xff", true).unwrap());
    assert_eq!(message.url().unwrap(), b"/?a=b&c=d\xff");
    assert!(message.set_query_param(b"c", b"d\xff", false).unwrap());
    assert_eq!(message.url().unwrap(), b"/?a=b&c=d\xff");

    assert!(message.set_url(b"/?a=b&c=d\xff", false).unwrap().valid());
    assert!(message.remove_query_param(b"a").unwrap());
    assert!(message.remove_query_param(b"c").unwrap());
}

#[test]
fn httpmessage_get_single_or_nullptr() {
    let mut message = HttpMessage::new();
    let headers = message.headers_mut();
    headers.add(HttpHeaderCode::Host, b"facebook.com");
    headers.add(b"test", b"test");

    let missing = headers.get_single(b"missing");
    assert!(!missing.exists);
    assert!(missing.value.is_none());
    assert_eq!(missing.value_or_empty(), b"");

    let host = headers.get_single(HttpHeaderCode::Host);
    assert!(host.exists);
    assert!(host.value.is_some());
    assert_eq!(host.value_or_empty(), b"facebook.com");

    let test = headers.get_single(b"test");
    assert!(test.exists);
    assert!(test.value.is_some());
    assert_eq!(test.value_or_empty(), b"test");

    headers.add(b"test", b"test");
    let test = headers.get_single(b"test");
    assert!(test.exists);
    assert!(test.value.is_none());
    assert_eq!(test.value_or_empty(), b"");
}

#[test]
fn httpmessage_bad_apiusage() {
    let mut request = HttpMessage::new();
    request.set_url(b"/", false).unwrap();
    assert_eq!(
        request.status_code(),
        Err(MessageTypeError::ResponseApiOnRequest)
    );
    assert_eq!(request.url().unwrap(), b"/");

    let mut response = HttpMessage::new();
    response.set_status_code(200).unwrap();
    assert_eq!(
        response.query_string(),
        Err(MessageTypeError::RequestApiOnResponse)
    );
    assert_eq!(response.status_code().unwrap(), 200);
}

#[test]
fn message_policy_and_owned_byte_boundaries() {
    let mut message = HttpMessage::new();
    let mut raw_url = b"/path?obs=\x80&bad=%QQ".to_vec();
    message.set_url(&raw_url, false).unwrap();
    raw_url.fill(b'x');
    assert_eq!(message.url().unwrap(), b"/path?obs=\x80&bad=%QQ");
    assert_eq!(
        message.query_param(b"obs").unwrap().as_deref(),
        Some(b"\x80".as_slice())
    );
    assert_eq!(message.decoded_query_param(b"bad").unwrap().as_ref(), b"");
    message
        .set_url(b"/?x=a+b%QQ&y=ok%20then%4Z", false)
        .unwrap();
    assert_eq!(message.decoded_query_param(b"x").unwrap().as_ref(), b"a ");
    assert_eq!(message.decoded_query_param(b"y").unwrap().as_ref(), b"ok ");

    message.set_http_priority(HttpPriority::from_urgency_and_incremental(2, true));
    assert_eq!(message.http_priority().unwrap().urgency, 2);
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Priority),
        b"u=2,i"
    );
}

#[test]
fn message_byte_fields_preserve_short_and_long_copies() {
    for len in [0, 1, 7, 8, 21, 22, 23, 64] {
        let value = vec![0x80; len];
        let mut input = value.clone();
        let mut message = HttpMessage::new();
        message.set_local_ip(&input);
        message.set_secure_info(3, &input);
        message.set_advanced_protocol_string(&input);
        message.set_upgrade_protocol(&input);
        let parsed = message.set_url(&input, false).unwrap();
        let address = SocketAddress::new(&input, 123);
        message.set_destination_address(address.clone(), b"ip", b"123");
        message.set_client_address(address, b"ip", b"123").unwrap();
        input.fill(0);
        drop(input);
        let mut cloned = message.clone();
        let client_port = message.client_port().unwrap();
        let destination_port = message.destination_port();
        message.set_url(b"/replacement", false).unwrap();
        message.set_local_ip([0xff; 64]);
        message.set_local_ip(b"short");
        message.set_secure_info(4, b"new");
        message.set_advanced_protocol_string(b"new");
        message.set_upgrade_protocol(b"new");
        drop(message);
        assert_eq!(parsed.url(), value);
        assert_eq!(cloned.url().unwrap(), value);
        assert_eq!(cloned.local_ip(), value);
        assert_eq!(cloned.secure_cipher(), Some(value.as_slice()));
        assert_eq!(cloned.advanced_protocol_string(), Some(value.as_slice()));
        assert_eq!(cloned.upgrade_protocol(), Some(value.as_slice()));
        assert_eq!(cloned.destination_address().host(), value);
        assert_eq!(cloned.client_address().unwrap().host(), value);
        assert_eq!(client_port.as_ref(), b"123");
        assert_eq!(destination_port.as_ref(), b"123");

        let mut response = HttpMessage::new();
        response.set_status_message(&value).unwrap();
        let mut copied = response.clone();
        response.set_status_message([0xff; 64]).unwrap();
        response.set_status_message(b"short").unwrap();
        assert_eq!(copied.status_message().unwrap(), value);
    }
}

#[test]
fn url_storage_preserves_conversion_and_publication_order() {
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;

    struct UrlInput<'a> {
        bytes: &'static [u8],
        calls: &'a Cell<usize>,
        panics: bool,
    }

    impl AsRef<[u8]> for UrlInput<'_> {
        fn as_ref(&self) -> &[u8] {
            self.calls.set(self.calls.get() + 1);
            if self.panics {
                panic_any("URL input panic");
            }
            self.bytes
        }
    }

    let calls = Cell::new(0);
    let input = |bytes: &'static [u8], panics| UrlInput {
        bytes,
        calls: &calls,
        panics,
    };
    let mut message = HttpMessage::new();
    let parsed = message
        .set_url(input(b"/old?key=one", false), false)
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(parsed.url(), b"/old?key=one");
    assert_eq!(message.url().unwrap(), b"/old?key=one");
    assert_eq!(
        message.query_param(b"key").unwrap().as_deref(),
        Some(b"one".as_slice())
    );

    calls.set(0);
    let mut response = HttpMessage::new();
    response.set_status_code(204).unwrap();
    assert!(matches!(
        response.set_url(input(b"/new?key=two", false), false),
        Err(MessageTypeError::RequestApiOnResponse)
    ));
    assert_eq!(calls.get(), 1);
    assert_eq!(response.response_status(), Some(204));
    assert_eq!(response.request_url(), None);

    calls.set(0);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        message.set_url(input(b"/new?key=two", true), false)
    }))
    .expect_err("input coercion must panic before changing the URL");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"URL input panic"));
    assert_eq!(calls.get(), 1);
    assert_eq!(message.url().unwrap(), b"/old?key=one");
    assert_eq!(message.path().unwrap(), b"/old");
    assert_eq!(message.query_string().unwrap(), b"key=one");
    assert_eq!(
        message.query_param(b"key").unwrap().as_deref(),
        Some(b"one".as_slice())
    );
    drop(message);
    assert_eq!(parsed.url(), b"/old?key=one");
}

#[test]
fn message_url_components_survive_clone_and_replacement() {
    let mut message = HttpMessage::new();
    let parsed = message.set_url(b"/first?key=one#frag", false).unwrap();
    assert!(parsed.valid());
    assert_eq!(
        message.query_param(b"key").unwrap().as_deref(),
        Some(b"one".as_slice())
    );
    let mut cloned = message.clone();

    assert!(message.set_url(b"http://root", false).unwrap().valid());
    assert_eq!(message.path().unwrap(), b"/");
    assert_eq!(message.query_string().unwrap(), b"");
    assert_eq!(message.query_param(b"key").unwrap(), None);
    drop(message);

    assert_eq!(cloned.path().unwrap(), b"/first");
    assert_eq!(cloned.query_string().unwrap(), b"key=one");
    assert!(cloned.set_query_param(b"key", b"two", false).unwrap());
    assert_eq!(cloned.url().unwrap(), b"/first?key=two#frag");
    assert_eq!(cloned.path().unwrap(), b"/first");
    assert_eq!(cloned.query_string().unwrap(), b"key=two");
    assert_eq!(
        cloned.query_param(b"key").unwrap().as_deref(),
        Some(b"two".as_slice())
    );
    assert_eq!(parsed.path(), b"/first");
    assert_eq!(parsed.query(), b"key=one");
    assert_eq!(parsed.fragment(), b"frag");

    let partial = cloned
        .set_url(b"localhost:80abc/foo?bar#frag", false)
        .unwrap();
    assert!(!partial.valid());
    assert_eq!(cloned.path().unwrap(), b"");
    assert_eq!(cloned.query_string().unwrap(), b"");
    assert_eq!(cloned.query_param(b"key").unwrap(), None);
    drop(cloned);
    assert_eq!(partial.path(), b"/foo");
    assert_eq!(partial.query(), b"bar");
    assert_eq!(partial.fragment(), b"frag");
    assert_eq!(partial.port(), 80);
}

#[test]
fn max_forwards_and_direct_response_boundaries() {
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Trace).unwrap();
    request.headers_mut().set(b"Max-Forwards", b"2");
    assert_eq!(request.process_max_forwards().unwrap(), 0);
    assert_eq!(request.headers().get_single_or_empty(b"Max-Forwards"), b"1");
    request.headers_mut().set(b"Max-Forwards", b"0");
    assert_eq!(request.process_max_forwards().unwrap(), 501);
    request.headers_mut().set(b"Max-Forwards", b"-1");
    assert_eq!(request.process_max_forwards().unwrap(), 400);
    request.headers_mut().set(b"Max-Forwards", b"\x0b2\x0c");
    assert_eq!(
        request.headers().get_single_or_empty(b"Max-Forwards"),
        b"\x0b2\x0c",
    );
    assert_eq!(request.process_max_forwards().unwrap(), 0);
    assert_eq!(request.headers().get_single_or_empty(b"Max-Forwards"), b"1");

    let mut response = HttpMessage::new();
    response
        .construct_direct_response((1, 1), 204, b"No Content", 0)
        .unwrap();
    assert_eq!(response.status_code().unwrap(), 204);
    assert_eq!(response.status_message().unwrap(), b"No Content");
    assert_eq!(
        response
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentLength),
        b"0"
    );
    assert_eq!(
        response
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentType),
        b"text/plain"
    );
}

fn next_permutation<T: Ord>(values: &mut [T]) -> bool {
    let Some(pivot) = (0..values.len().saturating_sub(1))
        .rev()
        .find(|index| values[*index] < values[*index + 1])
    else {
        return false;
    };
    let successor = (pivot + 1..values.len())
        .rev()
        .find(|index| values[pivot] < values[*index])
        .expect("a successor exists for a valid pivot");
    values.swap(pivot, successor);
    values[pivot + 1..].reverse();
    true
}

#[test]
fn method_initializes_request_fields_and_rejects_responses() {
    let mut empty = HttpMessage::new();
    assert!(!empty.is_request());
    assert_eq!(empty.method(), Ok(None));
    assert!(empty.is_request());
    assert_eq!(
        empty.status_code(),
        Err(MessageTypeError::ResponseApiOnRequest)
    );

    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Put).unwrap();
    assert_eq!(request.method(), Ok(Some(HttpMethod::Put)));
    assert_eq!(request.method(), Ok(Some(HttpMethod::Put)));

    let mut response = HttpMessage::new();
    response.set_status_code(204).unwrap();
    assert_eq!(
        response.method(),
        Err(MessageTypeError::RequestApiOnResponse)
    );
    assert!(response.is_response());
    assert_eq!(response.status_code(), Ok(204));
}

fn populated_request(method: &str, masque: bool) -> HttpMessage {
    let mut request = HttpMessage::new();
    request.set_method(method).unwrap();
    request
        .set_url(
            b"https://example.com/a/long/path?first=1&second=two#frag",
            false,
        )
        .unwrap();
    request.set_http_version(1, 1);
    request
        .set_client_address(
            SocketAddress::new(b"2001:db8::1", 4321),
            b"2001:db8::1",
            b"4321",
        )
        .unwrap();
    request.set_destination_address(SocketAddress::new(b"10.0.0.2", 443), b"10.0.0.2", b"443");
    request.set_local_ip(b"10.0.0.1");
    request
        .headers_mut()
        .add(HttpHeaderCode::Host, b"example.com");
    request
        .headers_mut()
        .add(HttpHeaderCode::Connection, b"keep-alive, x-hop");
    request.headers_mut().add(b"x-hop", b"hop-value");
    request.headers_mut().add(HttpHeaderCode::Te, b"trailers");
    request.strip_per_hop_headers(false, None);
    assert_eq!(
        request.query_param(b"second").unwrap().as_deref(),
        Some(b"two".as_slice())
    );
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer", b"trailer-value");
    request.set_trailers(Box::new(trailers));
    request.set_secure_info(3, b"TLS_AES_128_GCM_SHA256");
    request.set_advanced_protocol_string(b"h2");
    request.set_upgrade_protocol(b"websocket");
    request.set_ingress_websocket_upgrade();
    request.set_http_priority(HttpPriority::from_urgency_and_incremental(1, true));
    request.set_legacy_priority(2);
    request.set_sequence_number(7);
    request.set_ingress_header_size(HttpHeaderSize {
        compressed: 11,
        uncompressed: 22,
        compressed_block: 9,
    });
    request.set_chunked(true);
    request.set_upgraded(true);
    request.set_wants_keepalive(false);
    request.set_trailers_allowed(true);
    if masque {
        request.set_masque();
    } else {
        request.set_secure(true);
    }
    request
}

fn assert_clone_matches(original: &HttpMessage) -> HttpMessage {
    let cloned = original.clone();
    assert_eq!(format!("{cloned:?}"), format!("{original:?}"));
    assert_eq!(cloned.start_time(), original.start_time());
    cloned
}

#[test]
fn clone_preserves_every_field_of_a_fully_populated_request() {
    for (method, masque, share) in [
        ("GET", false, false),
        ("SPECIAL", true, true),
        ("CONNECT", false, true),
    ] {
        let mut original = populated_request(method, masque);
        if share {
            original.headers_mut().share_entries();
        }
        let debug = format!("{original:?}");
        for populated in [
            "client_ip: Some",
            "client_port: Some",
            "query_params: Some",
            "stripped_per_hop_headers: Some",
            "trailers: Some",
            "secure_cipher: Some",
            "advanced_protocol: Some",
            "upgrade_protocol: Some",
        ] {
            assert!(debug.contains(populated), "{populated} missing in {debug}");
        }
        let mut cloned = assert_clone_matches(&original);
        assert_eq!(cloned.local_ip(), b"10.0.0.1");
        assert_ne!(cloned.local_ip().as_ptr(), original.local_ip().as_ptr());
        assert_eq!(cloned.destination_ip(), b"10.0.0.2");
        assert_eq!(cloned.destination_port().as_ref(), b"443");
        assert_eq!(cloned.client_ip().unwrap().as_ref(), b"2001:db8::1");
        assert_eq!(cloned.client_port().unwrap().as_ref(), b"4321");
        assert_eq!(
            cloned.advanced_protocol_string().unwrap().as_ptr(),
            original.advanced_protocol_string().unwrap().as_ptr(),
            "a borrowed advanced protocol stays borrowed"
        );
        assert_eq!(
            cloned
                .stripped_per_hop_headers()
                .unwrap()
                .get_single_or_empty(b"x-hop"),
            b"hop-value"
        );
        assert_eq!(
            cloned.trailers().unwrap().get_single_or_empty(b"x-trailer"),
            b"trailer-value"
        );
        cloned.headers_mut().add(b"x-clone-only", b"1");
        assert!(!original.headers().exists(b"x-clone-only"));
        assert!(cloned.headers().exists(b"x-clone-only"));

        original.set_advanced_protocol_string(b"custom-protocol");
        let owned = assert_clone_matches(&original);
        assert_eq!(
            owned.advanced_protocol_string(),
            Some(b"custom-protocol".as_slice())
        );
        assert_ne!(
            owned.advanced_protocol_string().unwrap().as_ptr(),
            original.advanced_protocol_string().unwrap().as_ptr(),
            "an owned advanced protocol is copied"
        );
    }
}

#[test]
fn clone_preserves_every_field_of_a_populated_response() {
    for (status, reason) in [
        (200, None),
        (418, Some(b"I am a teapot".as_slice())),
        (503, Some(b"".as_slice())),
    ] {
        let mut original = HttpMessage::new();
        original.set_status_code(status).unwrap();
        original
            .set_status_message(reason.unwrap_or(HttpMessage::default_reason(status)))
            .unwrap();
        original.set_http_version(2, 0);
        original
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"128");
        original
            .headers_mut()
            .add(HttpHeaderCode::ContentType, b"text/plain");
        original.headers_mut().share_entries();
        original.set_ingress_header_size(HttpHeaderSize {
            compressed: 5,
            uncompressed: 40,
            compressed_block: 4,
        });
        original.set_sequence_number(3);
        let mut cloned = assert_clone_matches(&original);
        let original_reason = original.status_message().unwrap().as_ptr();
        let cloned_reason = cloned.status_message().unwrap();
        assert_eq!(
            cloned_reason,
            reason.unwrap_or(HttpMessage::default_reason(status))
        );
        if reason.is_none() {
            assert_eq!(
                cloned_reason.as_ptr(),
                original_reason,
                "a static reason stays borrowed"
            );
        } else if !cloned_reason.is_empty() {
            assert_ne!(
                cloned_reason.as_ptr(),
                original_reason,
                "an owned reason is copied"
            );
        }
        assert_eq!(cloned.status_code(), Ok(status));
        cloned
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"0");
        assert_eq!(
            original
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"128"
        );
        assert_eq!(format!("{:?}", original.clone()), format!("{original:?}"));
    }
}

#[test]
fn clone_of_default_message_matches_the_original() {
    let empty = HttpMessage::new();
    let cloned = assert_clone_matches(&empty);
    assert!(!cloned.is_request());
    assert!(!cloned.is_response());
    assert_eq!(cloned.local_ip(), b"");
    assert_eq!(cloned.destination_ip(), b"");
    assert!(cloned.destination_port().is_empty());
    assert!(!cloned.destination_address().is_initialized());
    assert_eq!(cloned.secure_cipher(), None);
    assert_eq!(cloned.advanced_protocol_string(), None);
    assert_eq!(cloned.upgrade_protocol(), None);
    assert!(cloned.trailers().is_none());
    assert!(cloned.stripped_per_hop_headers().is_none());

    let mut request = HttpMessage::new();
    assert_eq!(request.method(), Ok(None));
    let mut cloned = assert_clone_matches(&request);
    assert!(cloned.is_request());
    assert_eq!(cloned.method(), Ok(None));
    assert!(cloned.client_ip().unwrap().is_empty());
    assert!(cloned.client_port().unwrap().is_empty());

    let mut response = HttpMessage::new();
    response.set_status_code(204).unwrap();
    let mut cloned = assert_clone_matches(&response);
    assert!(cloned.is_response());
    assert_eq!(cloned.status_message().unwrap(), b"");
    assert_eq!(format!("{cloned:?}"), format!("{response:?}"));
}

#[test]
fn with_start_time_differs_from_new_only_in_its_start_time() {
    // Ahead of the clock, so that a clock read cannot produce it.
    let start = Instant::now() + Duration::from_secs(3600);
    let given = HttpMessage::with_start_time(start);
    let mut stamped = HttpMessage::new();
    stamped.set_start_time(start);
    assert_eq!(format!("{given:?}"), format!("{stamped:?}"));
    assert_eq!(given.start_time(), stamped.start_time());
    assert_eq!(given.start_time(), start);
}

#[test]
fn default_message_headers_allocate_on_first_insert() {
    for mut message in [HttpMessage::new(), HttpMessage::default()] {
        assert!(message.headers().is_empty());
        assert_eq!(message.headers().capacity(), 0);
        message.headers_mut().add(HttpHeaderCode::Host, "h");
        assert_eq!(message.headers().capacity(), 16);
        assert_eq!(
            message.headers().get_single_or_empty(HttpHeaderCode::Host),
            b"h"
        );
    }
    let mut message = HttpMessage::new();
    message.headers_mut().set(HttpHeaderCode::Host, "h");
    assert_eq!(message.headers().capacity(), 16);

    let mut source = HttpMessage::new();
    source.headers_mut().add("x-a", "1");
    let taken = source.take();
    assert_eq!(source.headers().capacity(), 0);
    assert_eq!(taken.headers().capacity(), 16);
    assert_eq!(taken.headers().get_single_or_empty("x-a"), b"1");

    let mut extracted_from = HttpMessage::new();
    extracted_from.headers_mut().add("x-a", "1");
    let extracted = extracted_from.extract_headers();
    assert_eq!(extracted_from.headers().capacity(), 0);
    assert_eq!(extracted.capacity(), 16);
    assert_eq!(extracted.get_single_or_empty("x-a"), b"1");
    extracted_from.headers_mut().add("x-b", "2");
    assert_eq!(extracted_from.headers().capacity(), 16);

    assert_eq!(HttpHeaders::new().capacity(), 16);
    assert_eq!(HttpHeaders::default().capacity(), 0);
    // Taking moves the storage out and leaves the source with none. The next
    // insert allocates the initial capacity.
    let mut headers = HttpHeaders::new();
    headers.add("x-a", "1");
    let taken = headers.take();
    assert_eq!((headers.capacity(), taken.capacity()), (0, 16));
    headers.add("x-b", "2");
    assert_eq!(headers.capacity(), 16);
    // Move assignment leaves the source with the destination's storage, emptied.
    let mut destination = HttpHeaders::new();
    (0..20).for_each(|index| destination.add(format!("x-{index}").as_str(), "v"));
    let capacity = destination.capacity();
    assert!(capacity > 16);
    let mut source = taken;
    destination.move_from(&mut source);
    assert_eq!((source.len(), source.capacity()), (0, capacity));
    assert!(!source.exists("x-0"));
    assert_eq!(
        (destination.len(), destination.get_single_or_empty("x-a")),
        (1, b"1".as_slice())
    );
}
