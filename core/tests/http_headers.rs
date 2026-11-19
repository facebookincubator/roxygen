/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::headers::HttpCommonHeaderTable;
use crate::headers::HttpCommonHeaders;
use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaderName;
use crate::headers::HttpHeaders;

#[test]
fn httpcommon_headers_tests_test_hashing() {
    let common1 = String::from("Content-Length");
    let common2 = String::from("content-length");
    let uncommon = String::from("Uncommon");

    let common_code1 = HttpCommonHeaders::hash(&common1);
    let common_code2 = HttpCommonHeaders::hash(&common2);
    let uncommon_code = HttpCommonHeaders::hash(&uncommon);

    assert_eq!(uncommon_code, HttpHeaderCode::Other);
    assert_ne!(common_code1, HttpHeaderCode::Other);
    assert_eq!(common_code1, common_code2);
    assert_eq!(std::mem::size_of::<HttpHeaderCode>(), 1);
    assert_eq!(HttpCommonHeaders::NUM_CODES, 77);

    for value in HttpHeaderCode::COMMON_OFFSET..HttpCommonHeaders::NUM_CODES as u8 {
        let code = HttpHeaderCode::from_u8(value).unwrap();
        let camel = HttpCommonHeaders::name(code, HttpCommonHeaderTable::CamelCase);
        let lower = HttpCommonHeaders::name(code, HttpCommonHeaderTable::Lowercase);
        assert_eq!(HttpCommonHeaders::hash(camel), code);
        assert_eq!(HttpCommonHeaders::hash(lower), code);
        assert_eq!(HttpCommonHeaders::hash(camel.to_ascii_uppercase()), code);
    }
    for name in [
        b"".as_slice(),
        b"a",
        b"x-echo-bytes",
        b"content-lengtx",
        b"content\0length",
        b"content-\xffength",
        b"cross-origin-embedder-policy-report-only!",
    ] {
        assert_eq!(HttpCommonHeaders::hash(name), HttpHeaderCode::Other);
    }
}

#[test]
fn httpcommon_headers_tests_test_two_tables_initialized() {
    let code = HttpCommonHeaders::hash("Content-Length");
    let camel_name = HttpCommonHeaders::name(code, HttpCommonHeaderTable::CamelCase);
    let lower_name = HttpCommonHeaders::name(code, HttpCommonHeaderTable::Lowercase);

    assert_eq!(camel_name, b"Content-Length");
    assert_eq!(lower_name, b"content-length");
    assert_eq!(
        HttpCommonHeaders::code_from_table_name(Some(camel_name), HttpCommonHeaderTable::Lowercase,),
        HttpHeaderCode::Other,
    );
}

#[test]
fn httpcommon_headers_tests_test_is_common_header_name_from_table() {
    let table = HttpCommonHeaderTable::CamelCase;
    assert!(!HttpCommonHeaders::is_name_from_table(
        Some(HttpCommonHeaders::name(HttpHeaderCode::None, table)),
        table,
    ));
    assert!(!HttpCommonHeaders::is_name_from_table(
        Some(HttpCommonHeaders::name(HttpHeaderCode::Other, table)),
        table,
    ));

    if HttpCommonHeaders::NUM_CODES > usize::from(HttpHeaderCode::COMMON_OFFSET) {
        let first_checked = HttpHeaderCode::from_u8(HttpHeaderCode::COMMON_OFFSET + 1).unwrap();
        assert!(HttpCommonHeaders::is_name_from_table(
            Some(HttpCommonHeaders::name(first_checked, table)),
            table,
        ));

        let last = HttpHeaderCode::from_u8((HttpCommonHeaders::NUM_CODES - 1) as u8).unwrap();
        assert!(HttpCommonHeaders::is_name_from_table(
            Some(HttpCommonHeaders::name(last, table)),
            table,
        ));
    }

    let external_header = String::from("externalHeader");
    assert!(!HttpCommonHeaders::is_name_from_table(
        Some(external_header.as_bytes()),
        table,
    ));
}

#[test]
fn httpcommon_headers_tests_test_get_header_code_from_table_common_header_name() {
    let table = HttpCommonHeaderTable::CamelCase;
    for value in HttpHeaderCode::COMMON_OFFSET..HttpCommonHeaders::NUM_CODES as u8 {
        let code = HttpHeaderCode::from_u8(value).unwrap();
        assert_eq!(
            HttpCommonHeaders::code_from_table_name(
                Some(HttpCommonHeaders::name(code, table)),
                table,
            ),
            code,
        );
    }

    let external_header = String::from("externalHeader");
    assert_eq!(
        HttpCommonHeaders::code_from_table_name(Some(external_header.as_bytes()), table),
        HttpHeaderCode::Other,
    );
    let external_common_spelling = String::from("Content-Length");
    assert_eq!(
        HttpCommonHeaders::code_from_table_name(Some(external_common_spelling.as_bytes()), table,),
        HttpHeaderCode::Other,
    );
    assert_eq!(
        HttpCommonHeaders::code_from_table_name(None, table),
        HttpHeaderCode::None,
    );
}

#[test]
fn removed_common_names_keep_their_spelling() {
    let names: [&[u8]; 4] = [
        b"expect",
        b"link",
        b"x-forwarded-proto",
        b"WWW-Authenticate",
    ];
    let expected: Vec<_> = names
        .iter()
        .map(|name| (HttpHeaderCode::Other, name.to_vec()))
        .collect();
    let hashed: Vec<_> = names
        .iter()
        .map(|name| (HttpCommonHeaders::hash(name), name.to_vec()))
        .collect();
    assert_eq!(hashed, expected);
    let mut headers = HttpHeaders::new();
    names.iter().for_each(|name| headers.add(*name, b"v"));
    let stored: Vec<_> = headers
        .iter()
        .map(|(code, name, _)| (code, name.to_vec()))
        .collect();
    assert_eq!(stored, expected);
}

#[test]
fn added_common_names_use_the_table_spelling() {
    // Each name's code is its line in `HTTPCommonHeaders.txt` plus one.
    let added: [(&[u8], u8, &[u8]); 3] = [
        (b"No-Vary-Search", 45, b"no-vary-search"),
        (b"Reporting-Endpoints", 57, b"reporting-endpoints"),
        (b"X-Robots-Tag", 75, b"x-robots-tag"),
    ];
    let hashed: Vec<_> = added
        .iter()
        .map(|(name, _, _)| HttpCommonHeaders::hash(name).as_u8())
        .collect();
    assert_eq!(hashed, [45, 57, 75]);
    let mut headers = HttpHeaders::new();
    added
        .iter()
        .for_each(|(name, _, _)| headers.add(*name, b"v"));
    let printed: Vec<_> = headers
        .iter()
        .map(|(code, name, _)| (code.as_u8(), name.to_vec()))
        .collect();
    let expected: Vec<_> = added
        .iter()
        .map(|(_, code, spelling)| (*code, spelling.to_vec()))
        .collect();
    assert_eq!(printed, expected);
}

/// Proxygen's `HTTPCommonHeaders.txt` at `9d6c281ba22e`, verbatim.
const COMMON_HEADER_LIST: &str = "\
:Authority
:Method
:Path
:Scheme
:Status
:Protocol
Accept
Accept-Encoding
Accept-Language
Accept-Ranges
Access-Control-Allow-Credentials
Access-Control-Allow-Methods
Access-Control-Allow-Origin
Access-Control-Expose-Headers
Age
Alt-Svc
Authorization
Cache-Control
client_timeout
Connection
content-digest
Content-Disposition
Content-Encoding
Content-Language
Content-Length
Content-Range
content-security-policy
Content-Type
Cookie
cross-origin-embedder-policy-report-only
cross-origin-opener-policy
cross-origin-resource-policy
Date
document-policy
Edge-Control
ETag
Expires
Host
If-Modified-Since
If-None-Match
Keep-Alive
Last-Modified
Location
no-vary-search
permissions-policy
Pragma
Priority
Proxy-Authenticate
Proxy-Authorization
Proxy-Connection
Proxy-Status
queue_timeout
Range
Referer
report-to
reporting-endpoints
rpckind
Sec-WebSocket-Accept
Sec-WebSocket-Key
Server
Set-Cookie
Strict-Transport-Security
TE
timing-allow-origin
Trailer
Transfer-Encoding
Upgrade
User-Agent
Vary
Via
X-Content-Type-Options
X-Forwarded-For
X-Frame-Options
x-robots-tag
X-XSS-Protection
";

#[test]
fn common_header_table_matches_proxygens_list() {
    let table: Vec<(u8, &str)> = HttpHeaderCode::ALL[2..]
        .iter()
        .map(|code| {
            let camel = HttpCommonHeaders::name(*code, HttpCommonHeaderTable::CamelCase);
            (code.as_u8(), std::str::from_utf8(camel).unwrap())
        })
        .collect();
    let expected: Vec<(u8, &str)> = (HttpHeaderCode::COMMON_OFFSET..)
        .zip(COMMON_HEADER_LIST.lines())
        .collect();
    assert_eq!(table, expected);
    for code in &HttpHeaderCode::ALL[2..] {
        assert_eq!(
            HttpCommonHeaders::name(*code, HttpCommonHeaderTable::Lowercase),
            HttpCommonHeaders::name(*code, HttpCommonHeaderTable::CamelCase).to_ascii_lowercase(),
            "{code:?}"
        );
    }
}

#[test]
fn httpheaders_add_string_piece() {
    let input = "name:value";
    let (name, value) = input.split_once(':').unwrap();
    let mut headers = HttpHeaders::new();

    headers.add(name, value);

    assert_eq!(headers.get_single_or_empty("name"), b"value");
}

#[test]
fn httpheaders_initializer_list() {
    let mut headers = HttpHeaders::new();

    headers.add_all([(HttpHeaderName::from("name"), "value")]);
    headers.add_all([(HttpHeaderName::from(HttpHeaderCode::Connection), "close")]);
    headers.add_all([
        (HttpHeaderName::from("a"), "b"),
        (HttpHeaderName::from(HttpHeaderCode::Connection), "foo"),
        (HttpHeaderName::from(HttpHeaderCode::Server), "x"),
    ]);

    assert_eq!(headers.get_single_or_empty("name"), b"value");
    assert_eq!(
        headers.combine_default(HttpHeaderCode::Connection).as_ref(),
        b"close, foo",
    );
    assert_eq!(headers.get_single_or_empty(HttpHeaderCode::Server), b"x");
}

#[test]
fn httpheaders_initializer_list_string_view() {
    let input = "name:value";
    let (name, value) = input.split_once(':').unwrap();
    let mut headers = HttpHeaders::new();

    headers.add_all([
        (HttpHeaderName::from(name), value),
        (HttpHeaderName::from(HttpHeaderCode::Connection), value),
    ]);

    assert_eq!(headers.get_single_or_empty("name"), b"value");
    assert_eq!(
        headers.get_single_or_empty(HttpHeaderCode::Connection),
        b"value",
    );
}

fn repeated_value(start: u8, offset: usize) -> Vec<u8> {
    vec![start.wrapping_add(offset as u8); 50]
}

fn fill_headers(headers: &mut HttpHeaders, count: usize, start: u8) {
    for index in 0..count {
        headers.add(index.to_string().as_str(), repeated_value(start, index));
    }
}

fn owned_entries(headers: &HttpHeaders) -> Vec<(HttpHeaderCode, Vec<u8>, Vec<u8>)> {
    headers
        .iter()
        .map(|(code, name, value)| (code, name.to_vec(), value.to_vec()))
        .collect()
}

#[test]
fn httpheaders_grow_test() {
    let mut headers = HttpHeaders::new();
    fill_headers(&mut headers, 32, b'a');

    assert_eq!(headers.get_single_or_empty("0").first(), Some(&b'a'));
    assert_eq!(headers.get_single_or_empty("25").first(), Some(&b'z'));
}

#[test]
fn httpheaders_clear_test() {
    let mut headers = HttpHeaders::new();
    fill_headers(&mut headers, 32, b'a');
    assert_eq!(headers.get_single_or_empty("25").first(), Some(&b'z'));

    headers.remove_all();
    assert_eq!(headers.size(), 0);
    fill_headers(&mut headers, 32, b'A');

    assert_eq!(headers.get_single_or_empty("25").first(), Some(&b'Z'));
}

fn copy_and_move_test(multiplier: usize) {
    let mut headers = HttpHeaders::new();
    fill_headers(&mut headers, 16 * multiplier, b'a');

    let headers2 = headers.clone();
    assert_eq!(headers2.get_single_or_empty("3").first(), Some(&b'd'));

    let mut headers3 = HttpHeaders::new();
    headers3.add("blown", "away");
    headers3.clone_from(&headers2);
    assert_eq!(headers2.get_single_or_empty("4").first(), Some(&b'e'));

    let headers4 = headers3.take();
    assert_eq!(headers4.get_single_or_empty("5").first(), Some(&b'f'));

    let mut headers4 = headers4;
    let mut headers5 = HttpHeaders::new();
    headers5.add("blown", "away");
    headers5.move_from(&mut headers4);
    assert_eq!(headers5.get_single_or_empty("6").first(), Some(&b'g'));
}

#[test]
fn httpheaders_copy_and_move_test() {
    copy_and_move_test(1);
    copy_and_move_test(2);
    copy_and_move_test(3);
}

#[test]
fn httpheaders_pretty_print() {
    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::Host, "www.facebook.com");
    headers.add(HttpHeaderCode::Connection, "close");
    headers.add("Foo", "Bar");
    headers.remove(HttpHeaderCode::Connection);

    assert_eq!(headers.size(), 2);
}

#[test]
fn httpheaders_get_set_on_resize() {
    let mut headers = HttpHeaders::new();
    for _ in 0..15 {
        headers.add(HttpHeaderCode::Connection, "token");
    }
    let value = vec![b'a'; 32];
    headers.add(HttpHeaderCode::Server, &value);
    assert_eq!(headers.size(), 16);

    let value_from_headers = headers.get_single_or_empty(HttpHeaderCode::Server).to_vec();
    headers.set(HttpHeaderCode::Server, &value_from_headers);

    assert_eq!(
        headers.get_single_or_empty(HttpHeaderCode::Server),
        value.as_slice(),
    );
}

#[test]
fn for_each_value_lends_values_for_the_headers_borrow() {
    let mut headers = HttpHeaders::new();
    headers.add("x-dup", "first");
    headers.add(HttpHeaderCode::Host, "h");
    headers.add("X-DUP", "second");
    let mut kept = Vec::new();
    assert!(!headers.for_each_value("x-Dup", |value| {
        kept.push(value);
        false
    }));
    let mut first = None;
    assert!(headers.for_each_value(HttpHeaderCode::Host, |value| {
        first = Some(value);
        true
    }));
    assert_eq!(kept, [b"first".as_slice(), b"second".as_slice()]);
    assert_eq!(first, Some(b"h".as_slice()));
    let stored: Vec<_> = headers.iter().map(|(_, _, value)| value.as_ptr()).collect();
    assert_eq!(
        [kept[0].as_ptr(), kept[1].as_ptr()],
        [stored[0], stored[2]],
        "the lent values are the stored ones"
    );
}

#[test]
fn httpheaders_move_from_test() {
    let mut headers1 = HttpHeaders::new();
    let headers2 = headers1.take();
    assert!(!headers1.exists(HttpHeaderCode::Connection));
    assert!(!headers1.for_each_value(HttpHeaderCode::Host, |_| { panic!("unreachable") }));
    headers1.add(HttpHeaderCode::Connection, "close");

    let mut headers3 = headers2;
    headers3.move_from(&mut headers1);
    assert_eq!(headers1.size(), 0);
    assert_eq!(headers3.size(), 1);
}

#[test]
fn duplicates_order_case_insensitive_removal_and_value_trimming() {
    let mut headers = HttpHeaders::new();
    headers.add("x-custom", " first\r\n");
    headers.add("HOST", " example.test ");
    headers.add("X-CUSTOM", "\tsecond ");

    assert_eq!(headers.number_of_values("X-Custom"), 2);
    assert_eq!(
        headers.combine_default("x-CUSTOM").as_ref(),
        b"first, second",
    );
    assert_eq!(
        owned_entries(&headers),
        vec![
            (
                HttpHeaderCode::Other,
                b"x-custom".to_vec(),
                b"first".to_vec()
            ),
            (
                HttpHeaderCode::Host,
                b"Host".to_vec(),
                b"example.test".to_vec()
            ),
            (
                HttpHeaderCode::Other,
                b"X-CUSTOM".to_vec(),
                b"second".to_vec()
            ),
        ],
    );
    assert!(headers.remove("X-custom"));
    assert_eq!(
        owned_entries(&headers),
        vec![(
            HttpHeaderCode::Host,
            b"Host".to_vec(),
            b"example.test".to_vec(),
        )],
    );
}

#[test]
fn duplicate_single_result_distinguishes_presence_from_uniqueness() {
    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::SetCookie, "a=1");
    headers.add(HttpHeaderCode::SetCookie, "b=2");

    let result = headers.get_single(HttpHeaderCode::SetCookie);
    assert!(result.exists);
    assert_eq!(result.value, None);
    assert_eq!(result.value_or_empty(), b"");
}

#[test]
fn code_queries_follow_copy_move_remove_and_clear() {
    for &code in HttpHeaderCode::ALL {
        let mut original = HttpHeaders::new();
        assert!(!original.exists(code));
        assert!(!original.get_single(code).exists);
        assert_eq!(original.number_of_values(code), 0);
        assert!(original.combine(code, b"|").is_empty());
        assert!(!original.for_each_value(code, |_| panic!("absent header visited")));
        assert!(!original.remove(code));
        original.add(code, b"value");

        let mut copied = HttpHeaders::new();
        original.copy_to(&mut copied);
        let mut moved = HttpHeaders::new();
        moved.add("x-replaced", b"old");
        moved.move_from(&mut copied);
        assert!(copied.is_empty());
        assert_eq!(original, moved);
        assert!(moved.exists(code));
        assert_eq!(moved.get_single(code).value, Some(b"value".as_slice()));
        assert_eq!(moved.number_of_values(code), 1);
        assert_eq!(moved.combine(code, b"|").as_ref(), b"value");
        let mut visited = 0;
        assert!(!moved.for_each_value(code, |value| {
            assert_eq!(value, b"value");
            visited += 1;
            false
        }));
        assert_eq!(visited, 1);

        assert!(moved.remove(code));
        assert!(!moved.exists(code));
        assert!(!moved.get_single(code).exists);
        assert_eq!(moved, HttpHeaders::new());
        moved.add(code, b"replacement");
        moved.clear();
        assert_eq!(moved, HttpHeaders::new());
        moved.add(code, b"after clear");
        assert_eq!(
            moved.get_single(code).value,
            Some(b"after clear".as_slice())
        );
    }
}

#[test]
fn header_equality_ignores_removed_names() {
    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::Cookie, b"temporary");
    headers.add("x-temporary", b"temporary");
    headers.remove(HttpHeaderCode::Cookie);
    headers.remove("x-temporary");
    headers.add(HttpHeaderCode::Host, b"example.test");

    let mut same = HttpHeaders::new();
    same.add(HttpHeaderCode::Host, b"example.test");
    assert_eq!(headers, same);
    assert_eq!(headers.clone(), same);
    assert_eq!(format!("{headers:?}"), format!("{same:?}"));
    assert!(!headers.exists("X-Temporary"));
    same.add(HttpHeaderCode::Host, b"duplicate");
    assert_ne!(headers, same);
}

#[test]
fn stripped_headers_remain_queryable_after_transfer_and_copy() {
    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::Connection, b"x-remove, user-agent");
    headers.add("X-Remove", b"custom");
    headers.add(HttpHeaderCode::UserAgent, b"agent");
    headers.add(HttpHeaderCode::Host, b"example.test");
    let mut stripped = HttpHeaders::new();
    headers.strip_per_hop_headers(&mut stripped, false, None);

    assert_eq!(
        stripped.get_single("x-remove").value,
        Some(b"custom".as_slice())
    );
    assert_eq!(
        stripped.get_single(HttpHeaderCode::UserAgent).value,
        Some(b"agent".as_slice())
    );
    assert!(stripped.exists(HttpHeaderCode::Connection));
    assert!(!headers.exists("x-remove"));
    assert!(!headers.exists(HttpHeaderCode::UserAgent));
    assert!(headers.exists(HttpHeaderCode::Host));

    let mut copied = HttpHeaders::new();
    stripped.copy_to(&mut copied);
    assert_eq!(
        copied.get_single("X-REMOVE").value,
        Some(b"custom".as_slice())
    );
    assert!(!copied.exists("x-unrelated"));
    assert_eq!(copied.number_of_values(HttpHeaderCode::Other), 1);
}

#[test]
fn one_version_and_per_hop_stripping_match_source_order() {
    let mut headers = HttpHeaders::new();
    headers.add("content_length", "1");
    assert!(!headers.exists(HttpHeaderCode::ContentLength));
    assert!(headers.remove_all_versions(HttpHeaderCode::ContentLength, b"content-length"));
    assert!(headers.is_empty());
    headers.add("x-test_value", "old underscore");
    headers.add("X-Test-Value", "old hyphen");
    headers.set_one_version("x-test-value", HttpHeaderCode::Other, "new");
    assert_eq!(headers.combine_default("X-Test-Value").as_ref(), b"new",);

    headers.add("x-hop", "one");
    headers.add(HttpHeaderCode::Connection, " x-hop, Upgrade ");
    headers.add(HttpHeaderCode::Upgrade, "websocket");
    headers.add(HttpHeaderCode::Host, "example.test");
    let mut stripped = HttpHeaders::new();

    headers.strip_per_hop_headers(&mut stripped, false, None);

    assert_eq!(
        owned_entries(&headers),
        vec![
            (
                HttpHeaderCode::Other,
                b"x-test-value".to_vec(),
                b"new".to_vec(),
            ),
            (
                HttpHeaderCode::Host,
                b"Host".to_vec(),
                b"example.test".to_vec()
            ),
        ],
    );
    assert_eq!(
        owned_entries(&stripped),
        vec![
            (HttpHeaderCode::Other, b"x-hop".to_vec(), b"one".to_vec()),
            (
                HttpHeaderCode::Upgrade,
                b"Upgrade".to_vec(),
                b"websocket".to_vec(),
            ),
            (
                HttpHeaderCode::Connection,
                b"Connection".to_vec(),
                b"x-hop, Upgrade".to_vec(),
            ),
        ],
    );
}

#[test]
fn non_utf8_names_and_values_round_trip_without_utf8_expansion() {
    let name = b"x-obs\xff";
    let value = b" \x80\xff\0 ";
    let mut headers = HttpHeaders::new();

    headers.add(name, value);

    assert_eq!(headers.get_single_or_empty(name), b"\x80\xff\0");
    headers.add(name, b"\x81");
    assert_eq!(
        headers.combine(name, b"\xfe").as_ref(),
        b"\x80\xff\0\xfe\x81",
    );
    assert_eq!(
        owned_entries(&headers),
        vec![
            (HttpHeaderCode::Other, name.to_vec(), b"\x80\xff\0".to_vec(),),
            (HttpHeaderCode::Other, name.to_vec(), b"\x81".to_vec()),
        ],
    );
}

#[test]
fn value_trimming_preserves_vertical_tab_and_form_feed() {
    let mut headers = HttpHeaders::new();

    headers.add("x-control", b" \r\x0b\x0cvalue\x0b\x0c\t\n");

    assert_eq!(
        headers.get_single_or_empty("x-control"),
        b"\x0b\x0cvalue\x0b\x0c",
    );
}

#[test]
fn verbatim_values_keep_surrounding_whitespace_where_add_trims() {
    let fields: [(&[u8], &[u8]); 5] = [
        (b"user-agent", b" ua\t"),
        (b"x-lead", b"  leading"),
        (b"x-only-space", b" \t "),
        (b"x-empty", b""),
        (b"x-lead", b"second "),
    ];
    let mut verbatim = HttpHeaders::new();
    let mut trimmed = HttpHeaders::new();
    for (name, value) in fields {
        if HttpCommonHeaders::hash(name) == HttpHeaderCode::Other {
            verbatim.add_uncommon_verbatim(name, value);
        } else {
            verbatim.add_verbatim(name, value);
        }
        trimmed.add(name, value);
    }
    let collect = |headers: &HttpHeaders| {
        let mut seen = Vec::new();
        headers.for_each_with_code(|code, name, value| {
            seen.push((code, name.to_vec(), value.to_vec()));
        });
        seen
    };
    assert_eq!(
        collect(&verbatim),
        [
            (
                HttpHeaderCode::UserAgent,
                b"User-Agent".to_vec(),
                b" ua\t".to_vec()
            ),
            (
                HttpHeaderCode::Other,
                b"x-lead".to_vec(),
                b"  leading".to_vec()
            ),
            (
                HttpHeaderCode::Other,
                b"x-only-space".to_vec(),
                b" \t ".to_vec()
            ),
            (HttpHeaderCode::Other, b"x-empty".to_vec(), b"".to_vec()),
            (
                HttpHeaderCode::Other,
                b"x-lead".to_vec(),
                b"second ".to_vec()
            ),
        ]
    );
    assert_eq!(
        verbatim.combine_default("x-lead"),
        b"  leading, second ".to_vec().into()
    );
    assert_eq!(trimmed.get_single_or_empty("user-agent"), b"ua");
    assert_eq!(trimmed.get_single_or_empty("x-only-space"), b"");
    assert_eq!(
        trimmed.combine_default("x-lead"),
        b"leading, second".to_vec().into()
    );
}

#[test]
fn short_and_long_header_bytes_remain_independently_owned() {
    for len in [0, 1, 7, 8, 21, 22, 23, 64] {
        let mut name = vec![b'x'; len.max(1)];
        name[0] = 0xff;
        let value = vec![0x80; len];
        let mut input = value.clone();
        let mut headers = HttpHeaders::new();
        headers.add(&name, &input);
        input.fill(0);
        assert_eq!(headers.get_single_or_empty(&name), value);
        assert_eq!(
            format!("{headers:?}"),
            format!("{:?}", [(name.as_slice(), value.as_slice())]),
        );

        let mut cloned = headers.clone();
        let mut copied = HttpHeaders::new();
        headers.copy_to(&mut copied);
        assert_eq!(headers, copied);
        let owned = headers.raw_get(&name);
        headers.set(&name, [0xff; 64]);
        headers.set(&name, b"short");
        assert_eq!(cloned.get_single_or_empty(&name), value);
        assert_eq!(owned.as_ref(), value);

        cloned.add(&name, b"\0\xff");
        let mut combined = value.clone();
        if !combined.is_empty() {
            combined.extend_from_slice(b"|");
        }
        combined.extend_from_slice(b"\0\xff");
        assert_eq!(cloned.combine(&name, b"|").as_ref(), combined);
        let moved = copied.take();
        assert!(copied.is_empty());
        assert_eq!(moved.get_single_or_empty(&name), value);
    }
}

#[test]
fn connection_tokenization_stops_at_first_nul() {
    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::Connection, b"keep-alive\0, authorization");
    headers.add(HttpHeaderCode::KeepAlive, "timeout=5");
    headers.add(HttpHeaderCode::Authorization, "secret");
    let mut stripped = HttpHeaders::new();

    headers.strip_per_hop_headers(&mut stripped, false, None);

    assert!(!headers.exists(HttpHeaderCode::KeepAlive));
    assert!(stripped.exists(HttpHeaderCode::KeepAlive));
    assert_eq!(
        stripped.get_single_or_empty(HttpHeaderCode::Connection),
        b"keep-alive\0, authorization",
    );
    assert_eq!(
        headers.get_single_or_empty(HttpHeaderCode::Authorization),
        b"secret",
    );
    assert!(!stripped.exists(HttpHeaderCode::Authorization));
}

#[test]
fn connection_self_removal_stops_scanning_later_connection_values() {
    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::Connection, "connection");
    headers.add(HttpHeaderCode::Connection, "authorization");
    headers.add(HttpHeaderCode::Authorization, "secret");
    let mut stripped = HttpHeaders::new();

    headers.strip_per_hop_headers(&mut stripped, false, None);

    assert_eq!(
        headers.get_single_or_empty(HttpHeaderCode::Authorization),
        b"secret",
    );
    assert_eq!(headers.number_of_values(HttpHeaderCode::Connection), 0);
    assert_eq!(stripped.number_of_values(HttpHeaderCode::Connection), 2,);
    assert!(!stripped.exists(HttpHeaderCode::Authorization));
}

#[test]
#[should_panic(expected = "HTTP header names must not be empty")]
fn empty_header_name_panics_in_all_build_modes() {
    let mut headers = HttpHeaders::new();
    headers.set(b"", b"value");
}

#[test]
fn stored_header_names_preserve_reserved_codes_and_ownership() {
    let mut headers = HttpHeaders::new();
    let mut custom_name = b"X-CuStOm\xff".to_vec();
    headers.add(HttpHeaderCode::None, b"none");
    headers.add(HttpHeaderCode::Other, b"other");
    headers.add(&custom_name, b"custom");
    headers.add(HttpHeaderCode::Host, b"code");
    headers.add("hOsT", b"name");
    custom_name.fill(b'?');
    drop(custom_name);

    let expected: &[(HttpHeaderCode, &[u8], &[u8])] = &[
        (HttpHeaderCode::None, b"", b"none"),
        (HttpHeaderCode::Other, b"", b"other"),
        (HttpHeaderCode::Other, b"X-CuStOm\xff", b"custom"),
        (HttpHeaderCode::Host, b"Host", b"code"),
        (HttpHeaderCode::Host, b"Host", b"name"),
    ];
    assert_eq!(headers.iter().collect::<Vec<_>>().as_slice(), expected);
    assert_eq!(headers.get_single_or_empty(b"x-custom\xff"), b"custom");
    assert_eq!(headers.get_single_or_empty(HttpHeaderCode::None), b"none");
    assert_eq!(
        headers.combine_default(HttpHeaderCode::Other).as_ref(),
        b"other, custom",
    );
    for (_, name, _) in headers.iter() {
        for table in [
            HttpCommonHeaderTable::CamelCase,
            HttpCommonHeaderTable::Lowercase,
        ] {
            assert_eq!(
                HttpCommonHeaders::code_from_table_name(Some(name), table),
                HttpHeaderCode::Other,
            );
        }
    }

    assert!(headers.remove(HttpHeaderCode::Other));
    assert_eq!(
        headers.iter().collect::<Vec<_>>(),
        [expected[0], expected[3], expected[4]],
    );
}

#[test]
fn combined_header_bytes_remain_owned_across_boundaries() {
    for (name, code, value) in [
        (
            b"hOsT".as_slice(),
            HttpHeaderCode::Host,
            b"127.0.0.1".to_vec(),
        ),
        (b"hOsT".as_slice(), HttpHeaderCode::Host, vec![0x80; 22]),
        (b"hOsT".as_slice(), HttpHeaderCode::Host, vec![0xff; 23]),
        (b"X-\xff".as_slice(), HttpHeaderCode::Other, vec![0x80; 19]),
        (b"X-\xff".as_slice(), HttpHeaderCode::Other, vec![0; 20]),
    ] {
        let stored_name = if code == HttpHeaderCode::Host {
            b"Host".as_slice()
        } else {
            name
        };
        let expected = vec![(code, stored_name.to_vec(), value.clone())];
        let mut input_name = name.to_vec();
        let mut input_value = b" \r".to_vec();
        input_value.extend_from_slice(&value);
        input_value.extend_from_slice(b"\t\n");
        let mut headers = HttpHeaders::new();
        headers.add(&input_name, &input_value);
        input_name.fill(b'?');
        input_value.fill(b'?');
        drop(input_name);
        drop(input_value);
        assert_eq!(owned_entries(&headers), expected);

        let cloned = headers.clone();
        let mut copied = HttpHeaders::new();
        headers.copy_to(&mut copied);
        let raw = headers.raw_get(name);
        let mut moved = headers.take();
        assert!(headers.is_empty());
        drop(headers);
        let mut destination = HttpHeaders::new();
        destination.add("discard", "old");
        destination.move_from(&mut moved);
        assert!(moved.is_empty());
        drop(moved);
        assert_eq!(owned_entries(&destination), expected);
        drop(destination);

        for survivor in [cloned, copied] {
            assert_eq!(owned_entries(&survivor), expected);
            assert_eq!(survivor.get_single_or_empty(name), value);
        }
        assert_eq!(raw.as_ref(), value);
    }
}

#[test]
fn header_conversion_panics_preserve_publication_order() {
    use std::cell::RefCell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

    struct Name<'a> {
        events: &'a RefCell<Vec<&'static str>>,
        bytes: &'static [u8],
        panics: bool,
    }

    impl From<Name<'_>> for HttpHeaderName<'static> {
        fn from(name: Name<'_>) -> Self {
            name.events.borrow_mut().push("name");
            assert!(!name.panics, "name conversion panic");
            Self::Name(name.bytes)
        }
    }

    struct Value<'a> {
        events: &'a RefCell<Vec<&'static str>>,
        panics: bool,
    }

    impl AsRef<[u8]> for Value<'_> {
        fn as_ref(&self) -> &[u8] {
            self.events.borrow_mut().push("value");
            assert!(!self.panics, "value conversion panic");
            b" \x80\0\xff\t "
        }
    }

    let events = RefCell::new(Vec::new());
    let name = |bytes, panics| Name {
        events: &events,
        bytes,
        panics,
    };
    let value = |panics| Value {
        events: &events,
        panics,
    };
    let mut headers = HttpHeaders::new();
    headers.add("x-before", "keep");
    headers.add(HttpHeaderCode::Host, "first");
    headers.add("x-after", "keep");
    headers.add(name(b"hOsT", false), value(false));
    assert_eq!(events.borrow().as_slice(), ["name", "value"]);
    assert_eq!(
        headers.combine(HttpHeaderCode::Host, b"|").as_ref(),
        b"first|\x80\0\xff",
    );
    let before = owned_entries(&headers);

    events.borrow_mut().clear();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            headers.add(name(b"hOsT", false), value(true));
        }))
        .is_err()
    );
    assert_eq!(events.borrow().as_slice(), ["name", "value"]);
    assert_eq!(owned_entries(&headers), before);

    events.borrow_mut().clear();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            headers.add(name(b"", false), value(false));
        }))
        .is_err()
    );
    assert_eq!(events.borrow().as_slice(), ["name"]);
    assert_eq!(owned_entries(&headers), before);

    events.borrow_mut().clear();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            headers.set(name(b"hOsT", true), value(false));
        }))
        .is_err()
    );
    assert_eq!(events.borrow().as_slice(), ["name"]);
    assert_eq!(owned_entries(&headers), before);

    events.borrow_mut().clear();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            headers.set(name(b"hOsT", false), value(true));
        }))
        .is_err()
    );
    assert_eq!(events.borrow().as_slice(), ["name", "value"]);
    assert_eq!(
        owned_entries(&headers),
        vec![
            (
                HttpHeaderCode::Other,
                b"x-before".to_vec(),
                b"keep".to_vec()
            ),
            (HttpHeaderCode::Other, b"x-after".to_vec(), b"keep".to_vec()),
        ],
    );
}

/// The header names of the synthetic HTTP backend benchmark's response and
/// request heads.
const BENCH_HEADER_NAMES: [&str; 33] = [
    "content-type",
    "x-fb-debug",
    "x-fb-trip-id",
    "x-fb-connection-quality",
    "alt-svc",
    "proxy-status",
    "x-enc-edge-host",
    "x-enc-edge-hit",
    "x-enc-edge-hit-type",
    "x-edge-cache-key",
    "x-enc-origin-host",
    "x-enc-origin-hit",
    "x-enc-origin-hit-type",
    "x-origin-cache-key",
    "x-enc-edge-req-rules",
    "strict-transport-security",
    "cache-control",
    "pragma",
    "expires",
    "vary",
    "x-content-type-options",
    "x-frame-options",
    "server-timing",
    "date",
    "content-length",
    "accept",
    "x-fb-info",
    "x-fb-debug",
    "x-fb-debug-rules",
    "x-fb-fwdproxy-debug",
    "client-request-endpoint",
    "user-agent",
    "accept-encoding",
];

fn brute_force_header_code(name: &[u8]) -> HttpHeaderCode {
    HttpHeaderCode::ALL[2..]
        .iter()
        .copied()
        .find(|code| {
            HttpCommonHeaders::name(*code, HttpCommonHeaderTable::CamelCase)
                .eq_ignore_ascii_case(name)
        })
        .unwrap_or(HttpHeaderCode::Other)
}

fn assert_hash_matches_brute_force(name: &[u8]) {
    assert_eq!(
        HttpCommonHeaders::hash(name),
        brute_force_header_code(name),
        "{:?}",
        name.escape_ascii().to_string()
    );
}

#[test]
fn common_header_hash_matches_a_brute_force_lookup() {
    let mut state = 0x5eed_u64;
    let mut next_mask = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state
    };
    let names: Vec<&[u8]> = HttpHeaderCode::ALL[2..]
        .iter()
        .map(|code| HttpCommonHeaders::name(*code, HttpCommonHeaderTable::CamelCase))
        .collect();
    let longest = names.iter().map(|name| name.len()).max().unwrap();
    for name in &names {
        assert_hash_matches_brute_force(name);
        assert_hash_matches_brute_force(&name.to_ascii_lowercase());
        assert_hash_matches_brute_force(&name.to_ascii_uppercase());
        for _ in 0..64 {
            let mask = next_mask();
            let mixed: Vec<u8> = name
                .iter()
                .enumerate()
                .map(|(index, byte)| {
                    if mask >> (index % 64) & 1 == 1 {
                        byte.to_ascii_uppercase()
                    } else {
                        byte.to_ascii_lowercase()
                    }
                })
                .collect();
            assert_hash_matches_brute_force(&mixed);
        }
        for index in 1..name.len().saturating_sub(1) {
            for replacement in [b'~', b'0', b'_', 0x00, 0xff, name[index] ^ 0x01] {
                let mut near_miss = name.to_vec();
                near_miss[index] = replacement;
                assert_hash_matches_brute_force(&near_miss);
            }
        }
        for byte in 0..=u8::MAX {
            let mut first = name.to_vec();
            first[0] = byte;
            assert_hash_matches_brute_force(&first);
            let mut last = name.to_vec();
            *last.last_mut().unwrap() = byte;
            assert_hash_matches_brute_force(&last);
        }
        assert_hash_matches_brute_force(&[name, b"x".as_slice()].concat());
        assert_hash_matches_brute_force(&name[..name.len() - 1]);
    }
    assert_hash_matches_brute_force(b"");
    for length in [1, longest, longest + 1] {
        assert_hash_matches_brute_force(&vec![b'a'; length]);
        assert_hash_matches_brute_force(&vec![b'-'; length]);
    }
    for name in BENCH_HEADER_NAMES {
        assert_hash_matches_brute_force(name.as_bytes());
        assert_hash_matches_brute_force(name.to_ascii_uppercase().as_bytes());
    }
    assert_eq!(HttpCommonHeaders::hash(b""), HttpHeaderCode::Other);
}

fn common_header_names() -> Vec<&'static [u8]> {
    HttpHeaderCode::ALL[2..]
        .iter()
        .map(|code| HttpCommonHeaders::name(*code, HttpCommonHeaderTable::CamelCase))
        .collect()
}

#[test]
fn common_header_hash_matches_brute_force_for_every_single_byte_edit() {
    let inserted = [b'a', b'A', b'-', b'_', 0x00, 0x0d, 0xff];
    for name in common_header_names() {
        for index in 0..name.len() {
            for byte in 0..=u8::MAX {
                let mut substituted = name.to_vec();
                substituted[index] = byte;
                assert_hash_matches_brute_force(&substituted);
            }
            let mut deleted = name.to_vec();
            deleted.remove(index);
            assert_hash_matches_brute_force(&deleted);
            for byte in inserted.into_iter().chain([name[index]]) {
                let mut extended = name.to_vec();
                extended.insert(index, byte);
                assert_hash_matches_brute_force(&extended);
            }
        }
        for length in 0..name.len() {
            assert_hash_matches_brute_force(&name[..length]);
            assert_hash_matches_brute_force(&name[name.len() - length..]);
        }
    }
}

#[test]
fn common_header_hash_matches_brute_force_for_every_short_name() {
    for first in 0..=u8::MAX {
        assert_hash_matches_brute_force(&[first]);
        for second in 0..=u8::MAX {
            assert_hash_matches_brute_force(&[first, second]);
        }
    }
    let mut alphabet: Vec<u8> = common_header_names()
        .into_iter()
        .flatten()
        .flat_map(|byte| [byte.to_ascii_lowercase(), byte.to_ascii_uppercase()])
        .chain([0x00, 0x0d, 0x7f, 0xff])
        .collect();
    alphabet.sort_unstable();
    alphabet.dedup();
    for &first in &alphabet {
        for &second in &alphabet {
            for &third in &alphabet {
                assert_hash_matches_brute_force(&[first, second, third]);
            }
        }
    }
}

#[test]
fn common_header_lookup_probes_one_slot_and_reads_names_a_word_at_a_time() {
    use crate::headers::CommonHeaderLookupWork;
    use crate::headers::take_common_header_lookup_work;

    fn work(name: &[u8]) -> (HttpHeaderCode, CommonHeaderLookupWork) {
        take_common_header_lookup_work();
        let code = HttpCommonHeaders::hash(name);
        (code, take_common_header_lookup_work())
    }
    let expected = |probes, name_reads| CommonHeaderLookupWork { probes, name_reads };
    // One slot, then two overlapping reads for a name of up to 16 bytes and
    // one more read per further 8 bytes.
    let literals = [
        (b"TE".as_slice(), HttpHeaderCode::Te, 2),
        (b"date", HttpHeaderCode::Date, 2),
        (b"Content-Type", HttpHeaderCode::ContentType, 2),
        (b"content-length", HttpHeaderCode::ContentLength, 2),
        (
            b"ACCESS-CONTROL-ALLOW-METHODS",
            HttpHeaderCode::AccessControlAllowMethods,
            4,
        ),
        (
            b"cross-origin-embedder-policy-report-only",
            HttpHeaderCode::CrossOriginEmbedderPolicyReportOnly,
            5,
        ),
    ];
    assert_eq!(
        literals.map(|(name, _, _)| work(name)),
        literals.map(|(_, code, name_reads)| (code, expected(1, name_reads))),
    );

    let words = |length: usize| length.div_ceil(8).max(2);
    let mut lookups = 0;
    let mut over = Vec::new();
    for name in common_header_names() {
        for spelling in [
            name.to_vec(),
            name.to_ascii_lowercase(),
            name.to_ascii_uppercase(),
        ] {
            lookups += 1;
            let (_, counted) = work(&spelling);
            if counted != expected(1, words(spelling.len())) {
                over.push((spelling.escape_ascii().to_string(), counted));
            }
        }
    }
    for name in BENCH_HEADER_NAMES
        .iter()
        .map(|name| name.as_bytes().to_vec())
        .chain(common_header_names().into_iter().flat_map(|name| {
            let mut near_miss = name.to_vec();
            near_miss[name.len() / 2] ^= 0x20;
            [near_miss, [name, b"!".as_slice()].concat()]
        }))
    {
        lookups += 1;
        let (_, counted) = work(&name);
        if counted.probes > 1 || counted.name_reads > words(name.len()) {
            over.push((name.escape_ascii().to_string(), counted));
        }
    }
    assert!(
        over.is_empty(),
        "{} of {lookups} lookups probe more than one slot or read the name more than twice or \
         more than once per 8 bytes; the first: {:?}",
        over.len(),
        &over[..over.len().min(6)],
    );
    assert_eq!(work(b""), (HttpHeaderCode::Other, expected(0, 0)));
}

#[test]
fn empty_and_near_miss_names_keep_their_query_results() {
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

    let mut headers = HttpHeaders::new();
    headers.add("x-custom", "custom");
    headers.add(HttpHeaderCode::ContentLength, "5");
    for name in [
        b"".as_slice(),
        b"content-lengtx",
        b"dontent-length",
        b"content_length",
        b"content-length ",
        b"content-lengt",
        b"x-custon",
        b"y-custom",
        b"x-custom!",
        b"@ONTENT-LENGTH",
    ] {
        let context = name.escape_ascii().to_string();
        assert!(!headers.exists(name), "{context}");
        assert!(!headers.get_single(name).exists, "{context}");
        assert_eq!(headers.get_single(name).value, None, "{context}");
        assert_eq!(headers.number_of_values(name), 0, "{context}");
        let mut removed = headers.clone();
        assert!(!removed.remove(name), "{context}");
        assert_eq!(removed, headers, "{context}");
    }
    for (name, value) in [
        (b"CONTENT-LENGTH".as_slice(), b"5".as_slice()),
        (b"X-CUSTOM", b"custom"),
    ] {
        assert!(headers.exists(name));
        assert_eq!(headers.get_single(name).value, Some(value));
    }

    headers.add(HttpHeaderCode::Other, b"other");
    assert!(headers.exists(b""));
    assert_eq!(headers.get_single(b"").value, Some(b"other".as_slice()));
    let mut removed = headers.clone();
    assert!(removed.remove(b""));
    assert!(!removed.exists(b""));
    assert_eq!(removed.number_of_values(b"x-custom"), 1);

    let panic = catch_unwind(AssertUnwindSafe(|| headers.set(b"", b"value")))
        .expect_err("an empty name panics");
    let message = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or_default();
    assert_eq!(message, "HTTP header names must not be empty");
}

#[test]
fn first_insert_into_unallocated_headers_reserves_sixteen() {
    let mut added = HttpHeaders::unallocated();
    assert_eq!(added.capacity(), 0);
    added.add("x-a", "1");
    assert_eq!(added.capacity(), 16);
    fill_headers(&mut added, 15, b'a');
    assert_eq!(added.capacity(), 16);

    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::Connection, "close");
    headers.add(HttpHeaderCode::Host, "h");
    let mut stripped = HttpHeaders::unallocated();
    headers.strip_per_hop_headers(&mut stripped, false, None);
    assert_eq!(stripped.capacity(), 16);
    assert_eq!(
        owned_entries(&stripped),
        [(
            HttpHeaderCode::Connection,
            b"Connection".to_vec(),
            b"close".to_vec()
        )]
    );

    let mut headers = HttpHeaders::new();
    headers.add(HttpHeaderCode::Connection, "x-hop");
    headers.add("x-hop", "1");
    let mut transferred = HttpHeaders::unallocated();
    headers.strip_per_hop_headers(&mut transferred, false, None);
    assert_eq!(transferred.capacity(), 16);
    assert_eq!(
        owned_entries(&transferred),
        [
            (HttpHeaderCode::Other, b"x-hop".to_vec(), b"1".to_vec()),
            (
                HttpHeaderCode::Connection,
                b"Connection".to_vec(),
                b"x-hop".to_vec()
            ),
        ]
    );

    let mut small = HttpHeaders::new();
    fill_headers(&mut small, 3, b'a');
    let mut copied = HttpHeaders::unallocated();
    small.copy_to(&mut copied);
    assert_eq!(copied.capacity(), 16);
    assert_eq!(copied, small);
    let mut large = HttpHeaders::new();
    fill_headers(&mut large, 20, b'a');
    let mut copied = HttpHeaders::unallocated();
    large.copy_to(&mut copied);
    assert!(copied.capacity() >= 20);
    assert_eq!(copied, large);

    let mut shared = HttpHeaders::unallocated();
    shared.share_entries();
    let sibling = shared.clone();
    shared.add("x-a", "1");
    assert_eq!(shared.capacity(), 16, "16 rather than the vector's first 4");
    assert!(sibling.is_empty());
    let mut alone = HttpHeaders::unallocated();
    alone.share_entries();
    alone.add("x-a", "1");
    assert_eq!(alone.capacity(), 16);

    let mut cleared = HttpHeaders::unallocated();
    fill_headers(&mut cleared, 20, b'a');
    let capacity = cleared.capacity();
    cleared.clear();
    assert_eq!(cleared.capacity(), capacity);

    assert_eq!(HttpHeaders::new().capacity(), 16);
    assert_eq!(HttpHeaders::default().capacity(), 16);
    // Taking moves the storage out, as Proxygen's move constructor leaves the
    // source with capacity 0; the source's next insert reserves 16 again.
    let mut taken = HttpHeaders::unallocated();
    taken.add("x-a", "1");
    let moved = taken.take();
    assert_eq!(taken.capacity(), 0);
    assert_eq!(moved.capacity(), 16);
    taken.add("x-b", "2");
    assert_eq!(taken.capacity(), 16);
}

#[test]
fn reserve_keeps_entries_in_place_for_reserved_adds() {
    for mut headers in [HttpHeaders::unallocated(), HttpHeaders::new()] {
        headers.reserve(25);
        assert!(headers.capacity() >= 25);
        let address = headers.entries_address();
        fill_headers(&mut headers, 25, b'a');
        assert_eq!(headers.entries_address(), address);
        assert_eq!(headers.len(), 25);
    }

    let mut small = HttpHeaders::unallocated();
    small.reserve(3);
    assert_eq!(small.capacity(), 16);
    let mut roomy = HttpHeaders::new();
    roomy.add("x-a", "1");
    let address = roomy.entries_address();
    roomy.reserve(15);
    assert_eq!(roomy.entries_address(), address);
    assert_eq!(roomy.capacity(), 16);
}

#[test]
fn reserve_never_detaches_shared_entries_with_room() {
    let mut headers = HttpHeaders::new();
    fill_headers(&mut headers, 5, b'a');
    headers.share_entries();
    let sibling = headers.clone();
    let address = headers.entries_address();
    assert_eq!(sibling.entries_address(), address);
    for additional in [0, 1, 11] {
        headers.reserve(additional);
        assert_eq!(headers.entries_address(), address, "{additional}");
        assert_eq!(sibling.entries_address(), address, "{additional}");
    }

    let mut detached = headers.clone();
    detached.reserve(12);
    assert_ne!(detached.entries_address(), address);
    assert!(detached.capacity() >= 17);
    assert_eq!(detached, sibling);
    assert_eq!(headers.entries_address(), address);
    assert_eq!(sibling.entries_address(), address);
}

#[test]
fn reserve_with_an_overflowing_request_does_not_panic() {
    let mut owned = HttpHeaders::new();
    fill_headers(&mut owned, 3, b'a');
    let expected = owned.clone();
    let mut unallocated = HttpHeaders::unallocated();
    let mut shared = expected.clone();
    shared.share_entries();
    let sibling = shared.clone();
    for additional in [
        usize::MAX,
        usize::MAX - 1,
        usize::MAX / 2,
        isize::MAX as usize / 8,
    ] {
        owned.reserve(additional);
        assert_eq!(owned, expected, "{additional}");
        unallocated.reserve(additional);
        assert!(unallocated.is_empty(), "{additional}");
        shared.reserve(additional);
        assert_eq!(shared, expected, "{additional}");
    }
    assert_eq!(sibling, expected);
    owned.add("x-after", "1");
    assert_eq!(owned.get_single_or_empty("x-after"), b"1");
    unallocated.add("x-after", "1");
    assert_eq!(unallocated.capacity(), 16);
    assert_eq!(unallocated.get_single_or_empty("x-after"), b"1");
}

#[test]
fn add_uncommon_matches_add() {
    let fields: Vec<(String, String)> = (0..20)
        .map(|i| {
            let name = match i % 3 {
                0 => format!("x-uncommon-{i}"),
                1 => format!("X-Mixed-Case-{i}"),
                _ => "x-repeated".to_owned(),
            };
            (name, format!(" \tvalue-{i}\r\n"))
        })
        .collect();
    let (added, uncommon) = fields.iter().fold(
        (HttpHeaders::new(), HttpHeaders::new()),
        |(mut added, mut uncommon), (name, value)| {
            added.add(name.as_bytes(), value);
            uncommon.add_uncommon(name.as_bytes(), value.as_bytes());
            (added, uncommon)
        },
    );

    assert_eq!(added, uncommon);
    assert!(
        added.iter().eq(uncommon.iter()),
        "add_uncommon stores the same code, name and trimmed value as add, in the same order"
    );
    for (name, _) in &fields {
        let upper = name.to_ascii_uppercase();
        for query in [name.as_str(), upper.as_str()] {
            assert_eq!(
                (
                    added.number_of_values(query),
                    added.get_single_or_empty(query),
                    added.combine_default(query),
                ),
                (
                    uncommon.number_of_values(query),
                    uncommon.get_single_or_empty(query),
                    uncommon.combine_default(query),
                ),
                "lookups of {query} agree"
            );
        }
    }
    assert_eq!(uncommon.number_of_values("x-repeated"), 6);
    assert_eq!(uncommon.get_single_or_empty("x-uncommon-0"), b"value-0");
}
