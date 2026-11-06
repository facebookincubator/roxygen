use crate::url::ParseUrl;
use crate::url::Url;
use crate::url::UrlMode;
use crate::url::UrlParts;

fn assert_parse(
    input: &[u8],
    scheme: &[u8],
    path: &[u8],
    query: &[u8],
    host: &[u8],
    port: u16,
    authority: &[u8],
    valid: bool,
    strict: bool,
) {
    let parsed = ParseUrl::parse_url_maybe_invalid(input, strict);
    assert_eq!(parsed.valid(), valid);
    if valid {
        assert_eq!(parsed.url(), input);
        assert_eq!(parsed.scheme(), scheme);
        assert_eq!(parsed.path(), path);
        assert_eq!(parsed.query(), query);
        assert_eq!(parsed.host(), host);
        assert_eq!(parsed.port(), port);
        assert_eq!(parsed.authority(), authority);
    }
}

fn assert_host_is_ip_address(input: &[u8], expected: bool) {
    let parsed = ParseUrl::parse_url_maybe_invalid(input, false);
    assert!(!expected || parsed.valid());
    assert_eq!(parsed.host_is_ip_address(), expected);
}

#[test]
fn get_path_and_query_parse_url() {
    for (input, expected_path, expected_query) in [
        (
            b"http://localhost:80/foo?bar#qqq".as_slice(),
            b"/foo".as_slice(),
            b"bar".as_slice(),
        ),
        (b"localhost:80/foo?bar#qqq", b"/foo", b"bar"),
        (b"localhost", b"/", b""),
        (b"/f/o/o?bar#qqq", b"/f/o/o", b"bar"),
        (b"#?hello", b"", b""),
    ] {
        let parsed = ParseUrl::parse_url_maybe_invalid(input, false);
        assert_eq!(parsed.url(), input);
        assert_eq!(parsed.request_path(), expected_path);
        assert_eq!(parsed.request_query(), expected_query);
    }
}

#[test]
fn urltest_root() {
    let url = Url::from_parts(UrlParts {
        scheme: b"http",
        host: b"www.facebook.com",
        ..UrlParts::default()
    });
    assert!(url.is_valid());
    assert_eq!(url.url(), b"http://www.facebook.com/");
}

#[test]
fn urltest_capital_sheme() {
    let url = Url::from_parts(UrlParts {
        scheme: b"HTTPS",
        host: b"www.facebook.com",
        ..UrlParts::default()
    });
    assert!(url.is_valid());
    assert_eq!(url.url(), b"https://www.facebook.com/");
}

#[test]
fn urltest_invalid() {
    let input = b"https://www.facebook.com/foo\xff";
    let strict = Url::parse(input, true, UrlMode::Strict);
    assert!(!strict.is_valid());
    assert_eq!(strict.host(), b"");
    assert_eq!(strict.path(), b"");

    let compatible = Url::parse(input, true, UrlMode::StrictCompat);
    assert!(compatible.is_valid());
    assert_eq!(compatible.host(), b"www.facebook.com");
    assert_eq!(compatible.path(), b"/foo\xff");
}

#[test]
fn urltest_non_httpscheme() {
    let url = Url::parse(b"masque://www.facebook.com/foo", true, UrlMode::Strict);
    assert!(!url.is_valid());
    assert_eq!(url.host(), b"www.facebook.com");
    assert_eq!(url.path(), b"/foo");
}

#[test]
fn urltest_get_port() {
    for (input, expected_port, expected_host_and_port) in [
        (
            b"https://www.facebook.com/foo".as_slice(),
            443,
            b"www.facebook.com".as_slice(),
        ),
        (b"http://www.facebook.com/foo", 80, b"www.facebook.com"),
        (
            b"http://www.facebook.com:8081/foo",
            8081,
            b"www.facebook.com:8081",
        ),
        (
            b"https://www.facebook.com:8081/foo",
            8081,
            b"www.facebook.com:8081",
        ),
    ] {
        let url = Url::parse(input, true, UrlMode::Strict);
        assert!(url.is_valid());
        assert_eq!(url.port(), expected_port);
        assert_eq!(
            url.host_and_port_omit_default().as_ref(),
            expected_host_and_port,
        );
    }
}

#[test]
fn parse_url_host_no_brackets() {
    for (input, host, host_no_brackets) in [
        (b"/bar".as_slice(), b"".as_slice(), b"".as_slice()),
        (b"localhost", b"localhost", b"localhost"),
        (b"localhost:80", b"localhost", b"localhost"),
        (b"[::1]:80", b"[::1]", b"::1"),
        (b"1.2.3.4:80", b"1.2.3.4", b"1.2.3.4"),
    ] {
        let parsed = ParseUrl::parse_url(input, false).expect("URL should parse");
        assert_eq!(parsed.host(), host);
        assert_eq!(parsed.host_no_brackets(), host_no_brackets);
    }
}

#[test]
fn parse_url_fully_formed_url() {
    assert_parse(
        b"http://localhost:80/foo?bar#qqq",
        b"http",
        b"/foo",
        b"bar",
        b"localhost",
        80,
        b"localhost:80",
        true,
        false,
    );
    assert_parse(
        b"http://localhost:80/foo?bar",
        b"http",
        b"/foo",
        b"bar",
        b"localhost",
        80,
        b"localhost:80",
        true,
        false,
    );
    assert_parse(
        b"http://localhost:80/foo",
        b"http",
        b"/foo",
        b"",
        b"localhost",
        80,
        b"localhost:80",
        true,
        false,
    );
    assert_parse(
        b"http://localhost:80/",
        b"http",
        b"/",
        b"",
        b"localhost",
        80,
        b"localhost:80",
        true,
        false,
    );
    assert_parse(
        b"http://localhost:80",
        b"http",
        b"",
        b"",
        b"localhost",
        80,
        b"localhost:80",
        true,
        false,
    );
    assert_parse(
        b"http://localhost",
        b"http",
        b"",
        b"",
        b"localhost",
        0,
        b"localhost",
        true,
        false,
    );
    assert_parse(
        b"http://[2401:db00:2110:3051:face:0:3f:0]/",
        b"http",
        b"/",
        b"",
        b"[2401:db00:2110:3051:face:0:3f:0]",
        0,
        b"[2401:db00:2110:3051:face:0:3f:0]",
        true,
        false,
    );
    assert_parse(
        b"http://[2401:db00:2110:3051:face:0:3f:0]:8080/",
        b"http",
        b"/",
        b"",
        b"[2401:db00:2110:3051:face:0:3f:0]",
        8080,
        b"[2401:db00:2110:3051:face:0:3f:0]:8080",
        true,
        false,
    );
}

#[test]
fn parse_url_valid_non_http_scheme() {
    for scheme in [b"https".as_slice(), b"rtmp", b"ftp", b"proxygen", b"test"] {
        let mut input = scheme.to_vec();
        input.extend_from_slice(b"://localhost:80/foo?bar#qqq");
        assert_parse(
            &input,
            scheme,
            b"/foo",
            b"bar",
            b"localhost",
            80,
            b"localhost:80",
            true,
            false,
        );
    }
}

#[test]
fn parse_url_invalid_scheme() {
    for input in [
        b"test123://localhost:80".as_slice(),
        b"test.1://localhost:80",
        b"://localhost:80",
        b"123://localhost:80",
        b"---://localhost:80",
    ] {
        assert!(!ParseUrl::parse_url_maybe_invalid(input, false).valid());
        assert!(ParseUrl::parse_url(input, false).is_none());
    }
}

#[test]
fn parse_url_no_scheme() {
    for (input, path, query, host, port, authority) in [
        (
            b"localhost:80/foo?bar#qqq".as_slice(),
            b"/foo".as_slice(),
            b"bar".as_slice(),
            b"localhost".as_slice(),
            80,
            b"localhost:80".as_slice(),
        ),
        (
            b"localhost:80/foo?bar",
            b"/foo",
            b"bar",
            b"localhost",
            80,
            b"localhost:80",
        ),
        (
            b"localhost:80/foo",
            b"/foo",
            b"",
            b"localhost",
            80,
            b"localhost:80",
        ),
        (
            b"localhost:80/",
            b"/",
            b"",
            b"localhost",
            80,
            b"localhost:80",
        ),
        (b"localhost:80", b"", b"", b"localhost", 80, b"localhost:80"),
        (b"localhost", b"", b"", b"localhost", 0, b"localhost"),
    ] {
        assert_parse(input, b"", path, query, host, port, authority, true, false);
    }
}

#[test]
fn parse_url_no_scheme_ip() {
    assert_parse(
        b"1.2.3.4:54321/foo?bar#qqq",
        b"",
        b"/foo",
        b"bar",
        b"1.2.3.4",
        54321,
        b"1.2.3.4:54321",
        true,
        false,
    );
    assert_parse(
        b"[::1]:80/foo?bar",
        b"",
        b"/foo",
        b"bar",
        b"[::1]",
        80,
        b"[::1]:80",
        true,
        false,
    );
    assert_parse(
        b"[::1]/foo?bar",
        b"",
        b"/foo",
        b"bar",
        b"[::1]",
        0,
        b"[::1]",
        true,
        false,
    );
}

#[test]
fn parse_url_path_only() {
    for (input, path, query) in [
        (
            b"/f/o/o?bar#qqq".as_slice(),
            b"/f/o/o".as_slice(),
            b"bar".as_slice(),
        ),
        (b"/f/o/o?bar", b"/f/o/o", b"bar"),
        (b"/f/o/o", b"/f/o/o", b""),
        (b"/", b"/", b""),
        (b"?foo=bar", b"", b"foo=bar"),
        (b"?#", b"", b""),
        (b"#/foo/bar", b"", b""),
    ] {
        assert_parse(input, b"", path, query, b"", 0, b"", true, false);
    }
}

#[test]
fn parse_url_query_is_url() {
    assert_parse(
        b"/?ids=http://vlc.afreecodec.com/download/",
        b"",
        b"/",
        b"ids=http://vlc.afreecodec.com/download/",
        b"",
        0,
        b"",
        true,
        false,
    );
    assert_parse(
        b"/plugins/facepile.php?href=http://www.vakan.nl/hotels",
        b"",
        b"/plugins/facepile.php",
        b"href=http://www.vakan.nl/hotels",
        b"",
        0,
        b"",
        true,
        false,
    );
}

#[test]
fn parse_url_invalid_url() {
    for input in [
        b"http://tel:198433511/".as_slice(),
        b"localhost:80/foo#bar?qqq",
        b"#?",
        b"#?hello",
        b"[::1/foo?bar",
        b"",
        b"http://tel:198433511/test\n",
        b"/test\n",
        b"localhost:80abc",
        b"localhost:0x50",
        b"localhost:-80",
        b"localhost:+80",
        b"localhost:80.0",
    ] {
        assert!(!ParseUrl::parse_url_maybe_invalid(input, false).valid());
    }

    assert!(!ParseUrl::parse_url_maybe_invalid(b"http://foo.com/test\xff", true).valid());
    assert_parse(
        b"http://foo.com/test\xff",
        b"http",
        b"/test\xff",
        b"",
        b"foo.com",
        0,
        b"foo.com",
        true,
        false,
    );
    assert!(!ParseUrl::parse_url_maybe_invalid(b"test\xff", true).valid());
    assert_parse(
        b"/test\xff",
        b"",
        b"/test\xff",
        b"",
        b"",
        0,
        b"",
        true,
        false,
    );
}

#[test]
fn parse_url_is_host_ipaddress() {
    for input in [
        b"http://127.0.0.1:80".as_slice(),
        b"127.0.0.1:80",
        b"127.0.0.1",
        b"http://[::1]:80",
        b"[::1]",
        b"[::1]:80",
        b"[::AB]",
    ] {
        assert_host_is_ip_address(input, true);
    }
    for input in [
        b"http://localhost:80".as_slice(),
        b"http://localhost",
        b"localhost:80",
        b"localhost",
        b"1.2.3.-1",
        b"1.2.3.999",
        b"::1",
        b"[::99999999]",
        b"",
        b"127.0.0.1:80/foo#bar?qqq",
    ] {
        assert_host_is_ip_address(input, false);
    }
}

#[test]
fn parse_url_supported_scheme() {
    for input in [
        b"http://example.com".as_slice(),
        b"http://example.com/",
        b"https://example.com",
        b"https://example.com/",
        b"//example.com",
        b"/path",
    ] {
        assert!(ParseUrl::is_supported_scheme(input));
    }
}

#[test]
fn parse_url_not_supported_scheme() {
    for input in [
        b"://".as_slice(),
        b"http ://example.com/",
        b"://example.com",
        b"HTTP://EXAMPLE.COM/",
        b"ftp://example.com",
        b"httpx://example.com/",
        b"shttp://example.com/",
        b"httpss://example.com/",
    ] {
        assert!(!ParseUrl::is_supported_scheme(input));
    }
}

#[test]
fn parse_url_port_overflow() {
    let storage = b"http://foo:12345";
    let visible = &storage[..storage.len() - 4];
    let parsed = ParseUrl::parse_url(visible, true).expect("visible URL should parse");
    assert_eq!(parsed.port(), 1);
}

#[test]
fn parse_url_get_query_param() {
    let parsed = ParseUrl::parse_url(b"localhost/?foo=1&bar=2&baz&bazz=3&bak=", false)
        .expect("query URL should parse");
    let query = parsed.query();

    assert_eq!(
        ParseUrl::get_query_param(query, b"foo"),
        Some(b"1".as_slice())
    );
    assert_eq!(
        ParseUrl::get_query_param(query, b"bar"),
        Some(b"2".as_slice())
    );
    assert_eq!(
        ParseUrl::get_query_param(query, b"baz"),
        Some(b"".as_slice())
    );
    assert_eq!(
        ParseUrl::get_query_param(query, b"bazz"),
        Some(b"3".as_slice())
    );
    assert_eq!(
        ParseUrl::get_query_param(query, b"bak"),
        Some(b"".as_slice())
    );
    assert_eq!(ParseUrl::get_query_param(query, b"fooo"), None);
}

#[test]
fn url_component_rendering_and_equality_follow_the_source() {
    let url = Url::from_parts(UrlParts {
        scheme: b"HTTP",
        host: b"example.com",
        port: 8080,
        path: b"path",
        query: b"a=1",
        fragment: b"frag",
    });
    assert_eq!(url.scheme(), b"http");
    assert_eq!(url.host_and_port().as_ref(), b"example.com:8080");
    assert_eq!(url.path(), b"path");
    assert_eq!(url.query(), b"a=1");
    assert_eq!(url.fragment(), b"frag");
    assert_eq!(url.url(), b"http://example.com:8080/path?a=1#frag");
    assert_eq!(url.make_relative_url().as_ref(), b"path?a=1#frag");
    assert_eq!(
        Url::create_url(b"http", b"example.com:8080", b"path", b"a=1", b"frag").as_ref(),
        url.url(),
    );
}

#[test]
fn public_api_owns_url_storage() {
    fn assert_owned<T: Send + Sync + 'static>() {}
    assert_owned::<ParseUrl>();
    assert_owned::<Url>();

    let mut input = b"http://example.com/path?query#fragment".to_vec();
    let parsed = ParseUrl::parse_url(&input, false).expect("URL should parse");
    input.fill(b'x');
    drop(input);
    assert_eq!(parsed.url(), b"http://example.com/path?query#fragment");
    assert_eq!(parsed.host(), b"example.com");
    assert_eq!(parsed.path(), b"/path");
    assert_eq!(parsed.query(), b"query");
    assert_eq!(parsed.fragment(), b"fragment");

    for (value, fragment) in [
        (b"/a?b#cd".as_slice(), b"cd".as_slice()),
        (b"/a?b#cde", b"cde"),
    ] {
        let parsed = {
            let mut input = value.to_vec();
            let parsed = ParseUrl::parse_url(&input, false).expect("URL should parse");
            input.fill(b'x');
            drop(input);
            parsed
        };
        assert_eq!(parsed.url(), value);
        assert_eq!(parsed.path(), b"/a");
        assert_eq!(parsed.query(), b"b");
        assert_eq!(parsed.fragment(), fragment);
    }
}

#[test]
fn fully_formed_urls_follow_http_parser_authority_and_strict_character_rules() {
    for input in [
        b"http://host#fragment".as_slice(),
        b"http://host:",
        b"http://host:65536",
        b"http://user@@host/",
        b"http://[::1/",
        b"http://[]/",
        b"http://host%name/",
    ] {
        assert!(!ParseUrl::parse_url_maybe_invalid(input, false).valid());
    }

    for character in *b"\"<>\\^`{|}" {
        let mut input = b"http://host/".to_vec();
        input.push(character);
        assert!(ParseUrl::parse_url_maybe_invalid(&input, false).valid());
        assert!(ParseUrl::parse_url_maybe_invalid(&input, true).valid());
    }

    assert!(ParseUrl::parse_url_maybe_invalid(b"http://host/\xff", false).valid());
    assert!(!ParseUrl::parse_url_maybe_invalid(b"http://host/\xff", true).valid());
}

#[test]
fn fully_formed_url_skips_repeated_fragment_start_delimiters() {
    let parsed = ParseUrl::parse_url(b"http://host/path##frag", false)
        .expect("URL with repeated fragment delimiters should parse");
    assert_eq!(parsed.path(), b"/path");
    assert_eq!(parsed.fragment(), b"frag");

    let parsed = ParseUrl::parse_url(b"http://host/path###?frag#tail", true)
        .expect("fragment content after repeated delimiters should parse");
    assert_eq!(parsed.fragment(), b"?frag#tail");
}

#[test]
fn invalid_partial_port_retains_the_parsed_numeric_prefix() {
    let parsed = ParseUrl::parse_url_maybe_invalid(b"localhost:80abc", false);
    assert!(!parsed.valid());
    assert_eq!(parsed.authority(), b"localhost:80abc");
    assert_eq!(parsed.host(), b"");
    assert_eq!(parsed.port(), 80);
}

#[test]
fn invalid_partial_url_preserves_raw_components() {
    let parsed = ParseUrl::parse_url_maybe_invalid(b"localhost:80/foo#bar?qqq", false);
    assert!(!parsed.valid());
    assert_eq!(parsed.path(), b"/foo");
    assert_eq!(parsed.query(), b"");
    assert_eq!(parsed.fragment(), b"");
    assert_eq!(parsed.request_path(), b"");
    assert_eq!(parsed.request_query(), b"");

    let parsed = ParseUrl::parse_url_maybe_invalid(b"localhost:80abc/foo?bar#frag", false);
    assert!(!parsed.valid());
    assert_eq!(parsed.path(), b"/foo");
    assert_eq!(parsed.query(), b"bar");
    assert_eq!(parsed.fragment(), b"frag");
    assert_eq!(parsed.port(), 80);
    assert_eq!(parsed.request_path(), b"");
    assert_eq!(parsed.request_query(), b"");
}

#[test]
fn redirect_destination_matches_absolute_relative_and_fallback_paths() {
    assert_eq!(
        ParseUrl::get_redirect_destination(
            b"http://old.example/original",
            b"https",
            b"https://new.example/next?x=1",
            b"fallback.example",
        )
        .as_deref(),
        Some(b"https://new.example/next?x=1".as_slice()),
    );
    assert_eq!(
        ParseUrl::get_redirect_destination(
            b"http://old.example:8080/original",
            b"https",
            b"/next?x=1",
            b"fallback.example",
        )
        .as_deref(),
        Some(b"https://old.example:8080/next?x=1".as_slice()),
    );
    assert_eq!(
        ParseUrl::get_redirect_destination(
            b"/original",
            b"http",
            b"?next=1",
            b"fallback.example:81",
        )
        .as_deref(),
        Some(b"http://fallback.example:81?next=1".as_slice()),
    );
    assert_eq!(
        ParseUrl::get_redirect_destination(
            b"http://old.example/original",
            b"http",
            b"#?invalid",
            b"fallback.example",
        ),
        None,
    );
    assert_eq!(
        ParseUrl::get_redirect_destination(b"/original", b"http", b"/next", b"/missing-host",),
        None,
    );
}
