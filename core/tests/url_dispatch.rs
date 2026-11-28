/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ops::Range;

use super::ABSOLUTE_URL_SCANS;
use super::ParseUrl;
use super::UrlComponents;
use super::find_bytes;
use super::parse_components;
use super::parse_fully_formed;
use super::parse_non_fully_formed;

/// Bytes that reach every branch of the dispatch and of both parsers: path,
/// scheme, query and fragment delimiters, scheme and host letters, a port
/// digit, user-info and IPv6 brackets, a host dot, a user-info `%`, and the
/// bytes `validate_url` rejects (space, DEL, and non-ASCII in strict mode).
const ALPHABET: &[u8] = b"/:ah1?#@[].%\x20\x7f\x80";

/// A `parse_components` that searches for `://` even in leading-slash targets,
/// for comparison with the dispatch that skips that search.
fn scheme_scanning_parse(url: &[u8], strict: bool) -> UrlComponents {
    if url == b"/" {
        return UrlComponents {
            path: 0..1,
            valid: true,
            ..UrlComponents::default()
        };
    }

    match find_bytes(url, b"://") {
        Some(scheme_end)
            if scheme_end != 0
                && url[..scheme_end]
                    .iter()
                    .all(|byte| byte.is_ascii_alphabetic()) =>
        {
            parse_fully_formed(url, scheme_end, strict)
        }
        _ => parse_non_fully_formed(url, strict),
    }
}

type Fields<'a> = (
    &'a [u8],
    &'a [u8],
    &'a [u8],
    Range<usize>,
    Range<usize>,
    Range<usize>,
    u16,
    bool,
);

fn fields(components: &UrlComponents) -> Fields<'_> {
    (
        &components.scheme,
        &components.authority,
        &components.host,
        components.path.clone(),
        components.query.clone(),
        components.fragment.clone(),
        components.port,
        components.valid,
    )
}

fn assert_dispatch_matches(url: &[u8]) {
    for strict in [false, true] {
        assert_eq!(
            fields(&parse_components(url, strict)),
            fields(&scheme_scanning_parse(url, strict)),
            "target {:?} strict={strict}",
            url.escape_ascii().to_string(),
        );
    }
}

/// Calls `check` with `prefix` followed by each `len`-byte string over
/// `ALPHABET`.
fn for_each_target(prefix: &[u8], len: u32, mut check: impl FnMut(&[u8])) {
    let mut target = Vec::new();
    for index in 0..ALPHABET.len().pow(len) {
        target.clear();
        target.extend_from_slice(prefix);
        target.extend((0..len).scan(index, |rest, _| {
            let byte = ALPHABET[*rest % ALPHABET.len()];
            *rest /= ALPHABET.len();
            Some(byte)
        }));
        check(&target);
    }
}

#[test]
fn leading_slash_targets_skip_the_absolute_url_scan() {
    for (target, scans) in [
        (b"/".as_slice(), 0),
        (b"/generate", 0),
        (b"/v1/chat/completions?stream=true#done", 0),
        (b"//host:80/path", 0),
        (b"/http://host/path", 0),
        (b"/\x20", 0),
        (b"http://host/path", 1),
        (b"localhost:80/foo", 1),
        (b"*", 1),
        (b"", 1),
    ] {
        ABSOLUTE_URL_SCANS.take();
        ParseUrl::parse_url_maybe_invalid(target, false);
        assert_eq!(
            ABSOLUTE_URL_SCANS.take(),
            scans,
            "target {:?}",
            target.escape_ascii().to_string(),
        );
    }
}

#[test]
fn dispatch_matches_the_scheme_scan_for_every_short_target() {
    for len in 0..=5 {
        for_each_target(b"", len, assert_dispatch_matches);
    }
}

#[test]
fn dispatch_matches_the_scheme_scan_for_every_leading_slash_target_up_to_seven_bytes() {
    for len in 0..=6 {
        for_each_target(b"/", len, assert_dispatch_matches);
    }
}

#[test]
fn dispatch_matches_the_scheme_scan_for_origin_form_and_absolute_targets() {
    for target in [
        b"/generate".as_slice(),
        b"/v1/chat/completions?stream=true#done",
        b"/a/b?c=d&e=f#g?h#i",
        b"//host:80/path",
        b"//[::1]:8080/path?q",
        b"/a://b",
        b"/http://host/path",
        b"/path\x20with\x20spaces",
        b"/caf\xc3\xa9",
        b"http://host/path",
        b"HTTPS://user@host:443/path?q#f",
        b"x://y",
        b"1http://host/path",
        b"localhost:80/foo?bar#qqq",
        b"*",
    ] {
        assert_dispatch_matches(target);
    }
}
