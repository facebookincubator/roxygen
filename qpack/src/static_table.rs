/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The QPACK static table of RFC 9204, indexed from 0.

use crate::StaticRef;

/// The static table's `(name, value)` entries.
pub const STATIC_TABLE: [(&[u8], &[u8]); 99] = [
    (b":authority", b""),
    (b":path", b"/"),
    (b"age", b"0"),
    (b"content-disposition", b""),
    (b"content-length", b"0"),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"referer", b""),
    (b"set-cookie", b""),
    (b":method", b"CONNECT"),
    (b":method", b"DELETE"),
    (b":method", b"GET"),
    (b":method", b"HEAD"),
    (b":method", b"OPTIONS"),
    (b":method", b"POST"),
    (b":method", b"PUT"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"103"),
    (b":status", b"200"),
    (b":status", b"304"),
    (b":status", b"404"),
    (b":status", b"503"),
    (b"accept", b"*/*"),
    (b"accept", b"application/dns-message"),
    (b"accept-encoding", b"gzip, deflate, br"),
    (b"accept-ranges", b"bytes"),
    (b"access-control-allow-headers", b"cache-control"),
    (b"access-control-allow-headers", b"content-type"),
    (b"access-control-allow-origin", b"*"),
    (b"cache-control", b"max-age=0"),
    (b"cache-control", b"max-age=2592000"),
    (b"cache-control", b"max-age=604800"),
    (b"cache-control", b"no-cache"),
    (b"cache-control", b"no-store"),
    (b"cache-control", b"public, max-age=31536000"),
    (b"content-encoding", b"br"),
    (b"content-encoding", b"gzip"),
    (b"content-type", b"application/dns-message"),
    (b"content-type", b"application/javascript"),
    (b"content-type", b"application/json"),
    (b"content-type", b"application/x-www-form-urlencoded"),
    (b"content-type", b"image/gif"),
    (b"content-type", b"image/jpeg"),
    (b"content-type", b"image/png"),
    (b"content-type", b"text/css"),
    (b"content-type", b"text/html; charset=utf-8"),
    (b"content-type", b"text/plain"),
    (b"content-type", b"text/plain;charset=utf-8"),
    (b"range", b"bytes=0-"),
    (b"strict-transport-security", b"max-age=31536000"),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains",
    ),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains; preload",
    ),
    (b"vary", b"accept-encoding"),
    (b"vary", b"origin"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-xss-protection", b"1; mode=block"),
    (b":status", b"100"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"302"),
    (b":status", b"400"),
    (b":status", b"403"),
    (b":status", b"421"),
    (b":status", b"425"),
    (b":status", b"500"),
    (b"accept-language", b""),
    (b"access-control-allow-credentials", b"FALSE"),
    (b"access-control-allow-credentials", b"TRUE"),
    (b"access-control-allow-headers", b"*"),
    (b"access-control-allow-methods", b"get"),
    (b"access-control-allow-methods", b"get, post, options"),
    (b"access-control-allow-methods", b"options"),
    (b"access-control-expose-headers", b"content-length"),
    (b"access-control-request-headers", b"content-type"),
    (b"access-control-request-method", b"get"),
    (b"access-control-request-method", b"post"),
    (b"alt-svc", b"clear"),
    (b"authorization", b""),
    (
        b"content-security-policy",
        b"script-src 'none'; object-src 'none'; base-uri 'none'",
    ),
    (b"early-data", b"1"),
    (b"expect-ct", b""),
    (b"forwarded", b""),
    (b"if-range", b""),
    (b"origin", b""),
    (b"purpose", b"prefetch"),
    (b"server", b""),
    (b"timing-allow-origin", b"*"),
    (b"upgrade-insecure-requests", b"1"),
    (b"user-agent", b""),
    (b"x-forwarded-for", b""),
    (b"x-frame-options", b"deny"),
    (b"x-frame-options", b"sameorigin"),
];

impl StaticRef {
    /// Returns the static table reference of a `:status` line with the three digits of `status`:
    /// its entry, or entry 24's name when the table does not list that status.
    pub const fn status(status: u16) -> Self {
        Self::Entry(match status {
            103 => 24,
            200 => 25,
            304 => 26,
            404 => 27,
            503 => 28,
            100 => 63,
            204 => 64,
            206 => 65,
            302 => 66,
            400 => 67,
            403 => 68,
            421 => 69,
            425 => 70,
            500 => 71,
            _ => return Self::Name(24),
        })
    }
}

/// The best static table entry for a field line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticMatch {
    /// The entry at this index has the field line's name and value.
    Full(u8),
    /// The entry at this index has the field line's name, and the table lacks an entry with both.
    Name(u8),
}

/// Finds the lowest-indexed entry with `name` and `value`, else the lowest-indexed entry with
/// `name`. Names compare byte for byte, so they must already be lowercase.
pub fn find(name: &[u8], value: &[u8]) -> Option<StaticMatch> {
    let indices = indices_for_name(name)?;
    let full = indices
        .iter()
        .find(|&&i| STATIC_TABLE[usize::from(i)].1 == value);
    Some(match full {
        Some(&i) => StaticMatch::Full(i),
        None => StaticMatch::Name(indices[0]),
    })
}

/// Finds like [`find`] for a line whose name is that of the entry at `name_index`, without
/// comparing names. Panics if `name_index` is not a static table index.
pub fn find_by_name_index(name_index: u8, value: &[u8]) -> StaticMatch {
    let first = FIRST_WITH_NAME[usize::from(name_index)];
    let mut index = first;
    while index != NO_ENTRY {
        if STATIC_TABLE[usize::from(index)].1 == value {
            return StaticMatch::Full(index);
        }
        index = NEXT_WITH_NAME[usize::from(index)];
    }
    StaticMatch::Name(first)
}

const NO_ENTRY: u8 = u8::MAX;
const NAME_LINKS: ([u8; 99], [u8; 99]) = name_links();
/// For each entry, the lowest index of an entry with its name.
static FIRST_WITH_NAME: [u8; 99] = NAME_LINKS.0;
/// For each entry, the next higher index of an entry with its name, or `NO_ENTRY`.
static NEXT_WITH_NAME: [u8; 99] = NAME_LINKS.1;

const fn name_links() -> ([u8; 99], [u8; 99]) {
    let mut first = [0; 99];
    let mut next = [NO_ENTRY; 99];
    let mut index = 0;
    while index < STATIC_TABLE.len() {
        let name = STATIC_TABLE[index].0;
        let mut other = 0;
        while !same_bytes(STATIC_TABLE[other].0, name) {
            other += 1;
        }
        first[index] = other as u8;
        other = index + 1;
        while other < STATIC_TABLE.len() && !same_bytes(STATIC_TABLE[other].0, name) {
            other += 1;
        }
        if other < STATIC_TABLE.len() {
            next[index] = other as u8;
        }
        index += 1;
    }
    (first, next)
}

const fn same_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

#[cfg(test)]
thread_local! {
    /// The number of names that this thread looked up byte by byte.
    pub(crate) static NAME_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Returns the indices of the entries with `name`, in increasing order.
fn indices_for_name(name: &[u8]) -> Option<&'static [u8]> {
    #[cfg(test)]
    NAME_LOOKUPS.set(NAME_LOOKUPS.get() + 1);
    Some(match name {
        b":authority" => &[0],
        b":path" => &[1],
        b"age" => &[2],
        b"content-disposition" => &[3],
        b"content-length" => &[4],
        b"cookie" => &[5],
        b"date" => &[6],
        b"etag" => &[7],
        b"if-modified-since" => &[8],
        b"if-none-match" => &[9],
        b"last-modified" => &[10],
        b"link" => &[11],
        b"location" => &[12],
        b"referer" => &[13],
        b"set-cookie" => &[14],
        b":method" => &[15, 16, 17, 18, 19, 20, 21],
        b":scheme" => &[22, 23],
        b":status" => &[24, 25, 26, 27, 28, 63, 64, 65, 66, 67, 68, 69, 70, 71],
        b"accept" => &[29, 30],
        b"accept-encoding" => &[31],
        b"accept-ranges" => &[32],
        b"access-control-allow-headers" => &[33, 34, 75],
        b"access-control-allow-origin" => &[35],
        b"cache-control" => &[36, 37, 38, 39, 40, 41],
        b"content-encoding" => &[42, 43],
        b"content-type" => &[44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54],
        b"range" => &[55],
        b"strict-transport-security" => &[56, 57, 58],
        b"vary" => &[59, 60],
        b"x-content-type-options" => &[61],
        b"x-xss-protection" => &[62],
        b"accept-language" => &[72],
        b"access-control-allow-credentials" => &[73, 74],
        b"access-control-allow-methods" => &[76, 77, 78],
        b"access-control-expose-headers" => &[79],
        b"access-control-request-headers" => &[80],
        b"access-control-request-method" => &[81, 82],
        b"alt-svc" => &[83],
        b"authorization" => &[84],
        b"content-security-policy" => &[85],
        b"early-data" => &[86],
        b"expect-ct" => &[87],
        b"forwarded" => &[88],
        b"if-range" => &[89],
        b"origin" => &[90],
        b"purpose" => &[91],
        b"server" => &[92],
        b"timing-allow-origin" => &[93],
        b"upgrade-insecure-requests" => &[94],
        b"user-agent" => &[95],
        b"x-forwarded-for" => &[96],
        b"x-frame-options" => &[97, 98],
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_rfc9204_appendix_a() {
        assert_eq!(STATIC_TABLE.len(), 99);
        let spot = [
            (0, ":authority", ""),
            (1, ":path", "/"),
            (15, ":method", "CONNECT"),
            (17, ":method", "GET"),
            (25, ":status", "200"),
            (31, "accept-encoding", "gzip, deflate, br"),
            (41, "cache-control", "public, max-age=31536000"),
            (47, "content-type", "application/x-www-form-urlencoded"),
            (52, "content-type", "text/html; charset=utf-8"),
            (54, "content-type", "text/plain;charset=utf-8"),
            (
                58,
                "strict-transport-security",
                "max-age=31536000; includesubdomains; preload",
            ),
            (63, ":status", "100"),
            (71, ":status", "500"),
            (
                85,
                "content-security-policy",
                "script-src 'none'; object-src 'none'; base-uri 'none'",
            ),
            (98, "x-frame-options", "sameorigin"),
        ];
        for (index, name, value) in spot {
            assert_eq!(
                STATIC_TABLE[index],
                (name.as_bytes(), value.as_bytes()),
                "{index}"
            );
        }
    }

    #[test]
    fn find_agrees_with_a_scan_of_the_table() {
        let first = |pred: &dyn Fn(&(&[u8], &[u8])) -> bool| {
            STATIC_TABLE.iter().position(pred).map(|i| i as u8)
        };
        for &(name, value) in &STATIC_TABLE {
            let with_name: Vec<u8> = (0..99)
                .filter(|&i| STATIC_TABLE[usize::from(i)].0 == name)
                .collect();
            assert_eq!(indices_for_name(name), Some(&with_name[..]));
            let full = first(&|e| e.0 == name && e.1 == value).unwrap();
            assert_eq!(find(name, value), Some(StaticMatch::Full(full)));
            let named = first(&|e| e.0 == name).unwrap();
            assert_eq!(find(name, b"\xffunlisted"), Some(StaticMatch::Name(named)));
        }
        assert_eq!(find(b"x-unlisted", b""), None);
        assert_eq!(find(b":Authority", b""), None);
        assert_eq!(find(b"", b""), None);
        assert_eq!(find(b":status", b"200"), Some(StaticMatch::Full(25)));
        assert_eq!(find(b":status", b"201"), Some(StaticMatch::Name(24)));
    }

    #[test]
    fn find_by_name_index_agrees_with_find() {
        for (index, &(name, _)) in STATIC_TABLE.iter().enumerate() {
            for value in STATIC_TABLE
                .iter()
                .map(|e| e.1)
                .chain([&b"\xffunlisted"[..]])
            {
                assert_eq!(
                    Some(find_by_name_index(index as u8, value)),
                    find(name, value),
                    "{index} {value:?}"
                );
            }
        }
    }

    #[test]
    fn status_refs_agree_with_find() {
        let mut entries = 0;
        for status in 0..=1100u16 {
            let expected = match find(b":status", status.to_string().as_bytes()) {
                Some(StaticMatch::Full(index)) => StaticRef::Entry(index),
                _ => StaticRef::Name(24),
            };
            entries += usize::from(matches!(expected, StaticRef::Entry(_)));
            assert_eq!(StaticRef::status(status), expected, "{status}");
        }
        assert_eq!(entries, 14);
    }
}
