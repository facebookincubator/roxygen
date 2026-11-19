/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Opt-in construction of CONNECT requests from HTTP/1 header fields.

use http::Method;
use http::Request;
use http::header::HOST;
use http::header::HeaderName;

/// Builds an authority-form CONNECT, removing connection-specific and body-framing fields.
///
/// Every `Connection` value contributes field names to remove. `Host` is omitted because the
/// URI supplies the authority. Retained values lose leading and trailing SP/HTAB, and the first
/// occurrence of each case-insensitive name wins. Invalid names and retained values fail;
/// discarded values are not parsed. Proxy credentials are retained unless `Connection` names
/// them, so callers can add or replace their credentials after construction.
///
/// Returns the removed names in input order, including duplicates, for caller-owned diagnostics.
/// An omitted `Host` is not reported unless `Connection` names it. This normalization is opt-in:
/// [`crate::ConnectClient::connect`] still validates the finished request, including authority,
/// metadata size, and sensitive-header restrictions.
pub fn connect_request<'a>(
    authority: &str,
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<(Request<()>, Vec<HeaderName>), http::Error> {
    let mut request = Request::builder()
        .method(Method::CONNECT)
        .uri(authority)
        .body(())?;
    let headers: Vec<_> = headers.into_iter().collect();
    let nominated: Vec<HeaderName> = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| value.split(','))
        .filter_map(|token| HeaderName::from_bytes(token.trim_matches([' ', '\t']).as_bytes()).ok())
        .collect();
    let mut dropped = Vec::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())?;
        if forbidden_header(&name) || nominated.contains(&name) {
            dropped.push(name);
            continue;
        }
        if name == HOST {
            continue;
        }
        request
            .headers_mut()
            .entry(name)
            .or_insert(value.trim_matches([' ', '\t']).parse()?);
    }
    Ok((request, dropped))
}

pub(crate) fn forbidden_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-connection"
            | "transfer-encoding"
            | "upgrade"
            | "te"
            | "content-length"
            | "trailer"
    )
}

#[cfg(test)]
mod tests {
    use http::header::PROXY_AUTHORIZATION;

    use super::*;

    #[test]
    fn construction_filters_framing_and_preserves_proxy_credentials() {
        let (request, dropped) = connect_request(
            "example.com:443",
            [
                ("Connection", "close"),
                ("Proxy-Connection", "keep-alive"),
                ("keep-alive", "timeout=5"),
                ("Transfer-Encoding", "chunked"),
                ("Upgrade", "h2c"),
                ("TE", "trailers"),
                ("Trailer", "Expires"),
                ("Content-Length", "0"),
                ("Host", "other.example"),
                ("Proxy-Authorization", "Basic credential"),
                ("X-Padded", " \tpadded value\t "),
                ("X-Empty", " \t "),
            ],
        )
        .expect("HTTP/1 fields can be normalized");
        assert_eq!(request.method(), Method::CONNECT);
        assert_eq!(request.uri(), "example.com:443");
        assert_eq!(request.headers().len(), 3);
        assert_eq!(request.headers()[PROXY_AUTHORIZATION], "Basic credential");
        assert_eq!(request.headers()["x-padded"], "padded value");
        assert_eq!(request.headers()["x-empty"], "");
        assert_eq!(
            dropped.iter().map(HeaderName::as_str).collect::<Vec<_>>(),
            [
                "connection",
                "proxy-connection",
                "keep-alive",
                "transfer-encoding",
                "upgrade",
                "te",
                "trailer",
                "content-length",
            ]
        );
    }

    #[test]
    fn all_connection_fields_nominate_headers_before_duplicate_names_are_collapsed() {
        let (request, dropped) = connect_request(
            "example.com:443",
            [
                ("X-Hop", "discard"),
                ("Connection", "X-Hop, ,KEEP-ALIVE ,x-other, not a name"),
                ("connection", "X-Third, host, Proxy-Authorization"),
                ("X-Other", "discard"),
                ("X-THIRD", "discard"),
                ("Host", "discard"),
                ("Proxy-Authorization", "discard"),
                ("X-Kept", "keep"),
            ],
        )
        .expect("valid Connection nominations are removed");
        assert_eq!(request.headers().len(), 1);
        assert_eq!(request.headers()["x-kept"], "keep");
        assert_eq!(
            dropped.iter().map(HeaderName::as_str).collect::<Vec<_>>(),
            [
                "x-hop",
                "connection",
                "connection",
                "x-other",
                "x-third",
                "host",
                "proxy-authorization",
            ]
        );
    }

    #[test]
    fn duplicate_names_keep_the_first_value_but_still_parse_later_values() {
        let (request, dropped) = connect_request(
            "example.com:443",
            [("X-Value", " first\t"), ("x-value", "second")],
        )
        .expect("case variants have a defined precedence");
        assert_eq!(request.headers().len(), 1);
        assert_eq!(request.headers()["x-value"], "first");
        assert!(dropped.is_empty());
        assert!(
            connect_request(
                "example.com:443",
                [("X-Value", "first"), ("x-value", "invalid\r\nvalue")],
            )
            .is_err()
        );
    }

    #[test]
    fn discarded_values_are_not_parsed_but_names_and_retained_values_are() {
        let (request, dropped) = connect_request(
            "example.com:443",
            [
                ("Connection", "X-Hop"),
                ("Content-Length", "invalid\r\nvalue"),
                ("X-Hop", "invalid\r\nvalue"),
                ("Host", "invalid\r\nvalue"),
            ],
        )
        .expect("discarded values never reach the request");
        assert!(request.headers().is_empty());
        assert_eq!(dropped.len(), 3, "Host omission is not reported");
        assert!(connect_request("example.com:443", [("bad name", "value")]).is_err());
        assert!(connect_request("example.com:443", [("x-kept", "invalid\r\nvalue")]).is_err());
        assert!(connect_request("invalid uri", []).is_err());
    }
}
