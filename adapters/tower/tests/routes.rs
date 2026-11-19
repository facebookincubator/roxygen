/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomPinned;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use http::HeaderValue;
use http::Method;
use http::Request;
use http::Uri;
use http::Version;
use http::header::CONNECTION;
use http::header::HOST;
use http::header::UPGRADE;

use crate::route::HopSecurity;
use crate::route::Origin;
use crate::route::PhysicalHop;
use crate::route::ProtocolPolicy;
use crate::route::ProxyEndpoint;
use crate::route::RouteError;
use crate::route::RouteSpec;
use crate::route::TargetForm;

fn origin(value: &str) -> Origin {
    value.parse().unwrap()
}

fn identity_hash(value: &impl Hash) -> u64 {
    let mut state = DefaultHasher::new();
    value.hash(&mut state);
    state.finish()
}

fn proxy(value: &str) -> ProxyEndpoint {
    ProxyEndpoint::tcp(value.parse().unwrap()).unwrap()
}

fn direct(value: &str) -> RouteSpec {
    RouteSpec::direct(origin(value), ProtocolPolicy::Negotiate)
}

fn forward(value: &str) -> RouteSpec {
    RouteSpec::forward(
        origin(value),
        proxy("http://proxy.example:8080"),
        ProtocolPolicy::Http1Only,
    )
    .unwrap()
}

struct UntouchedBody {
    drops: Rc<Cell<usize>>,
    _pinned: PhantomPinned,
}

impl Drop for UntouchedBody {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

fn owned_request(uri: Uri, drops: &Rc<Cell<usize>>) -> Request<UntouchedBody> {
    let mut request = Request::new(UntouchedBody {
        drops: Rc::clone(drops),
        _pinned: PhantomPinned,
    });
    *request.uri_mut() = uri;
    request
}

#[test]
fn origins_normalize_case_and_default_ports() {
    for (input, canonical, host, port) in [
        (
            "HTTP://EXAMPLE.Com:080/",
            "http://example.com",
            "example.com",
            80,
        ),
        (
            "https://EXAMPLE.com:443",
            "https://example.com",
            "example.com",
            443,
        ),
        (
            "https://Example.Com:8443/",
            "https://example.com:8443",
            "example.com",
            8443,
        ),
        ("http://127.0.0.1:80", "http://127.0.0.1", "127.0.0.1", 80),
        (
            "https://[2001:0DB8:0:0:0:0:0:1]:443/",
            "https://[2001:db8::1]",
            "2001:db8::1",
            443,
        ),
        ("http://[::1]:8080/", "http://[::1]:8080", "::1", 8080),
        (
            "http://BUILD_HOST.example/",
            "http://build_host.example",
            "build_host.example",
            80,
        ),
    ] {
        let parsed = origin(input);
        assert_eq!(parsed.to_string(), canonical);
        assert_eq!(parsed.host(), host);
        assert_eq!(parsed.port(), port);
        assert_eq!(parsed, origin(canonical));
        assert_eq!(Origin::try_from(parsed.as_uri()).unwrap(), parsed);
        assert_eq!(HashSet::from([parsed, origin(canonical)]).len(), 1);
    }
    assert_ne!(
        origin("https://example.com"),
        origin("https://example.com.")
    );
    assert_eq!(origin("https://EXAMPLE.com.").host(), "example.com.");
}

#[test]
fn origins_reject_non_origin_uris() {
    for input in [
        "",
        "/",
        "example.com",
        "//example.com",
        "ftp://example.com",
        "http://",
        "http:///",
        "http://user@example.com",
        "http://user:pass@example.com",
        "http://example.com/path",
        "http://example.com//",
        "http://example.com?query",
        "http://example.com/?",
        "http://example.com/#fragment",
        "http://example.com#",
        "http://example.com:",
        "http://example.com:0",
        "http://example.com:65536",
        "http://example.com:port",
        "http://example.com:+80",
        "http://example..com",
        "http://-example.com",
        "http://example-.com",
        "http://example,com",
        "http://[not-an-ip]",
        "http://[fe80::1%25eth0]",
        "http://12345",
        "http://127.1",
        "http://127.000.0.1",
        "http://999.0.0.1",
        "http://%65xample.com",
    ] {
        assert!(input.parse::<Origin>().is_err(), "accepted {input}");
    }
    assert!(matches!(
        "http://example.com/#x".parse::<Origin>(),
        Err(RouteError::Fragment)
    ));
    assert!(matches!(
        "https://example.com/a".parse::<Origin>(),
        Err(RouteError::NotAnOrigin)
    ));
    assert!(matches!(
        "https://user@example.com".parse::<Origin>(),
        Err(RouteError::UserInfo)
    ));
    assert!(Origin::try_from("https://example.com/a".parse::<Uri>().unwrap()).is_err());
}

#[test]
fn route_specs_preserve_logical_origin_and_physical_hop() {
    let route = direct("https://[::1]:8443");
    assert_eq!(route.physical_hop(), PhysicalHop::Tcp(route.origin()));
    assert_eq!(route.security(), HopSecurity::Tls);
    assert_eq!(route.target_form(), TargetForm::OriginForm);
    assert_eq!(route.connector_uri(), "https://[::1]:8443/");
    assert!(route.proxy().is_none());

    let route = forward("https://origin.example");
    let endpoint = origin("http://proxy.example:8080");
    assert_eq!(route.origin(), &origin("https://origin.example:443"));
    assert_eq!(route.origin().port(), 443);
    assert_eq!(route.physical_hop(), PhysicalHop::Tcp(&endpoint));
    assert_eq!(
        route.proxy().unwrap().physical_hop(),
        PhysicalHop::Tcp(&endpoint)
    );
    assert_eq!(route.security(), HopSecurity::Plaintext);
    assert_eq!(route.target_form(), TargetForm::AbsoluteForm);
    assert_eq!(route.connector_uri(), "http://origin.example/");
    assert_eq!(
        forward("https://origin.example:8443").connector_uri(),
        "http://origin.example:8443/"
    );
}

#[test]
fn forward_routes_require_http1_and_plaintext_proxy() {
    assert!(matches!(
        ProxyEndpoint::tcp("https://proxy.example".parse().unwrap()),
        Err(RouteError::ProxyRequiresPlaintext)
    ));
    for input in [
        "http://proxy.example/path",
        "http://proxy.example/?q",
        "http://user@proxy.example",
    ] {
        assert!(ProxyEndpoint::tcp(input.parse().unwrap()).is_err());
    }
    for policy in [ProtocolPolicy::Http2Only, ProtocolPolicy::Negotiate] {
        for endpoint in [
            proxy("http://proxy.example"),
            ProxyEndpoint::unix(PathBuf::from("/tmp/proxy.sock")).unwrap(),
        ] {
            assert!(matches!(
                RouteSpec::forward(origin("https://origin.example"), endpoint, policy),
                Err(RouteError::ProxyRequiresHttp1)
            ));
        }
    }
    for (value, policy, http1, http2) in [
        (
            "http://origin.example",
            ProtocolPolicy::Negotiate,
            true,
            false,
        ),
        (
            "https://origin.example",
            ProtocolPolicy::Negotiate,
            true,
            true,
        ),
        (
            "http://origin.example",
            ProtocolPolicy::Http1Only,
            true,
            false,
        ),
        (
            "https://origin.example",
            ProtocolPolicy::Http1Only,
            true,
            false,
        ),
        (
            "http://origin.example",
            ProtocolPolicy::Http2Only,
            false,
            true,
        ),
        (
            "https://origin.example",
            ProtocolPolicy::Http2Only,
            false,
            true,
        ),
    ] {
        let route = RouteSpec::direct(origin(value), policy);
        assert_eq!(route.protocol(), policy);
        assert_eq!(route.allows_http1(), http1);
        assert_eq!(route.allows_http2(), http2);
    }
    let route = forward("https://origin.example");
    assert!(route.allows_http1());
    assert!(!route.allows_http2());
}

#[test]
fn unix_proxy_paths_are_absolute_and_distinct() {
    for path in ["", "proxy.sock", "tmp/proxy.sock", "/tmp/proxy\0.sock"] {
        assert!(matches!(
            ProxyEndpoint::unix(PathBuf::from(path)),
            Err(RouteError::InvalidUnixPath)
        ));
    }
    let first = ProxyEndpoint::unix(PathBuf::from("/tmp/proxy-first.sock")).unwrap();
    let second = ProxyEndpoint::unix(PathBuf::from("/tmp/proxy-second.sock")).unwrap();
    assert_ne!(first, second);
    assert_eq!(
        HashSet::from([first.clone(), first.clone(), second]).len(),
        2
    );
    let route = RouteSpec::forward(
        origin("https://origin.example"),
        first,
        ProtocolPolicy::Http1Only,
    )
    .unwrap();
    assert_eq!(
        route.physical_hop(),
        PhysicalHop::Unix(Path::new("/tmp/proxy-first.sock"))
    );
    assert_eq!(route.security(), HopSecurity::Plaintext);
    assert_eq!(route.connector_uri(), "http://origin.example/");
    assert_eq!(route.origin().scheme(), "https");

    let spellings = ["/socket", "/socket/", "/socket/."];
    let endpoints: Vec<_> = spellings
        .iter()
        .map(|spelling| ProxyEndpoint::unix(PathBuf::from(spelling)).unwrap())
        .collect();
    for (index, endpoint) in endpoints.iter().enumerate() {
        let hop = endpoint.physical_hop();
        let PhysicalHop::Unix(path) = hop else {
            panic!("expected Unix endpoint");
        };
        assert_eq!(path.as_os_str(), Path::new(spellings[index]).as_os_str());
        let same = ProxyEndpoint::unix(PathBuf::from(spellings[index])).unwrap();
        assert_eq!(endpoint, &same);
        assert_eq!(identity_hash(endpoint), identity_hash(&same));
        assert_eq!(hop, same.physical_hop());
        assert_eq!(identity_hash(&hop), identity_hash(&same.physical_hop()));
        for other in endpoints.iter().skip(index + 1) {
            assert_ne!(endpoint, other);
            assert_ne!(hop, other.physical_hop());
        }
    }
    assert_eq!(endpoints.iter().cloned().collect::<HashSet<_>>().len(), 3);
    assert_eq!(
        endpoints
            .iter()
            .map(identity_hash)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    let hops: HashSet<_> = endpoints.iter().map(ProxyEndpoint::physical_hop).collect();
    assert_eq!(hops.len(), 3);
    assert_eq!(
        hops.iter().map(identity_hash).collect::<HashSet<_>>().len(),
        3
    );
}

#[test]
fn direct_targets_use_origin_form_and_forward_targets_use_absolute_form() {
    for (route, input, target, host) in [
        (
            direct("https://example.com"),
            "https://EXAMPLE.com:443/a%2Fb?x=1&x=2",
            "/a%2Fb?x=1&x=2",
            "example.com",
        ),
        (
            direct("http://example.com"),
            "http://example.com",
            "/",
            "example.com",
        ),
        (
            direct("https://example.com"),
            "https://example.com?query",
            "/?query",
            "example.com",
        ),
        (
            direct("https://[::1]:8443"),
            "/path?query",
            "/path?query",
            "[::1]:8443",
        ),
        (
            direct("https://example.com"),
            "//not-a-hop/path",
            "//not-a-hop/path",
            "example.com",
        ),
        (
            forward("https://example.com"),
            "https://example.com:443/a?x=1",
            "http://example.com/a?x=1",
            "example.com",
        ),
        (
            forward("https://example.com"),
            "https://example.com?query",
            "http://example.com/?query",
            "example.com",
        ),
        (
            forward("https://example.com:8443"),
            "/a?",
            "http://example.com:8443/a?",
            "example.com:8443",
        ),
        (
            forward("http://example.com"),
            "/path",
            "http://example.com/path",
            "example.com",
        ),
    ] {
        let drops = Rc::new(Cell::new(0));
        let mut request = owned_request(input.parse().unwrap(), &drops);
        let body = request.body() as *const UntouchedBody;
        *request.method_mut() = Method::POST;
        *request.version_mut() = Version::HTTP_11;
        let context = Arc::new(17_u64);
        request.extensions_mut().insert(Arc::clone(&context));
        let mut sensitive = HeaderValue::from_static("private");
        sensitive.set_sensitive(true);
        request.headers_mut().append("x-local", sensitive);
        request
            .headers_mut()
            .append("x-local", HeaderValue::from_static("second"));
        route.validate_request(&request).unwrap();
        assert_eq!(request.uri(), input);
        assert!(!request.headers().contains_key(HOST));
        route.project_request(&mut request).unwrap();
        assert_eq!(request.uri(), target);
        assert_eq!(request.headers()[HOST], host);
        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.version(), Version::HTTP_11);
        assert_eq!(request.body() as *const UntouchedBody, body);
        assert!(Arc::ptr_eq(
            request.extensions().get::<Arc<u64>>().unwrap(),
            &context
        ));
        let values: Vec<_> = request.headers().get_all("x-local").iter().collect();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], "private");
        assert!(values[0].is_sensitive());
        assert_eq!(values[1], "second");
        assert!(!values[1].is_sensitive());
        assert_eq!(drops.get(), 0);
        drop(request);
        assert_eq!(drops.get(), 1);
    }
    for route in [
        direct("https://example.com"),
        forward("https://example.com"),
    ] {
        let mut request = Request::builder().uri("/a").body(()).unwrap();
        let mut host = HeaderValue::from_static("EXAMPLE.com:0443");
        host.set_sensitive(true);
        request.headers_mut().insert(HOST, host.clone());
        route.project_request(&mut request).unwrap();
        assert_eq!(request.headers()[HOST], host);
        assert!(request.headers()[HOST].is_sensitive());
    }
    let mut options = Request::builder()
        .method(Method::OPTIONS)
        .uri("*")
        .body(())
        .unwrap();
    direct("http://example.com")
        .project_request(&mut options)
        .unwrap();
    assert_eq!(options.uri(), "*");
    assert_eq!(options.headers()[HOST], "example.com");
    let mut ipv6 = Request::builder()
        .uri("/")
        .header(HOST, "[0:0:0:0:0:0:0:1]:443")
        .body(())
        .unwrap();
    direct("https://[::1]").project_request(&mut ipv6).unwrap();
}

#[test]
fn route_projection_rejects_origin_host_and_upgrade_mismatches() {
    let route = direct("https://example.com");
    for (uri, method, headers, expected) in [
        ("https://other.example/a", Method::GET, vec![], "origin"),
        ("http://example.com/a", Method::GET, vec![], "origin"),
        ("https://example.com:8443/a", Method::GET, vec![], "origin"),
        ("/a", Method::GET, vec![(HOST, "other.example")], "host"),
        ("/a", Method::GET, vec![(HOST, "example.com:80")], "host"),
        (
            "/a",
            Method::GET,
            vec![(HOST, "example.com"), (HOST, "example.com")],
            "duplicate",
        ),
        (
            "/a",
            Method::GET,
            vec![(HOST, "example.com"), (HOST, "other.example")],
            "duplicate",
        ),
        (
            "/a",
            Method::GET,
            vec![(HOST, "example.com,other.example")],
            "invalid-host",
        ),
        (
            "/a",
            Method::GET,
            vec![(HOST, " example.com")],
            "invalid-host",
        ),
        (
            "/a",
            Method::GET,
            vec![(HOST, "example.com ")],
            "invalid-host",
        ),
        (
            "/a",
            Method::GET,
            vec![(HOST, "user@example.com")],
            "invalid-host",
        ),
        (
            "/a",
            Method::GET,
            vec![(HOST, "example.com:65536")],
            "invalid-host",
        ),
        (
            "/a",
            Method::GET,
            vec![(HOST, "example.com:")],
            "invalid-host",
        ),
        ("/a", Method::GET, vec![(HOST, "")], "invalid-host"),
        ("example.com:443", Method::CONNECT, vec![], "connect"),
        ("/a", Method::GET, vec![(UPGRADE, "websocket")], "upgrade"),
        (
            "/a",
            Method::GET,
            vec![(CONNECTION, "keep-alive, UpGrAdE")],
            "upgrade",
        ),
        (
            "/a",
            Method::GET,
            vec![(CONNECTION, "keep-alive"), (CONNECTION, "\tupgrade\t")],
            "upgrade",
        ),
        ("*", Method::GET, vec![], "target"),
        ("example.com:443", Method::GET, vec![], "target"),
    ] {
        let drops = Rc::new(Cell::new(0));
        let mut request = owned_request(uri.parse().unwrap(), &drops);
        *request.method_mut() = method.clone();
        for (name, value) in headers {
            request
                .headers_mut()
                .append(name, HeaderValue::from_str(value).unwrap());
        }
        let context = Arc::new(23_u64);
        request.extensions_mut().insert(Arc::clone(&context));
        let body = request.body() as *const UntouchedBody;
        let original_headers = request.headers().clone();
        let error = route.project_request(&mut request).unwrap_err();
        assert!(
            match expected {
                "origin" => matches!(error, RouteError::OriginMismatch),
                "host" => matches!(error, RouteError::HostMismatch),
                "duplicate" => matches!(error, RouteError::DuplicateHost),
                "invalid-host" => matches!(error, RouteError::InvalidHostHeader),
                "connect" => matches!(error, RouteError::ConnectUnsupported),
                "upgrade" => matches!(error, RouteError::UpgradeUnsupported),
                "target" => matches!(error, RouteError::UnsupportedTarget),
                _ => unreachable!(),
            },
            "{uri}: {error:?}"
        );
        assert_eq!(request.uri(), uri);
        assert_eq!(request.headers(), &original_headers);
        assert_eq!(request.method(), method);
        assert_eq!(request.body() as *const UntouchedBody, body);
        assert!(Arc::ptr_eq(
            request.extensions().get::<Arc<u64>>().unwrap(),
            &context
        ));
        assert_eq!(drops.get(), 0);
        drop(request);
        assert_eq!(drops.get(), 1);
    }
    let route = forward("https://example.com");
    for uri in ["http://example.com/a", "https://other.example/a"] {
        let mut request = Request::builder().uri(uri).body(()).unwrap();
        assert!(matches!(
            route.project_request(&mut request),
            Err(RouteError::OriginMismatch)
        ));
    }
    let mut request = Request::builder()
        .uri("/a")
        .header(HOST, "example.com:80")
        .body(())
        .unwrap();
    assert!(matches!(
        route.project_request(&mut request),
        Err(RouteError::HostMismatch)
    ));
    let mut request = Request::builder()
        .uri("/a")
        .header("http2-settings", "settings")
        .body(())
        .unwrap();
    assert!(matches!(
        route.project_request(&mut request),
        Err(RouteError::UpgradeUnsupported)
    ));
    let mut request = Request::builder()
        .method(Method::OPTIONS)
        .uri("*")
        .body(())
        .unwrap();
    assert!(matches!(
        route.project_request(&mut request),
        Err(RouteError::UnsupportedTarget)
    ));
    for route in [
        direct("https://example.com"),
        forward("https://example.com"),
    ] {
        let uri = Uri::builder()
            .scheme("https")
            .authority("example.com")
            .path_and_query("*")
            .build()
            .unwrap();
        let mut request = Request::builder()
            .method(Method::OPTIONS)
            .uri(uri.clone())
            .body(())
            .unwrap();
        assert!(matches!(
            route.project_request(&mut request),
            Err(RouteError::UnsupportedTarget)
        ));
        assert_eq!(request.uri(), &uri);
    }
}

#[test]
fn route_keys_separate_security_protocol_and_endpoints() {
    let mut routes = HashSet::new();
    for logical in [
        "http://example.com",
        "https://example.com",
        "https://example.com:8443",
        "https://other.example",
    ] {
        for protocol in [
            ProtocolPolicy::Http1Only,
            ProtocolPolicy::Http2Only,
            ProtocolPolicy::Negotiate,
        ] {
            assert!(routes.insert(RouteSpec::direct(origin(logical), protocol)));
        }
        for endpoint in [
            proxy("http://example.com"),
            proxy("http://proxy.example:8080"),
            proxy("http://proxy.example:8081"),
            proxy("http://other-proxy.example:8080"),
            ProxyEndpoint::unix(PathBuf::from("/tmp/first.sock")).unwrap(),
            ProxyEndpoint::unix(PathBuf::from("/tmp/second.sock")).unwrap(),
        ] {
            assert!(routes.insert(
                RouteSpec::forward(origin(logical), endpoint, ProtocolPolicy::Http1Only).unwrap()
            ));
        }
    }
    assert_eq!(routes.len(), 36);
    assert!(!routes.insert(direct("HTTPS://EXAMPLE.COM:443/")));
    assert!(
        !routes.insert(
            RouteSpec::forward(
                origin("HTTPS://EXAMPLE.COM:443/"),
                proxy("HTTP://PROXY.EXAMPLE:8080/"),
                ProtocolPolicy::Http1Only
            )
            .unwrap()
        )
    );
    assert_eq!(routes.len(), 36);

    let spellings = ["/socket", "/socket/", "/socket/."];
    let exact_routes: Vec<_> = spellings
        .iter()
        .map(|spelling| {
            RouteSpec::forward(
                origin("https://example.com"),
                ProxyEndpoint::unix(PathBuf::from(spelling)).unwrap(),
                ProtocolPolicy::Http1Only,
            )
            .unwrap()
        })
        .collect();
    for (index, route) in exact_routes.iter().enumerate() {
        let PhysicalHop::Unix(path) = route.physical_hop() else {
            panic!("expected Unix route");
        };
        assert_eq!(path.as_os_str(), Path::new(spellings[index]).as_os_str());
        assert!(routes.insert(route.clone()));
        assert!(!routes.insert(route.clone()));
        assert_eq!(identity_hash(route), identity_hash(&route.clone()));
        for other in exact_routes.iter().skip(index + 1) {
            assert_ne!(route, other);
            assert_ne!(route.proxy(), other.proxy());
            assert_ne!(route.physical_hop(), other.physical_hop());
        }
    }
    assert_eq!(routes.len(), 39);
    assert_eq!(
        exact_routes
            .iter()
            .map(identity_hash)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
}
