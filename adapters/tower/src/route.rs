/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Immutable client route values, and the rewrite of a request head into the
//! form a route sends on the wire.
//!
//! These are plain values that do not resolve hosts or open connections. A
//! client binds them to its connector configuration before use. Request
//! headers never choose the physical hop.

use std::ffi::OsString;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use http::HeaderValue;
use http::Method;
use http::Request;
use http::Uri;
use http::header::CONNECTION;
use http::header::HOST;
use http::header::UPGRADE;
use http::uri::Authority;

/// Why a route or request was rejected.
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("invalid URI")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("only HTTP and HTTPS origins are supported")]
    UnsupportedScheme,
    #[error("an origin requires an authority")]
    MissingAuthority,
    #[error("userinfo is not permitted in an authority")]
    UserInfo,
    #[error("invalid or unsupported host")]
    InvalidHost,
    #[error("an explicit port must be a nonzero decimal u16")]
    InvalidPort,
    #[error("an origin cannot contain a query or a non-root path")]
    NotAnOrigin,
    #[error("an origin cannot contain a fragment")]
    Fragment,
    #[error("a TCP forward proxy must use plaintext HTTP")]
    ProxyRequiresPlaintext,
    #[error("a forward route requires Http1Only")]
    ProxyRequiresHttp1,
    #[error("a Unix proxy path must be absolute and contain no NUL")]
    InvalidUnixPath,
    #[error("request target must be origin-form or a matching absolute URI")]
    UnsupportedTarget,
    #[error("absolute request origin differs from the route")]
    OriginMismatch,
    #[error("multiple Host fields are not permitted, even if identical")]
    DuplicateHost,
    #[error("Host must contain exactly one valid authority")]
    InvalidHostHeader,
    #[error("Host differs from the logical route authority or effective port")]
    HostMismatch,
    #[error("CONNECT is not supported")]
    ConnectUnsupported,
    #[error("protocol upgrades are not supported")]
    UpgradeUnsupported,
}

/// A canonical HTTP or HTTPS origin, independent of its physical connection.
///
/// Host names are lowercase ASCII DNS-style labels (underscores are allowed).
/// Hosts containing only ASCII digits and dots must be canonical dotted-decimal
/// IPv4 literals; abbreviated and single-number forms are rejected. IPv6 literals
/// are normalized and bracketed. Default ports are omitted, other ports retained.
/// Trailing DNS dots remain distinct. Parsing does not perform DNS, IDNA, or
/// filesystem lookups. Parse a string to reject fragments before `http::Uri`
/// discards them. A conversion from an already parsed `Uri` can validate only
/// retained metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Origin {
    scheme: &'static str,
    authority: Authority,
    port: u16,
}

impl Origin {
    /// The canonical logical scheme, either `http` or `https`.
    pub fn scheme(&self) -> &str {
        self.scheme
    }

    /// The canonical authority, omitting the scheme's default port.
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    /// The canonical host without IPv6 brackets or a port.
    pub fn host(&self) -> &str {
        let host = self.authority.host();
        host.strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host)
    }

    /// The effective logical port, including an omitted default port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// A canonical absolute URI with a root path and no query.
    pub fn as_uri(&self) -> Uri {
        self.uri_with_scheme(self.scheme)
    }

    fn uri_with_scheme(&self, scheme: &'static str) -> Uri {
        Uri::builder()
            .scheme(scheme)
            .authority(self.authority.clone())
            .path_and_query("/")
            .build()
            .expect("validated origin components form an absolute URI")
    }

    fn from_authority(scheme: &str, authority: &Authority) -> Result<Self, RouteError> {
        let (scheme, default_port) = if scheme.eq_ignore_ascii_case("http") {
            ("http", 80)
        } else if scheme.eq_ignore_ascii_case("https") {
            ("https", 443)
        } else {
            return Err(RouteError::UnsupportedScheme);
        };
        let raw = authority.as_str();
        if raw.contains('@') {
            return Err(RouteError::UserInfo);
        }
        let (host, port) = if let Some(literal) = raw.strip_prefix('[') {
            let (literal, suffix) = literal.split_once(']').ok_or(RouteError::InvalidHost)?;
            let address: Ipv6Addr = literal.parse().map_err(|_| RouteError::InvalidHost)?;
            let port = if suffix.is_empty() {
                None
            } else {
                Some(suffix.strip_prefix(':').ok_or(RouteError::InvalidPort)?)
            };
            (format!("[{address}]"), port)
        } else {
            let (host, port) = match raw.split_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (raw, None),
            };
            (normalize_host(host)?, port)
        };
        let port = match port {
            Some(port) => {
                if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(RouteError::InvalidPort);
                }
                let port: u16 = port.parse().map_err(|_| RouteError::InvalidPort)?;
                if port == 0 {
                    return Err(RouteError::InvalidPort);
                }
                port
            }
            None => default_port,
        };
        let authority = if port == default_port {
            host
        } else {
            format!("{host}:{port}")
        };
        Ok(Self {
            scheme,
            authority: authority.parse()?,
            port,
        })
    }
}

impl FromStr for Origin {
    type Err = RouteError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.contains('#') {
            return Err(RouteError::Fragment);
        }
        Self::try_from(value.parse::<Uri>()?)
    }
}

impl TryFrom<&Uri> for Origin {
    type Error = RouteError;

    fn try_from(uri: &Uri) -> Result<Self, Self::Error> {
        if uri.query().is_some() || !matches!(uri.path(), "" | "/") {
            return Err(RouteError::NotAnOrigin);
        }
        Self::from_authority(
            uri.scheme_str().ok_or(RouteError::UnsupportedScheme)?,
            uri.authority().ok_or(RouteError::MissingAuthority)?,
        )
    }
}

impl TryFrom<Uri> for Origin {
    type Error = RouteError;

    fn try_from(uri: Uri) -> Result<Self, Self::Error> {
        Self::try_from(&uri)
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}://{}", self.scheme, self.authority)
    }
}

fn normalize_host(host: &str) -> Result<String, RouteError> {
    if host
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        let address: Ipv4Addr = host.parse().map_err(|_| RouteError::InvalidHost)?;
        return Ok(address.to_string());
    }
    let name = host.strip_suffix('.').unwrap_or(host);
    if name.is_empty()
        || name.len() > 253
        || name.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
    {
        return Err(RouteError::InvalidHost);
    }
    Ok(host.to_ascii_lowercase())
}

/// Protocol selection on the immediate hop.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProtocolPolicy {
    /// Uses H1 only.
    Http1Only,
    /// Requires HTTP/2, using prior knowledge when the immediate hop is plaintext.
    Http2Only,
    /// TLS may select H2 or H1. Plaintext uses H1 and never tries an h2c upgrade.
    Negotiate,
}

/// Required encryption on the immediate hop. It does not imply an authenticated
/// peer.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HopSecurity {
    Plaintext,
    Tls,
}

/// Derived request-target form. Direct `OPTIONS *` is the sole asterisk exception.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TargetForm {
    OriginForm,
    AbsoluteForm,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ProxyAddress {
    Tcp(Origin),
    Unix(OsString),
}

/// A validated plaintext forward-proxy endpoint, with no fallback endpoint.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProxyEndpoint {
    address: ProxyAddress,
}

impl ProxyEndpoint {
    /// Validates an HTTP origin for a TCP proxy, rejecting paths and credentials.
    pub fn tcp(uri: Uri) -> Result<Self, RouteError> {
        let origin = Origin::try_from(uri)?;
        if origin.scheme() != "http" {
            return Err(RouteError::ProxyRequiresPlaintext);
        }
        Ok(Self {
            address: ProxyAddress::Tcp(origin),
        })
    }

    /// Retains an absolute Unix path without resolving symlinks or inspecting it.
    /// Equality and hashing use the exact OS string, including separators.
    pub fn unix(path: PathBuf) -> Result<Self, RouteError> {
        if !path.is_absolute() || path.as_os_str().as_encoded_bytes().contains(&0) {
            return Err(RouteError::InvalidUnixPath);
        }
        Ok(Self {
            address: ProxyAddress::Unix(path.into_os_string()),
        })
    }

    /// The actual physical endpoint, separate from the logical target URI.
    pub fn physical_hop(&self) -> PhysicalHop<'_> {
        match &self.address {
            ProxyAddress::Tcp(origin) => PhysicalHop::Tcp(origin),
            ProxyAddress::Unix(path) => PhysicalHop::Unix(Path::new(path)),
        }
    }
}

/// Borrowed physical dial identity. A connector must not derive it from Host.
/// Unix identities compare and hash the exact `OsStr`, not path components.
#[derive(Clone, Copy, Debug)]
pub enum PhysicalHop<'route> {
    Tcp(&'route Origin),
    Unix(&'route Path),
}

impl PartialEq for PhysicalHop<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tcp(origin), Self::Tcp(other_origin)) => origin == other_origin,
            (Self::Unix(path), Self::Unix(other_path)) => {
                path.as_os_str() == other_path.as_os_str()
            }
            _ => false,
        }
    }
}

impl Eq for PhysicalHop<'_> {}

impl Hash for PhysicalHop<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Tcp(origin) => origin.hash(state),
            Self::Unix(path) => path.as_os_str().hash(state),
        }
    }
}

/// The origin, protocol policy, and optional forward proxy a client uses for a
/// request.
///
/// Equality and hashing cover exactly these fields. Security and target form
/// are derived from them. [`Client::route`](crate::client::Client::route)
/// binds a spec to one client.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RouteSpec {
    origin: Origin,
    protocol: ProtocolPolicy,
    proxy: Option<ProxyEndpoint>,
}

impl RouteSpec {
    /// Selects the logical origin itself as the immediate TCP/TLS hop.
    pub fn direct(origin: Origin, protocol: ProtocolPolicy) -> Self {
        Self {
            origin,
            protocol,
            proxy: None,
        }
    }

    /// Selects a plaintext H1 forward-proxy hop, including for logical HTTPS origins.
    ///
    /// This does not implement CONNECT or proxy authentication, and cannot
    /// silently fall back to a direct connection or another proxy endpoint.
    pub fn forward(
        origin: Origin,
        proxy: ProxyEndpoint,
        protocol: ProtocolPolicy,
    ) -> Result<Self, RouteError> {
        if protocol != ProtocolPolicy::Http1Only {
            return Err(RouteError::ProxyRequiresHttp1);
        }
        Ok(Self {
            origin,
            protocol,
            proxy: Some(proxy),
        })
    }

    /// The logical origin, never rewritten to describe a forward proxy.
    pub fn origin(&self) -> &Origin {
        &self.origin
    }

    /// The immutable immediate-hop protocol policy.
    pub fn protocol(&self) -> ProtocolPolicy {
        self.protocol
    }

    /// The explicit proxy endpoint, or `None` for a direct route.
    pub fn proxy(&self) -> Option<&ProxyEndpoint> {
        self.proxy.as_ref()
    }

    /// The sole dial endpoint; logical request targets cannot override it.
    pub fn physical_hop(&self) -> PhysicalHop<'_> {
        match &self.proxy {
            Some(proxy) => proxy.physical_hop(),
            None => PhysicalHop::Tcp(&self.origin),
        }
    }

    /// Required encryption on the immediate hop, not end-to-end authentication.
    pub fn security(&self) -> HopSecurity {
        if self.proxy.is_none() && self.origin.scheme() == "https" {
            HopSecurity::Tls
        } else {
            HopSecurity::Plaintext
        }
    }

    /// The route's default wire target form.
    ///
    /// `project_request` separately preserves `OPTIONS *` on direct routes.
    pub fn target_form(&self) -> TargetForm {
        if self.proxy.is_some() {
            TargetForm::AbsoluteForm
        } else {
            TargetForm::OriginForm
        }
    }

    /// Whether this route may use an H1 connection.
    pub fn allows_http1(&self) -> bool {
        self.protocol != ProtocolPolicy::Http2Only
    }

    /// Whether this route may use H2; plaintext H2 requires explicit prior knowledge.
    pub fn allows_http2(&self) -> bool {
        self.protocol == ProtocolPolicy::Http2Only
            || (self.protocol == ProtocolPolicy::Negotiate && self.security() == HopSecurity::Tls)
    }

    /// URI for a route-aware connector, not necessarily its physical dial URI.
    ///
    /// Forward routes use HTTP with the logical origin's canonical authority,
    /// while `physical_hop` identifies the proxy. A logical HTTPS default port
    /// stays omitted in this forward-proxy view; its effective port remains in `origin`.
    /// The connector must be configured for this exact hop before invocation.
    pub fn connector_uri(&self) -> Uri {
        if self.proxy.is_some() {
            self.origin.uri_with_scheme("http")
        } else {
            self.origin.as_uri()
        }
    }

    /// Checks that `request` can be sent on this route. Its body is not
    /// touched, and `B` needs no trait bounds.
    ///
    /// An absent Host is allowed, because `project_request` adds it from the
    /// route. A present Host must be a single authority matching the logical
    /// origin's host and effective port, even on a plaintext forward hop.
    /// CONNECT and all upgrade indications are rejected.
    pub fn validate_request<B>(&self, request: &Request<B>) -> Result<(), RouteError> {
        self.projected_target(request).map(|_| ())
    }

    /// Rewrites the URI and, when Host is absent, adds the canonical Host, as
    /// one atomic change.
    ///
    /// On error the entire request is unchanged. On success the method, version,
    /// existing headers (including sensitivity), extensions, and B are unchanged.
    /// Absolute inputs must name the logical origin; origin-form inputs use this
    /// explicit route. Direct `OPTIONS *` is supported, forward asterisk is not.
    /// The result is the wire head, and the rewrite is neither lossless nor
    /// idempotent. A forwarded HTTPS head gets an HTTP wire URI, but the
    /// route's logical origin is still HTTPS. Already parsed `http::Uri` inputs
    /// cannot reveal discarded fragments.
    pub fn project_request<B>(&self, request: &mut Request<B>) -> Result<(), RouteError> {
        let target = self.projected_target(request)?;
        let host = if request.headers().contains_key(HOST) {
            None
        } else {
            Some(
                HeaderValue::from_str(self.origin.authority().as_str())
                    .map_err(|_| RouteError::InvalidHostHeader)?,
            )
        };
        *request.uri_mut() = target;
        if let Some(host) = host {
            request.headers_mut().insert(HOST, host);
        }
        Ok(())
    }

    fn projected_target<B>(&self, request: &Request<B>) -> Result<Uri, RouteError> {
        if request.method() == Method::CONNECT {
            return Err(RouteError::ConnectUnsupported);
        }
        if request.headers().contains_key(UPGRADE)
            || request.headers().contains_key("http2-settings")
            || request.headers().get_all(CONNECTION).iter().any(|value| {
                value
                    .as_bytes()
                    .split(|byte| *byte == b',')
                    .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
            })
        {
            return Err(RouteError::UpgradeUnsupported);
        }
        let host_fields = request.headers().get_all(HOST);
        let mut hosts = host_fields.iter();
        let host = hosts.next();
        if hosts.next().is_some() {
            return Err(RouteError::DuplicateHost);
        }
        if let Some(host) = host {
            let host: Authority = host
                .to_str()
                .map_err(|_| RouteError::InvalidHostHeader)?
                .parse()
                .map_err(|_| RouteError::InvalidHostHeader)?;
            let origin = Origin::from_authority(self.origin.scheme(), &host)
                .map_err(|_| RouteError::InvalidHostHeader)?;
            if origin != self.origin {
                return Err(RouteError::HostMismatch);
            }
        }
        let uri = request.uri();
        match (uri.scheme_str(), uri.authority()) {
            (Some(scheme), Some(authority)) => {
                if Origin::from_authority(scheme, authority)? != self.origin {
                    return Err(RouteError::OriginMismatch);
                }
            }
            (None, None) => {}
            _ => return Err(RouteError::UnsupportedTarget),
        }
        let path = uri.path_and_query().ok_or(RouteError::UnsupportedTarget)?;
        if path.as_str() == "*" {
            return if uri.scheme().is_none()
                && self.proxy.is_none()
                && request.method() == Method::OPTIONS
            {
                Ok(uri.clone())
            } else {
                Err(RouteError::UnsupportedTarget)
            };
        }
        let path = if path.as_str().starts_with('/') {
            path.as_str().to_owned()
        } else if uri.scheme().is_some() && path.as_str().starts_with('?') {
            format!("/{}", path.as_str())
        } else {
            return Err(RouteError::UnsupportedTarget);
        };
        if self.proxy.is_some() {
            Ok(format!("http://{}{path}", self.origin.authority()).parse()?)
        } else {
            Ok(path.parse()?)
        }
    }
}
