//! Trusted connection metadata, independent of request headers and worker state.

use std::sync::Arc;

use crate::http::CodecProtocol;
use crate::message::SocketAddress;

/// An actual transport endpoint, independent of the logical HTTP origin.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum TransportEndpoint {
    /// An Internet socket address in the existing Roxygen representation.
    Socket(SocketAddress),
    /// A Unix-domain pathname, represented as raw filesystem bytes.
    UnixPath(Box<[u8]>),
    /// A Linux abstract Unix-domain name, without its leading NUL byte.
    UnixAbstract(Box<[u8]>),
    /// An unnamed Unix-domain socket.
    UnnamedUnix,
    /// The connector did not supply an endpoint.
    #[default]
    Unknown,
}

impl TransportEndpoint {
    /// Returns an Internet address only; Unix endpoints are never hostnames.
    pub fn socket_address(&self) -> Option<&SocketAddress> {
        match self {
            Self::Socket(address) => Some(address),
            Self::UnixPath(_) | Self::UnixAbstract(_) | Self::UnnamedUnix | Self::Unknown => None,
        }
    }
}

/// Security of the immediate connection, not of a proxied origin.
/// TLS describes encryption; it does not assert certificate authentication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportSecurity {
    Unknown,
    Plaintext,
    Tls,
}

/// The origin scheme selected by a trusted connector or routing policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OriginSecurity {
    Http,
    Https,
}

/// A logical destination supplied by routing configuration, never by ingress headers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpstreamOrigin {
    authority: Box<str>,
    security: OriginSecurity,
}

impl UpstreamOrigin {
    /// Records a trusted routing decision without parsing a request authority.
    pub fn new(authority: impl Into<Box<str>>, security: OriginSecurity) -> Self {
        Self {
            authority: authority.into(),
            security,
        }
    }

    /// Returns the configured logical authority, not an immediate socket name.
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// Returns the configured origin scheme, independent of immediate-hop TLS.
    pub fn security(&self) -> OriginSecurity {
        self.security
    }
}

/// An immutable, transferable snapshot supplied by a trusted connector/acceptor.
///
/// This is data, not a session-liveness capability. Identity bytes preserve the
/// transport's encoding and verification policy; their presence does not prove
/// TLS authentication. Never construct this snapshot from request headers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionContext {
    local_endpoint: TransportEndpoint,
    peer_endpoint: TransportEndpoint,
    security: TransportSecurity,
    peer_identity: Option<Arc<[u8]>>,
    upstream_origin: Option<UpstreamOrigin>,
    http_protocol: Option<CodecProtocol>,
    requested_server_name: Option<Box<str>>,
    negotiated_application_protocol: Option<Box<[u8]>>,
    tls_cipher_suite: Option<Box<str>>,
    tls_key_exchange_group: Option<Box<str>>,
}

impl ConnectionContext {
    /// Records trusted transport facts without adding authentication guarantees.
    pub fn new(
        local_endpoint: TransportEndpoint,
        peer_endpoint: TransportEndpoint,
        security: TransportSecurity,
        peer_identity: Option<Arc<[u8]>>,
    ) -> Self {
        Self {
            local_endpoint,
            peer_endpoint,
            security,
            peer_identity,
            upstream_origin: None,
            http_protocol: None,
            requested_server_name: None,
            negotiated_application_protocol: None,
            tls_cipher_suite: None,
            tls_key_exchange_group: None,
        }
    }

    /// Adds an optional trusted logical origin without changing immediate-hop facts.
    pub fn with_upstream_origin(mut self, origin: Option<UpstreamOrigin>) -> Self {
        self.upstream_origin = origin;
        self
    }

    /// Records the wire protocol selected by the connector or acceptor.
    pub fn with_http_protocol(mut self, protocol: CodecProtocol) -> Self {
        self.http_protocol = Some(protocol);
        self
    }

    /// Records SNI and ALPN supplied by a completed TLS handshake.
    pub fn with_tls_handshake(
        mut self,
        requested_server_name: Option<Box<str>>,
        negotiated_application_protocol: Option<Box<[u8]>>,
    ) -> Self {
        self.requested_server_name = requested_server_name;
        self.negotiated_application_protocol = negotiated_application_protocol;
        self
    }

    /// Records the cipher suite and key-exchange group of a completed TLS
    /// handshake.
    pub fn with_tls_parameters(
        mut self,
        cipher_suite: Option<Box<str>>,
        key_exchange_group: Option<Box<str>>,
    ) -> Self {
        self.tls_cipher_suite = cipher_suite;
        self.tls_key_exchange_group = key_exchange_group;
        self
    }

    /// Returns the selected wire protocol, independently of any request head.
    pub fn http_protocol(&self) -> Option<CodecProtocol> {
        self.http_protocol
    }

    /// Returns SNI observed during the TLS handshake, when available.
    pub fn requested_server_name(&self) -> Option<&str> {
        self.requested_server_name.as_deref()
    }

    /// Returns the negotiated TLS ALPN bytes, when available.
    pub fn negotiated_application_protocol(&self) -> Option<&[u8]> {
        self.negotiated_application_protocol.as_deref()
    }

    /// Returns the negotiated TLS cipher suite, such as
    /// `TLS13_AES_128_GCM_SHA256`, when available.
    pub fn tls_cipher_suite(&self) -> Option<&str> {
        self.tls_cipher_suite.as_deref()
    }

    /// Returns the negotiated TLS key-exchange group, such as `X25519`, when
    /// available.
    pub fn tls_key_exchange_group(&self) -> Option<&str> {
        self.tls_key_exchange_group.as_deref()
    }

    /// Returns the immediate local endpoint.
    pub fn local_endpoint(&self) -> &TransportEndpoint {
        &self.local_endpoint
    }

    /// Returns the immediate peer endpoint.
    pub fn peer_endpoint(&self) -> &TransportEndpoint {
        &self.peer_endpoint
    }

    /// Returns immediate-hop security; TLS alone does not imply authentication.
    pub fn security(&self) -> TransportSecurity {
        self.security
    }

    /// Returns the transport's unmodified identity encoding.
    pub fn peer_identity(&self) -> Option<&[u8]> {
        self.peer_identity.as_deref()
    }

    /// Returns the separately configured logical destination, when supplied.
    pub fn upstream_origin(&self) -> Option<&UpstreamOrigin> {
        self.upstream_origin.as_ref()
    }
}
