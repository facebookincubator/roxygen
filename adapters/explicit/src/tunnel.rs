//! CONNECT tunnels: tunnel configuration, the CONNECT response head, and the CONNECT over an
//! attached proxy session.
//!
//! Host-agnostic, like `exchange`: the proxy dial and the tokio-rustls handshake over the tunnel
//! belong to `runtime`.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderName;
use http::HeaderValue;
use http::header;
use http::uri::Authority;
use roxygen_client::connector::ClientSession;
use roxygen_connect::async_transport::HttpConnectAsyncTransport;
use roxygen_connect::experimental::ConnectResponseHeadExt as _;
use roxygen_connect::session::LocalConnectFuture;
use roxygen_connect::stream::DEFAULT_EGRESS_BUFFER_SIZE;
use roxygen_connect::stream::HttpConnectStream;
use roxygen_connect::stream::RequestHeaderMap;
use roxygen_core::error::HttpError;
use roxygen_core::message::HttpMessage;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::established::AdoptedIoCompletion;
use roxygen_transport::established::AdoptedIoControl;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;

use crate::error::ErrorKind;
use crate::error::ExplicitError;
use crate::error::Phase;

/// A CONNECT tunnel through an HTTP/1.1 proxy, which a `Security::Tunnel` target dials at its
/// address, with TLS to the destination inside it.
#[derive(Debug)]
pub struct Tunnel {
    authority: Authority,
    connect_headers: RequestHeaderMap,
    tls_config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
}

impl Tunnel {
    /// Validates a tunnel. Performs no I/O.
    ///
    /// `authority` is the CONNECT request target and its `Host`, sent as given: `host:port`,
    /// `v4:port`, or `[v6]:port`. `connect_headers` decorate the CONNECT only; a `host` header is
    /// dropped, because the CONNECT's `Host` is the authority. `tls_config` must offer no ALPN
    /// protocol or exactly `http/1.1`. `tls_host` is the only name the tunnel's TLS verifies and,
    /// unless it is an IP literal, its SNI, as for [`crate::Security::tls`].
    pub fn new(
        authority: Authority,
        connect_headers: impl IntoIterator<Item = (HeaderName, HeaderValue)>,
        tls_config: Arc<ClientConfig>,
        tls_host: &str,
    ) -> Result<Self, ExplicitError> {
        let refuse =
            |detail: String| ExplicitError::new(Phase::Submit, ErrorKind::Protocol, detail);
        // `Authority::from_static` checks bytes only up to the first `/`, `?`, or `#` and keeps
        // the rest, which the CONNECT request line would carry verbatim.
        if Authority::try_from(authority.as_str()).is_err() {
            return Err(refuse(format!(
                "CONNECT authority {:?} does not parse as a whole",
                authority.as_str()
            )));
        }
        if authority.as_str().contains('@') {
            return Err(refuse("CONNECT authority carries userinfo".to_owned()));
        }
        if authority.host().is_empty() || authority.port_u16().is_none() {
            return Err(refuse(format!(
                "CONNECT authority {authority} needs a host and a port"
            )));
        }

        let owned = [
            header::CONNECTION,
            header::CONTENT_LENGTH,
            header::TRANSFER_ENCODING,
            header::UPGRADE,
        ];
        let mut headers = RequestHeaderMap::new();
        for (name, value) in connect_headers {
            if name == header::HOST {
                continue;
            }
            if owned.contains(&name) {
                return Err(refuse(format!(
                    "CONNECT header {name} belongs to the adapter"
                )));
            }
            let key: Box<[u8]> = name.as_str().as_bytes().into();
            if headers.insert(key, value.as_bytes().into()).is_some() {
                return Err(refuse(format!("CONNECT header {name} is repeated")));
            }
        }

        let alpn = &tls_config.alpn_protocols;
        if !(alpn.is_empty() || (alpn.len() == 1 && alpn[0] == b"http/1.1")) {
            return Err(ExplicitError::new(
                Phase::Tls,
                ErrorKind::Protocol,
                "the tunnel's TLS configuration offers ALPN other than http/1.1",
            ));
        }
        let server_name = server_name(tls_host)?;
        Ok(Self {
            authority,
            connect_headers: headers,
            tls_config,
            server_name,
        })
    }

    pub(crate) fn tls_config(&self) -> &Arc<ClientConfig> {
        &self.tls_config
    }

    pub(crate) fn server_name(&self) -> &ServerName<'static> {
        &self.server_name
    }
}

/// The TLS server name for `host`: a DNS name, or an IPv4 or IPv6 literal with or without one
/// pair of surrounding brackets, for which rustls sends no SNI.
pub(crate) fn server_name(host: &str) -> Result<ServerName<'static>, ExplicitError> {
    let literal = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(ip) = literal.parse::<IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(host.to_owned()).map_err(|error| {
        ExplicitError::new(
            Phase::Tls,
            ErrorKind::Other,
            format!("invalid TLS server name {host:?}: {error}"),
        )
    })
}

/// A final CONNECT response head: its status and its header lines in wire order, duplicates
/// kept.
///
/// Each name is as Roxygen delivered it (`DIV-011`): a common header takes the common-header
/// table's spelling, such as `Proxy-Status`, and any other name keeps the proxy's spelling.
/// Match names case-insensitively, as [`ConnectHead::get`] does.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectHead {
    pub status: u16,
    pub lines: Vec<(Bytes, Bytes)>,
}

impl ConnectHead {
    /// The value of the first line named `name`, matched case-insensitively.
    pub fn get(&self, name: &str) -> Option<&Bytes> {
        self.lines
            .iter()
            .find(|(line, _)| line.eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, value)| value)
    }

    /// The values of every line named `name`, matched case-insensitively, in wire order.
    pub fn get_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Bytes> + 'a {
        self.lines
            .iter()
            .filter(move |(line, _)| line.eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, value)| value)
    }

    /// Copies a response head's lines, as views of one buffer.
    pub(crate) fn from_message(status: u16, message: &HttpMessage) -> Self {
        let headers = message.headers();
        let mut buffer = Vec::with_capacity(
            headers
                .iter()
                .map(|(_, name, value)| name.len() + value.len())
                .sum(),
        );
        let spans: Vec<_> = headers
            .iter()
            .map(|(_, name, value)| {
                let start = buffer.len();
                buffer.extend_from_slice(name);
                let middle = buffer.len();
                buffer.extend_from_slice(value);
                (start, middle, buffer.len())
            })
            .collect();
        let buffer = Bytes::from(buffer);
        Self {
            status,
            lines: spans
                .into_iter()
                .map(|(start, middle, end)| {
                    (buffer.slice(start..middle), buffer.slice(middle..end))
                })
                .collect(),
        }
    }
}

/// Reserves the CONNECT on the proxy session and starts `connect_unique`, which takes a clone
/// of the session and drains it once the stream drops. A reservation failure is the proxy
/// leg's; the returned future's failures are the CONNECT's.
pub(crate) fn connect(
    context: &WorkerContext,
    proxy: &ClientSession,
    tunnel: &Tunnel,
    timeout: Duration,
    cancellation: CancellationToken,
) -> Result<LocalConnectFuture<Result<HttpConnectStream, HttpError>>, HttpError> {
    let reservation = proxy.reserve_request()?;
    Ok(HttpConnectStream::connect_unique(
        context,
        proxy.clone(),
        reservation,
        tunnel.authority.as_str(),
        timeout,
        tunnel.connect_headers.clone(),
        DEFAULT_EGRESS_BUFFER_SIZE,
        cancellation,
    ))
}

/// Takes an established tunnel's 2xx head and wraps the tunnel for TLS.
pub(crate) fn established(
    context: &WorkerContext,
    mut stream: HttpConnectStream,
) -> (ConnectHead, HttpConnectAsyncTransport) {
    let head = stream
        .take_connect_response()
        .map(|head| ConnectHead::from_message(head.response_status().unwrap_or_default(), &head))
        .unwrap_or_default();
    (head, HttpConnectAsyncTransport::new(context, stream))
}

/// Owns a clone of the proxy session and cancels it, with the proxy leg's adopted I/O, when
/// dropped.
pub(crate) struct ProxyGuard {
    session: ClientSession,
    control: AdoptedIoControl,
}

impl ProxyGuard {
    pub(crate) fn new(session: ClientSession, control: AdoptedIoControl) -> Self {
        Self { session, control }
    }

    pub(crate) fn session(&self) -> &ClientSession {
        &self.session
    }
}

impl Drop for ProxyGuard {
    fn drop(&mut self) {
        self.session.cancel();
        self.control.cancel();
    }
}

/// What the exchange hands the tunnel cleanup once it has adopted the inner stream: that
/// stream's retirement, and the proxy session's guard.
pub(crate) struct TunnelHandoff {
    pub(crate) inner: AdoptedIoCompletion,
    pub(crate) guard: ProxyGuard,
}

/// The guard, which the exchange holds until it adopts the inner stream, and the channel that
/// then moves it, with the inner stream's retirement, into the tunnel cleanup.
pub(crate) struct TunnelLink {
    pub(crate) handoff: futures::channel::oneshot::Sender<TunnelHandoff>,
    pub(crate) guard: ProxyGuard,
}

impl TunnelLink {
    /// Moves the guard into the tunnel cleanup. A cleanup that is gone drops the guard here.
    pub(crate) fn hand_off(self, inner: AdoptedIoCompletion) {
        let _ = self.handoff.send(TunnelHandoff {
            inner,
            guard: self.guard,
        });
    }
}
