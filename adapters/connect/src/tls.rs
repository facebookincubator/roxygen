//! TLS negotiation for an application-owned proxy socket.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::SocketAddress;
use crate::TransportEndpoint;
use crate::TransportSecurity;
use crate::runtime::ConnectedIo;
use crate::runtime::DialError;

/// Authenticates an already connected proxy socket and requires HTTP/2.
///
/// A copy of `config` offers h2 followed by HTTP/1.1, so a proxy that negotiates
/// HTTP/1.1 returns [`DialError::H2NotNegotiated`] rather than a TLS failure. All
/// other TLS settings are preserved. Socket options and deadlines belong to the
/// caller; [`crate::ConnectClient`] bounds its connector's future by the acquire
/// timeout.
pub async fn connect_tls(
    tcp: TcpStream,
    server_name: ServerName<'static>,
    config: Arc<ClientConfig>,
) -> Result<ConnectedIo, DialError> {
    let other = |error: io::Error| DialError::Other(Arc::new(error));
    let local_endpoint = endpoint(tcp.local_addr().map_err(other)?);
    let peer_endpoint = endpoint(tcp.peer_addr().map_err(other)?);
    let mut config = (*config).clone();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls = TlsConnector::from(Arc::new(config))
        .connect(server_name, tcp)
        .await
        .map_err(other)?;
    if tls.get_ref().1.alpn_protocol() != Some(b"h2".as_slice()) {
        return Err(DialError::H2NotNegotiated);
    }
    Ok(ConnectedIo {
        io: Box::new(tls),
        local_endpoint,
        peer_endpoint,
        security: TransportSecurity::Tls,
    })
}

fn endpoint(address: SocketAddr) -> TransportEndpoint {
    TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
}
