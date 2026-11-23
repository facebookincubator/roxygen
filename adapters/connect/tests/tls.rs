/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Proxy TLS negotiation and connector error classification.

use std::net::SocketAddr;
use std::sync::Arc;

use roxygen_connect_client::DialError;
use roxygen_connect_client::SocketAddress;
use roxygen_connect_client::TransportEndpoint;
use roxygen_connect_client::TransportSecurity;
use roxygen_connect_client::connect_tls;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::proxy::TEST_TIMEOUT;
use crate::proxy::tls_identity;

async fn sockets() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proxy listener");
    let address = listener.local_addr().expect("proxy address");
    let (client, server) = tokio::join!(TcpStream::connect(address), listener.accept());
    (
        client.expect("proxy dial"),
        server.expect("proxy accepts").0,
    )
}

fn endpoint(address: SocketAddr) -> TransportEndpoint {
    TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
}

#[tokio::test]
async fn h2_negotiation_preserves_the_callers_config_and_socket_endpoints() {
    timeout(TEST_TIMEOUT, async {
        let (server_config, client_config) = tls_identity(&[b"h2", b"http/1.1"]);
        let mut client_config = (*client_config).clone();
        client_config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let client_config = Arc::new(client_config);
        let (client, server) = sockets().await;
        let local = client.local_addr().expect("client address");
        let peer = client.peer_addr().expect("proxy address");
        let acceptor = TlsAcceptor::from(server_config);
        let name = ServerName::try_from("proxy.example").expect("server name");
        let (connected, server) = tokio::join!(
            connect_tls(client, name, Arc::clone(&client_config)),
            acceptor.accept(server),
        );
        let mut connected = connected.expect("HTTP/2 proxy connection");
        let mut server = server.expect("proxy TLS handshake");
        assert_eq!(server.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
        assert_eq!(server.get_ref().1.server_name(), Some("proxy.example"));
        assert_eq!(connected.local_endpoint, endpoint(local));
        assert_eq!(connected.peer_endpoint, endpoint(peer));
        assert_eq!(connected.security, TransportSecurity::Tls);
        assert_eq!(client_config.alpn_protocols, [b"http/1.1".to_vec()]);

        let mut received = [0; 5];
        let (sent, read) = tokio::join!(
            async {
                connected.io.write_all(b"hello").await?;
                connected.io.flush().await
            },
            server.read_exact(&mut received),
        );
        sent.expect("client writes over returned transport");
        read.expect("proxy reads over TLS");
        assert_eq!(&received, b"hello");
    })
    .await
    .expect("TLS negotiation completes");
}

#[tokio::test]
async fn successful_tls_without_h2_has_a_distinct_error() {
    timeout(TEST_TIMEOUT, async {
        for protocols in [&[b"http/1.1".as_slice()][..], &[][..]] {
            let (server_config, client_config) = tls_identity(protocols);
            let (client, server) = sockets().await;
            let acceptor = TlsAcceptor::from(server_config);
            let name = ServerName::try_from("proxy.example").expect("server name");
            let (connected, server) = tokio::join!(
                connect_tls(client, name, client_config),
                acceptor.accept(server),
            );
            assert!(matches!(connected, Err(DialError::H2NotNegotiated)));
            let server = server.expect("TLS succeeds without HTTP/2");
            assert_eq!(
                server.get_ref().1.alpn_protocol(),
                protocols.first().copied()
            );
        }
    })
    .await
    .expect("TLS negotiation completes");
}

#[tokio::test]
async fn certificate_or_name_failures_are_not_h2_negotiation_failures() {
    timeout(TEST_TIMEOUT, async {
        let (server_config, trusted) = tls_identity(&[b"h2"]);
        let untrusted = Arc::new(
            ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
                .with_safe_default_protocol_versions()
                .expect("TLS versions")
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        for (config, name) in [(trusted, "wrong.example"), (untrusted, "proxy.example")] {
            let (client, server) = sockets().await;
            let acceptor = TlsAcceptor::from(Arc::clone(&server_config));
            let name = ServerName::try_from(name).expect("server name");
            let (connected, server) =
                tokio::join!(connect_tls(client, name, config), acceptor.accept(server),);
            assert!(matches!(connected, Err(DialError::Other(_))));
            assert!(server.is_err(), "the proxy observes a failed TLS handshake");
        }
    })
    .await
    .expect("TLS rejection completes");
}

#[tokio::test]
async fn a_peer_closing_during_tls_is_a_connector_failure() {
    timeout(TEST_TIMEOUT, async {
        let (_, client_config) = tls_identity(&[b"h2"]);
        let (client, server) = sockets().await;
        drop(server);
        let name = ServerName::try_from("proxy.example").expect("server name");
        let connected = connect_tls(client, name, client_config).await;
        assert!(matches!(connected, Err(DialError::Other(_))));
    })
    .await
    .expect("TLS transport failure completes");
}
