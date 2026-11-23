/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_transport::tls::TlsAlpnMismatch;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::TlsServerSettings;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ClientConfig;
use rustls::ClientConnection;
use rustls::RootCertStore;
use rustls::StreamOwned;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;

use crate::scoped_server::ScopedHttpServer;
use crate::server::HandlerFactory;
use crate::server::ServerConfig;
use crate::server::StaticTlsCredentials;

const TIMEOUT: Duration = Duration::from_secs(10);

/// Reads each request head and responds with `200 OK`.
struct OkHandler;

impl HttpHandler for OkHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            request.read_header_event().await?;
            let body = BufQueue::from_bytes(Bytes::from_static(b"OK"), 2)
                .expect("fixed response body is bounded");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("status and body are valid"),
            )))
        })
    }
}

fn ok_factory() -> HandlerFactory {
    Arc::new(|_| Rc::new(OkHandler) as HttpHandlerRef)
}

/// Sends one closing HTTP/1.1 request over a new TLS connection, checks that
/// an HTTP/1.1 `200 OK` comes back, and returns the protocol that ALPN
/// selected for the client.
fn closing_http1_get(address: SocketAddr, config: &Arc<ClientConfig>) -> Option<Vec<u8>> {
    let socket = TcpStream::connect_timeout(&address, TIMEOUT).unwrap();
    socket.set_read_timeout(Some(TIMEOUT)).unwrap();
    socket.set_write_timeout(Some(TIMEOUT)).unwrap();
    let connection = ClientConnection::new(
        Arc::clone(config),
        ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();
    let mut stream = StreamOwned::new(connection, socket);
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    if let Err(error) = stream.read_to_end(&mut response) {
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof, "{error}");
    }
    assert!(
        response.starts_with(b"HTTP/1.1 200 OK\r\n"),
        "the HTTP/1.1 session answers: {}",
        String::from_utf8_lossy(&response)
    );
    stream.conn.alpn_protocol().map(<[u8]>::to_vec)
}

#[test]
fn no_alpn_offered_dispatches_h1() {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = generated.cert.der().to_vec();
    let options = TlsServerOptions::from_settings(
        TlsServerSettings::new(
            vec![certificate.clone().into_boxed_slice()],
            generated
                .signing_key
                .serialized_der()
                .to_vec()
                .into_boxed_slice(),
        )
        .with_alpn(
            vec![b"h2".to_vec().into(), b"http/1.1".to_vec().into()],
            TlsAlpnMismatch::ContinueWithoutAlpn,
        ),
    )
    .unwrap();
    let server = ScopedHttpServer::start(
        ServerConfig {
            tls_credentials: Some(Arc::new(StaticTlsCredentials::new(options))),
            ..ServerConfig::default()
        },
        ok_factory(),
        None,
    )
    .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(certificate)).unwrap();
    let client = |protocols: Vec<Vec<u8>>| {
        let mut config = ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots.clone())
            .with_no_client_auth();
        config.alpn_protocols = protocols;
        Arc::new(config)
    };
    let address = server.address().unwrap();

    assert_eq!(
        closing_http1_get(address, &client(Vec::new())),
        None,
        "a client without ALPN gets no protocol and is served HTTP/1.1"
    );
    assert_eq!(
        closing_http1_get(address, &client(vec![b"x-unknown/1".to_vec()])),
        None,
        "a client offering none of the server's protocols gets no protocol and is served HTTP/1.1"
    );
}
