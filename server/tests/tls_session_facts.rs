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
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::TlsSessionFacts;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ClientConfig;
use rustls::ClientConnection;
use rustls::HandshakeKind;
use rustls::RootCertStore;
use rustls::StreamOwned;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;

use crate::acceptor::ConnectionInfo;
use crate::acceptor::TlsHandshakeObserver;
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

/// Records each accepted TLS connection's session facts.
#[derive(Default)]
struct SessionFactsObserver {
    sessions: Mutex<Vec<Option<TlsSessionFacts>>>,
}

impl TlsHandshakeObserver for SessionFactsObserver {
    fn on_success(&self, connection: &ConnectionInfo) {
        self.sessions
            .lock()
            .unwrap()
            .push(connection.context.tls_session());
    }
}

/// Sends one closing HTTP/1.1 request over a new TLS connection, reads the
/// whole response, and returns how the client's handshake went.
fn closing_request(address: SocketAddr, config: &Arc<ClientConfig>) -> Option<HandshakeKind> {
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
        "{}",
        String::from_utf8_lossy(&response)
    );
    stream.conn.handshake_kind()
}

#[test]
fn tls_session_facts_report_resumption() {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = generated.cert.der().to_vec();
    let options = TlsServerOptions::new(
        vec![certificate.clone().into_boxed_slice()],
        generated
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice(),
        vec![b"http/1.1".to_vec().into_boxed_slice()],
    )
    .unwrap();
    let observer = Arc::new(SessionFactsObserver::default());
    let server = ScopedHttpServer::start(
        ServerConfig {
            tls_credentials: Some(Arc::new(StaticTlsCredentials::new(options))),
            tls_observer: Some(observer.clone()),
            ..ServerConfig::default()
        },
        ok_factory(),
        None,
    )
    .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(certificate)).unwrap();
    let mut config = ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    // The client's default in-memory session store keeps the ticket that the
    // first connection receives, and the second connection offers it.
    let config = Arc::new(config);
    let address = server.address().unwrap();

    assert_eq!(closing_request(address, &config), Some(HandshakeKind::Full));
    assert_eq!(
        closing_request(address, &config),
        Some(HandshakeKind::Resumed)
    );

    let sessions = observer.sessions.lock().unwrap().clone();
    let facts: Vec<_> = sessions
        .iter()
        .map(|session| session.map(|facts| (facts.protocol_version, facts.resumed)))
        .collect();
    assert_eq!(
        facts,
        [Some((0x0304, false)), Some((0x0304, true))],
        "TLS 1.3 is 0x0304; only the second connection resumed"
    );
}
