/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Version;
use http_body_util::BodyExt;
use http_body_util::Full;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_transport::established::EstablishedIo;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::client::connector::ConnectedIo;
use crate::client_tests as client_fixture;
use crate::server::ServerExchange;
use crate::server::ServerOptions;
use crate::server::prepare_connection;
use crate::server_network_tests::fixture as server_fixture;

const REQUEST_BODY: &[u8] = b"version request\n";
const RESPONSE_BODY: &[u8] = b"version response\n";

fn expected_version(protocol: CodecProtocol) -> Version {
    match protocol {
        CodecProtocol::Http11 => Version::HTTP_11,
        CodecProtocol::Http2 => Version::HTTP_2,
        _ => panic!("version fixture supports only H1/H2"),
    }
}

#[test]
fn client_response_version_matches_actual_h1_h2_connection() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        client_fixture::case(move |fixture| {
            Box::pin(async move {
                let version = expected_version(protocol);
                let handler: client_fixture::Handler = Arc::new(move |request| {
                    Box::pin(async move {
                        assert_eq!(request.version(), version);
                        assert_eq!(request.uri().path(), "/version");
                        let received = request.into_body().collect().await.unwrap();
                        assert!(received.trailers().is_none());
                        assert_eq!(received.to_bytes(), REQUEST_BODY);
                        Response::builder()
                            .status(StatusCode::ACCEPTED)
                            .header("x-version-source", "real-peer")
                            .body(client_fixture::full(Bytes::from_static(RESPONSE_BODY)))
                            .unwrap()
                    })
                });
                let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
                let address = listener.local_addr().unwrap();
                let peers = Arc::clone(&fixture.peers);
                let executor = fixture.executor.clone();
                let (completed, completion) = oneshot::channel();
                let mut completed = Some(completed);
                let identity = Arc::new(17_u64);
                let expected_identity = Arc::clone(&identity);
                let connector = client_fixture::TestConnector::new(move |request| {
                    let listener = Arc::clone(&listener);
                    let peers = Arc::clone(&peers);
                    let executor = executor.clone();
                    let handler = Arc::clone(&handler);
                    let identity = Arc::clone(&identity);
                    let completed = completed.take().expect("one actual connection");
                    Box::pin(async move {
                        let (client, accepted) =
                            tokio::join!(TcpStream::connect(address), listener.accept());
                        let client = client?;
                        let (peer, _) = accepted?;
                        let endpoint = |address: std::net::SocketAddr| {
                            TransportEndpoint::Socket(SocketAddress::new(
                                address.ip().to_string(),
                                address.port(),
                            ))
                        };
                        let local = endpoint(client.local_addr()?);
                        let remote = endpoint(client.peer_addr()?);
                        peers.lock().unwrap().spawn(async move {
                            client_fixture::serve_peer(peer, protocol, handler, executor).await;
                            let _ = completed.send(());
                        });
                        let mut extensions = http::Extensions::new();
                        extensions.insert(identity);
                        Ok(ConnectedIo::new(
                            request.route().clone(),
                            EstablishedIo::new(
                                Box::pin(client) as client_fixture::Io,
                                local,
                                remote,
                                None,
                            ),
                            protocol,
                            TransportSecurity::Plaintext,
                        )
                        .with_extensions(extensions))
                    })
                });
                let calls = Arc::clone(&connector.calls);
                let mut options = client_fixture::options();
                options.maximum_physical_connections = 1;
                options.maximum_connector_tasks = 1;
                options.pool.max_connections = 1;
                let client = fixture.start(connector, options).await;
                let route = client.route(client_fixture::route(protocol)).unwrap();
                let response = client
                    .try_request(
                        &route,
                        Request::builder()
                            .method("POST")
                            .uri("/version")
                            .body(Full::new(Bytes::from_static(REQUEST_BODY)))
                            .unwrap(),
                    )
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(response.version(), version);
                assert_eq!(response.status(), StatusCode::ACCEPTED);
                assert_eq!(response.headers()["x-version-source"], "real-peer");
                assert!(Arc::ptr_eq(
                    response.extensions().get::<Arc<u64>>().unwrap(),
                    &expected_identity,
                ));
                let exchange = response.body().exchange();
                let received = response.into_body().collect().await.unwrap();
                assert!(received.trailers().is_none());
                assert_eq!(received.to_bytes(), RESPONSE_BODY);
                exchange.wait().await.unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                drop(route);
                drop(client);
                fixture.close_clients().await;
                assert!(fixture.runs.is_empty());
                assert_eq!(fixture.run_results.len(), 1);
                assert!(fixture.run_results[0].is_ok(), "{:?}", fixture.run_results);
                completion.await.unwrap();
                assert!(
                    !fixture.stop.is_open(),
                    "peer finished before fixture cancellation"
                );
            })
        });
    }
}

struct ReleasePeerShutdown(server_fixture::Gate);

impl Drop for ReleasePeerShutdown {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[test]
fn server_request_version_matches_actual_h1_h2_connection() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        server_fixture::run_test(|worker, _| async move {
            let version = expected_version(protocol);
            let probe = server_fixture::Probe::new();
            let body_probe = probe.clone();
            let (observed, mut observation) = mpsc::channel(1);
            let service = server_fixture::service(probe.clone(), move |request| {
                assert_eq!(request.version(), version);
                assert_eq!(request.uri().path(), "/version");
                let context = request
                    .extensions()
                    .get::<Arc<ConnectionContext>>()
                    .unwrap();
                assert_eq!(context.security(), TransportSecurity::Plaintext);
                assert_eq!(
                    context.peer_identity(),
                    Some(b"verified-fixture-peer".as_slice())
                );
                assert!(matches!(
                    context.local_endpoint(),
                    TransportEndpoint::Socket(_)
                ));
                assert!(matches!(
                    context.peer_endpoint(),
                    TransportEndpoint::Socket(_)
                ));
                let exchange = request
                    .extensions()
                    .get::<ServerExchange>()
                    .unwrap()
                    .clone();
                observed.try_send(exchange).unwrap();
                let body_probe = body_probe.clone();
                async move {
                    let received = request.into_body().collect().await.unwrap();
                    assert!(received.trailers().is_none());
                    assert_eq!(received.to_bytes(), REQUEST_BODY);
                    Ok(Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(server_fixture::TestBody::data(RESPONSE_BODY).probed(body_probe))
                        .unwrap())
                }
            });
            let io = server_fixture::Probe::new();
            let (established, stream) = server_fixture::sockets(io.clone(), None).await;
            let mut options = ServerOptions::new(protocol, TransportSecurity::Plaintext);
            options.max_exchanges = NonZeroUsize::new(1).unwrap();
            options.dispatch_capacity = NonZeroUsize::new(1).unwrap();
            let mut parent = server_fixture::Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(worker, established, service, options).unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            let mut peers = server_fixture::PeerTasks::new();
            let peer_shutdown = server_fixture::Gate::default();
            let release_peer = ReleasePeerShutdown(peer_shutdown.clone());
            let peer_io = server_fixture::Probe::new();
            let (mut peer, completed) = server_fixture::PeerConnection::connect_gated(
                stream,
                protocol,
                &peers,
                peer_shutdown.clone(),
                peer_io.clone(),
            )
            .await;
            let response = peer
                .send(
                    Request::builder()
                        .method("POST")
                        .uri("/version")
                        .header("host", "fixture.invalid")
                        .body(server_fixture::TestBody::data(REQUEST_BODY))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.version(), version);
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let received = response.into_body().collect().await.unwrap();
            assert!(received.trailers().is_none());
            assert_eq!(received.to_bytes(), RESPONSE_BODY);
            observation
                .recv()
                .await
                .unwrap()
                .wait_completed()
                .await
                .unwrap();
            assert!(connection.begin_drain());
            parent.reap().await.unwrap();
            peer_shutdown.entered().await;
            drop(release_peer);
            completed.await.unwrap().unwrap();
            drop(peer);
            peers.finish().await;
            assert!(parent.tasks.is_empty());
            assert!(!parent.slots.is_closed());
            assert_eq!(parent.slots.available_permits(), 1);
            assert_eq!(probe.count("service call"), 1);
            assert_eq!(probe.count("service drop"), 1);
            assert_eq!(probe.count("body drop"), 1);
            assert_eq!(io.count("io drop"), 1);
            assert_eq!(peer_io.count("io drop"), 1);
            probe.assert_app("service call");
            probe.assert_app("service drop");
            probe.assert_app("body drop");
            io.assert_native("io drop");
            peer_io.assert_app("io drop");
        });
    }
}
