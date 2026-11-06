use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use http::Response;
use http_body_util::BodyExt;
use http_body_util::Full;
use roxygen_core::http::CodecProtocol;
use tower::ServiceExt;
use tower_service::Service;

use crate::client::ClientError;
use crate::client::connector::ConnectorPolicy;
use crate::client_tests::Gate;
use crate::client_tests::H2DrainMode;
use crate::client_tests::Handler;
use crate::client_tests::TestBody;
use crate::client_tests::body_bytes;
use crate::client_tests::case;
use crate::client_tests::full;
use crate::client_tests::held_sibling_body;
use crate::client_tests::options;
use crate::client_tests::probe_connector;
use crate::client_tests::route;
use crate::route::ProtocolPolicy;
use crate::route::ProxyEndpoint;
use crate::route::RouteSpec;

#[test]
fn route_keys_separate_origins_hops_protocols_and_generations() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|_| Box::pin(async { Response::new(full("ok")) }));
            let connector = fixture.connector(CodecProtocol::Http11, Arc::clone(&handler));
            let calls = Arc::clone(&connector.calls);
            let mut settings = options();
            settings.maximum_routes = 3;
            let client = fixture.start(connector, settings).await;
            let first = client.route(route(CodecProtocol::Http11)).unwrap();
            let second = client.route(route(CodecProtocol::Http2)).unwrap();
            assert_ne!(first.spec(), second.spec());
            let secure = client
                .route(RouteSpec::direct(
                    "https://example.test".parse().unwrap(),
                    ProtocolPolicy::Http1Only,
                ))
                .unwrap();
            assert_ne!(first.spec(), secure.spec());
            assert!(matches!(
                client.route(RouteSpec::direct(
                    "http://other.test".parse().unwrap(),
                    ProtocolPolicy::Http1Only
                )),
                Err(ClientError::Capacity)
            ));
            assert!(client.route(route(CodecProtocol::Http11)).is_ok());
            let connector = fixture.connector(CodecProtocol::Http11, Arc::clone(&handler));
            let other = fixture.start(connector, options()).await;
            assert!(matches!(
                other.service::<Full<Bytes>>(&first),
                Err(ClientError::WrongGeneration)
            ));
            for path in ["/socket", "/socket/", "/socket/."] {
                let proxy = ProxyEndpoint::unix(path.into()).unwrap();
                let spec = RouteSpec::forward(
                    "https://example.test".parse().unwrap(),
                    proxy.clone(),
                    ProtocolPolicy::Http1Only,
                )
                .unwrap();
                let mut settings = options();
                settings.connector_policy = ConnectorPolicy::Forward(proxy);
                let connector = fixture.connector(CodecProtocol::Http11, Arc::clone(&handler));
                let proxy_client = fixture.start(connector, settings).await;
                assert_eq!(proxy_client.route(spec.clone()).unwrap().spec(), &spec);
                assert!(matches!(
                    proxy_client.route(route(CodecProtocol::Http11)),
                    Err(ClientError::ConnectorPolicy)
                ));
                assert!(matches!(
                    proxy_client.service::<Full<Bytes>>(&first),
                    Err(ClientError::WrongGeneration)
                ));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        })
    });
}

#[test]
fn invalid_route_connect_upgrade_and_host_mismatch_never_call_connector() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|_| Box::pin(async { Response::new(full("unused")) }));
            let connector = fixture.connector(CodecProtocol::Http11, handler);
            let calls = Arc::clone(&connector.calls);
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            for (method, uri, headers) in [
                ("GET", "http://other.test/", vec![]),
                ("CONNECT", "example.test:80", vec![]),
                ("GET", "/", vec![("upgrade", "websocket")]),
                ("GET", "/", vec![("connection", "keep-alive, upgrade")]),
                ("GET", "/", vec![("host", "other.test")]),
                (
                    "GET",
                    "/",
                    vec![("host", "example.test"), ("host", "example.test")],
                ),
            ] {
                let body = TestBody::new(vec![]);
                let drops = Arc::clone(&body.drops);
                let polls = Arc::clone(&body.polls);
                let mut request = Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(body)
                    .unwrap();
                for (name, value) in headers {
                    request.headers_mut().append(
                        http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        value.parse().unwrap(),
                    );
                }
                let rejected = client
                    .try_request(&route, request)
                    .err()
                    .expect("invalid request rejected before admission");
                assert_eq!(rejected.request.uri(), uri);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                drop(rejected);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        })
    });
}

#[test]
fn tower_ready_clone_and_call_transfer_one_permit() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|_| Box::pin(async { Response::new(full("ok")) }));
            let connector = fixture.connector(CodecProtocol::Http11, handler);
            let mut settings = options();
            settings.maximum_exchanges = 1;
            settings.command_capacity = 1;
            let client = fixture.start(connector, settings).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let mut service = client.service::<Full<Bytes>>(&route).unwrap();
            service.ready().await.unwrap();
            service.ready().await.unwrap();
            let mut clone = service.clone();
            {
                let pending = futures::future::poll_fn(|context| clone.poll_ready(context));
                tokio::pin!(pending);
                assert!(futures::poll!(&mut pending).is_pending());
                drop(service);
                pending.await.unwrap();
            }
            let future = clone.call(
                Request::builder()
                    .uri("/")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            );
            let exchange = future.exchange();
            assert!(matches!(
                client
                    .try_request(
                        &route,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap()
                    )
                    .err()
                    .unwrap()
                    .error,
                ClientError::Capacity
            ));
            assert_eq!(body_bytes(future.await.unwrap().into_body()).await, "ok");
            exchange.wait().await.unwrap();
            clone.ready().await.unwrap();
            drop(clone);
            assert_eq!(client.shared.admission.available_permits(), 1);
        })
    });
}

#[test]
fn native_h1_reuses_and_h2_cancel_preserves_sibling() {
    h2_peer_fixture_contract(H2DrainMode::Healthy);
}

#[test]
fn h2_idle_peer_close_preserves_no_eof_contract() {
    h2_peer_fixture_contract(H2DrainMode::Idle);
}

fn h2_peer_fixture_contract(mode: H2DrainMode) {
    case(move |fixture| {
        Box::pin(async move {
            fixture.enable_graceful_reuse_peers();
            let handler: Handler = Arc::new(|_| Box::pin(async { Response::new(full("ok")) }));
            let connector = fixture.connector(CodecProtocol::Http11, handler);
            let calls = Arc::clone(&connector.calls);
            let client = fixture.start(connector, options()).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            for _request in 0..2 {
                let response = client
                    .try_request(
                        &bound,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .unwrap()
                    .await
                    .unwrap();
                let exchange = response.body().exchange();
                assert_eq!(body_bytes(response.into_body()).await, "ok");
                exchange.wait().await.unwrap();
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let seen = Gate::new();
            let release = Gate::new();
            let body_held = Gate::new();
            let body_release = Gate::new();
            let _release_cancelled = release.release_on_drop();
            let _release_body = body_release.release_on_drop();
            let handler_seen = seen.clone();
            let handler_release = release.clone();
            let handler_body_held = body_held.clone();
            let handler_body_release = body_release.clone();
            let handler: Handler = Arc::new(move |request| {
                let seen = handler_seen.clone();
                let release = handler_release.clone();
                let body_held = handler_body_held.clone();
                let body_release = handler_body_release.clone();
                Box::pin(async move {
                    if request.uri().path() == "/cancel" {
                        seen.open();
                        release.wait().await;
                    } else if mode == H2DrainMode::Healthy {
                        assert_eq!(request.uri().path(), "/ok");
                        return Response::builder()
                            .header("content-length", "7")
                            .body(held_sibling_body(body_held, body_release))
                            .unwrap();
                    }
                    Response::new(full("sibling"))
                })
            });
            let mut settings = options();
            let (io, drain) = fixture.enable_h2_drain(mode);
            let _release_write = (mode == H2DrainMode::Idle).then(|| drain.release_on_drop());
            settings.test_io = Some(io);
            let connector = fixture.connector(CodecProtocol::Http2, handler);
            let calls = Arc::clone(&connector.calls);
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http2)).unwrap();
            let doomed = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/cancel")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            seen.wait().await;
            let doomed_exchange = doomed.exchange();
            drop(doomed);
            let response = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/ok")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            let exchange = response.body().exchange();
            let mut body = response.into_body();
            if mode == H2DrainMode::Healthy {
                tokio::time::timeout(Duration::from_secs(3), async {
                    let prefix = body.frame().await.unwrap().unwrap().into_data().unwrap();
                    assert_eq!(prefix, "sib");
                    body_held.wait().await;
                    assert!(!body_release.is_open());
                })
                .await
                .expect("actual sibling prefix and producer-held suffix");
            } else {
                assert_eq!(body_bytes(body).await, "sibling");
                exchange.wait().await.unwrap();
                release.open();
                let completion = doomed_exchange.wait().await.unwrap_err();
                assert!(!completion.cleanup_unconfirmed());
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                fixture.join_idle_peer_close(&drain).await;
                return;
            }
            release.open();
            let completion = doomed_exchange.wait().await.unwrap_err();
            assert!(!completion.cleanup_unconfirmed());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            futures::join!(fixture.gracefully_join_reuse_peers(), async {
                drain.healthy().await;
                assert!(!body_release.is_open());
                body_release.open();
                let suffix = body_bytes(body).await;
                assert_eq!(suffix, "ling");
                assert_eq!([b"sib".as_slice(), suffix.as_ref()].concat(), b"sibling");
                exchange.wait().await.unwrap();
            });
        })
    });
}

#[test]
fn retiring_io_and_route_entries_still_consume_capacity() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|_| {
                Box::pin(async {
                    Response::builder()
                        .header("connection", "close")
                        .body(full("done"))
                        .unwrap()
                })
            });
            let mut connector = fixture.connector(CodecProtocol::Http11, handler);
            let shutdown = Gate::new();
            let claimed = Gate::new();
            let dropped = Arc::new(AtomicUsize::new(0));
            probe_connector(
                &mut connector,
                &fixture.worker,
                shutdown.clone(),
                claimed,
                Arc::clone(&dropped),
            );
            let calls = Arc::clone(&connector.calls);
            let mut settings = options();
            settings.maximum_physical_connections = 1;
            settings.maximum_routes = 1;
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let response = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            let exchange = response.body().exchange();
            body_bytes(response.into_body()).await;
            exchange.wait().await.unwrap();
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            assert_eq!(client.shared.physical.available_permits(), 0);
            assert!(matches!(
                client.route(RouteSpec::direct(
                    "http://other.test".parse().unwrap(),
                    ProtocolPolicy::Http1Only
                )),
                Err(ClientError::Capacity)
            ));
            let next = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            fixture.worker.call(|_| ()).unwrap().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            shutdown.open();
            let response = next.await.unwrap();
            let exchange = response.body().exchange();
            body_bytes(response.into_body()).await;
            exchange.wait().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        })
    });
}
