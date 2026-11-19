/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Retries, the negative cache, deadlines, error kinds, and request validation.

use std::io;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use http::HeaderValue;
use http::Method;
use http::Request;
use roxygen_connect_client::error::ConnectErrorKind;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::tunnel::PendingTunnel;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::proxy::Client;
use crate::proxy::Proxy;
use crate::proxy::ProxyConfig;
use crate::proxy::RawProxy;
use crate::proxy::RawScript;
use crate::proxy::TestConnector;
use crate::proxy::delayed_request;
use crate::proxy::echo;
use crate::proxy::eventually;
use crate::proxy::request;
use crate::proxy::run;
use crate::proxy::run_owned;
use crate::proxy::shutdown;
use crate::proxy::tls_identity;

fn connect_request(uri: &str) -> Request<()> {
    Request::builder()
        .method(Method::CONNECT)
        .uri(uri)
        .body(())
        .expect("request")
}

fn with_header(name: &'static str, value: &'static str) -> Request<()> {
    let mut request = connect_request("target.example:443");
    request
        .headers_mut()
        .insert(name, HeaderValue::from_static(value));
    request
}

#[test]
fn invalid_requests_fail_before_admission_without_dialing() {
    let options = ConnectOptions {
        max_tunnels: 1,
        max_request_bytes: 96,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let mut invalid: Vec<(&str, Request<()>)> = vec![
            (
                "GET",
                Request::builder()
                    .method(Method::GET)
                    .uri("target.example:443")
                    .body(())
                    .expect("request"),
            ),
            ("scheme", connect_request("https://target.example:443")),
            ("origin form", connect_request("/path")),
            ("missing port", connect_request("target.example")),
            ("port zero", connect_request("target.example:0")),
            ("userinfo", connect_request("user@target.example:443")),
            ("host mismatch", with_header("host", "other.example:443")),
            ("leading whitespace", with_header("x-padded", " value")),
            ("trailing whitespace", with_header("x-padded", "value ")),
        ];
        let mut oversize = connect_request("target.example:443");
        oversize.headers_mut().insert(
            "x-large",
            HeaderValue::from_str(&"x".repeat(80)).expect("value"),
        );
        invalid.push(("oversize", oversize));
        for name in [
            "connection",
            "keep-alive",
            "proxy-connection",
            "transfer-encoding",
            "upgrade",
            "te",
            "content-length",
            "trailer",
        ] {
            invalid.push((name, with_header(name, "1")));
        }
        invalid.push(("bracketed name", connect_request("[target.example]:443")));
        let mut extensions = connect_request("target.example:443");
        extensions.extensions_mut().insert(7_u32);
        invalid.push(("extensions", extensions));
        let mut duplicate = connect_request("target.example:443");
        duplicate
            .headers_mut()
            .append("x-dup", HeaderValue::from_static("one"));
        duplicate
            .headers_mut()
            .append("x-dup", HeaderValue::from_static("two"));
        invalid.push(("duplicate", duplicate));
        let mut sensitive = connect_request("target.example:443");
        let mut value = HeaderValue::from_static("secret");
        value.set_sensitive(true);
        sensitive.headers_mut().insert("x-private", value);
        invalid.push(("sensitive non-credential", sensitive));
        let count = invalid.len() as u64;
        for (name, request) in invalid {
            let error = client
                .connect("vm", request, connector.clone())
                .await
                .expect_err(name);
            assert_eq!(error.kind(), ConnectErrorKind::InvalidRequest, "{name}");
            assert!(!error.committed(), "{name}");
        }
        assert_eq!(connector.attempts(), 0, "invalid requests never dial");
        assert_eq!(proxy.connections(), 0);
        let stats = client.stats();
        assert_eq!(stats.invalid_request, count);
        assert_eq!(
            stats.admitted, 0,
            "invalid requests take no admission permit"
        );

        let mut valid = connect_request("[::1]:443");
        valid
            .headers_mut()
            .insert("host", HeaderValue::from_static("[::1]:443"));
        valid
            .headers_mut()
            .insert("x-custom", HeaderValue::from_static("Mixed Value"));
        valid
            .headers_mut()
            .insert("x-test-action", HeaderValue::from_static("hold-open"));
        let held = client
            .connect("vm", valid, connector.clone())
            .await
            .expect("bracketed IPv6 and a matching Host are accepted")
            .into_tunnel();
        let heads = proxy.observations.heads();
        assert_eq!(
            heads[0]["x-custom"], "Mixed Value",
            "accepted headers reach the CONNECT unchanged"
        );
        assert!(
            !heads[0].contains_key("host"),
            "HTTP/2 carries the authority, not Host"
        );
        assert_eq!(proxy.observations.authorities(), ["[::1]:443"]);
        let error = client
            .connect("vm", with_header("te", "trailers"), connector.clone())
            .await
            .expect_err("validation precedes admission");
        assert_eq!(
            error.kind(),
            ConnectErrorKind::InvalidRequest,
            "not Overloaded"
        );
        drop(held);
        proxy
    });
}

#[test]
fn refused_stream_is_retried_once() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        proxy.refuse_next(1);
        let mut tunnel = proxy.connect_with(&client, "vm", "echo").await;
        assert_eq!(tunnel.info().retries, 1);
        assert!(
            tunnel.info().reused_session,
            "the retry stays on the session"
        );
        assert_eq!(proxy.observations.requests(), 2);
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().retries, 1);
        echo(&mut tunnel, b"retried").await;
        drop(tunnel);
        proxy
    });
}

#[test]
fn connect_above_goaway_last_stream_is_retried_once() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = RawProxy::start(RawScript::GoawayBelowSecondStream).await;
        let connector = proxy.connector();
        let mut first = client
            .connect("vm", request("first"), connector.clone())
            .await
            .expect("first stream")
            .into_tunnel();
        let mut second = client
            .connect("vm", request("second"), connector)
            .await
            .expect("the refused CONNECT is retried")
            .into_tunnel();
        assert_eq!(second.info().retries, 1);
        assert!(
            !second.info().reused_session,
            "the retry dialed a new session"
        );
        assert_eq!(proxy.observations.connections(), 2);
        echo(&mut second, b"second").await;
        echo(&mut first, b"the stream below last-stream-id keeps working").await;
        drop((first, second));
        proxy.shutdown().await;
    });

    // A GOAWAY's error code fails the session, but its boundary still proves the CONNECT above
    // it unprocessed.
    run(ConnectOptions::default(), |client| async move {
        let proxy = RawProxy::start(RawScript::ErrorGoawayBelowSecondStream).await;
        let first = client
            .connect("vm", request("first"), proxy.connector())
            .await
            .expect("first stream")
            .into_tunnel();
        let mut second = client
            .connect("vm", request("second"), proxy.connector())
            .await
            .expect("the refused CONNECT is retried")
            .into_tunnel();
        assert_eq!(second.info().retries, 1);
        assert!(!second.info().reused_session);
        assert_eq!(proxy.observations.connections(), 2);
        echo(&mut second, b"retried above an error GOAWAY").await;
        drop((first, second));
        proxy.shutdown().await;
    });
    let options = ConnectOptions {
        max_unprocessed_retries: 0,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = RawProxy::start(RawScript::ErrorGoawayBelowSecondStream).await;
        let first = client
            .connect("vm", request("first"), proxy.connector())
            .await
            .expect("first stream")
            .into_tunnel();
        let error = client
            .connect("vm", request("second"), proxy.connector())
            .await
            .expect_err("no retry remains");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Refused, false),
            "a CONNECT above the GOAWAY's last-stream-id was provably not processed"
        );
        assert_eq!(proxy.observations.connections(), 1);
        drop(first);
        proxy.shutdown().await;
    });
}

#[test]
fn goaway_refusal_covering_the_stream_is_never_retried() {
    for retries in [0, 1] {
        let options = ConnectOptions {
            max_unprocessed_retries: retries,
            ..Default::default()
        };
        run(options, move |client| async move {
            let raw = RawProxy::start(RawScript::RefusedGoawayCoveringSecondStream).await;
            let earlier = client
                .connect("raw", request("first"), raw.connector())
                .await
                .expect("first stream")
                .into_tunnel();
            let error = client
                .connect("raw", request("second"), raw.connector())
                .await
                .expect_err("the GOAWAY covers the CONNECT, so the proxy may have processed it");
            assert_eq!(
                (error.kind(), error.committed()),
                (ConnectErrorKind::Failed, true),
                "max_unprocessed_retries {retries}"
            );
            assert_eq!(
                raw.observations.requests(),
                2,
                "one request for the second connect"
            );
            assert_eq!(raw.observations.connections(), 1);
            assert_eq!(client.stats().retries, 0);
            drop(earlier);
            raw.shutdown().await;
        });
    }
}

#[test]
fn draining_session_failure_is_retried_once() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let mut first = proxy.connect_with(&client, "vm", "hold-open").await;
        proxy.drain_and_refuse_next(1);
        let mut second = proxy.connect_with(&client, "vm", "echo").await;
        assert_eq!(second.info().retries, 1);
        assert_eq!(
            proxy.connections(),
            2,
            "the retry leaves the draining session"
        );
        echo(&mut second, b"second").await;
        echo(&mut first, b"first").await;
        drop((first, second));
        proxy
    });
}

#[test]
fn reused_session_closed_before_the_head_is_retried_once() {
    // The proxy ends the connection with a FIN, then with a TCP reset.
    for reset_on_close in [false, true] {
        let config = ProxyConfig {
            reset_on_close,
            ..Default::default()
        };
        let retrying = config.clone();
        run(ConnectOptions::default(), |client| async move {
            let proxy = Proxy::with_config(retrying).await;
            let mut earlier = proxy.connect_with(&client, "vm", "hold-open").await;
            proxy.close_on_next(1);
            let mut retried = proxy.connect_with(&client, "vm", "echo").await;
            assert_eq!(retried.info().retries, 1, "reset_on_close {reset_on_close}");
            assert!(!retried.info().reused_session);
            assert_eq!(proxy.connections(), 2);
            assert_eq!(client.stats().retries, 1);
            echo(&mut retried, b"retried on a new session").await;
            let error = earlier
                .read(&mut [0; 1])
                .await
                .expect_err("the closed session fails its tunnel");
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            drop((earlier, retried));
            proxy
        });
        let options = ConnectOptions {
            max_unprocessed_retries: 0,
            ..Default::default()
        };
        run(options, |client| async move {
            let proxy = Proxy::with_config(config).await;
            let earlier = proxy.connect_with(&client, "vm", "hold-open").await;
            proxy.close_on_next(1);
            let error = client
                .connect("vm", request("echo"), proxy.connector())
                .await
                .expect_err("no retry remains");
            assert_eq!(
                error.kind(),
                ConnectErrorKind::Failed,
                "reset_on_close {reset_on_close}"
            );
            assert!(
                error.committed(),
                "a transport failure after commit is no fallback case"
            );
            drop(earlier);
            proxy
        });
    }

    // A sibling stream's peer reset just before the close does not make the failure final.
    run(ConnectOptions::default(), |client| async move {
        let raw = RawProxy::start(RawScript::ResetSiblingThenCloseSecondStream).await;
        let mut sibling = client
            .connect("raw", request("first"), raw.connector())
            .await
            .expect("first stream")
            .into_tunnel();
        let mut retried = client
            .connect("raw", request("second"), raw.connector())
            .await
            .expect("retried on a new session")
            .into_tunnel();
        assert_eq!(retried.info().retries, 1);
        assert!(!retried.info().reused_session);
        assert_eq!(raw.observations.requests(), 3);
        assert_eq!(raw.observations.connections(), 2);
        assert_eq!(client.stats().retries, 1);
        echo(&mut retried, b"retried after a sibling's reset").await;
        let error = sibling
            .read(&mut [0; 1])
            .await
            .expect_err("the proxy reset the sibling");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        drop((sibling, retried));
        raw.shutdown().await;
    });
}

#[test]
fn response_progress_before_a_reused_session_closes_is_never_retried() {
    run(ConnectOptions::default(), |client| async move {
        let raw = RawProxy::start(RawScript::InformationalThenCloseSecondStream).await;
        let mut earlier = client
            .connect("raw", request("first"), raw.connector())
            .await
            .expect("first stream")
            .into_tunnel();
        let error = client
            .connect("raw", request("second"), raw.connector())
            .await
            .expect_err("a 103 shows the proxy processed the CONNECT");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Failed, true)
        );
        assert_eq!(raw.observations.requests(), 2, "no retry was sent");
        assert_eq!(raw.observations.connections(), 1);
        assert_eq!(client.stats().retries, 0);
        earlier
            .read(&mut [0; 1])
            .await
            .expect_err("the closed session fails its tunnel");
        drop(earlier);
        raw.shutdown().await;
    });

    // A complete but undecodable head fails the session on the peer's protocol error.
    run(ConnectOptions::default(), |client| async move {
        let raw = RawProxy::start(RawScript::MalformedHeadOnSecondStream).await;
        let mut earlier = client
            .connect("raw", request("first"), raw.connector())
            .await
            .expect("first stream")
            .into_tunnel();
        let error = client
            .connect("raw", request("second"), raw.connector())
            .await
            .expect_err("a malformed head is a response, not an unprocessed CONNECT");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Failed, true)
        );
        assert_eq!(raw.observations.requests(), 2, "no retry was sent");
        assert_eq!(raw.observations.connections(), 1);
        assert_eq!(client.stats().retries, 0);
        earlier
            .read(&mut [0; 1])
            .await
            .expect_err("the failed session fails its tunnel");
        drop(earlier);
        raw.shutdown().await;
    });
}

#[test]
fn rejected_cancelled_expired_and_dial_failures_are_not_retried() {
    let options = ConnectOptions {
        response_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let rejected = client
            .connect("vm", request("reject"), connector.clone())
            .await
            .expect_err("403");
        assert_eq!(rejected.kind(), ConnectErrorKind::Rejected);
        assert_eq!(proxy.observations.requests(), 1);

        let caller = {
            let client = client.clone();
            let connector = connector.clone();
            tokio::spawn(async move {
                client
                    .connect("vm", request("stall"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            })
        };
        proxy
            .observations
            .wait("the stalled CONNECT", |observed| observed.requests() == 2)
            .await;
        caller.abort();
        proxy
            .observations
            .wait("the cancelled stream's reset", |observed| {
                observed.resets() == 1
            })
            .await;

        let expired = client
            .connect("vm", request("stall"), connector.clone())
            .await
            .expect_err("response deadline");
        assert_eq!(expired.kind(), ConnectErrorKind::ResponseTimeout);

        let fresh = Proxy::start().await;
        fresh.close_on_next(1);
        let closed = client
            .connect("fresh", request("echo"), fresh.connector())
            .await
            .expect_err("a fresh session's failure is not retried");
        assert_eq!(closed.kind(), ConnectErrorKind::Failed);
        assert_eq!(fresh.observations.requests(), 1);

        let raw = RawProxy::start(RawScript::ResetThenCloseSecondStream).await;
        let mut earlier = client
            .connect("raw", request("first"), raw.connector())
            .await
            .expect("first stream")
            .into_tunnel();
        let reset = client
            .connect("raw", request("second"), raw.connector())
            .await
            .expect_err("a peer reset stays final when the session then closes");
        assert_eq!(
            (reset.kind(), reset.committed()),
            (ConnectErrorKind::Failed, true)
        );
        assert_eq!(raw.observations.requests(), 2);
        assert_eq!(raw.observations.connections(), 1);
        earlier
            .read(&mut [0; 1])
            .await
            .expect_err("the closed session fails its tunnel");
        drop(earlier);
        raw.shutdown().await;

        let refused = Arc::new(TestConnector::refused());
        let dial = client
            .connect("refused", request("echo"), refused.clone())
            .await
            .expect_err("TCP refused");
        assert_eq!(dial.kind(), ConnectErrorKind::Connector);
        assert_eq!(
            refused.attempts(),
            1,
            "one connection attempt per dial, no retry"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(proxy.observations.requests(), 3, "no failure was retried");
        assert_eq!(client.stats().retries, 0);
        vec![proxy, fresh]
    });
}

#[test]
fn retries_stop_at_max_unprocessed_retries() {
    for (retries, requests) in [(2, 3), (0, 1)] {
        let options = ConnectOptions {
            max_unprocessed_retries: retries,
            ..Default::default()
        };
        run(options, move |client| async move {
            let proxy = Proxy::start().await;
            proxy.refuse_next(10);
            let error = client
                .connect("vm", request("echo"), proxy.connector())
                .await
                .expect_err("every attempt is refused");
            assert_eq!(error.kind(), ConnectErrorKind::Refused);
            assert!(!error.committed(), "a refusal is provably unprocessed");
            assert_eq!(proxy.observations.requests(), requests);
            assert_eq!(client.stats().retries, u64::from(retries));
            proxy
        });
    }
}

#[test]
fn retries_share_the_acquire_window_and_restart_the_response_deadline() {
    let short_window = ConnectOptions {
        acquire_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    run(short_window, |client| async move {
        let proxy = Proxy::start().await;
        proxy.refuse_next(1);
        let error = client
            .connect(
                "vm",
                delayed_request("echo", Duration::from_millis(300)),
                proxy.connector(),
            )
            .await
            .expect_err("refused after the acquire window closed");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Refused, false)
        );
        assert_eq!(proxy.observations.requests(), 1, "nothing was retried");
        assert_eq!(client.stats().retries, 0);
        proxy
    });

    // A response deadline shared across attempts would expire before the retry's head.
    let per_attempt = ConnectOptions {
        response_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    run(per_attempt, |client| async move {
        let proxy = Proxy::start().await;
        proxy.refuse_next(1);
        let mut tunnel = client
            .connect(
                "vm",
                delayed_request("echo", Duration::from_millis(600)),
                proxy.connector(),
            )
            .await
            .expect("each attempt has its own response deadline")
            .into_tunnel();
        assert_eq!(tunnel.info().retries, 1);
        assert_eq!(proxy.observations.requests(), 2);
        echo(&mut tunnel, b"retried").await;
        drop(tunnel);
        proxy
    });

    let full_route = ConnectOptions {
        max_connections: 1,
        max_retiring: Some(0),
        acquire_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run(full_route, |client| async move {
        let proxy = Proxy::start().await;
        let mut held = proxy.connect_with(&client, "vm", "hold-open").await;
        proxy.drain_and_refuse_next(1);
        let error = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("the retry finds no capacity within the acquire window");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::AcquireTimeout, false)
        );
        let stats = client.stats();
        assert_eq!((stats.retries, stats.acquire_timeouts), (1, 1));
        assert_eq!(proxy.observations.requests(), 2);
        assert_eq!(
            proxy.connections(),
            1,
            "the draining session keeps the only slot"
        );
        echo(&mut held, b"the draining session's tunnel keeps working").await;
        drop(held);
        proxy
    });
}

#[test]
fn h2_not_negotiated_is_cached_for_the_route_ttl() {
    let options = ConnectOptions {
        h2_unsupported_ttl: Duration::from_millis(400),
        max_streams: 1,
        acquire_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    run(options.clone(), |client| async move {
        let (server, client_tls) = tls_identity(&[b"http/1.1"]);
        let http1 = Proxy::with_config(ProxyConfig {
            tls: Some(server),
            ..Default::default()
        })
        .await;
        let connector = Arc::new(TestConnector::plain(http1.address).tls(client_tls));
        let first = client
            .connect("http1", request("echo"), connector.clone())
            .await
            .expect_err("no h2");
        assert_eq!(first.kind(), ConnectErrorKind::H2NotNegotiated);
        assert!(!first.committed());
        let cached = client
            .connect("http1", request("echo"), connector.clone())
            .await
            .expect_err("cached");
        assert_eq!(cached.kind(), ConnectErrorKind::H2NotNegotiated);
        assert!(!cached.committed());
        assert_eq!(
            connector.attempts(),
            1,
            "a cached key fails without dialing"
        );
        assert_eq!(client.stats().negative_cache_hits, 1);
        let other = client
            .connect("other", request("echo"), connector.clone())
            .await
            .expect_err("other key dials");
        assert_eq!(other.kind(), ConnectErrorKind::H2NotNegotiated);
        assert_eq!(connector.attempts(), 2, "the cache is per route key");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let expired = client
            .connect("http1", request("echo"), connector.clone())
            .await
            .expect_err("still no h2");
        assert_eq!(expired.kind(), ConnectErrorKind::H2NotNegotiated);
        assert_eq!(connector.attempts(), 3, "after the TTL the key dials again");
        http1
    });

    // A TTL well beyond the overflow waiter's acquire deadline, so the entry is still cached.
    let mixed = ConnectOptions {
        h2_unsupported_ttl: Duration::from_secs(2),
        acquire_timeout: Duration::from_millis(300),
        ..options
    };
    run(mixed, |client| async move {
        let h2 = Proxy::start().await;
        let switching = Arc::new(TestConnector::plain(h2.address).h2_dials(1));
        let mut held = client
            .connect("mixed", request("hold-open"), switching.clone())
            .await
            .expect("the first dial negotiates h2")
            .into_tunnel();
        let waiting = client
            .connect("mixed", request("echo"), switching.clone())
            .await
            .expect_err("the overflow dial fails without h2");
        assert_eq!(
            waiting.kind(),
            ConnectErrorKind::AcquireTimeout,
            "the route's live session keeps the waiter queued until its deadline",
        );
        let dials = switching.attempts();
        let hit = client
            .connect("mixed", request("echo"), switching.clone())
            .await
            .expect_err("cached despite the open session");
        assert_eq!(hit.kind(), ConnectErrorKind::H2NotNegotiated);
        assert_eq!(switching.attempts(), dials, "the cached key does not dial");
        echo(&mut held, b"established tunnels are not disturbed").await;
        drop(held);
        h2
    });
}

#[test]
fn queued_connects_share_a_failed_h2_probe() {
    run(ConnectOptions::default(), |client| async move {
        let (server, client_tls) = tls_identity(&[b"http/1.1"]);
        let http1 = Proxy::with_config(ProxyConfig {
            tls: Some(server),
            ..Default::default()
        })
        .await;
        let gate = Arc::new(Semaphore::new(0));
        let connector = Arc::new(
            TestConnector::plain(http1.address)
                .tls(client_tls)
                .gated(Arc::clone(&gate)),
        );
        let mut queued = JoinSet::new();
        for _ in 0..5 {
            let client = client.clone();
            let connector = connector.clone();
            queued.spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        eventually(Duration::from_secs(5), "five waiters", || {
            client.stats().waiters == 5
        })
        .await;
        gate.add_permits(1);
        while let Some(joined) = queued.join_next().await {
            let error = joined.expect("caller joins").expect_err("no h2");
            assert_eq!(error.kind(), ConnectErrorKind::H2NotNegotiated);
        }
        let stats = client.stats();
        assert_eq!(stats.dial_attempts, 1);
        assert_eq!(stats.dial_failures, 1);
        assert_eq!(stats.h2_not_negotiated, 1);
        assert_eq!(
            stats.negative_cache_hits, 0,
            "sharing a dial's result is not a cache hit"
        );
        http1
    });
}

#[test]
fn certificate_and_transport_failures_are_not_cached() {
    run(ConnectOptions::default(), |client| async move {
        let (server, _) = tls_identity(&[b"h2"]);
        let (_, untrusting) = tls_identity(&[b"h2"]);
        let proxy = Proxy::with_config(ProxyConfig {
            tls: Some(server),
            ..Default::default()
        })
        .await;
        let connector = Arc::new(TestConnector::plain(proxy.address).tls(untrusting));
        for attempt in 1..=2 {
            let error = client
                .connect("tls", request("echo"), connector.clone())
                .await
                .expect_err("untrusted certificate");
            assert_eq!(error.kind(), ConnectErrorKind::Connector);
            assert!(!error.committed());
            assert_eq!(
                connector.attempts(),
                attempt,
                "certificate failures are never cached"
            );
        }
        let refused = Arc::new(TestConnector::refused());
        for attempt in 1..=2 {
            let error = client
                .connect("tcp", request("echo"), refused.clone())
                .await
                .expect_err("refused");
            assert_eq!(error.kind(), ConnectErrorKind::Connector);
            assert_eq!(refused.attempts(), attempt, "TCP failures are never cached");
        }
        assert_eq!(client.stats().negative_cache_hits, 0);
        proxy
    });
}

#[test]
fn error_kinds_and_commitment_follow_the_failure_phase() {
    let admission = ConnectOptions {
        max_tunnels: 1,
        max_routes: 1,
        ..Default::default()
    };
    run(admission, |client| async move {
        let proxy = Proxy::start().await;
        let held = proxy.connect_with(&client, "vm", "hold-open").await;
        let overloaded = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("overloaded");
        assert_eq!(
            (overloaded.kind(), overloaded.committed()),
            (ConnectErrorKind::Overloaded, false)
        );
        drop(held);
        eventually(Duration::from_secs(5), "the released permit", || {
            client.stats().active_tunnels == 0
        })
        .await;
        let capacity = client
            .connect("other", request("echo"), proxy.connector())
            .await
            .expect_err("route capacity");
        assert_eq!(
            (capacity.kind(), capacity.committed()),
            (ConnectErrorKind::RouteCapacity, false)
        );
        proxy
    });

    let phases = ConnectOptions {
        acquire_timeout: Duration::from_millis(200),
        response_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    run(phases, |client| async move {
        let proxy = Proxy::start().await;
        let invalid = client
            .connect("vm", connect_request("target.example"), proxy.connector())
            .await
            .expect_err("invalid");
        let not_h2 = client
            .connect(
                "not-h2",
                request("echo"),
                Arc::new(TestConnector::plain(proxy.address).h2_dials(0)),
            )
            .await
            .expect_err("no h2");
        let connector = client
            .connect(
                "refused",
                request("echo"),
                Arc::new(TestConnector::refused()),
            )
            .await
            .expect_err("refused");
        let acquire = client
            .connect(
                "stalled",
                request("echo"),
                Arc::new(TestConnector::plain(proxy.address).stalling(1)),
            )
            .await
            .expect_err("stalled dial");
        let response = client
            .connect("vm", request("stall"), proxy.connector())
            .await
            .expect_err("stalled head");
        let rejected = client
            .connect("vm", request("reject"), proxy.connector())
            .await
            .expect_err("403");
        proxy.refuse_next(2);
        let refused = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("refused twice");
        let failed = client
            .connect("vm", request("cancel"), proxy.connector())
            .await
            .expect_err("reset before the head");
        let table = [
            (invalid, ConnectErrorKind::InvalidRequest, false),
            (not_h2, ConnectErrorKind::H2NotNegotiated, false),
            (connector, ConnectErrorKind::Connector, false),
            (acquire, ConnectErrorKind::AcquireTimeout, false),
            (response, ConnectErrorKind::ResponseTimeout, true),
            (rejected, ConnectErrorKind::Rejected, true),
            (refused, ConnectErrorKind::Refused, false),
            (failed, ConnectErrorKind::Failed, true),
        ];
        for (error, kind, committed) in table {
            assert_eq!(error.kind(), kind, "{error:?}");
            assert_eq!(error.committed(), committed, "{error:?}");
        }
        assert_eq!(
            client.stats().retries,
            1,
            "only the first refusal is retried"
        );
        proxy
    });

    let closing = ConnectOptions {
        shutdown_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    run_owned(closing, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let awaiting_head = {
            let client = client.clone();
            let connector = proxy.connector();
            tokio::spawn(async move {
                client
                    .connect("vm", request("stall"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            })
        };
        proxy
            .observations
            .wait("the committed CONNECT", |observed| observed.requests() == 1)
            .await;
        shutdown(runtime).await.expect("shutdown");
        let after_commit = awaiting_head
            .await
            .expect("caller joins")
            .expect_err("shut down after commit");
        let closed = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("closed runtime");
        for (error, committed) in [(after_commit, true), (closed, false)] {
            assert_eq!(
                (error.kind(), error.committed()),
                (ConnectErrorKind::Closed, committed),
                "{error:?}"
            );
        }
        proxy
    });
}

#[test]
fn stalled_dial_times_out_and_next_connect_recovers() {
    let acquire_timeout = Duration::from_millis(600);
    let options = ConnectOptions {
        acquire_timeout,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = Arc::new(TestConnector::plain(proxy.address).stalling(1));
        let early = {
            let client = client.clone();
            let connector = connector.clone();
            tokio::spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            })
        };
        tokio::time::sleep(acquire_timeout / 2).await;
        let started = Instant::now();
        let late = client
            .connect("vm", request("echo"), connector.clone())
            .await
            .expect_err("the stalled dial fails its late joiner too");
        let waited = started.elapsed();
        assert_eq!(late.kind(), ConnectErrorKind::AcquireTimeout);
        assert!(!late.committed());
        assert!(
            waited < acquire_timeout - Duration::from_millis(100),
            "the factory's dial timeout, not the joiner's own window, ended it after {waited:?}",
        );
        let early = early
            .await
            .expect("caller joins")
            .expect_err("stalled dial");
        assert_eq!(early.kind(), ConnectErrorKind::AcquireTimeout);
        connector.dropped.wait_for(1).await;
        assert_eq!(
            connector.dropped.count(),
            1,
            "the timed-out dial is dropped"
        );
        let mut recovered = client
            .connect("vm", request("echo"), connector.clone())
            .await
            .expect("second dial")
            .into_tunnel();
        echo(&mut recovered, b"dial recovered").await;
        assert_eq!(connector.attempts(), 2);
        drop(recovered);
        proxy
    });
}

#[test]
fn stalled_connect_response_times_out_and_resets_the_stream() {
    let options = ConnectOptions {
        response_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let started = Instant::now();
        let error = client
            .connect("vm", request("stall"), proxy.connector())
            .await
            .expect_err("CONNECT head wait is bounded");
        assert_eq!(error.kind(), ConnectErrorKind::ResponseTimeout);
        assert!(error.committed());
        assert!(started.elapsed() >= Duration::from_millis(200));
        proxy
            .observations
            .wait("one RST_STREAM CANCEL", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let mut recovered = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut recovered, b"same session after timeout").await;
        assert!(recovered.info().reused_session);
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().response_timeouts, 1);
        drop(recovered);
        proxy
    });
}

/// Opens three tunnels on one connection, ends it with `fail`, and expects every tunnel's
/// pending and later reads to fail with `ConnectionReset`.
async fn every_tunnel_fails_with_a_reset(client: &Client, proxy: &Proxy, fail: fn(&Proxy)) {
    let mut readers = JoinSet::new();
    for _ in 0..3 {
        let mut tunnel = proxy.connect_with(client, "vm", "hold-open").await;
        readers.spawn(async move {
            let first = tunnel.read(&mut [0; 16]).await;
            let later = tunnel.read(&mut [0; 16]).await;
            (first, later)
        });
    }
    fail(proxy);
    while let Some(joined) = readers.join_next().await {
        let (first, later) = joined.expect("reader joins");
        let first = first.expect_err("a reset is never a clean EOF");
        assert_eq!(first.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(
            later.expect_err("later reads fail").kind(),
            io::ErrorKind::ConnectionReset
        );
    }
}

#[test]
fn proxy_connection_reset_fails_every_tunnel_and_the_route_recovers() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            reset_on_close: true,
            ..Default::default()
        })
        .await;
        every_tunnel_fails_with_a_reset(&client, &proxy, Proxy::close_connections).await;
        assert_eq!(client.stats().tunnels_reset_by_peer, 3);
        eventually(Duration::from_secs(5), "the reset session to close", || {
            client.stats().open_sessions == 0
        })
        .await;
        // A GOAWAY with an error code ends the next connection's streams the same way.
        every_tunnel_fails_with_a_reset(&client, &proxy, Proxy::abort_connections).await;
        assert_eq!(client.stats().tunnels_reset_by_peer, 6);
        eventually(
            Duration::from_secs(5),
            "the aborted session to close",
            || client.stats().open_sessions == 0,
        )
        .await;
        let mut recovered = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut recovered, b"new connection").await;
        assert_eq!(proxy.connections(), 3);
        assert_eq!(client.stats().open_sessions, 1);
        drop(recovered);
        proxy
    });
}
