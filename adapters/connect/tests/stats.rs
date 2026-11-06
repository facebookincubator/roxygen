//! Runtime stats snapshots.

use std::sync::Arc;
use std::time::Duration;

use http::HeaderValue;
use roxygen_connect_client::error::ConnectErrorKind;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::tunnel::PendingTunnel;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use crate::proxy::Proxy;
use crate::proxy::TestConnector;
use crate::proxy::delayed_request;
use crate::proxy::echo;
use crate::proxy::eventually;
use crate::proxy::request;
use crate::proxy::run;
use crate::proxy::run_owned;
use crate::proxy::shutdown;

#[test]
fn counters_follow_each_connect_outcome() {
    let options = ConnectOptions {
        max_tunnels: 2,
        max_routes: 3,
        acquire_timeout: Duration::from_millis(300),
        response_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        // 1: invalid, never admitted.
        let mut invalid = request("echo");
        invalid
            .headers_mut()
            .insert("connection", HeaderValue::from_static("close"));
        client
            .connect("vm", invalid, proxy.connector())
            .await
            .expect_err("invalid");
        // 2: established on a new session, then closed after both directions finished.
        let mut clean = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut clean, b"clean").await;
        clean.shutdown().await.expect("upload EOF");
        clean
            .read_to_end(&mut Vec::new())
            .await
            .expect("download EOF");
        drop(clean);
        // 3: rejected on the reused session.
        client
            .connect("vm", request("reject"), proxy.connector())
            .await
            .expect_err("403");
        // 4: refused once, retried, and established on the reused session.
        proxy.refuse_next(1);
        let retried = proxy.connect_with(&client, "vm", "hold-open").await;
        // 5: overloaded while two tunnels hold both permits.
        let held = proxy.connect_with(&client, "vm", "hold-open").await;
        client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("overloaded");
        // Dropping both before their streams finished resets them locally.
        drop((retried, held));
        eventually(Duration::from_secs(5), "released permits", || {
            client.stats().active_tunnels == 0
        })
        .await;
        // 6: response deadline.
        client
            .connect("vm", request("stall"), proxy.connector())
            .await
            .expect_err("stall");
        // 7: peer reset of an established tunnel.
        let mut reset = proxy.connect_with(&client, "vm", "reset").await;
        reset.write_all(b"trigger").await.expect("write");
        reset.read(&mut [0; 1]).await.expect_err("reset");
        drop(reset);
        // 8: a stalled dial on a second route, then 9: h2 not negotiated on a third route and
        // 10: the cached failure on that route.
        let stalled = Arc::new(TestConnector::plain(proxy.address).stalling(1));
        let acquire = client
            .connect("stalled", request("echo"), stalled)
            .await
            .expect_err("acquire deadline");
        assert_eq!(acquire.kind(), ConnectErrorKind::AcquireTimeout);
        let no_h2 = Arc::new(TestConnector::plain(proxy.address).h2_dials(0));
        for _ in 0..2 {
            client
                .connect("no-h2", request("echo"), no_h2.clone())
                .await
                .expect_err("no h2");
        }
        // 11: a fourth route exceeds max_routes.
        client
            .connect("fourth", request("echo"), proxy.connector())
            .await
            .expect_err("route capacity");
        eventually(Duration::from_secs(5), "the stalled dial's timeout", || {
            client.stats().dial_failures == 2
        })
        .await;

        let stats = client.stats();
        assert_eq!(
            stats.connects, 12,
            "eleven steps, the h2 step calling twice; retries are not calls"
        );
        assert_eq!(stats.invalid_request, 1);
        assert_eq!(
            stats.admitted, 10,
            "every call but the invalid one and the overloaded one"
        );
        assert_eq!(stats.overloaded, 1);
        assert_eq!(stats.rejected, 1);
        assert_eq!(stats.response_timeouts, 1);
        assert_eq!(stats.acquire_timeouts, 1);
        assert_eq!(stats.route_capacity, 1);
        assert_eq!(stats.retries, 1);
        assert_eq!(
            stats.tunnels_established, 4,
            "clean, retried, held, and reset"
        );
        assert_eq!(stats.tunnels_reset_by_peer, 1);
        assert_eq!(
            stats.tunnels_aborted, 2,
            "retried and held dropped before finishing"
        );
        assert_eq!(
            stats.dial_attempts, 3,
            "vm, the stalled dial, and one h2 probe"
        );
        assert_eq!(
            stats.dial_failures, 2,
            "the stalled dial's timeout and the h2 probe"
        );
        assert_eq!(stats.h2_not_negotiated, 1);
        assert_eq!(stats.negative_cache_hits, 1);
        assert_eq!(stats.sessions_opened, 1);
        assert_eq!(
            stats.streams_on_reused_session, 3,
            "retried, held, and reset reused the session"
        );
        assert_eq!(stats.ping_timeouts, 0, "no probes are configured");
        assert_eq!(stats.shutdown_errors, 0);
        assert_eq!(
            stats.acquire_latency.count, 7,
            "one sample per reservation: clean, rejected, both refused attempts, held, stall, reset"
        );
        assert_eq!(
            stats.response_latency.count, 5,
            "one sample per head: clean, rejected, retried, held, reset"
        );
        proxy
    });
}

#[test]
fn gauges_track_tunnels_routes_sessions_and_waiters() {
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let first = proxy.connect_with(&client, "vm", "hold-open").await;
        let second = proxy.connect_with(&client, "vm", "hold-open").await;
        let stats = client.stats();
        assert_eq!(
            (
                stats.active_tunnels,
                stats.routes,
                stats.open_sessions,
                stats.waiters
            ),
            (2, 1, 1, 0)
        );
        drop(first);
        eventually(Duration::from_secs(5), "one ended tunnel", || {
            client.stats().active_tunnels == 1
        })
        .await;

        let stalled = Arc::new(TestConnector::plain(proxy.address).stalling(1));
        let mut queued = JoinSet::new();
        for _ in 0..3 {
            let client = client.clone();
            let stalled = stalled.clone();
            queued.spawn(async move {
                client
                    .connect("stalled", request("echo"), stalled)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        eventually(Duration::from_secs(5), "three waiters", || {
            client.stats().waiters == 3
        })
        .await;
        let stats = client.stats();
        assert_eq!(
            (stats.active_tunnels, stats.routes, stats.open_sessions),
            (1, 2, 1)
        );

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
            .wait("the stalled CONNECT", |observed| observed.requests() == 3)
            .await;
        let stats = client.stats();
        assert_eq!(
            (stats.waiters, stats.active_tunnels),
            (3, 1),
            "a connect awaiting its head counts in neither gauge"
        );
        proxy.goaway();
        eventually(Duration::from_secs(5), "the GOAWAY drain", || {
            client.stats().session_drains == 1
        })
        .await;
        assert_eq!(
            client.stats().open_sessions,
            1,
            "a draining session is still open"
        );

        shutdown(runtime).await.expect("shutdown");
        while let Some(joined) = queued.join_next().await {
            joined.expect("caller joins").expect_err("shut down");
        }
        awaiting_head
            .await
            .expect("caller joins")
            .expect_err("shut down");
        drop(second);
        let stats = client.stats();
        assert_eq!(
            (
                stats.active_tunnels,
                stats.routes,
                stats.open_sessions,
                stats.waiters
            ),
            (0, 0, 0, 0),
            "gauges return to zero after shutdown",
        );
        proxy
    });
}

#[test]
fn acquire_and_response_latency_summaries_record_count_sum_and_max() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let delay = Duration::from_millis(150);
        let slow = client
            .connect(
                "vm",
                delayed_request("echo", delay),
                Arc::new(TestConnector::plain(proxy.address).delayed(delay)),
            )
            .await
            .expect("delayed dial and head")
            .into_tunnel();
        let stats = client.stats();
        assert_eq!(stats.response_latency.count, 1);
        assert!(
            stats.response_latency.sum_us >= 150_000,
            "{:?}",
            stats.response_latency
        );
        assert_eq!(stats.response_latency.max_us, stats.response_latency.sum_us);
        assert_eq!(stats.acquire_latency.count, 1);
        assert!(
            stats.acquire_latency.sum_us >= 150_000,
            "acquisition includes the dial: {:?}",
            stats.acquire_latency
        );
        assert_eq!(stats.acquire_latency.max_us, stats.acquire_latency.sum_us);
        assert!(
            slow.info().response_head >= delay,
            "the tunnel reports its own head latency",
        );
        assert!(
            slow.info().acquire >= delay,
            "the tunnel reports its own acquisition"
        );
        let fast = proxy.connect_with(&client, "vm", "echo").await;
        client
            .connect("vm", request("reject"), proxy.connector())
            .await
            .expect_err("a rejected head is a head too");
        let after = client.stats();
        assert_eq!(after.response_latency.count, 3);
        assert_eq!(after.acquire_latency.count, 3);
        assert_eq!(after.response_latency.max_us, stats.response_latency.max_us);
        assert!(after.response_latency.sum_us > stats.response_latency.sum_us);
        drop((slow, fast));
        proxy
    });
}

#[test]
fn session_counters_distinguish_new_and_reused_sessions() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let mut reused = Vec::new();
        for _ in 0..5 {
            let tunnel = proxy.connect_with(&client, "vm", "echo").await;
            reused.push(tunnel.info().reused_session);
        }
        assert_eq!(reused, [false, true, true, true, true]);
        let other = proxy.connect_with(&client, "other", "echo").await;
        assert!(!other.info().reused_session);
        let stats = client.stats();
        assert_eq!(stats.sessions_opened, 2);
        assert_eq!(stats.streams_on_reused_session, 4);
        assert_eq!(stats.open_sessions, 2);
        drop(other);
        proxy
    });
}

#[test]
fn session_drains_count_goaway_idle_and_age() {
    run_owned(ConnectOptions::default(), |runtime, client| async move {
        let proxy = Proxy::start().await;
        let tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
        proxy.goaway();
        eventually(Duration::from_secs(5), "the GOAWAY drain", || {
            client.stats().session_drains == 1
        })
        .await;
        drop(tunnel);
        let replacement = proxy.connect_with(&client, "vm", "echo").await;
        drop(replacement);
        shutdown(runtime).await.expect("shutdown");
        assert_eq!(
            client.stats().session_drains,
            1,
            "shutdown drains are not counted"
        );
        proxy
    });

    let aging = ConnectOptions {
        max_age: Duration::from_millis(200),
        max_connections: 1,
        acquire_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    run(aging, |client| async move {
        let proxy = Proxy::start().await;
        let tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let fresh = proxy.connect_with(&client, "vm", "echo").await;
        assert_eq!(
            client.stats().session_drains,
            1,
            "max-age expiry drains the aged session"
        );
        drop((tunnel, fresh));
        proxy
    });

    let idle = ConnectOptions {
        idle_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    run(idle, |client| async move {
        let proxy = Proxy::start().await;
        let tunnel = proxy.connect_with(&client, "vm", "echo").await;
        drop(tunnel);
        eventually(Duration::from_secs(5), "route retirement", || {
            client.stats().routes == 0
        })
        .await;
        assert_eq!(
            client.stats().session_drains,
            1,
            "the retired route drained its session"
        );
        proxy
    });
}
