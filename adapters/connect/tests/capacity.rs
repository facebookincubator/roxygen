/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Admission, route, stream-cap, and connection-overflow bounds.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use http::HeaderValue;
use roxygen_connect_client::TransportSecurity;
use roxygen_connect_client::error::ConnectErrorKind;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::runtime::ConnectedIo;
use roxygen_connect_client::runtime::Connector;
use roxygen_connect_client::runtime::DialError;
use roxygen_connect_client::tunnel::PendingTunnel;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::proxy::AFTER_HALF_CLOSE;
use crate::proxy::Client;
use crate::proxy::Proxy;
use crate::proxy::ProxyConfig;
use crate::proxy::TEST_TIMEOUT;
use crate::proxy::TestConnector;
use crate::proxy::echo;
use crate::proxy::eventually;
use crate::proxy::request;
use crate::proxy::run;

#[test]
fn stalled_connection_shutdowns_do_not_accumulate_connections() {
    let options = ConnectOptions {
        idle_timeout: Duration::from_millis(100),
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let live = Arc::new(AtomicUsize::new(0));
        for cycle in 1..=4 {
            let connector =
                Arc::new(TestConnector::plain(proxy.address).stalled_shutdowns(Arc::clone(&live)));
            let mut tunnel = client
                .connect("vm", request("echo"), connector)
                .await
                .expect("CONNECT")
                .into_tunnel();
            echo(&mut tunnel, b"before the route retires").await;
            tunnel.shutdown().await.expect("upload EOF");
            tunnel
                .read_to_end(&mut Vec::new())
                .await
                .expect("download EOF");
            drop(tunnel);
            eventually(
                Duration::from_secs(5),
                "the idle route's retirement",
                || {
                    let stats = client.stats();
                    stats.session_drains == cycle && stats.routes == 0 && stats.open_sessions == 0
                },
            )
            .await;
        }
        assert_eq!(proxy.connections(), 4);
        eventually(
            Duration::from_secs(2),
            "every stalled connection to close within shutdown_timeout",
            || live.load(Ordering::SeqCst) == 0,
        )
        .await;
        proxy
    });
}

#[test]
fn stream_cap_overflows_to_another_connection() {
    let options = ConnectOptions {
        max_streams: 1,
        max_connections: 2,
        acquire_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut first = proxy.connect_with(&client, "vm", "hold-open").await;
        let mut second = proxy.connect_with(&client, "vm", "hold-open").await;
        assert_eq!(
            proxy.connections(),
            2,
            "a full session overflows to a new connection"
        );
        let error = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("both connections are full");
        assert_eq!(error.kind(), ConnectErrorKind::AcquireTimeout);
        assert!(!error.committed());
        assert_eq!(proxy.connections(), 2, "never more than max_connections");
        assert_eq!(
            proxy.observations.requests(),
            2,
            "the queued connect sent nothing"
        );
        echo(&mut first, b"first").await;
        echo(&mut second, b"second").await;
        drop((first, second));
        proxy
    });
}

#[test]
fn peer_max_concurrent_streams_overflows_without_over_dial() {
    let config = ProxyConfig {
        max_concurrent_streams: Some(2),
        ..Default::default()
    };
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::with_config(config.clone()).await;
        let mut tunnels = Vec::new();
        for _ in 0..3 {
            tunnels.push(proxy.connect_with(&client, "sequential", "hold-open").await);
        }
        assert_eq!(
            proxy.connections(),
            2,
            "the third tunnel overflows the peer limit of two"
        );
        assert_eq!(
            tunnels[2].info().retries,
            0,
            "the session honours the peer's SETTINGS instead of a refusal"
        );
        assert_eq!(client.stats().retries, 0);
        for tunnel in &mut tunnels {
            echo(tunnel, b"sequential").await;
        }
        drop(tunnels);

        let burst_proxy = Proxy::with_config(config).await;
        let connector = burst_proxy.connector();
        let mut burst = JoinSet::new();
        for _ in 0..3 {
            let client = client.clone();
            let connector = connector.clone();
            burst.spawn(async move {
                client
                    .connect("burst", request("hold-open"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        let mut established = Vec::new();
        while let Some(joined) = burst.join_next().await {
            established.push(joined.expect("caller joins").expect("burst CONNECT"));
        }
        // One dial absorbs the burst; at most one more once SETTINGS cap its session at two.
        assert!(
            burst_proxy.connections() <= 2,
            "a burst does not over-dial, saw {} connections",
            burst_proxy.connections(),
        );
        assert!(
            connector.attempts() <= 2,
            "saw {} dials",
            connector.attempts()
        );
        for tunnel in &mut established {
            echo(tunnel, b"burst").await;
        }
        drop(established);
        vec![proxy, burst_proxy]
    });
}

#[test]
fn burst_on_an_empty_route_dials_once() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let mut burst = JoinSet::new();
        for _ in 0..32 {
            let client = client.clone();
            let connector = connector.clone();
            burst.spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        let mut tunnels = Vec::new();
        while let Some(joined) = burst.join_next().await {
            tunnels.push(joined.expect("caller joins").expect("CONNECT"));
        }
        assert_eq!(
            connector.attempts(),
            1,
            "one pending dial absorbs the burst"
        );
        assert_eq!(proxy.connections(), 1);
        assert_eq!(proxy.observations.requests(), 32, "one stream each");
        let stats = client.stats();
        assert_eq!((stats.dial_attempts, stats.retries), (1, 0));
        drop(tunnels);
        proxy
    });
}

#[test]
fn connections_never_exceed_the_configured_bounds() {
    let refusing = ConnectOptions {
        max_connections: 2,
        acquire_timeout: Duration::from_millis(400),
        ..Default::default()
    };
    run(refusing, |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            max_concurrent_streams: Some(0),
            ..Default::default()
        })
        .await;
        let connector = proxy.connector();
        let started = Instant::now();
        let mut burst = JoinSet::new();
        for _ in 0..6 {
            let client = client.clone();
            let connector = connector.clone();
            burst.spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        while let Some(joined) = burst.join_next().await {
            let error = joined
                .expect("caller joins")
                .expect_err("the peer admits no stream");
            assert!(
                matches!(
                    error.kind(),
                    ConnectErrorKind::AcquireTimeout | ConnectErrorKind::Refused
                ),
                "{error:?}",
            );
            assert!(!error.committed(), "{error:?}");
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "failures arrive within the acquire window"
        );
        assert!(
            proxy.connections() <= 2,
            "saw {} connections",
            proxy.connections()
        );
        proxy
    });

    let retiring = ConnectOptions {
        max_connections: 1,
        max_retiring: Some(1),
        acquire_timeout: Duration::from_millis(400),
        ..Default::default()
    };
    run(retiring, |client| async move {
        let proxy = Proxy::start().await;
        let mut first = proxy.connect_with(&client, "vm", "hold-open").await;
        proxy.goaway();
        let mut second = timeout(Duration::from_secs(5), async {
            loop {
                let tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
                if proxy.connections() == 2 {
                    return tunnel;
                }
                drop(tunnel);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a draining session frees its slot");
        proxy.goaway();
        eventually(Duration::from_secs(5), "both sessions draining", || {
            client.stats().session_drains == 2
        })
        .await;
        let error = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("two retiring sessions hold max_connections plus max_retiring");
        assert_eq!(error.kind(), ConnectErrorKind::AcquireTimeout);
        assert_eq!(
            proxy.connections(),
            2,
            "owned sessions never exceed the ownership bound"
        );
        echo(&mut first, b"retiring tunnels keep working").await;
        echo(&mut second, b"retiring tunnels keep working").await;

        let queued = {
            let client = client.clone();
            let connector = proxy.connector();
            tokio::spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            })
        };
        eventually(Duration::from_secs(5), "the queued waiter", || {
            client.stats().waiters == 1
        })
        .await;
        drop(first);
        let mut served = queued
            .await
            .expect("caller joins")
            .expect("a retiring session that closes frees the waiter's slot");
        echo(&mut served, b"third connection").await;
        assert_eq!(proxy.connections(), 3);
        drop((second, served));
        proxy
    });
}

#[test]
fn full_route_waits_then_fails_uncommitted_at_the_acquire_deadline() {
    let acquire_timeout = Duration::from_millis(300);
    let options = ConnectOptions {
        max_connections: 1,
        max_streams: 1,
        acquire_timeout,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut held = proxy.connect_with(&client, "vm", "hold-open").await;
        let started = Instant::now();
        let error = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("no capacity frees within the deadline");
        let waited = started.elapsed();
        assert_eq!(error.kind(), ConnectErrorKind::AcquireTimeout);
        assert!(!error.committed());
        assert!(waited >= acquire_timeout, "waited {waited:?}");
        assert!(
            waited < acquire_timeout + Duration::from_secs(2),
            "waited {waited:?}"
        );
        assert_eq!(
            proxy.observations.requests(),
            1,
            "nothing was sent for the waiter"
        );
        assert_eq!(client.stats().acquire_timeouts, 1);
        echo(&mut held, b"held tunnel unaffected").await;
        drop(held);
        proxy
    });
}

#[test]
fn queued_connect_is_served_when_a_stream_frees() {
    let options = ConnectOptions {
        max_connections: 1,
        max_streams: 1,
        acquire_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let held = proxy.connect_with(&client, "vm", "hold-open").await;
        let queue = |client: &Client| {
            let client = client.clone();
            let connector = proxy.connector();
            tokio::spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            })
        };
        let cancelled = queue(&client);
        eventually(Duration::from_secs(5), "the first waiter", || {
            client.stats().waiters == 1
        })
        .await;
        cancelled.abort();
        assert!(cancelled.await.expect_err("aborted caller").is_cancelled());
        eventually(
            Duration::from_secs(5),
            "the cancelled waiter to leave",
            || client.stats().waiters == 0,
        )
        .await;
        let queued = queue(&client);
        eventually(Duration::from_secs(5), "the queued waiter", || {
            client.stats().waiters == 1
        })
        .await;
        drop(held);
        let mut served = queued
            .await
            .expect("caller joins")
            .expect("the freed stream serves the waiter");
        echo(&mut served, b"served").await;
        assert_eq!(
            proxy.observations.requests(),
            2,
            "the cancelled waiter sent nothing"
        );
        assert_eq!(proxy.connections(), 1);
        drop(served);
        proxy
    });
}

#[test]
fn goaway_with_a_hold_open_tunnel_does_not_block_the_route() {
    let options = ConnectOptions {
        max_connections: 1,
        acquire_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut held = proxy.connect_with(&client, "vm", "hold-open").await;
        proxy.goaway();
        let replacement = timeout(Duration::from_secs(5), async {
            loop {
                let tunnel = proxy.connect_with(&client, "vm", "echo").await;
                if proxy.connections() == 2 {
                    return tunnel;
                }
                drop(tunnel);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let mut replacement = replacement.expect("the next connect uses a new connection in time");
        echo(&mut replacement, b"new connection").await;
        echo(&mut held, b"the draining session's tunnel keeps working").await;
        drop((held, replacement));
        proxy
    });
}

#[test]
fn route_and_tunnel_admission_are_bounded() {
    let options = ConnectOptions {
        max_routes: 1,
        max_tunnels: 2,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let mut unsupported = request("echo");
        let mut sensitive = HeaderValue::from_static("private application credential");
        sensitive.set_sensitive(true);
        unsupported
            .headers_mut()
            .insert("x-private-token", sensitive);
        let rejected = client
            .connect("vm", unsupported, connector.clone())
            .await
            .expect_err("unsupported metadata");
        assert_eq!(rejected.kind(), ConnectErrorKind::InvalidRequest);
        assert_eq!(connector.attempts(), 0);
        assert_eq!(proxy.connections(), 0, "unsupported metadata never dials");
        let mut first = proxy.connect_with(&client, "vm", "half-close").await;
        let other_route = client
            .connect("other-vm", request("echo"), proxy.connector())
            .await
            .expect_err("one route");
        assert_eq!(other_route.kind(), ConnectErrorKind::RouteCapacity);
        assert!(!other_route.committed());
        let mut second = proxy.connect_with(&client, "vm", "echo").await;
        let excess = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("two tunnels");
        assert_eq!(excess.kind(), ConnectErrorKind::Overloaded);
        assert!(!excess.committed());
        assert_eq!(proxy.connections(), 1, "rejected work must not dial");
        first.shutdown().await.expect("first upload completes");
        let mut prefix = [0];
        first
            .read_exact(&mut prefix)
            .await
            .expect("response starts");
        assert_eq!(prefix[0], AFTER_HALF_CLOSE[0]);
        echo(&mut second, b"owner progresses after first response EOF").await;
        let unread = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("unread bridge bytes keep admission");
        assert_eq!(
            unread.kind(),
            ConnectErrorKind::Overloaded,
            "a completed tunnel with unread bridge bytes still owns admission",
        );
        let mut remainder = Vec::new();
        first
            .read_to_end(&mut remainder)
            .await
            .expect("first response completes");
        assert_eq!(remainder, AFTER_HALF_CLOSE[1..]);
        let retained = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("a retained tunnel keeps admission");
        assert_eq!(retained.kind(), ConnectErrorKind::Overloaded);
        drop(first);
        echo(&mut second, b"existing admission remains valid").await;
        let mut recovered = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut recovered, b"released admission is reusable").await;
        drop((second, recovered));
        proxy
    });
}

#[test]
fn admission_is_released_before_an_error_reply() {
    let options = ConnectOptions {
        max_routes: 1,
        max_tunnels: 1,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        for round in 0..200 {
            let rejected = client
                .connect("vm", request("reject"), connector.clone())
                .await
                .expect_err("403");
            assert_eq!(rejected.kind(), ConnectErrorKind::Rejected, "round {round}");
            let capacity = client
                .connect("other-vm", request("echo"), connector.clone())
                .await
                .expect_err("one route");
            assert_eq!(
                capacity.kind(),
                ConnectErrorKind::RouteCapacity,
                "round {round}: the rejection released its permit before replying",
            );
        }
        let mut tunnel = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut tunnel, b"admission intact").await;
        assert_eq!(client.stats().overloaded, 0);
        drop(tunnel);
        proxy
    });
}

/// Parks the protocol worker in the `security()` call that creates its route until `admitted`
/// connects were admitted, so they all queue for the worker at once.
struct ParkingConnector {
    dial: Arc<TestConnector>,
    client: Client,
    admitted: u64,
}

impl Connector for ParkingConnector {
    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>> {
        self.dial.connect()
    }

    fn security(&self) -> TransportSecurity {
        let started = Instant::now();
        while self.client.stats().admitted < self.admitted {
            assert!(started.elapsed() < TEST_TIMEOUT, "the burst is admitted");
            thread::sleep(Duration::from_millis(1));
        }
        self.dial.security()
    }
}

#[test]
fn bursts_beyond_128_connects_never_see_overloaded() {
    const BURST: u64 = 300;
    let options = ConnectOptions {
        max_tunnels: BURST as usize,
        acquire_timeout: Duration::from_secs(20),
        ..Default::default()
    };
    run(options, |client| async move {
        // fwdproxy-hatch's limit, so a session that opened a 101st stream would see a refusal.
        let proxy = Proxy::with_config(ProxyConfig {
            max_concurrent_streams: Some(100),
            ..Default::default()
        })
        .await;
        let connector = Arc::new(ParkingConnector {
            dial: proxy.connector(),
            client: client.clone(),
            admitted: BURST,
        });
        let mut burst = JoinSet::new();
        for _ in 0..BURST {
            let client = client.clone();
            let connector = connector.clone();
            burst.spawn(async move {
                client
                    .connect("vm", request("hold-open"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        let mut tunnels = Vec::new();
        while let Some(joined) = burst.join_next().await {
            tunnels.push(
                joined
                    .expect("caller joins")
                    .expect("no connect sees Overloaded"),
            );
        }
        assert_eq!(tunnels.len(), BURST as usize);
        let stats = client.stats();
        assert_eq!((stats.overloaded, stats.retries), (0, 0));
        assert_eq!(proxy.observations.requests(), BURST as usize);
        assert!(proxy.connections() <= 4);
        drop(tunnels);
        proxy
    });
}

/// A small deterministic generator for the stress test's choices.
struct Choices(u64);

impl Choices {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

#[test]
fn random_cancellation_stress_restores_permits_and_routes() {
    const TUNNELS: usize = 64;
    let options = ConnectOptions {
        max_tunnels: TUNNELS,
        max_streams: 16,
        max_connections: 4,
        idle_timeout: Duration::from_millis(200),
        response_timeout: Duration::from_millis(500),
        acquire_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let mut choices = Choices(0x2545_f491_4f6c_dd1d);
        let mut work = JoinSet::new();
        for index in 0..400_u64 {
            let client = client.clone();
            let connector = connector.clone();
            let key = ["a", "b", "c"][choices.next(3) as usize];
            let action = match choices.next(4) {
                0 => "stall",
                1 => "hold-open",
                _ => "echo",
            };
            let cancel_after = Duration::from_millis(choices.next(20));
            let hold = Duration::from_millis(choices.next(20));
            let cancel = choices.next(3) == 0;
            work.spawn(async move {
                let connect = client.connect(key, request(action), connector);
                if cancel {
                    let _ = timeout(cancel_after, connect).await;
                    return;
                }
                if let Ok(mut tunnel) = connect.await.map(PendingTunnel::into_tunnel) {
                    if index % 2 == 0 {
                        let _ = tunnel.write_all(b"stress").await;
                    }
                    tokio::time::sleep(hold).await;
                }
            });
            if work.len() >= TUNNELS / 2 {
                work.join_next().await.expect("work").expect("stress task");
            }
        }
        while let Some(joined) = work.join_next().await {
            joined.expect("stress task");
        }
        eventually(Duration::from_secs(10), "admission accounting", || {
            let stats = client.stats();
            stats.waiters == 0 && stats.active_tunnels == 0
        })
        .await;
        let mut held = JoinSet::new();
        for _ in 0..TUNNELS {
            let client = client.clone();
            let connector = connector.clone();
            held.spawn(async move {
                client
                    .connect("a", request("hold-open"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        let mut tunnels = Vec::new();
        while let Some(joined) = held.join_next().await {
            tunnels.push(
                joined
                    .expect("caller joins")
                    .expect("every permit returned"),
            );
        }
        assert_eq!(tunnels.len(), TUNNELS);
        drop(tunnels);
        eventually(Duration::from_secs(10), "idle routes to retire", || {
            client.stats().routes == 0
        })
        .await;
        assert_eq!(client.stats().active_tunnels, 0);
        proxy
    });
}
