/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! PING probe oracles: probe options, acknowledged and unanswered probes, and probe drops.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use roxygen_connect_client::error::ConnectErrorKind;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::relay::RelayClose;
use roxygen_connect_client::relay::RelayIo;
use roxygen_connect_client::tunnel::Tunnel;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::proxy::Client;
use crate::proxy::Proxy;
use crate::proxy::ProxyConfig;
use crate::proxy::RawProxy;
use crate::proxy::RawScript;
use crate::proxy::Tap;
use crate::proxy::TestConnector;
use crate::proxy::echo;
use crate::proxy::eventually;
use crate::proxy::pattern;
use crate::proxy::request;
use crate::proxy::run;
use crate::proxy::written_pings;

const INTERVAL: Duration = Duration::from_millis(100);
/// The probe timeout where a probe drop is under test.
const PROBE_TIMEOUT: Duration = Duration::from_millis(100);
/// The probe timeout where every probe must be acknowledged, with room for a loaded host.
const ACKED_PROBE_TIMEOUT: Duration = Duration::from_millis(400);

fn probing() -> ConnectOptions {
    ConnectOptions {
        ping_interval: INTERVAL,
        ping_timeout: PROBE_TIMEOUT,
        ..Default::default()
    }
}

async fn tunnel(client: &Client, key: &'static str, connector: Arc<TestConnector>) -> Tunnel {
    client
        .connect(key, request("echo"), connector)
        .await
        .expect("CONNECT succeeds")
        .into_tunnel()
}

/// Writes more than the stream and bridge can buffer, so the write parks once flow control stalls.
async fn parked_upload(tunnel: &mut Tunnel) -> io::Result<()> {
    tunnel.write_all(&pattern(1 << 20)).await
}

fn assert_timed_out<T: std::fmt::Debug>(result: io::Result<T>, what: &str) {
    assert_eq!(
        result.expect_err(what).kind(),
        io::ErrorKind::TimedOut,
        "{what}"
    );
}

#[test]
fn acknowledged_probes_keep_a_quiet_tunnel_and_its_session() {
    let options = ConnectOptions {
        ping_timeout: ACKED_PROBE_TIMEOUT,
        ..probing()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let mut quiet = tunnel(&client, "vm", connector.clone()).await;
        echo(&mut quiet, b"before the quiet period").await;
        tokio::time::sleep(INTERVAL * 6).await;
        echo(&mut quiet, b"after several acknowledged probes").await;
        let probes = written_pings(&tap.written()[0]);
        assert!(probes >= 2, "the quiet session sent {probes} probes");
        let next = client
            .connect("vm", request("echo"), connector)
            .await
            .expect("CONNECT succeeds");
        assert!(next.info().reused_session, "the probed session is reused");
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().ping_timeouts, 0);
        drop((quiet, next));
        proxy
    });
}

#[test]
fn zero_ping_interval_sends_no_probe() {
    let options = ConnectOptions {
        ping_interval: Duration::ZERO,
        ..probing()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let mut quiet = tunnel(&client, "vm", connector).await;
        echo(&mut quiet, b"before the quiet period").await;
        tokio::time::sleep(INTERVAL * 4).await;
        echo(&mut quiet, b"after the quiet period").await;
        assert_eq!(written_pings(&tap.written()[0]), 0);
        assert_eq!(client.stats().ping_timeouts, 0);
        drop(quiet);
        proxy
    });
}

#[test]
fn unanswered_probe_on_an_idle_session_redials_the_route() {
    run(probing(), |client| async move {
        let raw = RawProxy::start(RawScript::IdleWithoutPingAcks).await;
        let mut first = tunnel(&client, "raw", raw.connector()).await;
        echo(&mut first, b"before the session idles").await;
        first.shutdown().await.expect("upload EOF");
        first
            .read_to_end(&mut Vec::new())
            .await
            .expect("download EOF");
        drop(first);
        eventually(
            Duration::from_secs(5),
            "the idle session's probe drop",
            || client.stats().ping_timeouts == 1,
        )
        .await;
        eventually(Duration::from_secs(5), "the dropped session", || {
            client.stats().open_sessions == 0
        })
        .await;
        let mut next = tunnel(&client, "raw", raw.connector()).await;
        assert!(!next.info().reused_session);
        echo(&mut next, b"on a new session").await;
        assert_eq!(raw.observations.connections(), 2);
        assert_eq!(client.stats().ping_timeouts, 1);
        drop(next);
        raw.shutdown().await;
    });
}

#[test]
fn unanswered_probe_times_out_bridged_tunnels() {
    run(probing(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let reading = proxy.connect_with(&client, "vm", "echo").await;
        let mut writing = proxy.connect_with(&client, "vm", "sink").await;
        let (mut reader, mut reading_writer) = tokio::io::split(reading);
        let pending_read = tokio::spawn(async move {
            let mut byte = [0; 1];
            let read = reader.read(&mut byte).await;
            (read, reader)
        });
        let pending_write = tokio::spawn(async move {
            let written = parked_upload(&mut writing).await;
            (written, writing)
        });
        tokio::time::sleep(INTERVAL).await;
        proxy.stop_reading();
        let (read, mut reader) = pending_read.await.expect("reader");
        assert_timed_out(read, "the pending read");
        let (written, mut writing) = pending_write.await.expect("writer");
        assert_timed_out(written, "the upload parked on a zero window");
        assert_timed_out(reader.read(&mut [0; 1]).await, "a later read");
        assert_timed_out(reading_writer.write(b"x").await, "a later write");
        assert_timed_out(writing.write(b"x").await, "a later write");
        assert_timed_out(writing.flush().await, "a later flush");
        assert_timed_out(writing.shutdown().await, "a later shutdown");
        assert_timed_out(writing.read(&mut [0; 1]).await, "a later read");
        let stats = client.stats();
        assert_eq!(stats.ping_timeouts, 1);
        assert_eq!(
            (stats.tunnels_reset_by_peer, stats.tunnels_aborted),
            (0, 0),
            "a probe drop is neither a peer reset nor an abort"
        );
        proxy
    });
}

#[test]
fn probe_drop_after_both_bridge_directions_ended_fails_the_held_upload() {
    run(probing(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let mut held = proxy.connect_with(&client, "vm", "early-eof").await;
        let downloaded = held
            .read_to_end(&mut Vec::new())
            .await
            .expect("download EOF");
        assert_eq!(downloaded, 0, "the proxy ended its download at once");
        held.write_all(b"held by the zero window")
            .await
            .expect("upload");
        held.shutdown().await.expect("the upload's end is queued");
        proxy.stop_reading();
        eventually(Duration::from_secs(5), "the bridge to end", || {
            client.stats().active_tunnels == 0
        })
        .await;
        let failure = |result: io::Result<()>, what: &str| {
            assert_eq!(
                result.expect_err(what).kind(),
                io::ErrorKind::ConnectionReset,
                "{what}"
            );
        };
        failure(held.flush().await, "a later flush");
        failure(held.shutdown().await, "a later shutdown");
        failure(held.write_all(b"x").await, "a later write");
        let stats = client.stats();
        assert_eq!(stats.ping_timeouts, 1);
        assert_eq!(
            (stats.tunnels_reset_by_peer, stats.tunnels_aborted),
            (1, 0),
            "a probe drop after the download's END_STREAM is a peer reset"
        );
        drop(held);
        proxy
    });
}

#[test]
fn peer_ending_after_the_download_end_fails_a_quiet_bridge() {
    for probe_drop in [true, false] {
        let options = ConnectOptions {
            max_tunnels: 1,
            idle_timeout: Duration::from_millis(200),
            ..probing()
        };
        run(options, move |client| async move {
            let case = if probe_drop {
                "probe drop"
            } else {
                "connection reset"
            };
            let proxy = Proxy::with_config(ProxyConfig {
                reset_on_close: true,
                ..Default::default()
            })
            .await;
            let mut quiet = proxy.connect_with(&client, "vm", "early-eof").await;
            let downloaded = quiet
                .read_to_end(&mut Vec::new())
                .await
                .expect("download EOF");
            assert_eq!(
                downloaded, 0,
                "{case}: the proxy ended its download at once"
            );
            if probe_drop {
                proxy.stop_reading();
            } else {
                proxy.close_connections();
            }
            eventually(Duration::from_secs(5), "the bridge to end", || {
                client.stats().active_tunnels == 0
            })
            .await;
            let reset = |result: io::Result<usize>, what: &str| {
                assert_eq!(
                    result.expect_err(what).kind(),
                    io::ErrorKind::ConnectionReset,
                    "{case}: {what}"
                );
            };
            reset(quiet.read(&mut [0; 1]).await, "a later read");
            reset(quiet.write(b"x").await, "a later write");
            reset(quiet.flush().await.map(|()| 0), "a later flush");
            reset(quiet.shutdown().await.map(|()| 0), "a later shutdown");
            let stats = client.stats();
            assert_eq!(
                (
                    stats.tunnels_reset_by_peer,
                    stats.tunnels_aborted,
                    stats.ping_timeouts
                ),
                (1, 0, u64::from(probe_drop)),
                "{case}"
            );
            eventually(
                Duration::from_secs(5),
                "the bridge's route to retire",
                || client.stats().routes == 0,
            )
            .await;

            // The failed tunnel's permit returns with it, as any retained tunnel's does.
            drop(quiet);
            let other = Proxy::start().await;
            let mut next = other.connect_with(&client, "other", "echo").await;
            echo(&mut next, b"at the admission bound").await;
            assert_eq!(client.stats().overloaded, 0, "{case}");
            drop(next);
            vec![proxy, other]
        });
    }
}

/// An endless application upload.
struct EndlessUpload;

impl AsyncRead for EndlessUpload {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let source = pattern(buffer.remaining());
        buffer.put_slice(&source);
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for EndlessUpload {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::task::Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[test]
fn unanswered_probe_times_out_a_relay() {
    run(probing(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let quiet = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect("CONNECT");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let mut application = TcpStream::connect(listener.local_addr().expect("address"))
            .await
            .expect("connect");
        let (relayed, _) = listener.accept().await.expect("accept");
        let quiet_relay = tokio::spawn(quiet.relay(relayed.into_std().expect("std socket")));
        let parked = client
            .connect("vm", request("sink"), proxy.connector())
            .await
            .expect("CONNECT");
        let parked_relay = tokio::spawn(parked.relay(RelayIo::Boxed(Box::new(EndlessUpload))));
        tokio::time::sleep(INTERVAL).await;
        proxy.stop_reading();
        let quiet = quiet_relay.await.expect("relay joins");
        assert_eq!(quiet.close(), RelayClose::TimedOut, "{quiet:?}");
        assert_eq!(
            quiet.error().map(io::Error::kind),
            Some(io::ErrorKind::TimedOut)
        );
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("the quiet application reads end of stream");
        assert!(rest.is_empty());
        let parked = parked_relay.await.expect("relay joins");
        assert_eq!(parked.close(), RelayClose::TimedOut, "{parked:?}");
        assert_eq!(
            parked.error().map(io::Error::kind),
            Some(io::ErrorKind::TimedOut)
        );
        assert!(
            parked.uploaded() > 0,
            "the parked relay uploaded into its buffer"
        );
        let stats = client.stats();
        assert_eq!(stats.ping_timeouts, 1);
        assert_eq!(stats.tunnels_relayed, 2);
        assert_eq!((stats.tunnels_reset_by_peer, stats.tunnels_aborted), (0, 0));
        proxy
    });
}

/// Long enough that a probe drop during a head wait comes inside the acquire window.
fn head_wait(acquire_timeout: Duration, max_unprocessed_retries: u32) -> ConnectOptions {
    ConnectOptions {
        acquire_timeout,
        response_timeout: Duration::from_secs(5),
        max_unprocessed_retries,
        ..probing()
    }
}

#[test]
fn probe_drop_during_a_reused_head_wait_is_retried_once() {
    run(head_wait(Duration::from_secs(5), 1), |client| async move {
        let raw = RawProxy::start(RawScript::SilentFrom(2)).await;
        let mut first = tunnel(&client, "raw", raw.connector()).await;
        echo(&mut first, b"the session carries a stream").await;
        let retried = client
            .connect("raw", request("second"), raw.connector())
            .await
            .expect("the probe drop is retried on a new session");
        let info = retried.info();
        assert_eq!(info.retries, 1);
        assert!(!info.reused_session);
        let mut retried = retried.into_tunnel();
        echo(&mut retried, b"on the new session").await;
        assert_timed_out(
            first.read(&mut [0; 1]).await,
            "the dropped session's tunnel",
        );
        assert_eq!(raw.observations.connections(), 2);
        let stats = client.stats();
        assert_eq!((stats.ping_timeouts, stats.retries), (1, 1));
        drop(retried);
        raw.shutdown().await;
    });
}

#[test]
fn probe_drop_during_a_head_wait_without_retry_fails_committed() {
    for (case, script, options) in [
        (
            "retries disabled",
            RawScript::SilentFrom(2),
            head_wait(Duration::from_secs(5), 0),
        ),
        (
            "a session that carried no earlier stream",
            RawScript::SilentFrom(1),
            head_wait(Duration::from_secs(5), 1),
        ),
        (
            "the acquire window closed before the drop",
            RawScript::SilentFrom(2),
            // Slower probes put the earliest drop 350 ms past the window.
            ConnectOptions {
                ping_interval: Duration::from_millis(250),
                ping_timeout: Duration::from_millis(250),
                ..head_wait(Duration::from_millis(150), 1)
            },
        ),
    ] {
        run(options, move |client| async move {
            let raw = RawProxy::start(script).await;
            let first = if script == RawScript::SilentFrom(2) {
                let mut first = tunnel(&client, "raw", raw.connector()).await;
                echo(&mut first, b"the session carries a stream").await;
                Some(first)
            } else {
                None
            };
            let error = client
                .connect("raw", request("silent"), raw.connector())
                .await
                .expect_err(case);
            assert_eq!(error.kind(), ConnectErrorKind::Failed, "{case}: {error:?}");
            assert!(error.committed(), "{case}");
            assert_eq!(raw.observations.connections(), 1, "{case}: no retry");
            let stats = client.stats();
            assert_eq!((stats.ping_timeouts, stats.retries), (1, 0), "{case}");
            drop(first);
            raw.shutdown().await;
        });
    }
}
