/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Cancellation races, job drops, shutdown, per-tunnel tasks, quiet sessions, and route expiry.

use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use roxygen_connect_client::TransportSecurity;
use roxygen_connect_client::error::ConnectErrorKind;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::runtime::ConnectRuntime;
use roxygen_connect_client::runtime::ConnectedIo;
use roxygen_connect_client::runtime::Connector;
use roxygen_connect_client::runtime::DialError;
use roxygen_connect_client::tunnel::PendingTunnel;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::proxy::AFTER_HALF_CLOSE;
use crate::proxy::BEYOND_NATIVE_TIMEOUT;
use crate::proxy::Proxy;
use crate::proxy::ProxyConfig;
use crate::proxy::RawProxy;
use crate::proxy::RawScript;
use crate::proxy::Tap;
use crate::proxy::TestConnector;
use crate::proxy::delayed_request;
use crate::proxy::echo;
use crate::proxy::eventually;
use crate::proxy::pattern;
use crate::proxy::request;
use crate::proxy::request_with;
use crate::proxy::run;
use crate::proxy::run_owned;
use crate::proxy::shutdown;
use crate::proxy::written_resets;

#[test]
fn quiet_session_outlives_the_native_read_timeout() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let mut tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
        echo(&mut tunnel, b"before the quiet period").await;
        tokio::time::sleep(BEYOND_NATIVE_TIMEOUT).await;
        echo(&mut tunnel, b"after the quiet period").await;
        let mut reused = proxy.connect_with(&client, "vm", "echo").await;
        assert!(
            reused.info().reused_session,
            "the quiet session was not drained"
        );
        echo(&mut reused, b"reused").await;
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().session_drains, 0);
        drop((tunnel, reused));
        proxy
    });
}

/// A forward guard: on HTTP/2 the native session arms no stream read timeout at request EOM, so
/// this test becomes sensitive to the adapter's zero `stream_read_timeout` only once
/// `repair.request_stream_read_timeout` lands.
#[test]
fn half_closed_tunnel_outlives_the_native_stream_read_timeout() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let mut tunnel = proxy.connect_with(&client, "vm", "late-response").await;
        tunnel.write_all(b"upload").await.expect("upload");
        tunnel.shutdown().await.expect("upload EOF");
        let mut received = Vec::new();
        tunnel
            .read_to_end(&mut received)
            .await
            .expect("the quiet download stays open");
        let mut expected = b"upload".to_vec();
        expected.extend_from_slice(AFTER_HALF_CLOSE);
        assert_eq!(received, expected);
        drop(tunnel);
        proxy
    });
}

#[test]
fn stalled_destination_beyond_the_native_write_timeout_keeps_its_tunnel() {
    const UPLOAD: usize = 1024 * 1024;
    run(ConnectOptions::default(), |client| async move {
        // A large connection window, so the stalled stream holds only its own window.
        let proxy = Proxy::with_config(ProxyConfig {
            initial_connection_window_size: Some(16 * 1024 * 1024),
            ..Default::default()
        })
        .await;
        proxy.close_gate();
        let stalled = proxy.connect_with(&client, "vm", "gated-sink").await;
        let mut sibling = proxy.connect_with(&client, "vm", "echo").await;
        let (mut reader, mut writer) = tokio::io::split(stalled);
        let upload = tokio::spawn(async move {
            writer
                .write_all(&pattern(UPLOAD))
                .await
                .expect("upload resumes");
            writer.shutdown().await.expect("upload EOF");
            writer
        });
        tokio::time::sleep(BEYOND_NATIVE_TIMEOUT).await;
        assert!(!upload.is_finished(), "the destination is not reading");
        echo(&mut sibling, b"siblings keep flowing during the stall").await;
        proxy.open_gate();
        let writer = upload.await.expect("upload task");
        let mut rest = Vec::new();
        reader
            .read_to_end(&mut rest)
            .await
            .expect("the tunnel ends cleanly");
        assert_eq!(proxy.observations.uploaded(), UPLOAD as u64);
        drop((reader, writer, sibling));
        proxy
    });
}

#[test]
fn connection_window_stall_beyond_the_native_write_timeout_keeps_every_tunnel() {
    const UPLOAD: usize = 256 * 1024;
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(4 * 1024 * 1024),
            ..Default::default()
        })
        .await;
        proxy.close_gate();
        let mut uploads = JoinSet::new();
        for _ in 0..3 {
            let tunnel = proxy.connect_with(&client, "vm", "gated-sink").await;
            uploads.spawn(async move {
                let (mut reader, mut writer) = tokio::io::split(tunnel);
                writer
                    .write_all(&pattern(UPLOAD))
                    .await
                    .expect("upload resumes");
                writer.shutdown().await.expect("upload EOF");
                let mut rest = Vec::new();
                reader.read_to_end(&mut rest).await.expect("clean end");
            });
        }
        tokio::time::sleep(BEYOND_NATIVE_TIMEOUT).await;
        assert!(
            uploads.try_join_next().is_none(),
            "every upload still waits on the connection window",
        );
        proxy.open_gate();
        while let Some(joined) = uploads.join_next().await {
            joined.expect("every tunnel resumes");
        }
        assert_eq!(proxy.observations.uploaded(), 3 * UPLOAD as u64);
        assert_eq!(proxy.connections(), 1);
        proxy
    });
}

/// Connects with a head delayed by `delay` and gives up after `cancel`.
async fn race(client: &crate::proxy::Client, proxy: &Proxy, delay: u64, cancel: Duration) -> bool {
    let request = delayed_request("echo", Duration::from_millis(delay));
    match timeout(cancel, client.connect("vm", request, proxy.connector())).await {
        Ok(tunnel) => {
            drop(tunnel.expect("CONNECT").into_tunnel());
            true
        }
        Err(_) => false,
    }
}

#[test]
fn cancellation_racing_the_2xx_resets_the_stream() {
    let options = ConnectOptions {
        max_connections: 1,
        max_streams: 1,
        acquire_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        // A warm route, so no swept attempt includes the dial.
        drop(proxy.connect_with(&client, "vm", "echo").await);
        proxy
            .observations
            .wait("the warm-up stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let mut attempts = 0;
        let mut established = 0;
        // Cancellation lands from 3 ms before the head to 12 ms after it.
        for delay in (20..60).step_by(8) {
            for jitter in 0..16 {
                attempts += 1;
                let cancel = Duration::from_millis(delay + jitter) - Duration::from_millis(3);
                if race(&client, &proxy, delay, cancel).await {
                    established += 1;
                }
            }
        }
        assert!(
            0 < established && established < attempts,
            "the sweep crosses the 2xx: {established} of {attempts} established"
        );
        let mut after = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut after, b"every stream slot was released").await;
        // The proxy has now seen every CONNECT; an attempt cancelled before commit sent none.
        proxy
            .observations
            .wait("a CANCEL reset for every earlier CONNECT", |observed| {
                observed.cancel_resets() + 1 == observed.requests()
            })
            .await;
        assert_eq!(proxy.connections(), 1, "the session survived every race");
        let stats = client.stats();
        assert_eq!((stats.waiters, stats.active_tunnels), (0, 1));
        drop(after);
        proxy
    });
}

#[test]
fn response_deadline_racing_the_2xx_resets_the_stream() {
    let options = ConnectOptions {
        max_connections: 1,
        max_streams: 1,
        response_timeout: Duration::from_millis(40),
        acquire_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut timed_out = 0;
        let mut attempts = 0;
        // The head comes from 10 ms before the deadline to 10 ms after it.
        for delay in 30..50 {
            for _ in 0..2 {
                attempts += 1;
                let request = delayed_request("echo", Duration::from_millis(delay));
                match client.connect("vm", request, proxy.connector()).await {
                    Ok(tunnel) => drop(tunnel.into_tunnel()),
                    Err(error) => {
                        assert_eq!(error.kind(), ConnectErrorKind::ResponseTimeout);
                        timed_out += 1;
                    }
                }
            }
        }
        assert!(
            0 < timed_out && timed_out < attempts,
            "the sweep crosses the deadline: {timed_out} of {attempts} timed out"
        );
        proxy
            .observations
            .wait("a CANCEL reset for every stream", |observed| {
                observed.cancel_resets() == attempts
            })
            .await;
        let mut after = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut after, b"every stream slot was released").await;
        assert_eq!(proxy.connections(), 1);
        drop(after);
        proxy
    });
}

#[test]
fn abandoned_connect_resets_stream_and_releases_admission() {
    let options = ConnectOptions {
        max_tunnels: 1,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let caller = {
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
            .wait("the CONNECT", |observed| observed.requests() == 1)
            .await;
        caller.abort();
        assert!(caller.await.expect_err("aborted caller").is_cancelled());
        proxy
            .observations
            .wait("one RST_STREAM CANCEL", |observed| {
                observed.cancel_resets() == 1
            })
            .await;

        // The worker hands over a tunnel that its dropped caller never receives.
        let mut undelivered = Box::pin(client.connect(
            "vm",
            request_with("record", "undelivered"),
            proxy.connector(),
        ));
        let in_flight = poll_fn(|context| Poll::Ready(undelivered.as_mut().poll(context))).await;
        assert!(in_flight.is_pending(), "the connect awaits the worker");
        eventually(Duration::from_secs(5), "the established tunnel", || {
            client.stats().active_tunnels == 1
        })
        .await;
        drop(undelivered);
        proxy
            .observations
            .wait("the undelivered stream's reset", |observed| {
                observed.record("undelivered").is_some()
            })
            .await;
        let record = proxy.observations.record("undelivered").expect("record");
        assert!(!record.end_stream);
        assert_eq!(record.reset, Some(h2::Reason::CANCEL));
        eventually(Duration::from_secs(5), "the ended tunnel task", || {
            client.stats().active_tunnels == 0
        })
        .await;
        let stats = client.stats();
        assert_eq!(
            (
                stats.tunnels_established,
                stats.tunnels_aborted,
                stats.streams_on_reused_session
            ),
            (0, 0, 0),
            "a tunnel that connect() never returned is not counted"
        );

        let mut recovered = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut recovered, b"admission was released").await;
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().tunnels_established, 1);
        drop(recovered);
        proxy
    });
}

#[test]
fn dropping_half_closed_tunnel_releases_native_stream_capacity() {
    let options = ConnectOptions {
        max_streams: 1,
        max_connections: 1,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut half_closed = proxy.connect_with(&client, "vm", "hold-open").await;
        echo(&mut half_closed, b"upload before half-close").await;
        half_closed.shutdown().await.expect("upload half-close");
        proxy
            .observations
            .wait("the upload EOF", |observed| observed.upload_eofs() == 1)
            .await;
        drop(half_closed);
        proxy
            .observations
            .wait("the reset of the half-closed stream", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let mut recovered = proxy.connect_with(&client, "vm", "echo").await;
        echo(
            &mut recovered,
            b"half-closed stream no longer owns capacity",
        )
        .await;
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().tunnels_aborted, 1);
        drop(recovered);
        proxy
    });

    // A wire-complete stream drops without a reset and is no abort, even with most of its
    // download unread.
    let options = ConnectOptions {
        max_streams: 1,
        max_connections: 1,
        bridge_capacity: 1024,
        ..Default::default()
    };
    run(options, |client| async move {
        let raw = RawProxy::start(RawScript::LargeResponseAfterUploadEnd).await;
        let mut complete = client
            .connect("raw", request("first"), raw.connector())
            .await
            .expect("first stream")
            .into_tunnel();
        complete.shutdown().await.expect("upload half-close");
        // The route's only stream slot frees once the response's END_STREAM arrived.
        let mut sibling = client
            .connect("raw", request("second"), raw.connector())
            .await
            .expect("a sibling on the freed slot")
            .into_tunnel();
        drop(complete);
        eventually(Duration::from_secs(5), "the dropped tunnel's task", || {
            client.stats().active_tunnels == 1
        })
        .await;
        // The sibling's round trip comes after anything the drop wrote.
        echo(&mut sibling, b"after the wire-complete drop").await;
        assert_eq!(
            raw.observations.resets(),
            [],
            "a wire-complete stream gets no RST_STREAM"
        );
        assert_eq!(raw.observations.connections(), 1);
        assert_eq!(client.stats().tunnels_aborted, 0);
        drop(sibling);
        raw.shutdown().await;
    });

    // A tunnel whose response ended while flow control still holds its upload and END_STREAM
    // keeps its reset until the stream is wire-complete.
    let options = ConnectOptions {
        max_streams: 1,
        max_connections: 1,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let mut blocked = proxy.connect_with(&client, "vm", "early-eof").await;
        blocked
            .write_all(b"an upload the proxy's window never admits")
            .await
            .expect("the bridge accepts the upload");
        blocked
            .shutdown()
            .await
            .expect("the native stream accepted the upload and its END_STREAM");
        let mut download = Vec::new();
        blocked
            .read_to_end(&mut download)
            .await
            .expect("the response ended");
        assert!(download.is_empty());
        drop(blocked);
        proxy
            .observations
            .wait("the blocked stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let sibling = proxy.connect_with(&client, "vm", "early-eof").await;
        assert!(
            sibling.info().reused_session,
            "the reset freed the session's only stream slot"
        );
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().tunnels_aborted, 1);
        drop(sibling);
        proxy
    });

    // The session's failure closes the blocked stream, so its retained tunnel's task ends.
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let mut blocked = proxy.connect_with(&client, "vm", "early-eof").await;
        blocked.write_all(b"blocked upload").await.expect("upload");
        blocked.shutdown().await.expect("upload half-close");
        blocked
            .read_to_end(&mut Vec::new())
            .await
            .expect("the response ended");
        assert_eq!(client.stats().active_tunnels, 1);
        proxy.close_connections();
        eventually(Duration::from_secs(5), "the closed stream's task", || {
            client.stats().active_tunnels == 0
        })
        .await;
        drop(blocked);
        assert_eq!(client.stats().tunnels_aborted, 0);
        proxy
    });
}

/// An application waker that panics; a tunnel task wakes it when the native stream accepts
/// the bytes a flush waits for.
struct PanickingWaker;

impl Wake for PanickingWaker {
    fn wake(self: Arc<Self>) {
        panic!("application waker panicked");
    }
}

/// An application connector whose drop panics; the worker drops a connector that a live route
/// ignores.
struct PanicsWhenDropped;

impl Connector for PanicsWhenDropped {
    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>> {
        Box::pin(pending())
    }

    fn security(&self) -> TransportSecurity {
        TransportSecurity::Plaintext
    }
}

impl Drop for PanicsWhenDropped {
    fn drop(&mut self) {
        panic!("application connector panicked");
    }
}

#[test]
fn tunnel_job_dropped_without_a_result_fails_the_tunnel() {
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run_owned(options.clone(), |runtime, client| async move {
        let proxy = Proxy::start().await;
        proxy.close_gate();
        let tunnel = proxy.connect_with(&client, "vm", "gated-sink").await;
        let (mut reader, mut writer) = tokio::io::split(tunnel);
        let read = tokio::spawn(async move {
            let result = reader.read(&mut [0; 16]).await;
            (reader, result)
        });
        let write = tokio::spawn(async move {
            let result = writer.write_all(&pattern(8 * 1024 * 1024)).await;
            (writer, result)
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !read.is_finished() && !write.is_finished(),
            "both directions are parked"
        );
        shutdown(runtime).await.expect("shutdown");
        let (mut reader, read) = read.await.expect("reader joins");
        assert_eq!(
            read.expect_err("never a clean EOF").kind(),
            io::ErrorKind::ConnectionAborted,
        );
        let (mut writer, written) = write.await.expect("writer joins");
        assert_eq!(
            written.expect_err("the parked write fails").kind(),
            io::ErrorKind::ConnectionAborted,
        );
        let later_read = reader
            .read(&mut [0; 16])
            .await
            .expect_err("later reads fail");
        assert_eq!(later_read.kind(), io::ErrorKind::ConnectionAborted);
        for result in [
            writer.write(b"x").await.map(drop),
            writer.flush().await,
            writer.shutdown().await,
        ] {
            assert_eq!(
                result.expect_err("writes fail").kind(),
                io::ErrorKind::ConnectionAborted
            );
        }
        assert_eq!(client.stats().tunnels_aborted, 1);
        proxy
            .observations
            .wait("the cancelled session's connection to close", |observed| {
                observed.closed() == 1
            })
            .await;
        proxy
    });

    // A tunnel task that panics fails its tunnel the same way, and shutdown reports the panic.
    let panicking = ConnectOptions {
        bridge_capacity: 8 * 1024,
        max_connections: 1,
        max_streams: 1,
        ..options.clone()
    };
    run_owned(panicking, |runtime, client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(4096),
            ..Default::default()
        })
        .await;
        proxy.close_gate();
        let mut tunnel = proxy.connect_with(&client, "vm", "gated-sink").await;
        let chunk = pattern(4 * 1024);
        while let Ok(written) = timeout(Duration::from_millis(200), tunnel.write(&chunk)).await {
            written.expect("bulk write");
        }
        let waker = Waker::from(Arc::new(PanickingWaker));
        assert!(
            Pin::new(&mut tunnel)
                .poll_flush(&mut Context::from_waker(&waker))
                .is_pending(),
            "the native stream has not accepted the blocked upload"
        );
        proxy.open_gate();
        let read = tunnel.read(&mut [0; 16]).await;
        assert_eq!(
            read.expect_err("never a clean EOF").kind(),
            io::ErrorKind::ConnectionAborted
        );
        proxy
            .observations
            .wait("the panicked task's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        assert_eq!(
            proxy.observations.upload_eofs(),
            0,
            "the abandoned upload never ends"
        );
        // The reset freed the route's only stream slot.
        let mut sibling = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut sibling, b"the panicked task's stream slot is free").await;
        sibling.shutdown().await.expect("upload EOF");
        sibling
            .read_to_end(&mut Vec::new())
            .await
            .expect("download EOF");
        drop(sibling);
        let error = shutdown(runtime).await.expect_err("a tunnel task panicked");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Closed, true)
        );
        assert_eq!(client.stats().tunnels_aborted, 1);
        proxy
    });

    // An application waker that panics while a half-closed tunnel's drop is published runs only
    // after the stream's reset, so the stream's slot is free again.
    let half_closed = ConnectOptions {
        max_connections: 1,
        max_streams: 1,
        ..options.clone()
    };
    run_owned(half_closed, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let mut tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
        echo(&mut tunnel, b"upload before half-close").await;
        tunnel.shutdown().await.expect("upload half-close");
        proxy
            .observations
            .wait("the upload EOF", |observed| observed.upload_eofs() == 1)
            .await;
        let waker = Waker::from(Arc::new(PanickingWaker));
        assert!(
            Pin::new(&mut tunnel)
                .poll_flush(&mut Context::from_waker(&waker))
                .is_ready(),
            "a completed flush still registers its waker for the failure notification"
        );
        drop(tunnel);
        proxy
            .observations
            .wait("the half-closed stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let mut sibling = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut sibling, b"the half-closed stream's slot is free").await;
        sibling.shutdown().await.expect("upload EOF");
        sibling
            .read_to_end(&mut Vec::new())
            .await
            .expect("download EOF");
        drop(sibling);
        assert_eq!(proxy.connections(), 1);
        let error = shutdown(runtime)
            .await
            .expect_err("the failure notification panicked");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Closed, true)
        );
        assert_eq!(client.stats().tunnels_aborted, 1);
        proxy
    });

    // A panic that stops the worker fails every tunnel and every later connect.
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let mut tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
        let lost = client
            .connect("vm", request("echo"), Arc::new(PanicsWhenDropped))
            .await
            .expect_err("the worker panicked");
        assert_eq!(
            (lost.kind(), lost.committed()),
            (ConnectErrorKind::Closed, true),
            "the caller cannot tell whether the lost worker sent its CONNECT"
        );
        let read = tunnel.read(&mut [0; 16]).await;
        assert_eq!(
            read.expect_err("never a clean EOF").kind(),
            io::ErrorKind::ConnectionAborted
        );
        let later = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("the worker is gone");
        assert_eq!(
            (later.kind(), later.committed()),
            (ConnectErrorKind::Closed, false),
            "admission closed before the lost worker failed its tunnels"
        );
        proxy
            .observations
            .wait("the lost worker's connection to close", |observed| {
                observed.closed() == 1
            })
            .await;
        shutdown(runtime).await.expect_err("the worker panicked");
        proxy
    });
}

#[test]
fn shutdown_is_bounded_with_a_peer_that_never_ends_a_tunnel() {
    let shutdown_timeout = Duration::from_millis(500);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let mut readers = JoinSet::new();
        for _ in 0..3 {
            let mut tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
            tunnel
                .write_all(b"upload still open")
                .await
                .expect("upload");
            readers.spawn(async move {
                let mut sink = Vec::new();
                tunnel.read_to_end(&mut sink).await
            });
        }
        let pending = {
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
            .wait("the pending CONNECT", |observed| observed.requests() == 4)
            .await;
        let started = Instant::now();
        shutdown(runtime).await.expect("shutdown");
        let took = started.elapsed();
        assert!(
            took < shutdown_timeout + Duration::from_secs(2),
            "shutdown took {took:?}"
        );
        let pending = pending.await.expect("caller joins").expect_err("shut down");
        assert_eq!(pending.kind(), ConnectErrorKind::Closed);
        assert!(pending.committed(), "the proxy saw the CONNECT");
        while let Some(joined) = readers.join_next().await {
            joined
                .expect("reader joins")
                .expect_err("an aborted tunnel never reads EOF");
        }
        proxy
            .observations
            .wait("the cancelled session's connection to close", |observed| {
                observed.closed() == 1
            })
            .await;
        proxy
    });
}

#[test]
fn shutdown_is_bounded_when_the_peer_stops_reading() {
    let shutdown_timeout = Duration::from_millis(500);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some((1 << 31) - 1),
            initial_connection_window_size: Some((1 << 31) - 1),
            recv_buffer_size: Some(4096),
            ..Default::default()
        })
        .await;
        let connector = Arc::new(TestConnector::plain(proxy.address).send_buffer(4096));
        let mut uploads = JoinSet::new();
        let blocked = Arc::new(AtomicU64::new(0));
        for _ in 0..2 {
            let tunnel = client
                .connect("vm", request("sink"), connector.clone())
                .await
                .expect("CONNECT")
                .into_tunnel();
            let blocked = Arc::clone(&blocked);
            uploads.spawn(async move {
                let (_reader, mut writer) = tokio::io::split(tunnel);
                let chunk = pattern(64 * 1024);
                loop {
                    match timeout(Duration::from_millis(300), writer.write_all(&chunk)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => return,
                        Err(_) => {
                            blocked.fetch_add(1, Ordering::SeqCst);
                            let _ = writer.write_all(&chunk).await;
                            return;
                        }
                    }
                }
            });
        }
        proxy
            .observations
            .wait("upload traffic", |observed| observed.uploaded() > 0)
            .await;
        proxy.stop_reading();
        eventually(Duration::from_secs(20), "blocked uploads", || {
            blocked.load(Ordering::SeqCst) == 2
        })
        .await;
        let started = Instant::now();
        shutdown(runtime).await.expect("shutdown");
        let took = started.elapsed();
        assert!(
            took < shutdown_timeout + Duration::from_secs(2),
            "shutdown took {took:?}"
        );
        while let Some(joined) = uploads.join_next().await {
            joined.expect("upload task");
        }
        proxy
    });
}

#[test]
fn shutdown_lets_tunnels_finish_within_the_drain_window() {
    let shutdown_timeout = Duration::from_secs(2);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    run_owned(options.clone(), |runtime, client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let mut tunnel = client
            .connect("vm", request("late-response:200"), connector)
            .await
            .expect("CONNECT")
            .into_tunnel();
        tunnel.write_all(b"upload").await.expect("upload");
        tunnel.shutdown().await.expect("upload EOF");
        let started = Instant::now();
        let shutting_down = tokio::spawn(shutdown(runtime));
        let mut received = Vec::new();
        tunnel
            .read_to_end(&mut received)
            .await
            .expect("the tunnel ends cleanly while the runtime drains");
        let mut expected = b"upload".to_vec();
        expected.extend_from_slice(AFTER_HALF_CLOSE);
        assert_eq!(received, expected);
        shutting_down
            .await
            .expect("shutdown task")
            .expect("shutdown");
        let took = started.elapsed();
        assert!(
            took < shutdown_timeout / 2,
            "shutdown returned once the tunnel ended: {took:?}"
        );
        assert_eq!(proxy.observations.upload_eofs(), 1);
        assert_eq!(
            written_resets(&tap.written()[0]),
            [],
            "the adapter reset nothing"
        );
        let stats = client.stats();
        assert_eq!(
            (
                stats.tunnels_aborted,
                stats.tunnels_reset_by_peer,
                stats.shutdown_errors
            ),
            (0, 0, 0)
        );
        drop(tunnel);
        proxy
    });

    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let mut tunnel = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut tunnel, b"before idling").await;
        tunnel.shutdown().await.expect("upload EOF");
        tunnel
            .read_to_end(&mut Vec::new())
            .await
            .expect("download EOF");
        drop(tunnel);
        eventually(Duration::from_secs(5), "an idle runtime", || {
            client.stats().active_tunnels == 0
        })
        .await;
        let started = Instant::now();
        shutdown(runtime).await.expect("shutdown");
        let took = started.elapsed();
        assert!(
            took < shutdown_timeout / 4,
            "an idle runtime does not wait out shutdown_timeout: {took:?}"
        );
        assert_eq!(client.stats().shutdown_errors, 0, "a clean shutdown");
        proxy
            .observations
            .wait("the idle connection to close", |observed| {
                observed.closed() == 1
            })
            .await;
        proxy
    });
}

#[test]
fn shutdown_fails_queued_connects_and_in_progress_dials() {
    run_owned(ConnectOptions::default(), |runtime, client| async move {
        let proxy = Proxy::start().await;
        let connector = Arc::new(TestConnector::plain(proxy.address).stalling(1));
        let mut queued = JoinSet::new();
        for _ in 0..3 {
            let client = client.clone();
            let connector = connector.clone();
            queued.spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            });
        }
        eventually(Duration::from_secs(5), "three waiters", || {
            client.stats().waiters == 3
        })
        .await;
        shutdown(runtime).await.expect("shutdown");
        while let Some(joined) = queued.join_next().await {
            let error = joined.expect("caller joins").expect_err("shut down");
            assert_eq!(error.kind(), ConnectErrorKind::Closed);
            assert!(!error.committed(), "queued connects sent nothing");
        }
        assert_eq!(
            connector.dropped.count(),
            1,
            "the in-progress dial was dropped"
        );
        assert_eq!(client.stats().waiters, 0);
        proxy
    });
}

/// A connector that owns the runtime it dials for, so retiring its route drops the runtime on
/// the protocol worker's own thread.
struct OwningConnector {
    dial: Arc<TestConnector>,
    _runtime: Mutex<ConnectRuntime>,
}

impl Connector for OwningConnector {
    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>> {
        self.dial.connect()
    }

    fn security(&self) -> TransportSecurity {
        self.dial.security()
    }
}

#[test]
fn shutdown_closes_idle_and_active_connections_and_joins() {
    let options = ConnectOptions {
        max_tunnels: 1,
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run_owned(options.clone(), |runtime, client| async move {
        let proxy = Proxy::start().await;
        let idle = proxy.connect_with(&client, "idle-vm", "hold-open").await;
        drop(idle);
        proxy
            .observations
            .wait("the idle route's reset", |observed| observed.resets() == 1)
            .await;
        let mut active = proxy.connect_with(&client, "active-vm", "echo").await;
        echo(&mut active, b"active before shutdown").await;
        shutdown(runtime).await.expect("CONNECT runtime joins");
        proxy
            .observations
            .wait("both connections closed", |observed| observed.closed() == 2)
            .await;
        assert!(
            active.read(&mut [0; 1]).await.is_err(),
            "shutdown fails an active tunnel"
        );
        let after = client
            .connect("closed", request("echo"), proxy.connector())
            .await
            .expect_err("closed");
        assert_eq!(after.kind(), ConnectErrorKind::Closed);
        drop(active);
        proxy
    });

    // Dropping the runtime runs the same shutdown.
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let mut active = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut active, b"active before the drop").await;
        tokio::task::spawn_blocking(move || drop(runtime))
            .await
            .expect("the drop joins");
        let read = active.read(&mut [0; 1]).await;
        assert_eq!(
            read.expect_err("the drop fails an active tunnel").kind(),
            io::ErrorKind::ConnectionAborted
        );
        let after = client
            .connect("closed", request("echo"), proxy.connector())
            .await
            .expect_err("closed");
        assert_eq!(
            (after.kind(), after.committed()),
            (ConnectErrorKind::Closed, false)
        );
        proxy
            .observations
            .wait("the connection to close", |observed| observed.closed() == 1)
            .await;
        drop(active);
        proxy
    });

    // On the worker's own thread, the last owner's drop starts the shutdown without a join.
    let retiring = ConnectOptions {
        idle_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    run_owned(retiring, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let connector = Arc::new(OwningConnector {
            dial: proxy.connector(),
            _runtime: Mutex::new(runtime),
        });
        let mut tunnel = client
            .connect("vm", request("echo"), connector)
            .await
            .expect("CONNECT")
            .into_tunnel();
        echo(&mut tunnel, b"before the route retires").await;
        drop(tunnel);
        proxy
            .observations
            .wait("the retired route's connection to close", |observed| {
                observed.closed() == 1
            })
            .await;
        let after = timeout(Duration::from_secs(5), async {
            loop {
                match client
                    .connect("vm", request("echo"), proxy.connector())
                    .await
                {
                    Err(error) => return error,
                    Ok(tunnel) => drop(tunnel.into_tunnel()),
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the worker does not wait for itself");
        assert_eq!(after.kind(), ConnectErrorKind::Closed);
        proxy
    });
}

#[test]
fn historical_session_errors_are_counted_not_returned_by_shutdown() {
    run_owned(ConnectOptions::default(), |runtime, client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            reset_on_close: true,
            ..Default::default()
        })
        .await;
        let mut tunnel = proxy.connect_with(&client, "vm", "hold-open").await;
        proxy.close_connections();
        tunnel
            .read(&mut [0; 1])
            .await
            .expect_err("the session failed");
        drop(tunnel);
        shutdown(runtime)
            .await
            .expect("a session failure during the runtime's life is not a shutdown failure");
        assert_eq!(client.stats().shutdown_errors, 1);
        proxy
    });
}

#[test]
fn busy_tunnel_does_not_starve_a_sibling() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let bulk = proxy.connect_with(&client, "vm", "echo").await;
        let mut sibling = proxy.connect_with(&client, "vm", "echo").await;
        let uploaded = Arc::new(AtomicU64::new(0));
        let downloaded = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (mut reader, mut writer) = tokio::io::split(bulk);
        let upload = {
            let uploaded = Arc::clone(&uploaded);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let chunk = pattern(16 * 1024);
                while !stop.load(Ordering::SeqCst) {
                    if writer.write_all(&chunk).await.is_err() {
                        break;
                    }
                    uploaded.fetch_add(chunk.len() as u64, Ordering::SeqCst);
                }
                writer
            })
        };
        let download = {
            let downloaded = Arc::clone(&downloaded);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut buffer = vec![0; 64 * 1024];
                while !stop.load(Ordering::SeqCst) {
                    match reader.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => downloaded.fetch_add(read as u64, Ordering::SeqCst),
                    };
                }
                reader
            })
        };
        eventually(Duration::from_secs(5), "bulk traffic", || {
            downloaded.load(Ordering::SeqCst) > 0
        })
        .await;
        let before = (
            uploaded.load(Ordering::SeqCst),
            downloaded.load(Ordering::SeqCst),
        );
        let started = Instant::now();
        let message = pattern(1024);
        for _ in 0..100 {
            echo(&mut sibling, &message).await;
        }
        let took = started.elapsed();
        let after = (
            uploaded.load(Ordering::SeqCst),
            downloaded.load(Ordering::SeqCst),
        );
        assert!(
            took < Duration::from_secs(10),
            "100 round trips took {took:?}"
        );
        assert!(
            after.0 > before.0 && after.1 > before.1,
            "bulk kept moving: {before:?} -> {after:?}"
        );
        stop.store(true, Ordering::SeqCst);
        drop(sibling);
        upload.abort();
        download.abort();
        proxy
    });
}

#[test]
fn tunnel_tasks_are_joined_before_shutdown_returns() {
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let mut tunnels = Vec::new();
        for _ in 0..3 {
            tunnels.push(proxy.connect_with(&client, "vm", "hold-open").await);
        }
        assert_eq!(client.stats().active_tunnels, 3);
        tunnels.pop();
        eventually(Duration::from_secs(5), "an ended tunnel task", || {
            client.stats().active_tunnels == 2
        })
        .await;
        let mut readers = JoinSet::new();
        for mut tunnel in tunnels {
            readers.spawn(async move {
                let mut sink = Vec::new();
                let _ = tunnel.read_to_end(&mut sink).await;
            });
        }
        shutdown(runtime).await.expect("shutdown");
        assert_eq!(
            client.stats().active_tunnels,
            0,
            "every tunnel task ended before shutdown returned"
        );
        timeout(Duration::from_secs(1), async {
            while let Some(joined) = readers.join_next().await {
                joined.expect("reader joins");
            }
        })
        .await
        .expect("every pending read resolved");
        proxy
    });
}

/// A TLS connector whose `security()` holds the protocol worker for a millisecond, so each of its
/// connects keeps the worker busy.
struct SlowTls;

impl Connector for SlowTls {
    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>> {
        Box::pin(pending())
    }

    fn security(&self) -> TransportSecurity {
        thread::sleep(Duration::from_millis(1));
        TransportSecurity::Tls
    }
}

#[test]
fn idle_route_is_evicted_and_recreated() {
    let idle_timeout = Duration::from_millis(200);
    let options = ConnectOptions {
        idle_timeout,
        ..Default::default()
    };
    run(options.clone(), |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let tunnel = client
            .connect("vm", request("echo"), connector.clone())
            .await
            .expect("CONNECT")
            .into_tunnel();
        assert_eq!(client.stats().routes, 1);
        drop(tunnel);
        let idle = Instant::now();
        eventually(Duration::from_secs(5), "retirement", || {
            client.stats().session_drains == 1
        })
        .await;
        let retired = idle.elapsed();
        // The maintenance tick is a quarter of idle_timeout.
        assert!(
            retired >= idle_timeout - Duration::from_millis(20)
                && retired < idle_timeout + idle_timeout / 4 + Duration::from_millis(150),
            "retired {retired:?} after going idle"
        );
        eventually(Duration::from_secs(5), "eviction", || {
            client.stats().routes == 0
        })
        .await;
        let mut recreated = client
            .connect("vm", request("echo"), connector.clone())
            .await
            .expect("new route")
            .into_tunnel();
        echo(&mut recreated, b"recreated").await;
        assert!(!recreated.info().reused_session);
        assert_eq!(connector.attempts(), 2, "the new route dials anew");
        assert_eq!(proxy.connections(), 2);
        drop(recreated);
        proxy
    });

    // Callers that keep a connect queued at all times do not starve the maintenance tick.
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        drop(proxy.connect_with(&client, "idle", "echo").await);
        let busy = proxy.connect_with(&client, "busy", "hold-open").await;
        let flooding = Arc::new(AtomicBool::new(true));
        let mut flood = JoinSet::new();
        for _ in 0..64 {
            let client = client.clone();
            let flooding = Arc::clone(&flooding);
            flood.spawn(async move {
                while flooding.load(Ordering::SeqCst) {
                    let error = client
                        .connect("busy", request("echo"), Arc::new(SlowTls))
                        .await
                        .expect_err("the plaintext route refuses a TLS connector");
                    assert_eq!(error.kind(), ConnectErrorKind::InvalidRequest);
                }
            });
        }
        eventually(
            Duration::from_secs(10),
            "the idle route's retirement",
            || client.stats().session_drains == 1,
        )
        .await;
        flooding.store(false, Ordering::SeqCst);
        while let Some(joined) = flood.join_next().await {
            joined.expect("flood task");
        }
        assert!(client.stats().invalid_request > 0);
        drop(busy);
        proxy
    });
}

#[test]
fn route_with_tunnels_or_waiters_is_never_evicted() {
    let options = ConnectOptions {
        idle_timeout: Duration::from_millis(100),
        max_connections: 1,
        max_streams: 1,
        acquire_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut held = proxy.connect_with(&client, "vm", "hold-open").await;
        let waiter = {
            let client = client.clone();
            let connector = proxy.connector();
            tokio::spawn(async move {
                client
                    .connect("vm", request("echo"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            })
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            client.stats().routes,
            1,
            "a tunnel and a waiter keep the route"
        );
        echo(&mut held, b"still the first route").await;
        drop(held);
        let mut served = waiter.await.expect("caller joins").expect("served");
        echo(&mut served, b"served on the same route").await;
        assert_eq!(proxy.connections(), 1);
        drop(served);
        eventually(Duration::from_secs(5), "eviction of the idle route", || {
            client.stats().routes == 0
        })
        .await;

        let slow =
            Arc::new(TestConnector::plain(proxy.address).delayed(Duration::from_millis(600)));
        let abandoned = timeout(
            Duration::from_millis(50),
            client.connect("dialing", request("echo"), slow.clone()),
        )
        .await;
        assert!(
            abandoned.is_err(),
            "the caller gives up while the dial continues"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(client.stats().routes, 1, "a pending dial keeps its route");
        eventually(Duration::from_secs(5), "the dialed session", || {
            client.stats().sessions_opened == 2
        })
        .await;
        eventually(
            Duration::from_secs(5),
            "eviction once the dial completed",
            || client.stats().routes == 0,
        )
        .await;
        assert_eq!(slow.attempts(), 1);
        proxy
    });
}

#[test]
fn retiring_routes_count_against_max_routes() {
    let options = ConnectOptions {
        max_routes: 1,
        idle_timeout: Duration::from_millis(100),
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some((1 << 31) - 1),
            initial_connection_window_size: Some((1 << 31) - 1),
            recv_buffer_size: Some(4096),
            ..Default::default()
        })
        .await;
        let connector = Arc::new(TestConnector::plain(proxy.address).send_buffer(4096));
        let tunnel = client
            .connect("first", request("sink"), connector.clone())
            .await
            .expect("CONNECT")
            .into_tunnel();
        let (reader, mut writer) = tokio::io::split(tunnel);
        proxy
            .observations
            .wait("upload traffic", |observed| {
                observed.uploaded() > 0 || observed.requests() == 1
            })
            .await;
        proxy.stop_reading();
        let chunk = pattern(64 * 1024);
        while timeout(Duration::from_millis(300), writer.write_all(&chunk))
            .await
            .is_ok_and(|written| written.is_ok())
        {}
        drop((reader, writer));
        eventually(Duration::from_secs(5), "the route's retirement", || {
            client.stats().session_drains == 1
        })
        .await;
        let stats = client.stats();
        assert_eq!(
            (stats.routes, stats.active_tunnels, stats.waiters),
            (1, 0, 0),
            "the idle route retired but its session cannot close"
        );
        let error = client
            .connect("second", request("echo"), connector.clone())
            .await
            .expect_err("the retiring route holds the only route slot");
        assert_eq!(error.kind(), ConnectErrorKind::RouteCapacity);
        proxy
    });
}

#[test]
fn max_age_retires_a_session_without_disturbing_its_tunnels() {
    // One connection, so the fresh session needs the slot the aged session frees.
    let options = ConnectOptions {
        max_age: Duration::from_millis(300),
        max_connections: 1,
        acquire_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut aged = proxy.connect_with(&client, "vm", "half-close").await;
        echo(&mut aged, b"before expiry").await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut fresh = proxy.connect_with(&client, "vm", "echo").await;
        assert!(
            !fresh.info().reused_session,
            "the aged session served no new stream"
        );
        assert_eq!(proxy.connections(), 2);
        echo(&mut fresh, b"fresh").await;
        echo(&mut aged, b"the aged session's tunnel keeps transferring").await;
        aged.shutdown().await.expect("upload EOF");
        let mut rest = Vec::new();
        aged.read_to_end(&mut rest).await.expect("clean end");
        assert_eq!(rest, AFTER_HALF_CLOSE);
        assert_eq!(client.stats().session_drains, 1);
        drop((aged, fresh));
        proxy
    });
}
