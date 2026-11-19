/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tunnel data path, half-close, resets, response metadata, key isolation, and HPACK policy.

use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use roxygen_connect_client::error::ConnectErrorKind;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::tunnel::PendingTunnel;
use roxygen_connect_client::tunnel::Tunnel;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::proxy::AFTER_HALF_CLOSE;
use crate::proxy::BEFORE_RESET;
use crate::proxy::INFORMATIONAL_DELAY;
use crate::proxy::Proxy;
use crate::proxy::ProxyConfig;
use crate::proxy::RawProxy;
use crate::proxy::RawScript;
use crate::proxy::Tap;
use crate::proxy::TestConnector;
use crate::proxy::decode_header_blocks;
use crate::proxy::echo;
use crate::proxy::eventually;
use crate::proxy::pattern;
use crate::proxy::request;
use crate::proxy::request_with;
use crate::proxy::run;
use crate::proxy::tls_identity;
use crate::proxy::written_resets;

#[test]
fn concurrent_tunnels_share_one_connection() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let mut requests = JoinSet::new();
        for index in 0..8 {
            let client = client.clone();
            let connector = connector.clone();
            requests.spawn(async move {
                let id = format!("tunnel-{index}");
                let tunnel = client
                    .connect("vm", request_with("echo", &id), connector)
                    .await
                    .map(PendingTunnel::into_tunnel);
                (id, tunnel)
            });
        }
        let mut tunnels = Vec::new();
        while let Some(joined) = requests.join_next().await {
            let (id, tunnel) = joined.expect("application task joins");
            tunnels.push((id, tunnel.expect("CONNECT")));
        }
        assert_eq!(
            proxy.connections(),
            1,
            "concurrent connects share one connection"
        );
        assert_eq!(connector.attempts(), 1);
        assert_eq!(proxy.observations.requests(), 8, "one stream each");
        assert_eq!(client.stats().retries, 0);
        for (id, tunnel) in &mut tunnels {
            assert_eq!(tunnel.response().status(), StatusCode::OK);
            assert_eq!(tunnel.response().headers()["x-test-id"], id.as_str());
            assert_eq!(
                tunnel.response().headers()["x-test-authorization"],
                format!("Bearer token-{id}").as_str(),
                "each CONNECT carries only its own credential",
            );
            echo(tunnel, id.as_bytes()).await;
        }
        drop(tunnels);
        let mut reused = proxy.connect_with(&client, "vm", "echo").await;
        assert!(reused.info().reused_session);
        echo(&mut reused, b"reused after prior tunnels closed").await;
        assert_eq!(proxy.connections(), 1);
        assert_eq!(
            client.stats().streams_on_reused_session,
            8,
            "seven siblings and the later tunnel"
        );
        drop(reused);
        proxy
    });
}

#[test]
fn route_keys_isolate_connections() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let mut first = client
            .connect("first-vm", request("echo"), connector.clone())
            .await
            .expect("first route")
            .into_tunnel();
        let mut second = client
            .connect("second-vm", request("echo"), connector.clone())
            .await
            .expect("second route")
            .into_tunnel();
        echo(&mut first, b"first").await;
        echo(&mut second, b"second").await;
        assert_eq!(
            proxy.connections(),
            2,
            "distinct keys never share a connection"
        );
        assert!(!second.info().reused_session);
        drop((first, second));
        proxy
    });
}

#[test]
fn a_key_keeps_its_first_connector_until_the_route_retires() {
    let options = ConnectOptions {
        idle_timeout: Duration::from_millis(200),
        max_tunnels: 3,
        ..Default::default()
    };
    run(options, |client| async move {
        let first = Proxy::start().await;
        let second = Proxy::start().await;
        let mut tunnel = client
            .connect("vm", request("echo"), first.connector())
            .await
            .expect("first connector")
            .into_tunnel();
        let mut ignored = client
            .connect("vm", request("echo"), second.connector())
            .await
            .expect("live route")
            .into_tunnel();
        echo(&mut tunnel, b"first").await;
        echo(&mut ignored, b"still the first proxy").await;
        assert_eq!(first.connections(), 1);
        assert_eq!(
            second.connections(),
            0,
            "a live key ignores later connectors"
        );

        // Proxy-Authorization meant for TLS never goes out on the key's plaintext session.
        let (_, client_tls) = tls_identity(&[b"h2"]);
        let tls = Arc::new(TestConnector::plain(first.address).tls(client_tls));
        let mismatch = client
            .connect("vm", request("echo"), tls.clone())
            .await
            .expect_err("the route's security differs from the connector's");
        assert_eq!(
            (mismatch.kind(), mismatch.committed()),
            (ConnectErrorKind::InvalidRequest, false)
        );
        assert_eq!(tls.attempts(), 0, "the mismatched connector never dials");
        let mut third = client
            .connect("vm", request("echo"), first.connector())
            .await
            .expect("the mismatch returned its admission permit")
            .into_tunnel();
        echo(&mut third, b"the route still serves its own security").await;
        assert_eq!(
            first.observations.requests(),
            3,
            "the mismatched CONNECT never reached the wire"
        );
        assert_eq!(client.stats().invalid_request, 1);
        drop((tunnel, ignored, third));
        eventually(Duration::from_secs(5), "route retirement", || {
            client.stats().routes == 0
        })
        .await;
        let mut replaced = client
            .connect("vm", request("echo"), second.connector())
            .await
            .expect("new route")
            .into_tunnel();
        echo(&mut replaced, b"second").await;
        assert_eq!(
            second.connections(),
            1,
            "a new route uses its first connector"
        );
        drop(replaced);
        vec![first, second]
    });
}

#[test]
fn payload_larger_than_windows_crosses_bounded_bridge() {
    let options = ConnectOptions {
        bridge_capacity: 1024,
        stream_receive_window: 64 * 1024,
        connection_receive_window: 128 * 1024,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let tunnel = proxy.connect_with(&client, "vm", "echo").await;
        let payload = pattern(1024 * 1024);
        let (mut reader, mut writer) = tokio::io::split(tunnel);
        let upload = async {
            writer.write_all(&payload).await.expect("large upload");
            writer.shutdown().await.expect("upload half-close");
        };
        let download = async {
            let mut received = Vec::new();
            reader.read_to_end(&mut received).await.expect("large echo");
            assert!(received == payload, "the echo matches the upload");
        };
        tokio::join!(upload, download);
        drop((reader, writer));
        proxy
    });
}

#[test]
fn upload_half_close_preserves_response_direction() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let mut tunnel = client
            .connect("vm", request("half-close"), connector.clone())
            .await
            .expect("CONNECT")
            .into_tunnel();
        tunnel.write_all(b"upload").await.expect("upload");
        tunnel.shutdown().await.expect("upload EOF");
        let mut received = Vec::new();
        tunnel
            .read_to_end(&mut received)
            .await
            .expect("response survives upload EOF");
        let mut expected = b"upload".to_vec();
        expected.extend_from_slice(AFTER_HALF_CLOSE);
        assert_eq!(received, expected);
        drop(tunnel);
        eventually(Duration::from_secs(5), "the ended tunnel task", || {
            client.stats().active_tunnels == 0
        })
        .await;
        // A later stream's round trip on the connection flushes anything the drop queued.
        let mut sibling = client
            .connect("vm", request("echo"), connector)
            .await
            .expect("sibling")
            .into_tunnel();
        echo(&mut sibling, b"after the drop").await;
        assert_eq!(
            written_resets(&tap.written()[0]),
            [],
            "a wire-complete tunnel drops without an RST_STREAM"
        );
        assert_eq!(client.stats().tunnels_aborted, 0);
        drop(sibling);
        proxy
    });
}

#[test]
fn dropping_one_tunnel_preserves_its_sibling() {
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut abandoned = client
            .connect("vm", request_with("record", "abandoned"), proxy.connector())
            .await
            .expect("recorded tunnel")
            .into_tunnel();
        let mut sibling = proxy.connect_with(&client, "vm", "echo").await;
        let accepted = pattern(256 * 1024);
        abandoned
            .write_all(&accepted)
            .await
            .expect("writes are accepted into the bridge");
        drop(abandoned);
        proxy
            .observations
            .wait("the abandoned stream's reset", |observed| {
                observed.record("abandoned").is_some()
            })
            .await;
        let record = proxy.observations.record("abandoned").expect("record");
        assert!(!record.end_stream, "an abortive close sends no END_STREAM");
        assert_eq!(record.reset, Some(h2::Reason::CANCEL));
        assert!(
            accepted.starts_with(&record.bytes),
            "the proxy saw a prefix of the accepted bytes ({} of {})",
            record.bytes.len(),
            accepted.len(),
        );
        echo(&mut sibling, b"sibling remains writable and readable").await;
        let mut third = proxy.connect_with(&client, "vm", "echo").await;
        drop(client);
        // Longer than shutdown_timeout: the last handle's drop must not start a shutdown.
        tokio::time::sleep(Duration::from_millis(500)).await;
        echo(&mut third, b"tunnel survives the last client handle").await;
        echo(&mut sibling, b"the runtime still owns both tunnels").await;
        assert_eq!(proxy.connections(), 1);
        drop((sibling, third));
        proxy
    });
}

#[test]
fn peer_reset_is_an_io_error_not_eof() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let mut tunnel = proxy.connect_with(&client, "vm", "reset").await;
        tunnel
            .write_all(b"trigger reset")
            .await
            .expect("reset trigger");
        let error = tunnel
            .read(&mut [0_u8; 1])
            .await
            .expect_err("RST_STREAM must not become clean EOF");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        let later = tunnel
            .read(&mut [0_u8; 1])
            .await
            .expect_err("later reads fail too");
        assert_eq!(later.kind(), io::ErrorKind::ConnectionReset);
        let write = tunnel
            .write_all(b"after reset")
            .await
            .expect_err("writes fail");
        assert_eq!(write.kind(), io::ErrorKind::ConnectionReset);
        drop(tunnel);
        let mut sibling = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut sibling, b"session survives stream reset").await;
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().tunnels_reset_by_peer, 1);
        drop(sibling);

        // DATA and the RST_STREAM in one flight: the DATA may be discarded, never read as EOF.
        let raw = RawProxy::start(RawScript::DataThenReset).await;
        let mut tunnel = client
            .connect("raw", request("data-then-reset"), raw.connector())
            .await
            .expect("CONNECT")
            .into_tunnel();
        tunnel.write_all(b"trigger").await.expect("reset trigger");
        let received = read_to_reset(&mut tunnel).await;
        assert!(
            pattern(BEFORE_RESET).starts_with(&received),
            "the application read a prefix of the DATA before the reset"
        );
        assert_eq!(client.stats().tunnels_reset_by_peer, 2);
        drop(tunnel);
        raw.shutdown().await;

        // DATA already in the bridge when the reset fails the tunnel is read before the error.
        let late = RawProxy::start(RawScript::DataThenLateReset).await;
        let mut tunnel = client
            .connect("late", request("data-then-reset"), late.connector())
            .await
            .expect("CONNECT")
            .into_tunnel();
        tunnel.write_all(b"trigger").await.expect("reset trigger");
        eventually(Duration::from_secs(5), "the counted reset", || {
            client.stats().tunnels_reset_by_peer == 3
        })
        .await;
        assert_eq!(read_to_reset(&mut tunnel).await, pattern(BEFORE_RESET));
        drop(tunnel);
        late.shutdown().await;
        proxy
    });
}

/// Reads until the tunnel fails, which must be `ConnectionReset` on that and later reads,
/// never EOF; returns the bytes read before it.
async fn read_to_reset(tunnel: &mut Tunnel) -> Vec<u8> {
    let mut received = Vec::new();
    let error = loop {
        let mut buffer = [0; 1024];
        match tunnel.read(&mut buffer).await {
            Ok(0) => panic!("a reset read as EOF after {} bytes", received.len()),
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(error) => break error,
        }
    };
    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    let later = tunnel
        .read(&mut [0_u8; 1])
        .await
        .expect_err("later reads fail too");
    assert_eq!(later.kind(), io::ErrorKind::ConnectionReset);
    received
}

#[test]
fn slow_reader_backpressures_only_its_own_stream() {
    const FLOOD: usize = 4 * 1024 * 1024;
    const WINDOW: u32 = 256 * 1024;
    const BRIDGE: usize = 16 * 1024;
    let options = ConnectOptions {
        bridge_capacity: BRIDGE,
        stream_receive_window: WINDOW,
        connection_receive_window: 8 * 1024 * 1024,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let mut slow = proxy
            .connect_with(&client, "vm", &format!("flood:{FLOOD}"))
            .await;
        let mut sibling = proxy.connect_with(&client, "vm", "echo").await;
        for round in 0..20 {
            echo(&mut sibling, format!("round {round}").as_bytes()).await;
        }
        // The stream window plus the bridge, the bridge's 16 KiB copy buffer, and slack.
        let held = WINDOW as u64 + 2 * BRIDGE as u64 + 64 * 1024;
        assert!(
            proxy.observations.downloaded() <= held,
            "the unread tunnel holds the flood back with its own window: {} sent",
            proxy.observations.downloaded(),
        );
        let mut received = vec![0; FLOOD];
        slow.read_exact(&mut received)
            .await
            .expect("credit returns as the reader consumes");
        assert!(received == pattern(FLOOD), "the flood arrives intact");
        echo(&mut sibling, b"after the flood").await;
        assert_eq!(proxy.connections(), 1);
        drop((slow, sibling));
        proxy
    });
}

#[test]
fn flush_completes_when_the_native_stream_accepts_bytes() {
    let options = ConnectOptions {
        bridge_capacity: 8 * 1024,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(4096),
            ..Default::default()
        })
        .await;
        proxy.close_gate();
        let mut tunnel = proxy.connect_with(&client, "vm", "gated-sink").await;
        tunnel.write_all(b"small").await.expect("small write");
        timeout(Duration::from_secs(5), tunnel.flush())
            .await
            .expect("bytes the native stream accepts flush before the peer reads them")
            .expect("flush");
        assert_eq!(
            proxy.observations.uploaded(),
            0,
            "the peer has read nothing yet"
        );
        // Fill the bridge until writes block behind the peer's closed 4 KiB window.
        let chunk = pattern(4 * 1024);
        let mut written = 0;
        while let Ok(accepted) = timeout(Duration::from_millis(200), tunnel.write(&chunk)).await {
            written += accepted.expect("bulk write");
        }
        assert!(written > 0);
        assert!(
            timeout(Duration::from_millis(500), tunnel.flush())
                .await
                .is_err(),
            "bytes the native stream has not accepted do not flush"
        );
        proxy.open_gate();
        timeout(Duration::from_secs(20), tunnel.flush())
            .await
            .expect("flush completes once the peer opens its window")
            .expect("flush");
        assert!(proxy.observations.uploaded() > 0);
        drop(tunnel);
        proxy
    });
}

#[test]
fn dropped_tunnels_with_unread_data_return_connection_credit() {
    const FLOOD: usize = 128 * 1024;
    let options = ConnectOptions {
        bridge_capacity: 1024,
        stream_receive_window: FLOOD as u32,
        connection_receive_window: 2 * FLOOD as u32,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        for round in 1..=6_u64 {
            let flooded = proxy
                .connect_with(&client, "vm", &format!("flood:{FLOOD}"))
                .await;
            proxy
                .observations
                .wait("a delivered flood", |observed| {
                    observed.downloaded() >= round * FLOOD as u64
                })
                .await;
            drop(flooded);
        }
        let mut last = proxy
            .connect_with(&client, "vm", &format!("flood:{}", 4 * FLOOD))
            .await;
        let mut received = vec![0; 4 * FLOOD];
        last.read_exact(&mut received)
            .await
            .expect("the connection window recovered the unread bytes");
        assert_eq!(proxy.connections(), 1);
        drop(last);
        proxy
    });
}

#[test]
fn established_tunnel_exposes_the_2xx_head_and_info() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let mut first = client
            .connect("vm", request_with("echo", "first"), proxy.connector())
            .await
            .expect("first tunnel")
            .into_tunnel();
        assert_eq!(first.response().status(), StatusCode::OK);
        assert_eq!(first.response().version(), http::Version::HTTP_2);
        assert_eq!(first.response().headers()["x-test-proxy"], "independent-h2");
        assert_eq!(first.response().headers()["x-test-id"], "first");
        let info = first.info();
        assert!(!info.reused_session);
        assert_eq!(info.retries, 0);
        assert!(info.acquire < Duration::from_secs(5));
        assert!(info.response_head < Duration::from_secs(5));
        echo(&mut first, b"first").await;
        let second = proxy.connect_with(&client, "vm", "echo").await;
        assert!(second.info().reused_session);
        drop((first, second));

        let informational = RawProxy::start(RawScript::InformationalThenOk).await;
        let mut tunnel = client
            .connect("raw", request("informational"), informational.connector())
            .await
            .expect("the final 2xx establishes the tunnel")
            .into_tunnel();
        assert_eq!(tunnel.response().status(), StatusCode::OK);
        assert!(
            !tunnel.response().headers().contains_key("x-informational"),
            "the 103 head is skipped"
        );
        assert!(
            tunnel.info().response_head >= INFORMATIONAL_DELAY,
            "head latency runs to the final head"
        );
        echo(&mut tunnel, b"after an informational head").await;
        drop(tunnel);
        informational.shutdown().await;
        proxy
    });
}

#[test]
fn rejected_connect_preserves_response_metadata() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let error = client
            .connect("vm", request_with("reject", "denied"), proxy.connector())
            .await
            .expect_err("403 must be reported as a rejected CONNECT");
        assert_eq!(error.kind(), ConnectErrorKind::Rejected);
        assert!(error.committed());
        let response = error.response().expect("rejected head");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()["x-test-proxy"], "independent-h2");
        assert_eq!(response.headers()["x-test-id"], "denied");
        let native = error.source().expect("the native rejection is the source");
        assert_eq!(
            native.to_string(),
            "CONNECT to target.example:443 failed status=403"
        );
        assert_eq!(
            native.source().map(ToString::to_string).as_deref(),
            Some("CONNECT to target.example:443 failed status=403"),
            "the native rejection keeps its typed cause"
        );
        assert!(
            !format!("{error:?}").contains("denied"),
            "Debug still redacts header values"
        );
        let mut recovered = proxy.connect_with(&client, "vm", "echo").await;
        echo(&mut recovered, b"rejection did not drain the session").await;
        assert!(recovered.info().reused_session);
        assert_eq!(proxy.connections(), 1);
        drop(recovered);

        let informational = RawProxy::start(RawScript::InformationalThenReject).await;
        let error = client
            .connect("raw", request("informational"), informational.connector())
            .await
            .expect_err("a 403 after a 103 is a rejection");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Rejected, true)
        );
        let response = error.response().expect("rejected head");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()["x-test-proxy"], "raw");
        assert!(!response.headers().contains_key("x-informational"));
        informational.shutdown().await;
        proxy
    });
}

#[test]
fn goaway_after_a_rejection_moves_later_connects_to_a_new_connection() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = RawProxy::start(RawScript::RejectThenGoaway).await;
        let connector = proxy.connector();
        let rejected = {
            let client = client.clone();
            let connector = connector.clone();
            tokio::spawn(async move {
                client
                    .connect("vm", request("first"), connector)
                    .await
                    .map(PendingTunnel::into_tunnel)
            })
        };
        proxy
            .observations
            .wait("the first CONNECT", |observed| observed.requests() >= 1)
            .await;
        let mut concurrent = client
            .connect("vm", request("second"), connector.clone())
            .await
            .expect("the refused CONNECT is retried on a new connection")
            .into_tunnel();
        let error = rejected
            .await
            .expect("caller joins")
            .expect_err("403 is a rejection");
        assert_eq!(error.kind(), ConnectErrorKind::Rejected);
        assert_eq!(
            error.response().expect("head").headers()["x-test-proxy"],
            "raw"
        );
        assert_eq!(concurrent.info().retries, 1);
        assert_eq!(proxy.observations.connections(), 2);
        echo(&mut concurrent, b"on the new connection").await;
        let mut next = client
            .connect("vm", request("third"), connector)
            .await
            .expect("next connect")
            .into_tunnel();
        echo(&mut next, b"next").await;
        assert_eq!(
            proxy.observations.connections(),
            2,
            "the draining connection is not reused"
        );
        assert_eq!(
            proxy.observations.streams(),
            [(0, 1), (0, 3), (1, 1), (1, 3)],
            "connection 0 saw only the rejected and the refused CONNECT"
        );
        drop((concurrent, next));
        proxy.shutdown().await;
    });
}

#[test]
fn unconvertible_2xx_head_resets_the_stream() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = RawProxy::start(RawScript::UnconvertibleHead).await;
        let error = client
            .connect("vm", request("bad-head"), proxy.connector())
            .await
            .expect_err("an unrepresentable 2xx head never yields a tunnel");
        assert_eq!(error.kind(), ConnectErrorKind::Failed);
        assert!(error.committed());
        proxy
            .observations
            .wait("the reset", |observed| !observed.resets().is_empty())
            .await;
        assert_eq!(
            proxy.observations.resets(),
            [(0, 1, 8)],
            "one RST_STREAM CANCEL"
        );
        proxy.shutdown().await;
    });
}

#[test]
fn credentials_stay_out_of_the_peer_dynamic_table_across_streams() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        for index in 0..20 {
            let mut request = request_with("echo", &format!("stream-{index}"));
            let mut cookie =
                http::HeaderValue::from_str(&format!("session=cookie-{index}")).expect("cookie");
            cookie.set_sensitive(true);
            request.headers_mut().insert("cookie", cookie);
            request.headers_mut().insert(
                "authorization",
                http::HeaderValue::from_str(&format!("Basic origin-{index}")).expect("credential"),
            );
            request
                .headers_mut()
                .insert("user-agent", http::HeaderValue::from_static("connect-test"));
            let mut tunnel = client
                .connect("vm", request, connector.clone())
                .await
                .expect("CONNECT")
                .into_tunnel();
            echo(&mut tunnel, b"x").await;
        }
        assert_eq!(proxy.connections(), 1);
        let written = tap.written();
        assert_eq!(written.len(), 1);
        let blocks = decode_header_blocks(&written[0]);
        assert_eq!(blocks.len(), 20, "one header block per CONNECT");
        let credential = |name: &[u8]| {
            matches!(
                name,
                b"authorization" | b"proxy-authorization" | b"cookie" | b"set-cookie"
            )
        };
        for (index, (headers, table)) in blocks.iter().enumerate() {
            let sent = |name: &str| {
                headers
                    .iter()
                    .find(|header| header.name.as_bytes() == name.as_bytes())
                    .map(|header| header.value.to_vec())
            };
            assert_eq!(
                sent("proxy-authorization"),
                Some(format!("Bearer token-stream-{index}").into_bytes())
            );
            assert_eq!(
                sent("cookie"),
                Some(format!("session=cookie-{index}").into_bytes())
            );
            assert_eq!(
                sent("authorization"),
                Some(format!("Basic origin-{index}").into_bytes())
            );
            assert!(
                table.iter().all(|entry| !credential(entry.name.as_bytes())),
                "block {index}: no credential enters the dynamic table",
            );
            assert!(
                table
                    .iter()
                    .any(|entry| entry.name.as_bytes() == b"user-agent"),
                "block {index}: ordinary headers are still indexed",
            );
        }
        proxy
    });
}
