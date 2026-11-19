/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Frozen `HTTPConnectTransportTest` Async cases for `HttpConnectAsyncTransport`.

use bytes::BytesMut;
use roxygen_client::connector::ClientSession;
use roxygen_client::connector::SessionParams;
use roxygen_connect::async_transport::HttpConnectAsyncTransport;
use roxygen_core::connection::TransportEndpoint;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::connect_stream::H2cProxy;
use crate::connect_stream::connect_unique;
use crate::connect_stream::headers;
use crate::connect_stream::run_on_worker;

const BUFFER_SIZE: usize = 65_536;

/// `connectAsync` (HTTPConnectTransportTest.cpp:182-197).
pub(crate) async fn connect_async(
    context: &WorkerContext,
    proxy: &H2cProxy,
) -> (ClientSession, HttpConnectAsyncTransport) {
    let session = proxy.connect(context, SessionParams::default()).await;
    let stream = connect_unique(
        context,
        &session,
        headers(&[("Foo", "Bar")]),
        CancellationToken::never(),
    )
    .await
    .expect("the proxy accepts the CONNECT");
    (session, HttpConnectAsyncTransport::new(context, stream))
}

#[test]
fn connect_success_reports_x_connected_to_peer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect_async(&context, &proxy).await;
            assert_eq!(transport.peer_address().host(), b"1.2.3.4");
            assert_eq!(transport.peer_address().port(), 443, "the authority's port");
            let observed = proxy.log.requests.borrow()[0].peer.clone();
            assert_eq!(
                observed,
                Some(TransportEndpoint::Socket(transport.local_address().clone())),
                "the proxy observes the tunnel's local address as its client"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn write_flush_shutdown_then_read_to_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, mut transport) = connect_async(&context, &proxy).await;
            let sent = vec![b'a'; BUFFER_SIZE];
            transport.write_all(&sent).await.expect("tunnel write");
            transport.flush().await.expect("tunnel write completes");
            let mut received = vec![0; BUFFER_SIZE];
            transport
                .read_exact(&mut received)
                .await
                .expect("the proxy echoes every byte");
            assert_eq!(received, sent);
            transport.shutdown().await.expect("tunnel EOM");
            let mut rest = Vec::new();
            transport.read_to_end(&mut rest).await.expect("tunnel EOF");
            assert!(rest.is_empty(), "nothing follows the echo");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

// Similar to the test above, but shutdown immediately follows the write.
#[test]
fn shutdown_immediately_after_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, mut transport) = connect_async(&context, &proxy).await;
            let sent = vec![b'a'; BUFFER_SIZE];
            transport.write_all(&sent).await.expect("tunnel write");
            transport.shutdown().await.expect("tunnel EOM");
            let mut received = Vec::new();
            transport
                .read_to_end(&mut received)
                .await
                .expect("the echo ends with EOF");
            assert_eq!(received, sent);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn owned_buffer_read_path() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, mut transport) = connect_async(&context, &proxy).await;
            let sent = vec![b'a'; BUFFER_SIZE];
            transport.write_all(&sent).await.expect("tunnel write");
            transport.flush().await.expect("tunnel write completes");
            let mut received = BytesMut::with_capacity(BUFFER_SIZE);
            while received.len() < BUFFER_SIZE {
                let read = transport
                    .read_buf(&mut received)
                    .await
                    .expect("tunnel read");
                assert_ne!(read, 0, "EOF before the echo completed");
            }
            assert_eq!(received.as_ref(), sent.as_slice());
            transport.shutdown().await.expect("tunnel EOM");
            let mut rest = BytesMut::with_capacity(16);
            assert_eq!(
                transport.read_buf(&mut rest).await.expect("tunnel EOF"),
                0,
                "EOF follows the echo"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}
