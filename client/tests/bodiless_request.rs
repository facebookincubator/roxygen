/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `SessionParams::unframed_bodiless_http1_requests` sends a bodiless request
//! as a head with no body framing, as reqwest sends a `POST` with an empty body.

use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::time::Duration;

use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::ManagedWorker;

use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::PoolParams;

/// The bytes an engine receives for a bodiless `POST /generate` sent through
/// an `HttpSessionPool`, up to the response.
fn bodiless_post_on_the_wire(unframed: bool) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        let mut input = Vec::new();
        let mut buffer = [0; 1024];
        while !input.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).expect("engine reads");
            assert_ne!(read, 0, "the request head arrives");
            input.extend_from_slice(&buffer[..read]);
        }
        // Anything the client frames after the head (an empty last chunk)
        // arrives with it or right after it.
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("engine read timeout");
        while let Ok(read) = stream.read(&mut buffer) {
            if read == 0 {
                break;
            }
            input.extend_from_slice(&buffer[..read]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .expect("engine replies");
        stream.set_read_timeout(None).expect("engine read timeout");
        while stream.read(&mut buffer).is_ok_and(|read| read != 0) {}
        input
    });
    let worker = ManagedWorker::new(64).expect("client worker starts");
    let call = worker
        .handle()
        .call_local(move |context| async move {
            let pool = HttpSessionPool::new(
                context,
                SocketAddress::new(address.ip().to_string(), address.port()),
                PoolParams {
                    connect_timeout: Duration::from_secs(1),
                    ..PoolParams::default()
                },
                ConnectionParams::default(),
                SessionParams {
                    stream_read_timeout: Some(Duration::ZERO),
                    connection_read_timeout: Some(Duration::ZERO),
                    write_timeout: Some(Duration::ZERO),
                    unframed_bodiless_http1_requests: unframed,
                    ..SessionParams::default()
                },
            );
            let GetSessionResult {
                reservation,
                session,
            } = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("acquisition succeeds");
            let mut message = HttpMessage::new();
            message.set_method(HttpMethod::Post).expect("valid method");
            message.set_url(b"/generate", false).expect("valid target");
            message.set_http_version(1, 1);
            let head = HttpHeaderEvent::new(message, true).expect("valid head");
            let mut response = session
                .send_reserved_request_with_headers(
                    head,
                    HttpSourceHolder::empty(),
                    reservation,
                    CancellationToken::never(),
                )
                .expect("request is committed");
            let head = response
                .source
                .read_header_event()
                .await
                .expect("response head");
            assert_eq!(head.headers().response_status(), Some(200));
            drop(response);
            let _ = pool.drain_and_join().await;
        })
        .expect("client task is admitted");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded client task")
        })
        .expect("client task completes");
    worker.shutdown().expect("client worker shuts down");
    engine.join().expect("engine finishes")
}

#[test]
fn a_bodiless_post_goes_out_as_an_empty_chunked_body_by_default() {
    assert_eq!(
        bodiless_post_on_the_wire(false),
        b"POST /generate HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n0\r\n\r\n"
    );
}

#[test]
fn a_bodiless_post_goes_out_unframed_when_enabled() {
    assert_eq!(
        bodiless_post_on_the_wire(true),
        b"POST /generate HTTP/1.1\r\nConnection: keep-alive\r\n\r\n"
    );
}
