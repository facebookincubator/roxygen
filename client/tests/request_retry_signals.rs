/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! What a caller needs to retry, as hyper-util's `retry_canceled_requests`
//! does, a request that failed unstarted on a reused connection (`DIV-035`):
//! `UniplexResponse::sequence_number` counts the requests the connection
//! carried before, and `UniplexResponseProgressExt::request_started` says
//! whether any of the request was serialized.

use std::io::Read;
use std::io::Write;
#[cfg(not(fbcode_build))]
use std::net::Shutdown;
use std::net::TcpListener;
#[cfg(not(fbcode_build))]
use std::net::TcpStream;
#[cfg(not(fbcode_build))]
use std::rc::Rc;
#[cfg(not(fbcode_build))]
use std::sync::mpsc;
use std::time::Duration;

use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_session::experimental::UniplexResponseProgressExt;

use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::PoolParams;

#[test]
fn a_pooled_connection_reports_its_earlier_requests_and_each_request_s_start() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    // One keep-alive connection that answers two GETs.
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        let mut input = Vec::new();
        let mut buffer = [0; 1024];
        for _ in 0..2 {
            let end = loop {
                if let Some(end) = input.windows(4).position(|window| window == b"\r\n\r\n") {
                    break end;
                }
                let read = stream.read(&mut buffer).expect("engine reads");
                assert_ne!(read, 0, "the request arrives");
                input.extend_from_slice(&buffer[..read]);
            };
            input.drain(..end + 4);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .expect("engine replies");
        }
        while stream.read(&mut buffer).is_ok_and(|read| read != 0) {}
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
                    ..SessionParams::default()
                },
            );
            let mut seen = Vec::new();
            for _ in 0..2 {
                let GetSessionResult {
                    reservation,
                    session,
                } = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                let request =
                    HttpFixedSource::make_get_request(b"/", None).expect("test request is valid");
                let mut response = session
                    .send_reserved_request(
                        HttpSourceHolder::new(Box::new(request)),
                        reservation,
                        CancellationToken::never(),
                    )
                    .await
                    .expect("request is sent");
                let head = response
                    .source
                    .read_header_event()
                    .await
                    .expect("response head");
                assert!(head.eom());
                seen.push((response.sequence_number, response.request_started()));
                drop(response);
                while session.active_transactions() != 0 {
                    tokio::task::yield_now().await;
                }
            }
            let _ = pool.drain_and_join().await;
            seen
        })
        .expect("client task is admitted");
    let seen = tokio::runtime::Builder::new_current_thread()
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
    engine.join().expect("engine finishes");
    // The second request reused the connection that carried the first; both
    // were started.
    assert_eq!(seen, [(0, true), (1, true)]);
}

/// Reads one request head from `stream`, keeping any bytes after it in `input`.
#[cfg(not(fbcode_build))]
fn read_request_head(stream: &mut TcpStream, input: &mut Vec<u8>) {
    let mut buffer = [0; 1024];
    let end = loop {
        if let Some(end) = input.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
        let read = stream.read(&mut buffer).expect("engine reads");
        assert_ne!(read, 0, "the request arrives");
        input.extend_from_slice(&buffer[..read]);
    };
    input.drain(..end + 4);
}

/// What `stale_reused_connection` observed of the second request.
#[cfg(not(fbcode_build))]
#[derive(Debug, PartialEq)]
struct StaleReuse {
    sequence_number: u64,
    started_before_the_eof: bool,
    started_after: bool,
    /// The head read's error within a bound; `None` while it is still pending.
    head_error: Option<roxygen_core::error::HttpErrorCode>,
    /// What the engine received after its FIN.
    engine_received: Vec<u8>,
}

/// A pooled connection that carried one request is reused for a second one,
/// whose session writer is held (the session's `test-hooks` feature) while the
/// engine closes its side (FIN), as when that FIN and the request's commit fall
/// in one turn: the shape of hyper-util's `retry_canceled_requests` case.
/// With session timers off, as the router sets them.
#[cfg(not(fbcode_build))]
fn stale_reused_connection() -> StaleReuse {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let (close, closed) = mpsc::channel::<()>();
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        let mut input = Vec::new();
        read_request_head(&mut stream, &mut input);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .expect("engine replies");
        closed.recv().expect("the client asks for the FIN");
        stream.shutdown(Shutdown::Write).expect("engine sends FIN");
        let mut buffer = [0; 1024];
        while let Ok(read) = stream.read(&mut buffer)
            && read != 0
        {
            input.extend_from_slice(&buffer[..read]);
        }
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
                    ..SessionParams::default()
                },
            );
            let get = || {
                HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_get_request(b"/", None).expect("test request is valid"),
                ))
            };
            let GetSessionResult {
                reservation,
                session,
            } = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("first acquisition succeeds");
            let mut first = session
                .send_reserved_request(get(), reservation, CancellationToken::never())
                .await
                .expect("first request is sent");
            assert!(
                first
                    .source
                    .read_header_event()
                    .await
                    .expect("first response head")
                    .eom()
            );
            drop(first);
            while session.active_transactions() != 0 {
                tokio::task::yield_now().await;
            }
            let GetSessionResult {
                reservation,
                session: reused,
            } = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("second acquisition succeeds");
            let release = Rc::new(tokio::sync::Notify::new());
            reused.hold_writer_for_test(Rc::clone(&release));
            let mut response = reused
                .send_reserved_request(get(), reservation, CancellationToken::never())
                .await
                .expect("second request is committed");
            let started_before_the_eof = response.request_started();
            close.send(()).expect("engine waits for the FIN request");
            // Loopback delivers the FIN at once; the session reads it meanwhile.
            tokio::time::sleep(Duration::from_millis(100)).await;
            release.notify_one();
            let head_error = tokio::time::timeout(
                Duration::from_millis(300),
                response.source.read_header_event(),
            )
            .await
            .ok()
            .map(|head| head.err().expect("no response head arrives").code());
            let outcome = (
                response.sequence_number,
                started_before_the_eof,
                response.request_started(),
                head_error,
            );
            drop(response);
            let _ = pool.drain_and_join().await;
            outcome
        })
        .expect("client task is admitted");
    let (sequence_number, started_before_the_eof, started_after, head_error) =
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
    StaleReuse {
        sequence_number,
        started_before_the_eof,
        started_after,
        head_error,
        engine_received: engine.join().expect("engine finishes"),
    }
}

/// The request fails unstarted with `TRANSPORT_EOF` on the
/// reused connection, never written: hyper-util's retry condition
/// (`sequence_number != 0 && !request_started()`).
#[cfg(not(fbcode_build))]
#[test]
fn a_request_committed_on_a_stale_reused_connection_fails_unstarted() {
    assert_eq!(
        stale_reused_connection(),
        StaleReuse {
            sequence_number: 1,
            started_before_the_eof: false,
            started_after: false,
            head_error: Some(roxygen_core::error::HttpErrorCode::TRANSPORT_EOF),
            engine_received: Vec::new(),
        }
    );
}
