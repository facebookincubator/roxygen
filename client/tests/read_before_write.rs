/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Read-before-write arbitration of HTTP/1.1 client sessions (`DIV-042`)
//! through an `HttpSessionPool` over loopback TCP, on a `ManagedWorker` and a
//! `HostedWorker`: after one or two exchanges on a pooled connection, the
//! engine closes it (FIN) and a request is committed on it in the worker turn
//! that sees the FIN become readable, before the session's next poll. The request
//! must fail unstarted regardless of how many exchanges the connection carried.

use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::mpsc;
use std::time::Duration;

use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerCall;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::experimental::UniplexResponseProgressExt;
use roxygen_session::uniplex::UniplexResponse;

use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::PoolParams;

const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

/// What the request committed after the engine's FIN met, and what the engine saw.
#[derive(Debug, PartialEq)]
struct FinCommit {
    sequence_number: u64,
    started: bool,
    head_error: Option<HttpErrorCode>,
    /// Bytes the engine read on the closed connection after its FIN.
    received_after_fin: usize,
    /// The resend on a fresh connection, when the request failed unstarted:
    /// its sequence number, and whether it got the engine's 200.
    resent: Option<(u64, bool)>,
}

/// Reads request heads from `stream` until it ends, answering each with `OK`
/// while `answer` lasts, and returns how many heads it read.
fn serve(stream: &mut TcpStream, mut answer: usize) -> usize {
    let mut input = Vec::new();
    let mut buffer = [0; 1024];
    let mut heads = 0;
    loop {
        if let Some(end) = input.windows(4).position(|window| window == b"\r\n\r\n") {
            input.drain(..end + 4);
            heads += 1;
            if answer != 0 {
                answer -= 1;
                stream.write_all(OK).expect("engine replies");
                if answer == 0 {
                    return heads;
                }
            }
            continue;
        }
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => return heads,
            Ok(read) => input.extend_from_slice(&buffer[..read]),
        }
    }
}

fn get() -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_get_request(b"/", None).expect("test request is valid"),
    ))
}

async fn send(pool: &HttpSessionPool) -> UniplexResponse {
    let GetSessionResult {
        reservation,
        session,
    } = pool
        .get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds");
    session
        .send_reserved_request(get(), reservation, CancellationToken::never())
        .await
        .expect("request is committed")
}

async fn read_to_end(response: &mut UniplexResponse) -> Result<(), HttpErrorCode> {
    let head = response
        .source
        .read_header_event()
        .await
        .map_err(|error| error.code())?;
    let mut eom = head.eom();
    while !eom {
        eom = response
            .source
            .read_body_event(u32::MAX)
            .await
            .map_err(|error| error.code())?
            .eom();
    }
    Ok(())
}

/// `prior_exchanges` exchanges on one pooled connection, then the FIN and the
/// commit in one worker turn, with session timers off.
/// The worker thread blocks while the engine writes a byte on a side connection
/// and then sends its FIN, so its next reactor turn sees both, the side byte
/// first: the client task runs before the session, takes the idle pooled
/// session and commits the request.
fn fin_commit(
    context: WorkerContext,
    prior_exchanges: usize,
) -> Pin<Box<dyn Future<Output = FinCommit>>> {
    Box::pin(async move {
        let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
        let address = listener.local_addr().expect("engine address");
        let side_listener = TcpListener::bind("127.0.0.1:0").expect("side binds");
        let side_client = TcpStream::connect(side_listener.local_addr().expect("side address"))
            .expect("side connects");
        side_client.set_nonblocking(true).expect("nonblocking side");
        let (mut side_engine, _) = side_listener.accept().expect("side accepted");
        let (close, closes) = mpsc::channel::<()>();
        let (closed, closed_rx) = mpsc::channel::<()>();
        let engine = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("engine accepts");
            assert_eq!(serve(&mut first, prior_exchanges), prior_exchanges);
            closes.recv().expect("the client asks for the FIN");
            side_engine.write_all(b"x").expect("side byte");
            first.shutdown(Shutdown::Write).expect("engine sends FIN");
            closed.send(()).expect("the client waits");
            let mut received_after_fin = 0;
            let mut buffer = [0; 1024];
            while let Ok(read) = first.read(&mut buffer)
                && read != 0
            {
                received_after_fin += read;
            }
            drop(first);
            listener
                .set_nonblocking(true)
                .expect("nonblocking listener");
            let resent = (0..200).find_map(|_| match listener.accept() {
                Ok((mut fresh, _)) => {
                    fresh.set_nonblocking(false).expect("blocking stream");
                    Some(serve(&mut fresh, 1))
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(10));
                    None
                }
            });
            (received_after_fin, resent)
        });
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
        for _ in 0..prior_exchanges {
            let mut response = send(&pool).await;
            read_to_end(&mut response).await.expect("earlier response");
        }
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let side = tokio::net::TcpStream::from_std(side_client).expect("side registered");
        close.send(()).expect("engine listens");
        closed_rx.recv().expect("engine sent its FIN");
        side.readable().await.expect("side byte readable");
        let mut response = send(&pool).await;
        let head_error = tokio::time::timeout(Duration::from_secs(5), read_to_end(&mut response))
            .await
            .expect("the response ends within the bound")
            .err();
        let (sequence_number, started) = (response.sequence_number, response.request_started());
        drop(response);
        let resent = if started || head_error.is_none() {
            None
        } else {
            let mut retry = send(&pool).await;
            let delivered = read_to_end(&mut retry).await.is_ok();
            Some((retry.sequence_number, delivered))
        };
        let _ = pool.drain_and_join().await;
        drop(side);
        let (received_after_fin, engine_resent) = engine.join().expect("engine finishes");
        assert_eq!(
            engine_resent,
            resent.map(|_| 1),
            "the engine serves exactly one request when the client resends"
        );
        FinCommit {
            sequence_number,
            started,
            head_error,
            received_after_fin,
            resent,
        }
    })
}

fn bounded<T>(call: WorkerCall<T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(20), call)
                .await
                .expect("bounded client task")
        })
        .expect("client task completes")
}

/// Runs `fin_commit` on a `ManagedWorker` and on a `HostedWorker`.
fn on_workers(prior_exchanges: usize) -> [FinCommit; 2] {
    let worker = ManagedWorker::new(64).expect("client worker starts");
    let managed = bounded(
        worker
            .handle()
            .call_local(move |context| fin_commit(context, prior_exchanges))
            .expect("client task is admitted"),
    );
    worker.shutdown().expect("client worker shuts down");
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(64).expect("nonzero capacity"),
        thread_name: "read-before-write".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted client worker starts");
    let hosted = bounded(
        worker
            .handle()
            .call_local(move |context| fin_commit(context, prior_exchanges))
            .expect("client task is admitted"),
    );
    drop(worker);
    [managed, hosted]
}

#[test]
fn a_request_committed_after_the_engine_s_fin_fails_unstarted_and_is_resent() {
    for prior_exchanges in [1, 2] {
        for outcome in on_workers(prior_exchanges) {
            assert_eq!(
                outcome,
                FinCommit {
                    sequence_number: prior_exchanges as u64,
                    started: false,
                    head_error: Some(HttpErrorCode::TRANSPORT_EOF),
                    received_after_fin: 0,
                    resent: Some((0, true)),
                },
                "after {prior_exchanges} exchanges"
            );
        }
    }
}
