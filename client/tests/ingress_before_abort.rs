//! `SessionParams::deliver_ingress_before_abort` lets a response's reader take
//! the head and body bytes that arrived before the response failed, then the
//! error, as Hyper's body channel and h2 do (`DIV-033`).

use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::thread::JoinHandle;
use std::time::Duration;

use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerCall;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::uniplex::UniplexResponse;

use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::PoolParams;

#[derive(Clone, Copy)]
enum WaitForIngress {
    BeforeHead,
    BeforeBody,
}

/// What a reader saw: the head's status or the error code it got instead,
/// then each body event's bytes and the error that ended the body.
#[derive(Debug, PartialEq)]
enum Seen {
    Head(u16),
    Body(Vec<u8>),
    Error(HttpErrorCode),
}

/// An HTTP/1.1 peer that answers one request on one connection with `reply`,
/// then closes the connection.
fn raw_engine(reply: &'static [u8]) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).expect("engine reads");
            assert_ne!(read, 0, "the request head arrives");
            request.extend_from_slice(&buffer[..read]);
        }
        stream.write_all(reply).expect("engine replies");
        stream.shutdown(Shutdown::Write).expect("engine closes");
        while stream.read(&mut buffer).is_ok_and(|read| read != 0) {}
    });
    (address, engine)
}

fn bounded<T>(call: WorkerCall<T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded client task")
        })
        .expect("client task completes")
}

/// Runs the client task on a `ManagedWorker` and on a `HostedWorker` (one
/// Tokio task per session, as a router uses) and returns both results.
fn on_workers<F, T>(factory: F) -> [T; 2]
where
    F: FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T> + 'static>> + Clone + Send + 'static,
    T: Send + 'static,
{
    let worker = ManagedWorker::new(64).expect("client worker starts");
    let managed = bounded(
        worker
            .handle()
            .call_local(factory.clone())
            .expect("client task is admitted"),
    );
    worker.shutdown().expect("client worker shuts down");

    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(64).expect("nonzero capacity"),
        thread_name: "ingress-abort".to_owned(),
    })
    .expect("hosted client worker starts");
    let hosted = bounded(
        worker
            .handle()
            .call_local(factory)
            .expect("client task is admitted"),
    );
    let completion = worker.completion();
    drop(worker);
    assert!(
        matches!(*completion.borrow(), Some(Ok(()))),
        "the hosted worker joins cleanly"
    );
    [managed, hosted]
}

fn pool(
    context: WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
    deliver_ingress_before_abort: bool,
) -> HttpSessionPool {
    HttpSessionPool::new(
        context,
        SocketAddress::new(address.ip().to_string(), address.port()),
        PoolParams {
            connect_timeout: Duration::from_secs(1),
            ..PoolParams::default()
        },
        ConnectionParams {
            plaintext_protocol: protocol,
            ..ConnectionParams::default()
        },
        SessionParams {
            stream_read_timeout: Some(Duration::ZERO),
            connection_read_timeout: Some(Duration::ZERO),
            write_timeout: Some(Duration::ZERO),
            deliver_ingress_before_abort,
            ..SessionParams::default()
        },
    )
}

async fn send(pool: &HttpSessionPool) -> UniplexResponse {
    let GetSessionResult {
        reservation,
        session,
    } = pool
        .get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds");
    let request = HttpFixedSource::make_get_request(b"/", None).expect("test request is valid");
    session
        .send_reserved_request(
            HttpSourceHolder::new(Box::new(request)),
            reservation,
            CancellationToken::never(),
        )
        .await
        .expect("request is sent")
}

/// Waits for the producer's terminal event before reading the selected part
/// of the response.
async fn read(mut response: UniplexResponse, wait: WaitForIngress) -> Vec<Seen> {
    let ingress = response
        .source
        .ingress_completion()
        .expect("a fresh response has an ingress receipt");
    if matches!(wait, WaitForIngress::BeforeHead) {
        let _ = ingress.wait().await;
    }
    let mut seen = Vec::new();
    let head = match response.source.read_header_event().await {
        Ok(head) => head,
        Err(error) => {
            seen.push(Seen::Error(error.code()));
            return seen;
        }
    };
    seen.push(Seen::Head(head.headers().response_status().unwrap_or(0)));
    let mut eom = head.eom();
    if matches!(wait, WaitForIngress::BeforeBody) {
        let _ = ingress.wait().await;
    }
    while !eom {
        match response.source.read_body_event(u32::MAX).await {
            Ok(event) => {
                if let Some(body) = event.as_body()
                    && !body.is_empty()
                {
                    let mut bytes = Vec::new();
                    body.segments()
                        .for_each(|segment| bytes.extend_from_slice(segment));
                    seen.push(Seen::Body(bytes));
                }
                eom = event.eom();
            }
            Err(error) => {
                seen.push(Seen::Error(error.code()));
                break;
            }
        }
    }
    seen
}

fn h1_exchange(
    reply: &'static [u8],
    deliver_ingress_before_abort: bool,
    wait: WaitForIngress,
) -> [Vec<Seen>; 2] {
    on_workers(move |context| {
        Box::pin(async move {
            let (address, engine) = raw_engine(reply);
            let pool = pool(
                context,
                address,
                CodecProtocol::Http11,
                deliver_ingress_before_abort,
            );
            let seen = read(send(&pool).await, wait).await;
            // The failed session reports its error to the join.
            let _ = pool.drain_and_join().await;
            engine.join().expect("engine finishes");
            seen
        })
    })
}

const TRUNCATED_CHUNKED: &[u8] =
    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nfirst\r\n";
const TRUNCATED_LENGTH: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nfirst";

#[test]
fn a_truncated_chunked_body_yields_its_bytes_before_the_error_when_enabled() {
    for seen in h1_exchange(TRUNCATED_CHUNKED, true, WaitForIngress::BeforeBody) {
        assert_eq!(
            seen,
            [
                Seen::Head(200),
                Seen::Body(b"first".to_vec()),
                Seen::Error(HttpErrorCode::TRANSPORT_EOF)
            ]
        );
    }
}

#[test]
fn a_truncated_content_length_body_yields_its_bytes_before_the_error_when_enabled() {
    for seen in h1_exchange(TRUNCATED_LENGTH, true, WaitForIngress::BeforeBody) {
        assert_eq!(
            seen,
            [
                Seen::Head(200),
                Seen::Body(b"first".to_vec()),
                Seen::Error(HttpErrorCode::TRANSPORT_EOF)
            ]
        );
    }
}

#[test]
fn a_head_read_after_the_failure_is_delivered_when_enabled() {
    for seen in h1_exchange(TRUNCATED_CHUNKED, true, WaitForIngress::BeforeHead) {
        assert_eq!(
            seen,
            [
                Seen::Head(200),
                Seen::Body(b"first".to_vec()),
                Seen::Error(HttpErrorCode::TRANSPORT_EOF)
            ]
        );
    }
}

#[test]
fn a_truncated_response_loses_its_unread_head_and_bytes_by_default() {
    for seen in h1_exchange(TRUNCATED_CHUNKED, false, WaitForIngress::BeforeHead) {
        assert_eq!(seen, [Seen::Error(HttpErrorCode::TRANSPORT_EOF)]);
    }
}

#[test]
fn a_complete_body_is_unchanged_when_enabled() {
    let reply = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nfirst";
    for seen in h1_exchange(reply, true, WaitForIngress::BeforeHead) {
        assert_eq!(seen, [Seen::Head(200), Seen::Body(b"first".to_vec())]);
    }
}

/// One HTTP/2 frame: length, type, flags, stream and payload.
fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("small test frame");
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// An h2c peer that answers stream 1 with a 200 head and `first`, then resets
/// it with `CANCEL`, all in one write, and keeps the connection open until the
/// client closes it.
fn h2_engine() -> (SocketAddr, JoinHandle<()>) {
    const SETTINGS: u8 = 4;
    const HEADERS: u8 = 1;
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        stream
            .write_all(&frame(SETTINGS, 0, 0, &[]))
            .expect("engine settings");
        let mut input = Vec::new();
        let mut buffer = [0; 1024];
        // The 24-byte preface, then frames until the request's HEADERS.
        let mut offset = 24;
        loop {
            while input.len() < offset + 9 {
                let read = stream.read(&mut buffer).expect("engine reads");
                assert_ne!(read, 0, "the request arrives");
                input.extend_from_slice(&buffer[..read]);
            }
            let header = &input[offset..offset + 9];
            let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            if header[3] == HEADERS {
                break;
            }
            offset += 9 + length;
        }
        let mut reply = frame(SETTINGS, 1, 0, &[]);
        // HEADERS (END_HEADERS) with `:status: 200` as HPACK static index 8.
        reply.extend(frame(HEADERS, 4, 1, &[0x88]));
        reply.extend(frame(0, 0, 1, b"first"));
        // RST_STREAM with CANCEL.
        reply.extend(frame(3, 0, 1, &8_u32.to_be_bytes()));
        stream.write_all(&reply).expect("engine replies");
        while stream.read(&mut buffer).is_ok_and(|read| read != 0) {}
    });
    (address, engine)
}

#[test]
fn an_http2_reset_after_data_yields_the_data_first_when_enabled() {
    for enabled in [true, false] {
        let seen = on_workers(move |context| {
            Box::pin(async move {
                let (address, engine) = h2_engine();
                let pool = pool(context, address, CodecProtocol::Http2, enabled);
                let seen = read(send(&pool).await, WaitForIngress::BeforeHead).await;
                // The failed session reports its error to the join.
                let _ = pool.drain_and_join().await;
                engine.join().expect("engine finishes");
                seen
            })
        });
        let expected = if enabled {
            vec![
                Seen::Head(200),
                Seen::Body(b"first".to_vec()),
                Seen::Error(HttpErrorCode::CANCEL),
            ]
        } else {
            vec![Seen::Error(HttpErrorCode::CANCEL)]
        };
        for seen in seen {
            assert_eq!(seen, expected, "enabled: {enabled}");
        }
    }
}
