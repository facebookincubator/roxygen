use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;

use crate::connection::ServeTransportOptions;
use crate::connection::serve_transport;
use crate::scoped_server::ScopedHttpServer;
use crate::server::HandlerFactory;
use crate::server::HttpServerState;
use crate::server::ServerConfig;
use crate::server::WorkerPollStats;

const TIMEOUT: Duration = Duration::from_secs(10);

/// Reads each request to its end and answers `200 OK`.
struct OkHandler;

impl HttpHandler for OkHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            if !head.eom() {
                loop {
                    let body = request.read_body_event(u32::MAX).await?;
                    if body.eom() {
                        break;
                    }
                }
            }
            let body = BufQueue::from_bytes(Bytes::from_static(b"OK"), 2)
                .expect("fixed response body is bounded");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("status and body are valid"),
            )))
        })
    }
}

fn ok_factory() -> HandlerFactory {
    Arc::new(|_| Rc::new(OkHandler) as HttpHandlerRef)
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, TIMEOUT).expect("client connects");
    stream
        .set_read_timeout(Some(TIMEOUT))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(TIMEOUT))
        .expect("write timeout");
    stream
}

fn closing_request(address: SocketAddr) {
    let mut stream = connect(address);
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("request written");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("response read to EOF");
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with(b"\r\n\r\nOK"));
}

#[test]
fn worker_poll_stats_cover_each_worker_and_outlive_the_workers() {
    let config = ServerConfig {
        num_io_threads: 2,
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, ok_factory(), None).expect("server starts");
    let control = server.control();
    let started = server.worker_poll_stats();
    assert_eq!(started.len(), 2);
    for stats in &started {
        assert!(stats.driver_polls > 0, "{stats:?}");
        assert!(
            stats.command_polls > 0,
            "the worker loop is a command: {stats:?}"
        );
    }

    let address = server.address().expect("one listener");
    for _ in 0..4 {
        closing_request(address);
    }
    for (worker, (served, before)) in server.worker_poll_stats().iter().zip(&started).enumerate() {
        assert!(
            served.since(before).cleanup_polls > 0,
            "worker {worker} polled the cleanups of its sessions: {served:?}"
        );
    }

    assert!(server.drain());
    assert!(server.wait_for_stop().expect("workers join"));
    assert_eq!(server.state(), HttpServerState::Stopped);
    let stopped = server.worker_poll_stats();
    assert_eq!(stopped.len(), 2, "stats outlive the joined workers");
    assert_eq!(control.worker_poll_stats(), stopped);
    for stats in &stopped {
        assert!(stats.ready_entries > 0, "{stats:?}");
        assert_eq!(
            (stats.cleanups_registered, stats.cleanups_active),
            (0, 0),
            "{stats:?}"
        );
    }
    let mut total = WorkerPollStats::default();
    stopped.iter().for_each(|stats| total.merge(stats));
    assert_eq!(
        total.driver_polls,
        stopped[0].driver_polls + stopped[1].driver_polls
    );
    assert_eq!(server.worker_poll_stats(), stopped);
}

fn socket_address(address: SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

/// Reads one `Content-Length` framed response.
fn read_framed_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        if let Some(end) = response.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = std::str::from_utf8(&response[..end]).expect("ASCII response head");
            let length = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
            if response.len() >= end + 4 + length {
                return response;
            }
        }
        let count = stream.read(&mut chunk).expect("response bytes");
        assert_ne!(count, 0, "the response completes before EOF");
        response.extend_from_slice(&chunk[..count]);
    }
}

fn keep_alive_request(stream: &mut TcpStream) {
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    let response = read_framed_response(stream);
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with(b"\r\n\r\nOK"));
}

fn read_to_eof(stream: &mut TcpStream) -> Vec<u8> {
    let mut rest = Vec::new();
    stream
        .read_to_end(&mut rest)
        .expect("the server closes the connection");
    rest
}

/// The roles of the sessions that one worker serves at once.
const KEEP_ALIVE: usize = 0;
const IDLE: std::ops::RangeInclusive<usize> = 1..=8;
const TIMED_OUT: usize = 9;
const ABORTED: usize = 10;
const SESSIONS: usize = ABORTED + 1;

/// Many Uniplex sessions on one worker, whose cleanups wake each other only
/// through their own wakers: keep-alive exchanges before and after sibling
/// sessions end, an idle timeout, a peer abort mid-request, and a drain.
#[test]
fn woken_worker_serves_keep_alive_timed_out_aborted_and_drained_sessions() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener binds");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let address = listener.local_addr().expect("listener address");
    let worker = ManagedWorker::new(4).expect("worker starts");
    let handle = worker.handle();
    let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
    let served = handle
        .call_local(move |context| async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("worker reactor");
            let mut connections = Vec::new();
            for index in 0..SESSIONS {
                let (io, remote) = listener.accept().await.expect("worker accepts");
                let transport = adopt_local(
                    &context,
                    EstablishedIo::new(
                        io,
                        TransportEndpoint::Socket(socket_address(address)),
                        TransportEndpoint::Socket(socket_address(remote)),
                        None,
                    ),
                    ByteStreamLimits::new(4096, 4096, 8).expect("bounded transport"),
                )
                .expect("accepted I/O adopted");
                let mut options =
                    ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
                if index == TIMED_OUT {
                    options.session.connection_idle_timeout = Duration::from_millis(200);
                }
                connections.push(
                    serve_transport(&context, transport, Rc::new(OkHandler), options)
                        .expect("session attached"),
                );
            }
            let _ = drain_rx.await;
            for connection in &connections {
                let _ = connection.initiate_drain();
            }
            let mut closed = Vec::new();
            for connection in &connections {
                closed.push(connection.wait_closed().await.is_ok());
            }
            closed
        })
        .expect("server command admitted");

    let mut streams: Vec<_> = (0..TIMED_OUT)
        .map(|index| {
            let mut stream = connect(address);
            keep_alive_request(&mut stream);
            if index == KEEP_ALIVE {
                keep_alive_request(&mut stream);
                keep_alive_request(&mut stream);
            }
            stream
        })
        .collect();
    let mut timed_out = connect(address);
    keep_alive_request(&mut timed_out);
    assert!(
        read_to_eof(&mut timed_out).is_empty(),
        "the idle timeout closes its session"
    );
    let mut aborted = connect(address);
    aborted
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .expect("partial request written");
    drop(aborted);

    keep_alive_request(&mut streams[KEEP_ALIVE]);
    for index in IDLE.step_by(3) {
        keep_alive_request(&mut streams[index]);
    }
    drain_tx
        .send(())
        .expect("server command waits for the drain");
    for (index, stream) in streams.iter_mut().enumerate() {
        assert!(
            read_to_eof(stream).is_empty(),
            "the drain closes idle session {index}"
        );
    }
    let closed = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async { tokio::time::timeout(TIMEOUT, served).await })
        .expect("every session closes")
        .expect("server command completes");
    assert_eq!(closed.len(), SESSIONS);
    assert!(
        closed[..=TIMED_OUT].iter().all(|closed| *closed),
        "{closed:?}"
    );
    worker.shutdown().expect("worker shuts down");
    let stats = handle.poll_stats();
    assert_eq!(
        (stats.cleanups_registered, stats.cleanups_active),
        (0, 0),
        "{stats:?}"
    );
    assert!(stats.ready_entries > 0, "{stats:?}");
}
