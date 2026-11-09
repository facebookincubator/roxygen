use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;

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
