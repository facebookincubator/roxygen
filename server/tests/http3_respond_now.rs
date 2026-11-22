/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `HttpHandler::respond_now` on HTTP/3, end to end over loopback with the test client of
//! `h3_client`.

use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::FixedResponse;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;

use crate::experimental::StaticFiles;
use crate::h3_client::H3Client;
use crate::h3_client::Request;
use crate::h3_client::Response;
use crate::h3_client::Seen;
use crate::h3_client::WAIT;
use crate::http3_tests::closed_without_error;
use crate::http3_tests::free_udp_address;
use crate::http3_tests::quic_listener;
use crate::server::HandlerFactory;
use crate::server::HttpServer;
use crate::server::ServerConfig;

const BODY: &[u8] = b"fixed body";

/// How often the handlers of a server had each method called, across its workers.
#[derive(Default)]
struct Calls {
    respond_now: AtomicUsize,
    handle_request: AtomicUsize,
    /// Request body bytes that `handle_request` read.
    request_bytes: AtomicUsize,
}

impl Calls {
    /// Returns how many times `respond_now` and `handle_request` were called.
    fn counts(&self) -> (usize, usize) {
        (
            self.respond_now.load(Ordering::SeqCst),
            self.handle_request.load(Ordering::SeqCst),
        )
    }
}

/// Serves HTTP/3 on a fresh loopback address with one worker, whose handler `make` builds.
fn start(make: impl Fn() -> HttpHandlerRef + Send + Sync + 'static) -> (HttpServer, H3Client) {
    let addr = free_udp_address();
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let handlers: HandlerFactory = Arc::new(move |_| make());
    let server = HttpServer::start(config, handlers, None).expect("the server starts");
    (server, H3Client::new())
}

/// Returns the response that [`Fixed`] answers every request with, from either method.
fn fixed_parts() -> (HttpMessage, BufQueue) {
    let mut head = HttpMessage::new();
    head.set_status_code(200).expect("a response head");
    head.headers_mut()
        .add(HttpHeaderCode::ContentType, b"text/plain");
    head.headers_mut().add("X-Fixed", b"one");
    let body = BufQueue::from_bytes(Bytes::from_static(BODY), BODY.len())
        .expect("the body fits its queue");
    (head, body)
}

/// Returns the response the client decodes from [`fixed_parts`].
fn fixed_response() -> Response {
    let headers = [
        ("content-type", "text/plain"),
        ("x-fixed", "one"),
        ("content-length", "10"),
    ];
    Response {
        status: 200,
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        body: BODY.to_vec(),
    }
}

/// A handler that answers every request with [`fixed_parts`]: from `respond_now` when `now` is
/// set, and otherwise from `handle_request` after reading the whole request.
struct Fixed {
    now: bool,
    calls: Arc<Calls>,
}

impl HttpHandler for Fixed {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let calls = Arc::clone(&self.calls);
        calls.handle_request.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let mut eom = request.read_header_event().await?.eom();
            while !eom {
                let event = request.read_body_event(u32::MAX).await?;
                if let Some(body) = event.as_body() {
                    calls
                        .request_bytes
                        .fetch_add(body.chain_length(), Ordering::SeqCst);
                }
                eom = event.eom();
            }
            let (head, body) = fixed_parts();
            Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
                head,
                Some(body),
            ))))
        })
    }

    fn respond_now(&self, _request: &HttpMessage) -> Option<Result<FixedResponse, HttpError>> {
        self.calls.respond_now.fetch_add(1, Ordering::SeqCst);
        if !self.now {
            return None;
        }
        let (head, body) = fixed_parts();
        Some(Ok(FixedResponse::new(head, body)))
    }
}

fn fixed(now: bool) -> (HttpServer, H3Client, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    let shared = Arc::clone(&calls);
    let (server, client) = start(move || {
        Rc::new(Fixed {
            now,
            calls: Arc::clone(&shared),
        }) as HttpHandlerRef
    });
    (server, client, calls)
}

/// A GET, whose HEADERS and FIN the client sends together, is answered from `respond_now`, and
/// the handler's `handle_request` never starts.
#[test]
fn a_get_is_answered_by_respond_now_without_starting_the_handler() {
    let (server, client, calls) = fixed(true);
    let conn = client.connect(server.quic_addresses()[0]);
    for _ in 0..3 {
        assert_eq!(conn.fetch(Request::get("/fixed")), fixed_response());
    }
    assert_eq!(calls.counts(), (3, 0), "only respond_now runs");
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// A GET that `respond_now` declines is answered by `handle_request` with the same response.
#[test]
fn a_get_that_respond_now_declines_is_answered_by_handle_request() {
    let (server, client, calls) = fixed(false);
    let conn = client.connect(server.quic_addresses()[0]);
    assert_eq!(conn.fetch(Request::get("/fixed")), fixed_response());
    assert_eq!(
        calls.counts(),
        (1, 1),
        "respond_now declines, then the handler answers"
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// A request with a body is not offered to `respond_now`: `handle_request` reads its body and
/// answers it.
#[test]
fn a_request_with_a_body_is_left_to_handle_request() {
    let (server, client, calls) = fixed(true);
    let conn = client.connect(server.quic_addresses()[0]);
    assert_eq!(
        conn.fetch(Request::post("/fixed", "hello")),
        fixed_response()
    );
    assert_eq!(calls.counts(), (0, 1));
    assert_eq!(calls.request_bytes.load(Ordering::SeqCst), 5);
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// A directory under the temporary directory, removed with its files when dropped.
struct Root(PathBuf);

impl Root {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "roxygen-h3-respond-now-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("the directory is created");
        Self(path)
    }

    fn write(&self, name: &str, contents: &[u8]) {
        std::fs::write(self.0.join(name), contents).expect("the file is written");
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A handler serving the files under its root, with precompressed siblings, through either method.
struct Files {
    files: Rc<StaticFiles>,
    calls: Arc<Calls>,
}

fn accept_encoding(request: &HttpMessage) -> &[u8] {
    request
        .headers()
        .get_single_or_empty(HttpHeaderCode::AcceptEncoding)
}

impl HttpHandler for Files {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.calls.handle_request.fetch_add(1, Ordering::SeqCst);
        let files = Rc::clone(&self.files);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let request = head.headers();
            files.respond(
                request.request_url().unwrap_or_default(),
                accept_encoding(request),
            )
        })
    }

    fn respond_now(&self, request: &HttpMessage) -> Option<Result<FixedResponse, HttpError>> {
        self.calls.respond_now.fetch_add(1, Ordering::SeqCst);
        let path = request.request_url().unwrap_or_default();
        Some(self.files.respond_now(path, accept_encoding(request)))
    }
}

fn field(response: &Response, name: &str) -> Option<String> {
    response
        .headers
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.clone())
}

/// `StaticFiles::respond_now` answers over HTTP/3 with the precompressed sibling that the
/// request's `Accept-Encoding` allows, and with the plain file otherwise.
#[test]
fn static_files_answer_through_respond_now_with_the_precompressed_sibling() {
    let root = Root::new();
    root.write("app.js", b"plain");
    root.write("app.js.br", b"brotli");
    let calls = Arc::new(Calls::default());
    let (shared, path) = (Arc::clone(&calls), root.0.clone());
    let (server, client) = start(move || {
        Rc::new(Files {
            files: Rc::new(StaticFiles::new(&path).with_precompressed(true)),
            calls: Arc::clone(&shared),
        }) as HttpHandlerRef
    });
    let conn = client.connect(server.quic_addresses()[0]);

    let brotli = conn.fetch(Request::get("/app.js").with_field("accept-encoding", "br"));
    assert_eq!(brotli.status, 200);
    assert_eq!(brotli.body, b"brotli");
    assert_eq!(field(&brotli, "content-encoding").as_deref(), Some("br"));
    assert_eq!(
        field(&brotli, "content-type").as_deref(),
        Some("application/javascript")
    );
    assert_eq!(field(&brotli, "vary").as_deref(), Some("accept-encoding"));
    assert_eq!(field(&brotli, "content-length").as_deref(), Some("6"));

    let plain = conn.fetch(Request::get("/app.js"));
    assert_eq!(plain.body, b"plain");
    assert_eq!(field(&plain, "content-encoding"), None);
    assert_eq!(field(&plain, "content-length").as_deref(), Some("5"));

    let missing = conn.fetch(Request::get("/missing.js").with_field("accept-encoding", "br"));
    assert_eq!(missing.status, 404);
    assert_eq!(calls.counts(), (3, 0), "only respond_now runs");
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// Requests answered from `respond_now` hold and free their request streams as other requests
/// do, past the hundred a connection may open at once, and a drain afterwards still sends
/// GOAWAY and closes the connection without an error.
#[test]
fn a_drain_after_requests_answered_by_respond_now_completes() {
    const REQUESTS: usize = 150;
    let (server, client, calls) = fixed(true);
    let conn = client.connect(server.quic_addresses()[0]);
    let pending: Vec<_> = (0..REQUESTS)
        .map(|i| conn.send(Request::get(&format!("/{i}"))))
        .collect();
    for response in pending {
        let response = response
            .recv_timeout(WAIT)
            .expect("a response arrives")
            .expect("the request succeeds");
        assert_eq!(response, fixed_response());
    }
    assert_eq!(calls.counts(), (REQUESTS, 0), "only respond_now runs");
    assert!(server.drain());
    // The request streams were 0, 4, ..., 596; the GOAWAY carries the next one.
    let seen = conn.await_seen(closed_without_error);
    assert!(
        seen.contains(&Seen::Goaway(4 * REQUESTS as u64)),
        "saw {seen:?}"
    );
    server.wait_for_stop().expect("the drain finishes");
    client.shutdown();
}
