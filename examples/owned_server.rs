/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves HTTP on Roxygen's experimental owned session drivers.
//!
//! ```sh
//! cargo run --bin owned_server
//! curl http://127.0.0.1:3100/
//! ```
//!
//! Pass `--h2c` to serve HTTP/2 with prior knowledge instead of HTTP/1.1, and
//! test it with `curl --http2-prior-knowledge`. Ctrl-C drains open connections
//! and exits, and a second Ctrl-C closes them at once.
//!
//! The handler is the same `HttpHandler` that `hello_server` uses. The default
//! driver spawns a task for each request. An owned driver runs one future per
//! connection and polls each request's handler and response inside it.

use std::error::Error;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_server::server::HandlerFactory;
use roxygen_server::server::HttpServer;
use roxygen_server::server::ServerConfig;

struct Hello;

impl HttpHandler for Hello {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let path = String::from_utf8_lossy(head.headers().request_url().unwrap_or_default());
            let body = format!("Hello from Roxygen! You asked for {path}\n");
            let body = BufQueue::from_vec(body.into_bytes(), usize::MAX).map_err(internal)?;
            let response =
                HttpFixedSource::make_fixed_response(200, Some(body)).map_err(internal)?;
            Ok(HttpSourceHolder::new(Box::new(response)))
        })
    }
}

fn internal(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut config = ServerConfig {
        // `main` handles Ctrl-C itself.
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    config.socket.bind_addresses = vec!["127.0.0.1:3100".parse()?];
    if std::env::args().any(|arg| arg == "--h2c") {
        config.plaintext_protocol = CodecProtocol::Http2;
        // The owned HTTP/2 driver applies the session timeouts.
        config.session.owned_http2 = true;
    } else {
        // The owned HTTP/1.1 driver does not apply timeouts, so `HttpServer::start`
        // rejects `owned_http1` unless the three session timeouts are zero.
        config.session.owned_http1 = true;
        config.session.stream_read_timeout = Duration::ZERO;
        config.session.connection_idle_timeout = Duration::ZERO;
        config.session.write_timeout = Duration::ZERO;
    }
    let handlers: HandlerFactory = Arc::new(|_worker_index| Rc::new(Hello) as HttpHandlerRef);
    let server = HttpServer::start(config, handlers, None)?;
    println!(
        "listening on http://{}",
        server.address().expect("one listener")
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(tokio::signal::ctrl_c())?;
    server.drain();
    // A drain waits for open requests, and the owned HTTP/1.1 driver does not
    // time out a client that stalls mid-request.
    let control = server.control();
    std::thread::spawn(move || {
        if runtime.block_on(tokio::signal::ctrl_c()).is_ok() {
            control.force_stop();
        }
    });
    server.wait_for_stop()?;
    Ok(())
}
