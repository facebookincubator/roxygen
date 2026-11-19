/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A minimal server written against Roxygen's native handler API.
//!
//! ```sh
//! cargo run --bin hello_server
//! curl http://127.0.0.1:3000/
//! ```
//!
//! Pass `--h2c` to serve HTTP/2 with prior knowledge instead of HTTP/1.1, and
//! test it with `curl --http2-prior-knowledge`. Ctrl-C drains open connections
//! and exits.

use std::error::Error;
use std::rc::Rc;
use std::sync::Arc;

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
    config.socket.bind_addresses = vec!["127.0.0.1:3000".parse()?];
    if std::env::args().any(|arg| arg == "--h2c") {
        config.plaintext_protocol = CodecProtocol::Http2;
    }
    // Each worker thread calls this once. A handler stays on the worker that
    // created it, so it does not need to be `Send`.
    let handlers: HandlerFactory = Arc::new(|_worker_index| Rc::new(Hello) as HttpHandlerRef);
    let server = HttpServer::start(config, handlers, None)?;
    println!(
        "listening on http://{}",
        server.address().expect("one listener")
    );

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(tokio::signal::ctrl_c())?;
    server.drain();
    server.wait_for_stop()?;
    Ok(())
}
