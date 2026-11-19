/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Streams response bodies with Roxygen's native handler API.
//!
//! ```sh
//! cargo run --bin streaming_server
//! curl -N http://127.0.0.1:3000/ticks
//! curl -N -T - http://127.0.0.1:3000/echo
//! ```
//!
//! `/ticks` sends a line every 200 ms from a source that the session pulls
//! when it has room to write. `/echo` sends the request body back as it
//! arrives.

use std::error::Error;
use std::future::ready;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::hybrid_source::HttpHybridSource;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_server::server::HandlerFactory;
use roxygen_server::server::HttpServer;
use roxygen_server::server::ServerConfig;

const TICK: Duration = Duration::from_millis(200);

struct Ticks {
    remaining: u32,
    pending: BufQueue,
}

impl HttpSource for Ticks {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let head = ok_head().and_then(|head| HttpHeaderEvent::new(head, false).map_err(internal));
        Box::pin(ready(head))
    }

    // A read may return at most `maximum` bytes. Each new line waits one tick.
    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let fresh = self.pending.is_empty();
        if fresh {
            self.remaining -= 1;
            let line = format!("tick {}\n", self.remaining).into_bytes();
            self.pending = BufQueue::from_vec(line, usize::MAX).expect("the queue has no limit");
        }
        let body = self.pending.split_at_most(maximum as usize);
        let eom = self.pending.is_empty() && self.remaining == 0;
        Box::pin(async move {
            if fresh {
                tokio::time::sleep(TICK).await;
            }
            Ok(HttpBodyEvent::body(body, eom))
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct Streaming;

impl HttpHandler for Streaming {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let response: Box<dyn HttpSource> = match head.headers().request_url() {
                Some(b"/echo") => {
                    Box::new(HttpHybridSource::new(ok_head()?, Some(request)).map_err(internal)?)
                }
                _ => Box::new(Ticks {
                    remaining: 5,
                    pending: BufQueue::new(0),
                }),
            };
            Ok(HttpSourceHolder::new(response))
        })
    }
}

/// A 200 head without `content-length`, which HTTP/1.1 sends chunked.
fn ok_head() -> Result<HttpMessage, HttpError> {
    let mut head = HttpMessage::new();
    head.set_status_code(200).map_err(internal)?;
    head.set_status_message(HttpMessage::default_reason(200))
        .map_err(internal)?;
    head.set_http_version(1, 1);
    Ok(head)
}

fn internal(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    config.socket.bind_addresses = vec!["127.0.0.1:3000".parse()?];
    let handlers: HandlerFactory = Arc::new(|_| Rc::new(Streaming) as HttpHandlerRef);
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
