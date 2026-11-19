/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves HTTPS, negotiating HTTP/2 or HTTP/1.1 with ALPN.
//!
//! ```sh
//! cargo run --bin tls_server
//! curl -k https://localhost:3443/
//! curl -k --http1.1 https://localhost:3443/
//! ```
//!
//! The certificate is self-signed and generated at startup. For a real one,
//! pass its DER chain and key to `TlsServerOptions::new`, or build a
//! `rustls::ServerConfig` and use `TlsServerOptions::from_rustls_config`.

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
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_server::server::HandlerFactory;
use roxygen_server::server::HttpServer;
use roxygen_server::server::ServerConfig;
use roxygen_server::server::StaticTlsCredentials;
use roxygen_transport::tls::TlsServerOptions;

struct Hello;

impl HttpHandler for Hello {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let alpn = context
            .connection_context()
            .and_then(|connection| connection.negotiated_application_protocol())
            .map_or_else(
                || "none".to_owned(),
                |protocol| String::from_utf8_lossy(protocol).into_owned(),
            );
        Box::pin(async move {
            request.read_header_event().await?;
            let body = format!("Hello over TLS with ALPN protocol {alpn}\n");
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
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let tls = TlsServerOptions::new(
        vec![certified.cert.der().to_vec().into_boxed_slice()],
        certified.signing_key.serialized_der().into(),
        vec![b"h2".as_slice().into(), b"http/1.1".as_slice().into()],
    )?;
    let mut config = ServerConfig {
        tls_credentials: Some(Arc::new(StaticTlsCredentials::new(tls))),
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    config.socket.bind_addresses = vec!["127.0.0.1:3443".parse()?];
    let handlers: HandlerFactory = Arc::new(|_| Rc::new(Hello) as HttpHandlerRef);
    let server = HttpServer::start(config, handlers, None)?;
    println!(
        "listening on https://localhost:{}",
        server.address().expect("one listener").port()
    );

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(tokio::signal::ctrl_c())?;
    server.drain();
    server.wait_for_stop()?;
    Ok(())
}
