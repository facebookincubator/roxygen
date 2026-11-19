/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fetches one URL with Roxygen's client and prints the response.
//!
//! ```sh
//! cargo run --bin client_get -- https://rust-lang.org/
//! ```
//!
//! A Roxygen connection belongs to one worker thread, and requests run as
//! futures on that worker. The example starts a worker, sends it the request,
//! and blocks until the response arrives. HTTPS negotiates HTTP/2 when the
//! server offers it.

use std::error::Error;
use std::io::Write;
use std::time::Duration;

use roxygen_client::HttpClient;
use roxygen_client::RequestOptions;
use roxygen_client::connector::ClientTransportOptions;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::IdentityValidation;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_transport::tls::TlsClientOptions;

fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:3000/".to_owned());
    let worker = ManagedWorker::new(16)?;
    let call = worker.handle().call_local(move |context| async move {
        // These options hold worker-local state, so they are built on the worker.
        let options = RequestOptions {
            timeout: Some(Duration::from_secs(10)),
            connection: Some(ConnectionParams {
                // Trust the system roots and reject certificates for other names.
                transport: ClientTransportOptions::Tls(TlsClientOptions::new("")),
                identity_validation: IdentityValidation::Enforcing,
                ..ConnectionParams::default()
            }),
            ..RequestOptions::default()
        };
        HttpClient::get(&context, url, options).await
    })?;
    let response = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(call)??;
    worker.shutdown()?;

    let head = &response.headers;
    println!("status: {}", head.response_status().unwrap_or_default());
    for (_, name, value) in head.headers().iter() {
        println!(
            "{}: {}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(value)
        );
    }
    println!();
    let mut stdout = std::io::stdout().lock();
    for segment in response.body.segments() {
        stdout.write_all(segment)?;
    }
    Ok(())
}
