/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves an Axum router with Roxygen's HTTP/1.1 sessions in place of hyper's.
//!
//! ```sh
//! cargo run --bin axum_router
//! curl http://127.0.0.1:3000/hello/roxygen
//! ```
//!
//! The router runs on an ordinary Tokio runtime. Roxygen parses and writes HTTP
//! on its own worker thread, and the Tower adapter carries requests, responses
//! and bodies between the two. Ctrl-C drains open connections and exits.

use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::pin::pin;
use std::time::Duration;

use axum::Router;
use axum::extract::Path;
use axum::routing::get;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_hyper_adapter::body::RoxygenBody;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_tower_adapter::server::ServerOptions;
use roxygen_tower_adapter::server::prepare_connection;
use roxygen_transport::established::EstablishedIo;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

async fn hello(Path(name): Path<String>) -> String {
    format!("Hello, {name}!\n")
}

fn endpoint(address: SocketAddr) -> TransportEndpoint {
    TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
}

fn main() -> Result<(), Box<dyn Error>> {
    let app = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let listener = app.block_on(TcpListener::bind("127.0.0.1:3000"))?;
    println!("listening on http://{}", listener.local_addr()?);

    // Roxygen's workers are started and joined outside any Tokio runtime.
    let mut native = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 64,
    })?;
    let native_handle = native.handle();
    let worker = native_handle.worker(0).expect("the runtime has one worker");

    app.block_on(async {
        let router = Router::new().route("/hello/{name}", get(hello));
        let options = ServerOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
        // A connection is served only while its run future is polled, and is
        // cancelled when its last `ServerConnection` is dropped.
        let mut runs = JoinSet::new();
        let mut connections = HashMap::new();
        let mut next_id = 0_u64;
        let mut ctrl_c = pin!(tokio::signal::ctrl_c());
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, peer)) = accepted else { continue };
                    let Ok(local) = stream.local_addr() else { continue };
                    let established =
                        EstablishedIo::new(stream, endpoint(local), endpoint(peer), None);
                    let service = router.clone().into_service::<RoxygenBody>();
                    let (startup, run) =
                        prepare_connection(worker.clone(), established, service, options.clone())
                            .expect("the options are valid and the Tokio runtime is entered");
                    let id = next_id;
                    next_id += 1;
                    runs.spawn(async move { (id, run.await) });
                    match startup.start().await {
                        Ok(connection) => {
                            connections.insert(id, connection);
                        }
                        Err(error) => eprintln!("{peer}: {error}"),
                    }
                }
                Some(finished) = runs.join_next() => {
                    if let Ok((id, _)) = finished {
                        connections.remove(&id);
                    }
                }
                _ = &mut ctrl_c => break,
            }
        }
        for connection in connections.values() {
            connection.begin_drain();
        }
        let (_, quiesced) = tokio::join!(
            async { while runs.join_next().await.is_some() {} },
            native_handle.quiesce(Duration::from_secs(5)),
        );
        quiesced
    })?;
    native.join()?;
    Ok(())
}
