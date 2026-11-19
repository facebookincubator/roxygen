/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runs the Axum integration fixture, an end-to-end test of
//! `roxygen_tower_adapter`.
//!
//! A Roxygen Tower client sends streaming requests over plaintext loopback TCP
//! to an Axum router served by the same adapter. The binary runs four
//! scenarios over HTTP/1.1, then again over HTTP/2 with prior knowledge:
//!
//! - an upload with trailers, echoed back while it is still being sent,
//! - a response body that fails,
//! - a startup that fails because the worker has already quiesced,
//! - cancellation during the upload.
//!
//! The binary checks the operation, cleanup and join results of each scenario,
//! and exits with a failure status if any check fails.
//!
//! This is a test fixture, not a tutorial. It checks ownership and failure
//! paths in detail, so it is much longer than an application needs. For a
//! short server, see examples/axum_router.rs.
//!
//! ```sh
//! cargo run --bin axum_tower_example
//! cargo test -p ferroxygen_axum_tower_fixture
//! ```

use std::process::ExitCode;

use roxygen_axum_tower_fixture::Protocol;
use roxygen_axum_tower_fixture::Scenario;
use roxygen_axum_tower_fixture::run;

fn main() -> ExitCode {
    let mut failed = false;
    for protocol in [Protocol::Http1, Protocol::Http2] {
        for scenario in [
            Scenario::Streaming,
            Scenario::BodyError,
            Scenario::StartupFailure,
            Scenario::Cancellation,
        ] {
            match run(protocol, scenario) {
                Ok(report) => {
                    if let Err(error) = report.verify() {
                        eprintln!("{protocol:?}/{scenario:?}: {error}\n{report:#?}");
                        failed = true;
                    } else {
                        println!("{protocol:?}/{scenario:?}: prototype checks satisfied");
                        println!("  native join: {:?}", report.phases.native_join);
                    }
                }
                Err(error) => {
                    eprintln!("{protocol:?}/{scenario:?}: bootstrap failed: {error}");
                    failed = true;
                }
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
