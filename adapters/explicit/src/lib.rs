/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An HTTP/1.1 client that sends each request on a new connection to a numeric address.
//!
//! Each [`Target`] holds a numeric `SocketAddr` and, for TLS, the application's own rustls
//! `ClientConfig`. A send dials a new connection, runs one HTTP/1.1 GET over a `roxygen_client`
//! session, drains the response body, and closes the connection. The crate does not resolve names,
//! reuse connections, or support HTTP/2. Most applications use `roxygen_client`, which resolves
//! names and pools HTTP/1.1 and HTTP/2 connections.
//!
//! The target's [`Security`] selects the connection:
//!
//! - [`Security::Plaintext`] dials the destination over TCP.
//! - [`Security::Tls`] dials the destination and runs TLS with the caller's configuration, which
//!   must not offer ALPN.
//! - [`Security::Tunnel`] dials a plaintext HTTP/1.1 proxy, opens a CONNECT tunnel to the
//!   destination with `roxygen_connect`, and runs TLS inside it. That TLS configuration must offer
//!   either no ALPN protocol or exactly `http/1.1`. The proxy's response head is returned in
//!   [`Response::connect_head`], or in [`ExplicitError::connect_head`] after a rejection or a
//!   later failure.
//!
//! The crate has two layers:
//!
//! - The [`exchange`](mod@exchange), [`tunnel`], [`error`], and [`stats`] modules are
//!   host-agnostic and do not use the Tokio runtime API. [`exchange()`] runs one exchange over any
//!   `WorkerContext` and an [`EstablishedIo`](roxygen_transport::established::EstablishedIo), so
//!   an embedder on another host can supply its own dialing, TLS, and worker.
//! - The [`runtime`] module is the Tokio host. [`ExplicitRuntime`] runs a fixed set of worker
//!   slots, dials TCP, runs TLS with tokio-rustls, and restarts a slot after its worker stops, up
//!   to `max_restarts_per_minute`. [`ExplicitClient::send`] sends one request to its target, and
//!   [`SlotHealth`] lets an embedder route around a failing slot.
//!
//! The API is experimental and may change.

pub mod error;
pub mod exchange;
pub mod runtime;
pub mod stats;
pub mod tunnel;

pub use crate::error::ErrorKind;
pub use crate::error::ExplicitError;
pub use crate::error::Phase;
pub use crate::exchange::BodyOutcome;
pub use crate::exchange::ConnectionInfo;
pub use crate::exchange::ExchangeInput;
pub use crate::exchange::ExchangeProgress;
pub use crate::exchange::RawHeaders;
pub use crate::exchange::Request;
pub use crate::exchange::Response;
pub use crate::exchange::Timings;
pub use crate::exchange::exchange;
pub use crate::runtime::CohostExecutor;
pub use crate::runtime::CohostExecutorRequest;
pub use crate::runtime::ExplicitClient;
pub use crate::runtime::ExplicitRuntime;
pub use crate::runtime::ExplicitRuntimeConfig;
pub use crate::runtime::Security;
pub use crate::runtime::SlotHealth;
pub use crate::runtime::SlotState;
pub use crate::runtime::Target;
pub use crate::stats::ExplicitStats;
pub use crate::stats::HISTOGRAM_BUCKET_UPPER_BOUNDS_US;
pub use crate::stats::HistogramSnapshot;
pub use crate::stats::RuntimeStats;
pub use crate::tunnel::ConnectHead;
pub use crate::tunnel::Tunnel;

#[cfg(test)]
#[path = "../tests/errors.rs"]
mod errors_tests;

#[cfg(test)]
#[path = "../tests/exchange.rs"]
mod exchange_tests;

#[cfg(test)]
#[path = "../tests/runtime.rs"]
mod runtime_tests;

#[cfg(test)]
#[path = "../tests/cohost.rs"]
mod cohost_tests;

#[cfg(test)]
#[path = "../tests/tunnel.rs"]
mod tunnel_tests;
