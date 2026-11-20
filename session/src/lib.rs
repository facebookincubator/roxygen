/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/1.1 and HTTP/2 connection state machines, each owned by one worker thread.
//!
//! A session drives one connection. It runs the codec, keeps each stream's state, applies
//! flow control and timeouts, and moves header and body events between the connection and
//! the application's sources. A downstream session passes each request to an
//! [`HttpHandler`], and an upstream session sends requests and returns their responses. Most
//! applications use `roxygen_server` and `roxygen_client`, which start sessions on the
//! connections they accept and dial.
//!
//! The main entry points:
//!
//! - [`UniplexSession`] runs HTTP/1.1 or HTTP/2 on a [`UniplexTransport`], which every
//!   [`ByteStreamTransport`] implements. [`UniplexConfig`] sets its buffer sizes and timeouts,
//!   and [`Http2SessionConfig`] sets its HTTP/2 settings and flow-control windows.
//!   [`UniplexSession::run`] returns the future that drives the connection.
//! - [`UniplexHandle`], returned with the session, sends requests and HTTP/2 PINGs and starts a
//!   drain from the same worker while the session runs.
//! - [`SessionCore`] and [`StreamState`] hold the protocol-neutral connection and stream
//!   lifecycles that `UniplexSession` builds on. [`shutdown`] orders a session's drain and
//!   teardown.
//! - [`http2`] holds HTTP/2 priority scheduling, flood limits and statistics hooks.
//! - [`owned_h1`] and [`owned_h2`] are experimental poll-driven drivers. Each runs one future
//!   per connection without a task per request. A downstream driver serves each request with an
//!   [`OwnedExchange`] from the application's [`OwnedApplication`], and
//!   [`OwnedH1UpstreamSession`] and [`OwnedH2UpstreamSession`] send the requests that a caller
//!   reserves and submits. `roxygen_server` serves on the downstream drivers when
//!   `ServerSessionConfig::owned_http1` or `owned_http2` is set, and
//!   `roxygen_client::experimental::owned` runs client connections on the upstream ones.
//! - [`experimental`] holds unstable additions such as HTTP/2 PING probes and stream aborts.
//!
//! # Cargo features
//!
//! - `test-hooks`: exposes a hidden test hook on [`UniplexHandle`] to other crates' tests.
//!
//! [`ByteStreamTransport`]: roxygen_transport::byte_stream::ByteStreamTransport
//! [`Http2SessionConfig`]: http2::Http2SessionConfig
//! [`HttpHandler`]: roxygen_core::handler::HttpHandler
//! [`OwnedApplication`]: owned::OwnedApplication
//! [`OwnedExchange`]: owned::OwnedExchange
//! [`OwnedH1UpstreamSession`]: owned_h1::OwnedH1UpstreamSession
//! [`OwnedH2UpstreamSession`]: owned_h2::OwnedH2UpstreamSession
//! [`SessionCore`]: session::SessionCore
//! [`StreamState`]: stream_state::StreamState
//! [`UniplexConfig`]: uniplex::UniplexConfig
//! [`UniplexHandle`]: uniplex::UniplexHandle
//! [`UniplexSession`]: uniplex::UniplexSession
//! [`UniplexSession::run`]: uniplex::UniplexSession::run
//! [`UniplexTransport`]: uniplex::UniplexTransport

mod exchange;
pub mod experimental;
pub mod http2;
pub mod owned;
pub mod owned_h1;
pub mod owned_h2;
pub mod session;
pub mod shutdown;
pub mod stream_state;
pub mod uniplex;

pub use owned::DeadlineExpiry;
pub use owned::OwnedConnectionDeadlines;

#[cfg(test)]
#[path = "../tests/core.rs"]
mod core_tests;

#[cfg(test)]
#[path = "../tests/drain_lifetime.rs"]
mod drain_lifetime_tests;

#[cfg(test)]
#[path = "../tests/http2.rs"]
mod http2_tests;

#[cfg(test)]
#[path = "../tests/uniplex.rs"]
mod uniplex_tests;

#[cfg(test)]
#[path = "../tests/exchange_completion.rs"]
mod exchange_completion_tests;

#[cfg(test)]
#[path = "../tests/external_host.rs"]
mod external_host_tests;

#[cfg(test)]
#[path = "../tests/connect_upgrade.rs"]
mod connect_upgrade_tests;

#[cfg(test)]
#[path = "../tests/external_byte_stream.rs"]
mod external_byte_stream_tests;

#[cfg(test)]
#[path = "../tests/stream_abort.rs"]
mod stream_abort_tests;

#[cfg(test)]
#[path = "../tests/ping_probes.rs"]
mod ping_probes_tests;
