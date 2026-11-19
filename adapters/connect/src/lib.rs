/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A pool of HTTP/2 CONNECT tunnels for Tokio applications.
//!
//! A [`ConnectRuntime`] runs one protocol worker thread that keeps a pool of HTTP/2 proxy sessions
//! per route key and owns every CONNECT stream on them. Tokio code calls
//! [`ConnectClient::connect`] on a cloneable client handle and receives a [`PendingTunnel`] once
//! the proxy returns 2xx. The crate builds on `roxygen_client` sessions and `roxygen_connect`
//! streams, and does not use hyper or the `h2` crate. For a single tunnel on a session that you
//! manage on a Roxygen worker, use `roxygen_connect` directly.
//!
//! The application supplies a [`Connector`] that dials each proxy connection. It handles proxy
//! selection, DNS, TLS, ALPN, and socket options. Equal route keys must name the same proxy and TLS
//! configuration, because a key's first connector serves its route until the route retires.
//! [`connect_tls`] runs a TLS handshake that requires h2, and [`connect_request`] builds a CONNECT
//! from HTTP/1-style header fields. The crate does not fall back to HTTP/1.1. When a connector
//! returns [`DialError::H2NotNegotiated`], the connect fails with
//! [`ConnectErrorKind::H2NotNegotiated`], and later connects on that route key fail the same way
//! for `h2_unsupported_ttl`.
//!
//! The caller then chooses one of two data paths. Bytes that the proxy sends right after the 2xx
//! reach the chosen path first.
//!
//! - [`PendingTunnel::into_tunnel`] returns a [`Tunnel`], a transferable `AsyncRead` and
//!   `AsyncWrite` connected to the stream through bounded buffers.
//! - [`PendingTunnel::relay`] hands a socket or other I/O to the protocol worker, which copies
//!   between it and the stream. The returned future resolves to a [`RelayOutcome`].
//!
//! A CONNECT that the proxy provably did not process, such as one reset with `REFUSED_STREAM`, is
//! retried up to `max_unprocessed_retries` times. An established tunnel is never replayed.
//! [`ConnectError::committed`] reports whether the proxy may have acted on a failed CONNECT.
//! Sessions send an HTTP/2 PING after `ping_interval` without reads, and drop the proxy connection
//! if the PING is not acknowledged within `ping_timeout`.
//!
//! Creating, shutting down, and dropping a [`ConnectRuntime`] block the calling thread, so do them
//! outside asynchronous Tokio tasks, for example in `spawn_blocking`. [`LazyConnectRuntime`]
//! starts the runtime on first use. [`ConnectOptions`] holds the limits and deadlines, and
//! [`ConnectStats`] the counters.
//!
//! The API is experimental and may change.

pub mod error;
pub mod lazy;
pub mod options;
pub mod relay;
pub mod request;
pub mod runtime;
pub mod stats;
pub mod tls;
pub mod tunnel;
mod worker;

pub use roxygen_core::connection::TransportEndpoint;
pub use roxygen_core::connection::TransportSecurity;
pub use roxygen_core::message::SocketAddress;

pub use crate::error::ConnectError;
pub use crate::error::ConnectErrorKind;
pub use crate::lazy::LazyConnectError;
pub use crate::lazy::LazyConnectRuntime;
pub use crate::options::ConnectOptions;
pub use crate::relay::RelayClose;
pub use crate::relay::RelayIo;
pub use crate::relay::RelayOutcome;
pub use crate::request::connect_request;
pub use crate::runtime::ConnectClient;
pub use crate::runtime::ConnectRuntime;
pub use crate::runtime::ConnectedIo;
pub use crate::runtime::Connector;
pub use crate::runtime::DialError;
pub use crate::runtime::ProxyIo;
pub use crate::stats::ConnectStats;
pub use crate::stats::LatencySummary;
pub use crate::tls::connect_tls;
pub use crate::tunnel::PendingTunnel;
pub use crate::tunnel::Tunnel;
pub use crate::tunnel::TunnelInfo;
