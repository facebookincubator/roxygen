/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP CONNECT tunnels over Roxygen client sessions.
//!
//! A tunnel is one CONNECT request on an HTTP/1.1 or HTTP/2 proxy session.
//! Its request body carries the bytes sent to the target, and its response
//! body carries the bytes that come back. `roxygen_client` implements
//! [`ConnectSession`](session::ConnectSession) for its `ClientSession`, so a
//! client session can carry tunnels directly. For a pool of HTTP/2 tunnels
//! driven from Tokio code, use `roxygen_connect_client`, which builds on this
//! crate.
//!
//! - [`HttpConnectStream`](stream::HttpConnectStream) is an established
//!   tunnel. [`connect`](stream::HttpConnectStream::connect) opens one on a
//!   session that another owner, such as a pool, keeps.
//!   [`connect_unique`](stream::HttpConnectStream::connect_unique) takes the
//!   session and drains it when the tunnel is dropped.
//! - [`HttpConnectTransport`](transport::HttpConnectTransport) is a Roxygen
//!   `ByteStreamTransport` over a tunnel.
//! - [`HttpConnectAsyncTransport`](async_transport::HttpConnectAsyncTransport)
//!   implements Tokio's `AsyncRead` and `AsyncWrite` over a tunnel, for
//!   example to run tokio-rustls inside it.
//!
//! These types are owner-local. Create, use and drop them on the worker that
//! owns the proxy session.
//!
//! The [`experimental`] module exposes the final CONNECT response head, the
//! response that rejected a connect, and an explicit stream reset. Its API is
//! experimental and may change.
//!
//! # Example
//!
//! Opening a tunnel on a proxy session that the caller keeps, and adapting it
//! for Tokio I/O. With `roxygen_client`, `proxy` is a `ClientSession` and the
//! reservation comes from its `reserve_request` method.
//!
//! ```
//! use std::time::Duration;
//!
//! use roxygen_connect::async_transport::HttpConnectAsyncTransport;
//! use roxygen_connect::session::ConnectSession;
//! use roxygen_connect::stream::DEFAULT_EGRESS_BUFFER_SIZE;
//! use roxygen_connect::stream::HttpConnectStream;
//! use roxygen_connect::stream::RequestHeaderMap;
//! use roxygen_core::error::HttpError;
//! use roxygen_runtime::cancellation::CancellationToken;
//! use roxygen_runtime::worker::WorkerContext;
//! use roxygen_session::session::RequestReservation;
//!
//! async fn open_tunnel(
//!     context: &WorkerContext,
//!     proxy: &impl ConnectSession,
//!     reservation: RequestReservation,
//! ) -> Result<HttpConnectAsyncTransport, HttpError> {
//!     let stream = HttpConnectStream::connect(
//!         context,
//!         proxy,
//!         reservation,
//!         "example.com:443",
//!         Duration::from_secs(30),
//!         RequestHeaderMap::new(),
//!         DEFAULT_EGRESS_BUFFER_SIZE,
//!         CancellationToken::never(),
//!     )
//!     .await?;
//!     Ok(HttpConnectAsyncTransport::new(context, stream))
//! }
//! ```

pub mod async_transport;
pub mod experimental;
pub mod session;
pub mod stream;
pub mod transport;
