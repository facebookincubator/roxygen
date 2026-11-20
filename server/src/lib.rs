/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A multi-worker HTTP/1.1 and HTTP/2 server with TLS and graceful drain.
//!
//! [`HttpServer`] binds its listeners and starts a fixed set of worker threads, one by
//! default. The first worker accepts connections and hands them to the workers in turn, and
//! each connection runs as a `roxygen_session` session on its worker. Requests go to an
//! [`HttpHandler`] that each worker builds for itself, so a handler does not need to be
//! `Send`. Most server applications use this crate, and `roxygen_tower_adapter` builds on it
//! to serve Tower services.
//!
//! The main entry points:
//!
//! - [`HttpServer::start`] takes a [`ServerConfig`] and a [`HandlerFactory`], which each worker
//!   calls with its index. [`HttpServer::start_with_worker_factory`] also passes the worker's
//!   context. `ServerConfig` holds the listen addresses, the worker count, the protocol of
//!   plaintext connections (HTTP/1.1 by default) and TLS. Its [`ServerSessionConfig`] holds
//!   per-connection limits and timeouts.
//! - [`ServerSessionConfig::owned_http1`] and [`ServerSessionConfig::owned_http2`] serve
//!   HTTP/1.1 or HTTP/2 on the experimental owned drivers, which poll each handler from the
//!   connection's future and do not spawn a task for it. Both apply
//!   `ServerSessionConfig::owned_deadlines`. `owned_http1` applies no other session timeout and
//!   requires the stream read, connection idle and write timeouts to be zero.
//!   `roxygen_tower_adapter` rejects both, because these drivers do not yet report when an
//!   exchange's cleanup finishes.
//! - [`HttpServer::drain`] stops accepting connections and drains the open ones.
//!   [`HttpServer::wait_for_stop`] then blocks until they close and every worker exits. It
//!   fails if a drain has not started. [`HttpServer::force_stop`] closes the connections
//!   immediately and joins the workers, as dropping the server does. By default, SIGINT
//!   starts a drain. Set `shutdown_on_signals` to change that.
//! - `HttpServer` is not `Send`, so it stays on the thread that started it.
//!   [`HttpServerControl`], from [`HttpServer::control`], is `Send` and drains or force-stops
//!   the server from other threads.
//! - Set `tls_credentials` to a [`TlsCredentialProvider`], such as [`StaticTlsCredentials`], to
//!   serve TLS. A connection that negotiates `h2` through ALPN runs HTTP/2, and any other TLS
//!   connection runs HTTP/1.1. [`HttpServer::update_tls_credentials`] reloads the credentials
//!   for new connections.
//! - Sessions do not send `100 Continue` on their own. [`ExpectContinueWrapperHandler`] wraps
//!   a handler. For `Expect: 100-continue` it returns a `100 Continue` head before it calls the
//!   wrapped handler, and it rejects other expectations with 417.
//! - [`serve_transport`] serves one connection that the application accepted itself, on the
//!   current worker.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::rc::Rc;
//! use std::sync::Arc;
//!
//! use roxygen_core::buf_queue::BufQueue;
//! use roxygen_core::error::HttpError;
//! use roxygen_core::error::HttpErrorCode;
//! use roxygen_core::fixed_source::HttpFixedSource;
//! use roxygen_core::handler::HandlerSessionContext;
//! use roxygen_core::handler::HttpHandler;
//! use roxygen_core::handler::HttpHandlerRef;
//! use roxygen_core::handler::LocalHandlerFuture;
//! use roxygen_core::source::HttpSource;
//! use roxygen_core::source_holder::HttpSourceHolder;
//! use roxygen_server::server::HandlerFactory;
//! use roxygen_server::server::HttpServer;
//! use roxygen_server::server::ServerConfig;
//!
//! struct Hello;
//!
//! impl HttpHandler for Hello {
//!     fn handle_request(
//!         &self,
//!         _context: HandlerSessionContext,
//!         mut request: HttpSourceHolder,
//!     ) -> LocalHandlerFuture {
//!         Box::pin(async move {
//!             request.read_header_event().await?;
//!             let body = b"Hello, world!\n".to_vec();
//!             let body = BufQueue::from_vec(body, usize::MAX).map_err(internal)?;
//!             let response =
//!                 HttpFixedSource::make_fixed_response(200, Some(body)).map_err(internal)?;
//!             Ok(HttpSourceHolder::new(Box::new(response)))
//!         })
//!     }
//! }
//!
//! fn internal(error: impl std::fmt::Display) -> HttpError {
//!     HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
//! }
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut config = ServerConfig {
//!         // Ctrl-C is handled below.
//!         shutdown_on_signals: Vec::new(),
//!         ..ServerConfig::default()
//!     };
//!     config.socket.bind_addresses = vec!["127.0.0.1:3000".parse()?];
//!     // Each worker thread builds its own handler, which stays on that thread.
//!     let handlers: HandlerFactory = Arc::new(|_worker| Rc::new(Hello) as HttpHandlerRef);
//!     let server = HttpServer::start(config, handlers, None)?;
//!
//!     tokio::runtime::Builder::new_current_thread()
//!         .enable_all()
//!         .build()?
//!         .block_on(tokio::signal::ctrl_c())?;
//!     server.drain();
//!     server.wait_for_stop()?;
//!     Ok(())
//! }
//! ```
//!
//! [`ExpectContinueWrapperHandler`]: expect_continue::ExpectContinueWrapperHandler
//! [`HandlerFactory`]: server::HandlerFactory
//! [`HttpHandler`]: roxygen_core::handler::HttpHandler
//! [`HttpServer`]: server::HttpServer
//! [`HttpServer::control`]: server::HttpServer::control
//! [`HttpServer::drain`]: server::HttpServer::drain
//! [`HttpServer::force_stop`]: server::HttpServer::force_stop
//! [`HttpServer::start`]: server::HttpServer::start
//! [`HttpServer::start_with_worker_factory`]: server::HttpServer::start_with_worker_factory
//! [`HttpServer::update_tls_credentials`]: server::HttpServer::update_tls_credentials
//! [`HttpServer::wait_for_stop`]: server::HttpServer::wait_for_stop
//! [`HttpServerControl`]: server::HttpServerControl
//! [`ServerConfig`]: server::ServerConfig
//! [`ServerSessionConfig`]: acceptor::ServerSessionConfig
//! [`ServerSessionConfig::owned_http1`]: acceptor::ServerSessionConfig::owned_http1
//! [`ServerSessionConfig::owned_http2`]: acceptor::ServerSessionConfig::owned_http2
//! [`StaticTlsCredentials`]: server::StaticTlsCredentials
//! [`TlsCredentialProvider`]: server::TlsCredentialProvider
//! [`serve_transport`]: connection::serve_transport

#![cfg_attr(all(test, target_os = "linux", fbcode_build), feature(linux_pidfd))]

pub mod acceptor;
pub mod connection;
pub mod expect_continue;
pub mod experimental;
pub mod scoped_server;
pub mod server;
pub mod shutdown;

pub use acceptor::ConnectionId;
pub use acceptor::ConnectionLifecycleObserver;
pub use acceptor::ConnectionOutcome;
pub use server::AcceptDistribution;
pub use server::AcceptStats;
pub use server::StopAccepting;

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/accept_distribution.rs"]
mod accept_distribution_tests;

#[cfg(test)]
#[path = "../tests/blocking_pool.rs"]
mod blocking_pool_tests;

#[cfg(test)]
#[path = "../tests/cleanup_polling.rs"]
mod cleanup_polling_tests;

#[cfg(test)]
#[path = "../tests/expect_continue.rs"]
mod expect_continue_tests;

#[cfg(test)]
#[path = "../tests/lifecycle.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "../tests/attachment.rs"]
mod attachment_tests;

#[cfg(test)]
#[path = "../tests/allocation_count.rs"]
mod allocation_count;

#[cfg(test)]
#[path = "../tests/eof_abort.rs"]
mod eof_abort_tests;

#[cfg(test)]
#[path = "../tests/http1_egress_batching.rs"]
mod http1_egress_batching_tests;

#[cfg(test)]
#[path = "../tests/non_abortive_close.rs"]
mod non_abortive_close_tests;

#[cfg(test)]
#[path = "../tests/polled_response_body.rs"]
mod polled_response_body_tests;

#[cfg(test)]
#[path = "../tests/router_ingress.rs"]
mod router_ingress_tests;

#[cfg(test)]
#[path = "../tests/request_fields.rs"]
mod request_fields_tests;

#[cfg(test)]
#[path = "../tests/native_readiness.rs"]
mod native_readiness_tests;

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/native_lifecycle_readiness.rs"]
mod native_lifecycle_readiness_tests;

#[cfg(test)]
#[path = "../tests/tls_session_facts.rs"]
mod tls_session_facts_tests;

#[cfg(test)]
#[path = "../tests/owned_deadlines.rs"]
mod owned_deadlines_tests;
