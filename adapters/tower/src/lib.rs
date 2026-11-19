/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves a Tower service, such as an Axum router, on Roxygen, and provides a
//! Tower client backed by Roxygen.
//!
//! Application code keeps its `http` types, `http_body` bodies and Tower
//! services on its own Tokio runtime, while the HTTP/1.1 and HTTP/2 sessions
//! run on a Roxygen worker thread. The hyper client and server are not used.
//! This crate builds on `roxygen_http_adapter` and `roxygen_hyper_adapter`.
//! Applications that write Roxygen handlers directly use `roxygen_server` and
//! `roxygen_client` instead.
//!
//! Start the workers with a [`RuntimeOwner`] outside any Tokio runtime, and
//! give the application their [`WorkerHandle`]s. Each served connection and
//! each client does its work in a run future, [`ServerRun`] or [`ClientRun`],
//! and only the run joins that work. Prepare the connection or client on the
//! application's Tokio runtime, spawn its run there before awaiting startup,
//! and poll the run to completion even if startup fails. At shutdown, keep
//! polling the runs while awaiting [`RuntimeHandle::quiesce`], then call
//! [`RuntimeOwner::join`] outside Tokio before dropping the Tokio runtime.
//!
//! To serve, accept a connection and complete any TLS handshake yourself, then
//! pass a worker handle, the [`EstablishedIo`], a `Send` Tower service and
//! [`ServerOptions`] for the negotiated protocol to [`prepare_connection`].
//! Spawn the run, await [`ServerStartup::start`], and stop with
//! [`ServerConnection::begin_drain`] or [`ServerConnection::cancel`]. Requests
//! arrive with a [`RoxygenBody`]. The response body for a HEAD request, or for
//! a 204 or 304 response, is dropped without being polled.
//!
//! To send, pass a worker handle, a connector and [`ClientOptions`] to
//! [`Client::prepare`]. The connector is a Tower service that returns
//! [`ConnectedIo`], and [`HyperConnector`] wraps a hyper-util connector.
//! Spawn the run, await [`ClientStartup::start`], register a [`RouteSpec`]
//! with [`Client::route`], send through [`Client::service`] or
//! [`Client::try_request`], and stop with [`ClientOwner::shutdown`]. Roxygen's
//! session and pool timers apply on top of the connector's own timeouts.
//! [`ClientOptions::with_external_timeouts`] turns them off for applications
//! that enforce their own deadlines.
//!
//! [`RoxygenService`](service::RoxygenService) runs a worker-local service
//! behind a `Send` Tower service.
//!
//! See examples/axum_router.rs in the repository for an Axum router served
//! this way.
//!
//! [`RuntimeOwner`]: roxygen_runtime::managed::RuntimeOwner
//! [`RuntimeOwner::join`]: roxygen_runtime::managed::RuntimeOwner::join
//! [`RuntimeHandle::quiesce`]: roxygen_runtime::managed::RuntimeHandle::quiesce
//! [`WorkerHandle`]: roxygen_runtime::worker::WorkerHandle
//! [`ServerRun`]: server::ServerRun
//! [`ClientRun`]: client::ClientRun
//! [`EstablishedIo`]: roxygen_transport::established::EstablishedIo
//! [`ServerOptions`]: server::ServerOptions
//! [`prepare_connection`]: server::prepare_connection
//! [`ServerStartup::start`]: server::ServerStartup::start
//! [`ServerConnection::begin_drain`]: server::ServerConnection::begin_drain
//! [`ServerConnection::cancel`]: server::ServerConnection::cancel
//! [`RoxygenBody`]: roxygen_hyper_adapter::body::RoxygenBody
//! [`ClientOptions`]: client::ClientOptions
//! [`Client::prepare`]: client::Client::prepare
//! [`ConnectedIo`]: client::connector::ConnectedIo
//! [`HyperConnector`]: client::connector::HyperConnector
//! [`ClientStartup::start`]: client::ClientStartup::start
//! [`RouteSpec`]: route::RouteSpec
//! [`Client::route`]: client::Client::route
//! [`Client::service`]: client::Client::service
//! [`Client::try_request`]: client::Client::try_request
//! [`ClientOwner::shutdown`]: client::ClientOwner::shutdown
//! [`ClientOptions::with_external_timeouts`]: client::ClientOptions::with_external_timeouts

pub mod client;
mod extensions;
pub mod route;
pub mod server;
pub mod service;

#[cfg(test)]
#[path = "../tests/client.rs"]
mod client_tests;

#[cfg(test)]
#[path = "../tests/client_h1_duplex.rs"]
mod client_h1_duplex_tests;

#[cfg(test)]
#[path = "../tests/client_routes.rs"]
mod client_routes_tests;

#[cfg(test)]
#[path = "../tests/client_connectors.rs"]
mod client_connectors_tests;

#[cfg(test)]
#[path = "../tests/client_lifecycle.rs"]
mod client_lifecycle_tests;

#[cfg(test)]
#[path = "../tests/network_head_versions.rs"]
mod network_head_versions_tests;

#[cfg(test)]
#[path = "../tests/routes.rs"]
mod routes_tests;

#[cfg(test)]
#[path = "../tests/server_network.rs"]
mod server_network_tests;

#[cfg(test)]
#[path = "../tests/server_lifecycle.rs"]
mod server_lifecycle_tests;

#[cfg(test)]
#[path = "../tests/service.rs"]
mod service_tests;
