//! Bounded Tower service adapters for worker-local Roxygen sessions.

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
