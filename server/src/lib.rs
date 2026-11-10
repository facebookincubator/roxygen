//! Server lifecycle primitives for Roxygen.

#![cfg_attr(all(test, target_os = "linux", fbcode_build), feature(linux_pidfd))]

pub mod acceptor;
pub mod connection;
pub mod expect_continue;
pub mod scoped_server;
pub mod server;
pub mod shutdown;

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
#[path = "../tests/native_readiness.rs"]
mod native_readiness_tests;

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/native_lifecycle_readiness.rs"]
mod native_lifecycle_readiness_tests;
