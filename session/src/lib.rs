/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Worker-local HTTP session and stream lifecycle primitives.

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
