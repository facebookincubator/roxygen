/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Experimental client APIs. They may change or be removed.
//!
//! - [`owned`]: client sessions on the owned upstream drivers
//!   (`roxygen_session::owned_h1::OwnedH1UpstreamSession` and
//!   `roxygen_session::owned_h2::OwnedH2UpstreamSession`), which run one
//!   future per connection with no task per request, and a session pool for
//!   them with `HttpSessionPool`'s scheduler. They reject the session
//!   parameters they cannot support instead of ignoring them.
//! - [`owned::OwnedClientReservation::submit_pull_through`]: HTTP/1.1 owned
//!   exchanges whose response body reads drive the connection
//!   (`roxygen_session::owned_h1::OwnedH1UpstreamSession::run_pull_through`).

pub mod owned;

#[cfg(test)]
#[path = "../tests/owned_client.rs"]
mod owned_client_tests;

#[cfg(test)]
#[path = "../tests/owned_pool.rs"]
mod owned_pool_tests;

#[cfg(test)]
#[path = "../tests/owned_h1_egress.rs"]
mod owned_h1_egress_tests;
