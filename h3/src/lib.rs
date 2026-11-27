/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/3 for Roxygen.
//!
//! [`codec`] holds the wire layer: frames, SETTINGS, unidirectional stream headers and error
//! codes, for an HTTP/3 endpoint on any QUIC transport. It does not perform I/O, and each of its
//! decoders keeps only the state of one stream.
#![cfg_attr(
    target_os = "linux",
    doc = "",
    doc = "[`H3Application`] serves HTTP/3 over mvfstr: it is the driver's application for one \
           endpoint, and runs an [`H3Session`] on each connection that hosts a Roxygen \
           [`OwnedApplication`](roxygen_session::owned::OwnedApplication) unchanged."
)]
//!
//! The server session runs on mvfstr, which is Linux only, so it is built only on Linux. On other
//! systems the crate holds just [`codec`].

pub mod codec;
#[cfg(target_os = "linux")]
mod credit;
#[cfg(target_os = "linux")]
mod fields;
#[cfg(target_os = "linux")]
mod session;
#[cfg(target_os = "linux")]
mod transport;

#[cfg(target_os = "linux")]
pub use session::H3Application;
#[cfg(target_os = "linux")]
pub use session::H3ApplicationBuilder;
#[cfg(target_os = "linux")]
pub use session::H3ConfigError;
#[cfg(target_os = "linux")]
pub use session::H3Session;
#[cfg(target_os = "linux")]
pub use session::H3Settings;

#[cfg(test)]
#[path = "../tests/errors.rs"]
mod errors_contract;

#[cfg(test)]
#[path = "../tests/varint.rs"]
mod varint_contract;

#[cfg(test)]
#[path = "../tests/frames.rs"]
mod frames_contract;

#[cfg(test)]
#[path = "../tests/settings.rs"]
mod settings_contract;

#[cfg(test)]
#[path = "../tests/stream_types.rs"]
mod stream_types_contract;

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/allocations.rs"]
mod allocations;

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/fake.rs"]
mod fake;

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/session.rs"]
mod session_contract;
