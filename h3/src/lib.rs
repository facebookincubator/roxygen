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
//!
//! [`H3Application`] serves HTTP/3 over mvfstr: it is the driver's application for one endpoint,
//! and runs an [`H3Session`] on each connection that hosts a Roxygen
//! [`OwnedApplication`](roxygen_session::owned::OwnedApplication) unchanged.

pub mod codec;
mod credit;
mod fields;
mod session;
mod transport;

pub use session::H3Application;
pub use session::H3ApplicationBuilder;
pub use session::H3ConfigError;
pub use session::H3Session;
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

#[cfg(test)]
#[path = "../tests/fake.rs"]
mod fake;

#[cfg(test)]
#[path = "../tests/session.rs"]
mod session_contract;
