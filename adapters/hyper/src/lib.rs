/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `http_body::Body` implementations over Roxygen body sources, and Roxygen
//! sources that read an `http_body::Body`.
//!
//! A Roxygen body is an [`HttpSource`] that stays on one worker thread, while
//! an `http_body::Body` is polled by the application, usually on Tokio. The
//! types here connect the two and keep each side's state on its own thread.
//! Each bridge has at most one read in flight, so a slow reader pauses the
//! producer on the other side. Trailers pass through in both directions. Most
//! applications use `roxygen_tower_adapter`, which carries request and response
//! bodies with these types.
//!
//! - [`RoxygenBody`] is a `Send` `http_body::Body` that reads a source owned by
//!   a worker. [`RoxygenBody::new`] creates the source on the worker from a
//!   `Send` factory, [`RoxygenBody::try_from_local`] exports a source that is
//!   already there, and [`RoxygenBody::prepare_local`] registers the body
//!   before the exchange that produces its source starts. Dropping the body
//!   requests cancellation of the source on its worker.
//! - [`ApplicationBodySource`] is an [`HttpSource`] that reads an application's
//!   `http_body::Body`. [`ApplicationBodySource::prepare`] runs on the worker
//!   and returns the source with a `Send` [`ApplicationBodyAttachment`].
//!   Calling `attach` on the application's Tokio runtime spawns the body there,
//!   and that runtime must keep running until the worker has quiesced.
//! - [`admit_owned_bytes`] moves `Bytes` into a bounded `BufQueue` without
//!   copying, and [`admit_untrusted_bytes`] copies them.
//!
//! [`HttpSource`]: roxygen_core::source::HttpSource
//! [`RoxygenBody`]: body::RoxygenBody
//! [`RoxygenBody::new`]: body::RoxygenBody::new
//! [`RoxygenBody::try_from_local`]: body::RoxygenBody::try_from_local
//! [`RoxygenBody::prepare_local`]: body::RoxygenBody::prepare_local
//! [`ApplicationBodySource`]: source::ApplicationBodySource
//! [`ApplicationBodySource::prepare`]: source::ApplicationBodySource::prepare
//! [`ApplicationBodyAttachment`]: source::ApplicationBodyAttachment
//! [`admit_owned_bytes`]: body::admit_owned_bytes
//! [`admit_untrusted_bytes`]: body::admit_untrusted_bytes

/// Exposes a Roxygen source owned by a worker as an `http_body::Body`.
pub mod body;
pub mod source;

#[cfg(test)]
#[path = "../tests/body_stream.rs"]
mod body_stream_tests;

#[cfg(test)]
#[path = "../tests/body_source.rs"]
mod body_source_tests;
