/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An HTTP/2 codec, with HPACK header compression and connection flow control.
//!
//! [`Http2Codec`] implements [`HttpCodec`](roxygen_codec::HttpCodec) for HTTP/2.
//! `roxygen_session` drives it on HTTP/2 connections, so most applications use
//! `roxygen_server` and `roxygen_client` instead of this crate. Like the HTTP/1 codec, it does
//! not perform I/O.
//!
//! - [`Http2Codec::new`] creates a codec for one side of a connection. It parses frames,
//!   decodes and validates header blocks, and reports each stream's messages as codec events.
//!   [`Http2Codec::egress_settings_mut`] changes the SETTINGS values that it sends.
//! - [`flow_control::FlowControlledCodec`] wraps a codec and keeps the connection's send and
//!   receive windows. `roxygen_session` keeps the window of each stream.
//! - [`hpack`] is the HPACK encoder and decoder (RFC 7541).
//! - [`frame`] parses and writes single frames, and [`settings`] stores SETTINGS values.
//!
//! The codec also handles server push, extended `CONNECT` (RFC 8441), RFC 9218 priority
//! updates, and graceful shutdown with GOAWAY.

pub mod codec_impl;
pub mod constants;
pub mod flow_control;
pub mod frame;
pub mod header_decode;
pub mod hpack;
pub mod parallel;
pub mod settings;
pub mod util;

pub use codec_impl::Http2Codec;

#[cfg(test)]
#[path = "../tests/flow_control.rs"]
mod flow_control_contract;

#[cfg(test)]
#[path = "../tests/framer.rs"]
mod framer_contract;

#[cfg(test)]
#[path = "../tests/hpack.rs"]
mod hpack_contract;

#[cfg(test)]
#[path = "../tests/codec.rs"]
mod codec_contract;
