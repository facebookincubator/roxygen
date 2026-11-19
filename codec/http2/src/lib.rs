/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/2 framing, flow control, and HPACK (RFC 7541).

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
