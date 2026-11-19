/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Streaming Hyper and `http-body` adapters for Roxygen sources.

pub mod body;
pub mod source;

#[cfg(test)]
#[path = "../tests/body_stream.rs"]
mod body_stream_tests;

#[cfg(test)]
#[path = "../tests/body_source.rs"]
mod body_source_tests;
