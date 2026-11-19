/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Unstable APIs.
//!
//! Every item in this module may change or be removed in any release.
//!
//! - [`StaticFiles`] serves the files under one directory from a per-worker cache that a `stat`
//!   on every request keeps in step with the disk, with optional precompressed `.br` and `.gz`
//!   siblings.

mod static_files;

pub use static_files::StaticFiles;
