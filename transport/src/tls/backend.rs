/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The rustls crypto backend behind the built-in provider, the ticketers and
//! the certificate digest: aws-lc-rs when the `aws-lc-rs` feature is on, even
//! if `ring` is on too, else ring.

#[cfg(not(any(feature = "aws-lc-rs", feature = "ring")))]
compile_error!("roxygen_transport needs the `aws-lc-rs` or `ring` feature");

#[cfg(feature = "aws-lc-rs")]
use rustls::crypto::aws_lc_rs as selected;
#[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
use rustls::crypto::ring as selected;
pub(crate) use selected::Ticketer;
pub(crate) use selected::cipher_suite;
pub(crate) use selected::default_provider;
