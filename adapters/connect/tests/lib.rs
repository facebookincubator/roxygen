/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Black-box pooled CONNECT tests against independent `h2` proxy peers.

mod capacity;
mod failures;
mod lifecycle;
mod liveness;
mod proxy;
mod relay;
mod stats;
mod tls;
mod tunnel;
