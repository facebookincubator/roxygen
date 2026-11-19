/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Native Rust output of the upstream llhttp grammar, plus its HTTP helpers.
//!
//! `generated` is emitted from llhttp 9.4.4 by
//! `codec/http1/generator/generate.cjs`. `runtime` ports `src/native/http.c` and
//! the lifecycle and callback conventions in `src/native/api.c` from the same
//! revision.

#[path = "llhttp/generated.rs"]
mod generated;
#[path = "llhttp/runtime.rs"]
mod runtime;

pub(crate) use generated::Error;
pub(crate) use generated::Event;
pub(crate) use generated::ParserType;
pub(crate) use generated::flags;
pub(crate) use generated::lenient_flags;
pub(crate) use runtime::Callbacks;
pub(crate) use runtime::Parser;

#[cfg(test)]
#[path = "llhttp/tests.rs"]
mod tests;
