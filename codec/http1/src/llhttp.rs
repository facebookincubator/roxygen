/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The HTTP/1 parser, generated from the llhttp grammar.
//!
//! `llhttp/generated.rs` is produced from unmodified [llhttp] 9.4.4, revision
//! `0ae8c68c3c5a8fce13dbff87190e1ba40d8f9666`, by `codec/http1/generator/generate.cjs`.
//! Do not edit it by hand. `llhttp/runtime.rs` is a hand-written port of the HTTP decisions in
//! upstream `src/native/http.c` and of the lifecycle and callback conventions in
//! `src/native/api.c` from the same revision. Building and testing need only Rust.
//!
//! The codec leaves all of the parser's leniency flags off. `Parser::finish` signals EOF, and
//! `execute` with empty input resumes pending transitions without signaling EOF. A callback
//! pauses the parser by returning `Error::PAUSED.0`. To continue, call `resume` and then pass
//! the unconsumed suffix to `execute`.
//!
//! To regenerate, with Node 20 or later, run from the repository root:
//!
//! ```text
//! git clone https://github.com/nodejs/llhttp.git /tmp/llhttp
//! git -C /tmp/llhttp checkout --detach 0ae8c68c3c5a8fce13dbff87190e1ba40d8f9666
//! npm --prefix /tmp/llhttp ci
//! node codec/http1/generator/generate.cjs --upstream /tmp/llhttp --output codec/http1/src/llhttp
//! rustfmt --edition 2024 codec/http1/src/llhttp/generated.rs
//! ```
//!
//! The generator checks the llhttp version, the versions of llparse (7.3.1), llparse-frontend
//! (3.0.0) and llparse-builder (1.5.2), and the SHA-256 hashes of the grammar sources and of
//! `src/native/http.c` and `src/native/api.c`. If a native-helper hash differs, generation
//! fails. Review `llhttp/runtime.rs` against the new C code before updating that hash in
//! `generate.cjs`. Before accepting new output, compare its events, errors and consumed byte
//! counts with those of the C parser from the same revision, including for fragmented input,
//! pausing callbacks, EOF and leniency flags.
//!
//! The parts derived from llhttp are MIT licensed, Copyright (c) 2018 Fedor Indutny. The
//! license text is in `llhttp/LICENSE`.
//!
//! [llhttp]: https://github.com/nodejs/llhttp

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
