/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A curl-like command-line client built on `roxygen_client`.
//!
//! `ferroxygen_curl` accepts a subset of curl's options and fetches `http` and
//! `https` URLs over HTTP/1.1 or HTTP/2. Run it with Cargo:
//!
//! ```sh
//! cargo run --bin ferroxygen_curl -- -sS -L https://example.com/
//! ```
//!
//! It supports these options. Each short option also has a long form, and
//! `--help` prints a usage summary.
//!
//! - Requests: `-X`, `-H`, `-d`, `--data-raw`, `--data-binary`,
//!   `--data-urlencode`, `--json`, `-F`, `-T`, `-G`, `-u`, `-A`, `-e`, `-r`,
//!   `-b`, `--url`.
//! - Output: `-o`, `-O`, `-i`, `-I`, `-s`, `-S`, `-v`, `-f`, `-w`, `-D`,
//!   `-c`, `-C`, `-#`, `--trace`, `--trace-ascii`, `--create-dirs`,
//!   `--limit-rate`.
//! - Transfers: `-L`, `-m`, `--connect-timeout`, `--retry`, `--retry-delay`,
//!   `--retry-max-time`, `-Z`, `--compressed`.
//! - Connections: `-x`, `--proxy-user`, `--noproxy`, `--resolve`,
//!   `--dns-servers`, `-4`, `-6`, `--no-keepalive`, `--http1.1`, `--http2`.
//! - TLS: `-k`, `--cacert`, `--cert`, `--key`, `--tlsv1.2`, `--tlsv1.3`.
//! - Configuration: `-K`, `-q`, `-n`, `-V`, `-h`.
//!
//! Switches such as `--location` also accept a `--no-` form. A proxy carries
//! only `http` URLs, because CONNECT is not supported. HTTP/3 is out of scope.

fn main() {
    std::process::exit(roxygen_curl::run_from_env());
}
