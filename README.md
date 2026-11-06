# Roxygen

Roxygen is a native Rust HTTP/1.1 and HTTP/2 stack inspired by Proxygen's
coroutine HTTP stack. It provides owned HTTP types, streaming bodies,
worker-local sessions, TCP/TLS transports, and client/server APIs. Optional
adapters integrate with `http`, Hyper, Tower, and Axum.

HTTP/3 and QUIC are outside the current scope. This project is under active
development; its APIs may change.

## Build and test

Use Rust 1.98.1, the version exercised by CI:

```sh
cargo build --workspace --all-targets
cargo test --workspace --no-fail-fast
cargo test --workspace --no-fail-fast --features ferroxygen_runtime/observability
cargo clippy --workspace --all-targets --all-features
cargo doc --workspace --no-deps
```

The lockfile tracks the versions used by the internal build. Cargo may refresh
registry checksums on the first external build.

## Packages

Cargo package names start with `ferroxygen_`. Rust library names retain the
`roxygen_` prefix. For example, depend on the client from this repository with:

```toml
[dependencies]
roxygen_client = { package = "ferroxygen_client", git = "https://github.com/facebookincubator/roxygen" }
```

The workspace includes the core types, runtime, codecs, sessions, transport,
client and server, plus HTTP, Hyper, Tower, explicit-connection, and CONNECT
adapters. Packages are not yet published to crates.io.

Generate the API documentation with `cargo doc --workspace --no-deps`. The
`examples/axum_tower` package demonstrates application integration:

```sh
cargo run -p ferroxygen_axum_tower_fixture --bin axum_tower_example
```

The curl-compatible command supports a subset of curl options:

```sh
cargo run -p ferroxygen_curl -- --help
```

## License

Roxygen is licensed under the [MIT license](LICENSE).
