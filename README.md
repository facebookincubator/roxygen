# Roxygen

[![CI](https://github.com/facebookincubator/roxygen/actions/workflows/ci.yml/badge.svg)](https://github.com/facebookincubator/roxygen/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Roxygen is an HTTP/1.1 and HTTP/2 client and server library for Rust. It
brings the design of [Proxygen](https://github.com/facebook/proxygen), Meta's
C++ HTTP library, to Rust: every connection belongs to one worker thread, and
a request or response is a stream of header and body events that the
connection pulls when it is ready for more.

Roxygen is under active development. It is not published on crates.io yet, and
its APIs will change.

## Why Roxygen

- **Thread-per-core connections.** A worker is an OS thread that owns its
  connections, codecs, timers and handlers. That state does not move between
  threads, so handlers and body sources do not need to be `Send` or `Sync`.
  Other threads, including your own Tokio runtime, reach a worker through a
  cloneable `WorkerHandle`.
- **Its own protocol stack.** The HTTP/1 parser is Rust generated from
  [llhttp](https://github.com/nodejs/llhttp), the parser that Node.js uses.
  HTTP/2, HPACK, flow control and stream priorities are implemented here, and
  TLS uses [rustls](https://github.com/rustls/rustls).
- **Pull-based streaming.** A connection pulls more of a body only when it can
  send it, so a slow peer pauses the producer instead of growing a buffer.
- **Few copies.** Bodies are queues of shared `bytes::Bytes` segments, so
  forwarding, splitting and trimming a body do not copy it. On Linux the TCP
  transport can send with `MSG_ZEROCOPY`.
- **Byte events.** A sender can register a callback for when a given byte
  reaches the transport and when the kernel accepts the write that carries it.
- **Works with the ecosystem.** Optional adapters convert to and from the
  [`http`](https://docs.rs/http) and [`http-body`](https://docs.rs/http-body)
  types, serve a Tower service such as an Axum router, and provide a Tower
  client.

Pick [hyper](https://hyper.rs) instead if you need a stable release on
crates.io, HTTP/3, Windows support, or a model in which every handler is `Send`
and runs on a work-stealing runtime.

## Example

A server that answers every request with a fixed body. Roxygen is not on
crates.io yet, so depend on it from git:

```toml
[dependencies]
ferroxygen_core = { git = "https://github.com/facebookincubator/roxygen" }
ferroxygen_server = { git = "https://github.com/facebookincubator/roxygen" }
tokio = { version = "1", features = ["rt", "signal"] }
```

```rust,no_run
use std::rc::Rc;
use std::sync::Arc;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::{HttpError, HttpErrorCode};
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::{
    HandlerSessionContext, HttpHandler, HttpHandlerRef, LocalHandlerFuture,
};
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_server::server::{HandlerFactory, HttpServer, ServerConfig};

struct Hello;

impl HttpHandler for Hello {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            request.read_header_event().await?;
            let body = b"Hello, world!\n".to_vec();
            let body = BufQueue::from_vec(body, usize::MAX).map_err(internal)?;
            let response =
                HttpFixedSource::make_fixed_response(200, Some(body)).map_err(internal)?;
            Ok(HttpSourceHolder::new(Box::new(response)))
        })
    }
}

fn internal(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ServerConfig {
        // Ctrl-C is handled below.
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    config.socket.bind_addresses = vec!["127.0.0.1:3000".parse()?];
    // Each worker thread builds its own handler, which stays on that thread.
    let handlers: HandlerFactory = Arc::new(|_worker| Rc::new(Hello) as HttpHandlerRef);
    let server = HttpServer::start(config, handlers, None)?;

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(tokio::signal::ctrl_c())?;
    server.drain();
    server.wait_for_stop()?;
    Ok(())
}
```

## More examples

The [`examples`](examples) directory has complete programs. Run one with
`cargo run --bin <name>`.

| Example | What it shows |
| --- | --- |
| [`hello_server`](examples/hello_server.rs) | The handler API, over HTTP/1.1 or HTTP/2 with prior knowledge |
| [`streaming_server`](examples/streaming_server.rs) | A streamed response, and echoing a request body as it arrives |
| [`tls_server`](examples/tls_server.rs) | HTTPS with ALPN choosing HTTP/2 or HTTP/1.1 |
| [`client_get`](examples/client_get.rs) | Fetching a URL with the client |
| [`axum_router`](examples/axum_router.rs) | An Axum router served by Roxygen through the Tower adapter |

The [`curl`](curl) crate is a larger client: a command-line tool that accepts a
subset of curl's options.

```sh
cargo run --bin ferroxygen_curl -- -sS -L https://example.com/
```

## Crates

Cargo packages are named `ferroxygen_*`, and the libraries they contain are
named `roxygen_*`. Your `Cargo.toml` names the package and your code names the
library: depend on `ferroxygen_server` and write `use roxygen_server::...`.

| Package | Contents |
| --- | --- |
| `ferroxygen_core` | Messages, headers, URLs, body buffers, and the source, filter and handler traits |
| `ferroxygen_runtime` | Worker threads and their handles, timers, cancellation and task groups |
| `ferroxygen_codec`, `ferroxygen_http1`, `ferroxygen_http2` | The codec interface and the HTTP/1.x and HTTP/2 codecs |
| `ferroxygen_transport` | TCP and TLS byte streams, byte events, and adoption of connections you established |
| `ferroxygen_session` | HTTP/1.1 and HTTP/2 connection state machines |
| `ferroxygen_client` | Connector, connection pool and a simple request API |
| `ferroxygen_server` | A multi-worker server with TLS, graceful drain and `100-continue` |
| `ferroxygen_connect` | HTTP CONNECT tunnels over a client session |
| `ferroxygen_http_adapter` | Conversions to and from the `http` crate's types |
| `ferroxygen_hyper_adapter` | `http_body::Body` implementations over Roxygen bodies, and the reverse |
| `ferroxygen_tower_adapter` | Serve a Tower service on Roxygen, or use Roxygen as a Tower client |
| `ferroxygen_explicit` | Experimental HTTP/1.1 client for applications that dial their own connections |
| `ferroxygen_connect_client` | Experimental pool of CONNECT tunnels over HTTP/2 |

## Building

Roxygen builds with stable Rust. CI builds and tests it with Rust 1.98.1 on
Linux and macOS; Linux is the primary platform.

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo doc --workspace --no-deps --open
```

The runtime's `observability` feature reports task spans to the `tracing` crate:

```sh
cargo test --workspace --features ferroxygen_runtime/observability
```

`Cargo.lock` tracks the versions used by Meta's internal build, so Cargo may
refresh registry checksums on the first build.

HTTP/3, QUIC and WebTransport are out of scope for now.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Roxygen is developed in Meta's internal
repository and exported here; pull requests are imported and land there first.
Report security issues as described in [SECURITY.md](SECURITY.md). Everyone
taking part in the project is expected to follow the
[code of conduct](CODE_OF_CONDUCT.md).

## License

Roxygen is licensed under the [MIT license](LICENSE). The HTTP/1 parser is
derived from llhttp, which is also MIT licensed.
