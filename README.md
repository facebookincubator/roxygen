# Roxygen

[![CI](https://github.com/facebookincubator/roxygen/actions/workflows/ci.yml/badge.svg)](https://github.com/facebookincubator/roxygen/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)][license]

Roxygen is an HTTP/1.1 and HTTP/2 client and server library for Rust. It
brings the design of [Proxygen](https://github.com/facebook/proxygen), Meta's
C++ HTTP library, to Rust: every connection belongs to one worker thread, and
a request or response is a stream of header and body events that the
connection pulls when it is ready for more.

Roxygen is under active development, and its APIs will change before 1.0.

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
- **One future per connection.** The experimental owned drivers poll a
  connection's codec, handlers and bodies from a single future, without a task
  per request. On the server this is a configuration switch, and an
  `HttpHandler` runs on it unchanged. See [Session drivers](#session-drivers).
- **Few copies.** Bodies are queues of shared `bytes::Bytes` segments, so
  forwarding, splitting and trimming a body do not copy it. On Linux the TCP
  transport can send with `MSG_ZEROCOPY`.
- **Byte events.** A sender can register a callback for when a given byte
  reaches the transport and when the kernel accepts the write that carries it.
- **Works with the ecosystem.** Optional adapters convert to and from the
  [`http`](https://docs.rs/http) and [`http-body`](https://docs.rs/http-body)
  types, serve a Tower service such as an Axum router, and provide a Tower
  client.

## Performance

Roxygen is in the top 3 on the HTTP/2 profiles of
[HttpArena](https://github.com/MDA2AV/HttpArena). An `HttpServer` on the owned
HTTP/2 driver serves:

| Profile | Requests per second |
| --- | --- |
| `baseline-h2`: query parsing over TLS | 16.4M |
| `baseline-h2c`: query parsing over cleartext HTTP/2 | 16.6M |
| `static-h2`: static files over TLS | 2.1M |

The server ran on 64 CPUs of an AMD EPYC 9004 machine, with h2load on 64
others. Each request costs the server about 2.3 µs of CPU.

## Example

A server that answers every request with a fixed body:

```toml
[dependencies]
ferroxygen_core = "0.1"
ferroxygen_server = "0.1"
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

## Session drivers

Roxygen has two drivers for a connection. The owned variants are recommended
for higher performance, subject to the limits below.

- **`UniplexSession`** is the default, and `HttpServer` and `ClientSession` use
  it. On the server, it runs each request's handler as a separate task on the
  worker.
- **The owned drivers** (`roxygen_session::owned_h1` and `owned_h2`) are
  experimental. Each connection runs as one future, which polls the codec, the
  handlers and the body sources in place and does not spawn a task for each
  request.

Both drivers keep a connection on one worker thread, so handlers and body
sources do not need to be `Send` under either.

On the server, each switch moves the connections of one protocol to its owned
driver, and an `HttpHandler` runs there unchanged:

```rust
// The owned HTTP/2 driver applies the session timeouts.
config.session.owned_http2 = true;
// The owned HTTP/1.1 driver does not apply timeouts, so these must be zero.
// HTTP/2 connections use the same fields, so this leaves them untimed too.
config.session.owned_http1 = true;
config.session.stream_read_timeout = Duration::ZERO;
config.session.connection_idle_timeout = Duration::ZERO;
config.session.write_timeout = Duration::ZERO;
```

The server adapts each `HttpHandler` to the drivers' `OwnedApplication` trait,
so each request still has a boxed handler future and boxed request and
response sources. An application that implements `OwnedApplication` itself
exchanges header and body events with the driver directly, but it runs the
drivers on connections it accepts, and that path does not have an example yet.

On the client, `roxygen_client::experimental::owned` provides
`OwnedClientSession` and a pool, `OwnedHttpSessionPool`. You reserve a slot,
submit a request head and body, and read the response from the
`HttpResponseSource` that the submission returns. See
[`examples/owned_server.rs`][owned-server] and
[`examples/owned_client.rs`][owned-client].

Current limits of the owned drivers:

- The HTTP/1.1 driver does not apply timeouts in either direction. On a
  server, an idle HTTP/1.1 connection stays open until the client closes it or
  the server drains, and a drain waits for a stalled request until the client
  leaves or `force_stop` runs. The driver also cancels byte events instead of
  firing them.
- The owned server modes do not record statistics, so `stats_factory` must be
  `None`, and `owned_http1` does not support lifecycle observers. The Tower
  adapter rejects both modes, because the owned drivers do not yet report when
  an exchange's cleanup finishes.
- `OwnedClientSession::connect` and the pool dial plaintext TCP only. For TLS,
  set up the connection yourself and pass it to `OwnedClientSession::attach`,
  as the example does. Owned client sessions do not support lifecycle
  observers, session statistics or PING probes, and HTTP/2 sessions refuse
  server push. Dropping every handle does not close a session, so call
  `close_when_idle`.
- `HttpServer` and the owned client reject settings that the owned drivers do
  not support when they start, and the error names the setting.

## More examples

The [`examples`][examples-dir] directory has complete programs. Run one with
`cargo run --bin <name>`.

| Example | What it shows |
| --- | --- |
| [`hello_server`][hello-server] | The handler API, over HTTP/1.1 or HTTP/2 with prior knowledge |
| [`owned_server`][owned-server] | The `hello_server` handler on the experimental owned drivers, without a task per request |
| [`streaming_server`][streaming-server] | A streamed response, and echoing a request body as it arrives |
| [`tls_server`][tls-server] | HTTPS with ALPN choosing HTTP/2 or HTTP/1.1 |
| [`client_get`][client-get] | Fetching a URL with the client |
| [`owned_client`][owned-client] | Fetching a URL with the experimental owned client session |
| [`axum_router`][axum-router] | An Axum router served by Roxygen through the Tower adapter |

The [`curl`][curl-dir] crate is a larger client: a command-line tool that accepts a
subset of curl's options.

```sh
cargo run --bin ferroxygen_curl -- -sS -L https://example.com/
```

`cargo install ferroxygen_curl` installs it from crates.io.

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
| `ferroxygen_session` | HTTP/1.1 and HTTP/2 connection state machines, including the experimental owned drivers |
| `ferroxygen_client` | Connector, connection pool, a simple request API, and an experimental owned client |
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

See [CONTRIBUTING.md][contributing]. Roxygen is developed in Meta's internal
repository and exported here; pull requests are imported and land there first.
Report security issues as described in [SECURITY.md][security]. Everyone
taking part in the project is expected to follow the
[code of conduct][code-of-conduct].

## License

Roxygen is licensed under the [MIT license][license]. The HTTP/1 parser is
derived from llhttp, which is also MIT licensed.

[axum-router]: https://github.com/facebookincubator/roxygen/blob/main/examples/axum_router.rs
[client-get]: https://github.com/facebookincubator/roxygen/blob/main/examples/client_get.rs
[code-of-conduct]: https://github.com/facebookincubator/roxygen/blob/main/CODE_OF_CONDUCT.md
[contributing]: https://github.com/facebookincubator/roxygen/blob/main/CONTRIBUTING.md
[curl-dir]: https://github.com/facebookincubator/roxygen/tree/main/curl
[examples-dir]: https://github.com/facebookincubator/roxygen/tree/main/examples
[hello-server]: https://github.com/facebookincubator/roxygen/blob/main/examples/hello_server.rs
[license]: https://github.com/facebookincubator/roxygen/blob/main/LICENSE
[owned-client]: https://github.com/facebookincubator/roxygen/blob/main/examples/owned_client.rs
[owned-server]: https://github.com/facebookincubator/roxygen/blob/main/examples/owned_server.rs
[security]: https://github.com/facebookincubator/roxygen/blob/main/SECURITY.md
[streaming-server]: https://github.com/facebookincubator/roxygen/blob/main/examples/streaming_server.rs
[tls-server]: https://github.com/facebookincubator/roxygen/blob/main/examples/tls_server.rs
