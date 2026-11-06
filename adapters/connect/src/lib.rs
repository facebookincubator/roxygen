//! Experimental pooled HTTP/2 CONNECT tunnels for Tokio applications over native Roxygen
//! sessions.
//!
//! One `HostedWorker` owns an attached `HttpSessionPool` per application route key, the pool's
//! sessions, and every CONNECT stream. Tokio callers submit through bounded admission and receive
//! a `PendingTunnel` once the proxy answers 2xx, then choose its data path: a transferable `Tunnel`
//! over bounded bridges, or a relay that copies between the stream and the caller's own I/O on
//! the protocol worker. Attached sessions send HTTP/2 PING probes and drop a proxy connection that
//! stops answering them. Application connectors own the proxy address, DNS, TLS, ALPN, and socket
//! options, with optional request preparation, TLS handshake, and lazy runtime helpers.
//! `runtime` holds the Tokio-facing owner and client handle, `worker` runs on the protocol
//! worker, `tunnel` is the application side of an established tunnel and its choice, `relay` is
//! the worker-side relay, and `options`, `error`, and `stats` hold its bounds, failure
//! classification, and counters.

pub mod error;
pub mod lazy;
pub mod options;
pub mod relay;
pub mod request;
pub mod runtime;
pub mod stats;
pub mod tls;
pub mod tunnel;
mod worker;

pub use roxygen_core::connection::TransportEndpoint;
pub use roxygen_core::connection::TransportSecurity;
pub use roxygen_core::message::SocketAddress;

pub use crate::error::ConnectError;
pub use crate::error::ConnectErrorKind;
pub use crate::lazy::LazyConnectError;
pub use crate::lazy::LazyConnectRuntime;
pub use crate::options::ConnectOptions;
pub use crate::relay::RelayClose;
pub use crate::relay::RelayIo;
pub use crate::relay::RelayOutcome;
pub use crate::request::connect_request;
pub use crate::runtime::ConnectClient;
pub use crate::runtime::ConnectRuntime;
pub use crate::runtime::ConnectedIo;
pub use crate::runtime::Connector;
pub use crate::runtime::DialError;
pub use crate::runtime::ProxyIo;
pub use crate::stats::ConnectStats;
pub use crate::stats::LatencySummary;
pub use crate::tls::connect_tls;
pub use crate::tunnel::PendingTunnel;
pub use crate::tunnel::Tunnel;
pub use crate::tunnel::TunnelInfo;
