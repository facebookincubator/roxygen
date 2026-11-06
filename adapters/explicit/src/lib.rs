//! Experimental explicit-connection HTTP/1.1 client over Roxygen client sessions.
//!
//! `error`, `exchange`, `stats`, and `tunnel` are host-agnostic: they run one exchange, directly
//! or through a CONNECT tunnel, over any `WorkerContext` and an adopted byte stream. `runtime` is
//! the Tokio host: TCP dialing, tokio-rustls, a tunnel's proxy leg, `HostedWorker` slots, slot
//! restart, and fail-open state.

pub mod error;
pub mod exchange;
pub mod runtime;
pub mod stats;
pub mod tunnel;

pub use crate::error::ErrorKind;
pub use crate::error::ExplicitError;
pub use crate::error::Phase;
pub use crate::exchange::BodyOutcome;
pub use crate::exchange::ConnectionInfo;
pub use crate::exchange::ExchangeInput;
pub use crate::exchange::ExchangeProgress;
pub use crate::exchange::RawHeaders;
pub use crate::exchange::Request;
pub use crate::exchange::Response;
pub use crate::exchange::Timings;
pub use crate::exchange::exchange;
pub use crate::runtime::CohostExecutor;
pub use crate::runtime::CohostExecutorRequest;
pub use crate::runtime::ExplicitClient;
pub use crate::runtime::ExplicitRuntime;
pub use crate::runtime::ExplicitRuntimeConfig;
pub use crate::runtime::Security;
pub use crate::runtime::SlotHealth;
pub use crate::runtime::SlotState;
pub use crate::runtime::Target;
pub use crate::stats::ExplicitStats;
pub use crate::stats::HISTOGRAM_BUCKET_UPPER_BOUNDS_US;
pub use crate::stats::HistogramSnapshot;
pub use crate::stats::RuntimeStats;
pub use crate::tunnel::ConnectHead;
pub use crate::tunnel::Tunnel;

#[cfg(test)]
#[path = "../tests/errors.rs"]
mod errors_tests;

#[cfg(test)]
#[path = "../tests/exchange.rs"]
mod exchange_tests;

#[cfg(test)]
#[path = "../tests/runtime.rs"]
mod runtime_tests;

#[cfg(test)]
#[path = "../tests/cohost.rs"]
mod cohost_tests;

#[cfg(test)]
#[path = "../tests/tunnel.rs"]
mod tunnel_tests;
