//! Worker-local byte-stream transport contracts and adapters.

pub mod byte_event;
pub mod byte_stream;
pub mod established;
pub mod tcp;
mod tcp_zero_copy;
pub mod tls;

pub use byte_stream::TransportRetirement;
pub use byte_stream::TransportRetirementError;

#[cfg(test)]
#[path = "../tests/byte_event_delivery.rs"]
mod byte_event_delivery_tests;

#[cfg(test)]
#[path = "../tests/byte_stream.rs"]
mod byte_stream_tests;

#[cfg(test)]
#[path = "../tests/established.rs"]
mod established_tests;

#[cfg(test)]
#[path = "../tests/tcp_tls.rs"]
mod tcp_tls_tests;

#[cfg(test)]
#[path = "../tests/exchange_completion.rs"]
mod exchange_completion_tests;
