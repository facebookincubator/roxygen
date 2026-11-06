//! Streaming Hyper and `http-body` adapters for Roxygen sources.

pub mod body;
pub mod source;

#[cfg(test)]
#[path = "../tests/body_stream.rs"]
mod body_stream_tests;

#[cfg(test)]
#[path = "../tests/body_source.rs"]
mod body_source_tests;
