/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP messages, body buffers, and the source, filter and handler traits shared by the other
//! Roxygen crates.
//!
//! Applications use these types directly too, to write handlers for `roxygen_server` and to send
//! or read bodies with `roxygen_client`.
//!
//! A request or response is an [`HttpSource`](source::HttpSource), a single-consumer stream of
//! one header event followed by body events. The consumer pulls each event, and each body read
//! passes a byte limit for the event it returns. The traits do not require `Send`, and their
//! futures are not `Send`, so sources, filters and handlers stay on the worker thread that owns
//! their connection.
//!
//! Main entry points:
//!
//! - [`message::HttpMessage`] is a request or response head, with its
//!   [`headers::HttpHeaders`]. The [`url`] module parses URLs.
//! - [`buf_queue::BufQueue`] holds bytes as a queue of shared `bytes::Bytes` segments, so
//!   splitting, appending and trimming a body do not copy it. Each queue has a length limit.
//! - [`source_holder::HttpSourceHolder`] owns a source and stops it if the holder is dropped
//!   before the message ends. [`fixed_source::HttpFixedSource`] serves an in-memory message,
//!   [`stream_source::HttpStreamSource`] is fed by a separate producer, and
//!   [`hybrid_source::HttpHybridSource`] returns a given head and then forwards body reads to
//!   another source.
//! - [`handler::HttpHandler`] turns a request source into a response source.
//!   [`handler_chain::HttpHandlerChain`] links handlers, and
//!   [`filter_factory::HttpFilterFactoryHandler`] wraps each request and response in
//!   [`source_filter::HttpSourceFilter`]s.
//! - [`error::HttpError`] carries an [`error::HttpErrorCode`] and is the error type of every
//!   read and handler.
//! - [`byte_event`] defines callbacks that fire when given bytes are written, transmitted by the
//!   network card, or acknowledged by the peer.
//!
//! Reading an in-memory response four bytes at a time:
//!
//! ```
//! use roxygen_core::buf_queue::BufQueue;
//! use roxygen_core::fixed_source::HttpFixedSource;
//! use roxygen_core::source::HttpSource;
//! use roxygen_core::source_holder::HttpSourceHolder;
//! # use std::future::Future;
//! # use std::pin::pin;
//! # use std::task::{Context, Poll, Waker};
//! # fn block_on<F: Future>(future: F) -> F::Output {
//! #     match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
//! #         Poll::Ready(output) => output,
//! #         Poll::Pending => unreachable!("an in-memory source is always ready"),
//! #     }
//! # }
//! # block_on(async {
//!
//! let body = BufQueue::from_vec(b"hello world".to_vec(), usize::MAX)?;
//! let response = HttpFixedSource::make_fixed_response(200, Some(body))?;
//! let mut source = HttpSourceHolder::new(Box::new(response));
//!
//! let mut head = source.read_header_event().await?;
//! assert_eq!(head.headers_mut().status_code()?, 200);
//!
//! let mut received: Vec<u8> = Vec::new();
//! loop {
//!     let event = source.read_body_event(4).await?;
//!     if let Some(body) = event.as_body() {
//!         for segment in body.segments() {
//!             received.extend_from_slice(segment);
//!         }
//!     }
//!     if event.eom() {
//!         break;
//!     }
//! }
//! assert_eq!(received, b"hello world");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # }).unwrap();
//! ```

pub mod body_event_queue;
pub mod buf_queue;
pub mod byte_event;
pub mod connection;
pub mod egress_state;
pub mod error;
pub mod events;
pub mod exchange;
pub mod filter_chain;
pub mod filter_factory;
pub mod fixed_source;
pub mod flow_control;
pub mod handler;
pub mod handler_chain;
pub mod headers;
pub mod http;
pub mod hybrid_source;
pub mod ids;
pub mod ingress_completion;
pub mod ingress_state;
pub mod message;
mod owned_bytes;
pub mod priority;
pub mod rfc2616;
pub mod source;
pub mod source_filter;
pub mod source_holder;
pub mod source_reader;
pub mod stream_source;
pub mod stream_source_holder;
pub mod url;

#[cfg(test)]
#[path = "../tests/body_event_queue.rs"]
mod body_event_queue_tests;

#[cfg(test)]
#[path = "../tests/polled_body_reads.rs"]
mod polled_body_reads_tests;

#[cfg(test)]
#[path = "../tests/flow_control.rs"]
mod flow_control_tests;

#[cfg(test)]
#[path = "../tests/filter_factory_handler.rs"]
mod filter_factory_handler_tests;

#[cfg(test)]
#[path = "../tests/fixed_hybrid_source.rs"]
mod fixed_hybrid_source_tests;

#[cfg(test)]
#[path = "../tests/handler_chain.rs"]
mod handler_chain_tests;

#[cfg(test)]
#[path = "../tests/byte_event_registration.rs"]
mod byte_event_registration_tests;

#[cfg(test)]
#[path = "../tests/egress_state_machine.rs"]
mod egress_state_machine_tests;

#[cfg(test)]
#[path = "../tests/errors.rs"]
mod errors_tests;

#[cfg(test)]
#[path = "../tests/http_headers.rs"]
mod http_headers_tests;

#[cfg(test)]
#[path = "../tests/http_message.rs"]
mod http_message_tests;

#[cfg(test)]
#[path = "../tests/http_types.rs"]
mod http_types_tests;

#[cfg(test)]
#[path = "../tests/ingress_state_machine.rs"]
mod ingress_state_machine_tests;

#[cfg(test)]
#[path = "../tests/http_priority.rs"]
mod http_priority_tests;

#[cfg(test)]
#[path = "../tests/rfc2616.rs"]
mod rfc2616_tests;

#[cfg(test)]
#[path = "../tests/segmented_buffer.rs"]
mod segmented_buffer_tests;

#[cfg(test)]
#[path = "../tests/source_events.rs"]
mod source_events_tests;

#[cfg(test)]
#[path = "../tests/source_filter_chain.rs"]
mod source_filter_chain_tests;

#[cfg(test)]
#[path = "../tests/source_reader.rs"]
mod source_reader_tests;

#[cfg(test)]
#[path = "../tests/stream_source.rs"]
mod stream_source_tests;

#[cfg(test)]
#[path = "../tests/url.rs"]
mod url_tests;

#[cfg(test)]
#[path = "../tests/exchange_completion.rs"]
mod exchange_completion_tests;
