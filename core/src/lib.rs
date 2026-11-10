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
