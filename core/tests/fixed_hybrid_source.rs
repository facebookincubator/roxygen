/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;

use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpBodyEventKind;
use crate::events::HttpBodyEventType;
use crate::events::HttpHeaderEvent;
use crate::fixed_source::DECIMAL_DIGITS;
use crate::fixed_source::FixedSourceError;
use crate::fixed_source::HttpErrorSource;
use crate::fixed_source::HttpFixedSource;
use crate::fixed_source::format_decimal;
use crate::fixed_source::get_error_response;
use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::http::HttpHeaderSize;
use crate::http::HttpMethod;
use crate::hybrid_source::HttpHybridSource;
use crate::ids::StreamId;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_holder::HttpSourceHolder;
use crate::url::ParseUrl;

fn ready<T>(future: LocalSourceFuture<T>) -> T {
    let mut future = future;
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("fixed source unexpectedly returned a pending future"),
    }
}

fn queue(bytes: &'static [u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).unwrap()
}

fn request(path: &[u8]) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Get).unwrap();
    message.set_url(path, false).unwrap();
    message
}

fn response(status: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    message
}

fn event_bytes(event: &HttpBodyEvent) -> Vec<u8> {
    let body = event.as_body().expect("expected a body event");
    body.coalesce(body.chain_length()).unwrap().to_vec()
}

#[derive(Default)]
struct SourceObservations {
    stops: usize,
    drops: usize,
    maximums: Vec<u32>,
    timeouts: Vec<Duration>,
}

struct ObservedBodySource {
    observations: Rc<RefCell<SourceObservations>>,
    body: Option<HttpBodyEvent>,
    stream_id: StreamId,
}

impl HttpSource for ObservedBodySource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"observed body source has no header event",
        ))))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.observations.borrow_mut().maximums.push(maximum);
        let event = self
            .body
            .take()
            .unwrap_or_else(|| HttpBodyEvent::body(BufQueue::new(maximum as usize), true));
        Box::pin(std::future::ready(Ok(event)))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.observations.borrow_mut().stops += 1;
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        Some(self.stream_id)
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.observations.borrow_mut().timeouts.push(timeout);
    }
}

impl Drop for ObservedBodySource {
    fn drop(&mut self) {
        self.observations.borrow_mut().drops += 1;
    }
}

mod fixed_source {
    use super::*;

    #[test]
    fn basic_copy() {
        let mut request_source = HttpFixedSource::new(request(b"https://test.facebook.com/"), None);
        let mut request_copy = request_source.copy_fixed();
        assert!(!std::ptr::eq(
            request_source.message().unwrap(),
            request_copy.message().unwrap()
        ));

        let headers = ready(request_source.read_header_event()).unwrap();
        let headers_copy = ready(request_copy.read_header_event()).unwrap();
        assert_eq!(headers.eom(), headers_copy.eom());
        let mut message = headers.into_headers();
        let mut message_copy = headers_copy.into_headers();
        assert_eq!(
            message.method_string().unwrap(),
            message_copy.method_string().unwrap()
        );
        assert_eq!(message.url().unwrap(), message_copy.url().unwrap());
    }

    #[test]
    fn basic_move() {
        let mut request_source = HttpFixedSource::new(request(b"https://test.facebook.com/"), None);
        let mut request_move = request_source.take_fixed();

        assert!(request_source.message().is_none());
        assert!(request_source.body().is_empty());
        let headers = ready(request_move.read_header_event()).unwrap();
        let mut message = headers.into_headers();
        assert_eq!(message.method_string().unwrap(), "GET");
        assert_eq!(message.url().unwrap(), b"https://test.facebook.com/");
    }

    #[test]
    fn copy_and_move_only_transfer_message_and_body() {
        let mut source = HttpFixedSource::new(request(b"/"), Some(queue(b"body")));
        source.push_event(HttpBodyEvent::push_promise(request(b"/push"), None, false));
        source.set_trailers(HttpHeaders::new());
        source
            .message_mut()
            .unwrap()
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"999");

        let copy = source.copy_fixed();
        assert_eq!(copy.body().chain_length(), 4);
        assert_eq!(
            copy.message()
                .unwrap()
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"4"
        );
        assert_eq!(copy.pushes_len(), 0);
        assert!(!copy.has_trailers());
        assert_eq!(source.pushes_len(), 1);
        assert!(source.has_trailers());

        let moved = source.take_fixed();
        assert!(source.message().is_none());
        assert!(source.body().is_empty());
        assert_eq!(source.pushes_len(), 1);
        assert!(source.has_trailers());
        assert_eq!(moved.body().chain_length(), 4);
        assert_eq!(
            moved
                .message()
                .unwrap()
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"4"
        );
        assert_eq!(moved.pushes_len(), 0);
        assert!(!moved.has_trailers());
    }

    #[test]
    fn fixed_event_order_and_bounded_body_reads() {
        let mut source = HttpFixedSource::make_fixed_response(200, Some(queue(b"abcdef"))).unwrap();
        source.push_event(HttpBodyEvent::push_promise(request(b"/push"), None, false));
        let mut trailers = HttpHeaders::new();
        trailers.add(b"x-trailer", b"value");
        source.set_trailers(trailers);

        let headers = ready(source.read_header_event()).unwrap();
        assert!(!headers.eom());
        assert_eq!(
            headers
                .headers()
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"6"
        );

        let push = ready(source.read_body_event(1)).unwrap();
        assert_eq!(push.event_type(), HttpBodyEventType::PushPromise);
        assert!(!push.eom());

        let zero = ready(source.read_body_event(0)).unwrap();
        assert_eq!(event_bytes(&zero), b"");
        assert!(!zero.eom());
        assert_eq!(source.body().chain_length(), 6);

        for expected in [b"ab".as_slice(), b"cd".as_slice(), b"ef".as_slice()] {
            let body = ready(source.read_body_event(2)).unwrap();
            assert_eq!(event_bytes(&body), expected);
            assert!(!body.eom());
        }
        let trailers = ready(source.read_body_event(u32::MAX)).unwrap();
        assert_eq!(trailers.event_type(), HttpBodyEventType::Trailers);
        assert!(trailers.eom());
        let HttpBodyEventKind::Trailers(headers) = trailers.kind() else {
            panic!("trailers changed event kind");
        };
        assert_eq!(headers.get_single_or_empty(b"x-trailer"), b"value");
    }

    /// Polling the source in place yields the events that its read futures yield.
    #[test]
    fn polled_events_match_read_events() {
        let make = || {
            let mut source =
                HttpFixedSource::make_fixed_response(200, Some(queue(b"abcdef"))).unwrap();
            source.push_event(HttpBodyEvent::padding(3, false));
            let mut trailers = HttpHeaders::new();
            trailers.add(b"x-trailer", b"value");
            source.set_trailers(trailers);
            source
        };
        let mut read = make();
        let mut polled = make();
        assert!(polled.polls_header_events());
        assert!(polled.polls_body_events());
        let mut context = Context::from_waker(Waker::noop());

        let expected = ready(read.read_header_event()).unwrap();
        let Poll::Ready(Ok(headers)) = polled.poll_header_event(&mut context) else {
            panic!("the header poll is ready");
        };
        assert_eq!(headers.eom(), expected.eom());
        assert_eq!(
            headers
                .headers()
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"6"
        );

        for maximum in [1, 0, 4, 4, u32::MAX] {
            let expected = ready(read.read_body_event(maximum)).unwrap();
            let Poll::Ready(Ok(event)) = polled.poll_body_event(&mut context, maximum) else {
                panic!("the body poll is ready");
            };
            assert_eq!(
                event.event_type(),
                expected.event_type(),
                "maximum {maximum}"
            );
            assert_eq!(event.eom(), expected.eom(), "maximum {maximum}");
            if expected.as_body().is_some() {
                assert_eq!(event_bytes(&event), event_bytes(&expected));
            }
        }
        let Poll::Ready(Err(error)) = polled.poll_header_event(&mut context) else {
            panic!("a second header poll fails");
        };
        assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    }

    /// A holder takes each event of a fixed source when its read is first polled. A
    /// read dropped unpolled leaves the event, and a read started before a clear fails.
    #[test]
    fn holder_reads_take_fixed_events_when_first_polled() {
        let fixed =
            || Box::new(HttpFixedSource::make_fixed_response(200, Some(queue(b"abc"))).unwrap());
        let mut holder = HttpSourceHolder::new(fixed());
        drop(holder.read_header_event());
        assert!(!ready(holder.read_header_event()).unwrap().eom());
        drop(holder.read_body_event(16));
        let body = ready(holder.read_body_event(16)).unwrap();
        assert_eq!(event_bytes(&body), b"abc");
        assert!(body.eom());

        let mut holder = HttpSourceHolder::new(fixed());
        let header = holder.read_header_event();
        holder.clear(None);
        let error = ready(header).err().expect("the header read fails");
        assert_eq!(error.code(), HttpErrorCode::DROPPED);

        let mut holder = HttpSourceHolder::new(fixed());
        ready(holder.read_header_event()).unwrap();
        let body = holder.read_body_event(16);
        holder.clear(None);
        let error = ready(body).err().expect("the body read fails");
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
    }

    #[test]
    fn format_decimal_matches_to_string() {
        let mut digits = [0; DECIMAL_DIGITS];
        for value in [0, 9, 10, 99, 100, 12345, usize::MAX] {
            assert_eq!(
                format_decimal(value, &mut digits),
                value.to_string().as_bytes()
            );
        }
    }

    #[test]
    fn header_eom_depends_on_body_not_queued_metadata() {
        let mut source = HttpFixedSource::new(response(200), None);
        source.push_event(HttpBodyEvent::padding(3, false));
        source.set_trailers(HttpHeaders::new());

        let headers = ready(source.read_header_event()).unwrap();
        assert!(headers.eom());
        assert_eq!(source.pushes_len(), 1);
        assert!(source.has_trailers());
    }

    #[test]
    fn absolute_url_factory_sets_relative_target_secure_and_host() {
        let url = ParseUrl::parse_url(b"https://example.com:443/a/b?q=1", false).unwrap();
        let mut source =
            HttpFixedSource::make_fixed_request_url(&url, HttpMethod::Post, Some(queue(b"x")))
                .unwrap();
        let headers = ready(source.read_header_event()).unwrap();
        let mut message = headers.into_headers();
        assert_eq!(message.method_string().unwrap(), "POST");
        assert_eq!(message.url().unwrap(), b"/a/b?q=1");
        assert!(message.is_secure());
        assert_eq!(
            message.headers().get_single_or_empty(HttpHeaderCode::Host),
            b"example.com"
        );

        let invalid = ParseUrl::parse_url_maybe_invalid(b"https://[", false);
        assert!(matches!(
            HttpFixedSource::make_fixed_request_url(&invalid, HttpMethod::Get, None),
            Err(FixedSourceError::InvalidUrl)
        ));
    }

    #[test]
    fn body_only_and_terminal_empty_body() {
        let mut body_source = HttpFixedSource::make_fixed_body(queue(b"xyz"));
        let first = ready(body_source.read_body_event(2)).unwrap();
        assert_eq!(event_bytes(&first), b"xy");
        assert!(!first.eom());
        let second = ready(body_source.read_body_event(2)).unwrap();
        assert_eq!(event_bytes(&second), b"z");
        assert!(second.eom());

        let header_error = match ready(body_source.read_header_event()) {
            Ok(_) => panic!("body-only source unexpectedly produced headers"),
            Err(error) => error,
        };
        assert_eq!(header_error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    }

    #[test]
    fn error_source_yields_owned_error_once() {
        let mut source = HttpErrorSource::new(HttpError::with_message(
            HttpErrorCode::READ_TIMEOUT,
            b"deadline",
        ));
        let error = match ready(source.read_header_event()) {
            Ok(_) => panic!("error source unexpectedly produced headers"),
            Err(error) => error,
        };
        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
        assert_eq!(error.message(), b"deadline");
        let second = match ready(source.read_header_event()) {
            Ok(_) => panic!("consumed error source unexpectedly produced headers"),
            Err(error) => error,
        };
        assert_eq!(second.code(), HttpErrorCode::DROPPED);

        let mut body_source =
            HttpErrorSource::new(HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR));
        let body_error = match ready(body_source.read_body_event(1)) {
            Ok(_) => panic!("error source unexpectedly produced a body"),
            Err(error) => error,
        };
        assert_eq!(body_error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    }

    #[test]
    fn error_response_sets_status_body_length_and_non_keepalive() {
        let mut source = get_error_response(503, b"unavailable").unwrap();
        let headers = ready(source.read_header_event()).unwrap();
        assert!(!headers.eom());
        let mut message = headers.into_headers();
        assert_eq!(message.status_code().unwrap(), 503);
        assert_eq!(message.status_message().unwrap(), b"Service Unavailable");
        assert!(!message.wants_keepalive());
        assert_eq!(
            message
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"11"
        );
        let body = ready(source.read_body_event(u32::MAX)).unwrap();
        assert_eq!(event_bytes(&body), b"unavailable");
        assert!(body.eom());

        let mut empty = get_error_response(500, b"").unwrap();
        let empty_headers = ready(empty.read_header_event()).unwrap();
        assert!(empty_headers.eom());
        assert_eq!(
            empty_headers
                .headers()
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"0"
        );
    }
}

mod hybrid_source {
    use super::*;

    #[test]
    fn preserves_upstream_holder_ownership() {
        let fixed = HttpFixedSource::make_get_request(b"/request", Some(queue(b"abcdef"))).unwrap();
        let mut upstream = HttpSourceHolder::new(Box::new(fixed));
        let mut original_headers = ready(upstream.read_header_event()).unwrap();
        assert!(!original_headers.eom());
        let encoded = Rc::new(Cell::new(false));
        let observed = Rc::clone(&encoded);
        original_headers.set_egress_headers_callback(move |_| observed.set(true));

        let hybrid = HttpHybridSource::from_header_event(original_headers, upstream);
        assert!(hybrid.has_upstream());
        assert_eq!(hybrid.filter_name(), "HybridSource");
        let mut combined = HttpSourceHolder::new(Box::new(hybrid));
        assert!(combined.readable());

        let mut headers = ready(combined.read_header_event()).unwrap();
        assert!(!headers.eom());
        assert!(headers.on_headers_encoded(HttpHeaderSize::default()));
        assert!(encoded.get());
        assert!(!headers.on_headers_encoded(HttpHeaderSize::default()));
        let first = ready(combined.read_body_event(2)).unwrap();
        assert_eq!(event_bytes(&first), b"ab");
        assert!(!first.eom());
        let second = ready(combined.read_body_event(8)).unwrap();
        assert_eq!(event_bytes(&second), b"cdef");
        assert!(second.eom());
        assert!(!combined.readable());
    }

    #[test]
    fn stop_without_upstream_is_terminal_once() {
        let hybrid = HttpHybridSource::new(request(b"/"), None).unwrap();
        assert!(!hybrid.has_upstream());
        assert_eq!(hybrid.get_stream_id(), None);
        let mut combined = HttpSourceHolder::new(Box::new(hybrid));
        assert!(combined.readable());

        combined.stop_reading(None);
        assert!(!combined.readable());
        combined.stop_reading(None);
        assert!(!combined.readable());
    }

    #[test]
    fn header_event_only_reads_as_an_empty_upstream() {
        let header = || HttpHeaderEvent::new(request(b"/"), true).unwrap();
        for only in [false, true] {
            let mut hybrid = if only {
                HttpHybridSource::from_header_event_only(header())
            } else {
                HttpHybridSource::from_header_event(header(), HttpSourceHolder::empty())
            };
            assert!(!hybrid.has_upstream());
            assert_eq!(hybrid.get_stream_id(), None);
            assert!(hybrid.ingress_completion().is_none());
            hybrid.set_read_timeout(Duration::from_millis(5));
            assert!(ready(hybrid.read_header_event()).unwrap().eom());
            let errors = [
                ready(hybrid.read_header_event()).err(),
                ready(hybrid.read_body_event(1)).err(),
            ];
            for error in errors {
                let error = error.expect("the empty upstream fails the read");
                assert_eq!(error.code(), HttpErrorCode::DROPPED, "only {only}");
                assert_eq!(error.message(), b"HTTP source is no longer readable");
            }
            hybrid.stop_reading(None);
        }
    }

    #[test]
    fn attached_stop_forwards_once_and_destroys_upstream() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let upstream = HttpSourceHolder::new(Box::new(ObservedBodySource {
            observations: Rc::clone(&observations),
            body: Some(HttpBodyEvent::body(queue(b"body"), true)),
            stream_id: StreamId::new(31),
        }));
        let hybrid = HttpHybridSource::new(request(b"/"), Some(upstream)).unwrap();
        let mut combined = HttpSourceHolder::new(Box::new(hybrid));

        combined.stop_reading(Some(HttpErrorCode::CANCEL));
        assert_eq!(observations.borrow().stops, 1);
        assert_eq!(observations.borrow().drops, 1);
        combined.stop_reading(None);
        assert_eq!(observations.borrow().stops, 1);
        assert_eq!(observations.borrow().drops, 1);
    }

    #[test]
    fn body_limit_timeout_and_stream_id_forward_to_upstream() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let upstream = HttpSourceHolder::new(Box::new(ObservedBodySource {
            observations: Rc::clone(&observations),
            body: Some(HttpBodyEvent::padding(17, true)),
            stream_id: StreamId::new(91),
        }));
        let hybrid = HttpHybridSource::new(request(b"/"), Some(upstream)).unwrap();
        let mut combined = HttpSourceHolder::new(Box::new(hybrid));

        assert_eq!(combined.get_stream_id(), Some(StreamId::new(91)));
        combined.set_read_timeout(Duration::from_millis(125));
        let headers = ready(combined.read_header_event()).unwrap();
        assert!(!headers.eom());
        let body = ready(combined.read_body_event(7)).unwrap();
        assert_eq!(body.event_type(), HttpBodyEventType::Padding);
        assert!(body.eom());
        assert_eq!(observations.borrow().maximums, [7]);
        assert_eq!(observations.borrow().timeouts, [Duration::from_millis(125)]);
        assert_eq!(observations.borrow().stops, 0);
        assert_eq!(observations.borrow().drops, 1);
    }

    #[test]
    fn later_header_reads_forward_to_upstream() {
        let upstream = HttpSourceHolder::new(Box::new(
            HttpFixedSource::make_get_request(b"/upstream", None).unwrap(),
        ));
        let hybrid = HttpHybridSource::new(response(103), Some(upstream)).unwrap();
        let mut combined = HttpSourceHolder::new(Box::new(hybrid));

        let mut informational = ready(combined.read_header_event()).unwrap();
        assert!(!informational.is_final().unwrap());
        assert!(!informational.eom());
        let mut upstream_headers = ready(combined.read_header_event()).unwrap();
        assert!(upstream_headers.is_final().unwrap());
        assert!(upstream_headers.eom());
        assert!(!combined.readable());
    }
}

mod fixed_response {
    use super::*;
    use crate::handler::FixedResponse;

    fn fields(message: &HttpMessage) -> Vec<(Vec<u8>, Vec<u8>)> {
        message
            .headers()
            .iter()
            .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
            .collect()
    }

    /// A head without `Content-Length`, with a stale one, and with an empty body.
    fn heads() -> [(HttpMessage, &'static [u8]); 3] {
        let mut typed = response(200);
        typed
            .headers_mut()
            .add(HttpHeaderCode::ContentType, b"text/plain");
        let mut stale = typed.clone();
        stale
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"99");
        stale.headers_mut().add("X-After", b"1");
        [(typed, b"body"), (stale, b"body"), (response(204), b"")]
    }

    #[test]
    fn new_frames_the_response_as_a_fixed_source_does() {
        for (head, body) in heads() {
            let source = HttpFixedSource::new(head.clone(), Some(queue(body)));
            let (fixed, fixed_body) = FixedResponse::new(head, queue(body)).into_parts();
            assert_eq!(fields(&fixed), fields(source.message().unwrap()));
            assert_eq!(fixed_body.chain_length(), body.len());
        }
    }

    #[test]
    fn from_parts_sends_the_head_as_it_is() {
        let mut head = response(200);
        head.headers_mut()
            .add(HttpHeaderCode::ContentType, b"text/plain");
        head.headers_mut().share_entries();
        let fixed = FixedResponse::from_parts(head.clone(), queue(b"body"));
        assert_eq!(fields(fixed.head()), fields(&head));
        assert!(!fixed.head().headers().exists(HttpHeaderCode::ContentLength));

        // A response to HEAD carries the length of a body that it does not send.
        let mut bodiless = response(200);
        bodiless
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, b"4");
        let fixed = FixedResponse::from_parts(bodiless, BufQueue::new(0));
        assert_eq!(
            fixed
                .head()
                .headers()
                .get_single_or_empty(HttpHeaderCode::ContentLength),
            b"4"
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "Content-Length differs from the length of its body")]
    fn from_parts_rejects_a_stale_content_length_in_debug_builds() {
        let mut head = response(200);
        head.headers_mut().add(HttpHeaderCode::ContentLength, b"5");
        let _ = FixedResponse::from_parts(head, queue(b"body"));
    }
}
