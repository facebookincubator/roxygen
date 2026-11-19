/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use bytes::Bytes;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;

use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::events::ResumeStatus;
use crate::fixed_source::HttpErrorSource;
use crate::fixed_source::HttpFixedSource;
use crate::headers::HttpHeaders;
use crate::http::HttpMethod;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_filter::HttpSourceFilter;
use crate::source_filter::HttpSourceFilterHooks;
use crate::source_reader::CallbackRegistrationError;
use crate::source_reader::ErrorContext;
use crate::source_reader::HttpSourceReader;

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match poll_once(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future unexpectedly remained pending"),
    }
}

fn body(bytes: &[u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).unwrap()
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

fn header(message: HttpMessage, eom: bool) -> HttpHeaderEvent {
    HttpHeaderEvent::new(message, eom).unwrap()
}

#[derive(Default)]
struct SourceObservations {
    header_reads: usize,
    body_maximums: Vec<u32>,
    stops: usize,
    drops: usize,
}

struct ScriptedSource {
    observations: Rc<RefCell<SourceObservations>>,
    headers: VecDeque<Result<HttpHeaderEvent, HttpError>>,
    bodies: VecDeque<Result<HttpBodyEvent, HttpError>>,
}

impl ScriptedSource {
    fn new(observations: Rc<RefCell<SourceObservations>>) -> Self {
        Self {
            observations,
            headers: VecDeque::new(),
            bodies: VecDeque::new(),
        }
    }

    fn with_header(mut self, event: Result<HttpHeaderEvent, HttpError>) -> Self {
        self.headers.push_back(event);
        self
    }

    fn with_body(mut self, event: Result<HttpBodyEvent, HttpError>) -> Self {
        self.bodies.push_back(event);
        self
    }
}

impl HttpSource for ScriptedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.observations.borrow_mut().header_reads += 1;
        let result = self.headers.pop_front().unwrap_or_else(|| {
            Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"header script exhausted",
            ))
        });
        Box::pin(std::future::ready(result))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.observations.borrow_mut().body_maximums.push(maximum);
        let result = self.bodies.pop_front().unwrap_or_else(|| {
            Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"body script exhausted",
            ))
        });
        Box::pin(std::future::ready(result))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.observations.borrow_mut().stops += 1;
    }
}

impl Drop for ScriptedSource {
    fn drop(&mut self) {
        self.observations.borrow_mut().drops += 1;
    }
}

struct StopProbe {
    stops: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}

impl HttpSource for StopProbe {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::new(
            HttpErrorCode::DROPPED,
        ))))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::new(
            HttpErrorCode::DROPPED,
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.stops.set(self.stops.get() + 1);
    }
}

impl Drop for StopProbe {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

fn cancellation_control(
    token: CancellationToken,
    error: Option<HttpError>,
) -> LocalSourceFuture<Result<bool, HttpError>> {
    Box::pin(async move {
        let baton = CancellableBaton::new();
        assert_eq!(baton.wait(token).await, WaitStatus::Cancelled);
        match error {
            Some(error) => Err(error),
            None => Ok(HttpSourceReader::CONTINUE),
        }
    })
}

#[test]
fn dispatches_all_configured_callbacks() {
    let push_stops = Rc::new(Cell::new(0));
    let push_drops = Rc::new(Cell::new(0));
    let mut fixed = HttpFixedSource::make_fixed_response(200, Some(body(b"body"))).unwrap();
    fixed.push_event(HttpBodyEvent::push_promise(
        request(b"/push"),
        Some(Box::new(StopProbe {
            stops: Rc::clone(&push_stops),
            drops: Rc::clone(&push_drops),
        })),
        false,
    ));
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer1", b"trailer1");
    fixed.set_trailers(trailers);

    let effects = Rc::new(RefCell::new(Vec::new()));
    let mut reader = HttpSourceReader::with_source(Box::new(fixed));
    let pre_effects = Rc::clone(&effects);
    reader
        .pre_read(move || {
            pre_effects.borrow_mut().push("pre");
            HttpSourceReader::CONTINUE
        })
        .unwrap();
    let header_effects = Rc::clone(&effects);
    reader
        .on_headers(move |mut message, final_headers, eom| {
            header_effects.borrow_mut().push("headers");
            assert_eq!(message.status_code().unwrap(), 200);
            assert!(final_headers);
            assert!(!eom);
            HttpSourceReader::CONTINUE
        })
        .unwrap();
    let push_effects = Rc::clone(&effects);
    reader
        .on_push_promise(move |mut push, eom| {
            push_effects.borrow_mut().push("push");
            assert_eq!(push.promise_mut().path().unwrap(), b"/push");
            assert!(!eom);
            drop(push.move_push_source());
            HttpSourceReader::CONTINUE
        })
        .unwrap();
    let body_effects = Rc::clone(&effects);
    reader
        .on_body(move |body, eom| {
            body_effects.borrow_mut().push("body");
            assert_eq!(body.coalesce(4).unwrap(), b"body"[..]);
            assert!(!eom);
            HttpSourceReader::CONTINUE
        })
        .unwrap();
    let trailer_effects = Rc::clone(&effects);
    reader
        .on_trailers(move |trailers| {
            trailer_effects.borrow_mut().push("trailers");
            assert_eq!(trailers.get_single_or_empty(b"x-trailer1"), b"trailer1");
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(
        &*effects.borrow(),
        &[
            "pre", "headers", "pre", "push", "pre", "body", "pre", "trailers"
        ]
    );
    assert_eq!(push_stops.get(), 1);
    assert_eq!(push_drops.get(), 1);
}

#[test]
fn trailers_fall_back_to_empty_terminal_body() {
    let mut fixed = HttpFixedSource::make_fixed_response(200, Some(body(b"body"))).unwrap();
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-trailer", b"fb-header");
    fixed.set_trailers(trailers);
    let pre_reads = Rc::new(Cell::new(0));
    let bodies = Rc::new(RefCell::new(Vec::new()));
    let mut reader = HttpSourceReader::with_source(Box::new(fixed));
    let pre_reads_callback = Rc::clone(&pre_reads);
    reader
        .pre_read(move || {
            pre_reads_callback.set(pre_reads_callback.get() + 1);
            HttpSourceReader::CONTINUE
        })
        .unwrap();
    let bodies_callback = Rc::clone(&bodies);
    reader
        .on_body(move |body, eom| {
            bodies_callback
                .borrow_mut()
                .push((body.chain_length(), eom));
            HttpSourceReader::CONTINUE
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(pre_reads.get(), 3);
    assert_eq!(&*bodies.borrow(), &[(4, false), (0, true)]);
}

#[test]
fn callback_stop_stops_source_once() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(Rc::clone(&observations))
        .with_header(Ok(header(response(200), false)))
        .with_body(Ok(HttpBodyEvent::body(body(b"unread"), true)));
    let body_calls = Rc::new(Cell::new(0));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader
        .on_headers(|_, final_headers, eom| {
            assert!(final_headers);
            assert!(!eom);
            HttpSourceReader::CANCEL
        })
        .unwrap();
    let body_calls_callback = Rc::clone(&body_calls);
    reader
        .on_body(move |_, _| {
            body_calls_callback.set(body_calls_callback.get() + 1);
            HttpSourceReader::CONTINUE
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(body_calls.get(), 0);
    assert_eq!(observations.borrow().stops, 1);
    assert_eq!(observations.borrow().drops, 1);
}

#[test]
fn terminal_callback_cancel_is_ignored_without_stopping_completed_source() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source =
        ScriptedSource::new(Rc::clone(&observations)).with_header(Ok(header(response(200), true)));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader
        .on_headers(|_, _, eom| {
            assert!(eom);
            HttpSourceReader::CANCEL
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(observations.borrow().stops, 0);
    assert_eq!(observations.borrow().drops, 1);
}

#[test]
fn pre_read_cancel_stops_before_source_read() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source =
        ScriptedSource::new(Rc::clone(&observations)).with_header(Ok(header(response(200), true)));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader.pre_read(|| HttpSourceReader::CANCEL).unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(observations.borrow().header_reads, 0);
    assert_eq!(observations.borrow().stops, 1);
}

#[test]
fn dropping_pending_reader_future_stops_released_source_once() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source =
        ScriptedSource::new(Rc::clone(&observations)).with_header(Ok(header(response(200), false)));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader
        .on_headers_async(|_, _, _| Box::pin(std::future::pending()))
        .unwrap();

    let mut read = Box::pin(reader.read(u32::MAX));
    assert!(poll_once(read.as_mut()).is_pending());
    drop(read);
    assert_eq!(observations.borrow().stops, 1);
    assert_eq!(observations.borrow().drops, 1);
}

#[test]
fn cancel_async_header_callback() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(Rc::clone(&observations))
        .with_header(Ok(header(response(200), false)))
        .with_body(Ok(HttpBodyEvent::body(body(b"unread"), true)));
    let cancellation = CancellationSource::new();
    let callback_token = cancellation.token();
    let body_called = Rc::new(Cell::new(false));
    let error_called = Rc::new(Cell::new(false));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader
        .on_headers_async(move |_, _, _| cancellation_control(callback_token.clone(), None))
        .unwrap();
    let body_called_callback = Rc::clone(&body_called);
    reader
        .on_body(move |_, _| {
            body_called_callback.set(true);
            HttpSourceReader::CONTINUE
        })
        .unwrap();
    let error_called_callback = Rc::clone(&error_called);
    reader
        .on_error(move |_, _| error_called_callback.set(true))
        .unwrap();

    let mut read = Box::pin(reader.read_with_cancellation(u32::MAX, cancellation.token()));
    assert!(poll_once(read.as_mut()).is_pending());
    assert!(cancellation.request_cancellation());
    assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(()))));
    drop(read);
    assert!(!body_called.get());
    assert!(!error_called.get());
    assert_eq!(observations.borrow().stops, 1);
}

#[test]
fn cancel_async_pre_read() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source =
        ScriptedSource::new(Rc::clone(&observations)).with_header(Ok(header(response(200), true)));
    let cancellation = CancellationSource::new();
    let callback_token = cancellation.token();
    let header_called = Rc::new(Cell::new(false));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader
        .pre_read_async(move || cancellation_control(callback_token.clone(), None))
        .unwrap();
    let header_called_callback = Rc::clone(&header_called);
    reader
        .on_headers(move |_, _, _| {
            header_called_callback.set(true);
            HttpSourceReader::CONTINUE
        })
        .unwrap();

    let mut read = Box::pin(reader.read_with_cancellation(u32::MAX, cancellation.token()));
    assert!(poll_once(read.as_mut()).is_pending());
    assert!(cancellation.request_cancellation());
    assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(()))));
    drop(read);
    assert!(!header_called.get());
    assert_eq!(observations.borrow().header_reads, 0);
    assert_eq!(observations.borrow().stops, 1);
}

#[test]
fn callback_cancellation_error_mapping() {
    let source = HttpFixedSource::make_fixed_response(200, Some(body(b"unread"))).unwrap();
    let cancellation = CancellationSource::new();
    let callback_token = cancellation.token();
    let observed = Rc::new(RefCell::new(None));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader
        .on_headers_async(move |_, _, _| {
            cancellation_control(
                callback_token.clone(),
                Some(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"Operation cancelled",
                )),
            )
        })
        .unwrap();
    let observed_callback = Rc::clone(&observed);
    reader
        .on_error(move |context, error| {
            *observed_callback.borrow_mut() =
                Some((context, error.code(), error.message().to_vec()));
        })
        .unwrap();

    let mut read = Box::pin(reader.read_with_cancellation(u32::MAX, cancellation.token()));
    assert!(poll_once(read.as_mut()).is_pending());
    assert!(cancellation.request_cancellation());
    assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(()))));
    drop(read);
    assert_eq!(
        *observed.borrow(),
        Some((
            ErrorContext::Headers,
            HttpErrorCode::INTERNAL_ERROR,
            b"Operation cancelled".to_vec(),
        ))
    );
}

#[test]
fn header_error_callback_receives_exact_error() {
    let observed = Rc::new(RefCell::new(None));
    let source = HttpErrorSource::new(HttpError::with_message(
        HttpErrorCode::PROTOCOL_ERROR,
        b"oops",
    ));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    let observed_callback = Rc::clone(&observed);
    reader
        .on_error(move |context, error| {
            *observed_callback.borrow_mut() =
                Some((context, error.code(), error.message().to_vec()));
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(
        *observed.borrow(),
        Some((
            ErrorContext::Headers,
            HttpErrorCode::PROTOCOL_ERROR,
            b"oops".to_vec()
        ))
    );
}

#[test]
fn asynchronous_error_callback_receives_exact_error() {
    let observed = Rc::new(RefCell::new(None));
    let source = HttpErrorSource::new(HttpError::with_message(
        HttpErrorCode::PROTOCOL_ERROR,
        b"async oops",
    ));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    let observed_callback = Rc::clone(&observed);
    reader
        .on_error_async(move |context, error| {
            *observed_callback.borrow_mut() =
                Some((context, error.code(), error.message().to_vec()));
            Box::pin(std::future::ready(Ok(())))
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(
        *observed.borrow(),
        Some((
            ErrorContext::Headers,
            HttpErrorCode::PROTOCOL_ERROR,
            b"async oops".to_vec(),
        ))
    );
}

#[test]
fn outer_cancellation_during_async_error_callback_is_graceful() {
    let source = HttpErrorSource::new(HttpError::new(HttpErrorCode::PROTOCOL_ERROR));
    let cancellation = CancellationSource::new();
    let callback_token = cancellation.token();
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    reader
        .on_error_async(move |_, _| {
            let callback_token = callback_token.clone();
            Box::pin(async move {
                let baton = CancellableBaton::new();
                assert_eq!(baton.wait(callback_token).await, WaitStatus::Cancelled);
                Ok(())
            })
        })
        .unwrap();

    let mut read = Box::pin(reader.read_with_cancellation(u32::MAX, cancellation.token()));
    assert!(poll_once(read.as_mut()).is_pending());
    assert!(cancellation.request_cancellation());
    assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(()))));
}

#[test]
fn outer_cancellation_during_suspend_is_graceful() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(observations)
        .with_header(Ok(header(response(200), false)))
        .with_body(Ok(HttpBodyEvent::suspend(Box::pin(std::future::pending()))));
    let cancellation = CancellationSource::new();
    let mut reader = HttpSourceReader::with_source(Box::new(source));

    let mut read = Box::pin(reader.read_with_cancellation(u32::MAX, cancellation.token()));
    assert!(poll_once(read.as_mut()).is_pending());
    assert!(cancellation.request_cancellation());
    assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(()))));
}

#[test]
fn passes_max_body_size() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(Rc::clone(&observations))
        .with_header(Ok(header(response(200), false)))
        .with_body(Ok(HttpBodyEvent::body(body(&[0; 100]), false)))
        .with_body(Ok(HttpBodyEvent::body(BufQueue::new(0), true)));
    let mut reader = HttpSourceReader::with_source(Box::new(source));

    assert!(ready(reader.read(100)).is_ok());
    assert_eq!(observations.borrow().body_maximums, [100, 100]);
}

struct ByteCountFilter(Rc<Cell<usize>>);

impl HttpSourceFilterHooks for ByteCountFilter {
    fn filter_name(&self) -> &'static str {
        "TestByteCount"
    }

    fn read_body_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let bytes = Rc::clone(&self.0);
        Box::pin(async move {
            let event = downstream.await?;
            if let Some(body) = event.as_body() {
                bytes.set(bytes.get() + body.chain_length());
            }
            Ok(event)
        })
    }
}

#[test]
fn runs_inserted_filter() {
    let bytes = Rc::new(Cell::new(0));
    let effects = Rc::new(Cell::new(0));
    let mut reader = HttpSourceReader::new();
    reader
        .insert_filter(HttpSourceFilter::new(ByteCountFilter(Rc::clone(&bytes))))
        .unwrap();
    assert!(
        reader
            .set_source(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body(&[0; 100]))).unwrap(),
            ))
            .is_ok()
    );
    let effects_header = Rc::clone(&effects);
    reader
        .on_headers(move |_, _, _| {
            effects_header.set(effects_header.get() + 1);
            HttpSourceReader::CONTINUE
        })
        .unwrap();
    let effects_body = Rc::clone(&effects);
    reader
        .on_body(move |_, eom| {
            assert!(eom);
            effects_body.set(effects_body.get() + 1);
            HttpSourceReader::CONTINUE
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(bytes.get(), 100);
    assert_eq!(effects.get(), 2);
}

struct TerminalFilter {
    saw_headers: Rc<Cell<bool>>,
    drops: Rc<Cell<usize>>,
}

impl HttpSourceFilterHooks for TerminalFilter {
    fn filter_name(&self) -> &'static str {
        "TerminalFilter"
    }

    fn read_header_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let saw_headers = Rc::clone(&self.saw_headers);
        Box::pin(async move {
            let mut event = downstream.await?;
            assert_eq!(event.headers_mut().status_code().unwrap(), 200);
            saw_headers.set(true);
            Ok(event)
        })
    }
}

impl Drop for TerminalFilter {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn terminal_filter_cleanup_is_exactly_once() {
    let saw_headers = Rc::new(Cell::new(false));
    let drops = Rc::new(Cell::new(0));
    let mut reader = HttpSourceReader::new();
    reader
        .insert_filter(HttpSourceFilter::new(TerminalFilter {
            saw_headers: Rc::clone(&saw_headers),
            drops: Rc::clone(&drops),
        }))
        .unwrap();
    assert!(
        reader
            .set_source(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body(b"body"))).unwrap(),
            ))
            .is_ok()
    );

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert!(saw_headers.get());
    assert_eq!(drops.get(), 1);
    drop(reader);
    assert_eq!(drops.get(), 1);
}

#[test]
fn asynchronous_event_hooks_preserve_order() {
    let mut fixed = HttpFixedSource::make_fixed_response(200, Some(body(b"body"))).unwrap();
    fixed.push_event(HttpBodyEvent::push_promise(request(b"/push"), None, false));
    fixed.set_trailers(HttpHeaders::new());
    let effects = Rc::new(RefCell::new(Vec::new()));
    let mut reader = HttpSourceReader::with_source(Box::new(fixed));
    let pre = Rc::clone(&effects);
    reader
        .pre_read_async(move || {
            pre.borrow_mut().push("pre");
            Box::pin(std::future::ready(Ok(HttpSourceReader::CONTINUE)))
        })
        .unwrap();
    let headers = Rc::clone(&effects);
    reader
        .on_headers_async(move |_, _, _| {
            headers.borrow_mut().push("headers");
            Box::pin(std::future::ready(Ok(HttpSourceReader::CONTINUE)))
        })
        .unwrap();
    let push = Rc::clone(&effects);
    reader
        .on_push_promise_async(move |_, _| {
            push.borrow_mut().push("push");
            Box::pin(std::future::ready(Ok(HttpSourceReader::CONTINUE)))
        })
        .unwrap();
    let body_effects = Rc::clone(&effects);
    reader
        .on_body_async(move |_, _| {
            body_effects.borrow_mut().push("body");
            Box::pin(std::future::ready(Ok(HttpSourceReader::CONTINUE)))
        })
        .unwrap();
    let trailers = Rc::clone(&effects);
    reader
        .on_trailers_async(move |_| {
            trailers.borrow_mut().push("trailers");
            Box::pin(std::future::ready(Ok(())))
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(
        &*effects.borrow(),
        &[
            "pre", "headers", "pre", "push", "pre", "body", "pre", "trailers"
        ]
    );
}

#[test]
fn body_error_uses_body_context_and_unhandled_error_propagates() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(observations)
        .with_header(Ok(header(response(200), false)))
        .with_body(Err(HttpError::with_message(
            HttpErrorCode::TRANSPORT_READ_ERROR,
            b"body failed",
        )));
    let seen = Rc::new(RefCell::new(None));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    let seen_callback = Rc::clone(&seen);
    reader
        .on_error(move |context, error| {
            *seen_callback.borrow_mut() = Some((context, error.code()));
        })
        .unwrap();
    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(
        *seen.borrow(),
        Some((ErrorContext::Body, HttpErrorCode::TRANSPORT_READ_ERROR))
    );

    let mut reader = HttpSourceReader::with_source(Box::new(HttpErrorSource::new(
        HttpError::with_message(HttpErrorCode::READ_TIMEOUT, b"exact"),
    )));
    let error = ready(reader.read(u32::MAX)).unwrap_err();
    assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
    assert_eq!(error.message(), b"exact");
}

#[test]
fn upgrade_and_padding_are_noops_but_eom_still_terminates() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(Rc::clone(&observations))
        .with_header(Ok(header(response(200), false)))
        .with_body(Ok(HttpBodyEvent::upgrade(false)))
        .with_body(Ok(HttpBodyEvent::padding(9, false)))
        .with_body(Ok(HttpBodyEvent::upgrade(true)));
    let body_calls = Rc::new(Cell::new(0));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    let body_calls_callback = Rc::clone(&body_calls);
    reader
        .on_body(move |_, _| {
            body_calls_callback.set(body_calls_callback.get() + 1);
            HttpSourceReader::CONTINUE
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(body_calls.get(), 0);
    assert_eq!(observations.borrow().body_maximums, [u32::MAX; 3]);
}

#[test]
fn cancelled_suspend_propagates_without_error_callback() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(observations)
        .with_header(Ok(header(response(200), false)))
        .with_body(Ok(HttpBodyEvent::suspend(Box::pin(std::future::ready(
            ResumeStatus::Cancelled,
        )))));
    let error_called = Rc::new(Cell::new(false));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    let error_called_callback = Rc::clone(&error_called);
    reader
        .on_error(move |_, _| error_called_callback.set(true))
        .unwrap();

    let error = ready(reader.read(u32::MAX)).unwrap_err();
    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
    assert_eq!(error.message(), b"Read cancelled");
    assert!(!error_called.get());
}

#[test]
fn noncancelled_suspend_statuses_resume_body_dispatch() {
    let observations = Rc::new(RefCell::new(SourceObservations::default()));
    let source = ScriptedSource::new(Rc::clone(&observations))
        .with_header(Ok(header(response(200), false)))
        .with_body(Ok(HttpBodyEvent::suspend(Box::pin(std::future::ready(
            ResumeStatus::Signalled,
        )))))
        .with_body(Ok(HttpBodyEvent::suspend(Box::pin(std::future::ready(
            ResumeStatus::TimedOut,
        )))))
        .with_body(Ok(HttpBodyEvent::body(body(b"done"), true)));
    let bodies = Rc::new(Cell::new(0));
    let mut reader = HttpSourceReader::with_source(Box::new(source));
    let bodies_callback = Rc::clone(&bodies);
    reader
        .on_body(move |body, eom| {
            assert_eq!(body.coalesce(4).unwrap(), b"done"[..]);
            assert!(eom);
            bodies_callback.set(bodies_callback.get() + 1);
            HttpSourceReader::CONTINUE
        })
        .unwrap();

    assert!(ready(reader.read(u32::MAX)).is_ok());
    assert_eq!(bodies.get(), 1);
    assert_eq!(observations.borrow().body_maximums, [u32::MAX; 3]);
}

#[test]
fn synchronous_and_asynchronous_hooks_are_mutually_exclusive() {
    let mut sync_first = HttpSourceReader::new();
    sync_first.on_headers(|_, _, _| false).unwrap();
    let error = match sync_first.on_headers_async(|_, _, _| Box::pin(std::future::ready(Ok(false))))
    {
        Ok(_) => panic!("async registration unexpectedly replaced a plain callback"),
        Err(error) => error,
    };
    assert_eq!(error, CallbackRegistrationError::PlainAlreadySet);

    let mut async_first = HttpSourceReader::new();
    async_first
        .on_body_async(|_, _| Box::pin(std::future::ready(Ok(false))))
        .unwrap();
    let error = match async_first.on_body(|_, _| false) {
        Ok(_) => panic!("plain registration unexpectedly replaced an async callback"),
        Err(error) => error,
    };
    assert_eq!(error, CallbackRegistrationError::AsyncAlreadySet);
}
