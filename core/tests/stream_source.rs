/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::error::Error;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::host::LocalHost;
use roxygen_runtime::host::TokioHost;
use tokio::runtime::Builder;
use tokio::time;

use crate::alloc_count::count_allocations;
use crate::alloc_count::std_allocations_observable;
use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpBodyEventKind;
use crate::events::HttpBodyEventType;
use crate::events::HttpHeaderEvent;
use crate::events::ResumeFuture;
use crate::events::ResumeStatus;
use crate::fixed_source::HttpFixedSource;
use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::http::HttpMethod;
use crate::hybrid_source::HttpHybridSource;
use crate::ids::MAX_STREAM_ID;
use crate::ids::StreamId;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_filter::MutateFilter;
use crate::source_holder::HttpSourceHolder;
use crate::stream_source::DeferredReadTimeout;
use crate::stream_source::FlowControlState;
use crate::stream_source::HttpBodyReader;
use crate::stream_source::HttpIngressBody;
use crate::stream_source::HttpResponseReader;
use crate::stream_source::HttpResponseSource;
use crate::stream_source::HttpResponseSourceOwner;
use crate::stream_source::HttpStreamSource;
use crate::stream_source::HttpStreamSourceCallback;
use crate::stream_source::HttpStreamSourceOwner;
use crate::stream_source::HttpStreamSourceProducer;
use crate::stream_source::HttpStreamSourceSharedCallback;
use crate::stream_source::StreamSourceCreateError;
use crate::stream_source_holder::HttpStreamSourceHolder;

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready<T>(mut future: LocalSourceFuture<T>) -> T {
    match poll_once(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future was expected to complete deterministically"),
    }
}

fn run_paused(test: impl Future<Output = ()>) {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async move {
            time::pause();
            test.await;
        });
}

fn body(bytes: &[u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).unwrap()
}

fn sized_body(size: usize) -> BufQueue {
    body(&vec![b'x'; size])
}

fn request(content_length: Option<&[u8]>) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Post).unwrap();
    message.set_url(b"/", false).unwrap();
    if let Some(content_length) = content_length {
        message
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, content_length);
    }
    message
}

fn response(status: u16, content_length: Option<&[u8]>) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    if let Some(content_length) = content_length {
        message
            .headers_mut()
            .add(HttpHeaderCode::ContentLength, content_length);
    }
    message
}

#[derive(Default)]
struct CallbackLog {
    processed: Vec<(StreamId, usize, usize)>,
    opened: Vec<StreamId>,
    completions: Vec<Option<HttpErrorCode>>,
}

struct RecordingCallback(Rc<RefCell<CallbackLog>>);

impl HttpStreamSourceCallback for RecordingCallback {
    fn bytes_processed(&mut self, id: StreamId, amount: usize, to_acknowledge: usize) {
        self.0
            .borrow_mut()
            .processed
            .push((id, amount, to_acknowledge));
    }

    fn window_open(&mut self, id: StreamId) {
        self.0.borrow_mut().opened.push(id);
    }

    fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
        self.0
            .borrow_mut()
            .completions
            .push(error.map(|error| error.code()));
    }
}

fn session_source(
    capacity: u32,
) -> (
    HttpStreamSource,
    HttpStreamSourceProducer,
    Rc<RefCell<CallbackLog>>,
) {
    let log = Rc::new(RefCell::new(CallbackLog::default()));
    let source = HttpStreamSource::new_session(
        StreamId::new(7),
        RecordingCallback(Rc::clone(&log)),
        capacity,
        std::time::Duration::ZERO,
    )
    .unwrap();
    let producer = source.producer();
    (source, producer, log)
}

#[test]
fn drain_owner_preserves_late_ingress_credit_and_producer_completion() {
    let (mut source, producer, log) = session_source(4);
    let drain = source.drain_on_drop();
    let completion = source.ingress_completion().unwrap();
    assert!(producer.headers(request(Some(b"12")), false));
    ready(source.read_header_event()).unwrap();
    assert_eq!(
        producer.body(body(b"abc"), 0, false),
        FlowControlState::Open
    );
    drop(source);
    assert!(producer.is_live());
    assert_eq!(producer.body_bytes_buffered(), Some(0));
    assert!(completion.snapshot().is_none());
    assert!(log.borrow().completions.is_empty());

    for bytes in [b"def", b"ghi", b"jkl"] {
        assert_eq!(producer.body(body(bytes), 0, false), FlowControlState::Open);
        assert_eq!(producer.body_bytes_buffered(), Some(0));
    }
    assert_eq!(completion.body_bytes_received(), 12);
    assert_eq!(
        log.borrow()
            .processed
            .iter()
            .map(|(_, _, credit)| credit)
            .sum::<usize>(),
        12
    );
    let before_eom = Instant::now();
    assert!(producer.eom());
    let completed = completion.snapshot().unwrap().unwrap();
    assert!(completed >= before_eom && completed <= Instant::now());
    assert_eq!(log.borrow().completions, [None]);
    drop(drain);
    assert!(!producer.is_live());
}

#[test]
fn drain_owner_stops_pending_consumer_without_finishing_producer() {
    let (mut source, producer, log) = session_source(4);
    let drain = source.drain_on_drop();
    let completion = source.ingress_completion().unwrap();
    let mut read = source.read_header_event();
    assert!(poll_once(read.as_mut()).is_pending());
    source.stop_reading(None);
    drop(source);
    assert_eq!(
        ready(read).err().expect("consumer stopped").code(),
        HttpErrorCode::NO_ERROR
    );
    assert!(completion.snapshot().is_none());
    assert!(log.borrow().completions.is_empty());
    assert!(producer.headers(request(Some(b"0")), true));
    assert!(completion.snapshot().unwrap().is_ok());
    assert_eq!(log.borrow().completions, [None]);
    drop(drain);
}

#[test]
fn drain_owner_retirement_after_eom_preserves_pending_consumer_result() {
    let (mut source, producer, log) = session_source(4);
    let drain = source.drain_on_drop();
    let completion = source.ingress_completion().unwrap();
    let mut read = source.read_header_event();
    assert!(poll_once(read.as_mut()).is_pending());
    source.stop_reading(None);
    drop(source);
    assert!(producer.headers(request(Some(b"0")), true));
    assert!(log.borrow().completions.is_empty());
    drop(drain);
    assert_eq!(
        ready(read).err().expect("consumer stopped").code(),
        HttpErrorCode::NO_ERROR
    );
    assert!(completion.snapshot().unwrap().is_ok());
    assert_eq!(log.borrow().completions, [None]);
    assert!(!producer.is_live());
}

#[test]
fn drain_owner_honors_explicit_abort_and_owner_retirement() {
    for termination in ["producer", "consumer", "owner", "owner_before_consumer"] {
        let (mut source, producer, log) = session_source(4);
        let mut drain = Some(source.drain_on_drop());
        let completion = source.ingress_completion().unwrap();
        assert!(producer.headers(request(None), false));
        ready(source.read_header_event()).unwrap();
        if termination == "consumer" {
            source.stop_reading(Some(HttpErrorCode::CANCEL));
        } else if termination == "owner_before_consumer" {
            drop(drain.take());
        }
        drop(source);
        let expected = match termination {
            "producer" => {
                assert!(producer.abort(HttpErrorCode::CANCEL, b"peer reset"));
                HttpErrorCode::CANCEL
            }
            "owner" => {
                drop(drain.take());
                HttpErrorCode::DROPPED
            }
            "owner_before_consumer" => HttpErrorCode::NO_ERROR,
            _ => HttpErrorCode::CANCEL,
        };
        assert_eq!(completion.snapshot().unwrap().unwrap_err().code(), expected);
        assert_eq!(log.borrow().completions.len(), 1, "{termination}");
        drop(drain);
        assert!(!producer.is_live());
        assert_eq!(log.borrow().completions.len(), 1, "{termination}");
    }
}

#[test]
fn drain_owner_preserves_content_length_validation() {
    let (source, producer, log) = session_source(4);
    let drain = source.drain_on_drop();
    let completion = source.ingress_completion().unwrap();
    assert!(producer.headers(request(Some(b"3")), false));
    drop(source);
    assert_eq!(producer.body(body(b"ab"), 0, false), FlowControlState::Open);
    assert!(producer.eom());
    assert_eq!(completion.body_bytes_received(), 2);
    assert_eq!(
        completion.snapshot().unwrap().unwrap_err().code(),
        HttpErrorCode::CONTENT_LENGTH_MISMATCH
    );
    assert_eq!(
        log.borrow().completions,
        [Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH)]
    );
    drop(drain);
}

#[test]
fn drain_owner_completes_even_when_producer_observer_panics() {
    struct PanicDrainWake;
    impl Wake for PanicDrainWake {
        fn wake(self: Arc<Self>) {
            panic!("producer observer wake");
        }
    }
    let (source, producer, log) = session_source(4);
    let drain = source.drain_on_drop();
    let completion = source.ingress_completion().unwrap();
    assert!(producer.headers(request(Some(b"0")), false));
    drop(source);
    let mut completed = completion.wait();
    let waker = Waker::from(Arc::new(PanicDrainWake));
    assert!(
        completed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| producer.eom())).is_err());
    assert!(completion.snapshot().unwrap().is_ok());
    assert_eq!(log.borrow().completions, [None]);
    drop(drain);
    assert!(!producer.is_live());
}

#[test]
fn producer_byte_count_survives_consumption_cancellation_and_retirement() {
    for observe_before_data in [false, true] {
        let (mut source, producer, _) = session_source(8);
        assert!(producer.headers(response(200, None), false));
        let early = observe_before_data.then(|| source.ingress_completion().unwrap());
        assert_eq!(
            producer.body(body(b"abc"), 2, false),
            FlowControlState::Open
        );
        let completion = early.unwrap_or_else(|| source.ingress_completion().unwrap());
        assert_eq!(completion.body_bytes_received(), 3);
        ready(source.read_header_event()).unwrap();
        let event = ready(source.read_body_event(1)).unwrap();
        assert_eq!(event.as_body().unwrap().chain_length(), 1);
        assert_eq!(completion.body_bytes_received(), 3);
        assert_eq!(source.body_bytes_buffered(), 2);
        assert_eq!(producer.body(body(b"de"), 0, false), FlowControlState::Open);
        assert_eq!(completion.body_bytes_received(), 5);
        source.stop_reading(Some(HttpErrorCode::CANCEL));
        assert!(completion.snapshot().unwrap().is_err());
        drop(source);
        assert_eq!(
            producer.body(body(b"ignored"), 0, false),
            FlowControlState::Error
        );
        assert_eq!(completion.body_bytes_received(), 5);
    }
}

#[test]
fn producer_byte_count_excludes_rejected_flow_control_input() {
    let (source, producer, _) = session_source(1);
    assert!(producer.headers(response(200, None), false));
    assert_eq!(
        producer.body(body(b"a"), 0, false),
        FlowControlState::Closed
    );
    let completion = source.ingress_completion().unwrap();
    assert_eq!(
        producer.body(body(b"bc"), 0, false),
        FlowControlState::Error
    );
    assert_eq!(completion.body_bytes_received(), 1);
    assert_eq!(
        completion.snapshot().unwrap().unwrap_err().code(),
        HttpErrorCode::FLOW_CONTROL_ERROR
    );
    drop(source);
    assert_eq!(completion.body_bytes_received(), 1);
}

#[test]
fn ingress_completion_precedes_consumption_for_each_terminal_event() {
    for terminal in ["headers", "body", "trailers", "eom"] {
        let (mut source, producer, log) = session_source(3);
        let completion = source.ingress_completion().expect("stream observation");
        let mut pending = completion.wait();
        assert!(poll_once(pending.as_mut()).is_pending());
        assert!(producer.headers(response(100, None), false));
        assert!(completion.snapshot().is_none());
        assert!(!ready(source.read_header_event()).unwrap().eom());
        let before = Instant::now();
        let header_eom = terminal == "headers";
        assert!(producer.headers(
            response(200, Some(if header_eom { b"0" } else { b"3" })),
            header_eom,
        ));
        if !header_eom {
            assert!(completion.snapshot().is_none());
            assert_eq!(
                producer.body(body(b"abc"), 0, terminal == "body"),
                FlowControlState::Closed,
            );
            match terminal {
                "trailers" => {
                    assert!(completion.snapshot().is_none());
                    assert!(producer.trailers(HttpHeaders::new()));
                }
                "eom" => {
                    assert!(completion.snapshot().is_none());
                    assert!(producer.eom());
                }
                _ => {}
            }
        }
        let arrived = ready(pending).expect("producer EOM");
        assert!(before <= arrived && arrived <= Instant::now());
        assert_eq!(source.body_bytes_buffered(), if header_eom { 0 } else { 3 });
        assert!(log.borrow().processed.is_empty());
        assert!(log.borrow().completions.is_empty());
        assert_eq!(ready(source.read_header_event()).unwrap().eom(), header_eom);
        if !header_eom {
            while !ready(source.read_body_event(3)).unwrap().eom() {}
        }
        assert_eq!(log.borrow().completions, vec![None]);
        assert_eq!(source.body_bytes_buffered(), 0);
        drop(source);
        assert!(!producer.is_live());
        assert_eq!(ready(completion.wait()).unwrap(), arrived);
    }
}

#[test]
fn ingress_completion_preserves_errors_without_retaining_source() {
    let (source, producer, _) = session_source(8);
    let completion = source.ingress_completion().unwrap();
    let cause = Arc::new(SourceCause);
    assert!(producer.abort_with_error(
        HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR).with_cause(cause.clone()),
        b"broken peer",
    ));
    drop(source);
    assert!(!producer.is_live());
    let error = ready(completion.wait()).expect_err("producer failure");
    assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
    assert_source_cause(&error, &cause);

    let (source, producer, _) = session_source(8);
    assert!(producer.headers(response(200, Some(b"4")), false));
    producer.body(body(b"abc"), 0, true);
    let completion = source.ingress_completion().unwrap();
    assert_eq!(
        ready(completion.wait()).expect_err("invalid EOM").code(),
        HttpErrorCode::CONTENT_LENGTH_MISMATCH,
    );

    let (source, producer, _) = session_source(8);
    let completion = source.ingress_completion().unwrap();
    drop(source);
    assert!(!producer.is_live());
    assert!(
        ready(completion.wait()).is_err(),
        "drop is not producer EOM"
    );
}

#[test]
fn ingress_completion_waits_are_independent() {
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (source, producer, _) = session_source(8);
    let completion = source.ingress_completion().unwrap();
    let cancelled_wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let active_wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let mut cancelled = completion.wait();
    let mut active = completion.wait();
    assert!(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&Waker::from(
                cancelled_wakes.clone()
            ),))
            .is_pending()
    );
    assert!(
        active
            .as_mut()
            .poll(&mut Context::from_waker(&Waker::from(active_wakes.clone()),))
            .is_pending()
    );
    drop(cancelled);
    assert!(producer.headers(response(204, None), true));
    assert_eq!(cancelled_wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(active_wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(ready(active).unwrap(), ready(completion.wait()).unwrap());
}

#[test]
fn ingress_completion_passes_through_native_adaptation() {
    let (mut source, producer, log) = session_source(3);
    assert!(producer.headers(response(200, Some(b"3")), false));
    let header = ready(source.read_header_event()).unwrap();
    let hybrid =
        HttpHybridSource::from_header_event(header, HttpSourceHolder::new(Box::new(source)));
    let filter = MutateFilter::with_source(Box::new(hybrid), None, None);
    let mut outer = HttpSourceHolder::new(Box::new(filter));
    let completion = outer.ingress_completion().expect("forwarded observation");
    assert!(!ready(outer.read_header_event()).unwrap().eom());
    assert!(completion.snapshot().is_none());
    producer.body(body(b"abc"), 0, true);
    assert!(completion.snapshot().unwrap().is_ok());
    assert!(log.borrow().completions.is_empty());
    assert!(ready(outer.read_body_event(3)).unwrap().eom());
    assert!(!producer.is_live());
    assert!(ready(completion.wait()).is_ok());
}

#[test]
fn eom_at_reports_the_supplied_completion_time() {
    let completed_at = Instant::now() + Duration::from_secs(3600);
    let (source, producer, _) = session_source(8);
    let completion = source.ingress_completion().unwrap();
    assert!(source.initialize_body_only(&mut request(None), false));
    assert!(producer.eom_at(completed_at));
    assert_eq!(completion.snapshot().unwrap().unwrap(), completed_at);

    let (source, producer, _) = session_source(8);
    let completion = source.ingress_completion().unwrap();
    assert!(source.initialize_body_only(&mut request(Some(b"3")), false));
    assert!(producer.eom_at(completed_at));
    assert_eq!(
        completion.snapshot().unwrap().unwrap_err().code(),
        HttpErrorCode::CONTENT_LENGTH_MISMATCH
    );
}

/// A header-only request's body retires its source before handoff, completing it once with the
/// terminal empty read. A source error fails the retirement with that error, and a body that holds
/// more than the empty EOM fails it as an invalid state transition.
#[test]
fn retire_empty_body_completes_a_header_only_source() {
    let mut context = Context::from_waker(Waker::noop());
    let header_only = |content_length: Option<&[u8]>| {
        let log = Rc::new(RefCell::new(CallbackLog::default()));
        let ingress = HttpIngressBody::new(
            StreamId::new(7),
            RecordingCallback(Rc::clone(&log)),
            8,
            Host::tokio(),
        )
        .unwrap();
        assert!(ingress.initialize_body_only(&mut request(content_length), false));
        (ingress, log)
    };

    for drained in [false, true] {
        let (ingress, log) = header_only(None);
        let producer = ingress.producer();
        let completion = ingress.ingress_completion().unwrap();
        let drain = drained.then(|| ingress.drain_on_drop());
        assert!(producer.eom());
        assert!(log.borrow().completions.is_empty());
        let ingress = ingress
            .retire_empty_body(&mut context)
            .expect("the terminal empty read");
        assert_eq!(log.borrow().completions, vec![None]);
        assert!(ingress.into_source_owner().source().is_none());
        assert_eq!(producer.is_live(), drained);
        drop(drain);
        assert!(!producer.is_live());
        assert_eq!(log.borrow().completions, vec![None]);
        assert!(completion.snapshot().unwrap().is_ok());
    }

    let (ingress, log) = header_only(Some(b"3"));
    assert!(ingress.producer().eom());
    let error = ingress
        .retire_empty_body(&mut context)
        .err()
        .expect("the length mismatch");
    assert_eq!(error.code(), HttpErrorCode::CONTENT_LENGTH_MISMATCH);
    assert_eq!(
        log.borrow().completions,
        vec![Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH)]
    );

    let (ingress, log) = header_only(None);
    let producer = ingress.producer();
    assert_eq!(producer.body(body(b"ab"), 0, false), FlowControlState::Open);
    assert!(producer.eom());
    let error = ingress
        .retire_empty_body(&mut context)
        .err()
        .expect("the body bytes");
    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    assert!(!producer.is_live());
    assert_eq!(
        log.borrow().completions,
        vec![Some(HttpErrorCode::NO_ERROR)]
    );
}

/// A header-only body is the body that `retire_empty_body` leaves: it holds no source, reports no
/// completion, converts to an empty holder, and its drop calls back nothing.
#[test]
fn a_header_only_ingress_body_holds_no_source() {
    let log = Rc::new(RefCell::new(CallbackLog::default()));
    let retired = HttpIngressBody::new(
        StreamId::new(7),
        RecordingCallback(Rc::clone(&log)),
        8,
        Host::tokio(),
    )
    .unwrap();
    assert!(retired.initialize_body_only(&mut request(None), false));
    assert!(retired.producer().eom());
    let retired = retired
        .retire_empty_body(&mut Context::from_waker(Waker::noop()))
        .expect("the terminal empty read");
    assert_eq!(log.borrow().completions, vec![None]);

    for (name, body) in [
        ("header-only", HttpIngressBody::header_only()),
        ("retired", retired),
    ] {
        assert!(!body.readable(), "{name}");
        assert!(body.ingress_completion().is_none(), "{name}");
        let owner = body.into_source_owner();
        assert!(owner.source().is_none(), "{name}");
        assert!(!owner.into_source_holder().readable(), "{name}");
    }
    assert_eq!(
        log.borrow().completions,
        vec![None],
        "dropping the retired body calls back nothing"
    );
}

#[derive(Debug, thiserror::Error)]
#[error("owned source failure")]
struct SourceCause;

fn assert_source_cause(error: &HttpError, cause: &Arc<SourceCause>) {
    assert!(std::ptr::eq(
        error
            .source()
            .unwrap()
            .downcast_ref::<SourceCause>()
            .unwrap(),
        cause.as_ref(),
    ));
}

#[test]
fn owned_abort_retains_cause_and_first_error() {
    let (source, producer, log) = session_source(8);
    let first = Arc::new(SourceCause);
    let second = Arc::new(SourceCause);
    assert!(
        producer.abort_with_error(
            HttpError::with_message(HttpErrorCode::TRANSPORT_READ_ERROR, b"original diagnostic")
                .with_cause(first.clone()),
            b"connection error",
        )
    );
    assert!(!producer.abort_with_error(
        HttpError::new(HttpErrorCode::TRANSPORT_WRITE_ERROR).with_cause(second.clone()),
        b"later",
    ));
    assert!(!producer.abort(HttpErrorCode::CORO_CANCELLED, b"synthetic"));
    for _ in 0..2 {
        let error = read_header(&source).err().expect("first ingress failure");
        assert_source_cause(&error, &first);
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
        assert_eq!(
            error.message(),
            b"HTTP source aborted err=TRANSPORT_READ_ERROR, details=connection error"
        );
    }
    assert_eq!(Arc::strong_count(&second), 1);
    assert_eq!(log.borrow().completions, vec![None]);
    let (source, producer, _) = session_source(8);
    assert!(producer.abort(HttpErrorCode::CANCEL, b"selected reset"));
    assert!(!producer.abort_with_error(
        HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR).with_cause(first),
        b"later"
    ));
    let error = read_header(&source)
        .err()
        .expect("selected cause-free error");
    assert_eq!(error.code(), HttpErrorCode::CANCEL);
    assert!(error.source().is_none());
}

#[test]
fn owned_abort_preserves_complete_source() {
    for header_eom in [false, true] {
        let (source, producer, log) = session_source(8);
        let cause = Arc::new(SourceCause);
        assert!(producer.headers(
            response(200, Some(if header_eom { b"0" } else { b"3" })),
            header_eom
        ));
        if !header_eom {
            producer.body(body(b"end"), 0, true);
        }
        assert!(!producer.abort_with_error(
            HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR).with_cause(cause.clone()),
            b"too late"
        ));
        assert_eq!(Arc::strong_count(&cause), 1);
        assert_eq!(read_header(&source).unwrap().eom(), header_eom);
        if !header_eom {
            let event = read_body(&source, 8).unwrap();
            assert!(event.eom());
            assert!(
                matches!(event.into_kind(), HttpBodyEventKind::Body(data) if data.chain_length() == 3)
            );
        }
        assert_eq!(log.borrow().completions, vec![None]);
        assert_eq!(source.body_bytes_buffered(), 0);
    }
}

struct AbortOrderCallback(Rc<RefCell<Vec<&'static str>>>);

impl HttpStreamSourceCallback for AbortOrderCallback {
    fn bytes_processed(&mut self, _: StreamId, amount: usize, to_acknowledge: usize) {
        assert_eq!((amount, to_acknowledge), (2, 0));
        self.0.borrow_mut().push("credit");
    }

    fn window_open(&mut self, _: StreamId) {
        self.0.borrow_mut().push("window");
    }

    fn source_complete(&mut self, _: StreamId, error: Option<HttpError>) {
        assert!(error.is_none());
        self.0.borrow_mut().push("complete");
    }
}

#[test]
fn owned_abort_preserves_callbacks_and_headers() {
    let order = Rc::new(RefCell::new(Vec::new()));
    let source = HttpStreamSource::new_async(
        None,
        Some(Box::new(AbortOrderCallback(Rc::clone(&order)))),
        2,
    )
    .unwrap();
    let producer = source.producer();
    assert!(producer.headers(response(103, None), false));
    assert!(producer.headers(response(200, None), false));
    assert_eq!(
        producer.body(body(b"ab"), 0, false),
        FlowControlState::Closed
    );
    let cause = Arc::new(SourceCause);
    assert!(producer.abort_with_error(
        HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR).with_cause(cause.clone()),
        b"connection error"
    ));
    assert_eq!(source.body_bytes_buffered(), 0);
    assert_eq!(*order.borrow(), vec!["credit", "window"]);
    let mut error = read_header(&source).err().expect("owned abort");
    assert_source_cause(&error, &cause);
    assert_eq!(
        error.http_message_mut().unwrap().status_code().unwrap(),
        200
    );
    assert_eq!(*order.borrow(), vec!["credit", "window", "complete"]);
    drop(source);
    assert_eq!(*order.borrow(), vec!["credit", "window", "complete"]);
}

fn timed_session_source(
    capacity: u32,
    timeout: Duration,
) -> (
    HttpStreamSource,
    HttpStreamSourceProducer,
    Rc<RefCell<CallbackLog>>,
) {
    let log = Rc::new(RefCell::new(CallbackLog::default()));
    let source = HttpStreamSource::new_session(
        StreamId::new(7),
        RecordingCallback(Rc::clone(&log)),
        capacity,
        timeout,
    )
    .unwrap();
    let producer = source.producer();
    (source, producer, log)
}

#[test]
fn deferred_timeout_updates_preserve_a_pending_read_before_and_after_arming() {
    run_paused(async {
        let (mut source, producer, _) = session_source(8);
        assert!(
            producer
                .defer_read_timeout(DeferredReadTimeout::DeferUpdates(Duration::from_millis(10),))
        );
        let mut read = source.read_header_event_unboxed(CancellationToken::never());
        assert!(poll_once(Pin::new(&mut read)).is_pending());
        source.set_read_timeout(Duration::from_millis(5));
        time::advance(Duration::from_millis(20)).await;
        assert!(poll_once(Pin::new(&mut read)).is_pending());

        assert!(producer.arm_read_timeout());
        assert!(poll_once(Pin::new(&mut read)).is_pending());
        time::advance(Duration::from_millis(4)).await;
        assert!(poll_once(Pin::new(&mut read)).is_pending());
        source.set_read_timeout(Duration::from_millis(20));
        assert!(poll_once(Pin::new(&mut read)).is_pending());
        time::advance(Duration::from_millis(6)).await;
        assert!(poll_once(Pin::new(&mut read)).is_pending());
        assert!(!producer.arm_read_timeout());

        time::advance(Duration::from_millis(15)).await;
        let finished_at = time::Instant::now();
        assert_eq!(error_code(read.await), HttpErrorCode::READ_TIMEOUT);
        assert_eq!(time::Instant::now(), finished_at);
    });
}

fn take_resume(event: HttpBodyEvent) -> ResumeFuture {
    match event.into_kind() {
        HttpBodyEventKind::Suspend(resume) => resume,
        _ => panic!("expected a suspend event"),
    }
}

fn read_header(source: &HttpStreamSource) -> Result<HttpHeaderEvent, HttpError> {
    ready(source.read_header_event_with_cancellation(Default::default()))
}

fn read_body(source: &HttpStreamSource, maximum: u32) -> Result<HttpBodyEvent, HttpError> {
    ready(source.read_body_event_with_cancellation(maximum, Default::default()))
}

fn error_code<T>(result: Result<T, HttpError>) -> HttpErrorCode {
    match result {
        Ok(_) => panic!("expected a source error"),
        Err(error) => error.code(),
    }
}

mod stream_source {
    use super::*;

    fn shared_source(
        response_source: bool,
        callback: Rc<dyn HttpStreamSourceSharedCallback>,
        capacity: u32,
        host: Host,
    ) -> Result<HttpStreamSourceOwner, StreamSourceCreateError> {
        if response_source {
            HttpResponseSource::new_shared(StreamId::new(7), callback, capacity, host)
                .map(HttpResponseSource::into_source_owner)
        } else {
            HttpIngressBody::new_shared(StreamId::new(7), callback, capacity, host)
                .map(HttpIngressBody::into_source_owner)
        }
    }

    #[test]
    fn headers_eom() {
        let (source, producer, log) = session_source(65_535);
        assert!(producer.headers(request(None), true));
        let mut event = ready(Box::pin(
            source.read_header_event_unboxed(CancellationToken::never()),
        ))
        .unwrap();
        assert_eq!(event.headers_mut().path().unwrap(), b"/");
        assert!(event.is_final().unwrap());
        assert!(event.eom());
        assert_eq!(log.borrow().completions, vec![None]);
    }

    #[test]
    fn body_split_push_promise_eom() {
        let (source, producer, _) = session_source(65_535);
        assert!(producer.headers(response(200, None), false));
        assert_eq!(
            producer.body(body(b"body"), 0, false),
            FlowControlState::Open
        );
        let push = HttpFixedSource::make_fixed_response(200, Some(body(b"push body"))).unwrap();
        let mut promise = request(None);
        promise.set_url(b"/push", false).unwrap();
        assert!(producer.push_promise(promise, Box::new(push), false));
        assert!(producer.eom());

        assert!(!read_header(&source).unwrap().eom());
        let mut joined = Vec::new();
        for _ in 0..4 {
            let event = read_body(&source, 1).unwrap();
            assert_eq!(event.event_type(), HttpBodyEventType::Body);
            assert!(!event.eom());
            joined.extend_from_slice(&event.as_body().unwrap().coalesce(1).unwrap());
        }
        assert_eq!(joined, b"body");
        let event = read_body(&source, 1).unwrap();
        assert_eq!(event.event_type(), HttpBodyEventType::PushPromise);
        assert!(event.eom());
        let HttpBodyEventKind::PushPromise(mut push) = event.into_kind() else {
            panic!("expected a push promise")
        };
        assert_eq!(push.promise_mut().path().unwrap(), b"/push");
        assert!(push.move_push_source().is_some());
    }

    #[test]
    fn content_length_zero() {
        let (source, producer, _) = session_source(65_535);
        assert!(producer.headers(request(Some(b"0")), false));
        assert!(producer.eom());
        assert!(read_header(&source).unwrap().eom());
    }

    fn assert_content_length_mismatch(content_length: &[u8], bytes: &[u8]) {
        let (source, producer, _) = session_source(65_535);
        assert!(producer.headers(request(Some(content_length)), false));
        if !bytes.is_empty() {
            producer.body(body(bytes), 0, false);
        }
        producer.eom();
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::CONTENT_LENGTH_MISMATCH
        );
    }

    #[test]
    fn content_length_body_exceeds_declared() {
        assert_content_length_mismatch(b"2", b"body");
    }

    #[test]
    fn content_length_eom_before_declared() {
        assert_content_length_mismatch(b"10", b"body");
    }

    #[test]
    fn malformed_content_length_disables_secondary_check() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(request(Some(b"xxx")), false);
        producer.body(body(b"body"), 0, false);
        producer.eom();
        assert!(!read_header(&source).unwrap().eom());
        let event = read_body(&source, u32::MAX).unwrap();
        assert_eq!(event.as_body().unwrap().coalesce(4).unwrap(), b"body"[..]);
        assert!(event.eom());
    }

    /// A request whose `content-length` field is present and empty.
    fn request_with_empty_content_length() -> HttpMessage {
        let mut message = request(None);
        message
            .headers_mut()
            .add_verbatim(HttpHeaderCode::ContentLength, b"");
        message
    }

    #[test]
    fn an_empty_content_length_disables_the_check_by_default() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(request_with_empty_content_length(), false);
        producer.body(body(b"body"), 0, true);
        assert!(!read_header(&source).unwrap().eom());
        let event = read_body(&source, u32::MAX).unwrap();
        assert_eq!(event.as_body().unwrap().coalesce(4).unwrap(), b"body"[..]);
        assert!(event.eom());
    }

    #[test]
    fn an_empty_content_length_reads_as_zero_when_enabled() {
        let (source, producer, _) = session_source(65_535);
        source.read_empty_content_length_as_zero();
        producer.headers(request_with_empty_content_length(), true);
        assert!(
            read_header(&source).unwrap().eom(),
            "an empty body matches a zero length"
        );

        let (source, producer, _) = session_source(65_535);
        source.read_empty_content_length_as_zero();
        producer.headers(request_with_empty_content_length(), false);
        producer.body(body(b"body"), 0, true);
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::CONTENT_LENGTH_MISMATCH,
            "a body overruns a zero length"
        );
    }

    #[test]
    fn reading_an_empty_content_length_as_zero_leaves_a_missing_one_unchecked() {
        let (source, producer, _) = session_source(65_535);
        source.read_empty_content_length_as_zero();
        producer.headers(request(None), false);
        producer.body(body(b"body"), 0, true);
        assert!(!read_header(&source).unwrap().eom());
        assert!(read_body(&source, u32::MAX).unwrap().eom());
    }

    #[test]
    fn content_length_accepts_c_whitespace() {
        for content_length in [b"\x0b5\x0c".as_slice(), b"\x0c5\x0b".as_slice()] {
            assert_content_length_mismatch(content_length, b"body");
        }
    }

    #[test]
    fn content_length_rejects_a_leading_plus() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(request(Some(b"+5")), false);
        producer.body(body(b"body"), 0, true);
        assert!(!read_header(&source).unwrap().eom());
        assert!(read_body(&source, u32::MAX).unwrap().eom());
    }

    #[test]
    fn empty_get_response_length_mismatch() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, Some(b"10")), true);
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::CONTENT_LENGTH_MISMATCH
        );
    }

    #[test]
    fn empty_head_response_is_exempt() {
        let (source, producer, _) = session_source(65_535);
        source.skip_content_length_validation();
        producer.headers(response(200, Some(b"10")), true);
        assert!(read_header(&source).unwrap().eom());
    }

    #[test]
    fn chunked_content_length_is_still_checked() {
        let (source, producer, _) = session_source(65_535);
        let mut message = request(Some(b"40"));
        message.set_chunked(true);
        message
            .headers_mut()
            .add(HttpHeaderCode::TransferEncoding, b"chunked");
        producer.headers(message, false);
        producer.body(body(b"body"), 0, false);
        producer.eom();
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::CONTENT_LENGTH_MISMATCH
        );
    }

    #[test]
    fn response_304_length_is_exempt() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(304, Some(b"10")), true);
        assert!(read_header(&source).unwrap().eom());
    }

    #[test]
    fn reject_headers_after_final_headers() {
        let (source, producer, _) = session_source(65_535);
        assert!(source.headers_allowed());
        assert!(producer.headers(response(100, None), false));
        assert!(source.headers_allowed());
        assert!(producer.headers(response(200, None), false));
        assert!(!source.headers_allowed());
        assert!(!producer.headers(response(100, None), false));
        assert_eq!(
            error_code(ready(Box::pin(
                source.read_header_event_unboxed(CancellationToken::never())
            ))),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
    }

    #[test]
    fn abort_attaches_partial_headers() {
        let (source, producer, log) = session_source(65_535);
        producer.headers(request(None), false);
        producer.abort(HttpErrorCode::HEADER_PARSE_ERROR, b"");
        let mut error = match read_header(&source) {
            Err(error) => error,
            Ok(_) => panic!("expected abort"),
        };
        assert_eq!(error.code(), HttpErrorCode::HEADER_PARSE_ERROR);
        assert_eq!(
            error.http_message_mut().unwrap().method_string().unwrap(),
            "POST"
        );
        assert_eq!(log.borrow().completions, vec![None]);
    }

    #[test]
    fn first_terminal_error_wins() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(request(None), false);
        assert!(!producer.headers(request(None), false));
        producer.abort(HttpErrorCode::HEADER_PARSE_ERROR, b"");
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
    }

    #[test]
    fn reject_body_before_final_headers() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(100, None), false);
        assert_eq!(
            producer.body(body(b"body"), 0, false),
            FlowControlState::Error
        );
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
    }

    #[test]
    fn body_only_source() {
        let (source, producer, _) = session_source(65_535);
        let mut message = request(Some(b"100"));
        assert!(source.initialize_body_only(&mut message, false));
        assert_eq!(
            producer.body(sized_body(100), 0, true),
            FlowControlState::Open
        );
        let event = read_body(&source, u32::MAX).unwrap();
        assert_eq!(event.as_body().unwrap().chain_length(), 100);
        assert!(event.eom());
    }

    #[test]
    fn informational_content_length_is_ignored() {
        for status in [100, 102, 103] {
            let (source, producer, _) = session_source(65_535);
            producer.headers(response(status, Some(b"10")), false);
            producer.headers(response(200, Some(b"0")), true);
            let mut informational = ready(Box::pin(
                source.read_header_event_unboxed(CancellationToken::never()),
            ))
            .unwrap();
            assert!(!informational.is_final().unwrap());
            assert!(read_header(&source).unwrap().eom());
        }
    }

    #[test]
    fn body_backpressure_opens_after_consume() {
        let log = Rc::new(RefCell::new(CallbackLog::default()));
        let source = HttpStreamSource::new_async(
            None,
            Some(Box::new(RecordingCallback(Rc::clone(&log)))),
            4_000,
        )
        .unwrap();
        let producer = source.producer();
        producer.headers(response(200, None), false);
        assert_eq!(
            producer.body(sized_body(100), 0, false),
            FlowControlState::Open
        );
        assert_eq!(
            producer.body(sized_body(3_899), 0, false),
            FlowControlState::Open
        );
        assert_eq!(
            producer.body(sized_body(1), 0, false),
            FlowControlState::Closed
        );
        assert_eq!(
            producer.body(sized_body(10), 0, false),
            FlowControlState::Closed
        );
        read_header(&source).unwrap();

        let first = read_body(&source, 100).unwrap();
        assert_eq!(first.as_body().unwrap().chain_length(), 100);
        assert_eq!(log.borrow().opened, vec![MAX_STREAM_ID]);
        producer.eom();
        let mut count = 1;
        let mut eom = first.eom();
        while !eom {
            let event = read_body(&source, 100).unwrap();
            eom = event.eom();
            count += 1;
        }
        assert_eq!(count, 41);
    }

    #[test]
    fn minimum_window_update_threshold() {
        let (source, producer, log) = session_source(1024 * 1024);
        producer.headers(response(200, None), false);
        producer.body(sized_body(128 * 1024), 0, false);
        read_header(&source).unwrap();
        read_body(&source, 128 * 1024).unwrap();
        assert_eq!(
            log.borrow().processed,
            vec![(StreamId::new(7), 128 * 1024, 128 * 1024)]
        );
    }

    #[test]
    fn resume_threshold_reports_only_the_crossing_read() {
        let (source, producer, log) = session_source(64 << 10);
        assert!(producer.set_resume_threshold(Some(16 << 10)));
        assert!(producer.headers(request(None), false));
        read_header(&source).unwrap();
        assert_eq!(
            producer.body(sized_body(20 << 10), 0, false),
            FlowControlState::Open
        );
        // 20 KiB to 16 KiB stays at the threshold; 16 KiB to 12 KiB crosses below it.
        for _ in 0..2 {
            read_body(&source, 4 << 10).unwrap();
        }
        assert_eq!(log.borrow().processed, [(StreamId::new(7), 4 << 10, 0)]);
        // Below the threshold, more input and reads are not reported.
        assert_eq!(
            producer.body(sized_body(2 << 10), 0, true),
            FlowControlState::Open
        );
        while !read_body(&source, 4 << 10).unwrap().eom() {}
        assert_eq!(log.borrow().processed, [(StreamId::new(7), 4 << 10, 0)]);
        assert!(log.borrow().opened.is_empty());
    }

    #[test]
    fn resume_threshold_still_reports_a_reopened_window() {
        let (source, producer, log) = session_source(32 << 10);
        assert!(producer.set_resume_threshold(Some(8 << 10)));
        assert!(producer.headers(request(None), false));
        read_header(&source).unwrap();
        assert_eq!(
            producer.body(sized_body(32 << 10), 0, false),
            FlowControlState::Closed
        );
        // 32 KiB to 28 KiB reopens the window above the threshold: only `window_open`.
        read_body(&source, 4 << 10).unwrap();
        assert!(log.borrow().processed.is_empty());
        assert_eq!(log.borrow().opened, [StreamId::new(7)]);
        // 8 KiB to 4 KiB crosses below the threshold; the window opens only once.
        for _ in 0..6 {
            read_body(&source, 4 << 10).unwrap();
        }
        assert_eq!(log.borrow().processed, [(StreamId::new(7), 4 << 10, 0)]);
        assert_eq!(log.borrow().opened, [StreamId::new(7)]);
    }

    #[test]
    fn trailers_are_terminal() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(request(None), false);
        let mut trailers = HttpHeaders::new();
        trailers.add(b"foo", b"bar");
        producer.trailers(trailers);
        assert!(!read_header(&source).unwrap().eom());
        let event = read_body(&source, u32::MAX).unwrap();
        assert_eq!(event.event_type(), HttpBodyEventType::Trailers);
        assert!(event.eom());
    }

    #[test]
    fn invalid_producer_transitions_clear_queue() {
        let scenarios = [0, 1, 2, 3];
        for scenario in scenarios {
            let (source, producer, _) = session_source(65_535);
            match scenario {
                0 => {
                    producer.body(body(b"body"), 0, false);
                    producer.eom();
                }
                1 => {
                    producer.headers(response(200, None), false);
                    producer.body(body(b"body"), 0, false);
                    producer.headers(response(200, None), false);
                }
                2 => {
                    producer.headers(response(200, None), true);
                    let push = HttpFixedSource::make_fixed_response(200, None).unwrap();
                    producer.push_promise(request(None), Box::new(push), false);
                }
                _ => {
                    producer.trailers(HttpHeaders::new());
                }
            }
            assert_eq!(
                error_code(read_header(&source)),
                HttpErrorCode::INVALID_STATE_TRANSITION
            );
        }
    }

    #[test]
    fn reject_header_read_after_body_phase() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, None), false);
        producer.body(sized_body(100), 0, false);
        read_header(&source).unwrap();
        read_body(&source, u32::MAX).unwrap();
        assert_eq!(
            error_code(ready(Box::pin(
                source.read_header_event_unboxed(CancellationToken::never())
            ))),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
    }

    #[test]
    fn reject_body_read_before_headers() {
        let (source, producer, _) = session_source(65_535);
        let mut read = source.read_body_event_with_cancellation(u32::MAX, Default::default());
        producer.headers(response(200, None), false);
        assert_eq!(
            match poll_once(read.as_mut()) {
                Poll::Ready(result) => error_code(result),
                Poll::Pending => panic!("invalid read must complete"),
            },
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
    }

    #[test]
    fn unpolled_valid_header_read_does_not_reserve_the_reader() {
        let (source, producer, _) = session_source(65_535);
        let unpolled = source.read_header_event_with_cancellation(Default::default());
        let named = source.read_header_event_unboxed(CancellationToken::never());
        producer.headers(response(200, None), true);

        assert!(read_header(&source).unwrap().eom());
        drop(unpolled);
        drop(named);
    }

    #[test]
    fn unpolled_valid_body_read_does_not_reserve_the_reader() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, None), false);
        read_header(&source).unwrap();
        let unpolled = source.read_body_event_with_cancellation(u32::MAX, Default::default());
        producer.body(body(b"body"), 0, true);

        assert!(read_body(&source, u32::MAX).unwrap().eom());
        drop(unpolled);
    }

    #[test]
    fn unpolled_invalid_body_read_does_not_poison_header_input() {
        let (source, producer, _) = session_source(65_535);
        let invalid = source.read_body_event_with_cancellation(u32::MAX, Default::default());
        producer.headers(response(200, None), true);

        assert!(read_header(&source).unwrap().eom());
        drop(invalid);
    }

    #[test]
    fn unpolled_invalid_header_read_does_not_poison_body_input() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, None), false);
        read_header(&source).unwrap();
        let invalid = source.read_header_event_with_cancellation(Default::default());
        let named = source.read_header_event_unboxed(CancellationToken::never());
        producer.body(body(b"body"), 0, true);

        assert!(read_body(&source, u32::MAX).unwrap().eom());
        drop(invalid);
        drop(named);
    }

    #[test]
    fn invalid_transition_wakes_header_wait() {
        let (source, producer, _) = session_source(65_535);
        let mut read = source.read_header_event_with_cancellation(Default::default());
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(!producer.eom());
        let error = match poll_once(read.as_mut()) {
            Poll::Ready(result) => error_code(result),
            Poll::Pending => panic!("invalid transition must wake the reader"),
        };
        assert_eq!(error, HttpErrorCode::INVALID_STATE_TRANSITION);
    }

    #[test]
    fn stop_while_waiting_completes_on_owner() {
        let (source, producer, log) = session_source(4_000);
        let mut read = source.read_header_event_with_cancellation(Default::default());
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(producer.stop_reading(None));
        let error = match poll_once(read.as_mut()) {
            Poll::Ready(result) => error_code(result),
            Poll::Pending => panic!("stop must wake the reader"),
        };
        assert_eq!(error, HttpErrorCode::NO_ERROR);
        assert_eq!(
            log.borrow().completions,
            vec![Some(HttpErrorCode::NO_ERROR)]
        );
    }

    #[test]
    fn body_error_is_terminal() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, None), false);
        read_header(&source).unwrap();
        let suspend = read_body(&source, u32::MAX).unwrap();
        assert_eq!(suspend.event_type(), HttpBodyEventType::Suspend);
        drop(suspend);
        let mut read = source.read_body_event_with_cancellation(u32::MAX, Default::default());
        assert!(poll_once(read.as_mut()).is_pending());
        producer.abort(HttpErrorCode::TRANSPORT_READ_ERROR, b"");
        let error = match poll_once(read.as_mut()) {
            Poll::Ready(result) => error_code(result),
            Poll::Pending => panic!("abort must wake the reader"),
        };
        assert_eq!(error, HttpErrorCode::TRANSPORT_READ_ERROR);
    }

    #[test]
    fn stop_reading_completes_once() {
        let (source, producer, log) = session_source(65_535);
        let mut read = source.read_header_event_with_cancellation(Default::default());
        assert!(poll_once(read.as_mut()).is_pending());
        producer.headers(response(200, None), false);
        assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(_))));
        producer.stop_reading(None);
        assert!(!producer.stop_reading(None));
        assert_eq!(log.borrow().completions.len(), 1);
    }

    #[test]
    fn cancelled_wait_returns_coro_cancelled() {
        let (source, producer, _) = session_source(65_535);
        let cancellation = CancellationSource::new();
        let mut read = source.read_header_event_unboxed(cancellation.token());
        assert!(poll_once(Pin::new(&mut read)).is_pending());
        producer.headers(response(200, None), false);
        cancellation.request_cancellation();
        let error = match poll_once(Pin::new(&mut read)) {
            Poll::Ready(result) => error_code(result),
            Poll::Pending => panic!("cancellation must wake the reader"),
        };
        assert_eq!(error, HttpErrorCode::CORO_CANCELLED);
    }

    #[test]
    fn first_wait_uses_latest_pre_wait_timeout() {
        run_paused(async {
            let (mut source, producer, _) = timed_session_source(8, Duration::from_millis(5));
            source.set_read_timeout(Duration::from_millis(10));
            assert_eq!(source.read_timeout(), Duration::from_millis(10));
            assert!(producer.set_read_timeout(Duration::from_millis(20)));
            assert_eq!(source.read_timeout(), Duration::from_millis(20));
            let mut read = source.read_header_event_unboxed(CancellationToken::never());
            time::sleep(Duration::from_millis(50)).await;

            assert!(poll_once(Pin::new(&mut read)).is_pending());
            assert!(
                time::timeout(Duration::from_millis(15), &mut read)
                    .await
                    .is_err()
            );
            time::sleep(Duration::from_millis(10)).await;
            let finished_at = time::Instant::now();
            let error = match read.await {
                Err(error) => error,
                Ok(_) => panic!("the pending header read must time out"),
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"Read timeout after 20");
            assert_eq!(time::Instant::now(), finished_at);
        });
    }

    #[test]
    fn unpolled_suspend_preserves_producer_signal() {
        let (source, producer, log) = session_source(8);
        assert!(producer.headers(response(200, None), false));
        assert!(!read_header(&source).unwrap().eom());
        let mut resume = take_resume(read_body(&source, 8).unwrap());
        assert_eq!(
            producer.body(body(b"body"), 0, true),
            FlowControlState::Open
        );
        assert_eq!(
            poll_once(resume.as_mut()),
            Poll::Ready(ResumeStatus::Signalled)
        );
        drop(resume);

        let event = read_body(&source, 8).unwrap();
        assert_eq!(
            event.as_body().unwrap().coalesce(8).unwrap().as_ref(),
            b"body"
        );
        assert!(event.eom());
        assert_eq!(log.borrow().completions, [None]);
    }

    #[test]
    fn source_teardown_preserves_host_through_callback_drop() {
        use std::rc::Weak;

        use roxygen_runtime::host::Host;
        use roxygen_runtime::host::LocalFuture;
        use roxygen_runtime::host::LocalHost;
        use roxygen_runtime::host::TokioHost;

        struct RecordingHost(Rc<RefCell<Vec<&'static str>>>);

        impl LocalHost for RecordingHost {
            fn spawn_local(&self, future: LocalFuture) {
                TokioHost.spawn_local(future);
            }

            fn now(&self) -> Instant {
                TokioHost.now()
            }

            fn sleep_until(&self, deadline: Instant) -> LocalFuture {
                TokioHost.sleep_until(deadline)
            }

            fn yield_to_io(&self) -> LocalFuture {
                TokioHost.yield_to_io()
            }
        }

        impl Drop for RecordingHost {
            fn drop(&mut self) {
                self.0.borrow_mut().push("host");
            }
        }

        struct HostCallback {
            host: Weak<RecordingHost>,
            log: Rc<RefCell<Vec<&'static str>>>,
            host_was_live: Rc<Cell<bool>>,
        }

        impl HttpStreamSourceCallback for HostCallback {}
        impl HttpStreamSourceSharedCallback for HostCallback {}

        impl Drop for HostCallback {
            fn drop(&mut self) {
                self.host_was_live.set(self.host.upgrade().is_some());
                self.log.borrow_mut().push("callback");
            }
        }

        run_paused(async {
            for registered_wait in [false, true] {
                let log = Rc::new(RefCell::new(Vec::new()));
                let host_was_live = Rc::new(Cell::new(false));
                let host = Rc::new(RecordingHost(Rc::clone(&log)));
                let weak_host = Rc::downgrade(&host);
                let source = HttpStreamSource::new_session_with_host(
                    StreamId::new(7),
                    HostCallback {
                        host: weak_host.clone(),
                        log: Rc::clone(&log),
                        host_was_live: Rc::clone(&host_was_live),
                    },
                    8,
                    Duration::ZERO,
                    Host::new(host.clone()),
                )
                .unwrap();
                let producer = source.producer();
                drop(host);
                let mut read = source.read_header_event_unboxed(CancellationToken::never());
                if registered_wait {
                    assert!(poll_once(Pin::new(&mut read)).is_pending());
                }
                assert!(producer.headers(response(200, None), true));
                assert!(ready(Box::pin(read)).unwrap().eom());
                assert!(log.borrow().is_empty());
                drop(source);

                assert!(host_was_live.get(), "registered_wait={registered_wait}");
                assert_eq!(*log.borrow(), ["callback", "host"]);
                assert!(weak_host.upgrade().is_none());
            }
            for response_source in [false, true] {
                for capacity in [8, u32::MAX] {
                    let log = Rc::new(RefCell::new(Vec::new()));
                    let host_was_live = Rc::new(Cell::new(false));
                    let host = Rc::new(RecordingHost(Rc::clone(&log)));
                    let weak_host = Rc::downgrade(&host);
                    let callback = Rc::new(HostCallback {
                        host: weak_host.clone(),
                        log: Rc::clone(&log),
                        host_was_live: Rc::clone(&host_was_live),
                    });
                    let weak_callback = Rc::downgrade(&callback);
                    let owner = shared_source(response_source, callback, capacity, Host::new(host));
                    if capacity == u32::MAX {
                        assert!(owner.is_err());
                        assert_eq!(*log.borrow(), ["host", "callback"]);
                        assert!(!host_was_live.get());
                    } else {
                        let owner = owner.unwrap();
                        let source = owner.source().unwrap();
                        let producer = source.producer();
                        let mut read = source.read_header_event_unboxed(CancellationToken::never());
                        assert!(poll_once(Pin::new(&mut read)).is_pending());
                        drop(owner);
                        assert!(producer.is_live());
                        assert!(weak_callback.upgrade().is_some());
                        assert!(weak_host.upgrade().is_some());
                        assert!(log.borrow().is_empty());
                        drop(read);
                        assert!(!producer.is_live());
                        assert!(host_was_live.get());
                        assert_eq!(*log.borrow(), ["callback", "host"]);
                    }
                    assert!(weak_callback.upgrade().is_none());
                    assert!(weak_host.upgrade().is_none());
                }
            }
        });
    }

    #[test]
    fn transferred_suspend_cancellation_does_not_consume_signalled_body() {
        run_paused(async {
            let (source, producer, _) = timed_session_source(65_535, Duration::from_secs(2));
            producer.headers(response(200, None), false);
            read_header(&source).unwrap();

            let cancellation = CancellationSource::new();
            let mut resume = take_resume(
                ready(source.read_body_event_with_cancellation(u32::MAX, cancellation.token()))
                    .unwrap(),
            );
            assert!(poll_once(resume.as_mut()).is_pending());
            producer.body(body(b"body"), 0, true);
            cancellation.request_cancellation();
            assert_eq!(resume.await, ResumeStatus::Cancelled);

            let event = read_body(&source, u32::MAX).unwrap();
            assert_eq!(event.as_body().unwrap().coalesce(4).unwrap(), b"body"[..]);
            assert!(event.eom());
        });
    }

    #[test]
    fn transferred_suspend_rejects_an_overlapping_read() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, None), false);
        read_header(&source).unwrap();

        let mut resume = take_resume(read_body(&source, u32::MAX).unwrap());
        assert!(poll_once(resume.as_mut()).is_pending());
        assert_eq!(
            error_code(read_body(&source, u32::MAX)),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );

        drop(resume);
        producer.body(body(b"body"), 0, true);
        assert!(read_body(&source, u32::MAX).unwrap().eom());
    }

    #[test]
    fn transferred_suspend_timeout_does_not_consume_later_body() {
        run_paused(async {
            let (source, producer, _) = timed_session_source(65_535, Duration::from_secs(2));
            producer.headers(response(200, None), false);
            read_header(&source).unwrap();

            let mut resume = take_resume(read_body(&source, u32::MAX).unwrap());
            assert!(poll_once(resume.as_mut()).is_pending());
            time::advance(Duration::from_secs(2)).await;
            assert_eq!(resume.await, ResumeStatus::TimedOut);

            producer.body(body(b"body"), 0, true);
            let event = read_body(&source, u32::MAX).unwrap();
            assert_eq!(event.as_body().unwrap().coalesce(4).unwrap(), b"body"[..]);
            assert!(event.eom());
        });
    }

    #[test]
    fn ordinary_wait_uses_exact_timeout_message() {
        run_paused(async {
            let (source, producer, _) = timed_session_source(65_535, Duration::from_secs(2));
            producer.headers(response(200, None), false);
            read_header(&source).unwrap();
            drop(read_body(&source, u32::MAX).unwrap());

            let mut read = source.read_body_event_with_cancellation(u32::MAX, Default::default());
            assert!(poll_once(read.as_mut()).is_pending());
            time::advance(Duration::from_secs(2)).await;
            let error = match read.await {
                Err(error) => error,
                Ok(_) => panic!("timed body read unexpectedly succeeded"),
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"Read timeout after 2000");
        });
    }

    #[test]
    fn padding_preserves_event_order_without_body_credit() {
        let (source, producer, _) = session_source(10);
        producer.headers(response(200, None), false);
        producer.padding(7);
        producer.body(body(b"x"), 0, true);
        read_header(&source).unwrap();
        let padding = read_body(&source, 1).unwrap();
        assert_eq!(padding.event_type(), HttpBodyEventType::Padding);
        assert!(!padding.eom());
        assert_eq!(source.body_bytes_buffered(), 1);
        assert!(read_body(&source, 1).unwrap().eom());
    }

    #[test]
    fn strict_flow_control_failure_is_atomic_and_terminal() {
        let (source, producer, _) = session_source(1);
        producer.headers(response(200, None), false);
        assert_eq!(
            producer.body(sized_body(2), 0, false),
            FlowControlState::Error
        );
        assert_eq!(source.body_bytes_buffered(), 0);
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::FLOW_CONTROL_ERROR
        );
    }

    #[test]
    fn terminal_queue_clear_returns_credit_before_completion() {
        let log = Rc::new(RefCell::new(CallbackLog::default()));
        let source = HttpStreamSource::new_async(
            None,
            Some(Box::new(RecordingCallback(Rc::clone(&log)))),
            2,
        )
        .unwrap();
        let producer = source.producer();
        producer.headers(response(200, None), false);
        assert_eq!(
            producer.body(sized_body(2), 0, false),
            FlowControlState::Closed
        );
        assert!(!producer.headers(response(200, None), false));
        assert_eq!(source.body_bytes_buffered(), 0);
        assert_eq!(log.borrow().processed, vec![(MAX_STREAM_ID, 2, 0)]);
        assert_eq!(log.borrow().opened, vec![MAX_STREAM_ID]);
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
    }

    struct ReadBodyOwner {
        bytes: [u8; 4],
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for ReadBodyOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for ReadBodyOwner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn owned_read_body(drops: &Arc<AtomicUsize>) -> (BufQueue, *const u8) {
        let bytes = Bytes::from_owner(ReadBodyOwner {
            bytes: *b"body",
            drops: Arc::clone(drops),
        });
        let backing = bytes.as_ptr();
        (BufQueue::from_bytes(bytes, 4).unwrap(), backing)
    }

    #[test]
    fn body_queue_refill_preserves_order_credit_and_payload_lifetimes() {
        let (source, producer, log) = session_source(8);
        let drops = Arc::new(AtomicUsize::new(0));
        assert!(producer.headers(response(200, None), false));
        let (tracked, backing) = owned_read_body(&drops);
        assert_eq!(producer.body(tracked, 0, false), FlowControlState::Open);
        assert!(producer.padding(7));
        assert_eq!(
            producer.body(body(b"tail"), 0, false),
            FlowControlState::Closed
        );
        assert_eq!(source.body_bytes_buffered(), 8);
        assert!(!read_header(&source).unwrap().eom());

        let first = read_body(&source, 2).unwrap();
        let bytes = first.as_body().unwrap().front().unwrap();
        assert_eq!(bytes.as_ref(), b"bo");
        assert_eq!(bytes.as_ptr(), backing);
        assert!(!first.eom());
        assert_eq!(source.body_bytes_buffered(), 6);
        let remainder = read_body(&source, 8).unwrap();
        let bytes = remainder.as_body().unwrap().front().unwrap();
        assert_eq!(bytes.as_ref(), b"dy");
        assert_eq!(bytes.as_ptr(), backing.wrapping_add(2));
        assert!(!remainder.eom());
        drop(remainder);
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        let padding = read_body(&source, 8).unwrap();
        assert!(matches!(padding.kind(), HttpBodyEventKind::Padding(7)));
        assert!(!padding.eom());
        assert_eq!(source.body_bytes_buffered(), 4);
        assert_eq!(log.borrow().processed.len(), 2);
        let tail = read_body(&source, 8).unwrap();
        assert_eq!(
            tail.as_body().unwrap().coalesce(8).unwrap().as_ref(),
            b"tail"
        );
        assert!(!tail.eom());
        assert_eq!(source.body_bytes_buffered(), 0);
        assert!(log.borrow().completions.is_empty());
        drop(first);
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let (tracked, backing) = owned_read_body(&drops);
        assert_eq!(producer.body(tracked, 0, false), FlowControlState::Open);
        assert_eq!(
            producer.body(body(b"more"), 0, false),
            FlowControlState::Closed
        );
        let mut trailers = HttpHeaders::new();
        trailers.add(b"x-trailer", b"done");
        assert!(producer.trailers(trailers));
        assert_eq!(source.body_bytes_buffered(), 8);
        let refilled = read_body(&source, 8).unwrap();
        let body = refilled.as_body().unwrap();
        assert_eq!(body.coalesce(8).unwrap().as_ref(), b"bodymore");
        assert_eq!(body.front().unwrap().as_ptr(), backing);
        assert!(!refilled.eom());
        assert_eq!(source.body_bytes_buffered(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(log.borrow().completions.is_empty());

        let terminal = read_body(&source, 8).unwrap();
        assert!(terminal.eom());
        let HttpBodyEventKind::Trailers(trailers) = terminal.into_kind() else {
            panic!("expected terminal trailers")
        };
        assert_eq!(trailers.get_single_or_empty(b"x-trailer"), b"done");
        assert_eq!(
            log.borrow().processed,
            [
                (StreamId::new(7), 2, 0),
                (StreamId::new(7), 2, 4),
                (StreamId::new(7), 4, 4),
                (StreamId::new(7), 8, 8),
            ]
        );
        assert_eq!(log.borrow().opened, [StreamId::new(7), StreamId::new(7)]);
        assert_eq!(log.borrow().completions, [None]);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(refilled);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(source);
        assert_eq!(log.borrow().completions, [None]);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    struct SharedReentrantCallback {
        producer: RefCell<Option<HttpStreamSourceProducer>>,
        log: Rc<RefCell<Vec<&'static str>>>,
        body_drops: Arc<AtomicUsize>,
        panic_markers: Option<(Arc<()>, Arc<()>)>,
    }

    impl HttpStreamSourceSharedCallback for SharedReentrantCallback {
        fn bytes_processed(&self, id: StreamId, amount: usize, acknowledged: usize) {
            self.log.borrow_mut().push("credit.enter");
            assert_eq!((id, amount, acknowledged), (StreamId::new(7), 4, 0));
            assert_eq!(self.body_drops.load(Ordering::SeqCst), 0);
            let producer = self.producer.borrow().as_ref().unwrap().clone();
            assert!(producer.stop_reading(Some(HttpErrorCode::READ_TIMEOUT)));
            self.log.borrow_mut().push("credit.exit");
            if let Some((first, _)) = &self.panic_markers {
                std::panic::panic_any(ReadCallbackPanic(Arc::clone(first)));
            }
        }

        fn window_open(&self, id: StreamId) {
            assert_eq!(id, StreamId::new(7));
            self.log.borrow_mut().push("window");
        }

        fn source_complete(&self, id: StreamId, error: Option<HttpError>) {
            self.log.borrow_mut().push("complete");
            assert_eq!(id, StreamId::new(7));
            assert_eq!(error.unwrap().code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(
                self.body_drops.load(Ordering::SeqCst),
                usize::from(self.panic_markers.is_some()),
            );
            if let Some((_, cleanup)) = &self.panic_markers {
                self.log.borrow_mut().push("cleanup.panic");
                std::panic::panic_any(ReadCallbackPanic(Arc::clone(cleanup)));
            }
        }
    }

    impl Drop for SharedReentrantCallback {
        fn drop(&mut self) {
            self.log.borrow_mut().push("callback.drop");
        }
    }

    #[test]
    fn shared_callbacks_serialize_reentrant_credit_and_completion() {
        for response_source in [false, true] {
            let log = Rc::new(RefCell::new(Vec::new()));
            let drops = Arc::new(AtomicUsize::new(0));
            let callback = Rc::new(SharedReentrantCallback {
                producer: RefCell::new(None),
                log: Rc::clone(&log),
                body_drops: Arc::clone(&drops),
                panic_markers: None,
            });
            let weak_callback = Rc::downgrade(&callback);
            let owner = shared_source(response_source, callback.clone(), 4, Host::tokio()).unwrap();
            let source = owner.source().unwrap();
            let producer = source.producer();
            *callback.producer.borrow_mut() = Some(producer.clone());
            drop(callback);
            assert!(producer.headers(response(200, None), false));
            let (body, _) = owned_read_body(&drops);
            assert_eq!(producer.body(body, 0, false), FlowControlState::Closed);
            assert!(producer.stop_reading(Some(HttpErrorCode::READ_TIMEOUT)));
            assert!(source.source_complete());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                *log.borrow(),
                ["credit.enter", "credit.exit", "complete", "window"],
            );
            drop(owner);
            assert!(weak_callback.upgrade().is_none());
            assert_eq!(
                *log.borrow(),
                [
                    "credit.enter",
                    "credit.exit",
                    "complete",
                    "window",
                    "callback.drop",
                ],
            );
            assert!(!producer.is_live());
        }
    }

    #[test]
    fn shared_callback_panic_releases_pending_read_and_preserves_first_panic() {
        for response_source in [false, true] {
            let log = Rc::new(RefCell::new(Vec::new()));
            let drops = Arc::new(AtomicUsize::new(0));
            let first = Arc::new(());
            let cleanup = Arc::new(());
            let callback = Rc::new(SharedReentrantCallback {
                producer: RefCell::new(None),
                log: Rc::clone(&log),
                body_drops: Arc::clone(&drops),
                panic_markers: Some((Arc::clone(&first), cleanup)),
            });
            let weak_callback = Rc::downgrade(&callback);
            let owner = shared_source(response_source, callback.clone(), 4, Host::tokio()).unwrap();
            let source = owner.source().unwrap();
            let producer = source.producer();
            *callback.producer.borrow_mut() = Some(producer.clone());
            drop(callback);
            assert!(producer.headers(response(200, None), false));
            read_header(source).unwrap();
            drop(read_body(source, 4).unwrap());
            let mut read = source.read_body_event_unboxed(4, CancellationToken::never());
            assert!(poll_once(Pin::new(&mut read)).is_pending());
            let (body, _) = owned_read_body(&drops);
            assert_eq!(producer.body(body, 0, true), FlowControlState::Closed);
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = poll_once(Pin::new(&mut read));
            }))
            .expect_err("the original shared callback panic must escape");
            assert!(Arc::ptr_eq(
                &panic.downcast_ref::<ReadCallbackPanic>().unwrap().0,
                &first,
            ));
            drop(panic);
            assert_eq!(
                *log.borrow(),
                ["credit.enter", "credit.exit", "complete", "cleanup.panic"],
            );
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(source.source_complete());
            assert_eq!(source.body_bytes_buffered(), 0);
            let mut successor = source.read_body_event_unboxed(4, CancellationToken::never());
            match poll_once(Pin::new(&mut successor)) {
                Poll::Ready(Err(error)) => assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT),
                _ => panic!("callback cleanup must release the failed read's admission"),
            }
            source.set_callback(None);
            assert!(weak_callback.upgrade().is_none());
            assert_eq!(
                *log.borrow(),
                [
                    "credit.enter",
                    "credit.exit",
                    "complete",
                    "cleanup.panic",
                    "callback.drop",
                ],
            );
            drop(read);
            drop(successor);
            drop(owner);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn shared_callback_replacement_preserves_drop_priority_and_pending_order() {
        struct Replacement {
            log: Rc<RefCell<Vec<&'static str>>>,
            panic_once: Rc<Cell<bool>>,
            marker: Arc<()>,
        }

        impl HttpStreamSourceCallback for Replacement {}

        impl Drop for Replacement {
            fn drop(&mut self) {
                self.log.borrow_mut().push("replacement.drop");
                if self.panic_once.replace(false) {
                    std::panic::panic_any(ReadCallbackPanic(Arc::clone(&self.marker)));
                }
            }
        }

        struct Callback {
            owner: RefCell<std::rc::Weak<HttpStreamSourceOwner>>,
            first_credit: Cell<bool>,
            log: Rc<RefCell<Vec<&'static str>>>,
            replacement_panic: Rc<Cell<bool>>,
            original_marker: Arc<()>,
            replacement_marker: Arc<()>,
        }

        impl HttpStreamSourceSharedCallback for Callback {
            fn bytes_processed(&self, _: StreamId, amount: usize, acknowledged: usize) {
                assert_eq!(acknowledged, 0);
                if self.first_credit.replace(false) {
                    assert_eq!(amount, 1);
                    self.log.borrow_mut().push("credit1");
                    let owner = self.owner.borrow().upgrade().unwrap();
                    let source = owner.source().unwrap();
                    source.set_callback(Some(Box::new(Replacement {
                        log: Rc::clone(&self.log),
                        panic_once: Rc::clone(&self.replacement_panic),
                        marker: Arc::clone(&self.replacement_marker),
                    })));
                    assert!(
                        source
                            .producer()
                            .stop_reading(Some(HttpErrorCode::READ_TIMEOUT))
                    );
                    self.log.borrow_mut().push("queued.complete");
                    self.log.borrow_mut().push("original.panic");
                    std::panic::panic_any(ReadCallbackPanic(Arc::clone(&self.original_marker)));
                }
                assert_eq!(amount, 2);
                self.log.borrow_mut().push("credit2");
            }

            fn window_open(&self, _: StreamId) {
                self.log.borrow_mut().push("window");
            }

            fn source_complete(&self, _: StreamId, error: Option<HttpError>) {
                assert_eq!(error.unwrap().code(), HttpErrorCode::READ_TIMEOUT);
                self.log.borrow_mut().push("complete");
            }
        }

        for replacement_panics in [false, true] {
            let log = Rc::new(RefCell::new(Vec::new()));
            let replacement_panic = Rc::new(Cell::new(replacement_panics));
            let original_marker = Arc::new(());
            let replacement_marker = Arc::new(());
            let callback = Rc::new(Callback {
                owner: RefCell::new(std::rc::Weak::new()),
                first_credit: Cell::new(true),
                log: Rc::clone(&log),
                replacement_panic: Rc::clone(&replacement_panic),
                original_marker: Arc::clone(&original_marker),
                replacement_marker: Arc::clone(&replacement_marker),
            });
            let owner = Rc::new(shared_source(false, callback.clone(), 3, Host::tokio()).unwrap());
            *callback.owner.borrow_mut() = Rc::downgrade(&owner);
            drop(callback);
            let source = owner.source().unwrap();
            let producer = source.producer();
            assert!(producer.headers(response(200, None), false));
            assert_eq!(producer.body(body(b"a"), 0, false), FlowControlState::Open);
            assert!(producer.padding(1));
            assert_eq!(
                producer.body(body(b"bc"), 0, false),
                FlowControlState::Closed
            );
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                producer.stop_reading(Some(HttpErrorCode::READ_TIMEOUT));
            }));
            replacement_panic.set(false);
            if replacement_panics {
                source.set_callback(None);
            }
            let panic = result.expect_err("the callback or replacement destructor must panic");
            assert_eq!(
                *log.borrow(),
                [
                    "credit1",
                    "queued.complete",
                    "original.panic",
                    "replacement.drop"
                ],
            );
            let expected = if replacement_panics {
                &replacement_marker
            } else {
                &original_marker
            };
            assert!(Arc::ptr_eq(
                &panic.downcast_ref::<ReadCallbackPanic>().unwrap().0,
                expected,
            ));
            drop(panic);
            assert!(source.source_complete());
            assert_eq!(source.body_bytes_buffered(), 2);
            if !replacement_panics {
                source.mark_egress_only();
                assert_eq!(source.body_bytes_buffered(), 0);
                assert_eq!(
                    *log.borrow(),
                    [
                        "credit1",
                        "queued.complete",
                        "original.panic",
                        "replacement.drop",
                        "credit2",
                        "complete",
                    ],
                );
                source.set_callback(None);
            }
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum ReadCallbackStage {
        Credit,
        Completion,
    }

    struct ReadCallbackPanic(Arc<()>);

    struct ReadBoundaryCallback {
        source: std::rc::Weak<HttpStreamSource>,
        log: Rc<RefCell<Vec<&'static str>>>,
        body_drops: Arc<AtomicUsize>,
        panic_at: Option<(ReadCallbackStage, Arc<()>)>,
    }

    impl ReadBoundaryCallback {
        fn panic_if(&self, stage: ReadCallbackStage) {
            if let Some((expected, marker)) = &self.panic_at
                && *expected == stage
            {
                std::panic::panic_any(ReadCallbackPanic(Arc::clone(marker)));
            }
        }
    }

    impl HttpStreamSourceCallback for ReadBoundaryCallback {
        fn bytes_processed(&mut self, _id: StreamId, amount: usize, to_acknowledge: usize) {
            self.log.borrow_mut().push("credit");
            let source = self.source.upgrade().unwrap();
            assert!(amount > 0);
            assert_eq!(to_acknowledge, 0);
            assert!(!source.source_complete());
            assert_eq!(self.body_drops.load(Ordering::SeqCst), 0);
            let error = match read_body(&source, u32::MAX) {
                Err(error) => error,
                Ok(_) => panic!("credit callbacks retain the active read"),
            };
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(
                error.message(),
                b"HTTP stream source already has an active read or resume wait"
            );
            self.panic_if(ReadCallbackStage::Credit);
        }

        fn window_open(&mut self, _id: StreamId) {
            self.log.borrow_mut().push("window");
            assert!(!self.source.upgrade().unwrap().source_complete());
            assert_eq!(self.body_drops.load(Ordering::SeqCst), 0);
        }

        fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
            self.log.borrow_mut().push("complete");
            let source = self.source.upgrade().unwrap();
            assert!(error.is_none());
            assert!(source.source_complete());
            assert_eq!(source.body_bytes_buffered(), 0);
            assert!(!source.producer().stop_reading(None));
            assert_eq!(self.body_drops.load(Ordering::SeqCst), 0);
            self.panic_if(ReadCallbackStage::Completion);
        }
    }

    #[test]
    fn ready_body_keeps_backing_across_reentrant_credit_and_completion() {
        let (source, producer, _) = session_source(4);
        let source = Rc::new(source);
        let log = Rc::new(RefCell::new(Vec::new()));
        let drops = Arc::new(AtomicUsize::new(0));
        source.set_callback(Some(Box::new(ReadBoundaryCallback {
            source: Rc::downgrade(&source),
            log: Rc::clone(&log),
            body_drops: Arc::clone(&drops),
            panic_at: None,
        })));
        assert!(producer.headers(response(200, None), false));
        read_header(&source).unwrap();
        let (body, backing) = owned_read_body(&drops);
        assert_eq!(producer.body(body, 0, true), FlowControlState::Closed);

        let first = read_body(&source, 2).unwrap();
        let bytes = first.as_body().unwrap().front().unwrap();
        assert_eq!(bytes.as_ptr(), backing);
        assert_eq!(bytes.as_ref(), b"bo");
        assert!(!first.eom());
        assert_eq!(source.body_bytes_buffered(), 2);
        assert_eq!(*log.borrow(), ["credit", "window"]);
        drop(first);
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        let last = read_body(&source, 2).unwrap();
        let bytes = last.as_body().unwrap().front().unwrap();
        assert_eq!(bytes.as_ptr(), backing.wrapping_add(2));
        assert_eq!(bytes.as_ref(), b"dy");
        assert!(last.eom());
        assert_eq!(*log.borrow(), ["credit", "window", "credit", "complete"]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(last);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ready_body_callback_panic_drops_result_once_and_preserves_first_panic() {
        for stage in [ReadCallbackStage::Credit, ReadCallbackStage::Completion] {
            let (source, producer, _) = session_source(4);
            let source = Rc::new(source);
            let log = Rc::new(RefCell::new(Vec::new()));
            let drops = Arc::new(AtomicUsize::new(0));
            let marker = Arc::new(());
            source.set_callback(Some(Box::new(ReadBoundaryCallback {
                source: Rc::downgrade(&source),
                log: Rc::clone(&log),
                body_drops: Arc::clone(&drops),
                panic_at: Some((stage, Arc::clone(&marker))),
            })));
            assert!(producer.headers(response(200, None), false));
            read_header(&source).unwrap();
            let (body, _) = owned_read_body(&drops);
            assert_eq!(producer.body(body, 0, true), FlowControlState::Closed);

            let mut read = source.read_body_event_with_cancellation(u32::MAX, Default::default());
            let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = poll_once(read.as_mut());
            }))
            .expect_err("the callback panic must propagate");
            let actual = payload.downcast_ref::<ReadCallbackPanic>().unwrap();
            assert!(Arc::ptr_eq(&actual.0, &marker));
            drop(payload);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(source.body_bytes_buffered(), 0);
            source.set_callback(None);
            match stage {
                ReadCallbackStage::Credit => {
                    assert_eq!(*log.borrow(), ["credit"]);
                    assert!(!source.source_complete());
                    let retry = read_body(&source, u32::MAX).unwrap();
                    assert_eq!(retry.event_type(), HttpBodyEventType::Suspend);
                    drop(retry);
                    let mut successor =
                        source.read_body_event_with_cancellation(u32::MAX, Default::default());
                    assert!(poll_once(successor.as_mut()).is_pending());
                    drop(read);
                    assert_eq!(
                        error_code(read_body(&source, u32::MAX)),
                        HttpErrorCode::INVALID_STATE_TRANSITION
                    );
                    assert!(producer.stop_reading(None));
                    assert_eq!(error_code(ready(successor)), HttpErrorCode::NO_ERROR);
                    assert!(source.source_complete());
                }
                ReadCallbackStage::Completion => {
                    assert_eq!(*log.borrow(), ["credit", "window", "complete"]);
                    assert!(source.source_complete());
                    assert!(!producer.stop_reading(None));
                    drop(read);
                    assert!(source.source_complete());
                    assert!(!producer.stop_reading(None));
                }
            }
            drop(source);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn pending_body_callback_panic_releases_admission_and_preserves_first_panic() {
        struct PendingPanicCallback {
            source: std::rc::Weak<HttpStreamSource>,
            log: Rc<RefCell<Vec<&'static str>>>,
            drops: Arc<AtomicUsize>,
            first: Arc<()>,
            cleanup: Arc<()>,
            fail_source: bool,
        }

        impl HttpStreamSourceCallback for PendingPanicCallback {
            fn bytes_processed(&mut self, _: StreamId, amount: usize, acknowledged: usize) {
                self.log.borrow_mut().push("credit");
                assert_eq!((amount, acknowledged), (4, 0));
                assert_eq!(self.drops.load(Ordering::SeqCst), 0);
                let source = self.source.upgrade().unwrap();
                assert!(!source.source_complete());
                if self.fail_source {
                    assert!(
                        source
                            .producer()
                            .stop_reading(Some(HttpErrorCode::READ_TIMEOUT))
                    );
                }
                std::panic::panic_any(ReadCallbackPanic(Arc::clone(&self.first)));
            }

            fn source_complete(&mut self, _: StreamId, error: Option<HttpError>) {
                self.log.borrow_mut().push("complete");
                assert!(self.fail_source);
                assert_eq!(error.unwrap().code(), HttpErrorCode::READ_TIMEOUT);
                assert!(self.source.upgrade().unwrap().source_complete());
                assert_eq!(self.drops.load(Ordering::SeqCst), 1);
                std::panic::panic_any(ReadCallbackPanic(Arc::clone(&self.cleanup)));
            }
        }

        for fail_source in [false, true] {
            let (source, producer, _) = session_source(4);
            let source = Rc::new(source);
            let log = Rc::new(RefCell::new(Vec::new()));
            let drops = Arc::new(AtomicUsize::new(0));
            let first = Arc::new(());
            let cleanup = Arc::new(());
            source.set_callback(Some(Box::new(PendingPanicCallback {
                source: Rc::downgrade(&source),
                log: Rc::clone(&log),
                drops: Arc::clone(&drops),
                first: Arc::clone(&first),
                cleanup,
                fail_source,
            })));
            assert!(producer.headers(response(200, None), false));
            read_header(&source).unwrap();
            let suspend = read_body(&source, 4).unwrap();
            assert_eq!(suspend.event_type(), HttpBodyEventType::Suspend);
            drop(suspend);
            let mut read = source.read_body_event_unboxed(4, CancellationToken::never());
            assert!(poll_once(Pin::new(&mut read)).is_pending());
            let (body, _) = owned_read_body(&drops);
            assert_eq!(producer.body(body, 0, true), FlowControlState::Closed);

            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = poll_once(Pin::new(&mut read));
            }))
            .expect_err("the original body callback panic must escape");
            assert!(Arc::ptr_eq(
                &panic.downcast_ref::<ReadCallbackPanic>().unwrap().0,
                &first,
            ));
            drop(panic);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(source.body_bytes_buffered(), 0);
            source.set_callback(None);
            if fail_source {
                assert_eq!(*log.borrow(), ["credit", "complete"]);
                assert!(source.source_complete());
                let mut successor = source.read_body_event_unboxed(4, CancellationToken::never());
                match poll_once(Pin::new(&mut successor)) {
                    Poll::Ready(Err(error)) => {
                        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT)
                    }
                    _ => panic!("cleanup must release admission before the failed read drops"),
                }
                drop(read);
                assert!(!producer.stop_reading(None));
                assert_eq!(*log.borrow(), ["credit", "complete"]);
            } else {
                assert_eq!(*log.borrow(), ["credit"]);
                assert!(!source.source_complete());
                let suspend = read_body(&source, 4).unwrap();
                assert_eq!(suspend.event_type(), HttpBodyEventType::Suspend);
                drop(suspend);
                let mut successor = source.read_body_event_unboxed(4, CancellationToken::never());
                assert!(poll_once(Pin::new(&mut successor)).is_pending());
                drop(read);
                assert_eq!(
                    error_code(read_body(&source, 4)),
                    HttpErrorCode::INVALID_STATE_TRANSITION
                );
                assert!(producer.stop_reading(None));
                match poll_once(Pin::new(&mut successor)) {
                    Poll::Ready(Err(error)) => assert_eq!(error.code(), HttpErrorCode::NO_ERROR),
                    _ => panic!("the successor must retain admission until it completes"),
                }
                assert!(source.source_complete());
            }
            drop(source);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn pending_header_poll_panic_releases_admission_and_preserves_first_panic() {
        struct HeaderWakePanic {
            marker: Arc<()>,
            calls: AtomicUsize,
        }

        impl Wake for HeaderWakePanic {
            fn wake(self: Arc<Self>) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(ReadCallbackPanic(Arc::clone(&self.marker)));
            }
        }

        struct HeaderCompletionPanic {
            source: std::rc::Weak<HttpStreamSource>,
            log: Rc<RefCell<CallbackLog>>,
            marker: Arc<()>,
        }

        impl HttpStreamSourceCallback for HeaderCompletionPanic {
            fn source_complete(&mut self, _: StreamId, error: Option<HttpError>) {
                let error = error.unwrap().code();
                self.log.borrow_mut().completions.push(Some(error));
                assert_eq!(error, HttpErrorCode::READ_TIMEOUT);
                assert!(self.source.upgrade().unwrap().source_complete());
                std::panic::panic_any(ReadCallbackPanic(Arc::clone(&self.marker)));
            }
        }

        run_paused(async {
            let (source, producer, log) = timed_session_source(8, Duration::from_secs(2));
            let source = Rc::new(source);
            let first = Arc::new(());
            let cleanup = Arc::new(());
            source.set_callback(Some(Box::new(HeaderCompletionPanic {
                source: Rc::downgrade(&source),
                log: Rc::clone(&log),
                marker: cleanup,
            })));
            let mut read = source.read_header_event_unboxed(CancellationToken::never());
            assert!(poll_once(Pin::new(&mut read)).is_pending());
            // Await a later timer before installing the panic waker.
            time::sleep(Duration::from_secs(3)).await;
            assert!(log.borrow().completions.is_empty());
            let wake = Arc::new(HeaderWakePanic {
                marker: Arc::clone(&first),
                calls: AtomicUsize::new(0),
            });
            let waker = Waker::from(Arc::clone(&wake));
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = Pin::new(&mut read).poll(&mut Context::from_waker(&waker));
            }));
            if panic.is_ok() {
                source.set_callback(None);
            }
            let panic = panic.expect_err("the expired wait's poll wake must panic");
            assert!(Arc::ptr_eq(
                &panic.downcast_ref::<ReadCallbackPanic>().unwrap().0,
                &first,
            ));
            drop(panic);
            assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                log.borrow().completions,
                [Some(HttpErrorCode::READ_TIMEOUT)]
            );
            assert!(source.source_complete());
            let mut successor = source.read_header_event_unboxed(CancellationToken::never());
            match poll_once(Pin::new(&mut successor)) {
                Poll::Ready(Err(error)) => assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT),
                _ => panic!("cleanup must release admission before the failed header read drops"),
            }
            drop(read);
            assert!(!producer.stop_reading(None));
            drop(successor);
            drop(source);
            assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                log.borrow().completions,
                [Some(HttpErrorCode::READ_TIMEOUT)]
            );
        });
    }

    struct PushResultCompletionCallback {
        source: std::rc::Weak<HttpStreamSource>,
        log: Rc<RefCell<Vec<&'static str>>>,
        stops: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        marker: Arc<()>,
    }

    impl HttpStreamSourceCallback for PushResultCompletionCallback {
        fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
            self.log.borrow_mut().push("complete");
            assert!(error.is_none());
            assert!(self.source.upgrade().unwrap().source_complete());
            assert_eq!(self.stops.get(), 0);
            assert_eq!(self.drops.get(), 0);
            std::panic::panic_any(ReadCallbackPanic(Arc::clone(&self.marker)));
        }
    }

    #[test]
    fn ready_push_is_retired_once_after_completion_callback_panics() {
        let (source, producer, _) = session_source(4);
        let source = Rc::new(source);
        let log = Rc::new(RefCell::new(Vec::new()));
        let stops = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let marker = Arc::new(());
        source.set_callback(Some(Box::new(PushResultCompletionCallback {
            source: Rc::downgrade(&source),
            log: Rc::clone(&log),
            stops: Rc::clone(&stops),
            drops: Rc::clone(&drops),
            marker: Arc::clone(&marker),
        })));
        assert!(producer.headers(response(200, None), false));
        read_header(&source).unwrap();
        let on_stop_source = Rc::downgrade(&source);
        let on_stop_log = Rc::clone(&log);
        assert!(producer.push_promise(
            request(None),
            Box::new(ReentrantStopProbe {
                stops: Rc::clone(&stops),
                drops: Rc::clone(&drops),
                on_stop: Some(Box::new(move || {
                    assert!(on_stop_source.upgrade().unwrap().source_complete());
                    on_stop_log.borrow_mut().push("push stop");
                })),
            }),
            false,
        ));
        assert!(producer.eom());

        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = read_body(&source, u32::MAX);
        }))
        .expect_err("completion panic must precede retiring the owned push");
        let actual = payload.downcast_ref::<ReadCallbackPanic>().unwrap();
        assert!(Arc::ptr_eq(&actual.0, &marker));
        drop(payload);
        assert_eq!(*log.borrow(), ["complete", "push stop"]);
        assert_eq!(stops.get(), 1);
        assert_eq!(drops.get(), 1);
        assert!(!producer.stop_reading(None));
        drop(source);
        assert_eq!(stops.get(), 1);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn initial_ready_header_and_body_ignore_an_already_cancelled_token() {
        let (source, producer, log) = session_source(4);
        assert!(producer.headers(response(200, None), false));
        assert_eq!(
            producer.body(body(b"body"), 0, true),
            FlowControlState::Closed
        );
        let cancellation = CancellationSource::new();
        cancellation.request_cancellation();

        let header = ready(Box::pin(
            source.read_header_event_unboxed(cancellation.token()),
        ))
        .expect("an initial ready header has no registered cancellation wait");
        assert!(!header.eom());
        let body = ready(source.read_body_event_with_cancellation(4, cancellation.token()))
            .expect("an initial ready body has no registered cancellation wait");
        assert_eq!(body.as_body().unwrap().front().unwrap().as_ref(), b"body");
        assert!(body.eom());
        assert_eq!(log.borrow().completions, [None]);
    }

    #[test]
    fn registered_body_wait_cancellation_precedes_signalled_body() {
        let (source, producer, log) = session_source(4);
        assert!(producer.headers(response(200, None), false));
        read_header(&source).unwrap();
        drop(read_body(&source, u32::MAX).unwrap());
        let cancellation = CancellationSource::new();
        let mut read = source.read_body_event_with_cancellation(4, cancellation.token());
        assert!(poll_once(read.as_mut()).is_pending());
        assert_eq!(
            producer.body(body(b"body"), 0, true),
            FlowControlState::Closed
        );
        cancellation.request_cancellation();

        let error = match ready(read) {
            Err(error) => error,
            Ok(_) => panic!("the registered wait observes cancellation"),
        };
        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
        assert_eq!(source.body_bytes_buffered(), 0);
        assert_eq!(log.borrow().processed, [(StreamId::new(7), 4, 0)]);
        assert_eq!(
            log.borrow().completions,
            [Some(HttpErrorCode::CORO_CANCELLED)]
        );
    }

    #[test]
    fn dropping_a_registered_read_releases_the_single_waiter() {
        let (source, producer, _) = session_source(65_535);
        let mut first = source.read_header_event_with_cancellation(Default::default());
        assert!(poll_once(first.as_mut()).is_pending());
        drop(first);
        producer.headers(response(200, None), true);
        assert!(read_header(&source).unwrap().eom());
    }

    struct StopProbe(Rc<Cell<usize>>);

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
            self.0.set(self.0.get() + 1);
        }
    }

    struct ReentrantStopProbe {
        stops: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        on_stop: Option<Box<dyn FnOnce()>>,
    }

    impl HttpSource for ReentrantStopProbe {
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
            if let Some(on_stop) = self.on_stop.take() {
                on_stop();
            }
        }
    }

    impl Drop for ReentrantStopProbe {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn terminal_error_drops_queued_push_outside_state_borrow_once() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, None), false);
        let stops = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let reentrant_producer = producer.clone();
        assert!(producer.push_promise(
            request(None),
            Box::new(ReentrantStopProbe {
                stops: Rc::clone(&stops),
                drops: Rc::clone(&drops),
                on_stop: Some(Box::new(move || {
                    assert!(reentrant_producer.padding(7));
                })),
            }),
            false,
        ));

        assert!(!producer.headers(response(200, None), false));
        assert_eq!(stops.get(), 1);
        assert_eq!(drops.get(), 1);
        assert_eq!(source.body_bytes_buffered(), 0);
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
        drop(source);
        assert_eq!(stops.get(), 1);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn rejected_push_source_is_stopped_once() {
        let (source, producer, _) = session_source(65_535);
        producer.headers(response(200, None), true);
        let stops = Rc::new(Cell::new(0));
        assert!(!producer.push_promise(
            request(None),
            Box::new(StopProbe(Rc::clone(&stops))),
            false,
        ));
        assert_eq!(stops.get(), 1);
        assert_eq!(
            error_code(read_header(&source)),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
    }

    #[test]
    fn response_capability_rejections_preserve_usable_ownership() {
        run_paused(async {
            let source = HttpResponseSource::new(StreamId::new(1), None, 8, Host::tokio()).unwrap();
            let other = HttpResponseSource::new(StreamId::new(3), None, 8, Host::tokio()).unwrap();
            let producer = source.producer();
            let other_producer = other.producer();
            let (owner, mut reader) = source.split();
            let (other_owner, mut other_reader) = other.split();
            assert!(matches!(
                reader.arm_header(&other_owner),
                Err(error) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION
            ));

            let Err((owner, mut reader)) = owner.try_into_source_holder(reader) else {
                panic!("conversion before final headers must return both capabilities");
            };
            let mut context = Context::from_waker(Waker::noop());
            reader.arm_header(&owner).unwrap();
            assert!(reader.poll_header(&mut context).is_pending());
            let Err((owner, mut reader)) = owner.try_into_source_holder(reader) else {
                panic!("conversion during a pending read must return both capabilities");
            };
            assert!(producer.headers(response(200, Some(b"4")), false));
            let Poll::Ready(Ok(header)) = reader.poll_header(&mut context) else {
                panic!("the returned reader must finish its original header operation");
            };
            assert_eq!(header.headers().response_status(), Some(200));
            assert!(!header.eom());
            other_reader.arm_header(&other_owner).unwrap();
            assert!(other_producer.headers(response(200, Some(b"5")), false));
            let Poll::Ready(Ok(header)) = other_reader.poll_header(&mut context) else {
                panic!("the mismatched owner must retain its own usable reader");
            };
            assert!(!header.eom());
            let Err((other_owner, reader)) = other_owner.try_into_source_holder(reader) else {
                panic!("idle readers must still match their source owner");
            };
            let Ok(mut source) = owner.try_into_source_holder(reader) else {
                panic!("the returned reader still converts with its matching owner");
            };
            let Ok(mut other) = other_owner.try_into_source_holder(other_reader) else {
                panic!("the returned owner still converts with its matching reader");
            };
            assert!(producer.is_live());
            assert!(other_producer.is_live());
            assert_eq!(
                producer.body(body(b"body"), 0, true),
                FlowControlState::Open
            );
            assert_eq!(
                other_producer.body(body(b"other"), 0, true),
                FlowControlState::Open
            );
            let event = ready(source.read_body_event(16)).unwrap();
            assert!(event.eom());
            assert_eq!(
                event.as_body().unwrap().coalesce(4).unwrap().as_ref(),
                b"body"
            );
            let event = ready(other.read_body_event(16)).unwrap();
            assert!(event.eom());
            assert_eq!(
                event.as_body().unwrap().coalesce(5).unwrap().as_ref(),
                b"other"
            );
            drop((source, other));
            assert!(!producer.is_live());
            assert!(!other_producer.is_live());
        });
    }

    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A response source split after its 200 head was read through the reader.
    fn reading_response() -> (
        HttpResponseSourceOwner,
        HttpResponseReader,
        HttpStreamSourceProducer,
    ) {
        let source =
            HttpResponseSource::new(StreamId::new(1), None, 65_535, Host::tokio()).unwrap();
        let producer = source.producer();
        let (owner, mut reader) = source.split();
        reader.arm_header(&owner).unwrap();
        assert!(producer.headers(response(200, None), false));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            reader.poll_header(&mut context),
            Poll::Ready(Ok(header)) if !header.eom()
        ));
        (owner, reader, producer)
    }

    /// Reads `bodies` paced bodies through one exclusive reader, producing each
    /// only after a poll returned `Pending`, and returns every event's type.
    /// `None` leaves the reader's suspend setting at its default.
    fn paced_exclusive_reads(suspend_free: Option<bool>, bodies: usize) -> Vec<HttpBodyEventType> {
        let (owner, mut reader, producer) = reading_response();
        if let Some(suspend_free) = suspend_free {
            reader.set_suspend_free(suspend_free);
        }
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        let mut events = Vec::new();
        let mut produced = 0;
        loop {
            if !reader.is_active() {
                reader.arm_body(&owner, u32::MAX).unwrap();
            }
            match reader.poll_body(&mut context) {
                Poll::Ready(Ok(event)) => {
                    events.push(event.event_type());
                    if event.eom() {
                        return events;
                    }
                }
                Poll::Ready(Err(error)) => panic!("paced read failed: {error:?}"),
                Poll::Pending => {
                    assert!(
                        produced < bodies,
                        "a pending read outlived the paced bodies"
                    );
                    produced += 1;
                    let woken = wakes.0.load(Ordering::SeqCst);
                    assert_eq!(
                        producer.body(body(b"token"), 0, produced == bodies),
                        FlowControlState::Open
                    );
                    assert_eq!(
                        wakes.0.load(Ordering::SeqCst),
                        woken + 1,
                        "each body must wake the pending read once"
                    );
                }
            }
        }
    }

    #[test]
    fn suspend_free_exclusive_reads_wait_through_paced_bodies() {
        run_paused(async {
            // Each empty read stays pending in place until its body arrives.
            assert_eq!(
                paced_exclusive_reads(Some(true), 4),
                [HttpBodyEventType::Body; 4]
            );
        });
    }

    #[test]
    fn default_exclusive_reads_suspend_before_each_paced_body() {
        run_paused(async {
            // The first empty read, and each empty read after a wait, suspends,
            // by default and after an explicit opt-out.
            let suspending = [
                HttpBodyEventType::Suspend,
                HttpBodyEventType::Body,
                HttpBodyEventType::Suspend,
                HttpBodyEventType::Body,
                HttpBodyEventType::Suspend,
                HttpBodyEventType::Body,
            ];
            assert_eq!(paced_exclusive_reads(None, 3), suspending);
            assert_eq!(paced_exclusive_reads(Some(false), 3), suspending);
        });
    }

    #[test]
    fn suspend_free_pending_read_fails_with_the_producer_abort() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            let mut context = Context::from_waker(Waker::noop());
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(reader.poll_body(&mut context).is_pending());

            assert!(producer.abort(HttpErrorCode::TRANSPORT_READ_ERROR, b""));
            let Poll::Ready(Err(error)) = reader.poll_body(&mut context) else {
                panic!("the abort must fail the pending read");
            };
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
        });
    }

    #[test]
    fn suspend_free_pending_read_times_out_with_the_source_timeout() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            assert!(producer.set_read_timeout(Duration::from_secs(2)));
            reader.arm_body(&owner, u32::MAX).unwrap();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context).is_pending());

            time::advance(Duration::from_secs(2)).await;
            let error = match std::future::poll_fn(|context| reader.poll_body(context)).await {
                Err(error) => error,
                Ok(event) => panic!("expected a read timeout, got {:?}", event.event_type()),
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"Read timeout after 2000");
        });
    }

    #[test]
    fn turning_suspend_free_off_does_not_interrupt_a_pending_read() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            let mut context = Context::from_waker(Waker::noop());
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(reader.poll_body(&mut context).is_pending());

            reader.set_suspend_free(false);
            assert!(
                reader.poll_body(&mut context).is_pending(),
                "the read already waiting keeps waiting instead of suspending"
            );
            assert_eq!(
                producer.body(body(b"token"), 0, false),
                FlowControlState::Open
            );
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("the body must complete the pending read");
            };
            assert_eq!(event.event_type(), HttpBodyEventType::Body);

            // The next empty body read follows the new setting.
            assert!(!reader.is_active(), "a default reader retires its read");
            reader.arm_body(&owner, u32::MAX).unwrap();
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("an empty read of a default reader must suspend");
            };
            assert_eq!(event.event_type(), HttpBodyEventType::Suspend);
        });
    }

    #[test]
    fn suspend_free_reader_leaves_the_converted_source_suspending() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            let Ok(mut source) = owner.try_into_source_holder(reader) else {
                panic!("an idle reader after final headers must convert");
            };
            // The opt-in belongs to the exclusive reader, not to the source.
            let event = ready(source.read_body_event(16)).unwrap();
            assert_eq!(event.event_type(), HttpBodyEventType::Suspend);
            drop(event);

            assert_eq!(
                producer.body(body(b"body"), 0, true),
                FlowControlState::Open
            );
            let event = ready(source.read_body_event(16)).unwrap();
            assert!(event.eom());
        });
    }

    /// A Tokio host that a test wraps in an `Rc` to count references to it.
    struct CountedHost;

    impl LocalHost for CountedHost {
        fn spawn_local(&self, future: LocalFuture) {
            TokioHost.spawn_local(future);
        }

        fn now(&self) -> Instant {
            TokioHost.now()
        }

        fn sleep_until(&self, deadline: Instant) -> LocalFuture {
            TokioHost.sleep_until(deadline)
        }

        fn yield_to_io(&self) -> LocalFuture {
            TokioHost.yield_to_io()
        }
    }

    #[test]
    fn pending_untimed_exclusive_reads_hold_no_host_reference() {
        run_paused(async {
            let host = Rc::new(CountedHost);
            let source =
                HttpResponseSource::new(StreamId::new(1), None, 65_535, Host::new(host.clone()))
                    .unwrap();
            let producer = source.producer();
            let (owner, mut reader) = source.split();
            reader.set_suspend_free(true);
            let mut context = Context::from_waker(Waker::noop());

            reader.arm_header(&owner).unwrap();
            assert!(reader.poll_header(&mut context).is_pending());
            let pending_header = Rc::strong_count(&host);
            assert!(producer.headers(response(200, None), false));
            assert!(matches!(
                reader.poll_header(&mut context),
                Poll::Ready(Ok(_))
            ));
            let idle = Rc::strong_count(&host);
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(reader.poll_body(&mut context).is_pending());
            let pending_body = Rc::strong_count(&host);

            // The test and the source hold the host. A wait with no read
            // timeout and no cancellation needs neither a wait object nor the
            // event baton, which would each hold it too.
            assert_eq!((pending_header, idle, pending_body), (2, 2, 2));
        });
    }

    #[test]
    fn read_timeout_set_during_a_pending_exclusive_read_counts_from_the_update() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            reader.arm_body(&owner, u32::MAX).unwrap();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context).is_pending());

            time::advance(Duration::from_secs(1)).await;
            let updated = time::Instant::now();
            assert!(producer.set_read_timeout(Duration::from_secs(2)));
            // The read is next polled after its deadline has passed.
            time::advance(Duration::from_secs(3)).await;
            let error = match std::future::poll_fn(|context| reader.poll_body(context)).await {
                Err(error) => error,
                Ok(event) => panic!("expected a read timeout, got {:?}", event.event_type()),
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"Read timeout after 2000");
            assert_eq!(time::Instant::now() - updated, Duration::from_secs(3));
        });
    }

    #[test]
    fn pending_exclusive_read_wakes_on_event_changes_and_timeout_updates() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(reader.poll_body(&mut context).is_pending());

            // A timeout update wakes the pending read.
            assert!(producer.set_read_timeout(Duration::ZERO));
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
            assert!(reader.poll_body(&mut context).is_pending());

            // Only the first event before the next poll wakes it.
            assert_eq!(
                producer.body(body(b"one"), 0, false),
                FlowControlState::Open
            );
            assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
            assert_eq!(
                producer.body(body(b"two"), 0, false),
                FlowControlState::Open
            );
            assert_eq!(wakes.0.load(Ordering::SeqCst), 2);

            // The woken read already has its poll coming, so an update does not wake it again.
            assert!(producer.set_read_timeout(Duration::ZERO));
            assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("the woken read must return the queued body");
            };
            assert_eq!(event.event_type(), HttpBodyEventType::Body);
            assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
        });
    }

    /// The body that signals a pending exclusive read wakes it with the waker the read stored,
    /// so the reader holds no extra reference until its next poll, and is woken once.
    #[test]
    fn direct_wait_signal_moves_the_reader_waker() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let baseline = Arc::strong_count(&wakes);
            let mut context = Context::from_waker(&waker);
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(reader.poll_body(&mut context).is_pending());
            assert_eq!(
                Arc::strong_count(&wakes),
                baseline + 1,
                "the pending read stores its waker"
            );

            assert_eq!(
                producer.body(body(b"token"), 0, false),
                FlowControlState::Open
            );
            assert_eq!(
                Arc::strong_count(&wakes),
                baseline,
                "the body's wake takes the stored waker"
            );
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("the woken read must return the body");
            };
            assert_eq!(event.event_type(), HttpBodyEventType::Body);
            assert_eq!(
                wakes.0.load(Ordering::SeqCst),
                1,
                "the reader is woken once"
            );
            assert_eq!(Arc::strong_count(&wakes), baseline);
        });
    }

    /// A timeout update after a body signalled the pending exclusive read, and before the reader
    /// polls, does not wake it again: the body's wake already scheduled the poll that returns it.
    #[test]
    fn retime_after_signal_needs_no_second_wake() {
        run_paused(async {
            for timeout in [Duration::ZERO, Duration::from_secs(2)] {
                let (owner, mut reader, producer) = reading_response();
                reader.set_suspend_free(true);
                let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
                let waker = Waker::from(Arc::clone(&wakes));
                let mut context = Context::from_waker(&waker);
                reader.arm_body(&owner, u32::MAX).unwrap();
                assert!(reader.poll_body(&mut context).is_pending());
                assert_eq!(
                    producer.body(body(b"token"), 0, false),
                    FlowControlState::Open
                );
                assert_eq!(wakes.0.load(Ordering::SeqCst), 1);

                assert!(producer.set_read_timeout(timeout));
                assert_eq!(
                    wakes.0.load(Ordering::SeqCst),
                    1,
                    "the signalled read is not woken again (timeout {timeout:?})"
                );
                let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                    panic!("the signalled read must return the body (timeout {timeout:?})");
                };
                assert_eq!(event.event_type(), HttpBodyEventType::Body);
                assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
            }
        });
    }

    /// A timeout update moves a pending exclusive read's wait to a timed wait registered with
    /// the waker the read stored, so a later body wakes that waker before the read polls again.
    #[test]
    fn retimed_exclusive_read_keeps_its_waker() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            reader.set_suspend_free(true);
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let baseline = Arc::strong_count(&wakes);
            let mut context = Context::from_waker(&waker);
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(reader.poll_body(&mut context).is_pending());

            assert!(producer.set_read_timeout(Duration::from_secs(2)));
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
            assert_eq!(
                Arc::strong_count(&wakes),
                baseline + 1,
                "the timed wait holds the stored waker"
            );
            assert_eq!(
                producer.body(body(b"token"), 0, false),
                FlowControlState::Open
            );
            assert_eq!(
                wakes.0.load(Ordering::SeqCst),
                2,
                "the body wakes the stored waker"
            );
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("the retimed read must return the body");
            };
            assert_eq!(event.event_type(), HttpBodyEventType::Body);
            assert_eq!(Arc::strong_count(&wakes), baseline);
        });
    }

    #[test]
    fn retired_pending_exclusive_read_leaves_the_next_read_free_to_wait() {
        run_paused(async {
            for retimed in [false, true] {
                let (owner, mut reader, producer) = reading_response();
                reader.set_suspend_free(true);
                let mut context = Context::from_waker(Waker::noop());
                reader.arm_body(&owner, u32::MAX).unwrap();
                assert!(reader.poll_body(&mut context).is_pending());
                if retimed {
                    assert!(producer.set_read_timeout(Duration::from_secs(2)));
                }
                reader.retire_active_read();

                let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
                let waker = Waker::from(Arc::clone(&wakes));
                let mut context = Context::from_waker(&waker);
                reader.arm_body(&owner, u32::MAX).unwrap();
                assert!(reader.poll_body(&mut context).is_pending());
                assert_eq!(
                    producer.body(body(b"token"), 0, true),
                    FlowControlState::Open
                );
                assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "retimed={retimed}");
                let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                    panic!("the next read must return the body, retimed={retimed}");
                };
                assert!(event.eom());
            }
        });
    }

    /// Splits a 200 response, opts its reader in, and reads one paced body.
    fn reader_after_one_body() -> (
        HttpResponseSourceOwner,
        HttpResponseReader,
        HttpStreamSourceProducer,
    ) {
        let (owner, mut reader, producer) = reading_response();
        reader.set_suspend_free(true);
        reader.arm_body(&owner, u32::MAX).unwrap();
        let mut context = Context::from_waker(Waker::noop());
        assert!(reader.poll_body(&mut context).is_pending());
        assert_eq!(
            producer.body(body(b"one"), 0, false),
            FlowControlState::Open
        );
        let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
            panic!("the paced body must complete the read");
        };
        assert!(!event.eom());
        (owner, reader, producer)
    }

    #[test]
    fn suspend_free_reader_stays_armed_between_body_events() {
        run_paused(async {
            let (_owner, mut reader, producer) = reader_after_one_body();
            assert!(reader.is_active());

            // The next body read needs no `arm_body`.
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context).is_pending());
            assert_eq!(producer.body(body(b"two"), 0, true), FlowControlState::Open);
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("the armed reader must read the next body");
            };
            assert_eq!(
                event.as_body().unwrap().coalesce(8).unwrap().as_ref(),
                b"two"
            );

            // The terminal event retires the read.
            assert!(event.eom());
            assert!(!reader.is_active());
        });
    }

    #[test]
    fn default_reader_retires_after_each_body_event() {
        run_paused(async {
            let (owner, mut reader, producer) = reading_response();
            let mut context = Context::from_waker(Waker::noop());
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(matches!(
                reader.poll_body(&mut context),
                Poll::Ready(Ok(event)) if event.event_type() == HttpBodyEventType::Suspend
            ));
            assert!(!reader.is_active());
            assert_eq!(
                producer.body(body(b"one"), 0, false),
                FlowControlState::Open
            );
            reader.arm_body(&owner, u32::MAX).unwrap();
            assert!(matches!(
                reader.poll_body(&mut context),
                Poll::Ready(Ok(event)) if event.event_type() == HttpBodyEventType::Body
            ));
            assert!(!reader.is_active());
        });
    }

    #[test]
    fn releasing_the_owner_between_body_events_destroys_the_source() {
        run_paused(async {
            let (mut owner, reader, producer) = reader_after_one_body();
            // The armed reader keeps no strong reference between operations.
            owner.clear(None);
            assert!(!producer.is_live());
            drop(reader);
        });
    }

    #[test]
    fn armed_suspend_free_reader_reports_a_released_owner() {
        run_paused(async {
            let (mut owner, mut reader, _producer) = reader_after_one_body();
            owner.clear(None);
            let mut context = Context::from_waker(Waker::noop());
            let Poll::Ready(Err(error)) = reader.poll_body(&mut context) else {
                panic!("a read of a released source must fail");
            };
            // The error `arm_body` returns for a released owner.
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
            assert_eq!(error.message(), b"HTTP source is no longer readable");
            assert!(!reader.is_active());
        });
    }

    #[test]
    fn armed_suspend_free_reader_rearms_with_a_new_maximum() {
        run_paused(async {
            let (owner, mut reader, producer) = reader_after_one_body();
            reader.arm_body(&owner, 2).unwrap();
            assert_eq!(producer.body(body(b"two"), 0, true), FlowControlState::Open);
            let mut context = Context::from_waker(Waker::noop());
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("the re-armed reader must read the next body");
            };
            assert_eq!(
                event.as_body().unwrap().coalesce(8).unwrap().as_ref(),
                b"tw"
            );
            assert!(!event.eom());
        });
    }

    #[test]
    fn armed_suspend_free_reader_refuses_a_header_read() {
        run_paused(async {
            let (_owner, mut reader, _producer) = reader_after_one_body();
            let mut context = Context::from_waker(Waker::noop());
            let Poll::Ready(Err(error)) = reader.poll_header(&mut context) else {
                panic!("a header poll of a body reader must fail");
            };
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
            assert_eq!(error.message(), b"response read is not armed");
            assert!(!reader.is_active());
        });
    }

    #[test]
    fn suspend_free_read_error_retires_the_read() {
        run_paused(async {
            let (_owner, mut reader, producer) = reader_after_one_body();
            assert!(producer.abort(HttpErrorCode::TRANSPORT_READ_ERROR, b""));
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                reader.poll_body(&mut context),
                Poll::Ready(Err(error)) if error.code() == HttpErrorCode::TRANSPORT_READ_ERROR
            ));
            assert!(!reader.is_active());
        });
    }

    /// A response source with recorded callbacks, split after its producer
    /// enabled inline reads, unless `inline` is false.
    fn inline_response(
        receive_window: u32,
        inline: bool,
        deliver_ingress_before_abort: bool,
    ) -> (
        HttpResponseSourceOwner,
        HttpResponseReader,
        HttpStreamSourceProducer,
        Rc<RefCell<CallbackLog>>,
    ) {
        let log = Rc::new(RefCell::new(CallbackLog::default()));
        let source = HttpResponseSource::new(
            StreamId::new(1),
            Some(Box::new(RecordingCallback(Rc::clone(&log)))),
            receive_window,
            Host::tokio(),
        )
        .unwrap();
        if deliver_ingress_before_abort {
            source.deliver_ingress_before_abort();
        }
        let producer = source.producer();
        if inline {
            assert!(producer.enable_inline_reads());
        }
        let (owner, reader) = source.split();
        (owner, reader, producer, log)
    }

    fn body_text(event: &HttpBodyEvent) -> Vec<u8> {
        let body = event.as_body().expect("a body event");
        body.coalesce(body.chain_length()).unwrap().to_vec()
    }

    #[test]
    fn inline_reads_take_ready_events_without_waiting_or_suspending() {
        run_paused(async {
            let (mut owner, mut reader, producer, log) = inline_response(64, true, false);
            assert!(reader.try_read_header(&owner).is_none());
            assert!(!reader.is_active(), "an empty inline read admits nothing");

            assert!(producer.headers(response(100, None), false));
            assert!(producer.headers(response(200, Some(b"8")), false));
            let head = reader.try_read_header(&owner).unwrap().unwrap();
            assert_eq!(head.headers().response_status(), Some(100));
            let head = reader.try_read_header(&owner).unwrap().unwrap();
            assert_eq!(head.headers().response_status(), Some(200));
            assert!(!head.eom());

            for _ in 0..3 {
                assert!(
                    reader.try_read_body(&owner, 64).is_none(),
                    "an empty inline body read returns nothing, not a suspend"
                );
                assert!(!reader.is_active());
            }

            // Two bodies coalesce into one queued event, split at the maximum.
            assert_eq!(
                producer.body(body(b"abcd"), 0, false),
                FlowControlState::Open
            );
            assert_eq!(producer.body(body(b"ef"), 0, false), FlowControlState::Open);
            let event = reader.try_read_body(&owner, 4).unwrap().unwrap();
            assert_eq!(body_text(&event), b"abcd");
            assert!(!event.eom());
            let event = reader.try_read_body(&owner, 64).unwrap().unwrap();
            assert_eq!(body_text(&event), b"ef");
            assert!(!event.eom());
            assert!(log.borrow().completions.is_empty());

            assert_eq!(producer.body(body(b"gh"), 0, true), FlowControlState::Open);
            let event = reader.try_read_body(&owner, 64).unwrap().unwrap();
            assert_eq!(body_text(&event), b"gh");
            assert!(event.eom());
            assert_eq!(
                log.borrow().completions,
                [None],
                "the EOM read completes the source"
            );

            owner.retire_terminal();
            let Some(Err(error)) = reader.try_read_body(&owner, 64) else {
                panic!("an inline read of a retired source must fail");
            };
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
            assert_eq!(
                log.borrow().completions,
                [None],
                "the source completes once"
            );
        });
    }

    #[test]
    fn an_empty_inline_read_registers_no_waker() {
        run_paused(async {
            let (owner, mut reader, producer, _log) = inline_response(64, true, false);
            reader.set_suspend_free(true);
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            // A waiting read registers the counting waker, then retires, as a
            // reader that switches to inline reads does.
            reader.arm_header(&owner).unwrap();
            assert!(reader.poll_header(&mut context).is_pending());
            reader.retire_active_read();

            for _ in 0..3 {
                assert!(reader.try_read_header(&owner).is_none());
            }
            assert!(producer.headers(response(200, None), false));
            assert_eq!(
                wakes.0.load(Ordering::SeqCst),
                0,
                "a header after empty inline reads wakes nothing"
            );
            let head = reader.try_read_header(&owner).unwrap().unwrap();
            assert_eq!(head.headers().response_status(), Some(200));

            for _ in 0..3 {
                assert!(reader.try_read_body(&owner, 64).is_none());
            }
            assert_eq!(
                producer.body(body(b"token"), 0, false),
                FlowControlState::Open
            );
            assert_eq!(
                wakes.0.load(Ordering::SeqCst),
                0,
                "a body after empty inline reads wakes nothing"
            );
            let event = reader.try_read_body(&owner, 64).unwrap().unwrap();
            assert_eq!(body_text(&event), b"token");
        });
    }

    #[test]
    fn inline_reads_report_the_ingress_error_a_waiting_read_reports() {
        run_paused(async {
            let mut outcomes = Vec::new();
            for inline in [true, false] {
                let (owner, mut reader, producer, log) = inline_response(64, true, false);
                assert!(producer.headers(response(200, Some(b"2")), false));
                let mut context = Context::from_waker(Waker::noop());
                if inline {
                    assert!(reader.try_read_header(&owner).unwrap().is_ok());
                } else {
                    reader.arm_header(&owner).unwrap();
                    assert!(matches!(
                        reader.poll_header(&mut context),
                        Poll::Ready(Ok(_))
                    ));
                }
                producer.body(body(b"abc"), 0, true);
                let result = if inline {
                    reader.try_read_body(&owner, 64).unwrap()
                } else {
                    reader.arm_body(&owner, 64).unwrap();
                    let Poll::Ready(result) = reader.poll_body(&mut context) else {
                        panic!("the waiting read must find the error");
                    };
                    result
                };
                let Err(error) = result else {
                    panic!("a content-length mismatch must fail the read, inline={inline}");
                };
                outcomes.push((
                    error.code(),
                    error.message().to_vec(),
                    log.borrow().completions.clone(),
                ));
            }
            assert_eq!(
                outcomes[0],
                (
                    HttpErrorCode::CONTENT_LENGTH_MISMATCH,
                    b"Content-Length/body mismatch on ingress: expected= 2, actual= 3".to_vec(),
                    vec![Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH)],
                )
            );
            assert_eq!(outcomes[0], outcomes[1]);
        });
    }

    enum CreditStep {
        Produce(&'static [u8]),
        Eom,
        Read(u32),
    }

    /// Reads a 200 response from an 8-byte window with a resume threshold of 6,
    /// inline or with suspend-free waiting reads, running `steps` in order.
    /// Returns each step's outcome followed by the callback log.
    fn credit_transcript(inline: bool, steps: &[CreditStep]) -> Vec<String> {
        let (owner, mut reader, producer, log) = inline_response(8, inline, false);
        reader.set_suspend_free(true);
        assert!(producer.set_resume_threshold(Some(6)));
        assert!(producer.headers(response(200, None), false));
        let mut context = Context::from_waker(Waker::noop());
        if inline {
            assert!(reader.try_read_header(&owner).unwrap().is_ok());
        } else {
            reader.arm_header(&owner).unwrap();
            assert!(matches!(
                reader.poll_header(&mut context),
                Poll::Ready(Ok(_))
            ));
        }
        let mut transcript = Vec::new();
        let mut pending = false;
        for step in steps {
            let outcome = match step {
                CreditStep::Produce(bytes) => {
                    format!("{:?}", producer.body(body(bytes), 0, false))
                }
                CreditStep::Eom => format!("eom {}", producer.eom()),
                CreditStep::Read(maximum) => {
                    let result = if inline {
                        reader.try_read_body(&owner, *maximum)
                    } else {
                        if !pending {
                            reader.arm_body(&owner, *maximum).unwrap();
                        }
                        match reader.poll_body(&mut context) {
                            Poll::Ready(result) => Some(result),
                            Poll::Pending => None,
                        }
                    };
                    pending = !inline && result.is_none();
                    match result {
                        None => "empty".to_owned(),
                        Some(Ok(event)) => format!(
                            "{:?} {:?} eom={}",
                            event.event_type(),
                            String::from_utf8(body_text(&event)).unwrap(),
                            event.eom()
                        ),
                        Some(Err(error)) => format!("error {:?}", error.code()),
                    }
                }
            };
            let log = log.borrow();
            transcript.push(format!(
                "{outcome} | processed={:?} opened={:?} completions={:?}",
                log.processed, log.opened, log.completions
            ));
        }
        transcript
    }

    #[test]
    fn inline_reads_keep_credit_and_completion_as_waiting_reads() {
        run_paused(async {
            let steps = [
                CreditStep::Produce(b"abcdefgh"),
                CreditStep::Read(3),
                CreditStep::Read(3),
                CreditStep::Read(3),
                CreditStep::Read(3),
                CreditStep::Produce(b"ijklmn"),
                CreditStep::Read(3),
                CreditStep::Eom,
                CreditStep::Read(8),
            ];
            let inline = credit_transcript(true, &steps);
            let id = StreamId::new(1);
            // The first read crosses the threshold (8 to 5 buffered) and opens
            // the closed window. The window does not need an update until 4
            // bytes are read. The fifth read crosses it again (6 to 3) and
            // acknowledges the 5 bytes read since the last update. The EOM read
            // completes.
            assert_eq!(
                inline.last().unwrap(),
                &format!(
                    "Body \"lmn\" eom=true | processed={:?} opened={:?} completions=[None]",
                    [(id, 3, 0), (id, 3, 5)],
                    [id]
                )
            );
            assert_eq!(inline[4].split(" | ").next(), Some("empty"));
            assert_eq!(inline, credit_transcript(false, &steps));
        });
    }

    /// Reads a response whose head and body were queued before an ingress
    /// abort, inline or with waiting reads, with or without delivery of ingress
    /// before the abort.
    fn kept_events_transcript(inline: bool, keep: bool) -> Vec<String> {
        let (owner, mut reader, producer, log) = inline_response(64, true, keep);
        reader.set_suspend_free(true);
        assert!(producer.headers(response(200, None), false));
        assert_eq!(
            producer.body(body(b"first"), 0, false),
            FlowControlState::Open
        );
        assert!(producer.abort(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
        let mut context = Context::from_waker(Waker::noop());
        let mut transcript = Vec::new();
        let header = if inline {
            reader.try_read_header(&owner).unwrap()
        } else {
            reader.arm_header(&owner).unwrap();
            let Poll::Ready(result) = reader.poll_header(&mut context) else {
                panic!("the aborted source must not wait");
            };
            result
        };
        let header_read = header.is_ok();
        transcript.push(match header {
            Ok(head) => format!("head {:?}", head.headers().response_status()),
            Err(error) => format!("error {:?}", error.code()),
        });
        transcript.push(format!("completions={:?}", log.borrow().completions));
        if header_read {
            for _ in 0..2 {
                let result = if inline {
                    reader.try_read_body(&owner, u32::MAX).unwrap()
                } else {
                    if !reader.is_active() {
                        reader.arm_body(&owner, u32::MAX).unwrap();
                    }
                    let Poll::Ready(result) = reader.poll_body(&mut context) else {
                        panic!("the aborted source must not wait");
                    };
                    result
                };
                transcript.push(match result {
                    Ok(event) => format!(
                        "body {:?} eom={}",
                        String::from_utf8(body_text(&event)).unwrap(),
                        event.eom()
                    ),
                    Err(error) => format!("error {:?}", error.code()),
                });
                transcript.push(format!("completions={:?}", log.borrow().completions));
            }
        }
        transcript
    }

    #[test]
    fn inline_reads_return_kept_events_then_the_abort_error() {
        run_paused(async {
            let eof = format!("error {:?}", HttpErrorCode::TRANSPORT_EOF);
            let kept = [
                "head Some(200)",
                "completions=[]",
                "body \"first\" eom=false",
                "completions=[None]",
                &eof,
                "completions=[None]",
            ];
            assert_eq!(kept_events_transcript(true, true), kept);
            assert_eq!(kept_events_transcript(false, true), kept);

            let discarded = [eof.as_str(), "completions=[None]"];
            assert_eq!(kept_events_transcript(true, false), discarded);
            assert_eq!(kept_events_transcript(false, false), discarded);
        });
    }

    #[test]
    fn inline_reads_are_refused_unless_the_producer_enabled_them() {
        run_paused(async {
            let (owner, mut reader, producer, log) = inline_response(64, false, false);
            assert!(producer.headers(response(200, None), false));
            let Some(Err(error)) = reader.try_read_header(&owner) else {
                panic!("an inline read of a source without inline reads must be refused");
            };
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert!(!reader.is_active());

            // The refusal has no effect, so a waiting read takes the head.
            let mut context = Context::from_waker(Waker::noop());
            reader.arm_header(&owner).unwrap();
            assert!(matches!(
                reader.poll_header(&mut context),
                Poll::Ready(Ok(head)) if head.headers().response_status() == Some(200)
            ));
            assert!(matches!(
                reader.try_read_body(&owner, 64),
                Some(Err(error)) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION
            ));
            assert!(log.borrow().completions.is_empty());

            // Control: the same head on a source with inline reads.
            let (owner, mut reader, producer, _log) = inline_response(64, true, false);
            assert!(producer.headers(response(200, None), false));
            assert!(matches!(
                reader.try_read_header(&owner),
                Some(Ok(head)) if head.headers().response_status() == Some(200)
            ));
        });
    }

    #[test]
    fn inline_reads_refuse_an_armed_reader() {
        run_paused(async {
            let refused = |result: Option<Result<HttpHeaderEvent, HttpError>>| matches!(result, Some(Err(error)) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION);
            let mut context = Context::from_waker(Waker::noop());

            // An armed read that was never polled.
            let (owner, mut reader, producer, _log) = inline_response(64, true, false);
            assert!(producer.headers(response(200, None), false));
            reader.arm_header(&owner).unwrap();
            assert!(refused(reader.try_read_header(&owner)));
            assert!(reader.is_active(), "the refusal leaves the armed read");
            assert!(matches!(
                reader.poll_header(&mut context),
                Poll::Ready(Ok(head)) if head.headers().response_status() == Some(200)
            ));

            // A pending read.
            let (owner, mut reader, producer, _log) = inline_response(64, true, false);
            reader.set_suspend_free(true);
            reader.arm_header(&owner).unwrap();
            assert!(reader.poll_header(&mut context).is_pending());
            assert!(refused(reader.try_read_header(&owner)));
            assert!(reader.is_active());
            assert!(producer.headers(response(200, None), false));
            assert!(matches!(
                reader.poll_header(&mut context),
                Poll::Ready(Ok(_))
            ));

            // A suspend-free reader armed between body events.
            reader.arm_body(&owner, 64).unwrap();
            assert_eq!(
                producer.body(body(b"one"), 0, false),
                FlowControlState::Open
            );
            assert!(matches!(reader.poll_body(&mut context), Poll::Ready(Ok(_))));
            assert!(reader.is_active());
            assert_eq!(
                producer.body(body(b"two"), 0, false),
                FlowControlState::Open
            );
            assert!(matches!(
                reader.try_read_body(&owner, 64),
                Some(Err(error)) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION
            ));
            assert!(reader.is_active());

            // Control: once retired, the reader reads inline.
            reader.retire_active_read();
            let event = reader.try_read_body(&owner, 64).unwrap().unwrap();
            assert_eq!(body_text(&event), b"two");
        });
    }

    #[test]
    fn inline_reads_are_refused_for_a_source_with_a_read_timeout() {
        run_paused(async {
            // A source with a read timeout.
            let (owner, mut reader, producer, _log) = inline_response(64, false, false);
            reader.set_suspend_free(true);
            assert!(producer.set_read_timeout(Duration::from_secs(2)));
            assert!(!producer.enable_inline_reads());
            assert!(matches!(
                reader.try_read_header(&owner),
                Some(Err(error)) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION
            ));
            // The refusal has no effect, so a waiting read still times out.
            reader.arm_header(&owner).unwrap();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_header(&mut context).is_pending());
            time::advance(Duration::from_secs(2)).await;
            let Err(error) = std::future::poll_fn(|context| reader.poll_header(context)).await
            else {
                panic!("the read timeout must still apply");
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);

            // A source with a deferred read timeout.
            let (owner, mut reader, producer, _log) = inline_response(64, false, false);
            assert!(
                producer
                    .defer_read_timeout(DeferredReadTimeout::DeferUpdates(Duration::from_secs(2)))
            );
            assert!(!producer.enable_inline_reads());
            assert!(matches!(
                reader.try_read_header(&owner),
                Some(Err(error)) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION
            ));
            assert!(producer.arm_read_timeout(), "the deferred timeout stays");

            // Control: a source without a read timeout.
            let (_owner, _reader, producer, _log) = inline_response(64, false, false);
            assert!(producer.enable_inline_reads());
        });
    }

    #[test]
    fn inline_reads_are_refused_while_a_read_is_pending() {
        run_paused(async {
            let (owner, mut reader, producer, _log) = inline_response(64, false, false);
            reader.set_suspend_free(true);
            reader.arm_header(&owner).unwrap();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_header(&mut context).is_pending());
            assert!(!producer.enable_inline_reads());
            reader.retire_active_read();
            assert!(matches!(
                reader.try_read_header(&owner),
                Some(Err(error)) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION
            ));

            // Control: with no read pending, the producer enables them.
            assert!(producer.enable_inline_reads());
            assert!(reader.try_read_header(&owner).is_none());
        });
    }

    #[test]
    fn a_nonzero_read_timeout_is_refused_after_inline_reads_are_enabled() {
        run_paused(async {
            let (owner, mut reader, producer, _log) = inline_response(64, true, false);
            assert!(!producer.set_read_timeout(Duration::from_secs(2)));
            for deferred in [
                DeferredReadTimeout::DeferUpdates(Duration::from_secs(2)),
                DeferredReadTimeout::ReplaceUpdates(Duration::from_secs(2)),
                DeferredReadTimeout::CancelOnUpdate(Duration::from_secs(2)),
            ] {
                assert!(!producer.defer_read_timeout(deferred));
            }
            assert!(!producer.arm_read_timeout(), "nothing was deferred");
            // Zero timeouts stay accepted.
            assert!(producer.set_read_timeout(Duration::ZERO));
            assert!(producer.defer_read_timeout(DeferredReadTimeout::DeferUpdates(Duration::ZERO)));
            assert!(producer.arm_read_timeout());

            // The refusals had no effect, so a waiting read does not time out.
            reader.set_suspend_free(true);
            reader.arm_header(&owner).unwrap();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_header(&mut context).is_pending());
            time::advance(Duration::from_secs(3)).await;
            assert!(reader.poll_header(&mut context).is_pending());
            assert!(producer.headers(response(200, None), false));
            assert!(matches!(
                reader.poll_header(&mut context),
                Poll::Ready(Ok(_))
            ));

            // Control: a source without inline reads accepts a timeout.
            let (_owner, _reader, producer, _log) = inline_response(64, false, false);
            assert!(producer.set_read_timeout(Duration::from_secs(2)));
        });
    }

    /// A callback whose first credit report panics.
    struct PanicOnFirstCredit(Rc<Cell<bool>>);

    impl HttpStreamSourceCallback for PanicOnFirstCredit {
        fn bytes_processed(&mut self, _id: StreamId, _amount: usize, _to_acknowledge: usize) {
            if !self.0.replace(true) {
                panic!("first credit report");
            }
        }
    }

    #[test]
    fn an_inline_read_that_unwinds_releases_its_admission() {
        run_paused(async {
            let reported = Rc::new(Cell::new(false));
            let source = HttpResponseSource::new(
                StreamId::new(1),
                Some(Box::new(PanicOnFirstCredit(Rc::clone(&reported)))),
                64,
                Host::tokio(),
            )
            .unwrap();
            let producer = source.producer();
            assert!(producer.enable_inline_reads());
            let (owner, mut reader) = source.split();
            assert!(producer.headers(response(200, None), false));
            assert!(reader.try_read_header(&owner).unwrap().is_ok());
            assert_eq!(
                producer.body(body(b"one"), 0, false),
                FlowControlState::Open
            );
            let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reader.try_read_body(&owner, 64)
            }));
            assert!(unwound.is_err(), "the credit callback panic propagates");
            assert!(reported.get());

            // The unwound read is not left admitted: the next one reads on.
            assert_eq!(producer.body(body(b"two"), 0, true), FlowControlState::Open);
            let event = reader.try_read_body(&owner, 64).unwrap().unwrap();
            assert_eq!(body_text(&event), b"two");
            assert!(event.eom());
        });
    }

    #[test]
    fn a_waiting_read_after_inline_reads_waits_and_is_woken() {
        run_paused(async {
            let (owner, mut reader, producer, log) = inline_response(64, true, false);
            assert!(reader.try_read_header(&owner).is_none());
            assert!(producer.headers(response(200, None), false));
            assert!(reader.try_read_header(&owner).unwrap().is_ok());
            assert!(reader.try_read_body(&owner, 64).is_none());
            assert_eq!(
                producer.body(body(b"one"), 0, false),
                FlowControlState::Open
            );
            let event = reader.try_read_body(&owner, 64).unwrap().unwrap();
            assert_eq!(body_text(&event), b"one");
            assert!(reader.try_read_body(&owner, 64).is_none());

            // The reader falls back to a suspend-free waiting read.
            reader.set_suspend_free(true);
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            reader.arm_body(&owner, 64).unwrap();
            assert!(reader.poll_body(&mut context).is_pending());
            assert_eq!(producer.body(body(b"two"), 0, true), FlowControlState::Open);
            assert_eq!(
                wakes.0.load(Ordering::SeqCst),
                1,
                "the body wakes the read once"
            );
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context) else {
                panic!("the woken read must return the body");
            };
            assert_eq!(body_text(&event), b"two");
            assert!(event.eom());
            assert!(!reader.is_active());
            assert_eq!(log.borrow().completions, [None]);
        });
    }

    /// A request body source after its head, as an owned server hands it to
    /// its application.
    fn request_owner() -> (
        HttpStreamSourceOwner,
        HttpStreamSourceProducer,
        Rc<RefCell<CallbackLog>>,
    ) {
        let log = Rc::new(RefCell::new(CallbackLog::default()));
        let ingress = HttpIngressBody::new(
            StreamId::new(1),
            RecordingCallback(Rc::clone(&log)),
            65_535,
            Host::tokio(),
        )
        .unwrap();
        assert!(ingress.initialize_body_only(&mut request(None), false));
        let producer = ingress.producer();
        (ingress.into_source_owner(), producer, log)
    }

    /// Reads `bodies` paced bodies of a request source through the general
    /// API, producing each only after a poll returned `Pending`, and returns
    /// every event's type. With `await_resume` the consumer awaits each
    /// suspend's resume before it reads again; otherwise it drops the suspend.
    fn paced_general_reads(await_resume: bool, bodies: usize) -> Vec<HttpBodyEventType> {
        let (owner, producer, _log) = request_owner();
        let source = owner.source().unwrap();
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        let mut produced = 0;
        let mut produce = || {
            assert!(
                produced < bodies,
                "a pending wait outlived the paced bodies"
            );
            produced += 1;
            let woken = wakes.0.load(Ordering::SeqCst);
            assert_eq!(
                producer.body(body(b"token"), 0, produced == bodies),
                FlowControlState::Open
            );
            assert_eq!(
                wakes.0.load(Ordering::SeqCst),
                woken + 1,
                "each body must wake the pending wait once"
            );
        };
        let mut events = Vec::new();
        loop {
            let mut read = source.read_body_event_unboxed(u32::MAX, CancellationToken::never());
            let event = loop {
                match Pin::new(&mut read).poll(&mut context) {
                    Poll::Ready(result) => break result.unwrap(),
                    Poll::Pending => produce(),
                }
            };
            events.push(event.event_type());
            if event.eom() {
                return events;
            }
            if await_resume && event.event_type() == HttpBodyEventType::Suspend {
                let mut resume = take_resume(event);
                while resume.as_mut().poll(&mut context).is_pending() {
                    produce();
                }
            }
        }
    }

    #[test]
    fn general_request_reads_suspend_before_paced_bodies() {
        run_paused(async {
            use HttpBodyEventType::Body;
            use HttpBodyEventType::Suspend;
            // A consumer that drops each suspend sees one before every body.
            assert_eq!(
                paced_general_reads(false, 3),
                [Suspend, Body, Suspend, Body, Suspend, Body]
            );
            // One that awaits each resume reads the next body, then waits in
            // a read for the body after it.
            assert_eq!(
                paced_general_reads(true, 3),
                [Suspend, Body, Body, Suspend, Body]
            );
        });
    }

    /// Where a [`LoggedCallback`] panics.
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum CallbackPanic {
        None,
        SourceComplete,
        Drop,
    }

    type OrderLog = Arc<Mutex<Vec<String>>>;

    fn entries(log: &OrderLog) -> Vec<String> {
        log.lock().unwrap().clone()
    }

    /// The entries a [`LoggedCallback`] wrote, without the waker's.
    fn callback_entries(log: &OrderLog) -> Vec<String> {
        entries(log)
            .into_iter()
            .filter(|entry| !entry.starts_with("waker"))
            .collect()
    }

    /// A callback that logs its completion and its own drop.
    struct LoggedCallback {
        log: OrderLog,
        panic: CallbackPanic,
    }

    impl HttpStreamSourceCallback for LoggedCallback {
        fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
            let code = error.map(|error| error.code());
            self.log
                .lock()
                .unwrap()
                .push(format!("callback complete {code:?}"));
            if self.panic == CallbackPanic::SourceComplete {
                panic!("callback completion");
            }
        }
    }

    impl Drop for LoggedCallback {
        fn drop(&mut self) {
            self.log.lock().unwrap().push("callback dropped".to_owned());
            if self.panic == CallbackPanic::Drop {
                panic!("callback drop");
            }
        }
    }

    /// A request source like [`request_owner`]'s whose callback logs into
    /// `log` and panics where `panic` says.
    fn logged_request_owner(
        log: &OrderLog,
        panic: CallbackPanic,
    ) -> (HttpStreamSourceOwner, HttpStreamSourceProducer) {
        let ingress = HttpIngressBody::new(
            StreamId::new(1),
            LoggedCallback {
                log: Arc::clone(log),
                panic,
            },
            65_535,
            Host::tokio(),
        )
        .unwrap();
        assert!(ingress.initialize_body_only(&mut request(None), false));
        let producer = ingress.producer();
        (ingress.into_source_owner(), producer)
    }

    fn panic_text(payload: &(dyn std::any::Any + Send)) -> &'static str {
        payload
            .downcast_ref::<&'static str>()
            .copied()
            .unwrap_or("<not a static str>")
    }

    /// How an owner or reader releases its source.
    #[derive(Clone, Copy, Debug)]
    enum Release {
        Clear,
        RetireTerminal,
        Drop,
    }

    /// The callback panic that `release` delivers: the completion of a stop,
    /// or for `retire_terminal`, the callback's drop with the source.
    fn delivered_panic(release: Release) -> CallbackPanic {
        match release {
            Release::Clear | Release::Drop => CallbackPanic::SourceComplete,
            Release::RetireTerminal => CallbackPanic::Drop,
        }
    }

    #[test]
    fn owner_retirement_propagates_a_callback_panic_once() {
        for release in [Release::Clear, Release::RetireTerminal, Release::Drop] {
            let log = OrderLog::default();
            let (mut owner, producer) = logged_request_owner(&log, delivered_panic(release));
            // A clear or retire_terminal that unwinds leaves the owner alive,
            // so the call itself destroyed the source.
            let unwound = match release {
                Release::Clear => std::panic::catch_unwind(AssertUnwindSafe(|| {
                    owner.clear(Some(HttpErrorCode::CANCEL));
                })),
                Release::RetireTerminal => {
                    std::panic::catch_unwind(AssertUnwindSafe(|| owner.retire_terminal()))
                }
                Release::Drop => std::panic::catch_unwind(AssertUnwindSafe(|| drop(owner))),
            };
            let payload = unwound.expect_err("the callback panic propagates");
            let expected = match release {
                Release::RetireTerminal => "callback drop",
                Release::Clear | Release::Drop => "callback completion",
            };
            assert_eq!(panic_text(&*payload), expected, "{release:?}");
            assert!(!producer.is_live(), "{release:?}");
            let code = match release {
                Release::Clear => HttpErrorCode::CANCEL,
                Release::RetireTerminal | Release::Drop => HttpErrorCode::NO_ERROR,
            };
            assert_eq!(
                entries(&log),
                [
                    format!("callback complete {:?}", Some(code)),
                    "callback dropped".to_owned()
                ],
                "{release:?}"
            );
        }

        // A drop while already unwinding drops the callback's payload.
        let log = OrderLog::default();
        let (owner, producer) = logged_request_owner(&log, CallbackPanic::SourceComplete);
        let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _owner = owner;
            panic!("outer");
        }));
        assert_eq!(panic_text(&*unwound.unwrap_err()), "outer");
        assert!(!producer.is_live());
        assert_eq!(callback_entries(&log).len(), 2);
    }

    #[test]
    fn producer_stop_completes_a_source_whose_consumer_holds_only_a_resume() {
        let (owner, producer, log) = request_owner();
        let source = owner.source().unwrap();
        let resume = take_resume(read_body(source, u32::MAX).unwrap());
        assert!(producer.stop_reading(Some(HttpErrorCode::CANCEL)));
        assert_eq!(log.borrow().completions, [Some(HttpErrorCode::CANCEL)]);
        drop(resume);
        assert_eq!(
            error_code(read_body(source, u32::MAX)),
            HttpErrorCode::CANCEL
        );
        assert_eq!(log.borrow().completions, [Some(HttpErrorCode::CANCEL)]);
    }

    #[test]
    fn body_reader_is_small() {
        assert!(size_of::<HttpBodyReader>() <= 32, "the timed wait is boxed");
    }

    #[test]
    fn body_reader_waits_without_a_timed_baton_or_resume_future() {
        run_paused(async {
            let (owner, producer, _log) = request_owner();
            let mut reader = owner.into_body_reader();
            let (_twin, twin_producer, _twin_log) = request_owner();
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            let observable = std_allocations_observable();

            let (pending, allocations) =
                count_allocations(|| reader.poll_body(&mut context, u32::MAX).is_pending());
            assert!(pending);
            // The body that wakes the read allocates what the same body
            // allocates on a source with no read pending.
            let (bodies, twin_bodies) = (body(b"token"), body(b"token"));
            let (state, push) = count_allocations(|| producer.body(bodies, 0, false));
            let (twin_state, twin_push) =
                count_allocations(|| twin_producer.body(twin_bodies, 0, false));
            assert_eq!(
                (state, twin_state),
                (FlowControlState::Open, FlowControlState::Open)
            );
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
            if observable {
                assert_eq!(allocations, 0, "a pending read allocates nothing");
                assert_eq!(push, twin_push, "the wake adds nothing to the push");
            } else {
                eprintln!("allocation counts need a statically linked std");
            }
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context, u32::MAX) else {
                panic!("the body must complete the pending read");
            };
            assert_eq!(body_text(&event), b"token");
            assert!(!producer.event_baton_created_for_test());

            // Control: the general API's first empty read creates the baton and
            // boxes the suspend's resume.
            let (control, control_producer, _control_log) = request_owner();
            let mut read = control
                .source()
                .unwrap()
                .read_body_event_unboxed(u32::MAX, CancellationToken::never());
            let (event, allocations) =
                count_allocations(|| match Pin::new(&mut read).poll(&mut context) {
                    Poll::Ready(Ok(event)) => event,
                    _ => panic!("the first empty general read must suspend"),
                });
            assert_eq!(event.event_type(), HttpBodyEventType::Suspend);
            assert!(control_producer.event_baton_created_for_test());
            if observable {
                assert_eq!(allocations, 2, "the event baton and the boxed resume");
            }
        });
    }

    /// One producer step of a paced body script.
    #[derive(Clone, Copy)]
    enum Step {
        Body(&'static [&'static [u8]]),
        Padding(u16),
        Trailers,
        Eom,
        Abort,
    }

    fn apply(producer: &HttpStreamSourceProducer, step: Step) {
        match step {
            Step::Body(segments) => {
                let mut queue = BufQueue::new(usize::MAX);
                for segment in segments {
                    queue.push_bytes(Bytes::from_static(segment)).unwrap();
                }
                assert_eq!(producer.body(queue, 0, false), FlowControlState::Open);
            }
            Step::Padding(bytes) => assert!(producer.padding(bytes)),
            Step::Trailers => {
                let mut trailers = HttpHeaders::new();
                trailers.add(b"trailer", b"value");
                assert!(producer.trailers(trailers));
            }
            Step::Eom => assert!(producer.eom()),
            Step::Abort => assert!(producer.abort(HttpErrorCode::TRANSPORT_READ_ERROR, b"gone")),
        }
    }

    /// What a consumer saw of one body read.
    #[derive(Debug, Eq, PartialEq)]
    enum Seen {
        Body(Vec<u8>, bool),
        Padding(u16, bool),
        Trailers(Vec<u8>),
        Error(HttpErrorCode, Vec<u8>),
    }

    fn seen(result: Result<HttpBodyEvent, HttpError>) -> Seen {
        let event = match result {
            Ok(event) => event,
            Err(error) => return Seen::Error(error.code(), error.message().to_vec()),
        };
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(body) => {
                Seen::Body(body.coalesce(body.chain_length()).unwrap().to_vec(), eom)
            }
            HttpBodyEventKind::Padding(bytes) => Seen::Padding(bytes, eom),
            HttpBodyEventKind::Trailers(trailers) => {
                assert!(eom);
                Seen::Trailers(trailers.get_single_or_empty(b"trailer").to_vec())
            }
            _ => panic!("unexpected body event"),
        }
    }

    type ReadTranscript = (Vec<Seen>, Vec<(StreamId, usize, usize)>);

    /// Reads a request source through the general API, three bytes at most per
    /// event, applying the next step of `script` whenever a read or a resume is
    /// pending. Suspends are awaited and left out of the transcript.
    fn general_transcript(script: &[Step]) -> ReadTranscript {
        let (owner, producer, log) = request_owner();
        let source = owner.source().unwrap();
        let mut steps = script.iter();
        let mut next = || apply(&producer, *steps.next().expect("a step for each wait"));
        let mut context = Context::from_waker(Waker::noop());
        let mut transcript = Vec::new();
        loop {
            let mut read = source.read_body_event_unboxed(3, CancellationToken::never());
            let result = loop {
                match Pin::new(&mut read).poll(&mut context) {
                    Poll::Ready(result) => break result,
                    Poll::Pending => next(),
                }
            };
            if let Ok(event) = &result
                && event.event_type() == HttpBodyEventType::Suspend
            {
                let mut resume = take_resume(result.unwrap());
                while resume.as_mut().poll(&mut context).is_pending() {
                    next();
                }
                continue;
            }
            let last = result.as_ref().map_or(true, HttpBodyEvent::eom);
            transcript.push(seen(result));
            if last {
                return (transcript, log.borrow().processed.clone());
            }
        }
    }

    /// Reads as [`general_transcript`] does, through a body reader.
    fn reader_transcript(script: &[Step]) -> ReadTranscript {
        let (owner, producer, log) = request_owner();
        let mut reader = owner.into_body_reader();
        let mut steps = script.iter();
        let mut context = Context::from_waker(Waker::noop());
        let mut transcript = Vec::new();
        loop {
            let result = loop {
                match reader.poll_body(&mut context, 3) {
                    Poll::Ready(result) => break result,
                    Poll::Pending => apply(&producer, *steps.next().expect("a step for each wait")),
                }
            };
            let last = result.as_ref().map_or(true, HttpBodyEvent::eom);
            transcript.push(seen(result));
            if last {
                return (transcript, log.borrow().processed.clone());
            }
        }
    }

    #[test]
    fn body_reader_events_match_the_general_read_without_suspends() {
        run_paused(async {
            let start: [Step; 3] = [
                Step::Body(&[b"ab", b"cde", b"f"]),
                Step::Padding(4),
                Step::Body(&[b"ghij", b"k"]),
            ];
            for end in [Step::Trailers, Step::Eom, Step::Abort] {
                let script = [start[0], start[1], start[2], end];
                let general = general_transcript(&script);
                let reader = reader_transcript(&script);
                assert_eq!(reader, general);
                assert!(general.0.len() >= 6, "the bodies split at the maximum");
            }
        });
    }

    #[test]
    fn body_reader_pending_read_fails_with_the_producer_abort() {
        run_paused(async {
            let (owner, producer, log) = request_owner();
            let mut reader = owner.into_body_reader();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());

            assert!(producer.abort(HttpErrorCode::TRANSPORT_READ_ERROR, b""));
            let Poll::Ready(Err(error)) = reader.poll_body(&mut context, u32::MAX) else {
                panic!("the abort must fail the pending read");
            };
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
            assert_eq!(log.borrow().completions, [None]);
        });
    }

    #[test]
    fn body_reader_pending_read_times_out_with_the_source_timeout() {
        run_paused(async {
            let (owner, producer, _log) = request_owner();
            assert!(producer.set_read_timeout(Duration::from_secs(2)));
            let mut reader = owner.into_body_reader();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());
            let started = time::Instant::now();

            time::advance(Duration::from_secs(2)).await;
            let error =
                match std::future::poll_fn(|context| reader.poll_body(context, u32::MAX)).await {
                    Err(error) => error,
                    Ok(event) => panic!("expected a read timeout, got {:?}", event.event_type()),
                };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"Read timeout after 2000");
            // One timeout fails the read; the timer rounds up to a millisecond.
            let elapsed = time::Instant::now() - started;
            assert!(
                (Duration::from_secs(2)..Duration::from_secs(4)).contains(&elapsed),
                "{elapsed:?}"
            );
        });
    }

    #[test]
    fn body_reader_read_timeout_set_while_waiting_counts_from_the_update() {
        run_paused(async {
            let (owner, producer, _log) = request_owner();
            let mut reader = owner.into_body_reader();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());

            time::advance(Duration::from_secs(1)).await;
            let updated = time::Instant::now();
            assert!(producer.set_read_timeout(Duration::from_secs(2)));
            // The read is next polled after its deadline has passed.
            time::advance(Duration::from_secs(3)).await;
            let error =
                match std::future::poll_fn(|context| reader.poll_body(context, u32::MAX)).await {
                    Err(error) => error,
                    Ok(event) => panic!("expected a read timeout, got {:?}", event.event_type()),
                };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"Read timeout after 2000");
            assert_eq!(time::Instant::now() - updated, Duration::from_secs(3));
        });
    }

    #[test]
    fn body_reader_on_an_empty_owner_is_not_readable() {
        let mut context = Context::from_waker(Waker::noop());
        let (mut owner, _producer, _log) = request_owner();
        owner.clear(None);
        let mut reader = owner.into_body_reader();
        assert!(!reader.readable());
        let Poll::Ready(Err(error)) = reader.poll_body(&mut context, u32::MAX) else {
            panic!("a read of an empty owner must fail");
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
        assert_eq!(error.message(), b"HTTP source is no longer readable");

        // So is a read after the reader released its source.
        let (owner, _producer, _log) = request_owner();
        let mut reader = owner.into_body_reader();
        assert!(reader.readable());
        reader.clear(Some(HttpErrorCode::CANCEL));
        assert!(!reader.readable());
        assert!(matches!(
            reader.poll_body(&mut context, u32::MAX),
            Poll::Ready(Err(error)) if error.code() == HttpErrorCode::DROPPED
        ));
    }

    #[test]
    fn body_reader_refuses_a_read_while_another_is_active() {
        run_paused(async {
            let mut context = Context::from_waker(Waker::noop());
            let refused = |reader: &mut HttpBodyReader, context: &mut Context<'_>| {
                let Poll::Ready(Err(error)) = reader.poll_body(context, u32::MAX) else {
                    panic!("an overlapping read must fail");
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                assert_eq!(
                    error.message(),
                    b"HTTP stream source already has an active read or resume wait"
                );
            };

            // A suspend's resume wait.
            let (owner, _producer, _log) = request_owner();
            let resume = take_resume(read_body(owner.source().unwrap(), u32::MAX).unwrap());
            let mut reader = owner.into_body_reader();
            refused(&mut reader, &mut context);
            drop(resume);
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());

            // A general read waiting after its suspend.
            let (owner, _producer, _log) = request_owner();
            drop(read_body(owner.source().unwrap(), u32::MAX).unwrap());
            let mut read = owner
                .source()
                .unwrap()
                .read_body_event_unboxed(u32::MAX, CancellationToken::never());
            assert!(Pin::new(&mut read).poll(&mut context).is_pending());
            let mut reader = owner.into_body_reader();
            refused(&mut reader, &mut context);
            drop(read);
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());
        });
    }

    /// A waker that logs its drop, and panics in it if `panics`.
    struct LoggedWaker {
        log: OrderLog,
        panics: bool,
    }

    #[expect(
        clippy::manual_noop_waker,
        reason = "the waker must log its drop, which `Waker::noop()` cannot"
    )]
    impl Wake for LoggedWaker {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for LoggedWaker {
        fn drop(&mut self) {
            self.log.lock().unwrap().push("waker dropped".to_owned());
            if self.panics {
                panic!("waker drop");
            }
        }
    }

    /// The wait of a pending body reader read.
    #[derive(Clone, Copy, Debug)]
    enum PendingWait {
        /// The source has no read timeout.
        Direct,
        /// A read timeout set during the wait, then one poll.
        Retimed,
    }

    /// A body reader of a [`logged_request_owner`] source, with a read pending
    /// in `wait`. The reader holds the only reference to the poll's waker.
    fn reader_with_pending_read(
        log: &OrderLog,
        panic: CallbackPanic,
        wait: PendingWait,
        waker_panics: bool,
    ) -> (HttpBodyReader, HttpStreamSourceProducer) {
        let (owner, producer) = logged_request_owner(log, panic);
        let mut reader = owner.into_body_reader();
        let waker = Waker::from(Arc::new(LoggedWaker {
            log: Arc::clone(log),
            panics: waker_panics,
        }));
        let mut context = Context::from_waker(&waker);
        assert!(reader.poll_body(&mut context, u32::MAX).is_pending());
        if let PendingWait::Retimed = wait {
            assert!(producer.set_read_timeout(Duration::from_secs(2)));
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());
        }
        (reader, producer)
    }

    /// The callback transcript of `release` on a [`logged_request_owner`]
    /// source with no read pending.
    fn twin_transcript(release: Release) -> Vec<String> {
        let log = OrderLog::default();
        let (mut owner, _producer) = logged_request_owner(&log, CallbackPanic::None);
        match release {
            Release::Clear => owner.clear(Some(HttpErrorCode::CANCEL)),
            Release::RetireTerminal => owner.retire_terminal(),
            Release::Drop => drop(owner),
        }
        entries(&log)
    }

    /// Releases a reader with each kind of pending wait, and checks that the
    /// callback saw what the owner's `release` gives with no read pending, and
    /// that the read was retired, dropping its waker, before the source was
    /// destroyed.
    fn check_release_of_a_pending_read(release: Release) {
        run_paused(async {
            for wait in [PendingWait::Direct, PendingWait::Retimed] {
                let log = OrderLog::default();
                let (mut reader, producer) =
                    reader_with_pending_read(&log, CallbackPanic::None, wait, false);
                assert!(entries(&log).is_empty(), "{wait:?}");
                match release {
                    Release::Clear => reader.clear(Some(HttpErrorCode::CANCEL)),
                    Release::RetireTerminal => reader.retire_terminal(),
                    Release::Drop => drop(reader),
                }
                assert!(!producer.is_live(), "{wait:?}");
                assert_eq!(callback_entries(&log), twin_transcript(release), "{wait:?}");
                let log = entries(&log);
                let position = |entry: &str| log.iter().position(|logged| logged == entry);
                let waker = position("waker dropped").expect("the waker is dropped");
                let callback = position("callback dropped").expect("the callback is dropped");
                assert!(waker < callback, "{wait:?}: {log:?}");
            }
        });
    }

    #[test]
    fn body_reader_clear_stops_the_source_with_its_code() {
        check_release_of_a_pending_read(Release::Clear);
    }

    #[test]
    fn body_reader_retire_terminal_releases_the_wait_without_a_stop() {
        check_release_of_a_pending_read(Release::RetireTerminal);
    }

    #[test]
    fn body_reader_drop_releases_a_pending_wait() {
        check_release_of_a_pending_read(Release::Drop);
    }

    #[test]
    fn body_reader_retirement_propagates_a_callback_panic() {
        run_paused(async {
            for release in [Release::Clear, Release::RetireTerminal, Release::Drop] {
                for waker_panics in [false, true] {
                    let label = format!("{release:?}, waker panics: {waker_panics}");
                    let log = OrderLog::default();
                    let (mut reader, producer) = reader_with_pending_read(
                        &log,
                        delivered_panic(release),
                        PendingWait::Direct,
                        waker_panics,
                    );
                    // A clear or retire_terminal that unwinds leaves the
                    // reader alive, so the call itself destroyed the source.
                    let unwound = match release {
                        Release::Clear => std::panic::catch_unwind(AssertUnwindSafe(|| {
                            reader.clear(Some(HttpErrorCode::CANCEL));
                        })),
                        Release::RetireTerminal => {
                            std::panic::catch_unwind(AssertUnwindSafe(|| reader.retire_terminal()))
                        }
                        Release::Drop => {
                            std::panic::catch_unwind(AssertUnwindSafe(|| drop(reader)))
                        }
                    };
                    let payload = unwound.expect_err("a retirement panic propagates");
                    let expected = match (waker_panics, release) {
                        (true, _) => "waker drop",
                        (false, Release::RetireTerminal) => "callback drop",
                        (false, Release::Clear | Release::Drop) => "callback completion",
                    };
                    assert_eq!(panic_text(&*payload), expected, "{label}");
                    assert!(!producer.is_live(), "{label}");
                    assert_eq!(
                        entries(&log),
                        [
                            "waker dropped".to_owned(),
                            twin_transcript(release)[0].clone(),
                            "callback dropped".to_owned()
                        ],
                        "{label}"
                    );
                }
            }

            // A drop while already unwinding drops both payloads.
            let log = OrderLog::default();
            let (reader, producer) = reader_with_pending_read(
                &log,
                CallbackPanic::SourceComplete,
                PendingWait::Direct,
                true,
            );
            let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _reader = reader;
                panic!("outer");
            }));
            assert_eq!(panic_text(&*unwound.unwrap_err()), "outer");
            assert!(!producer.is_live());
            assert_eq!(entries(&log).len(), 3, "the waker and the callback");
        });
    }

    #[test]
    fn body_reader_producer_stop_completes_the_source_at_the_next_poll() {
        run_paused(async {
            let (owner, producer, log) = request_owner();
            let mut reader = owner.into_body_reader();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());

            assert!(producer.stop_reading(Some(HttpErrorCode::CANCEL)));
            assert!(
                log.borrow().completions.is_empty(),
                "the pending read defers the completion"
            );
            let Poll::Ready(Err(error)) = reader.poll_body(&mut context, u32::MAX) else {
                panic!("the stop must fail the pending read");
            };
            assert_eq!(error.code(), HttpErrorCode::CANCEL);
            assert_eq!(log.borrow().completions, [Some(HttpErrorCode::CANCEL)]);
        });
    }

    #[test]
    fn body_reader_keeps_the_armed_maximum() {
        run_paused(async {
            let (owner, producer, _log) = request_owner();
            let mut reader = owner.into_body_reader();
            let mut context = Context::from_waker(Waker::noop());
            assert!(reader.poll_body(&mut context, 4).is_pending());
            assert!(reader.poll_body(&mut context, u32::MAX).is_pending());

            assert_eq!(
                producer.body(body(b"0123456789"), 0, false),
                FlowControlState::Open
            );
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context, u32::MAX) else {
                panic!("the body must complete the pending read");
            };
            assert_eq!(body_text(&event), b"0123");
            let Poll::Ready(Ok(event)) = reader.poll_body(&mut context, u32::MAX) else {
                panic!("the next read takes the rest of the body");
            };
            assert_eq!(body_text(&event), b"456789");
        });
    }

    #[derive(Debug, Eq, PartialEq)]
    enum Delivered {
        Processed(usize),
        Window,
        Complete(Option<HttpErrorCode>),
    }

    type DispatchHook = Rc<RefCell<Option<Box<dyn FnOnce()>>>>;

    /// Logs each event without allocating while the log has capacity, and runs
    /// the installed hook once, from a `bytes_processed` that is being
    /// dispatched.
    struct HookedCallback {
        log: Rc<RefCell<Vec<Delivered>>>,
        hook: DispatchHook,
    }

    impl HttpStreamSourceCallback for HookedCallback {
        fn bytes_processed(&mut self, _id: StreamId, amount: usize, _to_acknowledge: usize) {
            self.log.borrow_mut().push(Delivered::Processed(amount));
            let hook = self.hook.borrow_mut().take();
            if let Some(hook) = hook {
                hook();
            }
        }

        fn window_open(&mut self, _id: StreamId) {
            self.log.borrow_mut().push(Delivered::Window);
        }

        fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
            self.log
                .borrow_mut()
                .push(Delivered::Complete(error.map(|error| error.code())));
        }
    }

    fn hooked_callback() -> (HookedCallback, Rc<RefCell<Vec<Delivered>>>, DispatchHook) {
        let log = Rc::new(RefCell::new(Vec::with_capacity(64)));
        let hook = DispatchHook::default();
        let callback = HookedCallback {
            log: Rc::clone(&log),
            hook: Rc::clone(&hook),
        };
        (callback, log, hook)
    }

    /// A session source after its request head, with a [`HookedCallback`].
    fn hooked_session_source(
        capacity: u32,
    ) -> (
        HttpStreamSource,
        HttpStreamSourceProducer,
        Rc<RefCell<Vec<Delivered>>>,
        DispatchHook,
    ) {
        let (callback, log, hook) = hooked_callback();
        let source =
            HttpStreamSource::new_session(StreamId::new(7), callback, capacity, Duration::ZERO)
                .unwrap();
        let producer = source.producer();
        assert!(producer.headers(request(None), false));
        read_header(&source).unwrap();
        (source, producer, log, hook)
    }

    /// [`request_owner`] with a [`HookedCallback`].
    fn hooked_request_owner() -> (
        HttpStreamSourceOwner,
        HttpStreamSourceProducer,
        Rc<RefCell<Vec<Delivered>>>,
    ) {
        let (callback, log, _hook) = hooked_callback();
        let ingress =
            HttpIngressBody::new(StreamId::new(1), callback, 65_535, Host::tokio()).unwrap();
        assert!(ingress.initialize_body_only(&mut request(None), false));
        let producer = ingress.producer();
        (ingress.into_source_owner(), producer, log)
    }

    #[test]
    fn callback_events_queued_during_dispatch_are_delivered_in_order() {
        let (mut source, producer, log, hook) = hooked_session_source(8);
        let _drain = source.drain_on_drop();
        assert_eq!(
            producer.body(body(b"abcd"), 0, false),
            FlowControlState::Open
        );
        // During the drain's first credit, a body that closes the window and an
        // abort queue a credit, a window update and the completion.
        let reentrant = producer.clone();
        *hook.borrow_mut() = Some(Box::new(move || {
            assert_eq!(
                reentrant.body(body(b"efghijkl"), 0, false),
                FlowControlState::Closed
            );
            assert!(reentrant.abort(HttpErrorCode::TRANSPORT_READ_ERROR, b"gone"));
        }));
        source.stop_reading(None);
        assert_eq!(
            *log.borrow(),
            [
                Delivered::Processed(4),
                Delivered::Processed(8),
                Delivered::Window,
                Delivered::Complete(None),
            ]
        );
    }

    #[test]
    fn deferred_callback_events_allocate_at_the_first_deferral() {
        let observable = std_allocations_observable();
        // Each source drains to a sink, so every body is credited as it arrives.
        let draining = || {
            let (mut source, producer, log, hook) = hooked_session_source(65_535);
            let drain = source.drain_on_drop();
            source.stop_reading(None);
            (source, drain, producer, log, hook)
        };
        let bodies = || [body(b"one"), body(b"two"), body(b"three")];

        // The second and third credits arrive while the first is dispatched.
        let (_source, _drain, producer, deferred_log, hook) = draining();
        let [first, second, third] = bodies();
        let reentrant = producer.clone();
        *hook.borrow_mut() = Some(Box::new(move || {
            assert_eq!(reentrant.body(second, 0, false), FlowControlState::Open);
            assert_eq!(reentrant.body(third, 0, false), FlowControlState::Open);
        }));
        let (state, deferring) = count_allocations(|| producer.body(first, 0, false));
        assert_eq!(state, FlowControlState::Open);

        // Control: the same credits, each delivered at once.
        let (_source, _drain, producer, direct_log, _hook) = draining();
        let [first, second, third] = bodies();
        let ((), direct) = count_allocations(|| {
            for body in [first, second, third] {
                assert_eq!(producer.body(body, 0, false), FlowControlState::Open);
            }
        });

        let delivered = [
            Delivered::Processed(3),
            Delivered::Processed(3),
            Delivered::Processed(5),
        ];
        assert_eq!(*deferred_log.borrow(), delivered);
        assert_eq!(*direct_log.borrow(), delivered);
        println!("deferring dispatch: {deferring} allocations, direct: {direct}");
        if observable {
            // The queue's box and its buffer, both kept for later deferrals.
            assert_eq!(
                deferring - direct,
                2,
                "the first deferral allocates the deferral queue"
            );
        } else {
            eprintln!("allocation counts need a statically linked std");
        }
    }

    #[test]
    fn body_reader_paced_body_reads_allocate_nothing() {
        run_paused(async {
            let observable = std_allocations_observable();
            let (owner, producer, log) = hooked_request_owner();
            let mut reader = owner.into_body_reader();
            let mut context = Context::from_waker(Waker::noop());
            let bodies = [body(b"one"), body(b"two"), body(b"three")];
            let ((), allocations) = count_allocations(|| {
                for body in bodies {
                    assert!(reader.poll_body(&mut context, u32::MAX).is_pending());
                    assert_eq!(producer.body(body, 0, false), FlowControlState::Open);
                    let Poll::Ready(Ok(event)) = reader.poll_body(&mut context, u32::MAX) else {
                        panic!("the body must complete the pending read");
                    };
                    assert!(!event.eom());
                }
                assert!(reader.poll_body(&mut context, u32::MAX).is_pending());
                assert!(producer.eom());
                let Poll::Ready(Ok(event)) = reader.poll_body(&mut context, u32::MAX) else {
                    panic!("the EOM must complete the pending read");
                };
                assert!(event.eom());
            });
            assert_eq!(
                *log.borrow(),
                [
                    Delivered::Processed(3),
                    Delivered::Processed(3),
                    Delivered::Processed(5),
                    Delivered::Complete(None),
                ]
            );
            println!("paced body reader reads: {allocations} allocations");
            if observable {
                assert_eq!(allocations, 0);
            } else {
                eprintln!("allocation counts need a statically linked std");
            }
        });
    }

    #[test]
    fn general_read_suspend_and_resume_allocation_count() {
        run_paused(async {
            let observable = std_allocations_observable();
            let (owner, producer, log) = hooked_request_owner();
            let source = owner.source().unwrap();
            let mut context = Context::from_waker(Waker::noop());
            let mut bodies = [body(b"one"), body(b"two"), body(b"three")].into_iter();
            let mut produce = || {
                let body = bodies.next().expect("a body for each wait");
                assert_eq!(producer.body(body, 0, false), FlowControlState::Open);
            };
            let (suspends, allocations) = count_allocations(|| {
                let mut suspends = 0;
                let mut read_bodies = 0;
                while read_bodies < 3 {
                    let mut read =
                        source.read_body_event_unboxed(u32::MAX, CancellationToken::never());
                    let event = loop {
                        match Pin::new(&mut read).poll(&mut context) {
                            Poll::Ready(result) => break result.unwrap(),
                            Poll::Pending => produce(),
                        }
                    };
                    if event.event_type() == HttpBodyEventType::Suspend {
                        suspends += 1;
                        let mut resume = take_resume(event);
                        while resume.as_mut().poll(&mut context).is_pending() {
                            produce();
                        }
                        continue;
                    }
                    assert!(!event.eom());
                    read_bodies += 1;
                }
                suspends
            });
            // A read after a suspend's resume returns the body, and the next
            // empty read waits for the body after it instead of suspending.
            assert_eq!(suspends, 2);
            assert_eq!(
                *log.borrow(),
                [
                    Delivered::Processed(3),
                    Delivered::Processed(3),
                    Delivered::Processed(5),
                ]
            );
            println!("general read suspend and resume: {allocations} allocations");
            if observable {
                // The event baton once, then one boxed resume per suspend.
                assert_eq!(allocations, 3);
            } else {
                eprintln!("allocation counts need a statically linked std");
            }
        });
    }

    #[test]
    fn deferred_read_timeout_applied_at_eom_allocation_count() {
        run_paused(async {
            let observable = std_allocations_observable();
            let (owner, producer, log) = hooked_request_owner();
            let source = owner.source().unwrap();
            let mut context = Context::from_waker(Waker::noop());
            let ((), allocations) = count_allocations(|| {
                assert!(
                    producer.defer_read_timeout(DeferredReadTimeout::DeferUpdates(
                        Duration::from_secs(5)
                    ))
                );
                // The request's EOM applies the deferred timeout.
                assert!(producer.arm_read_timeout());
            });
            assert!(!producer.arm_read_timeout());
            let mut read = source.read_body_event_unboxed(u32::MAX, CancellationToken::never());
            let Poll::Ready(Ok(event)) = Pin::new(&mut read).poll(&mut context) else {
                panic!("an empty general read must suspend");
            };
            let mut resume = take_resume(event);
            time::advance(Duration::from_secs(5)).await;
            assert_eq!(
                std::future::poll_fn(|context| resume.as_mut().poll(context)).await,
                ResumeStatus::TimedOut
            );
            assert!(log.borrow().is_empty());
            println!("deferred read timeout set and applied: {allocations} allocations");
            if observable {
                assert_eq!(allocations, 0);
            } else {
                eprintln!("allocation counts need a statically linked std");
            }
        });
    }

    #[test]
    fn stream_shared_fits_the_512_class() {
        let (_owner, producer, _log) = hooked_request_owner();
        let (size, fields) = producer.shared_layout_for_test();
        for (field, bytes) in &fields {
            println!("{field}: {bytes} B");
        }
        // An `Rc` allocation holds the strong and weak counts before the value.
        let allocation = size + 2 * size_of::<usize>();
        println!("StreamShared: {size} B, Rc allocation: {allocation} B");
        assert!(
            allocation <= 512,
            "each stream source allocates {allocation} B for its shared state"
        );
    }
}

mod stream_source_holder {
    use super::*;

    #[test]
    fn start_retains_holder_until_source_completion() {
        let holder = HttpStreamSourceHolder::with_default_capacity(None).unwrap();
        let weak = Rc::downgrade(&holder);
        let producer = holder.producer();
        holder.start();
        drop(holder);

        let retained = weak.upgrade().expect("start retains the live holder");
        producer.headers(response(200, None), true);
        assert!(ready(retained.read_header_event()).unwrap().eom());
        assert!(!retained.is_live());
        drop(retained);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn source_completes_and_releases_inner() {
        let holder = HttpStreamSourceHolder::with_default_capacity(None).unwrap();
        holder.start();
        let producer = holder.producer();
        producer.headers(request(Some(b"1000")), false);
        for _ in 0..10 {
            producer.body(sized_body(100), 0, false);
        }
        producer.eom();
        assert!(!ready(holder.read_header_event()).unwrap().eom());
        let event = ready(holder.read_body_event(u32::MAX)).unwrap();
        assert_eq!(event.as_body().unwrap().chain_length(), 1000);
        assert!(event.eom());
        assert!(!holder.is_live());
        assert!(!producer.is_live());
    }

    #[test]
    fn drop_unfinished_source_safely() {
        let holder = HttpStreamSourceHolder::with_default_capacity(None).unwrap();
        let producer = holder.producer();
        producer.headers(request(Some(b"1000")), false);
        drop(holder);
        assert!(!producer.is_live());
    }

    #[test]
    fn capacity_wait_reopens_after_consume() {
        let holder = HttpStreamSourceHolder::make(None, 2).unwrap();
        let producer = holder.producer();
        producer.headers(request(Some(b"4")), false);

        assert_eq!(
            producer.body(sized_body(1), 0, false),
            FlowControlState::Open
        );
        let mut available = Box::pin(holder.await_egress_buffer());
        assert!(poll_once(available.as_mut()).is_ready());

        assert_eq!(
            producer.body(sized_body(1), 0, false),
            FlowControlState::Closed
        );
        let mut blocked = Box::pin(holder.await_egress_buffer());
        assert!(poll_once(blocked.as_mut()).is_pending());
        ready(holder.read_header_event()).unwrap();
        ready(holder.read_body_event(u32::MAX)).unwrap();
        assert_eq!(
            producer.body(sized_body(2), 0, false),
            FlowControlState::Closed
        );
        assert!(poll_once(blocked.as_mut()).is_ready());

        let mut blocked = Box::pin(holder.await_egress_buffer());
        assert!(poll_once(blocked.as_mut()).is_pending());
        ready(holder.read_body_event(u32::MAX)).unwrap();
        assert!(poll_once(blocked.as_mut()).is_ready());
    }
}

mod coalesced_body_growth {
    use std::collections::VecDeque;

    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn range(&mut self, low: usize, high: usize) -> usize {
            low + (self.next() % (high - low + 1) as u64) as usize
        }
    }

    fn segment_views(body: &BufQueue) -> Vec<(usize, usize)> {
        body.segments()
            .map(|segment| (segment.as_ptr() as usize, segment.len()))
            .collect()
    }

    /// A body piece of `segments` distinct four-byte segments, as the codec
    /// delivers a DATA payload that spans several reads.
    fn piece(segments: usize) -> BufQueue {
        let mut queue = BufQueue::new(4 * segments);
        (0..segments).for_each(|_| queue.push_bytes(Bytes::from(vec![0x5a; 4])).unwrap());
        queue
    }

    /// Queues `pieces` pieces of two segments each and reads them as one
    /// coalesced event, returning its container's growth hint.
    fn coalescing_cycle(
        source: &HttpStreamSource,
        producer: &HttpStreamSourceProducer,
        pieces: usize,
    ) -> u16 {
        let mut expected = Vec::new();
        for _ in 0..pieces {
            let body = piece(2);
            expected.extend(segment_views(&body));
            assert_eq!(producer.body(body, 0, false), FlowControlState::Open);
        }
        let event = read_body(source, u32::MAX).unwrap();
        let body = event.as_body().unwrap();
        assert_eq!(segment_views(body), expected);
        assert!(!event.eom());
        body.segment_growth_hint()
    }

    #[test]
    fn coalesced_streamed_body_matches_model() {
        // Streamed pieces of one to three segments, read at random maxima,
        // deliver the model's segments, split points, eom and credit.
        const CAPACITY: usize = 4096;
        let mut rng = Rng(0x6a09_e667_f3bc_c908);
        for round in 0..40 {
            let (source, producer, log) = session_source(CAPACITY as u32);
            assert!(producer.headers(response(200, None), false));
            assert!(!read_header(&source).unwrap().eom());
            let mut pending: VecDeque<(usize, usize)> = VecDeque::new();
            let mut outstanding = 0;
            let mut to_acknowledge = 0;
            let mut expected_processed = Vec::new();
            let mut expected_opened = Vec::new();
            let mut eom_sent = false;
            for step in 0..400 {
                let buffered: usize = pending.iter().map(|(_, length)| length).sum();
                let length = rng.range(1, 3 * 300);
                let push = !eom_sent && outstanding + length <= CAPACITY && rng.range(0, 2) != 0;
                if push {
                    let mut body = BufQueue::new(length);
                    let mut left = length;
                    while left != 0 {
                        let segment = rng.range(1, left.min(300));
                        body.push_bytes(Bytes::from(vec![0x33; segment])).unwrap();
                        left -= segment;
                    }
                    pending.extend(segment_views(&body));
                    outstanding += length;
                    let eom = step > 300 && rng.range(0, 9) == 0;
                    eom_sent = eom;
                    let expected_state = if outstanding < CAPACITY {
                        FlowControlState::Open
                    } else {
                        FlowControlState::Closed
                    };
                    assert_eq!(
                        producer.body(body, 0, eom),
                        expected_state,
                        "round {round} step {step}"
                    );
                    continue;
                }
                if buffered == 0 {
                    continue;
                }
                let maximum = match rng.range(0, 3) {
                    0 => u32::MAX,
                    _ => rng.range(1, buffered + 64) as u32,
                };
                let event = read_body(&source, maximum).unwrap();
                let taken = buffered.min(maximum as usize);
                let mut expected = Vec::new();
                let mut left = taken;
                while left != 0 {
                    let (pointer, length) = pending.pop_front().unwrap();
                    if length > left {
                        expected.push((pointer, left));
                        pending.push_front((pointer + left, length - left));
                        left = 0;
                    } else {
                        expected.push((pointer, length));
                        left -= length;
                    }
                }
                assert_eq!(
                    segment_views(event.as_body().unwrap()),
                    expected,
                    "round {round} step {step}"
                );
                assert_eq!(
                    event.eom(),
                    eom_sent && pending.is_empty(),
                    "round {round} step {step}"
                );
                let was_blocked = outstanding == CAPACITY;
                outstanding -= taken;
                to_acknowledge += taken;
                let acknowledged = if to_acknowledge >= CAPACITY / 2 {
                    std::mem::take(&mut to_acknowledge)
                } else {
                    0
                };
                expected_processed.push((
                    StreamId::new(7),
                    taken,
                    if eom_sent { 0 } else { acknowledged },
                ));
                if was_blocked {
                    expected_opened.push(StreamId::new(7));
                }
                if eom_sent && pending.is_empty() {
                    break;
                }
            }
            assert_eq!(log.borrow().processed, expected_processed, "round {round}");
            assert_eq!(log.borrow().opened, expected_opened, "round {round}");
        }
    }

    #[test]
    fn coalescing_grows_once_to_the_last_departed_capacity() {
        let (source, producer, _) = session_source(65_535);
        assert!(producer.headers(response(200, None), false));
        read_header(&source).unwrap();
        // Ten two-segment pieces double a four-segment container to 32.
        assert_eq!(coalescing_cycle(&source, &producer, 10), 32);
        // Three pieces need six segments; the container grows straight to the
        // 32 segments the last event departed with, instead of to 8.
        assert_eq!(coalescing_cycle(&source, &producer, 3), 32);
        // Two pieces fit the first piece's container, which does not grow.
        assert_eq!(coalescing_cycle(&source, &producer, 2), 4);
        // That short cycle does not lower the hint.
        assert_eq!(coalescing_cycle(&source, &producer, 3), 32);
    }

    #[test]
    fn coalescing_hint_is_capped_at_sixty_four_segments() {
        let (source, producer, _) = session_source(65_535);
        assert!(producer.headers(response(200, None), false));
        read_header(&source).unwrap();
        // 100 pieces grow the container to 256 segments; the hint keeps 64.
        assert_eq!(coalescing_cycle(&source, &producer, 100), 64);
        assert_eq!(coalescing_cycle(&source, &producer, 3), 64);
        // A cycle past the hint doubles from it.
        assert_eq!(coalescing_cycle(&source, &producer, 40), 64);
    }

    #[test]
    fn split_read_records_the_departing_prefix() {
        let (source, producer, _) = session_source(65_535);
        assert!(producer.headers(response(200, None), false));
        read_header(&source).unwrap();
        let mut expected = Vec::new();
        for _ in 0..10 {
            let body = piece(2);
            expected.extend(segment_views(&body));
            assert_eq!(producer.body(body, 0, false), FlowControlState::Open);
        }
        // The 76-byte prefix leaves one segment behind, so it takes the grown
        // container; the remainder keeps coalescing.
        let prefix = read_body(&source, 76).unwrap();
        assert_eq!(segment_views(prefix.as_body().unwrap()), expected[..19]);
        assert_eq!(prefix.as_body().unwrap().segment_growth_hint(), 32);
        let body = piece(2);
        expected.extend(segment_views(&body));
        assert_eq!(producer.body(body, 0, false), FlowControlState::Open);
        let rest = read_body(&source, u32::MAX).unwrap();
        assert_eq!(segment_views(rest.as_body().unwrap()), expected[19..]);
        assert_eq!(coalescing_cycle(&source, &producer, 3), 32);
    }
}

fn body_bytes(event: HttpBodyEvent) -> Vec<u8> {
    let body = event.as_body().expect("a body event");
    body.coalesce(body.chain_length()).unwrap().to_vec()
}

#[test]
fn ingress_abort_discards_queued_body_by_default() {
    let (source, producer, log) = session_source(65_535);
    assert!(producer.headers(response(200, None), false));
    read_header(&source).unwrap();
    assert_eq!(
        producer.body(body(b"first"), 0, false),
        FlowControlState::Open
    );
    assert!(producer.abort(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 5, 0)]);
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(log.borrow().completions, [None]);
}

#[test]
fn ingress_abort_delivers_queued_head_and_body_first_when_enabled() {
    let (source, producer, log) = session_source(65_535);
    source.deliver_ingress_before_abort();
    assert!(producer.headers(response(200, None), false));
    assert_eq!(
        producer.body(body(b"first"), 0, false),
        FlowControlState::Open
    );
    assert!(producer.abort(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
    assert!(log.borrow().processed.is_empty());

    let head = read_header(&source).expect("the queued head comes first");
    assert_eq!(head.headers().response_status(), Some(200));
    assert!(!head.eom());
    let first = read_body(&source, u32::MAX).expect("the queued body comes next");
    assert!(!first.eom());
    assert_eq!(body_bytes(first), b"first");
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 5, 0)]);
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(log.borrow().completions, [None]);
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(log.borrow().completions, [None]);
}

#[test]
fn source_completes_only_after_kept_events_are_read_when_enabled() {
    let (source, producer, log) = session_source(65_535);
    source.deliver_ingress_before_abort();
    assert!(producer.headers(response(200, None), false));
    read_header(&source).unwrap();
    producer.body(body(b"ab"), 0, false);
    let push = HttpFixedSource::make_fixed_response(200, None).unwrap();
    assert!(producer.push_promise(request(None), Box::new(push), false));
    producer.body(body(b"cd"), 0, false);
    assert!(producer.abort(HttpErrorCode::CANCEL, b"received reset from peer"));
    assert_eq!(body_bytes(read_body(&source, 1).unwrap()), b"a");
    assert_eq!(body_bytes(read_body(&source, u32::MAX).unwrap()), b"b");
    assert_eq!(
        read_body(&source, u32::MAX).unwrap().event_type(),
        HttpBodyEventType::PushPromise
    );
    assert!(log.borrow().completions.is_empty());
    assert_eq!(body_bytes(read_body(&source, u32::MAX).unwrap()), b"cd");
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::CANCEL
    );
    assert_eq!(log.borrow().completions, [None]);
    let credited: usize = log
        .borrow()
        .processed
        .iter()
        .map(|(_, amount, _)| amount)
        .sum();
    assert_eq!(credited, 4);
}

#[test]
fn dropping_the_source_releases_events_kept_after_an_ingress_abort() {
    let (source, producer, log) = session_source(65_535);
    source.deliver_ingress_before_abort();
    assert!(producer.headers(response(200, None), false));
    read_header(&source).unwrap();
    producer.body(body(b"abcd"), 0, false);
    assert!(producer.abort(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
    assert!(log.borrow().processed.is_empty());
    drop(source);
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 4, 0)]);
    assert_eq!(log.borrow().completions, [None]);
    assert!(!producer.is_live());
}

/// A body read before the kept final head is a state error that the source
/// detects itself. It returns the ingress error, which stays first, discards the
/// kept head and body, credits the body once and completes the source, so a later
/// read in the right order cannot return them.
#[test]
fn an_early_body_read_discards_events_kept_after_an_ingress_abort() {
    let (source, producer, log) = session_source(65_535);
    source.deliver_ingress_before_abort();
    assert!(producer.headers(response(200, None), false));
    producer.body(body(b"first"), 0, false);
    assert!(producer.abort(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 5, 0)]);
    assert_eq!(log.borrow().completions, [None]);
    assert_eq!(
        error_code(read_header(&source)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 5, 0)]);
    assert_eq!(log.borrow().completions, [None]);
}

/// As `an_early_body_read_discards_events_kept_after_an_ingress_abort`, for a
/// body read with a zero maximum after the kept head was read.
#[test]
fn a_zero_maximum_body_read_discards_events_kept_after_an_ingress_abort() {
    let (source, producer, log) = session_source(65_535);
    source.deliver_ingress_before_abort();
    assert!(producer.headers(response(200, None), false));
    producer.body(body(b"first"), 0, false);
    assert!(producer.abort(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
    read_header(&source).expect("the kept head comes first");
    assert_eq!(
        error_code(read_body(&source, 0)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 5, 0)]);
    assert_eq!(log.borrow().completions, [None]);
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::TRANSPORT_EOF
    );
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 5, 0)]);
    assert_eq!(log.borrow().completions, [None]);
}

#[test]
fn locally_detected_errors_still_discard_queued_events_when_enabled() {
    let (source, producer, log) = session_source(65_535);
    source.deliver_ingress_before_abort();
    assert!(producer.headers(response(200, Some(b"5")), false));
    read_header(&source).unwrap();
    producer.body(body(b"ab"), 0, false);
    producer.body(body(b"cdefg"), 0, false);
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::CONTENT_LENGTH_MISMATCH
    );
    let credited: usize = log
        .borrow()
        .processed
        .iter()
        .map(|(_, amount, _)| amount)
        .sum();
    assert_eq!(credited, 7);
}

#[test]
fn a_consumer_requested_abort_discards_queued_events_when_enabled() {
    let (source, producer, log) = session_source(65_535);
    source.deliver_ingress_before_abort();
    assert!(producer.headers(response(200, None), false));
    read_header(&source).unwrap();
    producer.body(body(b"abcd"), 0, false);
    assert!(
        producer.abort_discarding_queued(HttpError::new(HttpErrorCode::CANCEL), b"stream aborted")
    );
    assert_eq!(log.borrow().processed, [(StreamId::new(7), 4, 0)]);
    assert_eq!(
        error_code(read_body(&source, u32::MAX)),
        HttpErrorCode::CANCEL
    );
    assert_eq!(log.borrow().completions, [None]);
}

/// A fresh response source, as an owned upstream session builds it, can enable
/// `deliver_ingress_before_abort` before it is split or converted.
#[test]
fn a_response_source_delivers_ingress_before_abort_when_enabled() {
    run_paused(async {
        let source =
            HttpResponseSource::new(StreamId::new(1), None, 65_535, Host::tokio()).unwrap();
        source.deliver_ingress_before_abort();
        let producer = source.producer();
        assert!(producer.headers(response(200, None), false));
        assert_eq!(
            producer.body(body(b"first"), 0, false),
            FlowControlState::Open
        );
        assert!(producer.abort(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));

        let owner = source.into_source_owner();
        let source = owner.source().expect("the response is owned");
        let head = read_header(source).expect("the queued head comes first");
        assert_eq!(head.headers().response_status(), Some(200));
        let first = read_body(source, u32::MAX).expect("the queued body comes next");
        assert_eq!(body_bytes(first), b"first");
        assert_eq!(
            error_code(read_body(source, u32::MAX)),
            HttpErrorCode::TRANSPORT_EOF
        );
    });
}
