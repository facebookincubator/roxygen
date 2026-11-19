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
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;

use crate::buf_queue::BufQueue;
use crate::byte_event::HttpByteEvent;
use crate::byte_event::HttpByteEventCallback;
use crate::byte_event::HttpByteEventRegistration;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HeaderEventError;
use crate::events::HttpBodyEvent;
use crate::events::HttpBodyEventKind;
use crate::events::HttpBodyEventType;
use crate::events::HttpHeaderEvent;
use crate::events::ResumeStatus;
use crate::events::take_registration_drops_for_test;
use crate::headers::HttpHeaders;
use crate::http::HttpHeaderSize;
use crate::ids::StreamId;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_holder::HttpSourceHolder;
use crate::source_holder::SourceReleaseError;

#[derive(Default)]
struct SourceObservations {
    stops: usize,
    stop_errors: Vec<Option<HttpErrorCode>>,
    drops: usize,
    body_maximums: Vec<u32>,
    timeouts: Vec<Duration>,
}

#[test]
fn changing_header_eom_preserves_callbacks_and_rejects_informational_completion() {
    let mut message = HttpMessage::new();
    message.set_status_code(103).unwrap();
    let mut event = HttpHeaderEvent::new(message, false).unwrap();
    let encoded = Rc::new(Cell::new(false));
    let observed = Rc::clone(&encoded);
    event.set_egress_headers_callback(move |_| observed.set(true));
    assert_eq!(event.set_eom(true), Err(HeaderEventError::InformationalEom));
    assert!(!event.eom());
    assert!(event.has_egress_headers_callback());
    event.headers_mut().set_status_code(200).unwrap();
    event.set_eom(true).unwrap();
    assert!(event.eom());
    event.set_eom(false).unwrap();
    assert!(!event.eom());
    assert!(event.on_headers_encoded(HttpHeaderSize::default()));
    assert!(encoded.get());
    assert!(!event.on_headers_encoded(HttpHeaderSize::default()));
}

struct ScriptedSource {
    observations: Rc<RefCell<SourceObservations>>,
    headers: VecDeque<LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>>,
    bodies: VecDeque<Result<HttpBodyEvent, HttpError>>,
    stream_id: Option<StreamId>,
    on_drop: Option<Box<dyn FnOnce()>>,
}

impl ScriptedSource {
    fn new(observations: Rc<RefCell<SourceObservations>>) -> Self {
        Self {
            observations,
            headers: VecDeque::new(),
            bodies: VecDeque::new(),
            stream_id: None,
            on_drop: None,
        }
    }

    fn with_header(mut self, event: Result<HttpHeaderEvent, HttpError>) -> Self {
        self.headers.push_back(Box::pin(std::future::ready(event)));
        self
    }

    fn with_header_future(
        mut self,
        future: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> Self {
        self.headers.push_back(future);
        self
    }

    fn with_body(mut self, event: Result<HttpBodyEvent, HttpError>) -> Self {
        self.bodies.push_back(event);
        self
    }

    fn with_stream_id(mut self, stream_id: StreamId) -> Self {
        self.stream_id = Some(stream_id);
        self
    }

    fn with_drop_callback(mut self, callback: impl FnOnce() + 'static) -> Self {
        self.on_drop = Some(Box::new(callback));
        self
    }
}

impl HttpSource for ScriptedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.headers.pop_front().unwrap_or_else(|| {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"test header script exhausted",
            ))))
        })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.observations.borrow_mut().body_maximums.push(maximum);
        let event = self.bodies.pop_front().unwrap_or_else(|| {
            Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"test body script exhausted",
            ))
        });
        Box::pin(std::future::ready(event))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        let mut observations = self.observations.borrow_mut();
        observations.stops += 1;
        observations.stop_errors.push(error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.stream_id
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.observations.borrow_mut().timeouts.push(timeout);
    }
}

impl Drop for ScriptedSource {
    fn drop(&mut self) {
        self.observations.borrow_mut().drops += 1;
        if let Some(callback) = self.on_drop.take() {
            callback();
        }
    }
}

struct ManualCompletion<T> {
    result: Rc<RefCell<Option<T>>>,
}

impl<T> ManualCompletion<T> {
    fn complete(&self, result: T) {
        assert!(self.result.borrow_mut().replace(result).is_none());
    }
}

struct ManualFuture<T> {
    result: Rc<RefCell<Option<T>>>,
}

impl<T> Future for ManualFuture<T> {
    type Output = T;

    fn poll(self: std::pin::Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        self.result
            .borrow_mut()
            .take()
            .map_or(Poll::Pending, Poll::Ready)
    }
}

fn manual_future<T: 'static>() -> (ManualCompletion<T>, LocalSourceFuture<T>) {
    let result = Rc::new(RefCell::new(None));
    (
        ManualCompletion {
            result: Rc::clone(&result),
        },
        Box::pin(ManualFuture { result }),
    )
}

fn poll_once<T>(future: &mut LocalSourceFuture<T>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Future::poll(future.as_mut(), &mut context)
}

fn ready<T>(future: LocalSourceFuture<T>) -> T {
    let mut future = future;
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test source unexpectedly returned a pending future"),
    }
}

fn request() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method("GET").unwrap();
    message.set_url(b"/", false).unwrap();
    message
}

fn response(status: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    message
}

fn body(bytes: &'static [u8], eom: bool) -> HttpBodyEvent {
    HttpBodyEvent::body(
        BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).unwrap(),
        eom,
    )
}

#[derive(Default)]
struct CancellationCounter(Cell<usize>);

impl HttpByteEventCallback for CancellationCounter {
    fn on_byte_event(&self, _event: HttpByteEvent) {}

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
        self.0.set(self.0.get() + 1);
    }
}

mod source_holder {
    use super::*;

    #[test]
    fn drop_stops_unfinished_source_once() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        {
            let holder =
                HttpSourceHolder::new(Box::new(ScriptedSource::new(Rc::clone(&observations))));
            assert!(holder.readable());
            assert_eq!(holder.filter_name(), "SourceHolder");
        }
        let observations = observations.borrow();
        assert_eq!(observations.stops, 1);
        assert_eq!(observations.stop_errors, [None]);
        assert_eq!(observations.drops, 1);
    }

    #[test]
    fn pending_old_generation_cannot_consume_replacement() {
        let old_observations = Rc::new(RefCell::new(SourceObservations::default()));
        let new_observations = Rc::new(RefCell::new(SourceObservations::default()));
        let (old_completion, old_future) = manual_future::<Result<HttpHeaderEvent, HttpError>>();
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&old_observations)).with_header_future(old_future),
        ));
        let mut old_read = holder.read_header_event();
        assert!(matches!(poll_once(&mut old_read), Poll::Pending));

        holder.set_source(Box::new(
            ScriptedSource::new(Rc::clone(&new_observations))
                .with_header(Ok(HttpHeaderEvent::new(response(200), false).unwrap())),
        ));
        assert_eq!(old_observations.borrow().stops, 1);
        assert_eq!(old_observations.borrow().drops, 1);

        old_completion.complete(Ok(HttpHeaderEvent::new(request(), true).unwrap()));
        let Poll::Ready(Ok(stale_terminal)) = poll_once(&mut old_read) else {
            panic!("old source did not complete with its terminal header");
        };
        assert!(stale_terminal.eom());
        assert!(holder.readable());
        assert_eq!(new_observations.borrow().drops, 0);

        let Poll::Ready(Ok(new_header)) = poll_once(&mut holder.read_header_event()) else {
            panic!("replacement source was consumed by a stale completion");
        };
        assert!(!new_header.eom());
        drop(holder);
        assert_eq!(new_observations.borrow().stops, 1);
        assert_eq!(new_observations.borrow().drops, 1);
    }

    #[test]
    fn concurrent_read_is_rejected_without_consuming_source() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let (completion, pending) = manual_future::<Result<HttpHeaderEvent, HttpError>>();
        let (next_completion, next_pending) = manual_future::<Result<HttpHeaderEvent, HttpError>>();
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations))
                .with_header_future(pending)
                .with_header_future(next_pending),
        ));
        let mut first = holder.read_header_event();
        assert!(matches!(poll_once(&mut first), Poll::Pending));

        let second = match ready(holder.read_header_event()) {
            Ok(_) => panic!("concurrent read unexpectedly reached the source"),
            Err(error) => error,
        };
        assert_eq!(second.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        assert_eq!(second.message(), b"HTTP source already has an active read");
        assert!(holder.readable());

        completion.complete(Ok(HttpHeaderEvent::new(response(103), false).unwrap()));
        assert!(matches!(poll_once(&mut first), Poll::Ready(Ok(_))));
        let mut next = holder.read_header_event();
        assert!(matches!(poll_once(&mut next), Poll::Pending));
        drop(first);
        let Err(still_active) = ready(holder.read_body_event(1)) else {
            panic!("completed read drop released its successor");
        };
        assert_eq!(still_active.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        next_completion.complete(Ok(HttpHeaderEvent::new(response(200), false).unwrap()));
        assert!(matches!(poll_once(&mut next), Poll::Ready(Ok(_))));
    }

    #[test]
    fn unboxed_reads_admit_eagerly_without_retaining_the_holder_borrow() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations))
                .with_header(Ok(HttpHeaderEvent::new(response(200), false).unwrap()))
                .with_body(Ok(body(b"body", true))),
        ));
        let header = holder.read_header_event_unboxed();
        assert!(matches!(
            holder.release(),
            Err(SourceReleaseError::ReadInProgress)
        ));
        let mut rejected = holder.read_body_event_unboxed(7);
        assert!(observations.borrow().body_maximums.is_empty());
        let mut context = Context::from_waker(Waker::noop());
        let Poll::Ready(Err(error)) = Pin::new(&mut rejected).poll(&mut context) else {
            panic!("an unpolled header read must exclude a concurrent body read");
        };
        assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        drop(header);

        let mut body_read = holder.read_body_event_unboxed(7);
        assert_eq!(observations.borrow().body_maximums, [7]);
        assert!(matches!(
            holder.release(),
            Err(SourceReleaseError::ReadInProgress)
        ));
        let Poll::Ready(Ok(event)) = Pin::new(&mut body_read).poll(&mut context) else {
            panic!("dropping the unpolled read must release admission");
        };
        assert!(event.eom());
        assert!(!holder.readable());
        assert_eq!(observations.borrow().stops, 0);
        assert_eq!(observations.borrow().drops, 1);
    }

    #[test]
    fn dropping_pending_read_releases_single_consumer_slot() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let (_completion, pending) = manual_future::<Result<HttpHeaderEvent, HttpError>>();
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations))
                .with_header_future(pending)
                .with_header(Ok(HttpHeaderEvent::new(response(200), false).unwrap())),
        ));
        let mut abandoned = holder.read_header_event();
        assert!(matches!(poll_once(&mut abandoned), Poll::Pending));
        drop(abandoned);

        let Poll::Ready(Ok(retry)) = poll_once(&mut holder.read_header_event()) else {
            panic!("dropping the pending read did not release source admission");
        };
        assert!(!retry.eom());
        assert_eq!(observations.borrow().stops, 0);
    }

    #[test]
    fn inner_read_drop_can_admit_another_read() {
        struct OnDrop(Option<Box<dyn FnOnce()>>);

        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.take().unwrap()();
            }
        }

        let holder = Rc::new(RefCell::new(HttpSourceHolder::empty()));
        let admitted = Rc::new(RefCell::new(None));
        let holder_weak = Rc::downgrade(&holder);
        let admitted_on_drop = Rc::clone(&admitted);
        let guard = OnDrop(Some(Box::new(move || {
            let holder = holder_weak.upgrade().unwrap();
            let read = holder.borrow_mut().read_header_event();
            assert!(admitted_on_drop.borrow_mut().replace(read).is_none());
        })));
        let first_pending = Box::pin(async move {
            let _guard = guard;
            std::future::pending::<Result<HttpHeaderEvent, HttpError>>().await
        });
        let (completion, pending) = manual_future::<Result<HttpHeaderEvent, HttpError>>();
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        holder.borrow_mut().set_source(Box::new(
            ScriptedSource::new(Rc::clone(&observations))
                .with_header_future(first_pending)
                .with_header_future(pending),
        ));
        let mut first = holder.borrow_mut().read_header_event();
        assert!(matches!(poll_once(&mut first), Poll::Pending));
        drop(first);

        let mut next = admitted
            .borrow_mut()
            .take()
            .expect("inner drop admitted a read");
        assert!(matches!(poll_once(&mut next), Poll::Pending));
        let Err(still_active) = ready(holder.borrow_mut().read_body_event(1)) else {
            panic!("old wrapper released the reentrant read");
        };
        assert_eq!(still_active.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        completion.complete(Ok(HttpHeaderEvent::new(response(200), true).unwrap()));
        assert!(matches!(poll_once(&mut next), Poll::Ready(Ok(_))));
        assert!(!holder.borrow().readable());
        assert_eq!(observations.borrow().stops, 0);
        assert_eq!(observations.borrow().drops, 1);
    }

    #[test]
    fn stale_read_does_not_release_replacement_active_read() {
        for stale_result in [
            Some(Ok(HttpHeaderEvent::new(request(), true).unwrap())),
            Some(Err(HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR))),
            None,
        ] {
            let old_observations = Rc::new(RefCell::new(SourceObservations::default()));
            let new_observations = Rc::new(RefCell::new(SourceObservations::default()));
            let (old_completion, old_pending) =
                manual_future::<Result<HttpHeaderEvent, HttpError>>();
            let (new_completion, new_pending) =
                manual_future::<Result<HttpHeaderEvent, HttpError>>();
            let mut holder = HttpSourceHolder::new(Box::new(
                ScriptedSource::new(Rc::clone(&old_observations)).with_header_future(old_pending),
            ));
            let mut old_read = holder.read_header_event();
            assert!(matches!(poll_once(&mut old_read), Poll::Pending));

            holder.set_source(Box::new(
                ScriptedSource::new(Rc::clone(&new_observations)).with_header_future(new_pending),
            ));
            let mut new_read = holder.read_header_event();
            assert!(matches!(poll_once(&mut new_read), Poll::Pending));

            if let Some(result) = stale_result {
                let expected_error = result.is_err();
                old_completion.complete(result);
                let Poll::Ready(result) = poll_once(&mut old_read) else {
                    panic!("stale read did not complete");
                };
                assert_eq!(result.is_err(), expected_error);
            } else {
                drop(old_read);
            }
            let still_active = match ready(holder.read_body_event(1)) {
                Ok(_) => panic!("stale read released the replacement read"),
                Err(error) => error,
            };
            assert_eq!(still_active.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(new_observations.borrow().drops, 0);

            new_completion.complete(Ok(HttpHeaderEvent::new(request(), true).unwrap()));
            assert!(matches!(poll_once(&mut new_read), Poll::Ready(Ok(_))));
            assert!(!holder.readable());
            assert_eq!(new_observations.borrow().stops, 0);
            assert_eq!(new_observations.borrow().drops, 1);
        }
    }

    #[test]
    fn terminal_source_destructor_can_reenter_holder_state() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let reentrant_readable = Rc::new(Cell::new(None));
        let mut holder = HttpSourceHolder::empty();
        let probe = holder.readable_probe();
        let callback_result = Rc::clone(&reentrant_readable);
        holder.set_source(Box::new(
            ScriptedSource::new(Rc::clone(&observations))
                .with_header(Ok(HttpHeaderEvent::new(request(), true).unwrap()))
                .with_drop_callback(move || callback_result.set(Some(probe()))),
        ));

        assert!(ready(holder.read_header_event()).unwrap().eom());
        assert_eq!(reentrant_readable.get(), Some(false));
        assert_eq!(observations.borrow().drops, 1);
        assert_eq!(observations.borrow().stops, 0);
    }

    #[test]
    fn release_rejects_pending_read_without_changing_ownership() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let (completion, pending) = manual_future::<Result<HttpHeaderEvent, HttpError>>();
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations)).with_header_future(pending),
        ));
        let mut read = holder.read_header_event();
        assert!(matches!(poll_once(&mut read), Poll::Pending));

        assert!(matches!(
            holder.release(),
            Err(SourceReleaseError::ReadInProgress)
        ));
        assert!(holder.readable());
        assert_eq!(observations.borrow().stops, 0);
        assert_eq!(observations.borrow().drops, 0);

        completion.complete(Ok(HttpHeaderEvent::new(response(200), false).unwrap()));
        assert!(matches!(poll_once(&mut read), Poll::Ready(Ok(_))));
        assert!(holder.readable());
    }

    #[test]
    fn dropping_pending_read_allows_successful_release() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let (_completion, pending) = manual_future::<Result<HttpHeaderEvent, HttpError>>();
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations)).with_header_future(pending),
        ));
        let mut read = holder.read_header_event();
        assert!(matches!(poll_once(&mut read), Poll::Pending));
        drop(read);

        let mut released = holder.release().unwrap().unwrap();
        assert!(!holder.readable());
        assert_eq!(observations.borrow().stops, 0);
        assert_eq!(observations.borrow().drops, 0);
        released.stop_reading(None);
        drop(released);
        assert_eq!(observations.borrow().stops, 1);
        assert_eq!(observations.borrow().drops, 1);
    }

    #[test]
    fn successful_release_transfers_without_stop_or_early_drop() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let mut holder =
            HttpSourceHolder::new(Box::new(ScriptedSource::new(Rc::clone(&observations))));

        let mut released = holder.release().unwrap().unwrap();
        assert!(!holder.readable());
        assert_eq!(observations.borrow().stops, 0);
        assert_eq!(observations.borrow().drops, 0);
        assert!(holder.release().unwrap().is_none());
        drop(holder);
        assert_eq!(observations.borrow().stops, 0);
        assert_eq!(observations.borrow().drops, 0);

        released.stop_reading(None);
        drop(released);
        assert_eq!(observations.borrow().stops, 1);
        assert_eq!(observations.borrow().drops, 1);
    }
}

/// A holder read that panics consumes its source generation without stopping it.
#[cfg(panic = "unwind")]
mod panicking_reads {
    use std::marker::PhantomData;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;

    use super::*;

    type Lifecycle = Rc<RefCell<Vec<&'static str>>>;

    #[derive(Clone, Copy, Debug)]
    enum ReadPanic {
        /// `read_header_event` or `read_body_event` panics.
        Start,
        /// `polls_body_events`, which a body read calls first, panics.
        PollsBodyEvents,
        /// `polls_header_events`, which a header read calls first, panics.
        PollsHeaderEvents,
        /// The future the read returned panics when polled.
        Poll,
        /// The source serves header reads in place, and `poll_header_event` panics.
        HeaderPoll,
    }

    struct DropProbe(Lifecycle, &'static str);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.borrow_mut().push(self.1);
        }
    }

    struct PanickingRead<T> {
        _probe: DropProbe,
        output: PhantomData<T>,
    }

    impl<T> Future for PanickingRead<T> {
        type Output = Result<T, HttpError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            panic_any("read poll")
        }
    }

    struct PanickingSource {
        lifecycle: Lifecycle,
        panic: ReadPanic,
        drop_panics: bool,
    }

    impl PanickingSource {
        fn read<T: 'static>(&mut self) -> LocalSourceFuture<Result<T, HttpError>> {
            if matches!(self.panic, ReadPanic::Start) {
                panic_any("read start");
            }
            Box::pin(PanickingRead {
                _probe: DropProbe(Rc::clone(&self.lifecycle), "read.drop"),
                output: PhantomData,
            })
        }
    }

    impl HttpSource for PanickingSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.read()
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.read()
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
            self.lifecycle.borrow_mut().push("source.stop");
        }

        fn polls_body_events(&self) -> bool {
            if matches!(self.panic, ReadPanic::PollsBodyEvents) {
                panic_any("polls body events");
            }
            false
        }

        fn polls_header_events(&self) -> bool {
            match self.panic {
                ReadPanic::PollsHeaderEvents => panic_any("polls header events"),
                ReadPanic::HeaderPoll => true,
                _ => false,
            }
        }

        fn poll_header_event(
            &mut self,
            _context: &mut Context<'_>,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
            panic_any("header poll")
        }
    }

    impl Drop for PanickingSource {
        fn drop(&mut self) {
            self.lifecycle.borrow_mut().push("source.drop");
            if self.drop_panics {
                panic_any("source drop");
            }
        }
    }

    fn panicking_holder(panic: ReadPanic, drop_panics: bool) -> (HttpSourceHolder, Lifecycle) {
        let lifecycle = Lifecycle::default();
        let holder = HttpSourceHolder::new(Box::new(PanickingSource {
            lifecycle: Rc::clone(&lifecycle),
            panic,
            drop_panics,
        }));
        (holder, lifecycle)
    }

    fn panic_message(operation: impl FnOnce()) -> &'static str {
        let payload =
            catch_unwind(AssertUnwindSafe(operation)).expect_err("the source's panic continues");
        payload
            .downcast_ref::<&'static str>()
            .copied()
            .expect("a static message")
    }

    /// Polls `read`, which panics, and checks that the poisoned future was
    /// dropped before its source while the read is still held.
    fn panic_in_poll<T>(mut read: LocalSourceFuture<T>, lifecycle: &Lifecycle) -> &'static str {
        let message = panic_message(|| drop(poll_once(&mut read)));
        assert_eq!(*lifecycle.borrow(), ["read.drop", "source.drop"]);
        drop(read);
        message
    }

    fn start(holder: &mut HttpSourceHolder, header: bool) {
        if header {
            drop(holder.read_header_event());
        } else {
            drop(holder.read_body_event(16));
        }
    }

    fn assert_consumed(holder: &mut HttpSourceHolder) {
        assert!(!holder.readable());
        let Err(error) = ready(holder.read_body_event(16)) else {
            panic!("a consumed holder has nothing to read");
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
    }

    #[test]
    fn a_panicking_read_retires_its_source_without_stop_reading() {
        for header in [false, true] {
            let (mut holder, lifecycle) = panicking_holder(ReadPanic::Poll, false);
            let message = if header {
                panic_in_poll(holder.read_header_event(), &lifecycle)
            } else {
                panic_in_poll(holder.read_body_event(16), &lifecycle)
            };
            assert_eq!(message, "read poll", "header {header}");
            assert_consumed(&mut holder);
            drop(holder);
            assert_eq!(
                *lifecycle.borrow(),
                ["read.drop", "source.drop"],
                "header {header}"
            );
        }
    }

    #[test]
    fn a_panicking_read_start_retires_its_source() {
        for (panic, header, expected) in [
            (ReadPanic::Start, true, "read start"),
            (ReadPanic::Start, false, "read start"),
            (ReadPanic::PollsBodyEvents, false, "polls body events"),
            (ReadPanic::PollsHeaderEvents, true, "polls header events"),
        ] {
            let (mut holder, lifecycle) = panicking_holder(panic, false);
            let message = panic_message(|| start(&mut holder, header));
            assert_eq!(message, expected, "{panic:?}, header {header}");
            assert_eq!(
                *lifecycle.borrow(),
                ["source.drop"],
                "{panic:?}, header {header}"
            );
            assert_consumed(&mut holder);
            drop(holder);
            assert_eq!(
                *lifecycle.borrow(),
                ["source.drop"],
                "{panic:?}, header {header}"
            );
        }
    }

    /// An in-place header read whose source poll panics consumes the source in
    /// the same way. The read has no future of its own to drop.
    #[test]
    fn a_panicking_in_place_header_poll_retires_its_source_without_stop_reading() {
        let (mut holder, lifecycle) = panicking_holder(ReadPanic::HeaderPoll, false);
        let mut read = holder.read_header_event();
        assert_eq!(panic_message(|| drop(poll_once(&mut read))), "header poll");
        assert_eq!(*lifecycle.borrow(), ["source.drop"]);
        drop(read);
        assert_consumed(&mut holder);
        drop(holder);
        assert_eq!(*lifecycle.borrow(), ["source.drop"]);
    }

    /// Calling `polls_body_events` on the holder itself outside a read, as an
    /// owned exchange does before its body read or an outer holder does on a
    /// nested holder when a body read starts, consumes the source without
    /// stopping it when the source's `polls_body_events` panics.
    #[test]
    fn a_panicking_polls_body_events_asked_of_the_holder_retires_its_source() {
        for nested in [false, true] {
            let (holder, lifecycle) = panicking_holder(ReadPanic::PollsBodyEvents, false);
            let mut holder = if nested {
                HttpSourceHolder::new(Box::new(holder))
            } else {
                holder
            };
            let message = panic_message(|| {
                if nested {
                    start(&mut holder, false);
                } else {
                    HttpSource::polls_body_events(&holder);
                }
            });
            assert_eq!(message, "polls body events", "nested {nested}");
            assert_eq!(*lifecycle.borrow(), ["source.drop"], "nested {nested}");
            assert_consumed(&mut holder);
            drop(holder);
            assert_eq!(*lifecycle.borrow(), ["source.drop"], "nested {nested}");
        }
    }

    /// A source whose drop also panics while it is retired keeps the
    /// read's panic, and the process does not abort.
    #[test]
    fn a_panic_while_retiring_a_panicked_source_keeps_the_first() {
        let (mut holder, lifecycle) = panicking_holder(ReadPanic::Poll, true);
        assert_eq!(
            panic_in_poll(holder.read_body_event(16), &lifecycle),
            "read poll"
        );
        assert_consumed(&mut holder);

        let (mut holder, lifecycle) = panicking_holder(ReadPanic::Start, true);
        assert_eq!(panic_message(|| start(&mut holder, false)), "read start");
        assert_eq!(*lifecycle.borrow(), ["source.drop"]);
        assert_consumed(&mut holder);
    }
}

mod events {
    use super::*;

    #[test]
    fn move_replaces_terminal_trailers_without_double_cleanup() {
        let callback = Rc::new(CancellationCounter::default());
        let mut trailer_event = HttpBodyEvent::trailers(HttpHeaders::new());
        trailer_event
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                Some(StreamId::new(1)),
                crate::byte_event::ByteEventType::TransportWrite.bit(),
                &callback,
            ));

        trailer_event = body(b"", true);
        assert_eq!(callback.0.get(), 1);
        assert_eq!(trailer_event.event_type(), HttpBodyEventType::Body);
        assert!(trailer_event.eom());
        drop(trailer_event);
        assert_eq!(callback.0.get(), 1);
    }

    #[test]
    fn into_kind_drops_only_registration_storage_the_event_still_owns() {
        let callback = Rc::new(CancellationCounter::default());
        let registration = || {
            HttpByteEventRegistration::new(
                Some(StreamId::new(1)),
                crate::byte_event::ByteEventType::TransportWrite.bit(),
                &callback,
            )
        };
        take_registration_drops_for_test();

        // Without registrations, consuming the event skips the registration drop.
        assert!(matches!(
            body(b"a", false).into_kind(),
            HttpBodyEventKind::Body(_)
        ));
        assert_eq!(take_registration_drops_for_test(), 0);

        // Taking the registrations empties the event, and the taker still owns them.
        let mut taken = body(b"b", false);
        taken.byte_event_registrations_mut().push(registration());
        let kept = taken.take_byte_event_registrations();
        assert!(matches!(taken.into_kind(), HttpBodyEventKind::Body(_)));
        assert_eq!(take_registration_drops_for_test(), 0);
        assert_eq!(callback.0.get(), 0, "taken registrations stay pending");

        // Owned registrations are cancelled once when the event is consumed.
        let mut owned = body(b"c", true);
        owned.byte_event_registrations_mut().push(registration());
        assert!(matches!(owned.into_kind(), HttpBodyEventKind::Body(_)));
        assert_eq!(take_registration_drops_for_test(), 1);
        assert_eq!(callback.0.get(), 1);

        // Emptied storage that kept its capacity is still freed.
        let mut emptied = body(b"d", false);
        emptied.byte_event_registrations_mut().push(registration());
        drop(emptied.byte_event_registrations_mut().pop());
        assert_eq!(callback.0.get(), 2);
        assert!(matches!(emptied.into_kind(), HttpBodyEventKind::Body(_)));
        assert_eq!(take_registration_drops_for_test(), 1);
        assert_eq!(
            callback.0.get(),
            2,
            "an emptied vector cancels nothing more"
        );

        drop(kept);
        assert_eq!(callback.0.get(), 3);
    }
}

mod source_events {
    use super::*;

    #[test]
    fn unclaimed_push_source_stops_exactly_once() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        {
            let event = HttpBodyEvent::push_promise(
                request(),
                Some(Box::new(ScriptedSource::new(Rc::clone(&observations)))),
                false,
            );
            assert_eq!(event.event_type(), HttpBodyEventType::PushPromise);
        }
        assert_eq!(observations.borrow().stops, 1);
        assert_eq!(observations.borrow().drops, 1);

        let claimed_observations = Rc::new(RefCell::new(SourceObservations::default()));
        let mut event = HttpBodyEvent::push_promise(
            request(),
            Some(Box::new(ScriptedSource::new(Rc::clone(
                &claimed_observations,
            )))),
            false,
        );
        let claimed = event
            .push_promise_mut()
            .unwrap()
            .move_push_source()
            .unwrap();
        drop(event);
        assert_eq!(claimed_observations.borrow().stops, 0);
        drop(claimed);
        assert_eq!(claimed_observations.borrow().stops, 1);
        assert_eq!(claimed_observations.borrow().drops, 1);
    }

    #[test]
    fn terminal_holder_drop_has_no_self_delete_path() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let terminal = HttpHeaderEvent::new(request(), true).unwrap();
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations)).with_header(Ok(terminal)),
        ));

        assert!(ready(holder.read_header_event()).unwrap().eom());
        assert!(!holder.readable());
        assert_eq!(observations.borrow().drops, 1);
        assert_eq!(observations.borrow().stops, 0);
        drop(holder);
        assert_eq!(observations.borrow().drops, 1);
        assert_eq!(observations.borrow().stops, 0);

        let error_observations = Rc::new(RefCell::new(SourceObservations::default()));
        let mut error_holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&error_observations)).with_body(Err(
                HttpError::with_message(HttpErrorCode::TRANSPORT_EOF, b"eof"),
            )),
        ));
        let error = match ready(error_holder.read_body_event(u32::MAX)) {
            Ok(_) => panic!("terminal source error unexpectedly produced a body event"),
            Err(error) => error,
        };
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(!error_holder.readable());
        assert_eq!(error_observations.borrow().drops, 1);
        assert_eq!(error_observations.borrow().stops, 0);
        let later = match ready(error_holder.read_header_event()) {
            Ok(_) => panic!("consumed holder unexpectedly produced another event"),
            Err(error) => error,
        };
        assert_eq!(later.code(), HttpErrorCode::DROPPED);
        assert_eq!(error_observations.borrow().drops, 1);
    }

    #[test]
    fn consuming_header_moves_message_and_retires_metadata_once() {
        struct CallbackCapture {
            cancellations: Rc<CancellationCounter>,
            observed: Rc<RefCell<Vec<usize>>>,
        }

        impl Drop for CallbackCapture {
            fn drop(&mut self) {
                self.observed.borrow_mut().push(self.cancellations.0.get());
            }
        }

        let cancellations = Rc::new(CancellationCounter::default());
        let observed = Rc::new(RefCell::new(Vec::new()));
        let encoded_calls = Rc::new(Cell::new(0));
        let capture = CallbackCapture {
            cancellations: Rc::clone(&cancellations),
            observed: Rc::clone(&observed),
        };
        let mut event = HttpHeaderEvent::new(response(200), false).unwrap();
        event.headers_mut().set_status_code(201).unwrap();
        event
            .headers_mut()
            .headers_mut()
            .add(b"x-moved", b"retained");
        event
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                Some(StreamId::new(1)),
                crate::byte_event::ByteEventType::TransportWrite.bit(),
                &cancellations,
            ));
        let callback_calls = Rc::clone(&encoded_calls);
        event.set_egress_headers_callback(move |_| {
            callback_calls.set(callback_calls.get() + 1);
            drop(capture);
        });
        assert_eq!(cancellations.0.get(), 0);
        assert!(observed.borrow().is_empty());

        let moved = event;
        let mut message = moved.into_headers();
        assert_eq!(message.status_code(), Ok(201));
        assert_eq!(
            message.headers().get_single_or_empty(b"x-moved"),
            b"retained"
        );
        assert_eq!(cancellations.0.get(), 1);
        assert_eq!(&*observed.borrow(), &[1]);
        assert_eq!(encoded_calls.get(), 0);

        drop(message);
        assert_eq!(cancellations.0.get(), 1);
        assert_eq!(&*observed.borrow(), &[1]);
        assert_eq!(encoded_calls.get(), 0);
    }

    #[test]
    fn header_size_callback_moves_with_event() {
        let sizes = Rc::new(RefCell::new(Vec::new()));
        let mut original = HttpHeaderEvent::new(response(200), false).unwrap();
        let callback_sizes = Rc::clone(&sizes);
        original.set_egress_headers_callback(move |size| {
            callback_sizes.borrow_mut().push(size);
        });

        let mut moved = original;
        assert!(moved.has_egress_headers_callback());
        assert!(moved.on_headers_encoded(HttpHeaderSize {
            compressed: 17,
            uncompressed: 29,
            compressed_block: 13,
        }));
        assert_eq!(sizes.borrow().len(), 1);
    }

    #[test]
    fn header_size_callback_observes_post_codec_size() {
        let observed = Rc::new(RefCell::new(None));
        let mut event = HttpHeaderEvent::new(response(200), false).unwrap();
        let callback_observed = Rc::clone(&observed);
        event.set_egress_headers_callback(move |size| {
            *callback_observed.borrow_mut() = Some(size);
        });

        let serialized_size = HttpHeaderSize {
            compressed: 41,
            uncompressed: 113,
            compressed_block: 37,
        };
        assert!(event.on_headers_encoded(serialized_size));
        assert_eq!(*observed.borrow(), Some(serialized_size));
    }

    #[test]
    fn header_size_callback_fires_exactly_once() {
        let calls = Rc::new(Cell::new(0));
        let mut event = HttpHeaderEvent::new(response(204), true).unwrap();
        let callback_calls = Rc::clone(&calls);
        event.set_egress_headers_callback(move |_| callback_calls.set(callback_calls.get() + 1));

        assert!(event.on_headers_encoded(HttpHeaderSize::default()));
        assert!(!event.on_headers_encoded(HttpHeaderSize::default()));
        drop(event);
        assert_eq!(calls.get(), 1);

        let cancelled_calls = Rc::new(Cell::new(0));
        let mut cancelled_event = HttpHeaderEvent::new(response(400), true).unwrap();
        let callback_calls = Rc::clone(&cancelled_calls);
        cancelled_event
            .set_egress_headers_callback(move |_| callback_calls.set(callback_calls.get() + 1));
        assert!(cancelled_event.cancel_egress_headers_callback());
        assert!(!cancelled_event.cancel_egress_headers_callback());
        assert!(!cancelled_event.on_headers_encoded(HttpHeaderSize::default()));
        assert_eq!(cancelled_calls.get(), 0);

        let dropped_calls = Rc::new(Cell::new(0));
        let mut dropped_event = HttpHeaderEvent::new(response(500), true).unwrap();
        let callback_calls = Rc::clone(&dropped_calls);
        dropped_event
            .set_egress_headers_callback(move |_| callback_calls.set(callback_calls.get() + 1));
        drop(dropped_event);
        assert_eq!(dropped_calls.get(), 0);
    }

    #[test]
    fn header_size_callback_runs_on_owner_worker() {
        let owner = std::thread::current().id();
        let observed = Rc::new(RefCell::new(None::<ThreadId>));
        let mut event = HttpHeaderEvent::new(response(200), false).unwrap();
        let callback_observed = Rc::clone(&observed);
        event.set_egress_headers_callback(move |_| {
            *callback_observed.borrow_mut() = Some(std::thread::current().id());
        });

        assert!(event.on_headers_encoded(HttpHeaderSize::default()));
        assert_eq!(*observed.borrow(), Some(owner));
    }

    #[test]
    fn header_encoded_callback_runs_once_on_owner() {
        let owner = std::thread::current().id();
        let observations = Rc::new(RefCell::new(Vec::new()));
        let mut event = HttpHeaderEvent::new(response(200), false).unwrap();
        let callback_observations = Rc::clone(&observations);
        event.set_egress_headers_callback(move |size| {
            callback_observations
                .borrow_mut()
                .push((std::thread::current().id(), size));
        });

        let post_codec = HttpHeaderSize {
            compressed: 7,
            uncompressed: 19,
            compressed_block: 5,
        };
        assert!(event.on_headers_encoded(post_codec));
        assert!(!event.on_headers_encoded(HttpHeaderSize::default()));
        assert_eq!(observations.borrow().as_slice(), &[(owner, post_codec)]);
    }

    #[test]
    fn common_eom_covers_all_legal_terminal_kinds() {
        let terminal_events = [
            body(b"a", true),
            HttpBodyEvent::push_promise(request(), None, true),
            HttpBodyEvent::trailers(HttpHeaders::new()),
            HttpBodyEvent::padding(9, true),
            HttpBodyEvent::upgrade(true),
        ];
        assert!(terminal_events.iter().all(HttpBodyEvent::eom));
        assert_eq!(
            terminal_events.map(|event| event.event_type()),
            [
                HttpBodyEventType::Body,
                HttpBodyEventType::PushPromise,
                HttpBodyEventType::Trailers,
                HttpBodyEventType::Padding,
                HttpBodyEventType::Upgrade,
            ]
        );

        let suspend = HttpBodyEvent::suspend(Box::pin(std::future::ready(ResumeStatus::Signalled)));
        assert!(!suspend.eom());
        assert_eq!(suspend.event_type(), HttpBodyEventType::Suspend);
        assert_eq!(HttpBodyEventType::Upgrade as u8, 0);
        assert_eq!(HttpBodyEventType::Body as u8, 1);
        assert_eq!(HttpBodyEventType::PushPromise as u8, 3);
        assert_eq!(HttpBodyEventType::Trailers as u8, 4);
        assert_eq!(HttpBodyEventType::Suspend as u8, 5);
        assert_eq!(HttpBodyEventType::Padding as u8, 6);
        assert_eq!(ResumeStatus::NotReady as u8, 0);
        assert_eq!(ResumeStatus::Signalled as u8, 1);
        assert_eq!(ResumeStatus::TimedOut as u8, 2);
        assert_eq!(ResumeStatus::Cancelled as u8, 3);
    }

    #[test]
    fn informational_headers_precede_one_final_header() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let informational = HttpHeaderEvent::new(response(103), false).unwrap();
        let final_headers = HttpHeaderEvent::new(response(200), false).unwrap();
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations))
                .with_header(Ok(informational))
                .with_header(Ok(final_headers)),
        ));

        let mut first = ready(holder.read_header_event()).unwrap();
        assert!(!first.is_final().unwrap());
        assert!(!first.eom());
        assert!(holder.readable());
        let mut second = ready(holder.read_header_event()).unwrap();
        assert!(second.is_final().unwrap());
        assert!(!second.eom());
        assert!(holder.readable());
        drop(holder);
        assert_eq!(observations.borrow().stops, 1);
    }

    #[test]
    fn informational_header_cannot_carry_eom() {
        let error = HttpHeaderEvent::new(response(103), true).err().unwrap();
        assert_eq!(error, HeaderEventError::InformationalEom);
    }

    #[test]
    fn maximum_and_non_body_event_are_forwarded_intact() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&observations))
                .with_body(Ok(HttpBodyEvent::padding(u16::MAX, false))),
        ));

        let event = ready(holder.read_body_event(1)).unwrap();
        assert_eq!(observations.borrow().body_maximums, [1]);
        assert_eq!(event.event_type(), HttpBodyEventType::Padding);
        let HttpBodyEventKind::Padding(size) = event.kind() else {
            panic!("padding event changed kind");
        };
        assert_eq!(*size, u16::MAX);
    }

    #[test]
    fn timeout_stream_id_stop_error_and_replacement_forward_once() {
        let first = Rc::new(RefCell::new(SourceObservations::default()));
        let second = Rc::new(RefCell::new(SourceObservations::default()));
        let mut holder = HttpSourceHolder::new(Box::new(
            ScriptedSource::new(Rc::clone(&first)).with_stream_id(StreamId::new(77)),
        ));
        assert_eq!(holder.get_stream_id(), Some(StreamId::new(77)));
        holder.set_read_timeout(Duration::from_millis(250));
        assert_eq!(first.borrow().timeouts, [Duration::from_millis(250)]);

        holder.set_source(Box::new(
            ScriptedSource::new(Rc::clone(&second)).with_stream_id(StreamId::new(88)),
        ));
        assert_eq!(first.borrow().stops, 1);
        assert_eq!(first.borrow().drops, 1);
        assert_eq!(holder.get_stream_id(), Some(StreamId::new(88)));
        holder.stop_reading(Some(HttpErrorCode::CANCEL));
        assert_eq!(second.borrow().stops, 1);
        assert_eq!(second.borrow().stop_errors, [Some(HttpErrorCode::CANCEL)]);
        assert_eq!(second.borrow().drops, 1);
        drop(holder);
        assert_eq!(second.borrow().stops, 1);

        let released_observations = Rc::new(RefCell::new(SourceObservations::default()));
        let mut released_holder = HttpSourceHolder::new(Box::new(ScriptedSource::new(Rc::clone(
            &released_observations,
        ))));
        let mut released = released_holder.release().unwrap().unwrap();
        assert!(!released_holder.readable());
        drop(released_holder);
        assert_eq!(released_observations.borrow().stops, 0);
        released.stop_reading(None);
        drop(released);
        assert_eq!(released_observations.borrow().stops, 1);
        assert_eq!(released_observations.borrow().drops, 1);
    }

    #[test]
    fn body_accessor_preserves_segments_and_non_body_kinds() {
        let mut event = body(b"abcdef", false);
        let prefix = crate::events::as_body_ev(&mut event)
            .unwrap()
            .split_at_most(2);
        assert_eq!(prefix.coalesce(2).unwrap().as_ref(), b"ab");
        assert_eq!(
            event.as_body().unwrap().coalesce(4).unwrap().as_ref(),
            b"cdef"
        );
        assert!(crate::events::as_body_ev(&mut HttpBodyEvent::padding(1, false)).is_none());
    }
}
