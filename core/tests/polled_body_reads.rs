/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `HttpSource::poll_body_event` (`DIV-039`) through `HttpSourceHolder`.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;

use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_holder::HttpSourceHolder;
use crate::source_holder::SourceReleaseError;

#[derive(Default)]
struct Log {
    /// The `maximum` of every `poll_body_event` call.
    polls: RefCell<Vec<u32>>,
    /// `read_body_event` calls.
    reads: Cell<usize>,
    stops: RefCell<Vec<Option<HttpErrorCode>>>,
    drops: Cell<usize>,
}

/// Serves scripted body results, one per `poll_body_event` call (a `Pending`
/// entry answers one poll) or per `read_body_event` call.
struct ScriptedSource {
    log: Rc<Log>,
    polls_body_events: bool,
    script: VecDeque<Poll<Result<HttpBodyEvent, HttpError>>>,
    exhausted_panic: Option<&'static str>,
}

impl ScriptedSource {
    fn new(
        log: &Rc<Log>,
        polls_body_events: bool,
        script: impl IntoIterator<Item = Poll<Result<HttpBodyEvent, HttpError>>>,
    ) -> Box<Self> {
        Box::new(Self {
            log: Rc::clone(log),
            polls_body_events,
            script: script.into_iter().collect(),
            exhausted_panic: None,
        })
    }

    /// Makes the call after the last scripted result panic with `message`.
    #[cfg(panic = "unwind")]
    fn panicking_when_exhausted(mut self: Box<Self>, message: &'static str) -> Box<Self> {
        self.exhausted_panic = Some(message);
        self
    }

    fn next(&mut self) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if self.script.is_empty()
            && let Some(message) = self.exhausted_panic
        {
            panic_any(message);
        }
        self.script.pop_front().expect("a scripted body result")
    }
}

impl HttpSource for ScriptedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"body-only test source",
        ))))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.log.reads.set(self.log.reads.get() + 1);
        match self.next() {
            Poll::Ready(result) => Box::pin(std::future::ready(result)),
            Poll::Pending => Box::pin(std::future::pending()),
        }
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.log.stops.borrow_mut().push(error);
    }

    fn polls_body_events(&self) -> bool {
        self.polls_body_events
    }

    fn poll_body_event(
        &mut self,
        _context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.log.polls.borrow_mut().push(maximum);
        self.next()
    }
}

impl Drop for ScriptedSource {
    fn drop(&mut self) {
        self.log.drops.set(self.log.drops.get() + 1);
    }
}

fn body(bytes: &'static [u8], eom: bool) -> Poll<Result<HttpBodyEvent, HttpError>> {
    Poll::Ready(Ok(HttpBodyEvent::body(
        BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).expect("body fits"),
        eom,
    )))
}

fn poll<F: Future + Unpin>(read: &mut F) -> Poll<F::Output> {
    Pin::new(read).poll(&mut Context::from_waker(Waker::noop()))
}

fn bytes_of(event: &HttpBodyEvent) -> Vec<u8> {
    event
        .as_body()
        .expect("a body event")
        .segments()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}

#[test]
fn a_polling_source_serves_holder_body_reads_in_place() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(
        &log,
        true,
        [body(b"first", false), Poll::Pending, body(b"last", true)],
    ));

    let mut first = holder.read_body_event_unboxed(7);
    let Poll::Ready(Ok(event)) = poll(&mut first) else {
        panic!("the first read is ready");
    };
    assert_eq!(bytes_of(&event), b"first");
    assert!(!event.eom());

    let mut last = holder.read_body_event_unboxed(5);
    assert!(poll(&mut last).is_pending());
    assert_eq!(
        holder.release().err(),
        Some(SourceReleaseError::ReadInProgress),
        "a pending read keeps its admission"
    );
    let Poll::Ready(Ok(event)) = poll(&mut last) else {
        panic!("the second poll is ready");
    };
    assert_eq!(bytes_of(&event), b"last");
    assert!(event.eom());

    assert_eq!(
        *log.polls.borrow(),
        [7, 5, 5],
        "every poll reaches the source"
    );
    assert_eq!(log.reads.get(), 0, "no boxed read was made");
    assert!(!holder.readable(), "the terminal event consumed the source");
    assert_eq!(log.drops.get(), 1);
    assert!(log.stops.borrow().is_empty());
}

#[test]
fn a_source_that_does_not_poll_keeps_its_boxed_reads() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, false, [body(b"only", true)]));
    let mut read = holder.read_body_event_unboxed(7);
    let Poll::Ready(Ok(event)) = poll(&mut read) else {
        panic!("the boxed read is ready");
    };
    assert_eq!(bytes_of(&event), b"only");
    assert_eq!(log.reads.get(), 1);
    assert!(log.polls.borrow().is_empty());
}

#[test]
fn an_in_place_error_is_terminal() {
    let log = Rc::new(Log::default());
    let error = HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, b"failed");
    let mut holder =
        HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Ready(Err(error))]));
    let mut read = holder.read_body_event_unboxed(7);
    let Poll::Ready(Err(error)) = poll(&mut read) else {
        panic!("the error is returned");
    };
    assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
    assert_eq!(*log.polls.borrow(), [7]);
    assert_eq!(log.reads.get(), 0);
    assert!(!holder.readable());
    assert_eq!(log.drops.get(), 1);
    assert!(
        log.stops.borrow().is_empty(),
        "a finished source is not stopped"
    );
}

#[test]
fn a_pending_in_place_read_of_a_cleared_source_fails_without_polling_it() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    let mut read = holder.read_body_event_unboxed(7);
    assert!(poll(&mut read).is_pending());
    holder.clear(Some(HttpErrorCode::CANCEL));
    assert_eq!(*log.stops.borrow(), [Some(HttpErrorCode::CANCEL)]);
    assert_eq!(log.drops.get(), 1);
    let Poll::Ready(Err(error)) = poll(&mut read) else {
        panic!("the read of a cleared source fails");
    };
    assert_eq!(error.code(), HttpErrorCode::DROPPED);
    assert_eq!(
        *log.polls.borrow(),
        [7],
        "the cleared source is not polled again"
    );
}

#[test]
fn dropping_a_pending_in_place_read_releases_admission() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(
        &log,
        true,
        [Poll::Pending, body(b"after", true)],
    ));
    let mut abandoned = holder.read_body_event_unboxed(7);
    assert!(poll(&mut abandoned).is_pending());
    drop(abandoned);
    let mut read = holder.read_body_event_unboxed(9);
    let Poll::Ready(Ok(event)) = poll(&mut read) else {
        panic!("a new read is admitted and served");
    };
    assert_eq!(bytes_of(&event), b"after");
    assert_eq!(*log.polls.borrow(), [7, 9]);
}

#[test]
fn in_place_reads_reach_a_polling_source_through_a_holder_used_as_a_source() {
    let log = Rc::new(Log::default());
    let inner = HttpSourceHolder::new(ScriptedSource::new(
        &log,
        true,
        [Poll::Pending, body(b"through", true)],
    ));
    assert!(inner.polls_body_events());
    let mut outer = HttpSourceHolder::new(Box::new(inner));
    let mut read = outer.read_body_event_unboxed(7);
    assert!(poll(&mut read).is_pending());
    let Poll::Ready(Ok(event)) = poll(&mut read) else {
        panic!("the inner source's event arrives");
    };
    assert_eq!(bytes_of(&event), b"through");
    assert_eq!(*log.polls.borrow(), [7, 7]);
    assert_eq!(log.reads.get(), 0);
    assert_eq!(
        log.drops.get(),
        1,
        "the terminal event consumed the inner source"
    );
    assert!(!outer.readable());
}

#[test]
fn a_holder_with_an_active_read_refuses_to_be_polled_as_a_source() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    let mut read = holder.read_body_event_unboxed(7);
    assert!(poll(&mut read).is_pending());
    let Poll::Ready(Err(error)) =
        holder.poll_body_event(&mut Context::from_waker(Waker::noop()), 9)
    else {
        panic!("a second consumer is refused");
    };
    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    assert_eq!(*log.polls.borrow(), [7], "the source is not polled for it");
}

/// Starts a read of its own holder from inside its poll, and records the
/// error that read returns.
struct ReentrantSource {
    holder: Rc<RefCell<Option<HttpSourceHolder>>>,
    refused: Rc<Cell<Option<HttpErrorCode>>>,
}

impl HttpSource for ReentrantSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}

    fn polls_body_events(&self) -> bool {
        true
    }

    fn poll_body_event(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let mut nested = self
            .holder
            .borrow_mut()
            .as_mut()
            .expect("the holder is installed")
            .read_body_event_unboxed(1);
        let Poll::Ready(Err(error)) = poll(&mut nested) else {
            panic!("the nested read is refused at once");
        };
        self.refused.set(Some(error.code()));
        body(b"", true)
    }
}

#[test]
fn a_read_started_from_inside_an_in_place_poll_is_refused() {
    let slot = Rc::new(RefCell::new(None));
    let refused = Rc::new(Cell::new(None));
    let holder = HttpSourceHolder::new(Box::new(ReentrantSource {
        holder: Rc::clone(&slot),
        refused: Rc::clone(&refused),
    }));
    *slot.borrow_mut() = Some(holder);
    let mut read = slot
        .borrow_mut()
        .as_mut()
        .expect("the holder is installed")
        .read_body_event_unboxed(7);
    let Poll::Ready(Ok(event)) = poll(&mut read) else {
        panic!("the outer read completes");
    };
    assert!(event.eom());
    assert_eq!(refused.get(), Some(HttpErrorCode::INVALID_STATE_TRANSITION));
    drop(read);
    drop(slot.borrow_mut().take());
}

/// How a test makes a pending in-place read's source generation unavailable.
#[derive(Clone, Copy, Debug)]
enum Invalidation {
    Clear,
    StopReading,
    Replace,
    Destroy,
}

/// Awaits, on a Tokio executor, an in-place read of a source that returns
/// `Pending` without keeping the waker (as a source that drops its underlying
/// operation when stopped does), invalidates the source's generation with
/// `invalidation` once the read is pending, and returns the read's error code,
/// or `None` if the executor never polled the read again within a bound.
fn awaited_read_after(invalidation: Invalidation) -> (Option<HttpErrorCode>, Rc<Log>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let log = Rc::new(Log::default());
        let mut holder = Some(HttpSourceHolder::new(ScriptedSource::new(
            &log,
            true,
            [Poll::Pending],
        )));
        let read = holder
            .as_mut()
            .expect("the holder is live")
            .read_body_event_unboxed(7);
        let reader = tokio::task::spawn_local(read);
        while log.polls.borrow().is_empty() {
            tokio::task::yield_now().await;
        }
        let live = holder.as_mut().expect("the holder is live");
        match invalidation {
            Invalidation::Clear => live.clear(Some(HttpErrorCode::CANCEL)),
            Invalidation::StopReading => {
                HttpSource::stop_reading(live, Some(HttpErrorCode::CANCEL));
            }
            Invalidation::Replace => live.set_source(ScriptedSource::new(&log, true, [])),
            Invalidation::Destroy => drop(holder.take()),
        }
        let code = tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .ok()
            .map(|joined| {
                joined
                    .expect("the reader task finishes")
                    .err()
                    .expect("the read fails")
                    .code()
            });
        drop(holder);
        (code, log)
    })
}

/// A pending in-place read whose generation goes away is woken and fails
/// `DROPPED`, whether the generation is cleared, stopped, replaced or
/// destroyed; the executor alone drives it.
#[test]
fn a_pending_in_place_read_is_woken_when_its_source_generation_goes_away() {
    for invalidation in [
        Invalidation::Clear,
        Invalidation::StopReading,
        Invalidation::Replace,
        Invalidation::Destroy,
    ] {
        let (code, log) = awaited_read_after(invalidation);
        assert_eq!(
            code,
            Some(HttpErrorCode::DROPPED),
            "{invalidation:?}: the reader is woken and fails"
        );
        assert_eq!(
            *log.polls.borrow(),
            [7],
            "{invalidation:?}: the stopped source is not polled again"
        );
        assert_eq!(
            log.drops.get(),
            match invalidation {
                Invalidation::Replace => 2,
                _ => 1,
            },
            "{invalidation:?}: every source generation is dropped"
        );
    }
}

/// The same through a holder used as another holder's source: clearing the
/// outer holder wakes the outer read pending in place on the inner source.
#[test]
fn a_pending_in_place_read_through_a_holder_is_woken_when_the_outer_holder_clears() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let log = Rc::new(Log::default());
        let inner = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
        let mut outer = HttpSourceHolder::new(Box::new(inner));
        let reader = tokio::task::spawn_local(outer.read_body_event_unboxed(7));
        while log.polls.borrow().is_empty() {
            tokio::task::yield_now().await;
        }
        outer.clear(Some(HttpErrorCode::CANCEL));
        let error = tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .expect("the reader is woken")
            .expect("the reader task finishes")
            .err()
            .expect("the read fails");
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
        assert_eq!(*log.stops.borrow(), [Some(HttpErrorCode::CANCEL)]);
        assert_eq!(log.drops.get(), 1);
    });
}

/// How a test ends the generation of a holder whose poll as a source is pending.
#[derive(Clone, Copy, Debug)]
enum HolderInvalidation {
    Clear,
    StopReading,
    Replace,
    Release,
    Destroy,
}

impl HolderInvalidation {
    /// Ends the holder's generation; a replacement serves `b"replacement"`.
    fn apply(self, holder: &RefCell<Option<HttpSourceHolder>>, log: &Rc<Log>) {
        if let Self::Destroy = self {
            drop(holder.borrow_mut().take());
            return;
        }
        let mut holder = holder.borrow_mut();
        let holder = holder.as_mut().expect("the holder is live");
        match self {
            Self::Clear => holder.clear(Some(HttpErrorCode::CANCEL)),
            Self::StopReading => HttpSource::stop_reading(holder, Some(HttpErrorCode::CANCEL)),
            Self::Replace => {
                holder.set_source(ScriptedSource::new(log, true, [body(b"replacement", true)]));
            }
            Self::Release => {
                let released = holder.release().expect("no holder read is active");
                drop(released.expect("the holder had a source"));
            }
            Self::Destroy => unreachable!("handled above"),
        }
    }
}

/// Forwards a body poll to a holder that other code can also reach, as a
/// source wrapping a shared holder does.
struct Forwarding(Rc<RefCell<Option<HttpSourceHolder>>>);

impl HttpSource for Forwarding {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        if let Some(holder) = self.0.borrow_mut().as_mut() {
            HttpSource::stop_reading(holder, error);
        }
    }

    fn polls_body_events(&self) -> bool {
        true
    }

    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        match self.0.borrow_mut().as_mut() {
            Some(holder) => holder.poll_body_event(context, maximum),
            None => Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"the shared holder is gone",
            ))),
        }
    }
}

/// What a consumer polling a shared holder as a source saw after the holder's
/// generation ended while its poll was pending.
struct ForwardedOutcome {
    /// The consumer's result, or `None` if the executor never polled it again
    /// within a bound.
    result: Option<Result<Vec<u8>, HttpErrorCode>>,
    log: Rc<Log>,
    /// The holder's next poll as a source after that result, when it is live.
    next: Option<Result<Vec<u8>, HttpErrorCode>>,
}

fn outcome_of(result: Result<HttpBodyEvent, HttpError>) -> Result<Vec<u8>, HttpErrorCode> {
    result
        .map(|event| bytes_of(&event))
        .map_err(|error| error.code())
}

/// Awaits, on a Tokio executor, a read of a holder used as a source by
/// `through_outer_holder`: either a consumer polling the shared holder itself
/// (`HttpSource::poll_body_event`), or an outer holder's in-place read of a
/// `Forwarding` source over it. The shared holder's source returns `Pending`
/// without keeping the waker; once that poll is pending, `invalidation` ends the
/// shared holder's generation through the shared handle.
fn forwarded_read_after(
    invalidation: HolderInvalidation,
    through_outer_holder: bool,
) -> ForwardedOutcome {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let log = Rc::new(Log::default());
        let shared = Rc::new(RefCell::new(Some(HttpSourceHolder::new(
            ScriptedSource::new(&log, true, [Poll::Pending]),
        ))));
        let mut outer = None;
        let reader = if through_outer_holder {
            let outer = outer.insert(HttpSourceHolder::new(Box::new(Forwarding(Rc::clone(
                &shared,
            )))));
            let read = outer.read_body_event_unboxed(7);
            tokio::task::spawn_local(async move { outcome_of(read.await) })
        } else {
            let mut forwarding = Forwarding(Rc::clone(&shared));
            tokio::task::spawn_local(async move {
                outcome_of(
                    std::future::poll_fn(|context| forwarding.poll_body_event(context, 7)).await,
                )
            })
        };
        while log.polls.borrow().is_empty() {
            tokio::task::yield_now().await;
        }
        invalidation.apply(&shared, &log);
        let result = tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .ok()
            .map(|joined| joined.expect("the reader task finishes"));
        let next = shared.borrow_mut().as_mut().map(|holder| {
            let Poll::Ready(result) =
                holder.poll_body_event(&mut Context::from_waker(Waker::noop()), 9)
            else {
                panic!("the next poll is ready");
            };
            outcome_of(result)
        });
        drop(outer);
        drop(shared.borrow_mut().take());
        ForwardedOutcome { result, log, next }
    })
}

fn assert_forwarded_read_fails(invalidation: HolderInvalidation, through_outer_holder: bool) {
    let ForwardedOutcome { result, log, next } =
        forwarded_read_after(invalidation, through_outer_holder);
    let case = format!("{invalidation:?}, through an outer holder: {through_outer_holder}");
    assert_eq!(
        result,
        Some(Err(HttpErrorCode::DROPPED)),
        "{case}: the pending reader is woken and fails, never served by a replacement"
    );
    assert_eq!(
        *log.polls.borrow(),
        match invalidation {
            HolderInvalidation::Replace => vec![7, 9],
            _ => vec![7],
        },
        "{case}: the ended source is not polled again; only a later read reaches a replacement"
    );
    let expected_next = match invalidation {
        HolderInvalidation::Replace => Some(Ok(b"replacement".to_vec())),
        HolderInvalidation::Destroy => None,
        _ => Some(Err(HttpErrorCode::DROPPED)),
    };
    assert_eq!(next, expected_next, "{case}: the next read");
    assert_eq!(
        log.drops.get(),
        match invalidation {
            HolderInvalidation::Replace => 2,
            _ => 1,
        },
        "{case}: every source generation is dropped"
    );
    if matches!(invalidation, HolderInvalidation::Release) {
        assert!(
            log.stops.borrow().is_empty(),
            "release transfers the source unstopped"
        );
    }
}

/// A consumer polling a holder as a source (`HttpSource::poll_body_event`) is
/// registered like an in-place read: when the polled generation is cleared,
/// stopped, replaced, released or destroyed by another owner of the holder, the
/// consumer is woken and its read fails `DROPPED`, and a replacement source is
/// left to a later read. The executor alone drives the consumer.
#[test]
fn a_pending_poll_of_a_shared_holder_is_woken_when_its_generation_goes_away() {
    for invalidation in [
        HolderInvalidation::Clear,
        HolderInvalidation::StopReading,
        HolderInvalidation::Replace,
        HolderInvalidation::Release,
        HolderInvalidation::Destroy,
    ] {
        assert_forwarded_read_fails(invalidation, false);
    }
}

/// The same for an outer holder's in-place read of a forwarding source over
/// an independently reachable inner holder: ending the inner holder's
/// generation leaves the outer generation intact, and still wakes and fails
/// the outer read.
#[test]
fn a_pending_in_place_read_through_a_forwarding_source_is_woken_when_the_inner_holder_ends() {
    for invalidation in [
        HolderInvalidation::Clear,
        HolderInvalidation::StopReading,
        HolderInvalidation::Replace,
    ] {
        assert_forwarded_read_fails(invalidation, true);
    }
}

struct WakerCallbacks {
    on_wake: Option<Box<dyn FnOnce()>>,
    on_drop: Option<Box<dyn FnOnce()>>,
}

thread_local! {
    static WAKER_CALLBACKS: RefCell<Option<WakerCallbacks>> = const { RefCell::new(None) };
}

/// A waker whose wake and last drop run owner-local callbacks (kept in TLS, so
/// the waker itself holds only thread-safe data), as a custom executor's waker
/// may run code that reaches the holder.
struct OwnerWaker(ThreadId);

impl Wake for OwnerWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        assert_eq!(self.0, std::thread::current().id());
        let callback = WAKER_CALLBACKS.with(|callbacks| {
            callbacks
                .borrow_mut()
                .as_mut()
                .and_then(|callbacks| callbacks.on_wake.take())
        });
        if let Some(callback) = callback {
            callback();
        }
    }
}

impl Drop for OwnerWaker {
    fn drop(&mut self) {
        assert_eq!(self.0, std::thread::current().id());
        let callbacks = WAKER_CALLBACKS.with(|callbacks| callbacks.borrow_mut().take());
        if let Some(callback) = callbacks.and_then(|callbacks| callbacks.on_drop) {
            callback();
        }
    }
}

fn owner_waker(on_wake: Option<Box<dyn FnOnce()>>, on_drop: Option<Box<dyn FnOnce()>>) -> Waker {
    WAKER_CALLBACKS.with(|callbacks| {
        let mut callbacks = callbacks.borrow_mut();
        assert!(callbacks.is_none(), "one owner waker at a time");
        *callbacks = Some(WakerCallbacks { on_wake, on_drop });
    });
    Waker::from(Arc::new(OwnerWaker(std::thread::current().id())))
}

/// Whether the holder's state was mutably borrowed when `probe` ran.
fn borrowed_during(probe: &dyn Fn() -> bool) -> bool {
    catch_unwind(AssertUnwindSafe(probe)).is_err()
}

/// A stale cached waker, left by a finished in-place read, whose destructor
/// panics is dropped only after the retired generation was stopped and
/// dropped, outside the holder's borrow, for a clear and for a replacement.
#[test]
fn cached_waker_drop_panic_does_not_skip_clear_retirement() {
    for replace in [false, true] {
        let log = Rc::new(Log::default());
        let mut holder = HttpSourceHolder::new(ScriptedSource::new(
            &log,
            true,
            [Poll::Pending, body(b"more", false)],
        ));
        let probe = holder.readable_probe();
        let observation = Rc::new(Cell::new(None));
        let on_drop_observation = Rc::clone(&observation);
        let on_drop_log = Rc::clone(&log);
        let woken = Rc::new(Cell::new(false));
        let on_wake = Rc::clone(&woken);
        let waker = owner_waker(
            Some(Box::new(move || on_wake.set(true))),
            Some(Box::new(move || {
                on_drop_observation.set(Some((
                    probe(),
                    on_drop_log.stops.borrow().len(),
                    on_drop_log.drops.get(),
                )));
                panic!("cached reader drop");
            })),
        );
        let mut read = holder.read_body_event_unboxed(7);
        assert!(
            Pin::new(&mut read)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        assert!(matches!(poll(&mut read), Poll::Ready(Ok(_))));
        drop(read);

        let successor = Rc::new(Log::default());
        let panic = catch_unwind(AssertUnwindSafe(|| {
            if replace {
                holder.set_source(ScriptedSource::new(&successor, true, []));
            } else {
                holder.clear(Some(HttpErrorCode::CANCEL));
            }
        }))
        .expect_err("the cached reader destructor panics");
        let expected_stop = if replace {
            None
        } else {
            Some(HttpErrorCode::CANCEL)
        };
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"cached reader drop"));
        assert_eq!(
            observation.get(),
            Some((replace, 1, 1)),
            "replace {replace}: (readable, stops, drops) when the cached waker drops"
        );
        assert!(!woken.get(), "a completed read is not woken");
        assert_eq!(*log.stops.borrow(), [expected_stop], "replace {replace}");
        assert_eq!(log.drops.get(), 1);
        assert_eq!(holder.readable(), replace);
        holder.clear(None);
        assert_eq!(log.drops.get(), 1);
    }
}

/// A terminal event or error drops the generation's cached waker after its
/// source, outside the holder's borrow, for a read and for a forwarded poll.
#[test]
fn terminal_read_drops_cached_waker_after_source_outside_holder_borrow() {
    for forwarded in [false, true] {
        for error in [false, true] {
            let log = Rc::new(Log::default());
            let last = if error {
                Poll::Ready(Err(HttpError::new(HttpErrorCode::INTERNAL_ERROR)))
            } else {
                body(b"last", true)
            };
            let mut holder =
                HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending, last]));
            let probe = holder.readable_probe();
            let observation = Rc::new(Cell::new(None));
            let on_drop_observation = Rc::clone(&observation);
            let on_drop_log = Rc::clone(&log);
            let waker = owner_waker(
                None,
                Some(Box::new(move || {
                    on_drop_observation.set(Some((
                        probe(),
                        on_drop_log.stops.borrow().len(),
                        on_drop_log.drops.get(),
                    )));
                })),
            );
            let result = if forwarded {
                assert!(
                    holder
                        .poll_body_event(&mut Context::from_waker(&waker), 7)
                        .is_pending()
                );
                drop(waker);
                holder.poll_body_event(&mut Context::from_waker(Waker::noop()), 7)
            } else {
                let mut read = holder.read_body_event_unboxed(7);
                assert!(
                    Pin::new(&mut read)
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                drop(waker);
                poll(&mut read)
            };
            let Poll::Ready(result) = result else {
                panic!("the terminal read completes");
            };
            assert_eq!(result.is_err(), error);
            assert_eq!(
                observation.get(),
                Some((false, 0, 1)),
                "forwarded {forwarded}, error {error}: (readable, stops, drops) at the waker's drop"
            );
            assert!(!holder.readable());
        }
    }
}

/// A pending read that registers a new task drops the task it replaces
/// outside the holder's borrow, so that destructor may replace the source and
/// start a read of the successor; the old read then fails, and the successor
/// keeps its admission.
#[test]
fn replacing_pending_waker_can_replace_source_without_retiring_successor() {
    let old_log = Rc::new(Log::default());
    let new_log = Rc::new(Log::default());
    let holder = Rc::new(RefCell::new(HttpSourceHolder::new(ScriptedSource::new(
        &old_log,
        true,
        [Poll::Pending, Poll::Pending],
    ))));
    let successor = Rc::new(RefCell::new(None));
    let on_drop_holder = Rc::clone(&holder);
    let on_drop_successor = Rc::clone(&successor);
    let on_drop_log = Rc::clone(&new_log);
    let waker = owner_waker(
        None,
        Some(Box::new(move || {
            let mut holder = on_drop_holder.borrow_mut();
            holder.set_source(ScriptedSource::new(
                &on_drop_log,
                true,
                [Poll::Pending, body(b"new", true)],
            ));
            let mut read = holder.read_body_event_unboxed(9);
            assert!(poll(&mut read).is_pending());
            *on_drop_successor.borrow_mut() = Some(read);
        })),
    );
    let mut old = holder.borrow_mut().read_body_event_unboxed(7);
    assert!(
        Pin::new(&mut old)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    let Poll::Ready(Err(error)) = poll(&mut old) else {
        panic!("the old generation was replaced during reader registration");
    };
    assert_eq!(error.code(), HttpErrorCode::DROPPED);
    drop(old);
    let mut rejected = holder.borrow_mut().read_body_event_unboxed(1);
    let Poll::Ready(Err(error)) = poll(&mut rejected) else {
        panic!("the successor still owns admission");
    };
    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    let mut next = successor
        .borrow_mut()
        .take()
        .expect("the callback admitted a read");
    let Poll::Ready(Ok(event)) = poll(&mut next) else {
        panic!("the successor completes");
    };
    assert_eq!(bytes_of(&event), b"new");
    assert_eq!(*old_log.stops.borrow(), [None]);
    assert_eq!(old_log.drops.get(), 1);
    assert!(new_log.stops.borrow().is_empty());
    assert_eq!(new_log.drops.get(), 1);
}

/// The same holds for a forwarded poll: the task a pending poll replaces is
/// dropped outside the holder's borrow, and the poll stays registered.
#[test]
fn a_forwarded_poll_drops_the_task_it_replaces_outside_the_holder_borrow() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(
        &log,
        true,
        [Poll::Pending, Poll::Pending],
    ));
    let probe = holder.readable_probe();
    let borrowed = Rc::new(Cell::new(None));
    let on_drop_borrowed = Rc::clone(&borrowed);
    let waker = owner_waker(
        None,
        Some(Box::new(move || {
            on_drop_borrowed.set(Some(borrowed_during(&*probe)));
        })),
    );
    assert!(
        holder
            .poll_body_event(&mut Context::from_waker(&waker), 7)
            .is_pending()
    );
    drop(waker);
    assert!(
        holder
            .poll_body_event(&mut Context::from_waker(Waker::noop()), 7)
            .is_pending()
    );
    assert_eq!(
        borrowed.get(),
        Some(false),
        "the replaced task is dropped with the holder's state free"
    );
    assert_eq!(*log.polls.borrow(), [7, 7]);
}

/// Dropping a pending read releases its admission before its task is dropped,
/// so that destructor can start the next read of the same generation.
#[test]
fn pending_read_waker_drop_can_admit_same_generation_successor() {
    let log = Rc::new(Log::default());
    let holder = Rc::new(RefCell::new(HttpSourceHolder::new(ScriptedSource::new(
        &log,
        true,
        [Poll::Pending, Poll::Pending, body(b"next", true)],
    ))));
    let successor = Rc::new(RefCell::new(None));
    let on_drop_holder = Rc::clone(&holder);
    let on_drop_successor = Rc::clone(&successor);
    let waker = owner_waker(
        None,
        Some(Box::new(move || {
            let mut read = on_drop_holder.borrow_mut().read_body_event_unboxed(9);
            assert!(poll(&mut read).is_pending());
            *on_drop_successor.borrow_mut() = Some(read);
        })),
    );
    let mut old = holder.borrow_mut().read_body_event_unboxed(7);
    assert!(
        Pin::new(&mut old)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(old);
    let mut rejected = holder.borrow_mut().read_body_event_unboxed(1);
    let Poll::Ready(Err(error)) = poll(&mut rejected) else {
        panic!("the reentrant read still owns admission");
    };
    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    let mut next = successor
        .borrow_mut()
        .take()
        .expect("the callback admitted a read");
    let Poll::Ready(Ok(event)) = poll(&mut next) else {
        panic!("the successor completes");
    };
    assert_eq!(bytes_of(&event), b"next");
    assert_eq!(*log.polls.borrow(), [7, 9, 9]);
    assert!(log.stops.borrow().is_empty());
    assert_eq!(log.drops.get(), 1);
}

/// A release whose pending forwarded poll's wake panics cannot hand the source
/// out: it stops and drops it, then resumes the panic.
#[test]
fn release_wake_panic_stops_and_drops_detached_source() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    let probe = holder.readable_probe();
    let observation = Rc::new(Cell::new(None));
    let on_wake_observation = Rc::clone(&observation);
    let waker = owner_waker(
        Some(Box::new(move || {
            on_wake_observation.set(Some(probe()));
            panic!("released reader wake");
        })),
        None,
    );
    assert!(
        holder
            .poll_body_event(&mut Context::from_waker(&waker), 7)
            .is_pending()
    );
    drop(waker);
    let panic = catch_unwind(AssertUnwindSafe(|| holder.release()))
        .err()
        .expect("the released reader wake panics");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"released reader wake"));
    assert_eq!(observation.get(), Some(false));
    assert_eq!(*log.stops.borrow(), [None]);
    assert_eq!(log.drops.get(), 1);
    assert!(!holder.readable());
    assert!(holder.release().expect("no admitted read").is_none());
}

/// A retired pending read's task is woken and dropped in separate panic
/// containment, after the source is stopped and dropped: a wake that panics
/// and a drop that panics do not abort the process, and the first panic wins.
#[test]
fn pending_read_wake_and_drop_panics_follow_source_retirement() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    let probe = holder.readable_probe();
    let observation = Rc::new(Cell::new(None));
    let on_wake_observation = Rc::clone(&observation);
    let on_wake_log = Rc::clone(&log);
    let reader_dropped = Rc::new(Cell::new(false));
    let on_drop = Rc::clone(&reader_dropped);
    let waker = owner_waker(
        Some(Box::new(move || {
            on_wake_observation.set(Some((
                probe(),
                on_wake_log.stops.borrow().len(),
                on_wake_log.drops.get(),
            )));
            panic!("retired reader wake");
        })),
        Some(Box::new(move || {
            on_drop.set(true);
            panic!("retired reader drop");
        })),
    );
    let mut read = holder.read_body_event_unboxed(7);
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        holder.clear(Some(HttpErrorCode::CANCEL))
    }))
    .expect_err("the retired reader wake panics");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"retired reader wake"));
    assert!(
        reader_dropped.get(),
        "reader destruction was attempted after wake panicked"
    );
    assert_eq!(observation.get(), Some((false, 1, 1)));
    assert_eq!(*log.stops.borrow(), [Some(HttpErrorCode::CANCEL)]);
    assert_eq!(log.drops.get(), 1);
    let Poll::Ready(Err(error)) = poll(&mut read) else {
        panic!("the read was invalidated before waking its reader");
    };
    assert_eq!(error.code(), HttpErrorCode::DROPPED);
}

/// Dropping a pending read whose task's destructor panics releases admission
/// first and resumes the panic; while the thread is already unwinding, the
/// destructor's panic is contained instead of aborting the process.
#[test]
fn a_dropped_pending_read_contains_its_task_drop_panic_while_unwinding() {
    for unwinding in [false, true] {
        let log = Rc::new(Log::default());
        let mut holder = HttpSourceHolder::new(ScriptedSource::new(
            &log,
            true,
            [Poll::Pending, body(b"after", true)],
        ));
        let waker = owner_waker(None, Some(Box::new(|| panic!("abandoned reader drop"))));
        let mut read = holder.read_body_event_unboxed(7);
        assert!(
            Pin::new(&mut read)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        let panic = catch_unwind(AssertUnwindSafe(move || {
            let _read = read;
            if unwinding {
                panic!("already unwinding");
            }
        }))
        .expect_err("a panic leaves the drop");
        let expected = if unwinding {
            "already unwinding"
        } else {
            "abandoned reader drop"
        };
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&expected),
            "unwinding {unwinding}"
        );
        let mut next = holder.read_body_event_unboxed(9);
        let Poll::Ready(Ok(event)) = poll(&mut next) else {
            panic!("unwinding {unwinding}: the dropped read released admission");
        };
        assert_eq!(bytes_of(&event), b"after");
        assert!(log.stops.borrow().is_empty());
    }
}

/// Counts wakes; its strong count shows whether a poll kept a clone of the task.
#[derive(Default)]
struct CountingWake(std::sync::atomic::AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl CountingWake {
    fn wakes(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The holder's owner polls its source in place, with each poll's `maximum`,
/// and no read future; a terminal event consumes the source unstopped.
#[test]
fn an_exclusive_poll_serves_a_polling_source_in_place() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(
        &log,
        true,
        [body(b"first", false), Poll::Pending, body(b"last", true)],
    ));
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(event)) = holder.poll_body_event_exclusive(&mut context, 7) else {
        panic!("the first poll is ready");
    };
    assert_eq!(bytes_of(&event), b"first");
    assert!(
        holder
            .poll_body_event_exclusive(&mut context, 5)
            .is_pending()
    );
    let Poll::Ready(Ok(event)) = holder.poll_body_event_exclusive(&mut context, 6) else {
        panic!("the third poll is ready");
    };
    assert_eq!(bytes_of(&event), b"last");
    assert!(event.eom());
    assert_eq!(*log.polls.borrow(), [7, 5, 6]);
    assert_eq!(log.reads.get(), 0, "no boxed read was made");
    assert!(!holder.readable(), "the terminal event consumed the source");
    assert_eq!(log.drops.get(), 1);
    assert!(log.stops.borrow().is_empty());
    let Poll::Ready(Err(error)) = holder.poll_body_event_exclusive(&mut context, 7) else {
        panic!("a consumed holder fails");
    };
    assert_eq!(error.code(), HttpErrorCode::DROPPED);
    assert_eq!(*log.polls.borrow(), [7, 5, 6]);
}

#[test]
fn an_exclusive_poll_error_is_terminal() {
    let log = Rc::new(Log::default());
    let error = HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, b"failed");
    let mut holder =
        HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Ready(Err(error))]));
    let Poll::Ready(Err(error)) =
        holder.poll_body_event_exclusive(&mut Context::from_waker(Waker::noop()), 7)
    else {
        panic!("the error is returned");
    };
    assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
    assert!(!holder.readable());
    assert_eq!(log.drops.get(), 1);
    assert!(log.stops.borrow().is_empty());
}

#[test]
fn an_exclusive_poll_of_an_empty_holder_fails_dropped() {
    let mut holder = HttpSourceHolder::empty();
    let Poll::Ready(Err(error)) =
        holder.poll_body_event_exclusive(&mut Context::from_waker(Waker::noop()), 7)
    else {
        panic!("an empty holder fails");
    };
    assert_eq!(error.code(), HttpErrorCode::DROPPED);
}

/// Only the owner can end the generation an exclusive poll polls, so a
/// `Pending` exclusive poll keeps no clone of its task, ending the generation
/// wakes nothing, and a source the owner installs after that poll serves the
/// next one; a forwarded poll registers its task, as before.
#[test]
fn a_pending_exclusive_poll_registers_no_task_and_leaves_no_orphan() {
    let wake = Arc::new(CountingWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    assert!(
        holder
            .poll_body_event_exclusive(&mut Context::from_waker(&waker), 7)
            .is_pending()
    );
    assert_eq!(Arc::strong_count(&wake), 2, "no task clone was kept");
    holder.set_source(ScriptedSource::new(&log, true, [body(b"successor", true)]));
    assert_eq!(wake.wakes(), 0, "ending the generation woke nothing");
    let Poll::Ready(Ok(event)) =
        holder.poll_body_event_exclusive(&mut Context::from_waker(&waker), 9)
    else {
        panic!("the owner's replacement serves the next poll");
    };
    assert_eq!(bytes_of(&event), b"successor");
    assert_eq!(*log.polls.borrow(), [7, 9]);
    assert_eq!(*log.stops.borrow(), [None]);

    let mut forwarded = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    assert!(
        forwarded
            .poll_body_event(&mut Context::from_waker(&waker), 7)
            .is_pending()
    );
    assert_eq!(
        Arc::strong_count(&wake),
        3,
        "a forwarded poll registers its task"
    );
    drop(forwarded);
    assert_eq!(wake.wakes(), 1);
}

/// While anything else could reach the holder's state (a read future or
/// another handle) or a forwarded poll is pending, an exclusive poll is the
/// forwarded poll, with its guarantees.
#[test]
fn an_exclusive_poll_keeps_the_forwarded_poll_guarantees_otherwise() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(
        &log,
        true,
        [Poll::Pending, body(b"read", true)],
    ));
    let mut read = holder.read_body_event_unboxed(7);
    assert!(poll(&mut read).is_pending());
    let Poll::Ready(Err(error)) =
        holder.poll_body_event_exclusive(&mut Context::from_waker(Waker::noop()), 9)
    else {
        panic!("a holder with an active read refuses");
    };
    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    assert_eq!(*log.polls.borrow(), [7], "the source is not polled for it");
    assert!(matches!(poll(&mut read), Poll::Ready(Ok(_))));
    drop(read);

    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    assert!(
        holder
            .poll_body_event(&mut Context::from_waker(Waker::noop()), 7)
            .is_pending()
    );
    holder.set_source(ScriptedSource::new(
        &log,
        true,
        [body(b"replacement", true)],
    ));
    let Poll::Ready(Err(error)) =
        holder.poll_body_event_exclusive(&mut Context::from_waker(Waker::noop()), 9)
    else {
        panic!("the orphaned forwarded poll fails once");
    };
    assert_eq!(error.code(), HttpErrorCode::DROPPED);
    let Poll::Ready(Ok(event)) =
        holder.poll_body_event_exclusive(&mut Context::from_waker(Waker::noop()), 9)
    else {
        panic!("then the replacement serves");
    };
    assert_eq!(bytes_of(&event), b"replacement");

    let wake = Arc::new(CountingWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(ScriptedSource::new(&log, true, [Poll::Pending]));
    let probe = holder.readable_probe();
    assert!(
        holder
            .poll_body_event_exclusive(&mut Context::from_waker(&waker), 7)
            .is_pending()
    );
    assert_eq!(
        Arc::strong_count(&wake),
        3,
        "with another handle to the state, the poll registers its task"
    );
    holder.clear(Some(HttpErrorCode::CANCEL));
    assert_eq!(
        wake.wakes(),
        1,
        "and is woken when the generation goes away"
    );
    assert!(!probe());
}

/// `DIV-056`: an in-place read whose source poll panics consumes the
/// generation as an error does: the source is dropped without `stop_reading`,
/// then the task its earlier `Pending` poll registered, outside the holder's
/// borrow, and the panic continues.
#[cfg(panic = "unwind")]
#[test]
fn a_panicking_in_place_read_retires_its_source_without_stop_reading() {
    let log = Rc::new(Log::default());
    let mut holder = HttpSourceHolder::new(
        ScriptedSource::new(&log, true, [Poll::Pending]).panicking_when_exhausted("in-place poll"),
    );
    let probe = holder.readable_probe();
    let observation = Rc::new(Cell::new(None));
    let on_drop_observation = Rc::clone(&observation);
    let on_drop_log = Rc::clone(&log);
    let waker = owner_waker(
        None,
        Some(Box::new(move || {
            on_drop_observation.set(Some((probe(), on_drop_log.drops.get())));
        })),
    );
    let mut read = holder.read_body_event_unboxed(7);
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);

    let panic =
        catch_unwind(AssertUnwindSafe(|| drop(poll(&mut read)))).expect_err("the poll panics");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"in-place poll"));
    assert_eq!(
        observation.get(),
        Some((false, 1)),
        "(readable, drops) when the registered task drops"
    );
    assert!(log.stops.borrow().is_empty());
    assert_eq!(log.drops.get(), 1);
    drop(read);
    assert!(!holder.readable());
    let Poll::Ready(Err(error)) = poll(&mut holder.read_body_event_unboxed(9)) else {
        panic!("the consumed holder has nothing to read");
    };
    assert_eq!(error.code(), HttpErrorCode::DROPPED);
    drop(holder);
    assert!(log.stops.borrow().is_empty());
    assert_eq!(log.drops.get(), 1);
    assert_eq!(*log.polls.borrow(), [7, 7]);
}

#[cfg(panic = "unwind")]
#[derive(Clone, Copy, Debug)]
enum SourcePoll {
    Exclusive,
    Forwarded,
    ThroughOuterHolder,
}

/// `DIV-056`: a source poll that panics under an exclusive poll, or
/// under the holder polled as a source (directly, or by an outer holder's
/// in-place read), consumes the source as a panicking read does.
#[cfg(panic = "unwind")]
#[test]
fn a_panicking_exclusive_or_forwarded_poll_retires_its_source() {
    for path in [
        SourcePoll::Exclusive,
        SourcePoll::Forwarded,
        SourcePoll::ThroughOuterHolder,
    ] {
        let log = Rc::new(Log::default());
        let source = ScriptedSource::new(&log, true, []).panicking_when_exhausted("source poll");
        let mut holder = match path {
            SourcePoll::Exclusive | SourcePoll::Forwarded => HttpSourceHolder::new(source),
            SourcePoll::ThroughOuterHolder => {
                HttpSourceHolder::new(Box::new(HttpSourceHolder::new(source)))
            }
        };
        let poll_holder = |holder: &mut HttpSourceHolder| {
            let mut context = Context::from_waker(Waker::noop());
            match path {
                SourcePoll::Exclusive => holder.poll_body_event_exclusive(&mut context, 7),
                SourcePoll::Forwarded => holder.poll_body_event(&mut context, 7),
                SourcePoll::ThroughOuterHolder => poll(&mut holder.read_body_event_unboxed(7)),
            }
        };

        let panic = catch_unwind(AssertUnwindSafe(|| drop(poll_holder(&mut holder))))
            .expect_err("the source poll panics");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"source poll"),
            "{path:?}"
        );
        assert!(log.stops.borrow().is_empty(), "{path:?}");
        assert_eq!(
            log.drops.get(),
            1,
            "{path:?}: dropped before the panic continues"
        );
        assert!(!holder.readable(), "{path:?}");
        let Poll::Ready(Err(error)) = poll_holder(&mut holder) else {
            panic!("{path:?}: the next poll fails");
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED, "{path:?}");
        drop(holder);
        assert!(log.stops.borrow().is_empty(), "{path:?}");
        assert_eq!(log.drops.get(), 1, "{path:?}");
        assert_eq!(*log.polls.borrow(), [7], "{path:?}");
    }
}
