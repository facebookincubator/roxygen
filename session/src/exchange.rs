/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::future::ready;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::exchange::ExchangeCompletion;
use roxygen_core::exchange::ExchangeCompletionError;
use roxygen_core::exchange::ExchangeCompletionObserver;
use roxygen_core::exchange::LocalExchangeCompletionFuture;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use tokio::sync::watch;

type CompletionResult = Result<(), ExchangeCompletionError>;
type PanicPayload = Box<dyn Any + Send>;

enum ObservationState {
    Pending,
    Waiting(watch::Sender<Option<CompletionResult>>),
    Complete(CompletionResult),
}

/// One exchange's completion observation and retirement accounting, shared by
/// its owner, trackers, guards, and every completion handed out for it.
struct ExchangeState {
    observation: RefCell<ObservationState>,
    pending: Cell<usize>,
    detached: Cell<bool>,
    driver_applied: Cell<bool>,
    driver_complete: Cell<bool>,
    unconfirmed: Cell<bool>,
    /// The session began serializing the exchange's outgoing head.
    egress_started: Cell<bool>,
    wake: Rc<dyn Fn()>,
}

impl ExchangeState {
    fn finish(&self, result: CompletionResult) -> Option<watch::Sender<Option<CompletionResult>>> {
        let mut state = self.observation.borrow_mut();
        if matches!(*state, ObservationState::Complete(_)) {
            return None;
        }
        // Notify only after releasing this borrow: a waiter may reenter the observer.
        match mem::replace(&mut *state, ObservationState::Complete(result)) {
            ObservationState::Waiting(publisher) => Some(publisher),
            _ => None,
        }
    }
}

impl ExchangeCompletionObserver for ExchangeState {
    fn wait(&self) -> LocalExchangeCompletionFuture {
        let mut receiver = match &mut *self.observation.borrow_mut() {
            ObservationState::Complete(result) => return Box::pin(ready(*result)),
            ObservationState::Waiting(publisher) => publisher.subscribe(),
            state @ ObservationState::Pending => {
                let (publisher, receiver) = watch::channel(None);
                *state = ObservationState::Waiting(publisher);
                receiver
            }
        };
        Box::pin(async move {
            loop {
                if let Some(result) = *receiver.borrow_and_update() {
                    return result;
                }
                if receiver.changed().await.is_err() {
                    return Err(ExchangeCompletionError::CleanupUnconfirmed);
                }
            }
        })
    }
}

pub(crate) struct ExchangeOwner {
    tracker: ExchangeTracker,
}

impl ExchangeOwner {
    pub(crate) fn new(wake: Rc<dyn Fn()>, driver_required: bool) -> Self {
        Self {
            tracker: ExchangeTracker(Rc::new(ExchangeState {
                observation: RefCell::new(ObservationState::Pending),
                pending: Cell::new(0),
                detached: Cell::new(false),
                driver_applied: Cell::new(false),
                driver_complete: Cell::new(!driver_required),
                unconfirmed: Cell::new(false),
                egress_started: Cell::new(false),
                wake,
            })),
        }
    }

    pub(crate) fn tracker(&self) -> ExchangeTracker {
        self.tracker.clone()
    }

    pub(crate) fn detached_tracker(&self) -> Option<ExchangeTracker> {
        self.tracker.detached().then(|| self.tracker())
    }

    #[cfg(test)]
    pub(crate) fn completion(&self) -> ExchangeCompletion {
        self.tracker.completion()
    }

    /// Observes the exchange's shared state allocation without retaining it.
    #[cfg(test)]
    pub(crate) fn allocation_for_test(&self) -> std::rc::Weak<dyn Any> {
        let state: Rc<dyn Any> = self.tracker.0.clone();
        Rc::downgrade(&state)
    }

    pub(crate) fn ready(&self) -> bool {
        self.tracker.detached()
            && self.tracker.0.pending.get() == 0
            && self.tracker.0.driver_complete.get()
    }

    pub(crate) fn publish(self) {
        assert!(self.ready());
        let result = if self.tracker.0.unconfirmed.get() {
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        } else {
            Ok(())
        };
        if let Some(publisher) = self.tracker.0.finish(result) {
            publisher.send_replace(Some(result));
        }
    }
}

impl Drop for ExchangeOwner {
    fn drop(&mut self) {
        drop(
            self.tracker
                .0
                .finish(Err(ExchangeCompletionError::CleanupUnconfirmed)),
        );
    }
}

#[derive(Clone)]
pub(crate) struct ExchangeTracker(Rc<ExchangeState>);

impl ExchangeTracker {
    pub(crate) fn completion(&self) -> ExchangeCompletion {
        ExchangeCompletion::new(self.0.clone())
    }

    pub(crate) fn guard(&self) -> ExchangeGuard {
        self.0.pending.set(
            self.0
                .pending
                .get()
                .checked_add(1)
                .expect("exchange guard overflow"),
        );
        ExchangeGuard {
            tracker: self.clone(),
        }
    }

    pub(crate) fn source(&self, source: HttpSourceHolder) -> HttpSourceHolder {
        if !source.readable() {
            return source;
        }
        HttpSourceHolder::new(Box::new(ExchangeSource {
            inner: Some(source),
            tracker: self.clone(),
            retirement: Some(self.guard()),
        }))
    }

    pub(crate) fn mark_detached(&self) {
        self.0.detached.set(true);
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| (self.0.wake)())) {
            self.fail();
            resume_drop_panic(Some(payload));
        }
    }

    pub(crate) fn detached(&self) -> bool {
        self.0.detached.get()
    }

    pub(crate) fn claim_driver_update(&self) -> bool {
        !self.0.driver_applied.replace(true)
    }

    pub(crate) fn driver_applied(&self) -> bool {
        self.0.driver_applied.get()
    }

    pub(crate) fn finish_driver(&self) {
        self.0.driver_complete.set(true);
    }

    pub(crate) fn fail(&self) {
        self.0.unconfirmed.set(true);
    }

    pub(crate) fn mark_egress_started(&self) {
        self.0.egress_started.set(true);
    }

    pub(crate) fn egress_started(&self) -> bool {
        self.0.egress_started.get()
    }
}

pub(crate) struct ExchangeGuard {
    tracker: ExchangeTracker,
}

impl ExchangeGuard {
    pub(crate) fn fail(&self) {
        self.tracker.fail();
    }
}

impl Drop for ExchangeGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.fail();
        }
        let state = &self.tracker.0;
        state.pending.set(
            state
                .pending
                .get()
                .checked_sub(1)
                .expect("exchange guard underflow"),
        );
        if state.pending.get() == 0
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| (state.wake)()))
        {
            self.fail();
            resume_drop_panic(Some(payload));
        }
    }
}

fn retire<T>(
    inner: &mut Option<T>,
    retirement: &mut Option<ExchangeGuard>,
) -> Option<PanicPayload> {
    let mut panic = catch_unwind(AssertUnwindSafe(|| drop(inner.take()))).err();
    if panic.is_some() {
        if let Some(guard) = retirement.as_ref() {
            guard.fail();
        }
    }
    let retired = catch_unwind(AssertUnwindSafe(|| drop(retirement.take()))).err();
    if panic.is_none() {
        panic = retired;
    } else {
        forget_suppressed_panic(retired);
    }
    panic
}

fn resume_drop_panic(panic: Option<PanicPayload>) {
    if let Some(payload) = panic {
        if std::thread::panicking() {
            roxygen_runtime::panic::discard_panic(payload);
        } else {
            resume_unwind(payload);
        }
    }
}

fn forget_suppressed_panic(panic: Option<PanicPayload>) {
    if let Some(payload) = panic {
        roxygen_runtime::panic::discard_panic(payload);
    }
}

struct ExchangeSource {
    inner: Option<HttpSourceHolder>,
    tracker: ExchangeTracker,
    retirement: Option<ExchangeGuard>,
}

impl HttpSource for ExchangeSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let retirement = self.tracker.guard();
        Box::pin(ExchangeRead {
            inner: Some(
                self.inner
                    .as_mut()
                    .expect("live source")
                    .read_header_event_unboxed(),
            ),
            retirement: Some(retirement),
        })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let retirement = self.tracker.guard();
        Box::pin(ExchangeRead {
            inner: Some(
                self.inner
                    .as_mut()
                    .expect("live source")
                    .read_body_event_unboxed(maximum),
            ),
            retirement: Some(retirement),
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            self.inner
                .as_mut()
                .expect("live source")
                .stop_reading(error);
        })) {
            self.tracker.fail();
            resume_drop_panic(Some(payload));
        }
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.inner.as_ref().and_then(HttpSource::get_stream_id)
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.inner.as_ref().and_then(HttpSource::ingress_completion)
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.inner
            .as_mut()
            .expect("live source")
            .set_read_timeout(timeout);
    }

    /// A holder calls this before any read guard exists, so a panic in the
    /// application's implementation marks the cleanup unconfirmed here.
    fn polls_body_events(&self) -> bool {
        let polls = catch_unwind(AssertUnwindSafe(|| {
            self.inner
                .as_ref()
                .is_some_and(HttpSource::polls_body_events)
        }));
        polls.unwrap_or_else(|payload| {
            self.tracker.fail();
            resume_unwind(payload)
        })
    }

    /// Needs no per-read guard: unlike a read future, a poll leaves no foreign
    /// state behind, and the source itself holds `retirement`.
    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let polled = catch_unwind(AssertUnwindSafe(|| {
            self.inner
                .as_mut()
                .expect("live source")
                .poll_body_event(context, maximum)
        }));
        polled.unwrap_or_else(|payload| {
            self.tracker.fail();
            resume_unwind(payload)
        })
    }
}

impl Drop for ExchangeSource {
    fn drop(&mut self) {
        resume_drop_panic(retire(&mut self.inner, &mut self.retirement));
    }
}

struct ExchangeRead<F> {
    inner: Option<F>,
    retirement: Option<ExchangeGuard>,
}

impl<F> Future for ExchangeRead<F>
where
    F: Future + Unpin,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polled = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(this.inner.as_mut().expect("live source read")).poll(context)
        }));
        match polled {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                if let Some(payload) = retire(&mut this.inner, &mut this.retirement) {
                    forget_suppressed_panic(catch_unwind(AssertUnwindSafe(|| drop(result))).err());
                    resume_unwind(payload);
                }
                Poll::Ready(result)
            }
            Err(payload) => {
                if let Some(guard) = &this.retirement {
                    guard.fail();
                }
                forget_suppressed_panic(retire(&mut this.inner, &mut this.retirement));
                resume_unwind(payload)
            }
        }
    }
}

impl<F> Drop for ExchangeRead<F> {
    fn drop(&mut self) {
        resume_drop_panic(retire(&mut self.inner, &mut self.retirement));
    }
}
