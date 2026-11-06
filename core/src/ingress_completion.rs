//! Producer completion observation independent of source consumption.

use std::cell::RefCell;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Poll;
use std::task::Waker;
use std::time::Instant;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::source::LocalSourceFuture;
use crate::source_holder::finish_retirement_panic;
use crate::source_holder::retain_retirement_panic;

type Outcome = Result<Instant, HttpError>;
type Waiter = RefCell<Option<Waker>>;

struct CompletionState {
    outcome: Option<Outcome>,
    body_bytes_received: u64,
    waiters: Vec<Weak<Waiter>>,
}

/// Observes when a source's producer admitted EOM, independently of consumption.
///
/// Success records the producer's monotonic completion time. It does not imply
/// body consumption, transport writes, or exchange cleanup. Observations retain
/// terminal state only; they do not keep the source or buffered body alive.
#[derive(Clone)]
pub struct IngressCompletion(Rc<RefCell<CompletionState>>);

impl IngressCompletion {
    /// Returns accepted body bytes, including bytes not consumed before retirement.
    /// Padding and rejected input do not contribute to this cumulative count.
    pub fn body_bytes_received(&self) -> u64 {
        self.0.borrow().body_bytes_received
    }

    /// Returns the first terminal producer outcome, when available.
    pub fn snapshot(&self) -> Option<Outcome> {
        self.0.borrow().outcome.clone()
    }

    /// Waits for producer completion. Dropping a wait does not affect the source.
    pub fn wait(&self) -> LocalSourceFuture<Outcome> {
        if let Some(outcome) = self.snapshot() {
            return Box::pin(std::future::ready(outcome));
        }
        let waiter = Rc::new(RefCell::new(None));
        let mut state = self.0.borrow_mut();
        state.waiters.retain(|waiter| waiter.strong_count() != 0);
        state.waiters.push(Rc::downgrade(&waiter));
        drop(state);
        let completion = self.clone();
        Box::pin(poll_fn(move |context| {
            if let Some(outcome) = completion.snapshot() {
                return Poll::Ready(outcome);
            }
            let mut registered = waiter.borrow_mut();
            if registered
                .as_ref()
                .is_none_or(|waker: &Waker| !waker.will_wake(context.waker()))
            {
                *registered = Some(context.waker().clone());
            }
            Poll::Pending
        }))
    }

    fn finish(&self, outcome: Outcome) {
        let waiters = {
            let mut state = self.0.borrow_mut();
            if state.outcome.is_some() {
                return;
            }
            state.outcome = Some(outcome);
            std::mem::take(&mut state.waiters)
        };
        if !waiters.is_empty() {
            wake_waiters(waiters);
        }
    }
}

// Nothing between `finish` entry and this call can begin or end unwinding, so
// sampling `panicking()` here matches sampling it on entry.
#[inline(never)]
fn wake_waiters(waiters: Vec<Weak<Waiter>>) {
    let unwinding = std::thread::panicking();
    let mut first_panic = None;
    for waiter in waiters {
        let waker = waiter
            .upgrade()
            .and_then(|waiter| waiter.borrow_mut().take());
        if let Some(waker) = waker {
            retain_retirement_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| waker.wake())).err(),
            );
        }
    }
    finish_retirement_panic(first_panic, unwinding);
}

enum PublisherState {
    Unobserved {
        outcome: Option<Outcome>,
        body_bytes_received: u64,
    },
    Observed(IngressCompletion),
}

pub(crate) struct IngressCompletionPublisher(RefCell<PublisherState>);

impl Default for IngressCompletionPublisher {
    fn default() -> Self {
        Self(RefCell::new(PublisherState::Unobserved {
            outcome: None,
            body_bytes_received: 0,
        }))
    }
}

impl IngressCompletionPublisher {
    pub(crate) fn add_body_bytes(&self, bytes: usize) {
        match &mut *self.0.borrow_mut() {
            PublisherState::Unobserved {
                body_bytes_received,
                ..
            } => {
                *body_bytes_received = body_bytes_received.saturating_add(bytes as u64);
            }
            PublisherState::Observed(completion) => {
                let mut state = completion.0.borrow_mut();
                state.body_bytes_received = state.body_bytes_received.saturating_add(bytes as u64);
            }
        }
    }

    pub(crate) fn observe(&self) -> IngressCompletion {
        let mut state = self.0.borrow_mut();
        match &mut *state {
            PublisherState::Observed(completion) => completion.clone(),
            PublisherState::Unobserved {
                outcome,
                body_bytes_received,
            } => {
                let completion = IngressCompletion(Rc::new(RefCell::new(CompletionState {
                    outcome: outcome.take(),
                    body_bytes_received: *body_bytes_received,
                    waiters: Vec::new(),
                })));
                *state = PublisherState::Observed(completion.clone());
                completion
            }
        }
    }

    pub(crate) fn finish(&self, outcome: Outcome) {
        let completion = {
            let mut state = self.0.borrow_mut();
            match &mut *state {
                PublisherState::Unobserved {
                    outcome: stored, ..
                } => {
                    if stored.is_none() {
                        *stored = Some(outcome);
                    }
                    return;
                }
                PublisherState::Observed(completion) => completion.clone(),
            }
        };
        completion.finish(outcome);
    }
}

impl Drop for IngressCompletionPublisher {
    fn drop(&mut self) {
        let completed = self.0.try_borrow_mut().is_ok_and(|state| match &*state {
            PublisherState::Unobserved { outcome, .. } => outcome.is_some(),
            PublisherState::Observed(completion) => completion
                .0
                .try_borrow_mut()
                .is_ok_and(|state| state.outcome.is_some()),
        });
        if completed {
            return;
        }
        self.finish(Err(HttpError::with_message(
            HttpErrorCode::DROPPED,
            b"source retired before producer completion",
        )));
    }
}
