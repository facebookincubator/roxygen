/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Request-body credit and deferred stream work.
//!
//! The application consumes a request body outside the session's callbacks, possibly from another
//! thread, so credit is not returned at parse time. Each exchange owns a [`CreditCell`] that
//! accumulates the bytes the application consumed; the first addition after a drain queues the
//! stream on the session's [`WorkQueue`] and wakes the session, whose next callback returns the
//! credit with `consume`. The same queue carries work that `poll_send` cannot do itself, because
//! the pull sink does not offer stream operations: STOP_SENDING after an early response and a
//! reset after a failed response.
//!
//! The session is woken through the waker of its latest `poll_session`, or through its
//! `ConnHandle` before the first `poll_session`. A `ConnHandle` wake is dropped when the driver's
//! command queue is full, so later additions retry a dropped wake until the session drains the
//! queue.

use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use mvfstr_proto::StreamId;
use roxygen_core::ids::StreamId as HttpStreamId;
use roxygen_core::stream_source::HttpStreamSourceSharedCallback;

use crate::transport::SessionWake;

/// Streams with work for the session, and the waker and handle that wake the session.
///
/// Each stream is queued at most once per drain for credit, so the queue is bounded by the number
/// of exchanges plus the work `poll_send` queues for them.
#[derive(Debug, Default)]
pub(crate) struct WorkQueue {
    streams: Mutex<Vec<StreamId>>,
    handle: OnceLock<Box<dyn SessionWake>>,
    /// The waker `poll_session` was last given. It sets a flag instead of queueing a command, so
    /// its wakes are never dropped, and it is preferred over the handle once set.
    waker: Mutex<Option<Waker>>,
    /// Whether the last wake was dropped and the session has not drained the queue since.
    lost: AtomicBool,
}

impl WorkQueue {
    /// Installs the session handle at the session's first callback.
    pub(crate) fn set_handle(&self, handle: Box<dyn SessionWake>) {
        let _ = self.handle.set(handle);
    }

    /// Queues `stream` and wakes the session.
    pub(crate) fn push(&self, stream: StreamId) {
        self.streams
            .lock()
            .expect("the work queue lock is never held across a panic")
            .push(stream);
        self.wake();
    }

    /// Records the waker of the session's latest `poll_session`.
    pub(crate) fn set_waker(&self, waker: &Waker) {
        let mut slot = self
            .waker
            .lock()
            .expect("the waker lock is never held across a panic");
        if !slot.as_ref().is_some_and(|known| known.will_wake(waker)) {
            *slot = Some(waker.clone());
        }
    }

    /// Wakes the session without queueing a stream.
    pub(crate) fn wake(&self) {
        let waker = self
            .waker
            .lock()
            .expect("the waker lock is never held across a panic")
            .clone();
        let delivered = match waker {
            Some(waker) => {
                waker.wake();
                true
            }
            None => self.handle.get().is_some_and(|handle| handle.wake()),
        };
        self.lost.store(!delivered, Ordering::Release);
    }

    /// Returns whether the last wake was dropped since the session drained the queue, so that
    /// queued work may wait for a callback that never comes.
    pub(crate) fn wake_lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    /// Moves the queued streams into `into`, which the caller keeps empty between drains so that
    /// the two vectors trade their allocations.
    pub(crate) fn take(&self, into: &mut Vec<StreamId>) {
        debug_assert!(into.is_empty());
        self.lost.store(false, Ordering::Release);
        let mut streams = self
            .streams
            .lock()
            .expect("the work queue lock is never held across a panic");
        mem::swap(&mut *streams, into);
    }
}

/// The request-body bytes the application consumed and the session has not yet returned as
/// stream credit. It is `Send` and `Sync`, so a consumer on any thread can add to it.
#[derive(Debug)]
pub(crate) struct CreditCell {
    stream: StreamId,
    consumed: AtomicU64,
    queue: Arc<WorkQueue>,
}

impl CreditCell {
    pub(crate) fn new(stream: StreamId, queue: Arc<WorkQueue>) -> Self {
        Self {
            stream,
            consumed: AtomicU64::new(0),
            queue,
        }
    }

    /// Records `amount` consumed bytes. The first addition since the last [`take`](Self::take)
    /// queues the stream and wakes the session; a later one retries a wake that was dropped.
    pub(crate) fn add(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        if self.consumed.fetch_add(amount, Ordering::AcqRel) == 0 {
            self.queue.push(self.stream);
        } else if self.queue.wake_lost() {
            self.queue.wake();
        }
    }

    /// Takes the consumed bytes recorded so far.
    pub(crate) fn take(&self) -> u64 {
        self.consumed.swap(0, Ordering::AcqRel)
    }
}

/// The ingress body's callbacks for one exchange, which add consumed bytes to its credit cell.
pub(crate) struct IngressSignals {
    pub(crate) cell: Arc<CreditCell>,
}

impl HttpStreamSourceSharedCallback for IngressSignals {
    fn bytes_processed(&self, _id: HttpStreamId, amount: usize, _to_acknowledge: usize) {
        self.cell.add(amount as u64);
    }
}
