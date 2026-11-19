/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Per-cleanup wakers for the non-hosted driver's woken-only cleanup polling.
//!
//! Each activated cleanup gets its own `Send + Sync` waker. A wake records the
//! cleanup's `(slot, id)` once in the worker's ready list and wakes the driver,
//! and an open driver pass, after its commands, polls only the cleanups that
//! it drains from that list. The cleanup's `queued` flag is cleared
//! immediately before each poll and set by a wake, as `FuturesUnordered` does,
//! so a wake that races a poll is never lost and a cleanup appears in the list
//! at most once.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;

use super::WorkerShared;

/// A cleanup's registry key. Identifiers are never reused, so a stale entry
/// never resolves to the cleanup that reused its slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReadyEntry {
    pub(super) slot: usize,
    pub(super) id: u64,
}

/// One cleanup's waker.
pub(super) struct CleanupWakeNode {
    entry: ReadyEntry,
    /// Set by a wake, cleared by the driver immediately before it polls the
    /// cleanup, and latched set once the cleanup finishes.
    pub(super) queued: AtomicBool,
    shared: Arc<WorkerShared>,
}

impl Wake for CleanupWakeNode {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.queued.swap(true, Ordering::SeqCst) {
            return;
        }
        self.shared.push_ready_cleanup(self.entry);
    }
}

/// A cleanup's wake node and the waker used for each of its polls.
pub(super) struct WokenHandle {
    pub(super) node: Arc<CleanupWakeNode>,
    pub(super) waker: Waker,
}

/// The owner-local half of woken-only cleanup polling.
pub(super) struct WokenCleanups {
    /// The drained list, empty between passes; it alternates with the
    /// worker's ready list, so both keep the capacity of the largest batch.
    pub(super) spare: RefCell<Vec<ReadyEntry>>,
}

impl WokenCleanups {
    pub(super) fn new() -> Self {
        Self {
            spare: RefCell::new(Vec::new()),
        }
    }

    pub(super) fn handle_for(
        &self,
        shared: &Arc<WorkerShared>,
        slot: usize,
        id: u64,
    ) -> WokenHandle {
        let node = Arc::new(CleanupWakeNode {
            entry: ReadyEntry { slot, id },
            queued: AtomicBool::new(false),
            shared: Arc::clone(shared),
        });
        WokenHandle {
            waker: Waker::from(Arc::clone(&node)),
            node,
        }
    }
}
