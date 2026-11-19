/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Per-worker driver poll counters, written only by the worker's owner thread
//! and readable from any thread.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::ContainedPanicScope;

/// How many buckets `WorkerPollStats::cleanup_polls_per_driver_poll` has.
pub(crate) const CLEANUP_POLL_BUCKETS: usize = 8;

/// A snapshot of one worker's driver poll counters.
///
/// Counters count from worker creation. The gauges are the worker's current
/// counts. A snapshot reads each value independently, so values that a driver
/// poll publishes together may come from different polls.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct WorkerPollStats {
    /// Polls of the worker driver before it completed shutdown.
    pub driver_polls: u64,
    /// Polls of commands that a non-hosted driver polls itself.
    pub command_polls: u64,
    /// Polls of cleanup tasks, by the driver or, under a hosted driver, by
    /// each cleanup's own task.
    pub cleanup_polls: u64,
    /// Ready-list entries that open passes drained.
    pub ready_entries: u64,
    /// Drained entries whose cleanup had finished or was not active.
    pub stale_ready_entries: u64,
    /// Open driver passes that ended with the Tokio cooperative budget of the
    /// driver's task spent.
    pub budget_exhausted_passes: u64,
    /// Non-hosted driver polls by how many cleanup polls each made: 0, 1,
    /// 2-3, 4-7, 8-15, 16-31, 32-63, and 64 or more.
    pub cleanup_polls_per_driver_poll: [u64; CLEANUP_POLL_BUCKETS],
    /// The most cleanup polls that one non-hosted driver poll has made.
    pub max_cleanup_polls_per_driver_poll: u64,
    /// The most entries that one open pass has drained.
    pub max_ready_batch: u64,
    /// Registered cleanup tasks that have not finished.
    pub cleanups_registered: u64,
    /// Activated cleanup tasks that have not finished.
    pub cleanups_active: u64,
    /// Panics that Roxygen's owned HTTP drivers on this worker contained in
    /// one exchange's callbacks. Panics that are combined into one, such as a
    /// drop that panics while another panic unwinds, count once, so this and
    /// the next two counters are lower bounds.
    pub contained_exchange_panics: u64,
    /// Panics those drivers contained in a callback of a whole connection,
    /// such as a lifecycle observer.
    pub contained_connection_panics: u64,
    /// Panics those drivers contained at driver scope, outside any narrower
    /// boundary.
    pub contained_driver_panics: u64,
}

impl WorkerPollStats {
    /// The counters and histogram that accrued since `earlier`, an earlier
    /// snapshot of the same worker. Gauges and maxima are this snapshot's.
    pub fn since(&self, earlier: &Self) -> Self {
        let mut histogram = self.cleanup_polls_per_driver_poll;
        for (bucket, before) in histogram
            .iter_mut()
            .zip(earlier.cleanup_polls_per_driver_poll)
        {
            *bucket = bucket.wrapping_sub(before);
        }
        Self {
            driver_polls: self.driver_polls.wrapping_sub(earlier.driver_polls),
            command_polls: self.command_polls.wrapping_sub(earlier.command_polls),
            cleanup_polls: self.cleanup_polls.wrapping_sub(earlier.cleanup_polls),
            ready_entries: self.ready_entries.wrapping_sub(earlier.ready_entries),
            stale_ready_entries: self
                .stale_ready_entries
                .wrapping_sub(earlier.stale_ready_entries),
            budget_exhausted_passes: self
                .budget_exhausted_passes
                .wrapping_sub(earlier.budget_exhausted_passes),
            cleanup_polls_per_driver_poll: histogram,
            contained_exchange_panics: self
                .contained_exchange_panics
                .wrapping_sub(earlier.contained_exchange_panics),
            contained_connection_panics: self
                .contained_connection_panics
                .wrapping_sub(earlier.contained_connection_panics),
            contained_driver_panics: self
                .contained_driver_panics
                .wrapping_sub(earlier.contained_driver_panics),
            ..*self
        }
    }

    /// Adds `other`, another worker's snapshot: counters, histogram and gauges
    /// sum, and maxima take the larger value.
    pub fn merge(&mut self, other: &Self) {
        self.driver_polls = self.driver_polls.wrapping_add(other.driver_polls);
        self.command_polls = self.command_polls.wrapping_add(other.command_polls);
        self.cleanup_polls = self.cleanup_polls.wrapping_add(other.cleanup_polls);
        self.ready_entries = self.ready_entries.wrapping_add(other.ready_entries);
        self.stale_ready_entries = self
            .stale_ready_entries
            .wrapping_add(other.stale_ready_entries);
        self.budget_exhausted_passes = self
            .budget_exhausted_passes
            .wrapping_add(other.budget_exhausted_passes);
        for (bucket, added) in self
            .cleanup_polls_per_driver_poll
            .iter_mut()
            .zip(other.cleanup_polls_per_driver_poll)
        {
            *bucket = bucket.wrapping_add(added);
        }
        self.max_cleanup_polls_per_driver_poll = self
            .max_cleanup_polls_per_driver_poll
            .max(other.max_cleanup_polls_per_driver_poll);
        self.max_ready_batch = self.max_ready_batch.max(other.max_ready_batch);
        self.cleanups_registered = self
            .cleanups_registered
            .wrapping_add(other.cleanups_registered);
        self.cleanups_active = self.cleanups_active.wrapping_add(other.cleanups_active);
        self.contained_exchange_panics = self
            .contained_exchange_panics
            .wrapping_add(other.contained_exchange_panics);
        self.contained_connection_panics = self
            .contained_connection_panics
            .wrapping_add(other.contained_connection_panics);
        self.contained_driver_panics = self
            .contained_driver_panics
            .wrapping_add(other.contained_driver_panics);
    }
}

/// The `cleanup_polls_per_driver_poll` bucket of a driver poll that made
/// `polls` cleanup polls.
pub(crate) fn cleanup_poll_bucket(polls: u64) -> usize {
    if polls == 0 {
        0
    } else {
        (u64::BITS - polls.leading_zeros()).min(CLEANUP_POLL_BUCKETS as u32 - 1) as usize
    }
}

/// What one driver poll did, published once when the poll returns.
#[derive(Default)]
pub(crate) struct PassStats {
    pub(crate) command_polls: u64,
    pub(crate) cleanup_polls: u64,
    pub(crate) ready_entries: u64,
    pub(crate) stale_ready_entries: u64,
    pub(crate) max_ready_batch: u64,
    pub(crate) budget_exhausted: bool,
}

/// Adds `n` with a plain load and store; only the owner thread writes.
fn add(counter: &AtomicU64, n: u64) {
    counter.store(
        counter.load(Ordering::Relaxed).wrapping_add(n),
        Ordering::Relaxed,
    );
}

fn raise(maximum: &AtomicU64, value: u64) {
    if value > maximum.load(Ordering::Relaxed) {
        maximum.store(value, Ordering::Relaxed);
    }
}

fn sub(counter: &AtomicU64, n: u64) {
    counter.store(
        counter.load(Ordering::Relaxed).wrapping_sub(n),
        Ordering::Relaxed,
    );
}

/// The counters behind `WorkerPollStats`. A driver poll that unwinds before
/// it returns publishes only its `driver_polls`.
#[repr(align(64))]
#[derive(Default)]
pub(crate) struct PollCounters {
    driver_polls: AtomicU64,
    command_polls: AtomicU64,
    cleanup_polls: AtomicU64,
    ready_entries: AtomicU64,
    stale_ready_entries: AtomicU64,
    budget_exhausted_passes: AtomicU64,
    cleanup_polls_per_driver_poll: [AtomicU64; CLEANUP_POLL_BUCKETS],
    max_cleanup_polls_per_driver_poll: AtomicU64,
    max_ready_batch: AtomicU64,
    cleanups_registered: AtomicU64,
    cleanups_active: AtomicU64,
    contained_exchange_panics: AtomicU64,
    contained_connection_panics: AtomicU64,
    contained_driver_panics: AtomicU64,
}

impl PollCounters {
    pub(crate) fn record_driver_poll(&self) {
        add(&self.driver_polls, 1);
    }

    /// Publishes a driver poll; `inline` drivers also record its histogram
    /// bucket and maximum.
    pub(crate) fn record_pass(&self, pass: &PassStats, inline: bool) {
        add(&self.command_polls, pass.command_polls);
        add(&self.cleanup_polls, pass.cleanup_polls);
        if pass.budget_exhausted {
            add(&self.budget_exhausted_passes, 1);
        }
        if inline {
            add(
                &self.cleanup_polls_per_driver_poll[cleanup_poll_bucket(pass.cleanup_polls)],
                1,
            );
            raise(&self.max_cleanup_polls_per_driver_poll, pass.cleanup_polls);
        }
        if pass.ready_entries != 0 {
            add(&self.ready_entries, pass.ready_entries);
            add(&self.stale_ready_entries, pass.stale_ready_entries);
            raise(&self.max_ready_batch, pass.max_ready_batch);
        }
    }

    /// Counts a hosted cleanup task's poll of its cleanup.
    pub(crate) fn record_hosted_cleanup_poll(&self) {
        add(&self.cleanup_polls, 1);
    }

    pub(crate) fn cleanup_registered(&self) {
        add(&self.cleanups_registered, 1);
    }

    pub(crate) fn cleanup_activated(&self) {
        add(&self.cleanups_active, 1);
    }

    /// Removes a finished cleanup from the gauges that counted it.
    pub(crate) fn cleanup_finished(&self, registered: bool, activated: bool) {
        if registered {
            sub(&self.cleanups_registered, 1);
        }
        if activated {
            sub(&self.cleanups_active, 1);
        }
    }

    pub(crate) fn record_contained_panic(&self, scope: ContainedPanicScope) {
        let counter = match scope {
            ContainedPanicScope::Exchange => &self.contained_exchange_panics,
            ContainedPanicScope::Connection => &self.contained_connection_panics,
            ContainedPanicScope::Driver => &self.contained_driver_panics,
        };
        add(counter, 1);
    }

    pub(crate) fn snapshot(&self) -> WorkerPollStats {
        WorkerPollStats {
            driver_polls: self.driver_polls.load(Ordering::Relaxed),
            command_polls: self.command_polls.load(Ordering::Relaxed),
            cleanup_polls: self.cleanup_polls.load(Ordering::Relaxed),
            ready_entries: self.ready_entries.load(Ordering::Relaxed),
            stale_ready_entries: self.stale_ready_entries.load(Ordering::Relaxed),
            budget_exhausted_passes: self.budget_exhausted_passes.load(Ordering::Relaxed),
            cleanup_polls_per_driver_poll: std::array::from_fn(|bucket| {
                self.cleanup_polls_per_driver_poll[bucket].load(Ordering::Relaxed)
            }),
            max_cleanup_polls_per_driver_poll: self
                .max_cleanup_polls_per_driver_poll
                .load(Ordering::Relaxed),
            max_ready_batch: self.max_ready_batch.load(Ordering::Relaxed),
            cleanups_registered: self.cleanups_registered.load(Ordering::Relaxed),
            cleanups_active: self.cleanups_active.load(Ordering::Relaxed),
            contained_exchange_panics: self.contained_exchange_panics.load(Ordering::Relaxed),
            contained_connection_panics: self.contained_connection_panics.load(Ordering::Relaxed),
            contained_driver_panics: self.contained_driver_panics.load(Ordering::Relaxed),
        }
    }
}
