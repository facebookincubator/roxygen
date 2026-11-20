/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Per-worker driver poll counters, readable from any thread. The worker's
//! owner thread writes all of them except the queued-command maximum, which
//! enqueuers raise under the queue lock, and a `ManagedWorker`'s
//! blocking-thread counts, which its blocking-pool threads update.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::ContainedPanicScope;

/// How many buckets each per-driver-poll histogram in `WorkerPollStats` has.
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
    /// Registered cleanup tasks that have not finished. The ciphertext writer
    /// of each open Roxygen TLS connection counts here.
    pub cleanups_registered: u64,
    /// Activated cleanup tasks that have not finished. The ciphertext writer
    /// of each open Roxygen TLS connection counts here.
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
    /// Commands whose job the driver has created and not yet dropped, whether
    /// the driver polls it itself or a host task runs it. Queued commands do
    /// not count until a driver poll takes them.
    pub commands_live: u64,
    /// Non-hosted driver polls by how many command polls each made, in the
    /// buckets of `cleanup_polls_per_driver_poll`.
    pub command_polls_per_driver_poll: [u64; CLEANUP_POLL_BUCKETS],
    /// The most command polls that one non-hosted driver poll has made.
    pub max_command_polls_per_driver_poll: u64,
    /// The most commands that were queued at once. Every admitted command
    /// counts, whichever `WorkerHandle` method submitted it.
    pub max_queued_commands: u64,
    /// Blocking-pool threads of a `ManagedWorker`'s runtime, counted from
    /// their start hook to their stop hook. Tokio stops counting an exiting
    /// thread before its stop hook runs, so this can briefly over-count by
    /// the threads that are exiting: a replacement thread that starts in that
    /// window can bring it to, or past, the pool's `max_threads` while Tokio
    /// runs fewer threads. Always 0 for other workers.
    ///
    /// This, the next maximum and the cap-hit counter are reported-only
    /// diagnostics, not a saturation detector: at its cap Tokio queues
    /// blocking work without starting a thread, and its queue depth is not
    /// observable here.
    pub blocking_threads_live: u64,
    /// The most blocking-pool threads that were live at once, by the gauge
    /// above, so it includes any exiting threads that the gauge over-counted.
    /// A worker whose maximum reached (or, through the exit race, briefly
    /// reached) its `max_threads` may have been saturated, so its blocking
    /// work may have queued.
    pub max_blocking_threads_live: u64,
    /// Blocking-thread starts that brought the live count to, or past, the
    /// pool's `max_threads`, or 0 when `max_threads` is unset. Through the
    /// exit race a start can briefly reach the cap while Tokio runs fewer
    /// threads, so a hit does not prove that the pool was saturated. A pool
    /// that stays saturated counts no further hits, and `max_threads` short
    /// concurrent tasks count one without queueing.
    pub blocking_cap_hits: u64,
}

impl WorkerPollStats {
    /// The counters and histograms that accrued since `earlier`, an earlier
    /// snapshot of the same worker. Gauges and maxima are this snapshot's.
    pub fn since(&self, earlier: &Self) -> Self {
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
            cleanup_polls_per_driver_poll: histogram_since(
                self.cleanup_polls_per_driver_poll,
                earlier.cleanup_polls_per_driver_poll,
            ),
            command_polls_per_driver_poll: histogram_since(
                self.command_polls_per_driver_poll,
                earlier.command_polls_per_driver_poll,
            ),
            contained_exchange_panics: self
                .contained_exchange_panics
                .wrapping_sub(earlier.contained_exchange_panics),
            contained_connection_panics: self
                .contained_connection_panics
                .wrapping_sub(earlier.contained_connection_panics),
            contained_driver_panics: self
                .contained_driver_panics
                .wrapping_sub(earlier.contained_driver_panics),
            blocking_cap_hits: self
                .blocking_cap_hits
                .wrapping_sub(earlier.blocking_cap_hits),
            ..*self
        }
    }

    /// Adds `other`, another worker's snapshot: counters, histograms and gauges
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
        add_histogram(
            &mut self.cleanup_polls_per_driver_poll,
            other.cleanup_polls_per_driver_poll,
        );
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
        self.commands_live = self.commands_live.wrapping_add(other.commands_live);
        add_histogram(
            &mut self.command_polls_per_driver_poll,
            other.command_polls_per_driver_poll,
        );
        self.max_command_polls_per_driver_poll = self
            .max_command_polls_per_driver_poll
            .max(other.max_command_polls_per_driver_poll);
        self.max_queued_commands = self.max_queued_commands.max(other.max_queued_commands);
        self.blocking_threads_live = self
            .blocking_threads_live
            .wrapping_add(other.blocking_threads_live);
        self.max_blocking_threads_live = self
            .max_blocking_threads_live
            .max(other.max_blocking_threads_live);
        self.blocking_cap_hits = self.blocking_cap_hits.wrapping_add(other.blocking_cap_hits);
    }
}

fn histogram_since(
    mut later: [u64; CLEANUP_POLL_BUCKETS],
    earlier: [u64; CLEANUP_POLL_BUCKETS],
) -> [u64; CLEANUP_POLL_BUCKETS] {
    for (bucket, before) in later.iter_mut().zip(earlier) {
        *bucket = bucket.wrapping_sub(before);
    }
    later
}

fn add_histogram(sum: &mut [u64; CLEANUP_POLL_BUCKETS], added: [u64; CLEANUP_POLL_BUCKETS]) {
    for (bucket, count) in sum.iter_mut().zip(added) {
        *bucket = bucket.wrapping_add(count);
    }
}

/// The histogram bucket of a driver poll that made `polls` cleanup polls, or
/// `polls` command polls.
pub(crate) fn poll_bucket(polls: u64) -> usize {
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

/// The queued-command maximum, on its own cache line because enqueuers on any
/// thread raise it.
#[repr(align(64))]
#[derive(Default)]
struct QueuedCommandsMaximum(AtomicU64);

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
    commands_live: AtomicU64,
    command_polls_per_driver_poll: [AtomicU64; CLEANUP_POLL_BUCKETS],
    max_command_polls_per_driver_poll: AtomicU64,
    max_queued_commands: QueuedCommandsMaximum,
    blocking_threads: OnceLock<Arc<BlockingThreadCounters>>,
}

/// The blocking-pool thread counts of a worker's runtime, which the pool's
/// thread start and stop hooks update on the blocking threads.
pub(crate) struct BlockingThreadCounters {
    max_threads: Option<NonZeroUsize>,
    live: AtomicU64,
    max_live: AtomicU64,
    cap_hits: AtomicU64,
}

impl BlockingThreadCounters {
    /// Counts a blocking thread that started, and a cap hit if the live count
    /// reaches `max_threads`.
    pub(crate) fn thread_started(&self) {
        let live = self.live.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        self.max_live.fetch_max(live, Ordering::Relaxed);
        if self
            .max_threads
            .is_some_and(|max_threads| live >= max_threads.get() as u64)
        {
            self.cap_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Uncounts a blocking thread as it exits.
    pub(crate) fn thread_stopped(&self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

impl PollCounters {
    pub(crate) fn record_driver_poll(&self) {
        add(&self.driver_polls, 1);
    }

    /// Publishes a driver poll; `inline` drivers also record its histogram
    /// buckets and maxima.
    pub(crate) fn record_pass(&self, pass: &PassStats, inline: bool) {
        add(&self.command_polls, pass.command_polls);
        add(&self.cleanup_polls, pass.cleanup_polls);
        if pass.budget_exhausted {
            add(&self.budget_exhausted_passes, 1);
        }
        if inline {
            add(
                &self.cleanup_polls_per_driver_poll[poll_bucket(pass.cleanup_polls)],
                1,
            );
            raise(&self.max_cleanup_polls_per_driver_poll, pass.cleanup_polls);
            add(
                &self.command_polls_per_driver_poll[poll_bucket(pass.command_polls)],
                1,
            );
            raise(&self.max_command_polls_per_driver_poll, pass.command_polls);
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

    pub(crate) fn command_started(&self) {
        add(&self.commands_live, 1);
    }

    pub(crate) fn command_finished(&self) {
        sub(&self.commands_live, 1);
    }

    /// Raises the queued-command maximum to `queued`. Callers hold the queue
    /// lock, and the load skips the read-modify-write while the maximum holds.
    pub(crate) fn record_queued_commands(&self, queued: usize) {
        let queued = queued as u64;
        let maximum = &self.max_queued_commands.0;
        if queued > maximum.load(Ordering::Relaxed) {
            maximum.fetch_max(queued, Ordering::Relaxed);
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

    /// The blocking-thread counters that this worker's snapshots report,
    /// created on the first call with the runtime's `max_threads`.
    pub(crate) fn count_blocking_threads(
        &self,
        max_threads: Option<NonZeroUsize>,
    ) -> Arc<BlockingThreadCounters> {
        Arc::clone(self.blocking_threads.get_or_init(|| {
            Arc::new(BlockingThreadCounters {
                max_threads,
                live: AtomicU64::new(0),
                max_live: AtomicU64::new(0),
                cap_hits: AtomicU64::new(0),
            })
        }))
    }

    pub(crate) fn snapshot(&self) -> WorkerPollStats {
        let blocking = |counter: fn(&BlockingThreadCounters) -> &AtomicU64| {
            self.blocking_threads
                .get()
                .map_or(0, |counters| counter(counters).load(Ordering::Relaxed))
        };
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
            commands_live: self.commands_live.load(Ordering::Relaxed),
            command_polls_per_driver_poll: std::array::from_fn(|bucket| {
                self.command_polls_per_driver_poll[bucket].load(Ordering::Relaxed)
            }),
            max_command_polls_per_driver_poll: self
                .max_command_polls_per_driver_poll
                .load(Ordering::Relaxed),
            max_queued_commands: self.max_queued_commands.0.load(Ordering::Relaxed),
            blocking_threads_live: blocking(|counters| &counters.live),
            max_blocking_threads_live: blocking(|counters| &counters.max_live),
            blocking_cap_hits: blocking(|counters| &counters.cap_hits),
        }
    }
}
