/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime counters and timing histograms.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Upper bounds, in microseconds, of every histogram's buckets. A final bucket holds the rest.
pub const HISTOGRAM_BUCKET_UPPER_BOUNDS_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

#[derive(Debug, Default)]
struct Histogram {
    counts: [AtomicU64; HISTOGRAM_BUCKET_UPPER_BOUNDS_US.len() + 1],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Histogram {
    fn record(&self, sample: Duration) {
        let micros = u64::try_from(sample.as_micros()).unwrap_or(u64::MAX);
        let bucket = HISTOGRAM_BUCKET_UPPER_BOUNDS_US
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(HISTOGRAM_BUCKET_UPPER_BOUNDS_US.len());
        self.counts[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            bucket_upper_bounds_us: &HISTOGRAM_BUCKET_UPPER_BOUNDS_US,
            counts: self
                .counts
                .iter()
                .map(|count| count.load(Ordering::Relaxed))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
            sum_us: self.sum_us.load(Ordering::Relaxed),
        }
    }
}

/// Counters, gauges, and histograms that exchanges and the runtime record into.
///
/// Each metric covers one runtime, or the exchanges of one direct caller of `exchange`, and does
/// not carry a per-request, session, or stream label.
#[derive(Debug, Default)]
pub struct ExplicitStats {
    sends: AtomicU64,
    restarts: AtomicU64,
    failed_restarts: AtomicU64,
    panics: AtomicU64,
    overloads: AtomicU64,
    worker_losses: AtomicU64,
    caller_cancellations: AtomicU64,
    truncated_bodies: AtomicU64,
    dropped_response_headers: AtomicU64,
    unconfirmed_retirements: AtomicU64,
    sends_in_flight: AtomicU64,
    pending_teardowns: AtomicU64,
    restarts_joining: AtomicU64,
    queued: Histogram,
    tcp_connect: Histogram,
    tls_handshake: Histogram,
    first_byte: Histogram,
    close_to_retirement: Histogram,
}

impl ExplicitStats {
    /// Statistics with every metric at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// A point-in-time copy of every metric.
    pub fn snapshot(&self) -> RuntimeStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        RuntimeStats {
            sends: load(&self.sends),
            restarts: load(&self.restarts),
            failed_restarts: load(&self.failed_restarts),
            panics: load(&self.panics),
            overloads: load(&self.overloads),
            worker_losses: load(&self.worker_losses),
            caller_cancellations: load(&self.caller_cancellations),
            truncated_bodies: load(&self.truncated_bodies),
            dropped_response_headers: load(&self.dropped_response_headers),
            unconfirmed_retirements: load(&self.unconfirmed_retirements),
            sends_in_flight: load(&self.sends_in_flight),
            pending_teardowns: load(&self.pending_teardowns),
            restarts_joining: load(&self.restarts_joining),
            queued_us: self.queued.snapshot(),
            tcp_connect_us: self.tcp_connect.snapshot(),
            tls_handshake_us: self.tls_handshake.snapshot(),
            first_byte_us: self.first_byte.snapshot(),
            close_to_retirement_us: self.close_to_retirement.snapshot(),
        }
    }

    pub(crate) fn count_send(&self) {
        self.sends.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_restart(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_failed_restart(&self) {
        self.failed_restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_panic(&self) {
        self.panics.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_overload(&self) {
        self.overloads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_worker_loss(&self) {
        self.worker_losses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_caller_cancellation(&self) {
        self.caller_cancellations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_truncated_body(&self) {
        self.truncated_bodies.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_dropped_headers(&self, dropped: usize) {
        let dropped = u64::try_from(dropped).unwrap_or(u64::MAX);
        self.dropped_response_headers
            .fetch_add(dropped, Ordering::Relaxed);
    }

    pub(crate) fn send_admitted(&self) {
        self.sends_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn send_resolved(&self) {
        self.sends_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn restart_joining(&self) {
        self.restarts_joining.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn restart_joined(&self) {
        self.restarts_joining.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_queued(&self, sample: Duration) {
        self.queued.record(sample);
    }

    pub(crate) fn record_tcp_connect(&self, sample: Duration) {
        self.tcp_connect.record(sample);
    }

    pub(crate) fn record_tls_handshake(&self, sample: Duration) {
        self.tls_handshake.record(sample);
    }

    pub(crate) fn record_first_byte(&self, sample: Duration) {
        self.first_byte.record(sample);
    }
}

/// One adopted connection whose transport has not yet retired.
///
/// The pending-teardown gauge rises at construction and falls exactly once, at drop. A drop
/// without [`PendingTeardown::retired`] counts an unconfirmed retirement.
pub(crate) struct PendingTeardown {
    stats: Arc<ExplicitStats>,
    confirmed: bool,
}

impl PendingTeardown {
    pub(crate) fn new(stats: Arc<ExplicitStats>) -> Self {
        stats.pending_teardowns.fetch_add(1, Ordering::Relaxed);
        Self {
            stats,
            confirmed: false,
        }
    }

    /// Confirms retirement, recording the time since `close_when_idle` when it was called.
    pub(crate) fn retired(mut self, since_close: Option<Duration>) {
        self.confirmed = true;
        if let Some(sample) = since_close {
            self.stats.close_to_retirement.record(sample);
        }
    }
}

impl Drop for PendingTeardown {
    fn drop(&mut self) {
        if !self.confirmed {
            self.stats
                .unconfirmed_retirements
                .fetch_add(1, Ordering::Relaxed);
        }
        self.stats.pending_teardowns.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One adopted connection of a client made with `ExplicitClient::with_teardown_guard`. The
/// stream owns the guard, so that it drops when the stream is destroyed, and a cleanup task does
/// not wait for it.
///
/// The pending-teardown gauge rises at construction and falls exactly once, at drop. A drop while
/// the thread panics, as when the stream's own destruction panics, counts an unconfirmed
/// retirement.
pub(crate) struct TeardownGuard(Arc<ExplicitStats>);

impl TeardownGuard {
    pub(crate) fn new(stats: &Arc<ExplicitStats>) -> Self {
        stats.pending_teardowns.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(stats))
    }
}

impl Drop for TeardownGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0
                .unconfirmed_retirements
                .fetch_add(1, Ordering::Relaxed);
        }
        self.0.pending_teardowns.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A point-in-time copy of [`ExplicitStats`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeStats {
    /// Count of `ExplicitClient::send` calls.
    pub sends: u64,
    /// Count of slot restarts that published a new worker.
    pub restarts: u64,
    /// Count of admitted slot restarts that did not publish a worker.
    pub failed_restarts: u64,
    /// Count of exchange panics caught on a worker, including those of sends already dropped.
    pub panics: u64,
    /// Count of sends that returned `ErrorKind::Overloaded`.
    pub overloads: u64,
    /// Count of sends that returned `ErrorKind::WorkerLost`.
    pub worker_losses: u64,
    /// Count of send futures that were dropped before resolving and cancelled their exchange.
    pub caller_cancellations: u64,
    /// Count of response bodies that ended as `BodyOutcome::Truncated`, including those of sends
    /// already dropped.
    pub truncated_bodies: u64,
    /// Count of response headers that `http::HeaderMap` could not represent, including those of
    /// sends already dropped.
    pub dropped_response_headers: u64,
    /// Count of adopted connections whose transport retirement was not confirmed. For a client
    /// with a teardown guard, the count of its streams destroyed while their thread panicked.
    pub unconfirmed_retirements: u64,
    /// Gauge of sends admitted and not yet resolved or dropped.
    pub sends_in_flight: u64,
    /// Gauge of adopted connections whose transport has not yet retired. A client with a
    /// teardown guard counts each of its streams from just before adoption until the stream is
    /// destroyed.
    pub pending_teardowns: u64,
    /// Gauge of published restarts still joining their old worker.
    pub restarts_joining: u64,
    /// Microseconds from `send` to the exchange's start on its worker or, for a send whose
    /// deadline passed before that start, to the send's expiry (`Timings::queued`).
    pub queued_us: HistogramSnapshot,
    /// Microseconds from the exchange's start to an established TCP connection.
    pub tcp_connect_us: HistogramSnapshot,
    /// Microseconds from the exchange's start to a completed TLS handshake.
    pub tls_handshake_us: HistogramSnapshot,
    /// Microseconds from the exchange's start to the first response head.
    pub first_byte_us: HistogramSnapshot,
    /// Microseconds from `close_when_idle` to transport retirement. A client with a teardown
    /// guard does not record it.
    pub close_to_retirement_us: HistogramSnapshot,
}

/// Fixed-bucket histogram of microsecond samples.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HistogramSnapshot {
    /// The upper bound of each bucket, [`HISTOGRAM_BUCKET_UPPER_BOUNDS_US`].
    pub bucket_upper_bounds_us: &'static [u64],
    /// One count per bucket, plus a final count of samples above every bound.
    pub counts: Vec<u64>,
    /// The number of samples.
    pub count: u64,
    /// The sum of the samples, in microseconds.
    pub sum_us: u64,
}
