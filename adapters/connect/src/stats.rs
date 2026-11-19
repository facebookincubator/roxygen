/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime counters, gauges, and latency summaries without per-route labels.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// A point-in-time copy of one runtime's metrics.
///
/// Counters are monotonic runtime totals; gauges return to zero after shutdown. Metrics are not
/// labeled by route, session, stream, or tunnel.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConnectStats {
    /// `connect()` calls, including those that validation, admission, or a closed runtime failed.
    pub connects: u64,
    /// Connects that took an admission permit.
    pub admitted: u64,
    /// Connects failed with `Overloaded`, because admission was full or the pool rejected a waiter.
    pub overloaded: u64,
    /// Connects failed with `RouteCapacity`.
    pub route_capacity: u64,
    /// Connects failed with `InvalidRequest`.
    pub invalid_request: u64,
    /// Connects failed with `AcquireTimeout`, once per `connect()` whatever its retries.
    pub acquire_timeouts: u64,
    /// Connects failed with `ResponseTimeout`, once per `connect()`.
    pub response_timeouts: u64,
    /// Session factory invocations; each dial makes one connection attempt.
    pub dial_attempts: u64,
    /// Dials that ended without a session for any reason but shutdown.
    pub dial_failures: u64,
    /// The `dial_failures` whose connector returned `DialError::H2NotNegotiated`.
    pub h2_not_negotiated: u64,
    /// Connects failed immediately by a cached h2-unsupported entry, without dialing.
    pub negative_cache_hits: u64,
    /// Connects failed with `Rejected`, once per `connect()`.
    pub rejected: u64,
    /// Retry attempts started, established or not.
    pub retries: u64,
    /// Connects that returned a `PendingTunnel`.
    pub tunnels_established: u64,
    /// Pending tunnels whose relay reached the worker.
    pub tunnels_relayed: u64,
    /// Established tunnels that the peer ended: a bridge `Tunnel` that failed with
    /// `ConnectionReset` or a relay that ended `PeerReset`, also when the stream ended before the
    /// choice. A PING probe drop reported by the stream's read is not counted. A probe drop after
    /// the download's END_STREAM is counted, including one after both directions ended while flow
    /// control held the upload.
    pub tunnels_reset_by_peer: u64,
    /// Established tunnels whose stream the adapter reset before it was wire-complete: a dropped
    /// `PendingTunnel` or `Tunnel`, a dropped relay future, a relay's application I/O error, a
    /// job-drop guard, or forced shutdown. A stream reset before `connect()` returned its pending
    /// tunnel is not counted.
    pub tunnels_aborted: u64,
    /// Sessions the factory attached.
    pub sessions_opened: u64,
    /// Established tunnels whose session had carried an earlier CONNECT stream.
    pub streams_on_reused_session: u64,
    /// HTTP/2 PING probe timeouts that sessions recorded.
    pub ping_timeouts: u64,
    /// Sessions that started draining before shutdown began, once each: after a peer GOAWAY, at
    /// max-age expiry, or when their route retired after `idle_timeout`.
    pub session_drains: u64,
    /// Session failures that pools reported as they joined their sessions, at route retirement
    /// or shutdown.
    pub shutdown_errors: u64,
    /// Gauge of pending tunnels, bridges, and relays whose work on the worker has not ended. A
    /// bridge's task ends once its stream is closed on the wire or reset, and a `Tunnel` that the
    /// application still holds after that keeps its admission permit but is not counted. A relay
    /// stops counting before its outcome resolves.
    pub active_tunnels: u64,
    /// Gauge of live routes, retiring ones included.
    pub routes: u64,
    /// Gauge of attached sessions not yet closed, draining ones included.
    pub open_sessions: u64,
    /// Gauge of admitted connects that do not hold a stream reservation: queued for the worker,
    /// waiting in the pool, or waiting on a dial.
    pub waiters: u64,
    /// Admission to stream reservation, one sample per reservation.
    pub acquire_latency: LatencySummary,
    /// Request commit to final response head, one sample per head.
    pub response_latency: LatencySummary,
}

/// Count, sum, and maximum of latency samples, in microseconds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LatencySummary {
    pub count: u64,
    pub sum_us: u64,
    pub max_us: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Counter(AtomicU64);

impl Counter {
    pub(crate) fn add(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub(crate) struct Gauge(AtomicU64);

impl Gauge {
    pub(crate) fn increment(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decrement(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub(crate) struct Summary {
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl Summary {
    pub(crate) fn record(&self, sample: Duration) {
        let micros = u64::try_from(sample.as_micros()).unwrap_or(u64::MAX);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
        self.max_us.fetch_max(micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencySummary {
        LatencySummary {
            count: self.count.load(Ordering::Relaxed),
            sum_us: self.sum_us.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }
}

/// The metrics that the client handles and the worker record into.
#[derive(Debug, Default)]
pub(crate) struct Recorder {
    pub(crate) connects: Counter,
    pub(crate) admitted: Counter,
    pub(crate) overloaded: Counter,
    pub(crate) route_capacity: Counter,
    pub(crate) invalid_request: Counter,
    pub(crate) acquire_timeouts: Counter,
    pub(crate) response_timeouts: Counter,
    pub(crate) dial_attempts: Counter,
    pub(crate) dial_failures: Counter,
    pub(crate) h2_not_negotiated: Counter,
    pub(crate) negative_cache_hits: Counter,
    pub(crate) rejected: Counter,
    pub(crate) retries: Counter,
    pub(crate) tunnels_established: Counter,
    pub(crate) tunnels_relayed: Counter,
    pub(crate) tunnels_reset_by_peer: Counter,
    pub(crate) tunnels_aborted: Counter,
    pub(crate) sessions_opened: Counter,
    pub(crate) streams_on_reused_session: Counter,
    pub(crate) ping_timeouts: Counter,
    pub(crate) session_drains: Counter,
    pub(crate) shutdown_errors: Counter,
    pub(crate) active_tunnels: Gauge,
    pub(crate) routes: Gauge,
    pub(crate) open_sessions: Gauge,
    pub(crate) waiters: Gauge,
    pub(crate) acquire_latency: Summary,
    pub(crate) response_latency: Summary,
}

impl Recorder {
    pub(crate) fn snapshot(&self) -> ConnectStats {
        ConnectStats {
            connects: self.connects.get(),
            admitted: self.admitted.get(),
            overloaded: self.overloaded.get(),
            route_capacity: self.route_capacity.get(),
            invalid_request: self.invalid_request.get(),
            acquire_timeouts: self.acquire_timeouts.get(),
            response_timeouts: self.response_timeouts.get(),
            dial_attempts: self.dial_attempts.get(),
            dial_failures: self.dial_failures.get(),
            h2_not_negotiated: self.h2_not_negotiated.get(),
            negative_cache_hits: self.negative_cache_hits.get(),
            rejected: self.rejected.get(),
            retries: self.retries.get(),
            tunnels_established: self.tunnels_established.get(),
            tunnels_relayed: self.tunnels_relayed.get(),
            tunnels_reset_by_peer: self.tunnels_reset_by_peer.get(),
            tunnels_aborted: self.tunnels_aborted.get(),
            sessions_opened: self.sessions_opened.get(),
            streams_on_reused_session: self.streams_on_reused_session.get(),
            ping_timeouts: self.ping_timeouts.get(),
            session_drains: self.session_drains.get(),
            shutdown_errors: self.shutdown_errors.get(),
            active_tunnels: self.active_tunnels.get(),
            routes: self.routes.get(),
            open_sessions: self.open_sessions.get(),
            waiters: self.waiters.get(),
            acquire_latency: self.acquire_latency.snapshot(),
            response_latency: self.response_latency.snapshot(),
        }
    }
}

/// One unit of a gauge, released exactly once when dropped.
pub(crate) struct GaugeHold {
    stats: Arc<Recorder>,
    gauge: fn(&Recorder) -> &Gauge,
}

impl GaugeHold {
    pub(crate) fn new(stats: Arc<Recorder>, gauge: fn(&Recorder) -> &Gauge) -> Self {
        gauge(&stats).increment();
        Self { stats, gauge }
    }
}

impl Drop for GaugeHold {
    fn drop(&mut self) {
        (self.gauge)(&self.stats).decrement();
    }
}
