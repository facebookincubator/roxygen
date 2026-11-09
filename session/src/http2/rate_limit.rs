use std::time::Duration;
use std::time::Instant;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimitConfig {
    pub maximum: u64,
    pub interval: Duration,
    pub minimum: u64,
}

impl RateLimitConfig {
    pub const fn new(maximum: u64, interval: Duration, minimum: u64) -> Self {
        Self {
            maximum,
            interval,
            minimum,
        }
    }

    /// Whether the effective limit, the larger of `maximum` and `minimum`, never trips.
    pub const fn is_unbounded(&self) -> bool {
        let limit = if self.maximum > self.minimum {
            self.maximum
        } else {
            self.minimum
        };
        limit == u64::MAX
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Http2RateLimitConfig {
    pub control_frames: RateLimitConfig,
    pub header_frames: RateLimitConfig,
    pub resets: RateLimitConfig,
    pub direct_errors: RateLimitConfig,
}

impl Http2RateLimitConfig {
    /// Whether no category can trip.
    pub const fn is_unbounded(&self) -> bool {
        self.control_frames.is_unbounded()
            && self.header_frames.is_unbounded()
            && self.resets.is_unbounded()
            && self.direct_errors.is_unbounded()
    }
}

impl Default for Http2RateLimitConfig {
    fn default() -> Self {
        Self {
            control_frames: RateLimitConfig::new(50_000, Duration::from_millis(100), 100),
            header_frames: RateLimitConfig::new(50_000, Duration::from_millis(100), 100),
            resets: RateLimitConfig::new(1_000, Duration::from_millis(100), 100),
            direct_errors: RateLimitConfig::new(100, Duration::from_millis(100), 50),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateLimitKind {
    ControlFrames,
    HeaderFrames,
    Resets,
    DirectErrors,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimitSample {
    pub kind: RateLimitKind,
    pub count: u64,
    pub limit: u64,
    pub breached: bool,
}

#[derive(Clone, Copy, Debug)]
struct Counter {
    config: RateLimitConfig,
    started: Option<Instant>,
    count: u64,
    breached: bool,
}

impl Counter {
    fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            started: None,
            count: 0,
            breached: false,
        }
    }

    fn record(&mut self, now: Instant) -> Option<RateLimitSample> {
        let rolled = self
            .started
            .filter(|started| now.duration_since(*started) >= self.config.interval)
            .map(|_| self.roll());
        self.started.get_or_insert(now);
        self.count = self.count.saturating_add(1);
        if self.count > self.config.maximum.max(self.config.minimum) {
            self.breached = true;
        }
        if self.breached {
            Some(RateLimitSample {
                kind: RateLimitKind::ControlFrames,
                count: self.count,
                limit: self.config.maximum.max(self.config.minimum),
                breached: true,
            })
        } else {
            rolled
        }
    }

    fn expire(&mut self, now: Instant) -> Option<RateLimitSample> {
        self.started
            .filter(|started| now.duration_since(*started) >= self.config.interval)
            .map(|_| self.roll())
    }

    fn roll(&mut self) -> RateLimitSample {
        let sample = RateLimitSample {
            kind: RateLimitKind::ControlFrames,
            count: self.count,
            limit: self.config.maximum.max(self.config.minimum),
            breached: self.breached,
        };
        self.started = None;
        self.count = 0;
        self.breached = false;
        sample
    }

    fn deadline(&self) -> Option<Instant> {
        self.started.map(|started| started + self.config.interval)
    }
}

pub struct Http2RateLimiter {
    counters: [Counter; 4],
}

impl Http2RateLimiter {
    pub fn new(config: Http2RateLimitConfig, _now: Instant) -> Self {
        Self {
            counters: [
                Counter::new(config.control_frames),
                Counter::new(config.header_frames),
                Counter::new(config.resets),
                Counter::new(config.direct_errors),
            ],
        }
    }

    pub(crate) fn is_unbounded(&self, kind: RateLimitKind) -> bool {
        self.counters[kind as usize].config.is_unbounded()
    }

    pub fn record(&mut self, kind: RateLimitKind, now: Instant) -> (bool, Option<RateLimitSample>) {
        let index = kind as usize;
        let sample = self.counters[index].record(now).map(|mut sample| {
            sample.kind = kind;
            sample
        });
        (self.counters[index].breached, sample)
    }

    pub fn expire(&mut self, now: Instant) -> Vec<RateLimitSample> {
        let kinds = [
            RateLimitKind::ControlFrames,
            RateLimitKind::HeaderFrames,
            RateLimitKind::Resets,
            RateLimitKind::DirectErrors,
        ];
        self.counters
            .iter_mut()
            .zip(kinds)
            .filter_map(|(counter, kind)| {
                counter.expire(now).map(|mut sample| {
                    sample.kind = kind;
                    sample
                })
            })
            .collect()
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.counters.iter().filter_map(Counter::deadline).min()
    }
}

/// The connection error a breach of `kind` raises, with `UniplexSession`'s messages.
#[cold]
#[inline(never)]
pub(crate) fn rate_limit_exceeded(kind: RateLimitKind) -> HttpError {
    HttpError::with_message(
        HttpErrorCode::CANCEL,
        match kind {
            RateLimitKind::ControlFrames => b"HTTP/2 control-frame rate limit exceeded".as_slice(),
            RateLimitKind::HeaderFrames => b"HTTP/2 header rate limit exceeded",
            RateLimitKind::Resets => b"HTTP/2 reset rate limit exceeded",
            RateLimitKind::DirectErrors => b"HTTP/2 direct-error rate limit exceeded",
        },
    )
}
