//! HTTP/2-specific session state, configuration, scheduling, and accounting.

pub(crate) mod protocol;
pub mod rate_limit;
pub mod scheduler;
pub mod stats;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::collections::btree_map::Entry;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use roxygen_codec::CodecSetting;
use roxygen_core::flow_control::Window;
use roxygen_core::ids::StreamId;
use roxygen_core::priority::HttpPriority;
use roxygen_http2::hpack::HeaderCodecStats;
use roxygen_http2::hpack::HeaderIndexingStrategy;
use roxygen_runtime::host::Host;

use self::rate_limit::Http2RateLimitConfig;
use self::rate_limit::Http2RateLimiter;
use self::rate_limit::RateLimitKind;
use self::rate_limit::RateLimitSample;
use self::scheduler::Http2PriorityScheduler;
use self::stats::Http2SessionStats;
use crate::experimental::PingProbeConfig;
use crate::uniplex::BodySourcePublications;
use crate::uniplex::EgressTransferKey;
use crate::uniplex::PingProber;
use crate::uniplex::QueuedEgressTransfer;

pub const DEFAULT_STREAM_WINDOW: u32 = 65_535;
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 100;

/// Selects how the session derives its inbound per-stream flow-control window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StreamReceiveWindowMode {
    /// Mirror the final advertised `INITIAL_WINDOW_SIZE` setting.
    #[default]
    MirrorAdvertisedSettings,
    /// Keep the explicitly configured session window.
    PreserveConfigured,
}

#[derive(Clone)]
pub struct Http2SessionConfig {
    pub settings: Vec<CodecSetting>,
    pub connection_receive_window: u32,
    pub stream_receive_window: u32,
    pub stream_receive_window_mode: StreamReceiveWindowMode,
    pub max_concurrent_outgoing_streams: u32,
    pub rate_limits: Http2RateLimitConfig,
    pub stats: Option<Rc<dyn Http2SessionStats>>,
    pub header_codec_stats: Option<Rc<dyn HeaderCodecStats>>,
    pub header_indexing_strategy: Option<Rc<dyn HeaderIndexingStrategy>>,
    pub double_goaway_drain: bool,
    pub goaway_timeout: Duration,
    /// Experimental PING probes (`DIV-024`); `None`, the default, sends none.
    pub ping_probes: Option<PingProbeConfig>,
}

impl Default for Http2SessionConfig {
    fn default() -> Self {
        Self {
            settings: Vec::new(),
            connection_receive_window: DEFAULT_STREAM_WINDOW,
            stream_receive_window: DEFAULT_STREAM_WINDOW,
            stream_receive_window_mode: StreamReceiveWindowMode::default(),
            max_concurrent_outgoing_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            rate_limits: Http2RateLimitConfig::default(),
            stats: None,
            header_codec_stats: None,
            header_indexing_strategy: None,
            double_goaway_drain: true,
            goaway_timeout: Duration::from_secs(1),
            ping_probes: None,
        }
    }
}

pub(crate) struct Http2StreamState {
    pub send_window: Window,
    pub priority: HttpPriority,
    pub parent: Option<StreamId>,
    pub promised: bool,
    pub stalled: bool,
    pub egress_work: VecDeque<QueuedEgressTransfer>,
}

impl Http2StreamState {
    fn new(send_capacity: u32, priority: HttpPriority) -> Self {
        Self {
            send_window: Window::new(send_capacity)
                .expect("HTTP/2 stream send window must fit signed accounting"),
            priority,
            parent: None,
            promised: false,
            stalled: false,
            egress_work: VecDeque::new(),
        }
    }
}

pub(crate) enum Http2StatsEvent {
    TransactionOpened,
    TransactionClosed,
    SessionReused,
    TransactionsServed(u64),
    TransactionStalled,
    SessionStalled,
    BufferedRead(i64),
    BufferedWrite(i64),
    ContentLengthMismatch,
    IngressGetBody,
    RateLimit(RateLimitSample),
    ReadLoopLimit,
    PingTimeout,
}

pub(crate) struct Http2DriverState {
    pub host: Host,
    pub config: Http2SessionConfig,
    pub streams: BTreeMap<StreamId, Http2StreamState>,
    pub egress_order: VecDeque<EgressTransferKey>,
    pub orphaned_egress: BTreeMap<StreamId, VecDeque<QueuedEgressTransfer>>,
    pub head_requests: BTreeSet<StreamId>,
    pub scheduler: Http2PriorityScheduler,
    pub limiter: Http2RateLimiter,
    pub peer_initial_window: u32,
    pub peer_max_concurrent_streams: u32,
    pub peer_push_enabled: bool,
    pub last_good_stream: StreamId,
    pub received_goaway: bool,
    pub sent_initial_goaway: bool,
    pub sent_final_goaway: bool,
    pub goaway_deadline: Option<Instant>,
    pub pending_stats: Vec<Http2StatsEvent>,
    pub connection_credit_to_return: u32,
    pub transactions_opened: u64,
    pub stats_finalized: bool,
    pub body_source_publications: Arc<BodySourcePublications>,
    pub ping_prober: Option<Rc<PingProber>>,
}

impl Http2DriverState {
    pub fn new(config: Http2SessionConfig, host: Host) -> Self {
        Self {
            peer_initial_window: DEFAULT_STREAM_WINDOW,
            peer_max_concurrent_streams: u32::MAX,
            peer_push_enabled: true,
            limiter: Http2RateLimiter::new(config.rate_limits, host.now()),
            host,
            config,
            streams: BTreeMap::new(),
            egress_order: VecDeque::new(),
            orphaned_egress: BTreeMap::new(),
            head_requests: BTreeSet::new(),
            scheduler: Http2PriorityScheduler::new(),
            last_good_stream: StreamId::UNSPECIFIED,
            received_goaway: false,
            sent_initial_goaway: false,
            sent_final_goaway: false,
            goaway_deadline: None,
            pending_stats: Vec::new(),
            connection_credit_to_return: 0,
            transactions_opened: 0,
            stats_finalized: false,
            body_source_publications: Arc::new(BodySourcePublications::default()),
            ping_prober: None,
        }
    }

    pub fn record_rate(&mut self, kind: RateLimitKind) -> bool {
        if self.config.stats.is_none() && self.limiter.is_unbounded(kind) {
            return false;
        }
        let (breached, sample) = self.limiter.record(kind, self.host.now());
        if self.config.stats.is_some()
            && let Some(sample) = sample
        {
            self.pending_stats.push(Http2StatsEvent::RateLimit(sample));
        }
        breached
    }

    pub fn expire_rate_limits(&mut self, now: Instant) {
        let samples = self.limiter.expire(now);
        if self.config.stats.is_some() {
            self.pending_stats
                .extend(samples.into_iter().map(Http2StatsEvent::RateLimit));
        }
    }

    pub fn next_rate_limit_deadline(&self) -> Option<Instant> {
        self.limiter.next_deadline()
    }

    pub fn return_rejected_connection_credit(&mut self, amount: usize) -> bool {
        let Ok(amount) = u32::try_from(amount) else {
            return false;
        };
        let Some(total) = self.connection_credit_to_return.checked_add(amount) else {
            return false;
        };
        self.connection_credit_to_return = total;
        true
    }

    pub fn insert_stream(&mut self, stream: StreamId, priority: HttpPriority) {
        let mut state = Http2StreamState::new(self.peer_initial_window, priority);
        match self.streams.entry(stream) {
            Entry::Occupied(mut entry) => {
                state.egress_work = std::mem::take(&mut entry.get_mut().egress_work);
                entry.insert(state);
            }
            Entry::Vacant(entry) => {
                if let Some(orphaned) = self.orphaned_egress.remove(&stream) {
                    state.egress_work = orphaned;
                }
                entry.insert(state);
            }
        }
        self.scheduler.insert(stream, priority);
    }

    pub fn record_transaction_opened(&mut self) {
        if self.config.stats.is_some() && self.transactions_opened != 0 {
            self.pending_stats.push(Http2StatsEvent::SessionReused);
        }
        self.transactions_opened = self.transactions_opened.saturating_add(1);
        if self.config.stats.is_some() {
            self.pending_stats.push(Http2StatsEvent::TransactionOpened);
        }
    }

    pub fn finalize_stats(&mut self) {
        if self.stats_finalized {
            return;
        }
        self.stats_finalized = true;
        if self.config.stats.is_some() {
            self.pending_stats.push(Http2StatsEvent::TransactionsServed(
                self.transactions_opened,
            ));
        }
    }

    pub fn mark_stream_stalled(&mut self, stream: StreamId) -> bool {
        let Some(stream) = self.streams.get_mut(&stream) else {
            return false;
        };
        if stream.stalled {
            return false;
        }
        stream.stalled = true;
        if self.config.stats.is_some() {
            self.pending_stats.push(Http2StatsEvent::TransactionStalled);
        }
        true
    }

    pub fn remove_stream(&mut self, stream: StreamId) {
        if let Entry::Occupied(entry) = self.streams.entry(stream) {
            assert!(
                entry.get().egress_work.is_empty(),
                "queued transfers must be extracted before removing stream state"
            );
            entry.remove();
        }
        self.head_requests.remove(&stream);
        self.scheduler.remove(stream);
    }

    pub fn pushed_children(&self, parent: StreamId) -> Vec<StreamId> {
        self.streams
            .iter()
            .filter_map(|(stream, state)| (state.parent == Some(parent)).then_some(*stream))
            .collect()
    }

    pub fn update_priority(&mut self, stream: StreamId, priority: HttpPriority) {
        if let Some(state) = self.streams.get_mut(&stream) {
            state.priority = priority;
            self.scheduler.update(stream, priority);
        }
    }

    pub fn update_initial_send_window(&mut self, capacity: u32) -> bool {
        protocol::update_initial_send_window(
            &mut self.peer_initial_window,
            capacity,
            self.streams
                .values_mut()
                .map(|state| &mut state.send_window),
        )
    }

    pub fn stream_send_credit(&self, stream: StreamId) -> u32 {
        self.streams
            .get(&stream)
            .map_or(0, |state| state.send_window.non_negative_size())
    }

    pub fn reserve_stream_send(&mut self, stream: StreamId, amount: u32) -> bool {
        self.streams
            .get_mut(&stream)
            .is_some_and(|state| amount == 0 || state.send_window.reserve(amount, true))
    }

    pub fn free_stream_send(&mut self, stream: StreamId, amount: u32) -> bool {
        self.streams.get_mut(&stream).is_some_and(|state| {
            protocol::free_stream_send(&mut state.send_window, &mut state.stalled, amount)
        })
    }
}
