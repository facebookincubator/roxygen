/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Experimental poll-driven HTTP/2 driver, for downstream and upstream connections.
//!
//! One driver future per connection runs `Http2Codec`, every stream's exchange and every body
//! source, and does not spawn a task for each stream. [`OwnedH2Session`] serves a downstream
//! connection with an [`OwnedExchange`] for each request from the application's
//! [`OwnedApplication`]. [`OwnedH2Session::new`] lists the configuration that the driver applies
//! and rejects. The [`upstream`] module documents the client side, [`OwnedH2UpstreamSession`].

pub mod upstream;

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::hash::BuildHasher;
use std::hash::RandomState;
use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use foldhash::SharedSeed;
use foldhash::fast::SeedableRandomState;
use futures::task::ArcWake;
use futures::task::waker_ref;
use parking_lot::Mutex;
use roxygen_codec::CodecCallbackAction;
use roxygen_codec::CodecCallbackActions;
use roxygen_codec::CodecEvent;
use roxygen_codec::CodecSetting;
use roxygen_codec::EgressFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::HttpExceptionDirection;
use roxygen_codec::IngressStreamState;
use roxygen_codec::OwnedEgressFrame;
use roxygen_codec::error_adapter::h2_error_code_to_http_error_code;
use roxygen_codec::error_adapter::http_error_code_to_h2_error_code;
use roxygen_codec::error_adapter::http_exception_to_http_error_code;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::FieldSectionInfo;
use roxygen_core::byte_event::FieldSectionType;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::flow_control::Window;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::MessageTypeError;
use roxygen_core::priority::HttpPriorityHeaderAccess;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpResponseSource;
use roxygen_core::stream_source::HttpStreamSourceDrain;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_core::stream_source::HttpStreamSourceSharedCallback;
use roxygen_http2::Http2Codec;
use roxygen_http2::flow_control::ConnectionFlowControlCallback;
use roxygen_http2::flow_control::FlowControlledCodec;
use roxygen_http2::settings::SettingsId;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::timer::Expiry;
use roxygen_runtime::host::timer::Timeout;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::task_group::in_forced_teardown;
use roxygen_runtime::thread::current_id;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_event::AsyncSocketByteEventObserver;
use roxygen_transport::byte_event::ByteEventOffsets;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_event::SocketByteEventType;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
pub use upstream::OwnedH2ExclusiveResponse;
pub use upstream::OwnedH2ExclusiveSubmission;
pub use upstream::OwnedH2ResponseObserver;
pub use upstream::OwnedH2UpstreamHandle;
pub use upstream::OwnedH2UpstreamObserver;
pub use upstream::OwnedH2UpstreamRequest;
pub use upstream::OwnedH2UpstreamReservation;
pub use upstream::OwnedH2UpstreamResponse;
pub use upstream::OwnedH2UpstreamSession;
pub use upstream::OwnedH2UpstreamSubmission;

use crate::http2::DEFAULT_STREAM_WINDOW;
use crate::http2::Http2SessionConfig;
use crate::http2::StreamReceiveWindowMode;
use crate::http2::protocol::CodecFailureContext;
use crate::http2::protocol::CodecFailureDisposition;
use crate::http2::protocol::PeerSetting;
use crate::http2::protocol::SentGoaway;
use crate::http2::protocol::{self};
use crate::http2::rate_limit::Http2RateLimiter;
use crate::http2::rate_limit::RateLimitKind;
use crate::http2::rate_limit::rate_limit_exceeded;
use crate::http2::scheduler::PriorityEntry;
use crate::http2::scheduler::PriorityQueue;
use crate::owned::DeadlineExpiry;
use crate::owned::DeadlinePhase;
use crate::owned::DeadlineTimer;
pub use crate::owned::OwnedApplication as OwnedH2Application;
use crate::owned::OwnedApplication;
pub use crate::owned::OwnedExchange as OwnedH2Exchange;
use crate::owned::OwnedExchange;
use crate::owned::PanicSite;
use crate::owned::ReadSlot;
use crate::owned::report_contained_panic;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::session::LifecycleObserverId;
use crate::shutdown::DrainTrigger;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexLifecycleDispatcher;
use crate::uniplex::UniplexRunSummary;
use crate::uniplex::UniplexTransport;
use crate::uniplex::UniplexWriteQueue;

/// The turns that one poll runs before it yields to the host. Each turn runs every phase once.
pub(crate) const TURN_BUDGET: usize = 64;
/// The most stream owners that one turn polls. Like folly's `EventBase::runLoopCallbacks`, which
/// runs only the callbacks queued before it starts, a turn polls only owners queued before its
/// owner phase: an owner queued during the phase, by its own poll or another's, waits a turn. The
/// exception is a downstream poll that goes on from a final head to the body: it can remove the
/// entry that it queued for itself, if that entry is still the last, and serve that wake.
pub(crate) const OWNER_POLLS_PER_TURN: usize = 16;

/// The status of the error response sent after a downstream stream read timeout.
const REQUEST_TIMEOUT: u16 = 408;

/// Inputs selected once, before starting an owned HTTP/2 connection.
pub struct OwnedH2Options {
    pub context: WorkerContext,
    pub transport: Rc<dyn UniplexTransport>,
    pub io: UniplexConfig,
    pub http2: Http2SessionConfig,
}

// Critical sections only push, pop or clone and drop replaced wakers after unlocking, so
// the lock is never held during an unwind and a poisoning mutex would never be poisoned.
struct ReadyQueue {
    /// The streams that wakers queued. The session's own thread queues owners in
    /// `Control::owners` instead, without the lock.
    state: Mutex<ReadyQueueState>,
    /// Set by every wake and every push, except a wake that `IoWake::poll_inline` holds for a
    /// completed operation; cleared only by `settle`. Only the session's thread sets it
    /// outside the lock.
    busy: AtomicBool,
    /// Set under the lock when a waker queues a stream, and cleared when the session moves
    /// `state.streams` to its own queue.
    pushed: AtomicBool,
    /// The session's thread.
    owner: ThreadId,
}

impl Default for ReadyQueue {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            busy: AtomicBool::new(false),
            pushed: AtomicBool::new(false),
            owner: current_id(),
        }
    }
}

#[derive(Default)]
struct ReadyQueueState {
    streams: VecDeque<StreamId>,
    waker: Option<Arc<Waker>>,
}

/// An owner queued for a poll. `shared` entries came through `ReadyQueue::state`.
#[derive(Clone, Copy, Default)]
struct QueuedOwner {
    id: StreamId,
    shared: bool,
}

impl ReadyQueue {
    fn wake(&self) {
        self.wake_after(|| {});
    }

    /// Wakes the session after `mark`, which runs under the lock after the `busy` store. So
    /// `settle` observes both or neither, and the lock-free `Control::can_fold` never observes
    /// what `mark` sets without `busy`.
    fn wake_after(&self, mark: impl FnOnce()) {
        let wake = {
            let state = self.state.lock();
            self.busy.store(true, Ordering::Release);
            mark();
            state.waker.clone()
        };
        if let Some(wake) = wake {
            wake.wake_by_ref();
        }
    }

    /// Clears `busy` if the queue is empty and neither I/O wake is pending, and returns whether
    /// it did. A wake or push that the lock orders after this sets `busy` again. The caller
    /// checks that the session's own queue is empty.
    fn settle(&self, read: &IoWake, write: &IoWake) -> bool {
        let state = self.state.lock();
        let settled = state.streams.is_empty()
            && !read.ready.load(Ordering::Acquire)
            && !write.ready.load(Ordering::Acquire);
        if settled {
            self.busy.store(false, Ordering::Release);
        }
        settled
    }

    // Only the session owner installs wakers, and `installed` is its copy of the current one,
    // so a stable waker does not need the lock. Pushes racing with a poll are still seen, or
    // woken through the installed waker, because a push sets `pushed` under the lock before it
    // clones the waker.
    fn register(&self, installed: &RefCell<Option<Arc<Waker>>>, context: &Context<'_>) {
        if installed
            .borrow()
            .as_ref()
            .is_some_and(|waker| waker.will_wake(context.waker()))
        {
            return;
        }
        let replacement = Arc::new(context.waker().clone());
        let old = self.state.lock().waker.replace(Arc::clone(&replacement));
        let previous = installed.replace(Some(replacement));
        drop(old);
        drop(previous);
    }

    /// Appends the streams that wakers queued to `owners`, in queue order.
    #[cold]
    #[inline(never)]
    fn drain_into(&self, owners: &RefCell<VecDeque<QueuedOwner>>) {
        let mut state = self.state.lock();
        self.pushed.store(false, Ordering::Relaxed);
        owners.borrow_mut().extend(
            state
                .streams
                .drain(..)
                .map(|id| QueuedOwner { id, shared: true }),
        );
    }
}

/// `IoWake::inline` while an inline poll is not running.
const INLINE_IDLE: u8 = 0;
/// `IoWake::inline` while the session polls the wake's operation.
const INLINE_POLLING: u8 = 1;
/// `IoWake::inline` once that operation woke the wake during the poll.
const INLINE_WOKEN: u8 = 2;

struct IoWake {
    ready: AtomicBool,
    /// Where `poll_inline` is (`INLINE_*`).
    inline: AtomicU8,
    /// Whether the operation can wake this from inside the poll that completes it, in which case
    /// `poll_inline` holds wakes. Read and write futures can. The native reader and write queue
    /// complete without waking.
    completion_wakes: bool,
    queue: Arc<ReadyQueue>,
}

impl IoWake {
    fn new(queue: &Arc<ReadyQueue>, completion_wakes: bool) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            inline: AtomicU8::new(INLINE_IDLE),
            completion_wakes,
            queue: Arc::clone(queue),
        })
    }

    /// Whether the waker fired since the flag was last taken, without taking it.
    #[inline]
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    fn take_ready(&self) -> bool {
        self.is_ready() && self.ready.swap(false, Ordering::AcqRel)
    }

    /// Runs `poll`, which starts or polls this wake's transport read or write, from the session's
    /// turn with `waker`. A wake that arrives during the poll is held until the poll returns.
    ///
    /// If the operation completed, its wake only marks `ready` and does not schedule the session
    /// again, because this turn consumes the completion. `OwnedByteStream`'s read and write
    /// futures wake from inside the completing poll as they retire (a read's `TimedBaton::drop`, a
    /// write's `CompletionWake`), so this saves a session poll. A pending operation's wake is sent
    /// as usual. A poll that panics leaves the state polling or woken, and every wake is held
    /// until the next `poll_inline` resets it. That is safe because the session then tears down.
    /// `poll_panicked` stores both `ready` flags and wakes the session through its own context's
    /// waker, and a teardown poll that panics does the same for its operation. Without
    /// `completion_wakes` this only polls, since there is no wake to save.
    #[inline]
    fn poll_inline<T>(
        self: &Arc<Self>,
        waker: &Waker,
        poll: impl FnOnce(&mut Context<'_>) -> Poll<T>,
    ) -> Poll<T> {
        if self.completion_wakes {
            self.poll_holding(waker, poll)
        } else {
            poll(&mut Context::from_waker(waker))
        }
    }

    /// `poll_inline`'s hold. Cold and out of line, so that the common case, a transport that
    /// completes without waking, stays small.
    #[cold]
    #[inline(never)]
    fn poll_holding<T>(
        self: &Arc<Self>,
        waker: &Waker,
        poll: impl FnOnce(&mut Context<'_>) -> Poll<T>,
    ) -> Poll<T> {
        #[cfg(test)]
        INLINE_POLLS.with(|polls| polls.set(polls.get() + 1));
        self.inline.store(INLINE_POLLING, Ordering::Relaxed);
        let polled = poll(&mut Context::from_waker(waker));
        if self.inline.swap(INLINE_IDLE, Ordering::AcqRel) == INLINE_WOKEN {
            if polled.is_ready() {
                // No `busy` store: this turn cleared `quiescent`, and only its closing `settle`
                // publishes it again, which either observes this mark or clears `busy` itself.
                self.ready.store(true, Ordering::Release);
            } else {
                self.wake_by_ref();
            }
        }
        polled
    }

    /// Marks the running `poll_holding` woken. Returns false if it has already returned.
    #[cold]
    #[inline(never)]
    fn hold_wake(&self) -> bool {
        let mut inline = self.inline.load(Ordering::Acquire);
        while inline != INLINE_IDLE {
            match self.inline.compare_exchange_weak(
                inline,
                INLINE_WOKEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => inline = current,
            }
        }
        false
    }
}

impl Wake for IoWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.inline.load(Ordering::Acquire) != INLINE_IDLE && self.hold_wake() {
            return;
        }
        self.queue
            .wake_after(|| self.ready.store(true, Ordering::Release));
    }
}

/// The connection's timeouts, installed when the first one is armed, so a session with every
/// timeout set to zero never creates them.
struct ConnectionTimers {
    /// Set when a slot fires; each turn takes it once.
    wake: Arc<IoWake>,
    waker: Waker,
    /// The connection read timeout, which bounds each transport read. Stamped when a read is
    /// issued and checked only while that read is outstanding.
    read_idle: Timeout,
    /// Fires at `goaway_deadline` when `goaway_timeout` is nonzero.
    goaway: Timeout,
    /// The writer's `write_timeout` wait while flow control blocks egress.
    flow: Timeout,
    /// The blocked streams the `flow` slot is armed for, sorted by id; empty while it is not.
    flow_blocked: Vec<StreamId>,
    /// Whether the connection's send window blocks them, rather than their own.
    flow_connection: bool,
    /// The connection's progress when the `flow` slot was armed.
    flow_marks: FlowMarks,
    /// Scratch for the blocked streams of the next check.
    flow_scan: Vec<StreamId>,
}

impl ConnectionTimers {
    #[cold]
    #[inline(never)]
    fn new(host: &Host, queue: &Arc<ReadyQueue>) -> Box<Self> {
        let wake = IoWake::new(queue, false);
        Box::new(Self {
            waker: Waker::from(Arc::clone(&wake)),
            wake,
            read_idle: Timeout::new(host),
            goaway: Timeout::new(host),
            flow: Timeout::new(host),
            flow_blocked: Vec::new(),
            flow_connection: false,
            flow_marks: FlowMarks::default(),
            flow_scan: Vec::new(),
        })
    }
}

/// The progress that restarts the `flow` wait besides a change in the blocked streams: bytes read
/// and scheduled for writing, EOF, and stream retirement. They cover the points where
/// `UniplexSession` calls `note_flow_timeout_activity` (ingress parsing, writes, PING replies,
/// resets, drains, detaches).
#[derive(Clone, Copy, Default, Eq, PartialEq)]
struct FlowMarks {
    bytes_read: u64,
    bytes_scheduled: u64,
    eof: bool,
    /// The live stream count. A retiring stream lowers it, which restarts the wait. A stream
    /// opens only with bytes read or scheduled.
    streams: usize,
}

/// `queued` and `shared_queued` each mean that the owner has an entry that was queued and not yet
/// polled: one that the session's thread queued, or one that a waker queued through
/// `ReadyQueue::state`. An owner has at most one entry of each kind. Only the session's thread
/// accesses `queued`.
struct StreamWake {
    id: StreamId,
    queue: Arc<ReadyQueue>,
    queued: AtomicBool,
    shared_queued: AtomicBool,
    headers_poll_ready: AtomicBool,
    live: AtomicBool,
}

impl StreamWake {
    fn take_headers_ready(&self) -> bool {
        self.headers_poll_ready.load(Ordering::Acquire)
            && self.headers_poll_ready.swap(false, Ordering::AcqRel)
    }

    /// Queues the owner from any thread and wakes the session. The session's thread uses
    /// `Control::schedule` where it can reach the control.
    fn schedule(&self) {
        if !self.live.load(Ordering::Acquire)
            || (current_id() == self.queue.owner && self.queued.load(Ordering::Relaxed))
            || self.shared_queued.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let wake = {
            let mut state = self.queue.state.lock();
            self.queue.busy.store(true, Ordering::Release);
            self.queue.pushed.store(true, Ordering::Release);
            state.streams.push_back(self.id);
            state.waker.clone()
        };
        if let Some(wake) = wake {
            wake.wake_by_ref();
        }
    }

    /// Clears the mark of `owner`, an entry of this owner, just before its poll.
    #[inline(always)]
    fn take_mark(&self, owner: QueuedOwner) {
        if owner.shared {
            // The swap pairs with the wakers', so this poll observes what they published.
            self.shared_queued.swap(false, Ordering::AcqRel);
        } else {
            self.queued.store(false, Ordering::Relaxed);
        }
    }
}

impl ArcWake for StreamWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.headers_poll_ready.store(true, Ordering::Release);
        arc_self.schedule();
    }
}

struct IngressSignals {
    control: Rc<Control>,
    active: Cell<bool>,
    source_complete: Cell<bool>,
    response_complete: Cell<bool>,
    upstream: bool,
    ingress_complete: Cell<bool>,
    wake: Arc<StreamWake>,
    consumed: Cell<usize>,
    acknowledge: Cell<usize>,
    completed: Cell<Option<Result<(), HttpError>>>,
    /// The source completed with `READ_TIMEOUT`: a stream read timeout.
    read_timed_out: Cell<bool>,
    /// A waiter depends on the source's completion, so it always wakes the owner.
    wake_on_completion: Cell<bool>,
}

impl IngressSignals {
    fn finish_protocol(&self) {
        let ingress_complete = if self.upstream {
            self.ingress_complete.get()
        } else {
            self.source_complete.get()
        };
        if ingress_complete && self.response_complete.get() && self.active.replace(false) {
            self.control
                .active_incoming
                .set(self.control.active_incoming.get() - 1);
        }
    }

    fn close_protocol(&self) {
        if self.active.replace(false) {
            self.control
                .active_incoming
                .set(self.control.active_incoming.get() - 1);
        }
    }

    fn ingress_complete(&self) {
        self.ingress_complete.set(true);
        self.finish_protocol();
    }

    fn response_complete(&self) {
        self.response_complete.set(true);
        self.finish_protocol();
    }

    fn error_response_pending(&self) {
        self.response_complete.set(false);
        self.wake_on_completion.set(true);
        if !self.active.replace(true) {
            self.control
                .active_incoming
                .set(self.control.active_incoming.get() + 1);
        }
    }
}

impl HttpStreamSourceSharedCallback for IngressSignals {
    fn bytes_processed(&self, _id: StreamId, amount: usize, acknowledge: usize) {
        if acknowledge == 0
            && !self.upstream
            && self.wake.live.load(Ordering::Acquire)
            && self.control.fold_credit(amount)
        {
            return;
        }
        self.consumed
            .set(self.consumed.get().saturating_add(amount));
        self.acknowledge
            .set(self.acknowledge.get().saturating_add(acknowledge));
        self.control.schedule(&self.wake);
    }

    fn source_complete(&self, _id: StreamId, error: Option<HttpError>) {
        let fold = error.is_none()
            && !self.upstream
            && !self.response_complete.get()
            && !self.wake_on_completion.get()
            && self.wake.live.load(Ordering::Acquire)
            && self.control.can_fold(0);
        self.source_complete.set(true);
        self.finish_protocol();
        self.read_timed_out.set(
            error
                .as_ref()
                .is_some_and(|error| error.code() == HttpErrorCode::READ_TIMEOUT),
        );
        self.completed.set(Some(error.map_or(Ok(()), Err)));
        if fold {
            self.control.early_completed.borrow_mut().push(self.wake.id);
            self.control.folds_pending.set(true);
        } else {
            self.control.schedule(&self.wake);
        }
    }
}

struct Control {
    drain: Cell<bool>,
    cancel: Cell<bool>,
    complete: Cell<bool>,
    /// Set by `complete_handshake`.
    handshake_complete: Cell<bool>,
    active_incoming: Cell<usize>,
    ready: Arc<ReadyQueue>,
    /// Owners queued for a poll, in queue order. The session's thread queues them here
    /// without the lock, and moves the streams that wakers queued here before its own.
    owners: RefCell<VecDeque<QueuedOwner>>,
    /// Set while a poll of the session's run is in progress, by its `PollExit`.
    polling: Cell<bool>,
    /// The session's copy of the waker it last installed in `ready`, which wakes from the
    /// session's thread use without the lock.
    owner_waker: RefCell<Option<Arc<Waker>>>,
    /// Set once `owner_waker` has been woken since the last poll started.
    notified: Cell<bool>,
    upstream: Option<Rc<upstream::Admission>>,
    idle: Cell<bool>,
    host: Host,
    /// Published by a downstream session's poll that ended without pending work. Cleared when
    /// the next poll starts.
    quiescent: Cell<bool>,
    /// The codec's connection credit headroom when `quiescent` was published.
    credit_headroom: Cell<i64>,
    /// Credit reported while quiescent, which the next poll returns first.
    early_credit: Cell<u32>,
    /// Streams whose request source completed without error while quiescent.
    early_completed: RefCell<Vec<StreamId>>,
    folds_pending: Cell<bool>,
    /// The connection send window's last transition, `Some(true)` when it opened, which a
    /// lifecycle observer receives as `Http2FlowWindowOpen` or `Http2FlowWindowClosed`.
    window_transition: Cell<Option<bool>>,
}

impl Control {
    /// Marks the connection handshake complete. The session calls it for every non-ACK
    /// SETTINGS frame it processes; the peer's first one sets the flag.
    fn complete_handshake(&self) {
        self.handshake_complete.set(true);
    }

    /// Whether `amount` more credit can wait for the session's next poll: an idle session
    /// woken now would only return it, without emitting, before doing anything else.
    fn can_fold(&self, amount: usize) -> bool {
        self.quiescent.get()
            && !self.ready.busy.load(Ordering::Acquire)
            && i64::from(self.early_credit.get())
                .saturating_add(i64::try_from(amount).unwrap_or(i64::MAX))
                <= self.credit_headroom.get()
            && self.host.runs_wakes_this_iteration()
    }

    fn fold_credit(&self, amount: usize) -> bool {
        if !self.can_fold(amount) {
            return false;
        }
        // `can_fold` bounds the sum by the headroom, which is below `u32::MAX`.
        self.early_credit
            .set(self.early_credit.get() + amount as u32);
        self.folds_pending.set(true);
        true
    }

    /// Queues `wake`'s owner from the session's thread and returns whether it did. A queued
    /// owner needs another queue check or a session wake.
    fn enqueue(&self, wake: &StreamWake) -> bool {
        if !wake.live.load(Ordering::Acquire)
            || wake.queued.load(Ordering::Relaxed)
            || wake.shared_queued.load(Ordering::Acquire)
        {
            return false;
        }
        wake.queued.store(true, Ordering::Relaxed);
        // Owners that wakers queued earlier stay ahead, as in a single queue.
        if self.ready.pushed.load(Ordering::Acquire) {
            self.ready.drain_into(&self.owners);
        }
        self.ready.busy.store(true, Ordering::Release);
        self.owners.borrow_mut().push_back(QueuedOwner {
            id: wake.id,
            shared: false,
        });
        true
    }

    /// `enqueue`, then a session wake if it queued the owner outside a poll. During a poll the
    /// turns serve the owner, or `PollExit` wakes the session for it.
    fn schedule(&self, wake: &StreamWake) {
        if self.enqueue(wake) && !self.polling.get() {
            self.notify();
        }
    }

    /// Wakes the session through `owner_waker` once until the next poll starts. A waker that
    /// panics leaves `notified` clear.
    fn notify(&self) {
        if self.notified.get() {
            return;
        }
        let waker = self.owner_waker.borrow();
        let Some(waker) = waker.as_deref() else {
            return;
        };
        // Set before the wake, so that a host that polls the session inside it clears it.
        self.notified.set(true);
        if let Err(panic) = guarded(|| waker.wake_by_ref()) {
            self.notified.set(false);
            resume_unwind(panic);
        }
    }

    /// Removes `wake`'s entry if the session's thread queued it and it is still the last entry, so
    /// that a poll of its owner that starts now serves the wake that queued it.
    fn unqueue_last(&self, wake: &StreamWake) {
        if !wake.queued.load(Ordering::Relaxed) {
            return;
        }
        let mut owners = self.owners.borrow_mut();
        if owners
            .back()
            .is_some_and(|owner| owner.id == wake.id && !owner.shared)
        {
            owners.pop_back();
            wake.queued.store(false, Ordering::Relaxed);
        }
    }

    /// Moves the oldest queued owners into `batch`, up to its length, and returns how many.
    fn pop_batch(&self, batch: &mut [QueuedOwner]) -> usize {
        if self.ready.pushed.load(Ordering::Acquire) {
            self.ready.drain_into(&self.owners);
        }
        let mut owners = self.owners.borrow_mut();
        let count = batch.len().min(owners.len());
        for (slot, stream) in batch.iter_mut().zip(owners.drain(..count)) {
            *slot = stream;
        }
        count
    }

    /// `ReadyQueue::settle`, unless an owner is queued.
    fn settle(&self, read: &IoWake, write: &IoWake) -> bool {
        self.owners.borrow().is_empty() && self.ready.settle(read, write)
    }
}

/// Owner-local drain and cancel control for an owned HTTP/2 session. The session applies them
/// on its next turn, outside codec and application callbacks.
#[derive(Clone)]
pub struct OwnedH2Handle(Rc<Control>);

impl OwnedH2Handle {
    pub fn initiate_drain(&self) -> Result<bool, HttpError> {
        if self.0.complete.get() {
            return Ok(false);
        }
        let changed = !self.0.drain.replace(true);
        self.0.ready.wake();
        Ok(changed)
    }

    pub fn cancel(&self) {
        self.0.cancel.set(true);
        self.0.ready.wake();
    }

    /// Whether the connection handshake has completed: the session has processed the peer's
    /// first SETTINGS frame. It stays true once set.
    pub fn handshake_complete(&self) -> bool {
        self.0.handshake_complete.get()
    }

    /// Whether the session has finished: true once its run has completed, with or without an
    /// error. A session that was never run, or whose run was dropped before completing, has not
    /// finished. A run that unwound out of a poll finishes when a later poll returns its error.
    pub fn is_finished(&self) -> bool {
        self.0.complete.get()
    }
}

struct ConnectionWindowWake(Rc<Control>);

impl ConnectionFlowControlCallback for ConnectionWindowWake {
    // The window opens only inside a parse that consumed the WINDOW_UPDATE. That is progress,
    // so the turns serialize again before the poll returns or yields.
    fn on_connection_send_window_open(&self) {
        self.0.window_transition.set(Some(true));
        if !self.0.polling.get() {
            self.0.ready.busy.store(true, Ordering::Release);
            self.0.notify();
        }
    }

    fn on_connection_send_window_closed(&self) {
        self.0.window_transition.set(Some(false));
    }
}

/// Opt-in lifecycle observation of a downstream session, present only once an observer was
/// added. Events queue in the dispatcher, which delivers them at the end of each
/// poll and at teardown.
struct Observation {
    lifecycle: Rc<UniplexLifecycleDispatcher>,
    /// Only the first drain is reported, as `SessionCore` reports only the first.
    drain_started: bool,
    /// `Http2SessionConfig::max_concurrent_outgoing_streams`, which caps the peer's
    /// `MAX_CONCURRENT_STREAMS` in the outgoing capacity that `Http2StreamsFull` reports.
    max_outgoing: u32,
}

impl Observation {
    #[cold]
    #[inline(never)]
    fn new(max_outgoing: u32) -> Box<Self> {
        #[cfg(test)]
        OBSERVATIONS_CREATED.set(OBSERVATIONS_CREATED.get() + 1);
        Box::new(Self {
            lifecycle: Rc::new(UniplexLifecycleDispatcher::new(None)),
            drain_started: false,
            max_outgoing,
        })
    }

    #[cold]
    #[inline(never)]
    fn push(&self, event: LifecycleEvent) {
        self.lifecycle.on_event(event);
    }

    #[cold]
    #[inline(never)]
    fn drain(&mut self, trigger: DrainTrigger) {
        if !std::mem::replace(&mut self.drain_started, true) {
            self.push(LifecycleEvent::DrainStarted(trigger));
        }
    }

    #[cold]
    #[inline(never)]
    fn opened(&self, stream: StreamId, first: bool) {
        self.push(LifecycleEvent::RequestBegin {
            stream,
            sequence_number: stream.get() / 2,
        });
        if first {
            self.push(LifecycleEvent::ActivateConnection);
        }
        self.push(LifecycleEvent::TransactionAttached);
    }

    #[cold]
    #[inline(never)]
    fn retired(&self, stream: StreamId, last: bool) {
        self.push(LifecycleEvent::TransactionDetached {
            stream: Some(stream),
        });
        if last {
            self.push(LifecycleEvent::DeactivateConnection);
        }
    }

    /// Reports a peer SETTINGS frame as `UniplexSession` does: its size, each entry, and each
    /// change in whether the peer's `MAX_CONCURRENT_STREAMS` leaves room for an outgoing
    /// stream.
    #[cold]
    #[inline(never)]
    fn settings(&self, settings: &[CodecSetting], mut peer_maximum: u32, draining: bool) {
        self.push(LifecycleEvent::Http2Settings {
            count: settings.len(),
        });
        let available = |maximum: u32| !draining && maximum.min(self.max_outgoing) != 0;
        for setting in settings {
            self.push(LifecycleEvent::Http2Setting { setting: *setting });
            if let PeerSetting::MaxConcurrentStreams(maximum) = PeerSetting::from(*setting) {
                let was_available = available(peer_maximum);
                peer_maximum = maximum;
                if was_available != available(maximum) {
                    self.push(if available(maximum) {
                        LifecycleEvent::Http2StreamsNotFull
                    } else {
                        LifecycleEvent::Http2StreamsFull
                    });
                }
            }
        }
    }

    /// Reports the connection send window's last transition since the previous report.
    fn window_transition(&self, control: &Control) {
        if let Some(open) = control.window_transition.take() {
            self.push(if open {
                LifecycleEvent::Http2FlowWindowOpen
            } else {
                LifecycleEvent::Http2FlowWindowClosed
            });
        }
    }
}

enum PendingEgress {
    Headers(HttpHeaderEvent),
    Body(HttpBodyEvent),
}

impl PendingEgress {
    fn eom(&self) -> bool {
        match self {
            Self::Headers(headers) => headers.eom(),
            Self::Body(body) => body.eom(),
        }
    }
}

trait ExchangeStorage<const UPSTREAM: bool>: OwnedExchange + Sized {
    type Stored: 'static;

    fn store_downstream(exchange: Self) -> Self::Stored;
    fn store_upstream(exchange: upstream::Exchange) -> Self::Stored;
    fn downstream(exchange: &mut Self::Stored) -> &mut Self;
    fn upstream(exchange: &Self::Stored) -> Option<&upstream::Exchange>;
    fn upstream_mut(exchange: &mut Self::Stored) -> Option<&mut upstream::Exchange>;
    fn cancel_stored(exchange: &mut Self::Stored, error: &HttpError, peer: bool);
    fn response_complete_stored(exchange: &mut Self::Stored);
    fn complete_stored(exchange: &mut Option<Self::Stored>);
}

impl<E: OwnedExchange> ExchangeStorage<false> for E {
    type Stored = E;

    fn store_downstream(exchange: E) -> E {
        exchange
    }

    fn store_upstream(_exchange: upstream::Exchange) -> E {
        unreachable!("downstream streams cannot own upstream exchanges")
    }

    fn downstream(exchange: &mut E) -> &mut E {
        exchange
    }

    fn upstream(_exchange: &E) -> Option<&upstream::Exchange> {
        None
    }

    fn upstream_mut(_exchange: &mut E) -> Option<&mut upstream::Exchange> {
        None
    }

    fn cancel_stored(exchange: &mut E, error: &HttpError, peer: bool) {
        exchange.cancel(error, peer);
    }

    fn response_complete_stored(exchange: &mut E) {
        exchange.response_complete();
    }

    fn complete_stored(exchange: &mut Option<E>) {
        exchange
            .take()
            .expect("a stream owns one application exchange")
            .complete();
    }
}

impl<E: OwnedExchange> ExchangeStorage<true> for E {
    type Stored = upstream::Exchange;

    fn store_downstream(_exchange: E) -> upstream::Exchange {
        unreachable!("upstream streams cannot own downstream application exchanges")
    }

    fn store_upstream(exchange: upstream::Exchange) -> upstream::Exchange {
        exchange
    }

    fn downstream(_exchange: &mut upstream::Exchange) -> &mut E {
        unreachable!("upstream exchanges never enter downstream policy polling")
    }

    fn upstream(exchange: &upstream::Exchange) -> Option<&upstream::Exchange> {
        Some(exchange)
    }

    fn upstream_mut(exchange: &mut upstream::Exchange) -> Option<&mut upstream::Exchange> {
        Some(exchange)
    }

    fn cancel_stored(exchange: &mut upstream::Exchange, error: &HttpError, _peer: bool) {
        exchange.cancel(error);
    }

    fn response_complete_stored(_exchange: &mut upstream::Exchange) {}

    fn complete_stored(exchange: &mut Option<upstream::Exchange>) {
        exchange
            .as_ref()
            .expect("a stream owns one application exchange");
        *exchange = None;
    }
}

enum IngressSource {
    Request(HttpIngressBody),
    Response(HttpResponseSource),
}

impl IngressSource {
    fn initialize_body_only(&self, message: &mut HttpMessage, eom: bool) -> bool {
        match self {
            Self::Request(source) => source.initialize_body_only(message, eom),
            Self::Response(source) => source.initialize_body_only(message, eom),
        }
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        match self {
            Self::Request(source) => source.ingress_completion(),
            Self::Response(source) => source.ingress_completion(),
        }
    }
}

struct Stream<E, const UPSTREAM: bool>
where
    E: ExchangeStorage<UPSTREAM>,
{
    application: Option<E::Stored>,
    wake: Arc<StreamWake>,
    signals: Rc<IngressSignals>,
    source: Option<IngressSource>,
    producer: HttpStreamSourceProducer,
    drain: Option<HttpStreamSourceDrain>,
    head: Option<Box<HttpMessage>>,
    started: bool,
    wire_ingress_complete: bool,
    ingress_rejected: bool,
    consumed: bool,
    peer_reset: Option<HttpError>,
    reset_after_response: bool,
    resume: Option<roxygen_core::events::ResumeFuture>,
    request_head: bool,
    request_body_length: u64,
    request_body_events: bool,
    expected_request_length: Option<u64>,
    send_window: Window,
    scheduling: PriorityEntry,
    stalled: bool,
    pending: Option<PendingEgress>,
    response_headers: bool,
    /// The request body's read timed out while the handler ran: its final head must be a 408.
    expects_408: bool,
    response_complete: bool,
    write_boundary: Option<u64>,
    physically_complete: bool,
    /// A serialized event registered a byte event.
    registered_byte_events: bool,
    reset: bool,
    stream_offset: u64,
    body_offset: u64,
}

impl<E, const UPSTREAM: bool> Stream<E, UPSTREAM>
where
    E: ExchangeStorage<UPSTREAM>,
{
    fn upstream(&self) -> Option<&upstream::Exchange> {
        self.application
            .as_ref()
            .and_then(<E as ExchangeStorage<UPSTREAM>>::upstream)
    }

    fn upstream_mut(&mut self) -> Option<&mut upstream::Exchange> {
        self.application
            .as_mut()
            .and_then(<E as ExchangeStorage<UPSTREAM>>::upstream_mut)
    }

    /// Records a peer reset for the owner to apply, unless one is pending. The error is the
    /// request's selected cause, or else `reset`. A `refusal` may fail the request unprocessed
    /// and select `reset` (see `RequestStatus::peer_reset_error`), which then also fails the
    /// response's source immediately (`fail_refused_response`).
    fn record_peer_reset(&mut self, refusal: bool, reset: impl FnOnce() -> HttpError) {
        if self.peer_reset.is_none() {
            let (error, unprocessed) = match self.upstream() {
                Some(exchange) => exchange.status.peer_reset_error(refusal, reset),
                None => (reset(), false),
            };
            self.peer_reset = Some(error);
            if unprocessed {
                self.fail_refused_response();
            }
        }
    }

    fn release_upstream_capacity(&self) {
        if let Some(exchange) = self.upstream()
            && exchange.status.ingress_succeeded()
        {
            self.signals.ingress_complete();
            if self.response_complete {
                exchange.status.release();
            }
        }
    }

    /// Fails the response's source with the peer reset just recorded, for a request that the
    /// peer's refusal failed unprocessed. The owner applies the reset only when its wake is
    /// polled, which the turn budget can defer to a later poll, so a stream read timeout that
    /// expires before then would otherwise fail the response first. `cancel_owner` keeps the
    /// error the same whichever of the two fails the source.
    fn fail_refused_response(&self) {
        if let Some(error) = &self.peer_reset {
            self.producer
                .abort_with_error(error.clone(), b"owned H2 stream cancelled");
        }
    }

    fn protocol_active(&self) -> bool {
        self.signals.active.get()
    }

    /// Counted in `ConnectionState::awaiting_ingress`.
    fn awaits_ingress(&self) -> bool {
        !self.wire_ingress_complete && !self.reset
    }

    /// Whether flow control blocks the stream: its egress is open and its send window is
    /// exhausted.
    fn send_blocked(&self) -> bool {
        !self.reset
            && !self.response_complete
            && self.peer_reset.is_none()
            && self.send_window.non_negative_size() == 0
    }

    /// Whether the stream's next event is body that needs send credit. Used while the
    /// connection is out of credit.
    fn awaits_credit(&self) -> bool {
        !self.reset
            && matches!(
                &self.pending,
                Some(PendingEgress::Body(event))
                    if matches!(
                        event.kind(),
                        HttpBodyEventKind::Body(bytes)
                            if !bytes.is_empty() && (UPSTREAM || !self.request_head)
                    )
            )
    }

    fn can_serialize(&self, budget: SerializationBudget) -> bool {
        #[cfg(test)]
        SERIALIZE_PROBES.with(|probes| probes.set(probes.get() + 1));
        if !self.protocol_active() {
            return false;
        }
        match &self.pending {
            Some(PendingEgress::Headers(_)) => !self.reset || self.signals.source_complete.get(),
            Some(PendingEgress::Body(event)) => match event.kind() {
                HttpBodyEventKind::Body(bytes)
                    if (!UPSTREAM && self.request_head) || bytes.is_empty() =>
                {
                    budget.wire >= roxygen_http2::constants::FRAME_HEADER_SIZE
                }
                HttpBodyEventKind::Body(_) => {
                    budget.credit.min(self.send_window.non_negative_size()) != 0
                }
                HttpBodyEventKind::Padding(_) => {
                    budget.wire >= roxygen_http2::constants::FRAME_HEADER_SIZE
                }
                _ => true,
            },
            None => false,
        }
    }
}

type PanicPayload = Box<dyn Any + Send>;

fn retain_panic(first: &mut Option<PanicPayload>, panic: PanicPayload) {
    if first.is_none() {
        *first = Some(panic);
    } else {
        discard_panic(panic);
    }
}

fn capture<T>(first: &mut Option<PanicPayload>, action: impl FnOnce() -> T) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(result) => Some(result),
        Err(panic) => {
            retain_panic(first, panic);
            None
        }
    }
}

#[inline(always)]
fn guarded<T>(action: impl FnOnce() -> T) -> Result<T, PanicPayload> {
    catch_unwind(AssertUnwindSafe(action))
}

/// Runs `action` under its own unwind boundary. A panic is reported at `site` with
/// `stream`, disposed of, and marks `failure` panicked.
#[inline(always)]
fn contain<T>(
    context: &WorkerContext,
    failure: &mut DriverFailure,
    site: PanicSite,
    stream: Option<StreamId>,
    action: impl FnOnce() -> T,
) -> Option<T> {
    match guarded(action) {
        Ok(result) => Some(result),
        Err(panic) => {
            contained(context, failure, site, stream, panic);
            None
        }
    }
}

#[cold]
#[inline(never)]
fn contained(
    context: &WorkerContext,
    failure: &mut DriverFailure,
    site: PanicSite,
    stream: Option<StreamId>,
    panic: PanicPayload,
) {
    report_contained_panic(context, site, stream, &*panic);
    discard_panic(panic);
    failure.panicked = true;
}

/// Reports a panic in a teardown step or a released field, and disposes of it.
#[cold]
#[inline(never)]
fn teardown_panicked(context: &WorkerContext, panic: PanicPayload) {
    report_contained_panic(context, PanicSite::Teardown, None, &*panic);
    discard_panic(panic);
}

/// Reports a panic contained at `site` and disposes of it, leaving the connection running.
#[cold]
#[inline(never)]
fn report_panic(
    context: &WorkerContext,
    site: PanicSite,
    stream: Option<StreamId>,
    panic: PanicPayload,
) {
    report_contained_panic(context, site, stream, &*panic);
    discard_panic(panic);
}

/// Reports a panic contained at an Exchange-scope `site` of stream `id`, disposes of it, and
/// returns the error that fails that exchange alone.
#[cold]
#[inline(never)]
fn exchange_panicked(
    context: &WorkerContext,
    site: PanicSite,
    id: StreamId,
    panic: PanicPayload,
) -> HttpError {
    report_panic(context, site, Some(id), panic);
    failure(
        HttpErrorCode::INTERNAL_ERROR,
        b"owned H2 exchange callback panicked",
    )
}

/// Unwinds to the per-turn catch after a Driver-scope panic its site already reported, so the
/// catch starts the teardown without reporting it again.
struct ReportedPanic;

/// Reports a panic that reached the per-turn catch, unless its site already did.
#[cold]
#[inline(never)]
fn report_turn_panic(context: &WorkerContext, panic: &PanicPayload) {
    if !panic.is::<ReportedPanic>() {
        report_contained_panic(context, PanicSite::DriverTurn, None, &**panic);
    }
}

/// Drops `field` under its own unwind boundary, reporting a panic as a teardown step's.
#[inline(always)]
fn release_field<T>(context: &WorkerContext, field: T) {
    if let Err(panic) = guarded(|| drop(field)) {
        teardown_panicked(context, panic);
    }
}

impl<E, const UPSTREAM: bool> Drop for Stream<E, UPSTREAM>
where
    E: ExchangeStorage<UPSTREAM>,
{
    fn drop(&mut self) {
        self.wake.live.store(false, Ordering::Release);
        let mut panic = None;
        if self.application.is_some() {
            capture(&mut panic, || drop(self.application.take()));
        }
        capture(&mut panic, || drop(self.source.take()));
        capture(&mut panic, || drop(self.drain.take()));
        capture(&mut panic, || drop(self.pending.take()));
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.head.take()));
        if let Some(panic) = panic {
            if std::thread::panicking() {
                discard_panic(panic);
            } else {
                resume_unwind(panic);
            }
        }
    }
}

enum OwnerPollResult {
    Polled,
    Serialized(Result<(), HttpError>),
}

#[derive(Clone, Copy)]
struct SerializationBudget {
    wire: usize,
    credit: u32,
}

fn serialization_budget(
    config: &UniplexConfig,
    egress: &BufQueue,
    codec: &FlowControlledCodec,
) -> Option<SerializationBudget> {
    if egress.chain_length() >= config.maximum_write_size {
        return None;
    }
    let wire = config
        .maximum_write_size
        .saturating_sub(egress.chain_length())
        .min(egress.remaining_capacity());
    let frame = roxygen_http2::constants::MAX_FRAME_PAYLOAD_LENGTH_MIN as usize;
    let overhead = roxygen_http2::constants::FRAME_HEADER_SIZE;
    let payload =
        (wire / (frame + overhead)) * frame + (wire % (frame + overhead)).saturating_sub(overhead);
    Some(SerializationBudget {
        wire,
        credit: codec.available_send().min(payload as u32),
    })
}

struct Failure {
    stream: StreamId,
    error: HttpError,
    status: Option<u16>,
    wire_code: Option<H2ErrorCode>,
    direction: HttpExceptionDirection,
    new_stream: bool,
    /// Set by `CodecInput::fail` when parsing ingress raised the failure. A lifecycle observer
    /// receives it as `Http2IngressError`.
    ingress: bool,
    /// The stream's header block breached the header rate limit: the stream is aborted
    /// without a reset and the connection drains with one final GOAWAY.
    header_rate_limit: bool,
}

impl Failure {
    fn new(stream: StreamId, error: HttpError) -> Self {
        Self {
            stream,
            error,
            status: None,
            wire_code: None,
            direction: HttpExceptionDirection::IngressAndEgress,
            new_stream: false,
            ingress: false,
            header_rate_limit: false,
        }
    }
}

/// Live streams by id. Peers choose stream ids, so each session draws its own seed. Iteration
/// follows the hash, so every site whose order is observable visits the streams sorted by id.
/// A stream is reached mutably only through the methods below, which record in the flow index
/// which streams may have changed.
struct StreamTable<E, const UPSTREAM: bool>
where
    E: ExchangeStorage<UPSTREAM>,
{
    map: HashMap<StreamId, Box<Stream<E, UPSTREAM>>, SeedableRandomState>,
    /// The flow-blocked streams, under a nonzero `write_timeout`.
    flow: Option<Box<FlowIndex>>,
}

#[cfg(test)]
thread_local! {
    /// Full refreshes of a flow index on this thread, for tests.
    pub(crate) static FLOW_FULL_REFRESHES: Cell<usize> = const { Cell::new(0) };
}

/// The streams the `flow` slot can wait for, tracked per stream instead of by a pass over every
/// stream at each idle check. `Stream::send_blocked` and `Stream::awaits_credit` read only the
/// stream's own plain fields, which change only through a `&mut Stream`. The table hands one out
/// only after recording its id in `touched`, and `StreamTable::refresh_flow` evaluates both
/// predicates again for exactly those streams.
#[derive(Default)]
struct FlowIndex {
    /// Streams reached mutably since the last refresh; may repeat.
    touched: Vec<StreamId>,
    /// Every stream may have changed: `iter_mut`, or `touched` outgrew the table.
    all: bool,
    /// The streams whose `send_blocked` holds, sorted by id.
    send_blocked: Vec<StreamId>,
    /// The streams whose `awaits_credit` holds, sorted by id.
    awaits_credit: Vec<StreamId>,
}

impl FlowIndex {
    #[inline(always)]
    fn touch(&mut self, id: StreamId, live: usize) {
        if self.all || self.touched.last() == Some(&id) {
            return;
        }
        if self.touched.len() > 2 * live + 32 {
            self.touch_all();
        } else {
            self.touched.push(id);
        }
    }

    #[cold]
    #[inline(never)]
    fn touch_all(&mut self) {
        self.all = true;
        self.touched.clear();
    }

    fn set_member(set: &mut Vec<StreamId>, id: StreamId, member: bool) {
        match (set.binary_search(&id), member) {
            (Err(at), true) => set.insert(at, id),
            (Ok(at), false) => {
                set.remove(at);
            }
            _ => {}
        }
    }
}

impl<E, const UPSTREAM: bool> StreamTable<E, UPSTREAM>
where
    E: ExchangeStorage<UPSTREAM>,
{
    fn with_hasher(state: SeedableRandomState, flow_index: bool) -> Self {
        Self {
            map: HashMap::with_hasher(state),
            flow: flow_index.then(Box::default),
        }
    }

    #[inline(always)]
    fn touch(&mut self, id: StreamId) {
        if let Some(flow) = &mut self.flow {
            flow.touch(id, self.map.len());
        }
    }

    #[inline(always)]
    fn get_mut(&mut self, id: &StreamId) -> Option<&mut Box<Stream<E, UPSTREAM>>> {
        self.touch(*id);
        self.map.get_mut(id)
    }

    #[inline(always)]
    fn entry(&mut self, id: StreamId) -> Entry<'_, StreamId, Box<Stream<E, UPSTREAM>>> {
        self.touch(id);
        self.map.entry(id)
    }

    #[inline(always)]
    fn remove(&mut self, id: &StreamId) -> Option<Box<Stream<E, UPSTREAM>>> {
        self.touch(*id);
        self.map.remove(id)
    }

    #[inline(always)]
    fn insert(
        &mut self,
        id: StreamId,
        stream: Box<Stream<E, UPSTREAM>>,
    ) -> Option<Box<Stream<E, UPSTREAM>>> {
        self.touch(id);
        self.map.insert(id, stream)
    }

    fn iter_mut(
        &mut self,
    ) -> std::collections::hash_map::IterMut<'_, StreamId, Box<Stream<E, UPSTREAM>>> {
        if let Some(flow) = &mut self.flow {
            flow.touch_all();
        }
        self.map.iter_mut()
    }

    /// Brings the flow index up to date with every stream reached mutably since the last
    /// refresh, and returns it, or `None` if there is no index. Many records relative to the
    /// table, such as one read of WINDOW_UPDATE frames that unblocks every blocked stream, take
    /// one full pass instead of a sorted-set shift per stream.
    fn refresh_flow(&mut self) -> Option<&FlowIndex> {
        let Self { map, flow } = self;
        let flow = flow.as_deref_mut()?;
        if flow.touched.len() > 64 && 4 * flow.touched.len() > map.len() {
            flow.all = true;
        }
        if flow.all {
            #[cfg(test)]
            FLOW_FULL_REFRESHES.with(|count| count.set(count.get() + 1));
            flow.all = false;
            flow.touched.clear();
            flow.send_blocked.clear();
            flow.awaits_credit.clear();
            for (id, stream) in map.iter() {
                if stream.send_blocked() {
                    flow.send_blocked.push(*id);
                }
                if stream.awaits_credit() {
                    flow.awaits_credit.push(*id);
                }
            }
            flow.send_blocked.sort_unstable();
            flow.awaits_credit.sort_unstable();
        } else {
            for index in 0..flow.touched.len() {
                let id = flow.touched[index];
                let (blocked, credit) = map.get(&id).map_or((false, false), |stream| {
                    (stream.send_blocked(), stream.awaits_credit())
                });
                FlowIndex::set_member(&mut flow.send_blocked, id, blocked);
                FlowIndex::set_member(&mut flow.awaits_credit, id, credit);
            }
            flow.touched.clear();
        }
        Some(flow)
    }

    fn ids_in_order(&self) -> Vec<StreamId> {
        let mut ids: Vec<_> = self.map.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn drop_in_order(&mut self) {
        for id in self.ids_in_order() {
            drop(self.map.remove(&id));
        }
    }
}

/// Drops the remaining streams in id order, since drop order is observable and the map's own
/// order follows the hash. If a stream's drop panics, the rest still drop in id order while
/// unwinding.
impl<E, const UPSTREAM: bool> Drop for StreamTable<E, UPSTREAM>
where
    E: ExchangeStorage<UPSTREAM>,
{
    fn drop(&mut self) {
        struct Rest<'a, E, const UPSTREAM: bool>(&'a mut StreamTable<E, UPSTREAM>)
        where
            E: ExchangeStorage<UPSTREAM>;

        impl<E, const UPSTREAM: bool> Drop for Rest<'_, E, UPSTREAM>
        where
            E: ExchangeStorage<UPSTREAM>,
        {
            fn drop(&mut self) {
                self.0.drop_in_order();
            }
        }

        if !self.map.is_empty() {
            Rest(self).0.drop_in_order();
        }
    }
}

/// Read access only: mutable access goes through the tracked methods above.
impl<E, const UPSTREAM: bool> Deref for StreamTable<E, UPSTREAM>
where
    E: ExchangeStorage<UPSTREAM>,
{
    type Target = HashMap<StreamId, Box<Stream<E, UPSTREAM>>, SeedableRandomState>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

/// A per-session stream-table hash state. foldhash is built without `std`, so the per-hasher
/// seed comes from std's `RandomState`.
fn stream_hash_state() -> SeedableRandomState {
    SeedableRandomState::with_seed(
        RandomState::new().hash_one("roxygen owned h2 streams"),
        SharedSeed::global_random(),
    )
}

#[cfg(test)]
fn fixed_stream_hash_state(seed: u64) -> SeedableRandomState {
    SeedableRandomState::with_seed(seed, SharedSeed::global_fixed())
}

/// The order in which a stream table seeded with `seed` iterates `streams` inserted in order.
#[cfg(test)]
pub(crate) fn stream_seed_order(seed: u64, streams: &[u64]) -> Vec<u64> {
    let mut table = HashMap::with_hasher(fixed_stream_hash_state(seed));
    table.extend(streams.iter().map(|stream| (StreamId::new(*stream), ())));
    table.keys().map(|stream| stream.get()).collect()
}

#[cfg(test)]
thread_local! {
    static OWNER_WRITE_STATE_READS: Cell<usize> = const { Cell::new(0) };
    static READ_START_CHECKS: Cell<usize> = const { Cell::new(0) };
    static INLINE_POLLS: Cell<usize> = const { Cell::new(0) };
    static TURNS: Cell<usize> = const { Cell::new(0) };
    static OWNER_POLLS: Cell<usize> = const { Cell::new(0) };
    static SERIALIZE_PROBES: Cell<usize> = const { Cell::new(0) };
    static CONN_BLOCKED_PEAK: Cell<usize> = const { Cell::new(0) };
    static RUN_FAULT: Cell<bool> = const { Cell::new(false) };
    static COMMIT_FAULT: Cell<bool> = const { Cell::new(false) };
}

/// Makes the next pending poll of an owned H2 run on this thread panic outside the driver's
/// boundaries, where only a fault in the run's own code could.
#[cfg(test)]
pub(crate) fn fault_next_run_poll() {
    RUN_FAULT.set(true);
}

/// Makes the next request commitment of an owned H2 upstream on this thread panic midway, where
/// only a fault in Roxygen's commit code could.
#[cfg(test)]
pub(crate) fn fault_next_commit() {
    COMMIT_FAULT.set(true);
}

/// How many times stream-owner code on this thread has evaluated whether writes are pending.
#[cfg(test)]
pub(crate) fn owner_write_state_reads() -> usize {
    OWNER_WRITE_STATE_READS.with(Cell::get)
}

#[cfg(test)]
thread_local! {
    static DISCARDED_STREAMS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

/// The refused streams that a downstream session on this thread still tracked after its last
/// parse, and the peak count; resets both.
#[cfg(test)]
pub(crate) fn take_discarded_streams() -> (usize, usize) {
    DISCARDED_STREAMS.with(|streams| streams.replace((0, 0)))
}

/// How many times the owned H2 drivers on this thread, with no read started, have evaluated
/// whether to start one (`Driver::poll_read`).
#[cfg(test)]
pub(crate) fn read_start_checks() -> usize {
    READ_START_CHECKS.with(Cell::get)
}

/// How many transport polls the owned H2 drivers on this thread have held with
/// `IoWake::poll_inline`.
#[cfg(test)]
pub(crate) fn inline_polls() -> usize {
    INLINE_POLLS.with(Cell::get)
}

/// How many turns the owned H2 drivers on this thread have started.
#[cfg(test)]
pub(crate) fn turns() -> usize {
    TURNS.with(Cell::get)
}

/// How many stream owner polls the owned H2 drivers on this thread have run.
#[cfg(test)]
pub(crate) fn owner_polls() -> usize {
    OWNER_POLLS.with(Cell::get)
}

/// How many times the owned H2 drivers on this thread have checked whether a stream's pending
/// event can serialize.
#[cfg(test)]
pub(crate) fn serialize_probes() -> usize {
    SERIALIZE_PROBES.with(Cell::get)
}

/// The most entries that an owned H2 driver on this thread has held in its list of bodies blocked
/// on the connection window since the last call; resets the count.
#[cfg(test)]
pub(crate) fn take_conn_blocked_peak() -> usize {
    CONN_BLOCKED_PEAK.replace(0)
}

/// A downstream HTTP/2 driver with concrete exchange owners.
///
/// Policy and body futures are children owned directly by each stream, and the
/// driver does not spawn application tasks. Cancelling a stream retires those
/// futures before the stream's ingress-source and transport-write fences can complete.
pub struct OwnedH2Session<A: OwnedApplication>(Option<Driver<A, false>>);

/// A session dropped unrun is abandoned as a run would be.
impl<A: OwnedApplication> Drop for OwnedH2Session<A> {
    fn drop(&mut self) {
        if let Some(driver) = self.0.take() {
            driver.abandon(false);
        }
    }
}

/// Drives a [`Driver`] to completion. A poll never unwinds out of driver code. A panic that
/// reaches the per-turn boundary is reported and starts a teardown that joins native I/O,
/// after which the run returns `Err`. Completion closes admission, publishes `complete` and
/// releases the driver before returning. A run dropped unfinished is abandoned.
struct DriverRun<A, const UPSTREAM: bool>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    driver: Option<Driver<A, UPSTREAM>>,
    /// Set while a poll runs and cleared as it returns, so it stays set only after a poll that
    /// unwound. Such a run publishes `complete` and returns `Err` without running its driver
    /// again, and its abandonment does not assert that its cleanup joined.
    poisoned: bool,
}

impl<A, const UPSTREAM: bool> Drop for DriverRun<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abandon(self.poisoned);
        }
    }
}

/// Brackets one poll of a session's run, including its teardown and its lifecycle delivery, so
/// that `Control::schedule` only queues owners. On every way out, an owner left queued wakes the
/// session through `Control::owner_waker`, unless the session waits for a budget yield or tears
/// down, which does not poll owners. It holds the control weakly, so it never drops the last
/// reference to it.
struct PollExit(Option<Weak<Control>>);

impl PollExit {
    #[inline(always)]
    fn enter(control: &Rc<Control>) -> Self {
        control.notified.set(false);
        control.polling.set(true);
        Self(Some(Rc::downgrade(control)))
    }

    /// Ends a poll after which the run continues, reporting a panicking waker at Driver scope.
    #[inline(always)]
    fn finish<A, const UPSTREAM: bool>(mut self, driver: &Driver<A, UPSTREAM>)
    where
        A: OwnedApplication,
        A::Exchange: ExchangeStorage<UPSTREAM>,
    {
        self.0 = None;
        let control = &driver.connection.state.control;
        control.polling.set(false);
        if driver.budget_yield.is_none()
            && driver.teardown.is_none()
            && !control.owners.borrow().is_empty()
        {
            notify_at_exit(control, &driver.context);
        }
    }
}

impl Drop for PollExit {
    /// Ends a poll that completed the run or unwound, discarding a panicking waker's panic.
    fn drop(&mut self) {
        if let Some(control) = self.0.take().and_then(|control| control.upgrade()) {
            end_abandoned_poll(&control);
        }
    }
}

#[cold]
#[inline(never)]
fn notify_at_exit(control: &Control, context: &WorkerContext) {
    if let Err(panic) = guarded(|| control.notify()) {
        report_panic(context, PanicSite::DriverTurn, None, panic);
    }
}

#[cold]
#[inline(never)]
fn end_abandoned_poll(control: &Control) {
    control.polling.set(false);
    if control.owners.borrow().is_empty() {
        return;
    }
    if let Err(panic) = guarded(|| control.notify()) {
        discard_panic(panic);
    }
}

/// Marks the upstream admission as polled by its driver while the guard lives, then restores
/// the previous mark. It holds the admission weakly, so it never drops the last reference to
/// it. Downstream it is `None`.
struct AdmissionPolling(Option<(Weak<upstream::Admission>, bool)>);

impl AdmissionPolling {
    #[inline(always)]
    fn enter<A, const UPSTREAM: bool>(driver: &Driver<A, UPSTREAM>) -> Self
    where
        A: OwnedApplication,
        A::Exchange: ExchangeStorage<UPSTREAM>,
    {
        match &driver.connection.state.control.upstream {
            Some(admission) if UPSTREAM => Self(Some((
                Rc::downgrade(admission),
                admission.polling.replace(true),
            ))),
            _ => Self(None),
        }
    }
}

impl Drop for AdmissionPolling {
    fn drop(&mut self) {
        if let Some((admission, previous)) = &self.0
            && let Some(admission) = admission.upgrade()
        {
            admission.polling.set(*previous);
        }
    }
}

#[cold]
#[inline(never)]
fn poisoned_run() -> HttpError {
    failure(
        HttpErrorCode::INTERNAL_ERROR,
        b"owned H2 runner unwound out of an earlier poll",
    )
}

#[cold]
#[inline(never)]
fn polled_after_completion() -> HttpError {
    failure(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"owned H2 runner polled after completion",
    )
}

// The driver is only ever accessed through `&mut`, so its address need not stay fixed.
impl<A, const UPSTREAM: bool> Unpin for DriverRun<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
}

impl<A, const UPSTREAM: bool> Future for DriverRun<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    type Output = Result<UniplexRunSummary, HttpError>;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.poisoned {
            if let Some(driver) = &this.driver {
                driver.connection.state.control.complete.set(true);
            }
            return Poll::Ready(Err(poisoned_run()));
        }
        let Some(driver) = this.driver.as_mut() else {
            return Poll::Ready(Err(polled_after_completion()));
        };
        this.poisoned = true;
        let exit = PollExit::enter(&driver.connection.state.control);
        let polled = {
            let _polling = AdmissionPolling::enter(driver);
            let polled = if driver.teardown.is_some() {
                driver.poll_teardown(context)
            } else {
                driver.started = true;
                match catch_unwind(AssertUnwindSafe(|| driver.poll(context))) {
                    Ok(polled) => polled,
                    Err(panic) => {
                        report_turn_panic(&driver.context, &panic);
                        driver.poll_panicked(panic, context);
                        Poll::Pending
                    }
                }
            };
            polled.map(|result| {
                this.driver
                    .take()
                    .expect("owned H2 runner completes once")
                    .finish_run(result)
            })
        };
        if polled.is_pending()
            && let Some(driver) = this.driver.as_mut()
            && driver.teardown.is_none()
        {
            #[cfg(test)]
            if RUN_FAULT.take() {
                panic!("owned H2 run fault");
            }
            // Every step a session test drives ends with a pending poll.
            #[cfg(test)]
            driver.debug_assert_stream_counts();
            if driver.connection.state.observation.is_some() {
                driver.flush_observation(context);
            }
        }
        if let Some(driver) = &this.driver {
            exit.finish(driver);
        }
        this.poisoned = false;
        polled
    }
}

struct Driver<A, const UPSTREAM: bool>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    context: WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    application: A,
    h2: Http2SessionConfig,
    connection: OwnedConnection,
    streams: StreamTable<A::Exchange, UPSTREAM>,
    ingress: BufQueue,
    egress: BufQueue,
    read: ReadSlot,
    write: Option<LocalTransportFuture<Result<WriteInfo, TransportWriteError>>>,
    write_queue: Option<Box<dyn UniplexWriteQueue>>,
    read_wake: Arc<IoWake>,
    read_waker: Waker,
    write_wake: Arc<IoWake>,
    write_waker: Waker,
    cancellation: CancellationSource,
    write_cancellation: CancellationSource,
    transport_reset: bool,
    write_end: u64,
    eof_processed: bool,
    failure: DriverFailure,
    started: bool,
    eof: bool,
    sent_initial_goaway: bool,
    sent_final_goaway: bool,
    goaway_deadline: Option<Instant>,
    budget_yield: Option<roxygen_runtime::host::LocalFuture>,
    timers: Option<Box<ConnectionTimers>>,
    /// `Http2SessionConfig::deadlines` of a downstream session that sets one.
    deadline: Option<Box<DeadlineTimer<IoWake>>>,
    /// The last `poll_read` left a drained reader's next read waiting for its waker. For
    /// `read_idle`, that read counts as issued and outstanding.
    read_parked: bool,
    /// A queued write started without arming the writer, and `poll_complete` has not since
    /// registered its waker or reported the queue idle.
    unarmed_write: bool,
    /// Set once a Driver-scope panic started the teardown.
    teardown: Option<Box<TeardownState>>,
}

/// Counts each native operation's teardown polls that panicked, in total.
#[derive(Default)]
struct TeardownState {
    read_panics: u8,
    write_panics: u8,
    queue_panics: u8,
}

/// The panicking teardown polls after which a native operation is dropped unfinished.
const TEARDOWN_PANIC_LIMIT: u8 = 3;

struct OwnedConnection {
    codec: FlowControlledCodec,
    state: ConnectionState,
}

struct ConnectionState {
    observer: AsyncSocketByteEventObserver,
    control: Rc<Control>,
    config: UniplexConfig,
    scheduler: PriorityQueue,
    /// Streams whose body waits for connection credit, in the order they left the scheduler's
    /// head. An entry can outlive its stream's wait: each is checked when the list drains.
    conn_blocked: Vec<StreamId>,
    write_fences: VecDeque<(u64, StreamId)>,
    peer_initial_window: u32,
    peer_maximum_streams: u32,
    peer_push_enabled: bool,
    incoming_limit: u64,
    zero_write_pending: bool,
    /// Live streams that still await wire ingress: neither ingress-complete nor reset.
    awaiting_ingress: usize,
    /// Live streams with `Stream::consumed` set.
    consumed_streams: usize,
    /// Set before the first byte-event registration. Until then the observer is empty.
    byte_events_registered: bool,
    /// Streams this session refused itself, kept until their header block completes. A codec
    /// error for one is ignored, since the refusal already reset it.
    discarded: BTreeMap<StreamId, bool>,
    rejected_credit: usize,
    draining: bool,
    ingress_grace: bool,
    summary: UniplexRunSummary,
    observation: Option<Box<Observation>>,
    /// Downstream flood limits; `None` while every category is unbounded.
    rate_limits: Option<Box<Http2RateLimiter>>,
}

struct DriverFailure {
    failures: VecDeque<Failure>,
    fatal: Option<HttpError>,
    /// A boundary contained a panic, which aborts the connection where it would otherwise
    /// continue.
    panicked: bool,
}

impl ConnectionState {
    /// Call just before `stream` completes its wire ingress, is reset, or retires.
    fn settle_ingress<E, const UPSTREAM: bool>(&mut self, stream: &Stream<E, UPSTREAM>)
    where
        E: ExchangeStorage<UPSTREAM>,
    {
        if stream.awaits_ingress() {
            self.awaiting_ingress -= 1;
        }
    }
}

struct StreamConnection<'a, const UPSTREAM: bool> {
    connection: &'a mut OwnedConnection,
    context: &'a WorkerContext,
    failure: &'a mut DriverFailure,
    egress: &'a mut BufQueue,
    write: &'a Option<LocalTransportFuture<Result<WriteInfo, TransportWriteError>>>,
    write_queue: Option<&'a dyn UniplexWriteQueue>,
    transport_reset: bool,
}

fn writes_pending(
    write: &Option<LocalTransportFuture<Result<WriteInfo, TransportWriteError>>>,
    write_queue: Option<&dyn UniplexWriteQueue>,
) -> bool {
    write.is_some() || write_queue.is_some_and(|queue| !queue.is_idle())
}

struct NoopCallback;
impl HttpCodecCallback for NoopCallback {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}
}

#[cold]
#[inline(never)]
fn failure(code: HttpErrorCode, message: &'static [u8]) -> HttpError {
    HttpError::with_message(code, message)
}

/// `None` is a panic captured from the observer callback.
#[cold]
#[inline(never)]
fn retain_observer_failure<T>(
    failed: &mut Option<HttpError>,
    observed: Option<Result<T, HttpError>>,
) {
    match observed {
        Some(Ok(_)) => {}
        Some(Err(error)) => {
            failed.get_or_insert(error);
        }
        None => {
            failed.get_or_insert(failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"owned H2 callback panicked",
            ));
        }
    }
}

/// Only called for failed or short writes.
#[cold]
#[inline(never)]
fn retain_write_failure(
    failed: &mut Option<HttpError>,
    result: Result<WriteInfo, TransportWriteError>,
) {
    match result {
        Err(error) => {
            failed.get_or_insert_with(|| error.error().clone());
        }
        Ok(_) => {
            failed.get_or_insert(failure(
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"transport reported a short successful write",
            ));
        }
    }
}

#[cold]
#[inline(never)]
fn retain_cancellation(failed: &mut Option<HttpError>) {
    failed.get_or_insert(failure(
        HttpErrorCode::CORO_CANCELLED,
        b"owned H2 connection cancelled",
    ));
}

#[cold]
#[inline(never)]
fn push_connection_failure(failures: &mut VecDeque<Failure>, error: HttpError) {
    failures.push_back(Failure::new(StreamId::UNSPECIFIED, error));
}

/// Counts one event of a category, reading the clock only when the category is bounded.
#[cold]
#[inline(never)]
fn record_rate(
    limiter: &mut Http2RateLimiter,
    kind: RateLimitKind,
    now: impl FnOnce() -> Instant,
) -> bool {
    !limiter.is_unbounded(kind) && limiter.record(kind, now()).0
}

#[cfg(test)]
thread_local! {
    static OBSERVATIONS_CREATED: Cell<usize> = const { Cell::new(0) };
}

/// How many sessions on this thread have created their `Observation`.
#[cfg(test)]
pub(crate) fn observations_created() -> usize {
    OBSERVATIONS_CREATED.get()
}

/// Reports a refused or rejected stream's head as an ingress message, unless it has a forbidden
/// body, as `UniplexSession` does.
#[cold]
#[inline(never)]
fn observe_rejected_head(observation: &Observation, stream: StreamId, message: &mut HttpMessage) {
    if !crate::uniplex::request_has_forbidden_body(message) {
        observation.push(LifecycleEvent::Http2IngressMessage { stream });
    }
}

const fn transport_direction<const UPSTREAM: bool>() -> TransportDirection {
    if UPSTREAM {
        TransportDirection::Upstream
    } else {
        TransportDirection::Downstream
    }
}

/// Resolves the stream receive window and whether it must be advertised.
///
/// As in `UniplexSession`, `configured` is normalized against the last advertised
/// `INITIAL_WINDOW_SIZE`: a non-default window with no advertised value is advertised, and a
/// mirrored advertised value replaces the default window. Unlike `UniplexSession`, which keeps
/// both, a preserved window must equal an advertised one, because a source window below the
/// advertised one resets streams that stay within the peer's credit. Returns `None` to reject
/// the configuration.
fn owned_receive_window(
    mode: StreamReceiveWindowMode,
    advertised: Option<u64>,
    configured: u32,
) -> Option<(u32, bool)> {
    let representable = |window: u32| window != 0 && window <= i32::MAX as u32;
    if !representable(configured) {
        return None;
    }
    let Some(advertised) = advertised else {
        return Some((configured, configured != DEFAULT_STREAM_WINDOW));
    };
    let advertised = u32::try_from(advertised)
        .ok()
        .filter(|window| representable(*window))?;
    (configured == advertised
        || (mode == StreamReceiveWindowMode::MirrorAdvertisedSettings
            && configured == DEFAULT_STREAM_WINDOW))
        .then_some((advertised, false))
}

impl<A: OwnedApplication> OwnedH2Session<A> {
    /// Creates the session.
    ///
    /// It applies `read_timeout`, whose expiry drains the connection with grace, and
    /// `goaway_timeout`. `write_timeout` bounds a stalled transport write and the writer's wait
    /// for flow-control credit. `stream_read_timeout` bounds each wait for request body. If it
    /// expires before the response head is sent, the stream gets a 408 response. Only the flood
    /// categories that `Http2SessionConfig::rate_limits` bounds are limited.
    /// `Http2SessionConfig::abort_streams_on_eof` fails the connection on a peer EOF while a
    /// stream is open. Options the driver does not support, such as
    /// `Http2SessionConfig::stats` or PING probes, make this return an error.
    pub fn new(
        application: A,
        options: OwnedH2Options,
    ) -> Result<(Self, OwnedH2Handle), HttpError> {
        Self::new_with(options, |_| application)
    }

    /// Creates the session as `new` does, with the application that `make` builds from the
    /// session's handle, so the application can keep the handle and drain or cancel its own
    /// session. `make` runs once, after every check that can fail creation, so it does not run
    /// for a session that returns an error.
    pub fn new_with(
        options: OwnedH2Options,
        make: impl FnOnce(&OwnedH2Handle) -> A,
    ) -> Result<(Self, OwnedH2Handle), HttpError> {
        let (driver, handle) = Driver::<A, false>::new_with(options, make)?;
        Ok((Self(Some(driver)), handle))
    }

    /// Replaces the per-session random stream-table seed with a fixed one.
    #[cfg(test)]
    pub(crate) fn with_stream_seed(mut self, seed: u64) -> Self {
        let driver = self.0.take().expect("an unrun session holds its driver");
        Self(Some(driver.with_stream_seed(seed)))
    }

    /// Runs the session inside the caller's registered worker cleanup scope (see
    /// `WorkerContext::register_scope`).
    ///
    /// The run never resumes a callback panic. A panic that aborts the connection is counted in
    /// the worker's `WorkerPollStats`, and the run returns `Err` once native I/O has joined.
    ///
    /// Dropping the run after its first poll and before it completes panics, outside a forced
    /// worker teardown, because its native I/O has not joined. To stop early, call the handle's
    /// `cancel` and poll the run to completion. Dropping the session or its run before the first
    /// poll does not panic.
    pub fn run(mut self) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> {
        self.0
            .take()
            .expect("an unrun session holds its driver")
            .run()
    }

    /// Adds an owner-local lifecycle observer, as `UniplexSession::add_lifecycle_observer`
    /// does. Its `on_attached` runs before this returns. The run delivers events at the end of
    /// each poll, and `Destroy` when it completes. A session dropped unrun delivers only
    /// `Destroy`. A session without observers does not queue events.
    pub fn add_lifecycle_observer(
        &mut self,
        observer: Rc<dyn LifecycleObserver>,
    ) -> Result<LifecycleObserverId, HttpError> {
        let driver = self.0.as_mut().expect("an unrun session holds its driver");
        let lifecycle = match &driver.connection.state.observation {
            Some(observation) => Rc::clone(&observation.lifecycle),
            None => {
                driver.connection.state.control.window_transition.set(None);
                let observation = Observation::new(driver.h2.max_concurrent_outgoing_streams);
                let lifecycle = Rc::clone(&observation.lifecycle);
                driver.connection.state.observation = Some(observation);
                lifecycle
            }
        };
        Ok(lifecycle.add_observer(observer))
    }

    /// Whether the session observes its lifecycle.
    #[cfg(test)]
    pub(crate) fn observed(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|driver| driver.connection.state.observation.is_some())
    }

    /// Whether the session built a flood-limit counter.
    #[cfg(test)]
    pub(crate) fn limits_rates(&self) -> bool {
        self.0.as_ref().is_some_and(Driver::limits_rates)
    }
}

impl<A, const UPSTREAM: bool> Driver<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    #[cfg(test)]
    fn limits_rates(&self) -> bool {
        self.connection.state.rate_limits.is_some()
    }

    #[cfg(test)]
    fn with_stream_seed(mut self, seed: u64) -> Self {
        assert!(
            self.streams.is_empty(),
            "the seed is fixed before any stream opens"
        );
        let flow_index = self.streams.flow.is_some();
        self.streams = StreamTable::with_hasher(fixed_stream_hash_state(seed), flow_index);
        self
    }

    fn new(application: A, options: OwnedH2Options) -> Result<(Self, OwnedH2Handle), HttpError> {
        Self::new_with(options, |_| application)
    }

    fn new_with(
        options: OwnedH2Options,
        make: impl FnOnce(&OwnedH2Handle) -> A,
    ) -> Result<(Self, OwnedH2Handle), HttpError> {
        let OwnedH2Options {
            context,
            transport,
            io,
            mut http2,
        } = options;
        let io = io.validate()?;
        if !context.is_owner_thread()
            || context.is_cancelled()
            || io.buffer_limit == 0
            || io.maximum_read_size == 0
            || io.maximum_write_size <= roxygen_http2::constants::FRAME_HEADER_SIZE
            || io.maximum_read_size > io.buffer_limit
            || io.maximum_write_size > io.buffer_limit
            || (!UPSTREAM && !io.drain_request_body_on_drop)
            || (UPSTREAM && io.drain_request_body_on_drop)
            || (!UPSTREAM && io.deliver_ingress_before_abort)
            || (UPSTREAM && http2.abort_streams_on_eof)
            || http2.stats.is_some()
            || http2
                .ping_probes
                .is_some_and(|probes| !probes.interval.is_zero())
            || http2.connection_receive_window > i32::MAX as u32
            || transport.byte_event_capability(&context)? != ByteEventCapability::Unsupported
        {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H2 diagnostic configuration is not supported",
            ));
        }
        if UPSTREAM && io.exact_method_tokens {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H2 upstream does not support exact_method_tokens",
            ));
        }
        if UPSTREAM && io.verbatim_http2_header_values {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H2 upstream does not support verbatim_http2_header_values",
            ));
        }
        let advertised_window = http2.settings.iter().rev().find_map(|setting| {
            (setting.identifier == SettingsId::InitialWindowSize.as_u16()).then_some(setting.value)
        });
        let Some((stream_receive_window, advertise_window)) = owned_receive_window(
            http2.stream_receive_window_mode,
            advertised_window,
            http2.stream_receive_window,
        ) else {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H2 requires matching advertised and source receive windows",
            ));
        };
        http2.stream_receive_window = stream_receive_window;
        let incoming_limit = http2
            .settings
            .iter()
            .rev()
            .find_map(|setting| {
                (setting.identifier == SettingsId::MaxConcurrentStreams.as_u16())
                    .then_some(setting.value)
            })
            .unwrap_or(u64::from(u32::MAX));
        if UPSTREAM
            && (http2.max_concurrent_outgoing_streams == 0
                || http2.settings.iter().rev().find_map(|setting| {
                    (setting.identifier == SettingsId::EnablePush.as_u16()).then_some(setting.value)
                }) != Some(0))
        {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned upstream H2 requires nonzero admission and disabled push",
            ));
        }
        let rate_limits = (!UPSTREAM && !http2.rate_limits.is_unbounded()).then(|| {
            Box::new(Http2RateLimiter::new(
                http2.rate_limits,
                context.host().now(),
            ))
        });
        let ready = Arc::new(ReadyQueue::default());
        let upstream = UPSTREAM.then(|| {
            Rc::new(upstream::Admission::new(
                Arc::clone(&ready),
                context.clone(),
                http2.max_concurrent_outgoing_streams,
            ))
        });
        let control = Rc::new(Control {
            drain: Cell::new(false),
            cancel: Cell::new(false),
            complete: Cell::new(false),
            handshake_complete: Cell::new(false),
            active_incoming: Cell::new(0),
            ready,
            owners: RefCell::new(VecDeque::new()),
            polling: Cell::new(false),
            owner_waker: RefCell::new(None),
            notified: Cell::new(false),
            upstream,
            idle: Cell::new(false),
            host: context.host().clone(),
            quiescent: Cell::new(false),
            credit_headroom: Cell::new(0),
            early_credit: Cell::new(0),
            early_completed: RefCell::new(Vec::new()),
            folds_pending: Cell::new(false),
            window_transition: Cell::new(None),
        });
        let mut egress = BufQueue::new(io.buffer_limit);
        let mut codec = Http2Codec::new(transport_direction::<UPSTREAM>());
        codec.set_compact_data(true);
        for setting in &http2.settings {
            if let Some(id) = SettingsId::from_u16(setting.identifier) {
                codec.egress_settings_mut().set(id, setting.value);
            }
        }
        if advertise_window {
            codec.egress_settings_mut().set(
                SettingsId::InitialWindowSize,
                u64::from(stream_receive_window),
            );
        }
        if http2.double_goaway_drain {
            codec.enable_double_goaway_drain();
        } else {
            codec.disable_double_goaway_drain();
        }
        codec.set_header_codec_stats(http2.header_codec_stats.clone());
        if let Some(strategy) = &http2.header_indexing_strategy {
            codec.set_header_indexing_strategy(Rc::clone(strategy));
        }
        codec.set_exact_methods(io.exact_method_tokens && !UPSTREAM);
        codec.set_verbatim_field_values(io.verbatim_http2_header_values && !UPSTREAM);
        codec.set_add_date_header_to_response(io.add_date_header_to_response);
        codec.generate(
            &mut egress,
            EgressFrame::ConnectionPreface,
            &mut NoopCallback,
        )?;
        codec.generate_configured_settings(&mut egress, &mut NoopCallback)?;
        let codec = FlowControlledCodec::new(
            Box::new(codec),
            Rc::new(ConnectionWindowWake(Rc::clone(&control))),
            &mut egress,
            &mut NoopCallback,
            http2.connection_receive_window,
        )?;
        let observer = AsyncSocketByteEventObserver::new(&context)?;
        observer.set_tx_ack_events_enabled(&context, false)?;
        let cancellation = CancellationSource::new();
        let write_cancellation = CancellationSource::new();
        let write_queue = transport.try_write_queue(
            &context,
            io.maximum_write_size,
            io.write_timeout,
            write_cancellation.token(),
        )?;
        // The driver times reads itself (`read_timeout`'s idle slot), so the reader is untimed.
        let read = ReadSlot::new(
            transport.as_ref(),
            &context,
            Duration::ZERO,
            cancellation.token(),
        )?;
        let read_wake = IoWake::new(&control.ready, !read.uses_reader());
        let write_wake = IoWake::new(&control.ready, write_queue.is_none());
        let deadline = (!UPSTREAM && http2.deadlines.is_enabled()).then(|| {
            DeadlineTimer::new(
                context.host(),
                IoWake::new(&control.ready, false),
                http2.deadlines,
            )
        });
        let handle = OwnedH2Handle(control);
        let application = make(&handle);
        Ok((
            Self {
                context,
                transport,
                application,
                h2: http2,
                connection: OwnedConnection {
                    codec,
                    state: ConnectionState {
                        observer,
                        control: Rc::clone(&handle.0),
                        config: io,
                        scheduler: PriorityQueue::default(),
                        conn_blocked: Vec::new(),
                        write_fences: VecDeque::new(),
                        peer_initial_window: DEFAULT_STREAM_WINDOW,
                        peer_maximum_streams: u32::MAX,
                        peer_push_enabled: true,
                        incoming_limit,
                        zero_write_pending: false,
                        awaiting_ingress: 0,
                        consumed_streams: 0,
                        byte_events_registered: false,
                        discarded: BTreeMap::new(),
                        rejected_credit: 0,
                        draining: false,
                        ingress_grace: false,
                        summary: UniplexRunSummary::default(),
                        observation: None,
                        rate_limits,
                    },
                },
                streams: StreamTable::with_hasher(stream_hash_state(), !io.write_timeout.is_zero()),
                ingress: BufQueue::new(io.buffer_limit),
                egress,
                read,
                write: None,
                write_queue,
                read_waker: Waker::from(Arc::clone(&read_wake)),
                read_wake,
                write_waker: Waker::from(Arc::clone(&write_wake)),
                write_wake,
                cancellation,
                write_cancellation,
                transport_reset: false,
                write_end: 0,
                eof_processed: false,
                failure: DriverFailure {
                    failures: VecDeque::new(),
                    fatal: None,
                    panicked: false,
                },
                started: false,
                eof: false,
                sent_initial_goaway: false,
                sent_final_goaway: false,
                goaway_deadline: None,
                budget_yield: None,
                timers: None,
                deadline,
                read_parked: false,
                unarmed_write: false,
                teardown: None,
            },
            handle,
        ))
    }

    fn run(self) -> DriverRun<A, UPSTREAM> {
        DriverRun {
            driver: Some(self),
            poisoned: false,
        }
    }

    /// Starts the teardown after a Driver-scope panic that the caller reported. The connection
    /// aborts, and the run's next polls join native I/O.
    #[cold]
    #[inline(never)]
    fn poll_panicked(&mut self, panic: PanicPayload, context: &Context<'_>) {
        discard_panic(panic);
        self.teardown = Some(Box::default());
        if let Err(panic) = guarded(|| {
            self.abort(failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"owned H2 callback panicked",
            ))
        }) {
            teardown_panicked(&self.context, panic);
        }
        self.read_wake.ready.store(true, Ordering::Release);
        self.write_wake.ready.store(true, Ordering::Release);
        self.wake_teardown(context);
    }

    fn wake_teardown(&self, context: &Context<'_>) {
        if let Err(panic) = guarded(|| context.waker().wake_by_ref()) {
            teardown_panicked(&self.context, panic);
        }
    }

    /// One poll of the teardown. Native I/O is polled until it completes; timers and the codec
    /// are not polled. A teardown poll that panics is polled again, and an operation whose
    /// teardown polls panicked `TEARDOWN_PANIC_LIMIT` times in total is dropped unfinished. Once
    /// every native operation has finished or been dropped, the observers receive `Destroy`, and
    /// each stream is cancelled (unless its exchange already was) and dropped. Every step runs
    /// under its own unwind boundary.
    #[cold]
    #[inline(never)]
    fn poll_teardown(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<UniplexRunSummary, HttpError>> {
        let control = &self.connection.state.control;
        if let Err(panic) = guarded(|| control.ready.register(&control.owner_waker, context)) {
            teardown_panicked(&self.context, panic);
        }
        if !self.transport_reset
            && let Err(panic) = guarded(|| self.reset_transport())
        {
            teardown_panicked(&self.context, panic);
        }
        let mut teardown = self.teardown.take().unwrap_or_default();
        self.poll_teardown_read(&mut teardown, context);
        self.poll_teardown_write(&mut teardown, context);
        self.teardown = Some(teardown);
        if self.read.is_reading() || self.writes_pending() {
            return Poll::Pending;
        }
        self.observe_destroy();
        let error = failure(HttpErrorCode::INTERNAL_ERROR, b"owned H2 callback panicked");
        self.drop_streams(&error);
        Poll::Ready(Err(self.failure.fatal.take().unwrap_or(error)))
    }

    fn poll_teardown_read(&mut self, teardown: &mut TeardownState, context: &Context<'_>) {
        if !self.read.is_reading() || !self.read_wake.take_ready() {
            return;
        }
        let polled = guarded(|| {
            self.read_wake
                .poll_inline(&self.read_waker, |context| self.read.poll(context))
                .map(drop)
        });
        if let Err(panic) = polled
            && self.teardown_poll_panicked(
                &mut teardown.read_panics,
                &self.read_wake,
                context,
                panic,
            )
            && let Err(panic) = guarded(|| self.read.abandon())
        {
            teardown_panicked(&self.context, panic);
        }
    }

    fn poll_teardown_write(&mut self, teardown: &mut TeardownState, context: &Context<'_>) {
        if !self.write_wake.take_ready() {
            return;
        }
        if let Some(write) = &mut self.write {
            let polled = guarded(|| {
                self.write_wake
                    .poll_inline(&self.write_waker, |context| write.as_mut().poll(context))
                    .map(drop)
            });
            let finished = match polled {
                Ok(polled) => polled.is_ready(),
                Err(panic) => self.teardown_poll_panicked(
                    &mut teardown.write_panics,
                    &self.write_wake,
                    context,
                    panic,
                ),
            };
            if finished {
                release_field(&self.context, self.write.take());
            }
        }
        if let Some(queue) = &mut self.write_queue {
            let waker = &self.write_waker;
            let polled = guarded(|| {
                let mut context = Context::from_waker(waker);
                while !queue.is_idle() && queue.poll_complete(&mut context).is_ready() {}
            });
            if let Err(panic) = polled
                && self.teardown_poll_panicked(
                    &mut teardown.queue_panics,
                    &self.write_wake,
                    context,
                    panic,
                )
            {
                release_field(&self.context, self.write_queue.take());
            }
        }
    }

    /// Reports a teardown poll of a native operation that panicked and counts it in `panics`.
    /// Returns whether the operation reached the limit and must be dropped; otherwise `wake` is
    /// marked, so the next teardown poll polls it again.
    #[cold]
    #[inline(never)]
    fn teardown_poll_panicked(
        &self,
        panics: &mut u8,
        wake: &IoWake,
        context: &Context<'_>,
        panic: PanicPayload,
    ) -> bool {
        teardown_panicked(&self.context, panic);
        *panics += 1;
        if *panics >= TEARDOWN_PANIC_LIMIT {
            return true;
        }
        wake.ready.store(true, Ordering::Release);
        self.wake_teardown(context);
        false
    }

    /// Cancels each stream left with `error`, unless its exchange was cancelled, then drops it,
    /// in id order, each under its own unwind boundary.
    #[cold]
    #[inline(never)]
    fn drop_streams(&mut self, error: &HttpError) {
        for id in self.streams.ids_in_order() {
            let Some(mut stream) = self.streams.remove(&id) else {
                continue;
            };
            if !stream.reset
                && let Err(panic) = guarded(|| {
                    self.stream_connection()
                        .1
                        .cancel_owner(id, &mut stream, error, false)
                })
            {
                teardown_panicked(&self.context, panic);
            }
            contain(
                &self.context,
                &mut self.failure,
                PanicSite::ExchangeRetirement,
                Some(id),
                || drop(stream),
            );
        }
    }

    /// Abandons a run dropped unfinished, or a session dropped unrun.
    ///
    /// The observers receive `Destroy`. The connection then aborts with `CORO_CANCELLED`, which
    /// cancels every exchange and resets the transport. The streams, then the driver's fields,
    /// are dropped, each under its own unwind boundary. A started run then panics, because its
    /// owner dropped it before its cleanup joined, unless its poll unwound (`poisoned`), its
    /// thread is unwinding, or the worker is in a forced teardown.
    #[cold]
    #[inline(never)]
    fn abandon(mut self, poisoned: bool) {
        let polling = AdmissionPolling::enter(&self);
        self.observe_destroy();
        let error = failure(
            HttpErrorCode::CORO_CANCELLED,
            b"owned H2 runner abandoned before cleanup completed",
        );
        if let Err(panic) = guarded(|| self.abort(error.clone())) {
            teardown_panicked(&self.context, panic);
        }
        self.drop_streams(&error);
        let started = self.started;
        self.release();
        drop(polling);
        assert!(
            !started || poisoned || std::thread::panicking() || in_forced_teardown(),
            "started owned H2 runner dropped before application and native I/O cleanup joined"
        );
    }

    /// Drops every field in declaration order, then the worker context that the unwind
    /// boundaries report to. Each field that can run foreign code drops under its own boundary.
    #[cold]
    #[inline(never)]
    fn release(self) {
        let Self {
            context,
            transport,
            application,
            h2,
            connection,
            streams,
            ingress,
            egress,
            read,
            write,
            write_queue,
            read_wake,
            read_waker,
            write_wake,
            write_waker,
            cancellation,
            write_cancellation,
            transport_reset: _,
            write_end: _,
            eof_processed: _,
            failure,
            started: _,
            eof: _,
            sent_initial_goaway: _,
            sent_final_goaway: _,
            goaway_deadline: _,
            budget_yield,
            timers,
            deadline,
            read_parked: _,
            unarmed_write: _,
            teardown: _,
        } = self;
        let owner_waker = connection.state.control.owner_waker.take();
        release_field(&context, transport);
        release_field(&context, application);
        release_field(&context, h2);
        release_field(&context, connection);
        release_field(&context, streams);
        release_field(&context, ingress);
        release_field(&context, egress);
        release_field(&context, read);
        release_field(&context, write);
        release_field(&context, write_queue);
        release_field(&context, read_wake);
        release_field(&context, read_waker);
        release_field(&context, write_wake);
        release_field(&context, write_waker);
        release_field(&context, cancellation);
        release_field(&context, write_cancellation);
        release_field(&context, failure);
        release_field(&context, budget_yield);
        release_field(&context, timers);
        release_field(&context, deadline);
        release_field(&context, owner_waker);
        drop(context);
    }

    /// Delivers the queued lifecycle events. An observer panic aborts the connection and starts
    /// the teardown, as a Driver-scope panic does.
    #[cold]
    #[inline(never)]
    fn flush_observation(&mut self, context: &Context<'_>) {
        let Some(observation) = self.connection.state.observation.as_deref() else {
            return;
        };
        observation.window_transition(&self.connection.state.control);
        let lifecycle = Rc::clone(&observation.lifecycle);
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| lifecycle.flush())) {
            report_contained_panic(&self.context, PanicSite::LifecycleObserver, None, &*panic);
            self.poll_panicked(panic, context);
        }
    }

    /// Reports the end of the session once. Queues `TransactionDetached` for each remaining
    /// stream (then `DeactivateConnection`) and `Destroy`, delivers the queue and drops the
    /// observers. Panics are contained.
    #[cold]
    #[inline(never)]
    fn observe_destroy(&mut self) {
        let Some(observation) = self.connection.state.observation.take() else {
            return;
        };
        observation.window_transition(&self.connection.state.control);
        let remaining = self.streams.ids_in_order();
        for id in &remaining {
            observation.push(LifecycleEvent::TransactionDetached { stream: Some(*id) });
        }
        if !remaining.is_empty() {
            observation.push(LifecycleEvent::DeactivateConnection);
        }
        observation.push(LifecycleEvent::Destroy);
        let lifecycle = &observation.lifecycle;
        let site = PanicSite::LifecycleObserver;
        contain(&self.context, &mut self.failure, site, None, || {
            lifecycle.flush()
        });
        contain(&self.context, &mut self.failure, site, None, || {
            lifecycle.clear()
        });
        contain(&self.context, &mut self.failure, site, None, || {
            drop(observation)
        });
    }

    /// A panic contained while the completion closes admission or delivers the last lifecycle
    /// events fails a run that would have succeeded, as a panic at the end of a poll does.
    #[cold]
    #[inline(never)]
    fn finish_run(
        mut self,
        result: Result<UniplexRunSummary, HttpError>,
    ) -> Result<UniplexRunSummary, HttpError> {
        let panicked = self.failure.panicked;
        if let Some(admission) = &self.connection.state.control.upstream {
            contain(
                &self.context,
                &mut self.failure,
                PanicSite::UpstreamObserver,
                None,
                || {
                    admission.close(failure(
                        HttpErrorCode::TRANSPORT_EOF,
                        b"owned upstream H2 session finished",
                    ))
                },
            );
        }
        self.observe_destroy();
        self.connection.state.control.complete.set(true);
        let completion_panicked = self.failure.panicked && !panicked;
        self.release();
        if completion_panicked && result.is_ok() {
            return Err(failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"owned H2 callback panicked",
            ));
        }
        result
    }

    fn cancelled(&self) -> bool {
        self.connection.state.control.cancel.get() || self.context.is_cancelled()
    }

    fn check_cancel(&mut self) {
        if self.cancelled() && !self.transport_reset {
            self.abort_cancelled();
        }
    }

    #[cold]
    #[inline(never)]
    fn abort_cancelled(&mut self) {
        self.abort(failure(
            HttpErrorCode::CORO_CANCELLED,
            b"owned H2 connection cancelled",
        ));
    }

    // `ran` records whether a phase since the last check went past its guard. A phase that
    // stopped at its guard did not run application code or act on cancellation, so
    // `control.cancel` is unchanged. Skipping this check is then the same as a concurrent worker
    // cancellation arriving just after it, and the next check observes it.
    fn check_cancel_after(&mut self, ran: &mut bool) {
        if std::mem::take(ran) {
            self.check_cancel();
        }
    }

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRunSummary, HttpError>> {
        let polled = self.poll_turns(context);
        if polled.is_pending() && self.deadline.is_some() {
            self.sync_deadline();
        }
        polled
    }

    fn poll_turns(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<UniplexRunSummary, HttpError>> {
        if !UPSTREAM {
            if self.connection.state.control.folds_pending.get() {
                self.apply_folds();
            }
            self.connection.state.control.quiescent.set(false);
        }
        let control = &self.connection.state.control;
        control.ready.register(&control.owner_waker, context);
        // This poll consumes an unarmed write's completion, since its waker was never
        // registered.
        if self.unarmed_write {
            self.write_wake.ready.store(true, Ordering::Release);
        }
        if self.budget_yield.is_some() && self.poll_budget_yield(context).is_pending() {
            return Poll::Pending;
        }
        // Only an exit below publishes idleness, so it stays false for every turn until then.
        self.connection.state.control.idle.set(false);
        for turn in 0..TURN_BUDGET {
            #[cfg(test)]
            TURNS.with(|turns| turns.set(turns.get() + 1));
            if self.unarmed_write
                && !self.write_wake.ready.load(Ordering::Acquire)
                && self.write_completion_needed()
            {
                self.write_wake.ready.store(true, Ordering::Release);
            }
            self.check_cancel();
            let timer_fired = self
                .timers
                .as_ref()
                .is_some_and(|timers| timers.wake.take_ready());
            let deadline_fired = self
                .deadline
                .as_ref()
                .is_some_and(|deadline| deadline.wake.take_ready());
            // Taken before this turn queues a GOAWAY or starts a write.
            let output_pending =
                deadline_fired && (!self.egress.is_empty() || self.writes_pending());
            let mut ran = self.goaway_deadline.is_some();
            if self.connection.state.control.drain.get()
                && !self.connection.state.draining
                && self.failure.fatal.is_none()
            {
                ran = true;
                self.start_drain(DrainTrigger::Local);
            }
            if self.goaway_deadline.is_some() {
                self.poll_goaway(timer_fired);
            }
            let mut progress = false;
            if let Some(wrote) = self.poll_write() {
                progress = wrote;
                ran = true;
            }
            // At the writer's position: ingress read later in this turn does not beat the expiry.
            if timer_fired && self.flow_timeout() {
                progress = true;
                ran = true;
            }
            self.check_cancel_after(&mut ran);
            if let Some(read) = self.poll_read() {
                progress |= read;
                ran = true;
            }
            // Data or EOF the read phase just observed beats an expiry in the same turn.
            if timer_fired
                && (self.read.is_reading() || self.read_parked)
                && self.read_idle_expired()
            {
                self.read_idle_timeout();
                ran = true;
            }
            if deadline_fired {
                self.poll_deadline(output_pending);
                ran = true;
            }
            self.check_cancel_after(&mut ran);
            if self.failure.fatal.is_none()
                && (!self.ingress.is_empty() || (self.eof && !self.eof_processed))
            {
                match self.parse_input() {
                    Ok(consumed) => progress |= consumed,
                    Err(error) => self.parse_failed(error),
                }
                ran = true;
            }
            // Only a parse opens the connection window, but local resets also leave stale
            // entries, so the prune runs in turns without one.
            if !self.connection.state.conn_blocked.is_empty() {
                self.resume_conn_blocked();
            }
            self.check_cancel_after(&mut ran);
            if self.failure.fatal.is_none() && !self.failure.failures.is_empty() {
                self.handle_queued_failure();
                progress = true;
                ran = true;
            }
            if UPSTREAM && let Some(submitted) = self.poll_upstream_submission() {
                progress |= submitted;
                ran = true;
            }
            self.check_cancel_after(&mut ran);
            let mut owners = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];
            let polled = self.connection.state.control.pop_batch(&mut owners);
            // Owners are polled after a connection failure too: only their polls retire streams.
            for &owner in &owners[..polled] {
                match self.poll_stream(owner) {
                    Ok(OwnerPollResult::Polled | OwnerPollResult::Serialized(Ok(()))) => {}
                    Ok(OwnerPollResult::Serialized(Err(error))) => self.fail_connection(error),
                    Err(error) => self.reset_polled_stream(owner.id, error),
                }
                ran = true;
                // A cancel that this poll raised resets the later owners before their polls.
                self.check_cancel_after(&mut ran);
            }
            progress |= polled != 0;
            // Events are queued outside this loop, and each `Ok(true)` takes one or writes part of
            // one until egress reaches `maximum_write_size`, so the loop ends. On `Ok(false)` the
            // selection was empty and did not run application code.
            while self.failure.fatal.is_none() {
                match self.serialize_next() {
                    Ok(true) => {
                        progress = true;
                        ran = true;
                        // A cancel that this serialization raised stops the later ones.
                        self.check_cancel_after(&mut ran);
                        // After a contained panic a turn serializes at most one queued event,
                        // and the next owner poll aborts the connection.
                        if self.failure.panicked {
                            break;
                        }
                    }
                    Ok(false) => break,
                    Err((_stream, error)) => {
                        self.fail_connection(error);
                        ran = true;
                        break;
                    }
                }
            }
            self.check_cancel_after(&mut ran);
            if !self.transport_reset
                && (!self.egress.is_empty() || self.connection.state.zero_write_pending)
                && self.can_start_write()
                && (!progress
                    || self.egress.chain_length()
                        >= self.connection.state.config.maximum_write_size
                    || turn + 1 == TURN_BUDGET
                    || self.failure.fatal.is_some()
                    || self.connection.state.zero_write_pending)
            {
                if let Err(error) = self.start_write() {
                    self.abort(error);
                }
                progress = true;
                ran = true;
            }
            self.check_cancel_after(&mut ran);
            if self.failure.fatal.is_some()
                && self.egress.is_empty()
                && !self.writes_pending()
                && !self.transport_reset
            {
                progress |= self.close_failed();
            }
            if self.transport_reset
                && !self.writes_pending()
                && !self.read.is_reading()
                && self.streams.is_empty()
            {
                return Poll::Ready(Err(self.take_fatal()));
            }
            if self.failure.fatal.is_none()
                && (self.eof
                    || (self.connection.state.draining && !self.connection.state.ingress_grace))
                && self.streams.is_empty()
                && self.egress.is_empty()
                && !self.writes_pending()
            {
                if let Some(closed) = self.close_idle() {
                    return Poll::Ready(closed);
                }
                // Unless an owner is queued, only the outstanding read can progress: its
                // completion wakes the task.
                if !progress && self.connection.state.control.owners.borrow().is_empty() {
                    return Poll::Pending;
                }
                continue;
            }
            // An owner queued after this turn's owner polls is polled in another turn.
            if !progress && !self.connection.state.control.owners.borrow().is_empty() {
                continue;
            }
            if !progress || turn + 1 == TURN_BUDGET {
                self.connection.state.control.idle.set(
                    self.streams.is_empty()
                        && self
                            .connection
                            .state
                            .control
                            .upstream
                            .as_ref()
                            .is_none_or(|admission| admission.used.get() == 0)
                        && self.egress.is_empty()
                        && !self.connection.state.zero_write_pending
                        && !self.writes_pending()
                        && self.failure.fatal.is_none()
                        && !self.transport_reset,
                );
            }
            if !progress {
                if !self.connection.state.config.write_timeout.is_zero() {
                    self.sync_flow_timer();
                }
                if !UPSTREAM {
                    self.publish_quiescence();
                }
                return Poll::Pending;
            }
        }
        self.budget_yield = Some(self.context.host().yield_to_io());
        // A yield that completes on its first poll did not arrange a wake.
        if self.poll_budget_yield(context).is_ready() {
            context.waker().wake_by_ref();
        }
        Poll::Pending
    }

    /// Applies what the poll an idle session skipped would have done first: return the folded
    /// credit, which cannot emit, and consume the listed completions.
    #[inline(never)]
    fn apply_folds(&mut self) {
        let control = &self.connection.state.control;
        control.folds_pending.set(false);
        let credit = control.early_credit.replace(0);
        let mut completed = control.early_completed.borrow_mut();
        // A cancelled session's first turn would have aborted before any owner poll; the
        // listed completions stay in their sources.
        if control.cancel.get() || self.context.is_cancelled() {
            completed.clear();
            return;
        }
        if credit != 0 && self.failure.fatal.is_none() {
            match self.connection.codec.ingress_bytes_processed(
                &mut self.egress,
                credit,
                &mut NoopCallback,
            ) {
                Ok(emitted) => debug_assert!(!emitted, "folded credit stays below the headroom"),
                Err(error) => push_connection_failure(&mut self.failure.failures, error),
            }
        }
        for id in completed.drain(..) {
            let Some(stream) = self.streams.get_mut(&id) else {
                continue;
            };
            if stream.consumed || !stream.signals.source_complete.get() {
                continue;
            }
            if let Some(completion) = stream.signals.completed.take() {
                debug_assert!(completion.is_ok(), "only successful completions fold");
                stream.consumed = true;
                self.connection.state.consumed_streams += 1;
            }
        }
    }

    fn publish_quiescence(&mut self) {
        let state = &self.connection.state;
        // Only a live stream whose request source has not completed can still report credit.
        if state.consumed_streams != self.streams.len()
            && self.ingress.is_empty()
            && !(self.eof && !self.eof_processed)
            && self.egress.is_empty()
            && !state.zero_write_pending
            && self.budget_yield.is_none()
            && self.goaway_deadline.is_none()
            && !state.control.drain.get()
            && !state.draining
            && !self.cancelled()
            && self.failure.fatal.is_none()
            && self.failure.failures.is_empty()
            && state.control.settle(&self.read_wake, &self.write_wake)
        {
            state
                .control
                .credit_headroom
                .set(self.connection.codec.ingress_credit_headroom());
            state.control.quiescent.set(true);
        }
    }

    #[cold]
    #[inline(never)]
    fn poll_budget_yield(&mut self, context: &mut Context<'_>) -> Poll<()> {
        let Some(mut wait) = self.budget_yield.take() else {
            return Poll::Ready(());
        };
        let poll = wait.as_mut().poll(context);
        if poll.is_pending() {
            self.budget_yield = Some(wait);
        }
        poll
    }

    #[cold]
    #[inline(never)]
    fn start_drain(&mut self, trigger: DrainTrigger) {
        self.connection.state.draining = true;
        // A local drain and a read timeout drain the same way, so both keep the ingress grace.
        self.connection.state.ingress_grace = !UPSTREAM
            && self.h2.double_goaway_drain
            && matches!(trigger, DrainTrigger::Local | DrainTrigger::IdleTimeout);
        if UPSTREAM {
            self.sent_initial_goaway = true;
        }
        if let Some(observation) = self.connection.state.observation.as_deref_mut() {
            observation.drain(trigger);
        }
        if let Err(error) = self.generate_goaway(H2ErrorCode::NO_ERROR) {
            self.abort(error);
        }
    }

    /// Sends the final GOAWAY once the pending one's deadline passed. With a zero
    /// `goaway_timeout` that is immediately, since the deadline is the instant of the first
    /// GOAWAY. Otherwise it is when the `goaway` slot fires.
    #[cold]
    #[inline(never)]
    fn poll_goaway(&mut self, timer_fired: bool) {
        let due = self.h2.goaway_timeout.is_zero()
            || (timer_fired
                && self
                    .timers
                    .as_mut()
                    .is_some_and(|timers| timers.goaway.take_expired().is_some()));
        if due
            && self.failure.fatal.is_none()
            && let Err(error) = self.generate_goaway(H2ErrorCode::NO_ERROR)
        {
            self.abort(error);
        }
    }

    #[cold]
    #[inline(never)]
    fn parse_failed(&mut self, error: HttpError) {
        if let Some(observation) = self.connection.state.observation.as_deref() {
            observation.push(LifecycleEvent::Http2IngressError {
                stream: StreamId::UNSPECIFIED,
                code: error.code(),
            });
        }
        self.fail_connection(error);
    }

    /// Arms the `goaway` slot for `goaway_deadline`, or disarms it once a final GOAWAY cleared
    /// the deadline. A zero `goaway_timeout` does not use the slot.
    fn sync_goaway_timer(&mut self) {
        let armed = self
            .goaway_deadline
            .filter(|_| !self.h2.goaway_timeout.is_zero());
        if let Some(deadline) = armed {
            let ConnectionTimers { goaway, waker, .. } = self.timers();
            goaway.schedule_at(deadline, waker);
        } else if let Some(timers) = &mut self.timers {
            timers.goaway.cancel();
        }
    }

    #[cold]
    #[inline(never)]
    fn handle_queued_failure(&mut self) {
        if let Some(failure) = self.failure.failures.pop_front() {
            self.handle_failure(failure);
        }
    }

    #[cold]
    #[inline(never)]
    fn reset_polled_stream(&mut self, stream: StreamId, error: HttpError) {
        let wire = http_error_code_to_h2_error_code(error.code(), true);
        self.reset_stream(stream, error, false, Some(wire));
    }

    #[cold]
    #[inline(never)]
    fn take_fatal(&mut self) -> HttpError {
        self.failure
            .fatal
            .take()
            .expect("failed connection retains its cause")
    }

    /// `None` while the outstanding read has yet to observe cancellation.
    #[cold]
    #[inline(never)]
    fn close_idle(&mut self) -> Option<Result<UniplexRunSummary, HttpError>> {
        if let Some(observation) = self.connection.state.observation.as_deref_mut() {
            observation.drain(DrainTrigger::CloseWhenIdle);
        }
        if self.cancellation.request_cancellation() {
            self.read_wake.ready.store(true, Ordering::Release);
        }
        if self.read.is_reading() {
            return None;
        }
        Some(
            self.transport
                .close(&self.context)
                .map(|()| self.connection.state.summary),
        )
    }

    fn generate_goaway(&mut self, code: H2ErrorCode) -> Result<(), HttpError> {
        let plan = protocol::plan_goaway(
            code,
            self.connection.codec.last_incoming_stream_id(),
            self.h2.double_goaway_drain,
            self.sent_initial_goaway,
        );
        self.connection.codec.generate(
            &mut self.egress,
            EgressFrame::Goaway {
                last_stream: plan.last_stream,
                code,
                debug_data: None,
            },
            &mut NoopCallback,
        )?;
        if plan.phase.commit(
            SentGoaway {
                sent_initial: &mut self.sent_initial_goaway,
                sent_final: &mut self.sent_final_goaway,
                deadline: &mut self.goaway_deadline,
            },
            || self.context.host().deadline_after(self.h2.goaway_timeout),
        ) {
            self.connection.state.ingress_grace = false;
        }
        self.sync_goaway_timer();
        Ok(())
    }

    /// `None` when an outstanding read, or a drained reader's next read, was not ready, so the
    /// transport was not polled.
    fn poll_read(&mut self) -> Option<bool> {
        // Until a parked read's waker fires, a turn checks only that waker, as for an outstanding
        // read. Every close, reset and cancellation of the read sets that wake in the same call,
        // so the checks this skips cannot change meanwhile. A drained reader's next read parks
        // first, so the turn that parks it stamps its `read_idle` deadline.
        if self.read_parked && !self.read_wake.is_ready() {
            return None;
        }
        let polled = if self.read.is_reading() {
            if !self.read_wake.take_ready() {
                return None;
            }
            self.read_wake
                .poll_inline(&self.read_waker, |context| self.read.poll(context))
        } else {
            #[cfg(test)]
            READ_START_CHECKS.with(|count| count.set(count.get() + 1));
            let parked = std::mem::take(&mut self.read_parked);
            if self.eof
                || self.failure.fatal.is_some()
                || self.cancelled()
                || self.cancellation.is_cancellation_requested()
            {
                return Some(false);
            }
            let maximum = self
                .connection
                .state
                .config
                .maximum_read_size
                .min(self.ingress.remaining_capacity());
            if maximum == 0 {
                return Some(false);
            }
            // A parked read was issued when it parked, so its poll at the waker is not a new read.
            if !parked && !self.connection.state.config.read_timeout.is_zero() {
                self.stamp_read_idle();
            }
            // A drained reader's next read waits for its waker (`ReadSlot::is_drained`).
            if !self.read_wake.take_ready() && self.read.is_drained() {
                self.read_parked = true;
                return None;
            }
            let allocation_size = self
                .connection
                .state
                .config
                .read_allocation_size
                .min(maximum);
            self.read_wake.poll_inline(&self.read_waker, |context| {
                self.read.poll_start(
                    self.transport.as_ref(),
                    &self.context,
                    context,
                    maximum,
                    allocation_size,
                )
            })
        };
        let Poll::Ready(result) = polled else {
            return Some(false);
        };
        match result {
            Ok(read) => {
                if let Some(observation) = self.connection.state.observation.as_deref() {
                    observation.push(LifecycleEvent::Http2Read {
                        bytes: read.bytes_read(),
                        stream: None,
                    });
                }
                self.connection.state.summary.bytes_read = self
                    .connection
                    .state
                    .summary
                    .bytes_read
                    .saturating_add(read.bytes_read() as u64);
                if read.bytes_read() != 0
                    && let Some(deadline) = self.deadline.as_deref_mut()
                {
                    deadline.note_progress();
                }
                self.eof |= read.is_eof();
                let mut data = read.into_data();
                if self.ingress.append(&mut data).is_err() {
                    self.ingress_overflowed();
                }
            }
            Err(error) => self.read_failed(error),
        }
        Some(true)
    }

    fn timers(&mut self) -> &mut ConnectionTimers {
        self.timers.get_or_insert_with(|| {
            ConnectionTimers::new(self.context.host(), &self.connection.state.control.ready)
        })
    }

    /// Arms the read idle timeout for the read just issued.
    fn stamp_read_idle(&mut self) {
        let deadline = self
            .context
            .host()
            .deadline_after(self.connection.state.config.read_timeout);
        let ConnectionTimers {
            read_idle, waker, ..
        } = self.timers();
        read_idle.schedule_at(deadline, waker);
    }

    fn read_idle_expired(&mut self) -> bool {
        self.timers
            .as_mut()
            .is_some_and(|timers| timers.read_idle.take_expired().is_some())
    }

    /// The deadline phase the connection is in.
    fn deadline_phase(&self) -> DeadlinePhase {
        if self.connection.state.control.handshake_complete.get() {
            DeadlinePhase::Idle
        } else {
            DeadlinePhase::Handshake
        }
    }

    /// Applies the poll's phase change to the connection deadlines as the poll ends.
    fn sync_deadline(&mut self) {
        let phase = self.deadline_phase();
        let Some(deadline) = self.deadline.as_deref_mut() else {
            return;
        };
        if deadline.needs_sync(phase) {
            deadline.sync(phase, self.context.host().now());
        }
    }

    /// Handles the connection deadline's slot firing. `output_pending` says whether output the
    /// connection had before this turn was still unwritten when the slot fired. A deadline that
    /// passed with `HardClose`, or with `Drain` and a zero `goaway_timeout`, fails the connection.
    /// Otherwise `Drain` starts the GOAWAY drain. `goaway_timeout` later, a stream still live or
    /// output still pending fails the connection; otherwise the drain's final GOAWAY has one more
    /// `goaway_timeout` to go out.
    #[cold]
    #[inline(never)]
    fn poll_deadline(&mut self, output_pending: bool) {
        if self.failure.fatal.is_some() {
            return;
        }
        let phase = self.deadline_phase();
        let now = self.context.host().now();
        let Some(deadline) = self.deadline.as_deref_mut() else {
            return;
        };
        let Some(expired) = deadline.expire(phase, now) else {
            return;
        };
        let hard_close = match expired {
            DeadlinePhase::DrainGrace => !self.streams.is_empty() || output_pending,
            DeadlinePhase::DrainFlush => true,
            _ if deadline.on_expiry() == DeadlineExpiry::Drain
                && !self.h2.goaway_timeout.is_zero() =>
            {
                let grace = self.context.host().deadline_after(self.h2.goaway_timeout);
                deadline.start_grace(grace);
                if !self.connection.state.draining {
                    self.start_drain(DrainTrigger::IdleTimeout);
                }
                return;
            }
            _ => true,
        };
        if !hard_close {
            deadline.start_flush(self.context.host().deadline_after(self.h2.goaway_timeout));
            return;
        }
        deadline.finish();
        self.abort(failure(
            HttpErrorCode::READ_TIMEOUT,
            b"owned H2 connection deadline expired",
        ));
    }

    /// Handles a timed-out outstanding read. The connection drains and keeps reading with a fresh
    /// deadline, and the read stays outstanding.
    #[cold]
    #[inline(never)]
    fn read_idle_timeout(&mut self) {
        if !self.connection.state.draining && self.failure.fatal.is_none() {
            self.start_drain(DrainTrigger::IdleTimeout);
        }
        self.stamp_read_idle();
    }

    /// Whether the writer is waiting for work: egress is empty, every write has completed, and
    /// the connection has not failed or been reset.
    fn writer_idle(&self) -> bool {
        self.egress.is_empty()
            && !self.connection.state.zero_write_pending
            && !self.writes_pending()
            && self.failure.fatal.is_none()
            && !self.transport_reset
    }

    /// Mirrors `UniplexSession`'s `h2_blocked_streams`. Fills `blocked`, sorted by id, with the
    /// streams whose next body needs credit if the connection is out of send credit, and
    /// otherwise with the streams out of stream credit. Returns whether the connection is out of
    /// credit. Reading the flow index costs the streams reached mutably since the last check plus
    /// the blocked streams, not a pass over every stream.
    fn flow_blocked_streams(&mut self, blocked: &mut Vec<StreamId>) -> bool {
        let connection = self.connection.codec.available_send() == 0;
        blocked.clear();
        let flow = self
            .streams
            .refresh_flow()
            .expect("a nonzero write timeout keeps a flow index");
        blocked.extend_from_slice(if connection {
            &flow.awaits_credit
        } else {
            &flow.send_blocked
        });
        #[cfg(debug_assertions)]
        {
            let mut walked: Vec<StreamId> = self
                .streams
                .iter()
                .filter(|(_, stream)| {
                    if connection {
                        stream.awaits_credit()
                    } else {
                        stream.send_blocked()
                    }
                })
                .map(|(id, _)| *id)
                .collect();
            walked.sort_unstable();
            debug_assert_eq!(
                *blocked, walked,
                "the flow index matches a pass over the streams"
            );
        }
        connection
    }

    /// Syncs the `flow` slot, which applies `write_timeout` to an idle writer while flow control
    /// blocks egress. The slot is armed for the blocked streams, kept while they stay the same,
    /// and disarmed once every stream is unblocked.
    #[cold]
    #[inline(never)]
    fn sync_flow_timer(&mut self) {
        let (mut blocked, connection) = self.scan_flow_blocked();
        if blocked.is_empty() {
            if let Some(timers) = &mut self.timers {
                if !timers.flow_blocked.is_empty() {
                    timers.flow.cancel();
                    timers.flow_blocked.clear();
                }
                timers.flow_scan = blocked;
            }
            return;
        }
        let marks = self.flow_marks();
        let restamp = self.timers.as_ref().is_none_or(|timers| {
            timers.flow_blocked != blocked
                || timers.flow_connection != connection
                || timers.flow_marks != marks
        });
        let deadline = restamp.then(|| {
            self.context
                .host()
                .deadline_after(self.connection.state.config.write_timeout)
        });
        let ConnectionTimers {
            flow,
            waker,
            flow_blocked,
            flow_connection,
            flow_marks,
            flow_scan,
            ..
        } = self.timers();
        if let Some(deadline) = deadline {
            flow.schedule_at(deadline, waker);
            std::mem::swap(flow_blocked, &mut blocked);
            *flow_connection = connection;
            *flow_marks = marks;
        }
        *flow_scan = blocked;
    }

    /// Returns the blocked streams of an idle writer (empty while it has work), in the reused
    /// scan buffer, and whether the connection's send window blocks them.
    fn scan_flow_blocked(&mut self) -> (Vec<StreamId>, bool) {
        let mut blocked = self
            .timers
            .as_mut()
            .map_or_else(Vec::new, |timers| std::mem::take(&mut timers.flow_scan));
        blocked.clear();
        let connection = self.writer_idle() && self.flow_blocked_streams(&mut blocked);
        (blocked, connection)
    }

    fn flow_marks(&self) -> FlowMarks {
        FlowMarks {
            bytes_read: self.connection.state.summary.bytes_read,
            bytes_scheduled: self.connection.state.summary.bytes_scheduled,
            eof: self.eof,
            streams: self.streams.len(),
        }
    }

    /// Consumes an expiry of the `flow` slot and returns whether there was one, meaning the
    /// writer's wait timed out while flow control blocked egress. If the connection is out of
    /// credit with body waiting, it fails with FLOW_CONTROL_ERROR. Otherwise every flow-blocked
    /// stream is reset with that code. The expiry acts only if the blocked set, whether the
    /// connection window blocks it, and the progress marks are unchanged since the slot was
    /// armed, as activity would have restarted the wait. Otherwise the next idle check arms the
    /// slot again.
    #[cold]
    #[inline(never)]
    fn flow_timeout(&mut self) -> bool {
        let expired = self.timers.as_mut().is_some_and(|timers| {
            !timers.flow_blocked.is_empty() && timers.flow.take_expired() == Some(Expiry::Elapsed)
        });
        if !expired {
            return false;
        }
        let (blocked, connection) = self.scan_flow_blocked();
        let marks = self.flow_marks();
        let timers = self.timers.as_mut().expect("the flow slot fired");
        let unchanged = !blocked.is_empty()
            && timers.flow_blocked == blocked
            && timers.flow_connection == connection
            && timers.flow_marks == marks;
        timers.flow_blocked.clear();
        if unchanged && connection {
            self.fail_connection(failure(
                HttpErrorCode::FLOW_CONTROL_ERROR,
                b"HTTP/2 connection flow-control timeout",
            ));
        } else if unchanged {
            for &id in &blocked {
                self.reset_stream(
                    id,
                    failure(
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        b"HTTP/2 stream flow-control timeout",
                    ),
                    false,
                    Some(H2ErrorCode::FLOW_CONTROL_ERROR),
                );
            }
        }
        if let Some(timers) = &mut self.timers {
            timers.flow_scan = blocked;
        }
        true
    }

    #[cold]
    #[inline(never)]
    fn ingress_overflowed(&mut self) {
        self.fail_connection(failure(
            HttpErrorCode::TRANSPORT_READ_ERROR,
            b"transport read exceeded requested ingress capacity",
        ));
    }

    #[cold]
    #[inline(never)]
    fn read_failed(&mut self, error: HttpError) {
        if self.failure.fatal.is_none() && !self.cancellation.is_cancellation_requested() {
            self.fail_connection(error);
        }
    }

    fn writes_pending(&self) -> bool {
        writes_pending(&self.write, self.write_queue.as_deref())
    }

    fn can_start_write(&self) -> bool {
        match &self.write_queue {
            Some(queue) => queue.can_write(
                self.egress
                    .chain_length()
                    .min(self.connection.state.config.maximum_write_size),
            ),
            None => self.write.is_none(),
        }
    }

    /// Whether a queued write's completion must wake the session, because something waits on it
    /// and a later poll is not otherwise guaranteed. While a live stream awaits wire ingress and
    /// every term below is false, that ingress or the stream's consumer polls the session, and
    /// the poll consumes the completion (`unarmed_write`). A downstream session without streams
    /// needs only what the other terms cover, since its next request, EOF, cancel or drain
    /// consumes the completion. Under a write timeout every completion wakes the session,
    /// because the writer's flow-control wait starts only after the completion is consumed.
    fn write_completion_needed(&self) -> bool {
        let state = &self.connection.state;
        (state.awaiting_ingress == 0 && (UPSTREAM || !self.streams.is_empty()))
            || state.control.drain.get()
            || state.draining
            || self.sent_initial_goaway
            || self.sent_final_goaway
            || self.goaway_deadline.is_some()
            || self.eof
            || self.failure.fatal.is_some()
            || self.transport_reset
            || self.cancelled()
            || !self.can_start_write()
            || state.zero_write_pending
            || (state.byte_events_registered
                && !matches!(state.observer.pending_events(&self.context), Ok(0)))
            || (state.consumed_streams != 0
                && state.write_fences.iter().any(|(_, id)| {
                    self.streams
                        .get(id)
                        .is_some_and(|stream| stream.consumed && stream.response_complete)
                }))
            || !state.config.write_timeout.is_zero()
    }

    fn start_write(&mut self) -> Result<(), HttpError> {
        self.connection.state.zero_write_pending = false;
        let data = self
            .egress
            .split_at_most(self.connection.state.config.maximum_write_size);
        self.connection.state.summary.bytes_scheduled = self
            .connection
            .state
            .summary
            .bytes_scheduled
            .saturating_add(data.chain_length() as u64);
        self.write_end = self.connection.state.summary.bytes_scheduled;
        self.connection
            .state
            .observer
            .transport_write(&self.context, self.write_end)?;
        if self.cancelled() {
            return Err(failure(
                HttpErrorCode::CORO_CANCELLED,
                b"write observer cancelled the connection",
            ));
        }
        if let Some(queue) = &mut self.write_queue {
            queue.start_write(data)?;
            if self.write_completion_needed() {
                self.write_wake.ready.store(true, Ordering::Release);
            } else {
                self.unarmed_write = true;
            }
        } else {
            self.write = Some(self.transport.write(
                &self.context,
                data,
                ByteEventWriteFlags::default(),
                self.connection.state.config.write_timeout,
                self.write_cancellation.token(),
            ));
            self.write_wake.ready.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// `None` when no write was ready to poll.
    fn poll_write(&mut self) -> Option<bool> {
        if self.write_queue.is_some() {
            return self.poll_queued_write();
        }
        let Some(write) = &mut self.write else {
            return None;
        };
        if !self.write_wake.take_ready() {
            return None;
        }
        {
            let polled = self
                .write_wake
                .poll_inline(&self.write_waker, |context| write.as_mut().poll(context));
            let Poll::Ready(result) = &polled else {
                return Some(false);
            };
            self.write.take();
            let bytes = match result {
                Ok(info) => info.bytes_written,
                Err(error) => error.bytes_written(),
            };
            if bytes != 0
                && let Some(deadline) = self.deadline.as_deref_mut()
            {
                deadline.note_progress();
            }
            self.connection.state.summary.bytes_written = self
                .connection
                .state
                .summary
                .bytes_written
                .saturating_add(bytes as u64);
            let observed = self
                .connection
                .state
                .observer
                .socket_byte_event(
                    &self.context,
                    SocketByteEventType::Write,
                    self.connection.state.summary.bytes_written,
                )
                .and_then(|_| {
                    self.connection.state.observer.transport_write_complete(
                        &self.context,
                        self.connection.state.summary.bytes_written,
                        None,
                    )
                });
            if let Err(error) = observed {
                self.abort(error);
            }
            match result {
                Err(error) => self.abort(error.error().clone()),
                Ok(_) if self.connection.state.summary.bytes_written != self.write_end => self
                    .abort(failure(
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        b"transport reported a short successful write",
                    )),
                Ok(_) => {
                    if let Some(observation) = self.connection.state.observation.as_deref() {
                        observation.push(LifecycleEvent::Http2Write { bytes });
                    }
                }
            }
        }
        self.retire_write_fences();
        Some(true)
    }

    fn poll_queued_write(&mut self) -> Option<bool> {
        if !self.write_wake.take_ready() {
            return None;
        }
        let mut progress = false;
        let mut failed = None;
        loop {
            let queue = self
                .write_queue
                .as_mut()
                .expect("the queued writer is installed");
            // Polling an idle queue would register a waker that the next write's completion
            // fires even when the session is not waiting for it.
            if queue.is_idle() {
                break;
            }
            let completion = {
                let mut context = Context::from_waker(&self.write_waker);
                queue.poll_complete(&mut context)
            };
            let Poll::Ready(completion) = completion else {
                break;
            };
            progress = true;
            let bytes = match &completion.result {
                Ok(info) => info.bytes_written,
                Err(error) => error.bytes_written(),
            };
            if bytes != 0
                && let Some(deadline) = self.deadline.as_deref_mut()
            {
                deadline.note_progress();
            }
            self.connection.state.summary.bytes_written = self
                .connection
                .state
                .summary
                .bytes_written
                .saturating_add(bytes as u64);
            let observed = contain(
                &self.context,
                &mut self.failure,
                PanicSite::ByteEvent,
                None,
                || {
                    self.connection
                        .state
                        .observer
                        .socket_byte_event(
                            &self.context,
                            SocketByteEventType::Write,
                            self.connection.state.summary.bytes_written,
                        )
                        .and_then(|_| {
                            self.connection.state.observer.transport_write_complete(
                                &self.context,
                                self.connection.state.summary.bytes_written,
                                None,
                            )
                        })
                },
            );
            match observed {
                Some(Ok(_)) => {}
                observed => retain_observer_failure(&mut failed, observed),
            }
            match completion.result {
                Ok(_) if bytes == completion.requested_bytes => {
                    if let Some(observation) = self.connection.state.observation.as_deref() {
                        observation.push(LifecycleEvent::Http2Write { bytes });
                    }
                }
                result => retain_write_failure(&mut failed, result),
            }
            if self.cancelled() {
                retain_cancellation(&mut failed);
            }
        }
        self.unarmed_write = false;
        if let Some(error) = failed {
            self.abort(error);
        }
        self.retire_write_fences();
        Some(progress)
    }

    fn retire_write_fences(&mut self) {
        if self.transport_reset {
            self.retire_write_fences_after_reset();
        } else {
            self.retire_write_fences_with(false);
        }
    }

    #[cold]
    #[inline(never)]
    fn retire_write_fences_after_reset(&mut self) {
        let reset_complete = !self.writes_pending();
        self.retire_write_fences_with(reset_complete);
    }

    #[inline(always)]
    fn retire_write_fences_with(&mut self, reset_complete: bool) {
        while self
            .connection
            .state
            .write_fences
            .front()
            .is_some_and(|(offset, _)| {
                *offset <= self.connection.state.summary.bytes_written || reset_complete
            })
        {
            let (_, id) = self
                .connection
                .state
                .write_fences
                .pop_front()
                .expect("write fence was present");
            if let Some(stream) = self.streams.get_mut(&id)
                && (reset_complete
                    || stream
                        .write_boundary
                        .is_some_and(|end| end <= self.connection.state.summary.bytes_written))
            {
                stream.physically_complete = true;
                self.connection.state.control.enqueue(&stream.wake);
            }
        }
    }
}

struct CodecInput<'a, A, const UPSTREAM: bool>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    application: &'a A,
    context: &'a WorkerContext,
    streams: &'a mut StreamTable<A::Exchange, UPSTREAM>,
    state: &'a mut ConnectionState,
    failures: &'a mut VecDeque<Failure>,
    receive_window: u32,
    peer_goaway_pending: bool,
    peer_goaway_generated: bool,
    /// The clock read that one parse shares, for its rate limits and the ingress completions
    /// of its header-only requests.
    parse_time: Option<Instant>,
}

impl<A, const UPSTREAM: bool> CodecInput<'_, A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    /// The driver ignores `CodecEvent::FrameHeader`.
    const OBSERVES_FRAME_HEADERS: bool = false;

    #[cold]
    fn fail(&mut self, mut failure: Failure) {
        failure.ingress = true;
        if failure.status.is_some()
            && let Some(owner) = self.streams.get_mut(&failure.stream)
        {
            owner.ingress_rejected = true;
        }
        if failure.stream.is_unspecified() {
            self.failures.clear();
            self.failures.push_back(failure);
        } else if !self
            .failures
            .back()
            .is_some_and(|failure| failure.stream.is_unspecified())
        {
            self.failures.push_back(failure);
        }
    }

    /// Counts one `kind` event against the downstream flood limits and returns whether it
    /// breached them.
    #[inline(always)]
    fn rate_limited(&mut self, kind: RateLimitKind) -> bool {
        !UPSTREAM
            && match self.state.rate_limits.as_deref_mut() {
                None => false,
                Some(limiter) => {
                    let (parse_time, context) = (&mut self.parse_time, self.context);
                    record_rate(limiter, kind, || {
                        *parse_time.get_or_insert_with(|| context.host().now())
                    })
                }
            }
    }

    #[cold]
    #[inline(never)]
    fn fail_rate_limit(&mut self, kind: RateLimitKind) {
        self.fail(Failure::new(
            StreamId::UNSPECIFIED,
            rate_limit_exceeded(kind),
        ));
    }

    /// Counts a completed header block, unless its request has a forbidden body, which
    /// `UniplexSession` rejects before counting. On a breach the stream's head is not
    /// delivered and the stream is queued to be aborted without a reset.
    #[cold]
    #[inline(never)]
    fn header_rate_limited(&mut self, stream: StreamId, message: &mut HttpMessage) -> bool {
        if crate::uniplex::request_has_forbidden_body(message)
            || !self.rate_limited(RateLimitKind::HeaderFrames)
        {
            return false;
        }
        self.state.discarded.remove(&stream);
        if let Some(owner) = self.streams.get_mut(&stream) {
            owner.ingress_rejected = true;
        }
        // Not `fail`: as in `UniplexSession`, a breach is not a codec failure, so a lifecycle
        // observer does not receive `Http2IngressError` for it.
        self.failures.push_back(Failure {
            header_rate_limit: true,
            ..Failure::new(stream, rate_limit_exceeded(RateLimitKind::HeaderFrames))
        });
        true
    }

    fn new_stream(&mut self, id: StreamId) -> Result<(), HttpError> {
        let Entry::Vacant(entry) = self.streams.entry(id) else {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"request stream already exists",
            ));
        };
        let wake = Arc::new(StreamWake {
            id,
            queue: Arc::clone(&self.state.control.ready),
            queued: AtomicBool::new(false),
            shared_queued: AtomicBool::new(false),
            headers_poll_ready: AtomicBool::new(true),
            live: AtomicBool::new(true),
        });
        let signals = Rc::new(IngressSignals {
            control: Rc::clone(&self.state.control),
            active: Cell::new(true),
            source_complete: Cell::new(false),
            response_complete: Cell::new(false),
            upstream: false,
            ingress_complete: Cell::new(false),
            wake: Arc::clone(&wake),
            consumed: Cell::new(0),
            acknowledge: Cell::new(0),
            completed: Cell::new(None),
            read_timed_out: Cell::new(false),
            wake_on_completion: Cell::new(false),
        });
        let source = HttpIngressBody::new_shared(
            id,
            signals.clone(),
            self.receive_window,
            self.context.host().clone(),
        )
        .map_err(|_| {
            failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"invalid stream receive window",
            )
        })?;
        if self.state.config.verbatim_http2_header_values {
            source.read_empty_content_length_as_zero();
        }
        let producer = source.producer();
        // As in `UniplexSession`, a request body is timed from the stream's creation.
        let stream_read_timeout = self.state.config.stream_read_timeout;
        if !stream_read_timeout.is_zero() {
            producer.set_read_timeout(stream_read_timeout);
        }
        let drain = source.drain_on_drop();
        let application = self.application.new_exchange_with(id, || {
            source
                .ingress_completion()
                .expect("a stream source has an ingress receipt")
        });
        let send_window =
            Window::new(self.state.peer_initial_window).expect("peer initial window was validated");
        // Allocating before the value is built lets it be written in place instead of copied.
        entry.insert(Box::write(
            Box::new_uninit(),
            Stream {
                application: Some(
                    <A::Exchange as ExchangeStorage<UPSTREAM>>::store_downstream(application),
                ),
                wake,
                signals,
                source: Some(IngressSource::Request(source)),
                producer,
                drain: Some(drain),
                head: None,
                started: false,
                wire_ingress_complete: false,
                ingress_rejected: false,
                consumed: false,
                peer_reset: None,
                reset_after_response: false,
                resume: None,
                request_head: false,
                request_body_length: 0,
                request_body_events: false,
                expected_request_length: None,
                send_window,
                scheduling: PriorityEntry::default(),
                stalled: false,
                pending: None,
                response_headers: false,
                expects_408: false,
                response_complete: false,
                write_boundary: None,
                physically_complete: false,
                registered_byte_events: false,
                reset: false,
                stream_offset: 0,
                body_offset: 0,
            },
        ));
        self.state
            .control
            .active_incoming
            .set(self.state.control.active_incoming.get() + 1);
        self.state.awaiting_ingress += 1;
        Ok(())
    }

    #[cold]
    fn return_rejected_credit(&mut self, amount: usize) {
        match self.state.rejected_credit.checked_add(amount) {
            Some(total) if total <= u32::MAX as usize => self.state.rejected_credit = total,
            _ => self.fail(Failure::new(
                StreamId::UNSPECIFIED,
                failure(
                    HttpErrorCode::FLOW_CONTROL_ERROR,
                    b"rejected DATA connection credit overflowed",
                ),
            )),
        }
    }

    /// Hands a DATA frame or trailer block (`trailers`) that no live owner takes, or that
    /// arrives after both the request and the response ended, to the codec, which answers it as
    /// RFC 9113 requires for a stream that is not open.
    #[cold]
    #[inline(never)]
    fn closed_stream_frame(&self, stream: StreamId, trailers: bool) -> CodecCallbackActions {
        let state = self.ingress_stream_state(stream);
        vec![CodecCallbackAction::ClosedStreamFrame {
            stream,
            state,
            trailers,
        }]
        .into()
    }
}

impl<A, const UPSTREAM: bool> HttpCodecCallback for CodecInput<'_, A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if self.peer_goaway_pending
            && let CodecEvent::GeneratedFrameHeader { frame_type, .. } = event
            && frame_type == roxygen_http2::frame::FrameType::GOAWAY.as_u8()
        {
            self.peer_goaway_generated = true;
        }
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let mut actions = Vec::new();
        if self.state.control.cancel.get() || self.context.is_cancelled() {
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        }
        if self
            .failures
            .back()
            .is_some_and(|failure| failure.stream.is_unspecified())
        {
            return actions.into();
        }
        match event {
            CodecEvent::MessageBegin { stream, .. } => {
                if UPSTREAM {
                    return actions.into();
                }
                if self.state.control.active_incoming.get() as u64 >= self.state.incoming_limit
                    || (self.state.draining && !self.state.ingress_grace)
                {
                    self.state.discarded.insert(stream, false);
                    #[cfg(test)]
                    DISCARDED_STREAMS.with(|streams| {
                        let (current, peak) = streams.get();
                        streams.set((current, peak.max(self.state.discarded.len())));
                    });
                    actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::Reset {
                        stream,
                        code: H2ErrorCode::REFUSED_STREAM,
                    }));
                } else if let Err(error) = self.new_stream(stream) {
                    self.fail(Failure::new(stream, error));
                } else if let Some(observation) = self.state.observation.as_deref() {
                    observation.opened(stream, self.streams.len() == 1);
                }
            }
            CodecEvent::HeadersComplete {
                stream,
                mut message,
            } => {
                if !UPSTREAM
                    && self.state.rate_limits.is_some()
                    && self.header_rate_limited(stream, &mut message)
                {
                    return actions.into();
                }
                // Once its header block completes, a refused stream is ownerless like any
                // unknown one: its DATA returns credit, and its trailers, END_STREAM and codec
                // errors are ignored.
                if self.state.discarded.remove(&stream).is_some() {
                    if let Some(observation) = self.state.observation.as_deref() {
                        observe_rejected_head(observation, stream, &mut message);
                    }
                    return actions.into();
                }
                let owner = self.streams.get_mut(&stream);
                if owner.as_ref().is_some_and(|owner| owner.ingress_rejected) {
                    if let Some(observation) = self.state.observation.as_deref() {
                        observe_rejected_head(observation, stream, &mut message);
                    }
                    return actions.into();
                }
                if UPSTREAM {
                    if let Some(owner) = owner {
                        if !owner.producer.headers_boxed(message, false) {
                            self.fail(Failure::new(
                                stream,
                                failure(
                                    HttpErrorCode::HEADER_PARSE_ERROR,
                                    b"upstream source rejected response headers",
                                ),
                            ));
                        } else if let Some(exchange) = owner.upstream() {
                            exchange.status.response_head_arrived();
                        }
                    }
                    return actions.into();
                }
                if crate::uniplex::request_has_forbidden_body(&mut message) {
                    let mut error = failure(
                        HttpErrorCode::HEADER_PARSE_ERROR,
                        b"RFC2616 request body is not allowed for this method",
                    );
                    error.set_http_message(Some(*message));
                    self.fail(Failure {
                        stream,
                        error,
                        status: Some(400),
                        wire_code: None,
                        direction: HttpExceptionDirection::Ingress,
                        new_stream: true,
                        ingress: false,
                        header_rate_limit: false,
                    });
                    return actions.into();
                }
                if let Some(observation) = self.state.observation.as_deref() {
                    observation.push(LifecycleEvent::Http2IngressMessage { stream });
                }
                let Some(owner) = owner else {
                    return actions.into();
                };
                owner.request_head = message.method().ok().flatten() == Some(HttpMethod::Head);
                owner.expected_request_length = crate::uniplex::checked_content_length(
                    &message,
                    self.state.config.verbatim_http2_header_values,
                );
                if let Some(priority) = message.get_http_priority() {
                    self.state
                        .scheduler
                        .update(stream, &mut owner.scheduling, priority);
                }
                let source = owner
                    .source
                    .as_ref()
                    .expect("unstarted request retains its source");
                if !source.initialize_body_only(&mut message, false) {
                    let error = source
                        .ingress_completion()
                        .and_then(|receipt| receipt.snapshot())
                        .and_then(Result::err)
                        .unwrap_or_else(|| {
                            failure(
                                HttpErrorCode::HEADER_PARSE_ERROR,
                                b"ingress source rejected request head",
                            )
                        });
                    self.fail(Failure {
                        stream,
                        error,
                        status: Some(400),
                        wire_code: None,
                        direction: HttpExceptionDirection::Ingress,
                        new_stream: true,
                        ingress: false,
                        header_rate_limit: false,
                    });
                    return actions.into();
                }
                owner.head = Some(message);
                self.state.control.enqueue(&owner.wake);
            }
            CodecEvent::Body {
                stream,
                body,
                padding,
            } => {
                let length = body.chain_length();
                if self.state.discarded.contains_key(&stream) {
                    self.return_rejected_credit(length);
                    return actions.into();
                }
                let Some(owner) = self
                    .streams
                    .get_mut(&stream)
                    .filter(|owner| !owner.reset && !owner.ingress_rejected)
                else {
                    self.return_rejected_credit(length);
                    return self.closed_stream_frame(stream, false);
                };
                owner.request_body_events = true;
                owner.request_body_length = owner.request_body_length.saturating_add(length as u64);
                if owner.producer.body(body, padding, false) == FlowControlState::Error {
                    let complete = owner.wire_ingress_complete;
                    let closed = complete && owner.response_complete;
                    self.return_rejected_credit(length);
                    if closed {
                        return self.closed_stream_frame(stream, false);
                    }
                    let mut rejected = Failure::new(
                        stream,
                        failure(
                            if complete {
                                HttpErrorCode::INVALID_STATE_TRANSITION
                            } else {
                                HttpErrorCode::FLOW_CONTROL_ERROR
                            },
                            b"ingress source rejected body bytes",
                        ),
                    );
                    if complete {
                        rejected.direction = HttpExceptionDirection::Ingress;
                        rejected.wire_code = Some(H2ErrorCode::STREAM_CLOSED);
                    }
                    self.fail(rejected);
                }
            }
            CodecEvent::TrailersComplete { stream, trailers } => {
                if self.state.discarded.contains_key(&stream) {
                    return actions.into();
                }
                let Some(owner) = self
                    .streams
                    .get_mut(&stream)
                    .filter(|owner| !owner.ingress_rejected)
                else {
                    return self.closed_stream_frame(stream, true);
                };
                let peer_ended = owner.wire_ingress_complete;
                let closed = peer_ended && owner.response_complete;
                owner.request_body_events = true;
                // As `UniplexSession`: the source's `trailers` leaves the body open for a
                // later EOM, and the trailers end this message, so EOM follows them here.
                let accepted = owner.producer.trailers(*trailers) && owner.producer.eom();
                self.state.settle_ingress(owner);
                owner.wire_ingress_complete = true;
                self.state.control.enqueue(&owner.wake);
                if UPSTREAM {
                    owner.release_upstream_capacity();
                }
                if !accepted && closed {
                    return self.closed_stream_frame(stream, true);
                }
                if !accepted {
                    let mut rejected = Failure::new(
                        stream,
                        failure(
                            HttpErrorCode::INVALID_STATE_TRANSITION,
                            b"ingress source rejected trailers",
                        ),
                    );
                    if peer_ended {
                        rejected.direction = HttpExceptionDirection::Ingress;
                        rejected.wire_code = Some(H2ErrorCode::STREAM_CLOSED);
                    }
                    self.fail(rejected);
                }
            }
            CodecEvent::MessageComplete { stream, .. } => {
                if self.state.discarded.remove(&stream).is_some() {
                    return actions.into();
                }
                if let Some(owner) = self.streams.get_mut(&stream) {
                    self.state.settle_ingress(owner);
                    if owner.ingress_rejected {
                        owner.wire_ingress_complete = true;
                        return actions.into();
                    }
                    if !owner.wire_ingress_complete {
                        owner.wire_ingress_complete = true;
                        let accepted = if UPSTREAM || owner.request_body_events {
                            owner.producer.eom()
                        } else {
                            let context = self.context;
                            let now = *self.parse_time.get_or_insert_with(|| context.host().now());
                            owner.producer.eom_at(now)
                        };
                        // A rejected EOM (e.g. before any head) leaves the response source
                        // terminal, and the caller reads that error from the source.
                        if !accepted
                            && UPSTREAM
                            && let Some(exchange) = owner.upstream()
                        {
                            exchange.status.publish_response();
                        }
                    }
                    if UPSTREAM {
                        owner.release_upstream_capacity();
                    }
                    let mismatch = owner
                        .expected_request_length
                        .is_some_and(|expected| expected != owner.request_body_length);
                    self.state.control.enqueue(&owner.wake);
                    if mismatch {
                        self.fail(Failure {
                            stream,
                            error: failure(
                                HttpErrorCode::CONTENT_LENGTH_MISMATCH,
                                b"ingress Content-Length did not match body bytes",
                            ),
                            status: None,
                            wire_code: Some(H2ErrorCode::INTERNAL_ERROR),
                            direction: HttpExceptionDirection::Ingress,
                            new_stream: false,
                            ingress: false,
                            header_rate_limit: false,
                        });
                    }
                }
            }
            CodecEvent::Error {
                stream,
                error,
                new_stream,
            } => {
                if !stream.is_unspecified()
                    && error.codec_status_code().is_none()
                    && self.rate_limited(RateLimitKind::DirectErrors)
                {
                    self.fail_rate_limit(RateLimitKind::DirectErrors);
                    return actions.into();
                }
                let mut partial = error.partial_message().cloned();
                let code = if !UPSTREAM
                    && partial
                        .as_mut()
                        .is_some_and(crate::uniplex::request_has_forbidden_body)
                {
                    HttpErrorCode::HEADER_PARSE_ERROR
                } else {
                    http_exception_to_http_error_code(error)
                };
                let mut adapted = HttpError::with_message(code, error.message());
                adapted.set_http_message(partial);
                self.fail(Failure {
                    stream,
                    error: adapted,
                    status: error
                        .http_status_code()
                        .and_then(|status| u16::try_from(status).ok()),
                    wire_code: error.codec_status_code(),
                    direction: error.direction(),
                    new_stream,
                    ingress: false,
                    header_rate_limit: false,
                });
            }
            CodecEvent::Abort { stream, code } => {
                if self.rate_limited(RateLimitKind::Resets) {
                    self.fail_rate_limit(RateLimitKind::Resets);
                    return actions.into();
                }
                let code = h2_error_code_to_http_error_code(code);
                let peer_code = code;
                if let Some(observation) = self.state.observation.as_deref() {
                    // Observers are downstream-only and receive a peer's NO_ERROR as CANCEL.
                    let observed = if code == HttpErrorCode::NO_ERROR {
                        HttpErrorCode::CANCEL
                    } else {
                        code
                    };
                    observation.push(LifecycleEvent::PeerReset {
                        stream,
                        code: observed,
                    });
                }
                if let Some(owner) = self.streams.get_mut(&stream) {
                    // Once the whole response has arrived, an upstream peer's NO_ERROR stops
                    // the upload without error. RFC 9113 lets a server that has sent a complete
                    // response ask the client to stop sending the request this way.
                    let code = if code == HttpErrorCode::NO_ERROR
                        && !(UPSTREAM && owner.wire_ingress_complete)
                    {
                        HttpErrorCode::CANCEL
                    } else {
                        code
                    };
                    // The owner takes `peer_reset` when it applies it; `ingress_rejected` stays.
                    let first_reset = !owner.ingress_rejected;
                    owner.ingress_rejected = true;
                    owner.signals.close_protocol();
                    self.state.scheduler.dequeue(stream, &mut owner.scheduling);
                    owner.record_peer_reset(
                        code == HttpErrorCode::REFUSED_STREAM && first_reset,
                        || failure(code, b"peer reset cancelled local byte events"),
                    );
                    self.state.control.enqueue(&owner.wake);
                    if first_reset
                        && code != HttpErrorCode::NO_ERROR
                        && let Some(admission) = &self.state.control.upstream
                    {
                        admission.peer_reset(peer_code);
                    }
                }
            }
            CodecEvent::WindowUpdate { stream, amount } => {
                if !stream.is_unspecified() {
                    let Some(owner) = self
                        .streams
                        .get_mut(&stream)
                        .filter(|owner| owner.protocol_active())
                    else {
                        return actions.into();
                    };
                    let stalled = owner.stalled;
                    if !protocol::free_stream_send(
                        &mut owner.send_window,
                        &mut owner.stalled,
                        amount,
                    ) {
                        self.fail(Failure::new(
                            StreamId::UNSPECIFIED,
                            failure(
                                HttpErrorCode::FLOW_CONTROL_ERROR,
                                b"stream send-window update overflowed",
                            ),
                        ));
                    } else if stalled && !owner.stalled && owner.pending.is_some() {
                        self.state.scheduler.enqueue(stream, &mut owner.scheduling);
                    }
                }
            }
            CodecEvent::Settings { settings } => {
                if self.rate_limited(RateLimitKind::ControlFrames) {
                    self.fail_rate_limit(RateLimitKind::ControlFrames);
                    return actions.into();
                }
                self.state.control.complete_handshake();
                if let Some(observation) = self.state.observation.as_deref() {
                    observation.settings(
                        &settings,
                        self.state.peer_maximum_streams,
                        self.state.draining,
                    );
                }
                for setting in settings.iter() {
                    match PeerSetting::from(*setting) {
                        PeerSetting::MaxConcurrentStreams(maximum) => {
                            self.state.peer_maximum_streams = maximum;
                            if let Some(admission) = &self.state.control.upstream {
                                admission.set_peer_limit(maximum);
                            }
                        }
                        PeerSetting::EnablePush(enabled) => self.state.peer_push_enabled = enabled,
                        PeerSetting::InitialWindowSize(capacity) => {
                            let mut windows: Vec<_> = self
                                .streams
                                .iter_mut()
                                .filter(|(_, stream)| stream.protocol_active())
                                .collect();
                            windows.sort_unstable_by_key(|(id, _)| **id);
                            if !protocol::update_initial_send_window(
                                &mut self.state.peer_initial_window,
                                capacity,
                                windows
                                    .iter_mut()
                                    .map(|(_, stream)| &mut stream.send_window),
                            ) {
                                self.fail(Failure::new(
                                    StreamId::UNSPECIFIED,
                                    failure(
                                        HttpErrorCode::FLOW_CONTROL_ERROR,
                                        b"peer initial stream window is not representable",
                                    ),
                                ));
                            } else {
                                for (id, stream) in windows {
                                    if stream.stalled && stream.send_window.non_negative_size() != 0
                                    {
                                        stream.stalled = false;
                                        if stream.pending.is_some() {
                                            self.state
                                                .scheduler
                                                .enqueue(*id, &mut stream.scheduling);
                                        }
                                    }
                                }
                            }
                        }
                        PeerSetting::Other => {}
                    }
                }
                actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::SettingsAck));
            }
            CodecEvent::Goaway {
                last_good_stream,
                code,
                ..
            } => {
                if let Some(observation) = self.state.observation.as_deref() {
                    observation.push(LifecycleEvent::Http2Goaway {
                        last_good_stream,
                        code: h2_error_code_to_http_error_code(code),
                    });
                }
                for id in self.streams.ids_in_order() {
                    if protocol::peer_goaway_refuses(
                        id,
                        last_good_stream,
                        transport_direction::<UPSTREAM>(),
                    ) {
                        if let Some(observation) = self.state.observation.as_deref() {
                            observation.push(LifecycleEvent::PeerReset {
                                stream: id,
                                code: HttpErrorCode::REFUSED_STREAM,
                            });
                        }
                        let owner = self
                            .streams
                            .get_mut(&id)
                            .expect("GOAWAY visits live streams");
                        let first_reset = !owner.ingress_rejected;
                        owner.ingress_rejected = true;
                        owner.signals.close_protocol();
                        self.state.scheduler.dequeue(id, &mut owner.scheduling);
                        owner.record_peer_reset(first_reset, || {
                            failure(HttpErrorCode::REFUSED_STREAM, b"peer GOAWAY refused stream")
                        });
                        self.state.control.enqueue(&owner.wake);
                        if first_reset && let Some(admission) = &self.state.control.upstream {
                            admission.peer_reset(HttpErrorCode::REFUSED_STREAM);
                        }
                    }
                }
                if let Some(admission) = &self.state.control.upstream {
                    admission.close_for_peer_goaway(h2_error_code_to_http_error_code(code));
                }
                if code != H2ErrorCode::NO_ERROR {
                    self.fail(Failure::new(
                        StreamId::UNSPECIFIED,
                        failure(
                            h2_error_code_to_http_error_code(code),
                            b"peer GOAWAY carried a connection error",
                        ),
                    ));
                } else if !self.state.draining {
                    self.state.draining = true;
                    self.state.ingress_grace = false;
                    if let Some(observation) = self.state.observation.as_deref_mut() {
                        observation.drain(DrainTrigger::PeerGoaway);
                    }
                } else {
                    return actions.into();
                }
                // A peer's error gets NO_ERROR too, since the error is the peer's. The codec
                // closes on this reply, so the connection failure writes no second GOAWAY.
                actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::Goaway {
                    last_stream: StreamId::MAX,
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                }));
                self.peer_goaway_pending = true;
            }
            CodecEvent::PingRequest { data } => {
                if self.rate_limited(RateLimitKind::ControlFrames) {
                    self.fail_rate_limit(RateLimitKind::ControlFrames);
                    return actions.into();
                }
                if let Some(observation) = self.state.observation.as_deref() {
                    observation.push(LifecycleEvent::Http2PingReplySent {
                        latency: Duration::ZERO,
                    });
                }
                actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::PingReply {
                    data,
                }))
            }
            CodecEvent::SettingsAck => {
                if let Some(observation) = self.state.observation.as_deref() {
                    observation.push(LifecycleEvent::Http2SettingsAck);
                }
            }
            CodecEvent::PingReply { .. } => {
                if let Some(observation) = self.state.observation.as_deref() {
                    observation.push(LifecycleEvent::Http2PingReplyReceived);
                }
            }
            CodecEvent::Priority { .. } => {
                if self.rate_limited(RateLimitKind::ControlFrames) {
                    self.fail_rate_limit(RateLimitKind::ControlFrames);
                }
            }
            CodecEvent::FrameHeader { .. }
            | CodecEvent::PushPriority { .. }
            | CodecEvent::GeneratedFrameHeader { .. }
            | CodecEvent::ChunkHeader { .. }
            | CodecEvent::ChunkComplete { .. } => {}
            CodecEvent::PushMessageBegin { .. } => {
                self.fail(Failure::new(
                    StreamId::UNSPECIFIED,
                    failure(
                        HttpErrorCode::PROTOCOL_ERROR,
                        b"downstream peer sent a push promise",
                    ),
                ));
            }
        }
        actions.into()
    }

    fn num_incoming_streams(&self) -> u32 {
        if UPSTREAM {
            0
        } else {
            self.state
                .control
                .active_incoming
                .get()
                .min(u32::MAX as usize) as u32
        }
    }

    fn ingress_stream_state(&self, stream: StreamId) -> IngressStreamState {
        if self.state.discarded.contains_key(&stream) {
            return IngressStreamState::Discarded;
        }
        match self.streams.get(&stream) {
            None => IngressStreamState::Closed,
            Some(owner) if owner.ingress_rejected => IngressStreamState::Discarded,
            Some(owner) if owner.wire_ingress_complete && owner.response_complete => {
                IngressStreamState::Closed
            }
            Some(owner) if owner.wire_ingress_complete => IngressStreamState::PeerEnded,
            Some(_) => IngressStreamState::Open,
        }
    }

    fn observes_frame_headers(&self) -> bool {
        Self::OBSERVES_FRAME_HEADERS
    }
}

impl<A, const UPSTREAM: bool> Driver<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    fn parse_input(&mut self) -> Result<bool, HttpError> {
        let input = &self.ingress;
        let mut callback = CodecInput::<A, UPSTREAM> {
            application: &self.application,
            context: &self.context,
            streams: &mut self.streams,
            state: &mut self.connection.state,
            failures: &mut self.failure.failures,
            receive_window: self.h2.stream_receive_window,
            peer_goaway_pending: false,
            peer_goaway_generated: false,
            parse_time: None,
        };
        let (consumed, mut egress_results) = if input.is_empty() {
            (0, Vec::new())
        } else {
            let result = self
                .connection
                .codec
                .on_ingress(input, &mut callback, &mut self.egress);
            (result.consumed, result.egress_results)
        };
        let input_length = input.chain_length();
        let finishing_eof =
            self.eof && !self.eof_processed && (consumed == input_length || consumed == 0);
        if finishing_eof {
            self.eof_processed = true;
            if let Some(observation) = callback.state.observation.as_deref() {
                observation.push(LifecycleEvent::Http2IngressEof);
            }
            let result = self
                .connection
                .codec
                .on_ingress_eof(&mut callback, &mut self.egress);
            egress_results.extend(result.egress_results);
        }
        let peer_goaway_generated = callback.peer_goaway_generated;
        if finishing_eof && let Some(admission) = &self.connection.state.control.upstream {
            admission.close(failure(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
        }
        if peer_goaway_generated {
            let phase = if self.connection.codec.is_waiting_to_drain() {
                protocol::GoawayPhase::Initial
            } else {
                protocol::GoawayPhase::Final
            };
            if phase.commit(
                SentGoaway {
                    sent_initial: &mut self.sent_initial_goaway,
                    sent_final: &mut self.sent_final_goaway,
                    deadline: &mut self.goaway_deadline,
                },
                || self.context.host().deadline_after(self.h2.goaway_timeout),
            ) {
                self.connection.state.ingress_grace = false;
            }
        }
        if peer_goaway_generated {
            self.sync_goaway_timer();
        }
        if consumed > input_length {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"codec consumed beyond the supplied prefix",
            ));
        }
        self.ingress
            .trim_start(consumed)
            .expect("codec consumption was range checked");
        if self.connection.state.rejected_credit != 0 {
            let credit = std::mem::take(&mut self.connection.state.rejected_credit) as u32;
            self.connection.codec.ingress_bytes_processed(
                &mut self.egress,
                credit,
                &mut NoopCallback,
            )?;
        }
        for result in egress_results {
            result?;
        }
        let mut ingress_error_reported = false;
        while self.failure.fatal.is_none()
            && let Some(problem) = self.failure.failures.pop_front()
        {
            if problem.ingress
                && !ingress_error_reported
                && let Some(observation) = self.connection.state.observation.as_deref()
            {
                // `UniplexSession` reports one codec failure per parse.
                ingress_error_reported = true;
                observation.push(LifecycleEvent::Http2IngressError {
                    stream: problem.stream,
                    code: problem.error.code(),
                });
            }
            self.handle_failure(problem);
            self.check_cancel();
        }
        if finishing_eof
            && self.failure.fatal.is_none()
            && !UPSTREAM
            && self.h2.abort_streams_on_eof
            && self.has_open_stream()
        {
            self.abort_open_streams_on_eof();
        }
        if finishing_eof && self.failure.fatal.is_none() {
            let mut incomplete = self.streams.ids_in_order();
            incomplete.retain(|id| {
                let owner = &self.streams[id];
                !owner.wire_ingress_complete && !owner.reset && !owner.ingress_rejected
            });
            for id in incomplete {
                self.handle_failure(Failure {
                    stream: id,
                    error: failure(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"),
                    status: (!UPSTREAM).then_some(500),
                    wire_code: None,
                    direction: HttpExceptionDirection::Ingress,
                    new_stream: false,
                    ingress: false,
                    header_rate_limit: false,
                });
                self.check_cancel();
                if self.failure.fatal.is_some() {
                    break;
                }
            }
        }
        #[cfg(test)]
        DISCARDED_STREAMS.with(|streams| {
            let (_, peak) = streams.get();
            streams.set((self.connection.state.discarded.len(), peak));
        });
        Ok(consumed != 0 || finishing_eof)
    }

    /// Whether a request head is still arriving, or some stream's response END_STREAM is not
    /// yet serialized and the peer has not reset it. A stream with a recorded peer reset is not
    /// open, even before its owner applies the reset.
    fn has_open_stream(&self) -> bool {
        self.connection.codec.has_partial_request_head()
            || self
                .streams
                .values()
                .any(|stream| !stream.response_complete && stream.peer_reset.is_none())
    }

    /// Fails the connection on a peer EOF while a stream is open, as a transport read error
    /// does, which also skips the incomplete-stream failures that follow.
    #[cold]
    #[inline(never)]
    fn abort_open_streams_on_eof(&mut self) {
        self.fail_connection(failure(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
        self.check_cancel();
    }

    fn stream_connection(
        &mut self,
    ) -> (
        &mut StreamTable<A::Exchange, UPSTREAM>,
        StreamConnection<'_, UPSTREAM>,
    ) {
        (
            &mut self.streams,
            StreamConnection {
                connection: &mut self.connection,
                context: &self.context,
                failure: &mut self.failure,
                egress: &mut self.egress,
                write: &self.write,
                write_queue: self.write_queue.as_deref(),
                transport_reset: self.transport_reset,
            },
        )
    }

    fn poll_stream(&mut self, owner: QueuedOwner) -> Result<OwnerPollResult, HttpError> {
        #[cfg(test)]
        OWNER_POLLS.with(|polls| polls.set(polls.get() + 1));
        let id = owner.id;
        let mut serialized = None;
        let retired = {
            let (streams, mut connection) = self.stream_connection();
            let Entry::Occupied(mut entry) = streams.entry(id) else {
                return Ok(OwnerPollResult::Polled);
            };
            let stream = entry.get_mut();
            stream.wake.take_mark(owner);
            // Once a downstream owner's final head without END_STREAM is serialized, this poll
            // polls the owner again for the body. `then_body` allows that once, so an owner poll
            // makes at most two application polls. The second pass serves a wake that the first
            // queued, such as the request source's completion, so it removes that entry. Over a
            // write queue that is not saturated the entry stays: a stream there usually skips its
            // write fence, so its END_STREAM would queue the owner again, and retiring the stream
            // in this poll removes the entry.
            let mut then_body = !UPSTREAM;
            let retired = loop {
                if let Err(error) = connection.poll_stream_owner(id, stream) {
                    if self.failure.panicked && self.failure.fatal.is_none() {
                        self.abort_retirement_panic();
                    }
                    return Err(error);
                }
                // While the message's last event waits, the source's completion wakes the owner.
                if stream.pending.as_ref().is_some_and(PendingEgress::eom) {
                    stream.signals.wake_on_completion.set(true);
                }
                if connection.retirable(id, stream) {
                    break true;
                }
                match connection.serialize_polled(id, stream, then_body) {
                    Some(Ok(true)) => {
                        then_body = false;
                        if !connection
                            .write_queue
                            .is_some_and(|queue| !queue.is_saturated())
                        {
                            connection
                                .connection
                                .state
                                .control
                                .unqueue_last(&stream.wake);
                        }
                    }
                    polled => {
                        serialized = polled.map(|result| result.map(drop));
                        break matches!(serialized, Some(Ok(())))
                            && connection.retires_in_place(id, stream);
                    }
                }
            };
            if retired {
                connection.connection.state.settle_ingress(stream);
                connection.connection.state.consumed_streams -= 1;
                if stream.reset || !stream.wire_ingress_complete {
                    connection.connection.codec.abandon_stream(id);
                }
                Some(entry.remove())
            } else {
                None
            }
        };
        let retiring = retired.is_some();
        if let Some(mut stream) = retired {
            self.debug_assert_stream_counts();
            // Admission contains its observer's panics while the driver polls.
            if let Some(exchange) = stream.upstream() {
                exchange.status.release();
            }
            self.connection
                .state
                .scheduler
                .dequeue(id, &mut stream.scheduling);
            stream.wake.live.store(false, Ordering::Release);
            let site = PanicSite::ExchangeRetirement;
            if !stream.reset {
                contain(&self.context, &mut self.failure, site, Some(id), || {
                    <A::Exchange as ExchangeStorage<UPSTREAM>>::complete_stored(
                        &mut stream.application,
                    )
                });
            }
            contain(&self.context, &mut self.failure, site, Some(id), || {
                drop(stream)
            });
            if let Some(observation) = self.connection.state.observation.as_deref() {
                observation.retired(id, self.streams.is_empty());
            }
        }
        // A poll that serialized leaves a contained panic to the next owner poll, unless it
        // retired its stream.
        if (retiring || serialized.is_none())
            && self.failure.panicked
            && self.failure.fatal.is_none()
        {
            self.abort_retirement_panic();
        }
        Ok(serialized.map_or(OwnerPollResult::Polled, OwnerPollResult::Serialized))
    }

    fn debug_assert_stream_counts(&self) {
        debug_assert_eq!(
            self.connection.state.awaiting_ingress,
            self.streams
                .values()
                .filter(|stream| stream.awaits_ingress())
                .count(),
            "the awaiting-ingress count matches the live streams"
        );
        debug_assert_eq!(
            self.connection.state.consumed_streams,
            self.streams
                .values()
                .filter(|stream| stream.consumed)
                .count(),
            "the consumed count matches the live streams"
        );
    }

    #[cold]
    #[inline(never)]
    fn abort_retirement_panic(&mut self) {
        self.abort(failure(
            HttpErrorCode::INTERNAL_ERROR,
            b"owned H2 retirement panicked",
        ));
    }
}

impl<const UPSTREAM: bool> StreamConnection<'_, UPSTREAM> {
    /// Read only where a stream's `physically_complete` is determined.
    fn writes_pending(&self) -> bool {
        #[cfg(test)]
        OWNER_WRITE_STATE_READS.with(|reads| reads.set(reads.get() + 1));
        writes_pending(self.write, self.write_queue)
    }

    fn cancelled(&self) -> bool {
        self.connection.state.control.cancel.get() || self.context.is_cancelled()
    }

    /// Whether an owner poll can go on after it serializes an event: the session is not
    /// cancelled, the queue of connection failures is empty, and a requested local drain has
    /// started. The driver handles those before it polls owners.
    fn can_continue(&self) -> bool {
        self.failure.failures.is_empty()
            && !self.cancelled()
            && (self.connection.state.draining || !self.connection.state.control.drain.get())
    }

    /// Whether `stream` can retire: its request source's completion is consumed, its response is
    /// complete and no longer waits for its write, and its byte events are drained. Inlined
    /// because every owner poll runs it.
    #[inline(always)]
    fn retirable<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &Stream<E, UPSTREAM>,
    ) -> bool {
        // Until a byte event is registered the observer is empty, and an event cannot wait.
        stream.consumed
            && stream.response_complete
            && stream.physically_complete
            && (!self.connection.state.byte_events_registered || {
                let waker = waker_ref(&stream.wake);
                let mut context = Context::from_waker(&waker);
                match self.connection.state.observer.poll_stream_events_drained(
                    self.context,
                    id,
                    &mut context,
                ) {
                    Poll::Ready(Ok(())) => true,
                    Poll::Ready(Err(error)) => {
                        push_connection_failure(&mut self.failure.failures, error);
                        false
                    }
                    Poll::Pending => false,
                }
            })
    }

    /// Whether `stream`, whose event its owner's poll just serialized, retires in that poll. Its
    /// END_STREAM must have skipped its write fence, or its bytes must already be written, and the
    /// driver must not have a cancel, a queued connection failure, a requested local drain or a
    /// contained panic to handle first. Retiring removes the owner's entry if it is the local
    /// queue's last, because a retired owner's poll is empty.
    fn retires_in_place<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &Stream<E, UPSTREAM>,
    ) -> bool {
        if !stream.physically_complete
            || self.failure.panicked
            || !self.can_continue()
            || !self.retirable(id, stream)
        {
            return false;
        }
        self.connection.state.control.unqueue_last(&stream.wake);
        true
    }

    fn poll_stream_owner<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
    ) -> Result<(), HttpError> {
        if let Some(error) = stream.peer_reset.take() {
            self.reset_owner(id, stream, &error, true, None);
        }
        if UPSTREAM {
            let waker = waker_ref(&stream.wake);
            let mut context = Context::from_waker(&waker);
            if let Some(exchange) = stream
                .application
                .as_mut()
                .and_then(<E as ExchangeStorage<UPSTREAM>>::upstream_mut)
                && let Err(panic) = guarded(|| exchange.poll_response_observer(&mut context))
            {
                return self.response_observer_panicked(id, stream, panic);
            }
        }
        self.take_completion(id, stream, None);
        let consumed = stream.signals.consumed.replace(0);
        let acknowledge = stream.signals.acknowledge.replace(0);
        if self.failure.fatal.is_none() {
            let credit = (|| {
                if consumed != 0 {
                    let amount = u32::try_from(consumed).map_err(|_| {
                        failure(
                            HttpErrorCode::FLOW_CONTROL_ERROR,
                            b"connection return credit overflowed",
                        )
                    })?;
                    self.connection.codec.ingress_bytes_processed(
                        self.egress,
                        amount,
                        &mut NoopCallback,
                    )?;
                }
                if acknowledge != 0 && !stream.reset {
                    let amount = u32::try_from(acknowledge).map_err(|_| {
                        failure(
                            HttpErrorCode::FLOW_CONTROL_ERROR,
                            b"stream return credit overflowed",
                        )
                    })?;
                    self.connection.codec.generate(
                        self.egress,
                        EgressFrame::WindowUpdate { stream: id, amount },
                        &mut NoopCallback,
                    )?;
                }
                Ok::<_, HttpError>(())
            })();
            if let Err(error) = credit {
                push_connection_failure(&mut self.failure.failures, error);
                return Ok(());
            }
        }
        if stream.reset {
            if stream.consumed
                && stream.pending.is_some()
                && !stream.response_complete
                && self.failure.fatal.is_none()
            {
                self.connection
                    .state
                    .scheduler
                    .enqueue(id, &mut stream.scheduling);
            }
            if self.transport_reset && !self.writes_pending() {
                stream.physically_complete = true;
            }
            return Ok(());
        }
        if UPSTREAM {
            return self.poll_upstream_stream(id, stream);
        }
        let waker = waker_ref(&stream.wake);
        let mut context = Context::from_waker(&waker);
        if !stream.started
            && let Some(head) = stream.head.take()
        {
            let source = stream
                .source
                .take()
                .expect("unstarted request retains its body source");
            let IngressSource::Request(mut source) = source else {
                unreachable!("downstream requests retain a fresh ingress body");
            };
            let eom = stream.wire_ingress_complete && !stream.request_body_events;
            if eom {
                source = source.retire_empty_body(&mut context)?;
            }
            if self.cancelled() {
                return Ok(());
            }
            stream.started = true;
            <E as ExchangeStorage<UPSTREAM>>::downstream(
                stream
                    .application
                    .as_mut()
                    .expect("application remains owned"),
            )
            .request_body(head, source, eom);
        }
        if self.cancelled()
            || !stream.started
            || stream.pending.is_some()
            || stream.response_complete
        {
            return Ok(());
        }
        if let Some(resume) = &mut stream.resume {
            match resume.as_mut().poll(&mut context) {
                Poll::Pending => return Ok(()),
                Poll::Ready(roxygen_core::events::ResumeStatus::Cancelled) => {
                    return Err(failure(
                        HttpErrorCode::CORO_CANCELLED,
                        b"response body resume cancelled",
                    ));
                }
                Poll::Ready(_) => {
                    stream.resume.take();
                }
            }
        }
        if self.cancelled() {
            return Ok(());
        }
        let application = <E as ExchangeStorage<UPSTREAM>>::downstream(
            stream
                .application
                .as_mut()
                .expect("application remains owned"),
        );
        if !stream.response_headers {
            if !stream.wake.take_headers_ready() {
                return Ok(());
            }
            let handler_active = application.handler_active();
            let polled = application.poll_headers(&mut context);
            if polled.is_ready()
                && stream.signals.read_timed_out.get()
                && !stream.consumed
                && self.read_timeout_settled(id, stream, handler_active)
            {
                return Ok(());
            }
            match polled {
                Poll::Pending => {}
                Poll::Ready(Err(error)) => return Err(error),
                Poll::Ready(Ok(headers)) => {
                    // Informational headers can have an immediately ready successor.
                    stream
                        .wake
                        .headers_poll_ready
                        .store(true, Ordering::Release);
                    stream.pending = Some(PendingEgress::Headers(headers));
                    if stream.expects_408 && self.replaced_unexpected_head(id, stream) {
                        return Ok(());
                    }
                    self.connection
                        .state
                        .scheduler
                        .enqueue(id, &mut stream.scheduling);
                }
            }
        } else {
            let polled = application.poll_body(
                &mut context,
                self.connection.state.config.buffer_limit.min(64 << 10) as u32,
            );
            if polled.is_ready()
                && stream.signals.read_timed_out.get()
                && !stream.consumed
                && self.read_timeout_settled(id, stream, false)
            {
                return Ok(());
            }
            match polled {
                Poll::Pending => {}
                Poll::Ready(Err(error)) => return Err(error),
                Poll::Ready(Ok(event)) => {
                    if event.as_body().is_some_and(|body| {
                        body.chain_length()
                            > self.connection.state.config.buffer_limit.min(64 << 10)
                    }) {
                        return Err(failure(
                            HttpErrorCode::MESSAGE_ERROR,
                            b"response source exceeded its admitted body read",
                        ));
                    }
                    if matches!(event.kind(), HttpBodyEventKind::PushPromise(_)) {
                        return Err(failure(
                            HttpErrorCode::PROTOCOL_ERROR,
                            b"owned downstream experiment does not admit push responses",
                        ));
                    }
                    if matches!(event.kind(), HttpBodyEventKind::Suspend(_)) {
                        let HttpBodyEventKind::Suspend(resume) = event.into_kind() else {
                            unreachable!("event kind was checked");
                        };
                        stream.resume = Some(resume);
                        self.connection.state.control.schedule(&stream.wake);
                    } else {
                        stream.pending = Some(PendingEgress::Body(event));
                        self.connection
                            .state
                            .scheduler
                            .enqueue(id, &mut stream.scheduling);
                    }
                }
            }
        }
        Ok(())
    }

    /// Consumes the source's completion, which the shared source publishes once, before waking
    /// this owner. Inlined because every owner poll runs it. `handler_active` is whether the
    /// handler ran when the source completed, if known. Otherwise the exchange reports whether
    /// it runs now.
    #[inline(always)]
    fn take_completion<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        handler_active: Option<bool>,
    ) {
        let completion = if !stream.consumed && stream.signals.source_complete.get() {
            stream.signals.completed.take()
        } else {
            None
        };
        if let Some(completion) = completion {
            stream.consumed = true;
            self.connection.state.consumed_streams += 1;
            if !stream.reset
                && let Err(error) = completion
            {
                self.source_failed(id, stream, error, handler_active);
            }
        }
    }

    /// Whether a request body read timeout that the application's last poll observed has ended
    /// the stream (`Stream::reset`), with a reset or an immediate 408. The session core runs the
    /// source's completion callback, queued inside that read, before the handler's result or
    /// the response's failure, so the timeout determines the outcome and the poll's result is
    /// ignored. `handler_active` is whether the handler ran before that poll, and so when the
    /// read timed out.
    #[cold]
    #[inline(never)]
    fn read_timeout_settled<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        handler_active: bool,
    ) -> bool {
        self.take_completion(id, stream, Some(handler_active));
        stream.reset
    }

    /// Enforces the 408 that a request body read timeout requires of a running handler's
    /// pending head, as the session core's `ExpectedStatusSource` does. A final 408 is sent as
    /// written, and any other final head cancels the exchange and becomes a bare 408. Returns
    /// whether it replaced the head.
    #[cold]
    #[inline(never)]
    fn replaced_unexpected_head<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
    ) -> bool {
        let Some(PendingEgress::Headers(headers)) = &mut stream.pending else {
            unreachable!("a polled head is pending");
        };
        if !matches!(headers.is_final(), Ok(true)) {
            return false;
        }
        stream.expects_408 = false;
        if headers
            .headers_mut()
            .status_code()
            .is_ok_and(|status| status == REQUEST_TIMEOUT)
        {
            return false;
        }
        let error = failure(
            HttpErrorCode::CANCEL,
            b"handler answered a request body read timeout without 408",
        );
        self.respond_with_error(id, stream, REQUEST_TIMEOUT, &error);
        true
    }

    #[cold]
    #[inline(never)]
    fn source_failed<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        error: HttpError,
        handler_active: Option<bool>,
    ) {
        if UPSTREAM {
            if !stream.response_complete || !stream.wire_ingress_complete {
                let error = stream
                    .upstream()
                    .and_then(|exchange| exchange.status.selected_error())
                    .unwrap_or(error);
                let code = crate::session::source_complete_reset_code(error.code());
                let code = if code == HttpErrorCode::NO_ERROR {
                    HttpErrorCode::CANCEL
                } else {
                    code
                };
                let wire = http_error_code_to_h2_error_code(code, false);
                let error = if error.code() == HttpErrorCode::NO_ERROR {
                    failure(
                        HttpErrorCode::CANCEL,
                        b"upstream response source was abandoned",
                    )
                } else {
                    error
                };
                self.reset_owner(id, stream, &error, false, Some(wire));
            }
        } else if error.code() == HttpErrorCode::NO_ERROR {
            if !stream.response_complete {
                stream.reset_after_response = !stream.wire_ingress_complete;
            }
        } else if error.code() == HttpErrorCode::READ_TIMEOUT && !stream.response_headers {
            // The session core's stream read timeout before the response head: 408, from a
            // running handler when it responds, otherwise immediately.
            let handler_active = handler_active.unwrap_or_else(|| {
                stream.application.as_mut().is_some_and(|application| {
                    <E as ExchangeStorage<UPSTREAM>>::downstream(application).handler_active()
                })
            });
            if handler_active {
                stream.expects_408 = true;
            } else {
                self.respond_with_error(id, stream, REQUEST_TIMEOUT, &error);
            }
        } else if !stream.response_complete || !stream.wire_ingress_complete {
            let code = crate::session::source_complete_reset_code(error.code());
            let wire = http_error_code_to_h2_error_code(code, false);
            self.reset_owner(id, stream, &error, false, Some(wire));
        }
    }

    fn discard_pending<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        error: &HttpError,
    ) {
        if let Some(pending) = &mut stream.pending {
            let registrations = match pending {
                PendingEgress::Headers(headers) => headers.byte_event_registrations_mut(),
                PendingEgress::Body(body) => body.byte_event_registrations_mut(),
            };
            for registration in registrations {
                contain(
                    self.context,
                    self.failure,
                    PanicSite::ByteEvent,
                    Some(id),
                    || registration.cancel(error, None),
                );
            }
        }
        contain(
            self.context,
            self.failure,
            PanicSite::ExchangeRetirement,
            Some(id),
            || drop(stream.pending.take()),
        );
    }

    #[cold]
    fn cancel_owner<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        error: &HttpError,
        peer: bool,
    ) {
        if stream.reset {
            return;
        }
        stream.signals.wake_on_completion.set(true);
        self.connection.state.settle_ingress(stream);
        stream.reset = true;
        stream.ingress_rejected = true;
        stream.signals.close_protocol();
        stream.response_complete = true;
        stream.signals.response_complete();
        self.connection
            .state
            .scheduler
            .dequeue(id, &mut stream.scheduling);
        // A cause selected by the request's consumer discards the response's queued events, as
        // `UniplexStreamAbort` does.
        let discard = stream
            .upstream()
            .is_some_and(|exchange| exchange.status.cause_selected());
        let (context, site) = (self.context, PanicSite::ExchangeRetirement);
        contain(context, self.failure, site, Some(id), || {
            let details = b"owned H2 stream cancelled";
            if discard {
                stream
                    .producer
                    .abort_discarding_queued(error.clone(), details)
            } else {
                stream.producer.abort_with_error(error.clone(), details)
            }
        });
        if let Some(application) = &mut stream.application {
            if contain(context, self.failure, site, Some(id), || {
                <E as ExchangeStorage<UPSTREAM>>::cancel_stored(application, error, peer)
            })
            .is_none()
            {
                contain(context, self.failure, site, Some(id), || {
                    drop(stream.application.take())
                });
            }
        }
        contain(context, self.failure, site, Some(id), || {
            drop(stream.resume.take())
        });
        self.discard_pending(id, stream, error);
        contain(context, self.failure, site, Some(id), || {
            drop(stream.head.take())
        });
        if !UPSTREAM {
            contain(context, self.failure, site, Some(id), || {
                stream.producer.stop_reading(Some(error.code()))
            });
        }
        contain(context, self.failure, site, Some(id), || {
            drop(stream.source.take())
        });
        contain(
            context,
            self.failure,
            PanicSite::ByteEvent,
            Some(id),
            || {
                self.connection
                    .state
                    .observer
                    .cancel_stream_events(context, id, error)
            },
        );
        let control = &self.connection.state.control;
        contain(context, self.failure, site, Some(id), || {
            control.schedule(&stream.wake)
        });
    }

    /// Cancels the owner with `error` and responds with a bodiless `status` that ends the
    /// stream, like the session core's error response.
    #[cold]
    fn respond_with_error<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        status: u16,
        error: &HttpError,
    ) {
        self.cancel_owner(id, stream, error, false);
        let mut message = HttpMessage::new();
        message
            .set_status_code(status)
            .expect("error response status was validated");
        message.set_http_version(1, 1);
        message.set_wants_keepalive(false);
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"0");
        let headers =
            HttpHeaderEvent::new(message, true).expect("final error response accepts EOM");
        stream.pending = Some(PendingEgress::Headers(headers));
        stream.response_complete = false;
        stream.signals.error_response_pending();
        stream.reset_after_response = false;
        self.connection.state.control.schedule(&stream.wake);
        if stream.signals.source_complete.get() {
            self.connection
                .state
                .scheduler
                .enqueue(id, &mut stream.scheduling);
        }
    }
}

impl<A, const UPSTREAM: bool> Driver<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    #[cold]
    fn reset_stream(
        &mut self,
        id: StreamId,
        error: HttpError,
        peer: bool,
        wire: Option<H2ErrorCode>,
    ) {
        let Some(mut stream) = self.streams.remove(&id) else {
            return;
        };
        let (error, peer, wire) = if UPSTREAM {
            if let Some(error) = stream.peer_reset.take() {
                (error, true, None)
            } else if let Some(error) = stream
                .upstream()
                .and_then(|exchange| exchange.status.selected_error())
            {
                let wire = upstream::cancellation_wire_code(error.code());
                (error, peer, Some(wire))
            } else {
                (error, peer, wire)
            }
        } else {
            (error, peer, wire)
        };
        let reset = catch_unwind(AssertUnwindSafe(|| {
            self.stream_connection()
                .1
                .reset_owner(id, &mut stream, &error, peer, wire)
        }));
        self.streams.insert(id, stream);
        if let Err(panic) = reset {
            resume_unwind(panic);
        }
    }

    /// Aborts the stream whose header block breached the header rate limit, without a reset,
    /// then drains with one final GOAWAY covering it.
    #[cold]
    #[inline(never)]
    fn header_rate_limit_breached(&mut self, stream: StreamId, error: HttpError) {
        self.abort_stream_without_wire(stream, error);
        self.start_final_drain(DrainTrigger::HeaderRateLimit);
    }

    /// Cancels a live stream's exchange and sources as a local reset does, but does not
    /// write RST_STREAM.
    #[cold]
    fn abort_stream_without_wire(&mut self, id: StreamId, error: HttpError) {
        let Some(mut stream) = self.streams.remove(&id) else {
            return;
        };
        let aborted = catch_unwind(AssertUnwindSafe(|| {
            if !stream.reset {
                let mut connection = self.stream_connection().1;
                connection.cancel_owner(id, &mut stream, &error, false);
                connection.fence_reset(id, &mut stream);
            }
        }));
        self.streams.insert(id, stream);
        if let Err(panic) = aborted {
            resume_unwind(panic);
        }
    }

    /// Drains with one final GOAWAY that carries the last incoming stream, unless the
    /// connection already drains or failed.
    #[cold]
    #[inline(never)]
    fn start_final_drain(&mut self, trigger: DrainTrigger) {
        if self.connection.state.draining || self.failure.fatal.is_some() {
            return;
        }
        self.connection.state.draining = true;
        self.connection.state.ingress_grace = false;
        self.sent_initial_goaway = true;
        if let Some(observation) = self.connection.state.observation.as_deref_mut() {
            observation.drain(trigger);
        }
        if let Err(error) = self.generate_goaway(H2ErrorCode::NO_ERROR) {
            self.abort(error);
        }
    }
}

impl<const UPSTREAM: bool> StreamConnection<'_, UPSTREAM> {
    #[cold]
    fn reset_owner<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        error: &HttpError,
        peer: bool,
        wire: Option<H2ErrorCode>,
    ) {
        stream.signals.wake_on_completion.set(true);
        if stream.reset && self.failure.fatal.is_some() {
            stream.response_complete = true;
            stream.signals.response_complete();
        }
        if stream.reset && peer {
            if let Some(application) = &mut stream.application {
                contain(
                    self.context,
                    self.failure,
                    PanicSite::ExchangeRetirement,
                    Some(id),
                    || <E as ExchangeStorage<UPSTREAM>>::cancel_stored(application, error, true),
                );
            }
            if !stream.response_complete || stream.pending.is_some() {
                self.connection
                    .state
                    .scheduler
                    .dequeue(id, &mut stream.scheduling);
                self.discard_pending(id, stream, error);
                stream.response_complete = true;
                stream.signals.response_complete();
                let offset = self
                    .connection
                    .state
                    .summary
                    .bytes_scheduled
                    .saturating_add(self.egress.chain_length() as u64);
                stream.write_boundary = Some(offset);
                stream.physically_complete =
                    offset <= self.connection.state.summary.bytes_written && !self.writes_pending();
                if !stream.physically_complete {
                    self.connection.state.write_fences.push_back((offset, id));
                }
            }
        }
        if !stream.reset {
            self.cancel_owner(id, stream, error, peer);
            if !peer && self.failure.fatal.is_none() && (!UPSTREAM || stream.response_headers) {
                let code =
                    wire.unwrap_or_else(|| http_error_code_to_h2_error_code(error.code(), false));
                if let Err(error) = self.connection.codec.generate(
                    self.egress,
                    EgressFrame::Reset { stream: id, code },
                    &mut NoopCallback,
                ) {
                    push_connection_failure(&mut self.failure.failures, error);
                }
            }
            self.fence_reset(id, stream);
        }
    }

    /// Completes a just-reset stream once everything serialized so far is written.
    fn fence_reset<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
    ) {
        let offset = self
            .connection
            .state
            .summary
            .bytes_scheduled
            .saturating_add(self.egress.chain_length() as u64);
        stream.write_boundary = Some(offset);
        stream.physically_complete = (self.transport_reset
            || offset <= self.connection.state.summary.bytes_written)
            && !self.writes_pending();
        if !stream.physically_complete {
            self.connection.state.write_fences.push_back((offset, id));
        }
        self.connection.state.control.schedule(&stream.wake);
    }
}

impl<A, const UPSTREAM: bool> Driver<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    #[cold]
    fn reset_transport(&mut self) {
        self.read_wake.ready.store(true, Ordering::Release);
        self.write_wake.ready.store(true, Ordering::Release);
        let site = PanicSite::Transport;
        if !self.transport_reset {
            self.transport_reset = true;
            self.connection.state.summary.reset = true;
            contain(&self.context, &mut self.failure, site, None, || {
                self.cancellation.request_cancellation()
            });
            contain(&self.context, &mut self.failure, site, None, || {
                self.write_cancellation.request_cancellation()
            });
            contain(&self.context, &mut self.failure, site, None, || {
                self.transport.close_with_reset(&self.context)
            });
        }
        let control = &self.connection.state.control;
        for id in self.streams.ids_in_order() {
            let stream = &self.streams[&id];
            contain(&self.context, &mut self.failure, site, None, || {
                control.schedule(&stream.wake)
            });
        }
    }

    /// Closes a failed connection whose GOAWAY is written with FIN, so the peer
    /// reads the GOAWAY and then EOF. `false` while the outstanding read has yet
    /// to observe cancellation.
    #[cold]
    #[inline(never)]
    fn close_failed(&mut self) -> bool {
        if self.read.is_reading() {
            return false;
        }
        self.transport_reset = true;
        let site = PanicSite::Transport;
        contain(&self.context, &mut self.failure, site, None, || {
            if self.transport.close(&self.context).is_err() {
                self.connection.state.summary.reset = true;
                let _ = self.transport.close_with_reset(&self.context);
            }
        });
        let control = &self.connection.state.control;
        for id in self.streams.ids_in_order() {
            let stream = &self.streams[&id];
            contain(&self.context, &mut self.failure, site, None, || {
                control.schedule(&stream.wake)
            });
        }
        true
    }

    #[cold]
    fn fail_connection(&mut self, error: HttpError) {
        if self.failure.fatal.is_some() {
            return;
        }
        self.failure.fatal = Some(error.clone());
        if let Some(observation) = self.connection.state.observation.as_deref_mut() {
            observation.drain(DrainTrigger::ConnectionError);
        }
        if let Some(admission) = &self.connection.state.control.upstream {
            contain(
                &self.context,
                &mut self.failure,
                PanicSite::UpstreamObserver,
                None,
                || admission.close(error.clone()),
            );
        }
        self.failure.failures.clear();
        self.read_wake.ready.store(true, Ordering::Release);
        contain(
            &self.context,
            &mut self.failure,
            PanicSite::Transport,
            None,
            || self.cancellation.request_cancellation(),
        );
        let wire = protocol::connection_error_goaway_code(error.code());
        let generated = self.generate_goaway(wire);
        for id in self.streams.ids_in_order() {
            self.reset_stream(id, error.clone(), false, None);
        }
        contain(
            &self.context,
            &mut self.failure,
            PanicSite::ByteEvent,
            None,
            || {
                self.connection
                    .state
                    .observer
                    .cancel_events(&self.context, &error)
            },
        );
        if generated.is_err() {
            self.abort(error);
        }
    }

    #[cold]
    fn abort(&mut self, error: HttpError) {
        if self.failure.fatal.is_none() {
            self.failure.fatal = Some(error.clone());
            if let Some(observation) = self.connection.state.observation.as_deref_mut() {
                observation.drain(DrainTrigger::ConnectionError);
            }
        }
        if let Some(admission) = &self.connection.state.control.upstream {
            contain(
                &self.context,
                &mut self.failure,
                PanicSite::UpstreamObserver,
                None,
                || admission.close(error.clone()),
            );
        }
        self.failure.failures.clear();
        self.reset_transport();
        for id in self.streams.ids_in_order() {
            self.reset_stream(id, error.clone(), false, None);
        }
        self.egress = BufQueue::new(self.connection.state.config.buffer_limit);
        self.connection.state.zero_write_pending = false;
        contain(
            &self.context,
            &mut self.failure,
            PanicSite::ByteEvent,
            None,
            || {
                self.connection
                    .state
                    .observer
                    .cancel_events(&self.context, &error)
            },
        );
    }

    #[cold]
    fn handle_failure(&mut self, mut problem: Failure) {
        if !UPSTREAM && problem.header_rate_limit {
            self.header_rate_limit_breached(problem.stream, problem.error);
            return;
        }
        let owner = self.streams.get(&problem.stream);
        let decision = CodecFailureContext {
            direction: transport_direction::<UPSTREAM>(),
            stream: problem.stream,
            exception_direction: problem.direction,
            new_stream: problem.new_stream,
            http_status: problem.status,
            wire_code: problem.wire_code,
            live_stream: owner
                .filter(|owner| !owner.consumed || !owner.response_complete)
                .map(|owner| {
                    !owner.response_headers
                        && !owner.response_complete
                        && owner.peer_reset.is_none()
                }),
            discarded: self
                .connection
                .state
                .discarded
                .contains_key(&problem.stream)
                || owner.is_some_and(|owner| owner.reset || owner.ingress_rejected),
            protocol_stream_present: owner.is_some_and(|owner| owner.protocol_active()),
        }
        .disposition();
        match decision {
            CodecFailureDisposition::Connection => self.fail_connection(problem.error),
            CodecFailureDisposition::Ignore => {}
            CodecFailureDisposition::Reset {
                explicit_wire_code,
                fallback_wire_code,
            } => {
                if owner.is_some() {
                    self.reset_stream(problem.stream, problem.error, false, explicit_wire_code);
                } else if let Err(error) = self.connection.codec.generate(
                    &mut self.egress,
                    EgressFrame::Reset {
                        stream: problem.stream,
                        code: fallback_wire_code,
                    },
                    &mut NoopCallback,
                ) {
                    self.abort(error);
                }
            }
            CodecFailureDisposition::Response(status) => {
                let Some(mut stream) = self.streams.remove(&problem.stream) else {
                    return;
                };
                let response = catch_unwind(AssertUnwindSafe(|| {
                    if let Some(mut message) = problem.error.take_http_message()
                        && let Some(source) = &stream.source
                    {
                        source.initialize_body_only(&mut message, false);
                    }
                    self.stream_connection().1.respond_with_error(
                        problem.stream,
                        &mut stream,
                        status,
                        &problem.error,
                    );
                }));
                self.streams.insert(problem.stream, stream);
                if let Err(panic) = response {
                    resume_unwind(panic);
                }
            }
        }
    }
}

impl<A, const UPSTREAM: bool> Driver<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    /// Once the connection has send credit, returns the streams in `conn_blocked` to the front of
    /// the scheduler in their former order, before any owner queues an event in this turn. An
    /// entry whose stream was since reset, retired or stalled is dropped. While the window stays
    /// closed, the list keeps only streams with a pending event once it is more than twice as
    /// long as the stream table.
    #[cold]
    #[inline(never)]
    fn resume_conn_blocked(&mut self) {
        let state = &mut self.connection.state;
        if self.connection.codec.available_send() == 0 {
            if state.conn_blocked.len() > 2 * self.streams.len() {
                state.conn_blocked.retain(|id| {
                    self.streams
                        .get(id)
                        .is_some_and(|stream| stream.pending.is_some())
                });
            }
            return;
        }
        for id in state.conn_blocked.drain(..).rev() {
            if let Some(stream) = self.streams.get_mut(&id)
                && stream.pending.is_some()
                && !stream.stalled
            {
                state.scheduler.enqueue_front(id, &mut stream.scheduling);
            }
        }
    }

    fn serialize_next(&mut self) -> Result<bool, (StreamId, HttpError)> {
        if self.connection.state.scheduler.is_empty() {
            return Ok(false);
        }
        let Some(budget) = serialization_budget(
            &self.connection.state.config,
            &self.egress,
            &self.connection.codec,
        ) else {
            return Ok(false);
        };
        let (streams, mut connection) = self.stream_connection();
        while let Some(id) = connection.connection.state.scheduler.head() {
            let stream = streams
                .get_mut(&id)
                .expect("the scheduler holds live streams");
            if stream.can_serialize(budget) {
                return connection
                    .serialize_selected(id, stream, budget, false)
                    .map(|_| true)
                    .map_err(|error| (id, error));
            }
            if !connection.take_blocked_head(id, stream) {
                break;
            }
        }
        Ok(false)
    }
}

impl<const UPSTREAM: bool> StreamConnection<'_, UPSTREAM> {
    /// Serializes the event that the owner's poll left pending, if its stream heads the scheduler
    /// and the budget admits it. The result is `serialize_selected`'s.
    fn serialize_polled<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        then_body: bool,
    ) -> Option<Result<bool, HttpError>> {
        // Cancellation and retirement retain their checkpoint before serialization.
        if self.failure.fatal.is_some()
            || self.failure.panicked
            || self.cancelled()
            || self.connection.state.scheduler.head() != Some(id)
        {
            return None;
        }
        let budget = serialization_budget(
            &self.connection.state.config,
            self.egress,
            &self.connection.codec,
        )?;
        if !stream.can_serialize(budget) {
            self.take_blocked_head(id, stream);
            return None;
        }
        Some(self.serialize_selected(id, stream, budget, then_body))
    }

    /// Removes `stream`, the scheduler's head, whose pending event cannot serialize, unless only
    /// the turn's write budget holds the event back, and returns whether it did. A body that waits
    /// for connection credit is added to `conn_blocked`. Otherwise a body whose stream window is
    /// exhausted marks the stream stalled until a WINDOW_UPDATE or a larger initial window queues
    /// it again. Any other head is queued again with its owner's next event.
    fn take_blocked_head<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
    ) -> bool {
        if stream.protocol_active()
            && let Some(PendingEgress::Body(event)) = &stream.pending
        {
            match event.kind() {
                HttpBodyEventKind::Body(bytes)
                    if !bytes.is_empty() && (UPSTREAM || !stream.request_head) =>
                {
                    if self.connection.codec.available_send() == 0 {
                        let blocked = &mut self.connection.state.conn_blocked;
                        blocked.push(id);
                        #[cfg(test)]
                        CONN_BLOCKED_PEAK.set(CONN_BLOCKED_PEAK.get().max(blocked.len()));
                    } else if stream.send_window.non_negative_size() == 0 {
                        stream.stalled = true;
                    } else {
                        return false;
                    }
                }
                HttpBodyEventKind::Body(_) | HttpBodyEventKind::Padding(_) => return false,
                _ => {}
            }
        }
        self.connection
            .state
            .scheduler
            .dequeue(id, &mut stream.scheduling);
        true
    }

    /// Serializes the pending event, or the part of its body that `budget` admits, and queues the
    /// owner again once the event is taken, unless the stream waits for its write fence. With
    /// `then_body`, a final head without END_STREAM returns true instead of queueing the owner, and
    /// the caller polls the owner for the body, unless the session is cancelled, a connection
    /// failure is queued or a requested local drain has not started: the driver handles those
    /// before it polls the owner again.
    fn serialize_selected<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        budget: SerializationBudget,
        then_body: bool,
    ) -> Result<bool, HttpError> {
        let maximum = budget.credit.min(stream.send_window.non_negative_size());
        let mut event = stream
            .pending
            .take()
            .expect("serialization requires a pending event");
        let head = then_body && matches!(event, PendingEgress::Headers(_));
        if let PendingEgress::Body(body) = &mut event
            && (UPSTREAM || !stream.request_head)
            && let Some(bytes) = body.as_body_mut()
        {
            let maximum = maximum as usize;
            if bytes.chain_length() > maximum {
                let partial = bytes.split_at_most(maximum);
                stream.pending = Some(event);
                event = PendingEgress::Body(HttpBodyEvent::body(partial, false));
            }
        }
        self.serialize_event(id, stream, event)?;
        if stream.pending.is_none() {
            self.connection
                .state
                .scheduler
                .dequeue(id, &mut stream.scheduling);
        } else if self.connection.state.scheduler.head() == Some(id) {
            // A stream that rotated behind other incremental streams leaves only from the head,
            // which keeps `conn_blocked` in queue order.
            self.take_blocked_head(id, stream);
        }
        let body_next =
            head && stream.response_headers && !stream.response_complete && self.can_continue();
        if !body_next
            && stream.pending.is_none()
            && (!stream.response_complete || stream.physically_complete)
        {
            self.connection.state.control.enqueue(&stream.wake);
        }
        Ok(body_next)
    }

    fn serialize_event<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        event: PendingEgress,
    ) -> Result<(), HttpError> {
        if UPSTREAM
            && let Some((error, wire)) = stream
                .upstream()
                .and_then(|exchange| exchange.egress_error(stream.response_headers))
        {
            self.reset_before_egress(id, stream, event, (error, wire));
            return Ok(());
        }
        let mut section = None;
        let mut encoded_callback = None;
        let eom;
        let mut body_length = 0;
        let mut registrations;
        let generated = match event {
            PendingEgress::Headers(mut headers) => {
                registrations = headers.take_byte_event_registrations();
                eom = headers.eom();
                let final_headers = headers.is_final().map_err(message_type_failure)?;
                if let Some(priority) = headers.headers().get_http_priority() {
                    self.connection
                        .state
                        .scheduler
                        .update(id, &mut stream.scheduling, priority);
                }
                let result = self.connection.codec.generate(
                    self.egress,
                    EgressFrame::Headers {
                        stream: id,
                        message: headers.headers(),
                        end_stream: eom,
                        extra_headers: None,
                    },
                    &mut NoopCallback,
                );
                if let Err(error) = &result {
                    cancel_registrations(&mut registrations, error);
                }
                let generated = result?;
                stream.response_headers |= final_headers;
                if let Some(size) = generated.header_size {
                    section = Some(FieldSectionInfo {
                        section_type: FieldSectionType::Headers,
                        final_headers,
                        size,
                    });
                    encoded_callback = Some((headers, size));
                }
                generated
            }
            PendingEgress::Body(mut event) => {
                eom = event.eom();
                registrations = event.take_byte_event_registrations();
                let result = match event.into_kind() {
                    HttpBodyEventKind::Body(_body) if !UPSTREAM && stream.request_head => {
                        if eom {
                            self.connection.codec.generate(
                                self.egress,
                                EgressFrame::EndOfMessage { stream: id },
                                &mut NoopCallback,
                            )
                        } else {
                            Ok(roxygen_codec::GeneratedFrame::default())
                        }
                    }
                    HttpBodyEventKind::Body(body) => {
                        body_length = body.chain_length() as u64;
                        if !body.is_empty()
                            && !stream.send_window.reserve(body.chain_length() as u32, true)
                        {
                            Err(failure(
                                HttpErrorCode::FLOW_CONTROL_ERROR,
                                b"HTTP/2 stream send window is exhausted",
                            ))
                        } else {
                            self.connection.codec.generate(
                                self.egress,
                                EgressFrame::Body {
                                    stream: id,
                                    body,
                                    padding: None,
                                    end_stream: eom,
                                },
                                &mut NoopCallback,
                            )
                        }
                    }
                    HttpBodyEventKind::Trailers(trailers) => {
                        let generated = self.connection.codec.generate(
                            self.egress,
                            EgressFrame::Trailers {
                                stream: id,
                                trailers: &trailers,
                            },
                            &mut NoopCallback,
                        );
                        if let Ok(generated) = &generated
                            && let Some(size) = generated.header_size
                        {
                            section = Some(FieldSectionInfo {
                                section_type: FieldSectionType::Trailers,
                                final_headers: true,
                                size,
                            });
                        }
                        generated
                    }
                    HttpBodyEventKind::Padding(length) => self.connection.codec.generate(
                        self.egress,
                        EgressFrame::Padding { stream: id, length },
                        &mut NoopCallback,
                    ),
                    HttpBodyEventKind::Upgrade => Ok(roxygen_codec::GeneratedFrame::default()),
                    HttpBodyEventKind::PushPromise(_) => Err(failure(
                        HttpErrorCode::PROTOCOL_ERROR,
                        b"owned downstream experiment does not admit push responses",
                    )),
                    HttpBodyEventKind::Suspend(_) => Err(failure(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        b"a suspended event escaped body polling",
                    )),
                };
                if let Err(error) = &result {
                    cancel_registrations(&mut registrations, error);
                }
                let generated = result?;
                if stream.pending.is_some() {
                    self.connection.state.scheduler.consume(
                        id,
                        &stream.scheduling,
                        generated.bytes_written,
                    );
                }
                generated
            }
        };
        stream.stream_offset = stream
            .stream_offset
            .saturating_add(generated.bytes_written as u64);
        stream.body_offset = stream.body_offset.saturating_add(body_length);
        let mut end = self
            .connection
            .state
            .summary
            .bytes_scheduled
            .saturating_add(self.egress.chain_length() as u64);
        if generated.bytes_written == 0 && !registrations.is_empty() {
            self.connection.state.zero_write_pending = true;
        }
        stream.registered_byte_events |= !registrations.is_empty();
        if eom {
            stream.response_complete = true;
            stream.signals.response_complete();
            stream.release_upstream_capacity();
            if UPSTREAM {
                stream.producer.arm_read_timeout();
            }
            <E as ExchangeStorage<UPSTREAM>>::response_complete_stored(
                stream
                    .application
                    .as_mut()
                    .expect("application remains owned"),
            );
            if let Some(observation) = self.connection.state.observation.as_deref() {
                observation.push(LifecycleEvent::RequestEnd { stream: id });
            }
            if self.cancelled() {
                return Err(completion_cancelled(&mut registrations));
            }
            if stream.reset_after_response {
                self.connection.codec.generate(
                    self.egress,
                    EgressFrame::Reset {
                        stream: id,
                        code: H2ErrorCode::NO_ERROR,
                    },
                    &mut NoopCallback,
                )?;
            }
            let physical_end = self
                .connection
                .state
                .summary
                .bytes_scheduled
                .saturating_add(self.egress.chain_length() as u64);
            end = physical_end;
            stream.write_boundary = Some(physical_end);
            stream.physically_complete = (physical_end
                <= self.connection.state.summary.bytes_written
                && !self.writes_pending())
                || self.retires_at_serialization(stream);
            if !stream.physically_complete {
                self.connection
                    .state
                    .write_fences
                    .push_back((physical_end, id));
            }
        }
        if !registrations.is_empty() || self.context.is_cancelled() {
            self.connection.state.byte_events_registered = true;
            self.connection.state.observer.register_byte_events(
                self.context,
                ByteEventOffsets {
                    stream_id: id,
                    session_byte_offset: end,
                    session_bytes_scheduled: self.connection.state.summary.bytes_scheduled,
                    stream_offset: stream.stream_offset,
                    body_offset: stream.body_offset,
                },
                section,
                registrations,
                eom,
            )?;
        }
        if let Some((mut headers, size)) = encoded_callback {
            headers.on_headers_encoded(size);
        }
        if UPSTREAM && stream.response_headers {
            self.acknowledge_upstream(id, stream)?;
        }
        Ok(())
    }
}

impl<const UPSTREAM: bool> StreamConnection<'_, UPSTREAM> {
    /// Whether a downstream stream whose END_STREAM was just serialized skips its write
    /// fence. It must complete normally, its request must have fully arrived with no trailing
    /// RST pending, it must not have registered a byte event, and the write queue must not be
    /// saturated.
    /// The terms run in this order, so only an otherwise eligible stream queries the queue.
    fn retires_at_serialization<E: ExchangeStorage<UPSTREAM>>(
        &self,
        stream: &Stream<E, UPSTREAM>,
    ) -> bool {
        !UPSTREAM
            && !stream.reset
            && !stream.ingress_rejected
            && stream.wire_ingress_complete
            && !stream.reset_after_response
            && !stream.registered_byte_events
            && self.write_queue.is_some_and(|queue| !queue.is_saturated())
    }

    #[cold]
    #[inline(never)]
    fn reset_before_egress<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        event: PendingEgress,
        (error, wire): (HttpError, H2ErrorCode),
    ) {
        stream.pending = Some(event);
        self.reset_owner(id, stream, &error, false, Some(wire));
    }
}

#[cold]
#[inline(never)]
fn message_type_failure(error: MessageTypeError) -> HttpError {
    HttpError::with_message(HttpErrorCode::MESSAGE_ERROR, error.to_string())
}

#[cold]
#[inline(never)]
fn cancel_registrations(registrations: &mut [HttpByteEventRegistration], error: &HttpError) {
    for registration in registrations {
        registration.cancel(error, None);
    }
}

#[cold]
#[inline(never)]
fn completion_cancelled(registrations: &mut [HttpByteEventRegistration]) -> HttpError {
    let error = failure(
        HttpErrorCode::CORO_CANCELLED,
        b"response completion cancelled the connection",
    );
    cancel_registrations(registrations, &error);
    error
}

#[cfg(test)]
mod codec_input_tests {
    use roxygen_core::stream_source::HttpStreamSourceOwner;

    use super::*;

    /// Names a concrete `CodecInput`; never constructed.
    enum Unused {}

    impl OwnedExchange for Unused {
        fn request(&mut self, _head: Box<HttpMessage>, _body: HttpStreamSourceOwner, _eom: bool) {
            match *self {}
        }

        fn poll_headers(
            &mut self,
            _context: &mut Context<'_>,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
            match *self {}
        }

        fn poll_body(
            &mut self,
            _context: &mut Context<'_>,
            _maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>> {
            match *self {}
        }

        fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {
            match *self {}
        }

        fn response_complete(&mut self) {
            match *self {}
        }

        fn complete(self) {
            match self {}
        }
    }

    impl OwnedApplication for Unused {
        type Exchange = Self;

        fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> Self {
            match *self {}
        }
    }

    #[test]
    fn owned_h2_codec_input_opts_out() {
        assert_eq!(
            [
                CodecInput::<Unused, false>::OBSERVES_FRAME_HEADERS,
                CodecInput::<Unused, true>::OBSERVES_FRAME_HEADERS,
            ],
            [false, false],
            "the downstream and upstream drivers ignore frame-header events"
        );
    }
}

#[cfg(test)]
mod ready_queue_tests {
    use std::thread;

    use super::*;

    #[test]
    fn wake_after_publishes_busy_before_its_mark() {
        let queue = ReadyQueue::default();
        let mut busy_at_mark = None;
        queue.wake_after(|| busy_at_mark = Some(queue.busy.load(Ordering::Acquire)));
        assert_eq!(
            busy_at_mark,
            Some(true),
            "a lock-free `can_fold` that sees a marked `ready` must also see `busy`"
        );
    }

    #[test]
    fn io_wake_marks_ready_only_under_the_queue_lock() {
        let queue = Arc::new(ReadyQueue::default());
        let read = IoWake::new(&queue, false);
        let waker = Waker::from(Arc::clone(&read));
        let held = queue.state.lock();
        let waking = thread::spawn(move || waker.wake_by_ref());
        let deadline = Instant::now() + Duration::from_millis(200);
        while !read.ready.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::yield_now();
        }
        let marked_outside_the_lock = read.ready.load(Ordering::Acquire);
        drop(held);
        waking.join().expect("the wake completes");
        assert!(
            !marked_outside_the_lock,
            "an `IoWake` must mark `ready` through `wake_after`, under the queue lock and after \
             `busy`, or a quiescent session could fold credit ahead of a pending read"
        );
        assert!(read.ready.load(Ordering::Acquire) && queue.busy.load(Ordering::Acquire));
    }

    fn control() -> Control {
        Control {
            drain: Cell::new(false),
            cancel: Cell::new(false),
            complete: Cell::new(false),
            handshake_complete: Cell::new(false),
            active_incoming: Cell::new(0),
            ready: Arc::new(ReadyQueue::default()),
            owners: RefCell::new(VecDeque::new()),
            polling: Cell::new(false),
            owner_waker: RefCell::new(None),
            notified: Cell::new(false),
            upstream: None,
            idle: Cell::new(false),
            host: Host::tokio(),
            quiescent: Cell::new(false),
            credit_headroom: Cell::new(0),
            early_credit: Cell::new(0),
            early_completed: RefCell::new(Vec::new()),
            folds_pending: Cell::new(false),
            window_transition: Cell::new(None),
        }
    }

    fn stream_wake(control: &Control, id: u64) -> Arc<StreamWake> {
        Arc::new(StreamWake {
            id: StreamId::new(id),
            queue: Arc::clone(&control.ready),
            queued: AtomicBool::new(false),
            shared_queued: AtomicBool::new(false),
            headers_poll_ready: AtomicBool::new(false),
            live: AtomicBool::new(true),
        })
    }

    #[derive(Default)]
    struct SessionWakes(std::sync::atomic::AtomicUsize);

    impl Wake for SessionWakes {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Installs a counting session waker on `control`'s queue.
    fn install_waker(control: &Control) -> Arc<SessionWakes> {
        let wakes = Arc::new(SessionWakes::default());
        let waker = Waker::from(Arc::clone(&wakes));
        control
            .ready
            .register(&control.owner_waker, &Context::from_waker(&waker));
        wakes
    }

    fn session_wakes(wakes: &SessionWakes) -> usize {
        wakes.0.load(Ordering::SeqCst)
    }

    fn ids(owners: &[QueuedOwner]) -> Vec<u64> {
        owners.iter().map(|owner| owner.id.get()).collect()
    }

    #[test]
    fn pop_batch_takes_the_oldest_streams_up_to_the_batch_length() {
        let control = control();
        let queued = OWNER_POLLS_PER_TURN as u64 + 4;
        control
            .ready
            .state
            .lock()
            .streams
            .extend((1..=queued).map(StreamId::new));
        control.ready.pushed.store(true, Ordering::Release);
        let mut batch = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];

        assert_eq!(
            control.pop_batch(&mut batch),
            OWNER_POLLS_PER_TURN,
            "a full batch takes as many streams as it holds"
        );
        assert!(
            ids(&batch).into_iter().eq(1..=OWNER_POLLS_PER_TURN as u64),
            "the batch holds the oldest streams in queue order"
        );
        assert_eq!(
            control.pop_batch(&mut batch),
            4,
            "the next batch takes the four streams left over"
        );
        assert!(
            ids(&batch[..4]).into_iter().eq(queued - 3..=queued),
            "the batch holds the remaining streams in queue order"
        );
        assert_eq!(control.pop_batch(&mut batch), 0, "the queue is empty");
    }

    #[test]
    fn owner_thread_queues_skip_the_lock_and_keep_one_queue_order() {
        let control = control();
        let wakes = install_waker(&control);
        let [a, b, c] = [1, 3, 5].map(|id| stream_wake(&control, id));
        let b_waker = futures::task::waker(Arc::clone(&b));

        assert!(control.enqueue(&a));
        assert!(
            control.ready.state.lock().streams.is_empty()
                && !control.ready.pushed.load(Ordering::Acquire)
                && control.ready.busy.load(Ordering::Acquire),
            "the session's thread queues its owner locally and marks the queue busy"
        );
        b_waker.wake_by_ref();
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "a waker queues through the shared queue and wakes the session"
        );
        assert!(control.enqueue(&c));
        futures::task::waker_ref(&a).wake_by_ref();
        assert!(!control.enqueue(&a), "a queued owner is queued once");
        assert!(
            !control.enqueue(&b),
            "an owner that a waker queued is queued once"
        );
        assert!(
            control.ready.state.lock().streams.is_empty(),
            "a waker on the session's thread does not queue a locally queued owner again"
        );
        let mut batch = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];
        let polled = control.pop_batch(&mut batch);
        assert_eq!(
            ids(&batch[..polled]),
            [1, 3, 5],
            "owners keep the order in which they were queued, whichever queue took them"
        );
        assert_eq!(
            batch[..polled]
                .iter()
                .map(|owner| owner.shared)
                .collect::<Vec<_>>(),
            [false, true, false],
            "each entry records the queue that took it"
        );

        for (wake, owner) in [&a, &b, &c].into_iter().zip(&batch[..polled]) {
            wake.take_mark(*owner);
        }
        assert!(
            !b.shared_queued.load(Ordering::Acquire) && !a.queued.load(Ordering::Relaxed),
            "polling an entry clears its mark"
        );
        assert!(control.enqueue(&b), "a polled owner can be queued again");
        let polled = control.pop_batch(&mut batch);
        assert_eq!(ids(&batch[..polled]), [3]);
    }

    #[test]
    fn unqueue_last_removes_only_a_last_entry_that_the_sessions_thread_queued() {
        let control = control();
        install_waker(&control);
        let [a, b] = [1, 3].map(|id| stream_wake(&control, id));
        assert!(control.enqueue(&a));
        assert!(control.enqueue(&b));
        control.unqueue_last(&a);
        assert!(
            a.queued.load(Ordering::Relaxed) && control.owners.borrow().len() == 2,
            "an entry that is not the last stays queued, with its mark"
        );
        control.unqueue_last(&b);
        assert!(
            !b.queued.load(Ordering::Relaxed) && control.owners.borrow().len() == 1,
            "the last entry is removed, and its mark cleared"
        );
        control.unqueue_last(&b);
        assert_eq!(
            control.owners.borrow().len(),
            1,
            "an owner that is not queued removes nothing"
        );
        let waker = futures::task::waker(Arc::clone(&a));
        thread::spawn(move || waker.wake_by_ref())
            .join()
            .expect("the wake completes");
        control.ready.drain_into(&control.owners);
        control.unqueue_last(&a);
        assert!(
            a.queued.load(Ordering::Relaxed) && a.shared_queued.load(Ordering::Acquire),
            "an entry that a waker queued is not removed, and both marks stay"
        );
        let mut batch = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];
        let polled = control.pop_batch(&mut batch);
        assert_eq!(
            batch[..polled]
                .iter()
                .map(|owner| (owner.id.get(), owner.shared))
                .collect::<Vec<_>>(),
            [(1, false), (1, true)]
        );
    }

    #[test]
    fn a_wake_from_another_thread_queues_through_the_lock() {
        let control = control();
        let wakes = install_waker(&control);
        let a = stream_wake(&control, 1);
        assert!(control.enqueue(&a));
        let waker = futures::task::waker(Arc::clone(&a));
        thread::spawn(move || waker.wake_by_ref())
            .join()
            .expect("the wake completes");
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "another thread does not check the local mark, so it queues and wakes the session"
        );
        assert!(control.ready.pushed.load(Ordering::Acquire));
        let idle = IoWake::new(&control.ready, false);
        assert!(!control.settle(&idle, &idle));
    }

    /// One owner queues itself again on each of its polls, as an owner that serializes one
    /// chunk per poll does, and another thread wakes it every other turn while it is queued.
    #[test]
    fn a_cross_thread_wake_adds_one_entry_that_its_poll_retires() {
        let control = control();
        install_waker(&control);
        let a = stream_wake(&control, 1);
        let waker = futures::task::waker(Arc::clone(&a));
        assert!(control.enqueue(&a));
        let mut batch = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];
        let mut entries = Vec::new();
        for turn in 0..8 {
            if turn % 2 == 0 {
                let waker = waker.clone();
                thread::spawn(move || waker.wake_by_ref())
                    .join()
                    .expect("the wake completes");
            }
            let polled = control.pop_batch(&mut batch);
            entries.push(polled);
            for owner in &batch[..polled] {
                a.take_mark(*owner);
                control.enqueue(&a);
            }
        }
        assert_eq!(
            entries,
            [2, 1, 2, 1, 2, 1, 2, 1],
            "each cross-thread wake adds one poll, and the owner keeps at most one entry of \
             each kind"
        );
    }

    #[test]
    fn schedule_only_queues_during_a_poll() {
        let control = Rc::new(control());
        let wakes = install_waker(&control);
        let [a, b, c, d] = [1, 3, 5, 7].map(|id| stream_wake(&control, id));

        let exit = PollExit::enter(&control);
        control.schedule(&a);
        control.schedule(&b);
        assert_eq!(
            session_wakes(&wakes),
            0,
            "the turns poll the owners queued during the poll, without a wake"
        );
        let mut batch = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];
        let polled = control.pop_batch(&mut batch);
        assert_eq!(ids(&batch[..polled]), [1, 3]);
        drop(exit);
        assert_eq!(
            session_wakes(&wakes),
            0,
            "a poll that leaves no owner queued does not wake the session as it returns"
        );

        control.schedule(&c);
        control.schedule(&d);
        control.schedule(&c);
        assert_eq!(
            session_wakes(&wakes),
            1,
            "between polls the first schedule wakes the session, and the poll that wake brings \
             serves the later ones"
        );
        let exit = PollExit::enter(&control);
        let polled = control.pop_batch(&mut batch);
        assert_eq!(ids(&batch[..polled]), [5, 7]);
        drop(exit);
        for (wake, owner) in [&c, &d].into_iter().zip(&batch[..polled]) {
            wake.take_mark(*owner);
        }
        control.schedule(&c);
        assert_eq!(
            session_wakes(&wakes),
            2,
            "a schedule after the next poll started wakes the session again"
        );
    }

    #[test]
    fn a_poll_exit_wakes_the_session_once_for_an_owner_left_queued() {
        let control = Rc::new(control());
        let wakes = install_waker(&control);
        let [a, b] = [1, 3].map(|id| stream_wake(&control, id));

        let exit = PollExit::enter(&control);
        control.schedule(&a);
        drop(exit);
        assert!(!control.polling.get());
        assert_eq!(
            session_wakes(&wakes),
            1,
            "an owner left queued wakes the session as the poll returns"
        );
        control.schedule(&b);
        assert_eq!(
            session_wakes(&wakes),
            1,
            "that wake also covers the schedules made before the next poll"
        );
    }

    /// Counts its wakes and panics on the first `panics` of them.
    struct PanickingWakes {
        panics: usize,
        wakes: std::sync::atomic::AtomicUsize,
    }

    impl Wake for PanickingWakes {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if self.wakes.fetch_add(1, Ordering::SeqCst) < self.panics {
                panic!("session waker");
            }
        }
    }

    fn install_panicking_waker(control: &Control, panics: usize) -> Arc<PanickingWakes> {
        let wakes = Arc::new(PanickingWakes {
            panics,
            wakes: std::sync::atomic::AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&wakes));
        control
            .ready
            .register(&control.owner_waker, &Context::from_waker(&waker));
        wakes
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn a_schedule_whose_wake_panicked_lets_the_next_one_wake() {
        let control = control();
        let wakes = install_panicking_waker(&control, 1);
        let [a, b] = [1, 3].map(|id| stream_wake(&control, id));

        let panicked = catch_unwind(AssertUnwindSafe(|| control.schedule(&a)));
        assert!(
            panicked.is_err(),
            "between polls the waker's panic reaches the schedule's caller"
        );
        control.schedule(&b);
        assert_eq!(
            wakes.wakes.load(Ordering::SeqCst),
            2,
            "a wake that panicked does not hold back the next schedule's wake"
        );
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn a_panic_out_of_a_poll_leaves_polling_clear_and_a_later_schedule_wakes() {
        let control = Rc::new(control());
        let wakes = install_waker(&control);
        let [a, b] = [1, 3].map(|id| stream_wake(&control, id));

        let unwound = catch_unwind(AssertUnwindSafe(|| {
            let _exit = PollExit::enter(&control);
            control.schedule(&a);
            let mut batch = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];
            assert_eq!(control.pop_batch(&mut batch), 1);
            panic!("turn");
        }));
        assert!(unwound.is_err());
        assert!(!control.polling.get(), "the unwound poll is over");
        assert_eq!(
            session_wakes(&wakes),
            0,
            "the unwound poll left no owner queued, so it did not wake the session"
        );
        control.schedule(&b);
        assert_eq!(
            session_wakes(&wakes),
            1,
            "a later schedule wakes the session"
        );
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn a_waker_panic_as_a_poll_unwinds_is_discarded() {
        let control = Rc::new(control());
        let wakes = install_panicking_waker(&control, 1);
        let [a, b] = [1, 3].map(|id| stream_wake(&control, id));

        let unwound = catch_unwind(AssertUnwindSafe(|| {
            let _exit = PollExit::enter(&control);
            control.schedule(&a);
            panic!("turn");
        }));
        let payload = unwound.expect_err("the poll unwinds");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"turn"),
            "the poll's own panic unwinds, and the waker's panic at its exit is discarded"
        );
        assert_eq!(
            wakes.wakes.load(Ordering::SeqCst),
            1,
            "the exit wakes the session for the owner left queued"
        );
        control.schedule(&b);
        assert_eq!(
            wakes.wakes.load(Ordering::SeqCst),
            2,
            "a wake that panicked does not hold back the next schedule's wake"
        );
    }

    thread_local! {
        static INLINE_POLLED: RefCell<Option<Rc<Control>>> = const { RefCell::new(None) };
    }

    /// Counts its wakes and polls `INLINE_POLLED`'s session inside each one.
    #[derive(Default)]
    struct InlinePollWakes(std::sync::atomic::AtomicUsize);

    impl Wake for InlinePollWakes {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            INLINE_POLLED.with_borrow(|control| {
                let control = control.as_ref().expect("a session to poll");
                let _exit = PollExit::enter(control);
                let mut batch = [QueuedOwner::default(); OWNER_POLLS_PER_TURN];
                control.pop_batch(&mut batch);
            });
        }
    }

    #[test]
    fn a_poll_inside_the_wake_lets_a_later_schedule_wake() {
        let control = Rc::new(control());
        INLINE_POLLED.set(Some(Rc::clone(&control)));
        let wakes = Arc::new(InlinePollWakes::default());
        let waker = Waker::from(Arc::clone(&wakes));
        control
            .ready
            .register(&control.owner_waker, &Context::from_waker(&waker));
        let [a, b] = [1, 3].map(|id| stream_wake(&control, id));

        control.schedule(&a);
        control.schedule(&b);
        INLINE_POLLED.set(None);
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            2,
            "the poll that the first wake ran served its owner, so the next schedule wakes again"
        );
    }

    /// Each connection allocates one `Control` behind an `Rc`, which adds its two counts.
    #[test]
    fn control_allocation_stays_in_the_192_class() {
        let allocation = size_of::<Control>() + 2 * size_of::<usize>();
        assert!(
            allocation <= 192,
            "Control's Rc allocation is {allocation} B, past the 192 B class"
        );
    }
}

#[cfg(test)]
mod receive_window_tests {
    use super::*;

    #[test]
    fn owned_receive_window_normalizes_like_uniplex_but_refuses_a_preserved_mismatch() {
        use StreamReceiveWindowMode::MirrorAdvertisedSettings as Mirror;
        use StreamReceiveWindowMode::PreserveConfigured as Preserve;

        // (mode, last advertised INITIAL_WINDOW_SIZE, configured window, OwnedH2's window and
        // whether it adds INITIAL_WINDOW_SIZE). The comment names UniplexSession's result.
        let cases = [
            // Uniplex: the default window, without adding INITIAL_WINDOW_SIZE.
            (Mirror, None, 65_535, Some((65_535, false))),
            (Preserve, None, 65_535, Some((65_535, false))),
            // Uniplex: the configured window, and it adds INITIAL_WINDOW_SIZE.
            (Mirror, None, 262_144, Some((262_144, true))),
            (Preserve, None, 262_144, Some((262_144, true))),
            (
                Preserve,
                None,
                i32::MAX as u32,
                Some((i32::MAX as u32, true)),
            ),
            // Uniplex: the advertised window replaces the default one.
            (Mirror, Some(131_072), 65_535, Some((131_072, false))),
            // Uniplex: the matching window.
            (Mirror, Some(131_072), 131_072, Some((131_072, false))),
            (Preserve, Some(131_072), 131_072, Some((131_072, false))),
            (Mirror, Some(65_535), 65_535, Some((65_535, false))),
            // Uniplex: rejected as conflicting with the advertised SETTINGS.
            (Mirror, Some(131_072), 262_144, None),
            // Uniplex: keeps both windows.
            (Preserve, Some(131_072), 65_535, None),
            (Preserve, Some(131_072), 262_144, None),
            // Uniplex: rejected as not representable.
            (Mirror, Some(0), 65_535, None),
            (Mirror, Some(1 << 31), 65_535, None),
            (Mirror, Some(u64::from(u32::MAX) + 1), 65_535, None),
            (Mirror, None, 0, None),
            (Preserve, None, 1 << 31, None),
            (Preserve, Some(131_072), 0, None),
        ];
        let mismatches: Vec<_> = cases
            .into_iter()
            .filter(|&(mode, advertised, configured, expected)| {
                owned_receive_window(mode, advertised, configured) != expected
            })
            .collect();
        assert!(mismatches.is_empty(), "mismatched cases: {mismatches:?}");
    }
}
