/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tokio host: dialing, tokio-rustls, `HostedWorker` slots, restart, and fail-open state.

mod cohost;

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::future::ready;
use std::io;
use std::io::Cursor;
use std::io::IoSlice;
use std::io::Write as _;
use std::marker::PhantomData;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::JoinHandle;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use futures::FutureExt as _;
use futures::channel::oneshot;
use futures::future::select;
use roxygen_client::connector::ClientAttachOptions;
use roxygen_client::connector::ClientSession;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::thread::current_id;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorkerError;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerCall;
use roxygen_runtime::worker::WorkerCallError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use roxygen_transport::established::AdoptedIoCompletion;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local_with_completion;
use rustls::ClientConfig;
use rustls::ProtocolVersion;
use rustls::pki_types::ServerName;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

pub use self::cohost::CohostExecutor;
pub use self::cohost::CohostExecutorRequest;
use crate::error::ErrorKind;
use crate::error::ExplicitError;
use crate::error::Observed;
use crate::error::Phase;
use crate::error::Position;
use crate::exchange::BodyOutcome;
use crate::exchange::Dialed;
use crate::exchange::ExchangeHooks;
use crate::exchange::ExchangeInput;
use crate::exchange::ExchangeProgress;
use crate::exchange::Handoff;
use crate::exchange::HeaderForm;
use crate::exchange::Request;
use crate::exchange::Response;
use crate::exchange::TeardownWatch;
#[cfg(test)]
use crate::exchange::TestPause;
use crate::exchange::Timings;
use crate::exchange::retire;
use crate::exchange::run_dialed_exchange;
use crate::exchange::session_params;
use crate::exchange::stream_limits;
use crate::exchange::validate_request;
use crate::stats::ExplicitStats;
use crate::stats::PendingTeardown;
use crate::stats::RuntimeStats;
use crate::stats::TeardownGuard;
use crate::tunnel;
use crate::tunnel::ProxyGuard;
use crate::tunnel::Tunnel;
use crate::tunnel::TunnelHandoff;
use crate::tunnel::TunnelLink;

/// The trailing window over which a slot counts its restart starts.
const RESTART_WINDOW: Duration = Duration::from_secs(60);

/// Configuration for [`ExplicitRuntime::start`].
pub struct ExplicitRuntimeConfig {
    /// Number of slots, each with one `HostedWorker` thread.
    pub workers: NonZeroUsize,
    /// Bounds each worker's command queue. A send fails as `Overloaded` when the queue is full,
    /// even after `max_sends_in_flight` admitted it. A capacity below `max_sends_in_flight` can
    /// reject a burst of sends between two of the worker's runs, such as sends that wake
    /// together when a restart publishes.
    pub command_capacity: NonZeroUsize,
    /// Bounds each slot's sends in flight, counted from admission until a send resolves or is
    /// dropped. A send beyond the bound fails as `Overloaded`. Clients of every target share the
    /// slots, and a target that stops responding holds each send until its timeout. Size this
    /// with headroom above the sum of each target's rate times its timeout, divided by `workers`.
    pub max_sends_in_flight: NonZeroUsize,
    /// Slot `i` runs on `{prefix}-{i}`, and its restarts on `{prefix}-restart-{i}`.
    pub thread_name_prefix: &'static str,
    /// Bounds, per slot, both the restarts started within the trailing minute and the restarts
    /// still joining their old worker.
    pub max_restarts_per_minute: u32,
}

/// A set of `HostedWorker` slots that run explicit-connection exchanges.
///
/// Only `ExplicitRuntime` clones own the workers. Dropping the last clone joins every worker
/// and restart thread without a deadline, so drop it from a blocking context.
///
/// The exception is a last drop from caller code, such as a send's waker, that the runtime runs
/// on a thread that its teardown would wait for: a slot's worker or restart thread, or a thread
/// with an outstanding restart step. That drop does not wait for a restart step held by its own
/// thread. It does not join its own thread, or a restart thread that is joining its own thread's
/// worker. A runtime thread left unjoined this way exits when its closure ends, without a join
/// point. An embedder that needs every thread joined should drop the last clone from a thread
/// that the runtime does not own. The drop's other waits and joins run on whichever thread runs
/// it, including a worker's.
#[derive(Clone)]
pub struct ExplicitRuntime {
    owner: Arc<Owner>,
}

struct Owner {
    shared: Arc<Shared>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.shut_down();
    }
}

impl ExplicitRuntime {
    /// Spawns every slot's worker and blocks until each runs, so async callers should call it
    /// from a blocking context.
    pub fn start(config: ExplicitRuntimeConfig) -> Result<Self, ExplicitError> {
        Self::create(config, Arc::default())
    }

    fn create(
        config: ExplicitRuntimeConfig,
        hooks: Arc<RuntimeHooks>,
    ) -> Result<Self, ExplicitError> {
        let ExplicitRuntimeConfig {
            workers,
            command_capacity,
            max_sends_in_flight,
            thread_name_prefix,
            max_restarts_per_minute,
        } = config;
        let mut started = Vec::with_capacity(workers.get());
        for index in 0..workers.get() {
            match spawn_worker(thread_name_prefix, index, command_capacity, &hooks) {
                Ok(worker) => started.push(worker),
                Err(error) => {
                    drop(started);
                    return Err(ExplicitError::new(
                        Phase::Submit,
                        ErrorKind::WorkerLost,
                        format!("slot {index} worker failed to start: {error}"),
                    ));
                }
            }
        }
        let slots = started
            .into_iter()
            .enumerate()
            .map(|(index, worker)| Slot::new(index, worker))
            .collect();
        let shared = Arc::new(Shared {
            slots,
            next_client: AtomicUsize::new(0),
            candidate_state: AtomicU64::new(0),
            command_capacity,
            max_sends_in_flight,
            thread_name_prefix,
            max_restarts_per_minute,
            stats: Arc::new(ExplicitStats::new()),
            hooks,
        });
        Ok(Self {
            owner: Arc::new(Owner { shared }),
        })
    }

    /// Creates a client pinned to one slot, without I/O and without admitting a restart.
    /// Inside a [`CohostExecutor::scope`] task, the client uses that task's worker.
    ///
    /// With more than one slot, the client takes whichever of its round-robin slot and a distinct
    /// pseudo-random slot has fewer sends in flight, and a tie goes to the round-robin slot. A
    /// `Capped` slot, which fails every send immediately, is chosen only when every slot is
    /// `Capped`.
    pub fn client(&self, target: Arc<Target>) -> ExplicitClient {
        let shared = &self.owner.shared;
        let candidates = cohost::current_slot(shared)
            .map(|slot| Candidates {
                round_robin: slot,
                second: None,
            })
            .unwrap_or_else(|| shared.candidates());
        ExplicitClient {
            shared: Arc::clone(shared),
            slot: candidates.choose(shared),
            target,
            header_form: HeaderForm::Map,
            teardown_watch: TeardownWatch::CleanupTask,
            #[cfg(test)]
            candidates: (candidates.round_robin, candidates.second),
        }
    }

    /// A snapshot of the runtime's statistics.
    pub fn stats(&self) -> RuntimeStats {
        self.owner.shared.stats.snapshot()
    }

    /// Returns whether every slot's worker is open, and requests a restart of each slot with a
    /// worker that is not. It does not perform I/O or block, apart from running a waiting
    /// send's waker when a restart thread fails to spawn (see [`ExplicitRuntime`]).
    pub fn is_healthy(&self) -> bool {
        let shared = &self.owner.shared;
        shared
            .slots
            .iter()
            .map(|slot| slot.observe(shared, None, None).state())
            .filter(|state| *state != SlotState::Open)
            .count()
            == 0
    }
}

/// A numeric destination. This API does not resolve names.
pub struct Target {
    /// The address the exchange dials: the destination's, or a tunnel's proxy's.
    pub address: SocketAddr,
    /// The immediate hop's security.
    pub security: Security,
}

/// The immediate hop's security.
#[derive(Clone, Debug)]
pub enum Security {
    /// Plaintext TCP.
    Plaintext,
    /// TLS with the caller's configuration, which must not offer an ALPN protocol.
    Tls {
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
    },
    /// A CONNECT tunnel through the plaintext HTTP/1.1 proxy at `Target::address`, with TLS to
    /// the destination inside it.
    Tunnel(Arc<Tunnel>),
}

impl Security {
    /// Builds TLS security for `host`, which may be a DNS name, or an IPv4 or IPv6 literal with
    /// or without one pair of surrounding brackets. Does not perform I/O.
    pub fn tls(config: Arc<ClientConfig>, host: &str) -> Result<Self, ExplicitError> {
        if !config.alpn_protocols.is_empty() {
            return Err(alpn_offered());
        }
        let server_name = tunnel::server_name(host)?;
        Ok(Self::Tls {
            config,
            server_name,
        })
    }
}

fn alpn_offered() -> ExplicitError {
    ExplicitError::new(
        Phase::Tls,
        ErrorKind::Protocol,
        "the TLS configuration offers ALPN, which phase 1 does not support",
    )
}

/// A slot's state, as observed before any restart that the query admits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotState {
    /// The published worker is open.
    Open,
    /// The published worker is not open, and a restart is not in flight.
    Closed,
    /// A restart is in flight.
    Restarting,
    /// The published worker is not open and the restart bounds do not allow a restart.
    Capped,
}

/// A slot's health, for an embedder that routes around unhealthy slots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotHealth {
    /// The slot's state.
    pub state: SlotState,
    /// Sends admitted to the slot and not yet resolved or dropped.
    pub sends_in_flight: usize,
    /// Commands waiting in the published worker's queue.
    pub queued_commands: usize,
    /// `ExplicitRuntimeConfig::command_capacity`.
    pub command_capacity: usize,
    /// `ExplicitRuntimeConfig::max_sends_in_flight`.
    pub max_sends_in_flight: usize,
    /// `Overloaded` rejections since the slot last accepted a send or reported spare capacity.
    pub consecutive_overloads: u64,
}

/// Sends exchanges to one target on one pinned slot. It does not own a worker.
pub struct ExplicitClient {
    shared: Arc<Shared>,
    slot: usize,
    target: Arc<Target>,
    header_form: HeaderForm,
    teardown_watch: TeardownWatch,
    #[cfg(test)]
    candidates: (usize, Option<usize>),
}

impl ExplicitClient {
    /// Makes this client's responses keep their headers as the codec parsed them, in
    /// `Response::raw_headers`, and leave `Response::headers` empty, so that exchanges do not
    /// build a `HeaderMap`. `RawHeaders` lookups return what that map would hold, and
    /// `dropped_headers` counts what it would drop.
    pub fn with_raw_headers(mut self) -> Self {
        self.header_form = HeaderForm::Raw;
        self
    }

    /// Makes each adopted stream of this client, including a tunnel's proxy stream, own a guard
    /// that keeps it in `RuntimeStats::pending_teardowns` until it is destroyed. This replaces
    /// the default worker cleanup task per stream that awaits its retirement. A stream that
    /// never retires still stays counted, but its exchanges do not record a
    /// `close_to_retirement_us` sample. A stream destroyed while its thread panics counts as an
    /// unconfirmed retirement.
    pub fn with_teardown_guard(mut self) -> Self {
        self.teardown_watch = TeardownWatch::StreamGuard;
        self
    }

    /// Sends one GET on a new connection and returns once its body is drained.
    ///
    /// Poll it inside a Tokio runtime with time enabled. A send polled off its worker's thread
    /// arms its deadline timer in the runtime of its first such poll, which must keep running
    /// until the send resolves. Dropping the future before it resolves cancels the exchange.
    ///
    /// The runtime does not clone, wake, or drop the context's waker while holding a lock that
    /// its workers, restarts, or teardown take, so that waker may own the last runtime clone.
    /// See [`ExplicitRuntime`] for which thread that drop runs on. A panic from that waker on a
    /// worker's thread is caught and disposed of there, so it does not close the worker.
    pub async fn send(&mut self, request: Request) -> Result<Response, ExplicitError> {
        let stats = Arc::clone(&self.shared.stats);
        stats.count_send();
        let result = self.dispatch(request).await;
        let timings = match &result {
            Ok(response) => &response.timings,
            Err(error) => {
                match error.kind {
                    ErrorKind::Overloaded => stats.count_overload(),
                    ErrorKind::WorkerLost => stats.count_worker_loss(),
                    _ => {}
                }
                &error.timings
            }
        };
        record_timings(&stats, timings);
        result
    }

    /// Reports the pinned slot's health without I/O or blocking, requesting a restart when its
    /// worker is not open. A restart thread that fails to spawn wakes the slot's waiting sends
    /// on the calling thread, as [`ExplicitRuntime::is_healthy`] does.
    pub fn slot_health(&self) -> SlotHealth {
        let slot = &self.shared.slots[self.slot];
        let state = slot.observe(&self.shared, None, None).state();
        let queued_commands = slot
            .published_handle()
            .map_or(0, |handle| handle.queued_commands());
        let sends_in_flight = slot.sends_in_flight.load(Ordering::Acquire);
        let command_capacity = self.shared.command_capacity.get();
        let max_sends_in_flight = self.shared.max_sends_in_flight.get();
        if sends_in_flight < max_sends_in_flight && queued_commands < command_capacity {
            slot.consecutive_overloads.store(0, Ordering::Release);
        }
        SlotHealth {
            state,
            sends_in_flight,
            queued_commands,
            command_capacity,
            max_sends_in_flight,
            consecutive_overloads: slot.consecutive_overloads.load(Ordering::Acquire),
        }
    }

    async fn dispatch(&mut self, request: Request) -> Result<Response, ExplicitError> {
        let submitted_at = Instant::now();
        self.shared.hooks.request_validated();
        validate_request(&request)?;
        if let Security::Tls { config, .. } = &self.target.security
            && !config.alpn_protocols.is_empty()
        {
            return Err(alpn_offered());
        }
        let slot = &self.shared.slots[self.slot];
        let Some(admission) = SendAdmission::admit(&self.shared, slot) else {
            slot.consecutive_overloads.fetch_add(1, Ordering::AcqRel);
            return Err(overloaded(
                "the slot's sends in flight are at max_sends_in_flight",
            ));
        };
        let mut cancel = CancelGuard {
            progress: Arc::new(ExchangeProgress::new()),
            source: CancellationSource::new(),
            stats: Arc::clone(&self.shared.stats),
            armed: true,
        };
        let result = self.run(request, submitted_at, &cancel).await;
        cancel.armed = false;
        drop(admission);
        result
    }

    async fn run(
        &self,
        request: Request,
        submitted_at: Instant,
        cancel: &CancelGuard,
    ) -> Result<Response, ExplicitError> {
        let slot = &self.shared.slots[self.slot];
        let deadline = tokio::time::Instant::from_std(request.deadline);
        let mut seen = 0;
        let (call, owner) = loop {
            let (handle, generation) = match slot.observe(&self.shared, None, Some(&mut seen)) {
                Observation::Open(handle, generation) => (handle, generation),
                Observation::Restarting(failures) | Observation::Admitted(failures) => {
                    self.wait_for_publication(&mut seen, deadline, failures, submitted_at)
                        .await?;
                    continue;
                }
                Observation::Capped => {
                    return Err(worker_lost(
                        "the slot's worker is closed at its restart cap",
                    ));
                }
                Observation::Closed => return Err(worker_lost("the runtime has shut down")),
            };
            let job = Job {
                target: Arc::clone(&self.target),
                request: request.clone(),
                cancellation: cancel.source.token(),
                progress: Arc::clone(&cancel.progress),
                submitted_at,
                shared: Arc::clone(&self.shared),
                header_form: self.header_form,
                teardown_watch: self.teardown_watch,
            };
            let submitted = match &self.target.security {
                Security::Plaintext => submit_job::<Direct<PlaintextHop>>(&handle, job),
                Security::Tls { .. } => submit_job::<Direct<TlsHop>>(&handle, job),
                Security::Tunnel(_) => submit_job::<TunnelRoute>(&handle, job),
            };
            match submitted {
                Ok(call) => {
                    slot.consecutive_overloads.store(0, Ordering::Release);
                    break (call, handle.owner_thread());
                }
                Err(SubmitError::Closed) => {
                    slot.observe(&self.shared, Some(generation), None);
                }
                Err(SubmitError::Overloaded) => {
                    slot.consecutive_overloads.fetch_add(1, Ordering::AcqRel);
                    return Err(overloaded("the worker's command queue is full"));
                }
            }
        };

        let mut call = pin!(call);
        let mut sleep = pin!(None::<tokio::time::Sleep>);
        let mut expired = false;
        let call_waker = Arc::new(CallWaker::default());
        let waker = Waker::from(Arc::clone(&call_waker));
        let _close = CloseCallWaker(&call_waker);
        let result = poll_fn(|context| {
            // A passed deadline wins over a ready worker loss at every poll. The clock is read
            // first, because the send may resume after its deadline before a sleep fires. A send
            // polled on its worker's thread cannot use that thread's timer, which cannot fire
            // while the send is polled. It relies instead on the job's pre-dial expiry check and
            // on the wake from a command that a closing worker drops. Its sleep arms at its first
            // poll off that thread.
            if !expired {
                if tokio::time::Instant::now() >= deadline {
                    expired = true;
                } else {
                    if sleep.is_none() && current_id() != owner {
                        sleep.set(Some(tokio::time::sleep_until(deadline)));
                        self.shared.hooks.caller_sleep_armed();
                    }
                    expired = sleep
                        .as_mut()
                        .as_pin_mut()
                        .is_some_and(|sleep| sleep.poll(context).is_ready());
                }
                // Once the worker has started the exchange, its own deadline delivers the result.
                if expired && cancel.progress.claim_unstarted() {
                    return Poll::Ready(None);
                }
            }
            let replaced = call_waker.register(context.waker());
            let poll = call.as_mut().poll(&mut Context::from_waker(&waker));
            drop(replaced);
            poll.map(Some)
        })
        .await;
        match result {
            None => Err(self.expired_before_start(submitted_at)),
            Some(Ok(result)) => result,
            Some(Err(WorkerCallError::Cancelled)) => {
                let reached = cancel.progress.reached_session();
                let phase = if reached {
                    Phase::Read
                } else if !cancel.progress.started() {
                    Phase::Submit
                } else if let Security::Tunnel(_) = self.target.security {
                    TunnelRoute::SETUP_PHASE
                } else {
                    Phase::Connect
                };
                Err(ExplicitError::new(
                    phase,
                    ErrorKind::WorkerLost,
                    "the worker dropped the exchange",
                )
                .reached(reached))
            }
        }
    }

    /// Waits until the in-flight restart publishes. `failures` counts the failed restarts
    /// before it, and `seen` the publication signals already raised.
    ///
    /// A passed deadline wins over a signal already raised, as in the `WorkerCall` wait.
    async fn wait_for_publication(
        &self,
        seen: &mut u64,
        deadline: tokio::time::Instant,
        failures: u64,
        submitted_at: Instant,
    ) -> Result<(), ExplicitError> {
        let slot = &self.shared.slots[self.slot];
        self.shared.hooks.before_publication_wait().await;
        let mut sleep = pin!(tokio::time::sleep_until(deadline));
        let mut wait = SignalWait { slot, key: None };
        loop {
            let signalled = poll_fn(|context| {
                if tokio::time::Instant::now() >= deadline
                    || sleep.as_mut().poll(context).is_ready()
                {
                    return Poll::Ready(false);
                }
                wait.poll(*seen, context).map(|()| true)
            })
            .await;
            if !signalled {
                return Err(self.expired_before_start(submitted_at));
            }
            match slot.waited(failures, seen) {
                Waited::Restarting => {}
                Waited::Published => return Ok(()),
                Waited::Failed => return Err(worker_lost("the slot's restart failed")),
                Waited::ShutDown => return Err(worker_lost("the runtime has shut down")),
            }
        }
    }

    /// The error for a send whose deadline passed before its exchange started. Its queue delay
    /// is the time since `send`.
    fn expired_before_start(&self, submitted_at: Instant) -> ExplicitError {
        let mut error = ExplicitError::expired(Phase::Submit);
        error.timings.queued = submitted_at.elapsed();
        self.shared.stats.record_queued(error.timings.queued);
        error
    }
}

fn worker_lost(detail: &str) -> ExplicitError {
    ExplicitError::new(Phase::Submit, ErrorKind::WorkerLost, detail)
}

fn overloaded(detail: &str) -> ExplicitError {
    ExplicitError::new(Phase::Submit, ErrorKind::Overloaded, detail)
}

/// Records the stage timings of a resolved send. Queue delays are recorded where they are
/// measured.
fn record_timings(stats: &ExplicitStats, timings: &Timings) {
    timings
        .tcp_connect
        .into_iter()
        .for_each(|sample| stats.record_tcp_connect(sample));
    timings
        .tls_handshake
        .into_iter()
        .for_each(|sample| stats.record_tls_handshake(sample));
    timings
        .first_byte
        .into_iter()
        .for_each(|sample| stats.record_first_byte(sample));
}

/// Counts a send in flight on its slot from admission until it resolves or is dropped.
struct SendAdmission<'a> {
    slot: &'a Slot,
    stats: &'a ExplicitStats,
}

impl<'a> SendAdmission<'a> {
    fn admit(shared: &'a Shared, slot: &'a Slot) -> Option<Self> {
        let bound = shared.max_sends_in_flight.get();
        let mut count = slot.sends_in_flight.load(Ordering::Acquire);
        loop {
            if count >= bound {
                return None;
            }
            match slot.sends_in_flight.compare_exchange_weak(
                count,
                count + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => count = actual,
            }
        }
        shared.stats.send_admitted();
        Some(Self {
            slot,
            stats: &shared.stats,
        })
    }
}

impl Drop for SendAdmission<'_> {
    fn drop(&mut self) {
        self.slot.sends_in_flight.fetch_sub(1, Ordering::AcqRel);
        self.stats.send_resolved();
    }
}

/// Cancels an unresolved send's exchange when its future is dropped.
struct CancelGuard {
    progress: Arc<ExchangeProgress>,
    source: CancellationSource,
    stats: Arc<ExplicitStats>,
    armed: bool,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if !self.armed || !self.progress.claim() {
            return;
        }
        self.stats.count_caller_cancellation();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.source.request_cancellation()))
        {
            discard_panic(payload);
        }
    }
}

/// The waker that a send polls its `WorkerCall` with, in place of the caller's.
///
/// `WorkerCall::poll` clones the waker it is given, and drops the one it replaces, while holding
/// the response lock that the worker takes to deliver or cancel the result. If a caller's waker
/// released the last runtime clone there, the worker would be joined under that lock. This
/// waker's clone and drop only change its reference count, and the send holds its own reference
/// throughout. The caller's waker is kept in the `caller` mutex, which is never held while
/// caller code runs. The send clones and drops the caller's waker on its own thread, and a wake
/// wakes it by reference on the waking thread, each without a lock held.
#[derive(Default)]
struct CallWaker {
    caller: Mutex<CallerWaker>,
}

#[derive(Default)]
struct CallerWaker {
    waker: Option<Waker>,
    /// Set once the send resolves or is dropped. After that, a caller's waker is not stored.
    closed: bool,
}

impl CallWaker {
    fn lock(&self) -> MutexGuard<'_, CallerWaker> {
        self.caller.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores `waker` for the next wake unless the stored waker wakes the same task. It clones
    /// `waker` with the lock released and returns the waker it replaces, which the caller drops
    /// with no lock held.
    fn register(&self, waker: &Waker) -> Option<Waker> {
        if self
            .lock()
            .waker
            .as_ref()
            .is_some_and(|stored| stored.will_wake(waker))
        {
            return None;
        }
        let waker = waker.clone();
        let mut caller = self.lock();
        let replaced = caller.waker.replace(waker);
        drop(caller);
        replaced
    }

    /// Takes the caller's waker out for good, for the send to drop on its own thread.
    fn close(&self) -> Option<Waker> {
        let mut caller = self.lock();
        caller.closed = true;
        caller.waker.take()
    }
}

impl Wake for CallWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    /// Wakes the caller's waker by reference with the lock released, and then puts it back. If
    /// a newer registration has filled the slot or the send has closed it, the waker is dropped
    /// instead, after the lock is released.
    ///
    /// The waking thread may be a slot's worker. A panic there would unwind into the worker's
    /// command and close the worker under every other send on it. The caller's wake and drop
    /// therefore each run in their own unwind boundary, and a panic is disposed of here.
    fn wake_by_ref(self: &Arc<Self>) {
        let Some(waker) = self.lock().waker.take() else {
            return;
        };
        contain(|| waker.wake_by_ref());
        let mut caller = self.lock();
        if caller.waker.is_none() && !caller.closed {
            caller.waker = Some(waker);
            return;
        }
        drop(caller);
        contain(|| drop(waker));
    }
}

/// Runs caller code in its own unwind boundary and disposes of any panic it raises.
fn contain(callback: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(callback)) {
        discard_panic(payload);
    }
}

/// Closes a send's call waker when the send resolves or is dropped, dropping the caller's waker
/// on that thread with no lock held.
struct CloseCallWaker<'a>(&'a CallWaker);

impl Drop for CloseCallWaker<'_> {
    fn drop(&mut self) {
        drop(self.0.close());
    }
}

/// Plain data that crosses to the worker for one exchange.
struct Job {
    target: Arc<Target>,
    request: Request,
    cancellation: CancellationToken,
    progress: Arc<ExchangeProgress>,
    submitted_at: Instant,
    shared: Arc<Shared>,
    header_form: HeaderForm,
    teardown_watch: TeardownWatch,
}

/// Hands `job` to `handle` as `R`'s job future.
fn submit_job<R: Route>(
    handle: &WorkerHandle,
    job: Job,
) -> Result<WorkerCall<Result<Response, ExplicitError>>, SubmitError> {
    #[cfg(test)]
    let hooks = Arc::clone(&job.shared.hooks);
    let factory = move |context| run_job::<R>(context, job);
    #[cfg(test)]
    hooks
        .job_future_sizes
        .lock()
        .unwrap()
        .push(output_size(&factory));
    handle.call_local(factory)
}

/// What the dial and the core borrow from the job that `run_job` owns.
struct Dial<'a> {
    context: &'a WorkerContext,
    target: &'a Target,
    /// Taken when the core starts. The core consumes the request before it returns its future,
    /// so that other futures do not hold it.
    request: &'a mut Option<Request>,
    deadline: Instant,
    cancellation: &'a CancellationToken,
    progress: &'a Arc<ExchangeProgress>,
    shared: &'a Arc<Shared>,
    submitted_at: Instant,
    /// The dial's stages measured so far, which a caught panic reports.
    stages: &'a RefCell<Timings>,
    header_form: HeaderForm,
    teardown_watch: TeardownWatch,
}

/// How a job reaches its target. Each route is its own job future, so that a direct send's
/// future does not hold tunnel state.
trait Route: 'static {
    /// The phase of a worker loss or caught panic after the exchange started and before the
    /// hand-off to its session.
    const SETUP_PHASE: Phase;

    fn dial_and_exchange(dial: Dial<'_>) -> impl Future<Output = Result<Response, ExplicitError>>;
}

/// A direct target, with the immediate hop secured by `H`.
struct Direct<H>(PhantomData<H>);

impl<H: Hop> Route for Direct<H> {
    const SETUP_PHASE: Phase = Phase::Connect;

    fn dial_and_exchange(dial: Dial<'_>) -> impl Future<Output = Result<Response, ExplicitError>> {
        dial_and_exchange::<H>(dial)
    }
}

/// A `Security::Tunnel` target.
struct TunnelRoute;

impl Route for TunnelRoute {
    const SETUP_PHASE: Phase = Phase::TunnelConnect;

    fn dial_and_exchange(dial: Dial<'_>) -> impl Future<Output = Result<Response, ExplicitError>> {
        tunnel_dial_and_exchange(dial)
    }
}

/// The job future of one exchange that `R` routes.
///
/// The layers that carry the exchange's input by value are plain `fn`s that split it before
/// they return their futures, because a future keeps everything that an `async fn` takes, or an
/// `async` block captures, for its whole life.
fn run_job<R: Route>(
    context: WorkerContext,
    job: Job,
) -> impl Future<Output = Result<Response, ExplicitError>> {
    let Job {
        target,
        request,
        cancellation,
        progress,
        submitted_at,
        shared,
        header_form,
        teardown_watch,
    } = job;
    let deadline = request.deadline;
    let mut request = Some(request);
    async move {
        let stages = RefCell::default();
        let dial = Dial {
            context: &context,
            target: &target,
            request: &mut request,
            deadline,
            cancellation: &cancellation,
            progress: &progress,
            shared: &shared,
            submitted_at,
            stages: &stages,
            header_form,
            teardown_watch,
        };
        let caught = AssertUnwindSafe(R::dial_and_exchange(dial))
            .catch_unwind()
            .await;
        // The job's inputs drop inside an unwind boundary too, because once the caller has
        // dropped its client, the target may own the last reference to caller code, such as its
        // TLS configuration.
        let dropped = catch_unwind(AssertUnwindSafe(move || {
            drop((cancellation, request, target, context));
        }));
        let caught = match (caught, dropped) {
            (caught, Ok(())) => caught,
            (Ok(_), Err(payload)) => Err(payload),
            (Err(payload), Err(second)) => {
                discard_panic(second);
                Err(payload)
            }
        };
        let result = match caught {
            Ok(result) => result,
            Err(payload) => {
                shared.stats.count_panic();
                let detail = match panic_message(payload.as_ref()) {
                    Some(message) => format!("the exchange panicked on its worker: {message}"),
                    None => "the exchange panicked on its worker".to_owned(),
                };
                discard_panic(payload);
                let reached = progress.reached_session();
                let kind = if progress.finish() {
                    ErrorKind::Panicked
                } else {
                    ErrorKind::Cancelled
                };
                Err(ExplicitError {
                    timings: Box::new(stages.take()),
                    ..ExplicitError::new(
                        if reached { Phase::Read } else { R::SETUP_PHASE },
                        kind,
                        detail,
                    )
                    .reached(reached)
                })
            }
        };
        shared.hooks.job_finished(&result);
        result
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
}

enum Staged<T> {
    Done(T),
    Expired,
    Cancelled,
}

/// Races dial or handshake work against the exchange's token and its deadline. The exchange
/// creates the deadline sleep once and shares it between its dial and the core's races. Dropping
/// the work drops its socket.
///
/// A passed deadline wins even over dial or handshake work that is ready, as in the core's
/// reads. It is read from the clock before the work is polled, because a host may fire the
/// sleep later than its deadline. `Expired` ends the exchange, so a finished sleep is never
/// polled again.
async fn staged<Work: Future>(
    host: &Host,
    deadline: Instant,
    cancellation: &CancellationToken,
    sleep: &mut LocalFuture,
    work: Work,
) -> Staged<Work::Output> {
    let mut work = pin!(work);
    let mut registration = cancellation.wake_registration();
    poll_fn(|context| {
        if registration.update_waker(context.waker()) {
            return Poll::Ready(Staged::Cancelled);
        }
        if host.now() >= deadline {
            return Poll::Ready(Staged::Expired);
        }
        if let Poll::Ready(output) = work.as_mut().poll(context) {
            return Poll::Ready(Staged::Done(output));
        }
        if sleep.as_mut().poll(context).is_ready() {
            return Poll::Ready(Staged::Expired);
        }
        Poll::Pending
    })
    .await
}

/// The transport endpoint of a dialed stream's address, and the address that its
/// `ConnectionInfo` reports.
///
/// The endpoint's host is the IP address's text. The reported address is that text's address,
/// so a scoped IPv6 address loses its flow label and scope.
///
/// The peer's address is the one the exchange dialed, `Target::address` (the proxy's for a
/// tunnel), not the kernel's `getpeername`. The two differ for an unspecified destination, which
/// the kernel routes to loopback, and for a destination that a cgroup connect hook rewrites.
pub(crate) fn dialed_endpoint(
    address: Option<SocketAddr>,
) -> (TransportEndpoint, Option<SocketAddr>) {
    let Some(address) = address else {
        return (TransportEndpoint::Unknown, None);
    };
    let ip = address.ip();
    // Holds the longest IP text, `ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255`.
    let mut text = [0_u8; 45];
    let length = match ip {
        IpAddr::V4(ipv4) => write_ipv4(&mut text, ipv4),
        IpAddr::V6(_) => {
            let mut cursor = Cursor::new(&mut text[..]);
            write!(cursor, "{ip}").expect("should fit an IP address's text in 45 bytes");
            usize::try_from(cursor.position()).expect("should fit 45 in usize")
        }
    };
    let host = SocketAddress::new(&text[..length], address.port());
    (
        TransportEndpoint::Socket(host),
        Some(SocketAddr::new(ip, address.port())),
    )
}

/// Writes an IPv4 address's text as `Ipv4Addr`'s `Display` does, and returns its length.
fn write_ipv4(text: &mut [u8], ip: Ipv4Addr) -> usize {
    ip.octets().into_iter().fold(0, |mut length, octet| {
        if length > 0 {
            text[length] = b'.';
            length += 1;
        }
        let digits = [
            b'0' + octet / 100,
            b'0' + octet / 10 % 10,
            b'0' + octet % 10,
        ];
        let first = usize::from(octet < 100) + usize::from(octet < 10);
        let end = length + digits.len() - first;
        text[length..end].copy_from_slice(&digits[first..]);
        end
    })
}

/// How a job secures the TCP stream it dialed. It is a type, chosen from the target's
/// `Security` before the job is submitted, so that a plaintext job's future does not hold TLS
/// state.
trait Hop: 'static {
    type Io: AsyncRead + AsyncWrite + Unpin + 'static;
    const SECURITY: TransportSecurity;

    /// Secures `tcp`, returning the stream and its TLS version, or the failure the job reports.
    fn secure(
        tcp: TcpStream,
        step: SecureStep<'_>,
    ) -> impl Future<Output = Result<(Self::Io, Option<ProtocolVersion>), ExplicitError>>;
}

/// What a hop's securing step reads from its job.
struct SecureStep<'a> {
    security: &'a Security,
    host: &'a Host,
    deadline: Instant,
    cancellation: &'a CancellationToken,
    sleep: &'a mut LocalFuture,
    hooks: &'a RuntimeHooks,
}

struct PlaintextHop;

impl Hop for PlaintextHop {
    type Io = TcpStream;
    const SECURITY: TransportSecurity = TransportSecurity::Plaintext;

    fn secure(
        tcp: TcpStream,
        _: SecureStep<'_>,
    ) -> impl Future<Output = Result<(TcpStream, Option<ProtocolVersion>), ExplicitError>> {
        ready(Ok((tcp, None)))
    }
}

struct TlsHop;

impl Hop for TlsHop {
    /// Boxed once, so that only a pointer moves through the exchange's layers.
    type Io = Pin<Box<TlsStream<TcpStream>>>;
    const SECURITY: TransportSecurity = TransportSecurity::Tls;

    /// Starts the handshake, boxed so that its state does not set the job future's size.
    fn secure(
        tcp: TcpStream,
        step: SecureStep<'_>,
    ) -> impl Future<Output = Result<(Self::Io, Option<ProtocolVersion>), ExplicitError>> {
        let Security::Tls {
            config,
            server_name,
        } = step.security
        else {
            unreachable!("a TLS hop is chosen only for a target with TLS security");
        };
        #[cfg(test)]
        step.hooks
            .dialed_tls_configs
            .lock()
            .unwrap()
            .push(Arc::as_ptr(config) as usize);
        let handshake =
            Box::pin(TlsConnector::from(Arc::clone(config)).connect(server_name.clone(), tcp));
        step.hooks.before_handshake();
        async move {
            match staged(
                step.host,
                step.deadline,
                step.cancellation,
                step.sleep,
                handshake,
            )
            .await
            {
                Staged::Done(Ok(tls)) => {
                    let version = tls.get_ref().1.protocol_version();
                    Ok((Box::pin(tls), version))
                }
                Staged::Done(Err(error)) => Err(ExplicitError::from_tls_io(&error)),
                Staged::Expired => Err(ExplicitError::expired(Phase::Tls)),
                Staged::Cancelled => Err(ExplicitError::new(
                    Phase::Tls,
                    ErrorKind::Cancelled,
                    "cancelled during the TLS handshake",
                )),
            }
        }
    }
}

fn dial_and_exchange<H: Hop>(
    dial: Dial<'_>,
) -> impl Future<Output = Result<Response, ExplicitError>> {
    let Dial {
        context,
        target,
        request,
        deadline,
        cancellation,
        progress,
        shared,
        submitted_at,
        stages,
        header_form,
        teardown_watch,
    } = dial;
    async move {
        if !progress.try_start() {
            return Err(ExplicitError::new(
                Phase::Submit,
                ErrorKind::Cancelled,
                "the exchange was claimed before it started",
            ));
        }
        let host = context.host().clone();
        let started = host.now();
        let mut timings = Timings {
            queued: started.saturating_duration_since(submitted_at),
            ..Timings::default()
        };
        shared.stats.record_queued(timings.queued);
        stages.replace(timings.clone());
        let observed = || Observed {
            own_cancelled: cancellation.is_cancellation_requested(),
            worker_open: context.handle().state() == WorkerState::Open,
            deadline_passed: false,
        };
        let fail = |error: ExplicitError, timings: &Timings| {
            let error = if progress.finish() {
                error
            } else {
                ExplicitError {
                    kind: ErrorKind::Cancelled,
                    ..error
                }
            };
            Err(ExplicitError {
                timings: Box::new(timings.clone()),
                ..error.observed(observed())
            })
        };
        if cancellation.is_cancellation_requested() {
            let error = ExplicitError::new(
                Phase::Submit,
                ErrorKind::Cancelled,
                "cancelled before dialing",
            );
            return fail(error, &timings);
        }
        if host.now() >= deadline {
            return fail(ExplicitError::expired(Phase::Submit), &timings);
        }

        shared.hooks.before_connect();
        let mut sleep = host.sleep_until(deadline);
        shared.hooks.deadline_sleep_created();
        // The stream is borrowed before it moves to the hop, so it is scoped to this block. A
        // borrowed local of an `async` block stays in its future until the local's scope ends.
        let (secure, local, peer) = {
            let tcp = match staged(
                &host,
                deadline,
                cancellation,
                &mut sleep,
                TcpStream::connect(target.address),
            )
            .await
            {
                Staged::Done(Ok(tcp)) => tcp,
                Staged::Done(Err(error)) => {
                    return fail(ExplicitError::from_connect_io(&error), &timings);
                }
                Staged::Expired => return fail(ExplicitError::expired(Phase::Connect), &timings),
                Staged::Cancelled => {
                    let error = ExplicitError::new(
                        Phase::Connect,
                        ErrorKind::Cancelled,
                        "cancelled during connect",
                    );
                    return fail(error, &timings);
                }
            };
            if let Err(error) = tcp.set_nodelay(true) {
                return fail(ExplicitError::from_connect_io(&error), &timings);
            }
            #[cfg(test)]
            shared
                .hooks
                .nodelay
                .lock()
                .unwrap()
                .push(tcp.nodelay().unwrap_or(false));
            timings.tcp_connect = Some(host.now().saturating_duration_since(started));
            stages.replace(timings.clone());
            let local = tcp.local_addr().ok();
            let peer = Some(target.address);
            let step = SecureStep {
                security: &target.security,
                host: &host,
                deadline,
                cancellation,
                sleep: &mut sleep,
                hooks: &shared.hooks,
            };
            (H::secure(tcp, step), local, peer)
        };
        let (io, tls_version) = match secure.await {
            Ok(secured) => secured,
            Err(error) => return fail(error, &timings),
        };
        if H::SECURITY == TransportSecurity::Tls {
            timings.tls_handshake = Some(host.now().saturating_duration_since(started));
            stages.replace(timings.clone());
        }

        #[cfg(test)]
        let hooks = shared.hooks.exchange.lock().unwrap().clone();
        #[cfg(not(test))]
        let hooks = ExchangeHooks::default();
        let core = CoreJob {
            context,
            request: request
                .take()
                .expect("should hand the request to the core once"),
            cancellation,
            progress,
            started,
            shared,
            local,
            peer,
            header_form,
            teardown_watch,
        };
        let io = WatchedStream {
            io,
            mark: None,
            hold: None,
            _teardown: teardown_watch.guard(&shared.stats),
        };
        let mut result = core.run(io, H::SECURITY, &hooks, &mut sleep, None).await;
        match &mut result {
            Ok(response) => {
                add_dial_timings(&timings, &mut response.timings);
                if let BodyOutcome::Truncated { cause, .. } = &mut response.body {
                    add_dial_timings(&timings, &mut cause.timings);
                }
                response.connection.tls_version = tls_version;
            }
            Err(error) => add_dial_timings(&timings, &mut error.timings),
        }
        result
    }
}

/// Adds the dial's stages to the response-head and body offsets that the core measured.
fn add_dial_timings(dial: &Timings, core: &mut Timings) {
    core.queued = dial.queued;
    core.tcp_connect = dial.tcp_connect;
    core.tls_handshake = dial.tls_handshake;
}

/// The job of a `Security::Tunnel` target. It dials the proxy, attaches a plaintext HTTP/1.1
/// session to it, opens the tunnel with `connect_unique`, runs TLS over the tunnel, and runs the
/// core exchange inside, all under a single deadline sleep.
fn tunnel_dial_and_exchange(
    dial: Dial<'_>,
) -> impl Future<Output = Result<Response, ExplicitError>> {
    let Dial {
        context,
        target,
        request,
        deadline,
        cancellation,
        progress,
        shared,
        submitted_at,
        stages,
        header_form,
        teardown_watch,
    } = dial;
    async move {
        let Security::Tunnel(tunnel) = &target.security else {
            unreachable!("a tunnel route is chosen only for a tunnel target");
        };
        if !progress.try_start() {
            return Err(ExplicitError::new(
                Phase::Submit,
                ErrorKind::Cancelled,
                "the exchange was claimed before it started",
            ));
        }
        let host = context.host().clone();
        let started = host.now();
        let mut timings = Timings {
            queued: started.saturating_duration_since(submitted_at),
            ..Timings::default()
        };
        shared.stats.record_queued(timings.queued);
        stages.replace(timings.clone());
        let observed = || observe(context, cancellation, &host, deadline);
        let fail = |error: ExplicitError, timings: &Timings| {
            let error = if progress.finish() {
                error
            } else {
                ExplicitError {
                    kind: ErrorKind::Cancelled,
                    ..error
                }
            };
            Err(ExplicitError {
                timings: Box::new(timings.clone()),
                ..error.observed(observed())
            })
        };
        if cancellation.is_cancellation_requested() {
            let error = ExplicitError::new(
                Phase::Submit,
                ErrorKind::Cancelled,
                "cancelled before dialing",
            );
            return fail(error, &timings);
        }
        if host.now() >= deadline {
            return fail(ExplicitError::expired(Phase::Submit), &timings);
        }

        shared.hooks.before_connect();
        let mut sleep = host.sleep_until(deadline);
        shared.hooks.deadline_sleep_created();
        #[cfg(test)]
        let hooks = shared.hooks.exchange.lock().unwrap().clone();
        #[cfg(not(test))]
        let hooks = ExchangeHooks::default();

        let tcp = match staged(
            &host,
            deadline,
            cancellation,
            &mut sleep,
            TcpStream::connect(target.address),
        )
        .await
        {
            Staged::Done(Ok(tcp)) => tcp,
            Staged::Done(Err(error)) => {
                let error = ExplicitError::from_connect_io(&error).in_phase(Phase::ProxyConnect);
                return fail(error, &timings);
            }
            Staged::Expired => {
                return fail(ExplicitError::expired(Phase::ProxyConnect), &timings);
            }
            Staged::Cancelled => {
                let error = ExplicitError::new(
                    Phase::ProxyConnect,
                    ErrorKind::Cancelled,
                    "cancelled during the proxy connect",
                );
                return fail(error, &timings);
            }
        };
        if let Err(error) = tcp.set_nodelay(true) {
            let error = ExplicitError::from_connect_io(&error).in_phase(Phase::ProxyConnect);
            return fail(error, &timings);
        }
        #[cfg(test)]
        shared
            .hooks
            .nodelay
            .lock()
            .unwrap()
            .push(tcp.nodelay().unwrap_or(false));
        timings.tcp_connect = Some(host.now().saturating_duration_since(started));
        stages.replace(timings.clone());

        let setup = LegSetup {
            context,
            deadline,
            cancellation,
            stats: &shared.stats,
            exchange_hooks: &hooks,
            hooks: &shared.hooks,
            teardown_watch,
        };
        let ProxyLeg {
            guard,
            completion: proxy_completion,
            mark,
            local,
            peer,
        } = match attach_proxy_leg(&setup, tcp, target.address) {
            Ok(leg) => leg,
            Err(error) => return fail(error, &timings),
        };
        shared.hooks.proxy_attached().await;
        // A CONNECT with no time left would get a zero timeout, which means no timeout.
        if host.now() >= deadline {
            return fail(ExplicitError::expired(Phase::TunnelConnect), &timings);
        }
        let connect = match tunnel::connect(
            context,
            guard.session(),
            tunnel,
            remaining(&host, deadline),
            cancellation.clone(),
        ) {
            Ok(connect) => connect,
            Err(error) => {
                let error = ExplicitError::from_http(&error, Position::ProxyAttach, observed());
                return fail(error, &timings);
            }
        };
        let stream = match staged(&host, deadline, cancellation, &mut sleep, connect).await {
            Staged::Done(Ok(stream)) => stream,
            Staged::Done(Err(error)) => {
                return fail(ExplicitError::from_connect(&error, observed()), &timings);
            }
            Staged::Expired => {
                return fail(ExplicitError::expired(Phase::TunnelConnect), &timings);
            }
            Staged::Cancelled => {
                let error = ExplicitError::new(
                    Phase::TunnelConnect,
                    ErrorKind::Cancelled,
                    "cancelled during the CONNECT",
                );
                return fail(error, &timings);
            }
        };
        let (head, transport) = tunnel::established(context, stream);
        let head = Box::new(head);
        let with_head = |error: ExplicitError, timings: &Timings| {
            fail(
                ExplicitError {
                    connect_head: Some(head.clone()),
                    ..error
                },
                timings,
            )
        };

        let handshake = Box::pin(
            TlsConnector::from(Arc::clone(tunnel.tls_config()))
                .connect(tunnel.server_name().clone(), transport),
        );
        shared.hooks.before_handshake();
        let tls = match staged(&host, deadline, cancellation, &mut sleep, handshake).await {
            Staged::Done(Ok(tls)) => tls,
            Staged::Done(Err(error)) => {
                let error = ExplicitError::from_tunnel_tls_io(&error, observed());
                return with_head(error, &timings);
            }
            Staged::Expired => return with_head(ExplicitError::expired(Phase::Tls), &timings),
            Staged::Cancelled => {
                let error = ExplicitError::new(
                    Phase::Tls,
                    ErrorKind::Cancelled,
                    "cancelled during the tunnel's TLS handshake",
                );
                return with_head(error, &timings);
            }
        };
        let tls_version = tls.get_ref().1.protocol_version();
        timings.tls_handshake = Some(host.now().saturating_duration_since(started));
        stages.replace(timings.clone());

        let (handoff, receipt) = oneshot::channel();
        let cleanup = tunnel_cleanup(context, receipt, mark, proxy_completion, deadline);
        match context.register_cleanup_task(cleanup) {
            Ok(reservation) => {
                if let Ok(cleanup) = reservation.activate() {
                    cleanup.detach();
                }
            }
            Err(error) => {
                let error = ExplicitError::from_cleanup_registration(&error, Phase::TunnelConnect);
                return with_head(error, &timings);
            }
        }

        let core = CoreJob {
            context,
            request: request
                .take()
                .expect("should hand the request to the core once"),
            cancellation,
            progress,
            started,
            shared,
            local,
            peer,
            header_form,
            teardown_watch,
        };
        let link = TunnelLink { handoff, guard };
        let inner = WatchedStream {
            io: Box::pin(tls),
            mark: None,
            hold: shared.hooks.inner_shutdown_hold(),
            _teardown: teardown_watch.guard(&shared.stats),
        };
        let mut result = core
            .run(
                inner,
                TransportSecurity::Tls,
                &hooks,
                &mut sleep,
                Some(link),
            )
            .await;
        match &mut result {
            Ok(response) => {
                add_dial_timings(&timings, &mut response.timings);
                if let BodyOutcome::Truncated { cause, .. } = &mut response.body {
                    add_dial_timings(&timings, &mut cause.timings);
                }
                response.connection.tls_version = tls_version;
                response.connect_head = Some(head);
            }
            Err(error) => {
                add_dial_timings(&timings, &mut error.timings);
                error.connect_head.get_or_insert(head);
            }
        }
        result
    }
}

/// The time left until `deadline`, at least 1 ms, since a zero timeout means no timeout.
fn remaining(host: &Host, deadline: Instant) -> Duration {
    deadline
        .saturating_duration_since(host.now())
        .max(Duration::from_millis(1))
}

/// Exchange state that classifies a failure on the worker.
fn observe(
    context: &WorkerContext,
    cancellation: &CancellationToken,
    host: &Host,
    deadline: Instant,
) -> Observed {
    Observed {
        own_cancelled: cancellation.is_cancellation_requested(),
        worker_open: context.handle().state() == WorkerState::Open,
        deadline_passed: host.now() >= deadline,
    }
}

/// What a tunnel's proxy leg reads from its job.
pub(crate) struct LegSetup<'a> {
    pub(crate) context: &'a WorkerContext,
    pub(crate) deadline: Instant,
    pub(crate) cancellation: &'a CancellationToken,
    pub(crate) stats: &'a Arc<ExplicitStats>,
    pub(crate) exchange_hooks: &'a ExchangeHooks,
    pub(crate) hooks: &'a RuntimeHooks,
    pub(crate) teardown_watch: TeardownWatch,
}

/// A tunnel's attached proxy leg.
pub(crate) struct ProxyLeg {
    /// Owns the leg's session clone, and cancels it when dropped.
    pub(crate) guard: ProxyGuard,
    /// The leg's adopted stream's retirement.
    pub(crate) completion: AdoptedIoCompletion,
    /// Set once the leg's stream has shut down its write side.
    pub(crate) mark: ShutdownMark,
    /// The proxy stream's local address.
    pub(crate) local: Option<SocketAddr>,
    /// The proxy's address, as dialed.
    pub(crate) peer: Option<SocketAddr>,
}

/// Adopts the proxy stream dialed at `peer` and attaches a plaintext HTTP/1.1 session to it,
/// with the exchange's token and the time left. The stream's teardown is tracked by a cleanup
/// task that awaits its retirement or, for `TeardownWatch::StreamGuard`, by a guard that the
/// stream owns.
pub(crate) fn attach_proxy_leg(
    setup: &LegSetup<'_>,
    tcp: TcpStream,
    peer: SocketAddr,
) -> Result<ProxyLeg, ExplicitError> {
    let LegSetup {
        context,
        deadline,
        cancellation,
        stats,
        exchange_hooks,
        hooks,
        teardown_watch,
    } = *setup;
    let host = context.host();
    let observed = || observe(context, cancellation, host, deadline);
    let (local_endpoint, local) = dialed_endpoint(tcp.local_addr().ok());
    let (peer_endpoint, peer) = dialed_endpoint(Some(peer));

    let retirement = match teardown_watch {
        TeardownWatch::CleanupTask => {
            let (handoff, receipt) = oneshot::channel();
            let reservation = context
                .register_cleanup_task(retire(receipt, exchange_hooks.confirms_retirement()))
                .map_err(|error| {
                    ExplicitError::from_cleanup_registration(&error, Phase::ProxyConnect)
                })?;
            Some((handoff, reservation))
        }
        TeardownWatch::StreamGuard => None,
    };
    let mark = ShutdownMark::default();
    let io = WatchedStream {
        io: tcp,
        mark: Some(mark.clone()),
        hold: hooks.proxy_shutdown_hold(),
        _teardown: teardown_watch.guard(stats),
    };
    let limits = stream_limits();
    exchange_hooks.adopting(limits);
    let established = EstablishedIo::new(io, local_endpoint, peer_endpoint, None);
    let (transport, control) = match adopt_local_with_completion(context, established, limits) {
        Ok(adopted) => adopted,
        Err(error) => {
            if let Some((_, reservation)) = retirement {
                let _ = reservation.disarm();
            }
            return Err(ExplicitError::from_http(
                &error,
                Position::ProxyAttach,
                observed(),
            ));
        }
    };
    let completion = control.completion();
    if let Some((handoff, reservation)) = retirement {
        let _ = handoff.send(Handoff {
            completion: completion.clone(),
            teardown: PendingTeardown::new(Arc::clone(stats)),
            closed_at: Rc::new(Cell::new(None)),
            host: host.clone(),
        });
        if let Ok(cleanup) = reservation.activate() {
            cleanup.detach();
        }
    }

    let mut options = ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
    options.cancellation = cancellation.clone();
    options.session = session_params(host, deadline);
    exchange_hooks.attaching(&options.session);
    let session = match ClientSession::attach_transport(context, transport, options) {
        Ok(session) => session,
        Err(error) => {
            control.cancel();
            return Err(ExplicitError::from_http(
                &error,
                Position::ProxyAttach,
                observed(),
            ));
        }
    };
    Ok(ProxyLeg {
        guard: ProxyGuard::new(session, control),
        completion,
        mark,
        local,
        peer,
    })
}

/// Set by the proxy leg's stream at its first successful write shutdown. The adopted
/// transport shuts down only after it has flushed every queued byte, so by then the inner TLS
/// close_notify and the tunnel's FIN have been handed to the kernel.
#[derive(Clone, Default)]
pub(crate) struct ShutdownMark(Rc<MarkState>);

#[derive(Default)]
struct MarkState {
    marked: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl ShutdownMark {
    fn mark(&self) {
        self.0.marked.set(true);
        let waker = self.0.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Ready once the stream is marked.
    fn marked(&self) -> impl Future<Output = ()> + 'static {
        let state = Rc::clone(&self.0);
        poll_fn(move |context| {
            if state.marked.get() {
                return Poll::Ready(());
            }
            let replaced = state.waker.borrow_mut().replace(context.waker().clone());
            drop(replaced);
            Poll::Pending
        })
    }
}

/// A dialed stream. A tunnel's proxy leg marks its first successful write shutdown. Test builds
/// can hold either tunnel leg's write shutdown. The stream of a client with a teardown guard owns
/// that guard.
struct WatchedStream<Io> {
    io: Io,
    mark: Option<ShutdownMark>,
    /// Holds the write shutdown until it resolves.
    hold: Option<LocalFuture>,
    /// Drops after `io`, so that a panic in `io`'s destruction counts as unconfirmed.
    _teardown: Option<TeardownGuard>,
}

impl<Io: AsyncRead + Unpin> AsyncRead for WatchedStream<Io> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(context, buffer)
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for WatchedStream<Io> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(hold) = &mut this.hold {
            std::task::ready!(hold.as_mut().poll(context));
            this.hold = None;
        }
        let shutdown = std::task::ready!(Pin::new(&mut this.io).poll_shutdown(context));
        if shutdown.is_ok()
            && let Some(mark) = &this.mark
        {
            mark.mark();
        }
        Poll::Ready(shutdown)
    }
}

/// The tunnel cleanup task. It receives the proxy guard once the exchange has adopted the inner
/// stream, and then cancels the proxy session at the first of these:
///
/// - the proxy leg's write shutdown or retirement, once the inner stream has retired;
/// - the deadline;
/// - the worker's quiescing, since worker shutdown runs registered cleanup without a deadline.
///
/// It owns the guard's handoff, so dropping it, polled or not, cancels the proxy session.
///
/// Quiescing cancels immediately, even mid-exchange. That is safe only because a `HostedWorker`
/// quiesces solely in `begin_shutdown`, which drops every running command. A graceful
/// `RuntimeHandle::quiesce(grace)` would cut tunnels with exchanges that could still finish.
pub(crate) fn tunnel_cleanup(
    context: &WorkerContext,
    receipt: oneshot::Receiver<TunnelHandoff>,
    mark: ShutdownMark,
    proxy: AdoptedIoCompletion,
    deadline: Instant,
) -> impl Future<Output = ()> + 'static {
    let host = context.host().clone();
    let quiescing = context.quiescing();
    async move {
        let Ok(TunnelHandoff { inner, guard }) = receipt.await else {
            return;
        };
        let boundary = async move {
            let _ = inner.wait_retired().await;
            let shut_down = pin!(mark.marked());
            let retired = pin!(proxy.wait_retired());
            select(shut_down, retired).await;
        };
        let expiry = host.sleep_until(deadline);
        select(pin!(boundary), select(expiry, quiescing)).await;
        drop(guard);
    }
}

/// The core exchange's inputs other than the stream and the deadline sleep.
struct CoreJob<'a> {
    context: &'a WorkerContext,
    request: Request,
    cancellation: &'a CancellationToken,
    progress: &'a Arc<ExchangeProgress>,
    started: Instant,
    shared: &'a Arc<Shared>,
    local: Option<SocketAddr>,
    peer: Option<SocketAddr>,
    header_form: HeaderForm,
    teardown_watch: TeardownWatch,
}

impl<'a> CoreJob<'a> {
    fn run<Io>(
        self,
        io: Io,
        security: TransportSecurity,
        hooks: &'a ExchangeHooks,
        sleep: &'a mut LocalFuture,
        tunnel: Option<TunnelLink>,
    ) -> impl Future<Output = Result<Response, ExplicitError>> + 'a
    where
        Io: AsyncRead + AsyncWrite + 'static,
    {
        let (local_endpoint, local) = dialed_endpoint(self.local);
        let (peer_endpoint, peer) = dialed_endpoint(self.peer);
        run_dialed_exchange(
            ExchangeInput {
                worker: self.context.clone(),
                io: EstablishedIo::new(io, local_endpoint, peer_endpoint, None),
                security,
                request: self.request,
                cancellation: self.cancellation.clone(),
                progress: Arc::clone(self.progress),
                started: self.started,
                stats: Arc::clone(&self.shared.stats),
            },
            hooks,
            Dialed {
                sleep,
                local,
                peer,
                tunnel,
                header_form: self.header_form,
                teardown_watch: self.teardown_watch,
            },
        )
    }
}

/// The slot table, shared by runtime clones, clients, send futures, worker-side exchanges, and
/// restart threads. Only the owner's drop takes workers and restart threads out of it.
struct Shared {
    slots: Box<[Slot]>,
    next_client: AtomicUsize,
    /// The splitmix64 state behind clients' second candidates.
    candidate_state: AtomicU64,
    command_capacity: NonZeroUsize,
    max_sends_in_flight: NonZeroUsize,
    thread_name_prefix: &'static str,
    max_restarts_per_minute: u32,
    stats: Arc<ExplicitStats>,
    hooks: Arc<RuntimeHooks>,
}

impl Shared {
    fn restart_window(&self) -> Duration {
        self.hooks.restart_window().unwrap_or(RESTART_WINDOW)
    }

    /// A new client's candidate slots, chosen without reading any slot.
    fn candidates(&self) -> Candidates {
        let count = self.slots.len();
        if count == 1 {
            return Candidates {
                round_robin: 0,
                second: None,
            };
        }
        let round_robin = self.next_client.fetch_add(1, Ordering::Relaxed) % count;
        let offset = 1 + self.random_below(count - 1);
        Candidates {
            round_robin,
            second: Some((round_robin + offset) % count),
        }
    }

    /// The next value of the runtime's splitmix64 sequence, reduced below `bound`.
    fn random_below(&self, bound: usize) -> usize {
        const GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut mixed = self
            .candidate_state
            .fetch_add(GAMMA, Ordering::Relaxed)
            .wrapping_add(GAMMA);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        // Truncation keeps low bits, which the mix spreads as well as high ones.
        mixed as usize % bound
    }

    fn shut_down(&self) {
        let current = std::thread::current().id();
        let mut panic = None;
        for slot in self.slots.iter() {
            let mut inner = slot.lock();
            inner.shutdown = true;
            let signal = inner.raise();
            drop(inner);
            if let Some(payload) = signal.wake() {
                panic = first_panic(panic, payload);
            }
        }
        for slot in self.slots.iter() {
            let mut inner = slot.lock();
            let exempt = loop {
                let exempt = inner.exempt(current);
                if inner
                    .restart_steps
                    .iter()
                    .all(|held| exempt.contains(&held.holder))
                {
                    break exempt;
                }
                inner = slot
                    .restarts_done
                    .wait(inner)
                    .unwrap_or_else(PoisonError::into_inner);
            };
            let worker = inner.worker.take();
            inner.handle = None;
            let threads = std::mem::take(&mut inner.restart_threads);
            drop(inner);
            drop(worker);
            let (unjoined, joined): (Vec<_>, Vec<_>) = threads
                .into_iter()
                .partition(|thread| exempt.contains(&thread.thread().id()));
            drop(unjoined);
            joined.into_iter().for_each(join_restart_thread);
        }
        resume(panic);
    }
}

/// Resumes a panic that a publication signal caught, or disposes of it if this thread is
/// already unwinding.
fn resume(panic: Option<Box<dyn Any + Send>>) {
    let Some(panic) = panic else {
        return;
    };
    if std::thread::panicking() {
        discard_panic(panic);
    } else {
        resume_unwind(panic);
    }
}

/// Keeps the first of two caught panics and disposes of the later one, since its payload's
/// destructor may panic as well.
fn first_panic(
    first: Option<Box<dyn Any + Send>>,
    later: Box<dyn Any + Send>,
) -> Option<Box<dyn Any + Send>> {
    match first {
        Some(first) => {
            discard_panic(later);
            Some(first)
        }
        None => Some(later),
    }
}

/// Joins a restart thread and disposes of any panic it ended with.
fn join_restart_thread(thread: JoinHandle<()>) {
    if let Err(payload) = thread.join() {
        discard_panic(payload);
    }
}

fn spawn_worker(
    prefix: &str,
    index: usize,
    command_capacity: NonZeroUsize,
    hooks: &RuntimeHooks,
) -> Result<HostedWorker, ManagedWorkerError> {
    if hooks.worker_spawn_fails() {
        return Err(ManagedWorkerError::ThreadSpawn(
            "injected worker spawn failure".to_owned(),
        ));
    }
    HostedWorker::spawn(HostedWorkerOptions {
        command_capacity,
        thread_name: format!("{prefix}-{index}"),
        on_owner_thread_stop: None,
    })
}

/// The slots a new client chooses between.
#[derive(Clone, Copy)]
struct Candidates {
    round_robin: usize,
    /// A slot other than `round_robin`, when there is more than one slot.
    second: Option<usize>,
}

impl Candidates {
    /// The slot to pin the client to. Each candidate's state is read under its own slot lock,
    /// released before the next lock is taken. Its load is read from its atomic count of sends
    /// in flight.
    fn choose(self, shared: &Shared) -> usize {
        let Some(second) = self.second else {
            return self.round_robin;
        };
        let load = |index: usize| {
            let slot = &shared.slots[index];
            (slot.peek(shared) != SlotState::Capped)
                .then(|| slot.sends_in_flight.load(Ordering::Acquire))
        };
        match (load(self.round_robin), load(second)) {
            (Some(first), Some(other)) if other < first => second,
            (Some(_), _) => self.round_robin,
            (None, Some(_)) => second,
            (None, None) => {
                let count = shared.slots.len();
                (1..count)
                    .map(|step| (self.round_robin + step) % count)
                    .find(|index| shared.slots[*index].peek(shared) != SlotState::Capped)
                    .unwrap_or(self.round_robin)
            }
        }
    }
}

/// A slot as observed under its lock. `Restarting` and `Admitted` carry the slot's count of
/// failed restarts at that moment.
enum Observation {
    Open(WorkerHandle, u64),
    Restarting(u64),
    /// The published worker was not open, and this observation admitted its restart.
    Admitted(u64),
    Capped,
    /// The runtime is shutting down, or a restart already replaced the failed generation.
    Closed,
}

impl Observation {
    fn state(&self) -> SlotState {
        match self {
            Self::Open(..) => SlotState::Open,
            Self::Restarting(_) => SlotState::Restarting,
            Self::Admitted(_) | Self::Closed => SlotState::Closed,
            Self::Capped => SlotState::Capped,
        }
    }
}

/// The state of the restart that a send waits for, after a publication signal.
enum Waited {
    Restarting,
    Published,
    Failed,
    ShutDown,
}

struct Slot {
    index: usize,
    state: Mutex<SlotInner>,
    /// Signalled as each restart step completes. Only the last runtime drop waits on it.
    restarts_done: Condvar,
    sends_in_flight: AtomicUsize,
    consecutive_overloads: AtomicU64,
}

struct SlotInner {
    worker: Option<HostedWorker>,
    handle: Option<WorkerHandle>,
    generation: u64,
    restarting: bool,
    restart_starts: VecDeque<Instant>,
    /// Restarts still joining their old worker, from the swap until that join returns.
    joining: Vec<Joining>,
    /// The outstanding steps of the admitted restarts, two per restart until each completes.
    restart_steps: Vec<RestartStep>,
    next_restart: u64,
    restart_threads: Vec<JoinHandle<()>>,
    /// Admitted restarts that ended without publishing.
    restart_failures: u64,
    shutdown: bool,
    /// Publication signals raised: one when a restart publishes or fails, and one when the
    /// shutdown flag is set. A waiting send rechecks the slot under its lock once this changes.
    signals: u64,
    /// The wakers of sends waiting for the next signal, each under its wait's key.
    waiting: Vec<(u64, Waker)>,
    next_wait: u64,
}

impl SlotInner {
    /// The published worker's handle, if that worker is open.
    fn open_handle(&self) -> Option<&WorkerHandle> {
        // A handle can reject commands while it still reports `Open`.
        self.handle
            .as_ref()
            .filter(|handle| handle.state() == WorkerState::Open && handle.is_accepting())
    }

    /// Whether a restart bound blocks a restart at `now`. The bounds cover restarts started
    /// within the trailing window and restarts still joining their old worker. Drops the starts
    /// that have left the window.
    fn at_restart_cap(&mut self, shared: &Shared, now: Instant) -> bool {
        let window = shared.restart_window();
        while self
            .restart_starts
            .front()
            .is_some_and(|start| now.saturating_duration_since(*start) >= window)
        {
            self.restart_starts.pop_front();
        }
        let bound = usize::try_from(shared.max_restarts_per_minute).unwrap_or(usize::MAX);
        self.restart_starts.len() >= bound || self.joining.len() >= bound
    }

    /// Raises the publication signal and takes the waiting sends' wakers, which the caller
    /// wakes once it has released the slot lock.
    fn raise(&mut self) -> Signal {
        self.signals += 1;
        Signal(std::mem::take(&mut self.waiting))
    }

    fn complete(&mut self, restart: u64, step: Step) {
        self.restart_steps
            .retain(|held| (held.restart, held.step) != (restart, step));
    }

    /// Records that `holder`, the restart's own thread, holds its closure step from now on.
    fn hand_off(&mut self, restart: u64, holder: ThreadId) {
        self.restart_steps
            .iter_mut()
            .filter(|held| (held.restart, held.step) == (restart, Step::Closure))
            .for_each(|held| held.holder = holder);
    }

    /// The threads that a last drop running on `current` neither waits for nor joins: `current`
    /// itself, and each restart thread that is joining a worker whose owner thread is `current`.
    fn exempt(&self, current: ThreadId) -> Vec<ThreadId> {
        self.joining
            .iter()
            .filter(|joining| joining.owner == Some(current))
            .map(|joining| joining.restart)
            .chain([current])
            .collect()
    }
}

/// One outstanding step of an admitted restart, and the thread that holds it.
struct RestartStep {
    restart: u64,
    step: Step,
    holder: ThreadId,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Step {
    /// Held by the requester until it registers the restart thread's handle, or until the
    /// thread fails to spawn.
    Registration,
    /// Held by the requester, or by the restart thread once its closure starts or its handle is
    /// registered, until the closure ends or is dropped unrun.
    Closure,
}

/// A restart thread that is joining the worker it retired.
struct Joining {
    /// The retired worker's owner thread.
    owner: Option<ThreadId>,
    restart: ThreadId,
}

/// The wakers that a raised publication signal took from its slot.
#[must_use]
struct Signal(Vec<(u64, Waker)>);

impl Signal {
    /// Wakes each waiting send in its own unwind boundary, so that a panicking waker from the
    /// caller's executor cannot keep the signal from the other sends. Returns the first panic,
    /// for the raising step to resume after its bookkeeping.
    fn wake(self) -> Option<Box<dyn Any + Send>> {
        self.0.into_iter().fold(None, |panic, (_, waker)| {
            match catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                Ok(()) => panic,
                Err(payload) => first_panic(panic, payload),
            }
        })
    }
}

/// A send's registration for its slot's next publication signal, removed when dropped.
struct SignalWait<'a> {
    slot: &'a Slot,
    key: Option<u64>,
}

impl SignalWait<'_> {
    /// Ready once the slot's signal count differs from `seen`. Until then the slot holds a
    /// clone of `context`'s waker. The clone, and the drop of any waker it replaces, run without
    /// a slot lock held.
    fn poll(&mut self, seen: u64, context: &Context<'_>) -> Poll<()> {
        let waker = context.waker().clone();
        let mut inner = self.slot.lock();
        if inner.signals != seen {
            drop(inner);
            return Poll::Ready(());
        }
        let key = *self.key.get_or_insert_with(|| {
            inner.next_wait += 1;
            inner.next_wait
        });
        let replaced = match inner
            .waiting
            .iter_mut()
            .find(|(waiting, _)| *waiting == key)
        {
            Some((_, registered)) => Some(std::mem::replace(registered, waker)),
            None => {
                inner.waiting.push((key, waker));
                None
            }
        };
        drop(inner);
        drop(replaced);
        Poll::Pending
    }
}

impl Drop for SignalWait<'_> {
    fn drop(&mut self) {
        let Some(key) = self.key else {
            return;
        };
        let mut inner = self.slot.lock();
        let removed = inner
            .waiting
            .iter()
            .position(|(waiting, _)| *waiting == key)
            .map(|index| inner.waiting.swap_remove(index));
        drop(inner);
        drop(removed);
    }
}

impl Slot {
    fn new(index: usize, worker: HostedWorker) -> Self {
        let handle = worker.handle();
        Self {
            index,
            state: Mutex::new(SlotInner {
                worker: Some(worker),
                handle: Some(handle),
                generation: 0,
                restarting: false,
                restart_starts: VecDeque::new(),
                joining: Vec::new(),
                restart_steps: Vec::new(),
                next_restart: 0,
                restart_threads: Vec::new(),
                restart_failures: 0,
                shutdown: false,
                signals: 0,
                waiting: Vec::new(),
                next_wait: 0,
            }),
            restarts_done: Condvar::new(),
            sends_in_flight: AtomicUsize::new(0),
            consecutive_overloads: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SlotInner> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn published_handle(&self) -> Option<WorkerHandle> {
        self.lock().handle.clone()
    }

    /// The slot's state as `observe` would report it, read under the slot lock alone, without
    /// admitting a restart.
    fn peek(&self, shared: &Shared) -> SlotState {
        let mut inner = self.lock();
        if inner.shutdown {
            SlotState::Closed
        } else if inner.restarting {
            SlotState::Restarting
        } else if inner.open_handle().is_some() {
            SlotState::Open
        } else if inner.at_restart_cap(shared, Instant::now()) {
            SlotState::Capped
        } else {
            SlotState::Closed
        }
    }

    /// Reports the slot's state under its lock, and admits a restart when the published worker
    /// is not open, the bounds allow it, and `failed` (if any) is still the published
    /// generation. `seen` records the signals raised so far under the lock, so that a later
    /// signal wakes its wait.
    fn observe(
        &self,
        shared: &Arc<Shared>,
        failed: Option<u64>,
        seen: Option<&mut u64>,
    ) -> Observation {
        let mut inner = self.lock();
        if let Some(seen) = seen {
            *seen = inner.signals;
        }
        if inner.shutdown {
            return Observation::Closed;
        }
        if inner.restarting {
            return Observation::Restarting(inner.restart_failures);
        }
        if let Some(handle) = inner.open_handle() {
            return Observation::Open(handle.clone(), inner.generation);
        }
        if failed.is_some_and(|failed| failed != inner.generation) {
            return Observation::Closed;
        }
        let now = Instant::now();
        if inner.at_restart_cap(shared, now) {
            return Observation::Capped;
        }
        inner.restarting = true;
        inner.restart_starts.push_back(now);
        inner.next_restart += 1;
        let restart = inner.next_restart;
        let requester = std::thread::current().id();
        inner
            .restart_steps
            .extend([Step::Registration, Step::Closure].map(|step| RestartStep {
                restart,
                step,
                holder: requester,
            }));
        let failures = inner.restart_failures;
        let admission = RestartAdmission {
            shared: Arc::clone(shared),
            slot: self.index,
            restart,
            published: false,
        };
        drop(inner);
        self.spawn_restart(shared, admission);
        Observation::Admitted(failures)
    }

    /// Rechecks, under the slot lock, the restart that a send is waiting for, which started
    /// after `failures` failed restarts. Records the signals raised so far in `seen`, so that a
    /// later signal wakes the wait.
    fn waited(&self, failures: u64, seen: &mut u64) -> Waited {
        let inner = self.lock();
        *seen = inner.signals;
        if inner.shutdown {
            Waited::ShutDown
        } else if inner.restart_failures != failures {
            Waited::Failed
        } else if inner.restarting {
            Waited::Restarting
        } else {
            Waited::Published
        }
    }

    /// Spawns the admitted restart's thread with no slot lock held and registers its handle.
    fn spawn_restart(&self, shared: &Arc<Shared>, admission: RestartAdmission) {
        let mut registration = Registration {
            slot: self,
            restart: admission.restart,
            thread: None,
        };
        shared.hooks.before_restart_spawn();
        if shared.hooks.restart_thread_spawn_fails() {
            drop(admission);
            return;
        }
        let restart_shared = Arc::clone(shared);
        registration.thread = std::thread::Builder::new()
            .name(format!(
                "{}-restart-{}",
                shared.thread_name_prefix, self.index
            ))
            .spawn(move || {
                // The last runtime drop can leave a restart thread unjoined, and std aborts the
                // process when an unjoined thread's panic payload panics as it is dropped.
                if let Err(payload) =
                    catch_unwind(AssertUnwindSafe(|| restart(restart_shared, admission)))
                {
                    discard_panic(payload);
                }
            })
            .ok();
        shared.hooks.before_registration();
    }
}

/// Completes a restart's registration step: registers its thread, if it spawned, and hands
/// that thread the closure step.
struct Registration<'a> {
    slot: &'a Slot,
    restart: u64,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut inner = self.slot.lock();
        inner.complete(self.restart, Step::Registration);
        if let Some(thread) = self.thread.take() {
            inner.hand_off(self.restart, thread.thread().id());
            inner.restart_threads.push(thread);
        }
        drop(inner);
        self.slot.restarts_done.notify_all();
    }
}

/// Ends an admitted restart's closure step when dropped. If the restart did not publish, it
/// also ends the restarting state as a failure.
struct RestartAdmission {
    shared: Arc<Shared>,
    slot: usize,
    restart: u64,
    published: bool,
}

impl Drop for RestartAdmission {
    fn drop(&mut self) {
        let slot = &self.shared.slots[self.slot];
        let mut inner = slot.lock();
        let signal = if self.published {
            None
        } else {
            inner.restarting = false;
            inner.restart_failures += 1;
            self.shared.stats.count_failed_restart();
            Some(inner.raise())
        };
        inner.complete(self.restart, Step::Closure);
        drop(inner);
        let panic = signal.and_then(Signal::wake);
        slot.restarts_done.notify_all();
        resume(panic);
    }
}

/// Runs on the slot's restart thread: joins finished restart threads, spawns and publishes the
/// next worker, and then joins the old one with no slot lock held.
fn restart(shared: Arc<Shared>, mut admission: RestartAdmission) {
    let slot = &shared.slots[admission.slot];
    let current = std::thread::current().id();
    let finished: Vec<_> = {
        let mut inner = slot.lock();
        inner.hand_off(admission.restart, current);
        let (finished, running) = std::mem::take(&mut inner.restart_threads)
            .into_iter()
            .partition(|thread| thread.is_finished() && thread.thread().id() != current);
        inner.restart_threads = running;
        finished
    };
    finished.into_iter().for_each(join_restart_thread);

    shared.hooks.before_worker_spawn();
    let Ok(worker) = spawn_worker(
        shared.thread_name_prefix,
        slot.index,
        shared.command_capacity,
        &shared.hooks,
    ) else {
        return;
    };
    let mut inner = slot.lock();
    if inner.shutdown {
        drop(inner);
        drop(worker);
        return;
    }
    let handle = worker.handle();
    let old = inner.worker.replace(worker);
    let retired = inner
        .handle
        .replace(handle)
        .map(|retired| retired.owner_thread());
    inner.generation += 1;
    inner.restarting = false;
    inner.joining.push(Joining {
        owner: retired,
        restart: current,
    });
    let signal = inner.raise();
    admission.published = true;
    shared.stats.count_restart();
    shared.stats.restart_joining();
    drop(inner);
    let panic = signal.wake();

    #[cfg(test)]
    shared
        .hooks
        .join_threads
        .lock()
        .unwrap()
        .push(std::thread::current().name().unwrap_or_default().to_owned());
    drop(old);
    slot.lock()
        .joining
        .retain(|joining| joining.restart != current);
    shared.stats.restart_joined();
    resume(panic);
}

/// Crate-internal hooks that tests use to hold, fail, and observe the runtime.
#[derive(Default)]
pub(crate) struct RuntimeHooks {
    #[cfg(test)]
    pub(crate) exchange: Mutex<ExchangeHooks>,
    #[cfg(test)]
    pub(crate) spawn_pause: Mutex<Option<TestPause>>,
    #[cfg(test)]
    pub(crate) worker_spawn_failures: Mutex<VecDeque<bool>>,
    #[cfg(test)]
    pub(crate) fail_next_thread_spawns: AtomicUsize,
    #[cfg(test)]
    pub(crate) requester_pause: Mutex<Option<TestPause>>,
    #[cfg(test)]
    pub(crate) registration_pause: Mutex<Option<TestPause>>,
    #[cfg(test)]
    pub(crate) send_wait_pause: Mutex<Option<TestPause>>,
    #[cfg(test)]
    pub(crate) connect_pause: Mutex<Option<TestPause>>,
    #[cfg(test)]
    pub(crate) handshake_pause: Mutex<Option<TestPause>>,
    /// Holds a tunnel exchange after its proxy attach.
    #[cfg(test)]
    pub(crate) proxy_attached_pause: Mutex<Option<TestPause>>,
    /// Holds the write shutdown of a tunnel's proxy stream.
    #[cfg(test)]
    pub(crate) proxy_shutdown_pause: Mutex<Option<TestPause>>,
    /// Holds the write shutdown of a tunnel's inner stream, and so its retirement.
    #[cfg(test)]
    pub(crate) inner_shutdown_pause: Mutex<Option<TestPause>>,
    #[cfg(test)]
    pub(crate) cap_window: Mutex<Duration>,
    #[cfg(test)]
    pub(crate) join_threads: Mutex<Vec<String>>,
    #[cfg(test)]
    pub(crate) dialed_tls_configs: Mutex<Vec<usize>>,
    #[cfg(test)]
    pub(crate) nodelay: Mutex<Vec<bool>>,
    #[cfg(test)]
    pub(crate) job_future_sizes: Mutex<Vec<usize>>,
    /// The phase, kind, reached-wire flag, and detail of each job that failed.
    #[cfg(test)]
    pub(crate) job_errors: Mutex<Vec<(Phase, ErrorKind, bool, Box<str>)>>,
    /// Caller deadline sleeps that sends armed.
    #[cfg(test)]
    pub(crate) caller_sleeps: AtomicUsize,
}

/// The size of the job future that `factory` hands to `call_local`.
#[cfg(test)]
fn output_size<Factory, Work>(_: &Factory) -> usize
where
    Factory: FnOnce(WorkerContext) -> Work,
{
    size_of::<Work>()
}

/// Hook points, which only test builds act on.
impl RuntimeHooks {
    fn worker_spawn_fails(&self) -> bool {
        #[cfg(test)]
        if let Some(fail) = self.worker_spawn_failures.lock().unwrap().pop_front() {
            return fail;
        }
        false
    }

    fn restart_thread_spawn_fails(&self) -> bool {
        #[cfg(test)]
        {
            let mut count = self.fail_next_thread_spawns.load(Ordering::Acquire);
            while let Some(remaining) = count.checked_sub(1) {
                match self.fail_next_thread_spawns.compare_exchange_weak(
                    count,
                    remaining,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(actual) => count = actual,
                }
            }
        }
        false
    }

    fn before_restart_spawn(&self) {
        #[cfg(test)]
        {
            let pause = self.requester_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.pause_blocking();
            }
        }
    }

    fn before_registration(&self) {
        #[cfg(test)]
        {
            let pause = self.registration_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.pause_blocking();
            }
        }
    }

    fn before_worker_spawn(&self) {
        #[cfg(test)]
        {
            let pause = self.spawn_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.pause_blocking();
            }
        }
    }

    fn before_connect(&self) {
        #[cfg(test)]
        {
            let pause = self.connect_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.pause_blocking();
            }
        }
    }

    fn before_handshake(&self) {
        #[cfg(test)]
        {
            let pause = self.handshake_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.pause_blocking();
            }
        }
    }

    async fn proxy_attached(&self) {
        #[cfg(test)]
        {
            let pause = self.proxy_attached_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.pause().await;
            }
        }
    }

    fn proxy_shutdown_hold(&self) -> Option<LocalFuture> {
        #[cfg(test)]
        if let Some(pause) = self.proxy_shutdown_pause.lock().unwrap().clone() {
            return Some(Box::pin(async move { pause.pause().await }));
        }
        None
    }

    fn inner_shutdown_hold(&self) -> Option<LocalFuture> {
        #[cfg(test)]
        if let Some(pause) = self.inner_shutdown_pause.lock().unwrap().clone() {
            return Some(Box::pin(async move { pause.pause().await }));
        }
        None
    }

    async fn before_publication_wait(&self) {
        #[cfg(test)]
        {
            let pause = self.send_wait_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.pause().await;
            }
        }
    }

    fn request_validated(&self) {
        #[cfg(test)]
        self.exchange.lock().unwrap().request_validated();
    }

    fn deadline_sleep_created(&self) {
        #[cfg(test)]
        self.exchange.lock().unwrap().deadline_sleep_created();
    }

    fn caller_sleep_armed(&self) {
        #[cfg(test)]
        self.caller_sleeps.fetch_add(1, Ordering::SeqCst);
    }

    fn job_finished(&self, result: &Result<Response, ExplicitError>) {
        #[cfg(test)]
        if let Err(error) = result {
            self.job_errors.lock().unwrap().push((
                error.phase,
                error.kind,
                error.reached_wire,
                error.detail.clone(),
            ));
        }
        #[cfg(not(test))]
        let _ = result;
    }

    fn restart_window(&self) -> Option<Duration> {
        #[cfg(test)]
        {
            let window = *self.cap_window.lock().unwrap();
            if !window.is_zero() {
                return Some(window);
            }
        }
        None
    }
}

#[cfg(test)]
impl ExplicitRuntime {
    pub(crate) fn start_with_hooks(
        config: ExplicitRuntimeConfig,
        hooks: Arc<RuntimeHooks>,
    ) -> Result<Self, ExplicitError> {
        Self::create(config, hooks)
    }

    pub(crate) fn hooks(&self) -> &Arc<RuntimeHooks> {
        &self.owner.shared.hooks
    }

    pub(crate) fn set_exchange_hooks(&self, hooks: ExchangeHooks) {
        *self.owner.shared.hooks.exchange.lock().unwrap() = hooks;
    }

    pub(crate) fn worker_handle(&self, slot: usize) -> Option<WorkerHandle> {
        self.owner.shared.slots[slot].published_handle()
    }

    pub(crate) fn generation(&self, slot: usize) -> u64 {
        self.owner.shared.slots[slot].lock().generation
    }

    /// The slot's state, read without admitting a restart.
    pub(crate) fn slot_state(&self, slot: usize) -> SlotState {
        let shared = &self.owner.shared;
        shared.slots[slot].peek(shared)
    }

    /// Reports `SubmitError::Closed` from a handle of `generation`, as a late send would.
    pub(crate) fn report_closed_generation(&self, slot: usize, generation: u64) {
        let shared = &self.owner.shared;
        shared.slots[slot].observe(shared, Some(generation), None);
    }
}

#[cfg(test)]
impl ExplicitClient {
    pub(crate) fn slot(&self) -> usize {
        self.slot
    }

    /// The client's round-robin slot and, with more than one slot, its second candidate.
    pub(crate) fn candidates(&self) -> (usize, Option<usize>) {
        self.candidates
    }
}
