/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! DNS, identity validation, and fixed-worker TCP/TLS session connection.

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use roxygen_codec::CodecSetting;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_filter::HttpSourceFilterHooks;
use roxygen_core::source_holder::HolderHeaderRead;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http2::hpack::HeaderCodecStats;
use roxygen_http2::hpack::HeaderIndexingStrategy;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::CancellableWait;
use roxygen_runtime::wait::TimedBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerId;
use roxygen_runtime::worker::WorkerQuiescing;
use roxygen_runtime::worker::WorkerState;
use roxygen_session::experimental::PingProbeConfig;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::http2::rate_limit::RateLimitSample;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::session::OutgoingTransactionActivity;
use roxygen_session::session::RequestHeadGate;
use roxygen_session::session::RequestReservation;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexHandle;
use roxygen_session::uniplex::UniplexProtocolConfig;
use roxygen_session::uniplex::UniplexResponse;
use roxygen_session::uniplex::UniplexRunOwner;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexSession;
use roxygen_session::uniplex::UniplexTransport;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::ByteStreamTransport;
use roxygen_transport::byte_stream::OwnedByteStream;
use roxygen_transport::byte_stream::TransportRetirement;
use roxygen_transport::tcp::TcpConnectOptions;
use roxygen_transport::tcp::TcpTransport;
use roxygen_transport::tls::TlsCertificateVerifier;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::TlsServerNameLogFn;
use roxygen_transport::tls::TlsServerNameValidation;
pub use roxygen_transport::tls::TlsServerNameVerification as CertVerifyResult;
use roxygen_transport::tls::TlsTransport;

pub type LocalClientFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

pub trait TlsConnectionStats: Send + Sync + 'static {
    fn on_connection(&self, full_handshake: bool);

    fn on_connection_error(&self, verification_error: bool);
}

/// Protocol-neutral client session statistics.
pub trait ClientSessionStats: Http2SessionStats {}

impl<T> ClientSessionStats for T where T: Http2SessionStats {}

struct Http2ClientSessionStats(Rc<dyn ClientSessionStats>);

impl Http2SessionStats for Http2ClientSessionStats {
    fn record_transaction_opened(&self) {
        self.0.record_transaction_opened();
    }

    fn record_transaction_closed(&self) {
        self.0.record_transaction_closed();
    }

    fn record_session_reused(&self) {
        self.0.record_session_reused();
    }

    fn record_transactions_served(&self, transactions: u64) {
        self.0.record_transactions_served(transactions);
    }

    fn record_connection_idle_time(&self, idle: Duration) {
        self.0.record_connection_idle_time(idle);
    }

    fn record_transaction_stalled(&self) {
        self.0.record_transaction_stalled();
    }

    fn record_session_stalled(&self) {
        self.0.record_session_stalled();
    }

    fn record_buffered_read_bytes(&self, delta: i64) {
        self.0.record_buffered_read_bytes(delta);
    }

    fn record_buffered_write_bytes(&self, delta: i64) {
        self.0.record_buffered_write_bytes(delta);
    }

    fn record_content_length_mismatch(&self) {
        self.0.record_content_length_mismatch();
    }

    fn record_ingress_get_body(&self) {
        self.0.record_ingress_get_body();
    }

    fn record_ping_timeout(&self) {
        self.0.record_ping_timeout();
    }

    fn record_rate_limit(&self, sample: RateLimitSample) {
        self.0.record_rate_limit(sample);
    }

    fn record_read_loop_limit(&self) {
        self.0.record_read_loop_limit();
    }
}

const DEFAULT_STREAM_FLOW_CONTROL: u32 = 1 << 21;
const DEFAULT_CONNECTION_FLOW_CONTROL: u32 = 1 << 25;

pub const fn get_fallback_ca_path() -> Option<&'static str> {
    roxygen_transport::tls::get_fallback_ca_path()
}

static DEFAULT_CA_PATHS: OnceLock<RwLock<Vec<PathBuf>>> = OnceLock::new();
static DEFAULT_CERT_VERIFIER: OnceLock<RwLock<Option<Arc<dyn TlsCertificateVerifier>>>> =
    OnceLock::new();

fn default_ca_paths_slot() -> &'static RwLock<Vec<PathBuf>> {
    DEFAULT_CA_PATHS.get_or_init(|| RwLock::new(vec![PathBuf::from("/etc/pki/tls/cert.pem")]))
}

pub fn set_default_ca_paths(paths: Vec<PathBuf>) {
    *default_ca_paths_slot()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = paths;
}

pub fn get_default_ca_paths() -> Vec<PathBuf> {
    default_ca_paths_slot()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn default_cert_verifier_slot() -> &'static RwLock<Option<Arc<dyn TlsCertificateVerifier>>> {
    DEFAULT_CERT_VERIFIER.get_or_init(|| RwLock::new(None))
}

pub fn set_default_cert_verifier(verifier: Option<Arc<dyn TlsCertificateVerifier>>) {
    let old = {
        let mut slot = default_cert_verifier_slot()
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::replace(&mut *slot, verifier)
    };
    let _ = catch_unwind(AssertUnwindSafe(|| drop(old)));
}

pub fn get_default_cert_verifier() -> Option<Arc<dyn TlsCertificateVerifier>> {
    default_cert_verifier_slot()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub(crate) fn client_error(code: HttpErrorCode, message: impl AsRef<[u8]>) -> HttpError {
    HttpError::with_message(code, message)
}

/// Asynchronous hostname resolver used by the worker-local client.
pub trait DnsResolver: 'static {
    fn resolve(
        &self,
        hostname: Box<str>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>>;
}

/// Owner-local callbacks for one logical DNS resolution.
pub trait DnsStatsCollector: 'static {
    fn record_success(&self, _addresses: &[SocketAddress], _latency: Duration) {}

    fn record_error(&self, _error: &HttpError, _latency: Duration) {}

    fn record_query_result(&self, _response_code: u8) {}

    fn record_cache_hit(&self) {}

    fn record_cache_miss(&self) {}

    fn record_cache_partial_miss(&self) {}

    fn record_stale_cache_hit(&self) {}

    fn record_active_queries(&self, _count: usize) {}

    fn record_open_sockets(&self, _count: usize) {}

    fn record_socket_open(&self) {}

    fn record_socket_close(&self) {}
}

impl<F> DnsResolver for F
where
    F: Fn(
            Box<str>,
            Duration,
            CancellationToken,
        ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>>
        + 'static,
{
    fn resolve(
        &self,
        hostname: Box<str>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>> {
        self(hostname, timeout, cancellation)
    }
}

struct TokioDnsResolver;

impl DnsResolver for TokioDnsResolver {
    fn resolve(
        &self,
        hostname: Box<str>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>> {
        Box::pin(async move {
            if cancellation.is_cancellation_requested() {
                return Err(client_error(
                    HttpErrorCode::CORO_CANCELLED,
                    b"DNS resolution cancelled",
                ));
            }
            let lookup = tokio::net::lookup_host((hostname.as_ref(), 0));
            let timeout_baton = TimedBaton::new(Some(timeout));
            let wait = timeout_baton.wait(cancellation.clone());
            tokio::pin!(lookup);
            tokio::pin!(wait);
            let resolved = tokio::select! {
                result = &mut lookup => result.map_err(|error| {
                    client_error(HttpErrorCode::CONNECT_ERROR, error.to_string().as_bytes())
                })?,
                status = &mut wait => {
                    return Err(match status {
                        WaitStatus::Cancelled => client_error(
                            HttpErrorCode::CORO_CANCELLED,
                            b"DNS resolution cancelled",
                        ),
                        WaitStatus::TimedOut => client_error(
                            HttpErrorCode::READ_TIMEOUT,
                            b"DNS resolution timed out",
                        ),
                        WaitStatus::Signalled | WaitStatus::NotReady => client_error(
                            HttpErrorCode::INTERNAL_ERROR,
                            b"DNS timer completed without a terminal outcome",
                        ),
                    });
                }
            };
            if cancellation.is_cancellation_requested() {
                return Err(client_error(
                    HttpErrorCode::CORO_CANCELLED,
                    b"DNS resolution cancelled",
                ));
            }
            if timeout_baton.status() == WaitStatus::TimedOut {
                return Err(client_error(
                    HttpErrorCode::READ_TIMEOUT,
                    b"DNS resolution timed out",
                ));
            }
            let addresses: Vec<_> = resolved
                .map(|address| SocketAddress::new(address.ip().to_string(), 0))
                .collect();
            if addresses.is_empty() {
                return Err(client_error(
                    HttpErrorCode::CONNECT_ERROR,
                    b"DNS resolution returned no addresses",
                ));
            }
            Ok(addresses)
        })
    }
}

enum WorkerResolver {
    Default,
    Custom(Rc<dyn DnsResolver>),
    Disabled,
}

struct WorkerResolverEntry {
    resolver: WorkerResolver,
    active: Rc<Cell<bool>>,
    generation: Rc<CancellationSource>,
    registered_stats: Option<Rc<dyn DnsStatsCollector>>,
    default_stats: Option<Rc<dyn DnsStatsCollector>>,
    default_initialized: bool,
}

thread_local! {
    static WORKER_RESOLVERS: RefCell<HashMap<WorkerId, WorkerResolverEntry>> = RefCell::new(HashMap::new());
}

/// Worker-local coroutine DNS facade.
pub struct CoroDnsResolver;

impl CoroDnsResolver {
    pub fn sort_addresses_with_source(
        addresses: Vec<SocketAddress>,
        source: IpAddr,
    ) -> Vec<SocketAddress> {
        let mut candidates: Vec<_> = addresses
            .into_iter()
            .enumerate()
            .map(|(original_index, address)| Rfc6724Candidate {
                destination: std::str::from_utf8(address.host())
                    .ok()
                    .and_then(|address| address.parse().ok()),
                address,
                source: Some(source),
                original_index,
            })
            .collect();
        candidates.sort_by_key(Rfc6724Candidate::sort_key);
        candidates
            .into_iter()
            .map(|candidate| candidate.address)
            .collect()
    }

    pub fn set_worker_resolver(
        context: &WorkerContext,
        resolver: Rc<dyn DnsResolver>,
    ) -> Result<(), HttpError> {
        replace_worker_resolver(context, WorkerResolver::Custom(resolver))
    }

    pub fn restore_default_worker_resolver(context: &WorkerContext) -> Result<(), HttpError> {
        replace_worker_resolver(context, WorkerResolver::Default)
    }

    pub fn disable_worker_resolver(context: &WorkerContext) -> Result<(), HttpError> {
        replace_worker_resolver(context, WorkerResolver::Disabled)
    }

    pub fn emplace_worker_stats_collector(
        context: &WorkerContext,
        stats: Rc<dyn DnsStatsCollector>,
    ) -> Result<bool, HttpError> {
        if !context.is_owner_thread() || context.handle().state() != WorkerState::Open {
            return Err(client_error(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"DNS stats mutation requires its open worker owner thread",
            ));
        }
        ensure_worker_resolver_installed(context)?;
        let inserted = WORKER_RESOLVERS.with(|slots| {
            let mut slots = slots.borrow_mut();
            let entry = slots
                .get_mut(&context.worker_id())
                .expect("worker resolver is installed before stats mutation");
            if entry.registered_stats.is_some() {
                false
            } else {
                entry.registered_stats = Some(stats);
                true
            }
        });
        Ok(inserted)
    }

    pub fn resolve_host_all(
        context: &WorkerContext,
        hostname: impl Into<Box<str>>,
        timeout: Duration,
        cancellation: CancellationToken,
        resolver: Option<Rc<dyn DnsResolver>>,
    ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>> {
        if !context.is_owner_thread() || context.handle().state() != WorkerState::Open {
            return Box::pin(std::future::ready(Err(client_error(
                HttpErrorCode::DROPPED,
                b"DNS resolver requires an open owner worker",
            ))));
        }
        let worker = context.handle().clone();
        let (resolver, active, generation, stats) = if let Some(resolver) = resolver {
            (
                Some(resolver),
                Rc::new(Cell::new(true)),
                CancellationToken::never(),
                None,
            )
        } else {
            let inherited = acquire_worker_resolver(context);
            match inherited {
                Ok((resolver_state, active, generation, stats)) => (
                    match resolver_state {
                        WorkerResolver::Default => {
                            Some(Rc::new(TokioDnsResolver) as Rc<dyn DnsResolver>)
                        }
                        WorkerResolver::Custom(resolver) => Some(resolver),
                        WorkerResolver::Disabled => None,
                    },
                    active,
                    generation,
                    stats,
                ),
                Err(error) => return Box::pin(std::future::ready(Err(error))),
            }
        };
        let hostname = hostname.into();
        let Some(resolver) = resolver else {
            return Box::pin(std::future::ready(Err(client_error(
                HttpErrorCode::DROPPED,
                b"DNS resolver is shut down",
            ))));
        };
        Box::pin(async move {
            let started = Instant::now();
            let resolution = resolver.resolve(hostname, timeout, cancellation.clone());
            let generation_baton = TimedBaton::new(None);
            let generation_wait = generation_baton.wait(generation.clone());
            tokio::pin!(resolution);
            tokio::pin!(generation_wait);
            let resolved = tokio::select! {
                result = &mut resolution => result,
                status = &mut generation_wait => {
                    debug_assert_eq!(status, WaitStatus::Cancelled);
                    return Err(client_error(
                        HttpErrorCode::DROPPED,
                        b"DNS resolver was replaced",
                    ));
                }
            };
            if generation.is_cancellation_requested()
                || !active.get()
                || worker.state() != WorkerState::Open
            {
                return Err(client_error(
                    HttpErrorCode::DROPPED,
                    b"DNS resolver is shut down",
                ));
            }
            let addresses = match resolved {
                Ok(addresses) => {
                    notify_dns_success(stats.as_ref(), &addresses, started.elapsed());
                    addresses
                }
                Err(error) => {
                    notify_dns_error(stats.as_ref(), &error, started.elapsed());
                    return Err(error);
                }
            };
            if !active.get() || worker.state() != WorkerState::Open {
                return Err(client_error(
                    HttpErrorCode::DROPPED,
                    b"DNS resolver is shut down",
                ));
            }
            if addresses.is_empty() {
                return Err(client_error(
                    HttpErrorCode::CONNECT_ERROR,
                    b"DNS resolution returned no addresses",
                ));
            }
            let addresses = sort_rfc6724(addresses, cancellation.clone()).await?;
            if cancellation.is_cancellation_requested() {
                return Err(client_error(
                    HttpErrorCode::CORO_CANCELLED,
                    b"DNS resolution cancelled",
                ));
            }
            if !active.get() || worker.state() != WorkerState::Open {
                return Err(client_error(
                    HttpErrorCode::DROPPED,
                    b"DNS resolver is shut down",
                ));
            }
            Ok(addresses)
        })
    }

    pub fn resolve_host(
        context: &WorkerContext,
        hostname: impl Into<Box<str>>,
        timeout: Duration,
        cancellation: CancellationToken,
        resolver: Option<Rc<dyn DnsResolver>>,
    ) -> LocalClientFuture<Result<ResolvedHost, HttpError>> {
        let resolution = Self::resolve_host_all(context, hostname, timeout, cancellation, resolver);
        Box::pin(async move {
            let mut addresses = resolution.await?.into_iter();
            let primary = addresses
                .next()
                .expect("nonempty DNS results retain a primary address");
            let primary_is_ipv6 = primary.is_ipv6();
            let fallback = addresses.find(|address| address.is_ipv6() != primary_is_ipv6);
            Ok(ResolvedHost { primary, fallback })
        })
    }
}

fn replace_worker_resolver(
    context: &WorkerContext,
    resolver: WorkerResolver,
) -> Result<(), HttpError> {
    if !context.is_owner_thread() || context.handle().state() != WorkerState::Open {
        return Err(client_error(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"DNS resolver mutation requires its open worker owner thread",
        ));
    }
    let worker = context.worker_id();
    let previous = WORKER_RESOLVERS.with(|slots| {
        let mut slots = slots.borrow_mut();
        let previous = slots.remove(&worker);
        if let Some(previous) = &previous {
            previous.active.set(false);
        }
        let registered_stats = previous
            .as_ref()
            .and_then(|previous| previous.registered_stats.clone());
        slots.insert(
            worker,
            WorkerResolverEntry {
                resolver,
                active: Rc::new(Cell::new(true)),
                generation: Rc::new(CancellationSource::new()),
                registered_stats,
                default_stats: None,
                default_initialized: false,
            },
        );
        previous
    });
    let register_cleanup = previous.is_none();
    if let Some(previous) = &previous {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            previous.generation.request_cancellation()
        }));
    }
    drop_resolver_entry(previous);
    if register_cleanup {
        let _cleanup = context
            .register_cleanup(move || {
                let removed = WORKER_RESOLVERS.with(|slots| slots.borrow_mut().remove(&worker));
                if let Some(entry) = &removed {
                    entry.active.set(false);
                    let _ =
                        catch_unwind(AssertUnwindSafe(|| entry.generation.request_cancellation()));
                }
                drop_resolver_entry(removed);
            })
            .map_err(|error| {
                let removed = WORKER_RESOLVERS.with(|slots| slots.borrow_mut().remove(&worker));
                if let Some(entry) = &removed {
                    entry.active.set(false);
                    let _ =
                        catch_unwind(AssertUnwindSafe(|| entry.generation.request_cancellation()));
                }
                drop_resolver_entry(removed);
                client_error(HttpErrorCode::DROPPED, error.to_string())
            })?;
    }
    Ok(())
}

fn drop_resolver_entry(entry: Option<WorkerResolverEntry>) {
    let _ = catch_unwind(AssertUnwindSafe(|| drop(entry)));
}

fn ensure_worker_resolver_installed(context: &WorkerContext) -> Result<(), HttpError> {
    let worker = context.worker_id();
    let exists = WORKER_RESOLVERS.with(|slots| slots.borrow().contains_key(&worker));
    if !exists {
        replace_worker_resolver(context, WorkerResolver::Default)?;
    }
    Ok(())
}

fn acquire_worker_resolver(
    context: &WorkerContext,
) -> Result<
    (
        WorkerResolver,
        Rc<Cell<bool>>,
        CancellationToken,
        Option<Rc<dyn DnsStatsCollector>>,
    ),
    HttpError,
> {
    let worker = context.worker_id();
    ensure_worker_resolver_installed(context)?;
    WORKER_RESOLVERS.with(|slots| {
        let mut slots = slots.borrow_mut();
        let entry = slots
            .get_mut(&worker)
            .expect("worker resolver is installed before lookup");
        let resolver = match &entry.resolver {
            WorkerResolver::Default => {
                if !entry.default_initialized {
                    entry.default_stats = entry.registered_stats.clone();
                    entry.default_initialized = true;
                }
                WorkerResolver::Default
            }
            WorkerResolver::Custom(resolver) => WorkerResolver::Custom(Rc::clone(resolver)),
            WorkerResolver::Disabled => WorkerResolver::Disabled,
        };
        let stats = matches!(resolver, WorkerResolver::Default)
            .then(|| entry.default_stats.clone())
            .flatten();
        Ok((
            resolver,
            Rc::clone(&entry.active),
            entry.generation.token(),
            stats,
        ))
    })
}

fn notify_dns_success(
    stats: Option<&Rc<dyn DnsStatsCollector>>,
    addresses: &[SocketAddress],
    latency: Duration,
) {
    if let Some(stats) = stats {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            stats.record_success(addresses, latency)
        }));
    }
}

fn notify_dns_error(
    stats: Option<&Rc<dyn DnsStatsCollector>>,
    error: &HttpError,
    latency: Duration,
) {
    if let Some(stats) = stats {
        let _ = catch_unwind(AssertUnwindSafe(|| stats.record_error(error, latency)));
    }
}

fn rfc6724_precedence(address: IpAddr) -> u8 {
    match address {
        IpAddr::V4(_) => 35,
        IpAddr::V6(address) if address.is_loopback() => 50,
        IpAddr::V6(address) if address.to_ipv4_mapped().is_some() => 35,
        IpAddr::V6(address) if address.segments()[0] == 0x2002 => 30,
        IpAddr::V6(address) if address.segments()[0] == 0x2001 && address.segments()[1] == 0 => 5,
        IpAddr::V6(address) if address.segments()[0] & 0xfe00 == 0xfc00 => 3,
        IpAddr::V6(address)
            if address.segments()[..6] == [0; 6]
                || address.segments()[0] & 0xffc0 == 0xfec0
                || address.segments()[0] == 0x3ffe =>
        {
            1
        }
        IpAddr::V6(_) => 40,
    }
}

#[derive(Debug)]
struct Rfc6724Candidate {
    address: SocketAddress,
    destination: Option<IpAddr>,
    source: Option<IpAddr>,
    original_index: usize,
}

impl Rfc6724Candidate {
    async fn probe(address: SocketAddress, original_index: usize) -> Self {
        let destination = std::str::from_utf8(address.host())
            .ok()
            .and_then(|address| address.parse::<IpAddr>().ok());
        let source = match destination {
            Some(IpAddr::V4(destination)) => {
                probe_source_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED), destination.into()).await
            }
            Some(IpAddr::V6(destination)) => {
                probe_source_address(IpAddr::V6(Ipv6Addr::UNSPECIFIED), destination.into()).await
            }
            None => None,
        };
        Self {
            address,
            destination,
            source,
            original_index,
        }
    }

    fn sort_key(&self) -> (bool, bool, bool, u8, u8, u32, usize) {
        let Some(destination) = self.destination else {
            return (
                true,
                true,
                true,
                u8::MAX,
                u8::MAX,
                u32::MAX,
                self.original_index,
            );
        };
        let source_scope = self.source.map_or(1, rfc6724_scope);
        let source_label = self.source.map_or(1, rfc6724_label);
        let prefix = match (self.source, destination) {
            (Some(IpAddr::V6(source)), IpAddr::V6(destination)) => {
                common_prefix_length(source, destination)
            }
            _ => 0,
        };
        (
            self.source.is_none(),
            source_scope != rfc6724_scope(destination),
            source_label != rfc6724_label(destination),
            u8::MAX - rfc6724_precedence(destination),
            rfc6724_scope(destination),
            u32::MAX - prefix,
            self.original_index,
        )
    }
}

async fn sort_rfc6724(
    addresses: Vec<SocketAddress>,
    cancellation: CancellationToken,
) -> Result<Vec<SocketAddress>, HttpError> {
    let mut pending: VecDeque<_> = addresses.into_iter().enumerate().collect();
    let mut candidates = Vec::with_capacity(pending.len());
    while let Some((index, address)) = pending.pop_front() {
        if cancellation.is_cancellation_requested() {
            return Err(client_error(
                HttpErrorCode::CORO_CANCELLED,
                b"DNS resolution cancelled",
            ));
        }
        candidates.push(Rfc6724Candidate::probe(address, index).await);
    }
    candidates.sort_by_key(Rfc6724Candidate::sort_key);
    Ok(candidates
        .into_iter()
        .map(|candidate| candidate.address)
        .collect())
}

async fn probe_source_address(bind: IpAddr, destination: IpAddr) -> Option<IpAddr> {
    let socket = tokio::net::UdpSocket::bind(SocketAddr::new(bind, 0))
        .await
        .ok()?;
    socket.connect(SocketAddr::new(destination, 9)).await.ok()?;
    socket.local_addr().ok().map(|address| address.ip())
}

fn rfc6724_scope(address: IpAddr) -> u8 {
    match address {
        IpAddr::V4(address) if address.is_loopback() => 2,
        IpAddr::V4(address) if address.is_link_local() => 2,
        IpAddr::V4(_) => 14,
        IpAddr::V6(address) if address.is_multicast() => address.octets()[1] & 0x0f,
        IpAddr::V6(address) if address.is_loopback() => 2,
        IpAddr::V6(address) if address.is_unicast_link_local() => 2,
        IpAddr::V6(address) if address.segments()[0] & 0xffc0 == 0xfec0 => 5,
        IpAddr::V6(_) => 14,
    }
}

fn rfc6724_label(address: IpAddr) -> u8 {
    match address {
        IpAddr::V4(_) => 4,
        IpAddr::V6(address) if address.is_loopback() => 0,
        IpAddr::V6(address) if address.to_ipv4_mapped().is_some() => 4,
        IpAddr::V6(address) if address.segments()[0] == 0x2002 => 2,
        IpAddr::V6(address) if address.segments()[0] == 0x2001 && address.segments()[1] == 0 => 5,
        IpAddr::V6(address) if address.segments()[0] & 0xfe00 == 0xfc00 => 13,
        IpAddr::V6(address) if address.segments()[0] & 0xffc0 == 0xfec0 => 11,
        IpAddr::V6(address) if address.segments()[0] == 0x3ffe => 12,
        IpAddr::V6(address) if address.segments()[..6] == [0; 6] => 3,
        IpAddr::V6(_) => 1,
    }
}

fn common_prefix_length(left: Ipv6Addr, right: Ipv6Addr) -> u32 {
    (u128::from(left) ^ u128::from(right)).leading_zeros()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedHost {
    pub primary: SocketAddress,
    pub fallback: Option<SocketAddress>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum IdentityValidation {
    DisabledInsecure = 0,
    Enforcing = 1,
    Logging = 2,
}

impl TryFrom<u8> for IdentityValidation {
    type Error = HttpError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::DisabledInsecure),
            1 => Ok(Self::Enforcing),
            2 => Ok(Self::Logging),
            _ => Err(client_error(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"unknown identity validation mode",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationPolicy {
    Enforcing,
    Logging,
}

pub const fn validation_policy(mode: IdentityValidation) -> Option<ValidationPolicy> {
    match mode {
        IdentityValidation::DisabledInsecure => None,
        IdentityValidation::Enforcing => Some(ValidationPolicy::Enforcing),
        IdentityValidation::Logging => Some(ValidationPolicy::Logging),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpectedIdentity {
    Dns(Box<str>),
    Ip(IpAddr),
}

impl ExpectedIdentity {
    pub fn dns(hostname: impl Into<Box<str>>) -> Self {
        Self::Dns(hostname.into())
    }

    pub const fn ip(address: IpAddr) -> Self {
        Self::Ip(address)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PresentedCertificate {
    pub encoded: Box<[u8]>,
    pub dns_names: Vec<Box<str>>,
    pub ip_addresses: Vec<IpAddr>,
}

pub type CertVerifyLogFn = TlsServerNameLogFn;

pub trait CertificateVerifier: 'static {
    fn verify(&self, certificate: &PresentedCertificate) -> Result<(), HttpError>;

    fn is_insecure(&self) -> bool {
        false
    }
}

struct IdentityVerifier {
    inner: Rc<dyn CertificateVerifier>,
    expected: ExpectedIdentity,
    policy: ValidationPolicy,
    log: Option<CertVerifyLogFn>,
}

impl CertificateVerifier for IdentityVerifier {
    fn verify(&self, certificate: &PresentedCertificate) -> Result<(), HttpError> {
        self.inner.verify(certificate)?;
        if identity_matches(&self.expected, certificate) {
            if let Some(log) = &self.log {
                log(CertVerifyResult::Success, b"");
            }
            return Ok(());
        }
        let error = identity_error(&self.expected, certificate);
        if let Some(log) = &self.log {
            log(
                if self.policy == ValidationPolicy::Enforcing {
                    CertVerifyResult::Fail
                } else {
                    CertVerifyResult::SoftFail
                },
                &error,
            );
        }
        if self.policy == ValidationPolicy::Logging {
            return Ok(());
        }
        Err(client_error(HttpErrorCode::INADEQUATE_SECURITY, error))
    }
}

pub fn make_verifier(
    inner: Option<Rc<dyn CertificateVerifier>>,
    expected: ExpectedIdentity,
    policy: ValidationPolicy,
    log: Option<CertVerifyLogFn>,
) -> Result<Rc<dyn CertificateVerifier>, HttpError> {
    let inner = inner.ok_or_else(|| {
        client_error(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"certificate verifier must not be null",
        )
    })?;
    if inner.is_insecure() {
        return Ok(inner);
    }
    Ok(Rc::new(IdentityVerifier {
        inner,
        expected,
        policy,
        log,
    }))
}

fn identity_matches(expected: &ExpectedIdentity, certificate: &PresentedCertificate) -> bool {
    match expected {
        ExpectedIdentity::Dns(expected) => certificate
            .dns_names
            .iter()
            .any(|presented| dns_name_matches(expected, presented)),
        ExpectedIdentity::Ip(expected) => certificate.ip_addresses.contains(expected),
    }
}

fn dns_name_matches(expected: &str, presented: &str) -> bool {
    if expected.eq_ignore_ascii_case(presented) {
        return true;
    }
    let Some(suffix) = presented.strip_prefix("*.") else {
        return false;
    };
    let Some((_, expected_suffix)) = expected.split_once('.') else {
        return false;
    };
    suffix.eq_ignore_ascii_case(expected_suffix)
}

fn identity_error(expected: &ExpectedIdentity, certificate: &PresentedCertificate) -> Vec<u8> {
    let expected = match expected {
        ExpectedIdentity::Dns(hostname) => hostname.to_string(),
        ExpectedIdentity::Ip(address) => address.to_string(),
    };
    let sans = certificate
        .dns_names
        .iter()
        .map(|name| name.to_string())
        .chain(certificate.ip_addresses.iter().map(ToString::to_string))
        .collect::<Vec<_>>()
        .join(", ");
    format!("certificate identity verification failed: expected={expected}, cert_sans=[{sans}]")
        .into_bytes()
}

#[derive(Clone)]
pub enum ClientTransportOptions {
    Plaintext,
    Tls(TlsClientOptions),
}

#[derive(Clone)]
pub struct ConnectionParams {
    pub transport: ClientTransportOptions,
    pub tcp_options: TcpConnectOptions,
    pub plaintext_protocol: CodecProtocol,
    pub identity_validation: IdentityValidation,
    pub cert_verify_log: Option<CertVerifyLogFn>,
    pub fizz_psk_identity: Option<Box<str>>,
    pub tls_stats: Option<Arc<dyn TlsConnectionStats>>,
}

impl Default for ConnectionParams {
    fn default() -> Self {
        Self {
            transport: ClientTransportOptions::Plaintext,
            tcp_options: TcpConnectOptions::default(),
            plaintext_protocol: CodecProtocol::Http11,
            identity_validation: IdentityValidation::Logging,
            cert_verify_log: None,
            fizz_psk_identity: None,
            tls_stats: None,
        }
    }
}

impl ConnectionParams {
    pub(crate) fn cache_key(&self) -> usize {
        match &self.transport {
            ClientTransportOptions::Plaintext => 0,
            ClientTransportOptions::Tls(options) => options.cache_identity(),
        }
    }

    pub fn for_destination(self, host: &str, secure: bool) -> Self {
        self.for_destination_with_default_verifier(host, secure, get_default_cert_verifier())
    }

    pub(crate) fn for_destination_with_default_verifier(
        mut self,
        host: &str,
        secure: bool,
        default_verifier: Option<Arc<dyn TlsCertificateVerifier>>,
    ) -> Self {
        if !secure {
            self.transport = ClientTransportOptions::Plaintext;
            return self;
        }
        match &mut self.transport {
            ClientTransportOptions::Plaintext => {
                let mut options = TlsClientOptions::new(host);
                options.set_ca_paths(get_default_ca_paths());
                options.set_certificate_verifier(default_verifier);
                self.transport = ClientTransportOptions::Tls(options);
            }
            ClientTransportOptions::Tls(options) => {
                options.set_server_name(host);
            }
        }
        self.configure_tls_options();
        self
    }

    pub(crate) fn configure_tls_options(&mut self) {
        let ClientTransportOptions::Tls(options) = &mut self.transport else {
            return;
        };
        let policy = match self.identity_validation {
            IdentityValidation::DisabledInsecure => TlsServerNameValidation::DisabledInsecure,
            IdentityValidation::Enforcing => TlsServerNameValidation::Enforcing,
            IdentityValidation::Logging => TlsServerNameValidation::Logging,
        };
        let log = match self.identity_validation {
            IdentityValidation::DisabledInsecure => None,
            IdentityValidation::Enforcing | IdentityValidation::Logging => {
                self.cert_verify_log.clone()
            }
        };
        options.set_server_name_validation(policy, log);
        options.set_session_cache_identity(self.fizz_psk_identity.clone());
    }
}

#[derive(Clone, Default)]
pub struct SessionParams {
    pub settings: Vec<CodecSetting>,
    pub max_concurrent_outgoing_streams: Option<u32>,
    pub connection_flow_control: Option<u32>,
    pub stream_read_timeout: Option<Duration>,
    pub connection_read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub lifecycle_observer: Option<Rc<dyn LifecycleObserver>>,
    pub session_stats: Option<Rc<dyn ClientSessionStats>>,
    pub header_codec_stats: Option<Rc<dyn HeaderCodecStats>>,
    pub header_indexing_strategy: Option<Rc<dyn HeaderIndexingStrategy>>,
    /// Experimental PING probes for every HTTP/2 session built from these
    /// parameters (`DIV-024`); HTTP/1.1 sessions ignore them. `None`, the
    /// default, sends none.
    pub ping_probes: Option<PingProbeConfig>,
    /// Roxygen-only, default off (`DIV-033`): a response whose stream is aborted
    /// (a peer reset, a transport EOF or read error, a connection error) after
    /// part of it arrived still delivers the head and body events already
    /// received, in order, before the error, as Hyper's body channel and h2 do.
    /// Off, the frozen behavior, the error discards them.
    pub deliver_ingress_before_abort: bool,
    /// Roxygen-only, default off (`DIV-036`): an HTTP/1.1 request whose head ends the
    /// message (no body) and carries neither `Content-Length` nor a chunked
    /// `Transfer-Encoding` goes out with no body framing, as reqwest sends a `POST`
    /// with an empty body, instead of an empty chunked body. HTTP/2 sessions ignore it.
    pub unframed_bodiless_http1_requests: bool,
    /// Roxygen-only, default off (`DIV-038`): a session that resets its connection (a
    /// dropped response, a failed or cancelled session, an HTTP/1.1 stream reset)
    /// closes the socket without `SO_LINGER` zero, as hyper and reqwest close one by
    /// dropping it: the kernel still sends the bytes already written, then FIN, or RST
    /// only if received bytes are unread. Plaintext only: a connector refuses it for a
    /// TLS connection, and `ClientSession::attach_transport` for a TLS transport.
    pub non_abortive_close: bool,
}

/// Attaches already-connected I/O without performing DNS, TLS, or routing.
pub struct ClientAttachOptions {
    pub protocol: CodecProtocol,
    pub session: SessionParams,
    pub security: TransportSecurity,
    pub upstream_origin: Option<UpstreamOrigin>,
    /// Cancels the attached session, including its outstanding exchanges.
    /// Cancellation after the supervisor publishes native completion is not promised.
    pub cancellation: CancellationToken,
}

impl ClientAttachOptions {
    /// Requires an explicit wire protocol and immediate-hop security assertion.
    pub fn new(protocol: CodecProtocol, security: TransportSecurity) -> Self {
        Self {
            protocol,
            session: SessionParams::default(),
            security,
            upstream_origin: None,
            cancellation: CancellationToken::never(),
        }
    }
}

impl SessionParams {
    pub fn http_client_defaults(read_timeout: Option<Duration>) -> Self {
        Self {
            settings: vec![CodecSetting {
                identifier: 4,
                value: u64::from(1_u32 << 20),
            }],
            connection_flow_control: Some(10 * (1_u32 << 20)),
            stream_read_timeout: read_timeout,
            connection_read_timeout: read_timeout,
            ..Self::default()
        }
    }
}

pub(crate) fn uniplex_config_for_session_params(session: &SessionParams) -> UniplexConfig {
    let mut config = UniplexConfig::default();
    if let Some(timeout) = session.connection_read_timeout {
        config.read_timeout = timeout;
    }
    if let Some(timeout) = session.stream_read_timeout {
        config.stream_read_timeout = timeout;
    }
    if let Some(timeout) = session.write_timeout {
        config.write_timeout = timeout;
    }
    config.deliver_ingress_before_abort = session.deliver_ingress_before_abort;
    config.unframed_bodiless_http1_requests = session.unframed_bodiless_http1_requests;
    config.non_abortive_close = session.non_abortive_close;
    config
}

pub(crate) type SessionOutcome = Result<UniplexRunSummary, HttpError>;
type CompletionWaiter = RefCell<Option<Waker>>;

#[derive(Default)]
pub(crate) struct SessionCompletionState {
    outcome: Option<SessionOutcome>,
    closed: bool,
    waiters: Vec<Weak<CompletionWaiter>>,
}

/// Owner-local observation of one session scope's outcome. A published
/// outcome wins over a later close; both are sticky.
pub(crate) type SessionCompletion = Rc<RefCell<SessionCompletionState>>;

fn session_completion_outcome(completion: &SessionCompletion) -> Option<SessionOutcome> {
    let state = completion.borrow();
    if let Some(outcome) = &state.outcome {
        return Some(outcome.clone());
    }
    state.closed.then(|| {
        Err(client_error(
            HttpErrorCode::INTERNAL_ERROR,
            b"session scope failed",
        ))
    })
}

/// Publishes the scope's outcome once. Dropped before publishing, as when the
/// worker drops the scope, it closes the completion instead.
pub(crate) struct SessionCompletionPublisher(Option<SessionCompletion>);

impl SessionCompletionPublisher {
    pub(crate) fn new(completion: &SessionCompletion) -> Self {
        Self(Some(Rc::clone(completion)))
    }

    pub(crate) fn publish(mut self, outcome: SessionOutcome) {
        if let Some(completion) = self.0.take() {
            let waiters = {
                let mut state = completion.borrow_mut();
                state.outcome = Some(outcome);
                std::mem::take(&mut state.waiters)
            };
            wake_completion_waiters(waiters);
        }
    }
}

impl Drop for SessionCompletionPublisher {
    fn drop(&mut self) {
        if let Some(completion) = self.0.take() {
            let waiters = {
                let mut state = completion.borrow_mut();
                state.closed = true;
                std::mem::take(&mut state.waiters)
            };
            wake_completion_waiters(waiters);
        }
    }
}

// Nothing between a publisher entry point and this call can begin or end
// unwinding, so sampling `panicking()` here matches sampling it on entry.
fn wake_completion_waiters(waiters: Vec<Weak<CompletionWaiter>>) {
    let unwinding = std::thread::panicking();
    let mut first_panic = None;
    for waiter in waiters {
        let waker = waiter
            .upgrade()
            .and_then(|waiter| waiter.borrow_mut().take());
        if let Some(waker) = waker {
            retain_wake_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| waker.wake())).err(),
            );
        }
    }
    if let Some(payload) = first_panic {
        if unwinding {
            discard_panic(payload);
        } else {
            resume_unwind(payload);
        }
    }
}

/// Returns a completion and the publisher that settles it, without a session.
#[cfg(test)]
pub(crate) fn session_completion_for_test() -> (SessionCompletionPublisher, SessionCompletion) {
    let completion = SessionCompletion::default();
    (
        SessionCompletionPublisher(Some(Rc::clone(&completion))),
        completion,
    )
}

/// Returns the registered waiters of a completion, live or not.
#[cfg(test)]
pub(crate) fn completion_waiters_for_test(completion: &SessionCompletion) -> usize {
    completion.borrow().waiters.len()
}

/// Waits for a published outcome; a closed completion fails `session scope failed`.
pub(crate) fn wait_session_completion(
    completion: &SessionCompletion,
) -> LocalClientFuture<SessionOutcome> {
    if let Some(outcome) = session_completion_outcome(completion) {
        return Box::pin(std::future::ready(outcome));
    }
    let completion = Rc::clone(completion);
    let waiter = Rc::new(RefCell::new(None));
    {
        let mut state = completion.borrow_mut();
        state.waiters.retain(|waiter| waiter.strong_count() != 0);
        state.waiters.push(Rc::downgrade(&waiter));
    }
    Box::pin(poll_fn(move |context| {
        if let Some(outcome) = session_completion_outcome(&completion) {
            return Poll::Ready(outcome);
        }
        let mut registered = waiter.borrow_mut();
        if registered
            .as_ref()
            .is_none_or(|waker: &Waker| !waker.will_wake(context.waker()))
        {
            *registered = Some(context.waker().clone());
        }
        Poll::Pending
    }))
}

fn retain_wake_panic(first: &mut Option<Box<dyn Any + Send>>, next: Option<Box<dyn Any + Send>>) {
    if let Some(payload) = next {
        if first.is_none() {
            *first = Some(payload);
        } else {
            discard_panic(payload);
        }
    }
}

struct ClientSessionInner {
    handle: UniplexHandle,
    local_address: SocketAddress,
    peer_address: SocketAddress,
    protocol: CodecProtocol,
    tls_session_reused: bool,
    worker_id: WorkerId,
    worker: WorkerHandle,
    connection: Arc<ConnectionContext>,
    cancellation: CancellationSource,
    attachment_cancellation: CancellationToken,
    completion: SessionCompletion,
    transport_retirement: Option<TransportRetirement>,
    started: std::time::Instant,
    active_transactions: Cell<u32>,
    outgoing_activity: OutgoingTransactionActivity,
    peer_full: Cell<bool>,
    idle: Cell<bool>,
    draining: Cell<bool>,
    finished: Cell<bool>,
    availability: RefCell<Option<Weak<RefCell<crate::pool::PoolState>>>>,
}

impl Drop for ClientSessionInner {
    fn drop(&mut self) {
        let _ = catch_unwind(AssertUnwindSafe(|| self.handle.initiate_drain()));
    }
}

struct SessionLifecycle {
    session: Weak<ClientSessionInner>,
}

impl LifecycleObserver for SessionLifecycle {
    fn on_event(&self, event: LifecycleEvent) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        let enqueue_back = match event {
            LifecycleEvent::TransactionAttached => {
                session
                    .active_transactions
                    .set(session.active_transactions.get().saturating_add(1));
                let supports_more = session.outgoing_activity.supports_more();
                let was_full = session.peer_full.replace(!supports_more);
                session.idle.replace(false) || (!was_full && !supports_more)
            }
            LifecycleEvent::TransactionDetached { .. } => {
                session
                    .active_transactions
                    .set(session.active_transactions.get().saturating_sub(1));
                let supports_more = session.outgoing_activity.supports_more();
                let was_full = session.peer_full.replace(!supports_more);
                was_full && supports_more
            }
            LifecycleEvent::OutgoingCapacityReleased { .. } => {
                let supports_more = session.outgoing_activity.supports_more();
                let was_full = session.peer_full.replace(!supports_more);
                was_full && supports_more
            }
            LifecycleEvent::Http2StreamsFull => {
                session.peer_full.set(true);
                session.idle.set(false);
                true
            }
            LifecycleEvent::Http2StreamsNotFull => {
                session.peer_full.set(false);
                session.idle.set(false);
                true
            }
            LifecycleEvent::DeactivateConnection => {
                let supports_more = session.outgoing_activity.supports_more();
                session.peer_full.set(!supports_more);
                if supports_more {
                    !session.idle.replace(true)
                } else {
                    session.idle.set(false);
                    false
                }
            }
            LifecycleEvent::OutgoingAdmissionClosed | LifecycleEvent::DrainStarted(_) => {
                session.idle.set(false);
                session.draining.set(true);
                false
            }
            LifecycleEvent::Destroy => {
                session.idle.set(false);
                session.finished.set(true);
                false
            }
            _ => false,
        };
        let wake_count = match event {
            LifecycleEvent::TransactionAttached => 0,
            LifecycleEvent::TransactionDetached { .. }
            | LifecycleEvent::OutgoingCapacityReleased { .. }
            | LifecycleEvent::DeactivateConnection
            | LifecycleEvent::Http2StreamsNotFull => {
                if session.draining.get() || session.finished.get() || session.peer_full.get() {
                    0
                } else {
                    usize::try_from(session.outgoing_activity.available()).unwrap_or(usize::MAX)
                }
            }
            LifecycleEvent::OutgoingAdmissionClosed
            | LifecycleEvent::DrainStarted(_)
            | LifecycleEvent::Destroy => 1,
            _ => 0,
        };
        let pool = session
            .availability
            .borrow()
            .as_ref()
            .and_then(Weak::upgrade);
        if matches!(
            event,
            LifecycleEvent::TransactionAttached
                | LifecycleEvent::TransactionDetached { .. }
                | LifecycleEvent::OutgoingCapacityReleased { .. }
                | LifecycleEvent::OutgoingAdmissionClosed
                | LifecycleEvent::DeactivateConnection
                | LifecycleEvent::Http2StreamsFull
                | LifecycleEvent::Http2StreamsNotFull
                | LifecycleEvent::DrainStarted(_)
                | LifecycleEvent::Destroy
        ) && let Some(pool) = pool
        {
            let changed_session = crate::pool::PooledSession::Uniplex(ClientSession {
                inner: Rc::clone(&session),
            });
            crate::pool::notify_session_event(
                &pool,
                session.worker_id,
                Some(&changed_session),
                enqueue_back,
                wake_count,
                matches!(
                    event,
                    LifecycleEvent::TransactionDetached { .. }
                        | LifecycleEvent::OutgoingCapacityReleased { .. }
                        | LifecycleEvent::DeactivateConnection
                        | LifecycleEvent::Http2StreamsNotFull
                ),
            );
        }
    }
}

#[derive(Clone)]
pub(crate) struct SessionRequestCancellation {
    local: CancellationToken,
    attachment: CancellationToken,
}

impl SessionRequestCancellation {
    #[cfg(test)]
    pub(crate) fn for_test(local: CancellationToken, attachment: CancellationToken) -> Self {
        Self { local, attachment }
    }

    fn check(&self) -> Result<(), HttpError> {
        if self.local.is_cancellation_requested() || self.attachment.is_cancellation_requested() {
            Err(session_cancelled())
        } else {
            Ok(())
        }
    }
}

fn session_cancelled() -> HttpError {
    client_error(HttpErrorCode::CORO_CANCELLED, b"client session cancelled")
}

/// Gates a request head read on the session's tokens inside the send future.
impl RequestHeadGate for SessionRequestCancellation {
    type Read = SessionHeadRead<HolderHeaderRead>;

    fn gate(self, read: HolderHeaderRead) -> Self::Read {
        SessionHeadRead::new(self, read)
    }
}

impl HttpSourceFilterHooks for SessionRequestCancellation {
    fn filter_name(&self) -> &'static str {
        "SessionRequestCancellation"
    }

    fn read_header_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(SessionHeadRead::new(self.clone(), downstream))
    }
}

/// A request head read that fails `CORO_CANCELLED` when either session token is
/// cancelled. A token cancelled before the first poll drops the unpolled read.
/// The first poll polls the read once and builds the token waits only if it is
/// pending. Every exit drops the read before it checks the tokens.
pub(crate) struct SessionHeadRead<F> {
    downstream: Option<F>,
    cancellation: SessionRequestCancellation,
    waits: Option<HeadWaits>,
}

/// The token waits of a pending head read, in drop order: the waits, then the
/// test count, then the batons.
struct HeadWaits {
    attachment_wait: Option<Pin<Box<CancellableWait>>>,
    local_wait: Option<Pin<Box<CancellableWait>>>,
    #[cfg(test)]
    _live: head_wait_probe::LiveWaits,
    _attachment_baton: CancellableBaton,
    _local_baton: CancellableBaton,
}

impl HeadWaits {
    fn new(cancellation: &SessionRequestCancellation) -> Self {
        let local_baton = CancellableBaton::new();
        let attachment_baton = CancellableBaton::new();
        let local_wait = Box::pin(local_baton.wait(cancellation.local.clone()));
        let attachment_wait = Box::pin(attachment_baton.wait(cancellation.attachment.clone()));
        Self {
            attachment_wait: Some(attachment_wait),
            local_wait: Some(local_wait),
            #[cfg(test)]
            _live: head_wait_probe::LiveWaits::new(),
            _attachment_baton: attachment_baton,
            _local_baton: local_baton,
        }
    }

    fn poll_cancelled(&mut self, context: &mut Context<'_>) -> bool {
        poll_head_wait(&mut self.local_wait, context)
            || poll_head_wait(&mut self.attachment_wait, context)
    }
}

fn poll_head_wait(wait: &mut Option<Pin<Box<CancellableWait>>>, context: &mut Context<'_>) -> bool {
    wait.as_mut()
        .expect("a live head wait")
        .as_mut()
        .poll(context)
        .is_ready()
}

impl<F> SessionHeadRead<F> {
    fn new(cancellation: SessionRequestCancellation, downstream: F) -> Self {
        Self {
            downstream: Some(downstream),
            cancellation,
            waits: None,
        }
    }
}

impl<F> Future for SessionHeadRead<F>
where
    F: Future<Output = Result<HttpHeaderEvent, HttpError>> + Unpin,
{
    type Output = Result<HttpHeaderEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = if let Some(waits) = this.waits.as_mut() {
            if waits.poll_cancelled(context) {
                Err(session_cancelled())
            } else {
                let downstream = this.downstream.as_mut().expect("a live head read");
                let Poll::Ready(result) = Pin::new(downstream).poll(context) else {
                    return Poll::Pending;
                };
                result
            }
        } else {
            if let Err(error) = this.cancellation.check() {
                this.downstream = None;
                return Poll::Ready(Err(error));
            }
            let downstream = this.downstream.as_mut().expect("a live head read");
            if let Poll::Ready(result) = Pin::new(downstream).poll(context) {
                this.downstream = None;
                return Poll::Ready(this.cancellation.check().and(result));
            }
            // The read was polled in this task poll; its next poll follows a wake.
            let waits = this.waits.insert(HeadWaits::new(&this.cancellation));
            if !waits.poll_cancelled(context) {
                return Poll::Pending;
            }
            Err(session_cancelled())
        };
        this.downstream = None;
        let waits = this.waits.as_mut().expect("a pending head read has waits");
        waits.attachment_wait = None;
        waits.local_wait = None;
        let result = this.cancellation.check().and(result);
        this.waits = None;
        Poll::Ready(result)
    }
}

/// Test-only counts of the cancellation waits a request head read builds.
#[cfg(test)]
pub(crate) mod head_wait_probe {
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct HeadWaitCounts {
        /// Cancellation batons and their waits built, one pair per token.
        pub(crate) built: usize,
        /// Waits currently alive.
        pub(crate) live: usize,
    }

    thread_local! {
        static COUNTS: Cell<HeadWaitCounts> = Cell::new(HeadWaitCounts::default());
    }

    pub(crate) fn snapshot() -> HeadWaitCounts {
        COUNTS.with(Cell::get)
    }

    fn update(change: impl FnOnce(&mut HeadWaitCounts)) {
        COUNTS.with(|counts| {
            let mut current = counts.get();
            change(&mut current);
            counts.set(current);
        });
    }

    /// Accounts for the two waits of one head read while they are alive.
    pub(crate) struct LiveWaits;

    impl LiveWaits {
        pub(crate) fn new() -> Self {
            update(|counts| {
                counts.built += 2;
                counts.live += 2;
            });
            Self
        }
    }

    impl Drop for LiveWaits {
        fn drop(&mut self) {
            update(|counts| counts.live -= 2);
        }
    }
}

/// Cloneable owner-local handle for a live upstream session.
#[derive(Clone)]
pub struct ClientSession {
    inner: Rc<ClientSessionInner>,
}

impl ClientSession {
    /// Consumes an established transport on its exact owner worker.
    ///
    /// Protocol and immediate security are explicit trusted inputs. Endpoints
    /// and identity are read from the transport, never from request headers.
    /// Failure disposes the transport on its owner; successful attachment roots
    /// session completion independently of the returned handle.
    pub fn attach_transport(
        worker: &WorkerContext,
        transport: OwnedByteStream,
        options: ClientAttachOptions,
    ) -> Result<Self, HttpError> {
        check_attachment_admission(worker, &options.cancellation)?;
        if options.session.non_abortive_close {
            if options.security == TransportSecurity::Tls {
                return Err(non_abortive_close_with_tls());
            }
            transport.enable_non_abortive_close(worker)?;
        }
        let limits = transport.limits(worker)?;
        let connection = connection_context(
            worker,
            &transport,
            options.security,
            options.upstream_origin,
        )?;
        let mut config = uniplex_config_for_session_params(&options.session);
        config.maximum_read_size = config.maximum_read_size.min(limits.maximum_read_size());
        config.read_allocation_size = config.read_allocation_size.min(limits.maximum_read_size());
        config.maximum_write_size = config.maximum_write_size.min(limits.maximum_write_size());
        let transport_retirement = transport.retirement();
        start_client_session(
            worker,
            Rc::new(transport),
            ClientSessionSetup {
                protocol: options.protocol,
                connection,
                session: options.session,
                config,
                tls_session_reused: false,
                cancellation: options.cancellation,
                transport_retirement,
            },
        )
    }

    /// Returns immutable metadata that can cross a thread boundary without the session.
    pub fn connection_context(&self) -> &Arc<ConnectionContext> {
        &self.inner.connection
    }

    /// Returns independent backend-value and close-scope retirement observation.
    ///
    /// Native TCP/TLS supplies this receipt. `None` means unsupported, not
    /// completed cleanup. Unlike `wait_closed`, it includes the transport's
    /// close scope, but does not certify I/O success or physical worker joins.
    /// Retaining or dropping it does not change session or transport ownership.
    pub fn transport_retirement(&self) -> Option<TransportRetirement> {
        self.inner.transport_retirement.clone()
    }

    /// Requests cancellation locally without submitting a command or waiting for capacity.
    /// Cancellation after the supervisor publishes native completion is not promised.
    pub fn cancel(&self) {
        self.inner.draining.set(true);
        self.inner.cancellation.request_cancellation();
    }

    /// Observes session finalization, including its joined tasks and callbacks.
    /// Transport flush/shutdown can still be scheduled; runtime quiescence joins
    /// that separate scope. Dropping this observation does not cancel cleanup.
    pub fn wait_closed(&self) -> LocalClientFuture<Result<UniplexRunSummary, HttpError>> {
        wait_session_completion(&self.inner.completion)
    }

    /// Returns the registered `wait_closed` waiters, live or not.
    #[cfg(test)]
    pub(crate) fn completion_waiters_for_test(&self) -> usize {
        completion_waiters_for_test(&self.inner.completion)
    }

    /// Holds the session's writer, the next time it would stage egress, until
    /// `release` is notified (the session's `test-hooks` feature, which Buck's
    /// test build lacks).
    #[cfg(all(test, not(fbcode_build)))]
    pub(crate) fn hold_writer_for_test(&self, release: Rc<tokio::sync::Notify>) {
        self.inner.handle.hold_writer_for_test(release);
    }

    fn check_admission(&self) -> Result<(), HttpError> {
        if !self.inner.worker.is_accepting()
            || self.inner.draining.get()
            || self.inner.cancellation.token().is_cancellation_requested()
            || self
                .inner
                .attachment_cancellation
                .is_cancellation_requested()
        {
            return Err(client_error(
                HttpErrorCode::DROPPED,
                b"client session admission closed",
            ));
        }
        Ok(())
    }

    pub fn reserve_request(&self) -> Result<RequestReservation, HttpError> {
        self.check_admission()?;
        self.inner.handle.reserve_request()
    }

    fn request_cancellation(&self) -> SessionRequestCancellation {
        SessionRequestCancellation {
            local: self.inner.cancellation.token(),
            attachment: self.inner.attachment_cancellation.clone(),
        }
    }

    pub fn set_max_concurrent_outgoing_streams(&self, maximum: u32) -> Result<bool, HttpError> {
        self.inner
            .handle
            .set_max_concurrent_outgoing_streams(maximum)
    }

    pub fn send_request(
        &self,
        request: HttpSourceHolder,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<UniplexResponse, HttpError>> {
        if let Err(error) = self.check_admission() {
            return Box::pin(std::future::ready(Err(error)));
        }
        self.inner
            .handle
            .send_request_gated(request, cancellation, self.request_cancellation())
    }

    pub fn send_reserved_request(
        &self,
        request: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<UniplexResponse, HttpError>> {
        let body_cancellation = cancellation.clone();
        self.send_reserved_request_inner(
            request,
            reservation,
            cancellation,
            Some(body_cancellation),
        )
    }

    /// Commits `request` on `reservation`. `cancellation` covers the head read
    /// and admission; `body_cancellation`, when present, stays attached to the
    /// upload after admission.
    pub(crate) fn send_reserved_request_inner(
        &self,
        request: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
        body_cancellation: Option<CancellationToken>,
    ) -> LocalClientFuture<Result<UniplexResponse, HttpError>> {
        let session_cancellation = self.request_cancellation();
        if let Err(error) = session_cancellation.check() {
            return Box::pin(std::future::ready(Err(error)));
        }
        let preparation = UniplexHandle::prepare_request_gated(
            request,
            reservation,
            cancellation,
            session_cancellation.clone(),
        );
        let handle = self.inner.handle.clone();
        Box::pin(async move {
            let prepared = preparation.await?;
            session_cancellation.check()?;
            match body_cancellation {
                Some(body_cancellation) => {
                    handle.send_prepared_request_with_cancellation(prepared, body_cancellation)
                }
                None => handle.send_prepared_request(prepared),
            }
        })
    }

    /// Commits an already-read header and its remaining body using reserved capacity.
    /// Caller and session cancellation are checked before admission; caller
    /// cancellation remains attached to the upload after admission.
    pub fn send_reserved_request_with_headers(
        &self,
        headers: HttpHeaderEvent,
        body: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> Result<UniplexResponse, HttpError> {
        self.request_cancellation().check()?;
        if cancellation.is_cancellation_requested() {
            return Err(client_error(
                HttpErrorCode::CORO_CANCELLED,
                b"request admission cancelled",
            ));
        }
        let prepared = UniplexHandle::prepare_request_with_headers(headers, body, reservation);
        self.inner
            .handle
            .send_prepared_request_with_cancellation(prepared, cancellation)
    }

    pub fn initiate_drain(&self) -> Result<bool, HttpError> {
        self.inner.draining.set(true);
        self.inner.handle.initiate_drain()
    }

    /// Refuses new reservations and requests ahead of `initiate_drain`, so that
    /// callbacks run by another session's drain cannot reserve this one.
    pub(crate) fn close_admission(&self) {
        self.inner.draining.set(true);
    }

    pub fn close_when_idle(&self) -> Result<bool, HttpError> {
        self.inner.draining.set(true);
        self.inner.handle.close_when_idle()
    }

    pub fn protocol(&self) -> CodecProtocol {
        self.inner.protocol
    }

    /// Whether the native connector observed TLS resumption. Established-I/O
    /// attachment reports false and does not inspect an external TLS session.
    pub fn tls_session_reused(&self) -> bool {
        self.inner.tls_session_reused
    }

    pub fn local_address(&self) -> &SocketAddress {
        &self.inner.local_address
    }

    pub fn peer_address(&self) -> &SocketAddress {
        &self.inner.peer_address
    }

    pub fn worker_id(&self) -> WorkerId {
        self.inner.worker_id
    }

    pub fn active_transactions(&self) -> u32 {
        self.inner.active_transactions.get()
    }

    /// Reserved and active transactions as the session counts them, before the
    /// lifecycle events that `active_transactions` follows are delivered.
    pub(crate) fn admitted_transactions(&self) -> u32 {
        self.inner.outgoing_activity.admitted_transactions()
    }

    pub fn supports_more_transactions(&self) -> bool {
        self.check_admission().is_ok()
            && !self.inner.finished.get()
            && !self.inner.peer_full.get()
            && self.inner.outgoing_activity.supports_more()
    }

    pub(crate) fn available_transactions(&self) -> usize {
        if self.check_admission().is_err()
            || self.inner.finished.get()
            || self.inner.peer_full.get()
        {
            return 0;
        }
        usize::try_from(self.inner.outgoing_activity.available()).unwrap_or(usize::MAX)
    }

    pub fn is_idle(&self) -> bool {
        self.inner.idle.get() && !self.inner.finished.get()
    }

    pub fn is_finished(&self) -> bool {
        self.inner.finished.get() || self.inner.handle.is_finished()
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.inner.draining.get()
    }

    pub(crate) fn bind_pool(&self, pool: Weak<RefCell<crate::pool::PoolState>>) {
        self.inner.availability.replace(Some(pool));
    }

    pub(crate) fn unbind_pool(&self) {
        self.inner.availability.replace(None);
    }

    pub(crate) fn is_bound_to_pool(&self) -> bool {
        self.inner.availability.borrow().is_some()
    }

    pub(crate) fn is_same_session(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn mark_available(&self) {
        self.inner.idle.set(false);
    }

    pub(crate) fn age(&self) -> Duration {
        self.inner.started.elapsed()
    }
}

pub trait SessionConnector: 'static {
    fn connect(
        &self,
        context: WorkerContext,
        address: SocketAddress,
        timeout: Duration,
        connection: ConnectionParams,
        session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HttpConnector;

impl SessionConnector for HttpConnector {
    fn connect(
        &self,
        context: WorkerContext,
        address: SocketAddress,
        timeout: Duration,
        mut connection: ConnectionParams,
        session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        Box::pin(async move {
            check_attachment_admission(&context, &cancellation)?;
            if session.non_abortive_close
                && matches!(connection.transport, ClientTransportOptions::Tls(_))
            {
                return Err(non_abortive_close_with_tls());
            }
            let limits = client_byte_stream_limits();
            connection.configure_tls_options();
            let tcp_options = connection.tcp_options.clone();
            let tls_stats = connection.tls_stats.clone();
            let post_connect_cancellation = cancellation.clone();
            let (transport, protocol, metadata, tls_session_reused, transport_retirement) =
                match connection.transport {
                    ClientTransportOptions::Plaintext => {
                        let (transport, metadata) = dial_plaintext(
                            &context,
                            address,
                            limits,
                            timeout,
                            cancellation,
                            tcp_options,
                            session.non_abortive_close,
                        )
                        .await?;
                        let transport_retirement = transport.retirement();
                        (
                            Rc::new(transport) as Rc<dyn UniplexTransport>,
                            connection.plaintext_protocol,
                            metadata,
                            false,
                            transport_retirement,
                        )
                    }
                    ClientTransportOptions::Tls(options) => {
                        let connected = TlsTransport::connect_with_tcp_options(
                            &context,
                            address,
                            options,
                            limits,
                            timeout,
                            cancellation,
                            tcp_options,
                        )
                        .await;
                        tokio::task::yield_now().await;
                        if post_connect_cancellation.is_cancellation_requested() {
                            if let Ok(transport) = &connected {
                                let _ = ByteStreamTransport::close(transport, &context);
                            }
                            return Err(client_error(
                                HttpErrorCode::CORO_CANCELLED,
                                b"client connect cancelled after transport completion",
                            ));
                        }
                        let transport = match connected {
                            Ok(transport) => transport,
                            Err(error) => {
                                notify_tls_error(tls_stats.as_ref(), false);
                                return Err(error);
                            }
                        };
                        let protocol = match transport.connection_info().application_protocol() {
                            Some(b"h2") => CodecProtocol::Http2,
                            Some(b"http/1.1") | Some(_) | None => CodecProtocol::Http11,
                        };
                        let metadata =
                            connection_context(&context, &transport, TransportSecurity::Tls, None)?;
                        let tls_session_reused = transport.connection_info().session_resumed();
                        notify_tls_connection(tls_stats.as_ref(), !tls_session_reused);
                        let transport_retirement = transport.retirement();
                        (
                            Rc::new(transport) as Rc<dyn UniplexTransport>,
                            protocol,
                            metadata,
                            tls_session_reused,
                            transport_retirement,
                        )
                    }
                };
            check_attachment_admission(&context, &post_connect_cancellation)?;
            let config = uniplex_config_for_session_params(&session);
            start_client_session(
                &context,
                transport,
                ClientSessionSetup {
                    protocol,
                    connection: metadata,
                    session,
                    config,
                    tls_session_reused,
                    cancellation: CancellationToken::never(),
                    transport_retirement,
                },
            )
        })
    }
}

/// The byte-stream limits of the connections a connector dials.
pub(crate) fn client_byte_stream_limits() -> ByteStreamLimits {
    ByteStreamLimits::new(256 * 1024, 256 * 1024, 1_024)
        .expect("client byte-stream limits are nonzero")
}

/// Dials a plaintext TCP connection and reads its connection context, enabling
/// `non_abortive_close` (`DIV-038`) first. A cancellation that lands while the
/// connect completes closes the connected transport.
pub(crate) async fn dial_plaintext(
    context: &WorkerContext,
    address: SocketAddress,
    limits: ByteStreamLimits,
    timeout: Duration,
    cancellation: CancellationToken,
    tcp_options: TcpConnectOptions,
    non_abortive_close: bool,
) -> Result<(OwnedByteStream, Arc<ConnectionContext>), HttpError> {
    let post_connect_cancellation = cancellation.clone();
    let connected = TcpTransport::connect_with_options(
        context,
        address,
        limits,
        timeout,
        cancellation,
        tcp_options,
    )
    .await;
    tokio::task::yield_now().await;
    if post_connect_cancellation.is_cancellation_requested() {
        if let Ok(transport) = &connected {
            let _ = ByteStreamTransport::close(transport, context);
        }
        return Err(client_error(
            HttpErrorCode::CORO_CANCELLED,
            b"client connect cancelled after transport completion",
        ));
    }
    let transport = connected?;
    if non_abortive_close {
        transport.enable_non_abortive_close(context)?;
    }
    let metadata = connection_context(context, &transport, TransportSecurity::Plaintext, None)?;
    Ok((transport, metadata))
}

/// The TLS adapter's own failure paths reset, so `SessionParams::non_abortive_close`
/// is plaintext only (`DIV-038`).
pub(crate) fn non_abortive_close_with_tls() -> HttpError {
    client_error(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"non_abortive_close does not support TLS",
    )
}

pub(crate) fn check_attachment_admission(
    context: &WorkerContext,
    cancellation: &CancellationToken,
) -> Result<(), HttpError> {
    if !context.is_owner_thread() {
        return Err(client_error(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"session attachment requires its worker owner thread",
        ));
    }
    if cancellation.is_cancellation_requested() {
        return Err(client_error(
            HttpErrorCode::CORO_CANCELLED,
            b"session attachment cancelled",
        ));
    }
    if !context.handle().is_accepting() || context.is_cancelled() {
        return Err(client_error(
            HttpErrorCode::DROPPED,
            b"worker admission closed",
        ));
    }
    Ok(())
}

pub(crate) fn connection_context(
    context: &WorkerContext,
    transport: &impl ByteStreamTransport,
    security: TransportSecurity,
    origin: Option<UpstreamOrigin>,
) -> Result<Arc<ConnectionContext>, HttpError> {
    Ok(Arc::new(
        ConnectionContext::new(
            transport.local_endpoint(context)?,
            transport.peer_endpoint(context)?,
            security,
            transport
                .peer_identity(context)?
                .map(|identity| Arc::from(identity.as_bytes())),
        )
        .with_upstream_origin(origin),
    ))
}

struct ClientSessionSetup {
    protocol: CodecProtocol,
    connection: Arc<ConnectionContext>,
    session: SessionParams,
    config: UniplexConfig,
    tls_session_reused: bool,
    cancellation: CancellationToken,
    transport_retirement: Option<TransportRetirement>,
}

fn start_client_session(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    setup: ClientSessionSetup,
) -> Result<ClientSession, HttpError> {
    check_attachment_admission(context, &setup.cancellation)?;
    let ClientSessionSetup {
        protocol,
        connection,
        session,
        config,
        tls_session_reused,
        cancellation,
        transport_retirement,
    } = setup;
    if !matches!(protocol, CodecProtocol::Http11 | CodecProtocol::Http2) {
        return Err(client_error(
            HttpErrorCode::CONNECT_ERROR,
            b"unsupported attachment protocol",
        ));
    }
    let protocol_config = if protocol == CodecProtocol::Http2 {
        UniplexProtocolConfig::Http2(http2_session_config(&session))
    } else {
        UniplexProtocolConfig::Http1 {
            stats: session.session_stats.as_ref().map(|stats| {
                Rc::new(Http2ClientSessionStats(Rc::clone(stats))) as Rc<dyn Http2SessionStats>
            }),
        }
    };
    let (driver, handle) = UniplexSession::new_owned(
        context.clone(),
        TransportDirection::Upstream,
        Rc::clone(&transport),
        None,
        config,
        protocol_config,
    )?;
    let completion = SessionCompletion::default();
    let local_cancellation = CancellationSource::new();
    let owner = ClientSessionOwner {
        external: cancellation.wake_registration(),
        local: local_cancellation.token().wake_registration(),
        quiescing: context.quiescing(),
        context: context.clone(),
        handle: handle.clone(),
        transport,
        draining: false,
        aborting: false,
        failure: None,
        completed: SessionCompletionPublisher(Some(Rc::clone(&completion))),
    };
    let inner = Rc::new(ClientSessionInner {
        outgoing_activity: handle.outgoing_transaction_activity(),
        handle,
        local_address: connection
            .local_endpoint()
            .socket_address()
            .cloned()
            .unwrap_or_default(),
        peer_address: connection
            .peer_endpoint()
            .socket_address()
            .cloned()
            .unwrap_or_default(),
        connection,
        protocol,
        tls_session_reused,
        worker_id: context.worker_id(),
        worker: context.handle().clone(),
        cancellation: local_cancellation,
        attachment_cancellation: cancellation,
        completion,
        transport_retirement,
        started: std::time::Instant::now(),
        active_transactions: Cell::new(0),
        peer_full: Cell::new(false),
        idle: Cell::new(false),
        draining: Cell::new(false),
        finished: Cell::new(false),
        availability: RefCell::new(None),
    });
    if let Some(observer) = session.lifecycle_observer {
        driver.add_lifecycle_observer(observer)?;
    }
    driver.add_lifecycle_observer(Rc::new(SessionLifecycle {
        session: Rc::downgrade(&inner),
    }))?;
    check_attachment_admission(context, &inner.attachment_cancellation)?;
    driver
        .start(Box::new(owner))
        .map_err(|error| client_error(HttpErrorCode::DROPPED, error.to_string()))?;
    Ok(ClientSession { inner })
}

/// The client's side of a started session. The session's run task observes
/// the attachment's and the session's cancellation and worker quiescence
/// through it, then hands it the outcome to publish. The transport handle
/// drops before the outcome is published, or before the completion closes
/// when the run task drops this owner without an outcome.
struct ClientSessionOwner {
    external: CancellationWakeRegistration,
    local: CancellationWakeRegistration,
    quiescing: WorkerQuiescing,
    context: WorkerContext,
    handle: UniplexHandle,
    transport: Rc<dyn UniplexTransport>,
    draining: bool,
    aborting: bool,
    failure: Option<HttpError>,
    completed: SessionCompletionPublisher,
}

impl UniplexRunOwner for ClientSessionOwner {
    /// Attachment cancellation wins over session cancellation, and either
    /// wins over quiescence, which a cancellation still follows.
    fn poll_stop(&mut self, context: &mut Context<'_>) {
        loop {
            if !self.aborting && self.external.update_waker(context.waker()) {
                self.abort(b"attached session cancelled");
            } else if !self.aborting && self.local.update_waker(context.waker()) {
                self.abort(b"client session cancelled");
            } else if !self.draining && Pin::new(&mut self.quiescing).poll(context).is_ready() {
                self.draining = true;
                self.failure = self.stop(false);
                continue;
            }
            return;
        }
    }

    fn finish(mut self: Box<Self>, outcome: Result<UniplexRunSummary, HttpError>) {
        let result = self.failure.take().map_or(outcome, Err);
        let completed = std::mem::replace(&mut self.completed, SessionCompletionPublisher(None));
        drop(self);
        completed.publish(result);
    }
}

impl ClientSessionOwner {
    fn abort(&mut self, message: &[u8]) {
        self.aborting = true;
        self.draining = true;
        self.failure = Some(client_error(HttpErrorCode::CORO_CANCELLED, message));
        let _ = self.stop(true);
    }

    fn stop(&self, reset: bool) -> Option<HttpError> {
        let drained = catch_unwind(AssertUnwindSafe(|| {
            if self.handle.is_finished() {
                return Ok(false);
            }
            if !reset {
                match self.handle.protocol() {
                    Ok(_) => {}
                    Err(error) if error.code() == HttpErrorCode::DROPPED => return Ok(false),
                    Err(error) => return Err(error),
                }
            }
            self.handle.initiate_drain()
        }));
        let failure = match drained {
            Ok(Ok(_)) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some(client_error(
                HttpErrorCode::INTERNAL_ERROR,
                b"session drain callback panicked",
            )),
        };
        if reset || failure.is_some() {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                self.transport.close_with_reset(&self.context)
            }));
        }
        failure
    }
}

fn notify_tls_connection(stats: Option<&Arc<dyn TlsConnectionStats>>, full_handshake: bool) {
    if let Some(stats) = stats {
        let _ = catch_unwind(AssertUnwindSafe(|| stats.on_connection(full_handshake)));
    }
}

fn notify_tls_error(stats: Option<&Arc<dyn TlsConnectionStats>>, verification_error: bool) {
    if let Some(stats) = stats {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            stats.on_connection_error(verification_error)
        }));
    }
}

impl HttpConnector {
    pub fn connect(
        context: &WorkerContext,
        address: SocketAddress,
        timeout: Duration,
        connection: ConnectionParams,
        session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        <Self as SessionConnector>::connect(
            &Self,
            context.clone(),
            address,
            timeout,
            connection,
            session,
            cancellation,
        )
    }

    pub fn happy_eyeballs_connect(
        context: &WorkerContext,
        primary: SocketAddress,
        fallback: SocketAddress,
        timeout: Duration,
        delay: Duration,
        connection: ConnectionParams,
        session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        Self::happy_eyeballs_connect_with(
            Rc::new(Self),
            context,
            primary,
            fallback,
            timeout,
            delay,
            connection,
            session,
            cancellation,
        )
    }

    pub(crate) fn happy_eyeballs_connect_with(
        connector: Rc<dyn SessionConnector>,
        context: &WorkerContext,
        primary: SocketAddress,
        fallback: SocketAddress,
        timeout: Duration,
        delay: Duration,
        connection: ConnectionParams,
        session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        let context = context.clone();
        Box::pin(async move {
            let primary_future = connector.connect(
                context.clone(),
                primary,
                timeout,
                connection.clone(),
                session.clone(),
                cancellation.clone(),
            );
            tokio::pin!(primary_future);
            let delay_future = tokio::time::sleep(delay);
            tokio::pin!(delay_future);
            let primary_failed = tokio::select! {
                primary = &mut primary_future => match primary {
                    Ok(session) => return Ok(session),
                    Err(_) => true,
                },
                () = &mut delay_future => false,
            };
            let fallback_future = connector.connect(
                context,
                fallback,
                timeout,
                connection,
                session,
                cancellation,
            );
            tokio::pin!(fallback_future);
            if primary_failed {
                return fallback_future.await;
            }
            tokio::select! {
                primary = &mut primary_future => match primary {
                    Ok(session) => Ok(session),
                    Err(_) => fallback_future.await,
                },
                fallback = &mut fallback_future => match fallback {
                    Ok(session) => Ok(session),
                    Err(fallback_error) => match primary_future.await {
                        Ok(session) => Ok(session),
                        Err(_) => Err(fallback_error),
                    },
                },
            }
        })
    }
}

/// The HTTP/2 session configuration `session` selects for a client session.
pub(crate) fn http2_session_config(session: &SessionParams) -> Http2SessionConfig {
    let settings = normalized_http2_settings(&session.settings);
    let stream_receive_window = settings
        .iter()
        .rev()
        .find(|setting| setting.identifier == 4)
        .and_then(|setting| u32::try_from(setting.value).ok())
        .unwrap_or(DEFAULT_STREAM_FLOW_CONTROL);
    Http2SessionConfig {
        settings,
        connection_receive_window: session
            .connection_flow_control
            .unwrap_or(DEFAULT_CONNECTION_FLOW_CONTROL),
        stream_receive_window,
        max_concurrent_outgoing_streams: session
            .max_concurrent_outgoing_streams
            .filter(|maximum| *maximum != 0)
            .unwrap_or(100),
        stats: session.session_stats.as_ref().map(|stats| {
            Rc::new(Http2ClientSessionStats(Rc::clone(stats))) as Rc<dyn Http2SessionStats>
        }),
        header_codec_stats: session.header_codec_stats.clone(),
        header_indexing_strategy: session.header_indexing_strategy.clone(),
        ping_probes: session.ping_probes,
        ..Http2SessionConfig::default()
    }
}

fn normalized_http2_settings(configured: &[CodecSetting]) -> Vec<CodecSetting> {
    let mut settings = vec![CodecSetting {
        identifier: 4,
        value: u64::from(DEFAULT_STREAM_FLOW_CONTROL),
    }];
    configured.iter().for_each(|setting| {
        if let Some(existing) = settings
            .iter_mut()
            .find(|existing| existing.identifier == setting.identifier)
        {
            existing.value = setting.value;
        } else {
            settings.push(*setting);
        }
    });
    settings
}
