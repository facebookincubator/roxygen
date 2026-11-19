/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tokio-facing runtime owner, client handle, connector contract, and bounded admission.

use std::error::Error;
use std::future::Future;
use std::hash::Hash;
use std::net::Ipv6Addr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc as synchronous;
use std::thread;
use std::time::Instant;

use http::Method;
use http::Request;
use roxygen_connect::stream::RequestHeaderMap;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::TransportEndpoint;
use crate::TransportSecurity;
use crate::error::ConnectError;
use crate::options::ConnectOptions;
use crate::request::forbidden_header;
use crate::stats::ConnectStats;
use crate::stats::GaugeHold;
use crate::stats::Recorder;
use crate::tunnel::PendingTunnel;
use crate::worker;

/// Established I/O that a connector returns.
pub trait ProxyIo: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<Io> ProxyIo for Io where Io: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

/// An established, authenticated proxy transport that negotiated HTTP/2.
///
/// Endpoints describe the immediate proxy hop, not the CONNECT target.
pub struct ConnectedIo {
    pub io: Box<dyn ProxyIo>,
    pub local_endpoint: TransportEndpoint,
    pub peer_endpoint: TransportEndpoint,
    pub security: TransportSecurity,
}

/// Why a connector produced no HTTP/2 transport.
#[derive(Debug, thiserror::Error)]
pub enum DialError {
    /// The proxy completed TLS without negotiating h2. The route key fails at once with
    /// `H2NotNegotiated` for `h2_unsupported_ttl`.
    #[error("proxy did not negotiate HTTP/2")]
    H2NotNegotiated,
    /// TCP, TLS, certificate, or other connector failure; never cached.
    #[error("proxy connector failed: {0}")]
    Other(#[source] Arc<dyn Error + Send + Sync>),
}

/// The application's immutable proxy routing, TLS, ALPN, and socket policy for one route key.
///
/// The future is created, polled, and dropped on the protocol worker's Tokio runtime. It must be
/// cancellation-safe and spawn no detached work. The adapter bounds each dial and TLS handshake
/// by `acquire_timeout`.
pub trait Connector: Send + Sync + 'static {
    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>>;

    /// The immediate-hop security every transport of this connector reports.
    ///
    /// The protocol worker calls it for every connect, so it must return at once. A connect whose
    /// connector reports other security than the live route of its key fails with an uncommitted
    /// `InvalidRequest` before anything is sent.
    fn security(&self) -> TransportSecurity {
        TransportSecurity::Tls
    }
}

/// Owns the protocol worker shared by every route of its client handles.
///
/// Construction, shutdown, and drop block, so they run outside asynchronous Tokio workers, for
/// example in `spawn_blocking`. On the protocol worker's own thread, as when a connector that owns
/// the runtime is dropped there, shutdown and drop only start the shutdown. Client handles do not
/// keep the runtime alive.
pub struct ConnectRuntime {
    worker: Option<HostedWorker>,
    stop: CancellationSource,
    completion: synchronous::Receiver<Result<(), ConnectError>>,
    stats: Arc<Recorder>,
}

impl ConnectRuntime {
    pub(crate) fn worker_thread_id(&self) -> Option<thread::ThreadId> {
        self.worker
            .as_ref()
            .map(|worker| worker.handle().owner_thread())
    }

    /// Validates `options` and starts the protocol worker without opening any connection.
    pub fn new<K>(options: ConnectOptions) -> Result<(Self, ConnectClient<K>), ConnectError>
    where
        K: Eq + Hash + Clone + Send + 'static,
    {
        options.validate()?;
        let worker = HostedWorker::spawn(HostedWorkerOptions {
            command_capacity: NonZeroUsize::MIN,
            thread_name: "roxygen-connect".to_owned(),
            on_owner_thread_stop: None,
        })
        .map_err(|error| ConnectError::closed(false).with_source(error))?;
        // Every queued command holds an admission permit, so admission never finds it full.
        let (commands, requests) = mpsc::channel(options.max_tunnels);
        let stats = Arc::new(Recorder::default());
        let (completed, completion) = synchronous::sync_channel(1);
        let (initialized, initialization) = synchronous::sync_channel(1);
        let stop = CancellationSource::new();
        let owner_stop = stop.token();
        let owner_stats = Arc::clone(&stats);
        let owner_options = options.clone();
        worker
            .handle()
            .submit(move |context| {
                let owner = context.clone();
                let scope = context.register_scope(async move {
                    let result =
                        worker::run(owner, requests, owner_options, owner_stop, owner_stats).await;
                    let _ = completed.send(result);
                });
                let result = scope.and_then(|scope| scope.activate()).map(drop);
                let _ = initialized.send(result);
            })
            .map_err(|error| ConnectError::closed(false).with_source(error))?;
        initialization
            .recv()
            .map_err(|_| ConnectError::closed(false))?
            .map_err(|error| ConnectError::closed(false).with_source(error))?;
        let client = ConnectClient {
            commands,
            admission: Arc::new(Semaphore::new(options.max_tunnels)),
            max_request_bytes: options.max_request_bytes,
            stats: Arc::clone(&stats),
        };
        Ok((
            Self {
                worker: Some(worker),
                stop,
                completion,
                stats,
            },
            client,
        ))
    }

    /// Closes admission, drains every route for at most `shutdown_timeout`, then aborts the
    /// remaining tunnels, cancels the sessions, and joins the worker.
    ///
    /// Returns an error only for a failed join or a panic of the worker or one of its tasks;
    /// session failures are counted in `ConnectStats::shutdown_errors`. On the worker's own thread
    /// it returns without joining.
    pub fn shutdown(mut self) -> Result<(), ConnectError> {
        self.join()
    }

    pub fn stats(&self) -> ConnectStats {
        self.stats.snapshot()
    }

    fn join(&mut self) -> Result<(), ConnectError> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        self.stop.request_cancellation();
        // The owner thread cannot wait for itself; dropping the worker there starts its shutdown
        // without a join (`DIV-003`).
        if worker.handle().owner_thread() == thread::current().id() {
            return Ok(());
        }
        let owner = self.completion.recv().map_err(|_| {
            ConnectError::closed(true).with_source("CONNECT worker ended without a result")
        });
        let completion = worker.completion();
        worker.shutdown();
        drop(worker);
        let joined = completion.borrow().clone();
        match joined {
            Some(Ok(())) => owner?,
            Some(Err(error)) => Err(ConnectError::closed(true).with_source(error)),
            None => Err(ConnectError::closed(true).with_source("CONNECT worker was not joined")),
        }
    }
}

impl Drop for ConnectRuntime {
    fn drop(&mut self) {
        let _ = self.join();
    }
}

/// A cloneable, transferable connect handle. Equal keys must name the same proxy, TLS
/// configuration, and socket policy: a key's first connector serves its route until the route
/// retires, and a later connector whose `security()` differs fails its connect with
/// `InvalidRequest`.
pub struct ConnectClient<K> {
    commands: mpsc::Sender<worker::Command<K>>,
    admission: Arc<Semaphore>,
    max_request_bytes: usize,
    stats: Arc<Recorder>,
}

impl<K> Clone for ConnectClient<K> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            admission: Arc::clone(&self.admission),
            max_request_bytes: self.max_request_bytes,
            stats: Arc::clone(&self.stats),
        }
    }
}

impl<K> ConnectClient<K>
where
    K: Eq + Hash + Clone + Send + 'static,
{
    /// Establishes a tunnel through the route of `key` and returns it once the proxy answered
    /// 2xx, before its data path is chosen.
    ///
    /// A provably unprocessed CONNECT is retried up to `max_unprocessed_retries` times; an
    /// established tunnel is never replayed. Dropping the future cancels only this connect and
    /// resets its stream if the request committed. Authorization, Proxy-Authorization, Cookie,
    /// and Set-Cookie values never enter the session's HPACK dynamic table; they are sent as
    /// literals without indexing, not as never-indexed literals. A sensitivity flag on any other
    /// header, a duplicate header name, and HTTP/1 connection or framing headers are refused.
    pub async fn connect(
        &self,
        key: K,
        request: Request<()>,
        connector: Arc<dyn Connector>,
    ) -> Result<PendingTunnel, ConnectError> {
        self.stats.connects.add();
        let request = validate_request(request, self.max_request_bytes).inspect_err(|_| {
            self.stats.invalid_request.add();
        })?;
        if self.commands.is_closed() {
            return Err(ConnectError::closed(false));
        }
        let Ok(permit) = Arc::clone(&self.admission).try_acquire_owned() else {
            if self.commands.is_closed() {
                return Err(ConnectError::closed(false));
            }
            self.stats.overloaded.add();
            return Err(ConnectError::overloaded());
        };
        self.stats.admitted.add();
        let mut cancellation = CancelOnDrop {
            source: Arc::new(CancellationSource::new()),
            armed: true,
        };
        let (reply, result) = oneshot::channel();
        let command = worker::Command {
            key,
            request,
            connector,
            cancellation: Arc::clone(&cancellation.source),
            admitted: Instant::now(),
            pending: worker::Pending::new(
                reply,
                permit,
                GaugeHold::new(Arc::clone(&self.stats), |stats| &stats.waiters),
                Arc::clone(&self.stats),
            ),
        };
        if let Err(error) = self.commands.try_send(command) {
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => {
                    self.stats.overloaded.add();
                    ConnectError::overloaded()
                }
                mpsc::error::TrySendError::Closed(_) => ConnectError::closed(false),
            });
        }
        let pending = result.await.map_err(|_| ConnectError::closed(true))??;
        cancellation.armed = false;
        pending.received(&self.stats);
        Ok(pending)
    }

    pub fn stats(&self) -> ConnectStats {
        self.stats.snapshot()
    }
}

/// Cancels an unfinished connect or relay when its future drops.
pub(crate) struct CancelOnDrop {
    pub(crate) source: Arc<CancellationSource>,
    pub(crate) armed: bool,
}

impl CancelOnDrop {
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.source.request_cancellation();
        }
    }
}

/// A validated CONNECT: its authority and the headers it sends besides `:authority`.
pub(crate) struct NativeRequest {
    pub(crate) authority: Box<str>,
    pub(crate) headers: RequestHeaderMap,
}

pub(crate) fn credential_header(name: &[u8]) -> bool {
    matches!(
        name,
        b"authorization" | b"proxy-authorization" | b"cookie" | b"set-cookie"
    )
}

fn validate_request(request: Request<()>, maximum: usize) -> Result<NativeRequest, ConnectError> {
    let invalid = ConnectError::invalid_request;
    if request.method() != Method::CONNECT {
        return Err(invalid("method must be CONNECT"));
    }
    if request.uri().scheme().is_some() || request.uri().path_and_query().is_some() {
        return Err(invalid("URI must contain only an authority"));
    }
    let authority = request
        .uri()
        .authority()
        .ok_or_else(|| invalid("URI must contain an authority"))?;
    let host = authority.host();
    if host.is_empty() || authority.as_str().contains('@') {
        return Err(invalid("authority must have a host and no userinfo"));
    }
    let bracketed_ipv6 = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .map(|address| address.parse::<Ipv6Addr>().is_ok());
    if bracketed_ipv6 == Some(false) || (bracketed_ipv6.is_none() && host.contains(':')) {
        return Err(invalid("an IPv6 host must be bracketed"));
    }
    if !authority.port_u16().is_some_and(|port| port != 0) {
        return Err(invalid("authority must have a nonzero port"));
    }
    if !request.extensions().is_empty() {
        return Err(invalid("application extensions are unsupported"));
    }
    let mut size = authority.as_str().len();
    let mut headers = RequestHeaderMap::new();
    for name in request.headers().keys() {
        let mut values = request.headers().get_all(name).iter();
        let value = values
            .next()
            .expect("should list only present header names");
        if values.next().is_some() {
            return Err(invalid("duplicate header names are unsupported"));
        }
        if forbidden_header(name) {
            return Err(invalid(
                "HTTP/1 connection and body framing headers are refused",
            ));
        }
        if value.is_sensitive() && !credential_header(name.as_str().as_bytes()) {
            return Err(invalid(
                "sensitivity flags are accepted only on credential headers",
            ));
        }
        let value = value.as_bytes();
        size = size
            .saturating_add(name.as_str().len())
            .saturating_add(value.len());
        if name == http::header::HOST {
            if value != authority.as_str().as_bytes() {
                return Err(invalid("Host must equal the CONNECT authority"));
            }
            continue;
        }
        if value.first().is_some_and(u8::is_ascii_whitespace)
            || value.last().is_some_and(u8::is_ascii_whitespace)
        {
            return Err(invalid("header boundary whitespace is unsupported"));
        }
        headers.insert(name.as_str().as_bytes().into(), value.into());
    }
    if size > maximum {
        return Err(invalid("request metadata exceeds max_request_bytes"));
    }
    Ok(NativeRequest {
        authority: authority.as_str().into(),
        headers,
    })
}
