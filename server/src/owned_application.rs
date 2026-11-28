/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Hosts an application's own `OwnedApplication` on `HttpServer`'s workers.
//!
//! [`HttpServer::start_owned`](crate::server::HttpServer::start_owned) starts the workers with
//! an [`OwnedApplicationFactory`], which each worker calls once. The server accepts each
//! connection, runs its TLS handshake and admission, and hands it to the worker's
//! [`OwnedConnectionFactory::attach`], which serves it on an owned driver with
//! [`OwnedConnectionAttach::serve_http2`] or [`OwnedConnectionAttach::serve_http1`]. The
//! server keeps draining and stopping the connection as it does its own, and calls
//! [`OwnedConnectionFactory::worker_stopping`] once a stopping worker has no session left.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::ThreadId;

use roxygen_core::connection::ConnectionContext;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::http::CodecProtocol;
use roxygen_runtime::worker::WorkerClosed;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerPollStats;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::owned_h1::OwnedH1Handle;
use roxygen_session::owned_h2::OwnedH2Handle;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexTransport;
use roxygen_transport::tls::TlsCloseControl;
use tokio::sync::watch;

use crate::acceptor::ConnectionId;
use crate::acceptor::ConnectionInfo;
use crate::acceptor::RunFinished;
use crate::acceptor::ServerSession;
use crate::acceptor::ServerSessionConfig;
use crate::acceptor::SessionControl;
use crate::acceptor::hosted_http1_session;
use crate::acceptor::hosted_http2_session;

/// Builds each worker's connection factory, on that worker, with its context and index. An
/// error or a panic fails `start_owned` with `WorkerStartup`; a panic is also recorded as the
/// worker's callback failure.
pub type OwnedApplicationFactory = Arc<
    dyn Fn(WorkerContext, usize) -> Result<Rc<dyn OwnedConnectionFactory>, HttpError> + Send + Sync,
>;

/// One worker's factory of owned connections.
pub trait OwnedConnectionFactory: 'static {
    /// Serves one accepted connection on this worker, after its TLS handshake and admission:
    /// the factory calls one `serve_*` method of `connection` and returns its result. If the
    /// server is draining, it drains the returned session before the session first runs.
    ///
    /// An error closes only this connection and records no failure. A panic, including one in
    /// the `make` closure given to `serve_*`, also closes only this connection; it is recorded
    /// as the worker's callback failure (`ServerCallbackStage::HandlerFactory`), which the
    /// server's stop then returns. The worker keeps serving other connections in either case.
    fn attach(&self, connection: OwnedConnectionAttach<'_>) -> Result<OwnedAttached, HttpError>;

    /// Serves one accepted HTTP/3 connection on this worker, after its admission: the factory
    /// calls [`Http3ConnectionAttach::serve`](crate::quic::Http3ConnectionAttach::serve) and
    /// returns its result. An error refuses the connection with CONNECTION_REFUSED; so does a
    /// panic, which is recorded as for `attach`. By default every HTTP/3 connection is refused.
    /// Available on Linux with the `http3` feature only.
    #[cfg(all(target_os = "linux", feature = "http3"))]
    fn attach_http3(
        &self,
        connection: crate::quic::Http3ConnectionAttach<'_>,
    ) -> Result<crate::quic::Http3Attached, HttpError> {
        let _ = connection;
        Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"the application serves no HTTP/3",
        ))
    }

    /// Called on the worker as soon as its session set is empty (after a graceful drain, or
    /// after a forced stop has aborted and joined every session), before any further shutdown
    /// work. The worker thread exits after the returned future resolves, or after `abandon`
    /// fires: the worker then drops the future on this thread, possibly before its first poll.
    /// The future must therefore cancel and join every task it owns in its `Drop`,
    /// synchronously (for a `TaskGroup`: `abort_all`, then `try_join_next` until it returns
    /// `None`, which also consumes tracked completions), must not block in `Drop`, and must not
    /// rely on teardown code that runs only once the future is polled.
    ///
    /// The worker checks `abandon` before every poll, including the first: an abandon that has
    /// already fired drops the future unpolled. `HttpServer::force_stop()` and dropping
    /// `HttpServer` abandon before they stop the workers, so on those paths the future is
    /// always created already abandoned and is dropped without being polled.
    ///
    /// A panic in this call, in a poll of the future or in its drop is recorded as the worker's
    /// callback failure (`ServerCallbackStage::HandlerFactory`), and the worker stops.
    fn worker_stopping(&self, abandon: StopAbandon) -> Option<Pin<Box<dyn Future<Output = ()>>>> {
        let _ = abandon;
        None
    }
}

/// Resolves when the server's owner abandons graceful worker stop: `HttpServer::force_stop`,
/// dropping `HttpServer`, or `HttpServerControl::abandon_stopping`. Once fired, it stays
/// fired.
#[derive(Clone)]
pub struct StopAbandon {
    abandoned: watch::Receiver<bool>,
}

impl StopAbandon {
    pub(crate) fn new(abandoned: watch::Receiver<bool>) -> Self {
        Self { abandoned }
    }

    /// Whether the server's owner has abandoned graceful worker stop.
    pub fn is_abandoned(&self) -> bool {
        *self.abandoned.borrow()
    }

    /// Resolves once the server's owner abandons graceful worker stop, at once if it already
    /// has.
    pub async fn abandoned(&self) {
        let mut abandoned = self.abandoned.clone();
        if abandoned.wait_for(|abandoned| *abandoned).await.is_err() {
            // The server is gone without abandoning.
            std::future::pending::<()>().await;
        }
    }
}

/// An accepted connection, for [`OwnedConnectionFactory::attach`] to serve.
pub struct OwnedConnectionAttach<'a> {
    worker: &'a WorkerContext,
    worker_index: usize,
    id: ConnectionId,
    info: &'a ConnectionInfo,
    session: &'a ServerSessionConfig,
    transport: Rc<dyn UniplexTransport>,
    io: UniplexConfig,
    tls_close: Option<TlsCloseControl>,
}

impl<'a> OwnedConnectionAttach<'a> {
    pub(crate) fn new(
        worker: &'a WorkerContext,
        worker_index: usize,
        id: ConnectionId,
        info: &'a ConnectionInfo,
        session: &'a ServerSessionConfig,
        transport: Rc<dyn UniplexTransport>,
        io: UniplexConfig,
    ) -> Self {
        Self {
            worker,
            worker_index,
            id,
            info,
            session,
            transport,
            io,
            tls_close: None,
        }
    }

    /// Sets the control of how the connection's TLS close ends, for a TLS connection.
    pub(crate) fn with_tls_close(self, tls_close: Option<TlsCloseControl>) -> Self {
        Self { tls_close, ..self }
    }

    /// The worker that serves the connection.
    pub fn worker(&self) -> &WorkerContext {
        self.worker
    }

    /// The index of the worker that serves the connection.
    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    /// The connection's identity, as `ConnectionLifecycleObserver` reports it.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// The connection's addresses, protocol and TLS facts.
    pub fn info(&self) -> &ConnectionInfo {
        self.info
    }

    /// The connection's metadata, including the DER of the client's certificate when its TLS
    /// handshake verified one.
    pub fn context(&self) -> &Arc<ConnectionContext> {
        &self.info.context
    }

    /// The protocol the connection negotiated, or the server's plaintext protocol.
    pub fn protocol(&self) -> CodecProtocol {
        self.info.protocol
    }

    /// Serves the connection on the owned HTTP/2 driver with the application that `make`
    /// builds from the connection's control. `make` runs before the session is first polled.
    pub fn serve_http2<A: OwnedApplication>(
        self,
        make: impl FnOnce(OwnedConnectionControl) -> A,
    ) -> Result<OwnedAttached, HttpError> {
        if self.info.protocol != CodecProtocol::Http2 {
            return Err(wrong_protocol());
        }
        let (session, control) = hosted_http2_session(
            self.worker,
            self.transport,
            self.io,
            self.session,
            |handle: &OwnedH2Handle, finished: &RunFinished| {
                make(OwnedConnectionControl {
                    handle: ControlHandle::Http2(handle.clone(), finished.clone()),
                    tls_close: self.tls_close,
                })
            },
        )?;
        Ok(OwnedAttached { session, control })
    }

    /// Serves the connection on the owned HTTP/1.1 driver with the driver configuration `http1`
    /// and the application that `make` builds from the connection's control. `make` runs
    /// before the session is first polled.
    ///
    /// Every field of `http1` that has a `ServerSessionConfig` twin is overwritten with the
    /// session's value: today only `deadlines`, from `ServerSessionConfig::owned_deadlines`.
    /// The caller sets the rest, such as the upgrade policy.
    pub fn serve_http1<A: OwnedApplication>(
        self,
        http1: Http1DriverConfig,
        make: impl FnOnce(OwnedConnectionControl) -> A,
    ) -> Result<OwnedAttached, HttpError> {
        if self.info.protocol != CodecProtocol::Http11 {
            return Err(wrong_protocol());
        }
        let (session, control) = hosted_http1_session(
            self.worker,
            self.transport,
            self.io,
            self.session,
            http1,
            |handle: &OwnedH1Handle| {
                make(OwnedConnectionControl {
                    handle: ControlHandle::Http1(handle.clone()),
                    tls_close: self.tls_close,
                })
            },
        )?;
        Ok(OwnedAttached { session, control })
    }
}

fn wrong_protocol() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"the connection negotiated another protocol",
    )
}

/// A connection that [`OwnedConnectionAttach`] has started to serve, for
/// [`OwnedConnectionFactory::attach`] to return.
pub struct OwnedAttached {
    session: ServerSession,
    control: SessionControl,
}

impl OwnedAttached {
    pub(crate) fn into_parts(self) -> (ServerSession, SessionControl) {
        (self.session, self.control)
    }
}

/// Owner-local control of one hosted connection.
#[derive(Clone)]
pub struct OwnedConnectionControl {
    handle: ControlHandle,
    /// How a TLS connection's graceful close ends; `None` for plaintext.
    tls_close: Option<TlsCloseControl>,
}

#[derive(Clone)]
enum ControlHandle {
    Http1(OwnedH1Handle),
    /// The handle and the server's flag that the run has ended. A run that unwound out of a
    /// poll never marks its handle finished, because the server drops it at once.
    Http2(OwnedH2Handle, RunFinished),
}

impl OwnedConnectionControl {
    /// Drains the connection: an HTTP/2 session sends GOAWAY and admits no new stream, and an
    /// HTTP/1.1 session closes once its current exchange ends. Returns whether this call changed
    /// the session's state, which it never does once the session has finished.
    pub fn drain(&self) -> Result<bool, HttpError> {
        match &self.handle {
            ControlHandle::Http1(handle) => handle.initiate_drain(),
            ControlHandle::Http2(_, finished) if finished.get() => Ok(false),
            ControlHandle::Http2(handle, _) => handle.initiate_drain(),
        }
    }

    /// Fails the connection with `CORO_CANCELLED` at once: it sends no GOAWAY, response or
    /// close, and every exchange's `cancel` sees `CORO_CANCELLED`. The session then resets the
    /// transport, which closes as `ServerSessionConfig::non_abortive_close` describes: with
    /// `SO_LINGER` zero by default, so the peer sees a reset, or without it when that option is
    /// set, so the peer reads the bytes already written and then EOF, or sees a reset if
    /// received bytes are unread. On TLS, that EOF comes with no close_notify, and ciphertext
    /// that the socket has not taken yet is discarded.
    pub fn hard_close(&self) {
        match &self.handle {
            ControlHandle::Http1(handle) => handle.cancel(),
            ControlHandle::Http2(handle, _) => handle.cancel(),
        }
    }

    /// Whether the session has finished: true once its run has completed, with or without an
    /// error, including a run that panicked and a run that the server abandoned and finished in
    /// its cleanup.
    pub fn is_finished(&self) -> bool {
        match &self.handle {
            ControlHandle::Http1(handle) => handle.is_finished(),
            ControlHandle::Http2(handle, finished) => finished.get() || handle.is_finished(),
        }
    }

    /// Makes the connection's graceful close, once its last response is written, end with FIN
    /// and no TLS close_notify. It has no effect on a plaintext connection or on a close that has
    /// already started.
    pub fn skip_tls_close_notify(&self) {
        if let Some(tls_close) = &self.tls_close {
            tls_close.skip_close_notify();
        }
    }

    /// Whether the connection handshake has completed: an HTTP/2 session has processed the
    /// peer's first SETTINGS frame, or an HTTP/1.1 session has delivered its first request head.
    pub fn handshake_complete(&self) -> bool {
        match &self.handle {
            ControlHandle::Http1(handle) => handle.handshake_complete(),
            ControlHandle::Http2(handle, _) => handle.handshake_complete(),
        }
    }
}

/// A read-only view of one of `HttpServer`'s workers. It cannot stop the worker or give it work:
/// it exposes no `begin_shutdown`, `submit`, `spawn_local` or `call_local`.
#[derive(Clone)]
pub struct WorkerView {
    index: usize,
    handle: WorkerHandle,
}

impl WorkerView {
    pub(crate) fn new(index: usize, handle: WorkerHandle) -> Self {
        Self { index, handle }
    }

    /// The worker's index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The worker's driver poll counters, read from any thread.
    pub fn poll_stats(&self) -> WorkerPollStats {
        self.handle.poll_stats()
    }

    /// The worker's thread.
    pub fn owner_thread(&self) -> ThreadId {
        self.handle.owner_thread()
    }

    /// Resolves once the worker has closed.
    pub fn closed(&self) -> WorkerClosed {
        self.handle.closed()
    }
}
