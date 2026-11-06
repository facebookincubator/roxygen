//! Owner-local sessions on established transports, without listener ownership.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;

use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_transport::byte_stream::OwnedByteStream;
use tokio::sync::watch;

use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::AcceptedTransport;
use crate::acceptor::AcceptorConfig;
use crate::acceptor::NewConnectionFilter;
use crate::acceptor::ServerSession;
use crate::acceptor::ServerSessionConfig;
use crate::acceptor::SessionAttachment;
use crate::acceptor::SessionControl;
use crate::acceptor::attach_session;
use crate::acceptor::connection_info;

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct CompletionPauseForTest {
    pub(crate) entered: tokio::sync::oneshot::Sender<()>,
    pub(crate) release: tokio::sync::oneshot::Receiver<()>,
}

/// Explicit protocol, trusted security/routing data, and existing server policies.
/// No handshake or worker-lifecycle notifications are synthesized by attachment.
pub struct ServeTransportOptions {
    #[cfg(test)]
    pub(crate) completion_pause_for_test: Option<CompletionPauseForTest>,
    pub protocol: CodecProtocol,
    pub security: TransportSecurity,
    pub upstream_origin: Option<UpstreamOrigin>,
    pub session: ServerSessionConfig,
    pub new_connection_filter: Option<Arc<dyn NewConnectionFilter>>,
    pub connection_observer: Option<Arc<dyn AcceptedConnectionObserver>>,
    pub zero_copy_enable_threshold: usize,
    /// Cancels this connection and its exchanges, independently of worker admission.
    pub cancellation: CancellationToken,
}

impl ServeTransportOptions {
    /// Requires an explicit wire protocol and immediate-hop security assertion.
    pub fn new(protocol: CodecProtocol, security: TransportSecurity) -> Self {
        Self {
            #[cfg(test)]
            completion_pause_for_test: None,
            protocol,
            security,
            upstream_origin: None,
            session: ServerSessionConfig::default(),
            new_connection_filter: None,
            connection_observer: None,
            zero_copy_enable_threshold: 0,
            cancellation: CancellationToken::never(),
        }
    }
}

/// Owns admission and cancellation for one local downstream session.
/// Dropping it cancels; the worker still owns and joins the session cleanup.
pub struct ServerConnection {
    control: SessionControl,
    cancellation: CancellationSource,
    connection: Arc<ConnectionContext>,
    completion: watch::Receiver<Option<Result<UniplexRunSummary, HttpError>>>,
}

impl ServerConnection {
    /// Returns transferable data without transferring any session capability.
    pub fn connection_context(&self) -> &Arc<ConnectionContext> {
        &self.connection
    }

    /// Closes session admission and lets accepted exchanges finish.
    pub fn initiate_drain(&self) -> Result<bool, HttpError> {
        match catch_unwind(AssertUnwindSafe(|| self.control.try_drain())) {
            Ok(Ok(changed)) => Ok(changed),
            Ok(Err(error)) => {
                self.cancel();
                Err(error)
            }
            Err(_) => {
                self.cancel();
                Err(connection_error(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"session drain callback panicked",
                ))
            }
        }
    }

    /// Cancels owner-locally without ordinary worker admission.
    pub fn cancel(&self) {
        self.cancellation.request_cancellation();
        let _ = catch_unwind(AssertUnwindSafe(|| self.control.force_stop()));
    }

    #[cfg(test)]
    pub(crate) fn terminal_state_for_test(&self) -> (bool, bool) {
        (
            self.control.is_finished(),
            self.cancellation.token().is_cancellation_requested(),
        )
    }

    /// Observes session finalization. Dropping the observation never detaches work.
    /// Scheduled transport flush/shutdown is joined by runtime quiescence, not by
    /// this session-only receipt.
    pub fn wait_closed(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>> + 'static>> {
        let mut completion = self.completion.clone();
        Box::pin(async move {
            loop {
                if let Some(result) = completion.borrow().clone() {
                    return result;
                }
                completion.changed().await.map_err(|_| {
                    connection_error(
                        HttpErrorCode::INTERNAL_ERROR,
                        b"server session scope failed",
                    )
                })?;
            }
        })
    }
}

impl Drop for ServerConnection {
    fn drop(&mut self) {
        if self.completion.borrow().is_none() {
            let _ = catch_unwind(AssertUnwindSafe(|| self.cancel()));
        }
    }
}

/// Attaches a transport already established by the caller on this exact worker.
///
/// Admission filters and observers see actual endpoints and transport identity.
/// Immediate security and upstream origin are trusted configuration, never
/// inferred from headers. The caller retains its I/O reactor through runtime
/// quiescence. Attachment does not authenticate supplied identity or own a listener.
pub fn serve_transport(
    worker: &WorkerContext,
    transport: OwnedByteStream,
    handler: HttpHandlerRef,
    options: ServeTransportOptions,
) -> Result<ServerConnection, HttpError> {
    #[cfg(test)]
    let completion_pause = options.completion_pause_for_test;
    check_admission(worker, &options.cancellation)?;
    if !matches!(
        options.protocol,
        CodecProtocol::Http11 | CodecProtocol::Http2
    ) {
        return Err(connection_error(
            HttpErrorCode::CONNECT_ERROR,
            b"unsupported attachment protocol",
        ));
    }
    let limits = transport.limits(worker)?;
    let transport = AcceptedTransport::ByteStream(transport);
    let connection = Arc::new(
        transport
            .connection_context(worker, options.security, options.upstream_origin)?
            .with_http_protocol(options.protocol),
    );
    let cancellation = CancellationSource::new();
    let handler = Rc::new(AdmissionHandler {
        handler,
        worker: worker.handle().clone(),
        external: options.cancellation.clone(),
        local: cancellation.token(),
    });
    let config = AcceptorConfig {
        plaintext_protocol: options.protocol,
        session: options.session,
        connection_filter: options.new_connection_filter,
        tls: None,
        tls_observer: None,
        connection_observer: options.connection_observer,
        zero_copy_threshold: options.zero_copy_enable_threshold,
    };
    let failures = Arc::new(Mutex::new(None));
    let accepted = attach_session(
        transport,
        SessionAttachment {
            context: worker,
            handler,
            config: &config,
            connection: connection_info(options.protocol, Arc::clone(&connection)),
            draining: None,
            transport_limits: Some(limits),
            worker_index: 0,
            callback_failure: &failures,
        },
    )?;
    check_admission(worker, &options.cancellation)?;
    let (completed, completion) = watch::channel(None);
    let running = ServerSessionRun {
        context: worker.clone(),
        control: accepted.control.clone(),
        external: options.cancellation,
        local: cancellation.token(),
    };
    let registration = worker
        .register_scope(async move {
            let result = running.run(accepted.session).await;
            #[cfg(test)]
            if let Some(pause) = completion_pause {
                let _ = pause.entered.send(());
                let _ = pause.release.await;
            }
            let _ = completed.send_replace(Some(result));
        })
        .map_err(|error| connection_error(HttpErrorCode::DROPPED, error.to_string().as_bytes()))?;
    registration
        .activate()
        .map_err(|error| connection_error(HttpErrorCode::DROPPED, error.to_string().as_bytes()))?
        .detach();
    Ok(ServerConnection {
        control: accepted.control,
        cancellation,
        connection,
        completion,
    })
}

fn check_admission(
    worker: &WorkerContext,
    cancellation: &CancellationToken,
) -> Result<(), HttpError> {
    if !worker.is_owner_thread() {
        return Err(connection_error(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"server attachment requires its worker owner thread",
        ));
    }
    if cancellation.is_cancellation_requested() {
        return Err(connection_error(
            HttpErrorCode::CORO_CANCELLED,
            b"server attachment cancelled",
        ));
    }
    if !worker.handle().is_accepting() || worker.is_cancelled() {
        return Err(connection_error(
            HttpErrorCode::DROPPED,
            b"server admission closed",
        ));
    }
    Ok(())
}

pub(crate) fn bind_connection(
    handler: HttpHandlerRef,
    connection: Arc<ConnectionContext>,
) -> HttpHandlerRef {
    Rc::new(ConnectionHandler {
        handler,
        connection,
    })
}

struct ConnectionHandler {
    handler: HttpHandlerRef,
    connection: Arc<ConnectionContext>,
}

impl HttpHandler for ConnectionHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        match context.with_connection_context(Arc::clone(&self.connection)) {
            Ok(context) => self.handler.handle_request(context, request),
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }
}

struct AdmissionHandler {
    handler: HttpHandlerRef,
    worker: WorkerHandle,
    external: CancellationToken,
    local: CancellationToken,
}

impl HttpHandler for AdmissionHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        if !self.worker.is_accepting()
            || self.external.is_cancellation_requested()
            || self.local.is_cancellation_requested()
        {
            return Box::pin(std::future::ready(Err(connection_error(
                HttpErrorCode::DROPPED,
                b"server request admission closed",
            ))));
        }
        self.handler.handle_request(context, request)
    }
}

struct ServerSessionRun {
    context: WorkerContext,
    control: SessionControl,
    external: CancellationToken,
    local: CancellationToken,
}

impl ServerSessionRun {
    async fn run(self, session: ServerSession) -> Result<UniplexRunSummary, HttpError> {
        let mut running = session.run();
        let external_baton = CancellableBaton::new();
        let local_baton = CancellableBaton::new();
        let external = external_baton.wait(self.external.clone());
        let local = local_baton.wait(self.local.clone());
        let quiescing = self.context.quiescing();
        tokio::pin!(external, local, quiescing);
        let mut draining = false;
        let mut aborting = false;
        let mut failure = None;
        loop {
            tokio::select! {
                biased;
                _ = &mut external, if !aborting => {
                    aborting = true;
                    draining = true;
                    failure = Some(connection_error(HttpErrorCode::CORO_CANCELLED, b"server connection cancelled"));
                    let _ = self.stop(true);
                }
                _ = &mut local, if !aborting => {
                    aborting = true;
                    draining = true;
                    failure = Some(connection_error(HttpErrorCode::CORO_CANCELLED, b"server connection dropped or cancelled"));
                    let _ = self.stop(true);
                }
                () = &mut quiescing, if !draining => {
                    draining = true;
                    failure = self.stop(false);
                }
                result = &mut running => return failure.map_or(result, Err),
            }
        }
    }

    fn stop(&self, reset: bool) -> Option<HttpError> {
        if self.control.is_finished() {
            return None;
        }
        let result = catch_unwind(AssertUnwindSafe(|| self.control.try_drain()));
        let failure = match result {
            Ok(Ok(_)) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some(connection_error(
                HttpErrorCode::INTERNAL_ERROR,
                b"server drain callback panicked",
            )),
        };
        if reset || failure.is_some() {
            let _ = catch_unwind(AssertUnwindSafe(|| self.control.force_stop()));
        }
        failure
    }
}

fn connection_error(code: HttpErrorCode, message: &[u8]) -> HttpError {
    HttpError::with_message(code, message)
}
