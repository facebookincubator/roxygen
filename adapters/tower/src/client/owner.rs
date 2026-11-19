/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Caller-owned application execution and independently composed retirement.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::poll_fn;
use futures::stream::FuturesUnordered;
use http::Request;
use http::Response;
use http_body::Body;
use http_body::SizeHint;
use roxygen_client::connector::ClientAttachOptions;
use roxygen_client::connector::ClientSession;
use roxygen_client::connector::SessionParams;
use roxygen_client::pool::AttachedConnectFailure;
use roxygen_client::pool::HttpSessionPool;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::OriginSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http_adapter::NetworkHeadContext;
use roxygen_hyper_adapter::body::BodyCleanupObserver;
use roxygen_hyper_adapter::body::RoxygenBody;
use roxygen_hyper_adapter::source::ApplicationBodyCleanupObserver;
use roxygen_hyper_adapter::source::ApplicationBodyError;
use roxygen_hyper_adapter::source::ApplicationBodyJoinPreparation;
use roxygen_hyper_adapter::source::ApplicationBodySource;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::source_proxy::SourceAbortHandle;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::established::AdoptedIoControl;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local_with_completion;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tower_service::Service;

use super::AppFuture;
use super::BoxError;
use super::ClientBody;
use super::ClientError;
use super::ClientStartError;
use super::Shared;
use super::connector::ConnectRequest;
use super::connector::ConnectedIo;
use super::connector::ConnectorCommand;
use super::connector::Factory;
use super::connector::connector_failure;
use super::connector::failure;
use super::connector::security;
use crate::extensions::RetiringExtensions;
use crate::route::RouteSpec;

pub(crate) fn dispose<T>(value: T) -> bool {
    match catch_unwind(AssertUnwindSafe(|| drop(value))) {
        Ok(()) => true,
        Err(payload) => {
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                roxygen_runtime::panic::discard_panic(secondary);
            }
            false
        }
    }
}

pub(super) struct Guarded<F: Future> {
    future: Option<Pin<Box<F>>>,
    dispose_output: fn(F::Output) -> bool,
}

impl<F: Future> Guarded<F> {
    pub(super) fn new(future: F) -> Self {
        Self::with_output_disposer(future, dispose)
    }

    fn with_output_disposer(future: F, dispose_output: fn(F::Output) -> bool) -> Self {
        Self {
            future: Some(Box::pin(future)),
            dispose_output,
        }
    }
}

impl<F: Future> Future for Guarded<F> {
    type Output = Result<F::Output, ClientError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.future
                .as_mut()
                .expect("live future")
                .as_mut()
                .poll(context)
        }));
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => {
                if dispose(self.future.take()) {
                    Poll::Ready(Ok(output))
                } else {
                    (self.dispose_output)(output);
                    Poll::Ready(Err(ClientError::Panicked))
                }
            }
            Err(payload) => {
                dispose(payload);
                dispose(self.future.take());
                Poll::Ready(Err(ClientError::Panicked))
            }
        }
    }
}

impl<F: Future> Drop for Guarded<F> {
    fn drop(&mut self) {
        dispose(self.future.take());
    }
}

pub(super) struct Payload<T>(Option<T>, Option<Arc<AtomicBool>>);

impl<T> Payload<T> {
    pub(super) fn new(value: T) -> Self {
        Self(Some(value), None)
    }
    pub(super) fn tracked(value: T, failed: &Arc<AtomicBool>) -> Self {
        Self(Some(value), Some(Arc::clone(failed)))
    }
    pub(super) fn take(&mut self) -> T {
        self.0.take().expect("owned payload")
    }
    pub(super) fn get(&self) -> &T {
        self.0.as_ref().expect("owned payload")
    }
    pub(super) fn get_mut(&mut self) -> &mut T {
        self.0.as_mut().expect("owned payload")
    }
}

impl<T> Drop for Payload<T> {
    fn drop(&mut self) {
        if !dispose(self.0.take()) {
            if let Some(failed) = &self.1 {
                failed.store(true, Ordering::Release);
            }
        }
    }
}

pub(super) fn callback<T>(operation: impl FnOnce() -> T) -> Result<T, ClientError> {
    catch_unwind(AssertUnwindSafe(operation)).map_err(|payload| {
        dispose(payload);
        ClientError::Panicked
    })
}

pub(super) fn notify(shared: &Shared, operation: impl FnOnce()) {
    if callback(operation).is_err() {
        shared.notification_failed();
    }
}

fn deliver<T>(shared: &Shared, sender: oneshot::Sender<T>, value: T) -> bool {
    match callback(|| sender.send(value)) {
        Ok(Ok(())) => true,
        Ok(Err(value)) => {
            if !dispose(value) {
                shared.notification_failed();
            }
            false
        }
        Err(_) => {
            shared.notification_failed();
            false
        }
    }
}

fn publish<T>(shared: &Shared, sender: &watch::Sender<T>, value: T) {
    match callback(|| sender.send(value)) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            if !dispose(error.0) {
                shared.notification_failed();
            }
        }
        Err(_) => shared.notification_failed(),
    }
}

struct SafeWake {
    original: Payload<Waker>,
    shared: Weak<Shared>,
}
impl Wake for SafeWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if callback(|| self.original.get().wake_by_ref()).is_err() {
            if let Some(shared) = self.shared.upgrade() {
                shared.notification_failed();
            }
        }
    }
}

pub(super) struct Protected<F> {
    future: Pin<Box<F>>,
    shared: Weak<Shared>,
}
pub(super) fn protect<F: Future>(future: F, shared: Weak<Shared>) -> Protected<F> {
    Protected {
        future: Box::pin(future),
        shared,
    }
}
pub(super) fn safe_waker(context: &Context<'_>, shared: Weak<Shared>) -> Waker {
    let original = match callback(|| context.waker().clone()) {
        Ok(original) => original,
        Err(_) => {
            if let Some(owner) = shared.upgrade() {
                owner.notification_failed();
            }
            Waker::noop().clone()
        }
    };
    let original = match shared.upgrade() {
        Some(owner) => Payload::tracked(original, &owner.destruction_failed),
        None => Payload::new(original),
    };
    Waker::from(Arc::new(SafeWake { original, shared }))
}
impl<F: Future> Future for Protected<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = safe_waker(context, self.shared.clone());
        self.future.as_mut().poll(&mut Context::from_waker(&waker))
    }
}

pub(crate) struct Signal {
    cancelled: AtomicBool,
    notify: Notify,
    native: CancellationSource,
}

impl Signal {
    pub fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
            native: CancellationSource::new(),
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    pub fn token(&self) -> CancellationToken {
        self.native.token()
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.native.request_cancellation()))
        {
            dispose(payload);
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.notify.notify_waiters())) {
            dispose(payload);
        }
    }
    pub async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("exchange failed or its cleanup could not be confirmed")]
pub struct ClientCompletionError {
    #[source]
    failure: ClientError,
    unconfirmed: bool,
}

impl ClientCompletionError {
    pub fn cause(&self) -> &ClientError {
        &self.failure
    }
    pub fn cleanup_unconfirmed(&self) -> bool {
        self.unconfirmed
    }
    pub(super) fn failure(failure: ClientError, unconfirmed: bool) -> Self {
        Self {
            failure,
            unconfirmed,
        }
    }
}

/// The failure selected when the application-leaf snapshot was published.
///
/// A retained failure does not mean application tasks remain unjoined. This
/// snapshot does not wait for native completion or certify native/I/O retirement.
#[derive(Clone, Debug, thiserror::Error)]
#[error("application leaves failed to drain cleanly")]
pub struct ClientAppDrainError(#[source] pub ClientError);

/// A selected client failure reported when `ClientRun` returns.
///
/// This can retain an earlier pool/session failure, not only a failure first
/// encountered during shutdown. `PoolError::SessionJoin` is not a Tokio task
/// join failure and is not an alias for `ClientError::CleanupUnconfirmed`.
///
/// This type does not expose an independent run-wide cleanup-certainty result.
/// Preserve the selected cause and the separate execution-parent and retirement
/// outcomes; this error alone is not a certificate of complete cleanup.
#[derive(Clone, Debug, thiserror::Error)]
#[error("client shutdown failed or has unconfirmed cleanup")]
pub struct ClientShutdownError(#[source] pub ClientError);

type ExchangePublisher = watch::Sender<Option<Result<(), ClientCompletionError>>>;

/// Observation and cancellation only. Dropping an observer does not cancel.
#[derive(Clone)]
pub struct ClientExchange {
    pub(super) cancel: Arc<Signal>,
    pub(super) shared: Weak<Shared>,
    result: watch::Receiver<Option<Result<(), ClientCompletionError>>>,
}

impl ClientExchange {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    /// Includes upload-driver destruction, native exchange and scope retirement,
    /// and response-source retirement. Healthy pooled I/O has a separate lifetime.
    pub fn wait(&self) -> impl Future<Output = Result<(), ClientCompletionError>> + Send + 'static {
        let mut result = self.result.clone();
        protect(
            async move {
                loop {
                    if let Some(result) = result.borrow().clone() {
                        return result;
                    }
                    if result.changed().await.is_err() {
                        return Err(ClientCompletionError::failure(
                            ClientError::CleanupUnconfirmed,
                            true,
                        ));
                    }
                }
            },
            self.shared.clone(),
        )
    }
}

pub(super) fn new_exchange(shared: Weak<Shared>) -> (ClientExchange, ExchangePublisher) {
    let (publisher, result) = watch::channel(None);
    (
        ClientExchange {
            cancel: Arc::new(Signal::new()),
            shared,
            result,
        },
        publisher,
    )
}

/// Unique shutdown guard, not the parent of ClientRun or its application tasks.
pub struct ClientOwner {
    shared: Arc<Shared>,
    leaves: watch::Receiver<Option<Result<(), ClientAppDrainError>>>,
}

impl ClientOwner {
    pub(super) fn new(
        shared: Arc<Shared>,
        leaves: watch::Receiver<Option<Result<(), ClientAppDrainError>>>,
    ) -> Self {
        Self { shared, leaves }
    }
    /// Closes admission and requests cancellation of accepted exchanges.
    ///
    /// This is a signal, not a join. It does not promise response-body completion
    /// or a healthy `ClientRun` result. Keep the actual run driven on its captured
    /// application runtime and collect its result even after an operation fails.
    /// Continue collecting the separate retirement and physical-join outcomes;
    /// neither this call nor this shutdown guard replaces the run's execution owner.
    pub fn shutdown(&self) {
        self.shared.close();
    }
    /// Observes a one-time snapshot after the application leaves drain.
    ///
    /// Publication retains the failure selected at that boundary, if any. A
    /// later native failure does not revise an earlier `Ok(())`; repeated waits,
    /// including after `ClientRun` returns, observe the same published result.
    /// An error can retain a failure even though the application leaves drained.
    /// This does not certify native/source/I/O retirement or the enclosing run's
    /// parent join. Publisher loss before a snapshot reports cleanup uncertainty.
    pub fn wait_app_leaves_drained(
        &self,
    ) -> impl Future<Output = Result<(), ClientAppDrainError>> + Send + 'static {
        let mut leaves = self.leaves.clone();
        protect(
            async move {
                loop {
                    if let Some(result) = leaves.borrow().clone() {
                        return result;
                    }
                    if leaves.changed().await.is_err() {
                        return Err(ClientAppDrainError(ClientError::CleanupUnconfirmed));
                    }
                }
            },
            Arc::downgrade(&self.shared),
        )
    }
}

impl Drop for ClientOwner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Execution owned by the caller's bounded, live application lifetime scope.
/// Never abort/drop this join parent as a substitute for signalling shutdown.
/// Keep this future on the captured application runtime through its actual
/// parent join. Moving or destroying the parent elsewhere is unsupported.
///
/// Successful session joining does not imply a healthy run result. The output
/// may retain an earlier pool/session failure; this is distinct from failure
/// of the Tokio task driving this future.
///
/// Record operation/body results, exchange cleanup and this output separately.
/// This output does not replace native quiescence, physical worker joining or
/// the caller's actual reap of the execution task.
pub struct ClientRun {
    future: AppFuture<Result<(), ClientShutdownError>>,
    shared: Weak<Shared>,
}

impl Future for ClientRun {
    type Output = Result<(), ClientShutdownError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let shared = self.shared.upgrade();
        let waker = safe_waker(context, self.shared.clone());
        let result = self.future.as_mut().poll(&mut Context::from_waker(&waker));
        let destroyed = dispose(waker);
        if matches!(&result, Poll::Ready(Ok(())))
            && (!destroyed
                || shared.is_some_and(|shared| shared.destruction_failed.load(Ordering::Acquire)))
        {
            Poll::Ready(Err(ClientShutdownError(ClientError::Panicked)))
        } else {
            result
        }
    }
}

#[cfg(test)]
impl ClientRun {
    pub(crate) fn arm_terminal_drop(&mut self, armed: Arc<AtomicBool>) {
        let mut future = std::mem::replace(&mut self.future, Box::pin(std::future::pending()));
        self.future = Box::pin(async move {
            let result = future.as_mut().await;
            drop(future);
            armed.store(true, Ordering::Release);
            result
        });
    }
}

#[derive(Clone)]
struct ScopeReceipt {
    outcome: watch::Receiver<Option<Result<(), ClientError>>>,
    scope: WorkerCleanupObserver,
}

impl ScopeReceipt {
    async fn wait(self) -> Result<(), ClientError> {
        let mut outcome = self.outcome;
        let publication = async {
            loop {
                if let Some(result) = outcome.borrow().clone() {
                    return result;
                }
                if outcome.changed().await.is_err() {
                    return Err(ClientError::CleanupUnconfirmed);
                }
            }
        };
        let (publication, retirement) = futures::join!(publication, self.scope.wait());
        if retirement.is_err() {
            Err(ClientError::CleanupUnconfirmed)
        } else {
            publication
        }
    }
}

fn register_task(
    context: &WorkerContext,
    future: impl Future<Output = Result<(), ClientError>> + 'static,
) -> Result<ScopeReceipt, ClientError> {
    let (publisher, outcome) = watch::channel(None);
    let scope = context
        .register_scope(async move {
            let result = Guarded::new(future).await.and_then(|result| result);
            let _ = publisher.send(Some(result));
        })
        .map_err(|_| ClientError::Closed)?;
    let observer = scope.observe();
    drop(
        scope
            .activate()
            .map_err(|_| ClientError::CleanupUnconfirmed)?,
    );
    Ok(ScopeReceipt {
        outcome,
        scope: observer,
    })
}

pub(crate) struct Job {
    start: Box<dyn FnOnce(AppContext) -> AppFuture<AppFinished> + Send>,
    exchange: ClientExchange,
    publisher: ExchangePublisher,
    permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
struct AppContext {
    shared: Arc<Shared>,
    native: mpsc::Sender<NativeRequest>,
}

struct AppFinished {
    native: Option<ScopeReceipt>,
    source: Option<BodyCleanupObserver>,
    application: Option<ApplicationBodyCleanupObserver>,
    abort: Option<SourceAbortHandle<HttpError>>,
    failure: Option<ClientError>,
    unconfirmed: bool,
    #[cfg(test)]
    probe: Option<Arc<TestApplicationJoin>>,
}

impl AppFinished {
    fn failed(error: ClientError) -> Self {
        Self {
            native: None,
            source: None,
            application: None,
            abort: None,
            failure: Some(error),
            unconfirmed: false,
            #[cfg(test)]
            probe: None,
        }
    }
}

pub(super) fn request_job<B>(
    request: Request<B>,
    route: RouteSpec,
    exchange: ClientExchange,
    publisher: ExchangePublisher,
    response: oneshot::Sender<Result<Response<ClientBody>, ClientError>>,
    permit: OwnedSemaphorePermit,
) -> Job
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let observer = exchange.clone();
    Job {
        exchange,
        publisher,
        permit,
        start: Box::new(move |context| {
            Box::pin(run_request(context, request, route, observer, response))
        }),
    }
}

fn frame_http1_request<B: Body>(head: &mut HttpMessage, body: &B) -> Result<(), ClientError> {
    if head.headers().exists(HttpHeaderCode::ContentLength)
        || head.headers().exists(HttpHeaderCode::TransferEncoding)
        || body.is_end_stream()
    {
        return Ok(());
    }
    if head.http_version() >= (1, 1) {
        // The H1 codec otherwise auto-frames only POST. Chunking also preserves trailers.
        head.set_chunked(true);
        return Ok(());
    }
    if head.http_version() == (1, 0)
        && !head.headers().exists(HttpHeaderCode::Trailer)
        && let Some(length) = body.size_hint().exact()
    {
        head.headers_mut()
            .set(HttpHeaderCode::ContentLength, length.to_string());
        return Ok(());
    }
    Err(ClientError::Projection)
}

async fn receive_native_start<T>(
    mut start: oneshot::Receiver<Result<T, ClientError>>,
    closed: impl Future<Output = ()>,
) -> Result<T, ClientError> {
    tokio::select! {
        biased;
        result = &mut start => result.unwrap_or(Err(ClientError::CleanupUnconfirmed)),
        _ = closed => start.try_recv().unwrap_or(Err(ClientError::CleanupUnconfirmed)),
    }
}

async fn run_request<B>(
    context: AppContext,
    request: Request<B>,
    route: RouteSpec,
    exchange: ClientExchange,
    response: oneshot::Sender<Result<Response<ClientBody>, ClientError>>,
) -> AppFinished
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let mut request = Payload::tracked(request, &context.shared.destruction_failed);
    #[cfg(test)]
    let application_probe = {
        let index = context
            .shared
            .application_index
            .fetch_add(1, Ordering::AcqRel);
        if index == 0 {
            context.shared.options.test_application.clone()
        } else {
            context
                .shared
                .options
                .test_application_following
                .get(index - 1)
                .cloned()
        }
    };
    if context.shared.stop.is_cancelled() || exchange.cancel.is_cancelled() {
        deliver(&context.shared, response, Err(ClientError::Cancelled));
        return AppFinished::failed(ClientError::Cancelled);
    }
    if let Err(error) = route.project_request(request.get_mut()) {
        let error = ClientError::Route(Arc::new(error));
        deliver(&context.shared, response, Err(error.clone()));
        return AppFinished::failed(error);
    }
    let (parts, body) = request.take().into_parts();
    let mut body = Payload::tracked(body, &context.shared.destruction_failed);
    let (mut head, sidecar) = match roxygen_http_adapter::project_request_head(parts) {
        Ok(parts) => parts,
        Err(_) => {
            deliver(&context.shared, response, Err(ClientError::Projection));
            return AppFinished::failed(ClientError::Projection);
        }
    };
    let _sidecar = Payload::tracked(sidecar, &context.shared.destruction_failed);
    if route.allows_http1()
        && let Err(error) = frame_http1_request(&mut head, body.get_mut())
    {
        deliver(&context.shared, response, Err(error.clone()));
        return AppFinished::failed(error);
    }
    let (started, start_receiver) = oneshot::channel();
    let (headers, header_receiver) = oneshot::channel();
    let command = NativeRequest {
        head,
        route,
        exchange: exchange.clone(),
        started,
        response: headers,
    };
    if context.native.send(command).await.is_err() {
        deliver(&context.shared, response, Err(ClientError::Closed));
        return AppFinished::failed(ClientError::CleanupUnconfirmed);
    }
    let started = match receive_native_start(start_receiver, context.native.closed()).await {
        Ok(started) => started,
        Err(error) => {
            deliver(&context.shared, response, Err(error.clone()));
            return AppFinished::failed(error);
        }
    };
    let mut finished = AppFinished {
        native: Some(started.scope),
        #[cfg(test)]
        probe: application_probe.clone(),
        ..AppFinished::failed(ClientError::CleanupUnconfirmed)
    };
    #[cfg(test)]
    if let Some(probe) = &application_probe {
        *probe
            .native_scope
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            TestNativeReceipt(finished.native.clone());
    }
    let prepared = match started.prepared.await {
        Ok(prepared) => prepared,
        Err(_) => {
            deliver(
                &context.shared,
                response,
                Err(ClientError::CleanupUnconfirmed),
            );
            return finished;
        }
    };
    finished.source = prepared.source;
    finished.abort = prepared.abort;
    #[cfg(test)]
    if let Some(probe) = &application_probe {
        *probe
            .response_cleanup
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = finished.source.clone();
    }
    if let Some(error) = prepared.error {
        deliver(&context.shared, response, Err(error.clone()));
        finished.failure = Some(error);
        return finished;
    }
    let Some(application) = prepared.application else {
        deliver(
            &context.shared,
            response,
            Err(ClientError::CleanupUnconfirmed),
        );
        return finished;
    };
    let (driver, readiness, cleanup) = application.into_parts();
    #[cfg(test)]
    if let Some(probe) = &application_probe {
        *probe
            .cleanup
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(cleanup.clone());
    }
    finished.application = Some(cleanup);
    let abort = finished.abort.clone();
    let forwarding_failed = AtomicBool::new(false);
    let flow = async {
        let attachment_failure = match readiness.await {
            Err(_) => Some(ClientError::Closed),
            Ok(attachment)
                if context.shared.stop.is_cancelled() || exchange.cancel.is_cancelled() =>
            {
                drop(attachment);
                Some(ClientError::Cancelled)
            }
            Ok(attachment) => {
                #[cfg(test)]
                if let Some(latch) = application_probe
                    .as_ref()
                    .and_then(|probe| probe.before_attachment.as_ref())
                {
                    latch.until_release().await;
                }
                match attachment.attach(body.take()) {
                    Ok(()) => {
                        #[cfg(test)]
                        if let Some(probe) = &application_probe {
                            probe.attached.store(true, Ordering::Release);
                        }
                        None
                    }
                    Err(rejected) => {
                        #[cfg(test)]
                        if let Some(probe) = &application_probe {
                            probe.attachment_rejected.store(true, Ordering::Release);
                        }
                        Some(if dispose(rejected.body) {
                            ClientError::Closed
                        } else {
                            ClientError::Panicked
                        })
                    }
                }
            }
        };
        if attachment_failure.is_some() {
            exchange.cancel();
            if !forward_response_stop(
                abort.as_ref(),
                &exchange,
                None,
                #[cfg(test)]
                application_probe.as_deref(),
            ) {
                forwarding_failed.store(true, Ordering::Release);
            }
        }
        tokio::pin!(header_receiver);
        let result = tokio::select! {
            result = &mut header_receiver => result,
            _ = exchange.cancel.wait() => {
                if !forward_response_stop(abort.as_ref(), &exchange, None, #[cfg(test)] application_probe.as_deref()) {
                    forwarding_failed.store(true, Ordering::Release);
                }
                header_receiver.await
            }
        };
        if matches!(&attachment_failure, Some(ClientError::Panicked)) {
            dispose(result);
            return Err(ClientError::Panicked);
        }
        let native = result.map_err(|_| ClientError::CleanupUnconfirmed)??;
        if let Some(error) = attachment_failure {
            return Err(if dispose(native) {
                error
            } else {
                ClientError::Panicked
            });
        }
        let extensions = native.connection.extensions.get().0.clone();
        let parts = roxygen_http_adapter::response_head_to_http(
            &native.head,
            NetworkHeadContext::from_extensions(extensions),
        )
        .map_err(|_| ClientError::Projection)?;
        Ok(Response::from_parts(
            parts,
            ClientBody {
                inner: native.body,
                exchange: exchange.clone(),
            },
        ))
    };
    let flow = async {
        let result = Guarded::new(flow).await.and_then(|result| result);
        let failure = result.as_ref().err().cloned();
        if failure.is_some() {
            exchange.cancel();
            if !forward_response_stop(
                abort.as_ref(),
                &exchange,
                None,
                #[cfg(test)]
                application_probe.as_deref(),
            ) {
                forwarding_failed.store(true, Ordering::Release);
            }
        }
        if !deliver(&context.shared, response, result) {
            exchange.cancel();
        }
        failure
    };
    let application = async {
        let result = Guarded::new(driver).await;
        #[cfg(test)]
        if let Some(probe) = &application_probe {
            *probe
                .outcome
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(match &result {
                Ok(Ok(())) => "success",
                Ok(Err(ApplicationBodyError::Cancelled)) => "cancelled",
                Ok(Err(ApplicationBodyError::Application(_))) => "application",
                Ok(Err(_)) => "task",
                Err(_) => "driver-panic",
            });
            probe.reaped.store(true, Ordering::Release);
        }
        result
    };
    let joins = async { futures::join!(application, flow) };
    tokio::pin!(joins);
    let (application, response_failure) = tokio::select! {
        result = &mut joins => result,
        _ = exchange.cancel.wait() => {
            if !forward_response_stop(abort.as_ref(), &exchange, Some(HttpErrorCode::CORO_CANCELLED.into()), #[cfg(test)] application_probe.as_deref()) {
                forwarding_failed.store(true, Ordering::Release);
            }
            joins.await
        }
    };
    finished.unconfirmed |= forwarding_failed.load(Ordering::Acquire);
    finished.failure = match application {
        Ok(Ok(())) => response_failure,
        Ok(Err(ApplicationBodyError::Cancelled)) => {
            response_failure.or(Some(ClientError::Cancelled))
        }
        Ok(Err(error)) => Some(application_error(error)),
        Err(error) => Some(error),
    };
    finished
}

fn forward_response_stop(
    abort: Option<&SourceAbortHandle<HttpError>>,
    exchange: &ClientExchange,
    error: Option<HttpError>,
    #[cfg(test)] probe: Option<&TestApplicationJoin>,
) -> bool {
    if callback(|| {
        if let Some(abort) = abort {
            let accepted = abort.stop_reading(error);
            #[cfg(test)]
            if accepted {
                if let Some(probe) = probe {
                    probe.forwards.fetch_add(1, Ordering::AcqRel);
                    let waker = probe
                        .forward_wake
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take();
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
            }
            #[cfg(not(test))]
            let _ = accepted;
        }
    })
    .is_ok()
    {
        return true;
    }
    if let Some(shared) = exchange.shared.upgrade() {
        shared.notification_failed();
    }
    false
}

fn application_error(error: ApplicationBodyError) -> ClientError {
    let cause: Arc<dyn std::error::Error + Send + Sync> = match error {
        ApplicationBodyError::Application(error) => Arc::from(error),
        error => Arc::new(error),
    };
    ClientError::Application(AttachedConnectFailure::with_cause(
        HttpError::with_message(
            HttpErrorCode::BODY_PARSE_ERROR,
            b"application upload failed",
        ),
        cause,
    ))
}

async fn retire_exchange(
    finished: AppFinished,
    exchange: ClientExchange,
) -> Result<(), ClientCompletionError> {
    #[cfg(test)]
    if let Some(probe) = &finished.probe {
        probe.retirement_entered.store(true, Ordering::Release);
    }
    let joins = async {
        let native = async {
            if let Some(native) = finished.native {
                native.wait().await
            } else {
                Ok(())
            }
        };
        let source = async {
            if let Some(source) = finished.source {
                source
                    .wait()
                    .await
                    .map_err(|_| ClientError::CleanupUnconfirmed)
            } else {
                Ok(())
            }
        };
        let application = async {
            if let Some(application) = finished.application {
                application
                    .wait()
                    .await
                    .map_err(|_| ClientError::CleanupUnconfirmed)
            } else {
                Ok(())
            }
        };
        let (native, source, application) = futures::join!(native, source, application);
        let unconfirmed = finished.unconfirmed
            || native.is_err()
            || source.is_err()
            || application.is_err()
            || matches!(
                &finished.failure,
                Some(ClientError::CleanupUnconfirmed | ClientError::Panicked)
            );
        if unconfirmed {
            Err(ClientCompletionError::failure(
                finished.failure.unwrap_or(ClientError::CleanupUnconfirmed),
                true,
            ))
        } else if let Some(error) = finished.failure {
            Err(ClientCompletionError::failure(error, false))
        } else {
            Ok(())
        }
    };
    tokio::pin!(joins);
    let (result, forwarding_failed) = tokio::select! {
        result = &mut joins => (result, false),
        _ = exchange.cancel.wait() => {
            let failed = !forward_response_stop(finished.abort.as_ref(), &exchange, None, #[cfg(test)] finished.probe.as_deref());
            (joins.await, failed)
        }
    };
    if forwarding_failed {
        let mut error = result.err().unwrap_or_else(|| {
            ClientCompletionError::failure(ClientError::CleanupUnconfirmed, true)
        });
        error.unconfirmed = true;
        Err(error)
    } else {
        result
    }
}

struct NativeRequest {
    head: HttpMessage,
    route: RouteSpec,
    exchange: ClientExchange,
    started: oneshot::Sender<Result<NativeStarted, ClientError>>,
    response: oneshot::Sender<Result<NativeResponse, ClientError>>,
}

struct NativeStarted {
    scope: ScopeReceipt,
    prepared: oneshot::Receiver<NativePreparation>,
}

struct NativePreparation {
    application: Option<ApplicationBodyJoinPreparation>,
    source: Option<BodyCleanupObserver>,
    abort: Option<SourceAbortHandle<HttpError>>,
    error: Option<ClientError>,
}

struct NativeResponse {
    head: HttpMessage,
    body: RoxygenBody,
    connection: Arc<ConnectionRecord>,
}

struct NativeClient {
    commands: mpsc::Sender<NativeRequest>,
    scope: ScopeReceipt,
}

struct SealOnDrop(Arc<Shared>);
impl Drop for SealOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn start_native<Io>(
    context: &WorkerContext,
    shared: Arc<Shared>,
    connector: mpsc::Sender<ConnectorCommand<Io>>,
) -> Result<NativeClient, ClientError>
where
    Io: AsyncRead + AsyncWrite + Send + 'static,
{
    let (commands, mut requests) = mpsc::channel::<NativeRequest>(shared.options.maximum_exchanges);
    let context = context.clone();
    let owner = context.clone();
    let scope = register_task(&owner, async move {
        let _seal = SealOnDrop(Arc::clone(&shared));
        let mut pools: HashMap<RouteSpec, Rc<HttpSessionPool>> = HashMap::new();
        let records = Rc::new(RefCell::new(Vec::new()));
        let mut exchanges = FuturesUnordered::new();
        let mut failure = None;
        loop {
            tokio::select! {
                _ = shared.stop.wait() => break,
                _ = context.quiescing() => { shared.close(); break; }
                result = exchanges.next(), if !exchanges.is_empty() => {
                    if let Some(Err(error)) = result { failure.get_or_insert(error); shared.close(); }
                }
                request = requests.recv() => {
                    let Some(request) = request else { shared.close(); break; };
                    let pool = if let Some(pool) = pools.get(&request.route) { Ok(Rc::clone(pool)) }
                    else {
                        let factory = Rc::new(Factory {
                            shared: Arc::clone(&shared), route: request.route.clone(),
                            commands: connector.clone(), records: Rc::clone(&records),
                        });
                        HttpSessionPool::try_with_attached_factory_with_cause(
                            context.clone(), shared.options.pool, security(&request.route), factory,
                        ).map(Rc::new).map_err(ClientError::Pool)
                    };
                    let pool = match pool {
                        Ok(pool) => { pools.entry(request.route.clone()).or_insert_with(|| Rc::clone(&pool)); pool }
                        Err(error) => { let _ = request.started.send(Err(error.clone())); let _ = request.response.send(Err(error)); continue; }
                    };
                    let (prepared, preparation) = oneshot::channel();
                    let started = request.started;
                    let exchange = request.exchange;
                    let native = native_request(NativeRequestContext {
                        worker: context.clone(), shared: Arc::clone(&shared), records: Rc::clone(&records), pool,
                    }, request.head, request.route, exchange.clone(), prepared, request.response);
                    match register_task(&context, native) {
                        Ok(scope) => {
                            exchanges.push(scope.clone().wait());
                            if started.send(Ok(NativeStarted { scope, prepared: preparation })).is_err() { exchange.cancel(); }
                        }
                        Err(error) => { let _ = started.send(Err(error)); exchange.cancel(); }
                    }
                }
            }
        }
        requests.close();
        while let Ok(request) = requests.try_recv() {
            let _ = request.started.send(Err(ClientError::Closed));
            let _ = request.response.send(Err(ClientError::Closed));
        }
        let mut drains = FuturesUnordered::new();
        for pool in pools.values() {
            drains.push(pool.drain_and_join());
        }
        let drain_pools = async {
            let mut failed = None;
            while let Some(result) = drains.next().await {
                if let Err(error) = result {
                    failed.get_or_insert(ClientError::Pool(error));
                }
            }
            failed
        };
        let drain_requests = async {
            let mut failed = None;
            while let Some(result) = exchanges.next().await {
                if let Err(error) = result {
                    failed.get_or_insert(error);
                }
            }
            failed
        };
        let (pool_error, request_error) = futures::join!(drain_pools, drain_requests);
        drop(pools);
        drop(records);
        #[cfg(test)]
        if let Some(latch) = &shared.options.test_shutdown_return {
            latch.until_release().await;
        }
        failure.or(pool_error).or(request_error).map_or(Ok(()), Err)
    })?;
    Ok(NativeClient { commands, scope })
}

struct NativeRequestContext {
    worker: WorkerContext,
    shared: Arc<Shared>,
    records: Rc<RefCell<Vec<Arc<ConnectionRecord>>>>,
    pool: Rc<HttpSessionPool>,
}

async fn native_request(
    context: NativeRequestContext,
    mut head: HttpMessage,
    route: RouteSpec,
    exchange: ClientExchange,
    prepared: oneshot::Sender<NativePreparation>,
    response: oneshot::Sender<Result<NativeResponse, ClientError>>,
) -> Result<(), ClientError> {
    let NativeRequestContext {
        worker: context,
        shared,
        records,
        pool,
    } = context;
    let (export, source_cleanup) =
        match RoxygenBody::prepare_local(&context, shared.options.response.command_capacity) {
            Ok(export) => export,
            Err(_) => {
                let _ = prepared.send(NativePreparation {
                    application: None,
                    source: None,
                    abort: None,
                    error: Some(ClientError::Closed),
                });
                let _ = response.send(Err(ClientError::Closed));
                return Ok(());
            }
        };
    let abort = export.abort_handle();
    head.set_secure(route.proxy().is_none() && route.origin().scheme() == "https");
    let (source, application) = match ApplicationBodySource::prepare_with_application_join(
        &context,
        head,
        shared.options.upload,
    ) {
        Ok(application) => application,
        Err(_) => {
            drop(export);
            let _ = prepared.send(NativePreparation {
                application: None,
                source: Some(source_cleanup),
                abort: Some(abort),
                error: Some(ClientError::Closed),
            });
            let _ = response.send(Err(ClientError::Closed));
            return Ok(());
        }
    };
    let _ = prepared.send(NativePreparation {
        application: Some(application),
        source: Some(source_cleanup),
        abort: Some(abort.clone()),
        error: None,
    });
    let mut completion = None;
    let result = async {
        let mut acquisition = pool.get_session_with_reservation(exchange.cancel.token());
        #[cfg(test)]
        let mut acquisition_observed = false;
        let acquired = poll_fn(|context| {
            let result = acquisition.as_mut().poll(context);
            #[cfg(test)]
            if !acquisition_observed {
                acquisition_observed = true;
                if let Some(count) = &shared.options.test_acquisitions { count.fetch_add(1, Ordering::SeqCst); }
            }
            result
        }).await.map_err(ClientError::Pool)?;
        let connection = records.borrow().iter()
            .find(|record| record.matches(acquired.session.connection_context())).cloned()
            .ok_or(ClientError::CleanupUnconfirmed)?;
        let native = acquired.session.send_reserved_request(HttpSourceHolder::new(Box::new(source)), acquired.reservation, exchange.cancel.token())
            .await.map_err(ClientError::Native)?;
        completion = Some(native.completion.clone());
        let mut source = native.source;
        let (head, eom) = loop {
            let event = tokio::select! {
                result = source.read_header_event() => result.map_err(ClientError::Native)?,
                _ = exchange.cancel.wait() => { source.stop_reading(Some(HttpErrorCode::CORO_CANCELLED)); return Err(ClientError::Cancelled); }
            };
            let eom = event.eom();
            let mut head = event.into_headers();
            if head.is_final().map_err(|_| ClientError::Projection)? { break (head, eom); }
        };
        let body = if eom {
            drop(source);
            drop(export);
            RoxygenBody::empty()
        } else {
            let hint = std::str::from_utf8(head.headers().get_single_or_empty("content-length"))
                .ok().and_then(|length| length.parse::<u64>().ok()).map_or_else(SizeHint::new, SizeHint::with_exact);
            export.into_body(Box::new(source), shared.options.response.maximum_read, hint)
                .map_err(|_| ClientError::Closed)?
        };
        Ok(NativeResponse { head, body, connection })
    }.await;
    if result.is_err() {
        exchange.cancel();
        abort.stop_reading(None);
    }
    if let Err(result) = response.send(result) {
        dispose(result);
        exchange.cancel();
        abort.stop_reading(None);
    }
    if let Some(completion) = completion {
        completion
            .wait()
            .await
            .map_err(|_| ClientError::CleanupUnconfirmed)?;
    }
    #[cfg(test)]
    if let Some(latch) = &shared.options.test_native_return {
        latch.arrive(&shared.stop).await;
    }
    Ok(())
}

pub(super) struct ConnectionRecord {
    context: Mutex<Option<Arc<ConnectionContext>>>,
    extensions: Payload<RetiringExtensions>,
    pub retired: Arc<Signal>,
    control: Mutex<Option<AdoptedIoControl>>,
}

impl ConnectionRecord {
    fn matches(&self, expected: &Arc<ConnectionContext>) -> bool {
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, expected))
    }
}

enum ClaimState {
    Pending,
    Registering,
    Worker(ScopeReceipt),
    Application,
}

struct TransferState<Io> {
    pending: Option<EstablishedIo<TransitIo<Io>>>,
    claim: ClaimState,
}

pub(super) struct Handoff<Io> {
    transfer: Mutex<TransferState<Io>>,
    changed: Notify,
    abandoned: AtomicBool,
    protocol: roxygen_core::http::CodecProtocol,
    route: RouteSpec,
    security: roxygen_core::connection::TransportSecurity,
    pub record: Arc<ConnectionRecord>,
    destruction: watch::Receiver<Option<bool>>,
    retired: watch::Receiver<Option<Result<(), ClientError>>>,
    #[cfg(test)]
    claimed: Arc<AtomicBool>,
}

impl<Io> Handoff<Io> {
    fn new(
        connected: ConnectedIo<Io>,
        failures: &Arc<AtomicBool>,
        #[cfg(test)] claimed: Arc<AtomicBool>,
    ) -> (Arc<Self>, watch::Sender<Option<Result<(), ClientError>>>) {
        let (destruction, destroyed) = watch::channel(None);
        let (retirement, retired) = watch::channel(None);
        let EstablishedIo {
            io,
            local_endpoint,
            peer_endpoint,
            peer_identity,
        } = connected.established;
        (
            Arc::new(Self {
                transfer: Mutex::new(TransferState {
                    pending: Some(EstablishedIo::new(
                        TransitIo {
                            inner: Some(Box::pin(io)),
                            destruction,
                        },
                        local_endpoint,
                        peer_endpoint,
                        peer_identity,
                    )),
                    claim: ClaimState::Pending,
                }),
                changed: Notify::new(),
                abandoned: AtomicBool::new(false),
                protocol: connected.protocol,
                route: connected.route,
                security: connected.security,
                record: Arc::new(ConnectionRecord {
                    context: Mutex::new(None),
                    extensions: Payload::tracked(connected.extensions, failures),
                    retired: Arc::new(Signal::new()),
                    control: Mutex::new(None),
                }),
                destruction: destroyed,
                retired,
                #[cfg(test)]
                claimed,
            }),
            retirement,
        )
    }

    pub fn abandon(&self) {
        self.abandoned.store(true, Ordering::Release);
        let _ = callback(|| self.changed.notify_waiters());
    }

    fn dispose_unclaimed_on_application(&self) {
        let pending = {
            let mut transfer = self
                .transfer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if matches!(transfer.claim, ClaimState::Pending) {
                transfer.claim = ClaimState::Application;
            }
            transfer.pending.take()
        };
        dispose(pending);
    }

    fn begin_worker(&self) -> bool {
        let mut transfer = self
            .transfer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !matches!(transfer.claim, ClaimState::Pending) || self.abandoned.load(Ordering::Acquire)
        {
            return false;
        }
        transfer.claim = ClaimState::Registering;
        true
    }

    fn publish_claim(&self, scope: Option<ScopeReceipt>) {
        self.transfer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .claim = scope.map_or(ClaimState::Application, ClaimState::Worker);
        let _ = callback(|| self.changed.notify_waiters());
    }

    fn take(&self) -> Option<EstablishedIo<TransitIo<Io>>> {
        let mut transfer = self
            .transfer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.abandoned.load(Ordering::Acquire) {
            return None;
        }
        let pending = transfer.pending.take();
        #[cfg(test)]
        if pending.is_some() {
            self.claimed.store(true, Ordering::Release);
        }
        pending
    }

    pub async fn wait_retired(&self) -> Result<(), ClientError> {
        let mut retired = self.retired.clone();
        loop {
            if let Some(result) = retired.borrow().clone() {
                return result;
            }
            if retired.changed().await.is_err() {
                return Err(ClientError::CleanupUnconfirmed);
            }
        }
    }
}

impl<Io> Drop for Handoff<Io> {
    fn drop(&mut self) {
        let transfer = self
            .transfer
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        dispose(transfer.pending.take());
    }
}

struct TransitIo<Io> {
    inner: Option<Pin<Box<Io>>>,
    destruction: watch::Sender<Option<bool>>,
}

impl<Io: AsyncRead> AsyncRead for TransitIo<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner
            .as_mut()
            .expect("owned transit I/O")
            .as_mut()
            .poll_read(context, buffer)
    }
}

impl<Io: AsyncWrite> AsyncWrite for TransitIo<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.inner
            .as_mut()
            .expect("owned transit I/O")
            .as_mut()
            .poll_write(context, bytes)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner
            .as_mut()
            .expect("owned transit I/O")
            .as_mut()
            .poll_flush(context)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner
            .as_mut()
            .expect("owned transit I/O")
            .as_mut()
            .poll_shutdown(context)
    }
}

impl<Io> Drop for TransitIo<Io> {
    fn drop(&mut self) {
        let success = dispose(self.inner.take());
        let _ = self.destruction.send(Some(success));
    }
}

pub(super) async fn attach_handoff<Io>(
    context: &WorkerContext,
    handoff: Arc<Handoff<Io>>,
    shared: Arc<Shared>,
    stop: Arc<Signal>,
) -> Result<ClientSession, HttpError>
where
    Io: AsyncRead + AsyncWrite + Send + 'static,
{
    let cancelled = || {
        HttpError::with_message(
            HttpErrorCode::CORO_CANCELLED,
            b"connection handoff cancelled",
        )
    };
    if !handoff.begin_worker() {
        return Err(cancelled());
    }
    let (reply, receiver) = oneshot::channel();
    let scope_handoff = Arc::clone(&handoff);
    let worker = context.clone();
    #[cfg(test)]
    let registration_rejected = shared.options.test_claim_registration_rejected.clone();
    #[cfg(test)]
    if shared.options.test_close_claim_registration {
        context.handle().begin_shutdown();
    }
    let scope = register_task(context, async move {
        let Some(established) = scope_handoff.take() else {
            let _ = reply.send(Err(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"unclaimed connection",
            )));
            return Ok(());
        };
        #[cfg(test)]
        if let Some(latch) = &shared.options.test_handoff {
            latch.arrive(&shared.stop).await;
        }
        let (transport, control) =
            match adopt_local_with_completion(&worker, established, shared.options.transport) {
                Ok(result) => result,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return Ok(());
                }
            };
        *scope_handoff
            .record
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(control.clone());
        #[cfg(test)]
        if let Some(probe) = &shared.options.test_io {
            *probe
                .adopted
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(control.completion());
        }
        #[cfg(test)]
        if let Some(latch) = &shared.options.test_after_adoption {
            latch.arrive(&shared.stop).await;
        }
        let mut options = ClientAttachOptions::new(scope_handoff.protocol, scope_handoff.security);
        let settings = &shared.options.session;
        options.session = SessionParams {
            max_concurrent_outgoing_streams: settings.max_concurrent_outgoing_streams,
            connection_flow_control: settings.connection_flow_control,
            stream_read_timeout: settings.stream_read_timeout,
            connection_read_timeout: settings.connection_read_timeout,
            write_timeout: settings.write_timeout,
            ..SessionParams::default()
        };
        options.upstream_origin = Some(UpstreamOrigin::new(
            scope_handoff.route.origin().authority().as_str(),
            if scope_handoff.route.origin().scheme() == "https" {
                OriginSecurity::Https
            } else {
                OriginSecurity::Http
            },
        ));
        let session = match ClientSession::attach_transport(&worker, transport, options) {
            Ok(session) => session,
            Err(error) => {
                control.cancel();
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
        *scope_handoff
            .record
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            Some(Arc::clone(session.connection_context()));
        if stop.is_cancelled() || shared.stop.is_cancelled() {
            session.cancel();
            control.cancel();
        }
        if reply.send(Ok(session.clone())).is_err() {
            session.cancel();
            control.cancel();
        }
        let completion = session.wait_closed();
        tokio::pin!(completion);
        let mut drain_failure = None;
        let result = tokio::select! {
            result = &mut completion => result,
            _ = stop.wait() => { session.cancel(); control.cancel(); completion.await }
            _ = shared.stop.wait() => { drain_failure = session.initiate_drain().err(); completion.await }
        };
        drop(session);
        drain_failure.map_or_else(
            || result.map(|_| ()).map_err(ClientError::Native),
            |error| Err(ClientError::Native(error)),
        )
    });
    match scope {
        Ok(scope) => handoff.publish_claim(Some(scope)),
        Err(_) => {
            #[cfg(test)]
            if let Some(rejected) = registration_rejected {
                rejected.store(true, Ordering::Release);
            }
            handoff.publish_claim(None);
            handoff.abandon();
            return Err(cancelled());
        }
    }
    receiver.await.unwrap_or_else(|_| {
        Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"native handoff owner lost",
        ))
    })
}

async fn retire_io<Io>(handoff: Arc<Handoff<Io>>, shared: Arc<Shared>) -> Result<(), ClientError>
where
    Io: Send + 'static,
{
    let scope = loop {
        let notified = handoff.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if shared.stop.is_cancelled() {
            handoff.abandon();
        }
        if handoff.abandoned.load(Ordering::Acquire) {
            handoff.dispose_unclaimed_on_application();
        }
        let decision = {
            let transfer = handoff
                .transfer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match &transfer.claim {
                ClaimState::Worker(scope) => Some(Some(scope.clone())),
                ClaimState::Application => Some(None),
                _ => None,
            }
        };
        if let Some(scope) = decision {
            break scope;
        }
        tokio::select! {
            _ = &mut notified => {}
            _ = shared.stop.wait() => { handoff.abandon(); }
        }
    };
    let native = if let Some(scope) = scope {
        scope.wait().await
    } else {
        Ok(())
    };
    handoff.dispose_unclaimed_on_application();
    let control = handoff
        .record
        .control
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let adopted = async {
        if let Some(control) = control {
            if native.is_err() {
                control.cancel();
            }
            control
                .completion()
                .wait_retired()
                .await
                .map_err(|_| ClientError::CleanupUnconfirmed)
        } else {
            Ok(())
        }
    };
    let destruction = async {
        let mut destruction = handoff.destruction.clone();
        loop {
            if let Some(success) = *destruction.borrow() {
                return if success {
                    Ok(())
                } else {
                    Err(ClientError::CleanupUnconfirmed)
                };
            }
            if destruction.changed().await.is_err() {
                return Err(ClientError::CleanupUnconfirmed);
            }
        }
    };
    let (adopted, destruction) = futures::join!(adopted, destruction);
    let native_unconfirmed = !matches!(native, Ok(()) | Err(ClientError::Native(_)));
    #[cfg(test)]
    if let Some(probe) = &shared.options.test_io {
        probe.retired.store(true, Ordering::Release);
    }
    if native_unconfirmed || adopted.is_err() || destruction.is_err() {
        Err(ClientError::CleanupUnconfirmed)
    } else {
        Ok(())
    }
}

struct ConnectFinished<Io> {
    command: ConnectorCommand<Io>,
    result: Result<ConnectedIo<Io>, AttachedConnectFailure>,
    permit: Option<OwnedSemaphorePermit>,
}

async fn begin_connect<C, Io>(
    mut connector: Payload<C>,
    command: ConnectorCommand<Io>,
    shared: Arc<Shared>,
) -> (Payload<C>, AppFuture<ConnectFinished<Io>>)
where
    C: Service<ConnectRequest, Response = ConnectedIo<Io>> + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
    Io: AsyncRead + AsyncWrite + Send + 'static,
{
    let prepare = async {
        let permit = Arc::clone(&shared.physical)
            .acquire_owned()
            .await
            .map_err(|_| failure(ClientError::Closed))?;
        poll_fn(|context| connector.get_mut().poll_ready(context))
            .await
            .map_err(|error| connector_failure(error.into()))?;
        if command.stop.is_cancelled() || shared.stop.is_cancelled() {
            return Err(failure(ClientError::Cancelled));
        }
        let future = connector.get_mut().call(command.request.clone());
        Ok((permit, future))
    };
    let prepared = {
        let prepared = Guarded::new(prepare);
        tokio::pin!(prepared);
        tokio::select! {
            result = &mut prepared => result.unwrap_or_else(|error| Err(failure(error))),
            _ = command.stop.wait() => Err(failure(ClientError::Cancelled)),
            _ = shared.stop.wait() => Err(failure(ClientError::Cancelled)),
        }
    };
    let future: AppFuture<ConnectFinished<Io>> = match prepared {
        Err(error) => Box::pin(async move {
            ConnectFinished {
                command,
                result: Err(error),
                permit: None,
            }
        }),
        Ok((permit, future)) => Box::pin(async move {
            let mut future = Guarded::with_output_disposer(future, |result| match result {
                Ok(io) => io.retire(),
                Err(error) => dispose(error),
            });
            let result = tokio::select! {
                result = &mut future => match result {
                    Ok(Ok(io)) => Ok(io),
                    Ok(Err(error)) => Err(connector_failure(error.into())),
                    Err(error) => Err(failure(error)),
                },
                _ = command.stop.wait() => Err(failure(ClientError::Cancelled)),
                _ = shared.stop.wait() => Err(failure(ClientError::Cancelled)),
            };
            let destroyed = dispose(future.future.take());
            let result = match result {
                Ok(io)
                    if !destroyed
                        || command.stop.is_cancelled()
                        || shared.stop.is_cancelled()
                        || !io.agrees(&command.request) =>
                {
                    let error = if !destroyed {
                        ClientError::Panicked
                    } else if command.stop.is_cancelled() || shared.stop.is_cancelled() {
                        ClientError::Cancelled
                    } else {
                        ClientError::ConnectorMismatch
                    };
                    let dropped = io.retire();
                    Err(failure(if dropped {
                        error
                    } else {
                        ClientError::Panicked
                    }))
                }
                result if destroyed => result,
                result => {
                    dispose(result);
                    Err(failure(ClientError::Panicked))
                }
            };
            ConnectFinished {
                command,
                result,
                permit: Some(permit),
            }
        }),
    };
    (connector, future)
}

pub(super) fn make_run<C, Io>(
    shared: Arc<Shared>,
    connector: C,
    requests: mpsc::Receiver<Job>,
    activation: oneshot::Receiver<()>,
    ready: oneshot::Sender<Result<(), ClientStartError>>,
    leaves: watch::Sender<Option<Result<(), ClientAppDrainError>>>,
) -> ClientRun
where
    C: Service<ConnectRequest, Response = ConnectedIo<Io>> + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
    Io: AsyncRead + AsyncWrite + Send + 'static,
{
    ClientRun {
        shared: Arc::downgrade(&shared),
        future: Box::pin(run_client(
            shared, connector, requests, activation, ready, leaves,
        )),
    }
}

type RunningRequest = AppFuture<(
    ClientExchange,
    ExchangePublisher,
    OwnedSemaphorePermit,
    Result<AppFinished, ClientError>,
)>;
type RunningReceipt = AppFuture<(
    ClientExchange,
    ExchangePublisher,
    OwnedSemaphorePermit,
    Result<(), ClientCompletionError>,
)>;
type Dispatch<C, Io> = AppFuture<Result<(Payload<C>, AppFuture<ConnectFinished<Io>>), ClientError>>;

fn record_unconfirmed_receipt_error(
    first_error: &mut Option<ClientError>,
    destruction_failed: &AtomicBool,
) {
    first_error.get_or_insert_with(|| {
        if destruction_failed.load(Ordering::Acquire) {
            ClientError::Panicked
        } else {
            ClientError::CleanupUnconfirmed
        }
    });
}

fn run_job(job: Job, context: AppContext) -> RunningRequest {
    Box::pin(async move {
        #[cfg(test)]
        let shared = Arc::clone(&context.shared);
        let start = job.start;
        let result = Guarded::new(async move { start(context).await }).await;
        #[cfg(test)]
        if let Some(latch) = &shared.options.test_app_return {
            latch.arrive(&shared.stop).await;
        }
        (job.exchange, job.publisher, job.permit, result)
    })
}

async fn run_client<C, Io>(
    shared: Arc<Shared>,
    connector: C,
    mut requests: mpsc::Receiver<Job>,
    activation: oneshot::Receiver<()>,
    ready: oneshot::Sender<Result<(), ClientStartError>>,
    leaves: watch::Sender<Option<Result<(), ClientAppDrainError>>>,
) -> Result<(), ClientShutdownError>
where
    C: Service<ConnectRequest, Response = ConnectedIo<Io>> + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
    Io: AsyncRead + AsyncWrite + Send + 'static,
{
    let _seal = SealOnDrop(Arc::clone(&shared));
    let mut connector = Some(Payload::tracked(connector, &shared.destruction_failed));
    if !shared.entered() {
        deliver(
            &shared,
            ready,
            Err(ClientStartError::Client(ClientError::WrongRuntime)),
        );
        return Err(ClientShutdownError(ClientError::WrongRuntime));
    }
    let activated =
        tokio::select! { result = activation => result.is_ok(), _ = shared.stop.wait() => false };
    if !activated || shared.stop.is_cancelled() {
        let dropped = dispose(connector.take().map(|mut connector| connector.take()));
        deliver(
            &shared,
            ready,
            Err(ClientStartError::Client(ClientError::Closed)),
        );
        let result = if dropped {
            Ok(())
        } else {
            Err(ClientAppDrainError(ClientError::Panicked))
        };
        publish(&shared, &leaves, Some(result.clone()));
        if shared.destruction_failed.load(Ordering::Acquire) {
            return Err(ClientShutdownError(ClientError::Panicked));
        }
        return result.map_err(|error| ClientShutdownError(error.0));
    }
    let (connect, mut connections) = mpsc::channel(shared.options.maximum_connector_tasks);
    let native_shared = Arc::clone(&shared);
    let startup = match shared
        .worker
        .call(move |context| start_native(context, native_shared, connect))
    {
        Ok(startup) => startup
            .await
            .map_err(|_| ClientError::CleanupUnconfirmed)
            .and_then(|result| result),
        Err(_) => Err(ClientError::Closed),
    };
    let native = match startup {
        Ok(native) => native,
        Err(error) => {
            let dropped = dispose(connector.take().map(|mut connector| connector.take()));
            deliver(&shared, ready, Err(ClientStartError::Client(error.clone())));
            publish(
                &shared,
                &leaves,
                Some(if dropped {
                    Ok(())
                } else {
                    Err(ClientAppDrainError(ClientError::Panicked))
                }),
            );
            return Err(ClientShutdownError(error));
        }
    };
    let context = AppContext {
        shared: Arc::clone(&shared),
        native: native.commands,
    };
    let mut native_scope = Box::pin(native.scope.wait());
    let mut native_done = false;
    let mut app_jobs: FuturesUnordered<RunningRequest> = FuturesUnordered::new();
    let mut receipts: FuturesUnordered<RunningReceipt> = FuturesUnordered::new();
    let mut connecting: FuturesUnordered<AppFuture<Result<ConnectFinished<Io>, ClientError>>> =
        FuturesUnordered::new();
    let mut dispatch: Option<Dispatch<C, Io>> = None;
    let mut handoffs: Vec<Arc<Handoff<Io>>> = Vec::new();
    let mut io_retirements: FuturesUnordered<
        AppFuture<(
            Arc<Handoff<Io>>,
            OwnedSemaphorePermit,
            Result<(), ClientError>,
        )>,
    > = FuturesUnordered::new();
    let mut tombstones = Vec::new();
    let mut first_error = None;
    let mut leaf_published = false;
    let mut stopping = false;
    let mut connections_open = true;
    #[cfg(test)]
    let mut test_leaf_release = shared
        .options
        .test_app_leaf_snapshot
        .as_ref()
        .map(|probe| probe.publication.released.subscribe());
    #[cfg(test)]
    let mut test_leaf_release_seen = false;
    if !deliver(&shared, ready, Ok(())) {
        shared.close();
    }
    loop {
        if shared.destruction_failed.load(Ordering::Acquire) {
            first_error.get_or_insert(ClientError::Panicked);
            shared.close();
        }
        if shared.stop.is_cancelled() {
            stopping = true;
            notify(&shared, || requests.close());
            let no_publication = shared.publishing.load(Ordering::Acquire) == 0;
            while let Ok(job) = requests.try_recv() {
                app_jobs.push(run_job(job, context.clone()));
            }
            for handoff in &handoffs {
                handoff.abandon();
            }
            if !leaf_published
                && no_publication
                && app_jobs.is_empty()
                && connecting.is_empty()
                && dispatch.is_none()
            {
                if !dispose(connector.take().map(|mut connector| connector.take())) {
                    first_error.get_or_insert(ClientError::Panicked);
                }
                #[cfg(not(test))]
                let publish_leaf = true;
                #[cfg(test)]
                let publish_leaf = !shared
                    .options
                    .test_app_leaf_snapshot
                    .as_ref()
                    .is_some_and(|probe| probe.hold_publication());
                if publish_leaf {
                    publish(
                        &shared,
                        &leaves,
                        Some(
                            first_error
                                .clone()
                                .map_or(Ok(()), |error| Err(ClientAppDrainError(error))),
                        ),
                    );
                    leaf_published = true;
                }
            }
            let finished = no_publication
                && native_done
                && app_jobs.is_empty()
                && receipts.is_empty()
                && connecting.is_empty()
                && dispatch.is_none()
                && io_retirements.is_empty();
            #[cfg(test)]
            let finished =
                finished && (shared.options.test_app_leaf_snapshot.is_none() || leaf_published);
            if finished {
                break;
            }
        }
        let publication_changed = shared.published.notified();
        #[cfg(test)]
        let publication_changed = async {
            if let Some(released) = &mut test_leaf_release {
                tokio::select! {
                    _ = publication_changed => {}
                    _ = released.changed(), if !test_leaf_release_seen => {
                        test_leaf_release_seen = true;
                    }
                }
            } else {
                publication_changed.await;
            }
        };
        tokio::select! {
            _ = shared.stop.wait(), if !stopping => {}
            _ = publication_changed, if stopping => {}
            result = &mut native_scope, if !native_done => {
                native_done = true;
                if let Err(error) = result {
                    first_error.get_or_insert(error);
                    #[cfg(test)]
                    if let Some(probe) = &shared.options.test_app_leaf_snapshot {
                        probe.native_selected.send_replace(first_error.clone());
                    }
                }
                shared.close();
            }
            job = requests.recv(), if !stopping => {
                match job { Some(job) => app_jobs.push(run_job(job, context.clone())), None => shared.close() }
            }
            command = connections.recv(), if connections_open && (stopping || (connector.is_some() && dispatch.is_none() && connecting.len() < shared.options.maximum_connector_tasks)) => {
                if let Some(command) = command {
                    if stopping {
                        command.stop.cancel();
                        deliver(&shared, command.reply, Err(failure(ClientError::Cancelled)));
                    } else {
                        dispatch = Some(Box::pin(Guarded::new(begin_connect(connector.take().expect("available connector"), command, Arc::clone(&shared)))));
                    }
                } else { connections_open = false; }
            }
            result = async { dispatch.as_mut().expect("active dispatch").await }, if dispatch.is_some() => {
                dispatch = None;
                match result {
                    Ok((service, future)) => { connector = Some(service); connecting.push(Box::pin(Guarded::new(future))); }
                    Err(error) => { first_error.get_or_insert(error); shared.close(); }
                }
            }
            result = connecting.next(), if !connecting.is_empty() => {
                let finished = match result.expect("nonempty connector group") {
                    Ok(finished) => finished,
                    Err(error) => { first_error.get_or_insert(error); shared.close(); continue; }
                };
                let ConnectFinished { command, result, permit } = finished;
                match result {
                    Ok(connected) => {
                        let (handoff, publication) = Handoff::new(connected, &shared.destruction_failed, #[cfg(test)] Arc::clone(&command.request.claimed));
                        handoffs.push(Arc::clone(&handoff));
                        let retirement = Arc::clone(&handoff);
                        let io_shared = Arc::clone(&shared);
                        let permit = permit.expect("successful connector reserved physical capacity");
                        io_retirements.push(Box::pin(async move {
                            let result = Guarded::new(retire_io(Arc::clone(&retirement), Arc::clone(&io_shared))).await.and_then(|result| result);
                            publish(&io_shared, &publication, Some(result.clone()));
                            (retirement, permit, result)
                        }));
                        if !deliver(&shared, command.reply, Ok(Arc::clone(&handoff))) { handoff.abandon(); }
                    }
                    Err(error) => {
                        if error.external_cause().is_some_and(|error| error.downcast_ref::<ClientError>().is_some_and(|error| matches!(error, ClientError::Panicked))) {
                            first_error.get_or_insert(ClientError::Panicked); shared.close();
                        }
                        deliver(&shared, command.reply, Err(error));
                        notify(&shared, || drop(permit));
                    }
                }
            }
            result = app_jobs.next(), if !app_jobs.is_empty() => {
                let (exchange, publisher, permit, result) = result.expect("nonempty application group");
                let mut finished = match result {
                    Ok(finished) => finished,
                    Err(error) => { first_error.get_or_insert(error.clone()); shared.close(); AppFinished::failed(error) }
                };
                if shared.destruction_failed.load(Ordering::Acquire) {
                    finished.unconfirmed = true;
                    finished.failure.get_or_insert(ClientError::Panicked);
                }
                let observer = exchange.clone();
                receipts.push(Box::pin(async move {
                    let result = Guarded::new(retire_exchange(finished, observer)).await
                        .unwrap_or_else(|error| Err(ClientCompletionError::failure(error, true)));
                    (exchange, publisher, permit, result)
                }));
            }
            result = receipts.next(), if !receipts.is_empty() => {
                let (exchange, publisher, permit, result) = result.expect("nonempty receipt group");
                if result.as_ref().err().is_some_and(ClientCompletionError::cleanup_unconfirmed) {
                    record_unconfirmed_receipt_error(&mut first_error, &shared.destruction_failed);
                    shared.close();
                    tombstones.push(permit);
                } else { notify(&shared, || drop(permit)); }
                shared.exchanges.lock().unwrap_or_else(|error| error.into_inner()).retain(|cancel| !Arc::ptr_eq(cancel, &exchange.cancel));
                publish(&shared, &publisher, Some(result));
            }
            result = io_retirements.next(), if !io_retirements.is_empty() => {
                let (handoff, permit, result) = result.expect("nonempty I/O receipt group");
                if let Err(error) = result { first_error.get_or_insert(error); shared.close(); tombstones.push(permit); }
                else { notify(&shared, || drop(permit)); }
                handoff.record.retired.cancel();
                handoffs.retain(|entry| !Arc::ptr_eq(entry, &handoff));
            }
        }
    }
    notify(&shared, || connections.close());
    while let Ok(command) = connections.try_recv() {
        deliver(&shared, command.reply, Err(failure(ClientError::Closed)));
    }
    notify(&shared, || drop(handoffs));
    notify(&shared, || drop(tombstones));
    if shared.destruction_failed.load(Ordering::Acquire) {
        first_error.get_or_insert(ClientError::Panicked);
    }
    first_error.map_or(Ok(()), |error| Err(ClientShutdownError(error)))
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestLatch {
    entered: watch::Sender<bool>,
    released: watch::Sender<bool>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestAppLeafSnapshot {
    publication: Arc<TestLatch>,
    native_selected: watch::Sender<Option<ClientError>>,
}

#[cfg(test)]
impl TestAppLeafSnapshot {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            publication: TestLatch::new(),
            native_selected: watch::channel(None).0,
        })
    }

    fn hold_publication(&self) -> bool {
        self.publication.entered.send_replace(true);
        !*self.publication.released.borrow()
    }

    pub fn release(&self) {
        self.publication.release();
    }

    pub async fn wait_eligible(&self) {
        self.publication.wait_entered().await;
    }

    pub async fn native_error_selected(&self) -> ClientError {
        let mut selected = self.native_selected.subscribe();
        loop {
            if let Some(error) = selected.borrow_and_update().clone() {
                return error;
            }
            selected
                .changed()
                .await
                .expect("retained native selection publisher");
        }
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestApplicationJoin {
    pub before_attachment: Option<Arc<TestLatch>>,
    pub attachment_rejected: AtomicBool,
    pub attached: AtomicBool,
    pub reaped: AtomicBool,
    pub outcome: Mutex<Option<&'static str>>,
    pub cleanup: Mutex<Option<ApplicationBodyCleanupObserver>>,
    pub response_cleanup: Mutex<Option<BodyCleanupObserver>>,
    pub forwards: std::sync::atomic::AtomicUsize,
    pub forward_wake: Mutex<Option<Waker>>,
    pub native_scope: Mutex<TestNativeReceipt>,
    pub retirement_entered: AtomicBool,
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct TestNativeReceipt(Option<ScopeReceipt>);

#[cfg(test)]
impl TestApplicationJoin {
    pub async fn native_retired(&self) -> Result<(), ClientError> {
        let scope = self
            .native_scope
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .0
            .clone()
            .expect("actual native request scope");
        scope.wait().await
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestIoJoin {
    pub adopted: Mutex<Option<roxygen_transport::established::AdoptedIoCompletion>>,
    pub retired: AtomicBool,
}
#[cfg(test)]
impl std::fmt::Debug for TestIoJoin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestIoJoin")
            .field("retired", &self.retired.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl std::fmt::Debug for TestApplicationJoin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestApplicationJoin")
            .field("reaped", &self.reaped.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl TestLatch {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: watch::channel(false).0,
            released: watch::channel(false).0,
        })
    }
    pub fn release(&self) {
        self.released.send_replace(true);
    }
    pub async fn wait_entered(&self) {
        let mut entered = self.entered.subscribe();
        while !*entered.borrow_and_update() {
            if entered.changed().await.is_err() {
                break;
            }
        }
    }
    pub(super) async fn arrive(&self, stop: &Signal) {
        self.entered.send_replace(true);
        let mut released = self.released.subscribe();
        loop {
            if *released.borrow_and_update() {
                break;
            }
            tokio::select! { result = released.changed() => { if result.is_err() { break; } }, _ = stop.wait() => break }
        }
    }
    pub(super) async fn until_release(&self) {
        self.entered.send_replace(true);
        let mut released = self.released.subscribe();
        while !*released.borrow_and_update() {
            if released.changed().await.is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::Future;
    use std::pin::Pin;
    use std::ptr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use roxygen_client::pool::AttachedConnectFailure;
    use roxygen_client::pool::PoolError;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    use super::receive_native_start;
    use super::record_unconfirmed_receipt_error;
    use crate::client::ClientError;

    #[test]
    fn native_start_receiver_closure_breaks_late_permit_reply_cycle() {
        let (native, requests) = mpsc::channel(1);
        let (started, mut start) = oneshot::channel::<Result<(), ClientError>>();
        let permit = native.try_reserve().expect("native queue has capacity");
        drop(requests);
        permit.send(started);

        let mut context = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut start).poll(&mut context).is_pending());
        let mut wait = Box::pin(receive_native_start(start, native.closed()));
        assert!(matches!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Err(ClientError::CleanupUnconfirmed))
        ));
        assert!(native.is_closed());
    }

    #[test]
    fn native_start_published_during_receiver_closure_keeps_payload() {
        struct ScopeGuard(Arc<AtomicBool>);
        impl Drop for ScopeGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let scope = ScopeGuard(Arc::clone(&dropped));
        let (started, mut start) = oneshot::channel();
        let (native, requests) = mpsc::channel::<()>(1);
        let mut context = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut start).poll(&mut context).is_pending());
        let closed = async move {
            assert!(started.send(Ok(scope)).is_ok());
            drop(requests);
            native.closed().await;
        };
        let mut wait = Box::pin(receive_native_start(start, closed));
        let Poll::Ready(Ok(scope)) = wait.as_mut().poll(&mut context) else {
            panic!("startup published before closure must retain its payload");
        };
        drop(wait);
        assert!(!dropped.load(Ordering::Relaxed));
        drop(scope);
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[test]
    fn unconfirmed_receipt_preserves_prior_errors() {
        for caught_panic in [false, true] {
            let failed = AtomicBool::new(caught_panic);
            let mut first_error = None;
            record_unconfirmed_receipt_error(&mut first_error, &failed);
            if caught_panic {
                assert!(matches!(first_error, Some(ClientError::Panicked)));
            } else {
                assert!(matches!(first_error, Some(ClientError::CleanupUnconfirmed)));
            }

            let cause: Arc<dyn std::error::Error + Send + Sync> =
                Arc::new(std::io::Error::other("earlier operation failure"));
            let native = HttpError::with_message(
                HttpErrorCode::BODY_PARSE_ERROR,
                b"earlier operation failure",
            );
            let mut application = Some(ClientError::Application(
                AttachedConnectFailure::with_cause(native.clone(), Arc::clone(&cause)),
            ));
            record_unconfirmed_receipt_error(&mut application, &failed);
            let Some(ClientError::Application(selected)) = application else {
                panic!("an earlier application error must not be replaced");
            };
            assert!(Arc::ptr_eq(selected.external_cause().unwrap(), &cause));
            assert_eq!(selected.http_error().code(), native.code());
            assert_eq!(selected.http_error().message(), native.message());

            let mut pool = Some(ClientError::Pool(PoolError::SessionJoin {
                source: native.clone().with_cause(Arc::clone(&cause)),
            }));
            record_unconfirmed_receipt_error(&mut pool, &failed);
            let Some(ClientError::Pool(PoolError::SessionJoin { source })) = pool else {
                panic!("an earlier session error must not be replaced");
            };
            assert_eq!(source.code(), native.code());
            assert_eq!(source.message(), native.message());
            assert!(ptr::eq(
                source
                    .source()
                    .unwrap()
                    .downcast_ref::<std::io::Error>()
                    .unwrap(),
                cause.downcast_ref::<std::io::Error>().unwrap(),
            ));

            let mut earlier_uncertainty = Some(ClientError::CleanupUnconfirmed);
            record_unconfirmed_receipt_error(&mut earlier_uncertainty, &failed);
            assert!(matches!(
                earlier_uncertainty,
                Some(ClientError::CleanupUnconfirmed)
            ));
        }
    }
}
