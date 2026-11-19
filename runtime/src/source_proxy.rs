/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::context::ContextEnvelope;
use crate::context::ContextFuture;
use crate::observability::OperationKind;
use crate::observability::OperationSpan;
use crate::observability::TerminalOutcome;
use crate::worker::CleanupRegistrationError;
use crate::worker::PanicPayload;
use crate::worker::SubmitError;
use crate::worker::WorkerCleanupObservationError;
use crate::worker::WorkerCleanupObserver;
use crate::worker::WorkerCleanupRegistration;
use crate::worker::WorkerContext;
use crate::worker::WorkerHandle;
use crate::worker::WorkerId;
use crate::worker::WorkerState;
use crate::worker::discard_panic;
use crate::worker::retain_first_panic;

/// An owner-local future returned by a local HTTP source.
pub type LocalSourceFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// A body read either produces a transferable event or suspends on owner-local
/// work before the source is read again.
pub enum LocalBodyRead<B> {
    Event(B),
    Suspend(LocalSourceFuture<()>),
}

/// Common terminal metadata needed by the runtime bridge.
pub trait RemoteSourceEvent {
    fn is_terminal(&self) -> bool;
}

/// The worker-local side of a cross-executor HTTP source.
///
/// Read futures are deliberately non-`Send` and own their local borrows. This
/// lets timeout and stop commands run on the owner while a read is suspended.
pub trait LocalHttpSource: 'static {
    type HeaderEvent: RemoteSourceEvent + Send + 'static;
    type BodyEvent: RemoteSourceEvent + Send + 'static;
    type Error: std::error::Error + Send + 'static;

    fn read_header_event(&mut self) -> LocalSourceFuture<Result<Self::HeaderEvent, Self::Error>>;

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<LocalBodyRead<Self::BodyEvent>, Self::Error>>;

    fn stop_reading(&mut self, error: Option<Self::Error>);

    fn set_read_timeout(&mut self, timeout: Duration);
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RemoteSourceCreateError {
    #[error("source bridge command capacity must be non-zero")]
    ZeroCapacity,
    #[error(transparent)]
    Worker(#[from] SubmitError),
    #[error(transparent)]
    Cleanup(#[from] CleanupRegistrationError),
}

struct SourceShutdownRegistration(Option<WorkerCleanupRegistration>);

impl Drop for SourceShutdownRegistration {
    fn drop(&mut self) {
        if let Some(registration) = self.0.take() {
            let _ = registration.disarm();
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RemoteSourceError<E>
where
    E: std::error::Error + 'static,
{
    #[error(transparent)]
    Source(#[from] E),
    #[error("a source read is already in flight")]
    ReadInFlight,
    #[error("the remote source command queue is full")]
    CommandQueueFull,
    #[error("the remote source was stopped")]
    Stopped,
    #[error("the remote source is closed")]
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SourceControlError {
    #[error("the remote source control queue is full")]
    CommandQueueFull,
    #[error("the remote source is closed")]
    Closed,
}

enum BridgeLifecycle<E> {
    Open,
    StopRequested(StopRequest<E>),
    Stopped,
    Completed,
    Closed,
}

struct BridgeShared<E> {
    lifecycle: Mutex<BridgeLifecycle<E>>,
    read_in_flight: AtomicBool,
}

impl<E> BridgeShared<E> {
    fn new() -> Self {
        Self {
            lifecycle: Mutex::new(BridgeLifecycle::Open),
            read_in_flight: AtomicBool::new(false),
        }
    }

    fn lifecycle(&self) -> std::sync::MutexGuard<'_, BridgeLifecycle<E>> {
        self.lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn begin_read(&self) -> Result<(), RemoteSourceError<E>>
    where
        E: std::error::Error + 'static,
    {
        let lifecycle = self.lifecycle();
        match &*lifecycle {
            BridgeLifecycle::Open => {}
            BridgeLifecycle::StopRequested(_) | BridgeLifecycle::Stopped => {
                return Err(RemoteSourceError::Stopped);
            }
            BridgeLifecycle::Completed | BridgeLifecycle::Closed => {
                return Err(RemoteSourceError::Closed);
            }
        }
        if self
            .read_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RemoteSourceError::ReadInFlight);
        }
        Ok(())
    }

    fn finish_read(&self) {
        self.read_in_flight.store(false, Ordering::Release);
    }

    fn request_stop(&self, request: StopRequest<E>) -> bool {
        let mut lifecycle = self.lifecycle();
        if !matches!(&*lifecycle, BridgeLifecycle::Open) {
            return false;
        }
        *lifecycle = BridgeLifecycle::StopRequested(request);
        true
    }

    fn take_stop_request(&self) -> Option<StopRequest<E>> {
        let mut lifecycle = self.lifecycle();
        if !matches!(&*lifecycle, BridgeLifecycle::StopRequested(_)) {
            return None;
        }
        match std::mem::replace(&mut *lifecycle, BridgeLifecycle::Stopped) {
            BridgeLifecycle::StopRequested(request) => Some(request),
            _ => unreachable!("lifecycle was checked while locked"),
        }
    }

    fn commit_result(&self, terminal: bool) -> CommitResult {
        let mut lifecycle = self.lifecycle();
        match &*lifecycle {
            BridgeLifecycle::Open => {
                if terminal {
                    *lifecycle = BridgeLifecycle::Completed;
                }
                CommitResult::Deliver
            }
            BridgeLifecycle::StopRequested(_) => CommitResult::Stop,
            BridgeLifecycle::Stopped | BridgeLifecycle::Completed | BridgeLifecycle::Closed => {
                CommitResult::Closed
            }
        }
    }

    fn close_for_owner_drop(&self) -> OwnerDropAction<E> {
        let mut lifecycle = self.lifecycle();
        match std::mem::replace(&mut *lifecycle, BridgeLifecycle::Closed) {
            BridgeLifecycle::Open => OwnerDropAction::StopImplicit,
            BridgeLifecycle::StopRequested(request) => {
                *lifecycle = BridgeLifecycle::Stopped;
                OwnerDropAction::Stop(request)
            }
            BridgeLifecycle::Stopped => {
                *lifecycle = BridgeLifecycle::Stopped;
                OwnerDropAction::DropOnly
            }
            BridgeLifecycle::Completed => {
                *lifecycle = BridgeLifecycle::Completed;
                OwnerDropAction::DropOnly
            }
            BridgeLifecycle::Closed => OwnerDropAction::DropOnly,
        }
    }

    fn remote_closed_error(&self) -> RemoteSourceError<E>
    where
        E: std::error::Error + 'static,
    {
        match &*self.lifecycle() {
            BridgeLifecycle::StopRequested(_) | BridgeLifecycle::Stopped => {
                RemoteSourceError::Stopped
            }
            BridgeLifecycle::Open | BridgeLifecycle::Completed | BridgeLifecycle::Closed => {
                RemoteSourceError::Closed
            }
        }
    }

    fn accepts_control(&self) -> bool {
        matches!(&*self.lifecycle(), BridgeLifecycle::Open)
    }

    fn mark_closed_if_open(&self) {
        let mut lifecycle = self.lifecycle();
        if matches!(&*lifecycle, BridgeLifecycle::Open) {
            *lifecycle = BridgeLifecycle::Closed;
        }
    }

    fn take_failed_stop(&self) -> Option<StopRequest<E>> {
        let mut lifecycle = self.lifecycle();
        if !matches!(&*lifecycle, BridgeLifecycle::StopRequested(_)) {
            return None;
        }
        match std::mem::replace(&mut *lifecycle, BridgeLifecycle::Closed) {
            BridgeLifecycle::StopRequested(request) => Some(request),
            _ => unreachable!("lifecycle was checked while locked"),
        }
    }
}

enum CommitResult {
    Deliver,
    Stop,
    Closed,
}

enum OwnerDropAction<E> {
    Stop(StopRequest<E>),
    StopImplicit,
    DropOnly,
}

struct StopRequest<E> {
    error: Option<E>,
    context: ContextEnvelope,
    span: OperationSpan,
}

impl<E> Drop for StopRequest<E> {
    fn drop(&mut self) {
        if let Some(error) = self.error.take() {
            self.context.run(|| drop(error));
        }
    }
}

/// Cloneable cross-thread capability for one idempotent stop request.
pub struct SourceAbortHandle<E> {
    shared: Arc<BridgeShared<E>>,
    wake: mpsc::Sender<()>,
}

impl<E> Clone for SourceAbortHandle<E> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            wake: self.wake.clone(),
        }
    }
}

impl<E> SourceAbortHandle<E> {
    /// Creates an inert capability for a source whose terminal event was already
    /// consumed. It owns no source, worker, or task; stop requests return false.
    pub fn completed() -> Self {
        let (wake, receiver) = mpsc::channel(1);
        drop(receiver);
        Self {
            shared: Arc::new(BridgeShared {
                lifecycle: Mutex::new(BridgeLifecycle::Completed),
                read_in_flight: AtomicBool::new(false),
            }),
            wake,
        }
    }

    /// Requests owner-worker `stop_reading`. Only the first request is kept.
    pub fn stop_reading(&self, error: Option<E>) -> bool {
        let context = ContextEnvelope::capture();
        let span = OperationSpan::start(OperationKind::Cancellation, &context);
        if !self.shared.request_stop(StopRequest {
            error,
            context,
            span,
        }) {
            return false;
        }
        match self.wake.try_send(()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if let Some(mut request) = self.shared.take_failed_stop() {
                    request.context.run(|| drop(request.error.take()));
                    request.span.complete(TerminalOutcome::Shutdown);
                }
                false
            }
        }
    }
}

struct SourceReplyState<T> {
    accepting: bool,
    output: Option<SourceOutputGuard<T>>,
}

struct SourceReplySender<T> {
    state: Arc<Mutex<SourceReplyState<T>>>,
    notification: Option<oneshot::Sender<()>>,
}

struct SourceReplyReceiver<T> {
    state: Arc<Mutex<SourceReplyState<T>>>,
    notification: Option<oneshot::Receiver<()>>,
}

fn source_reply<T>() -> (SourceReplySender<T>, SourceReplyReceiver<T>) {
    let state = Arc::new(Mutex::new(SourceReplyState {
        accepting: true,
        output: None,
    }));
    let (sender, receiver) = oneshot::channel();
    (
        SourceReplySender {
            state: Arc::clone(&state),
            notification: Some(sender),
        },
        SourceReplyReceiver {
            state,
            notification: Some(receiver),
        },
    )
}

impl<T> SourceReplySender<T> {
    fn send(mut self, mut output: SourceOutputGuard<T>) -> bool {
        let rejected = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.accepting {
                output.disarm();
                state.output = Some(output);
                None
            } else {
                Some(output)
            }
        };
        if let Some(output) = rejected {
            drop(output);
            return false;
        }
        let notification = self.notification.take().expect("one reply notification");
        let _ = notification.send(());
        true
    }
}

impl<T> Drop for SourceReplySender<T> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let failure = catch_unwind(AssertUnwindSafe(|| drop(self.notification.take()))).err();
        finish_source_destruction(failure, unwinding);
    }
}

impl<T> SourceReplyReceiver<T> {
    async fn receive(mut self) -> Result<SourceOutputGuard<T>, oneshot::error::RecvError> {
        self.notification
            .as_mut()
            .expect("a live reply receiver")
            .await?;
        let output = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .output
            .take()
            .expect("notification follows committed output");
        Ok(output)
    }
}

impl<T> Drop for SourceReplyReceiver<T> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let output = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.accepting = false;
            state.output.take()
        };
        let mut failure = catch_unwind(AssertUnwindSafe(|| drop(output))).err();
        retain_first_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| drop(self.notification.take()))).err(),
        );
        finish_source_destruction(failure, unwinding);
    }
}

enum SourceCommandKind<H, B, E> {
    Header(SourceReplySender<Result<H, E>>),
    Body {
        maximum: u32,
        response: SourceReplySender<Result<B, E>>,
    },
}

struct SourceCommand<H, B, E> {
    context: ContextEnvelope,
    span: Option<OperationSpan>,
    kind: Option<SourceCommandKind<H, B, E>>,
}

impl<H, B, E> SourceCommand<H, B, E> {
    fn capture(kind: SourceCommandKind<H, B, E>) -> Self {
        let context = ContextEnvelope::capture();
        let span = OperationSpan::start(OperationKind::Stream, &context);
        Self {
            context,
            span: Some(span),
            kind: Some(kind),
        }
    }

    fn into_parts(mut self) -> (ContextEnvelope, OperationSpan, SourceCommandKind<H, B, E>) {
        (
            self.context.clone(),
            self.span.take().expect("a source command retains its span"),
            self.kind.take().expect("a source command executes once"),
        )
    }

    fn reject(mut self, kind: OperationKind, outcome: TerminalOutcome) {
        let mut rejection = OperationSpan::start(kind, &self.context);
        self.context.run(|| drop(self.kind.take()));
        if let Some(mut span) = self.span.take() {
            span.complete(outcome);
        }
        rejection.complete(outcome);
    }
}

impl<H, B, E> Drop for SourceCommand<H, B, E> {
    fn drop(&mut self) {
        if let Some(kind) = self.kind.take() {
            self.context.run(|| drop(kind));
        }
    }
}

enum SourceControlCommand {
    SetReadTimeout(Duration),
}

struct SourceControlRequest {
    context: ContextEnvelope,
    span: Option<OperationSpan>,
    command: Option<SourceControlCommand>,
}

impl SourceControlRequest {
    fn capture(command: SourceControlCommand) -> Self {
        let context = ContextEnvelope::capture();
        let span = OperationSpan::start(OperationKind::Command, &context);
        Self {
            context,
            span: Some(span),
            command: Some(command),
        }
    }

    fn complete(mut self, owner: impl FnOnce(SourceControlCommand)) {
        self.context.run(|| {
            owner(
                self.command
                    .take()
                    .expect("a source control command executes once"),
            );
        });
        self.span
            .as_mut()
            .expect("a source control command retains its span")
            .complete(TerminalOutcome::Success);
    }

    fn reject(mut self, kind: OperationKind, outcome: TerminalOutcome) {
        let mut rejection = OperationSpan::start(kind, &self.context);
        self.context.run(|| {
            let _command = self.command.take();
        });
        if let Some(mut span) = self.span.take() {
            span.complete(outcome);
        }
        rejection.complete(outcome);
    }
}

impl Drop for SourceControlRequest {
    fn drop(&mut self) {
        if let Some(command) = self.command.take() {
            self.context.run(|| {
                let _command = command;
            });
        }
    }
}

/// Cloneable bounded control capability for owner-worker source updates.
pub struct SourceControlHandle<E> {
    shared: Arc<BridgeShared<E>>,
    commands: mpsc::Sender<SourceControlRequest>,
}

impl<E> Clone for SourceControlHandle<E> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            commands: self.commands.clone(),
        }
    }
}

impl<E> SourceControlHandle<E>
where
    E: std::error::Error + Send + 'static,
{
    pub fn set_read_timeout(&self, timeout: Duration) -> Result<(), SourceControlError> {
        let request = SourceControlRequest::capture(SourceControlCommand::SetReadTimeout(timeout));
        if !self.shared.accepts_control() {
            request.reject(OperationKind::Shutdown, TerminalOutcome::Shutdown);
            return Err(SourceControlError::Closed);
        }
        match self.commands.try_send(request) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(request)) => {
                request.reject(OperationKind::Overload, TerminalOutcome::Overloaded);
                Err(SourceControlError::CommandQueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(request)) => {
                request.reject(OperationKind::Shutdown, TerminalOutcome::Shutdown);
                self.shared.mark_closed_if_open();
                Err(SourceControlError::Closed)
            }
        }
    }
}

/// Repeatable observation of actual source-scope retirement and known owned
/// destruction failures, including destruction performed during source polling.
///
/// This owns no execution or source. Confirmation is not operation success,
/// frame delivery, native/application retirement, child/worker joins, or I/O
/// completion. Dropping observation never cancels or joins the source.
/// Rejected/discarded outputs remain source-owned. Accepted read replies belong
/// to the consumer; their later destruction cannot change this receipt.
#[derive(Clone)]
pub struct SourceCleanupObserver {
    scope: WorkerCleanupObserver,
    destruction_failed: Arc<AtomicBool>,
}

impl SourceCleanupObserver {
    /// Waits for the actual registered scope before combining its outcome with
    /// known owned-destruction failures. Failure is sticky and never publishes
    /// early; ordinary poll panic with successful destruction can still confirm.
    pub fn wait(
        &self,
    ) -> impl Future<Output = Result<(), WorkerCleanupObservationError>> + Send + 'static {
        let scope = self.scope.clone();
        let destruction_failed = Arc::clone(&self.destruction_failed);
        async move {
            scope.wait().await?;
            if destruction_failed.load(Ordering::Acquire) {
                Err(WorkerCleanupObservationError::Unconfirmed)
            } else {
                Ok(())
            }
        }
    }
}

#[derive(Clone, Default)]
struct SourceDestructionReporter(Option<Arc<AtomicBool>>);

impl SourceDestructionReporter {
    fn record_failure(&self) {
        if let Some(failed) = &self.0 {
            failed.store(true, Ordering::Release);
        }
    }

    fn dispose<T>(&self, value: T) -> Option<PanicPayload> {
        let failure = catch_unwind(AssertUnwindSafe(|| drop(value))).err();
        if failure.is_some() {
            self.record_failure();
        }
        failure
    }
}

fn finish_source_destruction(failure: Option<PanicPayload>, unwinding: bool) {
    if let Some(payload) = failure {
        if unwinding {
            discard_panic(payload);
        } else {
            resume_unwind(payload);
        }
    }
}

struct SourceOutputGuard<T> {
    value: Option<T>,
    reporter: SourceDestructionReporter,
}

impl<T> SourceOutputGuard<T> {
    fn new(value: T, reporter: SourceDestructionReporter) -> Self {
        Self {
            value: Some(value),
            reporter,
        }
    }

    fn value(&self) -> &T {
        self.value
            .as_ref()
            .expect("an owned output retains its value")
    }

    fn disarm(&mut self) {
        self.reporter = SourceDestructionReporter::default();
    }

    fn into_value(mut self) -> T {
        self.value.take().expect("one output transfer")
    }
}

impl<T> Drop for SourceOutputGuard<T> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let failure = self.reporter.dispose(self.value.take());
        finish_source_destruction(failure, unwinding);
    }
}

struct SourceFutureGuard<T> {
    future: Option<LocalSourceFuture<T>>,
    reporter: SourceDestructionReporter,
}

impl<T> SourceFutureGuard<T> {
    fn new(future: LocalSourceFuture<T>, reporter: SourceDestructionReporter) -> Self {
        Self {
            future: Some(future),
            reporter,
        }
    }
}

impl<T> Future for SourceFutureGuard<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        self.future
            .as_mut()
            .expect("a live guard retains its future")
            .as_mut()
            .poll(context)
    }
}

impl<T> Drop for SourceFutureGuard<T> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let failure = self.reporter.dispose(self.future.take());
        finish_source_destruction(failure, unwinding);
    }
}

struct SourceOutputFuture<T> {
    read: SourceFutureGuard<T>,
}

struct StagedSourceFuture<'a, F: Future> {
    future: F,
    output: &'a mut Option<F::Output>,
}

impl<F: Future + Unpin> Future for StagedSourceFuture<'_, F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let Poll::Ready(output) = Pin::new(&mut this.future).poll(context) else {
            return Poll::Pending;
        };
        *this.output = Some(output);
        Poll::Ready(())
    }
}

impl<T> Future for SourceOutputFuture<T> {
    type Output = SourceOutputGuard<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.read)
            .poll(context)
            .map(|value| SourceOutputGuard::new(value, self.read.reporter.clone()))
    }
}

enum SourceBodyOutput<B, E> {
    Event(SourceOutputGuard<Result<B, E>>),
    Suspend(LocalSourceFuture<()>),
}

struct SourceBodyFuture<B, E> {
    read: SourceFutureGuard<Result<LocalBodyRead<B>, E>>,
}

impl<B, E> Future for SourceBodyFuture<B, E> {
    type Output = SourceBodyOutput<B, E>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.read).poll(context) {
            Poll::Ready(Ok(LocalBodyRead::Suspend(resume))) => {
                Poll::Ready(SourceBodyOutput::Suspend(Box::pin(SourceFutureGuard::new(
                    resume,
                    self.read.reporter.clone(),
                ))))
            }
            Poll::Ready(Ok(LocalBodyRead::Event(event))) => Poll::Ready(SourceBodyOutput::Event(
                SourceOutputGuard::new(Ok(event), self.read.reporter.clone()),
            )),
            Poll::Ready(Err(error)) => Poll::Ready(SourceBodyOutput::Event(
                SourceOutputGuard::new(Err(error), self.read.reporter.clone()),
            )),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct LocalSourceOwner<S>
where
    S: LocalHttpSource,
{
    source: Option<S>,
    shared: Arc<BridgeShared<S::Error>>,
    reporter: SourceDestructionReporter,
}

impl<S> LocalSourceOwner<S>
where
    S: LocalHttpSource,
{
    fn source_mut(&mut self) -> &mut S {
        self.source
            .as_mut()
            .expect("source exists until terminal completion")
    }

    fn complete(&mut self) {
        self.destroy();
    }

    fn destroy(&mut self) {
        let unwinding = std::thread::panicking();
        let failure = self.reporter.dispose(self.source.take());
        finish_source_destruction(failure, unwinding);
    }

    fn notify_stop_requested(&mut self) -> Option<StopRequest<S::Error>> {
        let mut request = self.shared.take_stop_request()?;
        request.context.run(|| {
            if let Some(source) = &mut self.source {
                source.stop_reading(request.error.take());
            }
        });
        Some(request)
    }

    fn finish_stop_requested(&mut self, mut request: StopRequest<S::Error>) {
        request.context.run(|| self.destroy());
        request.span.complete(TerminalOutcome::Success);
    }

    fn stop_requested(&mut self) {
        if let Some(request) = self.notify_stop_requested() {
            self.finish_stop_requested(request);
        }
    }

    fn notify_closed(&mut self) {
        self.shared.mark_closed_if_open();
        if let Some(source) = &mut self.source {
            source.stop_reading(None);
        }
    }

    fn close(&mut self) {
        self.notify_closed();
        self.destroy();
    }
}

impl<S> Drop for LocalSourceOwner<S>
where
    S: LocalHttpSource,
{
    fn drop(&mut self) {
        if self.source.is_none() {
            self.shared.finish_read();
            return;
        }
        let unwinding = std::thread::panicking();
        let mut failure = None;
        match self.shared.close_for_owner_drop() {
            OwnerDropAction::Stop(mut request) => {
                retain_first_panic(
                    &mut failure,
                    catch_unwind(AssertUnwindSafe(|| {
                        request
                            .context
                            .run(|| self.source_mut().stop_reading(request.error.take()));
                    }))
                    .err(),
                );
                retain_first_panic(
                    &mut failure,
                    catch_unwind(AssertUnwindSafe(|| {
                        request.context.run(|| self.destroy());
                    }))
                    .err(),
                );
                retain_first_panic(
                    &mut failure,
                    catch_unwind(AssertUnwindSafe(|| {
                        request.span.complete(TerminalOutcome::Success);
                    }))
                    .err(),
                );
                retain_first_panic(&mut failure, self.reporter.dispose(request));
            }
            OwnerDropAction::StopImplicit => {
                retain_first_panic(
                    &mut failure,
                    catch_unwind(AssertUnwindSafe(|| {
                        self.source_mut().stop_reading(None);
                    }))
                    .err(),
                );
            }
            OwnerDropAction::DropOnly => {}
        }
        retain_first_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| self.destroy())).err(),
        );
        self.shared.finish_read();
        if failure.is_some() {
            self.reporter.record_failure();
        }
        finish_source_destruction(failure, unwinding);
    }
}

/// A single-use, owner-local handoff to an already registered source scope.
///
/// Dropping an unused attachment requests cancellation without joining. It may
/// attach during grace, but not after forced shutdown or source cancellation.
/// The attachment deliberately remains `!Send`, even for a `Send` source.
pub struct LocalSourceAttachment<S: LocalHttpSource> {
    sender: Option<oneshot::Sender<LocalSourceOwner<S>>>,
    abort: SourceAbortHandle<S::Error>,
    worker: WorkerHandle,
    reporter: SourceDestructionReporter,
    _local: PhantomData<Rc<()>>,
}

impl<S: LocalHttpSource> LocalSourceAttachment<S> {
    /// Transfers a source on its original worker, without new worker admission.
    ///
    /// Rejection stops and destroys the input locally. Acceptance transfers
    /// ownership to the registered scope, including cancellation before that
    /// scope first consumes the handoff. No `Send`, `Sync`, or `Unpin` source
    /// bound is required.
    pub fn attach(mut self, source: S) -> Result<(), RemoteSourceCreateError> {
        let mut owner = LocalSourceOwner {
            source: Some(source),
            shared: Arc::new(BridgeShared::new()),
            reporter: SourceDestructionReporter::default(),
        };
        if self.worker.state() != WorkerState::Open || !self.abort.shared.accepts_control() {
            return Err(CleanupRegistrationError::Closed.into());
        }
        owner.shared = Arc::clone(&self.abort.shared);
        owner.reporter = self.reporter.clone();
        let sender = self
            .sender
            .take()
            .expect("an unused attachment has a sender");
        if let Err(mut owner) = sender.send(owner) {
            owner.reporter = SourceDestructionReporter::default();
            owner.shared = Arc::new(BridgeShared::new());
            return Err(CleanupRegistrationError::Closed.into());
        }
        Ok(())
    }
}

impl<S: LocalHttpSource> Drop for LocalSourceAttachment<S> {
    fn drop(&mut self) {
        if self.sender.is_some() {
            self.abort.stop_reading(None);
        }
    }
}

struct SourceHandoff<S: LocalHttpSource> {
    receiver: Option<oneshot::Receiver<LocalSourceOwner<S>>>,
    shared: Arc<BridgeShared<S::Error>>,
    attached: bool,
    reporter: SourceDestructionReporter,
}

impl<S: LocalHttpSource> Drop for SourceHandoff<S> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut failure = self.reporter.dispose(self.receiver.take());
        if !self.attached {
            retain_first_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| {
                    let mut owner = LocalSourceOwner::<S> {
                        source: None,
                        shared: Arc::clone(&self.shared),
                        reporter: self.reporter.clone(),
                    };
                    owner.stop_requested();
                }))
                .err(),
            );
            self.shared.mark_closed_if_open();
            self.shared.finish_read();
        }
        if failure.is_some() {
            self.reporter.record_failure();
        }
        finish_source_destruction(failure, unwinding);
    }
}

async fn drive_prepared_source<S: LocalHttpSource>(
    mut handoff: SourceHandoff<S>,
    commands: mpsc::Receiver<SourceCommand<S::HeaderEvent, S::BodyEvent, S::Error>>,
    controls: mpsc::Receiver<SourceControlRequest>,
    mut stops: mpsc::Receiver<()>,
) {
    let owner = tokio::select! {
        biased;
        _ = stops.recv() => return,
        result = handoff.receiver.as_mut().expect("an unconsumed handoff has a receiver") => {
            match result {
                Ok(owner) => owner,
                Err(_) => return,
            }
        }
    };
    handoff.attached = true;
    drive_source_owner(owner, commands, controls, stops).await;
}

/// A single-consumer handle that forwards reads to a source's original worker.
///
/// The `Cell` marker intentionally makes the handle `!Sync`; it remains `Send`
/// so its sole consumer may live on a different worker.
pub struct RemoteHttpSource<H, B, E> {
    owner: WorkerId,
    commands: mpsc::Sender<SourceCommand<H, B, E>>,
    control: SourceControlHandle<E>,
    abort: SourceAbortHandle<E>,
    shared: Arc<BridgeShared<E>>,
    _single_consumer: PhantomData<Cell<()>>,
}

impl<H, B, E> RemoteHttpSource<H, B, E>
where
    H: RemoteSourceEvent + Send + 'static,
    B: RemoteSourceEvent + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    /// Exports an existing owner-local source without a transferable factory.
    ///
    /// The worker driver owns and polls the source through a registered scope,
    /// including after the initializing command returns. Graceful quiescence
    /// lets existing reads finish; forced shutdown stops and destroys the source
    /// on its worker. Rejected sources are also stopped and destroyed locally.
    pub fn try_from_local<S>(
        context: &WorkerContext,
        command_capacity: usize,
        source: S,
    ) -> Result<Self, RemoteSourceCreateError>
    where
        S: LocalHttpSource<HeaderEvent = H, BodyEvent = B, Error = E>,
    {
        Self::try_from_local_with_observer(context, command_capacity, source)
            .map(|(source, _observer)| source)
    }

    /// Exports a local source and observes its actual registered driving scope.
    ///
    /// The receipt confirms future/capture retirement on the owner, not read
    /// success, delivery, child joins, worker shutdown, or transport completion.
    /// Dropping the observer does not cancel the source. Failure while destroying
    /// the scope future/captures, or loss of its publisher, is unconfirmed. A
    /// source operation panic followed by successful scope destruction can still
    /// confirm retirement; it does not certify successful source callbacks.
    /// Accepted reply ownership is independent of scope retirement, including
    /// replies abandoned by the consumer before receiving their notification.
    pub fn try_from_local_with_observer<S>(
        context: &WorkerContext,
        command_capacity: usize,
        source: S,
    ) -> Result<(Self, SourceCleanupObserver), RemoteSourceCreateError>
    where
        S: LocalHttpSource<HeaderEvent = H, BodyEvent = B, Error = E>,
    {
        let shared = Arc::new(BridgeShared::new());
        let mut owner = LocalSourceOwner {
            source: Some(source),
            shared: Arc::clone(&shared),
            reporter: SourceDestructionReporter::default(),
        };
        if command_capacity == 0 {
            return Err(RemoteSourceCreateError::ZeroCapacity);
        }
        if !context.handle().is_accepting() {
            return Err(CleanupRegistrationError::Closed.into());
        }
        let (command_tx, command_rx) = mpsc::channel(command_capacity);
        let (control_tx, control_rx) = mpsc::channel(command_capacity);
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let abort = SourceAbortHandle {
            shared: Arc::clone(&shared),
            wake: stop_tx,
        };
        let shutdown_abort = abort.clone();
        let shutdown = SourceShutdownRegistration(Some(context.register_cleanup(move || {
            shutdown_abort.stop_reading(None);
        })?));
        let destruction_failed = Arc::new(AtomicBool::new(false));
        owner.reporter = SourceDestructionReporter(Some(Arc::clone(&destruction_failed)));
        let scope = context.register_scope(async move {
            let _shutdown = shutdown;
            drive_source_owner(owner, command_rx, control_rx, stop_rx).await;
        })?;
        let observer = SourceCleanupObserver {
            scope: scope.observe(),
            destruction_failed,
        };
        drop(scope.activate()?);
        if !context.handle().is_accepting() {
            abort.stop_reading(None);
            return Err(CleanupRegistrationError::Closed.into());
        }
        Ok((
            Self {
                owner: context.handle().id(),
                commands: command_tx,
                control: SourceControlHandle {
                    shared: Arc::clone(&shared),
                    commands: control_tx,
                },
                abort,
                shared,
                _single_consumer: PhantomData,
            },
            observer,
        ))
    }

    /// Reserves a source-driving scope while worker admission is open.
    ///
    /// Returns an owner-local attachment, a transferable consumer, and the real
    /// scope receipt captured before activation. The same scope waits for the
    /// single handoff and drives the source. It holds graceful-shutdown credit
    /// until retirement; a previously admitted continuation may attach during
    /// grace without submitting a new command. Forced shutdown, either endpoint
    /// being dropped, or explicit source cancellation ends an unused reservation.
    /// Observation alone never keeps the scope alive or cancels it.
    pub fn prepare_local<S>(
        context: &WorkerContext,
        command_capacity: usize,
    ) -> Result<(LocalSourceAttachment<S>, Self, SourceCleanupObserver), RemoteSourceCreateError>
    where
        S: LocalHttpSource<HeaderEvent = H, BodyEvent = B, Error = E>,
    {
        if command_capacity == 0 {
            return Err(RemoteSourceCreateError::ZeroCapacity);
        }
        if !context.handle().is_accepting() {
            return Err(CleanupRegistrationError::Closed.into());
        }
        let shared = Arc::new(BridgeShared::new());
        let (command_tx, command_rx) = mpsc::channel(command_capacity);
        let (control_tx, control_rx) = mpsc::channel(command_capacity);
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let (sender, receiver) = oneshot::channel();
        let destruction_failed = Arc::new(AtomicBool::new(false));
        let reporter = SourceDestructionReporter(Some(Arc::clone(&destruction_failed)));
        let abort = SourceAbortHandle {
            shared: Arc::clone(&shared),
            wake: stop_tx,
        };
        let handoff = SourceHandoff {
            receiver: Some(receiver),
            shared: Arc::clone(&shared),
            attached: false,
            reporter: reporter.clone(),
        };
        let shutdown_abort = abort.clone();
        let shutdown = SourceShutdownRegistration(Some(context.register_cleanup(move || {
            shutdown_abort.stop_reading(None);
        })?));
        let scope = context.register_scope(async move {
            let _shutdown = shutdown;
            drive_prepared_source(handoff, command_rx, control_rx, stop_rx).await;
        })?;
        let observer = SourceCleanupObserver {
            scope: scope.observe(),
            destruction_failed,
        };
        drop(scope.activate()?);
        if !context.handle().is_accepting() {
            abort.stop_reading(None);
            return Err(CleanupRegistrationError::Closed.into());
        }
        let attachment = LocalSourceAttachment {
            sender: Some(sender),
            abort: abort.clone(),
            worker: context.handle().clone(),
            reporter,
            _local: PhantomData,
        };
        Ok((
            attachment,
            Self {
                owner: context.handle().id(),
                commands: command_tx,
                control: SourceControlHandle {
                    shared: Arc::clone(&shared),
                    commands: control_tx,
                },
                abort,
                shared,
                _single_consumer: PhantomData,
            },
            observer,
        ))
    }

    pub fn new<S, F>(
        worker: &WorkerHandle,
        command_capacity: usize,
        source_factory: F,
    ) -> Result<Self, RemoteSourceCreateError>
    where
        S: LocalHttpSource<HeaderEvent = H, BodyEvent = B, Error = E>,
        F: FnOnce(&WorkerContext) -> S + Send + 'static,
    {
        if command_capacity == 0 {
            return Err(RemoteSourceCreateError::ZeroCapacity);
        }

        let (command_tx, command_rx) = mpsc::channel(command_capacity);
        let (control_tx, control_rx) = mpsc::channel(command_capacity);
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let shared = Arc::new(BridgeShared::new());
        let owner = worker.id();
        let owner_shared = Arc::clone(&shared);
        worker.spawn_local(move |context| {
            let source = source_factory(&context);
            drive_source(source, owner_shared, command_rx, control_rx, stop_rx)
        })?;

        Ok(Self {
            owner,
            commands: command_tx,
            control: SourceControlHandle {
                shared: Arc::clone(&shared),
                commands: control_tx,
            },
            abort: SourceAbortHandle {
                shared: Arc::clone(&shared),
                wake: stop_tx,
            },
            shared,
            _single_consumer: PhantomData,
        })
    }

    pub fn owner_worker(&self) -> WorkerId {
        self.owner
    }

    pub fn abort_handle(&self) -> SourceAbortHandle<E> {
        self.abort.clone()
    }

    pub fn control_handle(&self) -> SourceControlHandle<E> {
        self.control.clone()
    }

    pub fn set_read_timeout(&self, timeout: Duration) -> Result<(), SourceControlError> {
        self.control.set_read_timeout(timeout)
    }

    pub fn stop_reading(&mut self, error: Option<E>) -> bool {
        self.abort.stop_reading(error)
    }

    pub async fn read_header_event(&mut self) -> Result<H, RemoteSourceError<E>> {
        self.shared.begin_read()?;
        let (response_tx, response_rx) = source_reply();
        let command = SourceCommand::capture(SourceCommandKind::Header(response_tx));
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                command.reject(OperationKind::Overload, TerminalOutcome::Overloaded);
                self.shared.finish_read();
                return Err(RemoteSourceError::CommandQueueFull);
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                command.reject(OperationKind::Shutdown, TerminalOutcome::Shutdown);
                self.shared.finish_read();
                self.shared.mark_closed_if_open();
                return Err(self.shared.remote_closed_error());
            }
        }
        match response_rx.receive().await {
            Ok(result) => result.into_value().map_err(RemoteSourceError::Source),
            Err(_) => Err(self.shared.remote_closed_error()),
        }
    }

    pub async fn read_body_event(&mut self, maximum: u32) -> Result<B, RemoteSourceError<E>> {
        self.shared.begin_read()?;
        let (response_tx, response_rx) = source_reply();
        let command = SourceCommand::capture(SourceCommandKind::Body {
            maximum,
            response: response_tx,
        });
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                command.reject(OperationKind::Overload, TerminalOutcome::Overloaded);
                self.shared.finish_read();
                return Err(RemoteSourceError::CommandQueueFull);
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                command.reject(OperationKind::Shutdown, TerminalOutcome::Shutdown);
                self.shared.finish_read();
                self.shared.mark_closed_if_open();
                return Err(self.shared.remote_closed_error());
            }
        }
        match response_rx.receive().await {
            Ok(result) => result.into_value().map_err(RemoteSourceError::Source),
            Err(_) => Err(self.shared.remote_closed_error()),
        }
    }
}

impl<H, B, E> Drop for RemoteHttpSource<H, B, E> {
    fn drop(&mut self) {
        let _ = self.abort.stop_reading(None);
    }
}

async fn drive_source<S>(
    source: S,
    shared: Arc<BridgeShared<S::Error>>,
    commands: mpsc::Receiver<SourceCommand<S::HeaderEvent, S::BodyEvent, S::Error>>,
    controls: mpsc::Receiver<SourceControlRequest>,
    stops: mpsc::Receiver<()>,
) where
    S: LocalHttpSource,
{
    let owner = LocalSourceOwner {
        source: Some(source),
        shared,
        reporter: SourceDestructionReporter::default(),
    };
    drive_source_owner(owner, commands, controls, stops).await;
}

async fn drive_source_owner<S>(
    mut owner: LocalSourceOwner<S>,
    mut commands: mpsc::Receiver<SourceCommand<S::HeaderEvent, S::BodyEvent, S::Error>>,
    mut controls: mpsc::Receiver<SourceControlRequest>,
    mut stops: mpsc::Receiver<()>,
) where
    S: LocalHttpSource,
{
    loop {
        tokio::select! {
            biased;
            stop = stops.recv() => {
                if stop.is_some() {
                    owner.stop_requested();
                } else {
                    owner.close();
                }
                owner.shared.finish_read();
                return;
            }
            control = controls.recv() => {
                match control {
                    Some(control) => apply_control(&mut owner, control),
                    None => {
                        owner.close();
                        owner.shared.finish_read();
                        return;
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    owner.close();
                    owner.shared.finish_read();
                    return;
                };
                let (context, span, kind) = command.into_parts();
                let finished = match kind {
                    SourceCommandKind::Header(response) => {
                        drive_header_read(
                            &mut owner,
                            context,
                            span,
                            response,
                            &mut controls,
                            &mut stops,
                        ).await
                    }
                    SourceCommandKind::Body { maximum, response } => {
                        drive_body_read(
                            &mut owner,
                            context,
                            span,
                            maximum,
                            response,
                            &mut controls,
                            &mut stops,
                        ).await
                    }
                };
                if finished {
                    return;
                }
            }
        }
    }
}

async fn drive_header_read<S>(
    owner: &mut LocalSourceOwner<S>,
    context: ContextEnvelope,
    span: OperationSpan,
    response: SourceReplySender<Result<S::HeaderEvent, S::Error>>,
    controls: &mut mpsc::Receiver<SourceControlRequest>,
    stops: &mut mpsc::Receiver<()>,
) -> bool
where
    S: LocalHttpSource,
{
    let mut future = None;
    context.run(|| {
        future = Some(SourceOutputFuture {
            read: SourceFutureGuard::new(
                owner.source_mut().read_header_event(),
                owner.reporter.clone(),
            ),
        });
    });
    let mut output = None;
    let future = context.wrap(StagedSourceFuture {
        future: future
            .take()
            .expect("a restored constructor retains its future"),
        output: &mut output,
    });
    match await_owner_operation(owner, future, controls, stops).await {
        OwnerOperation::Ready(()) => deliver_result(
            owner,
            &context,
            span,
            output
                .take()
                .expect("a ready source operation staged its output"),
            response,
        ),
        OwnerOperation::Stopped => {
            owner.shared.finish_read();
            true
        }
    }
}

async fn drive_body_read<S>(
    owner: &mut LocalSourceOwner<S>,
    context: ContextEnvelope,
    span: OperationSpan,
    maximum: u32,
    response: SourceReplySender<Result<S::BodyEvent, S::Error>>,
    controls: &mut mpsc::Receiver<SourceControlRequest>,
    stops: &mut mpsc::Receiver<()>,
) -> bool
where
    S: LocalHttpSource,
{
    loop {
        let mut future = None;
        context.run(|| {
            future = Some(SourceBodyFuture {
                read: SourceFutureGuard::new(
                    owner.source_mut().read_body_event(maximum),
                    owner.reporter.clone(),
                ),
            });
        });
        let mut output = None;
        let future = context.wrap(StagedSourceFuture {
            future: future
                .take()
                .expect("a restored constructor retains its future"),
            output: &mut output,
        });
        let result = match await_owner_operation(owner, future, controls, stops).await {
            OwnerOperation::Ready(()) => output
                .take()
                .expect("a ready source operation staged its output"),
            OwnerOperation::Stopped => {
                owner.shared.finish_read();
                return true;
            }
        };

        match result {
            SourceBodyOutput::Event(result) => {
                return deliver_result(owner, &context, span, result, response);
            }
            SourceBodyOutput::Suspend(resume) => {
                let resume = context.wrap(resume);
                if matches!(
                    await_owner_operation(owner, resume, controls, stops).await,
                    OwnerOperation::Stopped
                ) {
                    owner.shared.finish_read();
                    return true;
                }
            }
        }
    }
}

enum OwnerOperation<T> {
    Ready(T),
    Stopped,
}

async fn await_owner_operation<S, F>(
    owner: &mut LocalSourceOwner<S>,
    mut operation: ContextFuture<F>,
    controls: &mut mpsc::Receiver<SourceControlRequest>,
    stops: &mut mpsc::Receiver<()>,
) -> OwnerOperation<F::Output>
where
    S: LocalHttpSource,
    F: Future,
{
    loop {
        tokio::select! {
            biased;
            stop = stops.recv() => {
                if stop.is_some() {
                    let request = owner.notify_stop_requested();
                    drop(operation);
                    if let Some(request) = request {
                        owner.finish_stop_requested(request);
                    }
                } else {
                    owner.notify_closed();
                    drop(operation);
                    owner.destroy();
                }
                return OwnerOperation::Stopped;
            }
            control = controls.recv() => {
                match control {
                    Some(control) => apply_control(owner, control),
                    None => {
                        owner.notify_closed();
                        drop(operation);
                        owner.destroy();
                        return OwnerOperation::Stopped;
                    }
                }
            }
            result = &mut operation => return OwnerOperation::Ready(result),
        }
    }
}

fn deliver_result<S, T>(
    owner: &mut LocalSourceOwner<S>,
    context: &ContextEnvelope,
    mut span: OperationSpan,
    result: SourceOutputGuard<Result<T, S::Error>>,
    response: SourceReplySender<Result<T, S::Error>>,
) -> bool
where
    S: LocalHttpSource,
    T: RemoteSourceEvent,
{
    let failed = result.value().is_err();
    let terminal = result
        .value()
        .as_ref()
        .map_or(true, RemoteSourceEvent::is_terminal);
    let (finished, outcome) = context.run(|| match owner.shared.commit_result(terminal) {
        CommitResult::Deliver => {
            if terminal {
                owner.complete();
            }
            owner.shared.finish_read();
            let delivered = response.send(result);
            let outcome = if failed {
                TerminalOutcome::Failure
            } else if !delivered {
                TerminalOutcome::Cancelled
            } else {
                TerminalOutcome::Success
            };
            (terminal, outcome)
        }
        CommitResult::Stop => {
            drop(result);
            owner.stop_requested();
            owner.shared.finish_read();
            (true, TerminalOutcome::Cancelled)
        }
        CommitResult::Closed => {
            drop(result);
            owner.close();
            owner.shared.finish_read();
            (true, TerminalOutcome::Shutdown)
        }
    });
    span.complete(outcome);
    finished
}

fn apply_control<S>(owner: &mut LocalSourceOwner<S>, control: SourceControlRequest)
where
    S: LocalHttpSource,
{
    control.complete(|command| match command {
        SourceControlCommand::SetReadTimeout(timeout) => {
            owner.source_mut().set_read_timeout(timeout);
        }
    });
}
