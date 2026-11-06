use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::observability::SpanObserver;
use crate::thread::current_id;
use crate::worker::SubmitError;
use crate::worker::WorkerCall;
use crate::worker::WorkerContext;
use crate::worker::WorkerHandle;

static NEXT_CONTEXT_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_CONTEXT: Cell<Option<RuntimeContext>> = const { Cell::new(None) };
    static CURRENT_SPAN_LINK: Cell<Option<SpanLink>> = const { Cell::new(None) };
    static CURRENT_SPAN_OBSERVER: Cell<Option<SpanObserver>> = const { Cell::new(None) };
}

#[cfg(test)]
thread_local! {
    static CONTEXT_INSTALLS: Cell<usize> = const { Cell::new(0) };
    static COLD_RESTORES: Cell<usize> = const { Cell::new(0) };
}

/// How many envelope installs this thread has made.
#[cfg(test)]
pub(crate) fn context_installs() -> usize {
    CONTEXT_INSTALLS.get()
}

/// How many installs this thread has restored from `InstalledContext::drop`.
#[cfg(test)]
pub(crate) fn cold_context_restores() -> usize {
    COLD_RESTORES.get()
}

/// The span link this thread has installed, read without capturing an
/// envelope, which under `observability` would call the tracing subscriber.
#[cfg(test)]
pub(crate) fn current_span_link() -> Option<SpanLink> {
    CURRENT_SPAN_LINK.with(Cell::get)
}

/// The span observer this thread has installed, read without capturing an
/// envelope.
#[cfg(test)]
pub(crate) fn current_span_observer() -> Option<SpanObserver> {
    CURRENT_SPAN_OBSERVER.with(clone_current)
}

// Both stored types clone an `Arc`, which cannot re-enter the cell while it is briefly empty.
fn clone_current<T: Clone>(current: &Cell<Option<T>>) -> Option<T> {
    let value = current.take();
    let cloned = value.clone();
    current.set(value);
    cloned
}

#[derive(Debug, Eq, PartialEq)]
struct ContextIdentity(u64);

/// An owned request-scoped identity propagated across runtime commands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeContext(Arc<ContextIdentity>);

impl RuntimeContext {
    pub fn new() -> Self {
        Self(Arc::new(ContextIdentity(
            NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
        )))
    }

    pub fn current() -> Option<Self> {
        CURRENT_CONTEXT.with(clone_current)
    }

    pub fn set_current(context: Option<Self>) -> Option<Self> {
        CURRENT_CONTEXT.with(|current| current.replace(context))
    }

    pub fn enter(&self) -> RuntimeContextGuard {
        let previous = CURRENT_CONTEXT.with(|current| current.replace(Some(self.clone())));
        RuntimeContextGuard {
            previous,
            owner_thread: current_id(),
            _not_send: PhantomData,
        }
    }
}

impl Default for RuntimeContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Restores the request context that was current before entry.
pub struct RuntimeContextGuard {
    previous: Option<RuntimeContext>,
    owner_thread: std::thread::ThreadId,
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for RuntimeContextGuard {
    fn drop(&mut self) {
        assert_eq!(
            current_id(),
            self.owner_thread,
            "runtime context guard must be dropped on its owner thread",
        );
        CURRENT_CONTEXT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

/// Stable logical identity used to preserve span parentage across a queue.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SpanLink(pub(crate) u64);

/// A Send envelope captured at command submission and installed per poll.
#[derive(Debug)]
pub struct ContextEnvelope {
    request: Option<RuntimeContext>,
    #[cfg(feature = "observability")]
    span: tracing::Span,
    span_link: Option<SpanLink>,
    span_observer: Option<SpanObserver>,
}

impl Clone for ContextEnvelope {
    fn clone(&self) -> Self {
        Self {
            request: self.request_context(),
            #[cfg(feature = "observability")]
            span: self.span.clone(),
            span_link: self.span_link,
            span_observer: self.span_observer.clone(),
        }
    }
}

impl ContextEnvelope {
    pub fn capture() -> Self {
        Self {
            request: RuntimeContext::current(),
            #[cfg(feature = "observability")]
            span: tracing::Span::current(),
            span_link: CURRENT_SPAN_LINK.with(Cell::get),
            span_observer: CURRENT_SPAN_OBSERVER.with(clone_current),
        }
    }

    pub fn request_context(&self) -> Option<RuntimeContext> {
        self.request.clone()
    }

    pub fn linked_span(&self) -> Option<SpanLink> {
        self.span_link
    }

    pub fn with_span_observer(mut self, observer: SpanObserver) -> Self {
        self.span_observer = Some(observer);
        self
    }

    pub fn run<T>(&self, callback: impl FnOnce() -> T) -> T {
        let mut installed = self.install();
        let result = callback();
        drop(installed.restore_and_take_request());
        result
    }

    pub fn wrap<F>(&self, future: F) -> ContextFuture<F>
    where
        F: Future,
    {
        ContextFuture {
            future: Some(Box::pin(future)),
            envelope: self.clone(),
        }
    }

    #[cfg(feature = "observability")]
    pub(crate) fn child_with_span(&self, span: tracing::Span, link: SpanLink) -> Self {
        Self {
            request: self.request_context(),
            span,
            span_link: Some(link),
            span_observer: self.span_observer.clone(),
        }
    }

    #[cfg(not(feature = "observability"))]
    pub(crate) fn child_with_link(&self, link: SpanLink) -> Self {
        Self {
            request: self.request_context(),
            span_link: Some(link),
            span_observer: self.span_observer.clone(),
        }
    }

    pub(crate) fn span_observer(&self) -> Option<&SpanObserver> {
        self.span_observer.as_ref()
    }

    pub(crate) fn set_request_context(&mut self, request: Option<RuntimeContext>) {
        self.request = request;
    }

    #[cfg(feature = "observability")]
    pub(crate) fn span(&self) -> &tracing::Span {
        &self.span
    }

    #[inline(always)]
    fn install(&self) -> InstalledContext<'_> {
        #[cfg(test)]
        CONTEXT_INSTALLS.set(CONTEXT_INSTALLS.get() + 1);
        #[cfg(feature = "observability")]
        let span = self.span.enter();
        let request = self.request.clone();
        let previous_request = CURRENT_CONTEXT.with(|current| current.replace(request));
        let previous_span_link = CURRENT_SPAN_LINK.with(|current| current.replace(self.span_link));
        let previous_span_observer =
            CURRENT_SPAN_OBSERVER.with(|current| current.replace(self.span_observer.clone()));
        InstalledContext {
            previous_request: ManuallyDrop::new(previous_request),
            previous_span_link,
            previous_span_observer: ManuallyDrop::new(previous_span_observer),
            restored: false,
            #[cfg(feature = "observability")]
            _span: span,
            _lifetime: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn run_and_capture<T>(
        &self,
        callback: impl FnOnce() -> T,
    ) -> (T, Option<RuntimeContext>) {
        let mut installed = self.install();
        let result = callback();
        let request = installed.restore_and_take_request();
        (result, request)
    }
}

struct InstalledContext<'a> {
    // Restoration runs exactly once, explicitly or from `drop`, and moves both values out before
    // anything that can panic, so they never need dropping in place.
    previous_request: ManuallyDrop<Option<RuntimeContext>>,
    previous_span_link: Option<SpanLink>,
    previous_span_observer: ManuallyDrop<Option<SpanObserver>>,
    restored: bool,
    #[cfg(feature = "observability")]
    _span: tracing::span::Entered<'a>,
    _lifetime: PhantomData<&'a ()>,
}

impl InstalledContext<'_> {
    /// Returns the request context that was current just before restoration.
    #[inline(always)]
    fn restore_and_take_request(&mut self) -> Option<RuntimeContext> {
        self.restored = true;
        let previous_span_observer = self.previous_span_observer.take();
        let previous_request = self.previous_request.take();
        CURRENT_SPAN_LINK.with(|current| current.set(self.previous_span_link));
        drop(CURRENT_SPAN_OBSERVER.with(|current| current.replace(previous_span_observer)));
        CURRENT_CONTEXT.with(|current| current.replace(previous_request))
    }

    #[cold]
    #[inline(never)]
    fn restore(&mut self) {
        #[cfg(test)]
        COLD_RESTORES.set(COLD_RESTORES.get() + 1);
        drop(self.restore_and_take_request());
    }
}

impl Drop for InstalledContext<'_> {
    fn drop(&mut self) {
        if !self.restored {
            self.restore();
        }
    }
}

/// A future whose inner value is polled and destroyed under its captured context.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct ContextFuture<F> {
    future: Option<Pin<Box<F>>>,
    envelope: ContextEnvelope,
}

impl<F> ContextFuture<F> {
    pub(crate) fn envelope(&self) -> &ContextEnvelope {
        &self.envelope
    }
}

impl<F> Future for ContextFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        #[cfg(feature = "observability")]
        let envelope = this.envelope.clone();
        #[cfg(not(feature = "observability"))]
        let envelope = &this.envelope;
        let (result, next_request) = envelope.run_and_capture(|| {
            let result = this
                .future
                .as_mut()
                .expect("a completed context future is not polled again")
                .as_mut()
                .poll(task);
            if result.is_ready() {
                drop(this.future.take());
            }
            result
        });
        this.envelope.set_request_context(next_request);
        result
    }
}

impl<F> Drop for ContextFuture<F> {
    fn drop(&mut self) {
        let Some(future) = self.future.take() else {
            return;
        };
        let envelope = self.envelope.clone();
        let _installed = envelope.install();
        drop(future);
    }
}

/// A `ContextFuture` that holds its `Unpin` future inline, for the runtime's
/// task wrappers.
pub(crate) struct ContextTask<F> {
    future: Option<F>,
    envelope: ContextEnvelope,
}

impl<F> ContextTask<F> {
    pub(crate) fn new(future: F, envelope: ContextEnvelope) -> Self {
        Self {
            future: Some(future),
            envelope,
        }
    }

    pub(crate) fn envelope(&self) -> &ContextEnvelope {
        &self.envelope
    }

    /// The inner future, while it has not completed.
    #[cfg(not(feature = "observability"))]
    pub(crate) fn future_mut(&mut self) -> Option<&mut F> {
        self.future.as_mut()
    }

    /// Runs `poll` on the inner future under the envelope, and keeps the
    /// request context it leaves for the next install, as `ContextFuture`
    /// does. A ready result drops the inner future inside that install.
    #[inline(always)]
    pub(crate) fn poll_with<T>(&mut self, poll: impl FnOnce(&mut F) -> Poll<T>) -> Poll<T> {
        #[cfg(feature = "observability")]
        let envelope = self.envelope.clone();
        #[cfg(not(feature = "observability"))]
        let envelope = &self.envelope;
        let future = &mut self.future;
        let (result, next_request) = envelope.run_and_capture(|| {
            let result = poll(
                future
                    .as_mut()
                    .expect("a completed context task is not polled again"),
            );
            if result.is_ready() {
                drop(future.take());
            }
            result
        });
        self.envelope.set_request_context(next_request);
        result
    }
}

impl<F> Drop for ContextTask<F> {
    fn drop(&mut self) {
        let Some(future) = self.future.take() else {
            return;
        };
        let envelope = self.envelope.clone();
        let _installed = envelope.install();
        drop(future);
    }
}

pub type ContextWorkerCall<T> = WorkerCall<T>;

/// Context-capturing variants of worker submission APIs.
pub trait ContextWorkerExt {
    fn submit_contextual<F>(&self, callback: F) -> Result<(), SubmitError>
    where
        F: FnOnce(&WorkerContext) + Send + 'static;

    fn spawn_local_contextual<F, Fut>(&self, factory: F) -> Result<(), SubmitError>
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static;

    fn call_contextual<F, T>(&self, callback: F) -> Result<ContextWorkerCall<T>, SubmitError>
    where
        F: FnOnce(&WorkerContext) -> T + Send + 'static,
        T: Send + 'static;

    fn call_local_contextual<F, Fut, T>(
        &self,
        factory: F,
    ) -> Result<ContextWorkerCall<T>, SubmitError>
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static;
}

impl ContextWorkerExt for WorkerHandle {
    fn submit_contextual<F>(&self, callback: F) -> Result<(), SubmitError>
    where
        F: FnOnce(&WorkerContext) + Send + 'static,
    {
        self.submit(callback)
    }

    fn spawn_local_contextual<F, Fut>(&self, factory: F) -> Result<(), SubmitError>
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        self.spawn_local(factory)
    }

    fn call_contextual<F, T>(&self, callback: F) -> Result<ContextWorkerCall<T>, SubmitError>
    where
        F: FnOnce(&WorkerContext) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.call(callback)
    }

    fn call_local_contextual<F, Fut, T>(
        &self,
        factory: F,
    ) -> Result<ContextWorkerCall<T>, SubmitError>
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.call_local(factory)
    }
}
