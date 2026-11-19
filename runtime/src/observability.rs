/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use crate::context::ContextEnvelope;
use crate::context::ContextFuture;
use crate::context::ContextTask;
use crate::context::SpanLink;

static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
type SpanCompletionHook = for<'a> fn(SpanLink, TerminalOutcome, Option<&'a SpanObserver>);

#[cfg(test)]
thread_local! {
    static TASK_CATCH_UNWINDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SPAN_COMPLETION_HOOK: std::cell::Cell<Option<SpanCompletionHook>> =
        const { std::cell::Cell::new(None) };
}

/// Counts one `catch_unwind` that a task wrapper makes around a poll.
#[cfg(test)]
pub(crate) fn count_task_catch_unwind() {
    TASK_CATCH_UNWINDS.set(TASK_CATCH_UNWINDS.get() + 1);
}

/// How many task-wrapper `catch_unwind` calls this thread has made.
#[cfg(test)]
pub(crate) fn task_catch_unwinds() -> usize {
    TASK_CATCH_UNWINDS.get()
}

/// Replaces this thread's hook that each span completion calls, after its
/// observer records the outcome, and returns the previous hook.
#[cfg(test)]
pub(crate) fn set_span_completion_hook(
    hook: Option<SpanCompletionHook>,
) -> Option<SpanCompletionHook> {
    SPAN_COMPLETION_HOOK.replace(hook)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OperationKind {
    Command,
    Queue,
    Stream,
    Cancellation,
    Overload,
    Shutdown,
}

impl OperationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Queue => "queue",
            Self::Stream => "stream",
            Self::Cancellation => "cancellation",
            Self::Overload => "overload",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TerminalOutcome {
    Success,
    Failure,
    Cancelled,
    TimedOut,
    Overloaded,
    Shutdown,
}

impl TerminalOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Overloaded => "overloaded",
            Self::Shutdown => "shutdown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationSpanId(u64);

#[cfg(test)]
impl OperationSpanId {
    /// The link that this span's children see installed.
    pub(crate) fn link(self) -> SpanLink {
        SpanLink(self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationSpanSnapshot {
    pub id: OperationSpanId,
    pub parent: Option<OperationSpanId>,
    pub kind: OperationKind,
    pub outcome: Option<TerminalOutcome>,
    pub close_count: u8,
}

#[derive(Debug)]
struct SpanObserverState {
    capacity: usize,
    spans: VecDeque<OperationSpanSnapshot>,
    dropped_records: u64,
}

#[derive(Clone, Debug)]
pub struct SpanObserver {
    state: Arc<Mutex<SpanObserverState>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SpanObserverCreateError {
    #[error("span observer capacity must be non-zero")]
    ZeroCapacity,
}

impl SpanObserver {
    pub fn new(capacity: usize) -> Result<Self, SpanObserverCreateError> {
        if capacity == 0 {
            return Err(SpanObserverCreateError::ZeroCapacity);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(SpanObserverState {
                capacity,
                spans: VecDeque::with_capacity(capacity),
                dropped_records: 0,
            })),
        })
    }

    pub fn start(&self, kind: OperationKind, parent: &ContextEnvelope) -> OperationSpan {
        let parent = parent.clone().with_span_observer(self.clone());
        OperationSpan::start(kind, &parent)
    }

    pub fn snapshots(&self) -> Vec<OperationSpanSnapshot> {
        self.state().spans.iter().cloned().collect()
    }

    pub fn drain_completed(&self) -> Vec<OperationSpanSnapshot> {
        let mut state = self.state();
        let mut completed = Vec::new();
        let mut retained = VecDeque::with_capacity(state.capacity);
        while let Some(span) = state.spans.pop_front() {
            if span.outcome.is_some() {
                completed.push(span);
            } else {
                retained.push_back(span);
            }
        }
        state.spans = retained;
        completed
    }

    pub fn active_count(&self) -> usize {
        self.state()
            .spans
            .iter()
            .filter(|span| span.outcome.is_none())
            .count()
    }

    pub fn dropped_records(&self) -> u64 {
        self.state().dropped_records
    }

    /// Whether both handles share one observer.
    #[cfg(test)]
    pub(crate) fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, SpanObserverState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn register(&self, snapshot: OperationSpanSnapshot) -> bool {
        let mut state = self.state();
        if state.spans.len() == state.capacity {
            if let Some(index) = state.spans.iter().position(|span| span.outcome.is_some()) {
                state.spans.remove(index);
            } else {
                state.dropped_records = state.dropped_records.saturating_add(1);
                return false;
            }
        }
        state.spans.push_back(snapshot);
        true
    }

    fn close(&self, id: OperationSpanId, outcome: TerminalOutcome) {
        let mut state = self.state();
        let span = state
            .spans
            .iter_mut()
            .find(|span| span.id == id)
            .expect("a recorded operation span remains until completion");
        span.close_count = span.close_count.saturating_add(1);
        span.outcome = Some(outcome);
    }
}

impl Default for SpanObserver {
    fn default() -> Self {
        Self::new(256).expect("the default span observer capacity is non-zero")
    }
}

pub struct OperationSpan {
    id: OperationSpanId,
    context: Option<ContextEnvelope>,
    observer: Option<SpanObserver>,
    recorded: bool,
    completed: bool,
}

impl OperationSpan {
    pub fn start(kind: OperationKind, parent: &ContextEnvelope) -> Self {
        let id = OperationSpanId(NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed));
        let parent_id = parent.linked_span().map(|span| OperationSpanId(span.0));
        #[cfg(feature = "observability")]
        let span = tracing::span!(
            parent: parent.span(),
            tracing::Level::INFO,
            "roxygen.runtime.operation",
            operation = kind.as_str(),
            outcome = tracing::field::Empty,
        );
        #[cfg(feature = "observability")]
        let context = parent.child_with_span(span, SpanLink(id.0));
        #[cfg(not(feature = "observability"))]
        let context = parent.child_with_link(SpanLink(id.0));
        let observer = parent.span_observer().cloned();
        let recorded = observer.as_ref().is_some_and(|observer| {
            observer.register(OperationSpanSnapshot {
                id,
                parent: parent_id,
                kind,
                outcome: None,
                close_count: 0,
            })
        });
        Self {
            id,
            context: Some(context),
            observer,
            recorded,
            completed: false,
        }
    }

    pub fn id(&self) -> OperationSpanId {
        self.id
    }

    pub fn in_scope<T>(&self, callback: impl FnOnce() -> T) -> T {
        self.context
            .as_ref()
            .expect("a completed operation span cannot be re-entered")
            .run(callback)
    }

    pub(crate) fn context(&self) -> &ContextEnvelope {
        self.context
            .as_ref()
            .expect("an incomplete operation span retains its context")
    }

    pub fn complete(&mut self, outcome: TerminalOutcome) -> bool {
        if self.completed {
            return false;
        }
        self.completed = true;
        let context = self
            .context
            .take()
            .expect("an incomplete operation span retains its context");
        #[cfg(feature = "observability")]
        context.span().record("outcome", outcome.as_str());
        if self.recorded {
            self.observer
                .as_ref()
                .expect("a recorded span retains its observer")
                .close(self.id, outcome);
        }
        #[cfg(test)]
        if let Some(hook) = SPAN_COMPLETION_HOOK.get() {
            hook(self.id.link(), outcome, self.observer.as_ref());
        }
        drop(context);
        true
    }

    pub fn instrument<F, C>(self, future: F, classify: C) -> ObservedFuture<F, C>
    where
        F: Future,
        C: FnOnce(&F::Output) -> TerminalOutcome,
    {
        let future = self.context().wrap(future);
        ObservedFuture {
            future: Some(Box::pin(future)),
            operation: Some(self),
            classify: Some(Box::new(classify)),
        }
    }

    pub fn instrument_timeout<F, C>(
        self,
        future: F,
        timeout: Duration,
        classify: C,
    ) -> ObservedTimeout<F, C>
    where
        F: Future,
        C: FnOnce(&F::Output) -> TerminalOutcome,
    {
        let future = self.context().wrap(future);
        ObservedTimeout {
            future: Some(Box::pin(future)),
            timer: Box::pin(tokio::time::sleep(timeout)),
            operation: Some(self),
            classify: Some(Box::new(classify)),
        }
    }
}

impl Drop for OperationSpan {
    fn drop(&mut self) {
        let outcome = if std::thread::panicking() {
            TerminalOutcome::Failure
        } else {
            TerminalOutcome::Cancelled
        };
        self.complete(outcome);
    }
}

#[must_use = "futures do nothing unless polled or awaited"]
pub struct ObservedFuture<F, C>
where
    F: Future,
    C: FnOnce(&F::Output) -> TerminalOutcome,
{
    future: Option<Pin<Box<ContextFuture<F>>>>,
    operation: Option<OperationSpan>,
    classify: Option<Box<C>>,
}

impl<F, C> Future for ObservedFuture<F, C>
where
    F: Future,
    C: FnOnce(&F::Output) -> TerminalOutcome,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        #[cfg(test)]
        count_task_catch_unwind();
        let polled = catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("a completed observed future has no inner future")
                .as_mut()
                .poll(task)
        }));
        match polled {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(this.finish_ready(output)),
            Err(payload) => this.finish_panicked(payload),
        }
    }
}

impl<F, C> ObservedFuture<F, C>
where
    F: Future,
    C: FnOnce(&F::Output) -> TerminalOutcome,
{
    #[cold]
    #[inline(never)]
    fn finish_ready(&mut self, output: F::Output) -> F::Output {
        let context = self
            .future
            .as_ref()
            .expect("a terminal observed future retains its context")
            .envelope()
            .clone();
        let classified = catch_unwind(AssertUnwindSafe(|| {
            context.run(|| {
                drop(self.future.take());
                let classify = self
                    .classify
                    .take()
                    .expect("a terminal outcome is classified once");
                classify(&output)
            })
        }));
        match classified {
            Ok(outcome) => context.run(|| {
                finish_operation(&mut self.operation, outcome);
            }),
            Err(payload) => {
                context.run(|| {
                    finish_operation(&mut self.operation, TerminalOutcome::Failure);
                });
                resume_unwind(payload);
            }
        }
        output
    }

    #[cold]
    #[inline(never)]
    fn finish_panicked(&mut self, payload: Box<dyn Any + Send>) -> ! {
        finish_unwind(&mut self.future, &mut self.operation);
        resume_unwind(payload)
    }
}

impl<F, C> Drop for ObservedFuture<F, C>
where
    F: Future,
    C: FnOnce(&F::Output) -> TerminalOutcome,
{
    fn drop(&mut self) {
        cancel_operation(&mut self.future, &mut self.operation);
    }
}

/// An `ObservedFuture` that holds its `Unpin` future inline and classifies
/// every completion as `Success`, for the runtime's task wrappers. A wrapper
/// that already catches panics around its poll uses `poll_unguarded` and
/// runs `finish_ready` or `finish_unwind` itself.
pub(crate) struct ObservedTask<F> {
    future: Option<ContextTask<F>>,
    operation: Option<OperationSpan>,
}

impl<F> ObservedTask<F> {
    pub(crate) fn new(operation: OperationSpan, future: F) -> Self {
        let future = ContextTask::new(future, operation.context().clone());
        Self {
            future: Some(future),
            operation: Some(operation),
        }
    }

    #[cfg(not(feature = "observability"))]
    pub(crate) fn envelope(&self) -> &ContextEnvelope {
        self.future
            .as_ref()
            .expect("an incomplete observed task retains its context")
            .envelope()
    }

    /// The inner future, while it has not completed.
    #[cfg(not(feature = "observability"))]
    pub(crate) fn future_mut(&mut self) -> Option<&mut F> {
        self.future.as_mut().and_then(ContextTask::future_mut)
    }

    /// Polls under the task's context without catching a panic.
    #[inline(always)]
    pub(crate) fn poll_unguarded_with<T>(
        &mut self,
        poll: impl FnOnce(&mut F) -> Poll<T>,
    ) -> Poll<T> {
        self.future
            .as_mut()
            .expect("a completed observed task has no inner future")
            .poll_with(poll)
    }

    /// Drops the completed future and closes the span as `Success`, as
    /// `ObservedFuture` does after a ready poll.
    #[cold]
    #[inline(never)]
    pub(crate) fn finish_ready(&mut self) {
        let context = self
            .future
            .as_ref()
            .expect("a terminal observed task retains its context")
            .envelope()
            .clone();
        // With `observability`, dropping the task's span handle calls the
        // subscriber, which can panic. Otherwise this drop cannot panic.
        #[cfg(feature = "observability")]
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            context.run(|| drop(self.future.take()));
        })) {
            context.run(|| {
                finish_operation(&mut self.operation, TerminalOutcome::Failure);
            });
            resume_unwind(payload);
        }
        #[cfg(not(feature = "observability"))]
        drop(self.future.take());
        context.run(|| {
            finish_operation(&mut self.operation, TerminalOutcome::Success);
        });
    }

    /// Drops the future and closes the span as `Failure` after its poll
    /// panicked.
    #[cold]
    #[inline(never)]
    pub(crate) fn finish_unwind(&mut self) {
        finish_unwind(&mut self.future, &mut self.operation);
    }
}

impl<F> ObservedTask<F>
where
    F: Future + Unpin,
{
    #[inline(always)]
    pub(crate) fn poll_unguarded(&mut self, task: &mut Context<'_>) -> Poll<F::Output> {
        self.poll_unguarded_with(|future| Pin::new(future).poll(task))
    }

    /// Polls as `ObservedFuture` does, catching a panic from the poll.
    pub(crate) fn poll(&mut self, task: &mut Context<'_>) -> Poll<F::Output> {
        #[cfg(test)]
        count_task_catch_unwind();
        let polled = catch_unwind(AssertUnwindSafe(|| self.poll_unguarded(task)));
        match polled {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => {
                self.finish_ready();
                Poll::Ready(output)
            }
            Err(payload) => {
                self.finish_unwind();
                resume_unwind(payload)
            }
        }
    }
}

impl<F> Drop for ObservedTask<F> {
    fn drop(&mut self) {
        cancel_operation(&mut self.future, &mut self.operation);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("observed operation timed out")]
pub struct OperationTimeout;

#[must_use = "futures do nothing unless polled or awaited"]
pub struct ObservedTimeout<F, C>
where
    F: Future,
    C: FnOnce(&F::Output) -> TerminalOutcome,
{
    future: Option<Pin<Box<ContextFuture<F>>>>,
    timer: Pin<Box<tokio::time::Sleep>>,
    operation: Option<OperationSpan>,
    classify: Option<Box<C>>,
}

impl<F, C> Future for ObservedTimeout<F, C>
where
    F: Future,
    C: FnOnce(&F::Output) -> TerminalOutcome,
{
    type Output = Result<F::Output, OperationTimeout>;

    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polled = catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("a completed observed timeout has no inner future")
                .as_mut()
                .poll(task)
        }));
        match polled {
            Ok(Poll::Ready(output)) => {
                let context = this
                    .future
                    .as_ref()
                    .expect("a terminal observed timeout retains its context")
                    .envelope()
                    .clone();
                let classified = catch_unwind(AssertUnwindSafe(|| {
                    context.run(|| {
                        drop(this.future.take());
                        let classify = this
                            .classify
                            .take()
                            .expect("a terminal outcome is classified once");
                        classify(&output)
                    })
                }));
                match classified {
                    Ok(outcome) => context.run(|| {
                        finish_operation(&mut this.operation, outcome);
                    }),
                    Err(payload) => {
                        context.run(|| {
                            finish_operation(&mut this.operation, TerminalOutcome::Failure);
                        });
                        resume_unwind(payload);
                    }
                }
                Poll::Ready(Ok(output))
            }
            Ok(Poll::Pending) => {
                if this.timer.as_mut().poll(task).is_pending() {
                    return Poll::Pending;
                }
                let context = this
                    .future
                    .as_ref()
                    .expect("a timed out operation retains its context")
                    .envelope()
                    .clone();
                context.run(|| {
                    drop(this.future.take());
                    finish_operation(&mut this.operation, TerminalOutcome::TimedOut);
                });
                Poll::Ready(Err(OperationTimeout))
            }
            Err(payload) => {
                finish_unwind(&mut this.future, &mut this.operation);
                resume_unwind(payload)
            }
        }
    }
}

impl<F, C> Drop for ObservedTimeout<F, C>
where
    F: Future,
    C: FnOnce(&F::Output) -> TerminalOutcome,
{
    fn drop(&mut self) {
        cancel_operation(&mut self.future, &mut self.operation);
    }
}

fn finish_operation(operation: &mut Option<OperationSpan>, outcome: TerminalOutcome) {
    let mut operation = operation
        .take()
        .expect("an operation span is completed exactly once");
    operation.complete(outcome);
    drop(operation);
}

/// A future that is polled and dropped under a context envelope.
trait Enveloped {
    fn envelope(&self) -> &ContextEnvelope;
}

impl<F> Enveloped for Pin<Box<ContextFuture<F>>> {
    fn envelope(&self) -> &ContextEnvelope {
        self.as_ref().get_ref().envelope()
    }
}

impl<F> Enveloped for ContextTask<F> {
    fn envelope(&self) -> &ContextEnvelope {
        ContextTask::envelope(self)
    }
}

fn finish_unwind<T>(future: &mut Option<T>, operation: &mut Option<OperationSpan>)
where
    T: Enveloped,
{
    let context = future
        .as_ref()
        .expect("a panicking operation retains its context")
        .envelope()
        .clone();
    context.run(|| {
        drop(future.take());
        finish_operation(operation, TerminalOutcome::Failure);
    });
}

fn cancel_operation<T>(future: &mut Option<T>, operation: &mut Option<OperationSpan>)
where
    T: Enveloped,
{
    let Some(context) = future.as_ref().map(|future| future.envelope().clone()) else {
        return;
    };
    context.run(|| {
        drop(future.take());
        finish_operation(operation, TerminalOutcome::Cancelled);
    });
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MetricUnit {
    Count,
    Bytes,
    Microseconds,
}

impl MetricUnit {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Bytes => "bytes",
            Self::Microseconds => "microseconds",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricLabelSpec {
    key: &'static str,
    values: &'static [&'static str],
}

impl MetricLabelSpec {
    pub const fn new(key: &'static str, values: &'static [&'static str]) -> Self {
        Self { key, values }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MetricLabel {
    pub key: &'static str,
    pub value: &'static str,
}

impl MetricLabel {
    pub const fn new(key: &'static str, value: &'static str) -> Self {
        Self { key, value }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricDescriptor {
    name: &'static str,
    unit: MetricUnit,
    labels: &'static [MetricLabelSpec],
}

impl MetricDescriptor {
    pub fn new(
        name: &'static str,
        unit: MetricUnit,
        labels: &'static [MetricLabelSpec],
    ) -> Result<Self, MetricError> {
        if name.is_empty() {
            return Err(MetricError::EmptyName);
        }
        for (index, label) in labels.iter().enumerate() {
            if label.key.is_empty() || label.values.is_empty() {
                return Err(MetricError::UnboundedLabel(label.key));
            }
            if is_identifier_label(label.key) {
                return Err(MetricError::IdentifierLabel(label.key));
            }
            if labels[..index].iter().any(|other| other.key == label.key) {
                return Err(MetricError::DuplicateLabel(label.key));
            }
            for (value_index, value) in label.values.iter().enumerate() {
                if value.is_empty()
                    || label.values[..value_index]
                        .iter()
                        .any(|other| other == value)
                {
                    return Err(MetricError::UnboundedLabel(label.key));
                }
            }
        }
        Ok(Self { name, unit, labels })
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn unit(&self) -> MetricUnit {
        self.unit
    }
}

#[derive(Clone, Debug, Eq, thiserror::Error, PartialEq)]
pub enum MetricError {
    #[error("a metric name must not be empty")]
    EmptyName,
    #[error("metric label {0:?} must declare a non-empty finite value set")]
    UnboundedLabel(&'static str),
    #[error("metric label {0:?} must not contain a request, session, or stream identifier")]
    IdentifierLabel(&'static str),
    #[error("metric label {0:?} is declared more than once")]
    DuplicateLabel(&'static str),
    #[error("metric labels do not exactly match the descriptor")]
    LabelSetMismatch,
    #[error("value {value:?} is outside the bounded set for metric label {key:?}")]
    LabelValueOutOfRange {
        key: &'static str,
        value: &'static str,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MetricKey {
    name: &'static str,
    unit: MetricUnit,
    labels: Vec<MetricLabel>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricSample {
    pub name: &'static str,
    pub unit: MetricUnit,
    pub labels: Vec<MetricLabel>,
    pub value: u64,
}

#[derive(Clone, Default)]
pub struct MetricRegistry {
    values: Arc<Mutex<BTreeMap<MetricKey, u64>>>,
}

impl MetricRegistry {
    pub fn record(
        &self,
        descriptor: &MetricDescriptor,
        labels: &[MetricLabel],
        value: u64,
    ) -> Result<(), MetricError> {
        if labels.len() != descriptor.labels.len() {
            return Err(MetricError::LabelSetMismatch);
        }
        let mut ordered = Vec::with_capacity(labels.len());
        for spec in descriptor.labels {
            let mut matching = labels.iter().filter(|label| label.key == spec.key);
            let Some(label) = matching.next() else {
                return Err(MetricError::LabelSetMismatch);
            };
            if matching.next().is_some() {
                return Err(MetricError::LabelSetMismatch);
            }
            if !spec.values.contains(&label.value) {
                return Err(MetricError::LabelValueOutOfRange {
                    key: label.key,
                    value: label.value,
                });
            }
            ordered.push(*label);
        }

        let key = MetricKey {
            name: descriptor.name,
            unit: descriptor.unit,
            labels: ordered,
        };
        let mut values = self
            .values
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let total = values.entry(key).or_default();
        *total = total.saturating_add(value);
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<MetricSample> {
        self.values
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(key, value)| MetricSample {
                name: key.name,
                unit: key.unit,
                labels: key.labels.clone(),
                value: *value,
            })
            .collect()
    }
}

fn is_identifier_label(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    normalized.ends_with("requestid")
        || normalized.ends_with("sessionid")
        || normalized.ends_with("streamid")
}
