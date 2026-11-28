/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for task wrapping.

mod task_wrapping {
    use std::any::Any;
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::fmt::Write as _;
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    #[cfg(feature = "observability")]
    use std::process::Command;
    #[cfg(feature = "observability")]
    use std::process::Output;
    #[cfg(feature = "observability")]
    use std::process::Stdio;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::PoisonError;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Duration;
    use std::time::Instant;

    use futures::task::noop_waker;

    use crate::cancellation::CancellationToken;
    use crate::context::ContextEnvelope;
    use crate::context::RuntimeContext;
    use crate::context::SpanLink;
    use crate::driver::WorkerDriver;
    use crate::host::Host;
    use crate::host::LocalFuture;
    use crate::host::LocalHost;
    use crate::observability::OperationKind;
    #[cfg(feature = "observability")]
    use crate::observability::OperationSpan;
    use crate::observability::OperationSpanSnapshot;
    use crate::observability::SpanObserver;
    use crate::observability::TerminalOutcome;
    use crate::task_group::TaskGroup;
    use crate::task_group::TaskId;
    use crate::tokio_host::HostedWorker;
    use crate::tokio_host::HostedWorkerOptions;
    use crate::worker::CleanupRegistrationError;
    use crate::worker::WorkerCall;
    use crate::worker::WorkerContext;
    use crate::worker::WorkerHandle;

    const TEST_TIMEOUT: Duration = Duration::from_secs(30);

    /// What the thread had installed when an event happened.
    #[derive(Clone)]
    struct Seen {
        request: Option<RuntimeContext>,
        link: Option<u64>,
        observer: Option<SpanObserver>,
        unwinding: bool,
    }

    fn seen() -> Seen {
        Seen {
            request: RuntimeContext::current(),
            link: crate::context::current_span_link().map(|link| link.0),
            observer: crate::context::current_span_observer(),
            unwinding: std::thread::panicking(),
        }
    }

    enum Event {
        Poll(&'static str, Seen),
        Drop(&'static str, Seen),
        Factory(&'static str, Seen),
        FactoryDrop(&'static str, Seen),
        Close {
            span: u64,
            outcome: TerminalOutcome,
            observer: Option<SpanObserver>,
            seen: Seen,
        },
        Release(usize, Seen),
        Woken(&'static str, Seen),
        PayloadDrop(&'static str, Seen),
        Note(String),
        Snapshot(&'static str, Vec<OperationSpanSnapshot>),
        #[cfg(feature = "observability")]
        Trace(Trace, Seen),
    }

    type Sink = Arc<Mutex<Vec<Event>>>;

    thread_local! {
        static SINK: RefCell<Option<Sink>> = const { RefCell::new(None) };
        static MUTED: Cell<bool> = const { Cell::new(false) };
        /// How many more span completions run before the completion hook
        /// panics.
        static CLOSE_PANIC: Cell<Option<usize>> = const { Cell::new(None) };
        /// Whether the panic hook reports this thread's panics without a
        /// backtrace.
        #[cfg(feature = "observability")]
        static QUIET_PANICS: Cell<bool> = const { Cell::new(false) };
        /// The cast of the transcript this thread is recording, which a sweep
        /// child renders when it exits at an abort.
        #[cfg(feature = "observability")]
        static RECORDING_CAST: RefCell<Option<Cast>> = const { RefCell::new(None) };
    }

    fn record(event: Event) {
        if MUTED.get() {
            return;
        }
        let _ = SINK.try_with(|sink| {
            if let Some(sink) = &*sink.borrow() {
                sink.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(event);
            }
        });
    }

    fn note(text: impl Into<String>) {
        record(Event::Note(text.into()));
    }

    type CompletionHook = for<'a> fn(SpanLink, TerminalOutcome, Option<&'a SpanObserver>);

    fn on_span_completion(
        span: SpanLink,
        outcome: TerminalOutcome,
        observer: Option<&SpanObserver>,
    ) {
        record(Event::Close {
            span: span.0,
            outcome,
            observer: observer.cloned(),
            seen: seen(),
        });
        match CLOSE_PANIC.get() {
            Some(0) => {
                CLOSE_PANIC.set(None);
                raise("close");
            }
            Some(left) => CLOSE_PANIC.set(Some(left - 1)),
            None => {}
        }
    }

    fn on_work_release(left: usize, unwinding: bool) {
        let mut seen = seen();
        seen.unwinding = unwinding;
        record(Event::Release(left, seen));
    }

    /// Installs this thread's sink, the runtime's test hooks, and, under
    /// `observability`, a recording tracing subscriber.
    struct ThreadRecording {
        sink: Option<Sink>,
        completion: Option<CompletionHook>,
        release: Option<fn(usize, bool)>,
        #[cfg(feature = "observability")]
        _subscriber: tracing::subscriber::DefaultGuard,
    }

    impl ThreadRecording {
        fn start(sink: &Sink, first_trace_id: u64, panic_at: Option<usize>) -> Self {
            #[cfg(not(feature = "observability"))]
            let _ = (first_trace_id, panic_at);
            #[cfg(feature = "observability")]
            register_span_callsite();
            Self {
                sink: SINK.with_borrow_mut(|current| current.replace(Arc::clone(sink))),
                completion: crate::observability::set_span_completion_hook(Some(
                    on_span_completion as CompletionHook,
                )),
                release: crate::worker::set_work_release_hook(Some(on_work_release)),
                #[cfg(feature = "observability")]
                _subscriber: tracing::subscriber::set_default(Recorder::new(
                    first_trace_id,
                    panic_at,
                )),
            }
        }
    }

    impl Drop for ThreadRecording {
        fn drop(&mut self) {
            CLOSE_PANIC.set(None);
            crate::worker::set_work_release_hook(self.release.take());
            crate::observability::set_span_completion_hook(self.completion.take());
            SINK.with_borrow_mut(|current| *current = self.sink.take());
        }
    }

    /// Hits the runtime's span callsite once under a recorder. tracing-core
    /// caches a callsite's interest at its first hit and, while exactly one
    /// dispatcher is registered, takes it from the hitting thread's default. A
    /// first hit from a test thread without a recorder, while another thread's
    /// recorder is the only dispatcher, would disable the callsite for every
    /// thread until the next dispatcher registers.
    #[cfg(feature = "observability")]
    fn register_span_callsite() {
        static REGISTERED: std::sync::Once = std::sync::Once::new();
        REGISTERED.call_once(|| {
            tracing::subscriber::with_default(Recorder::new(1, None), || {
                drop(OperationSpan::start(
                    OperationKind::Command,
                    &ContextEnvelope::capture(),
                ));
            });
        });
    }

    #[cfg(feature = "observability")]
    #[derive(Clone, Debug)]
    enum Trace {
        New {
            id: u64,
            parent: Option<u64>,
            fields: String,
            place: usize,
        },
        Clone {
            id: u64,
            place: usize,
        },
        Enter {
            id: u64,
            place: usize,
        },
        Exit {
            id: u64,
            place: usize,
        },
        Record {
            id: u64,
            fields: String,
            place: usize,
        },
        TryClose {
            id: u64,
            closed: bool,
            place: usize,
        },
    }

    #[cfg(feature = "observability")]
    impl Trace {
        fn place(&self) -> usize {
            match self {
                Self::New { place, .. }
                | Self::Clone { place, .. }
                | Self::Enter { place, .. }
                | Self::Exit { place, .. }
                | Self::Record { place, .. }
                | Self::TryClose { place, .. } => *place,
            }
        }

        fn label(&self) -> String {
            match self {
                Self::New {
                    id, parent, fields, ..
                } => format!(
                    "new t{id} parent={}{fields}",
                    parent.map_or_else(|| "-".to_owned(), |parent| format!("t{parent}"))
                ),
                Self::Clone { id, .. } => format!("clone t{id}"),
                Self::Enter { id, .. } => format!("enter t{id}"),
                Self::Exit { id, .. } => format!("exit t{id}"),
                Self::Record { id, fields, .. } => format!("record t{id}{fields}"),
                Self::TryClose { id, closed, .. } => {
                    format!("try_close t{id}{}", if *closed { " closes" } else { "" })
                }
            }
        }
    }

    /// A subscriber that records each callback with the context installed in
    /// it, and panics once at the callback numbered `panic_at`.
    #[cfg(feature = "observability")]
    struct Recorder {
        next_id: std::sync::atomic::AtomicU64,
        places: std::sync::atomic::AtomicUsize,
        panic_at: Option<usize>,
        spans: Mutex<RecorderSpans>,
    }

    #[cfg(feature = "observability")]
    #[derive(Default)]
    struct RecorderSpans {
        references: std::collections::BTreeMap<u64, usize>,
        metadata: std::collections::BTreeMap<u64, &'static tracing::Metadata<'static>>,
        stack: Vec<u64>,
    }

    #[cfg(feature = "observability")]
    impl Recorder {
        fn new(first_id: u64, panic_at: Option<usize>) -> Self {
            Self {
                next_id: std::sync::atomic::AtomicU64::new(first_id),
                places: std::sync::atomic::AtomicUsize::new(0),
                panic_at,
                spans: Mutex::default(),
            }
        }

        fn spans(&self) -> std::sync::MutexGuard<'_, RecorderSpans> {
            self.spans.lock().unwrap_or_else(PoisonError::into_inner)
        }

        fn place(&self) -> usize {
            self.places
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }

        fn trace(&self, trace: Trace) {
            let place = trace.place();
            record(Event::Trace(trace, seen()));
            if self.panic_at == Some(place) {
                eprintln!("{PANIC_MARKER}{place}");
                std::panic::panic_any(Payload("subscriber"));
            }
        }
    }

    #[cfg(feature = "observability")]
    #[derive(Default)]
    struct Fields(String);

    #[cfg(feature = "observability")]
    impl tracing::field::Visit for Fields {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            let _ = write!(self.0, " {}={value}", field.name());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    #[cfg(feature = "observability")]
    impl tracing::Subscriber for Recorder {
        fn register_callsite(
            &self,
            _metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::sometimes()
        }

        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let id = self
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let parent = attributes
                .parent()
                .map(tracing::span::Id::into_u64)
                .or_else(|| {
                    attributes
                        .is_contextual()
                        .then(|| self.spans().stack.last().copied())
                        .flatten()
                });
            let mut fields = Fields::default();
            attributes.record(&mut fields);
            self.trace(Trace::New {
                id,
                parent,
                fields: fields.0,
                place: self.place(),
            });
            let mut spans = self.spans();
            spans.references.insert(id, 1);
            spans.metadata.insert(id, attributes.metadata());
            tracing::span::Id::from_u64(id)
        }

        fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
            let mut fields = Fields::default();
            values.record(&mut fields);
            self.trace(Trace::Record {
                id: span.into_u64(),
                fields: fields.0,
                place: self.place(),
            });
        }

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, _event: &tracing::Event<'_>) {}

        fn enter(&self, span: &tracing::span::Id) {
            self.trace(Trace::Enter {
                id: span.into_u64(),
                place: self.place(),
            });
            self.spans().stack.push(span.into_u64());
        }

        fn exit(&self, span: &tracing::span::Id) {
            {
                let mut spans = self.spans();
                if let Some(index) = spans.stack.iter().rposition(|id| *id == span.into_u64()) {
                    spans.stack.remove(index);
                }
            }
            self.trace(Trace::Exit {
                id: span.into_u64(),
                place: self.place(),
            });
        }

        fn clone_span(&self, span: &tracing::span::Id) -> tracing::span::Id {
            self.trace(Trace::Clone {
                id: span.into_u64(),
                place: self.place(),
            });
            *self.spans().references.entry(span.into_u64()).or_default() += 1;
            span.clone()
        }

        fn try_close(&self, span: tracing::span::Id) -> bool {
            let id = span.into_u64();
            let closed = {
                let mut spans = self.spans();
                let remaining = spans.references.get_mut(&id).map(|count| {
                    *count -= 1;
                    *count
                });
                if remaining == Some(0) {
                    spans.references.remove(&id);
                }
                remaining == Some(0)
            };
            self.trace(Trace::TryClose {
                id,
                closed,
                place: self.place(),
            });
            closed
        }

        fn current_span(&self) -> tracing_core::span::Current {
            let spans = self.spans();
            spans
                .stack
                .last()
                .and_then(|id| {
                    spans.metadata.get(id).map(|metadata| {
                        tracing_core::span::Current::new(tracing::span::Id::from_u64(*id), metadata)
                    })
                })
                .unwrap_or_else(tracing_core::span::Current::none)
        }
    }

    /// A panic payload that records its own drop.
    struct Payload(&'static str);

    impl Drop for Payload {
        fn drop(&mut self) {
            record(Event::PayloadDrop(self.0, seen()));
        }
    }

    fn raise(name: &'static str) -> ! {
        std::panic::panic_any(Payload(name))
    }

    fn describe(payload: &(dyn Any + Send)) -> String {
        if let Some(payload) = payload.downcast_ref::<Payload>() {
            format!("payload {}", payload.0)
        } else if let Some(error) = payload.downcast_ref::<crate::task_group::TaskGroupError>() {
            format!("{error:?}")
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_owned()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown payload".to_owned()
        }
    }

    /// Runs `run`, and records a panic from it as a note.
    fn act<T>(label: &str, run: impl FnOnce() -> T) -> Option<T> {
        match catch_unwind(AssertUnwindSafe(run)) {
            Ok(value) => Some(value),
            Err(payload) => {
                note(format!("{label} panicked: {}", describe(&*payload)));
                drop(payload);
                None
            }
        }
    }

    /// A factory capture that records its drop.
    struct FactoryCapture(&'static str);

    impl Drop for FactoryCapture {
        fn drop(&mut self) {
            record(Event::FactoryDrop(self.0, seen()));
        }
    }

    type Step = Box<dyn FnMut(usize, &mut Context<'_>) -> Poll<()>>;

    /// A user future that records each poll and its drop.
    struct Probe {
        name: &'static str,
        polls: usize,
        step: Step,
        on_drop: Option<Box<dyn FnOnce()>>,
    }

    impl Probe {
        fn new(
            name: &'static str,
            step: impl FnMut(usize, &mut Context<'_>) -> Poll<()> + 'static,
        ) -> Self {
            Self {
                name,
                polls: 0,
                step: Box::new(step),
                on_drop: None,
            }
        }

        fn on_drop(mut self, on_drop: impl FnOnce() + 'static) -> Self {
            self.on_drop = Some(Box::new(on_drop));
            self
        }
    }

    impl Future for Probe {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            record(Event::Poll(self.name, seen()));
            let index = self.polls;
            self.polls += 1;
            (self.step)(index, context)
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            record(Event::Drop(self.name, seen()));
            if let Some(on_drop) = self.on_drop.take() {
                on_drop();
            }
        }
    }

    /// Cross-thread progress of a probe on a `HostedWorker`.
    #[derive(Clone, Default)]
    struct Signal(Arc<(Mutex<SignalState>, Condvar)>);

    #[derive(Default)]
    struct SignalState {
        open: bool,
        waker: Option<Waker>,
        polls: usize,
        dropped: bool,
        woken: bool,
    }

    impl Signal {
        fn update<T>(&self, change: impl FnOnce(&mut SignalState) -> T) -> T {
            let (state, changed) = &*self.0;
            let result = change(&mut state.lock().unwrap_or_else(PoisonError::into_inner));
            changed.notify_all();
            result
        }

        fn wait(&self, done: impl Fn(&SignalState) -> bool) {
            let (state, changed) = &*self.0;
            let (_state, timeout) = changed
                .wait_timeout_while(
                    state.lock().unwrap_or_else(PoisonError::into_inner),
                    TEST_TIMEOUT,
                    |state| !done(state),
                )
                .unwrap_or_else(PoisonError::into_inner);
            assert!(!timeout.timed_out(), "the hosted worker progresses");
        }

        /// Counts a poll, and reports whether the gate is open.
        fn polled(&self, context: &Context<'_>) -> bool {
            self.update(|state| {
                state.polls += 1;
                if !state.open {
                    state.waker = Some(context.waker().clone());
                }
                state.open
            })
        }

        fn open(&self) {
            if let Some(waker) = self.update(|state| {
                state.open = true;
                state.waker.take()
            }) {
                waker.wake();
            }
        }
    }

    #[derive(Clone, Copy)]
    enum End {
        Ready,
        Panic,
        Never,
    }

    #[derive(Clone)]
    struct Plan {
        pending: usize,
        end: End,
        poll_sets: Option<RuntimeContext>,
        drop_sets: Option<RuntimeContext>,
        drop_panics: bool,
        gate: Option<Signal>,
    }

    impl Plan {
        fn ready(pending: usize) -> Self {
            Self {
                pending,
                end: End::Ready,
                poll_sets: None,
                drop_sets: None,
                drop_panics: false,
                gate: None,
            }
        }

        fn panic(pending: usize) -> Self {
            Self {
                end: End::Panic,
                ..Self::ready(pending)
            }
        }

        fn never() -> Self {
            Self {
                end: End::Never,
                ..Self::ready(0)
            }
        }

        fn setting(self, context: &RuntimeContext) -> Self {
            Self {
                poll_sets: Some(context.clone()),
                ..self
            }
        }

        fn drop_setting(self, context: &RuntimeContext) -> Self {
            Self {
                drop_sets: Some(context.clone()),
                ..self
            }
        }

        fn drop_panicking(self) -> Self {
            Self {
                drop_panics: true,
                ..self
            }
        }

        /// Pends until the signal opens. The plan's polls start after that.
        fn gated(self, gate: &Signal) -> Self {
            Self {
                gate: Some(gate.clone()),
                ..self
            }
        }

        fn probe(self, name: &'static str) -> Probe {
            let Self {
                pending,
                end,
                poll_sets,
                drop_sets,
                drop_panics,
                gate,
            } = self;
            let drop_gate = gate.clone();
            let mut started = None;
            Probe::new(name, move |index, context| {
                if let Some(gate) = &gate {
                    if !gate.polled(context) {
                        return Poll::Pending;
                    }
                }
                let index = index - *started.get_or_insert(index);
                if index == 0
                    && let Some(context) = &poll_sets
                {
                    RuntimeContext::set_current(Some(context.clone()));
                }
                if index < pending {
                    return Poll::Pending;
                }
                match end {
                    End::Ready => Poll::Ready(()),
                    End::Panic => raise("poll"),
                    End::Never => Poll::Pending,
                }
            })
            .on_drop(move || {
                if let Some(gate) = drop_gate {
                    gate.update(|state| state.dropped = true);
                }
                if let Some(context) = drop_sets {
                    RuntimeContext::set_current(Some(context));
                }
                if drop_panics {
                    raise("drop");
                }
            })
        }
    }

    /// A supervisor waker that records its wake and then panics.
    struct PanickingWake(&'static str);

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            record(Event::Woken(self.0, seen()));
            raise("wake");
        }
    }

    /// Records the wake of a `WorkerCall`'s response.
    struct WakeRecorder(&'static str, Option<Signal>);

    impl Wake for WakeRecorder {
        fn wake(self: Arc<Self>) {
            record(Event::Woken(self.0, seen()));
            if let Some(signal) = &self.1 {
                signal.update(|state| state.woken = true);
            }
        }
    }

    /// A host that runs its spawned tasks only when the test runs them.
    #[derive(Clone, Default)]
    struct ManualHost {
        tasks: Rc<RefCell<VecDeque<LocalFuture>>>,
    }

    impl ManualHost {
        fn run(&self) {
            let tasks: Vec<_> = self.tasks.borrow_mut().drain(..).collect();
            let waker = noop_waker();
            let pending: Vec<_> = tasks
                .into_iter()
                .filter_map(|mut task| {
                    task.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                        .then_some(task)
                })
                .collect();
            let mut queue = self.tasks.borrow_mut();
            let spawned: Vec<_> = queue.drain(..).collect();
            queue.extend(pending);
            queue.extend(spawned);
        }
    }

    impl LocalHost for ManualHost {
        fn spawn_local(&self, future: LocalFuture) {
            self.tasks.borrow_mut().push_back(future);
        }

        fn now(&self) -> Instant {
            Instant::now()
        }

        fn sleep_until(&self, _deadline: Instant) -> LocalFuture {
            Box::pin(std::future::pending())
        }

        fn yield_to_io(&self) -> LocalFuture {
            Box::pin(std::future::ready(()))
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct Unpark(std::thread::Thread);

        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut future = std::pin::pin!(future);
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                return output;
            }
            assert!(Instant::now() < deadline, "the hosted worker progresses");
            std::thread::park_timeout(Duration::from_millis(10));
        }
    }

    /// The contexts and observers that scenarios install, each named in the
    /// transcript: the submitter (A, OA), the driver's caller (D, OD), the
    /// host's poller (P, OP), the test's own control calls (C), and the
    /// contexts that a poll (X) and a drop (Y) install.
    #[derive(Clone)]
    struct Cast {
        submitter: RuntimeContext,
        driver: RuntimeContext,
        poller: RuntimeContext,
        control: RuntimeContext,
        poll_change: RuntimeContext,
        drop_change: RuntimeContext,
        submitter_observer: SpanObserver,
        driver_observer: SpanObserver,
        poller_observer: SpanObserver,
    }

    impl Cast {
        fn new() -> Self {
            Self {
                submitter: RuntimeContext::new(),
                driver: RuntimeContext::new(),
                poller: RuntimeContext::new(),
                control: RuntimeContext::new(),
                poll_change: RuntimeContext::new(),
                drop_change: RuntimeContext::new(),
                submitter_observer: SpanObserver::default(),
                driver_observer: SpanObserver::default(),
                poller_observer: SpanObserver::default(),
            }
        }

        fn context_name(&self, context: Option<&RuntimeContext>) -> &'static str {
            let Some(context) = context else {
                return "-";
            };
            [
                (&self.submitter, "A"),
                (&self.driver, "D"),
                (&self.poller, "P"),
                (&self.control, "C"),
                (&self.poll_change, "X"),
                (&self.drop_change, "Y"),
            ]
            .into_iter()
            .find_map(|(known, name)| (known == context).then_some(name))
            .unwrap_or("?")
        }

        fn observer_name(&self, observer: Option<&SpanObserver>) -> &'static str {
            let Some(observer) = observer else {
                return "-";
            };
            [
                (&self.submitter_observer, "OA"),
                (&self.driver_observer, "OD"),
                (&self.poller_observer, "OP"),
            ]
            .into_iter()
            .find_map(|(known, name)| known.is_same(observer).then_some(name))
            .unwrap_or("O?")
        }
    }

    /// Runs `run` with an envelope of `request` and `observer` installed.
    fn under<T>(
        request: &RuntimeContext,
        observer: Option<&SpanObserver>,
        run: impl FnOnce() -> T,
    ) -> T {
        let mut envelope = ContextEnvelope::capture();
        envelope.set_request_context(Some(request.clone()));
        let envelope = match observer {
            Some(observer) => envelope.with_span_observer(observer.clone()),
            None => envelope,
        };
        envelope.run(run)
    }

    #[derive(Default)]
    struct Links(Vec<u64>);

    impl Links {
        fn intern(&mut self, link: Option<u64>) {
            if let Some(link) = link
                && !self.0.contains(&link)
            {
                self.0.push(link);
            }
        }

        fn label(&self, link: Option<u64>) -> String {
            link.map_or_else(
                || "-".to_owned(),
                |link| match self.0.iter().position(|known| *known == link) {
                    Some(index) => format!("s{}", index + 1),
                    None => "s?".to_owned(),
                },
            )
        }
    }

    impl Event {
        fn intern(&self, links: &mut Links) {
            match self {
                Self::Poll(_, seen)
                | Self::Drop(_, seen)
                | Self::Factory(_, seen)
                | Self::FactoryDrop(_, seen)
                | Self::Release(_, seen)
                | Self::Woken(_, seen)
                | Self::PayloadDrop(_, seen) => links.intern(seen.link),
                Self::Close { span, seen, .. } => {
                    links.intern(seen.link);
                    links.intern(Some(*span));
                }
                Self::Snapshot(_, spans) => spans.iter().for_each(|span| {
                    links.intern(Some(span.id.link().0));
                    links.intern(span.parent.map(|parent| parent.link().0));
                }),
                Self::Note(_) => {}
                #[cfg(feature = "observability")]
                Self::Trace(..) => {}
            }
        }

        fn render(&self, cast: &Cast, links: &Links) -> String {
            let seen = |seen: &Seen| {
                format!(
                    "[{} {} {}{}]",
                    cast.context_name(seen.request.as_ref()),
                    links.label(seen.link),
                    cast.observer_name(seen.observer.as_ref()),
                    if seen.unwinding { " !" } else { "" },
                )
            };
            match self {
                Self::Poll(name, at) => format!("poll {name} {}", seen(at)),
                Self::Drop(name, at) => format!("drop {name} {}", seen(at)),
                Self::Factory(name, at) => format!("factory {name} {}", seen(at)),
                Self::FactoryDrop(name, at) => format!("factory-drop {name} {}", seen(at)),
                Self::Close {
                    span,
                    outcome,
                    observer,
                    seen: at,
                } => format!(
                    "close {} {} in {} {}",
                    links.label(Some(*span)),
                    outcome.as_str(),
                    cast.observer_name(observer.as_ref()),
                    seen(at)
                ),
                Self::Release(left, at) => format!("release left={left} {}", seen(at)),
                Self::Woken(name, at) => format!("woken {name} {}", seen(at)),
                Self::PayloadDrop(name, at) => format!("payload-drop {name} {}", seen(at)),
                Self::Note(text) => format!("= {text}"),
                Self::Snapshot(observer, spans) => {
                    let mut text = format!("{observer}:");
                    for span in spans {
                        let _ = write!(
                            text,
                            " {}<{} {} {}x{}",
                            links.label(Some(span.id.link().0)),
                            links.label(span.parent.map(|parent| parent.link().0)),
                            span.kind.as_str(),
                            span.outcome.map_or("open", TerminalOutcome::as_str),
                            span.close_count,
                        );
                    }
                    text
                }
                #[cfg(feature = "observability")]
                Self::Trace(trace, at) => {
                    format!("t:{} #{} {}", trace.label(), trace.place(), seen(at))
                }
            }
        }
    }

    fn render(events: &[Event], cast: &Cast) -> Vec<String> {
        let mut links = Links::default();
        events.iter().for_each(|event| event.intern(&mut links));
        #[cfg(feature = "observability")]
        events.iter().for_each(|event| {
            if let Event::Trace(_, seen) = event {
                links.intern(seen.link);
            }
        });
        events
            .iter()
            .map(|event| event.render(cast, &links))
            .collect()
    }

    /// Runs a scenario with this thread's recording installed and renders its
    /// events. Under `observability`, the subscriber panics once, at the
    /// callback numbered `panic_at`.
    fn transcript_with(run: impl FnOnce(&Cast), panic_at: Option<usize>) -> Vec<String> {
        let cast = Cast::new();
        let sink = Sink::default();
        #[cfg(feature = "observability")]
        RECORDING_CAST.set(Some(cast.clone()));
        {
            let _recording = ThreadRecording::start(&sink, 1, panic_at);
            run(&cast);
            for (name, observer) in [
                ("OA", &cast.submitter_observer),
                ("OD", &cast.driver_observer),
                ("OP", &cast.poller_observer),
            ] {
                let spans = observer.snapshots();
                if !spans.is_empty() {
                    record(Event::Snapshot(name, spans));
                }
            }
        }
        #[cfg(feature = "observability")]
        RECORDING_CAST.set(None);
        let events = std::mem::take(&mut *sink.lock().unwrap_or_else(PoisonError::into_inner));
        render(&events, &cast)
    }

    fn transcript(run: impl FnOnce(&Cast)) -> Vec<String> {
        transcript_with(run, None)
    }

    fn is_trace(line: &&str) -> bool {
        line.starts_with("t:")
    }

    fn assert_lines(name: &str, expected: &[&str], actual: &[&str]) {
        if expected == actual {
            return;
        }
        let first = expected
            .iter()
            .zip(actual)
            .position(|(expected, actual)| expected != actual)
            .unwrap_or(expected.len().min(actual.len()));
        panic!(
            "{name}: transcripts differ at line {first}\n--- expected ---\n{}\n--- actual ---\n{}",
            expected.join("\n"),
            actual.join("\n"),
        );
    }

    fn golden(name: &str) -> &'static str {
        GOLDENS
            .iter()
            .find_map(|(known, lines)| (*known == name).then_some(*lines))
            .unwrap_or_else(|| panic!("{name}: no recorded transcript"))
    }

    /// A tracing callback's line without the fields of Tokio's spawn spans
    /// that depend on the process's earlier tasks or on the runtime's source
    /// layout: the task id and the spawn location's line and column.
    #[cfg(feature = "observability")]
    fn stable_trace(line: &str) -> String {
        line.split(' ')
            .map(|word| match word.split_once('=') {
                Some((key @ ("task.id" | "loc.line" | "loc.col"), _)) => format!("{key}=_"),
                _ => word.to_owned(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Asserts that the scenario's transcript, without its tracing callbacks,
    /// equals its `GOLDENS` entry. Under `observability`, the whole transcript,
    /// with each tracing callback in its place among the other events and the
    /// context installed when it ran, is also compared by its length and hash
    /// in `TRACES`.
    fn assert_transcript(name: &str, lines: &[String]) {
        let actual: Vec<&str> = lines
            .iter()
            .map(String::as_str)
            .filter(|line| !is_trace(line))
            .collect();
        let expected: Vec<&str> = golden(name)
            .lines()
            .filter(|line| !is_trace(line))
            .collect();
        assert_lines(name, &expected, &actual);
        #[cfg(feature = "observability")]
        {
            let stable: Vec<String> = lines.iter().map(String::as_str).map(stable_trace).collect();
            let recorded = TRACES
                .iter()
                .find_map(|(known, count, hash)| (*known == name).then_some((*count, *hash)))
                .unwrap_or_else(|| panic!("{name}: no recorded tracing callbacks"));
            assert!(
                (stable.len(), fnv(&stable)) == recorded,
                "{name}: the transcript with its tracing callbacks differs from the {} lines recorded (hash {:08x}):\n{}",
                recorded.0,
                recorded.1,
                stable.join("\n"),
            );
        }
    }

    struct GroupRig<'a> {
        cast: &'a Cast,
        host: ManualHost,
        group: Rc<RefCell<TaskGroup>>,
    }

    impl<'a> GroupRig<'a> {
        fn new(cast: &'a Cast) -> Self {
            let host = ManualHost::default();
            let group = TaskGroup::with_host(Host::new(Rc::new(host.clone())));
            Self {
                cast,
                host,
                group: Rc::new(RefCell::new(group)),
            }
        }

        fn spawn(
            &self,
            tracked: bool,
            factory: impl FnOnce(CancellationToken) -> Probe,
        ) -> Option<TaskId> {
            let group = &self.group;
            let cast = self.cast;
            let spawned = act("spawn", || {
                under(&cast.submitter, Some(&cast.submitter_observer), || {
                    let mut group = group.borrow_mut();
                    if tracked {
                        group.spawn_local_tracked(factory).map(Some)
                    } else {
                        group.spawn_local(factory).map(|()| None)
                    }
                })
            })?;
            match spawned {
                Ok(id) => {
                    note(format!("spawned {id:?}"));
                    id
                }
                Err(error) => {
                    note(format!("spawn failed {error:?}"));
                    None
                }
            }
        }

        fn run(&self) {
            let cast = self.cast;
            act("run", || {
                under(&cast.poller, Some(&cast.poller_observer), || {
                    self.host.run()
                })
            });
        }

        fn abort(&self, id: TaskId) {
            let aborted = act("abort", || {
                under(&self.cast.control, None, || {
                    self.group.borrow_mut().abort(id)
                })
            });
            if let Some(aborted) = aborted {
                note(format!("abort {id:?} -> {aborted}"));
            }
        }

        fn abort_all(&self) {
            if act("abort_all", || {
                under(&self.cast.control, None, || {
                    self.group.borrow_mut().abort_all()
                })
            })
            .is_some()
            {
                note("abort_all");
            }
        }

        fn cancel(&self) {
            let cancelled = act("cancel", || {
                under(&self.cast.control, None, || {
                    self.group.borrow_mut().cancel()
                })
            });
            if let Some(cancelled) = cancelled {
                note(format!("cancel -> {cancelled}"));
            }
        }

        fn close(&self) {
            let closed = act("close", || {
                under(&self.cast.control, None, || self.group.borrow_mut().close())
            });
            if let Some(closed) = closed {
                note(format!("close -> {closed}"));
            }
        }

        fn drain(&self) {
            while let Some(Some(completion)) = act("join", || {
                under(&self.cast.control, None, || {
                    self.group.borrow_mut().try_join_next()
                })
            }) {
                note(format!("done {:?} {:?}", completion.id, completion.result));
                drop(completion);
            }
        }

        fn finish(self) {
            self.close();
            if !self.group.borrow().is_empty() {
                self.abort_all();
            }
            self.run();
            self.drain();
            let remaining = self.group.borrow().len();
            if remaining != 0 {
                note(format!("{remaining} tasks remain"));
            }
            let Self { group, host, .. } = self;
            act("group drop", move || drop(group));
            act("host drop", move || drop(host));
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum GroupPath {
        Ready,
        PollPanic,
        FactoryPanic,
        Abort,
        AbortAll,
        Cancelled,
        UnpolledAbort,
        Closed,
    }

    fn failing_task(_token: CancellationToken) -> Probe {
        let _capture = FactoryCapture("task");
        record(Event::Factory("task", seen()));
        raise("factory")
    }

    fn task_group_path(cast: &Cast, tracked: bool, path: GroupPath) {
        let rig = GroupRig::new(cast);
        let abort = |id: Option<TaskId>| match id {
            Some(id) => rig.abort(id),
            None => rig.abort_all(),
        };
        match path {
            GroupPath::Ready => {
                rig.spawn(tracked, |_| Plan::ready(1).probe("task"));
                rig.run();
                rig.run();
            }
            GroupPath::PollPanic => {
                rig.spawn(tracked, |_| Plan::panic(1).probe("task"));
                rig.run();
                rig.run();
            }
            GroupPath::FactoryPanic => {
                rig.spawn(tracked, failing_task);
            }
            GroupPath::Abort => {
                let id = rig.spawn(tracked, |_| Plan::never().probe("task"));
                rig.run();
                abort(id);
                rig.run();
            }
            GroupPath::AbortAll => {
                rig.spawn(tracked, |_| Plan::never().probe("task"));
                rig.run();
                rig.abort_all();
                rig.run();
            }
            GroupPath::Cancelled => {
                rig.spawn(tracked, |token| {
                    Probe::new("task", move |_, _| {
                        if token.is_cancellation_requested() {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    })
                });
                rig.run();
                rig.cancel();
                rig.run();
            }
            GroupPath::UnpolledAbort => {
                let id = rig.spawn(tracked, |_| Plan::never().probe("task"));
                abort(id);
                rig.run();
            }
            GroupPath::Closed => {
                rig.close();
                rig.spawn(tracked, |_| Plan::ready(0).probe("task"));
            }
        }
        rig.drain();
        rig.finish();
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Driving {
        Owner,
        Hosted,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Submit {
        Spawn,
        Call,
    }

    fn job(name: &'static str, plan: Plan) -> impl FnOnce(WorkerContext) -> Probe + Send + 'static {
        let capture = FactoryCapture(name);
        move |_context| {
            record(Event::Factory(name, seen()));
            let probe = plan.probe(name);
            drop(capture);
            probe
        }
    }

    fn failing_job(name: &'static str) -> impl FnOnce(WorkerContext) -> Probe + Send + 'static {
        let capture = FactoryCapture(name);
        move |_context| {
            let _capture = capture;
            record(Event::Factory(name, seen()));
            raise("factory")
        }
    }

    /// A job whose factory starts the worker's shutdown.
    fn closing_job(name: &'static str) -> impl FnOnce(WorkerContext) -> Probe + Send + 'static {
        let capture = FactoryCapture(name);
        move |context| {
            record(Event::Factory(name, seen()));
            note(format!(
                "{name} begins shutdown -> {}",
                context.handle().begin_shutdown()
            ));
            drop(capture);
            Plan::ready(0).probe(name)
        }
    }

    /// A job that unmutes its `HostedWorker` thread's recording first.
    fn unmuting_job(
        name: &'static str,
        plan: Plan,
    ) -> impl FnOnce(WorkerContext) -> Probe + Send + 'static {
        let job = job(name, plan);
        move |context| {
            MUTED.set(false);
            job(context)
        }
    }

    struct DriverRig<'a> {
        cast: &'a Cast,
        handle: WorkerHandle,
        driver: Option<WorkerDriver>,
        host: Option<ManualHost>,
        calls: Vec<(&'static str, WorkerCall<()>)>,
    }

    impl<'a> DriverRig<'a> {
        fn new(cast: &'a Cast, driving: Driving) -> Self {
            let host = (driving == Driving::Hosted).then(ManualHost::default);
            let (handle, driver) = match &host {
                None => WorkerDriver::new(8),
                Some(host) => WorkerDriver::with_host(8, Host::new(Rc::new(host.clone()))),
            }
            .expect("worker driver starts");
            Self {
                cast,
                handle,
                driver: Some(driver),
                host,
                calls: Vec::new(),
            }
        }

        fn context(&self) -> WorkerContext {
            self.driver
                .as_ref()
                .expect("a live driver")
                .context()
                .clone()
        }

        fn poll(&mut self) {
            let cast = self.cast;
            let Some(driver) = self.driver.as_mut() else {
                return;
            };
            let polled = act("driver poll", || {
                under(&cast.driver, Some(&cast.driver_observer), || {
                    Pin::new(&mut *driver).poll(&mut Context::from_waker(&noop_waker()))
                })
            });
            if let Some(polled) = polled {
                note(format!("driver {polled:?}"));
            }
        }

        fn run_host(&self) {
            if let Some(host) = &self.host {
                act("host run", || {
                    under(&self.cast.driver, Some(&self.cast.driver_observer), || {
                        host.run()
                    })
                });
            }
        }

        /// Polls the driver. A hosted driver's tasks then run once, and the
        /// driver is polled again to join them.
        fn turn(&mut self) {
            self.poll();
            if self.host.is_some() {
                self.run_host();
                self.poll();
            }
        }

        fn submit(
            &mut self,
            submit: Submit,
            name: &'static str,
            factory: impl FnOnce(WorkerContext) -> Probe + Send + 'static,
        ) {
            match submit {
                Submit::Spawn => self.spawn(name, factory),
                Submit::Call => self.call(name, factory),
            }
        }

        fn spawn(
            &self,
            name: &'static str,
            factory: impl FnOnce(WorkerContext) -> Probe + Send + 'static,
        ) {
            let cast = self.cast;
            let submitted = act("submit", || {
                under(&cast.submitter, Some(&cast.submitter_observer), || {
                    self.handle.spawn_local(factory)
                })
            });
            if let Some(submitted) = submitted {
                note(format!("submit {name} -> {submitted:?}"));
            }
        }

        /// Submits a `call_local`, then polls it once with a waker that
        /// records the response's wake.
        fn call(
            &mut self,
            name: &'static str,
            factory: impl FnOnce(WorkerContext) -> Probe + Send + 'static,
        ) {
            let cast = self.cast;
            let Some(submitted) = act("submit", || {
                under(&cast.submitter, Some(&cast.submitter_observer), || {
                    self.handle.call_local(factory)
                })
            }) else {
                return;
            };
            match submitted {
                Ok(mut call) => {
                    let waker = Waker::from(Arc::new(WakeRecorder(name, None)));
                    let polled = Pin::new(&mut call).poll(&mut Context::from_waker(&waker));
                    note(format!("submit {name} -> Ok, {polled:?}"));
                    self.calls.push((name, call));
                }
                Err(error) => note(format!("submit {name} -> Err({error:?})")),
            }
        }

        fn shutdown(&self) {
            let started = act("begin_shutdown", || {
                under(&self.cast.control, None, || self.handle.begin_shutdown())
            });
            if let Some(started) = started {
                note(format!("begin_shutdown -> {started}"));
            }
        }

        /// Registers a supervisor waker, as a quiescing `HostedWorker` does,
        /// that panics when the next `WorkCompletion` release wakes it.
        fn panic_at_release(&self) {
            let waker = Waker::from(Arc::new(PanickingWake("supervisor")));
            let (_, outstanding) = self.handle.quiesce_snapshot(&waker);
            note(format!("supervisor waits, outstanding={outstanding}"));
        }

        /// Makes the completion hook panic at the span completion after
        /// `skipped` more.
        fn panic_at_close(&self, skipped: usize) {
            CLOSE_PANIC.set(Some(skipped));
            note(format!("close panics after {skipped}"));
        }

        fn finish(mut self) {
            for (name, mut call) in std::mem::take(&mut self.calls) {
                let polled = Pin::new(&mut call).poll(&mut Context::from_waker(&noop_waker()));
                note(format!("call {name} {polled:?}"));
            }
            if let Some(driver) = self.driver.take() {
                let cast = self.cast;
                act("driver drop", || {
                    under(&cast.driver, Some(&cast.driver_observer), move || {
                        drop(driver)
                    })
                });
            }
            note(format!("worker {:?}", self.handle.state()));
            if let Some(host) = self.host.take() {
                act("host drop", move || drop(host));
            }
            // A subscriber panic that leaks a returned value can leak a
            // command's `WorkCompletion` with it.
            let outstanding = self.handle.outstanding_work();
            if outstanding != 0 {
                note(format!("outstanding work {outstanding}"));
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum CommandPath {
        Ready,
        PollPanic,
        FactoryPanic,
        PendingShutdown,
        UnpolledShutdown,
        QueuedShutdown,
        Closed,
        ReleaseWakePanic,
        CommandClosePanic,
        GroupClosePanic,
    }

    fn command_path(cast: &Cast, driving: Driving, submit: Submit, path: CommandPath) {
        let mut rig = DriverRig::new(cast, driving);
        match path {
            CommandPath::Ready => {
                rig.submit(submit, "command", job("command", Plan::ready(1)));
                rig.turn();
                rig.turn();
            }
            CommandPath::PollPanic => {
                rig.submit(submit, "command", job("command", Plan::panic(1)));
                rig.turn();
                rig.turn();
            }
            CommandPath::FactoryPanic => {
                rig.spawn("before", job("before", Plan::ready(0)));
                rig.submit(submit, "command", failing_job("command"));
                rig.spawn("after", job("after", Plan::ready(0)));
                rig.call("after call", job("after call", Plan::ready(0)));
                rig.turn();
                rig.turn();
            }
            CommandPath::PendingShutdown => {
                rig.submit(submit, "command", job("command", Plan::never()));
                rig.turn();
                rig.shutdown();
                rig.turn();
            }
            CommandPath::UnpolledShutdown => {
                rig.submit(submit, "command", job("command", Plan::never()));
                rig.spawn("closer", closing_job("closer"));
                rig.turn();
            }
            CommandPath::QueuedShutdown => {
                rig.submit(submit, "command", job("command", Plan::never()));
                rig.shutdown();
                rig.turn();
            }
            CommandPath::Closed => {
                rig.shutdown();
                rig.turn();
                rig.submit(submit, "command", job("command", Plan::ready(0)));
            }
            CommandPath::ReleaseWakePanic => {
                rig.submit(submit, "command", job("command", Plan::ready(1)));
                rig.turn();
                rig.panic_at_release();
                rig.turn();
            }
            CommandPath::CommandClosePanic | CommandPath::GroupClosePanic => {
                rig.submit(submit, "command", job("command", Plan::ready(1)));
                rig.turn();
                rig.panic_at_close(usize::from(matches!(path, CommandPath::GroupClosePanic)));
                rig.turn();
            }
        }
        rig.shutdown();
        rig.turn();
        rig.finish();
    }

    /// A `HostedWorker` whose owner thread records into its own sink, from
    /// the first job that unmutes it.
    struct WorkerRig<'a> {
        cast: &'a Cast,
        worker: HostedWorker,
        sink: Sink,
        calls: Vec<(&'static str, WorkerCall<()>, Signal)>,
    }

    impl<'a> WorkerRig<'a> {
        fn start(cast: &'a Cast) -> Self {
            MUTED.set(true);
            let worker = HostedWorker::spawn(HostedWorkerOptions {
                command_capacity: NonZeroUsize::new(8).expect("non-zero capacity"),
                thread_name: "task-wrapping-worker".to_owned(),
                on_owner_thread_stop: None,
            })
            .expect("hosted worker starts");
            let sink = Sink::default();
            let installed = Arc::clone(&sink);
            let setup = worker
                .handle()
                .call(move |_| {
                    MUTED.set(true);
                    std::mem::forget(ThreadRecording::start(&installed, 1001, None));
                })
                .expect("recording setup is admitted");
            block_on(setup).expect("recording setup completes");
            MUTED.set(false);
            Self {
                cast,
                worker,
                sink,
                calls: Vec::new(),
            }
        }

        fn submit(
            &mut self,
            submit: Submit,
            name: &'static str,
            factory: impl FnOnce(WorkerContext) -> Probe + Send + 'static,
        ) {
            let cast = self.cast;
            let handle = self.worker.handle();
            match submit {
                Submit::Spawn => {
                    let submitted = under(&cast.submitter, Some(&cast.submitter_observer), || {
                        handle.spawn_local(factory)
                    });
                    note(format!("submit {name} -> {submitted:?}"));
                }
                Submit::Call => {
                    let submitted = under(&cast.submitter, Some(&cast.submitter_observer), || {
                        handle.call_local(factory)
                    });
                    let mut call = submitted.expect("call_local is admitted");
                    note(format!("submit {name} -> Ok"));
                    let woken = Signal::default();
                    self.calls.push((
                        name,
                        {
                            let waker =
                                Waker::from(Arc::new(WakeRecorder(name, Some(woken.clone()))));
                            let polled = Pin::new(&mut call).poll(&mut Context::from_waker(&waker));
                            note(format!("call {name} {polled:?}"));
                            call
                        },
                        woken,
                    ));
                }
            }
        }

        fn wait_resolutions(&self) {
            for (_, _, woken) in &self.calls {
                woken.wait(|state| state.woken);
            }
        }

        fn finish(self) {
            let Self {
                worker,
                sink,
                calls,
                ..
            } = self;
            let handle = worker.handle();
            act("worker drop", move || drop(worker));
            note(format!("worker {:?}", handle.state()));
            for (name, mut call, _) in calls {
                let polled = Pin::new(&mut call).poll(&mut Context::from_waker(&noop_waker()));
                note(format!("call {name} {polled:?}"));
            }
            note("worker thread:");
            let events = std::mem::take(&mut *sink.lock().unwrap_or_else(PoisonError::into_inner));
            SINK.with_borrow(|current| {
                current
                    .as_ref()
                    .expect("the test thread records")
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .extend(events);
            });
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum WorkerPath {
        Ready,
        PollPanic,
        PendingShutdown,
        PollPanicDropPanic,
    }

    fn hosted_worker_path(cast: &Cast, submit: Submit, path: WorkerPath) {
        let mut rig = WorkerRig::start(cast);
        let gate = Signal::default();
        let plan = match path {
            WorkerPath::Ready => Plan::ready(0),
            WorkerPath::PollPanic => Plan::panic(0),
            WorkerPath::PendingShutdown => Plan::never(),
            WorkerPath::PollPanicDropPanic => Plan::panic(0).drop_panicking(),
        }
        .gated(&gate);
        rig.submit(submit, "command", unmuting_job("command", plan));
        gate.wait(|state| state.polls >= 1);
        if !matches!(path, WorkerPath::PendingShutdown) {
            gate.open();
            gate.wait(|state| state.dropped);
            rig.wait_resolutions();
        }
        if matches!(path, WorkerPath::PollPanic | WorkerPath::PollPanicDropPanic) {
            // The driver shuts the worker down once it joins the panicked
            // task; the test's own shutdown request must not race it.
            block_on(rig.worker.handle().closed());
        }
        rig.finish();
    }

    #[derive(Clone, Copy, Debug)]
    enum Subject {
        Group,
        Owner,
        Hosted,
    }

    #[derive(Clone, Copy, Debug)]
    enum ContextPath {
        PendingThenReady,
        PendingThenAbort,
        PendingThenPanic,
        PollSetsThenPanics,
        DropSetsOnReady,
        DropSetsOnAbort,
        DropSetsOnShutdown,
    }

    fn context_path(cast: &Cast, subject: Subject, path: ContextPath) {
        let plan = match path {
            ContextPath::PendingThenReady => Plan::ready(1).setting(&cast.poll_change),
            ContextPath::PendingThenAbort => Plan::never().setting(&cast.poll_change),
            ContextPath::PendingThenPanic => Plan::panic(1).setting(&cast.poll_change),
            ContextPath::PollSetsThenPanics => Plan::panic(0).setting(&cast.poll_change),
            ContextPath::DropSetsOnReady => Plan::ready(0).drop_setting(&cast.drop_change),
            ContextPath::DropSetsOnAbort | ContextPath::DropSetsOnShutdown => {
                Plan::never().drop_setting(&cast.drop_change)
            }
        };
        let aborts = matches!(
            path,
            ContextPath::PendingThenAbort | ContextPath::DropSetsOnAbort
        );
        let shuts_down = matches!(path, ContextPath::DropSetsOnShutdown);
        match subject {
            Subject::Group => {
                let rig = GroupRig::new(cast);
                let id = rig.spawn(true, |_| plan.probe("task"));
                rig.run();
                if aborts {
                    rig.abort(id.expect("a tracked task"));
                }
                if shuts_down {
                    rig.abort_all();
                }
                rig.run();
                rig.drain();
                rig.finish();
            }
            Subject::Owner | Subject::Hosted => {
                let driving = if matches!(subject, Subject::Owner) {
                    Driving::Owner
                } else {
                    Driving::Hosted
                };
                let mut rig = DriverRig::new(cast, driving);
                rig.spawn("command", job("command", plan));
                rig.turn();
                if aborts || shuts_down {
                    rig.shutdown();
                }
                rig.turn();
                rig.shutdown();
                rig.turn();
                rig.finish();
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum DropPanicPath {
        Ready,
        Abort,
        AbortAll,
        Shutdown,
    }

    fn drop_panic_path(cast: &Cast, subject: Subject, submit: Submit, path: DropPanicPath) {
        let plan = match path {
            DropPanicPath::Ready => Plan::ready(0),
            _ => Plan::never(),
        }
        .drop_panicking();
        match subject {
            Subject::Group => {
                let rig = GroupRig::new(cast);
                let id = rig.spawn(true, |_| plan.probe("task"));
                rig.run();
                match path {
                    DropPanicPath::Abort => id.into_iter().for_each(|id| rig.abort(id)),
                    DropPanicPath::AbortAll => rig.abort_all(),
                    DropPanicPath::Ready | DropPanicPath::Shutdown => {}
                }
                rig.run();
                rig.drain();
                rig.finish();
            }
            Subject::Owner | Subject::Hosted => {
                let driving = if matches!(subject, Subject::Owner) {
                    Driving::Owner
                } else {
                    Driving::Hosted
                };
                let mut rig = DriverRig::new(cast, driving);
                rig.submit(submit, "command", job("command", plan));
                rig.turn();
                rig.shutdown();
                rig.turn();
                rig.finish();
            }
        }
    }

    fn poll_panic_drop_panic_path(cast: &Cast, subject: Subject) {
        let plan = Plan::panic(1).drop_panicking();
        match subject {
            Subject::Group => {
                let rig = GroupRig::new(cast);
                rig.spawn(true, |_| plan.probe("task"));
                rig.run();
                rig.run();
                rig.drain();
                rig.finish();
            }
            Subject::Owner | Subject::Hosted => {
                let driving = if matches!(subject, Subject::Owner) {
                    Driving::Owner
                } else {
                    Driving::Hosted
                };
                let mut rig = DriverRig::new(cast, driving);
                rig.spawn("command", job("command", plan));
                rig.turn();
                rig.turn();
                rig.shutdown();
                rig.turn();
                rig.finish();
            }
        }
    }

    /// A task whose drop, or pending poll, aborts a task of its group.
    #[derive(Clone, Copy, Debug)]
    enum AbortPath {
        ReadyDropAbortsItself,
        ReadyDropAbortsOther,
        PanicDropAbortsItself,
        PanicDropAbortsOther,
        PendingPollAbortsItself,
    }

    fn abort_path(cast: &Cast, path: AbortPath) {
        let rig = GroupRig::new(cast);
        let target = Rc::new(Cell::new(None));
        let aborts_other = matches!(
            path,
            AbortPath::ReadyDropAbortsOther | AbortPath::PanicDropAbortsOther
        );
        if aborts_other {
            target.set(rig.spawn(true, |_| Plan::never().probe("other")));
        }
        let abort_target = {
            let group = Rc::clone(&rig.group);
            let target = Rc::clone(&target);
            move |by: &str| {
                let id: TaskId = target.get().expect("the target task is spawned");
                let aborted = group.borrow_mut().abort(id);
                note(format!("{by} aborts {id:?} -> {aborted}"));
            }
        };
        let id = rig.spawn(true, move |_| match path {
            AbortPath::ReadyDropAbortsItself | AbortPath::ReadyDropAbortsOther => Plan::ready(0)
                .probe("task")
                .on_drop(move || abort_target("drop")),
            AbortPath::PanicDropAbortsItself | AbortPath::PanicDropAbortsOther => Plan::panic(0)
                .probe("task")
                .on_drop(move || abort_target("drop")),
            AbortPath::PendingPollAbortsItself => Probe::new("task", move |_, _| {
                abort_target("poll");
                Poll::Pending
            }),
        });
        if !aborts_other {
            target.set(id);
        }
        rig.run();
        rig.run();
        rig.drain();
        rig.finish();
    }

    fn shutdown_while_unwinding(cast: &Cast, driving: Driving) {
        let mut rig = DriverRig::new(cast, driving);
        rig.spawn("pending", job("pending", Plan::never()));
        rig.call("pending call", job("pending call", Plan::never()));
        rig.turn();
        if driving == Driving::Hosted {
            rig.spawn("unpolled", job("unpolled", Plan::never()));
            rig.poll();
        }
        rig.spawn("queued", job("queued", Plan::never()));
        let driver = rig.driver.take();
        act("unwinding", || {
            under(&cast.driver, Some(&cast.driver_observer), move || {
                let _driver = driver;
                raise("test")
            })
        });
        rig.finish();
    }

    #[derive(Clone, Copy, Debug)]
    enum CleanupPath {
        Complete,
        PollPanic,
        DestructionPanic,
        Abort,
        AbortReserved,
    }

    fn cleanup_path(cast: &Cast, driving: Driving, path: CleanupPath) {
        let mut rig = DriverRig::new(cast, driving);
        let context = rig.context();
        let gate = Signal::default();
        let register = |name: &'static str, plan: Plan, scope: bool| {
            let future = plan.gated(&gate).probe(name);
            let registered = under(&cast.submitter, Some(&cast.submitter_observer), || {
                if scope {
                    context.register_scope(future)
                } else {
                    context.register_cleanup_task(future)
                }
            });
            note(format!(
                "register {name} -> {:?}",
                registered.as_ref().map(|_| ())
            ));
            registered.expect("cleanup registers on the owner while open")
        };
        let activate = |registration: crate::worker::WorkerCleanupTaskRegistration| {
            let activated = act("activate", || registration.activate())?;
            note(format!("activate -> {:?}", activated.as_ref().map(|_| ())));
            Some(activated.expect("cleanup activates while open"))
        };
        let mut tasks = Vec::new();
        let mut reserved = Vec::new();
        match path {
            CleanupPath::Complete => {
                tasks.extend(activate(register("cleanup", Plan::ready(0), false)));
            }
            CleanupPath::PollPanic => {
                tasks.extend(activate(register("cleanup", Plan::panic(0), false)));
            }
            CleanupPath::DestructionPanic => {
                tasks.extend(activate(register(
                    "scope",
                    Plan::ready(0).drop_panicking(),
                    true,
                )));
            }
            CleanupPath::Abort => {
                tasks.extend(activate(register("cleanup", Plan::never(), false)));
            }
            CleanupPath::AbortReserved => {
                reserved.push(register("reserved", Plan::never(), false));
                tasks.extend(activate(register("cleanup", Plan::never(), false)));
            }
        }
        rig.turn();
        gate.open();
        rig.turn();
        if !matches!(path, CleanupPath::Abort | CleanupPath::AbortReserved) {
            rig.turn();
            rig.shutdown();
            rig.turn();
        }
        for task in &tasks {
            note(format!("cleanup finished -> {}", task.is_finished()));
        }
        drop(tasks);
        rig.finish();
        note(format!("reserved {}", reserved.len()));
        act("reserved drop", move || drop(reserved));
    }

    fn hosted_worker_cleanup(cast: &Cast) {
        let mut rig = WorkerRig::start(cast);
        let gate = Signal::default();
        let plan = Plan::ready(0).gated(&gate);
        rig.submit(Submit::Spawn, "command", move |context: WorkerContext| {
            MUTED.set(false);
            record(Event::Factory("command", seen()));
            let registration = context
                .register_cleanup_task(plan.probe("cleanup"))
                .expect("cleanup registers on the owner while open");
            registration
                .activate()
                .expect("cleanup activates while open")
                .detach();
            Plan::ready(0).probe("command")
        });
        gate.wait(|state| state.polls >= 1);
        gate.open();
        gate.wait(|state| state.dropped);
        rig.finish();
    }

    const GROUP_PATHS: [GroupPath; 8] = [
        GroupPath::Ready,
        GroupPath::PollPanic,
        GroupPath::FactoryPanic,
        GroupPath::Abort,
        GroupPath::AbortAll,
        GroupPath::Cancelled,
        GroupPath::UnpolledAbort,
        GroupPath::Closed,
    ];

    #[test]
    fn task_group_tasks_keep_their_lifecycle_on_every_terminal_path() {
        for tracked in [false, true] {
            for path in GROUP_PATHS {
                if !tracked && matches!(path, GroupPath::Abort) {
                    continue;
                }
                let name = format!(
                    "group/{}/{path:?}",
                    if tracked { "tracked" } else { "untracked" }
                );
                assert_transcript(
                    &name,
                    &transcript(|cast| task_group_path(cast, tracked, path)),
                );
            }
        }
    }

    const COMMAND_PATHS: [CommandPath; 7] = [
        CommandPath::Ready,
        CommandPath::PollPanic,
        CommandPath::FactoryPanic,
        CommandPath::PendingShutdown,
        CommandPath::UnpolledShutdown,
        CommandPath::QueuedShutdown,
        CommandPath::Closed,
    ];

    #[test]
    #[cfg_attr(
        all(feature = "observability", not(fbcode_build)),
        ignore = "the recorded tracing transcripts include tokio's `runtime.spawn` spans, which need tokio's `tracing` feature and `--cfg tokio_unstable`, and spawn locations relative to a parent of the crate root"
    )]
    fn worker_commands_keep_their_lifecycle_on_every_terminal_path() {
        for driving in [Driving::Owner, Driving::Hosted] {
            for submit in [Submit::Spawn, Submit::Call] {
                for path in COMMAND_PATHS {
                    let name = format!("command/{driving:?}/{submit:?}/{path:?}");
                    assert_transcript(
                        &name,
                        &transcript(|cast| command_path(cast, driving, submit, path)),
                    );
                }
            }
        }
        // Panics on the ready path after the job's poll returns: in the
        // `WorkCompletion` release, and in the command span's and the group
        // span's `Success` close.
        for (driving, path) in [
            (Driving::Owner, CommandPath::ReleaseWakePanic),
            (Driving::Owner, CommandPath::CommandClosePanic),
            (Driving::Hosted, CommandPath::ReleaseWakePanic),
            (Driving::Hosted, CommandPath::CommandClosePanic),
            (Driving::Hosted, CommandPath::GroupClosePanic),
        ] {
            let name = format!("command/{driving:?}/Spawn/{path:?}");
            assert_transcript(
                &name,
                &transcript(|cast| command_path(cast, driving, Submit::Spawn, path)),
            );
        }
        for submit in [Submit::Spawn, Submit::Call] {
            for path in [
                WorkerPath::Ready,
                WorkerPath::PollPanic,
                WorkerPath::PendingShutdown,
            ] {
                let name = format!("command/HostedWorker/{submit:?}/{path:?}");
                assert_transcript(
                    &name,
                    &transcript(|cast| hosted_worker_path(cast, submit, path)),
                );
            }
        }
    }

    #[test]
    fn request_context_changes_keep_their_visibility_on_every_path() {
        for subject in [Subject::Group, Subject::Hosted, Subject::Owner] {
            for path in [
                ContextPath::PendingThenReady,
                ContextPath::PendingThenAbort,
                ContextPath::PendingThenPanic,
                ContextPath::PollSetsThenPanics,
                ContextPath::DropSetsOnReady,
                ContextPath::DropSetsOnAbort,
                ContextPath::DropSetsOnShutdown,
            ] {
                let name = format!("context/{subject:?}/{path:?}");
                assert_transcript(&name, &transcript(|cast| context_path(cast, subject, path)));
            }
        }
    }

    #[test]
    fn user_future_drop_panics_keep_their_lifecycle() {
        for path in [
            DropPanicPath::Ready,
            DropPanicPath::Abort,
            DropPanicPath::AbortAll,
        ] {
            let name = format!("drop_panic/Group/{path:?}");
            assert_transcript(
                &name,
                &transcript(|cast| drop_panic_path(cast, Subject::Group, Submit::Spawn, path)),
            );
        }
        for subject in [Subject::Owner, Subject::Hosted] {
            for submit in [Submit::Spawn, Submit::Call] {
                for path in [DropPanicPath::Ready, DropPanicPath::Shutdown] {
                    let name = format!("drop_panic/{subject:?}/{submit:?}/{path:?}");
                    assert_transcript(
                        &name,
                        &transcript(|cast| drop_panic_path(cast, subject, submit, path)),
                    );
                }
            }
        }
    }

    #[test]
    #[cfg_attr(
        all(feature = "observability", not(fbcode_build)),
        ignore = "the recorded tracing transcripts include tokio's `runtime.spawn` spans, which need tokio's `tracing` feature and `--cfg tokio_unstable`, and spawn locations relative to a parent of the crate root"
    )]
    fn poll_panics_whose_user_future_drop_panics_keep_their_lifecycle() {
        for subject in [Subject::Group, Subject::Owner, Subject::Hosted] {
            let name = format!("poll_drop_panic/{subject:?}");
            assert_transcript(
                &name,
                &transcript(|cast| poll_panic_drop_panic_path(cast, subject)),
            );
        }
        assert_transcript(
            "poll_drop_panic/HostedWorker",
            &transcript(|cast| {
                hosted_worker_path(cast, Submit::Spawn, WorkerPath::PollPanicDropPanic)
            }),
        );
    }

    #[test]
    fn worker_shutdown_while_unwinding_cancels_its_spans() {
        for driving in [Driving::Owner, Driving::Hosted] {
            let name = format!("unwinding/{driving:?}");
            assert_transcript(
                &name,
                &transcript(|cast| shutdown_while_unwinding(cast, driving)),
            );
        }
    }

    #[test]
    fn ready_path_drops_that_abort_tasks_keep_their_results() {
        for path in [
            AbortPath::ReadyDropAbortsItself,
            AbortPath::ReadyDropAbortsOther,
        ] {
            let name = format!("abort/{path:?}");
            assert_transcript(&name, &transcript(|cast| abort_path(cast, path)));
        }
    }

    #[test]
    fn poll_panic_drops_and_pending_polls_that_abort_tasks_keep_their_results() {
        for path in [
            AbortPath::PanicDropAbortsItself,
            AbortPath::PanicDropAbortsOther,
            AbortPath::PendingPollAbortsItself,
        ] {
            let name = format!("abort/{path:?}");
            assert_transcript(&name, &transcript(|cast| abort_path(cast, path)));
        }
    }

    #[test]
    #[cfg_attr(
        all(feature = "observability", not(fbcode_build)),
        ignore = "the recorded tracing transcripts include tokio's `runtime.spawn` spans, which need tokio's `tracing` feature and `--cfg tokio_unstable`, and spawn locations relative to a parent of the crate root"
    )]
    fn cleanup_tasks_keep_their_lifecycle_on_every_terminal_path() {
        for driving in [Driving::Owner, Driving::Hosted] {
            for path in [
                CleanupPath::Complete,
                CleanupPath::PollPanic,
                CleanupPath::DestructionPanic,
                CleanupPath::Abort,
                CleanupPath::AbortReserved,
            ] {
                let name = format!("cleanup/{driving:?}/{path:?}");
                assert_transcript(&name, &transcript(|cast| cleanup_path(cast, driving, path)));
            }
        }
        assert_transcript(
            "cleanup/HostedWorker/Complete",
            &transcript(hosted_worker_cleanup),
        );
    }

    /// Scenarios whose recorded transcripts include the tracing callbacks.
    #[cfg(feature = "observability")]
    const TRACED: [&str; 10] = [
        "group/tracked/Ready",
        "group/tracked/PollPanic",
        "group/tracked/Abort",
        "command/Owner/Spawn/Ready",
        "command/Owner/Spawn/PollPanic",
        "command/Hosted/Spawn/Ready",
        "command/Hosted/Spawn/PollPanic",
        "command/Hosted/Spawn/FactoryPanic",
        "command/Hosted/Spawn/PendingShutdown",
        "unwinding/Hosted",
    ];

    #[cfg(feature = "observability")]
    fn traced_transcript(name: &str) -> Vec<String> {
        transcript(|cast| match name {
            "group/tracked/Ready" => task_group_path(cast, true, GroupPath::Ready),
            "group/tracked/PollPanic" => task_group_path(cast, true, GroupPath::PollPanic),
            "group/tracked/Abort" => task_group_path(cast, true, GroupPath::Abort),
            "command/Owner/Spawn/Ready" => {
                command_path(cast, Driving::Owner, Submit::Spawn, CommandPath::Ready)
            }
            "command/Owner/Spawn/PollPanic" => {
                command_path(cast, Driving::Owner, Submit::Spawn, CommandPath::PollPanic)
            }
            "command/Hosted/Spawn/Ready" => {
                command_path(cast, Driving::Hosted, Submit::Spawn, CommandPath::Ready)
            }
            "command/Hosted/Spawn/PollPanic" => {
                command_path(cast, Driving::Hosted, Submit::Spawn, CommandPath::PollPanic)
            }
            "command/Hosted/Spawn/FactoryPanic" => command_path(
                cast,
                Driving::Hosted,
                Submit::Spawn,
                CommandPath::FactoryPanic,
            ),
            "command/Hosted/Spawn/PendingShutdown" => command_path(
                cast,
                Driving::Hosted,
                Submit::Spawn,
                CommandPath::PendingShutdown,
            ),
            "unwinding/Hosted" => shutdown_while_unwinding(cast, Driving::Hosted),
            _ => unreachable!("{name} is not traced"),
        })
    }

    /// Strips the context recorded with a tracing callback.
    #[cfg(feature = "observability")]
    fn without_context(line: &str) -> &str {
        line.rfind(" [").map_or(line, |index| &line[..index])
    }

    #[cfg(feature = "observability")]
    #[test]
    fn tracing_subscribers_see_the_same_enters_exits_and_closes() {
        for name in TRACED {
            let lines = traced_transcript(name);
            let actual: Vec<&str> = lines
                .iter()
                .map(String::as_str)
                .filter(is_trace)
                .map(without_context)
                .collect();
            let expected: Vec<&str> = golden(name)
                .lines()
                .filter(is_trace)
                .map(without_context)
                .collect();
            assert!(!expected.is_empty(), "{name} records tracing callbacks");
            assert_lines(name, &expected, &actual);
        }
    }

    #[cfg(feature = "observability")]
    #[test]
    fn subscriber_callbacks_see_the_same_runtime_context() {
        for name in TRACED {
            let lines = traced_transcript(name);
            let actual: Vec<&str> = lines.iter().map(String::as_str).collect();
            let expected: Vec<&str> = golden(name).lines().collect();
            assert_lines(name, &expected, &actual);
        }
    }

    #[cfg(feature = "observability")]
    fn fnv(lines: &[String]) -> u32 {
        let hash = lines
            .iter()
            .flat_map(|line| line.bytes().chain(*b"\n"))
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
            });
        (hash ^ (hash >> 32)) as u32
    }

    /// Set in a child test process to the sweep whose places it runs.
    #[cfg(feature = "observability")]
    const SWEEP_CHILD: &str = "ROXYGEN_TASK_WRAPPING_SWEEP_CHILD";

    /// The comma-separated places that a child test process runs.
    #[cfg(feature = "observability")]
    const SWEEP_CHILD_PLACES: &str = "ROXYGEN_TASK_WRAPPING_SWEEP_PLACES";

    /// Precedes each place's outcome on a child test process's output.
    #[cfg(feature = "observability")]
    const PLACE_MARKER: &str = "task_wrapping sweep place ";

    /// Precedes, on standard error, the place where the subscriber panics.
    #[cfg(feature = "observability")]
    const PANIC_MARKER: &str = "task_wrapping subscriber panics at #";

    /// Precedes, on a sweep child's standard error, the hash of the
    /// transcript recorded before it exits at `CLEANUP_PANIC`.
    #[cfg(feature = "observability")]
    const ABORT_MARKER: &str = "task_wrapping transcript before the abort: ";

    /// The panic that the runtime raises when a destructor panics during an
    /// unwind, just before it aborts the process.
    #[cfg(feature = "observability")]
    const CLEANUP_PANIC: &str = "panic in a destructor during cleanup";

    /// How a child test process exits at `CLEANUP_PANIC`.
    #[cfg(feature = "observability")]
    const CLEANUP_PANIC_EXIT: i32 = 86;

    /// Whether this process is a sweep child, which exits at `CLEANUP_PANIC`.
    #[cfg(feature = "observability")]
    static EXIT_AT_CLEANUP_PANIC: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    /// Installs, once, a panic hook that reports a panic in `quietly` without
    /// a backtrace and, in a sweep child, exits at `CLEANUP_PANIC` instead of
    /// letting the runtime abort. An abort would dump core, and some core dump
    /// handlers ignore `RLIMIT_CORE`. Other panics go to the hook it replaces.
    #[cfg(feature = "observability")]
    fn install_panic_hook() {
        static HOOK: std::sync::Once = std::sync::Once::new();
        HOOK.call_once(|| {
            let report = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                if EXIT_AT_CLEANUP_PANIC.load(std::sync::atomic::Ordering::SeqCst)
                    && info.payload().downcast_ref::<&str>() == Some(&CLEANUP_PANIC)
                {
                    eprintln!("{info}");
                    if let Some(hash) = transcript_so_far() {
                        eprintln!("{ABORT_MARKER}{hash:08x}");
                    }
                    std::process::exit(CLEANUP_PANIC_EXIT);
                }
                if QUIET_PANICS.get() {
                    eprintln!("{info}");
                } else {
                    report(info);
                }
            }));
        });
    }

    /// A hash of the transcript that this thread has recorded so far.
    #[cfg(feature = "observability")]
    fn transcript_so_far() -> Option<u32> {
        let sink = SINK
            .try_with(|sink| sink.try_borrow().ok()?.clone())
            .ok()??;
        let cast = RECORDING_CAST
            .try_with(|cast| cast.try_borrow().ok()?.clone())
            .ok()??;
        let events = sink.try_lock().ok()?;
        Some(fnv(&render(&events, &cast)))
    }

    /// Runs `run` with this thread's panics reported without a backtrace.
    /// When `RUST_BACKTRACE=1` is set, symbolizing a backtrace for each of a
    /// sweep's thousands of expected panics takes minutes.
    #[cfg(feature = "observability")]
    fn quietly<T>(run: impl FnOnce() -> T) -> T {
        struct Restore(bool);

        impl Drop for Restore {
            fn drop(&mut self) {
                QUIET_PANICS.set(self.0);
            }
        }

        install_panic_hook();
        let _restore = Restore(QUIET_PANICS.replace(true));
        run()
    }

    /// One run with the subscriber panicking at `place`: its notes' panics,
    /// completions, and call results, and a hash of its whole transcript.
    #[cfg(feature = "observability")]
    fn place_outcome(run: &dyn Fn(&Cast), place: usize) -> String {
        let lines = quietly(|| transcript_with(run, Some(place)));
        let outcomes: Vec<&str> = lines
            .iter()
            .map(String::as_str)
            .filter(|line| {
                line.starts_with("= ")
                    && (line.contains("panicked")
                        || line.starts_with("= done")
                        || line.starts_with("= call"))
            })
            .map(|line| line.trim_start_matches("= "))
            .collect();
        format!("{:08x} {}", fnv(&lines), outcomes.join("; "))
    }

    /// Runs `places` of the sweep `name` in a child test process, with leak
    /// detection off.
    #[cfg(feature = "observability")]
    fn sweep_child(name: &str, places: &[usize]) -> Output {
        let (_, module) = module_path!()
            .split_once("::")
            .expect("crate-qualified module path");
        let places: Vec<String> = places.iter().map(usize::to_string).collect();
        let asan = std::env::var("ASAN_OPTIONS").map_or_else(|_| String::new(), |asan| asan + ":");
        Command::new(std::env::current_exe().expect("test binary path"))
            .arg(format!(
                "{module}::subscriber_panics_at_each_span_callback_keep_their_outcomes"
            ))
            .args(["--exact", "--test-threads=1", "--nocapture"])
            .env(SWEEP_CHILD, name)
            .env(SWEEP_CHILD_PLACES, places.join(","))
            .env("ASAN_OPTIONS", format!("{asan}detect_leaks=0"))
            .stdin(Stdio::null())
            .output()
            .expect("child test process runs")
    }

    /// Checks, in a child process, that a subscriber panic at `place` makes a
    /// destructor panic during the unwind, where the runtime aborts the
    /// process, and hashes the transcript recorded before that abort.
    #[cfg(feature = "observability")]
    fn aborts_in_child(name: &str, place: usize) -> String {
        let child = sweep_child(name, &[place]);
        let stderr = String::from_utf8_lossy(&child.stderr);
        let transcript = stderr
            .lines()
            .find_map(|line| line.strip_prefix(ABORT_MARKER))
            .filter(|_| {
                child.status.code() == Some(CLEANUP_PANIC_EXIT)
                    && stderr.contains(&format!("{PANIC_MARKER}{place}\n"))
                    && stderr.contains(CLEANUP_PANIC)
            })
            .unwrap_or_else(|| {
                panic!(
                    "{name} #{place}: a subscriber panic there aborts the process: {}\nstdout:\n{}\nstderr:\n{stderr}",
                    child.status,
                    String::from_utf8_lossy(&child.stdout),
                )
            });
        format!("aborts after {transcript}, as a child process shows")
    }

    /// The outcomes of `places` of the sweep `name`, run in one child process.
    #[cfg(feature = "observability")]
    fn outcomes_in_child(name: &str, places: &[usize]) -> Vec<String> {
        let child = sweep_child(name, places);
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(
            child.status.success(),
            "{name}: a child process runs places {places:?}: {}\nstdout:\n{stdout}\nstderr:\n{}",
            child.status,
            String::from_utf8_lossy(&child.stderr),
        );
        let reported: Vec<(usize, String)> = stdout
            .lines()
            .filter_map(|line| line.split_once(PLACE_MARKER))
            .map(|(_, line)| {
                let (place, outcome) = line.split_once(' ').expect("a place and its outcome");
                (
                    place.parse().expect("a callback number"),
                    outcome.to_owned(),
                )
            })
            .collect();
        assert_eq!(
            reported.iter().map(|(place, _)| *place).collect::<Vec<_>>(),
            places,
            "{name}: the child process reports each place it runs"
        );
        reported.into_iter().map(|(_, outcome)| outcome).collect()
    }

    /// Runs `run` once per subscriber callback, with the subscriber
    /// panicking at that callback, and summarizes each run's outcome
    /// (`place_outcome`). A callback that runs while the thread unwinds,
    /// outside any `catch_unwind`, is left out, since a panic there aborts
    /// the process. A child process checks that such a panic aborts and hashes
    /// the transcript recorded before the abort. The callbacks that `IN_CHILD`
    /// lists run in child processes.
    #[cfg(feature = "observability")]
    fn sweep(name: &str, run: &dyn Fn(&Cast), unwinding_contained: bool) -> Vec<String> {
        let baseline = quietly(|| transcript(run));
        let (aborting, leaking) = IN_CHILD
            .iter()
            .find_map(|(known, aborting, leaking)| {
                (*known == name).then_some((*aborting, *leaking))
            })
            .unwrap_or((&[], &[]));
        let mut summary = vec![format!("baseline: {:08x}", fnv(&baseline))];
        let mut leaks = Vec::new();
        for line in baseline.iter().filter(|line| is_trace(&line.as_str())) {
            let label = without_context(line).trim_start_matches("t:");
            let (label, place) = label.rsplit_once(" #").expect("a numbered callback");
            let place: usize = place.parse().expect("a callback number");
            let outcome = if line.ends_with(" !]") && !unwinding_contained {
                format!(
                    "left out, runs while unwinding; {}",
                    aborts_in_child(name, place)
                )
            } else if aborting.contains(&place) {
                format!("left out; {}", aborts_in_child(name, place))
            } else if leaking.contains(&place) {
                leaks.push((summary.len(), place));
                String::new()
            } else {
                place_outcome(run, place)
            };
            summary.push(format!("#{place} {label}: {outcome}"));
        }
        if !leaks.is_empty() {
            let places: Vec<usize> = leaks.iter().map(|(_, place)| *place).collect();
            for ((index, _), outcome) in leaks.iter().zip(outcomes_in_child(name, &places)) {
                let _ = write!(
                    summary[*index],
                    "{outcome}; run in a child process without leak detection"
                );
            }
        }
        summary
    }

    /// Callbacks, by sweep and number, that run outside an unwind but where a
    /// subscriber panic still aborts the process (first list) or leaks memory
    /// that the leak sanitizer reports (second list), on the runtime that
    /// `GOLDENS` records. Each runs in a child process.
    ///
    /// A panic at the enter of a `WorkerDriver::new` command's terminal
    /// install, after its poll returns ready, closes the command span and
    /// leaves the observed future's inner future in place. The driver then
    /// drops the task as the panic leaves its poll, `cancel_operation` does not
    /// find a span, and the command's `CommandTask` re-raises that panic in a
    /// destructor. A panic at the new span of a hosted task group's `close`, in
    /// a shutdown that runs while the thread unwinds, leaves the group's tasks
    /// in place, so its `Drop` assertion panics during the unwind. Where the
    /// user future's drop panics, a panic that leaves that future in place (a
    /// span's clone when the task is wrapped, or the enter of the install that
    /// would drop it) makes it drop during the unwind. A child process checks
    /// that each of these aborts and hashes the transcript recorded before the
    /// abort.
    ///
    /// An exit or `try_close` that a return runs, from a guard or envelope
    /// dropped after the returned value exists, leaks that value when it
    /// panics. One child process per sweep runs these with leak detection off,
    /// and their outcomes are compared as the in-process ones are.
    #[cfg(feature = "observability")]
    const IN_CHILD: &[(&str, &[usize], &[usize])] = &[
        ("sweep/group/Ready", &[], &[2, 6, 10]),
        ("sweep/group/PollPanic", &[], &[2, 6]),
        ("sweep/group/Abort", &[], &[2, 6]),
        (
            "sweep/Owner/Ready",
            &[49, 66, 79, 92],
            &[
                7, 8, 12, 17, 18, 22, 27, 28, 32, 37, 38, 42, 46, 59, 63, 76, 89, 104, 105,
            ],
        ),
        (
            "sweep/Owner/PollPanic",
            &[49, 66, 79],
            &[
                7, 8, 12, 17, 18, 22, 27, 28, 32, 37, 38, 42, 46, 59, 63, 76, 106, 107,
            ],
        ),
        (
            "sweep/Owner/PendingShutdown",
            &[49, 66, 79],
            &[
                7, 8, 12, 17, 18, 22, 27, 28, 32, 37, 38, 42, 46, 59, 63, 76, 91, 92,
            ],
        ),
        (
            "sweep/Owner/Unwinding",
            &[],
            &[5, 6, 10, 15, 16, 20, 24, 28, 35, 36],
        ),
        (
            "sweep/Hosted/Ready",
            &[],
            &[
                7, 8, 12, 16, 21, 22, 26, 30, 35, 36, 40, 44, 49, 50, 54, 58, 64, 75, 90, 92, 98,
                109, 124, 135, 150, 161, 176, 177,
            ],
        ),
        (
            "sweep/Hosted/PollPanic",
            &[],
            &[
                7, 8, 12, 16, 21, 22, 26, 30, 35, 36, 40, 44, 49, 50, 54, 58, 64, 75, 90, 92, 98,
                109, 124, 135, 180, 181,
            ],
        ),
        (
            "sweep/Hosted/PendingShutdown",
            &[],
            &[
                7, 8, 12, 16, 21, 22, 26, 30, 35, 36, 40, 44, 49, 50, 54, 58, 64, 75, 90, 92, 98,
                109, 124, 135, 150, 151,
            ],
        ),
        (
            "sweep/Hosted/Unwinding",
            &[70],
            &[
                5, 6, 10, 14, 19, 20, 24, 28, 34, 36, 42, 44, 50, 51, 55, 59, 66, 67,
            ],
        ),
        ("sweep/group/DropPanic", &[3], &[2]),
        ("sweep/group/DropPanicAbort", &[3, 8, 9, 10, 11], &[2, 6]),
        ("sweep/group/PollDropPanic", &[3, 14, 15], &[2, 6]),
        ("sweep/Owner/DropPanic", &[10], &[4, 5, 9, 26, 27]),
        (
            "sweep/Owner/DropPanicShutdown",
            &[10, 21, 22, 23, 24],
            &[4, 5, 9, 13, 19, 20],
        ),
        (
            "sweep/Owner/PollDropPanic",
            &[10, 19, 20, 21, 22],
            &[4, 5, 9, 13, 34, 35],
        ),
        (
            "sweep/Owner/FactoryPanic",
            &[34],
            &[7, 8, 12, 17, 18, 31, 46, 47],
        ),
        ("sweep/Owner/Cleanup", &[], &[4, 5]),
        (
            "sweep/Hosted/DropPanic",
            &[10, 11, 12, 14],
            &[4, 5, 9, 13, 45, 46],
        ),
        (
            "sweep/Hosted/DropPanicShutdown",
            &[10, 11, 12, 14, 30, 31, 32, 33, 34, 35, 36, 37],
            &[4, 5, 9, 13, 19, 21, 27, 28],
        ),
        (
            "sweep/Hosted/PollDropPanic",
            &[10, 11, 12, 14, 31, 32],
            &[4, 5, 9, 13, 19, 21, 57, 58],
        ),
        (
            "sweep/Hosted/FactoryPanic",
            &[],
            &[7, 8, 12, 16, 21, 22, 37, 48, 63, 64],
        ),
        ("sweep/Hosted/Cleanup", &[], &[2, 25, 26]),
        ("sweep/Hosted/CleanupPollPanic", &[], &[2]),
    ];

    #[cfg(feature = "observability")]
    fn sweep_group(cast: &Cast, path: GroupPath) {
        task_group_path(cast, true, path);
    }

    /// A command with commands drained before and after it in one driver
    /// poll, the last of them a `call_local`.
    #[cfg(feature = "observability")]
    fn sweep_command(cast: &Cast, driving: Driving, plan: Plan, shuts_down: bool) {
        let mut rig = DriverRig::new(cast, driving);
        rig.spawn("before", job("before", Plan::ready(0)));
        rig.spawn("command", job("command", plan));
        rig.spawn("after", job("after", Plan::ready(0)));
        rig.call("after call", job("after call", Plan::ready(0)));
        rig.turn();
        if shuts_down {
            rig.shutdown();
        }
        rig.turn();
        rig.shutdown();
        rig.turn();
        rig.finish();
    }

    /// A sweep's name, scenario, and whether a callback that runs while the
    /// thread unwinds runs inside a `catch_unwind` that contains its panic.
    #[cfg(feature = "observability")]
    type Sweep = (String, Box<dyn Fn(&Cast)>, bool);

    #[cfg(feature = "observability")]
    fn sweeps() -> Vec<Sweep> {
        let mut sweeps: Vec<Sweep> = Vec::new();
        for path in [GroupPath::Ready, GroupPath::PollPanic, GroupPath::Abort] {
            sweeps.push((
                format!("sweep/group/{path:?}"),
                Box::new(move |cast| sweep_group(cast, path)),
                false,
            ));
        }
        for driving in [Driving::Owner, Driving::Hosted] {
            for (path, plan, shuts_down) in [
                ("Ready", Plan::ready(1), false),
                ("PollPanic", Plan::panic(1), false),
                ("PendingShutdown", Plan::never(), true),
            ] {
                sweeps.push((
                    format!("sweep/{driving:?}/{path}"),
                    Box::new(move |cast| sweep_command(cast, driving, plan.clone(), shuts_down)),
                    false,
                ));
            }
            sweeps.push((
                format!("sweep/{driving:?}/Unwinding"),
                Box::new(move |cast| shutdown_while_unwinding(cast, driving)),
                true,
            ));
        }
        for (name, run) in [
            (
                "sweep/group/DropPanic",
                (|cast| drop_panic_path(cast, Subject::Group, Submit::Spawn, DropPanicPath::Ready))
                    as fn(&Cast),
            ),
            ("sweep/group/DropPanicAbort", |cast| {
                drop_panic_path(cast, Subject::Group, Submit::Spawn, DropPanicPath::Abort);
            }),
            ("sweep/group/PollDropPanic", |cast| {
                poll_panic_drop_panic_path(cast, Subject::Group);
            }),
            ("sweep/Owner/DropPanic", |cast| {
                drop_panic_path(cast, Subject::Owner, Submit::Spawn, DropPanicPath::Ready);
            }),
            ("sweep/Owner/DropPanicShutdown", |cast| {
                drop_panic_path(cast, Subject::Owner, Submit::Spawn, DropPanicPath::Shutdown);
            }),
            ("sweep/Owner/PollDropPanic", |cast| {
                poll_panic_drop_panic_path(cast, Subject::Owner);
            }),
            ("sweep/Owner/FactoryPanic", |cast| {
                command_path(
                    cast,
                    Driving::Owner,
                    Submit::Spawn,
                    CommandPath::FactoryPanic,
                );
            }),
            ("sweep/Owner/Cleanup", |cast| {
                cleanup_path(cast, Driving::Owner, CleanupPath::Complete);
            }),
            ("sweep/Hosted/DropPanic", |cast| {
                drop_panic_path(cast, Subject::Hosted, Submit::Spawn, DropPanicPath::Ready);
            }),
            ("sweep/Hosted/DropPanicShutdown", |cast| {
                drop_panic_path(
                    cast,
                    Subject::Hosted,
                    Submit::Spawn,
                    DropPanicPath::Shutdown,
                );
            }),
            ("sweep/Hosted/PollDropPanic", |cast| {
                poll_panic_drop_panic_path(cast, Subject::Hosted);
            }),
            ("sweep/Hosted/FactoryPanic", |cast| {
                command_path(
                    cast,
                    Driving::Hosted,
                    Submit::Spawn,
                    CommandPath::FactoryPanic,
                );
            }),
            ("sweep/Hosted/Cleanup", |cast| {
                cleanup_path(cast, Driving::Hosted, CleanupPath::Complete);
            }),
            ("sweep/Hosted/CleanupPollPanic", |cast| {
                cleanup_path(cast, Driving::Hosted, CleanupPath::PollPanic);
            }),
        ] {
            sweeps.push((name.to_owned(), Box::new(run), false));
        }
        sweeps
    }

    /// In a child test process, runs the places that the parent names and
    /// prints each one's outcome.
    #[cfg(feature = "observability")]
    fn run_sweep_child(sweeps: &[Sweep], name: &str) {
        EXIT_AT_CLEANUP_PANIC.store(true, std::sync::atomic::Ordering::SeqCst);
        install_panic_hook();
        let (_, run, _) = sweeps
            .iter()
            .find(|(known, ..)| known == name)
            .unwrap_or_else(|| panic!("{name}: no such sweep"));
        let places = std::env::var(SWEEP_CHILD_PLACES).expect("the parent names the places");
        for place in places.split(',') {
            let place: usize = place.parse().expect("a callback number");
            println!(
                "{PLACE_MARKER}{place} {}",
                place_outcome(run.as_ref(), place)
            );
        }
    }

    #[cfg(feature = "observability")]
    #[test]
    fn subscriber_panics_at_each_span_callback_keep_their_outcomes() {
        let sweeps = sweeps();
        if let Ok(name) = std::env::var(SWEEP_CHILD) {
            run_sweep_child(&sweeps, &name);
            return;
        }
        for (name, run, unwinding_contained) in &sweeps {
            let summary = sweep(name, run.as_ref(), *unwinding_contained);
            let actual: Vec<&str> = summary.iter().map(String::as_str).collect();
            let expected: Vec<&str> = golden(name).lines().collect();
            assert_lines(name, &expected, &actual);
        }
    }

    /// The context installs and task-wrapper `catch_unwind` calls of one
    /// pending poll of a `TaskGroup` task, of a command on
    /// `WorkerDriver::new`, of a command on `WorkerDriver::with_host`, and of
    /// an activated cleanup on `WorkerDriver::with_host`.
    fn pending_poll_work() -> [(usize, usize); 4] {
        let counts = || {
            (
                crate::context::context_installs(),
                crate::observability::task_catch_unwinds(),
            )
        };
        let measure = |poll: &dyn Fn()| {
            let before = counts();
            poll();
            let after = counts();
            (after.0 - before.0, after.1 - before.1)
        };

        let host = ManualHost::default();
        let mut group = TaskGroup::with_host(Host::new(Rc::new(host.clone())));
        group
            .spawn_local(|_| Plan::never().probe("task"))
            .expect("the group admits the task");
        host.run();
        let task = measure(&|| host.run());
        group.abort_all();
        host.run();
        group.reap_finished();
        drop(group);

        let pending_command = |driving: Driving| {
            let cast = Cast::new();
            let rig = RefCell::new(DriverRig::new(&cast, driving));
            rig.borrow().spawn("command", job("command", Plan::never()));
            rig.borrow_mut().turn();
            let work = match driving {
                Driving::Owner => measure(&|| {
                    Pin::new(rig.borrow_mut().driver.as_mut().expect("a live driver"))
                        .poll(&mut Context::from_waker(&noop_waker()))
                        .is_pending();
                }),
                Driving::Hosted => measure(&|| {
                    rig.borrow().host.as_ref().expect("a hosted driver").run();
                }),
            };
            rig.into_inner().finish();
            work
        };

        let pending_cleanup = || {
            let cast = Cast::new();
            let mut rig = DriverRig::new(&cast, Driving::Hosted);
            let cleanup = rig
                .context()
                .register_cleanup_task(Plan::never().probe("cleanup"))
                .expect("cleanup registers on the owner while open")
                .activate()
                .expect("cleanup activates while open");
            rig.turn();
            let work = measure(&|| rig.host.as_ref().expect("a hosted driver").run());
            drop(cleanup);
            rig.finish();
            work
        };
        [
            task,
            pending_command(Driving::Owner),
            pending_command(Driving::Hosted),
            pending_cleanup(),
        ]
    }

    /// A hosted command's pending poll keeps two nested installs and two
    /// catches under `observability`, so that its command span's callbacks
    /// run under the group context. The base build makes one of each.
    const HOSTED_POLL_WORK: usize = if cfg!(feature = "observability") {
        2
    } else {
        1
    };

    #[test]
    fn each_pending_poll_counts_its_context_installs() {
        let installs = pending_poll_work().map(|(installs, ..)| installs);
        assert_eq!(
            installs,
            [1, 1, HOSTED_POLL_WORK, 1],
            "a TaskGroup task, a WorkerDriver::new command, a with_host command, and a with_host cleanup"
        );
    }

    #[test]
    fn each_pending_poll_counts_its_catch_unwind_calls() {
        let catches = pending_poll_work().map(|(_, catches)| catches);
        assert_eq!(
            catches,
            [1, 1, HOSTED_POLL_WORK, 1],
            "a TaskGroup task, a WorkerDriver::new command, a with_host command, and a with_host cleanup"
        );
    }

    /// Dropping the host's wrapper of a pending hosted cleanup finishes the
    /// cleanup at that drop, as dropping a `TaskGroup` task's wrapper finishes
    /// its task: the cleanup future is destroyed and its task reports finished.
    #[test]
    fn dropping_a_pending_hosted_cleanup_wrapper_finishes_the_cleanup() {
        struct SetOnDrop(Rc<Cell<bool>>);

        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let cast = Cast::new();
        let mut rig = DriverRig::new(&cast, Driving::Hosted);
        let dropped = Rc::new(Cell::new(false));
        let guard = SetOnDrop(Rc::clone(&dropped));
        let cleanup = rig
            .context()
            .register_cleanup_task(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            })
            .expect("cleanup registers on the owner while open")
            .activate()
            .expect("cleanup activates while open");
        rig.turn();
        assert!(
            !cleanup.is_finished(),
            "the cleanup is pending after its poll"
        );

        let wrappers: Vec<_> = rig
            .host
            .as_ref()
            .expect("a hosted driver")
            .tasks
            .borrow_mut()
            .drain(..)
            .collect();
        assert_eq!(wrappers.len(), 1, "the host holds the cleanup's wrapper");
        drop(wrappers);
        assert!(
            dropped.get(),
            "dropping the wrapper destroys the cleanup future"
        );
        assert!(
            cleanup.is_finished(),
            "dropping the wrapper finishes the cleanup"
        );
        drop(cleanup);
        rig.finish();
    }

    /// A task whose poll starts a span on a new observer from the installed
    /// context, and records the link installed there and the parent that the
    /// observer recorded.
    fn observed_parent(seen: &Rc<RefCell<Vec<(Option<SpanLink>, Option<SpanLink>)>>>) -> Probe {
        let seen = Rc::clone(seen);
        Probe::new("task", move |_, _| {
            let observer = SpanObserver::new(4).expect("observer starts");
            let span = observer.start(OperationKind::Stream, &ContextEnvelope::capture());
            let parent = observer.snapshots()[0].parent.map(|parent| parent.link());
            seen.borrow_mut()
                .push((crate::context::current_span_link(), parent));
            drop(span);
            Poll::Ready(())
        })
    }

    #[test]
    fn spans_keep_their_links_without_an_observer() {
        assert!(
            crate::context::current_span_observer().is_none(),
            "no observer is installed"
        );
        let seen = Rc::new(RefCell::new(Vec::new()));

        let host = ManualHost::default();
        let mut group = TaskGroup::with_host(Host::new(Rc::new(host.clone())));
        let probe = observed_parent(&seen);
        group
            .spawn_local(move |_| probe)
            .expect("the group admits the task");
        host.run();
        group.close();
        group.reap_finished();
        drop(group);

        thread_local! {
            static OBSERVED: RefCell<Vec<(Option<SpanLink>, Option<SpanLink>)>> =
                const { RefCell::new(Vec::new()) };
        }
        for driving in [Driving::Owner, Driving::Hosted] {
            let cast = Cast::new();
            let mut rig = DriverRig::new(&cast, driving);
            rig.handle
                .spawn_local(|_| {
                    let seen = Rc::new(RefCell::new(Vec::new()));
                    let probe = observed_parent(&seen);
                    let observed = Rc::clone(&seen);
                    probe.on_drop(move || {
                        OBSERVED.with_borrow_mut(|all| all.extend(observed.borrow().iter()));
                    })
                })
                .expect("the command is admitted");
            rig.turn();
            rig.shutdown();
            rig.turn();
            rig.finish();
        }
        seen.borrow_mut()
            .extend(OBSERVED.with_borrow_mut(std::mem::take));

        let seen = seen.borrow();
        assert_eq!(seen.len(), 3, "the task and both commands polled once");
        for (link, parent) in seen.iter() {
            assert!(link.is_some(), "each poll sees its span's link");
            assert_eq!(parent, link, "the started span's parent is that span");
        }
    }

    #[test]
    fn host_tokio_shares_one_implementation_per_thread() {
        let first = Host::tokio();
        let second = Host::tokio();
        assert_eq!(
            first.implementation_id(),
            second.implementation_id(),
            "one thread's Hosts share one implementation"
        );
        assert_eq!(first.owner_thread(), std::thread::current().id());
        assert_eq!(second.owner_thread(), std::thread::current().id());

        let (report, reported) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let other = std::thread::spawn(move || {
            let host = Host::tokio();
            report
                .send((host.implementation_id(), host.owner_thread()))
                .expect("the test receives the report");
            released.recv().expect("the test releases the thread");
            std::thread::current().id()
        });
        let (other_implementation, other_owner) =
            reported.recv().expect("the other thread reports");
        assert_ne!(
            other_implementation,
            first.implementation_id(),
            "another thread's Host has its own implementation"
        );
        release.send(()).expect("the other thread waits");
        let other_thread = other.join().expect("the other thread exits");
        assert_eq!(other_owner, other_thread);
    }

    /// A thread-local whose destructor calls `Host::tokio()` and reports the
    /// result.
    struct DestructorProbe(RefCell<Option<mpsc::Sender<std::thread::Result<(ThreadId, usize)>>>>);

    impl Drop for DestructorProbe {
        fn drop(&mut self) {
            let Some(report) = self.0.borrow_mut().take() else {
                return;
            };
            let result = catch_unwind(|| {
                let host = Host::tokio();
                (host.owner_thread(), crate::host::tokio_host_fallbacks())
            });
            let _ = report.send(result);
        }
    }

    thread_local! {
        static DESTRUCTOR_PROBE: DestructorProbe = const { DestructorProbe(RefCell::new(None)) };
    }

    #[test]
    fn host_tokio_falls_back_during_thread_local_destruction() {
        for cached_first in [true, false] {
            let (report, reported) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                DESTRUCTOR_PROBE.with(|probe| *probe.0.borrow_mut() = Some(report));
                if cached_first {
                    drop(Host::tokio());
                }
                std::thread::current().id()
            });
            let thread = thread.join().expect("the thread exits");
            let (owner, fallbacks) = reported
                .recv_timeout(TEST_TIMEOUT)
                .expect("the destructor reports")
                .expect("Host::tokio() does not panic in a destructor");
            assert_eq!(
                owner, thread,
                "the destructor's Host is owned by its thread"
            );
            if cached_first {
                assert_eq!(
                    fallbacks, 1,
                    "the cached Host was destroyed first, so the call fell back"
                );
            }
        }
    }

    #[test]
    fn host_tokio_uses_the_runtime_current_at_each_use() {
        let kept: RefCell<Option<Host>> = RefCell::new(None);
        for runtime_index in 0..2_u32 {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .expect("runtime starts");
            let local_set = tokio::task::LocalSet::new();
            local_set.block_on(&runtime, async {
                let fresh = Host::tokio();
                let host = kept.borrow_mut().get_or_insert_with(Host::tokio).clone();
                for (name, host) in [("kept", &host), ("fresh", &fresh)] {
                    let ran = Rc::new(Cell::new(None));
                    let task_ran = Rc::clone(&ran);
                    let id = tokio::runtime::Handle::current().id();
                    host.spawn_local(Box::pin(async move {
                        task_ran.set(Some(tokio::runtime::Handle::current().id()));
                    }));
                    tokio::task::yield_now().await;
                    assert_eq!(
                        ran.get(),
                        Some(id),
                        "{name} Host spawns on the current LocalSet under runtime {runtime_index}"
                    );

                    let start = host.now();
                    assert_eq!(start, tokio::time::Instant::now().into_std());
                    host.sleep_until(start + Duration::from_secs(10)).await;
                    assert_eq!(
                        host.now() - start,
                        Duration::from_secs(10),
                        "{name} Host sleeps on the paused clock of runtime {runtime_index}"
                    );
                }
            });
        }
    }

    /// A `Host` and a `TaskGroup` that share it, moved to one other thread
    /// and back so that their owner checks run there.
    struct Smuggled {
        host: Host,
        group: TaskGroup,
    }

    // SAFETY: `Host` and `TaskGroup` are `!Send` because they hold `Rc`s and
    // cells that one thread may access at a time. A `Smuggled` holds every
    // handle to its host's implementation and to its group's state, so the
    // thread that owns it is the only one that can reach them. The test moves
    // it to a thread it spawns, which hands it back from its closure, and
    // joins that thread before using the value again.
    unsafe impl Send for Smuggled {}

    #[test]
    fn owner_thread_checks_still_reject_other_threads() {
        let owner = std::thread::current().id();
        let host = Host::new(Rc::new(ManualHost::default()));
        let group = TaskGroup::with_host(host.clone());
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let travelling = Smuggled { host, group };
        let (back, other, cleanup_error, host_panic, group_panic) = std::thread::spawn(move || {
            // Binding the whole value first makes the closure capture all of
            // it, not its `!Send` fields one by one.
            let travelling = travelling;
            let Smuggled { host, mut group } = travelling;
            let other = std::thread::current().id();
            let context = WorkerContext::new(handle);
            let cleanup_error = match context.register_cleanup(|| {}) {
                Ok(_) => panic!("non-owner cleanup registration accepted"),
                Err(error) => error,
            };
            let host_panic = catch_unwind(AssertUnwindSafe(|| host.now()))
                .err()
                .map(|payload| describe(&*payload));
            let group_panic = catch_unwind(AssertUnwindSafe(|| group.spawn_local(|_| async {})))
                .err()
                .map(|payload| describe(&*payload));
            (
                Smuggled { host, group },
                other,
                cleanup_error,
                host_panic,
                group_panic,
            )
        })
        .join()
        .expect("the other thread reports");
        assert_eq!(
            cleanup_error,
            CleanupRegistrationError::WrongOwnerThread {
                expected: owner,
                actual: other,
            }
        );
        let mismatch = format!("left: {other:?}\n right: {owner:?}");
        for (what, panic) in [("Host", host_panic), ("TaskGroup", group_panic)] {
            let panic = panic.unwrap_or_else(|| panic!("{what} accepted another thread"));
            assert!(
                panic.contains("assertion `left == right` failed") && panic.contains(&mismatch),
                "{what}: {panic}"
            );
        }

        let Smuggled { host, mut group } = back;
        assert_eq!(host.owner_thread(), owner);
        group.close();
        drop(group);
        assert!(handle_shutdown(&mut driver));
    }

    fn handle_shutdown(driver: &mut WorkerDriver) -> bool {
        driver.handle().begin_shutdown();
        Pin::new(driver)
            .poll(&mut Context::from_waker(&noop_waker()))
            .is_ready()
    }

    /// A subscriber that keeps the fields of each Tokio `runtime.spawn` span.
    #[cfg(feature = "observability")]
    #[derive(Clone, Default)]
    struct SpawnSpans {
        spans: Arc<Mutex<Vec<String>>>,
        next_id: Arc<std::sync::atomic::AtomicU64>,
    }

    #[cfg(feature = "observability")]
    impl tracing::Subscriber for SpawnSpans {
        fn register_callsite(
            &self,
            _metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::sometimes()
        }

        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            if attributes.metadata().name() == "runtime.spawn" {
                let mut fields = Fields::default();
                attributes.record(&mut fields);
                self.spans
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(fields.0);
            }
            let id = self
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::span::Id::from_u64(id + 1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, _event: &tracing::Event<'_>) {}

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// The fields of the `runtime.spawn` spans that a `TaskGroup` on
    /// `Host::tokio()` creates for its tasks.
    #[cfg(feature = "observability")]
    fn tokio_task_group_spawn_spans() -> Vec<String> {
        let subscriber = SpawnSpans::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread test runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async {
            let _default = tracing::subscriber::set_default(subscriber.clone());
            let mut group = TaskGroup::new();
            group.spawn_local(|_| async {}).expect("task admitted");
            group
                .spawn_local_tracked(|_| async {})
                .expect("task admitted");
            group.join().await.expect("the tasks complete");
        });
        let spans = subscriber
            .spans
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        assert_eq!(spans.len(), 2, "one spawn span per task: {spans:?}");
        spans
    }

    #[cfg(feature = "observability")]
    fn span_field<'a>(fields: &'a str, name: &str) -> Option<&'a str> {
        fields
            .split(' ')
            .find_map(|word| word.split_once('=').filter(|(key, _)| *key == name))
            .map(|(_, value)| value)
    }

    #[cfg(feature = "observability")]
    #[test]
    #[cfg_attr(
        not(fbcode_build),
        ignore = "tokio emits `runtime.spawn` spans only when built with its `tracing` feature and `--cfg tokio_unstable`"
    )]
    fn tokio_spawn_span_location_stays_in_host_rs() {
        for fields in tokio_task_group_spawn_spans() {
            let file = span_field(&fields, "loc.file").expect("the span records its location");
            assert!(file.ends_with("runtime/src/host.rs"), "{fields}");
        }
    }

    #[cfg(feature = "observability")]
    #[test]
    #[cfg_attr(
        not(fbcode_build),
        ignore = "tokio emits `runtime.spawn` spans only when built with its `tracing` feature and `--cfg tokio_unstable`"
    )]
    fn tokio_spawn_span_reports_the_unboxed_task_size() {
        let size = crate::task_group::hosted_task_size().to_string();
        for fields in tokio_task_group_spawn_spans() {
            assert_eq!(
                span_field(&fields, "size.bytes"),
                Some(size.as_str()),
                "{fields}"
            );
            assert_eq!(span_field(&fields, "original_size.bytes"), None, "{fields}");
        }
    }

    /// Reference transcripts and sweep summaries, recorded on an earlier
    /// version of the runtime with only the `cfg(test)` hooks added. Both
    /// builds compare the lines that are not tracing callbacks (`t:`).
    /// `TRACES` hashes every scenario's whole transcript, tracing callbacks
    /// included, and the `TRACED` scenarios' transcripts here also list them.
    const GOLDENS: &[(&str, &str)] = &[
        (
            "abort/PanicDropAbortsItself",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            = drop aborts TaskId(0) -> true\n\
            close s1 failure in OA [A s1 OA]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop poll [- - -]\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "abort/PanicDropAbortsOther",
            "\
            = spawned Some(TaskId(0))\n\
            = spawned Some(TaskId(1))\n\
            poll other [A s1 OA]\n\
            poll task [A s2 OA]\n\
            drop task [A s2 OA]\n\
            drop other [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = drop aborts TaskId(0) -> true\n\
            close s2 failure in OA [A s2 OA]\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            = done TaskId(1) Err(TaskPanicked)\n\
            payload-drop poll [- - -]\n\
            close s3 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1 s2<- command failurex1\n\
            ",
        ),
        (
            "abort/PendingPollAbortsItself",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            = poll aborts TaskId(0) -> true\n\
            drop task [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "abort/ReadyDropAbortsItself",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            = drop aborts TaskId(0) -> true\n\
            close s1 success in OA [A s1 OA]\n\
            = done TaskId(0) Ok(())\n\
            close s2 success in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command successx1\n\
            ",
        ),
        (
            "abort/ReadyDropAbortsOther",
            "\
            = spawned Some(TaskId(0))\n\
            = spawned Some(TaskId(1))\n\
            poll other [A s1 OA]\n\
            poll task [A s2 OA]\n\
            drop task [A s2 OA]\n\
            drop other [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = drop aborts TaskId(0) -> true\n\
            close s2 success in OA [A s2 OA]\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            = done TaskId(1) Ok(())\n\
            close s3 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1 s2<- command successx1\n\
            ",
        ),
        (
            "cleanup/Hosted/Abort",
            "\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            = driver Pending\n\
            = cleanup finished -> false\n\
            close s2 success in OD [D - OD]\n\
            drop cleanup [- s1 -]\n\
            close s1 cancelled in - [- s1 -]\n\
            close s3 failure in OD [D - OD]\n\
            close s4 success in OD [D - OD]\n\
            close s5 shutdown in OD [D - OD]\n\
            = worker Closed\n\
            = reserved 0\n\
            OD: s5<- shutdown shutdownx1 s4<s5 cancellation successx1 s2<- shutdown successx1 s3<- shutdown failurex1\n\
            ",
        ),
        (
            "cleanup/Hosted/AbortReserved",
            "\
            = register reserved -> Ok(())\n\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            = driver Pending\n\
            = cleanup finished -> false\n\
            close s2 success in OD [D - OD]\n\
            drop cleanup [- s1 -]\n\
            close s1 cancelled in - [- s1 -]\n\
            close s3 failure in OD [D - OD]\n\
            drop reserved [D - OD]\n\
            close s4 success in OD [D - OD]\n\
            close s5 shutdown in OD [D - OD]\n\
            = worker Closed\n\
            = reserved 1\n\
            OD: s5<- shutdown shutdownx1 s4<s5 cancellation successx1 s2<- shutdown successx1 s3<- shutdown failurex1\n\
            ",
        ),
        (
            "cleanup/Hosted/Complete",
            "\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            drop cleanup [- s1 -]\n\
            close s1 success in - [- s1 -]\n\
            = driver Pending\n\
            = driver Pending\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s2 success in OD [D - OD]\n\
            close s3 success in OD [D - OD]\n\
            close s4 success in - [D - OD]\n\
            close s5 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = cleanup finished -> true\n\
            = worker Closed\n\
            = reserved 0\n\
            OD: s2<- shutdown successx1 s3<- shutdown successx1\n\
            ",
        ),
        (
            "cleanup/Hosted/DestructionPanic",
            "\
            = register scope -> Ok(())\n\
            = activate -> Ok(())\n\
            = driver Pending\n\
            poll scope [- s1 -]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll scope [- s1 -]\n\
            drop scope [- s1 -]\n\
            release left=0 [- s1 -]\n\
            close s1 success in - [- s1 -]\n\
            close s2 success in OD [D - OD]\n\
            close s3 success in OD [D - OD]\n\
            close s4 failure in - [D - OD]\n\
            close s5 shutdown in - [D - OD]\n\
            = driver poll panicked: payload drop\n\
            payload-drop drop [- - -]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = cleanup finished -> true\n\
            = worker Closed\n\
            = reserved 0\n\
            OD: s2<- shutdown successx1 s3<- shutdown successx1\n\
            ",
        ),
        (
            "cleanup/Hosted/PollPanic",
            "\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll cleanup [- s1 -]\n\
            drop cleanup [- s1 -]\n\
            close s1 success in - [- s1 -]\n\
            close s2 success in OD [D - OD]\n\
            close s3 success in OD [D - OD]\n\
            close s4 failure in - [D - OD]\n\
            close s5 shutdown in - [D - OD]\n\
            = driver poll panicked: payload poll\n\
            payload-drop poll [- - -]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = cleanup finished -> true\n\
            = worker Closed\n\
            = reserved 0\n\
            OD: s2<- shutdown successx1 s3<- shutdown successx1\n\
            ",
        ),
        (
            "cleanup/HostedWorker/Complete",
            "\
            = submit command -> Ok(())\n\
            = worker Closed\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            poll cleanup [A s2 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA]\n\
            close s1 success in OA [A s1 OA]\n\
            release left=0 [- s3 -]\n\
            close s3 success in - [- s3 -]\n\
            poll cleanup [A s2 OA]\n\
            drop cleanup [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            close s5 success in - [- s4 -]\n\
            close s6 success in - [- s4 -]\n\
            close s7 success in - [- s4 -]\n\
            close s8 shutdown in - [- s4 -]\n\
            close s4 success in - [- s4 -]\n\
            close s9 success in - [- - -]\n\
            OA: s10<- queue successx1 s1<s10 command successx1 s2<s1 command successx1\n\
            ",
        ),
        (
            "cleanup/Owner/Abort",
            "\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            poll cleanup [D - OD]\n\
            = driver Pending\n\
            poll cleanup [D - OD]\n\
            = driver Pending\n\
            = cleanup finished -> false\n\
            drop cleanup [D - OD]\n\
            close s1 success in OD [D - OD]\n\
            close s2 shutdown in OD [D - OD]\n\
            = worker Closed\n\
            = reserved 0\n\
            OD: s2<- shutdown shutdownx1 s1<s2 cancellation successx1\n\
            ",
        ),
        (
            "cleanup/Owner/AbortReserved",
            "\
            = register reserved -> Ok(())\n\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            poll cleanup [D - OD]\n\
            = driver Pending\n\
            poll cleanup [D - OD]\n\
            = driver Pending\n\
            = cleanup finished -> false\n\
            drop reserved [D - OD]\n\
            drop cleanup [D - OD]\n\
            close s1 success in OD [D - OD]\n\
            close s2 shutdown in OD [D - OD]\n\
            = worker Closed\n\
            = reserved 1\n\
            OD: s2<- shutdown shutdownx1 s1<s2 cancellation successx1\n\
            ",
        ),
        (
            "cleanup/Owner/Complete",
            "\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            poll cleanup [D - OD]\n\
            = driver Pending\n\
            poll cleanup [D - OD]\n\
            drop cleanup [D - OD]\n\
            = driver Pending\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s1 success in - [D - OD]\n\
            close s2 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = cleanup finished -> true\n\
            = worker Closed\n\
            = reserved 0\n\
            ",
        ),
        (
            "cleanup/Owner/DestructionPanic",
            "\
            = register scope -> Ok(())\n\
            = activate -> Ok(())\n\
            poll scope [D - OD]\n\
            = driver Pending\n\
            poll scope [D - OD]\n\
            drop scope [D - OD]\n\
            release left=0 [D - OD]\n\
            close s1 failure in OD [D - OD]\n\
            close s2 shutdown in OD [D - OD]\n\
            = driver poll panicked: payload drop\n\
            payload-drop drop [- - -]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = cleanup finished -> true\n\
            = worker Closed\n\
            = reserved 0\n\
            OD: s2<- shutdown shutdownx1 s1<s2 cancellation failurex1\n\
            ",
        ),
        (
            "cleanup/Owner/PollPanic",
            "\
            = register cleanup -> Ok(())\n\
            = activate -> Ok(())\n\
            poll cleanup [D - OD]\n\
            = driver Pending\n\
            poll cleanup [D - OD]\n\
            drop cleanup [D - OD]\n\
            close s1 failure in OD [D - OD]\n\
            close s2 shutdown in OD [D - OD]\n\
            = driver poll panicked: payload poll\n\
            payload-drop poll [- - -]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = cleanup finished -> true\n\
            = worker Closed\n\
            = reserved 0\n\
            OD: s2<- shutdown shutdownx1 s1<s2 cancellation failurex1\n\
            ",
        ),
        (
            "command/Hosted/Call/Closed",
            "\
            = begin_shutdown -> true\n\
            close s1 success in OD [D - OD]\n\
            close s2 success in OD [D - OD]\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            factory-drop command [A - OA]\n\
            close s5 shutdown in OA [A - OA]\n\
            close s6 shutdown in OA [A - OA]\n\
            = submit command -> Err(Closed)\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s5<- queue shutdownx1 s6<- shutdown shutdownx1\n\
            OD: s1<- shutdown successx1 s2<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Call/FactoryPanic",
            "\
            = submit before -> Ok(())\n\
            = submit command -> Ok, Pending\n\
            = submit after -> Ok(())\n\
            = submit after call -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory before [A s2 OA]\n\
            factory-drop before [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory command [A s4 OA]\n\
            factory-drop command [A s4 OA !]\n\
            woken command [A s4 OA !]\n\
            close s4 failure in OA [D - OD]\n\
            release left=3 [D - OD !]\n\
            factory-drop after [A - OA !]\n\
            close s5 failure in OA [D - OD !]\n\
            release left=2 [D - OD !]\n\
            factory-drop after call [A - OA !]\n\
            woken after call [A - OA !]\n\
            close s6 failure in OA [D - OD !]\n\
            release left=1 [D - OD !]\n\
            = driver poll panicked: payload factory\n\
            payload-drop factory [- - -]\n\
            poll before [A s2 OA]\n\
            drop before [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D s7 OD]\n\
            close s7 success in OD [D s7 OD]\n\
            = driver Pending\n\
            = driver Pending\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s8 success in OD [D - OD]\n\
            close s9 success in OD [D - OD]\n\
            close s10 success in - [D - OD]\n\
            close s11 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = call after call Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s5<- queue failurex1 s6<- queue failurex1 s2<s1 command successx1 s4<s3 command failurex1\n\
            OD: s7<- command successx1 s8<- shutdown successx1 s9<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Call/PendingShutdown",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 cancelled in OD [D s3 OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 success in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            OD: s3<- command cancelledx1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Call/PollPanic",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA !]\n\
            woken command [A s2 OA !]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop poll [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Call/QueuedShutdown",
            "\
            = submit command -> Ok, Pending\n\
            = begin_shutdown -> true\n\
            factory-drop command [A - OA]\n\
            woken command [A - OA]\n\
            close s1 cancelled in OA [D - OD]\n\
            release left=0 [D - OD]\n\
            close s2 success in OD [D - OD]\n\
            close s3 success in OD [D - OD]\n\
            close s4 success in - [D - OD]\n\
            close s5 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue cancelledx1\n\
            OD: s2<- shutdown successx1 s3<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Call/Ready",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 success in OD [D s3 OD]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s4 success in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 success in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call command Ready(Ok(()))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            OD: s3<- command successx1 s4<- shutdown successx1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Call/UnpolledShutdown",
            "\
            = submit command -> Ok, Pending\n\
            = submit closer -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory closer [A s4 OA]\n\
            = closer begins shutdown -> true\n\
            factory-drop closer [A s4 OA]\n\
            woken command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=1 [D s5 OD]\n\
            close s5 cancelled in OD [D s5 OD]\n\
            drop closer [A s4 OA]\n\
            close s4 cancelled in OA [A s4 OA]\n\
            release left=0 [D s6 OD]\n\
            close s6 cancelled in OD [D s6 OD]\n\
            close s7 failure in OD [D - OD]\n\
            close s8 success in OD [D - OD]\n\
            close s9 success in OA [D - OD]\n\
            close s10 shutdown in OA [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s2<s1 command cancelledx1 s4<s3 command cancelledx1 s10<s4 shutdown shutdownx1 s9<s10 cancellation successx1\n\
            OD: s5<- command cancelledx1 s6<- command cancelledx1 s7<- shutdown failurex1 s8<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/Closed",
            "\
            = begin_shutdown -> true\n\
            close s1 success in OD [D - OD]\n\
            close s2 success in OD [D - OD]\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            factory-drop command [A - OA]\n\
            close s5 shutdown in OA [A - OA]\n\
            close s6 shutdown in OA [A - OA]\n\
            = submit command -> Err(Closed)\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s5<- queue shutdownx1 s6<- shutdown shutdownx1\n\
            OD: s1<- shutdown successx1 s2<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/CommandClosePanic",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = close panics after 0\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop close [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/FactoryPanic",
            "\
            t:new t1 parent=- operation=queue #0 [A - OA]\n\
            = submit before -> Ok(())\n\
            t:new t2 parent=- operation=queue #1 [A - OA]\n\
            = submit command -> Ok(())\n\
            t:new t3 parent=- operation=queue #2 [A - OA]\n\
            = submit after -> Ok(())\n\
            t:new t4 parent=- operation=queue #3 [A - OA]\n\
            = submit after call -> Ok, Pending\n\
            t:enter t1 #4 [D - OD]\n\
            t:clone t1 #5 [A s1 OA]\n\
            t:new t5 parent=t1 operation=command #6 [A s1 OA]\n\
            t:try_close t1 #7 [A s1 OA]\n\
            t:exit t1 #8 [D - OD]\n\
            t:record t1 outcome=success #9 [D - OD]\n\
            close s1 success in OA [D - OD]\n\
            t:try_close t1 closes #10 [D - OD]\n\
            t:enter t5 #11 [D - OD]\n\
            factory before [A s2 OA]\n\
            factory-drop before [A s2 OA]\n\
            t:exit t5 #12 [D - OD]\n\
            t:clone t5 #13 [D - OD]\n\
            t:new t6 parent=- operation=command #14 [D - OD]\n\
            t:enter t6 #15 [D - OD]\n\
            t:exit t6 #16 [D - OD]\n\
            t:clone t6 #17 [D - OD]\n\
            t:enter t2 #18 [D - OD]\n\
            t:clone t2 #19 [A s3 OA]\n\
            t:new t7 parent=t2 operation=command #20 [A s3 OA]\n\
            t:try_close t2 #21 [A s3 OA]\n\
            t:exit t2 #22 [D - OD]\n\
            t:record t2 outcome=success #23 [D - OD]\n\
            close s3 success in OA [D - OD]\n\
            t:try_close t2 closes #24 [D - OD]\n\
            t:enter t7 #25 [D - OD]\n\
            factory command [A s4 OA]\n\
            factory-drop command [A s4 OA !]\n\
            t:exit t7 #26 [D - OD !]\n\
            t:record t7 outcome=failure #27 [D - OD]\n\
            close s4 failure in OA [D - OD]\n\
            t:try_close t7 closes #28 [D - OD]\n\
            release left=3 [D - OD !]\n\
            factory-drop after [A - OA !]\n\
            t:record t3 outcome=failure #29 [D - OD !]\n\
            close s5 failure in OA [D - OD !]\n\
            t:try_close t3 closes #30 [D - OD !]\n\
            release left=2 [D - OD !]\n\
            factory-drop after call [A - OA !]\n\
            woken after call [A - OA !]\n\
            t:record t4 outcome=failure #31 [D - OD !]\n\
            close s6 failure in OA [D - OD !]\n\
            t:try_close t4 closes #32 [D - OD !]\n\
            release left=1 [D - OD !]\n\
            = driver poll panicked: payload factory\n\
            payload-drop factory [- - -]\n\
            t:clone t6 #33 [D - OD]\n\
            t:enter t6 #34 [D - OD]\n\
            t:clone t5 #35 [D s7 OD]\n\
            t:enter t5 #36 [D s7 OD]\n\
            poll before [A s2 OA]\n\
            drop before [A s2 OA]\n\
            t:exit t5 #37 [D s7 OD]\n\
            t:try_close t5 #38 [D s7 OD]\n\
            t:clone t5 #39 [D s7 OD]\n\
            t:enter t5 #40 [D s7 OD]\n\
            t:try_close t5 #41 [A s2 OA]\n\
            t:exit t5 #42 [D s7 OD]\n\
            t:enter t5 #43 [D s7 OD]\n\
            t:record t5 outcome=success #44 [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            t:try_close t5 #45 [A s2 OA]\n\
            t:exit t5 #46 [D s7 OD]\n\
            t:try_close t5 closes #47 [D s7 OD]\n\
            release left=0 [D s7 OD]\n\
            t:exit t6 #48 [D - OD]\n\
            t:try_close t6 #49 [D - OD]\n\
            t:clone t6 #50 [D - OD]\n\
            t:enter t6 #51 [D - OD]\n\
            t:try_close t6 #52 [D s7 OD]\n\
            t:exit t6 #53 [D - OD]\n\
            t:enter t6 #54 [D - OD]\n\
            t:record t6 outcome=success #55 [D s7 OD]\n\
            close s7 success in OD [D s7 OD]\n\
            t:try_close t6 #56 [D s7 OD]\n\
            t:exit t6 #57 [D - OD]\n\
            t:try_close t6 closes #58 [D - OD]\n\
            = driver Pending\n\
            = driver Pending\n\
            = driver Pending\n\
            t:new t8 parent=- operation=shutdown #59 [C - -]\n\
            = begin_shutdown -> true\n\
            t:enter t8 #60 [D - OD]\n\
            t:clone t8 #61 [C s11 -]\n\
            t:new t9 parent=t8 operation=cancellation #62 [C s11 -]\n\
            t:try_close t8 #63 [C s11 -]\n\
            t:exit t8 #64 [D - OD]\n\
            t:new t10 parent=- operation=shutdown #65 [D - OD]\n\
            t:record t10 outcome=success #66 [D - OD]\n\
            close s8 success in OD [D - OD]\n\
            t:try_close t10 closes #67 [D - OD]\n\
            t:new t11 parent=- operation=shutdown #68 [D - OD]\n\
            t:record t11 outcome=success #69 [D - OD]\n\
            close s9 success in OD [D - OD]\n\
            t:try_close t11 closes #70 [D - OD]\n\
            t:record t9 outcome=success #71 [D - OD]\n\
            close s10 success in - [D - OD]\n\
            t:try_close t9 closes #72 [D - OD]\n\
            t:record t8 outcome=shutdown #73 [D - OD]\n\
            close s11 shutdown in - [D - OD]\n\
            t:try_close t8 closes #74 [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call after call Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s5<- queue failurex1 s6<- queue failurex1 s2<s1 command successx1 s4<s3 command failurex1\n\
            OD: s7<- command successx1 s8<- shutdown successx1 s9<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/GroupClosePanic",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = close panics after 1\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 success in OD [D s3 OD]\n\
            payload-drop close [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            OD: s3<- command successx1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/PendingShutdown",
            "\
            t:new t1 parent=- operation=queue #0 [A - OA]\n\
            = submit command -> Ok(())\n\
            t:enter t1 #1 [D - OD]\n\
            t:clone t1 #2 [A s1 OA]\n\
            t:new t2 parent=t1 operation=command #3 [A s1 OA]\n\
            t:try_close t1 #4 [A s1 OA]\n\
            t:exit t1 #5 [D - OD]\n\
            t:record t1 outcome=success #6 [D - OD]\n\
            close s1 success in OA [D - OD]\n\
            t:try_close t1 closes #7 [D - OD]\n\
            t:enter t2 #8 [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            t:exit t2 #9 [D - OD]\n\
            t:clone t2 #10 [D - OD]\n\
            t:new t3 parent=- operation=command #11 [D - OD]\n\
            t:enter t3 #12 [D - OD]\n\
            t:exit t3 #13 [D - OD]\n\
            t:clone t3 #14 [D - OD]\n\
            = driver Pending\n\
            t:clone t3 #15 [D - OD]\n\
            t:enter t3 #16 [D - OD]\n\
            t:clone t2 #17 [D s3 OD]\n\
            t:enter t2 #18 [D s3 OD]\n\
            poll command [A s2 OA]\n\
            t:exit t2 #19 [D s3 OD]\n\
            t:try_close t2 #20 [D s3 OD]\n\
            t:exit t3 #21 [D - OD]\n\
            t:try_close t3 #22 [D - OD]\n\
            = driver Pending\n\
            t:new t4 parent=- operation=shutdown #23 [C - -]\n\
            = begin_shutdown -> true\n\
            t:enter t4 #24 [D - OD]\n\
            t:clone t4 #25 [C s7 -]\n\
            t:new t5 parent=t4 operation=cancellation #26 [C s7 -]\n\
            t:try_close t4 #27 [C s7 -]\n\
            t:exit t4 #28 [D - OD]\n\
            t:new t6 parent=- operation=shutdown #29 [D - OD]\n\
            t:clone t3 #30 [D - OD]\n\
            t:enter t3 #31 [D - OD]\n\
            t:clone t3 #32 [D s3 OD]\n\
            t:enter t3 #33 [D s3 OD]\n\
            t:clone t2 #34 [D s3 OD]\n\
            t:enter t2 #35 [D s3 OD]\n\
            t:clone t2 #36 [A s2 OA]\n\
            t:enter t2 #37 [A s2 OA]\n\
            drop command [A s2 OA]\n\
            t:exit t2 #38 [A s2 OA]\n\
            t:try_close t2 #39 [A s2 OA]\n\
            t:try_close t2 #40 [A s2 OA]\n\
            t:record t2 outcome=cancelled #41 [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            t:try_close t2 #42 [A s2 OA]\n\
            t:exit t2 #43 [D s3 OD]\n\
            t:try_close t2 closes #44 [D s3 OD]\n\
            release left=0 [D s3 OD]\n\
            t:exit t3 #45 [D s3 OD]\n\
            t:try_close t3 #46 [D s3 OD]\n\
            t:try_close t3 #47 [D s3 OD]\n\
            t:record t3 outcome=cancelled #48 [D s3 OD]\n\
            close s3 cancelled in OD [D s3 OD]\n\
            t:try_close t3 #49 [D s3 OD]\n\
            t:exit t3 #50 [D - OD]\n\
            t:try_close t3 closes #51 [D - OD]\n\
            t:record t6 outcome=failure #52 [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            t:try_close t6 closes #53 [D - OD]\n\
            t:new t7 parent=- operation=shutdown #54 [D - OD]\n\
            t:record t7 outcome=success #55 [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            t:try_close t7 closes #56 [D - OD]\n\
            t:record t5 outcome=success #57 [D - OD]\n\
            close s6 success in - [D - OD]\n\
            t:try_close t5 closes #58 [D - OD]\n\
            t:record t4 outcome=shutdown #59 [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            t:try_close t4 closes #60 [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            OD: s3<- command cancelledx1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/PollPanic",
            "\
            t:new t1 parent=- operation=queue #0 [A - OA]\n\
            = submit command -> Ok(())\n\
            t:enter t1 #1 [D - OD]\n\
            t:clone t1 #2 [A s1 OA]\n\
            t:new t2 parent=t1 operation=command #3 [A s1 OA]\n\
            t:try_close t1 #4 [A s1 OA]\n\
            t:exit t1 #5 [D - OD]\n\
            t:record t1 outcome=success #6 [D - OD]\n\
            close s1 success in OA [D - OD]\n\
            t:try_close t1 closes #7 [D - OD]\n\
            t:enter t2 #8 [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            t:exit t2 #9 [D - OD]\n\
            t:clone t2 #10 [D - OD]\n\
            t:new t3 parent=- operation=command #11 [D - OD]\n\
            t:enter t3 #12 [D - OD]\n\
            t:exit t3 #13 [D - OD]\n\
            t:clone t3 #14 [D - OD]\n\
            = driver Pending\n\
            t:clone t3 #15 [D - OD]\n\
            t:enter t3 #16 [D - OD]\n\
            t:clone t2 #17 [D s3 OD]\n\
            t:enter t2 #18 [D s3 OD]\n\
            poll command [A s2 OA]\n\
            t:exit t2 #19 [D s3 OD]\n\
            t:try_close t2 #20 [D s3 OD]\n\
            t:exit t3 #21 [D - OD]\n\
            t:try_close t3 #22 [D - OD]\n\
            = driver Pending\n\
            = driver Pending\n\
            t:clone t3 #23 [D - OD]\n\
            t:enter t3 #24 [D - OD]\n\
            t:clone t2 #25 [D s3 OD]\n\
            t:enter t2 #26 [D s3 OD]\n\
            poll command [A s2 OA]\n\
            t:exit t2 #27 [D s3 OD !]\n\
            t:try_close t2 #28 [D s3 OD !]\n\
            t:clone t2 #29 [D s3 OD]\n\
            t:enter t2 #30 [D s3 OD]\n\
            t:clone t2 #31 [A s2 OA]\n\
            t:enter t2 #32 [A s2 OA]\n\
            drop command [A s2 OA]\n\
            t:exit t2 #33 [A s2 OA]\n\
            t:try_close t2 #34 [A s2 OA]\n\
            t:try_close t2 #35 [A s2 OA]\n\
            t:record t2 outcome=failure #36 [A s2 OA]\n\
            close s2 failure in OA [A s2 OA]\n\
            t:try_close t2 #37 [A s2 OA]\n\
            t:exit t2 #38 [D s3 OD]\n\
            t:try_close t2 closes #39 [D s3 OD]\n\
            t:exit t3 #40 [D - OD !]\n\
            t:try_close t3 #41 [D - OD !]\n\
            t:clone t3 #42 [D - OD]\n\
            t:enter t3 #43 [D - OD]\n\
            t:clone t3 #44 [D s3 OD]\n\
            t:enter t3 #45 [D s3 OD]\n\
            release left=0 [D s3 OD]\n\
            t:exit t3 #46 [D s3 OD]\n\
            t:try_close t3 #47 [D s3 OD]\n\
            t:try_close t3 #48 [D s3 OD]\n\
            t:record t3 outcome=failure #49 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            t:try_close t3 #50 [D s3 OD]\n\
            t:exit t3 #51 [D - OD]\n\
            t:try_close t3 closes #52 [D - OD]\n\
            payload-drop poll [D - OD]\n\
            t:new t4 parent=- operation=shutdown #53 [D - OD]\n\
            t:enter t4 #54 [D - OD]\n\
            t:clone t4 #55 [D s7 OD]\n\
            t:new t5 parent=t4 operation=cancellation #56 [D s7 OD]\n\
            t:try_close t4 #57 [D s7 OD]\n\
            t:exit t4 #58 [D - OD]\n\
            t:new t6 parent=- operation=shutdown #59 [D - OD]\n\
            t:record t6 outcome=failure #60 [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            t:try_close t6 closes #61 [D - OD]\n\
            t:new t7 parent=- operation=shutdown #62 [D - OD]\n\
            t:record t7 outcome=success #63 [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            t:try_close t7 closes #64 [D - OD]\n\
            t:record t5 outcome=failure #65 [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            t:try_close t5 closes #66 [D - OD]\n\
            t:record t4 outcome=shutdown #67 [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            t:try_close t4 closes #68 [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/QueuedShutdown",
            "\
            = submit command -> Ok(())\n\
            = begin_shutdown -> true\n\
            factory-drop command [A - OA]\n\
            close s1 cancelled in OA [D - OD]\n\
            release left=0 [D - OD]\n\
            close s2 success in OD [D - OD]\n\
            close s3 success in OD [D - OD]\n\
            close s4 success in - [D - OD]\n\
            close s5 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue cancelledx1\n\
            OD: s2<- shutdown successx1 s3<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/Ready",
            "\
            t:new t1 parent=- operation=queue #0 [A - OA]\n\
            = submit command -> Ok(())\n\
            t:enter t1 #1 [D - OD]\n\
            t:clone t1 #2 [A s1 OA]\n\
            t:new t2 parent=t1 operation=command #3 [A s1 OA]\n\
            t:try_close t1 #4 [A s1 OA]\n\
            t:exit t1 #5 [D - OD]\n\
            t:record t1 outcome=success #6 [D - OD]\n\
            close s1 success in OA [D - OD]\n\
            t:try_close t1 closes #7 [D - OD]\n\
            t:enter t2 #8 [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            t:exit t2 #9 [D - OD]\n\
            t:clone t2 #10 [D - OD]\n\
            t:new t3 parent=- operation=command #11 [D - OD]\n\
            t:enter t3 #12 [D - OD]\n\
            t:exit t3 #13 [D - OD]\n\
            t:clone t3 #14 [D - OD]\n\
            = driver Pending\n\
            t:clone t3 #15 [D - OD]\n\
            t:enter t3 #16 [D - OD]\n\
            t:clone t2 #17 [D s3 OD]\n\
            t:enter t2 #18 [D s3 OD]\n\
            poll command [A s2 OA]\n\
            t:exit t2 #19 [D s3 OD]\n\
            t:try_close t2 #20 [D s3 OD]\n\
            t:exit t3 #21 [D - OD]\n\
            t:try_close t3 #22 [D - OD]\n\
            = driver Pending\n\
            = driver Pending\n\
            t:clone t3 #23 [D - OD]\n\
            t:enter t3 #24 [D - OD]\n\
            t:clone t2 #25 [D s3 OD]\n\
            t:enter t2 #26 [D s3 OD]\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            t:exit t2 #27 [D s3 OD]\n\
            t:try_close t2 #28 [D s3 OD]\n\
            t:clone t2 #29 [D s3 OD]\n\
            t:enter t2 #30 [D s3 OD]\n\
            t:try_close t2 #31 [A s2 OA]\n\
            t:exit t2 #32 [D s3 OD]\n\
            t:enter t2 #33 [D s3 OD]\n\
            t:record t2 outcome=success #34 [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            t:try_close t2 #35 [A s2 OA]\n\
            t:exit t2 #36 [D s3 OD]\n\
            t:try_close t2 closes #37 [D s3 OD]\n\
            release left=0 [D s3 OD]\n\
            t:exit t3 #38 [D - OD]\n\
            t:try_close t3 #39 [D - OD]\n\
            t:clone t3 #40 [D - OD]\n\
            t:enter t3 #41 [D - OD]\n\
            t:try_close t3 #42 [D s3 OD]\n\
            t:exit t3 #43 [D - OD]\n\
            t:enter t3 #44 [D - OD]\n\
            t:record t3 outcome=success #45 [D s3 OD]\n\
            close s3 success in OD [D s3 OD]\n\
            t:try_close t3 #46 [D s3 OD]\n\
            t:exit t3 #47 [D - OD]\n\
            t:try_close t3 closes #48 [D - OD]\n\
            = driver Pending\n\
            t:new t4 parent=- operation=shutdown #49 [C - -]\n\
            = begin_shutdown -> true\n\
            t:enter t4 #50 [D - OD]\n\
            t:clone t4 #51 [C s7 -]\n\
            t:new t5 parent=t4 operation=cancellation #52 [C s7 -]\n\
            t:try_close t4 #53 [C s7 -]\n\
            t:exit t4 #54 [D - OD]\n\
            t:new t6 parent=- operation=shutdown #55 [D - OD]\n\
            t:record t6 outcome=success #56 [D - OD]\n\
            close s4 success in OD [D - OD]\n\
            t:try_close t6 closes #57 [D - OD]\n\
            t:new t7 parent=- operation=shutdown #58 [D - OD]\n\
            t:record t7 outcome=success #59 [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            t:try_close t7 closes #60 [D - OD]\n\
            t:record t5 outcome=success #61 [D - OD]\n\
            close s6 success in - [D - OD]\n\
            t:try_close t5 closes #62 [D - OD]\n\
            t:record t4 outcome=shutdown #63 [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            t:try_close t4 closes #64 [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            OD: s3<- command successx1 s4<- shutdown successx1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/ReleaseWakePanic",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = supervisor waits, outstanding=1\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            woken supervisor [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop wake [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "command/Hosted/Spawn/UnpolledShutdown",
            "\
            = submit command -> Ok(())\n\
            = submit closer -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory closer [A s4 OA]\n\
            = closer begins shutdown -> true\n\
            factory-drop closer [A s4 OA]\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=1 [D s5 OD]\n\
            close s5 cancelled in OD [D s5 OD]\n\
            drop closer [A s4 OA]\n\
            close s4 cancelled in OA [A s4 OA]\n\
            release left=0 [D s6 OD]\n\
            close s6 cancelled in OD [D s6 OD]\n\
            close s7 failure in OD [D - OD]\n\
            close s8 success in OD [D - OD]\n\
            close s9 success in OA [D - OD]\n\
            close s10 shutdown in OA [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s2<s1 command cancelledx1 s4<s3 command cancelledx1 s10<s4 shutdown shutdownx1 s9<s10 cancellation successx1\n\
            OD: s5<- command cancelledx1 s6<- command cancelledx1 s7<- shutdown failurex1 s8<- shutdown successx1\n\
            ",
        ),
        (
            "command/HostedWorker/Call/PendingShutdown",
            "\
            = submit command -> Ok\n\
            = call command Pending\n\
            = worker Closed\n\
            = call command Ready(Err(Cancelled))\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            factory-drop command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA]\n\
            woken command [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            release left=0 [- s2 -]\n\
            close s2 cancelled in - [- s2 -]\n\
            close s4 failure in - [- s3 -]\n\
            close s5 success in - [- s3 -]\n\
            close s6 success in - [- s3 -]\n\
            close s7 shutdown in - [- s3 -]\n\
            close s3 success in - [- s3 -]\n\
            close s8 success in - [- - -]\n\
            OA: s9<- queue successx1 s1<s9 command cancelledx1\n\
            ",
        ),
        (
            "command/HostedWorker/Call/PollPanic",
            "\
            = submit command -> Ok\n\
            = call command Pending\n\
            = worker Closed\n\
            = call command Ready(Err(Cancelled))\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            factory-drop command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA !]\n\
            woken command [A s1 OA !]\n\
            close s1 failure in OA [A s1 OA]\n\
            release left=0 [- s2 -]\n\
            close s2 failure in - [- s2 -]\n\
            payload-drop poll [- s3 -]\n\
            close s4 failure in - [- s3 -]\n\
            close s5 success in - [- s3 -]\n\
            close s6 failure in - [- s3 -]\n\
            close s7 shutdown in - [- s3 -]\n\
            close s3 failure in - [- s3 -]\n\
            close s8 failure in - [- - -]\n\
            OA: s9<- queue successx1 s1<s9 command failurex1\n\
            ",
        ),
        (
            "command/HostedWorker/Call/Ready",
            "\
            = submit command -> Ok\n\
            = call command Pending\n\
            = worker Closed\n\
            = call command Ready(Ok(()))\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            factory-drop command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA]\n\
            woken command [A s1 OA]\n\
            close s1 success in OA [A s1 OA]\n\
            release left=0 [- s2 -]\n\
            close s2 success in - [- s2 -]\n\
            close s4 success in - [- s3 -]\n\
            close s5 success in - [- s3 -]\n\
            close s6 success in - [- s3 -]\n\
            close s7 shutdown in - [- s3 -]\n\
            close s3 success in - [- s3 -]\n\
            close s8 success in - [- - -]\n\
            OA: s9<- queue successx1 s1<s9 command successx1\n\
            ",
        ),
        (
            "command/HostedWorker/Spawn/PendingShutdown",
            "\
            = submit command -> Ok(())\n\
            = worker Closed\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            factory-drop command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            release left=0 [- s2 -]\n\
            close s2 cancelled in - [- s2 -]\n\
            close s4 failure in - [- s3 -]\n\
            close s5 success in - [- s3 -]\n\
            close s6 success in - [- s3 -]\n\
            close s7 shutdown in - [- s3 -]\n\
            close s3 success in - [- s3 -]\n\
            close s8 success in - [- - -]\n\
            OA: s9<- queue successx1 s1<s9 command cancelledx1\n\
            ",
        ),
        (
            "command/HostedWorker/Spawn/PollPanic",
            "\
            = submit command -> Ok(())\n\
            = worker Closed\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            factory-drop command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA]\n\
            close s1 failure in OA [A s1 OA]\n\
            release left=0 [- s2 -]\n\
            close s2 failure in - [- s2 -]\n\
            payload-drop poll [- s3 -]\n\
            close s4 failure in - [- s3 -]\n\
            close s5 success in - [- s3 -]\n\
            close s6 failure in - [- s3 -]\n\
            close s7 shutdown in - [- s3 -]\n\
            close s3 failure in - [- s3 -]\n\
            close s8 failure in - [- - -]\n\
            OA: s9<- queue successx1 s1<s9 command failurex1\n\
            ",
        ),
        (
            "command/HostedWorker/Spawn/Ready",
            "\
            = submit command -> Ok(())\n\
            = worker Closed\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            factory-drop command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA]\n\
            close s1 success in OA [A s1 OA]\n\
            release left=0 [- s2 -]\n\
            close s2 success in - [- s2 -]\n\
            close s4 success in - [- s3 -]\n\
            close s5 success in - [- s3 -]\n\
            close s6 success in - [- s3 -]\n\
            close s7 shutdown in - [- s3 -]\n\
            close s3 success in - [- s3 -]\n\
            close s8 success in - [- - -]\n\
            OA: s9<- queue successx1 s1<s9 command successx1\n\
            ",
        ),
        (
            "command/Owner/Call/Closed",
            "\
            = begin_shutdown -> true\n\
            close s1 success in - [D - OD]\n\
            close s2 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            factory-drop command [A - OA]\n\
            close s3 shutdown in OA [A - OA]\n\
            close s4 shutdown in OA [A - OA]\n\
            = submit command -> Err(Closed)\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s3<- queue shutdownx1 s4<- shutdown shutdownx1\n\
            ",
        ),
        (
            "command/Owner/Call/FactoryPanic",
            "\
            = submit before -> Ok(())\n\
            = submit command -> Ok, Pending\n\
            = submit after -> Ok(())\n\
            = submit after call -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory before [A s2 OA]\n\
            factory-drop before [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory command [A s4 OA]\n\
            factory-drop command [A s4 OA !]\n\
            woken command [A s4 OA !]\n\
            close s4 failure in OA [D - OD]\n\
            release left=3 [D - OD !]\n\
            factory-drop after [A - OA !]\n\
            close s5 failure in OA [D - OD !]\n\
            release left=2 [D - OD !]\n\
            factory-drop after call [A - OA !]\n\
            woken after call [A - OA !]\n\
            close s6 failure in OA [D - OD !]\n\
            release left=1 [D - OD !]\n\
            = driver poll panicked: payload factory\n\
            payload-drop factory [- - -]\n\
            poll before [A s2 OA]\n\
            drop before [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s7 success in - [D - OD]\n\
            close s8 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = call after call Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s5<- queue failurex1 s6<- queue failurex1 s2<s1 command successx1 s4<s3 command failurex1\n\
            ",
        ),
        (
            "command/Owner/Call/PendingShutdown",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            ",
        ),
        (
            "command/Owner/Call/PollPanic",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA !]\n\
            woken command [A s2 OA !]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload poll\n\
            payload-drop poll [- - -]\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "command/Owner/Call/QueuedShutdown",
            "\
            = submit command -> Ok, Pending\n\
            = begin_shutdown -> true\n\
            factory-drop command [A - OA]\n\
            woken command [A - OA]\n\
            close s1 cancelled in OA [D - OD]\n\
            release left=0 [D - OD]\n\
            close s2 success in - [D - OD]\n\
            close s3 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue cancelledx1\n\
            ",
        ),
        (
            "command/Owner/Call/Ready",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = call command Ready(Ok(()))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            ",
        ),
        (
            "command/Owner/Call/UnpolledShutdown",
            "\
            = submit command -> Ok, Pending\n\
            = submit closer -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory closer [A s4 OA]\n\
            = closer begins shutdown -> true\n\
            factory-drop closer [A s4 OA]\n\
            woken command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=1 [D - OD]\n\
            drop closer [A s4 OA]\n\
            close s4 cancelled in OA [A s4 OA]\n\
            release left=0 [D - OD]\n\
            close s5 success in OA [D - OD]\n\
            close s6 shutdown in OA [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s2<s1 command cancelledx1 s4<s3 command cancelledx1 s6<s4 shutdown shutdownx1 s5<s6 cancellation successx1\n\
            ",
        ),
        (
            "command/Owner/Spawn/Closed",
            "\
            = begin_shutdown -> true\n\
            close s1 success in - [D - OD]\n\
            close s2 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            factory-drop command [A - OA]\n\
            close s3 shutdown in OA [A - OA]\n\
            close s4 shutdown in OA [A - OA]\n\
            = submit command -> Err(Closed)\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s3<- queue shutdownx1 s4<- shutdown shutdownx1\n\
            ",
        ),
        (
            "command/Owner/Spawn/CommandClosePanic",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = close panics after 0\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload close\n\
            payload-drop close [- - -]\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            ",
        ),
        (
            "command/Owner/Spawn/FactoryPanic",
            "\
            = submit before -> Ok(())\n\
            = submit command -> Ok(())\n\
            = submit after -> Ok(())\n\
            = submit after call -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory before [A s2 OA]\n\
            factory-drop before [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory command [A s4 OA]\n\
            factory-drop command [A s4 OA !]\n\
            close s4 failure in OA [D - OD]\n\
            release left=3 [D - OD !]\n\
            factory-drop after [A - OA !]\n\
            close s5 failure in OA [D - OD !]\n\
            release left=2 [D - OD !]\n\
            factory-drop after call [A - OA !]\n\
            woken after call [A - OA !]\n\
            close s6 failure in OA [D - OD !]\n\
            release left=1 [D - OD !]\n\
            = driver poll panicked: payload factory\n\
            payload-drop factory [- - -]\n\
            poll before [A s2 OA]\n\
            drop before [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s7 success in - [D - OD]\n\
            close s8 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = call after call Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s5<- queue failurex1 s6<- queue failurex1 s2<s1 command successx1 s4<s3 command failurex1\n\
            ",
        ),
        (
            "command/Owner/Spawn/PendingShutdown",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            ",
        ),
        (
            "command/Owner/Spawn/PollPanic",
            "\
            t:new t1 parent=- operation=queue #0 [A - OA]\n\
            = submit command -> Ok(())\n\
            t:enter t1 #1 [D - OD]\n\
            t:clone t1 #2 [A s1 OA]\n\
            t:new t2 parent=t1 operation=command #3 [A s1 OA]\n\
            t:try_close t1 #4 [A s1 OA]\n\
            t:exit t1 #5 [D - OD]\n\
            t:record t1 outcome=success #6 [D - OD]\n\
            close s1 success in OA [D - OD]\n\
            t:try_close t1 closes #7 [D - OD]\n\
            t:enter t2 #8 [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            t:exit t2 #9 [D - OD]\n\
            t:clone t2 #10 [D - OD]\n\
            t:clone t2 #11 [D - OD]\n\
            t:enter t2 #12 [D - OD]\n\
            poll command [A s2 OA]\n\
            t:exit t2 #13 [D - OD]\n\
            t:try_close t2 #14 [D - OD]\n\
            = driver Pending\n\
            t:clone t2 #15 [D - OD]\n\
            t:enter t2 #16 [D - OD]\n\
            poll command [A s2 OA]\n\
            t:exit t2 #17 [D - OD !]\n\
            t:try_close t2 #18 [D - OD !]\n\
            t:clone t2 #19 [D - OD]\n\
            t:enter t2 #20 [D - OD]\n\
            t:clone t2 #21 [A s2 OA]\n\
            t:enter t2 #22 [A s2 OA]\n\
            drop command [A s2 OA]\n\
            t:exit t2 #23 [A s2 OA]\n\
            t:try_close t2 #24 [A s2 OA]\n\
            t:try_close t2 #25 [A s2 OA]\n\
            t:record t2 outcome=failure #26 [A s2 OA]\n\
            close s2 failure in OA [A s2 OA]\n\
            t:try_close t2 #27 [A s2 OA]\n\
            t:exit t2 #28 [D - OD]\n\
            t:try_close t2 closes #29 [D - OD]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload poll\n\
            payload-drop poll [- - -]\n\
            t:new t3 parent=- operation=shutdown #30 [C - -]\n\
            = begin_shutdown -> true\n\
            t:enter t3 #31 [D - OD]\n\
            t:clone t3 #32 [C s4 -]\n\
            t:new t4 parent=t3 operation=cancellation #33 [C s4 -]\n\
            t:try_close t3 #34 [C s4 -]\n\
            t:exit t3 #35 [D - OD]\n\
            t:record t4 outcome=success #36 [D - OD]\n\
            close s3 success in - [D - OD]\n\
            t:try_close t4 closes #37 [D - OD]\n\
            t:record t3 outcome=shutdown #38 [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            t:try_close t3 closes #39 [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "command/Owner/Spawn/QueuedShutdown",
            "\
            = submit command -> Ok(())\n\
            = begin_shutdown -> true\n\
            factory-drop command [A - OA]\n\
            close s1 cancelled in OA [D - OD]\n\
            release left=0 [D - OD]\n\
            close s2 success in - [D - OD]\n\
            close s3 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue cancelledx1\n\
            ",
        ),
        (
            "command/Owner/Spawn/Ready",
            "\
            t:new t1 parent=- operation=queue #0 [A - OA]\n\
            = submit command -> Ok(())\n\
            t:enter t1 #1 [D - OD]\n\
            t:clone t1 #2 [A s1 OA]\n\
            t:new t2 parent=t1 operation=command #3 [A s1 OA]\n\
            t:try_close t1 #4 [A s1 OA]\n\
            t:exit t1 #5 [D - OD]\n\
            t:record t1 outcome=success #6 [D - OD]\n\
            close s1 success in OA [D - OD]\n\
            t:try_close t1 closes #7 [D - OD]\n\
            t:enter t2 #8 [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            t:exit t2 #9 [D - OD]\n\
            t:clone t2 #10 [D - OD]\n\
            t:clone t2 #11 [D - OD]\n\
            t:enter t2 #12 [D - OD]\n\
            poll command [A s2 OA]\n\
            t:exit t2 #13 [D - OD]\n\
            t:try_close t2 #14 [D - OD]\n\
            = driver Pending\n\
            t:clone t2 #15 [D - OD]\n\
            t:enter t2 #16 [D - OD]\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            t:exit t2 #17 [D - OD]\n\
            t:try_close t2 #18 [D - OD]\n\
            t:clone t2 #19 [D - OD]\n\
            t:enter t2 #20 [D - OD]\n\
            t:try_close t2 #21 [A s2 OA]\n\
            t:exit t2 #22 [D - OD]\n\
            t:enter t2 #23 [D - OD]\n\
            t:record t2 outcome=success #24 [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            t:try_close t2 #25 [A s2 OA]\n\
            t:exit t2 #26 [D - OD]\n\
            t:try_close t2 closes #27 [D - OD]\n\
            release left=0 [D - OD]\n\
            = driver Pending\n\
            t:new t3 parent=- operation=shutdown #28 [C - -]\n\
            = begin_shutdown -> true\n\
            t:enter t3 #29 [D - OD]\n\
            t:clone t3 #30 [C s4 -]\n\
            t:new t4 parent=t3 operation=cancellation #31 [C s4 -]\n\
            t:try_close t3 #32 [C s4 -]\n\
            t:exit t3 #33 [D - OD]\n\
            t:record t4 outcome=success #34 [D - OD]\n\
            close s3 success in - [D - OD]\n\
            t:try_close t4 closes #35 [D - OD]\n\
            t:record t3 outcome=shutdown #36 [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            t:try_close t3 closes #37 [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            ",
        ),
        (
            "command/Owner/Spawn/ReleaseWakePanic",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = supervisor waits, outstanding=1\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 success in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            woken supervisor [D - OD]\n\
            = driver poll panicked: payload wake\n\
            payload-drop wake [- - -]\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            ",
        ),
        (
            "command/Owner/Spawn/UnpolledShutdown",
            "\
            = submit command -> Ok(())\n\
            = submit closer -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory closer [A s4 OA]\n\
            = closer begins shutdown -> true\n\
            factory-drop closer [A s4 OA]\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=1 [D - OD]\n\
            drop closer [A s4 OA]\n\
            close s4 cancelled in OA [A s4 OA]\n\
            release left=0 [D - OD]\n\
            close s5 success in OA [D - OD]\n\
            close s6 shutdown in OA [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s2<s1 command cancelledx1 s4<s3 command cancelledx1 s6<s4 shutdown shutdownx1 s5<s6 cancellation successx1\n\
            ",
        ),
        (
            "context/Group/DropSetsOnAbort",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = abort TaskId(0) -> true\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "context/Group/DropSetsOnReady",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 success in OA [Y s1 OA]\n\
            = done TaskId(0) Ok(())\n\
            close s2 success in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command successx1\n\
            ",
        ),
        (
            "context/Group/DropSetsOnShutdown",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = abort_all\n\
            close s2 failure in - [C - -]\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            = close -> false\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "context/Group/PendingThenAbort",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [X s1 OA]\n\
            close s1 cancelled in OA [X s1 OA]\n\
            = abort TaskId(0) -> true\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "context/Group/PendingThenPanic",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            poll task [X s1 OA]\n\
            drop task [X s1 OA]\n\
            close s1 failure in OA [X s1 OA]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop poll [- - -]\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "context/Group/PendingThenReady",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            poll task [X s1 OA]\n\
            drop task [X s1 OA]\n\
            close s1 success in OA [X s1 OA]\n\
            = done TaskId(0) Ok(())\n\
            close s2 success in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command successx1\n\
            ",
        ),
        (
            "context/Group/PollSetsThenPanics",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 failure in OA [A s1 OA]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop poll [- - -]\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "context/Hosted/DropSetsOnAbort",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 cancelled in OD [D s3 OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 success in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            OD: s3<- command cancelledx1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "context/Hosted/DropSetsOnReady",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 success in OA [Y s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 success in OD [D s3 OD]\n\
            = driver Pending\n\
            = driver Pending\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s4 success in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 success in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            OD: s3<- command successx1 s4<- shutdown successx1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "context/Hosted/DropSetsOnShutdown",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 cancelled in OD [D s3 OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 success in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            OD: s3<- command cancelledx1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "context/Hosted/PendingThenAbort",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [X s2 OA]\n\
            close s2 cancelled in OA [X s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 cancelled in OD [D s3 OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 success in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            OD: s3<- command cancelledx1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "context/Hosted/PendingThenPanic",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll command [X s2 OA]\n\
            drop command [X s2 OA]\n\
            close s2 failure in OA [X s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop poll [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "context/Hosted/PendingThenReady",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll command [X s2 OA]\n\
            drop command [X s2 OA]\n\
            close s2 success in OA [X s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 success in OD [D s3 OD]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s4 success in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 success in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            OD: s3<- command successx1 s4<- shutdown successx1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "context/Hosted/PollSetsThenPanics",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop poll [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "context/Owner/DropSetsOnAbort",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            ",
        ),
        (
            "context/Owner/DropSetsOnReady",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 success in OA [Y s2 OA]\n\
            release left=0 [D - OD]\n\
            = driver Pending\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            ",
        ),
        (
            "context/Owner/DropSetsOnShutdown",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            close s2 cancelled in OA [A s2 OA]\n\
            release left=0 [D - OD]\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            ",
        ),
        (
            "context/Owner/PendingThenAbort",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [X s2 OA]\n\
            close s2 cancelled in OA [X s2 OA]\n\
            release left=0 [D - OD]\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            ",
        ),
        (
            "context/Owner/PendingThenPanic",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            poll command [X s2 OA]\n\
            drop command [X s2 OA]\n\
            close s2 failure in OA [X s2 OA]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload poll\n\
            payload-drop poll [- - -]\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "context/Owner/PendingThenReady",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            poll command [X s2 OA]\n\
            drop command [X s2 OA]\n\
            close s2 success in OA [X s2 OA]\n\
            release left=0 [D - OD]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command successx1\n\
            ",
        ),
        (
            "context/Owner/PollSetsThenPanics",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload poll\n\
            payload-drop poll [- - -]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "drop_panic/Group/Abort",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 failure in OA [C - - !]\n\
            = abort TaskId(0) -> true\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop drop [- - -]\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "drop_panic/Group/AbortAll",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 failure in OA [C - - !]\n\
            = abort_all\n\
            close s2 failure in - [C - -]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop drop [- - -]\n\
            = close -> false\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "drop_panic/Group/Ready",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 failure in OA [A s1 OA]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop drop [- - -]\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "drop_panic/Hosted/Call/Ready",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA !]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop drop [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "drop_panic/Hosted/Call/Shutdown",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA !]\n\
            close s2 failure in OA [D s3 OD !]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D - OD !]\n\
            close s4 failure in OD [D - OD]\n\
            payload-drop drop [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "drop_panic/Hosted/Spawn/Ready",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop drop [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "drop_panic/Hosted/Spawn/Shutdown",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            close s2 failure in OA [D s3 OD !]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D - OD !]\n\
            close s4 failure in OD [D - OD]\n\
            payload-drop drop [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in - [D - OD]\n\
            close s7 shutdown in - [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            OD: s3<- command failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "drop_panic/Owner/Call/Ready",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA !]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload drop\n\
            payload-drop drop [- - -]\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "drop_panic/Owner/Call/Shutdown",
            "\
            = submit command -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            woken command [A s2 OA !]\n\
            close s2 failure in OA [D - OD !]\n\
            release left=0 [D - OD]\n\
            close s3 failure in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver poll panicked: payload drop\n\
            payload-drop drop [- - -]\n\
            = call command Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "drop_panic/Owner/Spawn/Ready",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            close s2 failure in OA [A s2 OA]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload drop\n\
            payload-drop drop [- - -]\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "drop_panic/Owner/Spawn/Shutdown",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = begin_shutdown -> true\n\
            drop command [A s2 OA]\n\
            close s2 failure in OA [D - OD !]\n\
            release left=0 [D - OD]\n\
            close s3 failure in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver poll panicked: payload drop\n\
            payload-drop drop [- - -]\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "group/tracked/Abort",
            "\
            t:new t1 parent=- operation=command #0 [A - OA]\n\
            t:enter t1 #1 [A - OA]\n\
            t:exit t1 #2 [A - OA]\n\
            t:clone t1 #3 [A - OA]\n\
            = spawned Some(TaskId(0))\n\
            t:clone t1 #4 [P - OP]\n\
            t:enter t1 #5 [P - OP]\n\
            poll task [A s1 OA]\n\
            t:exit t1 #6 [P - OP]\n\
            t:try_close t1 #7 [P - OP]\n\
            t:clone t1 #8 [C - -]\n\
            t:enter t1 #9 [C - -]\n\
            t:clone t1 #10 [A s1 OA]\n\
            t:enter t1 #11 [A s1 OA]\n\
            drop task [A s1 OA]\n\
            t:exit t1 #12 [A s1 OA]\n\
            t:try_close t1 #13 [A s1 OA]\n\
            t:try_close t1 #14 [A s1 OA]\n\
            t:record t1 outcome=cancelled #15 [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            t:try_close t1 #16 [A s1 OA]\n\
            t:exit t1 #17 [C - -]\n\
            t:try_close t1 closes #18 [C - -]\n\
            = abort TaskId(0) -> true\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            t:new t2 parent=- operation=shutdown #19 [C - -]\n\
            t:record t2 outcome=failure #20 [C - -]\n\
            close s2 failure in - [C - -]\n\
            t:try_close t2 closes #21 [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "group/tracked/AbortAll",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = abort_all\n\
            close s2 failure in - [C - -]\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            = close -> false\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "group/tracked/Cancelled",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            close s2 success in - [C - -]\n\
            = cancel -> true\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 success in OA [A s1 OA]\n\
            close s3 success in - [C - -]\n\
            = done TaskId(0) Ok(())\n\
            = close -> false\n\
            OA: s1<- command successx1\n\
            ",
        ),
        (
            "group/tracked/Closed",
            "\
            close s1 success in - [C - -]\n\
            = close -> true\n\
            close s2 shutdown in OA [A - OA]\n\
            = spawn failed Closed\n\
            = close -> false\n\
            OA: s2<- shutdown shutdownx1\n\
            ",
        ),
        (
            "group/tracked/FactoryPanic",
            "\
            factory task [A s1 OA]\n\
            factory-drop task [A s1 OA !]\n\
            close s1 failure in OA [A - OA]\n\
            = spawn panicked: payload factory\n\
            payload-drop factory [- - -]\n\
            close s2 success in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "group/tracked/PollPanic",
            "\
            t:new t1 parent=- operation=command #0 [A - OA]\n\
            t:enter t1 #1 [A - OA]\n\
            t:exit t1 #2 [A - OA]\n\
            t:clone t1 #3 [A - OA]\n\
            = spawned Some(TaskId(0))\n\
            t:clone t1 #4 [P - OP]\n\
            t:enter t1 #5 [P - OP]\n\
            poll task [A s1 OA]\n\
            t:exit t1 #6 [P - OP]\n\
            t:try_close t1 #7 [P - OP]\n\
            t:clone t1 #8 [P - OP]\n\
            t:enter t1 #9 [P - OP]\n\
            poll task [A s1 OA]\n\
            t:exit t1 #10 [P - OP !]\n\
            t:try_close t1 #11 [P - OP !]\n\
            t:clone t1 #12 [P - OP]\n\
            t:enter t1 #13 [P - OP]\n\
            t:clone t1 #14 [A s1 OA]\n\
            t:enter t1 #15 [A s1 OA]\n\
            drop task [A s1 OA]\n\
            t:exit t1 #16 [A s1 OA]\n\
            t:try_close t1 #17 [A s1 OA]\n\
            t:try_close t1 #18 [A s1 OA]\n\
            t:record t1 outcome=failure #19 [A s1 OA]\n\
            close s1 failure in OA [A s1 OA]\n\
            t:try_close t1 #20 [A s1 OA]\n\
            t:exit t1 #21 [P - OP]\n\
            t:try_close t1 closes #22 [P - OP]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop poll [- - -]\n\
            t:new t2 parent=- operation=shutdown #23 [C - -]\n\
            t:record t2 outcome=failure #24 [C - -]\n\
            close s2 failure in - [C - -]\n\
            t:try_close t2 closes #25 [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "group/tracked/Ready",
            "\
            t:new t1 parent=- operation=command #0 [A - OA]\n\
            t:enter t1 #1 [A - OA]\n\
            t:exit t1 #2 [A - OA]\n\
            t:clone t1 #3 [A - OA]\n\
            = spawned Some(TaskId(0))\n\
            t:clone t1 #4 [P - OP]\n\
            t:enter t1 #5 [P - OP]\n\
            poll task [A s1 OA]\n\
            t:exit t1 #6 [P - OP]\n\
            t:try_close t1 #7 [P - OP]\n\
            t:clone t1 #8 [P - OP]\n\
            t:enter t1 #9 [P - OP]\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            t:exit t1 #10 [P - OP]\n\
            t:try_close t1 #11 [P - OP]\n\
            t:clone t1 #12 [P - OP]\n\
            t:enter t1 #13 [P - OP]\n\
            t:try_close t1 #14 [A s1 OA]\n\
            t:exit t1 #15 [P - OP]\n\
            t:enter t1 #16 [P - OP]\n\
            t:record t1 outcome=success #17 [A s1 OA]\n\
            close s1 success in OA [A s1 OA]\n\
            t:try_close t1 #18 [A s1 OA]\n\
            t:exit t1 #19 [P - OP]\n\
            t:try_close t1 closes #20 [P - OP]\n\
            = done TaskId(0) Ok(())\n\
            t:new t2 parent=- operation=shutdown #21 [C - -]\n\
            t:record t2 outcome=success #22 [C - -]\n\
            close s2 success in - [C - -]\n\
            t:try_close t2 closes #23 [C - -]\n\
            = close -> true\n\
            OA: s1<- command successx1\n\
            ",
        ),
        (
            "group/tracked/UnpolledAbort",
            "\
            = spawned Some(TaskId(0))\n\
            drop task [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = abort TaskId(0) -> true\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "group/untracked/AbortAll",
            "\
            = spawned None\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = abort_all\n\
            close s2 failure in - [C - -]\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            = close -> false\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "group/untracked/Cancelled",
            "\
            = spawned None\n\
            poll task [A s1 OA]\n\
            close s2 success in - [C - -]\n\
            = cancel -> true\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 success in OA [A s1 OA]\n\
            close s3 success in - [C - -]\n\
            = done TaskId(0) Ok(())\n\
            = close -> false\n\
            OA: s1<- command successx1\n\
            ",
        ),
        (
            "group/untracked/Closed",
            "\
            close s1 success in - [C - -]\n\
            = close -> true\n\
            close s2 shutdown in OA [A - OA]\n\
            = spawn failed Closed\n\
            = close -> false\n\
            OA: s2<- shutdown shutdownx1\n\
            ",
        ),
        (
            "group/untracked/FactoryPanic",
            "\
            factory task [A s1 OA]\n\
            factory-drop task [A s1 OA !]\n\
            close s1 failure in OA [A - OA]\n\
            = spawn panicked: payload factory\n\
            payload-drop factory [- - -]\n\
            close s2 success in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "group/untracked/PollPanic",
            "\
            = spawned None\n\
            poll task [A s1 OA]\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 failure in OA [A s1 OA]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop poll [- - -]\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command failurex1\n\
            ",
        ),
        (
            "group/untracked/Ready",
            "\
            = spawned None\n\
            poll task [A s1 OA]\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            close s1 success in OA [A s1 OA]\n\
            = done TaskId(0) Ok(())\n\
            close s2 success in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command successx1\n\
            ",
        ),
        (
            "group/untracked/UnpolledAbort",
            "\
            = spawned None\n\
            drop task [A s1 OA]\n\
            close s1 cancelled in OA [A s1 OA]\n\
            = abort_all\n\
            close s2 failure in - [C - -]\n\
            = done TaskId(0) Err(TaskCancelled)\n\
            = close -> false\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "poll_drop_panic/Group",
            "\
            = spawned Some(TaskId(0))\n\
            poll task [A s1 OA]\n\
            poll task [A s1 OA]\n\
            drop task [A s1 OA]\n\
            payload-drop poll [P - OP !]\n\
            close s1 cancelled in OA [P - OP]\n\
            = done TaskId(0) Err(TaskPanicked)\n\
            payload-drop drop [- - -]\n\
            close s2 failure in - [C - -]\n\
            = close -> true\n\
            OA: s1<- command cancelledx1\n\
            ",
        ),
        (
            "poll_drop_panic/Hosted",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            payload-drop poll [D s3 OD !]\n\
            close s2 cancelled in OA [D s3 OD]\n\
            release left=0 [D s3 OD]\n\
            close s3 failure in OD [D s3 OD]\n\
            payload-drop drop [D - OD]\n\
            close s4 failure in OD [D - OD]\n\
            close s5 success in OD [D - OD]\n\
            close s6 failure in OD [D - OD]\n\
            close s7 shutdown in OD [D - OD]\n\
            = driver poll panicked: TaskPanicked\n\
            = begin_shutdown -> false\n\
            = driver Ready(())\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command cancelledx1\n\
            OD: s3<- command failurex1 s7<- shutdown shutdownx1 s6<s7 cancellation failurex1 s4<- shutdown failurex1 s5<- shutdown successx1\n\
            ",
        ),
        (
            "poll_drop_panic/HostedWorker",
            "\
            = submit command -> Ok(())\n\
            = worker Closed\n\
            = worker thread:\n\
            factory command [A s1 OA]\n\
            factory-drop command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            poll command [A s1 OA]\n\
            drop command [A s1 OA]\n\
            payload-drop poll [- s2 - !]\n\
            close s1 cancelled in OA [- s2 -]\n\
            release left=0 [- s2 -]\n\
            close s2 failure in - [- s2 -]\n\
            payload-drop drop [- s3 -]\n\
            close s4 failure in - [- s3 -]\n\
            close s5 success in - [- s3 -]\n\
            close s6 failure in - [- s3 -]\n\
            close s7 shutdown in - [- s3 -]\n\
            close s3 failure in - [- s3 -]\n\
            close s8 failure in - [- - -]\n\
            OA: s9<- queue successx1 s1<s9 command cancelledx1\n\
            ",
        ),
        (
            "poll_drop_panic/Owner",
            "\
            = submit command -> Ok(())\n\
            close s1 success in OA [D - OD]\n\
            factory command [A s2 OA]\n\
            factory-drop command [A s2 OA]\n\
            poll command [A s2 OA]\n\
            = driver Pending\n\
            poll command [A s2 OA]\n\
            drop command [A s2 OA]\n\
            payload-drop poll [D - OD !]\n\
            close s2 failure in OA [D - OD !]\n\
            release left=0 [D - OD !]\n\
            = driver poll panicked: payload drop\n\
            payload-drop drop [- - -]\n\
            = begin_shutdown -> true\n\
            close s3 success in - [D - OD]\n\
            close s4 shutdown in - [D - OD]\n\
            = driver Ready(())\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s2<s1 command failurex1\n\
            ",
        ),
        (
            "sweep/Hosted/Cleanup",
            "\
            baseline: e8f74060\n\
            #0 new t1 parent=- operation=command: 119533dd activate panicked: payload subscriber\n\
            #1 enter t1: 5993043f activate panicked: payload subscriber\n\
            #2 exit t1: 547fa305 activate panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: 53217d43 activate panicked: payload subscriber\n\
            #4 clone t1: ac87d9b1 driver poll panicked: TaskPanicked\n\
            #5 enter t1: 6b5dac6f driver poll panicked: TaskPanicked\n\
            #6 exit t1: 36662226 driver poll panicked: TaskPanicked\n\
            #7 try_close t1: c2712fe7 driver poll panicked: TaskPanicked\n\
            #8 clone t1: 6e45393a driver poll panicked: TaskPanicked\n\
            #9 enter t1: 5362bc7c driver poll panicked: TaskPanicked\n\
            #10 exit t1: 360fb3f0 driver poll panicked: TaskPanicked\n\
            #11 try_close t1: 96104426 driver poll panicked: TaskPanicked\n\
            #12 clone t1: 8d7b715e driver poll panicked: TaskPanicked\n\
            #13 enter t1: 6c443ff3 driver poll panicked: TaskPanicked\n\
            #14 try_close t1: 70344dc6 driver poll panicked: TaskPanicked\n\
            #15 exit t1: 427bd486 driver poll panicked: TaskPanicked\n\
            #16 enter t1: 28f2d010 driver poll panicked: TaskPanicked\n\
            #17 record t1 outcome=success: e24b3384 driver poll panicked: TaskPanicked\n\
            #18 try_close t1: 1cc3d9a2 driver poll panicked: TaskPanicked\n\
            #19 exit t1: d864cb8d driver poll panicked: TaskPanicked\n\
            #20 try_close t1 closes: 300b616c driver poll panicked: TaskPanicked\n\
            #21 new t2 parent=- operation=shutdown: 9b94704b begin_shutdown panicked: payload subscriber\n\
            #22 enter t2: 479ea2b5 driver poll panicked: payload subscriber\n\
            #23 clone t2: 97749101 driver poll panicked: payload subscriber\n\
            #24 new t3 parent=t2 operation=cancellation: 0c3abac5 driver poll panicked: payload subscriber\n\
            #25 try_close t2: 43efa55d driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #26 exit t2: e78889ef driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #27 new t4 parent=- operation=shutdown: 3eb1bfb0 driver poll panicked: payload subscriber\n\
            #28 record t4 outcome=success: 6241253d driver poll panicked: payload subscriber\n\
            #29 try_close t4 closes: 065425d8 driver poll panicked: payload subscriber\n\
            #30 new t5 parent=- operation=shutdown: f0f2973b driver poll panicked: payload subscriber\n\
            #31 record t5 outcome=success: 9b06e654 driver poll panicked: payload subscriber\n\
            #32 try_close t5 closes: 3191454b driver poll panicked: payload subscriber\n\
            #33 record t3 outcome=success: 6a143ef7 driver poll panicked: payload subscriber\n\
            #34 try_close t3 closes: 3ead5419 driver poll panicked: payload subscriber\n\
            #35 record t2 outcome=shutdown: 39bad1f3 driver poll panicked: payload subscriber\n\
            #36 try_close t2 closes: 3ead5419 driver poll panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/Hosted/CleanupPollPanic",
            "\
            baseline: d3d04d5e\n\
            #0 new t1 parent=- operation=command: 119533dd activate panicked: payload subscriber\n\
            #1 enter t1: 5993043f activate panicked: payload subscriber\n\
            #2 exit t1: 547fa305 activate panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: 53217d43 activate panicked: payload subscriber\n\
            #4 clone t1: ac87d9b1 driver poll panicked: TaskPanicked\n\
            #5 enter t1: 6b5dac6f driver poll panicked: TaskPanicked\n\
            #6 exit t1: 36662226 driver poll panicked: TaskPanicked\n\
            #7 try_close t1: c2712fe7 driver poll panicked: TaskPanicked\n\
            #8 clone t1: 6e45393a driver poll panicked: TaskPanicked\n\
            #9 enter t1: 5362bc7c driver poll panicked: TaskPanicked\n\
            #10 clone t1: bfd7c3b6 driver poll panicked: payload poll\n\
            #11 new t2 parent=t1 operation=shutdown: ef65226e driver poll panicked: payload poll\n\
            #12 try_close t1: 1c4f8f2a driver poll panicked: payload poll\n\
            #13 exit t1: 6103daee driver poll panicked: payload poll\n\
            #14 try_close t1: b9a7011b driver poll panicked: payload poll\n\
            #15 clone t1: 2032c170 driver poll panicked: payload poll\n\
            #16 enter t1: 7a483668 driver poll panicked: payload poll\n\
            #17 try_close t1: 1d3c434c driver poll panicked: payload poll\n\
            #18 exit t1: 91721d55 driver poll panicked: payload poll\n\
            #19 enter t1: 92ef46eb driver poll panicked: payload poll\n\
            #20 record t1 outcome=success: 597efb7b driver poll panicked: payload poll\n\
            #21 try_close t1: 1cec1c95 driver poll panicked: payload poll\n\
            #22 exit t1: 8feefc0e driver poll panicked: payload poll\n\
            #23 try_close t1 closes: d20527e1 driver poll panicked: payload poll\n\
            #24 enter t2: 6c001c7d driver poll panicked: payload subscriber\n\
            #25 clone t2: a7326ecb driver poll panicked: payload subscriber\n\
            #26 new t3 parent=t2 operation=cancellation: 80a23b4a driver poll panicked: payload subscriber\n\
            #27 try_close t2: 48c3b28c driver poll panicked: payload subscriber\n\
            #28 exit t2: 94aa5423 driver poll panicked: payload subscriber\n\
            #29 new t4 parent=- operation=shutdown: c0343935 driver poll panicked: payload subscriber; driver poll panicked: payload poll\n\
            #30 record t4 outcome=success: c10c188d driver poll panicked: payload subscriber; driver poll panicked: payload poll\n\
            #31 try_close t4 closes: f3e9c95c driver poll panicked: payload subscriber; driver poll panicked: payload poll\n\
            #32 new t5 parent=- operation=shutdown: 8a593f06 driver poll panicked: payload subscriber; driver poll panicked: payload poll\n\
            #33 record t5 outcome=success: ed73e9e4 driver poll panicked: payload subscriber; driver poll panicked: payload poll\n\
            #34 try_close t5 closes: cc824f15 driver poll panicked: payload subscriber; driver poll panicked: payload poll\n\
            #35 record t3 outcome=failure: ad34620b driver poll panicked: payload poll\n\
            #36 try_close t3 closes: 1c811987 driver poll panicked: payload poll\n\
            #37 record t2 outcome=shutdown: 31dc9c82 driver poll panicked: payload poll\n\
            #38 try_close t2 closes: 50472ead driver poll panicked: payload poll\n\
            ",
        ),
        (
            "sweep/Hosted/DropPanic",
            "\
            baseline: e7deedce\n\
            #0 new t1 parent=- operation=queue: 691acaf1 submit panicked: payload subscriber\n\
            #1 enter t1: 36920424 driver poll panicked: payload subscriber\n\
            #2 clone t1: ac4a13f2 driver poll panicked: payload subscriber\n\
            #3 new t2 parent=t1 operation=command: f85da353 driver poll panicked: payload subscriber\n\
            #4 try_close t1: ca5c3ddb driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #5 exit t1: b5790a6e driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #6 record t1 outcome=success: 3ca45142 driver poll panicked: payload subscriber\n\
            #7 try_close t1 closes: 0c2f49e0 driver poll panicked: payload subscriber\n\
            #8 enter t2: 6690b477 driver poll panicked: payload subscriber\n\
            #9 exit t2: 391baac5 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #10 clone t2: left out; aborts after d0c2968b, as a child process shows\n\
            #11 new t3 parent=- operation=command: left out; aborts after f0312889, as a child process shows\n\
            #12 enter t3: left out; aborts after fe76359e, as a child process shows\n\
            #13 exit t3: d48f6653 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #14 clone t3: left out; aborts after eef1c3cc, as a child process shows\n\
            #15 clone t3: 1736f4bb driver poll panicked: TaskPanicked\n\
            #16 enter t3: abfb97f1 driver poll panicked: TaskPanicked\n\
            #17 clone t2: cb661180 driver poll panicked: TaskPanicked\n\
            #18 enter t2: eb9fb624 driver poll panicked: TaskPanicked\n\
            #19 exit t2: left out, runs while unwinding; aborts after 6033fac2, as a child process shows\n\
            #20 try_close t2: left out, runs while unwinding; aborts after 1af7d8f3, as a child process shows\n\
            #21 clone t2: aff88f09 driver poll panicked: TaskPanicked\n\
            #22 enter t2: 648bce24 driver poll panicked: TaskPanicked\n\
            #23 try_close t2: 95861002 driver poll panicked: TaskPanicked\n\
            #24 record t2 outcome=failure: a816414b driver poll panicked: TaskPanicked\n\
            #25 try_close t2: 60e084a7 driver poll panicked: TaskPanicked\n\
            #26 exit t2: b9ad5e0f driver poll panicked: TaskPanicked\n\
            #27 try_close t2 closes: 18d1e549 driver poll panicked: TaskPanicked\n\
            #28 exit t3: left out, runs while unwinding; aborts after c23421e7, as a child process shows\n\
            #29 try_close t3: left out, runs while unwinding; aborts after 03d4603d, as a child process shows\n\
            #30 clone t3: 405af8d6 driver poll panicked: TaskPanicked\n\
            #31 enter t3: a1305917 driver poll panicked: TaskPanicked\n\
            #32 clone t3: ea5aa6ad driver poll panicked: TaskPanicked\n\
            #33 enter t3: 54444a5f driver poll panicked: TaskPanicked\n\
            #34 exit t3: 30e60ba4 driver poll panicked: TaskPanicked\n\
            #35 try_close t3: 8eb5a45b driver poll panicked: TaskPanicked\n\
            #36 try_close t3: 793d8616 driver poll panicked: TaskPanicked\n\
            #37 record t3 outcome=failure: dec24185 driver poll panicked: TaskPanicked\n\
            #38 try_close t3: 31864b83 driver poll panicked: TaskPanicked\n\
            #39 exit t3: acb03477 driver poll panicked: TaskPanicked\n\
            #40 try_close t3 closes: 38791e70 driver poll panicked: TaskPanicked\n\
            #41 new t4 parent=- operation=shutdown: 0a5f6c78 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #42 enter t4: f98e3996 driver poll panicked: TaskPanicked\n\
            #43 clone t4: 86ffdf01 driver poll panicked: TaskPanicked\n\
            #44 new t5 parent=t4 operation=cancellation: 1e1e5490 driver poll panicked: TaskPanicked\n\
            #45 try_close t4: ef98ee12 driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #46 exit t4: dc5bebc7 driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #47 new t6 parent=- operation=shutdown: 58bb2784 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #48 record t6 outcome=failure: 08b3d5ab driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #49 try_close t6 closes: ee799e23 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #50 new t7 parent=- operation=shutdown: cec5b05c driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #51 record t7 outcome=success: 77673567 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #52 try_close t7 closes: 9bf71291 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #53 record t5 outcome=failure: fc2fc2d7 driver poll panicked: TaskPanicked\n\
            #54 try_close t5 closes: d758144d driver poll panicked: TaskPanicked\n\
            #55 record t4 outcome=shutdown: 77cd75a9 driver poll panicked: TaskPanicked\n\
            #56 try_close t4 closes: 900ee7b7 driver poll panicked: TaskPanicked\n\
            ",
        ),
        (
            "sweep/Hosted/DropPanicShutdown",
            "\
            baseline: 1ed65bb4\n\
            #0 new t1 parent=- operation=queue: 691acaf1 submit panicked: payload subscriber\n\
            #1 enter t1: 36920424 driver poll panicked: payload subscriber\n\
            #2 clone t1: ac4a13f2 driver poll panicked: payload subscriber\n\
            #3 new t2 parent=t1 operation=command: f85da353 driver poll panicked: payload subscriber\n\
            #4 try_close t1: ca5c3ddb driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #5 exit t1: b5790a6e driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #6 record t1 outcome=success: 3ca45142 driver poll panicked: payload subscriber\n\
            #7 try_close t1 closes: 0c2f49e0 driver poll panicked: payload subscriber\n\
            #8 enter t2: 6690b477 driver poll panicked: payload subscriber\n\
            #9 exit t2: 391baac5 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #10 clone t2: left out; aborts after d0c2968b, as a child process shows\n\
            #11 new t3 parent=- operation=command: left out; aborts after f0312889, as a child process shows\n\
            #12 enter t3: left out; aborts after fe76359e, as a child process shows\n\
            #13 exit t3: d48f6653 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #14 clone t3: left out; aborts after eef1c3cc, as a child process shows\n\
            #15 clone t3: 1736f4bb driver poll panicked: TaskPanicked\n\
            #16 enter t3: abfb97f1 driver poll panicked: TaskPanicked\n\
            #17 clone t2: cb661180 driver poll panicked: TaskPanicked\n\
            #18 enter t2: eb9fb624 driver poll panicked: TaskPanicked\n\
            #19 exit t2: 258b9ff3 driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #20 try_close t2: 7a08381d driver poll panicked: TaskPanicked\n\
            #21 exit t3: fefa9bf7 driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #22 try_close t3: 524809f8 driver poll panicked: TaskPanicked\n\
            #23 new t4 parent=- operation=shutdown: 68adc1c6 begin_shutdown panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #24 enter t4: c13fb041 driver poll panicked: payload subscriber\n\
            #25 clone t4: 569a38f1 driver poll panicked: payload subscriber\n\
            #26 new t5 parent=t4 operation=cancellation: 5c3ee29d driver poll panicked: payload subscriber\n\
            #27 try_close t4: 29780811 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #28 exit t4: 76dd3182 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #29 new t6 parent=- operation=shutdown: 60e1cfdc driver poll panicked: payload subscriber; driver drop panicked: task group must join every admitted task before drop\n\
            #30 clone t3: left out; aborts after 168f8053, as a child process shows\n\
            #31 enter t3: left out; aborts after 3c1a4469, as a child process shows\n\
            #32 clone t3: left out; aborts after 5ab9a843, as a child process shows\n\
            #33 enter t3: left out; aborts after ed7a286e, as a child process shows\n\
            #34 clone t2: left out; aborts after d08e515f, as a child process shows\n\
            #35 enter t2: left out; aborts after da162cd9, as a child process shows\n\
            #36 clone t2: left out; aborts after 189fb9cc, as a child process shows\n\
            #37 enter t2: left out; aborts after 690e3a98, as a child process shows\n\
            #38 exit t2: left out, runs while unwinding; aborts after c2e03c29, as a child process shows\n\
            #39 try_close t2: left out, runs while unwinding; aborts after 496e4f6c, as a child process shows\n\
            #40 try_close t2: left out, runs while unwinding; aborts after cd34c69d, as a child process shows\n\
            #41 exit t2: left out, runs while unwinding; aborts after 51f14308, as a child process shows\n\
            #42 try_close t2: left out, runs while unwinding; aborts after daa3e9d9, as a child process shows\n\
            #43 record t2 outcome=failure: left out, runs while unwinding; aborts after f51038c6, as a child process shows\n\
            #44 try_close t2 closes: left out, runs while unwinding; aborts after a8436c13, as a child process shows\n\
            #45 exit t3: left out, runs while unwinding; aborts after 30025718, as a child process shows\n\
            #46 try_close t3: left out, runs while unwinding; aborts after 082a94e6, as a child process shows\n\
            #47 try_close t3: left out, runs while unwinding; aborts after 6935245e, as a child process shows\n\
            #48 exit t3: left out, runs while unwinding; aborts after 748c54b9, as a child process shows\n\
            #49 try_close t3: left out, runs while unwinding; aborts after 2b68c36c, as a child process shows\n\
            #50 record t3 outcome=failure: left out, runs while unwinding; aborts after 1f7d9108, as a child process shows\n\
            #51 try_close t3 closes: left out, runs while unwinding; aborts after c2d60627, as a child process shows\n\
            #52 record t6 outcome=failure: 9323fdae driver poll panicked: payload subscriber\n\
            #53 try_close t6 closes: cd162a73 driver poll panicked: payload subscriber\n\
            #54 new t7 parent=- operation=shutdown: feaec095 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #55 record t7 outcome=success: ddd4918f driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #56 try_close t7 closes: 6e59d51e driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #57 record t5 outcome=failure: 5f8a8ee4 driver poll panicked: TaskPanicked\n\
            #58 try_close t5 closes: ff22769e driver poll panicked: TaskPanicked\n\
            #59 record t4 outcome=shutdown: bd613642 driver poll panicked: TaskPanicked\n\
            #60 try_close t4 closes: a80b26f9 driver poll panicked: TaskPanicked\n\
            ",
        ),
        (
            "sweep/Hosted/FactoryPanic",
            "\
            baseline: e2c60653\n\
            #0 new t1 parent=- operation=queue: 184ebcef submit panicked: payload subscriber; driver poll panicked: payload factory; call after call Ready(Err(Cancelled))\n\
            #1 new t2 parent=- operation=queue: 93d9f7a3 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: 8aedf9f1 submit panicked: payload subscriber; driver poll panicked: payload factory; call after call Ready(Err(Cancelled))\n\
            #3 new t4 parent=- operation=queue: cc5f9ace submit panicked: payload subscriber; driver poll panicked: payload factory\n\
            #4 enter t1: c2ba5075 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: 63e5a3ea driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: 87b48e13 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: d8cfa78b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 1a62ec31 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: c3cb1a63 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: 0b204cf2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: 24659d89 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: b02be0d4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: 871ffdec driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 new t6 parent=- operation=command: 8f0e46d4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 enter t6: 5079e0cf driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 exit t6: dade3f3d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #17 clone t6: 1c14aee2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #18 enter t2: 3551c758 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #19 clone t2: 6eafed60 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 new t7 parent=t2 operation=command: e515b10c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 try_close t2: 93ba3c25 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #22 exit t2: 2b5607b6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #23 record t2 outcome=success: 6f83edf8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 try_close t2 closes: a17a43df driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 enter t7: 9a90b8d8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #26 exit t7: left out, runs while unwinding; aborts after 68749641, as a child process shows\n\
            #27 record t7 outcome=failure: f3652fbf driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #28 try_close t7 closes: 7f88b07a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #29 record t3 outcome=failure: left out, runs while unwinding; aborts after 0be4fa08, as a child process shows\n\
            #30 try_close t3 closes: left out, runs while unwinding; aborts after c18b51a1, as a child process shows\n\
            #31 record t4 outcome=failure: left out, runs while unwinding; aborts after 4d976c43, as a child process shows\n\
            #32 try_close t4 closes: left out, runs while unwinding; aborts after c18b51a1, as a child process shows\n\
            #33 clone t6: 0c13ed3f driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #34 enter t6: de03560f driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #35 clone t5: 24db6223 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #36 enter t5: ec7aa4a0 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #37 exit t5: c0063068 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #38 try_close t5: 220b7bbf driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #39 clone t5: 2891c0be driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #40 enter t5: eb0ec5d2 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #41 try_close t5: a58878cc driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #42 exit t5: 585d3557 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #43 enter t5: 1435a8cd driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #44 record t5 outcome=success: 8d31fd56 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #45 try_close t5: 823565fe driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #46 exit t5: 609e278e driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #47 try_close t5 closes: fe4706de driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #48 exit t6: c76e1be9 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #49 try_close t6: 898f0d04 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #50 clone t6: 9a66a310 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #51 enter t6: 4a119e2b driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #52 try_close t6: 4c752bb0 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #53 exit t6: e1a145a4 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #54 enter t6: 97f4cdaa driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #55 record t6 outcome=success: 6f8903b9 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #56 try_close t6: 095b2c23 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #57 exit t6: ced5cc36 driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #58 try_close t6 closes: f06926ed driver poll panicked: payload factory; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #59 new t8 parent=- operation=shutdown: 8adf8a44 driver poll panicked: payload factory; begin_shutdown panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #60 enter t8: 304c2a3c driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #61 clone t8: 1d402028 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #62 new t9 parent=t8 operation=cancellation: 7a87a3a2 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #63 try_close t8: 7d8f7cb0 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #64 exit t8: 6f28faf6 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #65 new t10 parent=- operation=shutdown: 9c079b00 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #66 record t10 outcome=success: 66e6b563 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #67 try_close t10 closes: b6179bf2 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #68 new t11 parent=- operation=shutdown: 91acd240 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #69 record t11 outcome=success: 27caef9e driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #70 try_close t11 closes: 4c427118 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #71 record t9 outcome=success: 54586678 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #72 try_close t9 closes: fd2c4bfc driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #73 record t8 outcome=shutdown: e93488e6 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #74 try_close t8 closes: fd2c4bfc driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            ",
        ),
        (
            "sweep/Hosted/PendingShutdown",
            "\
            baseline: 149d7e3f\n\
            #0 new t1 parent=- operation=queue: 837b4fad submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #1 new t2 parent=- operation=queue: 2dd08f4d submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: 18781290 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #3 new t4 parent=- operation=queue: e41d7f28 submit panicked: payload subscriber\n\
            #4 enter t1: 58085f47 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: 9dd76712 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: a953c2cc driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: 43108e21 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 842d16ae driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: 54247b54 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: e11ea468 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: fe3ea527 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: 2f7e17e2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: d93e3e6b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 new t6 parent=- operation=command: 11b05cf6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 enter t6: 8cade815 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 exit t6: 8f7bf284 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #17 clone t6: b8295112 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #18 enter t2: 8666c4d3 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #19 clone t2: 031cfa67 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 new t7 parent=t2 operation=command: 392fae6a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 try_close t2: 5efcb615 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #22 exit t2: ea1a74b9 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #23 record t2 outcome=success: dc3bb09b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 try_close t2 closes: dc4a0b0f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 enter t7: 0056a968 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #26 exit t7: b478723f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #27 clone t7: d4a9ab86 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #28 new t8 parent=- operation=command: cd39ed26 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #29 enter t8: 9cc9021e driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #30 exit t8: b6e081b5 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #31 clone t8: 5bf328e9 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #32 enter t3: 29229cb0 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #33 clone t3: 1869712d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #34 new t9 parent=t3 operation=command: 292e47c1 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #35 try_close t3: b13edb53 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #36 exit t3: d4593fec driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #37 record t3 outcome=success: 5d4446fa driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #38 try_close t3 closes: bb1f2807 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #39 enter t9: aa138777 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #40 exit t9: 3f985895 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #41 clone t9: c83b2ba5 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #42 new t10 parent=- operation=command: aa1dc86d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #43 enter t10: 322b30ff driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #44 exit t10: 15126788 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #45 clone t10: 008af031 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #46 enter t4: 7609c9f9 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #47 clone t4: d0c05d1c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #48 new t11 parent=t4 operation=command: b63d2258 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #49 try_close t4: c074cdaf driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #50 exit t4: 6aa231b1 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #51 record t4 outcome=success: 39d5c56b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #52 try_close t4 closes: ec0f99b2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #53 enter t11: 209c8188 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #54 exit t11: c854ead2 driver poll panicked: payload subscriber; call after call Pending; run in a child process without leak detection\n\
            #55 clone t11: 7701ec16 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #56 new t12 parent=- operation=command: 10957f38 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #57 enter t12: 13989856 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #58 exit t12: 52bbe91a driver poll panicked: payload subscriber; call after call Pending; run in a child process without leak detection\n\
            #59 clone t12: 53fc96be driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #60 clone t6: d29f25c7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #61 enter t6: 2600fe11 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #62 clone t5: efc165a0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #63 enter t5: 060e1ed6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #64 exit t5: 81805949 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #65 try_close t5: f8493724 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #66 clone t5: 8ed02cf0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #67 enter t5: 4dc8af2b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #68 try_close t5: 6323d9c9 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #69 exit t5: 604d0d6c driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #70 enter t5: 78652cb9 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #71 record t5 outcome=success: df903e33 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #72 try_close t5: 24174b9b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #73 exit t5: cedd09b8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #74 try_close t5 closes: 58bf87d4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #75 exit t6: 5de9cfc7 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #76 try_close t6: 6312e352 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #77 clone t6: 1cade84f driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #78 enter t6: f6af94f9 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #79 try_close t6: 92089519 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #80 exit t6: f9f1b80b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #81 enter t6: 60789247 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #82 record t6 outcome=success: f1a53bf0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #83 try_close t6: abdd25da driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #84 exit t6: a5d1a1df driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #85 try_close t6 closes: 4b1dc38a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #86 clone t8: db4c69ca driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #87 enter t8: 7326f95f driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #88 clone t7: c035065e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #89 enter t7: 2cdcdc67 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #90 exit t7: 9ef2cd60 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #91 try_close t7: 75629893 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #92 exit t8: 6c19580b driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #93 try_close t8: f5c77e1d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #94 clone t10: 36c86037 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #95 enter t10: 08db66d5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #96 clone t9: 4c748f39 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #97 enter t9: 30936fa5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #98 exit t9: 99c0fa52 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #99 try_close t9: da9ecb78 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #100 clone t9: 45f98278 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #101 enter t9: 6ba4ef10 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #102 try_close t9: 84e959b5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #103 exit t9: d853df57 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #104 enter t9: fb4c5ec4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #105 record t9 outcome=success: 4905ede6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #106 try_close t9: 43f93d89 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #107 exit t9: f837de59 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #108 try_close t9 closes: 92ad2a39 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #109 exit t10: c4fd7139 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #110 try_close t10: 531bd61a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #111 clone t10: c985f324 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #112 enter t10: 57c01652 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #113 try_close t10: c2de4867 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #114 exit t10: f0f4d208 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #115 enter t10: 577742a5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #116 record t10 outcome=success: 2bb129c2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #117 try_close t10: 90b07c5e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #118 exit t10: 23fd857d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #119 try_close t10 closes: 4b1dc38a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #120 clone t12: adfd10d0 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #121 enter t12: 5c3d5d47 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #122 clone t11: be0f5725 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #123 enter t11: c7d3ede4 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #124 exit t11: 41a7f7f1 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #125 try_close t11: f7163235 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #126 clone t11: 6bb8b0c4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #127 enter t11: 4a2f7ac7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #128 try_close t11: 59293584 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #129 exit t11: bf77b550 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #130 enter t11: 7aaa2f29 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #131 record t11 outcome=success: 28f73190 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #132 try_close t11: 11735750 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #133 exit t11: ab8005f8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #134 try_close t11 closes: 651ebd7a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #135 exit t12: 698cb8c1 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #136 try_close t12: f46ca046 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #137 clone t12: 6e86f2ac driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #138 enter t12: b5880f48 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #139 try_close t12: 7a946518 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #140 exit t12: 64583e38 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #141 enter t12: 741aca3c driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #142 record t12 outcome=success: 37aa2a07 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #143 try_close t12: d61298bc driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #144 exit t12: 4bcb49fb driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #145 try_close t12 closes: 4b1dc38a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #146 new t13 parent=- operation=shutdown: b24f4588 begin_shutdown panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #147 enter t13: d25d54b8 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #148 clone t13: de52e991 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #149 new t14 parent=t13 operation=cancellation: 8dfa3786 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #150 try_close t13: 630a09c0 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #151 exit t13: a1d1b72b driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #152 new t15 parent=- operation=shutdown: 463e20de driver poll panicked: payload subscriber; call after call Ready(Ok(())); driver drop panicked: task group must join every admitted task before drop\n\
            #153 clone t8: f792c24d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #154 enter t8: 3267e9c6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #155 clone t8: 10ad6ef2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #156 enter t8: fd5557af driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #157 clone t7: 1a91df42 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #158 enter t7: c68621e0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #159 clone t7: 57ed1eef driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #160 enter t7: 6892572d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #161 exit t7: 29cc8435 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #162 try_close t7: e1ab6fb5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #163 try_close t7: ee206288 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #164 record t7 outcome=cancelled: 3b4ae4bb driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #165 try_close t7: 28f29766 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #166 exit t7: 4e92aaf7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #167 try_close t7 closes: 98ec08ce driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #168 exit t8: c7106b62 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #169 try_close t8: 4870c8e5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #170 try_close t8: bfc1d31e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #171 record t8 outcome=cancelled: 08b92060 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #172 try_close t8: c9fba6ba driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #173 exit t8: 39712e97 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #174 try_close t8 closes: dc20d483 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #175 record t15 outcome=failure: 262af35a driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #176 try_close t15 closes: b2a9ad01 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #177 new t16 parent=- operation=shutdown: 73e0770a driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #178 record t16 outcome=success: 99757578 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #179 try_close t16 closes: 13dd02ed driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #180 record t14 outcome=success: 7ccd4c21 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #181 try_close t14 closes: 9dc6c722 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #182 record t13 outcome=shutdown: 6ec43204 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #183 try_close t13 closes: 9dc6c722 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            ",
        ),
        (
            "sweep/Hosted/PollDropPanic",
            "\
            baseline: 0452f915\n\
            #0 new t1 parent=- operation=queue: 73bc378e submit panicked: payload subscriber\n\
            #1 enter t1: 6d405c2e driver poll panicked: payload subscriber\n\
            #2 clone t1: 04e32a0d driver poll panicked: payload subscriber\n\
            #3 new t2 parent=t1 operation=command: 2490d160 driver poll panicked: payload subscriber\n\
            #4 try_close t1: c41e9a21 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #5 exit t1: 7c57a9b5 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #6 record t1 outcome=success: 19ed5a37 driver poll panicked: payload subscriber\n\
            #7 try_close t1 closes: 240c33b5 driver poll panicked: payload subscriber\n\
            #8 enter t2: 1c6914eb driver poll panicked: payload subscriber\n\
            #9 exit t2: d225c505 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #10 clone t2: left out; aborts after d0c2968b, as a child process shows\n\
            #11 new t3 parent=- operation=command: left out; aborts after f0312889, as a child process shows\n\
            #12 enter t3: left out; aborts after fe76359e, as a child process shows\n\
            #13 exit t3: d28bbe89 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #14 clone t3: left out; aborts after eef1c3cc, as a child process shows\n\
            #15 clone t3: cee615bd driver poll panicked: TaskPanicked\n\
            #16 enter t3: 34b8d196 driver poll panicked: TaskPanicked\n\
            #17 clone t2: 64145aa3 driver poll panicked: TaskPanicked\n\
            #18 enter t2: 1dda1ba8 driver poll panicked: TaskPanicked\n\
            #19 exit t2: 7f1f0e50 driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #20 try_close t2: 50b276c6 driver poll panicked: TaskPanicked\n\
            #21 exit t3: 77740023 driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #22 try_close t3: 787de0ae driver poll panicked: TaskPanicked\n\
            #23 clone t3: 60af7b26 driver poll panicked: TaskPanicked\n\
            #24 enter t3: 897ca6af driver poll panicked: TaskPanicked\n\
            #25 clone t2: ba5c85cd driver poll panicked: TaskPanicked\n\
            #26 enter t2: a0757eb4 driver poll panicked: TaskPanicked\n\
            #27 exit t2: left out, runs while unwinding; aborts after e6b38205, as a child process shows\n\
            #28 try_close t2: left out, runs while unwinding; aborts after 8df38fb1, as a child process shows\n\
            #29 clone t2: ca995260 driver poll panicked: TaskPanicked\n\
            #30 enter t2: 950f034c driver poll panicked: TaskPanicked\n\
            #31 clone t2: left out; aborts after 9db83929, as a child process shows\n\
            #32 enter t2: left out; aborts after cfe9142e, as a child process shows\n\
            #33 exit t2: left out, runs while unwinding; aborts after 458bd340, as a child process shows\n\
            #34 try_close t2: left out, runs while unwinding; aborts after c1e43147, as a child process shows\n\
            #35 try_close t2: left out, runs while unwinding; aborts after 9ed371b9, as a child process shows\n\
            #36 exit t2: left out, runs while unwinding; aborts after 0c50acc2, as a child process shows\n\
            #37 try_close t2: left out, runs while unwinding; aborts after 8196ba4d, as a child process shows\n\
            #38 exit t3: left out, runs while unwinding; aborts after cb5cc9a1, as a child process shows\n\
            #39 try_close t3: left out, runs while unwinding; aborts after cbe53529, as a child process shows\n\
            #40 clone t3: 8d02576f driver poll panicked: TaskPanicked\n\
            #41 enter t3: 7a963c58 driver poll panicked: TaskPanicked\n\
            #42 clone t3: 5c3530c5 driver poll panicked: TaskPanicked\n\
            #43 enter t3: 03a910c2 driver poll panicked: TaskPanicked\n\
            #44 record t2 outcome=cancelled: 0b6dea1d driver poll panicked: TaskPanicked\n\
            #45 try_close t2 closes: fa056351 driver poll panicked: TaskPanicked\n\
            #46 exit t3: f74ade50 driver poll panicked: TaskPanicked\n\
            #47 try_close t3: 619827c1 driver poll panicked: TaskPanicked\n\
            #48 try_close t3: 89ea192f driver poll panicked: TaskPanicked\n\
            #49 record t3 outcome=failure: a24ae1db driver poll panicked: TaskPanicked\n\
            #50 try_close t3: 5fea736e driver poll panicked: TaskPanicked\n\
            #51 exit t3: 3e077df0 driver poll panicked: TaskPanicked\n\
            #52 try_close t3 closes: 4abc3f90 driver poll panicked: TaskPanicked\n\
            #53 new t4 parent=- operation=shutdown: 6cfbd61b driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #54 enter t4: c5819b78 driver poll panicked: TaskPanicked\n\
            #55 clone t4: fd1c1f7d driver poll panicked: TaskPanicked\n\
            #56 new t5 parent=t4 operation=cancellation: c8275d7e driver poll panicked: TaskPanicked\n\
            #57 try_close t4: 39c39aa8 driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #58 exit t4: cb9e757e driver poll panicked: TaskPanicked; run in a child process without leak detection\n\
            #59 new t6 parent=- operation=shutdown: 5f905bdf driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #60 record t6 outcome=failure: f6ed215e driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #61 try_close t6 closes: 4ed5f861 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #62 new t7 parent=- operation=shutdown: b63f203f driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #63 record t7 outcome=success: 888ea397 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #64 try_close t7 closes: c1cd1d1b driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #65 record t5 outcome=failure: 5e5c040b driver poll panicked: TaskPanicked\n\
            #66 try_close t5 closes: 5cb49333 driver poll panicked: TaskPanicked\n\
            #67 record t4 outcome=shutdown: ae8e91c5 driver poll panicked: TaskPanicked\n\
            #68 try_close t4 closes: 2458cfb3 driver poll panicked: TaskPanicked\n\
            ",
        ),
        (
            "sweep/Hosted/PollPanic",
            "\
            baseline: ab690f94\n\
            #0 new t1 parent=- operation=queue: b6781288 submit panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #1 new t2 parent=- operation=queue: 93d9f7a3 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: 83594a3a submit panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #3 new t4 parent=- operation=queue: 1495bd63 submit panicked: payload subscriber; driver poll panicked: TaskPanicked\n\
            #4 enter t1: c2ba5075 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: 63e5a3ea driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: 87b48e13 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: d8cfa78b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 1a62ec31 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: c3cb1a63 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: 0b204cf2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: 24659d89 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: b02be0d4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: 871ffdec driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 new t6 parent=- operation=command: 8f0e46d4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 enter t6: 5079e0cf driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 exit t6: dade3f3d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #17 clone t6: 1c14aee2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #18 enter t2: 3551c758 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #19 clone t2: 6eafed60 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 new t7 parent=t2 operation=command: e515b10c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 try_close t2: 93ba3c25 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #22 exit t2: 2b5607b6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #23 record t2 outcome=success: 6f83edf8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 try_close t2 closes: a17a43df driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 enter t7: 9a90b8d8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #26 exit t7: a5b6c660 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #27 clone t7: 84bb30a2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #28 new t8 parent=- operation=command: ad368eea driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #29 enter t8: 2e0f8776 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #30 exit t8: e6cfe819 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #31 clone t8: c9668b76 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #32 enter t3: 970e70d6 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #33 clone t3: ad248a71 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #34 new t9 parent=t3 operation=command: 72f7ac65 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #35 try_close t3: 01fcadcd driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #36 exit t3: cc25e7e0 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #37 record t3 outcome=success: 66436a25 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #38 try_close t3 closes: 2b123a33 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #39 enter t9: f4dcf3d2 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #40 exit t9: eb8b4acc driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #41 clone t9: cd3a6e0f driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #42 new t10 parent=- operation=command: 4cadf14d driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #43 enter t10: e0f41ec1 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #44 exit t10: 2357f1d7 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #45 clone t10: f8eba6fc driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #46 enter t4: 0ec7c871 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #47 clone t4: d5c76c47 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #48 new t11 parent=t4 operation=command: dcbd6627 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #49 try_close t4: e9ebfc40 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #50 exit t4: 21348a6b driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #51 record t4 outcome=success: ba7f11c2 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #52 try_close t4 closes: 421d57b7 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #53 enter t11: 7fd1b880 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #54 exit t11: 0039ca6b driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Pending; run in a child process without leak detection\n\
            #55 clone t11: 51027f6c driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #56 new t12 parent=- operation=command: 9a6a6d21 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #57 enter t12: 848851e6 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #58 exit t12: 4c332e9b driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Pending; run in a child process without leak detection\n\
            #59 clone t12: 4c3d73a1 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #60 clone t6: cd8152f1 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #61 enter t6: a7f59ee5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #62 clone t5: cb9225a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #63 enter t5: 903c521d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #64 exit t5: 987d16b8 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #65 try_close t5: 2381e724 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #66 clone t5: 275f6f28 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #67 enter t5: d167811b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #68 try_close t5: 79d1f1f8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #69 exit t5: c71cafbc driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #70 enter t5: 97bfb2b9 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #71 record t5 outcome=success: 16c6f93a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #72 try_close t5: b25682ac driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #73 exit t5: 0050c3d5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #74 try_close t5 closes: dd02f830 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #75 exit t6: be547751 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #76 try_close t6: 45b6ac86 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #77 clone t6: 7923a3c2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #78 enter t6: 240b2a27 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #79 try_close t6: 6854a58a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #80 exit t6: 0b02c53a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #81 enter t6: a9905c1e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #82 record t6 outcome=success: 6041afc8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #83 try_close t6: cc533db0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #84 exit t6: 27f130eb driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #85 try_close t6 closes: 7c1d22a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #86 clone t8: 053fde2e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #87 enter t8: 62df29a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #88 clone t7: 72e9d777 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #89 enter t7: dca0f3d7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #90 exit t7: 4a25cf5e driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #91 try_close t7: e500a628 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #92 exit t8: a3572d6c driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #93 try_close t8: 73783d55 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #94 clone t10: 848ccb1d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #95 enter t10: f5aa66a2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #96 clone t9: 281ab121 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #97 enter t9: d8a1f2d3 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #98 exit t9: edb703b2 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #99 try_close t9: fdfc78e2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #100 clone t9: 8fd162fd driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #101 enter t9: 9bdce7e7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #102 try_close t9: 84997761 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #103 exit t9: 52761aaf driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #104 enter t9: 312c87a5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #105 record t9 outcome=success: 32ca1ca6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #106 try_close t9: 60a810c0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #107 exit t9: 2169a9ea driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #108 try_close t9 closes: f5e08c48 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #109 exit t10: 4f99e5d8 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #110 try_close t10: 428a8ce8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #111 clone t10: 1d4d0ab6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #112 enter t10: 8163c63c driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #113 try_close t10: 4cd52351 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #114 exit t10: 82c5d806 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #115 enter t10: 0146296f driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #116 record t10 outcome=success: 6edd3122 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #117 try_close t10: 0a0ab972 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #118 exit t10: 3ab11685 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #119 try_close t10 closes: 7c1d22a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #120 clone t12: ef9030c4 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #121 enter t12: 99c390d9 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #122 clone t11: 9d69a137 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #123 enter t11: e1ab1998 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #124 exit t11: 160ef855 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #125 try_close t11: 26865fe4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #126 clone t11: a5a0766b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #127 enter t11: 1281e836 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #128 try_close t11: 58d6c9c7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #129 exit t11: cd50cfd8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #130 enter t11: 6d2d9aa7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #131 record t11 outcome=success: 9d0f6c15 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #132 try_close t11: 1ff7f475 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #133 exit t11: ebca5a51 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #134 try_close t11 closes: 654fbf85 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #135 exit t12: 187a5717 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #136 try_close t12: 40b58a9d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #137 clone t12: 2de30a70 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #138 enter t12: 5652b561 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #139 try_close t12: c9d47203 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #140 exit t12: e8c19144 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #141 enter t12: 933eb081 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #142 record t12 outcome=success: a126491b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #143 try_close t12: 42980138 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #144 exit t12: 6fb8251b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #145 try_close t12 closes: 7c1d22a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #146 clone t8: ebb2ad72 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #147 enter t8: 5a2d379c driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #148 clone t7: 66464996 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #149 enter t7: 16bb7f65 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #150 exit t7: left out, runs while unwinding; aborts after d9f13c3a, as a child process shows\n\
            #151 try_close t7: left out, runs while unwinding; aborts after 2df32de6, as a child process shows\n\
            #152 clone t7: 832175d6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #153 enter t7: d1ec78ef driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #154 clone t7: aa1e2d57 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #155 enter t7: c2f3d84a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #156 exit t7: ba9deca5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #157 try_close t7: e9bfb3cc driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #158 try_close t7: b73518e4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #159 record t7 outcome=failure: 264c2042 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #160 try_close t7: f9041ff5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #161 exit t7: d7406a61 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #162 try_close t7 closes: 6419391a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #163 exit t8: left out, runs while unwinding; aborts after 971999b5, as a child process shows\n\
            #164 try_close t8: left out, runs while unwinding; aborts after 8c28a339, as a child process shows\n\
            #165 clone t8: 4ca22dbe driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #166 enter t8: 03f9a5a9 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #167 clone t8: df4195ff driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #168 enter t8: bf93a72f driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #169 exit t8: 66913c3b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #170 try_close t8: 1952f767 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #171 try_close t8: badf0bd0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #172 record t8 outcome=failure: f9803f1a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #173 try_close t8: ddebcab0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #174 exit t8: 25a33636 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #175 try_close t8 closes: cff2df27 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #176 new t13 parent=- operation=shutdown: 5f695bfc driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #177 enter t13: 1ecc343e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #178 clone t13: 2fa5cc89 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #179 new t14 parent=t13 operation=cancellation: e11c3e4d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #180 try_close t13: d32995dc driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #181 exit t13: 59c7b05e driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #182 new t15 parent=- operation=shutdown: 4c456002 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #183 record t15 outcome=failure: 1809d876 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #184 try_close t15 closes: cd35ea43 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #185 new t16 parent=- operation=shutdown: 8dd76e63 driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #186 record t16 outcome=success: 85f4c0fc driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #187 try_close t16 closes: a468e27a driver poll panicked: payload subscriber; driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #188 record t14 outcome=failure: 58c116d3 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #189 try_close t14 closes: d605ca9d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #190 record t13 outcome=shutdown: 6663b853 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #191 try_close t13 closes: b99095d4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            ",
        ),
        (
            "sweep/Hosted/Ready",
            "\
            baseline: 9a4ea2bb\n\
            #0 new t1 parent=- operation=queue: e82befc6 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #1 new t2 parent=- operation=queue: 93d9f7a3 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: 92df76c0 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #3 new t4 parent=- operation=queue: 6861891e submit panicked: payload subscriber\n\
            #4 enter t1: c2ba5075 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: 63e5a3ea driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: 87b48e13 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: d8cfa78b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 1a62ec31 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: c3cb1a63 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: 0b204cf2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: 24659d89 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: b02be0d4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: 871ffdec driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 new t6 parent=- operation=command: 8f0e46d4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 enter t6: 5079e0cf driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 exit t6: dade3f3d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #17 clone t6: 1c14aee2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #18 enter t2: 3551c758 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #19 clone t2: 6eafed60 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 new t7 parent=t2 operation=command: e515b10c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 try_close t2: 93ba3c25 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #22 exit t2: 2b5607b6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #23 record t2 outcome=success: 6f83edf8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 try_close t2 closes: a17a43df driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 enter t7: 9a90b8d8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #26 exit t7: a5b6c660 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #27 clone t7: 84bb30a2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #28 new t8 parent=- operation=command: ad368eea driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #29 enter t8: 2e0f8776 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #30 exit t8: e6cfe819 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #31 clone t8: c9668b76 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #32 enter t3: b2835633 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #33 clone t3: d7733f77 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #34 new t9 parent=t3 operation=command: 77c47541 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #35 try_close t3: 864eea27 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #36 exit t3: f9a62f44 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #37 record t3 outcome=success: d556fe1f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #38 try_close t3 closes: 080d5163 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #39 enter t9: 896b12e4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #40 exit t9: a4543a53 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #41 clone t9: 3b8981c2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #42 new t10 parent=- operation=command: cadae6e7 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #43 enter t10: 06f816cf driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #44 exit t10: c1516af6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #45 clone t10: 27033eac driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #46 enter t4: b14644fb driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #47 clone t4: f7b8b376 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #48 new t11 parent=t4 operation=command: b5807ac3 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #49 try_close t4: edc6120a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #50 exit t4: 8dbdb6c1 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #51 record t4 outcome=success: 1a6e321e driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #52 try_close t4 closes: eb9b1117 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #53 enter t11: d313f250 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #54 exit t11: 9d86f154 driver poll panicked: payload subscriber; call after call Pending; run in a child process without leak detection\n\
            #55 clone t11: 77ea7fcf driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #56 new t12 parent=- operation=command: 94a447e0 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #57 enter t12: 4a2b3489 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #58 exit t12: 132f84d7 driver poll panicked: payload subscriber; call after call Pending; run in a child process without leak detection\n\
            #59 clone t12: 4b6652ae driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #60 clone t6: cd8152f1 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #61 enter t6: a7f59ee5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #62 clone t5: cb9225a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #63 enter t5: 903c521d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #64 exit t5: 987d16b8 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #65 try_close t5: 2381e724 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #66 clone t5: 275f6f28 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #67 enter t5: d167811b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #68 try_close t5: 79d1f1f8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #69 exit t5: c71cafbc driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #70 enter t5: 97bfb2b9 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #71 record t5 outcome=success: 16c6f93a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #72 try_close t5: b25682ac driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #73 exit t5: 0050c3d5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #74 try_close t5 closes: dd02f830 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #75 exit t6: be547751 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #76 try_close t6: 45b6ac86 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #77 clone t6: 7923a3c2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #78 enter t6: 240b2a27 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #79 try_close t6: 6854a58a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #80 exit t6: 0b02c53a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #81 enter t6: a9905c1e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #82 record t6 outcome=success: 6041afc8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #83 try_close t6: cc533db0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #84 exit t6: 27f130eb driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #85 try_close t6 closes: 7c1d22a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #86 clone t8: 053fde2e driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #87 enter t8: 62df29a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #88 clone t7: 72e9d777 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #89 enter t7: dca0f3d7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #90 exit t7: 4a25cf5e driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #91 try_close t7: e500a628 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #92 exit t8: a3572d6c driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #93 try_close t8: 73783d55 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #94 clone t10: 848ccb1d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #95 enter t10: f5aa66a2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #96 clone t9: 281ab121 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #97 enter t9: d8a1f2d3 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #98 exit t9: edb703b2 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #99 try_close t9: fdfc78e2 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #100 clone t9: 8fd162fd driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #101 enter t9: 9bdce7e7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #102 try_close t9: 84997761 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #103 exit t9: 52761aaf driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #104 enter t9: 312c87a5 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #105 record t9 outcome=success: 32ca1ca6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #106 try_close t9: 60a810c0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #107 exit t9: 2169a9ea driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #108 try_close t9 closes: f5e08c48 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #109 exit t10: 4f99e5d8 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #110 try_close t10: 428a8ce8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #111 clone t10: 1d4d0ab6 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #112 enter t10: 8163c63c driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #113 try_close t10: 4cd52351 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #114 exit t10: 82c5d806 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #115 enter t10: 0146296f driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #116 record t10 outcome=success: 6edd3122 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #117 try_close t10: 0a0ab972 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #118 exit t10: 3ab11685 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #119 try_close t10 closes: 7c1d22a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #120 clone t12: ef9030c4 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #121 enter t12: 99c390d9 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #122 clone t11: 9d69a137 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #123 enter t11: e1ab1998 driver poll panicked: TaskPanicked; call after call Ready(Err(Cancelled))\n\
            #124 exit t11: 160ef855 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #125 try_close t11: 26865fe4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #126 clone t11: a5a0766b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #127 enter t11: 1281e836 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #128 try_close t11: 58d6c9c7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #129 exit t11: cd50cfd8 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #130 enter t11: 6d2d9aa7 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #131 record t11 outcome=success: 9d0f6c15 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #132 try_close t11: 1ff7f475 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #133 exit t11: ebca5a51 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #134 try_close t11 closes: 654fbf85 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #135 exit t12: 187a5717 driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #136 try_close t12: 40b58a9d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #137 clone t12: 2de30a70 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #138 enter t12: 5652b561 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #139 try_close t12: c9d47203 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #140 exit t12: e8c19144 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #141 enter t12: 933eb081 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #142 record t12 outcome=success: a126491b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #143 try_close t12: 42980138 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #144 exit t12: 6fb8251b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #145 try_close t12 closes: 7c1d22a4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #146 clone t8: ebb2ad72 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #147 enter t8: 5a2d379c driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #148 clone t7: 66464996 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #149 enter t7: 16bb7f65 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #150 exit t7: 76254cbd driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #151 try_close t7: 1891d558 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #152 clone t7: de042a3b driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #153 enter t7: e39d0061 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #154 try_close t7: 2822f8e0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #155 exit t7: 34d92425 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #156 enter t7: bd9867ff driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #157 record t7 outcome=success: f8ba7745 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #158 try_close t7: c2404af0 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #159 exit t7: 4a58cf1c driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #160 try_close t7 closes: 8ab22657 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #161 exit t8: 38203b0d driver poll panicked: TaskPanicked; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #162 try_close t8: fcc6f7cc driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #163 clone t8: d1d164c4 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #164 enter t8: 16e8e0bf driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #165 try_close t8: 46597448 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #166 exit t8: 70381a6d driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #167 enter t8: 0959ce36 driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #168 record t8 outcome=success: c283531a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #169 try_close t8: 60090b7f driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #170 exit t8: cc628bfc driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #171 try_close t8 closes: ce00b22a driver poll panicked: TaskPanicked; call after call Ready(Ok(()))\n\
            #172 new t13 parent=- operation=shutdown: 0c7e85a1 begin_shutdown panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #173 enter t13: 544781ac driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #174 clone t13: ff78a35d driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #175 new t14 parent=t13 operation=cancellation: e7af8aa6 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #176 try_close t13: ea99ec7e driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #177 exit t13: 75c0211b driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #178 new t15 parent=- operation=shutdown: 3e9f10d3 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #179 record t15 outcome=success: 692669cf driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #180 try_close t15 closes: e146b35a driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #181 new t16 parent=- operation=shutdown: d488386e driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #182 record t16 outcome=success: 80e46521 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #183 try_close t16 closes: 3fa1f3b5 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #184 record t14 outcome=success: 3bb05a65 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #185 try_close t14 closes: c8936d0f driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #186 record t13 outcome=shutdown: 25e9e8c3 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #187 try_close t13 closes: c8936d0f driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            ",
        ),
        (
            "sweep/Hosted/Unwinding",
            "\
            baseline: 2e4b3e68\n\
            #0 new t1 parent=- operation=queue: b41b5a3f submit panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #1 new t2 parent=- operation=queue: 8de4baf6 submit panicked: payload subscriber; unwinding panicked: payload test\n\
            #2 enter t1: 47e2bfda driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #3 clone t1: 32cf0991 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #4 new t3 parent=t1 operation=command: b2c70bd8 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #5 try_close t1: 03135007 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #6 exit t1: 9bfa6ba7 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #7 record t1 outcome=success: d6622985 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #8 try_close t1 closes: e119d289 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #9 enter t3: 9d704728 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #10 exit t3: 0a3c0051 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #11 clone t3: c30ad33e driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #12 new t4 parent=- operation=command: c72adbfa driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #13 enter t4: 18df38e0 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #14 exit t4: 8fadbdc0 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #15 clone t4: 1e30cb94 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #16 enter t2: b90ae0d0 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #17 clone t2: 13f036c1 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #18 new t5 parent=t2 operation=command: a45941b5 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #19 try_close t2: 00cc809b driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #20 exit t2: 4645f1b4 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #21 record t2 outcome=success: c5bfcf93 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #22 try_close t2 closes: 82cd61c9 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #23 enter t5: 8bb7685c driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #24 exit t5: 4b5fa312 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Pending; run in a child process without leak detection\n\
            #25 clone t5: b69d7b21 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #26 new t6 parent=- operation=command: ca2d5af1 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #27 enter t6: 3cc79481 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #28 exit t6: 93a6dd02 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Pending; run in a child process without leak detection\n\
            #29 clone t6: 1ddab795 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #30 clone t4: 46a9040f driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #31 enter t4: d9133b2d driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #32 clone t3: 5b7fbebf driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #33 enter t3: 6ccbd4a9 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #34 exit t3: d1fd7a81 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #35 try_close t3: cdef1a40 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #36 exit t4: 31fb78ea driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #37 try_close t4: 1d892977 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #38 clone t6: 574b1369 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #39 enter t6: 81be2b81 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #40 clone t5: 84fe17ba driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #41 enter t5: b4549023 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #42 exit t5: 683e5f11 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #43 try_close t5: 5fb81167 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #44 exit t6: 15af55d9 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #45 try_close t6: a40c1152 driver poll panicked: TaskPanicked; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #46 new t7 parent=- operation=queue: 9e222554 submit panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #47 enter t7: 06985119 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #48 clone t7: 2748c44c driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #49 new t8 parent=t7 operation=command: 13f6a68b driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #50 try_close t7: cd4b575b driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #51 exit t7: 5274ff23 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #52 record t7 outcome=success: 8acf9d98 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #53 try_close t7 closes: b177aafb driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #54 enter t8: 8c10bd64 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #55 exit t8: 79359fc1 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #56 clone t8: 76beed6e driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #57 new t9 parent=- operation=command: 2a0c4bfc driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #58 enter t9: 17e88322 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #59 exit t9: 9ee153e3 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #60 clone t9: c406d809 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #61 new t10 parent=- operation=queue: e6d636c4 submit panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #62 new t11 parent=- operation=shutdown: 344622a0 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #63 enter t11: 859c18d9 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #64 clone t11: f8f7124f unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #65 new t12 parent=t11 operation=cancellation: 5ce4dc61 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #66 try_close t11: 83781253 unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #67 exit t11: 83781253 unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #68 record t10 outcome=failure: 8b8b2e6e unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #69 try_close t10 closes: e4d59525 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #70 new t13 parent=- operation=shutdown: left out; aborts after f064cb02, as a child process shows\n\
            #71 clone t4: 83894aa3 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #72 enter t4: a261ca3c unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #73 clone t4: 53332b63 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #74 enter t4: de046368 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #75 clone t3: 2772ad09 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #76 enter t3: f10f083a unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #77 clone t3: 866151cd unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #78 enter t3: 5c735038 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #79 exit t3: 8c7a27b8 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #80 try_close t3: 8c7a27b8 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #81 try_close t3: 8c7a27b8 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #82 record t3 outcome=cancelled: b7366250 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #83 try_close t3: b1f10727 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #84 exit t3: b1f10727 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #85 try_close t3 closes: b1f10727 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #86 exit t4: b1f10727 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #87 try_close t4: b1f10727 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #88 try_close t4: b1f10727 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #89 record t4 outcome=cancelled: e429db0b unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #90 try_close t4: d483c8b6 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #91 exit t4: d483c8b6 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #92 try_close t4 closes: d483c8b6 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #93 clone t6: 30b7e140 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #94 enter t6: 2ceadfff unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #95 clone t6: 6b4b04b2 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #96 enter t6: d336ac65 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #97 clone t5: 5052923c unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #98 enter t5: 752f3d30 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #99 clone t5: c1846c52 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #100 enter t5: 0a989d1f unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #101 exit t5: f75a3825 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #102 try_close t5: f75a3825 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #103 try_close t5: f75a3825 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #104 record t5 outcome=cancelled: 404424bd unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #105 try_close t5: 550be482 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #106 exit t5: 550be482 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #107 try_close t5 closes: 550be482 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #108 exit t6: 550be482 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #109 try_close t6: 550be482 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #110 try_close t6: 550be482 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #111 record t6 outcome=cancelled: 47b39ecc unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #112 try_close t6: d483c8b6 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #113 exit t6: d483c8b6 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #114 try_close t6 closes: d483c8b6 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #115 clone t9: 7d842a5b unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #116 enter t9: 69a00e93 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #117 clone t9: 589588da unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #118 enter t9: b72206f1 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #119 clone t8: 6dbfc1df unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #120 enter t8: 720cc0bb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #121 clone t8: 15f714a7 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #122 enter t8: 2afaa977 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #123 exit t8: f83fee43 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #124 try_close t8: f83fee43 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #125 try_close t8: f83fee43 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #126 record t8 outcome=cancelled: 9166538d unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #127 try_close t8: bdc3bc02 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #128 exit t8: bdc3bc02 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #129 try_close t8 closes: bdc3bc02 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #130 exit t9: bdc3bc02 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #131 try_close t9: bdc3bc02 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #132 try_close t9: bdc3bc02 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #133 record t9 outcome=cancelled: 8973043f unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #134 try_close t9: 718d9fd0 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #135 exit t9: 718d9fd0 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #136 try_close t9 closes: 718d9fd0 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #137 record t13 outcome=failure: 0dc092f8 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #138 try_close t13 closes: e4d59525 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #139 new t14 parent=- operation=shutdown: 501394d4 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #140 record t14 outcome=success: f0bcdce3 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #141 try_close t14 closes: e4d59525 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #142 record t12 outcome=success: fbc76155 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #143 try_close t12 closes: 8098b570 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #144 record t11 outcome=shutdown: 37eb5ba2 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #145 try_close t11 closes: 8098b570 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            ",
        ),
        (
            "sweep/Owner/Cleanup",
            "\
            baseline: a97262b8\n\
            #0 new t1 parent=- operation=shutdown: c16cb18e begin_shutdown panicked: payload subscriber\n\
            #1 enter t1: f240e33e driver poll panicked: payload subscriber\n\
            #2 clone t1: 77c13017 driver poll panicked: payload subscriber\n\
            #3 new t2 parent=t1 operation=cancellation: ca2353dc driver poll panicked: payload subscriber\n\
            #4 try_close t1: c1b69320 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #5 exit t1: 44c999b4 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #6 record t2 outcome=success: d403765f driver poll panicked: payload subscriber\n\
            #7 try_close t2 closes: eddf5858 driver poll panicked: payload subscriber\n\
            #8 record t1 outcome=shutdown: a6a70b8d driver poll panicked: payload subscriber\n\
            #9 try_close t1 closes: eddf5858 driver poll panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/Owner/DropPanic",
            "\
            baseline: 27720869\n\
            #0 new t1 parent=- operation=queue: b6985e87 submit panicked: payload subscriber\n\
            #1 enter t1: f097f55c driver poll panicked: payload subscriber\n\
            #2 clone t1: 14ef04cb driver poll panicked: payload subscriber\n\
            #3 new t2 parent=t1 operation=command: e48e5a7e driver poll panicked: payload subscriber\n\
            #4 try_close t1: 1137cee9 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #5 exit t1: 6406b405 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #6 record t1 outcome=success: 742636fc driver poll panicked: payload subscriber\n\
            #7 try_close t1 closes: ff27582e driver poll panicked: payload subscriber\n\
            #8 enter t2: 5975db0d driver poll panicked: payload subscriber\n\
            #9 exit t2: 94592f0a driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #10 clone t2: left out; aborts after d0c2968b, as a child process shows\n\
            #11 clone t2: 821e6635 driver poll panicked: payload drop\n\
            #12 enter t2: 9affe9ce driver poll panicked: payload drop\n\
            #13 exit t2: left out, runs while unwinding; aborts after b48dab3f, as a child process shows\n\
            #14 try_close t2: left out, runs while unwinding; aborts after fcf5e351, as a child process shows\n\
            #15 clone t2: 76d1fca9 driver poll panicked: payload subscriber\n\
            #16 enter t2: b173fb36 driver poll panicked: payload subscriber\n\
            #17 try_close t2: 860f8f48 driver poll panicked: payload subscriber\n\
            #18 record t2 outcome=failure: a2aa26bf driver poll panicked: payload subscriber\n\
            #19 try_close t2: f13cb082 driver poll panicked: payload subscriber\n\
            #20 exit t2: 9d71ccf0 driver poll panicked: payload subscriber\n\
            #21 try_close t2 closes: 6f873bf8 driver poll panicked: payload subscriber\n\
            #22 new t3 parent=- operation=shutdown: 3854529b driver poll panicked: payload drop; begin_shutdown panicked: payload subscriber\n\
            #23 enter t3: 445863d2 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #24 clone t3: b0aebe5e driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #25 new t4 parent=t3 operation=cancellation: 44c210d4 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #26 try_close t3: 80287d68 driver poll panicked: payload drop; driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #27 exit t3: d6e915d4 driver poll panicked: payload drop; driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #28 record t4 outcome=success: 81a7216b driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #29 try_close t4 closes: 4fc4e676 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #30 record t3 outcome=shutdown: 11075d4e driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #31 try_close t3 closes: 4fc4e676 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/Owner/DropPanicShutdown",
            "\
            baseline: e8e94eff\n\
            #0 new t1 parent=- operation=queue: b6985e87 submit panicked: payload subscriber\n\
            #1 enter t1: f097f55c driver poll panicked: payload subscriber\n\
            #2 clone t1: 14ef04cb driver poll panicked: payload subscriber\n\
            #3 new t2 parent=t1 operation=command: e48e5a7e driver poll panicked: payload subscriber\n\
            #4 try_close t1: 1137cee9 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #5 exit t1: 6406b405 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #6 record t1 outcome=success: 742636fc driver poll panicked: payload subscriber\n\
            #7 try_close t1 closes: ff27582e driver poll panicked: payload subscriber\n\
            #8 enter t2: 5975db0d driver poll panicked: payload subscriber\n\
            #9 exit t2: 94592f0a driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #10 clone t2: left out; aborts after d0c2968b, as a child process shows\n\
            #11 clone t2: 821e6635 driver poll panicked: payload drop\n\
            #12 enter t2: 9affe9ce driver poll panicked: payload drop\n\
            #13 exit t2: 38a2dbb7 driver poll panicked: payload drop; run in a child process without leak detection\n\
            #14 try_close t2: fc4aff93 driver poll panicked: payload drop\n\
            #15 new t3 parent=- operation=shutdown: 264bb0c7 begin_shutdown panicked: payload subscriber; driver poll panicked: payload drop\n\
            #16 enter t3: de042538 driver poll panicked: payload subscriber\n\
            #17 clone t3: 8d201801 driver poll panicked: payload subscriber\n\
            #18 new t4 parent=t3 operation=cancellation: 7356d83f driver poll panicked: payload subscriber\n\
            #19 try_close t3: 4d2a828b driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #20 exit t3: 11143171 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #21 clone t2: left out; aborts after c53fca2c, as a child process shows\n\
            #22 enter t2: left out; aborts after 5a191d5c, as a child process shows\n\
            #23 clone t2: left out; aborts after 15714584, as a child process shows\n\
            #24 enter t2: left out; aborts after ac2553e7, as a child process shows\n\
            #25 exit t2: left out, runs while unwinding; aborts after 1ec7bee4, as a child process shows\n\
            #26 try_close t2: left out, runs while unwinding; aborts after 47445937, as a child process shows\n\
            #27 try_close t2: left out, runs while unwinding; aborts after a3d870e3, as a child process shows\n\
            #28 exit t2: left out, runs while unwinding; aborts after 3ab6a0b4, as a child process shows\n\
            #29 try_close t2: left out, runs while unwinding; aborts after 47790be0, as a child process shows\n\
            #30 record t2 outcome=failure: left out, runs while unwinding; aborts after 19e9a375, as a child process shows\n\
            #31 try_close t2 closes: left out, runs while unwinding; aborts after 3b81c3e4, as a child process shows\n\
            #32 record t4 outcome=failure: cf52699e driver poll panicked: payload drop\n\
            #33 try_close t4 closes: 1410c7e9 driver poll panicked: payload drop\n\
            #34 record t3 outcome=shutdown: 62a06468 driver poll panicked: payload drop\n\
            #35 try_close t3 closes: c2f5aa7c driver poll panicked: payload drop\n\
            ",
        ),
        (
            "sweep/Owner/FactoryPanic",
            "\
            baseline: cbdadcab\n\
            #0 new t1 parent=- operation=queue: e813fef3 submit panicked: payload subscriber; driver poll panicked: payload factory; call after call Ready(Err(Cancelled))\n\
            #1 new t2 parent=- operation=queue: ddab09e8 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: fb49a975 submit panicked: payload subscriber; driver poll panicked: payload factory; call after call Ready(Err(Cancelled))\n\
            #3 new t4 parent=- operation=queue: d52bcaf6 submit panicked: payload subscriber; driver poll panicked: payload factory\n\
            #4 enter t1: 4aebb92d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: 9fd3c163 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: 36ac7760 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: d4f6700d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 75758822 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: 00ef0f79 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: d4837368 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: 72727572 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: a9cf8561 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: 7fe4d83a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 enter t2: f3325f03 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 clone t2: db0d524d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 new t6 parent=t2 operation=command: 84260464 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #17 try_close t2: a83d3c87 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #18 exit t2: ede32d18 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #19 record t2 outcome=success: af3d0e86 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 try_close t2 closes: 79fb35aa driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 enter t6: f4f91bb2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #22 exit t6: left out, runs while unwinding; aborts after 691658ba, as a child process shows\n\
            #23 record t6 outcome=failure: 07bae078 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 try_close t6 closes: 5cf82830 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 record t3 outcome=failure: left out, runs while unwinding; aborts after a1e4a57d, as a child process shows\n\
            #26 try_close t3 closes: left out, runs while unwinding; aborts after ca1dbab8, as a child process shows\n\
            #27 record t4 outcome=failure: left out, runs while unwinding; aborts after fefd2b6c, as a child process shows\n\
            #28 try_close t4 closes: left out, runs while unwinding; aborts after ca1dbab8, as a child process shows\n\
            #29 clone t5: baf9c3e1 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #30 enter t5: f6cc21e2 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #31 exit t5: 8b77d6f0 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #32 try_close t5: c1bd64f9 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #33 clone t5: f3148499 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #34 enter t5: left out; aborts after a751f51c, as a child process shows\n\
            #35 try_close t5: 795985ad driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #36 exit t5: d2598a65 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #37 enter t5: f0fc3006 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #38 record t5 outcome=success: 1016f831 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #39 try_close t5: daa9e258 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #40 exit t5: 9acc93a1 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #41 try_close t5 closes: 82c9bea3 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #42 new t7 parent=- operation=shutdown: ba31f727 driver poll panicked: payload factory; begin_shutdown panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #43 enter t7: f60e845f driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #44 clone t7: 28c24cce driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #45 new t8 parent=t7 operation=cancellation: bf65829e driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #46 try_close t7: 6f6ede2a driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #47 exit t7: 85955788 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #48 record t8 outcome=success: 18b65d90 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #49 try_close t8 closes: 63a68f0f driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #50 record t7 outcome=shutdown: cc72cc84 driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #51 try_close t7 closes: 63a68f0f driver poll panicked: payload factory; driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            ",
        ),
        (
            "sweep/Owner/PendingShutdown",
            "\
            baseline: 4ebafbdd\n\
            #0 new t1 parent=- operation=queue: 1787e864 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #1 new t2 parent=- operation=queue: 75c2becc submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: 34a78850 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #3 new t4 parent=- operation=queue: b4d502a8 submit panicked: payload subscriber\n\
            #4 enter t1: 4b161f99 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: fdab2344 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: 989fdd88 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: c739df4c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 4d31d208 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: c780f30f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: 0545b4ea driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: ff99938d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: 1eeff1e0 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: 0fce9d29 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 enter t2: 8bb77af1 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 clone t2: 836ef3d0 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 new t6 parent=t2 operation=command: c13fed99 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #17 try_close t2: e3e56a68 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #18 exit t2: fb646071 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #19 record t2 outcome=success: 874e0aeb driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 try_close t2 closes: 3e99528e driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 enter t6: 28e72648 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #22 exit t6: ca3db63f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #23 clone t6: 6a61f711 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 enter t3: 9e6aae52 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 clone t3: 3f7a5fa7 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #26 new t7 parent=t3 operation=command: 9321446e driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #27 try_close t3: ed041c9c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #28 exit t3: 43199968 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #29 record t3 outcome=success: eb286920 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #30 try_close t3 closes: eed5dd98 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #31 enter t7: 445cbb9c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #32 exit t7: 293fcba1 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #33 clone t7: 1afb1b35 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #34 enter t4: 3a91c602 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #35 clone t4: a65ee279 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #36 new t8 parent=t4 operation=command: 3783b420 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #37 try_close t4: fcd350f4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #38 exit t4: 7b363c33 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #39 record t4 outcome=success: c0c958fb driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #40 try_close t4 closes: 7696f94a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #41 enter t8: 08eb00be driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #42 exit t8: 4165821e driver poll panicked: payload subscriber; call after call Pending; run in a child process without leak detection\n\
            #43 clone t8: dc009076 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #44 clone t5: 4a6bc96b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #45 enter t5: 2a88fe18 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #46 exit t5: 092d846a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #47 try_close t5: 229a3754 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #48 clone t5: f8a28a5a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #49 enter t5: left out; aborts after c19bbc76, as a child process shows\n\
            #50 try_close t5: 81501794 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #51 exit t5: 1eb95799 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #52 enter t5: 02eee4ca driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #53 record t5 outcome=success: 0be9fb00 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #54 try_close t5: 9f7d9bb8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #55 exit t5: 0cbc8c3f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #56 try_close t5 closes: ae68c873 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #57 clone t6: 1e1e656f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #58 enter t6: c76c9a29 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #59 exit t6: e285f75f driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #60 try_close t6: 6562c1b2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #61 clone t7: 98c98cae driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #62 enter t7: adf888fa driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #63 exit t7: 516fd2f6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #64 try_close t7: d62a7300 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #65 clone t7: 677b3946 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #66 enter t7: left out; aborts after 66bc38f5, as a child process shows\n\
            #67 try_close t7: 978edf8c driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #68 exit t7: b0edc622 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #69 enter t7: abae9cfb driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #70 record t7 outcome=success: 3c1a99d0 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #71 try_close t7: 552c5dbd driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #72 exit t7: 7bb72689 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #73 try_close t7 closes: 81ac88e8 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #74 clone t8: 2eb5398b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #75 enter t8: 405c9cd2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #76 exit t8: 03788955 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #77 try_close t8: 1ee52940 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #78 clone t8: 49d7893d driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #79 enter t8: left out; aborts after 9e8fcb52, as a child process shows\n\
            #80 try_close t8: 07608735 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #81 exit t8: 1044317f driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #82 enter t8: f27c9206 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #83 record t8 outcome=success: 6fc43ab5 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #84 try_close t8: b9929411 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #85 exit t8: 50160c3e driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #86 try_close t8 closes: 8dc11801 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #87 new t9 parent=- operation=shutdown: 3f94cd51 begin_shutdown panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #88 enter t9: 76ba5c01 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #89 clone t9: 7939526c driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #90 new t10 parent=t9 operation=cancellation: 019e858b driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #91 try_close t9: 7822800b driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #92 exit t9: f1b2dd2a driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #93 clone t6: 70eb93cf driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #94 enter t6: 2f4ee7b8 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #95 clone t6: f98d0828 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #96 enter t6: 9b2811f7 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #97 exit t6: 89945ec1 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #98 try_close t6: b623d9d0 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #99 try_close t6: 415d9ad8 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #100 record t6 outcome=cancelled: 912aab02 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #101 try_close t6: 4d3700ff driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #102 exit t6: e0804abe driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #103 try_close t6 closes: aec8c590 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #104 record t10 outcome=success: c56de5f9 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #105 try_close t10 closes: e7169a25 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #106 record t9 outcome=shutdown: cbabcf7e driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #107 try_close t9 closes: e7169a25 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            ",
        ),
        (
            "sweep/Owner/PollDropPanic",
            "\
            baseline: 4b556ec1\n\
            #0 new t1 parent=- operation=queue: 30c9f249 submit panicked: payload subscriber\n\
            #1 enter t1: af2d39c1 driver poll panicked: payload subscriber\n\
            #2 clone t1: b857c3f6 driver poll panicked: payload subscriber\n\
            #3 new t2 parent=t1 operation=command: 74b61d1d driver poll panicked: payload subscriber\n\
            #4 try_close t1: 6dc66605 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #5 exit t1: 9e8b2bbf driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #6 record t1 outcome=success: 8531d1fe driver poll panicked: payload subscriber\n\
            #7 try_close t1 closes: 916c364a driver poll panicked: payload subscriber\n\
            #8 enter t2: 0814d5ea driver poll panicked: payload subscriber\n\
            #9 exit t2: a55dc131 driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #10 clone t2: left out; aborts after d0c2968b, as a child process shows\n\
            #11 clone t2: db07330c driver poll panicked: payload drop\n\
            #12 enter t2: 6ea5f655 driver poll panicked: payload drop\n\
            #13 exit t2: 2cc3b4e6 driver poll panicked: payload drop; run in a child process without leak detection\n\
            #14 try_close t2: 8f9afda3 driver poll panicked: payload drop\n\
            #15 clone t2: 90b0bd9a driver poll panicked: payload drop\n\
            #16 enter t2: 07260cda driver poll panicked: payload drop\n\
            #17 exit t2: left out, runs while unwinding; aborts after 63abfe1a, as a child process shows\n\
            #18 try_close t2: left out, runs while unwinding; aborts after 33ef72ca, as a child process shows\n\
            #19 clone t2: left out; aborts after 843edd8f, as a child process shows\n\
            #20 enter t2: left out; aborts after 6db1642d, as a child process shows\n\
            #21 clone t2: left out; aborts after 75a22599, as a child process shows\n\
            #22 enter t2: left out; aborts after 7848d973, as a child process shows\n\
            #23 exit t2: left out, runs while unwinding; aborts after 5320b3c1, as a child process shows\n\
            #24 try_close t2: left out, runs while unwinding; aborts after 72c75998, as a child process shows\n\
            #25 try_close t2: left out, runs while unwinding; aborts after a9188fc2, as a child process shows\n\
            #26 exit t2: left out, runs while unwinding; aborts after 27923ea8, as a child process shows\n\
            #27 try_close t2: left out, runs while unwinding; aborts after ecbbd887, as a child process shows\n\
            #28 record t2 outcome=failure: left out, runs while unwinding; aborts after f8c8e809, as a child process shows\n\
            #29 try_close t2 closes: left out, runs while unwinding; aborts after 76397caa, as a child process shows\n\
            #30 new t3 parent=- operation=shutdown: 81aa31b4 driver poll panicked: payload drop; begin_shutdown panicked: payload subscriber\n\
            #31 enter t3: f502edb0 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #32 clone t3: c4cee37f driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #33 new t4 parent=t3 operation=cancellation: 85babba1 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #34 try_close t3: 75c542e8 driver poll panicked: payload drop; driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #35 exit t3: a68d5e24 driver poll panicked: payload drop; driver poll panicked: payload subscriber; run in a child process without leak detection\n\
            #36 record t4 outcome=success: 0f1cdea1 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #37 try_close t4 closes: 7b377dfe driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #38 record t3 outcome=shutdown: db6effe3 driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            #39 try_close t3 closes: 7b377dfe driver poll panicked: payload drop; driver poll panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/Owner/PollPanic",
            "\
            baseline: f3cdf481\n\
            #0 new t1 parent=- operation=queue: 151d4b79 submit panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #1 new t2 parent=- operation=queue: ddab09e8 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: bac2f0d7 submit panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #3 new t4 parent=- operation=queue: a493cb5f submit panicked: payload subscriber; driver poll panicked: payload poll\n\
            #4 enter t1: 4aebb92d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: 9fd3c163 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: 36ac7760 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: d4f6700d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 75758822 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: 00ef0f79 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: d4837368 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: 72727572 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: a9cf8561 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: 7fe4d83a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 enter t2: f3325f03 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 clone t2: db0d524d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 new t6 parent=t2 operation=command: 84260464 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #17 try_close t2: a83d3c87 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #18 exit t2: ede32d18 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #19 record t2 outcome=success: af3d0e86 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 try_close t2 closes: 79fb35aa driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 enter t6: f4f91bb2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #22 exit t6: 3db1273b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #23 clone t6: 0e1eab11 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 enter t3: 8ddd87c6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 clone t3: 8f79cc7b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #26 new t7 parent=t3 operation=command: eaec70ba driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #27 try_close t3: f3fe2211 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #28 exit t3: f09579fb driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #29 record t3 outcome=success: e2db5c37 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #30 try_close t3 closes: 83d68186 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #31 enter t7: 0cfb8d0e driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #32 exit t7: 2697bb42 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #33 clone t7: cc540822 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #34 enter t4: b84476c6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #35 clone t4: 25ff23f3 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #36 new t8 parent=t4 operation=command: 7760a8e4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #37 try_close t4: a9df4188 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #38 exit t4: bcd4f323 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #39 record t4 outcome=success: 7ca91995 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #40 try_close t4 closes: 5ad50ab3 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #41 enter t8: 50fa5e3b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #42 exit t8: 8062122f driver poll panicked: payload subscriber; call after call Pending; run in a child process without leak detection\n\
            #43 clone t8: 34207c96 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #44 clone t5: c1c96a24 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #45 enter t5: d32327e7 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #46 exit t5: 882a7cec driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #47 try_close t5: 3b90ab0a driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #48 clone t5: 1e2c3d8f driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #49 enter t5: left out; aborts after c19bbc76, as a child process shows\n\
            #50 try_close t5: 9d006f6c driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #51 exit t5: bb17416e driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #52 enter t5: 5d238052 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #53 record t5 outcome=success: 90d6f0c9 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #54 try_close t5: dd80127d driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #55 exit t5: 92bc9eba driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #56 try_close t5 closes: 1f09b570 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #57 clone t6: 7be314e1 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #58 enter t6: 35043465 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #59 exit t6: 7881ac29 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #60 try_close t6: 5cd8dfa8 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #61 clone t7: c641f1b4 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #62 enter t7: b6e405dd driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #63 exit t7: a5175d60 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #64 try_close t7: 5128a1df driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #65 clone t7: 26e348f4 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #66 enter t7: left out; aborts after 66bc38f5, as a child process shows\n\
            #67 try_close t7: a1994030 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #68 exit t7: d0bb6095 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #69 enter t7: ae314fe1 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #70 record t7 outcome=success: 42062d91 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #71 try_close t7: e381962e driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #72 exit t7: 44214cec driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #73 try_close t7 closes: dfc9fdde driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #74 clone t8: 26afd56a driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Err(Cancelled))\n\
            #75 enter t8: ed46745c driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Err(Cancelled))\n\
            #76 exit t8: 917a562c driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #77 try_close t8: bce6fe99 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #78 clone t8: 67d008c9 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #79 enter t8: left out; aborts after 9e8fcb52, as a child process shows\n\
            #80 try_close t8: 16c553e0 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #81 exit t8: 52310019 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #82 enter t8: 31726f3e driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #83 record t8 outcome=success: ee2bad4b driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #84 try_close t8: d8a1b52e driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #85 exit t8: 31c228c2 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #86 try_close t8 closes: f464f6b2 driver poll panicked: payload subscriber; driver poll panicked: payload poll; call after call Ready(Ok(()))\n\
            #87 clone t6: f7242f61 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #88 enter t6: 168eb7be driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #89 exit t6: left out, runs while unwinding; aborts after 350da3cb, as a child process shows\n\
            #90 try_close t6: left out, runs while unwinding; aborts after 0e29c4d4, as a child process shows\n\
            #91 clone t6: 1a215aeb driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #92 enter t6: 1e679708 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #93 clone t6: da810980 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #94 enter t6: 6e12908a driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #95 exit t6: 33fae3a7 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #96 try_close t6: b51d6dd5 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #97 try_close t6: 275054d6 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #98 record t6 outcome=failure: d4f307ee driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #99 try_close t6: 5b7ccc01 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #100 exit t6: bb5c858b driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #101 try_close t6 closes: 3b674031 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #102 new t9 parent=- operation=shutdown: 2bc04bf6 driver poll panicked: payload poll; begin_shutdown panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #103 enter t9: d6021eb2 driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #104 clone t9: 2f17c858 driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #105 new t10 parent=t9 operation=cancellation: 10a0fe61 driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #106 try_close t9: 7e57b791 driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #107 exit t9: fb05eb4c driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #108 record t10 outcome=success: e37dacdf driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #109 try_close t10 closes: 90fb1abb driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #110 record t9 outcome=shutdown: b24040ff driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #111 try_close t9 closes: 90fb1abb driver poll panicked: payload poll; driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            ",
        ),
        (
            "sweep/Owner/Ready",
            "\
            baseline: 6f28cf25\n\
            #0 new t1 parent=- operation=queue: fbb20829 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #1 new t2 parent=- operation=queue: ddab09e8 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #2 new t3 parent=- operation=queue: 2d0a9cb1 submit panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #3 new t4 parent=- operation=queue: 1aa9daa7 submit panicked: payload subscriber\n\
            #4 enter t1: 4aebb92d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #5 clone t1: 9fd3c163 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #6 new t5 parent=t1 operation=command: 36ac7760 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #7 try_close t1: d4f6700d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #8 exit t1: 75758822 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #9 record t1 outcome=success: 00ef0f79 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #10 try_close t1 closes: d4837368 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #11 enter t5: 72727572 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #12 exit t5: a9cf8561 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #13 clone t5: 7fe4d83a driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #14 enter t2: f3325f03 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #15 clone t2: db0d524d driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #16 new t6 parent=t2 operation=command: 84260464 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #17 try_close t2: a83d3c87 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #18 exit t2: ede32d18 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #19 record t2 outcome=success: af3d0e86 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #20 try_close t2 closes: 79fb35aa driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #21 enter t6: f4f91bb2 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #22 exit t6: 3db1273b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #23 clone t6: 0e1eab11 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #24 enter t3: 8ddd87c6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #25 clone t3: 8f79cc7b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #26 new t7 parent=t3 operation=command: eaec70ba driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #27 try_close t3: f3fe2211 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #28 exit t3: f09579fb driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #29 record t3 outcome=success: e2db5c37 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #30 try_close t3 closes: 83d68186 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #31 enter t7: 0cfb8d0e driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #32 exit t7: 2697bb42 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #33 clone t7: cc540822 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #34 enter t4: b84476c6 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #35 clone t4: 25ff23f3 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #36 new t8 parent=t4 operation=command: 7760a8e4 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #37 try_close t4: a9df4188 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #38 exit t4: bcd4f323 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #39 record t4 outcome=success: 7ca91995 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #40 try_close t4 closes: 5ad50ab3 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #41 enter t8: 50fa5e3b driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #42 exit t8: 8062122f driver poll panicked: payload subscriber; call after call Pending; run in a child process without leak detection\n\
            #43 clone t8: 34207c96 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #44 clone t5: c1c96a24 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #45 enter t5: d32327e7 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #46 exit t5: 882a7cec driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #47 try_close t5: 3b90ab0a driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #48 clone t5: 1e2c3d8f driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #49 enter t5: left out; aborts after c19bbc76, as a child process shows\n\
            #50 try_close t5: 9d006f6c driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #51 exit t5: bb17416e driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #52 enter t5: 5d238052 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #53 record t5 outcome=success: 90d6f0c9 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #54 try_close t5: dd80127d driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #55 exit t5: 92bc9eba driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #56 try_close t5 closes: 1f09b570 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #57 clone t6: 7be314e1 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #58 enter t6: 35043465 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #59 exit t6: 7881ac29 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #60 try_close t6: 5cd8dfa8 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #61 clone t7: 2db2eb01 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #62 enter t7: 71842410 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #63 exit t7: a7070c15 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #64 try_close t7: 47b19d77 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #65 clone t7: 9219dff4 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #66 enter t7: left out; aborts after 66bc38f5, as a child process shows\n\
            #67 try_close t7: ff7581da driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #68 exit t7: c70875cd driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #69 enter t7: 1bcf8c44 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #70 record t7 outcome=success: 9d4191c5 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #71 try_close t7: 17b1d66a driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #72 exit t7: 1772b237 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #73 try_close t7 closes: 34ef5987 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #74 clone t8: 77730176 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #75 enter t8: 3ba77412 driver poll panicked: payload subscriber; call after call Ready(Err(Cancelled))\n\
            #76 exit t8: 549a2439 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #77 try_close t8: b398aea4 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #78 clone t8: 6a5ab0f6 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #79 enter t8: left out; aborts after 9e8fcb52, as a child process shows\n\
            #80 try_close t8: 4709eef0 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #81 exit t8: 3b46aedb driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #82 enter t8: cf7156f1 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #83 record t8 outcome=success: 17013d55 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #84 try_close t8: 5ed9f38b driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #85 exit t8: be5c6b95 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #86 try_close t8 closes: 7b0886b7 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #87 clone t6: f7242f61 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #88 enter t6: 168eb7be driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #89 exit t6: cd5b335d driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #90 try_close t6: 03052f24 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #91 clone t6: 59290727 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #92 enter t6: left out; aborts after 87288419, as a child process shows\n\
            #93 try_close t6: 1d5d2e27 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #94 exit t6: c74368a9 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #95 enter t6: b4a425b2 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #96 record t6 outcome=success: 858e5c9f driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #97 try_close t6: 6029a60f driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #98 exit t6: b001f873 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #99 try_close t6 closes: 50f458be driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #100 new t9 parent=- operation=shutdown: 599e2de5 begin_shutdown panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #101 enter t9: 9a85b516 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #102 clone t9: efb33aa2 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #103 new t10 parent=t9 operation=cancellation: 29aaca7e driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #104 try_close t9: d50a3115 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #105 exit t9: 000ee616 driver poll panicked: payload subscriber; call after call Ready(Ok(())); run in a child process without leak detection\n\
            #106 record t10 outcome=success: 1da45974 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #107 try_close t10 closes: 5a4af6cb driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #108 record t9 outcome=shutdown: a7565100 driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            #109 try_close t9 closes: 5a4af6cb driver poll panicked: payload subscriber; call after call Ready(Ok(()))\n\
            ",
        ),
        (
            "sweep/Owner/Unwinding",
            "\
            baseline: e1a689a7\n\
            #0 new t1 parent=- operation=queue: 5c3dedbd submit panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #1 new t2 parent=- operation=queue: 51806769 submit panicked: payload subscriber; unwinding panicked: payload test\n\
            #2 enter t1: 3d6e36c1 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #3 clone t1: 776551fe driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #4 new t3 parent=t1 operation=command: 103c5e4e driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #5 try_close t1: 67d224c2 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #6 exit t1: 4f233bec driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #7 record t1 outcome=success: e3833224 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #8 try_close t1 closes: d791b899 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #9 enter t3: f6cf6e9f driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #10 exit t3: b6da1682 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #11 clone t3: 0ef3f30a driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #12 enter t2: 06c1db57 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #13 clone t2: 05585218 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #14 new t4 parent=t2 operation=command: e29e2255 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #15 try_close t2: 3d420ff6 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #16 exit t2: e15768cc driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #17 record t2 outcome=success: 4c3f2a20 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #18 try_close t2 closes: 250c9e2a driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #19 enter t4: 2ddb6a3c driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #20 exit t4: f42f844d driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Pending; run in a child process without leak detection\n\
            #21 clone t4: 0c093476 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #22 clone t3: 9412b87a driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #23 enter t3: e15e0c9f driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #24 exit t3: 8762f12e driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #25 try_close t3: e44b2221 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #26 clone t4: 35af2223 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #27 enter t4: ce335228 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #28 exit t4: efdcacf1 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #29 try_close t4: ad08d797 driver poll panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #30 new t5 parent=- operation=queue: 5dfc52d2 submit panicked: payload subscriber; unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #31 new t6 parent=- operation=shutdown: 89e5cbd6 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #32 enter t6: 79709e8b unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #33 clone t6: 319d51e4 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #34 new t7 parent=t6 operation=cancellation: 90f6d205 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #35 try_close t6: 40374d5d unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #36 exit t6: 40374d5d unwinding panicked: payload test; call pending call Ready(Err(Cancelled)); run in a child process without leak detection\n\
            #37 record t5 outcome=failure: 9e561c44 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #38 try_close t5 closes: bc07dabb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #39 clone t3: 1f63a6ce unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #40 enter t3: 4921ac0e unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #41 clone t3: 22b97294 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #42 enter t3: 429d57d9 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #43 exit t3: b26b48f1 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #44 try_close t3: b26b48f1 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #45 try_close t3: b26b48f1 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #46 record t3 outcome=cancelled: 3512f66f unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #47 try_close t3: bc07dabb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #48 exit t3: bc07dabb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #49 try_close t3 closes: bc07dabb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #50 clone t4: a2d2b055 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #51 enter t4: beb5a370 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #52 clone t4: 6f583aba unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #53 enter t4: dcbe0c98 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #54 exit t4: 4c886b34 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #55 try_close t4: 4c886b34 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #56 try_close t4: 4c886b34 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #57 record t4 outcome=cancelled: 2aa9bae7 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #58 try_close t4: bc07dabb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #59 exit t4: bc07dabb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #60 try_close t4 closes: bc07dabb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #61 record t7 outcome=success: b51844bb unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #62 try_close t7 closes: 0de10d75 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #63 record t6 outcome=shutdown: 11f58c4e unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            #64 try_close t6 closes: 0de10d75 unwinding panicked: payload test; call pending call Ready(Err(Cancelled))\n\
            ",
        ),
        (
            "sweep/group/Abort",
            "\
            baseline: a91675da\n\
            #0 new t1 parent=- operation=command: e55203e9 spawn panicked: payload subscriber\n\
            #1 enter t1: 27b57416 spawn panicked: payload subscriber\n\
            #2 exit t1: cf73fbf1 spawn panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: 06b0e9e1 spawn panicked: payload subscriber\n\
            #4 clone t1: 3db84b1f done TaskId(0) Err(TaskPanicked)\n\
            #5 enter t1: cd8b9745 done TaskId(0) Err(TaskPanicked)\n\
            #6 exit t1: e1d6f571 done TaskId(0) Err(TaskPanicked); run in a child process without leak detection\n\
            #7 try_close t1: 153866cb done TaskId(0) Err(TaskPanicked)\n\
            #8 clone t1: 7bcc7149 done TaskId(0) Err(TaskPanicked)\n\
            #9 enter t1: a3501c18 done TaskId(0) Err(TaskPanicked)\n\
            #10 clone t1: 185a561a done TaskId(0) Err(TaskPanicked)\n\
            #11 enter t1: b602d279 done TaskId(0) Err(TaskPanicked)\n\
            #12 exit t1: acace936 done TaskId(0) Err(TaskPanicked)\n\
            #13 try_close t1: 11f654cc done TaskId(0) Err(TaskPanicked)\n\
            #14 try_close t1: cbae8e3d done TaskId(0) Err(TaskPanicked)\n\
            #15 record t1 outcome=cancelled: b876eb5b done TaskId(0) Err(TaskPanicked)\n\
            #16 try_close t1: 6d33cd86 done TaskId(0) Err(TaskPanicked)\n\
            #17 exit t1: 5212fbb6 done TaskId(0) Err(TaskPanicked)\n\
            #18 try_close t1 closes: fce5cfce done TaskId(0) Err(TaskPanicked)\n\
            #19 new t2 parent=- operation=shutdown: 61215386 done TaskId(0) Err(TaskCancelled); close panicked: payload subscriber\n\
            #20 record t2 outcome=failure: 73438f37 done TaskId(0) Err(TaskCancelled); close panicked: payload subscriber\n\
            #21 try_close t2 closes: a64cc513 done TaskId(0) Err(TaskCancelled); close panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/group/DropPanic",
            "\
            baseline: 082dfdc0\n\
            #0 new t1 parent=- operation=command: 98cface0 spawn panicked: payload subscriber\n\
            #1 enter t1: 95c9fef5 spawn panicked: payload subscriber\n\
            #2 exit t1: 94c2e483 spawn panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: left out; aborts after d6f7f650, as a child process shows\n\
            #4 clone t1: d647a461 done TaskId(0) Err(TaskPanicked)\n\
            #5 enter t1: 6f1d4007 done TaskId(0) Err(TaskPanicked)\n\
            #6 exit t1: left out, runs while unwinding; aborts after dc310142, as a child process shows\n\
            #7 try_close t1: left out, runs while unwinding; aborts after 991fe7c1, as a child process shows\n\
            #8 clone t1: 9581c7cd done TaskId(0) Err(TaskPanicked)\n\
            #9 enter t1: 920bebaf done TaskId(0) Err(TaskPanicked)\n\
            #10 try_close t1: 8d6a9bfe done TaskId(0) Err(TaskPanicked)\n\
            #11 record t1 outcome=failure: 5bc0f4ce done TaskId(0) Err(TaskPanicked)\n\
            #12 try_close t1: b94e32a7 done TaskId(0) Err(TaskPanicked)\n\
            #13 exit t1: c237d0da done TaskId(0) Err(TaskPanicked)\n\
            #14 try_close t1 closes: 69f04fb3 done TaskId(0) Err(TaskPanicked)\n\
            #15 new t2 parent=- operation=shutdown: de26012a done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #16 record t2 outcome=failure: 7d779704 done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #17 try_close t2 closes: d79d5a47 done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/group/DropPanicAbort",
            "\
            baseline: 3c75131d\n\
            #0 new t1 parent=- operation=command: 98cface0 spawn panicked: payload subscriber\n\
            #1 enter t1: 95c9fef5 spawn panicked: payload subscriber\n\
            #2 exit t1: 94c2e483 spawn panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: left out; aborts after d6f7f650, as a child process shows\n\
            #4 clone t1: 812e10be done TaskId(0) Err(TaskPanicked)\n\
            #5 enter t1: 38a0e93e done TaskId(0) Err(TaskPanicked)\n\
            #6 exit t1: aa625e28 done TaskId(0) Err(TaskPanicked); run in a child process without leak detection\n\
            #7 try_close t1: c7a5f405 done TaskId(0) Err(TaskPanicked)\n\
            #8 clone t1: left out; aborts after 5be24283, as a child process shows\n\
            #9 enter t1: left out; aborts after e49dd173, as a child process shows\n\
            #10 clone t1: left out; aborts after 3855de2a, as a child process shows\n\
            #11 enter t1: left out; aborts after 908b6e58, as a child process shows\n\
            #12 exit t1: left out, runs while unwinding; aborts after f9eadafe, as a child process shows\n\
            #13 try_close t1: left out, runs while unwinding; aborts after 8258553d, as a child process shows\n\
            #14 try_close t1: left out, runs while unwinding; aborts after 75427c4b, as a child process shows\n\
            #15 exit t1: left out, runs while unwinding; aborts after a944dc56, as a child process shows\n\
            #16 try_close t1: left out, runs while unwinding; aborts after 998c69d6, as a child process shows\n\
            #17 record t1 outcome=failure: left out, runs while unwinding; aborts after 39a2ee9c, as a child process shows\n\
            #18 try_close t1 closes: left out, runs while unwinding; aborts after 45638ff2, as a child process shows\n\
            #19 new t2 parent=- operation=shutdown: 6ef58054 done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #20 record t2 outcome=failure: e67eadbd done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #21 try_close t2 closes: 506b4af8 done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/group/PollDropPanic",
            "\
            baseline: b094f9cc\n\
            #0 new t1 parent=- operation=command: 98cface0 spawn panicked: payload subscriber\n\
            #1 enter t1: 95c9fef5 spawn panicked: payload subscriber\n\
            #2 exit t1: 94c2e483 spawn panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: left out; aborts after d6f7f650, as a child process shows\n\
            #4 clone t1: d647a461 done TaskId(0) Err(TaskPanicked)\n\
            #5 enter t1: 6f1d4007 done TaskId(0) Err(TaskPanicked)\n\
            #6 exit t1: 3b598ab1 done TaskId(0) Err(TaskPanicked); run in a child process without leak detection\n\
            #7 try_close t1: 46c328ec done TaskId(0) Err(TaskPanicked)\n\
            #8 clone t1: fed2a648 done TaskId(0) Err(TaskPanicked)\n\
            #9 enter t1: 12f28f90 done TaskId(0) Err(TaskPanicked)\n\
            #10 exit t1: left out, runs while unwinding; aborts after 635e8695, as a child process shows\n\
            #11 try_close t1: left out, runs while unwinding; aborts after e00ee86a, as a child process shows\n\
            #12 clone t1: 312f30ea done TaskId(0) Err(TaskPanicked)\n\
            #13 enter t1: d0577899 done TaskId(0) Err(TaskPanicked)\n\
            #14 clone t1: left out; aborts after b01cc9e7, as a child process shows\n\
            #15 enter t1: left out; aborts after 03332760, as a child process shows\n\
            #16 exit t1: left out, runs while unwinding; aborts after d3318f1a, as a child process shows\n\
            #17 try_close t1: left out, runs while unwinding; aborts after 4dab4e8b, as a child process shows\n\
            #18 try_close t1: left out, runs while unwinding; aborts after f694c7d6, as a child process shows\n\
            #19 exit t1: left out, runs while unwinding; aborts after 7199310b, as a child process shows\n\
            #20 try_close t1: left out, runs while unwinding; aborts after effbb2a1, as a child process shows\n\
            #21 record t1 outcome=cancelled: 5c6e2f3d done TaskId(0) Err(TaskPanicked)\n\
            #22 try_close t1 closes: ee44beed done TaskId(0) Err(TaskPanicked)\n\
            #23 new t2 parent=- operation=shutdown: 0e814740 done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #24 record t2 outcome=failure: fe50a62d done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #25 try_close t2 closes: 8948373a done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/group/PollPanic",
            "\
            baseline: c32c1a6a\n\
            #0 new t1 parent=- operation=command: 98cface0 spawn panicked: payload subscriber\n\
            #1 enter t1: 95c9fef5 spawn panicked: payload subscriber\n\
            #2 exit t1: 94c2e483 spawn panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: d164d6e2 spawn panicked: payload subscriber\n\
            #4 clone t1: ba36db11 done TaskId(0) Err(TaskPanicked)\n\
            #5 enter t1: 83e146b5 done TaskId(0) Err(TaskPanicked)\n\
            #6 exit t1: 448fd887 done TaskId(0) Err(TaskPanicked); run in a child process without leak detection\n\
            #7 try_close t1: 9202d241 done TaskId(0) Err(TaskPanicked)\n\
            #8 clone t1: 1923eee1 done TaskId(0) Err(TaskPanicked)\n\
            #9 enter t1: 5170826d done TaskId(0) Err(TaskPanicked)\n\
            #10 exit t1: left out, runs while unwinding; aborts after 635e8695, as a child process shows\n\
            #11 try_close t1: left out, runs while unwinding; aborts after e00ee86a, as a child process shows\n\
            #12 clone t1: 8893fa6b done TaskId(0) Err(TaskPanicked)\n\
            #13 enter t1: 07cec08c done TaskId(0) Err(TaskPanicked)\n\
            #14 clone t1: 0f64c5b7 done TaskId(0) Err(TaskPanicked)\n\
            #15 enter t1: 19c8ab1b done TaskId(0) Err(TaskPanicked)\n\
            #16 exit t1: 77bcdda6 done TaskId(0) Err(TaskPanicked)\n\
            #17 try_close t1: 0c84243b done TaskId(0) Err(TaskPanicked)\n\
            #18 try_close t1: d4607785 done TaskId(0) Err(TaskPanicked)\n\
            #19 record t1 outcome=failure: 94b675d6 done TaskId(0) Err(TaskPanicked)\n\
            #20 try_close t1: cc3b8048 done TaskId(0) Err(TaskPanicked)\n\
            #21 exit t1: a2ca9da1 done TaskId(0) Err(TaskPanicked)\n\
            #22 try_close t1 closes: 07249baa done TaskId(0) Err(TaskPanicked)\n\
            #23 new t2 parent=- operation=shutdown: 99e4fc24 done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #24 record t2 outcome=failure: 2a1b050f done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            #25 try_close t2 closes: c7871eff done TaskId(0) Err(TaskPanicked); close panicked: payload subscriber\n\
            ",
        ),
        (
            "sweep/group/Ready",
            "\
            baseline: cda91beb\n\
            #0 new t1 parent=- operation=command: 98cface0 spawn panicked: payload subscriber\n\
            #1 enter t1: 95c9fef5 spawn panicked: payload subscriber\n\
            #2 exit t1: 94c2e483 spawn panicked: payload subscriber; run in a child process without leak detection\n\
            #3 clone t1: d164d6e2 spawn panicked: payload subscriber\n\
            #4 clone t1: ba36db11 done TaskId(0) Err(TaskPanicked)\n\
            #5 enter t1: 83e146b5 done TaskId(0) Err(TaskPanicked)\n\
            #6 exit t1: 448fd887 done TaskId(0) Err(TaskPanicked); run in a child process without leak detection\n\
            #7 try_close t1: 9202d241 done TaskId(0) Err(TaskPanicked)\n\
            #8 clone t1: 1923eee1 done TaskId(0) Err(TaskPanicked)\n\
            #9 enter t1: 5170826d done TaskId(0) Err(TaskPanicked)\n\
            #10 exit t1: ee6ade89 done TaskId(0) Err(TaskPanicked); run in a child process without leak detection\n\
            #11 try_close t1: 616cfdac done TaskId(0) Err(TaskPanicked)\n\
            #12 clone t1: fa94c6ba done TaskId(0) Err(TaskPanicked)\n\
            #13 enter t1: 6d36eebd done TaskId(0) Err(TaskPanicked)\n\
            #14 try_close t1: 96f65de5 done TaskId(0) Err(TaskPanicked)\n\
            #15 exit t1: e88419ba done TaskId(0) Err(TaskPanicked)\n\
            #16 enter t1: aca90812 done TaskId(0) Err(TaskPanicked)\n\
            #17 record t1 outcome=success: af56b2d6 done TaskId(0) Err(TaskPanicked)\n\
            #18 try_close t1: dc04828d done TaskId(0) Err(TaskPanicked)\n\
            #19 exit t1: 0baa2580 done TaskId(0) Err(TaskPanicked)\n\
            #20 try_close t1 closes: edaacf38 done TaskId(0) Err(TaskPanicked)\n\
            #21 new t2 parent=- operation=shutdown: 34da8d39 done TaskId(0) Ok(()); close panicked: payload subscriber\n\
            #22 record t2 outcome=success: 0d444583 done TaskId(0) Ok(()); close panicked: payload subscriber\n\
            #23 try_close t2 closes: 7a8970de done TaskId(0) Ok(()); close panicked: payload subscriber\n\
            ",
        ),
        (
            "unwinding/Hosted",
            "\
            t:new t1 parent=- operation=queue #0 [A - OA]\n\
            = submit pending -> Ok(())\n\
            t:new t2 parent=- operation=queue #1 [A - OA]\n\
            = submit pending call -> Ok, Pending\n\
            t:enter t1 #2 [D - OD]\n\
            t:clone t1 #3 [A s1 OA]\n\
            t:new t3 parent=t1 operation=command #4 [A s1 OA]\n\
            t:try_close t1 #5 [A s1 OA]\n\
            t:exit t1 #6 [D - OD]\n\
            t:record t1 outcome=success #7 [D - OD]\n\
            close s1 success in OA [D - OD]\n\
            t:try_close t1 closes #8 [D - OD]\n\
            t:enter t3 #9 [D - OD]\n\
            factory pending [A s2 OA]\n\
            factory-drop pending [A s2 OA]\n\
            t:exit t3 #10 [D - OD]\n\
            t:clone t3 #11 [D - OD]\n\
            t:new t4 parent=- operation=command #12 [D - OD]\n\
            t:enter t4 #13 [D - OD]\n\
            t:exit t4 #14 [D - OD]\n\
            t:clone t4 #15 [D - OD]\n\
            t:enter t2 #16 [D - OD]\n\
            t:clone t2 #17 [A s3 OA]\n\
            t:new t5 parent=t2 operation=command #18 [A s3 OA]\n\
            t:try_close t2 #19 [A s3 OA]\n\
            t:exit t2 #20 [D - OD]\n\
            t:record t2 outcome=success #21 [D - OD]\n\
            close s3 success in OA [D - OD]\n\
            t:try_close t2 closes #22 [D - OD]\n\
            t:enter t5 #23 [D - OD]\n\
            factory pending call [A s4 OA]\n\
            factory-drop pending call [A s4 OA]\n\
            t:exit t5 #24 [D - OD]\n\
            t:clone t5 #25 [D - OD]\n\
            t:new t6 parent=- operation=command #26 [D - OD]\n\
            t:enter t6 #27 [D - OD]\n\
            t:exit t6 #28 [D - OD]\n\
            t:clone t6 #29 [D - OD]\n\
            = driver Pending\n\
            t:clone t4 #30 [D - OD]\n\
            t:enter t4 #31 [D - OD]\n\
            t:clone t3 #32 [D s8 OD]\n\
            t:enter t3 #33 [D s8 OD]\n\
            poll pending [A s2 OA]\n\
            t:exit t3 #34 [D s8 OD]\n\
            t:try_close t3 #35 [D s8 OD]\n\
            t:exit t4 #36 [D - OD]\n\
            t:try_close t4 #37 [D - OD]\n\
            t:clone t6 #38 [D - OD]\n\
            t:enter t6 #39 [D - OD]\n\
            t:clone t5 #40 [D s9 OD]\n\
            t:enter t5 #41 [D s9 OD]\n\
            poll pending call [A s4 OA]\n\
            t:exit t5 #42 [D s9 OD]\n\
            t:try_close t5 #43 [D s9 OD]\n\
            t:exit t6 #44 [D - OD]\n\
            t:try_close t6 #45 [D - OD]\n\
            = driver Pending\n\
            t:new t7 parent=- operation=queue #46 [A - OA]\n\
            = submit unpolled -> Ok(())\n\
            t:enter t7 #47 [D - OD]\n\
            t:clone t7 #48 [A s5 OA]\n\
            t:new t8 parent=t7 operation=command #49 [A s5 OA]\n\
            t:try_close t7 #50 [A s5 OA]\n\
            t:exit t7 #51 [D - OD]\n\
            t:record t7 outcome=success #52 [D - OD]\n\
            close s5 success in OA [D - OD]\n\
            t:try_close t7 closes #53 [D - OD]\n\
            t:enter t8 #54 [D - OD]\n\
            factory unpolled [A s6 OA]\n\
            factory-drop unpolled [A s6 OA]\n\
            t:exit t8 #55 [D - OD]\n\
            t:clone t8 #56 [D - OD]\n\
            t:new t9 parent=- operation=command #57 [D - OD]\n\
            t:enter t9 #58 [D - OD]\n\
            t:exit t9 #59 [D - OD]\n\
            t:clone t9 #60 [D - OD]\n\
            = driver Pending\n\
            t:new t10 parent=- operation=queue #61 [A - OA]\n\
            = submit queued -> Ok(())\n\
            t:new t11 parent=- operation=shutdown #62 [D - OD !]\n\
            t:enter t11 #63 [D - OD !]\n\
            t:clone t11 #64 [D s14 OD !]\n\
            t:new t12 parent=t11 operation=cancellation #65 [D s14 OD !]\n\
            t:try_close t11 #66 [D s14 OD !]\n\
            t:exit t11 #67 [D - OD !]\n\
            factory-drop queued [A - OA !]\n\
            t:record t10 outcome=failure #68 [D - OD !]\n\
            close s7 failure in OA [D - OD !]\n\
            t:try_close t10 closes #69 [D - OD !]\n\
            release left=3 [D - OD !]\n\
            t:new t13 parent=- operation=shutdown #70 [D - OD !]\n\
            t:clone t4 #71 [D - OD !]\n\
            t:enter t4 #72 [D - OD !]\n\
            t:clone t4 #73 [D s8 OD !]\n\
            t:enter t4 #74 [D s8 OD !]\n\
            t:clone t3 #75 [D s8 OD !]\n\
            t:enter t3 #76 [D s8 OD !]\n\
            t:clone t3 #77 [A s2 OA !]\n\
            t:enter t3 #78 [A s2 OA !]\n\
            drop pending [A s2 OA !]\n\
            t:exit t3 #79 [A s2 OA !]\n\
            t:try_close t3 #80 [A s2 OA !]\n\
            t:try_close t3 #81 [A s2 OA !]\n\
            t:record t3 outcome=cancelled #82 [A s2 OA !]\n\
            close s2 cancelled in OA [A s2 OA !]\n\
            t:try_close t3 #83 [A s2 OA !]\n\
            t:exit t3 #84 [D s8 OD !]\n\
            t:try_close t3 closes #85 [D s8 OD !]\n\
            release left=2 [D s8 OD !]\n\
            t:exit t4 #86 [D s8 OD !]\n\
            t:try_close t4 #87 [D s8 OD !]\n\
            t:try_close t4 #88 [D s8 OD !]\n\
            t:record t4 outcome=cancelled #89 [D s8 OD !]\n\
            close s8 cancelled in OD [D s8 OD !]\n\
            t:try_close t4 #90 [D s8 OD !]\n\
            t:exit t4 #91 [D - OD !]\n\
            t:try_close t4 closes #92 [D - OD !]\n\
            t:clone t6 #93 [D - OD !]\n\
            t:enter t6 #94 [D - OD !]\n\
            t:clone t6 #95 [D s9 OD !]\n\
            t:enter t6 #96 [D s9 OD !]\n\
            t:clone t5 #97 [D s9 OD !]\n\
            t:enter t5 #98 [D s9 OD !]\n\
            t:clone t5 #99 [A s4 OA !]\n\
            t:enter t5 #100 [A s4 OA !]\n\
            drop pending call [A s4 OA !]\n\
            woken pending call [A s4 OA !]\n\
            t:exit t5 #101 [A s4 OA !]\n\
            t:try_close t5 #102 [A s4 OA !]\n\
            t:try_close t5 #103 [A s4 OA !]\n\
            t:record t5 outcome=cancelled #104 [A s4 OA !]\n\
            close s4 cancelled in OA [A s4 OA !]\n\
            t:try_close t5 #105 [A s4 OA !]\n\
            t:exit t5 #106 [D s9 OD !]\n\
            t:try_close t5 closes #107 [D s9 OD !]\n\
            release left=1 [D s9 OD !]\n\
            t:exit t6 #108 [D s9 OD !]\n\
            t:try_close t6 #109 [D s9 OD !]\n\
            t:try_close t6 #110 [D s9 OD !]\n\
            t:record t6 outcome=cancelled #111 [D s9 OD !]\n\
            close s9 cancelled in OD [D s9 OD !]\n\
            t:try_close t6 #112 [D s9 OD !]\n\
            t:exit t6 #113 [D - OD !]\n\
            t:try_close t6 closes #114 [D - OD !]\n\
            t:clone t9 #115 [D - OD !]\n\
            t:enter t9 #116 [D - OD !]\n\
            t:clone t9 #117 [D s10 OD !]\n\
            t:enter t9 #118 [D s10 OD !]\n\
            t:clone t8 #119 [D s10 OD !]\n\
            t:enter t8 #120 [D s10 OD !]\n\
            t:clone t8 #121 [A s6 OA !]\n\
            t:enter t8 #122 [A s6 OA !]\n\
            drop unpolled [A s6 OA !]\n\
            t:exit t8 #123 [A s6 OA !]\n\
            t:try_close t8 #124 [A s6 OA !]\n\
            t:try_close t8 #125 [A s6 OA !]\n\
            t:record t8 outcome=cancelled #126 [A s6 OA !]\n\
            close s6 cancelled in OA [A s6 OA !]\n\
            t:try_close t8 #127 [A s6 OA !]\n\
            t:exit t8 #128 [D s10 OD !]\n\
            t:try_close t8 closes #129 [D s10 OD !]\n\
            release left=0 [D s10 OD !]\n\
            t:exit t9 #130 [D s10 OD !]\n\
            t:try_close t9 #131 [D s10 OD !]\n\
            t:try_close t9 #132 [D s10 OD !]\n\
            t:record t9 outcome=cancelled #133 [D s10 OD !]\n\
            close s10 cancelled in OD [D s10 OD !]\n\
            t:try_close t9 #134 [D s10 OD !]\n\
            t:exit t9 #135 [D - OD !]\n\
            t:try_close t9 closes #136 [D - OD !]\n\
            t:record t13 outcome=failure #137 [D - OD !]\n\
            close s11 failure in OD [D - OD !]\n\
            t:try_close t13 closes #138 [D - OD !]\n\
            t:new t14 parent=- operation=shutdown #139 [D - OD !]\n\
            t:record t14 outcome=success #140 [D - OD !]\n\
            close s12 success in OD [D - OD !]\n\
            t:try_close t14 closes #141 [D - OD !]\n\
            t:record t12 outcome=success #142 [D - OD !]\n\
            close s13 success in OD [D - OD !]\n\
            t:try_close t12 closes #143 [D - OD !]\n\
            t:record t11 outcome=shutdown #144 [D - OD !]\n\
            close s14 shutdown in OD [D - OD !]\n\
            t:try_close t11 closes #145 [D - OD !]\n\
            = unwinding panicked: payload test\n\
            payload-drop test [- - -]\n\
            = call pending call Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s2<s1 command cancelledx1 s4<s3 command cancelledx1 s5<- queue successx1 s6<s5 command cancelledx1 s7<- queue failurex1\n\
            OD: s8<- command cancelledx1 s9<- command cancelledx1 s10<- command cancelledx1 s14<- shutdown shutdownx1 s13<s14 cancellation successx1 s11<- shutdown failurex1 s12<- shutdown successx1\n\
            ",
        ),
        (
            "unwinding/Owner",
            "\
            = submit pending -> Ok(())\n\
            = submit pending call -> Ok, Pending\n\
            close s1 success in OA [D - OD]\n\
            factory pending [A s2 OA]\n\
            factory-drop pending [A s2 OA]\n\
            close s3 success in OA [D - OD]\n\
            factory pending call [A s4 OA]\n\
            factory-drop pending call [A s4 OA]\n\
            poll pending [A s2 OA]\n\
            poll pending call [A s4 OA]\n\
            = driver Pending\n\
            = submit queued -> Ok(())\n\
            factory-drop queued [A - OA !]\n\
            close s5 failure in OA [D - OD !]\n\
            release left=2 [D - OD !]\n\
            drop pending [A s2 OA !]\n\
            close s2 cancelled in OA [A s2 OA !]\n\
            release left=1 [D - OD !]\n\
            drop pending call [A s4 OA !]\n\
            woken pending call [A s4 OA !]\n\
            close s4 cancelled in OA [A s4 OA !]\n\
            release left=0 [D - OD !]\n\
            close s6 success in OD [D - OD !]\n\
            close s7 shutdown in OD [D - OD !]\n\
            = unwinding panicked: payload test\n\
            payload-drop test [- - -]\n\
            = call pending call Ready(Err(Cancelled))\n\
            = worker Closed\n\
            OA: s1<- queue successx1 s3<- queue successx1 s2<s1 command cancelledx1 s4<s3 command cancelledx1 s5<- queue failurex1\n\
            OD: s7<- shutdown shutdownx1 s6<s7 cancellation successx1\n\
            ",
        ),
    ];

    /// The length and hash of each scenario's whole transcript under
    /// `observability`, its tracing callbacks in place among the other events
    /// and each with the context installed when it ran (`stable_trace`),
    /// recorded on the same runtime as `GOLDENS`.
    #[cfg(feature = "observability")]
    const TRACES: &[(&str, usize, u32)] = &[
        ("abort/PanicDropAbortsItself", 32, 0x6b8e21cd),
        ("abort/PanicDropAbortsOther", 56, 0xbe384cbf),
        ("abort/PendingPollAbortsItself", 31, 0x9ef6af51),
        ("abort/ReadyDropAbortsItself", 29, 0x9283c94c),
        ("abort/ReadyDropAbortsOther", 53, 0x222fdfac),
        ("cleanup/Hosted/Abort", 57, 0x95ab9af2),
        ("cleanup/Hosted/AbortReserved", 59, 0xe7737dda),
        ("cleanup/Hosted/Complete", 60, 0xe8f74060),
        ("cleanup/Hosted/DestructionPanic", 64, 0x63d54057),
        ("cleanup/Hosted/PollPanic", 63, 0xd3d04d5e),
        ("cleanup/HostedWorker/Complete", 104, 0x1fcba631),
        ("cleanup/Owner/Abort", 23, 0xcdd6d84d),
        ("cleanup/Owner/AbortReserved", 25, 0x5d548106),
        ("cleanup/Owner/Complete", 25, 0xa97262b8),
        ("cleanup/Owner/DestructionPanic", 28, 0x47eae31b),
        ("cleanup/Owner/PollPanic", 27, 0x0c8fefdd),
        ("command/Hosted/Call/Closed", 39, 0x342feb2d),
        ("command/Hosted/Call/FactoryPanic", 117, 0x2243e075),
        ("command/Hosted/Call/PendingShutdown", 87, 0x9663eb38),
        ("command/Hosted/Call/PollPanic", 96, 0xaa5c68a1),
        ("command/Hosted/Call/QueuedShutdown", 38, 0x7277dc75),
        ("command/Hosted/Call/Ready", 91, 0x729c0e6e),
        ("command/Hosted/Call/UnpolledShutdown", 123, 0x1ad94da6),
        ("command/Hosted/Spawn/Closed", 39, 0x342feb2d),
        ("command/Hosted/Spawn/CommandClosePanic", 93, 0xb3315508),
        ("command/Hosted/Spawn/FactoryPanic", 115, 0xe2c60653),
        ("command/Hosted/Spawn/GroupClosePanic", 91, 0xc24c4c00),
        ("command/Hosted/Spawn/PendingShutdown", 85, 0xe1b406ac),
        ("command/Hosted/Spawn/PollPanic", 94, 0xa4f1a037),
        ("command/Hosted/Spawn/QueuedShutdown", 36, 0x0cf3f6c1),
        ("command/Hosted/Spawn/Ready", 89, 0x3906de4f),
        ("command/Hosted/Spawn/ReleaseWakePanic", 94, 0x8f51e2ae),
        ("command/Hosted/Spawn/UnpolledShutdown", 121, 0xe89d61d5),
        ("command/HostedWorker/Call/PendingShutdown", 79, 0x8e9f19db),
        ("command/HostedWorker/Call/PollPanic", 91, 0xba67a427),
        ("command/HostedWorker/Call/Ready", 84, 0x722eaa62),
        ("command/HostedWorker/Spawn/PendingShutdown", 76, 0x71f9391f),
        ("command/HostedWorker/Spawn/PollPanic", 88, 0x5d8b311c),
        ("command/HostedWorker/Spawn/Ready", 81, 0x30246212),
        ("command/Owner/Call/Closed", 28, 0x43c770b5),
        ("command/Owner/Call/FactoryPanic", 87, 0x5993dec7),
        ("command/Owner/Call/PendingShutdown", 55, 0x2289ee29),
        ("command/Owner/Call/PollPanic", 60, 0x37cd8782),
        ("command/Owner/Call/QueuedShutdown", 27, 0x645bf03d),
        ("command/Owner/Call/Ready", 57, 0x83149b3d),
        ("command/Owner/Call/UnpolledShutdown", 80, 0x2c56a3ee),
        ("command/Owner/Spawn/Closed", 28, 0x43c770b5),
        ("command/Owner/Spawn/CommandClosePanic", 57, 0x30bb3aa5),
        ("command/Owner/Spawn/FactoryPanic", 85, 0xcbdadcab),
        ("command/Owner/Spawn/PendingShutdown", 53, 0x25bc8a31),
        ("command/Owner/Spawn/PollPanic", 58, 0xe7334cd9),
        ("command/Owner/Spawn/QueuedShutdown", 25, 0x63fc1858),
        ("command/Owner/Spawn/Ready", 55, 0x90379b64),
        ("command/Owner/Spawn/ReleaseWakePanic", 58, 0xd137c9cd),
        ("command/Owner/Spawn/UnpolledShutdown", 78, 0xd6a96a6d),
        ("context/Group/DropSetsOnAbort", 31, 0xa91675da),
        ("context/Group/DropSetsOnReady", 28, 0xaa1d8de7),
        ("context/Group/DropSetsOnShutdown", 31, 0x6674829a),
        ("context/Group/PendingThenAbort", 31, 0xbb231e5d),
        ("context/Group/PendingThenPanic", 36, 0xf42653c9),
        ("context/Group/PendingThenReady", 33, 0x521b9f48),
        ("context/Group/PollSetsThenPanics", 31, 0x2774c1cf),
        ("context/Hosted/DropSetsOnAbort", 85, 0xe1b406ac),
        ("context/Hosted/DropSetsOnReady", 80, 0xb2e07a05),
        ("context/Hosted/DropSetsOnShutdown", 85, 0xe1b406ac),
        ("context/Hosted/PendingThenAbort", 85, 0x0f8235e8),
        ("context/Hosted/PendingThenPanic", 94, 0x887d0567),
        ("context/Hosted/PendingThenReady", 89, 0x2c3ffcad),
        ("context/Hosted/PollSetsThenPanics", 85, 0x4eaa5c7b),
        ("context/Owner/DropSetsOnAbort", 53, 0x25bc8a31),
        ("context/Owner/DropSetsOnReady", 50, 0xcf52d516),
        ("context/Owner/DropSetsOnShutdown", 53, 0x25bc8a31),
        ("context/Owner/PendingThenAbort", 53, 0x62866f36),
        ("context/Owner/PendingThenPanic", 58, 0x2da19955),
        ("context/Owner/PendingThenReady", 55, 0x41040f7d),
        ("context/Owner/PollSetsThenPanics", 53, 0x0623ec78),
        ("drop_panic/Group/Abort", 32, 0x3c75131d),
        ("drop_panic/Group/AbortAll", 32, 0x4f09aa4e),
        ("drop_panic/Group/Ready", 27, 0x082dfdc0),
        ("drop_panic/Hosted/Call/Ready", 85, 0x168dc895),
        ("drop_panic/Hosted/Call/Shutdown", 85, 0xa11002fa),
        ("drop_panic/Hosted/Spawn/Ready", 79, 0xe7deedce),
        ("drop_panic/Hosted/Spawn/Shutdown", 83, 0x1ed65bb4),
        ("drop_panic/Owner/Call/Ready", 54, 0x02bcc911),
        ("drop_panic/Owner/Call/Shutdown", 54, 0xd11e398e),
        ("drop_panic/Owner/Spawn/Ready", 48, 0x27720869),
        ("drop_panic/Owner/Spawn/Shutdown", 52, 0xe8e94eff),
        ("group/tracked/Abort", 31, 0xa91675da),
        ("group/tracked/AbortAll", 31, 0x6674829a),
        ("group/tracked/Cancelled", 40, 0x4a40024f),
        ("group/tracked/Closed", 12, 0xa7eddd87),
        ("group/tracked/FactoryPanic", 16, 0x4d16f6e2),
        ("group/tracked/PollPanic", 36, 0xc32c1a6a),
        ("group/tracked/Ready", 33, 0xcda91beb),
        ("group/tracked/UnpolledAbort", 26, 0xe2f2106e),
        ("group/untracked/AbortAll", 31, 0xfeaf53c8),
        ("group/untracked/Cancelled", 40, 0x820a198f),
        ("group/untracked/Closed", 12, 0xa7eddd87),
        ("group/untracked/FactoryPanic", 16, 0x4d16f6e2),
        ("group/untracked/PollPanic", 36, 0x4743fe47),
        ("group/untracked/Ready", 33, 0x411b19bf),
        ("group/untracked/UnpolledAbort", 26, 0x8b5356df),
        ("poll_drop_panic/Group", 37, 0xb094f9cc),
        ("poll_drop_panic/Hosted", 95, 0x0452f915),
        ("poll_drop_panic/HostedWorker", 89, 0x223b4c5c),
        ("poll_drop_panic/Owner", 59, 0x4b556ec1),
        ("unwinding/Hosted", 190, 0x2e4b3e68),
        ("unwinding/Owner", 95, 0xe1a689a7),
    ];
}
