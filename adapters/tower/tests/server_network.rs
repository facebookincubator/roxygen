/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::num::NonZeroU32;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::sync::Arc;

use bytes::Bytes;
use http::HeaderMap;
use http::Response;
use http_body::Frame;
use http_body_util::BodyExt;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::OriginSecurity;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::http::CodecProtocol;
use roxygen_hyper_adapter::source::ApplicationBodyError;
use roxygen_runtime::context::RuntimeContext;
use roxygen_runtime::worker::WorkerHandle;
use tokio::sync::mpsc;

use self::fixture::Gate;
use self::fixture::Probe;
use self::fixture::TestBody;
use self::fixture::TestError;
use self::fixture::assert_native_error_response;
use self::fixture::request;
use self::fixture::run_test;
use self::fixture::service;
use self::fixture::start;
use crate::server::Http1ConnectionSwitchRejection;
use crate::server::ServerExchange;
use crate::server::ServerFailure;
use crate::server::ServerOptions;

pub(crate) mod fixture {
    use std::any::Any;
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::io;
    use std::marker::PhantomPinned;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::resume_unwind;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::FutureExt;
    use futures::future::poll_fn;
    use futures::task::AtomicWaker;
    use http::Request;
    use http::Response;
    use http_body::Body;
    use http_body::Frame;
    use http_body::SizeHint;
    use http_body_util::BodyExt;
    use hyper::body::Incoming;
    use hyper::rt::Executor;
    use hyper_util::rt::TokioIo;
    use roxygen_core::connection::TransportEndpoint;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::events::HttpBodyEvent;
    use roxygen_core::events::HttpHeaderEvent;
    use roxygen_core::http::CodecProtocol;
    use roxygen_core::message::HttpMessage;
    use roxygen_core::message::SocketAddress;
    use roxygen_core::source::HttpSource;
    use roxygen_core::source::LocalSourceFuture;
    use roxygen_core::source_holder::HttpSourceHolder;
    use roxygen_hyper_adapter::body::RoxygenBody;
    use roxygen_runtime::managed::RuntimeConfig;
    use roxygen_runtime::managed::RuntimeHandle;
    use roxygen_runtime::managed::RuntimeOwner;
    use roxygen_runtime::worker::WorkerHandle;
    use roxygen_transport::byte_stream::PeerIdentity;
    use roxygen_transport::established::EstablishedIo;
    use tokio::io::AsyncRead;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWrite;
    use tokio::io::ReadBuf;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::runtime::Builder;
    use tokio::runtime::Handle;
    use tokio::sync::Notify;
    use tokio::sync::OwnedSemaphorePermit;
    use tokio::sync::Semaphore;
    use tokio::sync::oneshot;
    use tokio::task::AbortHandle;
    use tokio::task::JoinError;
    use tokio::task::JoinSet;
    use tower_service::Service;

    use crate::server::ServerConnection;
    use crate::server::ServerError;
    use crate::server::ServerOptions;
    use crate::server::ServerRun;
    use crate::server::prepare_connection;

    pub const TIMEOUT: Duration = Duration::from_secs(10);
    pub type Outcome = Result<(), Arc<ServerError>>;

    pub type Failure = Box<dyn Any + Send>;
    #[derive(Default)]
    pub struct Failures(Mutex<Option<Failure>>);
    impl Failures {
        pub fn record(&self, failure: Failure) {
            let retired = {
                let mut first = self.0.lock().unwrap_or_else(|error| error.into_inner());
                if first.is_none() {
                    *first = Some(failure);
                    None
                } else {
                    Some(failure)
                }
            };
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(retired))) {
                roxygen_runtime::panic::discard_panic(payload);
            }
        }
        pub fn capture<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
            match catch_unwind(AssertUnwindSafe(operation)) {
                Ok(value) => Some(value),
                Err(payload) => {
                    self.record(payload);
                    None
                }
            }
        }
        pub fn take(&self) -> Option<Failure> {
            self.0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
        }
        pub fn failed(&self) -> bool {
            self.0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_some()
        }
    }
    impl Drop for Failures {
        fn drop(&mut self) {
            let payload = self
                .0
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                roxygen_runtime::panic::discard_panic(secondary);
            }
        }
    }

    struct Owner {
        stop: Box<dyn Fn() + Send + Sync>,
        force: Box<dyn Fn() + Send + Sync>,
        reap: Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    }
    #[derive(Default)]
    struct Registry {
        owners: Mutex<Vec<Owner>>,
        failures: Arc<Failures>,
    }
    thread_local! { static OWNERS: RefCell<Option<Arc<Registry>>> = const { RefCell::new(None) }; }
    pub struct FixtureOwners {
        registry: Arc<Registry>,
        previous: Option<Arc<Registry>>,
    }
    impl FixtureOwners {
        pub fn new() -> Self {
            let registry = Arc::new(Registry::default());
            let previous = OWNERS.with(|slot| slot.replace(Some(Arc::clone(&registry))));
            Self { registry, previous }
        }
        pub fn failures(&self) -> &Arc<Failures> {
            &self.registry.failures
        }
        pub async fn run_case<Work: Future<Output = ()>>(&self, work: Work) {
            self.run_case_until(TIMEOUT, work).await;
        }
        pub async fn run_case_until<Work: Future<Output = ()>>(
            &self,
            timeout: Duration,
            work: Work,
        ) {
            let mut work = Some(Box::pin(
                AssertUnwindSafe(tokio::time::timeout(timeout, work)).catch_unwind(),
            ));
            match work.as_mut().unwrap().as_mut().await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => self
                    .registry
                    .failures
                    .record(Box::new("fixture case timeout")),
                Err(payload) => self.registry.failures.record(payload),
            }
            self.registry.failures.capture(|| drop(work.take()));
        }
        pub async fn finish(&self) {
            let owners = std::mem::take(&mut *self.registry.owners.lock().unwrap());
            for owner in &owners {
                self.registry.failures.capture(|| (owner.stop)());
            }
            for owner in &owners {
                let mut reaped = (owner.reap)();
                if tokio::time::timeout(TIMEOUT, &mut reaped).await.is_err() {
                    self.registry
                        .failures
                        .record(Box::new("fixture cleanup exceeded cancellation deadline"));
                    for owner in &owners {
                        self.registry.failures.capture(|| (owner.force)());
                    }
                    reaped.await;
                }
            }
        }
        pub fn report(&self) {
            if let Some(payload) = self.registry.failures.take() {
                resume_unwind(payload);
            }
        }
    }
    impl Drop for FixtureOwners {
        fn drop(&mut self) {
            OWNERS.with(|slot| {
                slot.replace(self.previous.take());
            });
        }
    }

    struct TaskState<T> {
        tasks: JoinSet<T>,
        closed: bool,
        abort: bool,
        stops: Vec<Arc<dyn Fn() + Send + Sync>>,
    }
    pub struct FixtureTasks<T: Send + 'static> {
        state: Arc<Mutex<TaskState<T>>>,
        failures: Arc<Failures>,
    }
    impl<T: Send + 'static> FixtureTasks<T> {
        pub fn new() -> Self {
            Self::create(true)
        }
        fn create(abort: bool) -> Self {
            let registry = OWNERS
                .with(|slot| slot.borrow().clone())
                .expect("fixture root owns every task group");
            let state = Arc::new(Mutex::new(TaskState {
                tasks: JoinSet::new(),
                closed: false,
                abort,
                stops: Vec::new(),
            }));
            let stop_state = Arc::clone(&state);
            let force_state = Arc::clone(&state);
            let reap_state = Arc::clone(&state);
            let failures = Arc::clone(&registry.failures);
            let stop_failures = Arc::clone(&failures);
            let reap_failures = Arc::clone(&failures);
            let owner = Owner {
                force: Box::new(move || force_state.lock().unwrap().tasks.abort_all()),
                stop: Box::new(move || {
                    let stops = {
                        let mut state = stop_state.lock().unwrap();
                        state.closed = true;
                        if state.abort {
                            state.tasks.abort_all();
                        }
                        state.stops.clone()
                    };
                    for stop in stops {
                        stop_failures.capture(|| stop());
                    }
                }),
                reap: Box::new(move || {
                    let state = Arc::clone(&reap_state);
                    let failures = Arc::clone(&reap_failures);
                    Box::pin(async move {
                        while let Some(result) =
                            poll_fn(|context| state.lock().unwrap().tasks.poll_join_next(context))
                                .await
                        {
                            match result {
                                Ok(value) => {
                                    failures.capture(|| drop(value));
                                }
                                Err(error) if error.is_panic() => {
                                    failures.record(error.into_panic())
                                }
                                Err(_) => {}
                            }
                        }
                    })
                }),
            };
            {
                let mut owners = registry.owners.lock().unwrap();
                assert!(owners.len() < 128, "bounded fixture owner inventory");
                owners.push(owner);
            }
            Self { state, failures }
        }
        pub fn track_run(&self, run: &ServerRun) {
            self.state
                .lock()
                .unwrap()
                .stops
                .push(crate::server::cancel_run(run));
        }
        pub fn try_spawn<Work>(&self, work: Work) -> Option<AbortHandle>
        where
            Work: Future<Output = T> + Send + 'static,
        {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                drop(state);
                self.failures.capture(|| drop(work));
                return None;
            }
            assert!(state.tasks.len() < 128, "bounded fixture child inventory");
            Some(state.tasks.spawn(work))
        }
        pub fn spawn<Work>(&mut self, work: Work) -> AbortHandle
        where
            Work: Future<Output = T> + Send + 'static,
        {
            self.try_spawn(work).expect("open fixture parent")
        }
        pub fn len(&self) -> usize {
            self.state.lock().unwrap().tasks.len()
        }
        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }
        pub fn abort_all(&mut self) {
            self.state.lock().unwrap().tasks.abort_all();
        }
        pub fn poll_join_next(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<Option<Result<T, JoinError>>> {
            self.state.lock().unwrap().tasks.poll_join_next(context)
        }
        pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
            poll_fn(|context| self.poll_join_next(context)).await
        }
    }

    pub fn run_test<Work: Future<Output = ()>>(
        work: impl FnOnce(WorkerHandle, RuntimeHandle) -> Work,
    ) {
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 32,
        })
        .unwrap();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let handle = owner.handle();
        let worker = handle.worker(0).unwrap();
        let owners = FixtureOwners::new();
        runtime.block_on(async {
            if let Some(work) = owners.failures().capture(|| work(worker, handle.clone())) {
                owners.run_case(work).await;
            }
            let (_, quiesced) = tokio::join!(owners.finish(), handle.quiesce(TIMEOUT));
            if quiesced.is_err() {
                owners
                    .failures()
                    .record(Box::new("native quiescence failed"));
            }
        });
        match owner.join() {
            Ok(joined) => {
                if !owners.failures().failed() {
                    owners.failures().capture(|| {
                        assert_eq!(joined.workers_joined, 1);
                        assert_eq!(joined.workers_forced, 0);
                    });
                }
            }
            Err(_) => owners
                .failures()
                .record(Box::new("native physical join failed")),
        }
        drop(runtime);
        owners.report();
    }

    #[derive(Default)]
    struct GateState {
        open: AtomicBool,
        entered: AtomicBool,
        waker: AtomicWaker,
        notification: Notify,
    }

    #[derive(Clone, Default)]
    pub struct Gate(Arc<GateState>);

    impl Gate {
        pub fn is_released(&self) -> bool {
            self.0.open.load(Ordering::SeqCst)
        }
        pub fn release(&self) {
            self.0.open.store(true, Ordering::SeqCst);
            self.0.waker.wake();
        }
        pub fn poll(&self, context: &mut Context<'_>) -> Poll<()> {
            self.0.entered.store(true, Ordering::SeqCst);
            self.0.notification.notify_one();
            self.0.waker.register(context.waker());
            if self.0.open.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
        pub async fn wait(&self) {
            poll_fn(|context| self.poll(context)).await;
        }
        pub async fn entered(&self) {
            loop {
                let notified = self.0.notification.notified();
                if self.0.entered.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        }
    }

    #[derive(Clone)]
    pub struct Probe {
        owner: ThreadId,
        events: Arc<Mutex<Vec<(&'static str, ThreadId, bool)>>>,
    }

    impl Probe {
        pub fn new() -> Self {
            Self {
                owner: std::thread::current().id(),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
        pub fn record(&self, event: &'static str) {
            self.events.lock().unwrap().push((
                event,
                std::thread::current().id(),
                Handle::try_current().is_ok(),
            ));
        }
        pub fn count(&self, event: &str) -> usize {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| entry.0 == event)
                .count()
        }
        pub fn assert_app(&self, event: &str) {
            let events = self.events.lock().unwrap();
            let matching: Vec<_> = events.iter().filter(|entry| entry.0 == event).collect();
            assert!(!matching.is_empty(), "missing {event}");
            for entry in matching {
                assert_eq!(entry.1, self.owner, "{event}");
                assert!(entry.2);
            }
        }
        pub fn assert_native(&self, event: &str) {
            let events = self.events.lock().unwrap();
            let matching: Vec<_> = events.iter().filter(|entry| entry.0 == event).collect();
            assert!(!matching.is_empty(), "missing {event}");
            for entry in matching {
                assert_ne!(entry.1, self.owner, "{event}");
                assert!(entry.2);
            }
        }
    }

    #[derive(Clone, Debug, thiserror::Error)]
    #[error("typed application marker")]
    pub struct TestError(pub Arc<()>);

    enum Content {
        Frames(VecDeque<Result<Frame<Bytes>, TestError>>),
        Echo(Pin<Box<RoxygenBody>>),
    }

    pub struct TestBody {
        content: Mutex<Content>,
        gate: Option<Gate>,
        trailer_gate: Option<Gate>,
        exact_size: bool,
        hint_observer: Option<Arc<dyn Fn(&'static str) + Send + Sync>>,
        probe: Option<Probe>,
        not_sync: Cell<()>,
        _pin: PhantomPinned,
        retirement: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl TestBody {
        pub fn frames(frames: Vec<Result<Frame<Bytes>, TestError>>) -> Self {
            Self {
                content: Mutex::new(Content::Frames(frames.into())),
                gate: None,
                trailer_gate: None,
                exact_size: false,
                hint_observer: None,
                probe: None,
                not_sync: Cell::new(()),
                _pin: PhantomPinned,
                retirement: None,
            }
        }
        pub fn data(data: &'static [u8]) -> Self {
            Self::frames(vec![Ok(Frame::data(Bytes::from_static(data)))])
        }
        pub fn empty() -> Self {
            Self::frames(Vec::new())
        }
        pub fn gated(mut self, gate: Gate) -> Self {
            self.gate = Some(gate);
            self
        }
        pub fn gated_trailers(mut self, gate: Gate) -> Self {
            self.trailer_gate = Some(gate);
            self
        }
        pub fn exact_size(mut self) -> Self {
            self.exact_size = true;
            self
        }
        pub fn observe_hints(
            mut self,
            observer: impl Fn(&'static str) + Send + Sync + 'static,
        ) -> Self {
            self.hint_observer = Some(Arc::new(observer));
            self
        }
        pub fn probed(mut self, probe: Probe) -> Self {
            self.probe = Some(probe);
            self
        }
        pub fn on_drop(mut self, retirement: impl Fn() + Send + Sync + 'static) -> Self {
            self.retirement = Some(Arc::new(retirement));
            self
        }
        pub fn echo(body: RoxygenBody, probe: Probe) -> Self {
            Self {
                content: Mutex::new(Content::Echo(Box::pin(body))),
                gate: None,
                trailer_gate: None,
                exact_size: false,
                hint_observer: None,
                probe: Some(probe),
                not_sync: Cell::new(()),
                _pin: PhantomPinned,
                retirement: None,
            }
        }
    }

    impl Body for TestBody {
        type Data = Bytes;
        type Error = TestError;
        fn poll_frame(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, TestError>>> {
            let this = self.as_ref().get_ref();
            this.not_sync.set(());
            if let Some(probe) = &this.probe {
                probe.record("body poll");
            }
            if let Some(gate) = &this.gate
                && gate.poll(context).is_pending()
            {
                return Poll::Pending;
            }
            match &mut *this.content.lock().unwrap() {
                Content::Frames(frames) => {
                    if frames
                        .front()
                        .is_some_and(|frame| frame.as_ref().is_ok_and(Frame::is_trailers))
                        && let Some(gate) = &this.trailer_gate
                        && gate.poll(context).is_pending()
                    {
                        return Poll::Pending;
                    }
                    let frame = frames.pop_front();
                    if frame.is_none()
                        && let Some(probe) = &this.probe
                    {
                        probe.record("body eof");
                    }
                    Poll::Ready(frame)
                }
                Content::Echo(body) => body
                    .as_mut()
                    .poll_frame(context)
                    .map(|frame| frame.map(|frame| frame.map_err(|_| TestError(Arc::new(()))))),
            }
        }
        fn is_end_stream(&self) -> bool {
            if let Some(observer) = &self.hint_observer {
                observer("body terminal hint");
            }
            match &*self.content.lock().unwrap() {
                Content::Frames(frames) => frames.is_empty(),
                Content::Echo(body) => body.is_end_stream(),
            }
        }
        fn size_hint(&self) -> SizeHint {
            if let Some(observer) = &self.hint_observer {
                observer("body size hint");
            }
            if !self.exact_size {
                return SizeHint::new();
            }
            match &*self.content.lock().unwrap() {
                Content::Frames(frames) => {
                    let remaining = frames.iter().try_fold(0_u64, |remaining, frame| {
                        let frame = frame.as_ref().ok()?;
                        remaining.checked_add(frame.data_ref().map_or(0, |data| data.len() as u64))
                    });
                    remaining.map_or_else(SizeHint::new, SizeHint::with_exact)
                }
                Content::Echo(body) => body.size_hint(),
            }
        }
    }

    impl Drop for TestBody {
        fn drop(&mut self) {
            if let Some(probe) = &self.probe {
                probe.record("body drop");
            }
            if let Some(retirement) = &self.retirement {
                retirement();
            }
        }
    }

    pub type ServiceFuture =
        Pin<Box<dyn Future<Output = Result<Response<TestBody>, TestError>> + Send>>;
    pub struct TestService {
        call: Box<dyn FnMut(Request<RoxygenBody>) -> ServiceFuture + Send>,
        pub readiness: Option<Gate>,
        probe: Probe,
        not_sync: Cell<usize>,
    }

    pub fn service<Work, Callback>(probe: Probe, mut callback: Callback) -> TestService
    where
        Work: Future<Output = Result<Response<TestBody>, TestError>> + Send + 'static,
        Callback: FnMut(Request<RoxygenBody>) -> Work + Send + 'static,
    {
        TestService {
            call: Box::new(move |request| Box::pin(callback(request))),
            readiness: None,
            probe,
            not_sync: Cell::new(0),
        }
    }

    struct FutureProbe(Probe);
    impl Drop for FutureProbe {
        fn drop(&mut self) {
            self.0.record("service future drop");
        }
    }
    struct ServiceProbeFuture {
        future: ServiceFuture,
        _guard: FutureProbe,
    }
    impl Future for ServiceProbeFuture {
        type Output = Result<Response<TestBody>, TestError>;
        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            self.future.as_mut().poll(context)
        }
    }

    impl Service<Request<RoxygenBody>> for TestService {
        type Response = Response<TestBody>;
        type Error = TestError;
        type Future = ServiceFuture;
        fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), TestError>> {
            self.probe.record("service ready");
            self.readiness
                .as_ref()
                .map_or(Poll::Ready(()), |gate| gate.poll(context))
                .map(Ok)
        }
        fn call(&mut self, request: Request<RoxygenBody>) -> Self::Future {
            self.probe.record("service call");
            self.not_sync.set(self.not_sync.get() + 1);
            let future = (self.call)(request);
            let guard = FutureProbe(self.probe.clone());
            Box::pin(ServiceProbeFuture {
                future,
                _guard: guard,
            })
        }
    }
    impl Drop for TestService {
        fn drop(&mut self) {
            self.probe.record("service drop");
        }
    }

    pub struct TestIo {
        stream: Mutex<TcpStream>,
        pub shutdown: Option<Gate>,
        probe: Probe,
        _pin: PhantomPinned,
        pub read_failure: Option<Gate>,
        pub retirement: Option<BlockingRetirement>,
    }
    pub struct BlockingRetirement {
        entered: Gate,
        release: std::sync::mpsc::Receiver<()>,
    }
    pub struct BlockingRelease {
        entered: Gate,
        release: std::sync::mpsc::SyncSender<()>,
    }
    pub fn retirement_gate() -> (BlockingRetirement, BlockingRelease) {
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let entered = Gate::default();
        (
            BlockingRetirement {
                entered: entered.clone(),
                release: receive,
            },
            BlockingRelease {
                entered,
                release: send,
            },
        )
    }
    impl BlockingRelease {
        pub async fn entered(&self) {
            self.entered.entered().await;
        }
        pub fn release(self) {
            let _ = self.release.send(());
        }
    }
    impl AsyncRead for TestIo {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.as_ref().get_ref();
            this.probe.record("io read");
            if let Some(gate) = &this.read_failure
                && gate.poll(context).is_ready()
            {
                return Poll::Ready(Err(io::Error::other("initiating native read marker")));
            }
            Pin::new(&mut *this.stream.lock().unwrap()).poll_read(context, buffer)
        }
    }
    impl AsyncWrite for TestIo {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut *self.as_ref().get_ref().stream.lock().unwrap())
                .poll_write(context, bytes)
        }
        fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut *self.as_ref().get_ref().stream.lock().unwrap()).poll_flush(context)
        }
        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.as_ref().get_ref();
            this.probe.record("io shutdown");
            if let Some(gate) = &this.shutdown
                && gate.poll(context).is_pending()
            {
                return Poll::Pending;
            }
            Pin::new(&mut *this.stream.lock().unwrap()).poll_shutdown(context)
        }
    }
    impl Drop for TestIo {
        fn drop(&mut self) {
            if let Some(retirement) = &self.retirement {
                let _ = retirement
                    .entered
                    .poll(&mut Context::from_waker(std::task::Waker::noop()));
                let _ = retirement.release.recv();
            }
            self.probe.record("io drop");
        }
    }

    pub async fn sockets(
        probe: Probe,
        shutdown: Option<Gate>,
    ) -> (EstablishedIo<TestIo>, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let (stream, peer) = accepted.unwrap();
        let local = stream.local_addr().unwrap();
        let endpoint = |address: std::net::SocketAddr| {
            TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
        };
        let established = EstablishedIo::new(
            TestIo {
                stream: Mutex::new(stream),
                shutdown,
                probe,
                _pin: PhantomPinned,
                read_failure: None,
                retirement: None,
            },
            endpoint(local),
            endpoint(peer),
            Some(PeerIdentity::new(b"verified-fixture-peer")),
        );
        (established, client.unwrap())
    }

    struct PeerState {
        tasks: FixtureTasks<()>,
        closed: bool,
    }
    pub struct PeerTasks(Arc<Mutex<PeerState>>);
    #[derive(Clone)]
    pub struct PeerExecutor(Arc<Mutex<PeerState>>);
    impl PeerTasks {
        pub fn new() -> Self {
            Self(Arc::new(Mutex::new(PeerState {
                tasks: FixtureTasks::new(),
                closed: false,
            })))
        }
        pub fn executor(&self) -> PeerExecutor {
            PeerExecutor(Arc::clone(&self.0))
        }
        pub fn len(&self) -> usize {
            self.0.lock().unwrap().tasks.len()
        }
        pub async fn finish(&mut self) {
            {
                let mut state = self.0.lock().unwrap();
                state.closed = true;
                state.tasks.abort_all();
            }
            while let Some(result) =
                poll_fn(|context| self.0.lock().unwrap().tasks.poll_join_next(context)).await
            {
                if let Err(error) = result
                    && error.is_panic()
                {
                    let failures = Arc::clone(&self.0.lock().unwrap().tasks.failures);
                    failures.record(error.into_panic());
                }
            }
            assert_eq!(self.len(), 0);
        }
    }
    impl<Work> Executor<Work> for PeerExecutor
    where
        Work: Future + Send + 'static,
        Work::Output: Send + 'static,
    {
        fn execute(&self, work: Work) {
            let state = self.0.lock().unwrap();
            if state.closed {
                let failures = Arc::clone(&state.tasks.failures);
                drop(state);
                failures.capture(|| drop(work));
                return;
            }
            assert!(state.tasks.len() < 128, "bounded peer task admission");
            state.tasks.try_spawn(async move {
                let _ = work.await;
            });
        }
    }

    pub enum PeerConnection {
        H1(hyper::client::conn::http1::SendRequest<TestBody>),
        H2(hyper::client::conn::http2::SendRequest<Pin<Box<TestBody>>>),
    }

    pub enum RejectionWire {
        Plain(Pin<Box<TestIo>>),
        ServerTls(Pin<Box<tokio_rustls::server::TlsStream<Pin<Box<TestIo>>>>>),
        ClientPlain(TcpStream),
        ClientTls(Pin<Box<tokio_rustls::client::TlsStream<TcpStream>>>),
    }

    impl AsyncRead for RejectionWire {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Self::Plain(stream) => stream.as_mut().poll_read(context, buffer),
                Self::ServerTls(stream) => stream.as_mut().poll_read(context, buffer),
                Self::ClientPlain(stream) => Pin::new(stream).poll_read(context, buffer),
                Self::ClientTls(stream) => stream.as_mut().poll_read(context, buffer),
            }
        }
    }

    impl AsyncWrite for RejectionWire {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.get_mut() {
                Self::Plain(stream) => stream.as_mut().poll_write(context, bytes),
                Self::ServerTls(stream) => stream.as_mut().poll_write(context, bytes),
                Self::ClientPlain(stream) => Pin::new(stream).poll_write(context, bytes),
                Self::ClientTls(stream) => stream.as_mut().poll_write(context, bytes),
            }
        }
        fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Self::Plain(stream) => stream.as_mut().poll_flush(context),
                Self::ServerTls(stream) => stream.as_mut().poll_flush(context),
                Self::ClientPlain(stream) => Pin::new(stream).poll_flush(context),
                Self::ClientTls(stream) => stream.as_mut().poll_flush(context),
            }
        }
        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Self::Plain(stream) => stream.as_mut().poll_shutdown(context),
                Self::ServerTls(stream) => stream.as_mut().poll_shutdown(context),
                Self::ClientPlain(stream) => Pin::new(stream).poll_shutdown(context),
                Self::ClientTls(stream) => stream.as_mut().poll_shutdown(context),
            }
        }
    }

    #[derive(Clone)]
    pub struct RejectionIoControl {
        pub write: Gate,
        pub shutdown: Gate,
        pub fail_write: Arc<AtomicBool>,
        pub probe: Probe,
    }

    impl RejectionIoControl {
        pub fn new() -> Self {
            Self {
                write: Gate::default(),
                shutdown: Gate::default(),
                fail_write: Arc::new(AtomicBool::new(false)),
                probe: Probe::new(),
            }
        }
    }

    pub struct RejectionIo {
        stream: Mutex<RejectionWire>,
        control: RejectionIoControl,
        retirement: Option<BlockingRetirement>,
        _pin: PhantomPinned,
    }

    impl AsyncRead for RejectionIo {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut *self.as_ref().get_ref().stream.lock().unwrap())
                .poll_read(context, buffer)
        }
    }

    impl AsyncWrite for RejectionIo {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.as_ref().get_ref();
            if this.control.write.poll(context).is_pending() {
                return Poll::Pending;
            }
            if this.control.fail_write.load(Ordering::SeqCst) {
                return Poll::Ready(Err(io::Error::other("rejection write failure")));
            }
            Pin::new(&mut *this.stream.lock().unwrap()).poll_write(context, bytes)
        }
        fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut *self.as_ref().get_ref().stream.lock().unwrap()).poll_flush(context)
        }
        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.as_ref().get_ref();
            this.control.probe.record("rejection io shutdown");
            if this.control.shutdown.poll(context).is_pending() {
                return Poll::Pending;
            }
            Pin::new(&mut *this.stream.lock().unwrap()).poll_shutdown(context)
        }
    }

    impl Drop for RejectionIo {
        fn drop(&mut self) {
            if let Some(retirement) = &self.retirement {
                let _ = retirement
                    .entered
                    .poll(&mut Context::from_waker(Waker::noop()));
                let _ = retirement.release.recv();
            }
            self.control.probe.record("adopted io drop");
        }
    }

    pub async fn rejection_sockets(
        tls: bool,
        control: RejectionIoControl,
        retirement: Option<BlockingRetirement>,
    ) -> (EstablishedIo<RejectionIo>, RejectionWire) {
        let (established, peer) = sockets(control.probe.clone(), None).await;
        let EstablishedIo {
            io,
            local_endpoint,
            peer_endpoint,
            peer_identity,
        } = established;
        let (server, peer) = if tls {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let generated =
                rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
            let certificate = generated.cert.der().clone();
            let key = rustls::pki_types::PrivateKeyDer::try_from(
                generated.signing_key.serialized_der().to_vec(),
            )
            .unwrap();
            let mut server = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![certificate.clone()], key)
                .unwrap();
            server.alpn_protocols = vec![b"http/1.1".to_vec()];
            let mut roots = rustls::RootCertStore::empty();
            roots.add(certificate).unwrap();
            let mut client = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            client.alpn_protocols = vec![b"http/1.1".to_vec()];
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
            let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
            let (server, client) = tokio::join!(
                acceptor.accept(Box::pin(io)),
                connector.connect(
                    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    peer
                ),
            );
            let server = server.unwrap();
            let client = client.unwrap();
            assert_eq!(
                server.get_ref().1.alpn_protocol(),
                Some(b"http/1.1".as_slice())
            );
            assert_eq!(
                client.get_ref().1.alpn_protocol(),
                Some(b"http/1.1".as_slice())
            );
            (
                RejectionWire::ServerTls(Box::pin(server)),
                RejectionWire::ClientTls(Box::pin(client)),
            )
        } else {
            (
                RejectionWire::Plain(Box::pin(io)),
                RejectionWire::ClientPlain(peer),
            )
        };
        (
            EstablishedIo::new(
                RejectionIo {
                    stream: Mutex::new(server),
                    control,
                    retirement,
                    _pin: PhantomPinned,
                },
                local_endpoint,
                peer_endpoint,
                if tls { None } else { peer_identity },
            ),
            peer,
        )
    }

    #[derive(Clone, Copy, Debug)]
    pub enum SwitchRequest {
        Connect,
        WebSocket,
        OtherUpgrade,
    }

    pub fn switch_request(kind: SwitchRequest, version: &str, lookahead: bool) -> Vec<u8> {
        let (request, headers) = match kind {
            SwitchRequest::Connect => ("CONNECT fixture.invalid:443", ""),
            SwitchRequest::WebSocket => (
                "GET /upgrade",
                "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n",
            ),
            SwitchRequest::OtherUpgrade => (
                "GET /upgrade",
                "Connection: Upgrade\r\nUpgrade: future-protocol\r\n",
            ),
        };
        let mut wire =
            format!("{request} HTTP/{version}\r\nHost: fixture.invalid\r\n{headers}\r\n")
                .into_bytes();
        if lookahead {
            wire.extend_from_slice(
                b"GET /must-not-dispatch HTTP/1.1\r\nHost: fixture.invalid\r\n\r\n",
            );
        }
        wire
    }

    pub fn unfinished_switch_request() -> Vec<u8> {
        let mut wire = switch_request(SwitchRequest::WebSocket, "1.1", false);
        wire.truncate(wire.len() - 2);
        wire.extend_from_slice(b"Content-Length: 1048576\r\n\r\nuncompleted upload");
        wire
    }

    pub async fn read_rejection(peer: &mut RejectionWire, status: u16, version: &str) {
        let mut bytes = Vec::new();
        let mut buffer = [0; 512];
        loop {
            let read = peer.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            assert!(bytes.len() <= 8192, "bounded rejection response");
        }
        let end = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        assert_eq!(
            end,
            bytes.len(),
            "empty entity and no second response; expected HTTP/{version} {status}; bounded raw reply: {:?}",
            String::from_utf8_lossy(&bytes),
        );
        let headers = std::str::from_utf8(&bytes).unwrap();
        assert!(
            headers.starts_with(&format!("HTTP/{version} {status} ")),
            "{headers}"
        );
        let headers = headers.to_ascii_lowercase();
        assert!(headers.contains("\r\ncontent-length: 0\r\n"), "{headers}");
        assert!(headers.contains("\r\nconnection: close\r\n"), "{headers}");
        assert!(!headers.contains("\r\nupgrade:"));
        assert!(!headers.contains("\r\nx-mononoke-host:"));
    }

    struct ProbedSource {
        source: HttpSourceHolder,
        probe: Probe,
        mutate: Option<fn(&mut HttpMessage)>,
    }

    pub fn observe_source(
        source: HttpSourceHolder,
        probe: Probe,
        mutate: Option<fn(&mut HttpMessage)>,
    ) -> HttpSourceHolder {
        HttpSourceHolder::new(Box::new(ProbedSource {
            source,
            probe,
            mutate,
        }))
    }

    impl HttpSource for ProbedSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            let header = self.source.read_header_event();
            let probe = self.probe.clone();
            let mutate = self.mutate;
            Box::pin(async move {
                let mut header = header.await?;
                probe.record("native request head");
                if header.eom() {
                    probe.record("native request eom");
                }
                if header.headers().is_upgraded() {
                    probe.record("native upgraded flag");
                }
                if header.headers().is_ingress_websocket_upgrade() {
                    probe.record("native websocket flag");
                }
                if let Some(mutate) = mutate {
                    mutate(header.headers_mut());
                }
                Ok(header)
            })
        }
        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.probe.record("request body read");
            self.source.read_body_event(maximum)
        }
        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.probe.record("request source stop");
            if error.is_some_and(|code| code != HttpErrorCode::NO_ERROR) {
                self.probe.record("request source error");
            }
            self.source.stop_reading(error);
        }
    }

    impl Drop for ProbedSource {
        fn drop(&mut self) {
            self.probe.record("request source drop");
        }
    }

    pub async fn observe_rejection(
        worker: WorkerHandle,
        probe: Probe,
        mutate: Option<fn(&mut HttpMessage)>,
    ) -> (
        impl Future<Output = Result<(), roxygen_runtime::worker::WorkerCleanupObservationError>> + Send,
        impl Future<Output = Result<(), roxygen_runtime::worker::WorkerCleanupObservationError>> + Send,
    ) {
        let (watcher, watched) = oneshot::channel();
        let (ingress, retired) = oneshot::channel();
        worker
            .call(move |_| {
                let mut watcher = Some(watcher);
                let mut ingress = Some(ingress);
                crate::server::install_setup_hook(move |phase, receipt| {
                    let sender = match phase {
                        "watcher activation" => watcher.take().unwrap(),
                        "ingress prepared" => ingress.take().unwrap(),
                        _ => panic!("unexpected setup phase: {phase}"),
                    };
                    assert!(sender.send(receipt).is_ok());
                });
                crate::server::install_head_hook(move |source| {
                    observe_source(source, probe.clone(), mutate)
                });
            })
            .unwrap()
            .await
            .unwrap();
        (
            async move { retired.await.unwrap().wait().await },
            async move { watched.await.unwrap().wait().await },
        )
    }

    pub async fn pending_rejection_read(
        mut peer: RejectionWire,
    ) -> FixtureTasks<io::Result<usize>> {
        let pending = Gate::default();
        let observed = pending.clone();
        let mut readers = FixtureTasks::new();
        readers.spawn(async move {
            let mut byte = [0; 1];
            let read = peer.read(&mut byte);
            tokio::pin!(read);
            assert_pending(read.as_mut()).await;
            observed.release();
            read.await
        });
        pending.wait().await;
        readers
    }
    impl PeerConnection {
        pub async fn connect(
            stream: TcpStream,
            protocol: CodecProtocol,
            tasks: &PeerTasks,
        ) -> Self {
            Self::connect_io(stream, protocol, tasks, None).await
        }

        pub async fn connect_gated(
            stream: TcpStream,
            protocol: CodecProtocol,
            tasks: &PeerTasks,
            shutdown: Gate,
            probe: Probe,
        ) -> (Self, oneshot::Receiver<Result<(), hyper::Error>>) {
            let (completed, completion) = oneshot::channel();
            let io = Box::pin(TestIo {
                stream: Mutex::new(stream),
                shutdown: Some(shutdown),
                probe,
                _pin: PhantomPinned,
                read_failure: None,
                retirement: None,
            });
            let peer = Self::connect_io(io, protocol, tasks, Some(completed)).await;
            (peer, completion)
        }

        async fn connect_io<Io>(
            stream: Io,
            protocol: CodecProtocol,
            tasks: &PeerTasks,
            completed: Option<oneshot::Sender<Result<(), hyper::Error>>>,
        ) -> Self
        where
            Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        {
            match protocol {
                CodecProtocol::Http11 => {
                    let (mut sender, connection) =
                        hyper::client::conn::http1::handshake(TokioIo::new(stream))
                            .await
                            .unwrap();
                    tasks.executor().execute(async move {
                        let result = connection.await;
                        if let Some(completed) = completed {
                            let _ = completed.send(result);
                        }
                    });
                    sender.ready().await.unwrap();
                    Self::H1(sender)
                }
                CodecProtocol::Http2 => {
                    let (mut sender, connection) = hyper::client::conn::http2::handshake(
                        tasks.executor(),
                        TokioIo::new(stream),
                    )
                    .await
                    .unwrap();
                    tasks.executor().execute(async move {
                        let result = connection.await;
                        if let Some(completed) = completed {
                            let _ = completed.send(result);
                        }
                    });
                    sender.ready().await.unwrap();
                    Self::H2(sender)
                }
                _ => panic!("fixture only supports H1/H2"),
            }
        }
        pub fn send(
            &mut self,
            request: Request<TestBody>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<Incoming>, hyper::Error>> + Send>>
        {
            match self {
                Self::H1(sender) => Box::pin(sender.send_request(request)),
                Self::H2(sender) => Box::pin(sender.send_request(request.map(Box::pin))),
            }
        }
    }

    pub struct Parent {
        pub slots: Arc<Semaphore>,
        pub tasks: FixtureTasks<(Outcome, OwnedSemaphorePermit)>,
    }
    impl Parent {
        pub fn new(capacity: usize) -> Self {
            Self {
                slots: Arc::new(Semaphore::new(capacity)),
                tasks: FixtureTasks::create(false),
            }
        }
        pub fn reserve(&self) -> OwnedSemaphorePermit {
            Arc::clone(&self.slots)
                .try_acquire_owned()
                .expect("bounded parent admission")
        }
        pub fn spawn(&mut self, run: ServerRun, permit: OwnedSemaphorePermit) {
            self.tasks.track_run(&run);
            self.tasks.spawn(async move { (run.await, permit) });
        }
        pub(super) fn spawn_observed(
            &mut self,
            run: ServerRun,
            permit: OwnedSemaphorePermit,
            context: super::ResponseAppContext,
        ) {
            self.tasks.track_run(&run);
            self.tasks.spawn(async move {
                tokio::pin!(run);
                let result = poll_fn(|task| {
                    context.observe("run before poll");
                    let polled = catch_unwind(AssertUnwindSafe(|| run.as_mut().poll(task)));
                    let restored = catch_unwind(AssertUnwindSafe(|| {
                        context.observe("run after poll");
                    }));
                    match polled {
                        Err(payload) => resume_unwind(payload),
                        Ok(result) => {
                            if let Err(payload) = restored {
                                resume_unwind(payload);
                            }
                            result
                        }
                    }
                })
                .await;
                (result, permit)
            });
        }
        pub async fn reap(&mut self) -> Outcome {
            let (result, permit) = self
                .tasks
                .join_next()
                .await
                .expect("parent owns run")
                .expect("run task did not panic");
            if result
                .as_ref()
                .is_err_and(|error| !error.cleanup_confirmed())
            {
                self.slots.close();
            }
            drop(permit);
            assert!(self.tasks.is_empty());
            result
        }
    }

    pub struct Started {
        pub connection: ServerConnection,
        pub peer: PeerConnection,
        pub parent: Parent,
        pub peers: PeerTasks,
        pub io: Probe,
    }
    pub async fn start(
        worker: WorkerHandle,
        service: TestService,
        options: ServerOptions,
        shutdown: Option<Gate>,
    ) -> Started {
        let protocol = options.protocol;
        let io = Probe::new();
        let (established, stream) = sockets(io.clone(), shutdown).await;
        let mut parent = Parent::new(1);
        let permit = parent.reserve();
        let (startup, run) = prepare_connection(worker, established, service, options).unwrap();
        parent.spawn(run, permit);
        let connection = startup.start().await.unwrap();
        let peers = PeerTasks::new();
        let peer = PeerConnection::connect(stream, protocol, &peers).await;
        Started {
            connection,
            peer,
            parent,
            peers,
            io,
        }
    }
    impl Started {
        pub async fn finish(mut self) -> Outcome {
            self.connection.cancel();
            drop(self.peer);
            self.peers.finish().await;
            let result = self.parent.reap().await;
            self.io.assert_native("io drop");
            assert_eq!(self.io.count("io drop"), 1);
            result
        }
    }

    pub fn request(path: &str, body: TestBody) -> Request<TestBody> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("host", "fixture.invalid")
            .header("trailer", "x-finish")
            .body(body)
            .unwrap()
    }

    pub async fn assert_native_error_response(response: Response<Incoming>, message: &[u8]) {
        assert_eq!(response.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            message,
        );
    }

    pub async fn assert_pending<Work: Future + ?Sized>(mut work: Pin<&mut Work>) {
        poll_fn(|context| {
            assert!(work.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }
}

#[derive(Clone)]
struct ResponseAppContext {
    ambient: RuntimeContext,
    probe: Probe,
}

impl ResponseAppContext {
    fn observe(&self, event: &'static str) {
        assert_eq!(RuntimeContext::current(), Some(self.ambient.clone()));
        self.probe.record(event);
    }
}

#[derive(Clone)]
struct ResponseDispatchContext {
    observed: Option<RuntimeContext>,
    probe: Probe,
}

impl ResponseDispatchContext {
    fn capture(app: &ResponseAppContext, probe: Probe) -> Self {
        let observed = RuntimeContext::current();
        assert_ne!(observed, Some(app.ambient.clone()));
        probe.record("service dispatch context");
        Self { observed, probe }
    }

    fn assert_current(&self) {
        assert_eq!(RuntimeContext::current(), self.observed);
    }

    fn observe_future(&self) {
        self.assert_current();
        self.probe.record("service future context");
    }
}

fn run_response_context_test<Work: Future<Output = ()>>(
    work: impl FnOnce(WorkerHandle, ResponseAppContext) -> Work,
) {
    let caller = RuntimeContext::current();
    let context = ResponseAppContext {
        ambient: RuntimeContext::new(),
        probe: Probe::new(),
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _entered = context.ambient.enter();
        context.observe("caller ambient entered");
        let child_context = context.clone();
        run_test(move |worker, _| work(worker, child_context));
        context.observe("caller ambient returned");
    }));
    let restored = catch_unwind(AssertUnwindSafe(|| {
        assert_eq!(RuntimeContext::current(), caller);
        context.probe.record("caller context restored");
    }));
    if let Err(payload) = result {
        resume_unwind(payload);
    }
    if let Err(payload) = restored {
        resume_unwind(payload);
    }
    for event in [
        "caller ambient entered",
        "caller ambient returned",
        "caller context restored",
    ] {
        assert_eq!(context.probe.count(event), 1);
    }
    context.probe.assert_app("run before poll");
    context.probe.assert_app("run after poll");
    context.probe.assert_app("controller restored");
    assert_eq!(
        context.probe.count("run before poll"),
        context.probe.count("run after poll")
    );
}

struct ResponseLengthPeer {
    running: fixture::Started,
    shutdown: Gate,
    completed: tokio::sync::oneshot::Receiver<Result<(), hyper::Error>>,
    context: ResponseAppContext,
}

impl ResponseLengthPeer {
    async fn start(
        worker: WorkerHandle,
        service: fixture::TestService,
        protocol: CodecProtocol,
        context: &ResponseAppContext,
    ) -> Self {
        context.observe("connection preparation");
        let io = Probe::new();
        let (established, stream) = fixture::sockets(io.clone(), None).await;
        let mut parent = fixture::Parent::new(1);
        let permit = parent.reserve();
        let options = ServerOptions::new(protocol, TransportSecurity::Plaintext);
        let (startup, run) =
            crate::server::prepare_connection(worker, established, service, options).unwrap();
        parent.spawn_observed(run, permit, context.clone());
        let connection = startup.start().await.unwrap();
        context.observe("controller restored");
        let peers = fixture::PeerTasks::new();
        let shutdown = Gate::default();
        let (peer, completed) = fixture::PeerConnection::connect_gated(
            stream,
            protocol,
            &peers,
            shutdown.clone(),
            Probe::new(),
        )
        .await;
        context.observe("controller restored");
        Self {
            running: fixture::Started {
                connection,
                peer,
                parent,
                peers,
                io,
            },
            shutdown,
            completed,
            context: context.clone(),
        }
    }

    async fn finish(mut self) {
        self.context.observe("controller restored");
        assert!(self.running.connection.begin_drain());
        self.running.parent.reap().await.unwrap();
        self.context.observe("controller restored");
        self.shutdown.entered().await;
        self.shutdown.release();
        self.completed.await.unwrap().unwrap();
        self.context.observe("controller restored");
        drop(self.running.peer);
        self.running.peers.finish().await;
        self.context.observe("controller restored");
        assert!(self.running.parent.tasks.is_empty());
        assert_eq!(self.running.parent.slots.available_permits(), 1);
        assert!(!self.running.parent.slots.is_closed());
        assert_eq!(self.running.io.count("io drop"), 1);
        self.running.io.assert_native("io drop");
    }
}

fn observe_response_length_hints(
    body: TestBody,
    probe: Probe,
    expected: ResponseDispatchContext,
    first: &'static str,
) -> TestBody {
    let inspected = std::sync::atomic::AtomicBool::new(false);
    body.probed(probe.clone()).observe_hints(move |event| {
        expected.assert_current();
        if !inspected.swap(true, std::sync::atomic::Ordering::SeqCst) {
            assert_eq!(event, first);
            assert_eq!(probe.count("body poll"), 0);
        }
        probe.record(event);
    })
}

fn exercise_response_length_entities(protocol: CodecProtocol) {
    run_response_context_test(|worker, context| async move {
        let app_context = context.clone();
        let service_probe = Probe::new();
        let (sent, mut received) = mpsc::channel(3);
        let json = b"{\"error\":\"invalid prefix\"}";
        let service = service(service_probe.clone(), move |request| {
            let exchange = request
                .extensions()
                .get::<ServerExchange>()
                .unwrap()
                .clone();
            let data_gate = Gate::default();
            let trailer_gate = Gate::default();
            let probe = Probe::new();
            let dispatch = ResponseDispatchContext::capture(&app_context, probe.clone());
            let (body, status) = match request.uri().path() {
                "/json" => (TestBody::data(json), http::StatusCode::BAD_REQUEST),
                path => {
                    let mut trailers = HeaderMap::new();
                    trailers.append("x-finish", "first".parse().unwrap());
                    trailers.append("x-finish", "second".parse().unwrap());
                    let mut frames = Vec::new();
                    if path == "/data-trailers" {
                        frames.push(Ok(Frame::data(Bytes::from_static(b"first"))));
                        frames.push(Ok(Frame::data(Bytes::from_static(b"second"))));
                    } else {
                        assert_eq!(path, "/zero-trailers");
                    }
                    frames.push(Ok(Frame::trailers(trailers)));
                    (TestBody::frames(frames), http::StatusCode::OK)
                }
            };
            let body = observe_response_length_hints(
                body.exact_size()
                    .gated(data_gate.clone())
                    .gated_trailers(trailer_gate.clone()),
                probe.clone(),
                dispatch.clone(),
                if protocol == CodecProtocol::Http2 {
                    "body terminal hint"
                } else {
                    "body size hint"
                },
            );
            sent.try_send((exchange, data_gate, trailer_gate, probe))
                .unwrap();
            let mut response = Response::builder().status(status);
            if status == http::StatusCode::OK {
                response = response.header("trailer", "x-finish");
            } else {
                response = response.header("content-type", "application/json");
            }
            let response = response.body(body).unwrap();
            assert_eq!(response.version(), http::Version::HTTP_11);
            async move {
                dispatch.observe_future();
                Ok(response)
            }
        });
        let mut peer = ResponseLengthPeer::start(worker, service, protocol, &context).await;
        for (path, expected) in [
            ("/json", json.as_slice()),
            ("/data-trailers", b"firstsecond".as_slice()),
            ("/zero-trailers", b"".as_slice()),
        ] {
            let response = peer
                .running
                .peer
                .send(request(path, TestBody::empty()))
                .await
                .unwrap();
            context.observe("controller restored");
            let (exchange, data_gate, trailer_gate, probe) = received.recv().await.unwrap();
            data_gate.entered().await;
            context.observe("controller restored");
            assert_eq!(response.status(), if path == "/json" { 400 } else { 200 });
            if protocol == CodecProtocol::Http2 {
                assert_eq!(response.version(), http::Version::HTTP_2);
                assert_eq!(
                    response.headers()["content-length"],
                    expected.len().to_string()
                );
                assert!(!response.headers().contains_key("transfer-encoding"));
            } else {
                assert_eq!(response.version(), http::Version::HTTP_11);
                assert!(!response.headers().contains_key("content-length"));
                assert_eq!(response.headers()["transfer-encoding"], "chunked");
            }
            if path == "/json" {
                assert_eq!(response.headers()["content-type"], "application/json");
            }
            let mut body = response.into_body();
            {
                let next = body.frame();
                tokio::pin!(next);
                fixture::assert_pending(next.as_mut()).await;
            }
            data_gate.release();
            let mut data = Vec::new();
            while data.len() < expected.len() {
                let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
                context.observe("controller restored");
                assert!(!frame.is_empty());
                data.extend_from_slice(&frame);
                assert!(data.len() <= expected.len());
            }
            assert_eq!(data, expected);
            if path != "/json" {
                trailer_gate.entered().await;
                context.observe("controller restored");
                let next = body.frame();
                tokio::pin!(next);
                fixture::assert_pending(next.as_mut()).await;
                let completed = exchange.wait_completed();
                tokio::pin!(completed);
                fixture::assert_pending(completed.as_mut()).await;
                assert_eq!(probe.count("body drop"), 0);
                trailer_gate.release();
                let trailers = next.await.unwrap().unwrap().into_trailers().unwrap();
                assert_eq!(trailers.len(), 2);
                assert_eq!(
                    trailers.get_all("x-finish").iter().collect::<Vec<_>>(),
                    vec!["first", "second"]
                );
                completed.await.unwrap();
                context.observe("controller restored");
            }
            assert!(body.frame().await.is_none());
            exchange.wait_completed().await.unwrap();
            context.observe("controller restored");
            assert_eq!(probe.count("body drop"), 1);
            assert_eq!(probe.count("service dispatch context"), 1);
            assert_eq!(probe.count("service future context"), 1);
            for event in [
                "service dispatch context",
                "service future context",
                "body terminal hint",
                "body size hint",
                "body poll",
                "body drop",
            ] {
                probe.assert_app(event);
            }
        }
        peer.finish().await;
        assert_eq!(service_probe.count("service call"), 3);
        assert_eq!(service_probe.count("service drop"), 1);
    });
}

#[test]
fn h2_exact_response_length_preserves_entities_and_trailers() {
    exercise_response_length_entities(CodecProtocol::Http2);
}

#[test]
fn h2_response_length_inference_preserves_explicit_and_unknown_framing() {
    run_response_context_test(|worker, context| async move {
        let app_context = context.clone();
        let (sent, mut received) = mpsc::channel(3);
        let service = service(Probe::new(), move |request| {
            let exchange = request
                .extensions()
                .get::<ServerExchange>()
                .unwrap()
                .clone();
            let path = request.uri().path();
            let probe = Probe::new();
            let dispatch = ResponseDispatchContext::capture(&app_context, probe.clone());
            let mut body = TestBody::data(b"wire");
            let mut response = Response::builder();
            match path {
                "/explicit" => {
                    body = body.exact_size();
                    response = response.header("content-length", "0004");
                }
                "/transfer-encoding" => {
                    body = body.exact_size();
                    response = response.header("transfer-encoding", "chunked");
                }
                _ => assert_eq!(path, "/unknown"),
            }
            let body = observe_response_length_hints(
                body,
                probe.clone(),
                dispatch.clone(),
                if path == "/unknown" {
                    "body terminal hint"
                } else {
                    "body size hint"
                },
            );
            sent.try_send((exchange, probe)).unwrap();
            async move {
                dispatch.observe_future();
                Ok(response.body(body).unwrap())
            }
        });
        let mut peer =
            ResponseLengthPeer::start(worker, service, CodecProtocol::Http2, &context).await;
        for path in ["/explicit", "/transfer-encoding", "/unknown"] {
            let response = peer
                .running
                .peer
                .send(request(path, TestBody::empty()))
                .await
                .unwrap();
            context.observe("controller restored");
            assert_eq!(response.status(), 200);
            assert_eq!(response.version(), http::Version::HTTP_2);
            if path == "/explicit" {
                assert_eq!(response.headers()["content-length"], "0004");
            } else {
                assert!(!response.headers().contains_key("content-length"));
            }
            assert!(!response.headers().contains_key("transfer-encoding"));
            let body = response.into_body().collect().await.unwrap();
            context.observe("controller restored");
            assert!(body.trailers().is_none());
            assert_eq!(body.to_bytes(), b"wire".as_slice());
            let (exchange, probe) = received.recv().await.unwrap();
            exchange.wait_completed().await.unwrap();
            context.observe("controller restored");
            assert_eq!(probe.count("body drop"), 1);
            assert_eq!(probe.count("service dispatch context"), 1);
            assert_eq!(probe.count("service future context"), 1);
            probe.assert_app("service dispatch context");
            probe.assert_app("service future context");
            probe.assert_app("body size hint");
            probe.assert_app("body drop");
        }
        peer.finish().await;
    });
    for phase in ["body terminal hint", "body size hint"] {
        run_response_context_test(|worker, context| async move {
            let app_context = context.clone();
            let marker = Arc::new(());
            let body_marker = Arc::clone(&marker);
            let probe = Probe::new();
            let body_probe = probe.clone();
            let (sent, mut received) = mpsc::channel(1);
            let service = service(Probe::new(), move |request| {
                sent.try_send(
                    request
                        .extensions()
                        .get::<ServerExchange>()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                let dispatch = ResponseDispatchContext::capture(&app_context, body_probe.clone());
                let expected = dispatch.clone();
                let marker = Arc::clone(&body_marker);
                let inspected = body_probe.clone();
                let body = TestBody::data(b"must-not-be-polled")
                    .exact_size()
                    .probed(body_probe.clone())
                    .observe_hints(move |event| {
                        expected.assert_current();
                        inspected.record(event);
                        if event == phase {
                            std::panic::panic_any(Arc::clone(&marker));
                        }
                    });
                async move {
                    dispatch.observe_future();
                    Ok(Response::new(body))
                }
            });
            let mut peer =
                ResponseLengthPeer::start(worker, service, CodecProtocol::Http2, &context).await;
            peer.shutdown.release();
            let mut requests = fixture::FixtureTasks::new();
            requests.spawn(
                peer.running
                    .peer
                    .send(request("/hint-panic", TestBody::empty())),
            );
            let exchange = received.recv().await.unwrap();
            let error = exchange.wait_completed().await.unwrap_err();
            context.observe("controller restored");
            assert!(matches!(error.cause(), ServerFailure::Panic(_)));
            assert!(!error.cleanup_confirmed());
            assert!(Arc::ptr_eq(
                &error.first_panic().unwrap().cloned::<Arc<()>>().unwrap(),
                &marker
            ));
            let result = peer.running.parent.reap().await.unwrap_err();
            context.observe("controller restored");
            assert!(matches!(result.cause(), ServerFailure::Panic(_)));
            assert!(!result.cleanup_confirmed());
            assert!(Arc::ptr_eq(
                &result.first_panic().unwrap().cloned::<Arc<()>>().unwrap(),
                &marker
            ));
            let repeated = exchange.wait_completed().await.unwrap_err();
            context.observe("controller restored");
            assert!(!repeated.cleanup_confirmed());
            assert!(Arc::ptr_eq(
                &repeated.first_panic().unwrap().cloned::<Arc<()>>().unwrap(),
                &marker
            ));
            drop(requests.join_next().await.unwrap().unwrap());
            let _peer_retirement = peer.completed.await.unwrap();
            drop(peer.running.peer);
            peer.running.peers.finish().await;
            context.observe("controller restored");
            assert!(requests.is_empty());
            assert!(peer.running.parent.tasks.is_empty());
            assert!(peer.running.parent.slots.is_closed());
            assert_eq!(peer.running.io.count("io drop"), 1);
            assert_eq!(probe.count("body poll"), 0);
            assert_eq!(probe.count(phase), 1);
            assert_eq!(probe.count("body drop"), 1);
            assert_eq!(probe.count("service dispatch context"), 1);
            assert_eq!(probe.count("service future context"), 1);
            probe.assert_app("service dispatch context");
            probe.assert_app("service future context");
            probe.assert_app(phase);
            probe.assert_app("body drop");
        });
    }
}

#[test]
fn h2_response_length_inference_preserves_no_body_semantics() {
    run_response_context_test(|worker, context| async move {
        let app_context = context.clone();
        let (sent, mut received) = mpsc::channel(7);
        let service = service(Probe::new(), move |request| {
            let exchange = request
                .extensions()
                .get::<ServerExchange>()
                .unwrap()
                .clone();
            let path = request.uri().path();
            let status = match path {
                "/204" => 204,
                "/205" => 205,
                "/304" => 304,
                _ if request.method() == http::Method::CONNECT => {
                    assert_eq!(
                        request.uri().authority().unwrap().as_str(),
                        "fixture.invalid:443"
                    );
                    assert!(request.uri().scheme().is_none());
                    assert!(request.uri().path_and_query().is_none());
                    403
                }
                _ => 200,
            };
            let body = if status == 403 {
                TestBody::data(b"no tunnel")
            } else {
                TestBody::empty()
            };
            let probe = Probe::new();
            let dispatch = ResponseDispatchContext::capture(&app_context, probe.clone());
            let body = observe_response_length_hints(
                body.exact_size(),
                probe.clone(),
                dispatch.clone(),
                if path == "/terminal" || status == 403 {
                    "body terminal hint"
                } else {
                    "body size hint"
                },
            );
            let mut response = Response::builder().status(status);
            if path == "/head-explicit" {
                response = response.header("content-length", "21");
            }
            sent.try_send((exchange, probe)).unwrap();
            async move {
                dispatch.observe_future();
                Ok(response.body(body).unwrap())
            }
        });
        let mut peer =
            ResponseLengthPeer::start(worker, service, CodecProtocol::Http2, &context).await;
        for (path, method, status) in [
            ("/204", http::Method::GET, 204),
            ("/205", http::Method::GET, 205),
            ("/304", http::Method::GET, 304),
            ("/head", http::Method::HEAD, 200),
            ("/head-explicit", http::Method::HEAD, 200),
            ("fixture.invalid:443", http::Method::CONNECT, 403),
            ("/terminal", http::Method::GET, 200),
        ] {
            let mut outgoing = request(path, TestBody::empty());
            if method == http::Method::CONNECT {
                outgoing.headers_mut().remove("host");
            }
            *outgoing.method_mut() = method;
            let response = peer.running.peer.send(outgoing).await.unwrap();
            context.observe("controller restored");
            assert_eq!(response.status(), status);
            assert_eq!(response.version(), http::Version::HTTP_2);
            if path == "/head-explicit" {
                assert_eq!(response.headers()["content-length"], "21");
            } else if status == 403 {
                assert_eq!(response.headers()["content-length"], "9");
            } else {
                assert!(!response.headers().contains_key("content-length"));
            }
            assert!(!response.headers().contains_key("transfer-encoding"));
            let body = response.into_body().collect().await.unwrap();
            context.observe("controller restored");
            assert!(body.trailers().is_none());
            assert_eq!(
                body.to_bytes(),
                if status == 403 {
                    b"no tunnel".as_slice()
                } else {
                    b"".as_slice()
                }
            );
            let (exchange, probe) = received.recv().await.unwrap();
            exchange.wait_completed().await.unwrap();
            context.observe("controller restored");
            assert_eq!(probe.count("body drop"), 1);
            assert_eq!(probe.count("service dispatch context"), 1);
            assert_eq!(probe.count("service future context"), 1);
            probe.assert_app("service dispatch context");
            probe.assert_app("service future context");
            if status == 403 {
                probe.assert_app("body terminal hint");
            }
            if status == 403 || status == 205 || path == "/terminal" {
                probe.assert_app("body size hint");
            } else {
                assert_eq!(probe.count("body terminal hint"), 0);
                assert_eq!(probe.count("body size hint"), 0);
                assert_eq!(probe.count("body poll"), 0);
            }
            probe.assert_app("body drop");
        }
        peer.finish().await;
    });
}

#[test]
fn h1_exact_response_trailers_do_not_infer_length() {
    exercise_response_length_entities(CodecProtocol::Http11);
}

#[test]
fn h1_bodyless_responses_suppress_application_frames_and_preserve_reuse() {
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::io::BufReader;
    use tokio::net::TcpStream;

    async fn read_head(peer: &mut BufReader<TcpStream>, method: &str, status: u16) -> String {
        let mut head = String::new();
        loop {
            let mut line = String::new();
            assert_ne!(
                peer.read_line(&mut line).await.unwrap(),
                0,
                "EOF during {method} {status}: {head:?}"
            );
            assert!(head.len() + line.len() <= 8192);
            head.push_str(&line);
            if line == "\r\n" {
                return head;
            }
        }
    }

    for (method, status) in [("HEAD", 200), ("GET", 100), ("GET", 204), ("GET", 304)] {
        run_test(|worker, _| async move {
            let io = Probe::new();
            let (established, peer) = fixture::sockets(io.clone(), None).await;
            let probe = Probe::new();
            let body_probe = probe.clone();
            let service = service(Probe::new(), move |request| {
                let response = if request.uri().path() == "/bodyless" {
                    Response::builder()
                        .status(status)
                        .header("content-length", "5")
                        .header("transfer-encoding", "chunked")
                        .body(
                            TestBody::data(b"must-not-reach-wire")
                                .gated(Gate::default())
                                .probed(body_probe.clone())
                                .observe_hints(|_| {
                                    panic!("bodyless response must not inspect its body")
                                }),
                        )
                        .unwrap()
                } else {
                    assert_eq!(request.uri().path(), "/after");
                    Response::builder()
                        .header("content-length", "5")
                        .body(TestBody::data(b"after"))
                        .unwrap()
                };
                async move { Ok(response) }
            });
            let mut parent = fixture::Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = crate::server::prepare_connection(
                worker,
                established,
                service,
                ServerOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext),
            )
            .unwrap();
            parent.spawn(run, permit);
            let _connection = startup.start().await.unwrap();
            let mut peer = BufReader::new(peer);
            peer.get_mut()
                .write_all(
                    format!("{method} /bodyless HTTP/1.1\r\nHost: fixture.invalid\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            if status == 100 {
                let mut wire = Vec::new();
                peer.read_to_end(&mut wire).await.unwrap();
                assert!(wire.is_empty(), "{wire:?}");
                let error = parent.reap().await.unwrap_err();
                assert!(
                    matches!(error.cause(), ServerFailure::Native(_)),
                    "{error:?}"
                );
                assert!(error.cleanup_confirmed());
            } else {
                let head = read_head(&mut peer, method, status)
                    .await
                    .to_ascii_lowercase();
                assert!(head.starts_with(&format!("http/1.1 {status} ")), "{head:?}");
                assert!(!head.contains("transfer-encoding:"));
                assert_eq!(
                    head.contains("content-length: 5\r\n"),
                    method == "HEAD" || status == 304
                );
                peer.get_mut()
                    .write_all(b"GET /after HTTP/1.1\r\nHost: fixture.invalid\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let head = read_head(&mut peer, "GET", 200).await;
                assert!(head.starts_with("HTTP/1.1 200 "), "{head:?}");
                let mut body = Vec::new();
                peer.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, b"after");
                parent.reap().await.unwrap();
            }
            assert_eq!(probe.count("body poll"), 0);
            assert_eq!(probe.count("body drop"), 1);
            probe.assert_app("body drop");
            assert_eq!(io.count("io drop"), 1);
            io.assert_native("io drop");
        });
    }
}

#[test]
fn server_connection_switch_rejection_status_validation() {
    for status in [400, 403, 499, 500, 599] {
        let status = http::StatusCode::from_u16(status).unwrap();
        assert_eq!(
            Http1ConnectionSwitchRejection::new(status)
                .unwrap()
                .status(),
            status
        );
    }
    for status in [100, 101, 200, 204, 299, 300, 399, 600, 999] {
        let status = http::StatusCode::from_u16(status).unwrap();
        assert_eq!(
            Http1ConnectionSwitchRejection::new(status)
                .unwrap_err()
                .status(),
            status
        );
    }
    assert_eq!(
        Http1ConnectionSwitchRejection::forbidden().status(),
        http::StatusCode::FORBIDDEN
    );
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        assert_eq!(
            ServerOptions::new(protocol, TransportSecurity::Plaintext)
                .http1_connection_switch_rejection,
            None
        );
    }
}

#[test]
fn server_http1_connection_switch_rejection_plain_and_tls() {
    use std::num::NonZeroUsize;

    use tokio::io::AsyncWriteExt;

    use self::fixture::Parent;
    use self::fixture::RejectionIoControl;
    use self::fixture::SwitchRequest;
    use self::fixture::observe_rejection;
    use self::fixture::read_rejection;
    use self::fixture::rejection_sockets;
    use self::fixture::switch_request;
    use crate::server::prepare_connection;

    for tls in [false, true] {
        for version in ["1.0", "1.1"] {
            for kind in [
                SwitchRequest::Connect,
                SwitchRequest::WebSocket,
                SwitchRequest::OtherUpgrade,
            ] {
                run_test(|worker, _| async move {
                    let io = RejectionIoControl::new();
                    io.write.release();
                    io.shutdown.release();
                    let (established, mut peer) = rejection_sockets(tls, io.clone(), None).await;
                    let probe = Probe::new();
                    let (ingress, watcher) =
                        observe_rejection(worker.clone(), probe.clone(), None).await;
                    let mut parent = Parent::new(1);
                    let permit = parent.reserve();
                    let mut options = ServerOptions::new(
                        CodecProtocol::Http11,
                        if tls {
                            TransportSecurity::Tls
                        } else {
                            TransportSecurity::Plaintext
                        },
                    );
                    let status = if version == "1.0" {
                        http::StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        http::StatusCode::FORBIDDEN
                    };
                    options.http1_connection_switch_rejection =
                        Some(Http1ConnectionSwitchRejection::new(status).unwrap());
                    options.max_exchanges = NonZeroUsize::new(1).unwrap();
                    options.dispatch_capacity = NonZeroUsize::new(1).unwrap();
                    let (startup, run) = prepare_connection(
                        worker,
                        established,
                        service(probe.clone(), |_| async {
                            panic!("connection-switch rejection must not invoke the service")
                        }),
                        options,
                    )
                    .unwrap();
                    parent.spawn(run, permit);
                    let connection = startup.start().await.unwrap();
                    let request = switch_request(kind, version, true);
                    eprintln!(
                        "rejection wire case: tls={tls}, HTTP/{version}, kind={kind:?}, expected={status}; request={:?}",
                        String::from_utf8_lossy(&request),
                    );
                    peer.write_all(&request).await.unwrap();
                    peer.flush().await.unwrap();
                    read_rejection(&mut peer, status.as_u16(), version).await;
                    ingress.await.unwrap();
                    watcher.await.unwrap();
                    connection.wait_app_leaves_drained().await.unwrap();
                    parent.reap().await.unwrap();
                    assert!(parent.tasks.is_empty());
                    assert_eq!(parent.slots.available_permits(), 1);
                    assert_eq!(probe.count("service ready"), 0);
                    assert_eq!(probe.count("service call"), 0);
                    assert_eq!(probe.count("service drop"), 1);
                    assert_eq!(probe.count("native request head"), 1);
                    assert_eq!(
                        probe.count("native upgraded flag"),
                        usize::from(matches!(kind, SwitchRequest::Connect))
                    );
                    assert_eq!(
                        probe.count("native websocket flag"),
                        usize::from(matches!(kind, SwitchRequest::WebSocket))
                    );
                    assert_eq!(
                        probe.count("request source stop"),
                        1 - probe.count("native request eom")
                    );
                    assert_eq!(probe.count("request source drop"), 1);
                    probe.assert_native("request source drop");
                    probe.assert_app("service drop");
                    assert_eq!(io.probe.count("adopted io drop"), 1);
                    assert_eq!(io.probe.count("io drop"), 1);
                    io.probe.assert_native("adopted io drop");
                    drop(peer);
                });
            }
        }
    }
}

#[test]
fn server_connection_switch_rejection_preserves_default_and_other_errors() {
    use roxygen_core::headers::HttpHeaders;
    use roxygen_core::message::HttpMessage;
    use roxygen_http_adapter::HttpAdapterError;
    use roxygen_http_adapter::UnsupportedMessageMetadata;
    use tokio::io::AsyncWriteExt;

    use self::fixture::Parent;
    use self::fixture::PeerConnection;
    use self::fixture::PeerTasks;
    use self::fixture::RejectionIoControl;
    use self::fixture::SwitchRequest;
    use self::fixture::observe_rejection;
    use self::fixture::pending_rejection_read;
    use self::fixture::rejection_sockets;
    use self::fixture::sockets;
    use self::fixture::switch_request;
    use crate::server::prepare_connection;

    let mutations: [Option<fn(&mut HttpMessage)>; 11] = [
        None,
        Some(|message| message.set_egress_websocket_upgrade()),
        Some(|message| message.set_upgrade_protocol(b"websocket")),
        Some(|message| message.set_masque()),
        Some(|message| message.set_trailers(Box::new(HttpHeaders::new()))),
        Some(|message| message.set_http_version(3, 0)),
        Some(|message| {
            message.set_url(b"invalid uri", false).unwrap();
        }),
        Some(|message| {
            message.set_url(b"https://fixture.invalid/", false).unwrap();
        }),
        Some(|message| {
            message.headers_mut().add(b"x-invalid", b"invalid\r\nvalue");
        }),
        Some(|message| {
            message.set_method("invalid method").unwrap();
        }),
        Some(|message| {
            message.headers_mut().add(b"invalid header", b"value");
        }),
    ];
    for (index, mutation) in mutations.into_iter().enumerate() {
        run_test(|worker, _| async move {
            let io = RejectionIoControl::new();
            io.shutdown.release();
            let (established, mut peer) = rejection_sockets(false, io.clone(), None).await;
            let probe = Probe::new();
            let (ingress, watcher) =
                observe_rejection(worker.clone(), probe.clone(), mutation).await;
            let mut options =
                ServerOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
            if index != 0 {
                options.http1_connection_switch_rejection =
                    Some(Http1ConnectionSwitchRejection::forbidden());
            }
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(
                worker,
                established,
                service(probe.clone(), |_| async {
                    Ok(Response::new(TestBody::empty()))
                }),
                options,
            )
            .unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            peer.write_all(&switch_request(SwitchRequest::Connect, "1.1", false))
                .await
                .unwrap();
            peer.flush().await.unwrap();
            io.write.entered().await;
            let mut readers = pending_rejection_read(peer).await;
            connection.begin_drain();
            let leaf = connection.wait_app_leaves_drained().await.unwrap_err();
            assert!(
                matches!(
                    leaf.cause(),
                    ServerFailure::Projection(HttpAdapterError::UnsupportedMessageMetadata(
                        UnsupportedMessageMetadata::Upgraded
                    ))
                ),
                "case {index}: {leaf:?}"
            );
            assert_eq!(probe.count("native upgraded flag"), 1);
            assert_eq!(probe.count("service ready"), 0);
            assert_eq!(probe.count("service call"), 0);
            assert_eq!(parent.slots.available_permits(), 0);
            readers.abort_all();
            assert!(
                readers
                    .join_next()
                    .await
                    .unwrap()
                    .unwrap_err()
                    .is_cancelled()
            );
            assert!(readers.is_empty());
            io.write.release();
            let result = parent.reap().await.unwrap_err();
            assert!(
                matches!(
                    result.cause(),
                    ServerFailure::Projection(HttpAdapterError::UnsupportedMessageMetadata(
                        UnsupportedMessageMetadata::Upgraded
                    ))
                ),
                "case {index}: {result:?}"
            );
            ingress.await.unwrap();
            watcher.await.unwrap();
            assert!(parent.tasks.is_empty());
            assert_eq!(probe.count("request source drop"), 1);
            assert_eq!(probe.count("service drop"), 1);
            assert_eq!(io.probe.count("adopted io drop"), 1);
            probe.assert_native("request source drop");
            probe.assert_app("service drop");
        });
    }
    for enabled in [false, true] {
        run_test(|worker, _| async move {
            let probe = Probe::new();
            let (ingress, watcher) = observe_rejection(
                worker.clone(),
                probe.clone(),
                Some(|message| message.set_upgrade_protocol(b"websocket")),
            )
            .await;
            let mut options =
                ServerOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
            options.http1_connection_switch_rejection =
                enabled.then(Http1ConnectionSwitchRejection::forbidden);
            let io = Probe::new();
            let (established, stream) = sockets(io.clone(), None).await;
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(
                worker,
                established,
                service(probe.clone(), |_| async {
                    panic!("unsupported H2 metadata must not reach the service")
                }),
                options,
            )
            .unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            let mut peers = PeerTasks::new();
            let peer_shutdown = Gate::default();
            let peer_io = Probe::new();
            let (mut peer, completed) = PeerConnection::connect_gated(
                stream,
                CodecProtocol::Http2,
                &peers,
                peer_shutdown.clone(),
                peer_io.clone(),
            )
            .await;
            assert_native_error_response(
                peer.send(request("/unsupported-protocol", TestBody::empty()))
                    .await
                    .unwrap(),
                b"application server exchange cancelled",
            )
            .await;
            assert!(connection.begin_drain());
            let leaf = connection.wait_app_leaves_drained().await.unwrap_err();
            assert!(
                matches!(
                    leaf.cause(),
                    ServerFailure::Projection(HttpAdapterError::UnsupportedMessageMetadata(
                        UnsupportedMessageMetadata::UpgradeProtocol
                    ))
                ),
                "option enabled={enabled}: {leaf:?}"
            );
            let result = parent.reap().await.unwrap_err();
            assert!(
                matches!(
                    result.cause(),
                    ServerFailure::Projection(HttpAdapterError::UnsupportedMessageMetadata(
                        UnsupportedMessageMetadata::UpgradeProtocol
                    ))
                ),
                "option enabled={enabled}: {result:?}"
            );
            assert!(result.cleanup_confirmed());
            ingress.await.unwrap();
            watcher.await.unwrap();
            assert_eq!(probe.count("native request head"), 1);
            assert_eq!(probe.count("service ready"), 0);
            assert_eq!(probe.count("service call"), 0);
            assert_eq!(probe.count("service drop"), 1);
            assert_eq!(probe.count("request source drop"), 1);
            assert_eq!(io.count("io drop"), 1);
            assert_eq!(peer_io.count("io drop"), 0);
            peer_shutdown.entered().await;
            peer_shutdown.release();
            completed.await.unwrap().unwrap();
            drop(peer);
            peers.finish().await;
            assert_eq!(peer_io.count("io drop"), 1);
            assert!(parent.tasks.is_empty());
            assert_eq!(parent.slots.available_permits(), 1);
            assert!(!parent.slots.is_closed());
            probe.assert_native("request source drop");
            probe.assert_app("service drop");
            io.assert_native("io drop");
            peer_io.assert_app("io drop");
        });
    }
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_test(|worker, _| async move {
            let probe = Probe::new();
            let service = service(probe.clone(), |request| {
                let context = request
                    .extensions()
                    .get::<Arc<ConnectionContext>>()
                    .unwrap();
                assert_eq!(context.security(), TransportSecurity::Plaintext);
                async {
                    Ok(Response::builder()
                        .status(202)
                        .body(TestBody::data(b"ordinary response"))
                        .unwrap())
                }
            });
            let mut options = ServerOptions::new(protocol, TransportSecurity::Plaintext);
            options.http1_connection_switch_rejection =
                Some(Http1ConnectionSwitchRejection::forbidden());
            let io = Probe::new();
            let (established, stream) = sockets(io.clone(), None).await;
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(worker, established, service, options).unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            let mut peers = PeerTasks::new();
            let peer_shutdown = Gate::default();
            let (mut peer, completed) = PeerConnection::connect_gated(
                stream,
                protocol,
                &peers,
                peer_shutdown.clone(),
                Probe::new(),
            )
            .await;
            let response = peer
                .send(request("/ordinary", TestBody::empty()))
                .await
                .unwrap();
            assert_eq!(response.status(), 202);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                b"ordinary response".as_slice()
            );
            assert!(connection.begin_drain());
            parent.reap().await.unwrap();
            peer_shutdown.entered().await;
            peer_shutdown.release();
            completed.await.unwrap().unwrap();
            drop(peer);
            peers.finish().await;
            assert_eq!(probe.count("service call"), 1);
            assert_eq!(io.count("io drop"), 1);
            assert!(parent.tasks.is_empty());
            assert_eq!(parent.slots.available_permits(), 1);
        });
    }
}

#[test]
fn server_streaming_frames_and_application_affinity() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_test(|worker, _| async move {
            let probe = Probe::new();
            let body_probe = probe.clone();
            let (sent, mut exchanges) = mpsc::channel(1);
            let service = service(probe.clone(), move |request| {
                sent.try_send(
                    request
                        .extensions()
                        .get::<ServerExchange>()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                let body = TestBody::echo(request.into_body(), body_probe.clone());
                async move {
                    Ok(Response::builder()
                        .header("trailer", "x-finish")
                        .body(body)
                        .unwrap())
                }
            });
            let mut options = ServerOptions::new(protocol, TransportSecurity::Plaintext);
            options.request_body.maximum_read = NonZeroU32::new(3).unwrap();
            options.response_body.maximum_demand_bytes = NonZeroU32::new(2).unwrap();
            let mut running = start(worker, service, options, None).await;
            let mut trailers = HeaderMap::new();
            trailers.insert("x-finish", "streamed".parse().unwrap());
            let body = TestBody::frames(vec![
                Ok(Frame::data(Bytes::from_static(b"first"))),
                Ok(Frame::data(Bytes::from_static(b"second"))),
                Ok(Frame::trailers(trailers)),
            ]);
            let response = running.peer.send(request("/stream", body)).await.unwrap();
            let collected = match response.into_body().collect().await {
                Ok(collected) => collected,
                Err(peer_error) => {
                    let exchange = exchanges.recv().await.unwrap();
                    let outcome =
                        tokio::time::timeout(fixture::TIMEOUT / 2, exchange.wait_completed()).await;
                    panic!("{protocol:?} peer body failed: {peer_error:?}; exchange: {outcome:?}");
                }
            };
            assert_eq!(collected.trailers().unwrap()["x-finish"], "streamed");
            assert_eq!(collected.to_bytes(), b"firstsecond".as_slice());
            let result = running.finish().await;
            assert!(result.is_ok() || result.unwrap_err().cleanup_confirmed());
            for event in [
                "service ready",
                "service call",
                "service future drop",
                "service drop",
                "body poll",
                "body drop",
            ] {
                probe.assert_app(event);
            }
            assert_eq!(probe.count("body drop"), 1);
        });
    }
}

#[test]
fn server_trusted_context_and_original_errors() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        for body_error in [false, true] {
            run_test(|worker, _| async move {
                let marker = Arc::new(());
                let original = Arc::clone(&marker);
                let (observed, mut receiver) = mpsc::channel(1);
                let body_gate = Gate::default();
                let failed_body = body_gate.clone();
                let service = service(Probe::new(), move |request| {
                    let exchange = request
                        .extensions()
                        .get::<ServerExchange>()
                        .unwrap()
                        .clone();
                    let context = request
                        .extensions()
                        .get::<Arc<ConnectionContext>>()
                        .unwrap()
                        .clone();
                    assert_eq!(request.headers()["x-peer-identity"], "forged");
                    observed.try_send((exchange, context)).unwrap();
                    let error = TestError(Arc::clone(&original));
                    let failed_body = failed_body.clone();
                    async move {
                        if body_error {
                            Ok(Response::new(
                                TestBody::frames(vec![Err(error)]).gated(failed_body),
                            ))
                        } else {
                            Err(error)
                        }
                    }
                });
                let mut options = ServerOptions::new(protocol, TransportSecurity::Plaintext);
                options.upstream_origin = Some(UpstreamOrigin::new(
                    "secure.origin:443",
                    OriginSecurity::Https,
                ));
                let mut running = start(worker, service, options, None).await;
                let mut request = request("/origin-form", TestBody::empty());
                request
                    .headers_mut()
                    .insert("x-peer-identity", "forged".parse().unwrap());
                let response = running.peer.send(request).await.unwrap();
                if body_error {
                    assert_eq!(response.status(), http::StatusCode::OK);
                    body_gate.entered().await;
                    body_gate.release();
                    assert!(response.into_body().collect().await.is_err());
                } else {
                    assert_native_error_response(
                        response,
                        b"application server exchange cancelled",
                    )
                    .await;
                }
                let (exchange, context) = receiver.recv().await.unwrap();
                assert_eq!(
                    context.peer_identity(),
                    Some(b"verified-fixture-peer".as_slice())
                );
                assert_eq!(context.security(), TransportSecurity::Plaintext);
                assert_eq!(
                    context.upstream_origin().unwrap().security(),
                    OriginSecurity::Https
                );
                assert_eq!(
                    context.upstream_origin().unwrap().authority(),
                    "secure.origin:443"
                );
                assert!(context.peer_endpoint().socket_address().is_some());
                let error = exchange.wait_completed().await.unwrap_err();
                let typed = match error.cause() {
                    ServerFailure::Service(error) => error.downcast_ref::<TestError>().unwrap(),
                    ServerFailure::Body(ApplicationBodyError::Application(error)) => {
                        error.downcast_ref::<TestError>().unwrap()
                    }
                    other => panic!("lost original application error: {other:?}"),
                };
                assert!(Arc::ptr_eq(&typed.0, &marker));
                let _ = running.finish().await;
            });
        }
    }
}

#[test]
fn server_early_response_survives_ingress_drop() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_test(|worker, _| async move {
            let response_gate = Gate::default();
            let upload_gate = Gate::default();
            let upload_probe = Probe::new();
            let gate = response_gate.clone();
            let service = service(Probe::new(), move |request| {
                drop(request);
                let body = TestBody::data(b"early-response").gated(gate.clone());
                async move { Ok(Response::new(body)) }
            });
            let mut running = start(
                worker,
                service,
                ServerOptions::new(protocol, TransportSecurity::Plaintext),
                None,
            )
            .await;
            let response = running
                .peer
                .send(request(
                    "/early",
                    TestBody::data(b"unfinished-upload")
                        .gated(upload_gate.clone())
                        .probed(upload_probe.clone()),
                ))
                .await
                .unwrap();
            upload_gate.entered().await;
            response_gate.entered().await;
            response_gate.release();
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                b"early-response".as_slice()
            );
            assert!(!upload_gate.is_released());
            assert_eq!(upload_probe.count("body eof"), 0);
            upload_gate.release();
            let result = running.finish().await;
            assert!(result.is_ok() || result.unwrap_err().cleanup_confirmed());
        });
    }
}

#[test]
fn server_h2_cancellation_preserves_sibling() {
    for remote_reset in [false, true] {
        run_test(|worker, _| async move {
            let gate = Gate::default();
            let pending_gate = gate.clone();
            let (sender, mut exchanges) = mpsc::channel(2);
            let service = service(Probe::new(), move |request| {
                let pending = request.uri().path() == "/cancel";
                sender
                    .try_send((
                        pending,
                        request
                            .extensions()
                            .get::<ServerExchange>()
                            .unwrap()
                            .clone(),
                    ))
                    .unwrap();
                let gate = pending_gate.clone();
                async move {
                    if pending {
                        gate.wait().await;
                    }
                    Ok(Response::new(TestBody::data(b"healthy-sibling")))
                }
            });
            let mut running = start(
                worker,
                service,
                ServerOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext),
                None,
            )
            .await;
            let cancelled = running.peer.send(request("/cancel", TestBody::empty()));
            let healthy = running.peer.send(request("/healthy", TestBody::empty()));
            let mut requests = fixture::FixtureTasks::new();
            requests.spawn(cancelled);
            let response = healthy.await.unwrap();
            let first = exchanges.recv().await.unwrap();
            let second = exchanges.recv().await.unwrap();
            let cancelled_exchange = if first.0 { first.1 } else { second.1 };
            gate.entered().await;
            if remote_reset {
                requests.abort_all();
            } else {
                cancelled_exchange.cancel();
            }
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                b"healthy-sibling".as_slice()
            );
            let joined = requests.join_next().await.unwrap();
            if remote_reset {
                assert!(joined.unwrap_err().is_cancelled());
            } else {
                assert_native_error_response(
                    joined.unwrap().unwrap(),
                    b"application server exchange cancelled",
                )
                .await;
            }
            assert!(cancelled_exchange.wait_completed().await.is_err());
            assert!(requests.is_empty());
            let response = running
                .peer
                .send(request("/still-healthy", TestBody::empty()))
                .await
                .unwrap();
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                b"healthy-sibling".as_slice()
            );
            let _ = running.finish().await;
        });
    }
}
